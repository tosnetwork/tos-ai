package containerdbackend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const managedLabel = "tos.ai.execution"

type sdkEngine struct {
	client         *containerd.Client
	namespace      string
	snapshotter    string
	fifoDir        string
	cleanupTimeout time.Duration
	imageReference string
	imageDigest    string
}

func newSDKEngine(config Config) (*sdkEngine, error) {
	client, err := containerd.New(
		config.SocketPath,
		containerd.WithDefaultNamespace(config.Namespace),
		containerd.WithDefaultRuntime(config.Runtime),
		containerd.WithTimeout(config.OperationTimeout),
	)
	if err != nil {
		return nil, err
	}
	return &sdkEngine{
		client: client, namespace: config.Namespace,
		snapshotter: config.Snapshotter, fifoDir: config.FIFODir,
		cleanupTimeout: config.CleanupTimeout,
		imageReference: config.ImageReference, imageDigest: config.ImageDigest,
	}, nil
}

func (e *sdkEngine) runtimeContext(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, e.namespace)
}

func (e *sdkEngine) CheckReady(ctx context.Context) error {
	if e == nil || e.client == nil {
		return errors.New("invalid containerd client")
	}
	_, err := e.client.Version(e.runtimeContext(ctx))
	return err
}

func (e *sdkEngine) CheckResidue(ctx context.Context) error {
	ctx = e.runtimeContext(ctx)
	containers, err := e.client.Containers(ctx, `id~=^[0-9a-f]{64}$`)
	if err != nil {
		return err
	}
	if len(containers) != 0 {
		return errors.New("managed container residue exists")
	}
	found := false
	err = e.client.SnapshotService(e.snapshotter).Walk(
		ctx,
		func(_ context.Context, info snapshots.Info) error {
			if len(info.Name) == 64 {
				found = true
			}
			return nil
		},
		`name~=^[0-9a-f]{64}$`,
	)
	if err != nil {
		return err
	}
	if found {
		return errors.New("managed snapshot residue exists")
	}
	return nil
}

func (e *sdkEngine) Run(
	ctx context.Context,
	id string,
	request executor.ContainerRequest,
	input []byte,
) (result executor.Result, returnedErr error) {
	ctx = e.runtimeContext(ctx)
	started := time.Now()
	image, err := e.resolveImage(ctx, request.ImageDigest)
	if err != nil {
		return executor.Result{}, err
	}
	unpacked, err := image.IsUnpacked(ctx, e.snapshotter)
	if err != nil || !unpacked {
		return executor.Result{}, errors.New("container image is not unpacked")
	}
	if err := e.ensureIdentityAbsent(ctx, id); err != nil {
		return executor.Result{}, err
	}

	container, err := e.client.NewContainer(
		ctx, id,
		containerd.WithSnapshotter(e.snapshotter),
		containerd.WithImage(image),
		containerd.WithContainerLabels(map[string]string{managedLabel: "v1"}),
		// containerd views are metadata-only read-only mounts and cannot back a
		// runnable task on the supported overlayfs/runc v2 stack. Create an
		// active snapshot, then enforce the immutable root at the OCI layer with
		// spec.Root.Readonly, empty capabilities, no-new-privileges and seccomp.
		// The digest-derived snapshot is still removed synchronously below.
		containerd.WithNewSnapshot(id, image),
		// WithImageConfig reads the new snapshot metadata. Containerd options
		// execute in order, so specification construction must follow snapshot
		// creation or it fails before runc starts.
		containerd.WithNewSpec(oci.WithImageConfig(image), fixedIsolationSpec(request)),
	)
	if err != nil {
		cleanupErr := e.removeFailedSnapshot(id)
		return executor.Result{}, errors.Join(err, cleanupErr)
	}
	var task containerd.Task
	defer func() {
		cleanupErr := e.cleanup(id, task, container)
		if cleanupErr != nil {
			result = executor.Result{}
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()

	output := newBoundedOutput(request.Limits.OutputBytes)
	task, err = container.NewTask(ctx, cio.NewCreator(
		cio.WithFIFODir(e.fifoDir),
		cio.WithStreams(bytes.NewReader(input), output, output),
	))
	if err != nil {
		return executor.Result{}, err
	}
	exitStatuses, err := task.Wait(ctx)
	if err != nil {
		return executor.Result{}, err
	}
	if err := task.Start(ctx); err != nil {
		return executor.Result{}, err
	}
	var status containerd.ExitStatus
	select {
	case <-ctx.Done():
		return executor.Result{}, ctx.Err()
	case received, open := <-exitStatuses:
		if !open {
			return executor.Result{}, errors.New("containerd task wait closed")
		}
		status = received
	}
	exitCode, _, err := status.Result()
	if err != nil {
		return executor.Result{}, err
	}
	if output.Exceeded() {
		return executor.Result{}, errors.New("container output exceeds limit")
	}
	duration := time.Since(started)
	metric, err := task.Metrics(ctx)
	if err != nil || metric.ID != id {
		return executor.Result{}, errors.New("collect container task metrics")
	}
	usage, err := usageFromMetric(metric, duration)
	if err != nil {
		return executor.Result{}, err
	}
	return executor.Result{
		ExitCode: int(exitCode), Output: output.Bytes(),
		Usage: usage,
	}, nil
}

func (e *sdkEngine) removeFailedSnapshot(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.cleanupTimeout)
	defer cancel()
	ctx = e.runtimeContext(ctx)
	err := e.client.SnapshotService(e.snapshotter).Remove(ctx, id)
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

func (e *sdkEngine) resolveImage(
	ctx context.Context,
	digest string,
) (containerd.Image, error) {
	if digest != e.imageDigest {
		return nil, errors.New("container image digest is not configured")
	}
	image, err := e.client.GetImage(ctx, e.imageReference)
	if err != nil {
		return nil, err
	}
	if image.Target().Digest.String() != digest {
		return nil, errors.New("container image reference changed digest")
	}
	return image, nil
}

func (e *sdkEngine) ensureIdentityAbsent(ctx context.Context, id string) error {
	if _, err := e.client.LoadContainer(ctx, id); err == nil {
		return errors.New("container identity already exists")
	} else if !errdefs.IsNotFound(err) {
		return err
	}
	if _, err := e.client.SnapshotService(e.snapshotter).Stat(ctx, id); err == nil {
		return errors.New("snapshot identity already exists")
	} else if !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

func (e *sdkEngine) cleanup(
	id string,
	task containerd.Task,
	container containerd.Container,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.cleanupTimeout)
	defer cancel()
	ctx = e.runtimeContext(ctx)
	var taskErr error
	if task != nil {
		_, taskErr = task.Delete(ctx, containerd.WithProcessKill)
		if errdefs.IsNotFound(taskErr) {
			taskErr = nil
		}
	}
	containerErr := container.Delete(ctx, containerd.WithSnapshotCleanup)
	if errdefs.IsNotFound(containerErr) {
		containerErr = nil
	}
	if containerErr != nil {
		snapshotErr := e.client.SnapshotService(e.snapshotter).Remove(ctx, id)
		if errdefs.IsNotFound(snapshotErr) {
			snapshotErr = nil
		}
		return errors.Join(taskErr, containerErr, snapshotErr)
	}
	return taskErr
}

func (e *sdkEngine) Close() error {
	if e == nil || e.client == nil {
		return nil
	}
	return e.client.Close()
}

type boundedOutput struct {
	mutex    sync.Mutex
	maximum  uint64
	buffer   bytes.Buffer
	exceeded bool
}

func newBoundedOutput(maximum uint64) *boundedOutput {
	return &boundedOutput{maximum: maximum}
}

func (w *boundedOutput) Write(value []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	available := w.maximum - uint64(w.buffer.Len())
	stored := uint64(len(value))
	if stored > available {
		stored = available
		w.exceeded = true
	}
	if stored > 0 {
		_, _ = w.buffer.Write(value[:int(stored)])
	}
	return len(value), nil
}

func (w *boundedOutput) Bytes() []byte {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return append([]byte(nil), w.buffer.Bytes()...)
}

func (w *boundedOutput) Exceeded() bool {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.exceeded
}

func (e *sdkEngine) String() string {
	return fmt.Sprintf("containerd namespace %q", e.namespace)
}
