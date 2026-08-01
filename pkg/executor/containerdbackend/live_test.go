package containerdbackend_test

import (
	"context"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/executor/backendtest"
	"github.com/tosnetwork/tos-ai/pkg/executor/containerdbackend"
)

const (
	liveSocketEnv    = "TOS_AI_CONTAINERD_TEST_SOCKET"
	liveNamespaceEnv = "TOS_AI_CONTAINERD_TEST_NAMESPACE"
	liveFIFOEnv      = "TOS_AI_CONTAINERD_TEST_FIFO_DIR"
	liveImageRefEnv  = "TOS_AI_CONTAINERD_TEST_IMAGE_REFERENCE"
	liveImageEnv     = "TOS_AI_CONTAINERD_TEST_IMAGE_DIGEST"
)

var liveManagedID = regexp.MustCompile(`^[0-9a-f]{64}$`)

type liveConfiguration struct {
	socket, namespace, fifoDir, imageReference, imageDigest string
}

type liveBackend struct {
	*containerdbackend.Backend
	admin *client.Client
}

func (b *liveBackend) Close() error {
	if b == nil {
		return nil
	}
	var backendErr error
	if b.Backend != nil {
		backendErr = b.Backend.Close()
	}
	var adminErr error
	if b.admin != nil {
		adminErr = b.admin.Close()
	}
	return errors.Join(backendErr, adminErr)
}

type liveInspector struct {
	admin     *client.Client
	namespace string
	store     snapshots.Snapshotter
}

func (i *liveInspector) Snapshot(
	ctx context.Context,
) (backendtest.Snapshot, error) {
	ctx = namespaces.WithNamespace(ctx, i.namespace)
	containers, err := i.admin.Containers(ctx)
	if err != nil {
		return backendtest.Snapshot{}, err
	}
	result := backendtest.Snapshot{Containers: len(containers)}
	for _, container := range containers {
		task, err := container.Task(ctx, nil)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return backendtest.Snapshot{}, err
		}
		result.Tasks++
		status, err := task.Status(ctx)
		if err != nil {
			return backendtest.Snapshot{}, err
		}
		if status.Status == client.Running || status.Status == client.Paused ||
			status.Status == client.Pausing {
			result.ActiveWorkloads++
		}
	}
	err = i.store.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if liveManagedID.MatchString(info.Name) {
			result.Snapshots++
		}
		return nil
	})
	return result, err
}

func TestContainerdBackendLiveConformance(t *testing.T) {
	configuration, enabled := liveTestConfiguration(t)
	if !enabled {
		t.Skip("private containerd live-test environment is not configured")
	}
	limits := executor.Limits{
		CPUMillis: 10_000, MemoryBytes: 128 << 20,
		DiskBytes: 12 << 20, PIDs: 32,
		ExecutionTime: 10 * time.Second, OutputBytes: 1024,
	}
	successDigest, err := executor.ExecutionDigest("containerd-live-success")
	if err != nil {
		t.Fatal(err)
	}
	cancellationDigest, err := executor.ExecutionDigest("containerd-live-cancel")
	if err != nil {
		t.Fatal(err)
	}
	request := func(digest string, entrypoint []string) executor.ContainerRequest {
		return executor.ContainerRequest{
			ExecutionDigest: digest, ImageDigest: configuration.imageDigest,
			Entrypoint: entrypoint, UserID: 65532, GroupID: 65532,
			ReadOnlyRoot: true, NoNewPrivileges: true,
			Network: executor.NetworkNone, Limits: limits,
		}
	}
	backendtest.Run(t, backendtest.Suite{
		New: func(ctx context.Context) (
			backendtest.Backend, backendtest.Inspector, error,
		) {
			admin, err := client.New(configuration.socket)
			if err != nil {
				return nil, nil, err
			}
			backend, err := containerdbackend.Open(ctx, containerdbackend.Config{
				SocketPath: configuration.socket, Namespace: configuration.namespace,
				Snapshotter: "overlayfs", Runtime: "io.containerd.runc.v2",
				FIFODir: configuration.fifoDir, MaxActive: 4,
				PolicyLimits: limits, ImageReference: configuration.imageReference,
				ImageDigest: configuration.imageDigest,
			})
			if err != nil {
				_ = admin.Close()
				return nil, nil, err
			}
			wrapped := &liveBackend{Backend: backend, admin: admin}
			return wrapped, &liveInspector{
				admin: admin, namespace: configuration.namespace,
				store: admin.SnapshotService("overlayfs"),
			}, nil
		},
		SuccessRequest: request(successDigest, []string{
			"/bin/sh", "-c", `read value; printf 'ok:%s' "$value"`,
		}),
		SuccessInput: []byte("fixture\n"), ExpectedOutput: []byte("ok:fixture"),
		CancellationRequest: request(cancellationDigest, []string{
			"/bin/sh", "-c", `while :; do sleep 1; done`,
		}),
		StartTimeout: 5 * time.Second, ReturnTimeout: 15 * time.Second,
		InspectTimeout: 5 * time.Second, Concurrency: 4,
	})
}

func liveTestConfiguration(t *testing.T) (liveConfiguration, bool) {
	t.Helper()
	configuration := liveConfiguration{
		socket: os.Getenv(liveSocketEnv), namespace: os.Getenv(liveNamespaceEnv),
		fifoDir: os.Getenv(liveFIFOEnv), imageReference: os.Getenv(liveImageRefEnv),
		imageDigest: os.Getenv(liveImageEnv),
	}
	values := []string{
		configuration.socket, configuration.namespace, configuration.fifoDir,
		configuration.imageReference, configuration.imageDigest,
	}
	configured := 0
	for _, value := range values {
		if value != "" {
			configured++
		}
	}
	if configured == 0 {
		return liveConfiguration{}, false
	}
	if configured != len(values) {
		t.Fatal("partial private containerd live-test environment")
	}
	return configuration, true
}
