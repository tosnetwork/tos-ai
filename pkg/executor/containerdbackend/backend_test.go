package containerdbackend

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

type fakeEngine struct {
	mutex        sync.Mutex
	readyErr     error
	residueErr   error
	runErr       error
	block        bool
	panicRun     bool
	panicReady   bool
	panicResidue bool
	closed       bool
	started      chan struct{}
	lastID       string
	lastRequest  executor.ContainerRequest
	lastInput    []byte
	active       int
	maximumAlive int
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{started: make(chan struct{}, 32)}
}

func (e *fakeEngine) CheckReady(context.Context) error {
	if e.panicReady {
		panic("readiness detail")
	}
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.readyErr
}

func (e *fakeEngine) CheckResidue(context.Context) error {
	if e.panicResidue {
		panic("residue detail")
	}
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.residueErr
}

func (e *fakeEngine) Run(
	ctx context.Context,
	id string,
	request executor.ContainerRequest,
	input []byte,
) (executor.Result, error) {
	if e.panicRun {
		panic("runtime detail")
	}
	e.mutex.Lock()
	e.lastID = id
	e.lastRequest = request
	e.lastInput = input
	e.active++
	if e.active > e.maximumAlive {
		e.maximumAlive = e.active
	}
	e.mutex.Unlock()
	defer func() {
		e.mutex.Lock()
		e.active--
		e.mutex.Unlock()
	}()
	select {
	case e.started <- struct{}{}:
	default:
	}
	if e.block {
		<-ctx.Done()
		return executor.Result{}, ctx.Err()
	}
	if e.runErr != nil {
		return executor.Result{}, e.runErr
	}
	return executor.Result{Output: []byte("ok")}, nil
}

func (e *fakeEngine) Close() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.closed = true
	return nil
}

func validRequest(t *testing.T, name string) executor.ContainerRequest {
	t.Helper()
	digest, err := executor.ExecutionDigest(name)
	if err != nil {
		t.Fatal(err)
	}
	image, err := executor.ExecutionDigest("image")
	if err != nil {
		t.Fatal(err)
	}
	return executor.ContainerRequest{
		ExecutionDigest: digest, ImageDigest: image,
		Entrypoint:  []string{"/bin/infer", "--json"},
		Environment: map[string]string{"MODEL": "fixed"},
		UserID:      1000, GroupID: 1000, ReadOnlyRoot: true,
		NoNewPrivileges: true, Network: executor.NetworkNone,
		Limits: executor.Limits{
			CPUMillis: 1000, MemoryBytes: 64 << 20,
			DiskBytes: 12 << 20, PIDs: 32,
			ExecutionTime: time.Second, OutputBytes: 1 << 20,
		},
	}
}

func TestBackendUsesDigestOnlyAndDefensiveCopies(t *testing.T) {
	engine := newFakeEngine()
	backend, err := newBackend(context.Background(), engine, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest(t, "task")
	input := []byte("input")
	result, err := backend.RunIsolated(context.Background(), request, input)
	if err != nil || string(result.Output) != "ok" {
		t.Fatalf("run result=%q err=%v", result.Output, err)
	}
	request.Entrypoint[0] = "changed"
	request.Environment["MODEL"] = "changed"
	input[0] = 'X'
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if engine.lastID != runtimeID(engine.lastRequest.ExecutionDigest) ||
		engine.lastRequest.Entrypoint[0] != "/bin/infer" ||
		engine.lastRequest.Environment["MODEL"] != "fixed" ||
		string(engine.lastInput) != "input" {
		t.Fatal("runtime received mutable or externally correlated state")
	}
}

func TestBackendBoundsCapacityAndCloseCancelsActiveRuns(t *testing.T) {
	engine := newFakeEngine()
	engine.block = true
	backend, err := newBackend(context.Background(), engine, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest(t, "first")
	done := make(chan error, 1)
	go func() {
		_, err := backend.RunIsolated(context.Background(), request, nil)
		done <- err
	}()
	<-engine.started
	if _, err := backend.RunIsolated(
		context.Background(), request, nil,
	); err == nil {
		t.Fatal("duplicate execution was accepted")
	}
	second := validRequest(t, "second")
	if _, err := backend.RunIsolated(
		context.Background(), second, nil,
	); err == nil {
		t.Fatal("execution above fixed capacity was accepted")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("shutdown did not cancel active execution")
	}
	engine.mutex.Lock()
	closed, maximumAlive := engine.closed, engine.maximumAlive
	engine.mutex.Unlock()
	if !closed || maximumAlive != 1 {
		t.Fatalf("closed=%v maximumAlive=%d", closed, maximumAlive)
	}
	if _, err := backend.RunIsolated(
		context.Background(), second, nil,
	); err == nil {
		t.Fatal("closed backend accepted work")
	}
}

func TestBackendFailsClosedOnReadinessResidueAndPanic(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*fakeEngine)
	}{
		{"readiness", func(engine *fakeEngine) { engine.readyErr = errors.New("secret") }},
		{"residue", func(engine *fakeEngine) { engine.residueErr = errors.New("secret") }},
		{"readiness panic", func(engine *fakeEngine) { engine.panicReady = true }},
		{"residue panic", func(engine *fakeEngine) { engine.panicResidue = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := newFakeEngine()
			test.configure(engine)
			if backend, err := newBackend(
				context.Background(), engine, nil, 1,
			); err == nil || backend != nil {
				t.Fatal("unsafe runtime state was accepted")
			}
		})
	}
	var typedNil *fakeEngine
	if backend, err := newBackend(context.Background(), typedNil, nil, 1); err == nil || backend != nil {
		t.Fatal("typed-nil containerd engine accepted")
	}
	engine := newFakeEngine()
	engine.panicRun = true
	backend, err := newBackend(context.Background(), engine, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.RunIsolated(
		context.Background(), validRequest(t, "panic"), nil,
	); err == nil || err.Error() != "containerd execution failed" {
		t.Fatalf("panic detail crossed backend boundary: %v", err)
	}
}

func TestFixedIsolationSpecEnforcesCPUOnlySandbox(t *testing.T) {
	request := validRequest(t, "spec")
	ctx := namespaces.WithNamespace(context.Background(), "tos-ai-test")
	spec, err := oci.GenerateSpec(
		ctx, nil, &containers.Container{ID: runtimeID(request.ExecutionDigest)},
		fixedIsolationSpec(request),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !spec.Root.Readonly || !spec.Process.NoNewPrivileges ||
		spec.Process.User.UID != 1000 || spec.Process.User.GID != 1000 ||
		len(spec.Process.Capabilities.Bounding) != 0 ||
		spec.Linux.Resources.Memory == nil ||
		*spec.Linux.Resources.Memory.Limit != int64(request.Limits.MemoryBytes) ||
		spec.Linux.Resources.Pids == nil ||
		spec.Linux.Resources.Pids.Limit != int64(request.Limits.PIDs) ||
		spec.Linux.Seccomp == nil ||
		spec.Linux.Seccomp.DefaultAction != specs.ActErrno ||
		len(spec.Linux.Seccomp.Syscalls) == 0 ||
		!hasPrivateNetworkNamespace(spec.Linux.Namespaces) {
		t.Fatal("generated OCI specification lost an isolation invariant")
	}
	for _, mount := range spec.Mounts {
		if mount.Type == "tmpfs" &&
			(mount.Destination == "/dev" || mount.Destination == "/dev/shm" ||
				mount.Destination == "/run") {
			if !contains(mount.Options, "noexec") {
				t.Fatalf("tmpfs %q remained executable", mount.Destination)
			}
		}
	}
}

func TestRequestRejectsNetworkGPUAndUnsafeBounds(t *testing.T) {
	base := validRequest(t, "invalid")
	cases := []executor.ContainerRequest{
		func() executor.ContainerRequest {
			value := base
			value.Network = executor.NetworkAllowlist
			value.AllowedHosts = []string{"example.com"}
			return value
		}(),
		func() executor.ContainerRequest {
			value := base
			value.AllowGPU = true
			value.Limits.GPUDeviceCount = 1
			return value
		}(),
		func() executor.ContainerRequest { value := base; value.ReadOnlyRoot = false; return value }(),
		func() executor.ContainerRequest { value := base; value.UserID = 0; return value }(),
		func() executor.ContainerRequest {
			value := base
			value.Limits.DiskBytes = minimumTmpfsBytes - 1
			return value
		}(),
	}
	for _, request := range cases {
		if err := validateRequest(request, nil); err == nil {
			t.Fatal("unsafe container request was accepted")
		}
	}
}

func TestPrivateSocketAndFIFODirectoryValidation(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "containerd.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateSocket(socketPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateSocket(socketPath); err == nil {
		t.Fatal("group-accessible containerd socket was accepted")
	}
	fifoDir := filepath.Join(directory, "fifo")
	if err := ensurePrivateDirectory(fifoDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fifoDir, "residue"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateFIFODir(fifoDir); err == nil {
		t.Fatal("containerd FIFO residue was accepted")
	}
}

func TestBoundedOutputNeverRetainsPastLimit(t *testing.T) {
	output := newBoundedOutput(4)
	if count, err := output.Write([]byte("abcdef")); err != nil || count != 6 {
		t.Fatalf("write count=%d err=%v", count, err)
	}
	if string(output.Bytes()) != "abcd" || !output.Exceeded() {
		t.Fatal("output limit was not enforced")
	}
}
