package gpuisolation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
)

type mockBackend struct {
	mu       sync.Mutex
	active   map[string]bool
	entered  chan []string
	release  chan struct{}
	panicNow bool
	fail     bool
}

type lateSuccessBackend struct {
	cancel context.CancelFunc
}

func TestNewRejectsTypedNilMOCKBackend(t *testing.T) {
	var backend *mockBackend
	if client, err := New([]string{"gpu-a"}, backend); err == nil || client != nil {
		t.Fatal("typed-nil GPU backend accepted")
	}
}

func (b lateSuccessBackend) RunIsolatedOnDevices(context.Context, executor.ContainerRequest, []byte, []string) (executor.Result, error) {
	b.cancel()
	return executor.Result{ExitCode: 0}, nil
}

func (m *mockBackend) RunIsolatedOnDevices(ctx context.Context, _ executor.ContainerRequest, _ []byte, devices []string) (executor.Result, error) {
	if m.panicNow {
		panic("injected")
	}
	m.mu.Lock()
	for _, device := range devices {
		if m.active[device] {
			m.mu.Unlock()
			return executor.Result{}, errors.New("shared device")
		}
		m.active[device] = true
	}
	m.mu.Unlock()
	if m.entered != nil {
		m.entered <- append([]string(nil), devices...)
	}
	if m.release != nil {
		select {
		case <-m.release:
		case <-ctx.Done():
		}
	}
	m.mu.Lock()
	for _, device := range devices {
		delete(m.active, device)
	}
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	if m.fail {
		return executor.Result{}, errors.New("injected")
	}
	return executor.Result{ExitCode: 0, Usage: executor.Usage{Duration: time.Millisecond}}, nil
}

func TestExclusiveLeasesCapacityAndCleanupWithMOCKBackend(t *testing.T) {
	backend := &mockBackend{active: make(map[string]bool), entered: make(chan []string, 1), release: make(chan struct{})}
	client, err := New([]string{"gpu-a"}, backend)
	if err != nil {
		t.Fatal(err)
	}
	request := requestFor(t, "task-a")
	done := make(chan error, 1)
	go func() { _, err := client.RunIsolated(context.Background(), request, nil); done <- err }()
	if devices := <-backend.entered; len(devices) != 1 || devices[0] != "gpu-a" {
		t.Fatalf("devices=%v", devices)
	}
	if client.Available() != 0 {
		t.Fatal("leased device reported available")
	}
	second := requestFor(t, "task-b")
	if _, err := client.RunIsolated(context.Background(), second, nil); err == nil {
		t.Fatal("overlapping lease was accepted")
	}
	close(backend.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if client.Available() != 1 {
		t.Fatal("device lease leaked")
	}
}

func TestLeaseReleasedAfterMOCKBackendPanic(t *testing.T) {
	client, err := New([]string{"gpu-a"}, &mockBackend{active: make(map[string]bool), panicNow: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RunIsolated(context.Background(), requestFor(t, "task-a"), nil); err == nil {
		t.Fatal("panic was accepted")
	}
	if client.Available() != 1 {
		t.Fatal("panic leaked device lease")
	}
}

func TestLeaseReleasedAfterMOCKCancellationAndFailure(t *testing.T) {
	for name, setup := range map[string]struct {
		backend *mockBackend
		cancel  bool
	}{
		"failure":      {backend: &mockBackend{active: make(map[string]bool), fail: true}},
		"cancellation": {backend: &mockBackend{active: make(map[string]bool), entered: make(chan []string, 1), release: make(chan struct{})}, cancel: true},
	} {
		t.Run(name, func(t *testing.T) {
			client, err := New([]string{"gpu-a"}, setup.backend)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if setup.cancel {
				go func() { <-setup.backend.entered; cancel() }()
			} else {
				defer cancel()
			}
			if _, err := client.RunIsolated(ctx, requestFor(t, "task-a"), nil); err == nil {
				t.Fatal("failure was accepted")
			}
			if client.Available() != 1 {
				t.Fatal("failed execution leaked device lease")
			}
		})
	}
}

func TestMOCKBackendLateSuccessAfterCancellationIsRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := New([]string{"gpu-a"}, lateSuccessBackend{cancel: cancel})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RunIsolated(ctx, requestFor(t, "task-a"), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("late success returned err=%v", err)
	}
	if client.Available() != 1 {
		t.Fatal("late success leaked device lease")
	}
}

func requestFor(t *testing.T, id string) executor.ContainerRequest {
	t.Helper()
	digest, err := executor.ExecutionDigest(id)
	if err != nil {
		t.Fatal(err)
	}
	return executor.ContainerRequest{ExecutionDigest: digest, AllowGPU: true, Limits: executor.Limits{GPUDeviceCount: 1}}
}
