package resourceguard

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/probe"
)

func healthyReport(vram uint64) probe.Report {
	report := probe.Report{
		Host:   probe.Host{MemoryBytes: 16 << 30},
		NVIDIA: probe.NVIDIAReport{Status: "no-devices"},
	}
	if vram > 0 {
		report.NVIDIA = probe.NVIDIAReport{
			Status: "available", DriverVersion: "test-driver",
			Devices: []probe.NVIDIADevice{{VRAMBytes: vram}},
		}
	}
	return report
}

func testConfig(sample Sampler) Config {
	return Config{
		Interval: time.Second, Timeout: time.Second,
		FailureThreshold: 2, RecoveryThreshold: 2,
		RequiredRAMBytes: 4 << 30, Initial: healthyReport(0), Sample: sample,
	}
}

func TestGuardRejectsInvalidConfigurationAndInitialCapacity(t *testing.T) {
	valid := testConfig(func(context.Context) (probe.Report, error) {
		return healthyReport(0), nil
	})
	invalid := []Config{
		{},
		func() Config { value := valid; value.Interval = MinInterval - 1; return value }(),
		func() Config { value := valid; value.Timeout = value.Interval + 1; return value }(),
		func() Config { value := valid; value.FailureThreshold = MaxThresholdHard + 1; return value }(),
		func() Config { value := valid; value.RecoveryThreshold = 0; return value }(),
		func() Config { value := valid; value.RequiredRAMBytes = 0; return value }(),
	}
	for _, config := range invalid {
		if guard, err := New(config); err == nil {
			_ = guard.Shutdown(context.Background())
			t.Fatalf("invalid configuration accepted: %#v", config)
		}
	}
	valid.RequiredRAMBytes = 16 << 30
	if _, err := New(valid); err == nil {
		t.Fatal("unsafe initial RAM observation accepted")
	}
	valid.RequiredRAMBytes = 4 << 30
	valid.RequiredVRAMBytes = 1
	if _, err := New(valid); err == nil {
		t.Fatal("missing initial GPU accepted")
	}
}

func TestGuardFailureAndRecoveryThresholds(t *testing.T) {
	samples := make(chan probe.Report, 4)
	config := testConfig(func(ctx context.Context) (probe.Report, error) {
		select {
		case report := <-samples:
			return report, nil
		case <-ctx.Done():
			return probe.Report{}, ctx.Err()
		}
	})
	config.Interval = 10 * time.Millisecond
	config.Timeout = 5 * time.Millisecond
	// Use package-local construction to exercise short deterministic timings
	// while public configuration keeps a one-second production floor.
	guard := newTestGuard(config)
	t.Cleanup(func() { _ = guard.Shutdown(context.Background()) })
	bad := probe.Report{Host: probe.Host{MemoryBytes: 1}}
	samples <- bad
	waitForCounters(t, guard, 1, 0)
	if !guard.Health().Ready {
		t.Fatal("single failed sample closed admission")
	}
	samples <- bad
	waitForHealth(t, guard, func(value probe.ResourceHealth) bool {
		return !value.Ready
	})
	samples <- healthyReport(0)
	waitForCounters(t, guard, 0, 1)
	if guard.Health().Ready {
		t.Fatal("single recovery sample reopened admission")
	}
	samples <- healthyReport(0)
	waitForHealth(t, guard, func(value probe.ResourceHealth) bool {
		return value.Ready && value.Status == StatusReady
	})
}

func TestGuardGPULossAndCPUOnlyBehavior(t *testing.T) {
	noGPU := healthyReport(0)
	if _, err := evaluate(noGPU, 1<<30, 0); err != nil {
		t.Fatalf("CPU-only policy rejected no-GPU host: %v", err)
	}
	gpu := healthyReport(8 << 30)
	if _, err := evaluate(gpu, 1<<30, 4<<30); err != nil {
		t.Fatal(err)
	}
	gpu.NVIDIA.Status = "unavailable"
	if _, err := evaluate(gpu, 1<<30, 4<<30); err == nil {
		t.Fatal("unavailable required GPU accepted")
	}
	gpu = healthyReport(2 << 30)
	if _, err := evaluate(gpu, 1<<30, 4<<30); err == nil {
		t.Fatal("insufficient total VRAM accepted")
	}
}

func TestGuardShutdownCancelsOneBoundedSampler(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	release := make(chan struct{})
	config := testConfig(func(ctx context.Context) (probe.Report, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		select {
		case <-release:
			return healthyReport(0), nil
		case <-ctx.Done():
			return probe.Report{}, ctx.Err()
		}
	})
	config.Interval = 5 * time.Millisecond
	config.Timeout = time.Second
	guard := newTestGuard(config)
	time.Sleep(20 * time.Millisecond)
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := guard.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 1 || active.Load() != 0 {
		t.Fatalf("sampler concurrency maximum=%d active=%d", maximum.Load(), active.Load())
	}
	close(release)
}

func TestGuardRecoversSamplerPanic(t *testing.T) {
	config := testConfig(func(context.Context) (probe.Report, error) {
		panic("fault")
	})
	config.FailureThreshold = 1
	config.Interval = time.Millisecond
	guard := newTestGuard(config)
	t.Cleanup(func() { _ = guard.Shutdown(context.Background()) })
	waitForHealth(t, guard, func(value probe.ResourceHealth) bool {
		return !value.Ready
	})
}

func TestGuardShutdownReportsSamplerIgnoringCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	config := testConfig(func(context.Context) (probe.Report, error) {
		close(started)
		<-release
		return healthyReport(0), nil
	})
	config.Interval = time.Millisecond
	guard := newTestGuard(config)
	<-started
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := guard.Shutdown(shutdownContext); !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("shutdown error=%v", err)
	}
	close(release)
	waitContext, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := guard.Shutdown(waitContext); err != nil {
		t.Fatal(err)
	}
}

func newTestGuard(config Config) *Guard {
	ctx, cancel := context.WithCancel(context.Background())
	health, err := evaluate(
		config.Initial, config.RequiredRAMBytes, config.RequiredVRAMBytes,
	)
	if err != nil {
		panic(err)
	}
	guard := &Guard{
		config: config, health: health, cancel: cancel, done: make(chan struct{}),
	}
	go guard.run(ctx)
	return guard
}

func waitForHealth(
	t *testing.T,
	guard *Guard,
	condition func(probe.ResourceHealth) bool,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition(guard.Health()) {
		if time.Now().After(deadline) {
			t.Fatalf("health condition not reached: %#v", guard.Health())
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCounters(t *testing.T, guard *Guard, failures, recoveries int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		guard.mu.Lock()
		matched := guard.failures == failures && guard.recoveries == recoveries
		guard.mu.Unlock()
		if matched {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("resource monitor counters did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
