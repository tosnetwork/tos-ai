// Package resourceguard continuously verifies that the host class which
// backed startup admission still exists. It gates new work; it does not
// resize capacity or preempt in-flight work.
package resourceguard

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/probe"
)

const (
	MinInterval        = time.Second
	MaxIntervalHard    = 5 * time.Minute
	MinTimeout         = 100 * time.Millisecond
	MaxTimeoutHard     = 30 * time.Second
	MaxThresholdHard   = 10
	MinimumObservedRAM = uint64(64 << 20)
	StatusReady        = "ready"
	StatusDegraded     = "degraded"
	defaultUnknownGPU  = "unknown"
)

var ErrShutdownIncomplete = errors.New("resource monitor shutdown incomplete")

type Sampler func(context.Context) (probe.Report, error)

type Config struct {
	Interval          time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	RecoveryThreshold int
	RequiredRAMBytes  uint64
	RequiredVRAMBytes uint64
	Initial           probe.Report
	Sample            Sampler
}

type Guard struct {
	mu           sync.Mutex
	config       Config
	health       probe.ResourceHealth
	failures     int
	recoveries   int
	cancel       context.CancelFunc
	done         chan struct{}
	shutdownOnce sync.Once
}

func New(config Config) (*Guard, error) {
	if config.Interval < MinInterval || config.Interval > MaxIntervalHard ||
		config.Timeout < MinTimeout || config.Timeout > MaxTimeoutHard ||
		config.Timeout > config.Interval || config.FailureThreshold <= 0 ||
		config.FailureThreshold > MaxThresholdHard ||
		config.RecoveryThreshold <= 0 ||
		config.RecoveryThreshold > MaxThresholdHard ||
		config.RequiredRAMBytes == 0 || config.Sample == nil {
		return nil, errors.New("invalid resource monitor configuration")
	}
	initialHealth, err := evaluate(
		config.Initial, config.RequiredRAMBytes, config.RequiredVRAMBytes,
	)
	if err != nil {
		return nil, errors.New("initial resource observation is unsafe")
	}
	monitorContext, cancel := context.WithCancel(context.Background())
	guard := &Guard{
		config: config, health: initialHealth, cancel: cancel,
		done: make(chan struct{}),
	}
	go guard.run(monitorContext)
	return guard, nil
}

func (g *Guard) Health() probe.ResourceHealth {
	if g == nil {
		return probe.ResourceHealth{
			Ready: false, Status: StatusDegraded, GPU: defaultUnknownGPU,
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.health
}

func (g *Guard) Shutdown(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if ctx == nil {
		return ErrShutdownIncomplete
	}
	g.shutdownOnce.Do(g.cancel)
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrShutdownIncomplete, ctx.Err())
	}
}

func (g *Guard) run(ctx context.Context) {
	defer close(g.done)
	timer := time.NewTimer(g.config.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		sampleContext, cancel := context.WithTimeout(ctx, g.config.Timeout)
		report, err := safeSample(sampleContext, g.config.Sample)
		cancel()
		g.record(report, err)
		timer.Reset(g.config.Interval)
	}
}

func safeSample(ctx context.Context, sample Sampler) (
	report probe.Report,
	err error,
) {
	defer func() {
		if recover() != nil {
			report = probe.Report{}
			err = errors.New("resource sampler failed")
		}
	}()
	return sample(ctx)
}

func (g *Guard) record(report probe.Report, sampleErr error) {
	next, evaluationErr := evaluate(
		report, g.config.RequiredRAMBytes, g.config.RequiredVRAMBytes,
	)
	good := sampleErr == nil && evaluationErr == nil
	g.mu.Lock()
	defer g.mu.Unlock()
	if good {
		g.failures = 0
		g.health.GPU = next.GPU
		if g.health.Ready {
			g.recoveries = 0
			return
		}
		g.recoveries++
		if g.recoveries >= g.config.RecoveryThreshold {
			g.health = next
			g.recoveries = 0
		}
		return
	}
	g.recoveries = 0
	g.failures++
	if validGPUStatus(report.NVIDIA.Status) {
		g.health.GPU = report.NVIDIA.Status
	}
	if g.failures >= g.config.FailureThreshold {
		g.health.Ready = false
		g.health.Status = StatusDegraded
		g.failures = g.config.FailureThreshold
	}
}

func evaluate(
	report probe.Report,
	requiredRAM uint64,
	requiredVRAM uint64,
) (probe.ResourceHealth, error) {
	health := probe.ResourceHealth{
		Ready: true, Status: StatusReady, GPU: report.NVIDIA.Status,
	}
	if !validGPUStatus(health.GPU) {
		health.GPU = defaultUnknownGPU
	}
	if report.Host.MemoryBytes < MinimumObservedRAM {
		return health, errors.New("observed host memory is unavailable")
	}
	maximumRAM := report.Host.MemoryBytes - report.Host.MemoryBytes/4
	if requiredRAM > maximumRAM {
		return health, errors.New("observed host memory is below policy")
	}
	if requiredVRAM == 0 {
		return health, nil
	}
	if report.NVIDIA.Status != "available" ||
		report.NVIDIA.DriverVersion == "" || len(report.NVIDIA.Devices) == 0 {
		return health, errors.New("required NVIDIA resources are unavailable")
	}
	var totalVRAM uint64
	for _, device := range report.NVIDIA.Devices {
		if device.VRAMBytes == 0 || device.VRAMUsedBytes > device.VRAMBytes ||
			^uint64(0)-totalVRAM < device.VRAMBytes {
			return health, errors.New("observed NVIDIA memory is invalid")
		}
		totalVRAM += device.VRAMBytes
	}
	if requiredVRAM > totalVRAM {
		return health, errors.New("observed NVIDIA memory is below policy")
	}
	return health, nil
}

func validGPUStatus(status string) bool {
	switch status {
	case "available", "degraded", "unavailable", "no-devices", "unknown":
		return true
	default:
		return false
	}
}
