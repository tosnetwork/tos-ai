package containerdbackend

import (
	"errors"
	"time"

	cgroup2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	containerdtypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/typeurl/v2"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const maxMetricIODevices = 4096

func usageFromMetric(
	metric *containerdtypes.Metric,
	duration time.Duration,
) (executor.Usage, error) {
	if metric == nil || metric.Data == nil || duration <= 0 ||
		!typeurl.Is(metric.Data, (*cgroup2.Metrics)(nil)) {
		return executor.Usage{}, errors.New("unsupported containerd task metrics")
	}
	var stats cgroup2.Metrics
	if err := typeurl.UnmarshalTo(metric.Data, &stats); err != nil ||
		stats.CPU == nil || stats.Memory == nil ||
		len(stats.GetIo().GetUsage()) > maxMetricIODevices {
		return executor.Usage{}, errors.New("invalid containerd task metrics")
	}
	cpuMillis := stats.CPU.UsageUsec / 1000
	if stats.CPU.UsageUsec%1000 != 0 {
		cpuMillis++
	}
	peakMemory := stats.Memory.MaxUsage
	if stats.Memory.Usage > peakMemory {
		peakMemory = stats.Memory.Usage
	}
	var diskWritten uint64
	for _, entry := range stats.GetIo().GetUsage() {
		if entry == nil || entry.Wbytes > ^uint64(0)-diskWritten {
			return executor.Usage{}, errors.New("invalid containerd task metrics")
		}
		diskWritten += entry.Wbytes
	}
	return executor.Usage{
		CPUMillis: cpuMillis, PeakMemory: peakMemory,
		DiskWritten: diskWritten, Duration: duration,
	}, nil
}
