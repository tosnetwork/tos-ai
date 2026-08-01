package containerdbackend

import (
	"strings"
	"testing"
	"time"

	cgroup2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	containerdtypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/typeurl/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

func taskMetric(t *testing.T, stats *cgroup2.Metrics) *containerdtypes.Metric {
	t.Helper()
	encoded, err := typeurl.MarshalAny(stats)
	if err != nil {
		t.Fatal(err)
	}
	return &containerdtypes.Metric{
		ID: "task", Data: typeurl.MarshalProto(encoded),
	}
}

func TestUsageFromMetricMapsCgroupV2WithoutUnderreporting(t *testing.T) {
	metric := taskMetric(t, &cgroup2.Metrics{
		CPU:    &cgroup2.CPUStat{UsageUsec: 1001},
		Memory: &cgroup2.MemoryStat{Usage: 2048, MaxUsage: 4096},
		Io: &cgroup2.IOStat{Usage: []*cgroup2.IOEntry{
			{Major: 8, Minor: 0, Wbytes: 10},
			{Major: 8, Minor: 1, Wbytes: 20},
		}},
	})
	usage, err := usageFromMetric(metric, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if usage.CPUMillis != 2 || usage.PeakMemory != 4096 ||
		usage.DiskWritten != 30 || usage.Duration != 5*time.Millisecond {
		t.Fatalf("unexpected usage: %#v", usage)
	}

	metric = taskMetric(t, &cgroup2.Metrics{
		CPU:    &cgroup2.CPUStat{},
		Memory: &cgroup2.MemoryStat{Usage: 8192, MaxUsage: 4096},
	})
	usage, err = usageFromMetric(metric, time.Nanosecond)
	if err != nil || usage.PeakMemory != 8192 {
		t.Fatalf("current memory fallback usage=%#v err=%v", usage, err)
	}
}

func TestUsageFromMetricRejectsUnknownMalformedAndUnboundedData(t *testing.T) {
	invalid := []*containerdtypes.Metric{
		nil,
		{},
		{Data: &anypb.Any{TypeUrl: "unknown", Value: []byte("bad")}},
		taskMetric(t, &cgroup2.Metrics{Memory: &cgroup2.MemoryStat{}}),
		taskMetric(t, &cgroup2.Metrics{CPU: &cgroup2.CPUStat{}}),
		taskMetric(t, &cgroup2.Metrics{
			CPU: &cgroup2.CPUStat{}, Memory: &cgroup2.MemoryStat{},
			Io: &cgroup2.IOStat{Usage: make(
				[]*cgroup2.IOEntry, maxMetricIODevices+1,
			)},
		}),
		taskMetric(t, &cgroup2.Metrics{
			CPU: &cgroup2.CPUStat{}, Memory: &cgroup2.MemoryStat{},
			Io: &cgroup2.IOStat{Usage: []*cgroup2.IOEntry{
				{Wbytes: ^uint64(0)}, {Wbytes: 1},
			}},
		}),
	}
	for index, metric := range invalid {
		if _, err := usageFromMetric(metric, time.Second); err == nil ||
			strings.Contains(err.Error(), "unknown") {
			t.Fatalf("invalid metric %d error=%v", index, err)
		}
	}
	if _, err := usageFromMetric(taskMetric(t, &cgroup2.Metrics{
		CPU: &cgroup2.CPUStat{}, Memory: &cgroup2.MemoryStat{},
	}), 0); err == nil {
		t.Fatal("zero metric duration was accepted")
	}
}
