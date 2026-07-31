// Package probe reports coarse host capabilities without exposing serial
// numbers, exact topology, customer data, or a stable hardware fingerprint.
package probe

import (
	"runtime"
	"time"
)

const MaxLogicalCPUs = 4096

type EvidenceLevel string

const (
	EvidenceDeclared        EvidenceLevel = "declared"
	EvidenceLocallyObserved EvidenceLevel = "locally-observed"
	EvidenceBenchmarked     EvidenceLevel = "benchmarked"
)

type Host struct {
	OS           string        `json:"os"`
	Architecture string        `json:"architecture"`
	LogicalCPUs  int           `json:"logicalCpus"`
	MemoryBytes  uint64        `json:"memoryBytes,omitempty"`
	CollectedAt  time.Time     `json:"collectedAt"`
	Evidence     EvidenceLevel `json:"evidence"`
}

type Report struct {
	Host   Host         `json:"host"`
	NVIDIA NVIDIAReport `json:"nvidia"`
}

func CollectHost() (Host, error) {
	memory, err := totalMemory()
	if err != nil {
		return Host{}, err
	}
	cpus := runtime.NumCPU()
	if cpus > MaxLogicalCPUs {
		cpus = MaxLogicalCPUs
	}
	return Host{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		LogicalCPUs:  cpus,
		MemoryBytes:  memory,
		CollectedAt:  time.Now().UTC(),
		Evidence:     EvidenceLocallyObserved,
	}, nil
}

func Collect(backend NVIDIABackend) (Report, error) {
	host, err := CollectHost()
	if err != nil {
		return Report{}, err
	}
	return Report{Host: host, NVIDIA: CollectNVIDIA(backend)}, nil
}
