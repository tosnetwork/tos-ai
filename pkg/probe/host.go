// Package probe reports coarse host capabilities without exposing serial
// numbers, exact topology, customer data, or a stable hardware fingerprint.
package probe

import (
	"runtime"
	"time"
)

type Host struct {
	OS           string    `json:"os"`
	Architecture string    `json:"architecture"`
	LogicalCPUs  int       `json:"logicalCpus"`
	MemoryBytes  uint64    `json:"memoryBytes,omitempty"`
	CollectedAt  time.Time `json:"collectedAt"`
	Evidence     string    `json:"evidence"`
}

func CollectHost() (Host, error) {
	memory, err := totalMemory()
	if err != nil {
		return Host{}, err
	}
	return Host{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		LogicalCPUs:  runtime.NumCPU(),
		MemoryBytes:  memory,
		CollectedAt:  time.Now().UTC(),
		Evidence:     "locally-observed",
	}, nil
}
