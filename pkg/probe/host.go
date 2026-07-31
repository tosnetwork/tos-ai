// Package probe reports coarse host capabilities without exposing serial
// numbers, exact topology, customer data, or a stable hardware fingerprint.
package probe

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	MaxLogicalCPUs       = 4096
	maxHostOSBytes       = 32
	maxArchitectureBytes = 32
)

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

// ResourceHealth is a compact, non-identifying liveness gate for admission.
// Detailed hardware inventory remains local to the probe layer.
type ResourceHealth struct {
	Ready  bool
	Status string
	GPU    string
}

// ResourceHealthProvider is owned by the worker lifecycle. Shutdown must stop
// any sampling before it returns successfully.
type ResourceHealthProvider interface {
	Health() ResourceHealth
	Shutdown(context.Context) error
}

func CollectHost() (Host, error) {
	memory, cpus, err := effectiveResources()
	if err != nil {
		return Host{}, err
	}
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

// ValidateReport validates the bounded subprocess representation before it is
// trusted by admission. It deliberately accepts degraded NVIDIA observations,
// because the resource policy decides whether a GPU is required.
func ValidateReport(report Report) error {
	if !validObservedString(report.Host.OS, maxHostOSBytes) ||
		!validObservedString(report.Host.Architecture, maxArchitectureBytes) ||
		report.Host.LogicalCPUs <= 0 || report.Host.LogicalCPUs > MaxLogicalCPUs ||
		report.Host.MemoryBytes == 0 || report.Host.CollectedAt.IsZero() ||
		report.Host.Evidence != EvidenceLocallyObserved ||
		report.NVIDIA.CollectedAt.IsZero() ||
		report.NVIDIA.Evidence != EvidenceLocallyObserved ||
		len(report.NVIDIA.Devices) > DefaultMaxGPUDevices ||
		!validOptionalObservedString(report.NVIDIA.DriverVersion, maxDriverBytes) {
		return errors.New("invalid resource observation")
	}
	switch report.NVIDIA.Status {
	case "available":
		if report.NVIDIA.DriverVersion == "" || len(report.NVIDIA.Devices) == 0 {
			return errors.New("invalid resource observation")
		}
	case "no-devices":
		if report.NVIDIA.DriverVersion == "" || len(report.NVIDIA.Devices) != 0 {
			return errors.New("invalid resource observation")
		}
	case "unavailable":
		if report.NVIDIA.DriverVersion != "" || len(report.NVIDIA.Devices) != 0 {
			return errors.New("invalid resource observation")
		}
	case "degraded":
	default:
		return errors.New("invalid resource observation")
	}
	previousIndex := -1
	for _, device := range report.NVIDIA.Devices {
		if device.Index <= previousIndex || device.Index < 0 ||
			device.Index >= DefaultMaxGPUDevices ||
			!validOptionalObservedString(device.Class, maxDeviceNameBytes) ||
			device.VRAMUsedBytes > device.VRAMBytes ||
			device.CUDAComputeMajor < 0 || device.CUDAComputeMajor > 99 ||
			device.CUDAComputeMinor < 0 || device.CUDAComputeMinor > 99 ||
			device.TemperatureC != nil && *device.TemperatureC > maxTemperatureC ||
			!validPowerObservation(device) {
			return errors.New("invalid resource observation")
		}
		previousIndex = device.Index
	}
	return nil
}

func validPowerObservation(device NVIDIADevice) bool {
	switch device.PowerState {
	case "unavailable":
		return device.PowerMilliwatts == nil && device.PowerLimitMilliwatts == nil
	case "normal", "above-limit":
		if device.PowerMilliwatts == nil || device.PowerLimitMilliwatts == nil ||
			*device.PowerLimitMilliwatts == 0 {
			return false
		}
		above := *device.PowerMilliwatts > *device.PowerLimitMilliwatts
		return above == (device.PowerState == "above-limit")
	default:
		return false
	}
}

func validObservedString(value string, maximum int) bool {
	return value != "" && validOptionalObservedString(value, maximum)
}

func validOptionalObservedString(value string, maximum int) bool {
	return len(value) <= maximum && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}
