package probe

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	DefaultMaxGPUDevices = 16
	maxDeviceNameBytes   = 96
	maxDriverBytes       = 64
	maxTemperatureC      = 150
)

type NVIDIABackend interface {
	Init() error
	Shutdown() error
	DriverVersion() (string, error)
	DeviceCount() (int, error)
	Device(int) (NVIDIADeviceBackend, error)
}

type NVIDIADeviceBackend interface {
	Name() (string, error)
	Memory() (total uint64, used uint64, err error)
	ComputeCapability() (major int, minor int, err error)
	TemperatureC() (uint32, error)
	PowerMilliwatts() (used uint32, limit uint32, err error)
}

type NVIDIAReport struct {
	Status        string         `json:"status"`
	DriverVersion string         `json:"driverVersion,omitempty"`
	Devices       []NVIDIADevice `json:"devices"`
	CollectedAt   time.Time      `json:"collectedAt"`
	Evidence      EvidenceLevel  `json:"evidence"`
}

type NVIDIADevice struct {
	Index                int     `json:"index"`
	Class                string  `json:"class,omitempty"`
	VRAMBytes            uint64  `json:"vramBytes,omitempty"`
	VRAMUsedBytes        uint64  `json:"vramUsedBytes,omitempty"`
	CUDAComputeMajor     int     `json:"cudaComputeMajor,omitempty"`
	CUDAComputeMinor     int     `json:"cudaComputeMinor,omitempty"`
	TemperatureC         *uint32 `json:"temperatureC,omitempty"`
	PowerMilliwatts      *uint32 `json:"powerMilliwatts,omitempty"`
	PowerLimitMilliwatts *uint32 `json:"powerLimitMilliwatts,omitempty"`
	PowerState           string  `json:"powerState"`
}

func CollectNVIDIA(backend NVIDIABackend) NVIDIAReport {
	report := NVIDIAReport{
		Status:      "unavailable",
		Devices:     make([]NVIDIADevice, 0),
		CollectedAt: time.Now().UTC(),
		Evidence:    EvidenceLocallyObserved,
	}
	if backend == nil || backend.Init() != nil {
		return report
	}
	defer backend.Shutdown()

	driver, err := backend.DriverVersion()
	if err != nil {
		report.Status = "degraded"
		return report
	}
	report.DriverVersion = boundedPrintable(driver, maxDriverBytes)
	count, err := backend.DeviceCount()
	if err != nil || count < 0 {
		report.Status = "degraded"
		return report
	}
	report.Status = "available"
	if count == 0 {
		report.Status = "no-devices"
		return report
	}
	if count > DefaultMaxGPUDevices {
		count = DefaultMaxGPUDevices
		report.Status = "degraded"
	}
	report.Devices = make([]NVIDIADevice, 0, count)
	for index := 0; index < count; index++ {
		device, deviceErr := backend.Device(index)
		if deviceErr != nil {
			report.Status = "degraded"
			continue
		}
		value := NVIDIADevice{Index: index, PowerState: "unavailable"}
		if name, nameErr := device.Name(); nameErr == nil {
			value.Class = boundedPrintable(name, maxDeviceNameBytes)
		} else {
			report.Status = "degraded"
		}
		if total, used, memoryErr := device.Memory(); memoryErr == nil && total > 0 && used <= total {
			value.VRAMBytes = total
			value.VRAMUsedBytes = used
		} else {
			report.Status = "degraded"
		}
		if major, minor, capabilityErr := device.ComputeCapability(); capabilityErr == nil &&
			major >= 1 && major <= 99 && minor >= 0 && minor <= 99 {
			value.CUDAComputeMajor = major
			value.CUDAComputeMinor = minor
		} else {
			report.Status = "degraded"
		}
		if temperature, temperatureErr := device.TemperatureC(); temperatureErr == nil &&
			temperature <= maxTemperatureC {
			value.TemperatureC = &temperature
		} else if temperatureErr == nil {
			report.Status = "degraded"
		}
		if used, limit, powerErr := device.PowerMilliwatts(); powerErr == nil && limit > 0 {
			value.PowerMilliwatts = &used
			value.PowerLimitMilliwatts = &limit
			value.PowerState = "normal"
			if used > limit {
				value.PowerState = "above-limit"
				report.Status = "degraded"
			}
		}
		report.Devices = append(report.Devices, value)
	}
	return report
}

func boundedPrintable(value string, maximum int) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	if len(value) <= maximum {
		return value
	}
	for len(value) > maximum {
		_, size := utf8.DecodeLastRuneInString(value)
		value = value[:len(value)-size]
	}
	return value
}
