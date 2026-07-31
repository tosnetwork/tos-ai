package probe

import (
	"errors"
	"testing"
)

func TestCollectHost(t *testing.T) {
	host, err := CollectHost()
	if err != nil {
		t.Fatal(err)
	}
	if host.OS == "" || host.Architecture == "" || host.LogicalCPUs < 1 ||
		host.Evidence != "locally-observed" {
		t.Fatalf("invalid host probe: %#v", host)
	}
}

type fakeNVIDIA struct {
	initErr  error
	count    int
	countErr error
	devices  []fakeGPU
}

func (f *fakeNVIDIA) Init() error                    { return f.initErr }
func (f *fakeNVIDIA) Shutdown() error                { return nil }
func (f *fakeNVIDIA) DriverVersion() (string, error) { return "560.1", nil }
func (f *fakeNVIDIA) DeviceCount() (int, error)      { return f.count, f.countErr }
func (f *fakeNVIDIA) Device(index int) (NVIDIADeviceBackend, error) {
	if index < 0 || index >= len(f.devices) {
		return nil, errors.New("missing")
	}
	return f.devices[index], nil
}

type fakeGPU struct {
	name         string
	total, used  uint64
	major, minor int
	temperature  uint32
	power, limit uint32
}

func (f fakeGPU) Name() (string, error)                    { return f.name, nil }
func (f fakeGPU) Memory() (uint64, uint64, error)          { return f.total, f.used, nil }
func (f fakeGPU) ComputeCapability() (int, int, error)     { return f.major, f.minor, nil }
func (f fakeGPU) TemperatureC() (uint32, error)            { return f.temperature, nil }
func (f fakeGPU) PowerMilliwatts() (uint32, uint32, error) { return f.power, f.limit, nil }

func TestCollectNVIDIAMultipleDevices(t *testing.T) {
	report := CollectNVIDIA(&fakeNVIDIA{
		count: 2,
		devices: []fakeGPU{
			{name: "NVIDIA A", total: 16 << 30, used: 1 << 30, major: 8, minor: 6, temperature: 55, power: 100_000, limit: 250_000},
			{name: "NVIDIA B", total: 24 << 30, used: 2 << 30, major: 9, minor: 0, temperature: 60, power: 120_000, limit: 300_000},
		},
	})
	if report.Status != "available" || len(report.Devices) != 2 ||
		report.Devices[1].VRAMBytes != 24<<30 {
		t.Fatalf("unexpected report: %#v", report)
	}
}

func TestCollectNVIDIAUnavailableDoesNotFail(t *testing.T) {
	report := CollectNVIDIA(&fakeNVIDIA{initErr: errors.New("no driver")})
	if report.Status != "unavailable" || len(report.Devices) != 0 {
		t.Fatalf("unexpected report: %#v", report)
	}
	report = CollectNVIDIA(&fakeNVIDIA{countErr: errors.New("driver error")})
	if report.Status != "degraded" {
		t.Fatalf("driver error status = %q", report.Status)
	}
	report = CollectNVIDIA(&fakeNVIDIA{})
	if report.Status != "no-devices" || len(report.Devices) != 0 {
		t.Fatalf("no-GPU report = %#v", report)
	}
}

func TestCollectNVIDIAClampsCountAndRejectsAnomalies(t *testing.T) {
	devices := make([]fakeGPU, DefaultMaxGPUDevices)
	for index := range devices {
		devices[index] = fakeGPU{
			name: "GPU\x00", total: 1, used: 2, major: 500, temperature: 999,
			power: 2, limit: 1,
		}
	}
	report := CollectNVIDIA(&fakeNVIDIA{count: DefaultMaxGPUDevices + 50, devices: devices})
	if report.Status != "degraded" || len(report.Devices) != DefaultMaxGPUDevices {
		t.Fatalf("unexpected bounded report: status=%q devices=%d", report.Status, len(report.Devices))
	}
	if report.Devices[0].VRAMBytes != 0 || report.Devices[0].TemperatureC != nil ||
		report.Devices[0].Class != "GPU" {
		t.Fatalf("anomalous fields were exposed: %#v", report.Devices[0])
	}
}
