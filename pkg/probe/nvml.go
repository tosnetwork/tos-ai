package probe

import (
	"errors"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

type NVMLBackend struct {
	library nvml.Interface
}

func NewNVMLBackend() *NVMLBackend {
	return &NVMLBackend{library: nvml.New()}
}

func (b *NVMLBackend) Init() error {
	return nvmlError(b.library.Init())
}

func (b *NVMLBackend) Shutdown() error {
	return nvmlError(b.library.Shutdown())
}

func (b *NVMLBackend) DriverVersion() (string, error) {
	value, result := b.library.SystemGetDriverVersion()
	return value, nvmlError(result)
}

func (b *NVMLBackend) DeviceCount() (int, error) {
	value, result := b.library.DeviceGetCount()
	return value, nvmlError(result)
}

func (b *NVMLBackend) Device(index int) (NVIDIADeviceBackend, error) {
	value, result := b.library.DeviceGetHandleByIndex(index)
	if err := nvmlError(result); err != nil {
		return nil, err
	}
	return nvmlDevice{library: b.library, device: value}, nil
}

type nvmlDevice struct {
	library nvml.Interface
	device  nvml.Device
}

func (d nvmlDevice) Name() (string, error) {
	value, result := d.library.DeviceGetName(d.device)
	return value, nvmlError(result)
}

func (d nvmlDevice) Memory() (uint64, uint64, error) {
	value, result := d.library.DeviceGetMemoryInfo(d.device)
	return value.Total, value.Used, nvmlError(result)
}

func (d nvmlDevice) ComputeCapability() (int, int, error) {
	major, minor, result := d.library.DeviceGetCudaComputeCapability(d.device)
	return major, minor, nvmlError(result)
}

func (d nvmlDevice) TemperatureC() (uint32, error) {
	value, result := d.library.DeviceGetTemperature(d.device, nvml.TEMPERATURE_GPU)
	return value, nvmlError(result)
}

func (d nvmlDevice) PowerMilliwatts() (uint32, uint32, error) {
	used, usedResult := d.library.DeviceGetPowerUsage(d.device)
	limit, limitResult := d.library.DeviceGetPowerManagementLimit(d.device)
	if err := nvmlError(usedResult); err != nil {
		return 0, 0, err
	}
	return used, limit, nvmlError(limitResult)
}

func nvmlError(result nvml.Return) error {
	if result == nvml.SUCCESS {
		return nil
	}
	return errors.New("NVML operation unavailable")
}
