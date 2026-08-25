//go:build !linux

package probe

import "errors"

type NVMLBackend struct{}

func NewNVMLBackend() *NVMLBackend { return &NVMLBackend{} }

func (*NVMLBackend) Init() error     { return errors.New("NVML is available only on Linux") }
func (*NVMLBackend) Shutdown() error { return nil }
func (*NVMLBackend) DriverVersion() (string, error) {
	return "", errors.New("NVML is available only on Linux")
}
func (*NVMLBackend) DeviceCount() (int, error) {
	return 0, errors.New("NVML is available only on Linux")
}
func (*NVMLBackend) Device(int) (NVIDIADeviceBackend, error) {
	return nil, errors.New("NVML is available only on Linux")
}
