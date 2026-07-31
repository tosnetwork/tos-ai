//go:build linux

package probe

import (
	"runtime"
	"syscall"
)

func effectiveResources() (uint64, int, error) {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0, 0, err
	}
	memory := info.Totalram * uint64(info.Unit)
	return effectiveLinuxResources(
		memory, runtime.NumCPU(), readLimitedResourceFile,
	)
}
