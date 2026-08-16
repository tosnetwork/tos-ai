//go:build darwin

package probe

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func effectiveResources() (uint64, int, error) {
	memory, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, 0, err
	}
	return memory, runtime.NumCPU(), nil
}
