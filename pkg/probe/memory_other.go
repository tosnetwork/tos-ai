//go:build !linux

package probe

import "runtime"

func effectiveResources() (uint64, int, error) {
	return 0, runtime.NumCPU(), nil
}
