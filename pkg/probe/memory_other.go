//go:build !linux

package probe

func totalMemory() (uint64, error) {
	return 0, nil
}
