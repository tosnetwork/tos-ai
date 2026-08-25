//go:build windows

package commercegate

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// Windows does not expose a portable directory fsync through os.File. Atomic
// replace and the per-record file flush remain the strongest available local
// primitive; the process-wide state lock prevents concurrent substitution.
func syncParentDirectory(string) error { return nil }

func openReadOnlyNoFollow(path string) (*os.File, error) {
	encoded, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(encoded, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil || info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DIRECTORY) != 0 {
		_ = windows.CloseHandle(handle)
		return nil, errors.New("execution input is a reparse point or directory")
	}
	return os.NewFile(uintptr(handle), path), nil
}
