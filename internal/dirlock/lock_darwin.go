//go:build darwin

package dirlock

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func openAndLock(path string) (*os.File, error) {
	directoryInfo, err := os.Lstat(filepath.Dir(path))
	if err != nil || !directoryInfo.IsDir() ||
		directoryInfo.Mode().Perm() != 0o700 ||
		directoryInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid private ownership directory")
	}
	directoryStat, valid := directoryInfo.Sys().(*syscall.Stat_t)
	if !valid || directoryStat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("invalid private ownership directory")
	}
	descriptor, err := syscall.Open(
		path,
		syscall.O_RDWR|syscall.O_CREAT|syscall.O_CLOEXEC|syscall.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, errors.New("open private directory ownership lock")
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = syscall.Close(descriptor)
		return nil, errors.New("open private directory ownership lock")
	}
	success := false
	defer func() {
		if !success {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("invalid private directory ownership lock")
	}
	stat, valid := info.Sys().(*syscall.Stat_t)
	if !valid || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return nil, errors.New("invalid private directory ownership lock")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !os.SameFile(pathInfo, info) {
		return nil, errors.New("substituted private directory ownership lock")
	}
	if err := syscall.Flock(descriptor, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrHeld
		}
		return nil, errors.New("acquire private directory ownership lock")
	}
	pathInfo, err = os.Lstat(path)
	if err != nil || !os.SameFile(pathInfo, info) {
		_ = syscall.Flock(descriptor, syscall.LOCK_UN)
		return nil, errors.New("substituted private directory ownership lock")
	}
	success = true
	return file, nil
}

func unlockAndClose(file *os.File) error {
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil || closeErr != nil {
		return errors.New("release private directory ownership lock")
	}
	return nil
}
