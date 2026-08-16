//go:build !linux && !darwin

package dirlock

import (
	"errors"
	"os"
)

func openAndLock(string) (*os.File, error) {
	return nil, errors.New("private directory ownership locks require Linux")
}

func unlockAndClose(file *os.File) error {
	if file != nil {
		_ = file.Close()
	}
	return errors.New("private directory ownership locks require Linux")
}
