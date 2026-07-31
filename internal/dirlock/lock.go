// Package dirlock provides non-blocking process ownership for private local
// state directories. It is an operational consistency boundary, not a
// substitute for filesystem permissions or protection from a hostile process
// running as the same user.
package dirlock

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const MaxNameBytes = 128

var ErrHeld = errors.New("private directory is already managed")

type Lock struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
}

func Acquire(directory string, name string) (*Lock, error) {
	if directory == "" || !filepath.IsAbs(directory) ||
		name == "" || len(name) > MaxNameBytes ||
		filepath.Base(name) != name || name == "." || name == ".." ||
		strings.IndexByte(name, filepath.Separator) >= 0 {
		return nil, errors.New("invalid directory lock configuration")
	}
	file, err := openAndLock(filepath.Join(directory, name))
	if err != nil {
		return nil, err
	}
	return &Lock{file: file}, nil
}

func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	file := l.file
	l.file = nil
	if file == nil {
		return errors.New("invalid directory ownership lock")
	}
	return unlockAndClose(file)
}
