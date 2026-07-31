package unixserver

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
)

const (
	activeSocketProbeLimit = 250 * time.Millisecond
	MaxConnectionsHard     = 4096
)

type Listener struct {
	net.Listener
	path       string
	sem        chan struct{}
	closed     chan struct{}
	ownership  *dirlock.Lock
	closeOnce  sync.Once
	closeError error
}

func Listen(path string) (*Listener, error) {
	return ListenLimited(path, 128)
}

func ListenLimited(path string, maxConnections int) (*Listener, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("Unix socket path must be absolute")
	}
	if maxConnections <= 0 || maxConnections > MaxConnectionsHard {
		return nil, errors.New("invalid Unix socket connection limit")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, fmt.Errorf("create socket directory: %w", err)
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 {
		return nil, errors.New("Unix socket directory must be a private non-symlink directory")
	}
	lockName, err := socketLockName(path)
	if err != nil {
		return nil, err
	}
	ownership, err := dirlock.Acquire(parent, lockName)
	if err != nil {
		return nil, errors.New("Unix socket is already managed")
	}
	if err := prepareSocketPath(path); err != nil {
		_ = ownership.Close()
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		_ = ownership.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		_ = ownership.Close()
		return nil, err
	}
	return &Listener{
		Listener: listener, path: path, sem: make(chan struct{}, maxConnections),
		closed: make(chan struct{}), ownership: ownership,
	}, nil
}

func socketLockName(path string) (string, error) {
	base := filepath.Base(path)
	name := "." + base + ".lock"
	if base == "." || base == ".." || len(name) > dirlock.MaxNameBytes {
		return "", errors.New("Unix socket name exceeds hard limits")
	}
	return name, nil
}

func prepareSocketPath(path string) error {
	existing, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("inspect Unix socket path")
	}
	if existing.Mode()&os.ModeSocket == 0 {
		return errors.New("refusing to replace non-socket path")
	}
	connection, dialErr := net.DialTimeout(
		"unix", path, activeSocketProbeLimit,
	)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("Unix socket already has an active listener")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return errors.New("cannot prove Unix socket is stale")
	}
	if err := os.Remove(path); err != nil {
		return errors.New("remove stale Unix socket")
	}
	return nil
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case l.sem <- struct{}{}:
	case <-l.closed:
		return nil, net.ErrClosed
	}
	connection, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitedConn{Conn: connection, release: func() { <-l.sem }}, nil
}

func (l *Listener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		close(l.closed)
		listenErr := l.Listener.Close()
		removeErr := os.Remove(l.path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		var ownershipErr error
		if l.ownership != nil {
			ownershipErr = l.ownership.Close()
		}
		l.closeError = errors.Join(listenErr, removeErr, ownershipErr)
	})
	return l.closeError
}

type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
