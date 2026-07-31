package unixserver

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type Listener struct {
	net.Listener
	path      string
	sem       chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func Listen(path string) (*Listener, error) {
	return ListenLimited(path, 128)
}

func ListenLimited(path string, maxConnections int) (*Listener, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("Unix socket path must be absolute")
	}
	if maxConnections <= 0 || maxConnections > 4096 {
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
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("Unix socket directory must be a private non-symlink directory")
	}
	if existing, err := os.Lstat(path); err == nil {
		if existing.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("refusing to replace non-socket path")
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale Unix socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return &Listener{
		Listener: listener, path: path, sem: make(chan struct{}, maxConnections),
		closed: make(chan struct{}),
	}, nil
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
	var listenErr error
	l.closeOnce.Do(func() {
		close(l.closed)
		listenErr = l.Listener.Close()
	})
	removeErr := os.Remove(l.path)
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return removeErr
	}
	return listenErr
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
