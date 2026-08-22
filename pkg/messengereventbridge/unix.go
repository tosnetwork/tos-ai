package messengereventbridge

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type UnixService struct {
	server    *Server
	listeners []net.Listener
	handlers  []http.Handler
	paths     []string
	closeOnce sync.Once
}

// ListenUnix creates only the configured profile sockets. Their private parent
// directories must already exist; an existing filesystem entry is never
// removed or replaced.
func ListenUnix(server *Server, a2aSocket, mcpSocket string) (*UnixService, error) {
	if server == nil || (a2aSocket == "" && mcpSocket == "") ||
		(a2aSocket != "" && server.a2a == nil) || (mcpSocket != "" && server.mcp == nil) ||
		(a2aSocket != "" && a2aSocket == mcpSocket) {
		return nil, errors.New("invalid Messenger protocol Unix service configuration")
	}
	service := &UnixService{server: server}
	for index, path := range []string{a2aSocket, mcpSocket} {
		if path == "" {
			continue
		}
		listener, err := listenPrivateUnix(path)
		if err != nil {
			_ = service.Close()
			return nil, err
		}
		service.listeners = append(service.listeners, listener)
		if index == 0 {
			service.handlers = append(service.handlers, server.a2aHandler())
		} else {
			service.handlers = append(service.handlers, server.mcpHandler())
		}
		service.paths = append(service.paths, path)
	}
	return service, nil
}

func listenPrivateUnix(path string) (net.Listener, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("Messenger protocol socket must be a clean absolute path")
	}
	parent, err := os.Stat(filepath.Dir(path))
	stat, owned := parentSyscallStat(parent)
	if err != nil || !parent.IsDir() || parent.Mode().Perm()&0o077 != 0 || !owned || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("Messenger protocol socket parent must be an existing private directory")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return nil, errors.New("Messenger protocol socket path already exists or cannot be inspected")
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return listener, nil
}

func parentSyscallStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func (s *UnixService) Run(ctx context.Context) error {
	if s == nil || s.server == nil || len(s.listeners) == 0 || ctx == nil {
		return errors.New("invalid Messenger protocol Unix service")
	}
	servers := make([]*http.Server, len(s.listeners))
	errorsOut := make(chan error, len(s.listeners))
	for index, listener := range s.listeners {
		server := &http.Server{Handler: s.handlers[index], ReadHeaderTimeout: 5 * time.Second}
		servers[index] = server
		go func(server *http.Server, listener net.Listener) {
			err := server.Serve(listener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			errorsOut <- err
		}(server, listener)
	}
	select {
	case <-ctx.Done():
		for _, server := range servers {
			_ = server.Shutdown(context.Background())
		}
		_ = s.Close()
		for range servers {
			<-errorsOut
		}
		return nil
	case err := <-errorsOut:
		for _, server := range servers {
			_ = server.Close()
		}
		_ = s.Close()
		return err
	}
}

func (s *UnixService) Close() error {
	if s == nil {
		return nil
	}
	var result error
	s.closeOnce.Do(func() {
		for _, listener := range s.listeners {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) && result == nil {
				result = err
			}
		}
		for _, path := range s.paths {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) && result == nil {
				result = err
			}
		}
	})
	return result
}
