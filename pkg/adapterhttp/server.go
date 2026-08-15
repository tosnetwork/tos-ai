package adapterhttp

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type ServerConfig struct {
	Address           string
	CertificateFile   string
	PrivateKeyFile    string
	ClientCAFile      string
	Boundary          BoundaryConfig
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

func NewServer(handler http.Handler, config ServerConfig) (*http.Server, error) {
	if handler == nil {
		return nil, errors.New("public adapter handler is required")
	}
	if _, _, err := net.SplitHostPort(config.Address); err != nil {
		return nil, errors.New("public adapter address must include host and port")
	}
	tlsConfig, err := loadTLS(config.CertificateFile, config.PrivateKeyFile, config.ClientCAFile)
	if err != nil {
		return nil, err
	}
	bounded, err := NewBoundary(handler, config.Boundary)
	if err != nil {
		return nil, err
	}
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = 5 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 30 * time.Second
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = 2 * time.Minute
	}
	if config.MaxHeaderBytes == 0 {
		config.MaxHeaderBytes = 32 << 10
	}
	if config.ReadHeaderTimeout <= 0 || config.ReadHeaderTimeout > 30*time.Second ||
		config.ReadTimeout <= 0 || config.ReadTimeout > 5*time.Minute ||
		config.IdleTimeout <= 0 || config.IdleTimeout > 10*time.Minute ||
		config.MaxHeaderBytes < 4<<10 || config.MaxHeaderBytes > 64<<10 {
		return nil, errors.New("invalid public adapter HTTP bounds")
	}
	return &http.Server{Addr: config.Address, Handler: bounded, TLSConfig: tlsConfig,
		ReadHeaderTimeout: config.ReadHeaderTimeout, ReadTimeout: config.ReadTimeout,
		IdleTimeout: config.IdleTimeout, MaxHeaderBytes: config.MaxHeaderBytes}, nil
}

// ListenAndServe starts the preloaded TLS server. Empty certificate arguments
// ensure net/http uses only the already validated in-memory key pair.
func ListenAndServe(server *http.Server) error {
	if server == nil || server.TLSConfig == nil || len(server.TLSConfig.Certificates) != 1 {
		return errors.New("public adapter TLS server is not configured")
	}
	return server.ListenAndServeTLS("", "")
}

func loadTLS(certificatePath, keyPath, clientCAPath string) (*tls.Config, error) {
	certificatePEM, err := readCredentialFile(certificatePath, 2<<20)
	if err != nil {
		return nil, errors.New("read public adapter certificate")
	}
	keyPEM, err := readCredentialFile(keyPath, 1<<20)
	if err != nil {
		return nil, errors.New("read public adapter private key")
	}
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, errors.New("invalid public adapter TLS key pair")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}}
	if clientCAPath != "" {
		caPEM, err := readCredentialFile(clientCAPath, 2<<20)
		if err != nil {
			return nil, errors.New("read public adapter client CA")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("public adapter client CA contains no certificate")
		}
		config.ClientCAs, config.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return config, nil
}

func readCredentialFile(path string, maximum int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("credential path must be absolute and clean")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 ||
		before.Mode().Perm()&0o022 != 0 || before.Size() <= 0 || before.Size() > maximum {
		return nil, errors.New("credential must be a protected bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		return nil, errors.New("credential changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) > maximum {
		return nil, errors.New("credential changed outside its size bound")
	}
	return raw, nil
}
