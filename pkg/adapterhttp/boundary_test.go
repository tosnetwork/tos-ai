package adapterhttp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testToken = "0123456789abcdef0123456789abcdef"

func TestBoundaryRequiresAuthenticationAndRejectsBrowserOrigin(t *testing.T) {
	called := 0
	boundary, err := NewBoundary(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		called++
		writer.WriteHeader(http.StatusNoContent)
	}), BoundaryConfig{BearerToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		authorization, origin string
		want                  int
	}{
		{"", "", http.StatusUnauthorized},
		{"Bearer wrong", "", http.StatusUnauthorized},
		{"Bearer " + testToken, "https://browser.example", http.StatusForbidden},
		{"Bearer " + testToken, "", http.StatusNoContent},
	} {
		request := httptest.NewRequest(http.MethodPost, "https://provider.example/a2a", nil)
		request.Header.Set("Authorization", test.authorization)
		request.Header.Set("Origin", test.origin)
		response := httptest.NewRecorder()
		boundary.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatalf("status = %d, want %d", response.Code, test.want)
		}
	}
	if called != 1 {
		t.Fatal("unauthorized request reached adapter")
	}
}

func TestBoundaryEnforcesBodyAndConcurrencyBounds(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	boundary, err := NewBoundary(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		entered <- struct{}{}
		<-release
		if _, err := io.ReadAll(request.Body); err != nil {
			http.Error(writer, "bounded", http.StatusRequestEntityTooLarge)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}), BoundaryConfig{BearerToken: testToken, MaxRequestBytes: 4, MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		request := authorizedRequest(bytes.NewReader([]byte("1234")))
		boundary.ServeHTTP(httptest.NewRecorder(), request)
	}()
	<-entered
	response := httptest.NewRecorder()
	boundary.ServeHTTP(response, authorizedRequest(nil))
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "1" {
		t.Fatal("concurrent request was not rejected deterministically")
	}
	close(release)
	wait.Wait()

	response = httptest.NewRecorder()
	boundary.ServeHTTP(response, authorizedRequest(bytes.NewReader([]byte("12345"))))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatal("oversized request was accepted")
	}
}

func TestServerRequiresProtectedTLS13KeyPair(t *testing.T) {
	directory := t.TempDir()
	certificatePath, keyPath := writeTestCertificate(t, directory)
	server, err := NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), ServerConfig{
		Address: "127.0.0.1:0", CertificateFile: certificatePath, PrivateKeyFile: keyPath,
		Boundary: BoundaryConfig{BearerToken: testToken},
	})
	if err != nil {
		t.Fatal(err)
	}
	if server.TLSConfig.MinVersion != 0x0304 || server.ReadHeaderTimeout == 0 || server.MaxHeaderBytes == 0 {
		t.Fatal("public adapter server omitted TLS 1.3 or HTTP bounds")
	}
	if err := os.Chmod(keyPath, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), ServerConfig{
		Address: "127.0.0.1:0", CertificateFile: certificatePath, PrivateKeyFile: keyPath,
		Boundary: BoundaryConfig{BearerToken: testToken},
	}); err == nil {
		t.Fatal("public adapter accepted a writable private key")
	}
}

func authorizedRequest(body io.Reader) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "https://provider.example/adapter", body)
	request.Header.Set("Authorization", "Bearer "+testToken)
	return request
}

func writeTestCertificate(t *testing.T, directory string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certificatePath := filepath.Join(directory, "server.crt")
	keyPath := filepath.Join(directory, "server.key")
	if err := os.WriteFile(certificatePath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certificatePath, keyPath
}
