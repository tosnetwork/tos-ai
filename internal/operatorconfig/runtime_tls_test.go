package operatorconfig

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

func TestLoadRuntimeTLSMutualAuthentication(t *testing.T) {
	server, rootPEM, clientCertificatePEM, clientKeyPEM := newMutualTLSServer(t)
	defer server.Close()
	rootFile := writePrivate(t, "runtime-ca.pem", string(rootPEM), 0o600)
	clientCertificateFile := writePrivate(
		t, "runtime-client.pem", string(clientCertificatePEM), 0o600,
	)
	clientKeyFile := writePrivate(
		t, "runtime-client-key.pem", string(clientKeyPEM), 0o600,
	)
	config := fmt.Sprintf(`{
		"version":2,
		"adapters":[{
			"type":"openai-compatible",
			"baseUrl":%q,
			"model":"approved-model",
			"modelDigest":"sha256:%s",
			"runtimeRevision":"private-vllm-v1",
			"maxOutputBytes":64,
			"tls":{
				"caFile":%q,
				"clientCertFile":%q,
				"clientKeyFile":%q,
				"serverName":"runtime.internal"
			}
		}]
	}`, server.URL, strings.Repeat("a", 64), rootFile,
		clientCertificateFile, clientKeyFile)
	configuration, err := Load(
		writePrivate(t, "runtime-mtls.json", config, 0o600),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAdapters(configuration.Adapters)
	response, err := configuration.Adapters[0].Execute(
		context.Background(), airuntime.Request{
			RequestID: "mutual-tls-request", Operation: "generate",
			Model: "approved-model", Payload: []byte("hello"),
			MaxOutputBytes: 64,
		},
	)
	if err != nil || string(response.Output) != "mutually-authenticated" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
}

func TestLoadRuntimeTLSRejectsUnsafeFilesAndAmbiguousPolicy(t *testing.T) {
	server, rootPEM, clientCertificatePEM, clientKeyPEM := newMutualTLSServer(t)
	defer server.Close()
	rootFile := writePrivate(t, "root.pem", string(rootPEM), 0o600)
	clientCertificateFile := writePrivate(
		t, "client.pem", string(clientCertificatePEM), 0o600,
	)
	clientKeyFile := writePrivate(t, "client-key.pem", string(clientKeyPEM), 0o600)
	insecureRoot := writePrivate(t, "insecure-root.pem", string(rootPEM), 0o644)
	malformedRoot := writePrivate(t, "malformed-root.pem", "not PEM", 0o600)
	oversizedRoot := writePrivate(
		t, "oversized-root.pem", strings.Repeat("x", (1<<20)+1), 0o600,
	)
	oversizedKey := writePrivate(
		t, "oversized-key.pem", strings.Repeat("x", int(maxTLSPrivateKeyBytes)+1), 0o600,
	)
	tests := []struct {
		name    string
		version int
		baseURL string
		tlsJSON string
	}{
		{
			name: "version one identity", version: 1, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(`{"caFile":%q}`, rootFile),
		},
		{
			name: "empty identity", version: 2, baseURL: server.URL,
			tlsJSON: `{}`,
		},
		{
			name: "certificate without key", version: 2, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(`{"clientCertFile":%q}`, clientCertificateFile),
		},
		{
			name: "key without certificate", version: 2, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(`{"clientKeyFile":%q}`, clientKeyFile),
		},
		{
			name: "insecure root file", version: 2, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(`{"caFile":%q}`, insecureRoot),
		},
		{
			name: "malformed root", version: 2, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(`{"caFile":%q}`, malformedRoot),
		},
		{
			name: "oversized root", version: 2, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(`{"caFile":%q}`, oversizedRoot),
		},
		{
			name: "oversized key", version: 2, baseURL: server.URL,
			tlsJSON: fmt.Sprintf(
				`{"clientCertFile":%q,"clientKeyFile":%q}`,
				clientCertificateFile, oversizedKey,
			),
		},
		{
			name: "TLS over plaintext", version: 2,
			baseURL: "http://127.0.0.1:11434",
			tlsJSON: fmt.Sprintf(`{"caFile":%q}`, rootFile),
		},
		{
			name: "wildcard server name", version: 2, baseURL: server.URL,
			tlsJSON: `{"serverName":"*.internal"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := runtimeTLSConfig(test.version, test.baseURL, test.tlsJSON)
			_, err := Load(writePrivate(t, "runtime.json", config, 0o600))
			if err == nil {
				t.Fatal("unsafe runtime TLS policy accepted")
			}
			if strings.Contains(err.Error(), rootFile) ||
				strings.Contains(err.Error(), clientKeyFile) {
				t.Fatalf("runtime TLS error leaked a private path: %v", err)
			}
		})
	}
}

func TestRuntimeTLSPEMParsersRejectDuplicatesAndExtraMaterial(t *testing.T) {
	server, rootPEM, clientCertificatePEM, clientKeyPEM := newMutualTLSServer(t)
	defer server.Close()
	if _, err := parseRootCertificates(append(rootPEM, rootPEM...)); err == nil {
		t.Fatal("duplicate runtime root accepted")
	}
	if _, err := parseRootCertificates(clientCertificatePEM); err == nil {
		t.Fatal("non-CA runtime root accepted")
	}
	if err := validateClientCertificatePEM(append(
		clientCertificatePEM, clientKeyPEM...,
	)); err == nil {
		t.Fatal("private key accepted in client certificate file")
	}
	if err := validatePrivateKeyPEM(append(
		clientKeyPEM, clientKeyPEM...,
	)); err == nil {
		t.Fatal("multiple client keys accepted")
	}
}

func runtimeTLSConfig(version int, baseURL string, tlsJSON string) string {
	return fmt.Sprintf(`{
		"version":%d,
		"adapters":[{
			"type":"ollama",
			"baseUrl":%q,
			"model":"approved",
			"modelDigest":"sha256:%s",
			"tls":%s
		}]
	}`, version, baseURL, strings.Repeat("b", 64), tlsJSON)
}

func newMutualTLSServer(
	t *testing.T,
) (*httptest.Server, []byte, []byte, []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := certificateTemplate(t, "tos-ai-test-ca", true, nil)
	caDER, err := x509.CreateCertificate(
		rand.Reader, ca, ca, &caKey.PublicKey, caKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverCertificatePEM, serverKeyPEM := issueCertificate(
		t, ca, caKey, "runtime.internal", x509.ExtKeyUsageServerAuth,
	)
	clientCertificatePEM, clientKeyPEM := issueCertificate(
		t, ca, caKey, "tos-ai-worker", x509.ExtKeyUsageClientAuth,
	)
	serverCertificate, err := tls.X509KeyPair(
		serverCertificatePEM, serverKeyPEM,
	)
	if err != nil {
		t.Fatal(err)
	}
	clientRoots := x509.NewCertPool()
	clientRoots.AddCert(ca)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) != 1 ||
			request.TLS.PeerCertificates[0].Subject.CommonName != "tos-ai-worker" {
			t.Error("runtime request did not carry the approved client identity")
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = writer.Write([]byte(
			`{"choices":[{"message":{"content":"mutually-authenticated"}}],"usage":{}}`,
		))
	}))
	server.TLS = &tls.Config{ // #nosec G402 -- generated test-only PKI.
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientRoots,
	}
	server.StartTLS()
	return server, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: caDER,
	}), clientCertificatePEM, clientKeyPEM
}

func issueCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	commonName string,
	usage x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := certificateTemplate(t, commonName, false, []x509.ExtKeyUsage{usage})
	if usage == x509.ExtKeyUsageServerAuth {
		template.DNSNames = []string{"runtime.internal"}
	}
	der, err := x509.CreateCertificate(
		rand.Reader, template, ca, &key.PublicKey, caKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: der,
	}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func certificateTemplate(
	t *testing.T,
	commonName string,
	isCA bool,
	usage []x509.ExtKeyUsage,
) *x509.Certificate {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	keyUsage := x509.KeyUsageDigitalSignature
	if isCA {
		keyUsage |= x509.KeyUsageCertSign
	}
	return &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              keyUsage,
		ExtKeyUsage:           usage,
		IsCA:                  isCA,
		BasicConstraintsValid: true,
	}
}
