package runtimehttp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"testing"
	"time"
)

func testConfig(endpoint string) Config {
	return Config{
		BaseURL: endpoint, Timeout: time.Second, ConnectTimeout: time.Second,
		MaxConnections: 2, MaxResponseHeaderBytes: 4096,
	}
}

func TestBuildRejectsRemotePlaintextAndRedirects(t *testing.T) {
	if _, _, err := Build(testConfig("http://runtime.example")); err == nil {
		t.Fatal("remote plaintext hostname accepted")
	}
	config := testConfig("http://10.0.0.5")
	config.AllowedPlaintextCIDRs = []string{"10.0.0.0/24"}
	base, client, err := Build(config)
	if err != nil {
		t.Fatal(err)
	}
	if Endpoint(base, "/v1/test") != "http://10.0.0.5/v1/test" {
		t.Fatalf("endpoint=%q", Endpoint(base, "/v1/test"))
	}
	if err := client.CheckRedirect(&http.Request{}, nil); err == nil {
		t.Fatal("redirect accepted")
	}
	config.MaxConnections = MaxConnectionsHard + 1
	if _, _, err := Build(config); err == nil {
		t.Fatal("connection hard limit accepted")
	}
}

func TestBuildRejectsInvalidEndpointPorts(t *testing.T) {
	for _, endpoint := range []string{
		"http://localhost:",
		"http://localhost:0",
		"http://localhost:65536",
		"https://runtime.example:99999",
	} {
		if _, _, err := Build(testConfig(endpoint)); err == nil {
			t.Fatalf("invalid endpoint port accepted: %q", endpoint)
		}
	}
	if _, client, err := Build(testConfig("http://localhost:65535")); err != nil {
		t.Fatal(err)
	} else {
		client.CloseIdleConnections()
	}
}

func TestBuildUsesPrivateRootsAndMutualTLS(t *testing.T) {
	clientCertificate, clientRoot := selfSignedClientCertificate(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) != 1 {
			t.Error("runtime did not receive the configured client identity")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{ // #nosec G402 -- test server policy.
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  clientRoot,
	}
	server.StartTLS()
	defer server.Close()

	serverRoots := x509.NewCertPool()
	serverRoots.AddCert(server.Certificate())
	config := testConfig(server.URL)
	config.RootCAs = serverRoots
	config.ClientCertificate = &clientCertificate
	_, client, err := Build(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil ||
		transport.TLSClientConfig.MinVersion != tls.VersionTLS12 ||
		transport.TLSClientConfig.InsecureSkipVerify ||
		transport.TLSClientConfig.RootCAs == serverRoots ||
		len(transport.TLSClientConfig.Certificates) != 1 {
		t.Fatal("runtime client did not enforce an isolated bounded TLS policy")
	}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", response.StatusCode)
	}

	config.ClientCertificate = nil
	_, clientWithoutIdentity, err := Build(config)
	if err != nil {
		t.Fatal(err)
	}
	defer clientWithoutIdentity.CloseIdleConnections()
	if response, err := clientWithoutIdentity.Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("mTLS runtime accepted a client without an identity")
	}
}

func TestBuildRejectsUnsafeOrAmbiguousTLSAuthority(t *testing.T) {
	root := x509.NewCertPool()
	root.AddCert(testCertificate(t, true, x509.ExtKeyUsageClientAuth))
	tooManyRoots := x509.NewCertPool()
	for range MaxTLSRootsHard + 1 {
		tooManyRoots.AddCert(testCertificate(t, true, x509.ExtKeyUsageClientAuth))
	}
	clientCertificate, _ := selfSignedClientCertificate(t)
	tests := []struct {
		name   string
		config func() Config
	}{
		{name: "plaintext roots", config: func() Config {
			value := testConfig("http://localhost:11434")
			value.RootCAs = root
			return value
		}},
		{name: "plaintext client identity", config: func() Config {
			value := testConfig("http://localhost:11434")
			value.ClientCertificate = &clientCertificate
			return value
		}},
		{name: "empty roots", config: func() Config {
			value := testConfig("https://runtime.example")
			value.RootCAs = x509.NewCertPool()
			return value
		}},
		{name: "too many roots", config: func() Config {
			value := testConfig("https://runtime.example")
			value.RootCAs = tooManyRoots
			return value
		}},
		{name: "invalid server name", config: func() Config {
			value := testConfig("https://runtime.example")
			value.TLSServerName = "*.runtime.example"
			return value
		}},
		{name: "IP server name override", config: func() Config {
			value := testConfig("https://runtime.example")
			value.TLSServerName = "127.0.0.1"
			return value
		}},
		{name: "missing private key", config: func() Config {
			value := testConfig("https://runtime.example")
			value.ClientCertificate = &tls.Certificate{
				Certificate: clientCertificate.Certificate,
			}
			return value
		}},
		{name: "oversized chain", config: func() Config {
			value := testConfig("https://runtime.example")
			chain := make([][]byte, MaxTLSChainHard+1)
			for index := range chain {
				chain[index] = []byte{1}
			}
			value.ClientCertificate = &tls.Certificate{
				Certificate: chain, PrivateKey: clientCertificate.PrivateKey,
			}
			return value
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := Build(test.config()); err == nil {
				t.Fatal("unsafe runtime TLS authority accepted")
			}
		})
	}
}

func selfSignedClientCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := testCertificateTemplate(t, false, x509.ExtKeyUsageClientAuth)
	der, err := x509.CreateCertificate(
		rand.Reader, template, template, &key.PublicKey, key,
	)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return tls.Certificate{
		Certificate: [][]byte{der}, PrivateKey: key, Leaf: certificate,
	}, roots
}

func testCertificate(t *testing.T, isCA bool, usage x509.ExtKeyUsage) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := testCertificateTemplate(t, isCA, usage)
	der, err := x509.CreateCertificate(
		rand.Reader, template, template, &key.PublicKey, key,
	)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func testCertificateTemplate(
	t *testing.T,
	isCA bool,
	usage x509.ExtKeyUsage,
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
		Subject:               pkix.Name{CommonName: "tos-ai-runtime-test-" + serial.String()},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              keyUsage,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
		IsCA:                  isCA,
		BasicConstraintsValid: true,
	}
}

func TestBuildRejectsPlaintextCIDRThatSpansNonLocalAddresses(t *testing.T) {
	for _, value := range []string{
		"10.0.0.0/7",
		"172.16.0.0/11",
		"192.168.0.0/15",
		"169.254.0.0/15",
		"fc00::/6",
		"fe80::/9",
	} {
		t.Run(value, func(t *testing.T) {
			config := testConfig("http://10.0.0.5")
			config.AllowedPlaintextCIDRs = []string{value}
			if _, _, err := Build(config); err == nil {
				t.Fatal("CIDR spanning non-local addresses was accepted")
			}
		})
	}

	config := testConfig("https://runtime.example")
	config.AllowedPlaintextCIDRs = []string{"10.0.0.0/7"}
	if _, _, err := Build(config); err == nil {
		t.Fatal("invalid unused plaintext CIDR was accepted for HTTPS")
	}
}

func TestBuildAcceptsOnlyContainedLocalPlaintextCIDRs(t *testing.T) {
	config := testConfig("http://10.23.4.5")
	config.AllowedPlaintextCIDRs = []string{
		"127.0.0.0/8",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16",
		"::1/128",
		"fc00::/7",
		"fe80::/10",
	}
	if _, client, err := Build(config); err != nil {
		t.Fatal(err)
	} else {
		client.CloseIdleConnections()
	}

	config.BaseURL = "http://192.169.0.1"
	if _, _, err := Build(config); err == nil {
		t.Fatal("public address adjacent to allowed CIDR was accepted")
	}
}

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(
	ctx context.Context,
	network string,
	host string,
) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

type dialerFunc func(context.Context, string, string) (net.Conn, error)

func (f dialerFunc) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	return f(ctx, network, address)
}

func TestLocalhostDialRequiresEveryResolvedAddressToBeLoopback(t *testing.T) {
	resolver := resolverFunc(func(
		context.Context, string, string,
	) ([]netip.Addr, error) {
		return []netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("203.0.113.10"),
		}, nil
	})
	dials := 0
	dialer := dialerFunc(func(
		context.Context, string, string,
	) (net.Conn, error) {
		dials++
		return nil, errors.New("must not dial")
	})
	client := localhostClient(t, time.Second, resolver, dialer)
	transport := client.Transport.(*http.Transport)
	if _, err := transport.DialContext(
		context.Background(), "tcp", "localhost:11434",
	); err == nil {
		t.Fatal("localhost with a non-loopback answer was dialed")
	}
	if dials != 0 {
		t.Fatalf("unsafe resolution reached dialer %d times", dials)
	}
}

func TestLocalhostDialIsBoundedAndCanUseVerifiedFallback(t *testing.T) {
	resolver := resolverFunc(func(
		context.Context, string, string,
	) ([]netip.Addr, error) {
		return []netip.Addr{
			netip.MustParseAddr("127.0.0.2"),
			netip.MustParseAddr("::1"),
		}, nil
	})
	var attempts []string
	var peer net.Conn
	dialer := dialerFunc(func(
		_ context.Context, _ string, address string,
	) (net.Conn, error) {
		attempts = append(attempts, address)
		if len(attempts) == 1 {
			return nil, errors.New("first loopback unavailable")
		}
		connection, remote := net.Pipe()
		peer = remote
		return connection, nil
	})
	client := localhostClient(t, time.Second, resolver, dialer)
	transport := client.Transport.(*http.Transport)
	connection, err := transport.DialContext(
		context.Background(), "tcp", "localhost:11434",
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	_ = peer.Close()
	if len(attempts) != 2 || attempts[0] != "127.0.0.2:11434" ||
		attempts[1] != "[::1]:11434" {
		t.Fatalf("dial attempts=%v", attempts)
	}

	tooMany := make([]netip.Addr, 0, MaxResolvedIPsHard+1)
	for index := 1; index <= MaxResolvedIPsHard+1; index++ {
		tooMany = append(tooMany, netip.MustParseAddr(
			"127.0.0."+strconv.Itoa(index),
		))
	}
	client = localhostClient(t, time.Second, resolverFunc(func(
		context.Context, string, string,
	) ([]netip.Addr, error) {
		return tooMany, nil
	}), dialer)
	transport = client.Transport.(*http.Transport)
	if _, err := transport.DialContext(
		context.Background(), "tcp", "localhost:11434",
	); err == nil {
		t.Fatal("oversized DNS answer set was accepted")
	}
}

func TestLocalhostResolutionHonorsConnectTimeoutAndTarget(t *testing.T) {
	resolver := resolverFunc(func(
		ctx context.Context, _ string, _ string,
	) ([]netip.Addr, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	dialer := dialerFunc(func(
		context.Context, string, string,
	) (net.Conn, error) {
		return nil, errors.New("must not dial")
	})
	client := localhostClient(t, 20*time.Millisecond, resolver, dialer)
	transport := client.Transport.(*http.Transport)
	if _, err := transport.DialContext(
		context.Background(), "tcp", "localhost:11434",
	); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("resolution timeout error=%v", err)
	}
	if _, err := transport.DialContext(
		context.Background(), "tcp", "localhost:11435",
	); err == nil {
		t.Fatal("different runtime port was accepted")
	}
}

func TestBuildConnectsToVerifiedSystemLocalhost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, client, err := Build(testConfig("http://localhost:" + port))
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://localhost:" + port)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", response.StatusCode)
	}
}

func TestRuntimeDialerPinsConfiguredAuthority(t *testing.T) {
	resolver := resolverFunc(func(
		context.Context, string, string,
	) ([]netip.Addr, error) {
		return nil, errors.New("resolver must not be called")
	})
	delegated := errors.New("delegated")
	dials := 0
	dialer := dialerFunc(func(
		context.Context, string, string,
	) (net.Conn, error) {
		dials++
		return nil, delegated
	})
	_, client, err := build(
		testConfig("https://runtime.example"), resolver, dialer,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	transport := client.Transport.(*http.Transport)
	if _, err := transport.DialContext(
		context.Background(), "tcp", "other.example:443",
	); err == nil {
		t.Fatal("different runtime host was accepted")
	}
	if _, err := transport.DialContext(
		context.Background(), "tcp", "runtime.example:444",
	); err == nil {
		t.Fatal("different runtime port was accepted")
	}
	if dials != 0 {
		t.Fatalf("mismatched authority reached dialer %d times", dials)
	}
	if _, err := transport.DialContext(
		context.Background(), "tcp", "runtime.example:443",
	); !errors.Is(err, delegated) {
		t.Fatalf("configured authority error=%v", err)
	}
	if dials != 1 {
		t.Fatalf("configured authority dial count=%d", dials)
	}
}

func localhostClient(
	t *testing.T,
	timeout time.Duration,
	resolver ipResolver,
	dialer contextDialer,
) *http.Client {
	t.Helper()
	config := testConfig("http://localhost:11434")
	config.ConnectTimeout = timeout
	_, client, err := build(config, resolver, dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}
