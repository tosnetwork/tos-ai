// Package runtimehttp constructs bounded HTTP clients for administrator-owned
// model runtimes. It never accepts task-selected endpoints.
package runtimehttp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
)

const (
	MaxConnectionsHard    = 256
	MaxHeaderBytesHard    = 64 << 10
	MaxEndpointBytes      = 2048
	MaxPlaintextCIDRs     = 16
	MaxResolvedIPsHard    = 16
	MaxTLSRootsHard       = 64
	MaxTLSChainHard       = 8
	MaxTLSMaterialBytes   = 1 << 20
	MaxTLSServerNameBytes = 253
)

var localPlaintextNetworks = [...]netip.Prefix{
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
}

type Config struct {
	BaseURL                string
	Timeout                time.Duration
	ConnectTimeout         time.Duration
	MaxConnections         int
	MaxResponseHeaderBytes int64
	AllowedPlaintextCIDRs  []string
	RootCAs                *x509.CertPool
	ClientCertificate      *tls.Certificate
	TLSServerName          string
}

func Build(config Config) (*url.URL, *http.Client, error) {
	dialer := &net.Dialer{
		Timeout: config.ConnectTimeout, KeepAlive: 30 * time.Second,
	}
	return build(config, net.DefaultResolver, dialer)
}

type ipResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type contextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

func build(
	config Config,
	resolver ipResolver,
	dialer contextDialer,
) (*url.URL, *http.Client, error) {
	if len(config.BaseURL) == 0 || len(config.BaseURL) > MaxEndpointBytes ||
		len(config.AllowedPlaintextCIDRs) > MaxPlaintextCIDRs ||
		nilcheck.IsNil(resolver) || nilcheck.IsNil(dialer) {
		return nil, nil, errors.New("runtime endpoint configuration exceeds hard limits")
	}
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || baseURL.Host == "" || baseURL.Hostname() == "" ||
		(baseURL.Scheme != "https" && baseURL.Scheme != "http") ||
		baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" ||
		!validEndpointPort(baseURL) {
		return nil, nil, errors.New(
			"runtime endpoint must be an absolute HTTP(S) URL without credentials or query",
		)
	}
	if config.Timeout <= 0 || config.Timeout > time.Hour ||
		config.ConnectTimeout <= 0 || config.ConnectTimeout > time.Minute ||
		config.MaxConnections <= 0 ||
		config.MaxConnections > MaxConnectionsHard ||
		config.MaxResponseHeaderBytes <= 0 ||
		config.MaxResponseHeaderBytes > MaxHeaderBytesHard {
		return nil, nil, errors.New("invalid runtime HTTP bounds")
	}
	plaintextNetworks, err := parsePlaintextNetworks(
		config.AllowedPlaintextCIDRs,
	)
	if err != nil {
		return nil, nil, err
	}
	tlsConfig, err := boundedTLSConfig(config, baseURL.Scheme)
	if err != nil {
		return nil, nil, err
	}
	port := baseURL.Port()
	if port == "" {
		port = "443"
		if baseURL.Scheme == "http" {
			port = "80"
		}
	}
	dialContext := (&authorityDialer{
		dialer: dialer, host: baseURL.Hostname(), port: port,
	}).DialContext
	if baseURL.Scheme == "http" {
		if err := validatePlaintextHost(baseURL.Hostname(), plaintextNetworks); err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(baseURL.Hostname(), "localhost") {
			dialContext = (&loopbackDialer{
				resolver: resolver,
				dialer:   dialer,
				host:     baseURL.Hostname(),
				port:     port,
				timeout:  config.ConnectTimeout,
			}).DialContext
		}
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           config.MaxConnections,
		MaxIdleConnsPerHost:    config.MaxConnections,
		MaxConnsPerHost:        config.MaxConnections,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    config.ConnectTimeout,
		ResponseHeaderTimeout:  config.Timeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
		TLSClientConfig:        tlsConfig,
	}
	client := &http.Client{
		Timeout:   config.Timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("runtime redirects are disabled")
		},
	}
	return baseURL, client, nil
}

func boundedTLSConfig(config Config, scheme string) (*tls.Config, error) {
	hasTLSAuthority := config.RootCAs != nil ||
		config.ClientCertificate != nil || config.TLSServerName != ""
	if scheme != "https" {
		if hasTLSAuthority {
			return nil, errors.New("TLS identity requires an HTTPS runtime endpoint")
		}
		return &tls.Config{MinVersion: tls.VersionTLS12}, nil
	}
	result := &tls.Config{ // #nosec G402 -- TLS 1.2 is the explicit floor.
		MinVersion: tls.VersionTLS12,
		ServerName: config.TLSServerName,
	}
	if config.TLSServerName != "" &&
		!validTLSServerName(config.TLSServerName) {
		return nil, errors.New("invalid runtime TLS server name")
	}
	if config.RootCAs != nil {
		subjects := config.RootCAs.Subjects()
		total := 0
		if len(subjects) == 0 || len(subjects) > MaxTLSRootsHard {
			return nil, errors.New("runtime TLS roots exceed hard limits")
		}
		for _, subject := range subjects {
			if len(subject) == 0 || total > MaxTLSMaterialBytes-len(subject) {
				return nil, errors.New("runtime TLS roots exceed hard limits")
			}
			total += len(subject)
		}
		result.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCertificate != nil {
		certificate, err := cloneClientCertificate(*config.ClientCertificate)
		if err != nil {
			return nil, err
		}
		result.Certificates = []tls.Certificate{certificate}
	}
	return result, nil
}

func cloneClientCertificate(source tls.Certificate) (tls.Certificate, error) {
	if len(source.Certificate) == 0 ||
		len(source.Certificate) > MaxTLSChainHard || nilcheck.IsNil(source.PrivateKey) ||
		len(source.OCSPStaple) != 0 ||
		len(source.SignedCertificateTimestamps) != 0 ||
		len(source.SupportedSignatureAlgorithms) > MaxTLSChainHard*4 {
		return tls.Certificate{}, errors.New(
			"runtime TLS client certificate exceeds hard limits",
		)
	}
	result := source
	result.Certificate = make([][]byte, len(source.Certificate))
	total := 0
	for index, certificate := range source.Certificate {
		if len(certificate) == 0 ||
			total > MaxTLSMaterialBytes-len(certificate) {
			return tls.Certificate{}, errors.New(
				"runtime TLS client certificate exceeds hard limits",
			)
		}
		total += len(certificate)
		result.Certificate[index] = append([]byte(nil), certificate...)
	}
	result.OCSPStaple = append([]byte(nil), source.OCSPStaple...)
	result.SignedCertificateTimestamps = cloneByteSlices(
		source.SignedCertificateTimestamps,
	)
	result.SupportedSignatureAlgorithms = append(
		[]tls.SignatureScheme(nil), source.SupportedSignatureAlgorithms...,
	)
	// The leaf is derived from the copied DER when needed. Do not retain a
	// caller-owned mutable parsed certificate pointer.
	result.Leaf = nil
	return result, nil
}

func cloneByteSlices(source [][]byte) [][]byte {
	if source == nil {
		return nil
	}
	result := make([][]byte, len(source))
	for index := range source {
		result[index] = append([]byte(nil), source[index]...)
	}
	return result
}

func validTLSServerName(value string) bool {
	if len(value) == 0 || len(value) > MaxTLSServerNameBytes ||
		strings.HasSuffix(value, ".") || net.ParseIP(value) != nil {
		return false
	}
	labels := strings.Split(value, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' ||
			label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

type authorityDialer struct {
	dialer contextDialer
	host   string
	port   string
}

func (d *authorityDialer) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	if d == nil || d.dialer == nil || d.host == "" || d.port == "" {
		return nil, errors.New("invalid runtime authority dialer")
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errors.New("invalid runtime network")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || !sameEndpointHost(host, d.host) || port != d.port {
		return nil, errors.New("runtime dial target does not match configured endpoint")
	}
	return d.dialer.DialContext(ctx, network, address)
}

func sameEndpointHost(candidate string, configured string) bool {
	candidateIP, candidateErr := netip.ParseAddr(candidate)
	configuredIP, configuredErr := netip.ParseAddr(configured)
	if candidateErr == nil || configuredErr == nil {
		return candidateErr == nil && configuredErr == nil &&
			candidateIP.Unmap() == configuredIP.Unmap()
	}
	return strings.EqualFold(candidate, configured)
}

func validEndpointPort(endpoint *url.URL) bool {
	port := endpoint.Port()
	if port == "" {
		return !strings.HasSuffix(endpoint.Host, ":")
	}
	value, err := strconv.ParseUint(port, 10, 16)
	return err == nil && value > 0
}

func Endpoint(base *url.URL, suffix string) string {
	value := *base
	value.Path = strings.TrimRight(value.Path, "/") + suffix
	return value.String()
}

func parsePlaintextNetworks(values []string) ([]netip.Prefix, error) {
	result := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		if len(value) == 0 || len(value) > 64 {
			return nil, errors.New("invalid plaintext runtime CIDR")
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix.Addr().Is4In6() ||
			!isLocalPlaintextPrefix(prefix.Masked()) {
			return nil, errors.New("plaintext runtime CIDR must be entirely local")
		}
		result = append(result, prefix.Masked())
	}
	return result, nil
}

func isLocalPlaintextPrefix(prefix netip.Prefix) bool {
	for _, local := range localPlaintextNetworks {
		if prefix.Addr().BitLen() == local.Addr().BitLen() &&
			prefix.Bits() >= local.Bits() && local.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}

func validatePlaintextHost(host string, allowed []netip.Prefix) error {
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return errors.New(
			"plaintext runtime endpoint requires localhost or an allowed literal local address",
		)
	}
	ip = ip.Unmap()
	if ip.IsLoopback() {
		return nil
	}
	for _, network := range allowed {
		if network.Contains(ip) {
			return nil
		}
	}
	return errors.New("plaintext runtime endpoint is not explicitly allowed")
}

type loopbackDialer struct {
	resolver ipResolver
	dialer   contextDialer
	host     string
	port     string
	timeout  time.Duration
}

func (d *loopbackDialer) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	if d == nil || d.resolver == nil || d.dialer == nil ||
		d.host == "" || d.port == "" || d.timeout <= 0 {
		return nil, errors.New("invalid loopback runtime dialer")
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errors.New("invalid loopback runtime network")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || !strings.EqualFold(host, d.host) || port != d.port {
		return nil, errors.New("runtime dial target does not match configured endpoint")
	}
	dialContext, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	addresses, err := d.resolver.LookupNetIP(dialContext, "ip", d.host)
	if err != nil {
		if dialContext.Err() != nil {
			return nil, dialContext.Err()
		}
		return nil, errors.New("resolve loopback runtime")
	}
	if len(addresses) == 0 || len(addresses) > MaxResolvedIPsHard {
		return nil, errors.New("loopback runtime resolution exceeds hard limits")
	}
	for _, resolved := range addresses {
		if !resolved.IsValid() || !resolved.Unmap().IsLoopback() {
			return nil, errors.New("localhost resolved outside loopback")
		}
	}
	var attempted bool
	for _, resolved := range addresses {
		resolved = resolved.Unmap()
		if network == "tcp4" && !resolved.Is4() ||
			network == "tcp6" && !resolved.Is6() {
			continue
		}
		attempted = true
		connection, dialErr := d.dialer.DialContext(
			dialContext, network, net.JoinHostPort(resolved.String(), d.port),
		)
		if dialErr == nil {
			return connection, nil
		}
		if dialContext.Err() != nil {
			return nil, dialContext.Err()
		}
	}
	if !attempted {
		return nil, errors.New("localhost has no address for runtime network")
	}
	return nil, errors.New("connect loopback runtime")
}
