package httpclient

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	MaxConnectionsHard = 256
	MaxHeaderBytesHard = 64 << 10
	MaxEndpointBytes   = 2048
	MaxPlaintextCIDRs  = 16
)

type Config struct {
	BaseURL                string
	Timeout                time.Duration
	ConnectTimeout         time.Duration
	MaxConnections         int
	MaxResponseHeaderBytes int64
	AllowedPlaintextCIDRs  []string
}

func Build(config Config) (*url.URL, *http.Client, error) {
	if len(config.BaseURL) == 0 || len(config.BaseURL) > MaxEndpointBytes ||
		len(config.AllowedPlaintextCIDRs) > MaxPlaintextCIDRs {
		return nil, nil, errors.New("runtime endpoint configuration exceeds hard limits")
	}
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "https" && baseURL.Scheme != "http") ||
		baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, nil, errors.New("runtime endpoint must be an absolute HTTP(S) URL without credentials or query")
	}
	if config.Timeout <= 0 || config.Timeout > time.Hour ||
		config.ConnectTimeout <= 0 || config.ConnectTimeout > time.Minute ||
		config.MaxConnections <= 0 || config.MaxConnections > MaxConnectionsHard ||
		config.MaxResponseHeaderBytes <= 0 || config.MaxResponseHeaderBytes > MaxHeaderBytesHard {
		return nil, nil, errors.New("invalid runtime HTTP bounds")
	}
	if baseURL.Scheme == "http" {
		if err := validatePlaintextHost(baseURL.Hostname(), config.AllowedPlaintextCIDRs); err != nil {
			return nil, nil, err
		}
	}
	dialer := &net.Dialer{Timeout: config.ConnectTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           config.MaxConnections,
		MaxIdleConnsPerHost:    config.MaxConnections,
		MaxConnsPerHost:        config.MaxConnections,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    config.ConnectTimeout,
		ResponseHeaderTimeout:  config.Timeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
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

func Endpoint(base *url.URL, suffix string) string {
	value := *base
	value.Path = strings.TrimRight(value.Path, "/") + suffix
	return value.String()
}

func validatePlaintextHost(host string, allowed []string) error {
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return errors.New("plaintext runtime endpoint requires localhost or an allowed literal local address")
	}
	if ip.IsLoopback() {
		return nil
	}
	for _, value := range allowed {
		if len(value) == 0 || len(value) > 64 {
			return errors.New("invalid plaintext runtime CIDR")
		}
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			return errors.New("invalid plaintext runtime CIDR")
		}
		base := network.IP
		if !(base.IsPrivate() || base.IsLoopback() || base.IsLinkLocalUnicast()) {
			return errors.New("plaintext runtime CIDR must be local")
		}
		if network.Contains(ip) {
			return nil
		}
	}
	return errors.New("plaintext runtime endpoint is not explicitly allowed")
}
