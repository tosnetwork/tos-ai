// Package metricsexport provides an explicitly configured, bounded remote
// export path for the worker's privacy-minimized Prometheus snapshot. It does
// not scrape the worker, schedule background work, or discover destinations.
package metricsexport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	MaxSnapshotBytes = 16 << 10
	MaxResponseBytes = 4 << 10
	MaxTokenBytes    = 8 << 10
)

type Exporter struct {
	endpoint string
	token    string
	client   *http.Client
}

func New(endpoint, bearerToken string, client *http.Client) (*Exporter, error) {
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		len(endpoint) > 2048 || len(bearerToken) == 0 || len(bearerToken) > MaxTokenBytes ||
		strings.IndexFunc(bearerToken, func(r rune) bool { return r <= ' ' || r == 0x7f }) >= 0 ||
		client == nil {
		return nil, errors.New("invalid metrics export configuration")
	}
	fixedClient := *client
	fixedClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Exporter{endpoint: parsed.String(), token: bearerToken, client: &fixedClient}, nil
}

// Export sends exactly one caller-supplied snapshot. The caller owns retry
// and cadence policy, preventing this package from creating hidden queues or
// goroutines.
func (e *Exporter) Export(ctx context.Context, snapshot []byte) error {
	if e == nil || ctx == nil || len(snapshot) == 0 || len(snapshot) > MaxSnapshotBytes ||
		!safeSnapshot(snapshot) {
		return errors.New("metrics snapshot rejected")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(snapshot))
	if err != nil {
		return errors.New("create metrics export request")
	}
	request.Header.Set("Authorization", "Bearer "+e.token)
	request.Header.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	request.Header.Set("X-TOS-Metrics-Version", "1")
	response, err := e.client.Do(request)
	if err != nil {
		return errors.New("metrics export transport failed")
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if readErr != nil || len(data) > MaxResponseBytes || response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("metrics export rejected")
	}
	return nil
}

func safeSnapshot(snapshot []byte) bool {
	lower := strings.ToLower(string(snapshot))
	for _, forbidden := range []string{"hostname", "gpu_uuid", "gpu_serial", "device_uuid", "device_serial"} {
		if strings.Contains(lower, forbidden) {
			return false
		}
	}
	for _, value := range snapshot {
		if value == '\n' || value == '\r' || value == '\t' {
			continue
		}
		if value < 0x20 || value > 0x7e {
			return false
		}
	}
	return true
}
