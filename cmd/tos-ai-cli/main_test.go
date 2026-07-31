package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetchMetricsAcceptsOnlyBoundedSafePrometheusText(t *testing.T) {
	valid := "# TYPE tos_ai_worker_ready gauge\ntos_ai_worker_ready 1\n"
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantError   bool
	}{
		{
			name: "valid", status: http.StatusOK,
			contentType: "text/plain; version=0.0.4; charset=utf-8", body: valid,
		},
		{
			name: "wrong status", status: http.StatusServiceUnavailable,
			contentType: "text/plain; version=0.0.4", body: valid, wantError: true,
		},
		{
			name: "wrong content type", status: http.StatusOK,
			contentType: "application/octet-stream", body: valid, wantError: true,
		},
		{
			name: "empty", status: http.StatusOK,
			contentType: "text/plain; version=0.0.4", wantError: true,
		},
		{
			name: "missing final newline", status: http.StatusOK,
			contentType: "text/plain; version=0.0.4",
			body:        strings.TrimSuffix(valid, "\n"), wantError: true,
		},
		{
			name: "terminal escape", status: http.StatusOK,
			contentType: "text/plain; version=0.0.4",
			body:        "metric 1\x1b[31m\n", wantError: true,
		},
		{
			name: "oversized", status: http.StatusOK,
			contentType: "text/plain; version=0.0.4",
			body:        strings.Repeat("x", maxMetricsResponseBytes) + "\n", wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				if request.Method != http.MethodGet || request.URL.Path != "/metrics" ||
					request.URL.RawQuery != "" || request.Body == nil {
					http.Error(writer, "invalid request", http.StatusBadRequest)
					return
				}
				writer.Header().Set("Content-Type", test.contentType)
				writer.WriteHeader(test.status)
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			client := server.Client()
			client.Transport = rewriteMetricsTransport{
				base: client.Transport, target: server.URL,
			}
			encoded, err := fetchMetrics(context.Background(), client)
			if test.wantError {
				if err == nil {
					t.Fatalf("unsafe metrics response accepted: %q", encoded)
				}
				return
			}
			if err != nil || string(encoded) != valid {
				t.Fatalf("metrics=%q err=%v", encoded, err)
			}
		})
	}
	if _, err := fetchMetrics(context.Background(), nil); err == nil {
		t.Fatal("nil metrics client accepted")
	}
}

type rewriteMetricsTransport struct {
	base   http.RoundTripper
	target string
}

func (t rewriteMetricsTransport) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.URL.Scheme = "http"
	clone.URL.Host = strings.TrimPrefix(t.target, "http://")
	return t.base.RoundTrip(clone)
}

func TestSafeMetricsText(t *testing.T) {
	if !safeMetricsText([]byte("metric{code=\"ok\"} 1\n")) {
		t.Fatal("safe metrics text rejected")
	}
	for _, value := range [][]byte{
		{0}, {'\r'}, {'\t'}, {0x1b}, {0x7f}, {0xc3, 0xa9},
	} {
		if safeMetricsText(value) {
			t.Fatalf("unsafe metrics bytes accepted: %v", value)
		}
	}
}
