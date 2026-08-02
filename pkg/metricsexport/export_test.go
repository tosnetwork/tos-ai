package metricsexport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExporterUsesFixedAuthenticatedTLSDestination(t *testing.T) {
	snapshot := []byte("tos_ai_rpc_requests_total{method=\"invoke\",outcome=\"ok\"} 7\n")
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/ingest" ||
			request.Header.Get("Authorization") != "Bearer operator-secret" ||
			request.Header.Get("X-TOS-Metrics-Version") != "1" {
			t.Fatalf("unexpected request: %s %s %v", request.Method, request.URL, request.Header)
		}
		data, _ := io.ReadAll(request.Body)
		if string(data) != string(snapshot) {
			t.Fatalf("snapshot=%q", data)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := New(server.URL+"/ingest", "operator-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
}

func TestExporterRejectsUnsafeOrUnboundedInputs(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(strings.Repeat("x", MaxResponseBytes+1)))
	}))
	defer server.Close()
	for _, endpoint := range []string{"http://example.test/ingest", server.URL + "/ingest?destination=other", "https://user@example.test/"} {
		if _, err := New(endpoint, "token", server.Client()); err == nil {
			t.Fatalf("unsafe endpoint accepted: %q", endpoint)
		}
	}
	exporter, err := New(server.URL+"/ingest", "token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range [][]byte{
		nil,
		[]byte(strings.Repeat("x", MaxSnapshotBytes+1)),
		[]byte("gpu_uuid{value=\"secret\"} 1\n"),
		[]byte("metric 1\x00"),
	} {
		if err := exporter.Export(context.Background(), snapshot); err == nil {
			t.Fatalf("unsafe snapshot accepted: %q", snapshot)
		}
	}
	if err := exporter.Export(context.Background(), []byte("metric 1\n")); err == nil {
		t.Fatal("oversized collector response accepted")
	}
}

func TestExporterDoesNotFollowRedirects(t *testing.T) {
	destinationReached := false
	destination := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		destinationReached = true
	}))
	defer destination.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	exporter, _ := New(redirect.URL, "operator-secret-token", redirect.Client())
	if err := exporter.Export(context.Background(), []byte("metric 1\n")); err == nil || destinationReached {
		t.Fatal("metrics exporter followed a redirect")
	}
}
