package metricsexport

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExporterCollectorMOCKFleetAggregationAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	collector, err := NewCollector(CollectorConfig{Credentials: map[string]string{
		"terminal-a": "terminal-a-secret-token", "terminal-b": "terminal-b-secret-token",
	}, TTL: time.Minute, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(collector.Handler())
	defer server.Close()
	for alias, token := range map[string]string{"terminal-a": "terminal-a-secret-token", "terminal-b": "terminal-b-secret-token"} {
		exporter, err := New(server.URL+CollectorPath, token, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		if err := exporter.Export(context.Background(), []byte("tos_ai_worker_ready 1\n")); err != nil {
			t.Fatalf("%s: %v", alias, err)
		}
	}
	snapshots, err := collector.Latest(now)
	if err != nil || len(snapshots) != 2 || snapshots[0].Alias != "terminal-a" || snapshots[1].Alias != "terminal-b" {
		t.Fatalf("snapshots=%#v err=%v", snapshots, err)
	}
	snapshots[0].Metrics[0] = 'X'
	again, _ := collector.Latest(now)
	if again[0].Metrics[0] == 'X' {
		t.Fatal("collector leaked mutable storage")
	}
	expired, err := collector.Latest(now.Add(time.Minute))
	if err != nil || len(expired) != 0 {
		t.Fatalf("expired=%#v err=%v", expired, err)
	}
}

func TestCollectorRejectsSharedOrMalformedCredentials(t *testing.T) {
	if _, err := NewCollector(CollectorConfig{Credentials: map[string]string{
		"terminal-a": "same-secret-token", "terminal-b": "same-secret-token",
	}, TTL: time.Minute, Now: time.Now}); err == nil {
		t.Fatal("shared credential accepted")
	}
	for _, concurrent := range []int{-1, MaxCollectorConcurrent + 1} {
		if _, err := NewCollector(CollectorConfig{
			Credentials: map[string]string{"terminal-a": "terminal-a-secret-token"},
			TTL:         time.Minute, Now: time.Now, MaxConcurrent: concurrent,
		}); err == nil {
			t.Fatalf("invalid concurrency %d accepted", concurrent)
		}
	}
}

func TestCollectorCapacityFailsBeforeReadingAnotherBody(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	collector, err := NewCollector(CollectorConfig{
		Credentials: map[string]string{"terminal-a": "terminal-a-secret-token"},
		TTL:         time.Minute, MaxConcurrent: 1,
		Now: func() time.Time {
			entered <- struct{}{}
			<-release
			return time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := func() *http.Request {
		result := httptest.NewRequest(http.MethodPost, CollectorPath, bytes.NewBufferString("tos_ai_worker_ready 1\n"))
		result.Header.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		result.Header.Set("X-TOS-Metrics-Version", "1")
		result.Header.Set("Authorization", "Bearer terminal-a-secret-token")
		return result
	}
	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		collector.Handler().ServeHTTP(first, request())
		close(done)
	}()
	<-entered
	second := httptest.NewRecorder()
	collector.Handler().ServeHTTP(second, request())
	if second.Code != http.StatusServiceUnavailable || second.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d headers=%v", second.Code, second.Header())
	}
	close(release)
	<-done
	if first.Code != http.StatusNoContent {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
}
