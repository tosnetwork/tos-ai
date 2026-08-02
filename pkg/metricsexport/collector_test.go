package metricsexport

import (
	"context"
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
}
