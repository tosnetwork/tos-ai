package thirdparty

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// TestCompletionStore_StabilizeSurvivesReopen proves the durability
// property an in-memory-only cache cannot give: a completion time
// recorded before the store is closed (standing in for a tos-ai worker
// restart) is still the value a later stabilize call for the same
// request_id returns after the store is reopened at the same path.
func TestCompletionStore_StabilizeSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "completions.db")
	future := time.Now().Add(time.Hour).UnixMilli()

	store, err := openCompletionStore(path)
	if err != nil {
		t.Fatalf("openCompletionStore: %v", err)
	}
	first, err := store.stabilize("req-1", 1000, future)
	if err != nil {
		t.Fatalf("stabilize: %v", err)
	}
	if first != 1000 {
		t.Fatalf("first stabilize = %d, want 1000", first)
	}
	if err := store.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := openCompletionStore(path)
	if err != nil {
		t.Fatalf("reopen openCompletionStore: %v", err)
	}
	defer reopened.close()

	second, err := reopened.stabilize("req-1", 9999, future)
	if err != nil {
		t.Fatalf("stabilize after reopen: %v", err)
	}
	if second != 1000 {
		t.Fatalf("stabilize after reopen = %d, want 1000 (the value recorded before close, not the freshly observed 9999)", second)
	}
}

// TestCompletionStore_PruneEvictsExpiredRecords proves the store is
// bounded by retain_until rather than growing without limit for the
// lifetime of a long-running worker process.
func TestCompletionStore_PruneEvictsExpiredRecords(t *testing.T) {
	store, err := openCompletionStore(filepath.Join(t.TempDir(), "completions.db"))
	if err != nil {
		t.Fatalf("openCompletionStore: %v", err)
	}
	defer store.close()

	now := time.Now()
	past := now.Add(-time.Minute).UnixMilli()
	if _, err := store.stabilize("req-expired", 1000, past); err != nil {
		t.Fatalf("stabilize: %v", err)
	}
	if err := store.prune(now); err != nil {
		t.Fatalf("prune: %v", err)
	}

	// The expired record must be gone -- a fresh stabilize call for the
	// same request_id records (and returns) the newly observed value
	// instead of the pruned one.
	after, err := store.stabilize("req-expired", 2000, now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatalf("stabilize after prune: %v", err)
	}
	if after != 2000 {
		t.Fatalf("stabilize after prune = %d, want 2000 (the expired record must have been evicted, not retained forever)", after)
	}
}

// TestCompletionStore_PruneKeepsUnexpiredRecords proves prune does not
// evict a record whose retention window has not yet passed.
func TestCompletionStore_PruneKeepsUnexpiredRecords(t *testing.T) {
	store, err := openCompletionStore(filepath.Join(t.TempDir(), "completions.db"))
	if err != nil {
		t.Fatalf("openCompletionStore: %v", err)
	}
	defer store.close()

	now := time.Now()
	future := now.Add(time.Hour).UnixMilli()
	if _, err := store.stabilize("req-live", 1000, future); err != nil {
		t.Fatalf("stabilize: %v", err)
	}
	if err := store.prune(now); err != nil {
		t.Fatalf("prune: %v", err)
	}

	after, err := store.stabilize("req-live", 9999, future)
	if err != nil {
		t.Fatalf("stabilize after prune: %v", err)
	}
	if after != 1000 {
		t.Fatalf("stabilize after prune = %d, want 1000 (an unexpired record must not be evicted)", after)
	}
}

// TestService_CompletedUnixMillisSurvivesRestart proves the fix for the
// gap an in-memory-only map left: a Query recovering a request_id whose
// terminal outcome an earlier Service instance's Invoke already
// stabilized, issued against a SECOND, freshly-constructed Service
// pointed at the same completion-store path (standing in for a tos-ai
// worker process restart between Invoke and the recovering Query), still
// returns the value the first Invoke recorded -- not a new value freshly
// computed from time.Now().UnixMilli() at query time, which is what an
// unbounded in-memory map (reset to empty on every process start) would
// have produced instead.
func TestService_CompletedUnixMillisSurvivesRestart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"completed","output":{"ok":true}}`))
	}))
	defer srv.Close()

	completionStorePath := filepath.Join(t.TempDir(), "completions.db")
	bindings := testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "http", EndpointRef: srv.URL, CapabilityID: "cap_restart_1",
		Timeout: 5 * time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
	})
	binding := &edgev1.ThirdPartyBindingRef{
		Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_HTTP,
		EndpointRef: srv.URL, CapabilityId: "cap_restart_1",
	}

	first, err := NewService(bindings, completionStorePath)
	if err != nil {
		t.Fatalf("NewService (first instance): %v", err)
	}
	invoke, err := first.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-restart-1", JobId: "job-restart-1", Binding: binding, Input: []byte(`{}`),
		DeadlineUnixMillis:    time.Now().Add(time.Minute).UnixMilli(),
		RetainUntilUnixMillis: time.Now().Add(time.Hour).UnixMilli(),
	}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if invoke.Msg.CompletedUnixMillis == 0 {
		t.Fatal("expected a non-zero completed_unix_millis from Invoke")
	}
	// Close (not Cleanup-deferred) simulates the worker process actually
	// exiting -- a real restart, not just a second live instance sharing
	// process memory.
	first.Close()

	time.Sleep(5 * time.Millisecond) // force wall-clock drift if it isn't actually durable

	second, err := NewService(bindings, completionStorePath)
	if err != nil {
		t.Fatalf("NewService (second instance, standing in for a worker restart): %v", err)
	}
	defer second.Close()

	query, err := second.Query(context.Background(), connect.NewRequest(&edgev1.ThirdPartyQueryRequest{
		RequestId: "req-restart-1", Binding: binding,
	}))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !query.Msg.Found {
		t.Fatal("expected Query to find the request the first Service instance completed")
	}
	if query.Msg.Result.CompletedUnixMillis != invoke.Msg.CompletedUnixMillis {
		t.Fatalf("completed_unix_millis did not survive a simulated worker restart: Invoke=%d Query(after restart)=%d, want identical",
			invoke.Msg.CompletedUnixMillis, query.Msg.Result.CompletedUnixMillis)
	}
}
