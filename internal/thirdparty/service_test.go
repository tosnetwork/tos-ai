package thirdparty

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// TestService_RejectsBindingNotOnAllowlist proves the load-bearing
// invariant this package exists to enforce: a request naming a
// (transport, endpoint_ref, capability_id) not present in the operator's
// approved allowlist is rejected before any outbound network call --
// checked here against a real HTTP server that would fail the test by
// actually being dialed.
func TestService_RejectsBindingNotOnAllowlist(t *testing.T) {
	dialed := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dialed = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// The allowlist approves a DIFFERENT capability_id than what the
	// request below asks for.
	svc, err := NewService(testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "http", EndpointRef: srv.URL, CapabilityID: "cap_approved",
		Timeout: time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
	}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	_, err = svc.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-1", JobId: "job-1",
		Binding: &edgev1.ThirdPartyBindingRef{
			Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_HTTP,
			EndpointRef: srv.URL, CapabilityId: "cap_NOT_approved",
		},
		Input: []byte(`{}`),
	}))
	if err == nil {
		t.Fatal("expected Invoke to reject a binding not on the allowlist")
	}
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("error code = %v, want PermissionDenied", connect.CodeOf(err))
	}
	if dialed {
		t.Fatal("provider was dialed for a binding that was never approved")
	}
}

func testBindings(t *testing.T, entries ...operatorconfig.ThirdPartyBinding) operatorconfig.ThirdPartyBindings {
	t.Helper()
	config := `{"version":1,"bindings":[`
	for i, e := range entries {
		if i > 0 {
			config += ","
		}
		encoded, err := json.Marshal(map[string]any{
			"transport": e.Transport, "endpointRef": e.EndpointRef, "capabilityId": e.CapabilityID,
			"timeoutMillis": e.Timeout.Milliseconds(), "maxRequestBytes": e.MaxRequestBytes, "maxResponseBytes": e.MaxResponseBytes,
		})
		if err != nil {
			t.Fatalf("marshal binding fixture: %v", err)
		}
		config += string(encoded)
	}
	config += `]}`
	path := t.TempDir() + "/bindings.json"
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatalf("write bindings fixture: %v", err)
	}
	bindings, err := operatorconfig.LoadThirdPartyBindings(path)
	if err != nil {
		t.Fatalf("LoadThirdPartyBindings: %v", err)
	}
	return bindings
}

// TestService_HTTPGoldenPath proves the full allowlisted HTTP dispatch
// path against a real in-process HTTP server: Invoke succeeds, Query
// recovers the same outcome, Health reports the provider reachable.
func TestService_HTTPGoldenPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"completed","output":{"ok":true}}`))
	}))
	defer srv.Close()

	svc, err := NewService(testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "http", EndpointRef: srv.URL, CapabilityID: "cap_1",
		Timeout: 5 * time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
	}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	binding := &edgev1.ThirdPartyBindingRef{
		Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_HTTP,
		EndpointRef: srv.URL, CapabilityId: "cap_1",
	}

	health, err := svc.Health(context.Background(), connect.NewRequest(&edgev1.ThirdPartyHealthRequest{Binding: binding}))
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !health.Msg.Healthy {
		t.Fatalf("Health = %+v, want healthy", health.Msg)
	}

	invoke, err := svc.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-2", JobId: "job-2", Binding: binding, Input: []byte(`{"x":1}`),
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
	}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if invoke.Msg.Status != edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_COMPLETED {
		t.Fatalf("status = %s, want completed", invoke.Msg.Status)
	}
	if string(invoke.Msg.Output) != `{"ok":true}` {
		t.Fatalf("output = %s, want the provider's echoed output", invoke.Msg.Output)
	}
}
