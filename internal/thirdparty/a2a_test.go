package thirdparty

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// TestService_A2AGoldenPath proves the allowlisted A2A dispatch path
// against a real in-process JSON-RPC server: Invoke calls message/send and
// decodes the returned Task.
func TestService_A2AGoldenPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"id":"task-1","status":{"state":"completed"},"artifacts":[{"name":"result","content":{"ok":true}}]}}`))
	}))
	defer srv.Close()

	svc, err := NewService(testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "a2a", EndpointRef: srv.URL, CapabilityID: "cap_a2a_1",
		Timeout: 5 * time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
	}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	binding := &edgev1.ThirdPartyBindingRef{
		Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_A2A,
		EndpointRef: srv.URL, CapabilityId: "cap_a2a_1",
	}
	invoke, err := svc.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-a2a-1", JobId: "job-a2a-1", Binding: binding, Input: []byte(`{"x":1}`),
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
	}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if invoke.Msg.Status != edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_COMPLETED {
		t.Fatalf("status = %s, want completed", invoke.Msg.Status)
	}
	if string(invoke.Msg.Output) != `{"ok":true}` {
		t.Fatalf("output = %s, want the provider's result artifact", invoke.Msg.Output)
	}
}

// TestService_A2ARejectsOversizedResponse proves the operator-configured
// MaxResponseBytes is actually enforced for A2A.
func TestService_A2ARejectsOversizedResponse(t *testing.T) {
	oversized := `{"jsonrpc":"2.0","id":1,"result":{"id":"task-1","status":{"state":"completed"},"artifacts":[{"name":"result","content":{"blob":"` +
		strings.Repeat("a", 4096) + `"}}]}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(oversized))
	}))
	defer srv.Close()

	svc, err := NewService(testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "a2a", EndpointRef: srv.URL, CapabilityID: "cap_a2a_2",
		Timeout: 5 * time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 512,
	}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	binding := &edgev1.ThirdPartyBindingRef{
		Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_A2A,
		EndpointRef: srv.URL, CapabilityId: "cap_a2a_2",
	}
	_, err = svc.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-a2a-2", JobId: "job-a2a-2", Binding: binding, Input: []byte(`{}`),
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
	}))
	if err == nil {
		t.Fatal("expected Invoke to reject a response exceeding the configured MaxResponseBytes")
	}
}
