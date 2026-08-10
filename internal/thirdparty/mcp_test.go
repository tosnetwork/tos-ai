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

// TestService_MCPGoldenPath proves the allowlisted MCP dispatch path
// against a real in-process JSON-RPC server: Invoke calls tools/call and
// decodes structuredContent.
func TestService_MCPGoldenPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"isError":false,"structuredContent":{"status":"completed","output":{"ok":true}}}}`))
	}))
	defer srv.Close()

	endpointRef := srv.URL + "#analyze"
	svc, err := NewService(testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "mcp", EndpointRef: endpointRef, CapabilityID: "cap_mcp_1",
		Timeout: 5 * time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
	}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	binding := &edgev1.ThirdPartyBindingRef{
		Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_MCP,
		EndpointRef: endpointRef, CapabilityId: "cap_mcp_1",
	}
	invoke, err := svc.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-mcp-1", JobId: "job-mcp-1", Binding: binding, Input: []byte(`{"x":1}`),
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

// TestService_MCPRejectsOversizedResponse proves the operator-configured
// MaxResponseBytes is actually enforced for MCP -- a provider that returns
// more than the configured limit must be rejected, not silently decoded
// into an unbounded read.
func TestService_MCPRejectsOversizedResponse(t *testing.T) {
	oversized := `{"jsonrpc":"2.0","id":1,"result":{"isError":false,"structuredContent":{"status":"completed","output":{"blob":"` +
		strings.Repeat("a", 4096) + `"}}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(oversized))
	}))
	defer srv.Close()

	endpointRef := srv.URL + "#analyze"
	svc, err := NewService(testBindings(t, operatorconfig.ThirdPartyBinding{
		Transport: "mcp", EndpointRef: endpointRef, CapabilityID: "cap_mcp_2",
		Timeout: 5 * time.Second, MaxRequestBytes: 1 << 20, MaxResponseBytes: 512,
	}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	binding := &edgev1.ThirdPartyBindingRef{
		Transport:   edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_MCP,
		EndpointRef: endpointRef, CapabilityId: "cap_mcp_2",
	}
	_, err = svc.Invoke(context.Background(), connect.NewRequest(&edgev1.ThirdPartyInvokeRequest{
		RequestId: "req-mcp-2", JobId: "job-mcp-2", Binding: binding, Input: []byte(`{}`),
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
	}))
	if err == nil {
		t.Fatal("expected Invoke to reject a response exceeding the configured MaxResponseBytes")
	}
}
