package thirdparty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// Wire contract with the third-party MCP provider (matches ATOS's own
// interim mcpadapter package): endpoint_ref is "<mcp-server-url>#<tool-
// name>"; invocation calls tools/call with arguments {job_id,
// capability_id, capability_version, idempotency_key, input, deadline},
// mirroring the HTTP wire body shape. A successful (isError=false) result's
// structuredContent is decoded the same way the HTTP wire response body is.
// MCP has no protocol-level "look up a prior call's result by id"
// operation, so Query always reports found=false.

const (
	defaultMCPConnectTimeout     = 5 * time.Second
	defaultMCPMaxConnections     = 8
	defaultMCPMaxResponseHeaders = int64(16 << 10)
	jsonRPCVersion               = "2.0"
	mcpCodeMethodNotFound        = -32601
)

type mcpRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type mcpRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *mcpRPCError    `json:"error,omitempty"`
}

type mcpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpToolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type mcpToolCallResult struct {
	IsError           bool           `json:"isError"`
	StructuredContent map[string]any `json:"structuredContent"`
}

type mcpStructuredOutcome struct {
	Status        string          `json:"status"`
	Output        json.RawMessage `json:"output"`
	Usage         httpWireUsage   `json:"usage"`
	FailureReason string          `json:"failure_reason"`
}

// mcpBinding is one operator-approved MCP binding's built transport.
type mcpBinding struct {
	serverURL string
	tool      string
	client    *http.Client
}

func newMCPBinding(b operatorconfig.ThirdPartyBinding) (*mcpBinding, error) {
	serverURL, tool, err := splitMCPEndpoint(b.EndpointRef)
	if err != nil {
		return nil, err
	}
	// runtimehttp.Build validates/parses BaseURL and applies the same
	// SSRF-resistant transport policy the HTTP binding uses; the MCP server
	// URL is operator-approved config here, never invocation data.
	_, client, err := runtimehttp.Build(runtimehttp.Config{
		BaseURL: serverURL, Timeout: b.Timeout, ConnectTimeout: defaultMCPConnectTimeout,
		MaxConnections: defaultMCPMaxConnections, MaxResponseHeaderBytes: defaultMCPMaxResponseHeaders,
		AllowedPlaintextCIDRs: b.AllowedPlaintextCIDRs,
	})
	if err != nil {
		return nil, fmt.Errorf("thirdparty: build MCP binding: %w", err)
	}
	return &mcpBinding{serverURL: serverURL, tool: tool, client: client}, nil
}

func (m *mcpBinding) close() { m.client.CloseIdleConnections() }

func splitMCPEndpoint(endpointRef string) (serverURL, tool string, err error) {
	idx := strings.LastIndex(endpointRef, "#")
	if idx < 0 || idx == len(endpointRef)-1 {
		return "", "", fmt.Errorf("thirdparty: MCP endpoint_ref must be \"<mcp-server-url>#<tool-name>\", got %q", endpointRef)
	}
	serverURL, tool = endpointRef[:idx], endpointRef[idx+1:]
	if serverURL == "" || tool == "" {
		return "", "", fmt.Errorf("thirdparty: MCP endpoint_ref must be \"<mcp-server-url>#<tool-name>\", got %q", endpointRef)
	}
	return serverURL, tool, nil
}

func (m *mcpBinding) call(ctx context.Context, method string, params any) (mcpRPCResponse, error) {
	body, err := json.Marshal(mcpRPCRequest{JSONRPC: jsonRPCVersion, ID: 1, Method: method, Params: params})
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: encode MCP request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.serverURL, bytes.NewReader(body))
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: build MCP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: MCP request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: non-2xx MCP response: %d", resp.StatusCode)
	}
	var rpcResp mcpRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: malformed MCP JSON-RPC response: %w", err)
	}
	if rpcResp.JSONRPC != jsonRPCVersion {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: unexpected MCP jsonrpc version %q", rpcResp.JSONRPC)
	}
	return rpcResp, nil
}

func (m *mcpBinding) invoke(ctx context.Context, req *edgev1.ThirdPartyInvokeRequest) (*edgev1.ThirdPartyInvokeResponse, error) {
	callCtx := ctx
	var cancel context.CancelFunc
	if req.DeadlineUnixMillis > 0 {
		callCtx, cancel = context.WithDeadline(ctx, time.UnixMilli(req.DeadlineUnixMillis))
		defer cancel()
	}
	args := map[string]any{
		"job_id": req.JobId, "capability_id": req.Binding.CapabilityId, "capability_version": req.Binding.CapabilityVersion,
		"idempotency_key": req.RequestId, "input": json.RawMessage(req.Input),
	}
	if req.DeadlineUnixMillis > 0 {
		args["deadline"] = time.UnixMilli(req.DeadlineUnixMillis)
	}
	rpcResp, err := m.call(callCtx, "tools/call", mcpToolCallParams{Name: m.tool, Arguments: args})
	if err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		if rpcResp.Error.Code == mcpCodeMethodNotFound {
			return nil, fmt.Errorf("thirdparty: MCP tool %q not found on provider server: %s", m.tool, rpcResp.Error.Message)
		}
		return nil, fmt.Errorf("thirdparty: MCP JSON-RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return m.decodeToolCallResult(req.RequestId, rpcResp.Result)
}

func (m *mcpBinding) decodeToolCallResult(requestID string, raw json.RawMessage) (*edgev1.ThirdPartyInvokeResponse, error) {
	var result mcpToolCallResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("thirdparty: malformed MCP tools/call result: %w", err)
	}
	if result.IsError {
		reason := ""
		if v, ok := result.StructuredContent["error"].(map[string]any); ok {
			if msg, ok := v["message"].(string); ok {
				reason = msg
			}
		}
		return &edgev1.ThirdPartyInvokeResponse{
			RequestId: requestID, Status: edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_FAILED,
			FailureReason: reason, CompletedUnixMillis: time.Now().UnixMilli(),
		}, nil
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return nil, fmt.Errorf("thirdparty: re-encode MCP structuredContent: %w", err)
	}
	var outcome mcpStructuredOutcome
	if err := json.Unmarshal(encoded, &outcome); err != nil {
		return nil, fmt.Errorf("thirdparty: malformed MCP structuredContent: %w", err)
	}
	status, err := decodeThirdPartyStatus(outcome.Status)
	if err != nil {
		return nil, err
	}
	return &edgev1.ThirdPartyInvokeResponse{
		RequestId: requestID, Status: status, Output: append([]byte(nil), outcome.Output...),
		FailureReason: outcome.FailureReason, CompletedUnixMillis: time.Now().UnixMilli(),
		Usage: &edgev1.Usage{
			InputBytes: outcome.Usage.InputBytes, OutputBytes: outcome.Usage.OutputBytes,
			InputTokens: outcome.Usage.InputTokens, OutputTokens: outcome.Usage.OutputTokens,
			ExecutionMillis: outcome.Usage.ExecutionMillis,
		},
	}, nil
}

// query always reports found=false: MCP has no protocol-level operation to
// look up a prior tools/call's outcome by an arbitrary caller-chosen
// identity -- honest per ThirdPartyExecutionService's Query doc comment,
// not a shortcut.
func (m *mcpBinding) query(context.Context, string) (*edgev1.ThirdPartyQueryResponse, error) {
	return &edgev1.ThirdPartyQueryResponse{Found: false}, nil
}

func (m *mcpBinding) health(ctx context.Context) *edgev1.ThirdPartyHealthResponse {
	healthCtx, cancel := context.WithTimeout(ctx, defaultHTTPHealthTimeout)
	defer cancel()
	start := time.Now()
	rpcResp, err := m.call(healthCtx, "tools/list", map[string]any{})
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: err.Error(), LatencyMillis: latency}
	}
	if rpcResp.Error != nil {
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: rpcResp.Error.Message, LatencyMillis: latency}
	}
	// deep_probe: unlike Health, this also verifies the specific bound tool
	// actually exists in the provider's tools/list -- genuinely more than
	// bare reachability, mirroring the interim ATOS-side
	// provideradapter.CertificationProbe precedent for MCP.
	var listed struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rpcResp.Result, &listed); err != nil {
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: "malformed tools/list result", LatencyMillis: latency}
	}
	for _, tool := range listed.Tools {
		if tool.Name == m.tool {
			return &edgev1.ThirdPartyHealthResponse{Healthy: true, LatencyMillis: latency, DeepProbe: true}
		}
	}
	return &edgev1.ThirdPartyHealthResponse{
		Healthy: false, FailureReason: fmt.Sprintf("tool %q not found in provider's tools/list", m.tool),
		LatencyMillis: latency, DeepProbe: true,
	}
}

var errMCPCancelUnsupported = errors.New("thirdparty: MCP transport does not support cancellation")

func (m *mcpBinding) cancel(context.Context, string, string) (*edgev1.ThirdPartyCancelResponse, error) {
	return nil, errMCPCancelUnsupported
}
