package thirdparty

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// Wire contract with the third-party A2A provider (matches ATOS's own
// interim a2aadapter package, which reuses the A2A protocol's own
// message/Task model): endpoint_ref is the provider agent's JSON-RPC
// endpoint URL. Invoke calls message/send with the Message's taskId set to
// the stable request_id -- reusing this execution's own durable identity as
// the A2A task id -- so a later Query can call tasks/get(id: request_id)
// for lost-response recovery, exactly as A2A's own task-tracking model
// intends. Cancel calls tasks/cancel, which A2A defines natively (unlike
// HTTP/MCP).

const a2aCommerceExtensionURI = "https://atos.im/extensions/commerce/v2"

type a2aPart struct {
	Kind string         `json:"kind"`
	Data map[string]any `json:"data,omitempty"`
}

type a2aMessage struct {
	Role     string         `json:"role"`
	Parts    []a2aPart      `json:"parts,omitempty"`
	TaskID   string         `json:"taskId,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type a2aTaskStatus struct {
	State string `json:"state"`
}

type a2aArtifact struct {
	Name    string         `json:"name,omitempty"`
	Content map[string]any `json:"content,omitempty"`
}

type a2aTask struct {
	ID        string        `json:"id"`
	Status    a2aTaskStatus `json:"status"`
	Artifacts []a2aArtifact `json:"artifacts,omitempty"`
}

type a2aMessageSendParams struct {
	Message a2aMessage `json:"message"`
}

type a2aTaskIDParams struct {
	ID string `json:"id"`
}

// a2aBinding is one operator-approved A2A binding's built transport.
type a2aBinding struct {
	endpointRef string
	client      *http.Client
	maxRequest  uint64
	maxResponse uint64
}

func newA2ABinding(b operatorconfig.ThirdPartyBinding) (*a2aBinding, error) {
	_, client, err := runtimehttp.Build(runtimehttp.Config{
		BaseURL: b.EndpointRef, Timeout: b.Timeout, ConnectTimeout: defaultMCPConnectTimeout,
		MaxConnections: defaultMCPMaxConnections, MaxResponseHeaderBytes: defaultMCPMaxResponseHeaders,
		AllowedPlaintextCIDRs: b.AllowedPlaintextCIDRs,
	})
	if err != nil {
		return nil, fmt.Errorf("thirdparty: build A2A binding: %w", err)
	}
	return &a2aBinding{
		endpointRef: b.EndpointRef, client: client,
		maxRequest: b.MaxRequestBytes, maxResponse: b.MaxResponseBytes,
	}, nil
}

func (a *a2aBinding) close() { a.client.CloseIdleConnections() }

func (a *a2aBinding) call(ctx context.Context, method string, params any) (mcpRPCResponse, error) {
	body, err := json.Marshal(mcpRPCRequest{JSONRPC: jsonRPCVersion, ID: 1, Method: method, Params: params})
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: encode A2A request: %w", err)
	}
	if uint64(len(body)) > a.maxRequest {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: A2A request exceeds %d byte limit", a.maxRequest)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpointRef, bytes.NewReader(body))
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: build A2A request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: A2A request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: non-2xx A2A response: %d", resp.StatusCode)
	}
	limited := io.LimitReader(resp.Body, int64(a.maxResponse)+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: read A2A response: %w", err)
	}
	if uint64(len(raw)) > a.maxResponse {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: A2A response exceeded %d byte limit", a.maxResponse)
	}
	var rpcResp mcpRPCResponse
	if err := json.Unmarshal(raw, &rpcResp); err != nil {
		return mcpRPCResponse{}, fmt.Errorf("thirdparty: malformed A2A JSON-RPC response: %w", err)
	}
	return rpcResp, nil
}

func a2aCommerceMetadata(capabilityID, requestID string) map[string]any {
	return map[string]any{
		a2aCommerceExtensionURI: map[string]any{
			"capability_id": capabilityID, "idempotency_key": requestID,
		},
	}
}

func (a *a2aBinding) invoke(ctx context.Context, req *edgev1.ThirdPartyInvokeRequest) (*edgev1.ThirdPartyInvokeResponse, error) {
	callCtx := ctx
	var cancel context.CancelFunc
	if req.DeadlineUnixMillis > 0 {
		callCtx, cancel = context.WithDeadline(ctx, time.UnixMilli(req.DeadlineUnixMillis))
		defer cancel()
	}
	var inputData map[string]any
	if len(req.Input) > 0 {
		if err := json.Unmarshal(req.Input, &inputData); err != nil {
			return nil, fmt.Errorf("thirdparty: A2A input is not a JSON object: %w", err)
		}
	}
	msg := a2aMessage{
		Role:     "user",
		Parts:    []a2aPart{{Kind: "data", Data: inputData}},
		TaskID:   req.RequestId,
		Metadata: a2aCommerceMetadata(req.Binding.CapabilityId, req.RequestId),
	}
	rpcResp, err := a.call(callCtx, "message/send", a2aMessageSendParams{Message: msg})
	if err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("thirdparty: A2A JSON-RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return a2aDecodeTaskResult(req.RequestId, rpcResp.Result)
}

func a2aDecodeTaskResult(requestID string, raw json.RawMessage) (*edgev1.ThirdPartyInvokeResponse, error) {
	var task a2aTask
	if err := json.Unmarshal(raw, &task); err != nil {
		return nil, fmt.Errorf("thirdparty: malformed A2A Task response: %w", err)
	}
	status, err := a2aMapTaskState(task.Status.State)
	if err != nil {
		return nil, err
	}
	var output []byte
	if content := a2aTaskOutput(task); content != nil {
		output, err = json.Marshal(content)
		if err != nil {
			return nil, fmt.Errorf("thirdparty: re-encode A2A task output: %w", err)
		}
	}
	return &edgev1.ThirdPartyInvokeResponse{
		RequestId: requestID, Status: status, Output: output, CompletedUnixMillis: time.Now().UnixMilli(),
	}, nil
}

// a2aMapTaskState fails closed (an explicit error, never a guessed
// completed/failed) on any A2A task state this binding does not recognize
// -- a provider on a newer/different A2A revision must not be silently
// misinterpreted as having succeeded or failed.
func a2aMapTaskState(state string) (edgev1.ThirdPartyInvokeStatus, error) {
	switch state {
	case "completed":
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_COMPLETED, nil
	case "failed", "canceled", "rejected":
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_FAILED, nil
	case "submitted", "working", "input-required":
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_PENDING, nil
	default:
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_UNSPECIFIED, fmt.Errorf("thirdparty: unknown A2A task state %q, failing closed", state)
	}
}

// a2aTaskOutput extracts result data from a Task's Artifacts, preferring
// one literally named "result" and otherwise falling back to the first
// artifact -- mirroring the interim ATOS-side a2aadapter's identical
// convention.
func a2aTaskOutput(task a2aTask) map[string]any {
	for _, artifact := range task.Artifacts {
		if artifact.Name == "result" {
			return artifact.Content
		}
	}
	if len(task.Artifacts) > 0 {
		return task.Artifacts[0].Content
	}
	return nil
}

// query calls tasks/get(id: requestID) -- recovering a lost message/send
// response via the same durable identity Invoke set as the task's TaskID.
// A provider reporting any JSON-RPC error (this binding does not attempt
// to distinguish A2A error codes beyond that) is treated as found=false, a
// legitimate absence of prior state, never surfaced as an error.
func (a *a2aBinding) query(ctx context.Context, requestID string) (*edgev1.ThirdPartyQueryResponse, error) {
	rpcResp, err := a.call(ctx, "tasks/get", a2aTaskIDParams{ID: requestID})
	if err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return &edgev1.ThirdPartyQueryResponse{Found: false}, nil
	}
	result, err := a2aDecodeTaskResult(requestID, rpcResp.Result)
	if err != nil {
		return nil, err
	}
	return &edgev1.ThirdPartyQueryResponse{Found: true, Result: result}, nil
}

func (a *a2aBinding) cancel(ctx context.Context, requestID, reason string) (*edgev1.ThirdPartyCancelResponse, error) {
	type params struct {
		ID       string         `json:"id"`
		Metadata map[string]any `json:"metadata"`
	}
	rpcResp, err := a.call(ctx, "tasks/cancel", params{
		ID:       requestID,
		Metadata: map[string]any{a2aCommerceExtensionURI: map[string]any{"idempotency_key": requestID, "reason": reason}},
	})
	if err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("thirdparty: A2A cancel JSON-RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return &edgev1.ThirdPartyCancelResponse{Accepted: true}, nil
}

// health calls tasks/get with a deliberately unused probe id and treats
// ANY well-formed JSON-RPC response (including a "task not found" error,
// which proves the method itself is implemented and reachable) as healthy
// -- pure reachability, mirroring the interim ATOS-side a2aadapter.
func (a *a2aBinding) health(ctx context.Context) *edgev1.ThirdPartyHealthResponse {
	healthCtx, cancel := context.WithTimeout(ctx, defaultHTTPHealthTimeout)
	defer cancel()
	start := time.Now()
	_, err := a.call(healthCtx, "tasks/get", a2aTaskIDParams{ID: "tosai-thirdparty-health-probe"})
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: err.Error(), LatencyMillis: latency}
	}
	return &edgev1.ThirdPartyHealthResponse{Healthy: true, LatencyMillis: latency}
}
