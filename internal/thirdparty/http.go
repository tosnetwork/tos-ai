package thirdparty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// Wire contract with the third-party HTTP provider (matches the contract
// ATOS's own interim httpadapter package used, since providers are
// registered against this exact shape regardless of which internal ATOS
// component ends up dialing them): POST {endpoint_ref} with header
// Idempotency-Key and JSON body {job_id, capability_id, capability_version,
// idempotency_key, input, deadline}; a 2xx response body is decoded as
// {status: "completed"|"failed"|"pending", output, usage, failure_reason}.
// GET {endpoint_ref}?idempotency_key=... with the same header for Query;
// 404 means "no record", not an error.

const (
	defaultHTTPConnectTimeout     = 5 * time.Second
	defaultHTTPMaxConnections     = 8
	defaultHTTPMaxResponseHeaders = int64(16 << 10)
	defaultHTTPHealthTimeout      = 5 * time.Second
)

type httpWireRequest struct {
	JobID             string          `json:"job_id"`
	CapabilityID      string          `json:"capability_id"`
	CapabilityVersion string          `json:"capability_version"`
	IdempotencyKey    string          `json:"idempotency_key"`
	Input             json.RawMessage `json:"input,omitempty"`
	Deadline          *time.Time      `json:"deadline,omitempty"`
}

type httpWireUsage struct {
	InputBytes      uint64 `json:"input_bytes"`
	OutputBytes     uint64 `json:"output_bytes"`
	InputTokens     uint64 `json:"input_tokens"`
	OutputTokens    uint64 `json:"output_tokens"`
	ExecutionMillis uint64 `json:"execution_millis"`
}

type httpWireResponse struct {
	Status        string          `json:"status"`
	Output        json.RawMessage `json:"output"`
	Usage         httpWireUsage   `json:"usage"`
	FailureReason string          `json:"failure_reason"`
}

// httpBinding is one operator-approved HTTP binding's built, ready-to-use
// transport -- constructed once at startup from operatorconfig.
// ThirdPartyBinding, never per-invocation, exactly like the openai
// adapter's *http.Client is built once from its own operator config.
type httpBinding struct {
	endpoint    *url.URL
	client      *http.Client
	maxRequest  uint64
	maxResponse uint64
}

func newHTTPBinding(b operatorconfig.ThirdPartyBinding) (*httpBinding, error) {
	baseURL, client, err := runtimehttp.Build(runtimehttp.Config{
		BaseURL: b.EndpointRef, Timeout: b.Timeout, ConnectTimeout: defaultHTTPConnectTimeout,
		MaxConnections: defaultHTTPMaxConnections, MaxResponseHeaderBytes: defaultHTTPMaxResponseHeaders,
		AllowedPlaintextCIDRs: b.AllowedPlaintextCIDRs,
	})
	if err != nil {
		return nil, fmt.Errorf("thirdparty: build HTTP binding: %w", err)
	}
	return &httpBinding{endpoint: baseURL, client: client, maxRequest: b.MaxRequestBytes, maxResponse: b.MaxResponseBytes}, nil
}

func (h *httpBinding) close() { h.client.CloseIdleConnections() }

func (h *httpBinding) invoke(ctx context.Context, req *edgev1.ThirdPartyInvokeRequest) (*edgev1.ThirdPartyInvokeResponse, error) {
	body := httpWireRequest{
		JobID: req.JobId, CapabilityID: req.Binding.CapabilityId, CapabilityVersion: req.Binding.CapabilityVersion,
		IdempotencyKey: req.RequestId, Input: json.RawMessage(req.Input),
	}
	if req.DeadlineUnixMillis > 0 {
		deadline := time.UnixMilli(req.DeadlineUnixMillis)
		body.Deadline = &deadline
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("thirdparty: encode HTTP invoke request: %w", err)
	}
	if uint64(len(encoded)) > h.maxRequest {
		return nil, fmt.Errorf("thirdparty: HTTP invoke request exceeds %d byte limit", h.maxRequest)
	}
	callCtx := ctx
	var cancel context.CancelFunc
	if req.DeadlineUnixMillis > 0 {
		callCtx, cancel = context.WithDeadline(ctx, time.UnixMilli(req.DeadlineUnixMillis))
		defer cancel()
	}
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, h.endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("thirdparty: build HTTP invoke request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.RequestId)
	resp, err := h.client.Do(httpReq)
	if err != nil {
		// Connection failure or timeout -- nothing here proves the provider
		// ever received or acted on the request, so this must surface as a
		// safe-to-retry-via-Query failed attempt, never a fabricated result.
		return nil, fmt.Errorf("thirdparty: HTTP invoke request failed: %w", err)
	}
	wire, err := h.decode(resp)
	if err != nil {
		return nil, err
	}
	return toThirdPartyInvokeResponse(req.RequestId, wire)
}

func (h *httpBinding) query(ctx context.Context, requestID string) (*edgev1.ThirdPartyQueryResponse, error) {
	u := *h.endpoint
	q := u.Query()
	q.Set("idempotency_key", requestID)
	u.RawQuery = q.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("thirdparty: build HTTP query request: %w", err)
	}
	httpReq.Header.Set("Idempotency-Key", requestID)
	resp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("thirdparty: HTTP query request failed: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return &edgev1.ThirdPartyQueryResponse{Found: false}, nil
	}
	wire, err := h.decode(resp)
	if err != nil {
		return nil, err
	}
	result, err := toThirdPartyInvokeResponse(requestID, wire)
	if err != nil {
		return nil, err
	}
	return &edgev1.ThirdPartyQueryResponse{Found: true, Result: result}, nil
}

func (h *httpBinding) health(ctx context.Context) *edgev1.ThirdPartyHealthResponse {
	healthCtx, cancel := context.WithTimeout(ctx, defaultHTTPHealthTimeout)
	defer cancel()
	start := time.Now()
	u := *h.endpoint
	q := u.Query()
	q.Set("idempotency_key", "tosai-thirdparty-health-probe-00000000")
	u.RawQuery = q.Encode()
	httpReq, err := http.NewRequestWithContext(healthCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: err.Error()}
	}
	resp, err := h.client.Do(httpReq)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: err.Error(), LatencyMillis: latency}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		// A clean "no record" for a synthetic, never-real key is exactly
		// the expected, healthy answer -- it proves the provider correctly
		// implements the Query side of the wire contract.
		return &edgev1.ThirdPartyHealthResponse{Healthy: true, LatencyMillis: latency}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return &edgev1.ThirdPartyHealthResponse{Healthy: false, FailureReason: fmt.Sprintf("unexpected status %d", resp.StatusCode), LatencyMillis: latency}
	}
	return &edgev1.ThirdPartyHealthResponse{Healthy: true, LatencyMillis: latency}
}

func (h *httpBinding) decode(resp *http.Response) (httpWireResponse, error) {
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, int64(h.maxResponse)+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return httpWireResponse{}, fmt.Errorf("thirdparty: read HTTP response: %w", err)
	}
	if uint64(len(raw)) > h.maxResponse {
		return httpWireResponse{}, fmt.Errorf("thirdparty: HTTP response exceeded %d byte limit", h.maxResponse)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpWireResponse{}, fmt.Errorf("thirdparty: non-2xx HTTP response: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
		return httpWireResponse{}, fmt.Errorf("thirdparty: unexpected HTTP content-type %q", ct)
	}
	var wire httpWireResponse
	if err := json.Unmarshal(raw, &wire); err != nil {
		return httpWireResponse{}, fmt.Errorf("thirdparty: malformed HTTP response JSON: %w", err)
	}
	return wire, nil
}

func toThirdPartyInvokeResponse(requestID string, wire httpWireResponse) (*edgev1.ThirdPartyInvokeResponse, error) {
	status, err := decodeThirdPartyStatus(wire.Status)
	if err != nil {
		return nil, err
	}
	return &edgev1.ThirdPartyInvokeResponse{
		RequestId: requestID, Status: status, Output: append([]byte(nil), wire.Output...),
		FailureReason: wire.FailureReason, CompletedUnixMillis: time.Now().UnixMilli(),
		Usage: &edgev1.Usage{
			InputBytes: wire.Usage.InputBytes, OutputBytes: wire.Usage.OutputBytes,
			InputTokens: wire.Usage.InputTokens, OutputTokens: wire.Usage.OutputTokens,
			ExecutionMillis: wire.Usage.ExecutionMillis,
		},
	}, nil
}

func decodeThirdPartyStatus(raw string) (edgev1.ThirdPartyInvokeStatus, error) {
	switch raw {
	case "completed":
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_COMPLETED, nil
	case "failed":
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_FAILED, nil
	case "pending":
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_PENDING, nil
	default:
		return edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_UNSPECIFIED, fmt.Errorf("thirdparty: unknown status %q in provider response", raw)
	}
}

func isJSONContentType(ct string) bool {
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "application/json")
}

var errHTTPCancelUnsupported = errors.New("thirdparty: HTTP transport does not support cancellation")

func (h *httpBinding) cancel(context.Context, string, string) (*edgev1.ThirdPartyCancelResponse, error) {
	return nil, errHTTPCancelUnsupported
}
