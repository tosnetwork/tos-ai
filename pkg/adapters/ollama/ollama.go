// Package ollama adapts an operator-controlled Ollama HTTP runtime. The
// adapter never starts Ollama and never accepts an arbitrary runtime URL from
// task payloads.
package ollama

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/adapters/internal/httpclient"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

type Config struct {
	BaseURL                string
	Model                  string
	ModelDigest            string
	MaxInputBytes          uint64
	MaxOutputBytes         uint64
	MaxRequestBytes        uint64
	MaxResponseBytes       uint64
	MaxConnections         int
	MaxResponseHeaderBytes int64
	Timeout                time.Duration
	ConnectTimeout         time.Duration
	AllowedPlaintextCIDRs  []string
	Admission              admission.Resources
}

const (
	maxBodyBytesHard = uint64(64 << 20)
	responseOverhead = uint64(64 << 10)
	jsonExpansion    = uint64(6)
)

type Adapter struct {
	endpoint          string
	preflightEndpoint string
	httpClient        *http.Client
	capability        airuntime.Capability
	maxRequest        uint64
	maxResponse       uint64
}

func New(config Config) (*Adapter, error) {
	if config.Model == "" || config.ModelDigest == "" || config.MaxInputBytes == 0 ||
		config.MaxOutputBytes == 0 || config.MaxRequestBytes == 0 ||
		config.MaxResponseBytes < config.MaxOutputBytes ||
		config.MaxInputBytes > maxBodyBytesHard || config.MaxOutputBytes > maxBodyBytesHard ||
		config.MaxRequestBytes > maxBodyBytesHard ||
		config.MaxResponseBytes > maxBodyBytesHard ||
		config.Admission.RAMBytes == 0 || config.Admission.ContextTokens == 0 ||
		config.Admission.BatchSize == 0 || config.Admission.ExecutionTime <= 0 {
		return nil, errors.New("invalid Ollama adapter configuration")
	}
	config.Admission.OutputBytes = config.MaxOutputBytes
	capability := airuntime.Capability{
		ServiceID:       "tos.ai.ollama",
		Operation:       "generate",
		Model:           config.Model,
		ModelDigest:     config.ModelDigest,
		Runtime:         "ollama",
		RuntimeRevision: "ollama-http-v1",
		MaxInputBytes:   config.MaxInputBytes,
		MaxOutputBytes:  config.MaxOutputBytes,
		AcceptedPriorities: []airuntime.Priority{
			airuntime.PriorityLocalAsync,
			airuntime.PriorityExternalService,
			airuntime.PriorityBackground,
		},
		Admission: config.Admission,
	}
	if err := airuntime.ValidateCapability(capability); err != nil {
		return nil, errors.New("invalid Ollama capability")
	}
	baseURL, client, err := httpclient.Build(httpclient.Config{
		BaseURL: config.BaseURL, Timeout: config.Timeout, ConnectTimeout: config.ConnectTimeout,
		MaxConnections: config.MaxConnections, MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
		AllowedPlaintextCIDRs: config.AllowedPlaintextCIDRs,
	})
	if err != nil {
		return nil, err
	}
	return &Adapter{
		endpoint:          httpclient.Endpoint(baseURL, "/api/generate"),
		preflightEndpoint: httpclient.Endpoint(baseURL, "/api/tags"),
		httpClient:        client,
		maxRequest:        config.MaxRequestBytes,
		maxResponse:       config.MaxResponseBytes,
		capability:        capability,
	}, nil
}

func (a *Adapter) Capability() airuntime.Capability {
	return a.capability
}

func (a *Adapter) Close() error {
	a.httpClient.CloseIdleConnections()
	return nil
}

func (a *Adapter) Preflight(ctx context.Context) (airuntime.Preflight, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.preflightEndpoint, nil)
	if err != nil {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorInternal, nil)
	}
	response, err := a.httpClient.Do(request)
	if err != nil {
		return airuntime.Preflight{}, classifyTransportError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorRemote, nil)
	}
	data, err := io.ReadAll(io.LimitReader(
		response.Body, int64(airuntime.MaxPreflightResponseBytesHard)+1,
	))
	if err != nil {
		return airuntime.Preflight{}, classifyTransportError(ctx, err)
	}
	if uint64(len(data)) > airuntime.MaxPreflightResponseBytesHard {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	var inventory struct {
		Models []struct {
			Name   string `json:"name"`
			Model  string `json:"model"`
			Digest string `json:"digest"`
		} `json:"models"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&inventory); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	if len(inventory.Models) > airuntime.MaxPreflightModelsHard {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	var result *airuntime.Preflight
	for _, candidate := range inventory.Models {
		if candidate.Name != a.capability.Model && candidate.Model != a.capability.Model {
			continue
		}
		if result != nil || !validDigestHex(candidate.Digest) {
			return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
		}
		value := airuntime.Preflight{
			Model:          a.capability.Model,
			ModelDigest:    "sha256:" + candidate.Digest,
			DigestEvidence: airuntime.BindingLocallyObserved,
		}
		result = &value
	}
	if result == nil {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorUnavailable, nil)
	}
	if err := airuntime.ValidatePreflight(a.capability, *result); err != nil {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	return *result, nil
}

func (a *Adapter) Execute(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
	if err := airuntime.ValidateRequest(a.capability, request); err != nil {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorInvalid, err)
	}
	body, err := json.Marshal(map[string]interface{}{
		"model":  a.capability.Model,
		"prompt": string(request.Payload),
		"stream": false,
	})
	if err != nil {
		return airuntime.Response{}, err
	}
	if uint64(len(body)) > a.maxRequest {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorLimit, errors.New("request body limit"))
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(body))
	if err != nil {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorInternal, nil)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	start := time.Now()
	httpResponse, err := a.httpClient.Do(httpRequest)
	if err != nil {
		return airuntime.Response{}, classifyTransportError(ctx, err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, 4<<10))
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorRemote, nil)
	}
	maxResponse := int64(responseLimit(a.maxResponse, request.MaxOutputBytes))
	data, err := io.ReadAll(io.LimitReader(httpResponse.Body, maxResponse+1))
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return airuntime.Response{}, airuntime.NewError(airuntime.ErrorCanceled, context.Canceled)
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return airuntime.Response{}, airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
		}
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorUnavailable, nil)
	}
	if int64(len(data)) > maxResponse {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	var response struct {
		Output          string `json:"response"`
		PromptTokens    uint64 `json:"prompt_eval_count"`
		GeneratedTokens uint64 `json:"eval_count"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		// Ollama adds timing and state fields. Decode the bounded response a
		// second time without treating those documented extensions as authority.
		if err := json.Unmarshal(data, &response); err != nil {
			return airuntime.Response{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
		}
	}
	output := []byte(response.Output)
	if uint64(len(output)) > request.MaxOutputBytes {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	return airuntime.Response{
		Output: output,
		Usage: airuntime.Usage{
			InputBytes:      uint64(len(request.Payload)),
			OutputBytes:     uint64(len(output)),
			InputTokens:     response.PromptTokens,
			OutputTokens:    response.GeneratedTokens,
			ExecutionMillis: airuntime.MillisecondsSince(start),
		},
		ModelRevision:   a.capability.ModelDigest,
		RuntimeRevision: a.capability.RuntimeRevision,
	}, nil
}

func responseLimit(maximum, outputBytes uint64) uint64 {
	encoded := outputBytes*jsonExpansion + responseOverhead
	if encoded < outputBytes || encoded > maximum {
		return maximum
	}
	return encoded
}

func classifyTransportError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return airuntime.NewError(airuntime.ErrorCanceled, context.Canceled)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(err, context.DeadlineExceeded) {
		return airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
	}
	return airuntime.NewError(airuntime.ErrorUnavailable, nil)
}

func validDigestHex(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
