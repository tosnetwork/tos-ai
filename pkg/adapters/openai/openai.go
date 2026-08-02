// Package openai adapts an administrator-configured OpenAI-compatible HTTP
// endpoint such as LocalAI or vLLM. Task payloads cannot select the endpoint.
package openai

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/adapters/internal/httpclient"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

const (
	MaxBodyBytesHard   = uint64(64 << 20)
	responseOverhead   = uint64(64 << 10)
	jsonExpansion      = uint64(6)
	MaxCredentialBytes = 8 << 10
)

type Config struct {
	BaseURL         string
	APIKey          string
	Model           string
	ModelDigest     string
	RuntimeRevision string
	// Runtime is an operator-selected fixed implementation identity. Supported
	// values share the OpenAI-compatible wire contract but are never selected
	// by an invocation payload.
	Runtime                string
	MaxInputBytes          uint64
	MaxOutputBytes         uint64
	MaxRequestBytes        uint64
	MaxResponseBytes       uint64
	MaxConnections         int
	MaxResponseHeaderBytes int64
	Timeout                time.Duration
	ConnectTimeout         time.Duration
	AllowedPlaintextCIDRs  []string
	RootCAs                *x509.CertPool
	ClientCertificate      *tls.Certificate
	TLSServerName          string
	Admission              admission.Resources
}

type Adapter struct {
	endpoint          string
	preflightEndpoint string
	apiKey            string
	httpClient        *http.Client
	capability        airuntime.Capability
	maxRequest        uint64
	maxResponse       uint64
}

func New(config Config) (*Adapter, error) {
	if config.Runtime == "" {
		config.Runtime = "openai-compatible"
	}
	if config.Model == "" || config.ModelDigest == "" || config.RuntimeRevision == "" ||
		config.MaxInputBytes == 0 || config.MaxOutputBytes == 0 ||
		config.MaxRequestBytes == 0 || config.MaxRequestBytes > MaxBodyBytesHard ||
		config.MaxResponseBytes == 0 || config.MaxResponseBytes > MaxBodyBytesHard ||
		config.MaxResponseBytes < config.MaxOutputBytes ||
		config.Admission.RAMBytes == 0 || config.Admission.ContextTokens == 0 ||
		config.Admission.BatchSize == 0 || config.Admission.ExecutionTime <= 0 ||
		!validCredential(config.APIKey) || !validRuntime(config.Runtime) {
		return nil, errors.New("invalid OpenAI-compatible adapter configuration")
	}
	config.Admission.OutputBytes = config.MaxOutputBytes
	capability := airuntime.Capability{
		ServiceID: "tos.ai.openai-compatible", Operation: "generate",
		Model: config.Model, ModelDigest: config.ModelDigest,
		Runtime: config.Runtime, RuntimeRevision: config.RuntimeRevision,
		MaxInputBytes: config.MaxInputBytes, MaxOutputBytes: config.MaxOutputBytes,
		AcceptedPriorities: []airuntime.Priority{
			airuntime.PriorityLocalAsync,
			airuntime.PriorityExternalService,
			airuntime.PriorityBackground,
		},
		Admission: config.Admission,
	}
	if err := airuntime.ValidateCapability(capability); err != nil {
		return nil, errors.New("invalid OpenAI-compatible capability")
	}
	baseURL, client, err := httpclient.Build(httpclient.Config{
		BaseURL: config.BaseURL, Timeout: config.Timeout, ConnectTimeout: config.ConnectTimeout,
		MaxConnections: config.MaxConnections, MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
		AllowedPlaintextCIDRs: config.AllowedPlaintextCIDRs,
		RootCAs:               config.RootCAs, ClientCertificate: config.ClientCertificate,
		TLSServerName: config.TLSServerName,
	})
	if err != nil {
		return nil, err
	}
	return &Adapter{
		endpoint:          httpclient.Endpoint(baseURL, "/v1/chat/completions"),
		preflightEndpoint: httpclient.Endpoint(baseURL, "/v1/models"),
		apiKey:            config.APIKey,
		httpClient:        client,
		maxRequest:        config.MaxRequestBytes,
		maxResponse:       config.MaxResponseBytes,
		capability:        capability,
	}, nil
}

func validRuntime(value string) bool {
	switch value {
	case "openai-compatible", "vllm", "llama.cpp", "localai":
		return true
	default:
		return false
	}
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
	if a.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	response, err := a.httpClient.Do(request)
	if err != nil {
		return airuntime.Preflight{}, classifyTransportError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
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
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&inventory); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	if inventory.Object != "list" {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	if len(inventory.Data) > airuntime.MaxPreflightModelsHard {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	matches := 0
	for _, candidate := range inventory.Data {
		if candidate.ID == a.capability.Model {
			matches++
		}
	}
	if matches == 0 {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorUnavailable, nil)
	}
	if matches != 1 {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	result := airuntime.Preflight{
		Model:          a.capability.Model,
		ModelDigest:    a.capability.ModelDigest,
		DigestEvidence: airuntime.BindingDeclared,
	}
	if err := airuntime.ValidatePreflight(a.capability, result); err != nil {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	return result, nil
}

func (a *Adapter) Execute(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
	if err := airuntime.ValidateRequest(a.capability, request); err != nil {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorInvalid, err)
	}
	body, err := json.Marshal(struct {
		Model    string    `json:"model"`
		Messages []message `json:"messages"`
		Stream   bool      `json:"stream"`
	}{
		Model:    a.capability.Model,
		Messages: []message{{Role: "user", Content: string(request.Payload)}},
		Stream:   false,
	})
	if err != nil || uint64(len(body)) > a.maxRequest {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorLimit, errors.New("request body limit"))
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(body))
	if err != nil {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorInternal, nil)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	start := time.Now()
	httpResponse, err := a.httpClient.Do(httpRequest)
	if err != nil {
		return airuntime.Response{}, classifyTransportError(ctx, err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, 4<<10))
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorRemote, nil)
	}
	limit := responseLimit(a.maxResponse, request.MaxOutputBytes)
	data, err := io.ReadAll(io.LimitReader(httpResponse.Body, int64(limit)+1))
	if err != nil {
		return airuntime.Response{}, classifyTransportError(ctx, err)
	}
	if uint64(len(data)) > limit {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	var response completionResponse
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&response); err != nil {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	if decoder.Decode(&struct{}{}) != io.EOF || len(response.Choices) != 1 {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	output := []byte(response.Choices[0].Message.Content)
	if uint64(len(output)) > request.MaxOutputBytes {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorLimit, nil)
	}
	return airuntime.Response{
		Output: output,
		Usage: airuntime.Usage{
			InputBytes: uint64(len(request.Payload)), OutputBytes: uint64(len(output)),
			InputTokens: response.Usage.PromptTokens, OutputTokens: response.Usage.CompletionTokens,
			ExecutionMillis: airuntime.MillisecondsSince(start),
		},
		ModelRevision: a.capability.ModelDigest, RuntimeRevision: a.capability.RuntimeRevision,
	}, nil
}

func responseLimit(maximum, outputBytes uint64) uint64 {
	encoded := outputBytes*jsonExpansion + responseOverhead
	if encoded < outputBytes || encoded > maximum {
		return maximum
	}
	return encoded
}

func validCredential(value string) bool {
	if len(value) > MaxCredentialBytes {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completionResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     uint64 `json:"prompt_tokens"`
		CompletionTokens uint64 `json:"completion_tokens"`
	} `json:"usage"`
}

func classifyTransportError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return airuntime.NewError(airuntime.ErrorCanceled, context.Canceled)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
	}
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Timeout() {
		return airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
	}
	return airuntime.NewError(airuntime.ErrorUnavailable, nil)
}
