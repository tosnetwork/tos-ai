// Package ollama adapts an operator-controlled Ollama HTTP runtime. The
// adapter never starts Ollama and never accepts an arbitrary runtime URL from
// task payloads.
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

type Config struct {
	BaseURL        string
	Model          string
	ModelDigest    string
	MaxInputBytes  uint64
	MaxOutputBytes uint64
	Timeout        time.Duration
}

type Adapter struct {
	baseURL    *url.URL
	httpClient *http.Client
	capability airuntime.Capability
}

func New(config Config) (*Adapter, error) {
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "https" && baseURL.Scheme != "http") {
		return nil, errors.New("Ollama base URL must be absolute HTTP(S)")
	}
	host := baseURL.Hostname()
	if baseURL.Scheme == "http" && host != "localhost" && net.ParseIP(host) == nil {
		return nil, errors.New("plaintext Ollama is allowed only on a literal IP or localhost")
	}
	if config.Model == "" || config.ModelDigest == "" || config.MaxInputBytes == 0 ||
		config.MaxOutputBytes == 0 || config.Timeout <= 0 {
		return nil, errors.New("invalid Ollama adapter configuration")
	}
	return &Adapter{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: config.Timeout},
		capability: airuntime.Capability{
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
		},
	}, nil
}

func (a *Adapter) Capability() airuntime.Capability {
	return a.capability
}

func (a *Adapter) Execute(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
	if err := airuntime.ValidateRequest(a.capability, request); err != nil {
		return airuntime.Response{}, err
	}
	body, err := json.Marshal(map[string]interface{}{
		"model":  a.capability.Model,
		"prompt": string(request.Payload),
		"stream": false,
	})
	if err != nil {
		return airuntime.Response{}, err
	}
	endpoint := *a.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/api/generate"
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return airuntime.Response{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	start := time.Now()
	httpResponse, err := a.httpClient.Do(httpRequest)
	if err != nil {
		return airuntime.Response{}, fmt.Errorf("Ollama request: %w", err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, 4<<10))
		return airuntime.Response{}, fmt.Errorf("Ollama HTTP status %d", httpResponse.StatusCode)
	}
	maxResponse := int64(request.MaxOutputBytes) + 64<<10
	data, err := io.ReadAll(io.LimitReader(httpResponse.Body, maxResponse+1))
	if err != nil {
		return airuntime.Response{}, err
	}
	if int64(len(data)) > maxResponse {
		return airuntime.Response{}, errors.New("Ollama response exceeds byte limit")
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
			return airuntime.Response{}, fmt.Errorf("decode Ollama response: %w", err)
		}
	}
	output := []byte(response.Output)
	if uint64(len(output)) > request.MaxOutputBytes {
		return airuntime.Response{}, errors.New("Ollama output exceeds requested limit")
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
