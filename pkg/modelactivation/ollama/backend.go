// Package ollama implements a fail-closed modelactivation backend for
// administrator-owned Ollama runtimes. It uploads only an already-open,
// SHA-256-approved GGUF artifact and never calls pull or accepts a URL/path.
package ollama

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
	"github.com/tosnetwork/tos-ai/pkg/modelactivation"
	"github.com/tosnetwork/tos-ai/pkg/ollamabinding"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

const (
	RuntimeRevision        = "ollama-http-v1"
	MaxResponseBytesHard   = uint64(1 << 20)
	MaxInventoryModelsHard = 256
	MaxConnectionsHard     = 4
	MaxCleanupTimeoutHard  = time.Minute
	maxJSONDepth           = 16
	maxJSONFields          = 64
	maxJSONArrayItems      = MaxInventoryModelsHard
	maxJSONValues          = 2048
	artifactFilename       = "approved-model.gguf"
)

type Config struct {
	BaseURL                string
	SlotID                 string
	Model                  string
	Timeout                time.Duration
	ConnectTimeout         time.Duration
	CleanupTimeout         time.Duration
	MaxConnections         int
	MaxResponseHeaderBytes int64
	MaxResponseBytes       uint64
	AllowedPlaintextCIDRs  []string
	RootCAs                *x509.CertPool
	ClientCertificate      *tls.Certificate
	TLSServerName          string
}

type Backend struct {
	client *http.Client

	tagsEndpoint     string
	showEndpoint     string
	createEndpoint   string
	generateEndpoint string
	deleteEndpoint   string
	blobEndpoint     string

	slotID         string
	model          string
	cleanupTimeout time.Duration
	maxResponse    uint64
	gate           chan struct{}
	closed         atomic.Bool
	closeOnce      sync.Once
}

func New(config Config) (*Backend, error) {
	if !ollamabinding.ValidSlotID(config.SlotID) ||
		!validIdentity(config.Model) ||
		config.CleanupTimeout <= 0 ||
		config.CleanupTimeout > MaxCleanupTimeoutHard ||
		config.MaxConnections <= 0 ||
		config.MaxConnections > MaxConnectionsHard ||
		config.MaxResponseBytes == 0 ||
		config.MaxResponseBytes > MaxResponseBytesHard {
		return nil, errors.New("invalid Ollama activation configuration")
	}
	baseURL, client, err := runtimehttp.Build(runtimehttp.Config{
		BaseURL: config.BaseURL, Timeout: config.Timeout,
		ConnectTimeout:         config.ConnectTimeout,
		MaxConnections:         config.MaxConnections,
		MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
		AllowedPlaintextCIDRs:  config.AllowedPlaintextCIDRs,
		RootCAs:                config.RootCAs, ClientCertificate: config.ClientCertificate,
		TLSServerName: config.TLSServerName,
	})
	if err != nil {
		return nil, errors.New("invalid Ollama activation endpoint")
	}
	backend := &Backend{
		client:           client,
		tagsEndpoint:     runtimehttp.Endpoint(baseURL, "/api/tags"),
		showEndpoint:     runtimehttp.Endpoint(baseURL, "/api/show"),
		createEndpoint:   runtimehttp.Endpoint(baseURL, "/api/create"),
		generateEndpoint: runtimehttp.Endpoint(baseURL, "/api/generate"),
		deleteEndpoint:   runtimehttp.Endpoint(baseURL, "/api/delete"),
		blobEndpoint:     runtimehttp.Endpoint(baseURL, "/api/blobs/"),
		slotID:           config.SlotID,
		model:            config.Model,
		cleanupTimeout:   config.CleanupTimeout,
		maxResponse:      config.MaxResponseBytes,
		gate:             make(chan struct{}, 1),
	}
	backend.gate <- struct{}{}
	return backend, nil
}

func (b *Backend) Load(
	ctx context.Context,
	request modelactivation.LoadRequest,
) (modelactivation.Binding, error) {
	if err := b.lock(ctx); err != nil {
		return modelactivation.Binding{}, err
	}
	defer b.unlock()
	if err := b.validateLoad(request); err != nil {
		return modelactivation.Binding{}, err
	}
	binding, err := b.binding(request.ModelDigest)
	if err != nil {
		return modelactivation.Binding{}, err
	}
	exists, err := b.show(ctx, binding)
	if err != nil {
		return modelactivation.Binding{}, err
	}
	if exists {
		return binding, nil
	}
	if err := b.ensureBlob(ctx, request); err != nil {
		return modelactivation.Binding{}, err
	}

	createAttempted := false
	success := false
	defer func() {
		if !success && createAttempted {
			cleanupContext, cancel := context.WithTimeout(
				context.Background(), b.cleanupTimeout,
			)
			_ = b.deleteModel(cleanupContext, binding.Handle)
			cancel()
		}
	}()
	createAttempted = true
	if err := b.createModel(ctx, binding); err != nil {
		return binding, err
	}
	exists, err = b.show(ctx, binding)
	if err != nil || !exists {
		return binding, errors.New("verify created Ollama model")
	}
	success = true
	return binding, nil
}

func (b *Backend) Health(
	ctx context.Context,
	binding modelactivation.Binding,
) error {
	if err := b.lock(ctx); err != nil {
		return err
	}
	defer b.unlock()
	if err := b.validateBinding(binding); err != nil {
		return err
	}
	exists, err := b.show(ctx, binding)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Ollama activation binding is unavailable")
	}
	if err := b.headBlob(ctx, binding.ModelDigest); err != nil {
		return err
	}
	return b.preload(ctx, binding.Handle)
}

func (b *Backend) Unload(
	ctx context.Context,
	binding modelactivation.Binding,
) error {
	if err := b.lock(ctx); err != nil {
		return err
	}
	defer b.unlock()
	if err := b.validateBinding(binding); err != nil {
		return err
	}
	if err := b.unloadMemory(ctx, binding.Handle); err != nil {
		return err
	}
	return b.deleteModel(ctx, binding.Handle)
}

func (b *Backend) Inspect(
	ctx context.Context,
	slotID string,
) (modelactivation.RecoveryBindings, error) {
	if err := b.lock(ctx); err != nil {
		return modelactivation.RecoveryBindings{}, err
	}
	defer b.unlock()
	if slotID != b.slotID {
		return modelactivation.RecoveryBindings{},
			errors.New("Ollama activation slot mismatch")
	}
	data, status, err := b.request(ctx, http.MethodGet, b.tagsEndpoint, nil, "")
	if err != nil {
		return modelactivation.RecoveryBindings{}, err
	}
	if status != http.StatusOK {
		return modelactivation.RecoveryBindings{},
			errors.New("inspect Ollama activation bindings")
	}
	var envelope struct {
		Models json.RawMessage `json:"models"`
	}
	if err := decodeSingle(data, &envelope); err != nil {
		return modelactivation.RecoveryBindings{},
			errors.New("invalid Ollama activation inventory")
	}
	var inventory []struct {
		Name  string `json:"name"`
		Model string `json:"model"`
	}
	modelsJSON := bytes.TrimSpace(envelope.Models)
	if len(modelsJSON) == 0 || modelsJSON[0] != '[' ||
		decodeSingle(modelsJSON, &inventory) != nil ||
		len(inventory) > MaxInventoryModelsHard {
		return modelactivation.RecoveryBindings{},
			errors.New("invalid Ollama activation inventory")
	}
	prefix, _ := ollamabinding.RuntimePrefix(b.slotID)
	bindings := make([]modelactivation.Binding, 0, modelactivation.MaxRecoveryBindingsHard)
	seen := make(map[string]struct{}, modelactivation.MaxRecoveryBindingsHard)
	for _, candidate := range inventory {
		name := candidate.Name
		if name == "" {
			name = candidate.Model
		}
		ownedName := strings.HasPrefix(candidate.Name, prefix)
		ownedModel := strings.HasPrefix(candidate.Model, prefix)
		if !ownedName && !ownedModel {
			continue
		}
		if candidate.Name != "" && candidate.Model != "" &&
			candidate.Name != candidate.Model {
			return modelactivation.RecoveryBindings{},
				errors.New("ambiguous Ollama activation inventory")
		}
		digest, err := ollamabinding.ParseRuntimeModel(b.slotID, name)
		if err != nil {
			return modelactivation.RecoveryBindings{},
				errors.New("invalid owned Ollama runtime model")
		}
		if _, duplicate := seen[name]; duplicate {
			return modelactivation.RecoveryBindings{},
				errors.New("duplicate Ollama activation binding")
		}
		seen[name] = struct{}{}
		if len(bindings) >= modelactivation.MaxRecoveryBindingsHard {
			return modelactivation.RecoveryBindings{},
				errors.New("Ollama activation binding limit")
		}
		binding, err := b.binding(digest)
		if err != nil {
			return modelactivation.RecoveryBindings{}, err
		}
		exists, err := b.show(ctx, binding)
		if err != nil || !exists {
			return modelactivation.RecoveryBindings{},
				errors.New("verify inspected Ollama binding")
		}
		bindings = append(bindings, binding)
	}
	sort.Slice(bindings, func(left, right int) bool {
		return bindings[left].Handle < bindings[right].Handle
	})
	result := modelactivation.RecoveryBindings{Count: len(bindings)}
	copy(result.Bindings[:], bindings)
	return result, nil
}

func (b *Backend) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		if b.client != nil {
			b.client.CloseIdleConnections()
		}
	})
	return nil
}

func (b *Backend) validateLoad(request modelactivation.LoadRequest) error {
	if request.SlotID != b.slotID || request.Model != b.model ||
		!ollamabinding.ValidDigest(request.ModelDigest) ||
		request.SizeBytes == 0 || request.Artifact == nil {
		return errors.New("invalid Ollama activation load request")
	}
	return nil
}

func (b *Backend) binding(
	digest string,
) (modelactivation.Binding, error) {
	handle, err := ollamabinding.RuntimeModel(b.slotID, digest)
	if err != nil {
		return modelactivation.Binding{}, errors.New(
			"invalid Ollama activation digest",
		)
	}
	return modelactivation.Binding{
		SlotID: b.slotID, Model: b.model, ModelDigest: digest,
		Runtime: "ollama", RuntimeRevision: RuntimeRevision, Handle: handle,
		DigestEvidence: airuntime.BindingLocallyObserved,
	}, nil
}

func (b *Backend) validateBinding(binding modelactivation.Binding) error {
	expected, err := b.binding(binding.ModelDigest)
	if err != nil || binding != expected {
		return errors.New("invalid Ollama activation binding")
	}
	return nil
}

func (b *Backend) ensureBlob(
	ctx context.Context,
	request modelactivation.LoadRequest,
) error {
	status, err := b.headBlobStatus(ctx, request.ModelDigest)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		return nil
	}
	if status != http.StatusNotFound {
		return errors.New("check Ollama model blob")
	}
	reader := io.NewSectionReader(
		request.Artifact, 0, int64(request.SizeBytes),
	)
	httpRequest, err := http.NewRequestWithContext(
		ctx, http.MethodPost, b.blobEndpoint+request.ModelDigest, reader,
	)
	if err != nil {
		return errors.New("create Ollama blob request")
	}
	httpRequest.ContentLength = int64(request.SizeBytes)
	httpRequest.Header.Set("Content-Type", "application/octet-stream")
	response, err := b.client.Do(httpRequest)
	if err != nil {
		return transportError(ctx)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	closeErr := response.Body.Close()
	if closeErr != nil ||
		(response.StatusCode != http.StatusCreated &&
			response.StatusCode != http.StatusOK) {
		return errors.New("upload approved Ollama blob")
	}
	return b.headBlob(ctx, request.ModelDigest)
}

func (b *Backend) headBlob(ctx context.Context, digest string) error {
	status, err := b.headBlobStatus(ctx, digest)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("approved Ollama blob is unavailable")
	}
	return nil
}

func (b *Backend) headBlobStatus(
	ctx context.Context,
	digest string,
) (int, error) {
	request, err := http.NewRequestWithContext(
		ctx, http.MethodHead, b.blobEndpoint+digest, nil,
	)
	if err != nil {
		return 0, errors.New("create Ollama blob check")
	}
	response, err := b.client.Do(request)
	if err != nil {
		return 0, transportError(ctx)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	closeErr := response.Body.Close()
	if closeErr != nil {
		return 0, errors.New("close Ollama blob check")
	}
	return response.StatusCode, nil
}

func (b *Backend) createModel(
	ctx context.Context,
	binding modelactivation.Binding,
) error {
	body, err := json.Marshal(struct {
		Model  string            `json:"model"`
		Files  map[string]string `json:"files"`
		Stream bool              `json:"stream"`
	}{
		Model: binding.Handle,
		Files: map[string]string{artifactFilename: binding.ModelDigest},
	})
	if err != nil {
		return errors.New("encode Ollama model creation")
	}
	data, status, err := b.request(
		ctx, http.MethodPost, b.createEndpoint, body, "application/json",
	)
	if err != nil {
		return err
	}
	if status != http.StatusOK || !successStatus(data) {
		return errors.New("create approved Ollama model")
	}
	return nil
}

func (b *Backend) show(
	ctx context.Context,
	binding modelactivation.Binding,
) (bool, error) {
	body, err := json.Marshal(struct {
		Model   string `json:"model"`
		Verbose bool   `json:"verbose"`
	}{Model: binding.Handle})
	if err != nil {
		return false, errors.New("encode Ollama model inspection")
	}
	data, status, err := b.request(
		ctx, http.MethodPost, b.showEndpoint, body, "application/json",
	)
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound {
		return false, nil
	}
	if status != http.StatusOK {
		return false, errors.New("inspect approved Ollama model")
	}
	if err := ollamabinding.VerifyShowResponse(
		data, binding.ModelDigest,
	); err != nil {
		return false, errors.New("verify approved Ollama model source")
	}
	return true, nil
}

func (b *Backend) preload(ctx context.Context, handle string) error {
	body, err := json.Marshal(struct {
		Model     string `json:"model"`
		Prompt    string `json:"prompt"`
		Stream    bool   `json:"stream"`
		KeepAlive string `json:"keep_alive"`
	}{
		Model: handle, KeepAlive: "5m",
	})
	if err != nil {
		return errors.New("encode Ollama model preload")
	}
	data, status, err := b.request(
		ctx, http.MethodPost, b.generateEndpoint, body, "application/json",
	)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("preload approved Ollama model")
	}
	var result struct {
		Model string `json:"model"`
		Done  bool   `json:"done"`
	}
	if err := decodeSingle(data, &result); err != nil || !result.Done ||
		(result.Model != "" && result.Model != handle) {
		return errors.New("invalid Ollama preload response")
	}
	return nil
}

func (b *Backend) unloadMemory(ctx context.Context, handle string) error {
	body, err := json.Marshal(struct {
		Model     string `json:"model"`
		Prompt    string `json:"prompt"`
		Stream    bool   `json:"stream"`
		KeepAlive int    `json:"keep_alive"`
	}{
		Model: handle,
	})
	if err != nil {
		return errors.New("encode Ollama model unload")
	}
	data, status, err := b.request(
		ctx, http.MethodPost, b.generateEndpoint, body, "application/json",
	)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status != http.StatusOK {
		return errors.New("unload Ollama model memory")
	}
	var result struct {
		Done bool `json:"done"`
	}
	if err := decodeSingle(data, &result); err != nil || !result.Done {
		return errors.New("invalid Ollama unload response")
	}
	return nil
}

func (b *Backend) deleteModel(ctx context.Context, handle string) error {
	body, err := json.Marshal(struct {
		Model string `json:"model"`
	}{Model: handle})
	if err != nil {
		return errors.New("encode Ollama model deletion")
	}
	_, status, err := b.request(
		ctx, http.MethodDelete, b.deleteEndpoint, body, "application/json",
	)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return errors.New("delete Ollama activation model")
	}
	return nil
}

func (b *Backend) request(
	ctx context.Context,
	method string,
	endpoint string,
	body []byte,
	contentType string,
) ([]byte, int, error) {
	request, err := http.NewRequestWithContext(
		ctx, method, endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return nil, 0, errors.New("create Ollama activation request")
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := b.client.Do(request)
	if err != nil {
		return nil, 0, transportError(ctx)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(
		response.Body, int64(b.maxResponse)+1,
	))
	if err != nil {
		return nil, 0, transportError(ctx)
	}
	if uint64(len(data)) > b.maxResponse {
		return nil, 0, errors.New("Ollama activation response limit")
	}
	return data, response.StatusCode, nil
}

func (b *Backend) lock(ctx context.Context) error {
	if b == nil || ctx == nil || b.client == nil || b.gate == nil {
		return errors.New("invalid Ollama activation backend")
	}
	if b.closed.Load() {
		return errors.New("Ollama activation backend is closed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.gate:
	}
	if b.closed.Load() {
		b.gate <- struct{}{}
		return errors.New("Ollama activation backend is closed")
	}
	return nil
}

func (b *Backend) unlock() {
	b.gate <- struct{}{}
}

func decodeSingle(data []byte, target any) error {
	if len(data) == 0 {
		return errors.New("empty JSON response")
	}
	if err := validateJSON(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON response")
	}
	return nil
}

func validateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	values := 0
	var consume func(int) error
	consume = func(depth int) error {
		if depth > maxJSONDepth {
			return errors.New("JSON depth limit")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		values++
		if values > maxJSONValues {
			return errors.New("JSON value limit")
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{}, 8)
			for decoder.More() {
				keyToken, err := decoder.Token()
				key, valid := keyToken.(string)
				if err != nil || !valid || len(key) == 0 || len(key) > 256 {
					return errors.New("invalid JSON object key")
				}
				if _, duplicate := seen[key]; duplicate {
					return errors.New("duplicate JSON object field")
				}
				if len(seen) >= maxJSONFields {
					return errors.New("JSON object field limit")
				}
				seen[key] = struct{}{}
				if err := consume(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid JSON object")
			}
		case '[':
			items := 0
			for decoder.More() {
				items++
				if items > maxJSONArrayItems {
					return errors.New("JSON array item limit")
				}
				if err := consume(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid JSON array")
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
		return nil
	}
	if err := consume(1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON response")
	}
	return nil
}

func successStatus(data []byte) bool {
	var result struct {
		Status string `json:"status"`
	}
	return decodeSingle(data, &result) == nil && result.Status == "success"
}

func transportError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("Ollama activation transport failure")
}

func validIdentity(value string) bool {
	return value != "" && len(value) <= modelactivation.MaxIdentityBytes &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}
