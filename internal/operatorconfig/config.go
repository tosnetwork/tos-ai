// Package operatorconfig loads the bounded, administrator-owned runtime
// configuration used by tos-ai-worker. It deliberately does not accept
// endpoint, credential, or model overrides from invocation payloads.
package operatorconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/adapters/ollama"
	"github.com/tosnetwork/tos-ai/pkg/adapters/openai"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

const (
	Version                  = 1
	MaxConfigBytes           = int64(1 << 20)
	MaxAdapters              = 64
	MaxJSONDepth             = 16
	MaxInputBytes            = uint64(1 << 20)
	MaxOutputBytes           = uint64(1 << 20)
	MaxRequestBytes          = uint64(8 << 20)
	MaxResponseBytes         = uint64(8 << 20)
	MaxConnectionsPerAdapter = 32
	MaxConnectionsTotal      = 256
	defaultMaxInputBytes     = uint64(1 << 20)
	defaultMaxOutputBytes    = uint64(1 << 20)
	defaultMaxRequestBytes   = uint64(8 << 20)
	defaultMaxResponseBytes  = uint64(8 << 20)
	defaultMaxConnections    = 8
	defaultMaxHeaderBytes    = int64(16 << 10)
	defaultTimeoutMillis     = int64((2 * time.Minute) / time.Millisecond)
	defaultConnectMillis     = int64((5 * time.Second) / time.Millisecond)
	defaultRAMBytes          = uint64(1 << 30)
	defaultContextTokens     = uint64(8192)
	defaultBatchSize         = uint32(1)
)

type fileConfig struct {
	Version  int             `json:"version"`
	Adapters []adapterConfig `json:"adapters"`
}

type adapterConfig struct {
	Type                   string         `json:"type"`
	BaseURL                string         `json:"baseUrl"`
	APIKeyFile             string         `json:"apiKeyFile,omitempty"`
	Model                  string         `json:"model"`
	ModelDigest            string         `json:"modelDigest"`
	RuntimeRevision        string         `json:"runtimeRevision,omitempty"`
	MaxInputBytes          uint64         `json:"maxInputBytes,omitempty"`
	MaxOutputBytes         uint64         `json:"maxOutputBytes,omitempty"`
	MaxRequestBytes        uint64         `json:"maxRequestBytes,omitempty"`
	MaxResponseBytes       uint64         `json:"maxResponseBytes,omitempty"`
	MaxConnections         int            `json:"maxConnections,omitempty"`
	MaxResponseHeaderBytes int64          `json:"maxResponseHeaderBytes,omitempty"`
	TimeoutMillis          int64          `json:"timeoutMillis,omitempty"`
	ConnectTimeoutMillis   int64          `json:"connectTimeoutMillis,omitempty"`
	AllowedPlaintextCIDRs  []string       `json:"allowedPlaintextCidrs,omitempty"`
	Admission              resourceConfig `json:"admission,omitempty"`
}

type resourceConfig struct {
	RAMBytes        uint64 `json:"ramBytes,omitempty"`
	VRAMBytes       uint64 `json:"vramBytes,omitempty"`
	KVCacheBytes    uint64 `json:"kvCacheBytes,omitempty"`
	ContextTokens   uint64 `json:"contextTokens,omitempty"`
	BatchSize       uint32 `json:"batchSize,omitempty"`
	ExecutionMillis int64  `json:"executionMillis,omitempty"`
}

// Load reads a private regular JSON file and constructs only the runtime
// adapters explicitly approved in that file.
func Load(path string) ([]airuntime.Adapter, error) {
	data, err := readPrivateFile(path, MaxConfigBytes, false)
	if err != nil {
		return nil, errors.New("load operator runtime configuration")
	}
	if err := validateJSON(data); err != nil {
		return nil, errors.New("invalid operator runtime configuration")
	}
	var config fileConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, errors.New("invalid operator runtime configuration")
	}
	if config.Version != Version || len(config.Adapters) == 0 ||
		len(config.Adapters) > MaxAdapters {
		return nil, errors.New("operator runtime configuration exceeds hard limits")
	}
	adapters := make([]airuntime.Adapter, 0, len(config.Adapters))
	totalConnections := 0
	for index, value := range config.Adapters {
		applyDefaults(&value)
		if value.MaxConnections > MaxConnectionsPerAdapter ||
			totalConnections > MaxConnectionsTotal-value.MaxConnections {
			return nil, errors.New("operator runtime configuration exceeds connection limits")
		}
		adapter, err := buildAdapter(value)
		if err != nil {
			return nil, fmt.Errorf("runtime adapter %d is invalid", index)
		}
		totalConnections += value.MaxConnections
		adapters = append(adapters, adapter)
	}
	return adapters, nil
}

func buildAdapter(config adapterConfig) (airuntime.Adapter, error) {
	applyDefaults(&config)
	if config.MaxInputBytes > MaxInputBytes || config.MaxOutputBytes > MaxOutputBytes ||
		config.MaxRequestBytes > MaxRequestBytes || config.MaxResponseBytes > MaxResponseBytes {
		return nil, errors.New("runtime body bound exceeds worker limit")
	}
	if config.TimeoutMillis <= 0 ||
		config.TimeoutMillis > int64(time.Hour/time.Millisecond) ||
		config.ConnectTimeoutMillis <= 0 ||
		config.ConnectTimeoutMillis > int64(time.Minute/time.Millisecond) ||
		config.Admission.ExecutionMillis <= 0 ||
		config.Admission.ExecutionMillis > int64(time.Hour/time.Millisecond) ||
		config.TimeoutMillis > config.Admission.ExecutionMillis {
		return nil, errors.New("invalid duration")
	}
	timeout := time.Duration(config.TimeoutMillis) * time.Millisecond
	connectTimeout := time.Duration(config.ConnectTimeoutMillis) * time.Millisecond
	executionTime := time.Duration(config.Admission.ExecutionMillis) * time.Millisecond
	resources := admission.Resources{
		RAMBytes: config.Admission.RAMBytes, VRAMBytes: config.Admission.VRAMBytes,
		KVCacheBytes: config.Admission.KVCacheBytes, ContextTokens: config.Admission.ContextTokens,
		BatchSize: config.Admission.BatchSize, OutputBytes: config.MaxOutputBytes,
		ExecutionTime: executionTime,
	}
	switch config.Type {
	case "ollama":
		if config.APIKeyFile != "" || config.RuntimeRevision != "" {
			return nil, errors.New("unsupported Ollama option")
		}
		return ollama.New(ollama.Config{
			BaseURL: config.BaseURL, Model: config.Model, ModelDigest: config.ModelDigest,
			MaxInputBytes: config.MaxInputBytes, MaxOutputBytes: config.MaxOutputBytes,
			MaxRequestBytes: config.MaxRequestBytes, MaxResponseBytes: config.MaxResponseBytes,
			MaxConnections:         config.MaxConnections,
			MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
			Timeout:                timeout, ConnectTimeout: connectTimeout,
			AllowedPlaintextCIDRs: config.AllowedPlaintextCIDRs, Admission: resources,
		})
	case "openai-compatible":
		var apiKey string
		if config.APIKeyFile != "" {
			value, err := readPrivateFile(config.APIKeyFile, openai.MaxCredentialBytes, true)
			if err != nil {
				return nil, errors.New("load runtime credential")
			}
			apiKey = string(value)
		}
		return openai.New(openai.Config{
			BaseURL: config.BaseURL, APIKey: apiKey, Model: config.Model,
			ModelDigest: config.ModelDigest, RuntimeRevision: config.RuntimeRevision,
			MaxInputBytes: config.MaxInputBytes, MaxOutputBytes: config.MaxOutputBytes,
			MaxRequestBytes: config.MaxRequestBytes, MaxResponseBytes: config.MaxResponseBytes,
			MaxConnections:         config.MaxConnections,
			MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
			Timeout:                timeout, ConnectTimeout: connectTimeout,
			AllowedPlaintextCIDRs: config.AllowedPlaintextCIDRs, Admission: resources,
		})
	default:
		return nil, errors.New("unsupported runtime adapter")
	}
}

func applyDefaults(config *adapterConfig) {
	if config.MaxInputBytes == 0 {
		config.MaxInputBytes = defaultMaxInputBytes
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = defaultMaxOutputBytes
	}
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = defaultMaxRequestBytes
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponseBytes
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = defaultMaxConnections
	}
	if config.MaxResponseHeaderBytes == 0 {
		config.MaxResponseHeaderBytes = defaultMaxHeaderBytes
	}
	if config.TimeoutMillis == 0 {
		config.TimeoutMillis = defaultTimeoutMillis
	}
	if config.ConnectTimeoutMillis == 0 {
		config.ConnectTimeoutMillis = defaultConnectMillis
	}
	if config.Admission.RAMBytes == 0 {
		config.Admission.RAMBytes = defaultRAMBytes
	}
	if config.Admission.ContextTokens == 0 {
		config.Admission.ContextTokens = defaultContextTokens
	}
	if config.Admission.BatchSize == 0 {
		config.Admission.BatchSize = defaultBatchSize
	}
	if config.Admission.ExecutionMillis == 0 {
		config.Admission.ExecutionMillis = config.TimeoutMillis
	}
}

func readPrivateFile(path string, maximum int64, allowEmpty bool) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || maximum <= 0 {
		return nil, errors.New("invalid private file")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid private file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open private file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(pathInfo, info) ||
		!privateFileMode(info.Mode()) || info.Size() > maximum ||
		(!allowEmpty && info.Size() == 0) {
		return nil, errors.New("private file policy rejected")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("private file owner rejected")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(data)) > maximum || (!allowEmpty && len(data) == 0) {
		return nil, errors.New("read private file")
	}
	return data, nil
}

func privateFileMode(mode os.FileMode) bool {
	permissions := mode.Perm()
	return permissions&0o400 != 0 && permissions&0o177 == 0
}

func validateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > MaxJSONDepth {
		return errors.New("JSON nesting exceeds hard limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, exists := keys[key]; exists {
				return errors.New("duplicate JSON object key")
			}
			keys[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("invalid JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
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
