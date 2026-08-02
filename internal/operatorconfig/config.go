// Package operatorconfig loads the bounded, administrator-owned runtime
// configuration used by tos-ai-worker. It deliberately does not accept
// endpoint, credential, or model overrides from invocation payloads.
package operatorconfig

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
	"github.com/tosnetwork/tos-ai/pkg/adapters/ollama"
	"github.com/tosnetwork/tos-ai/pkg/adapters/openai"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/modelactivation"
	activationollama "github.com/tosnetwork/tos-ai/pkg/modelactivation/ollama"
	"github.com/tosnetwork/tos-ai/pkg/ollamabinding"
	"github.com/tosnetwork/tos-ai/pkg/profile/textgeneration"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
)

const (
	Version                  = 2
	previousVersion          = 1
	MaxConfigBytes           = int64(1 << 20)
	MaxAdapters              = 64
	MaxJSONDepth             = 16
	MaxInputBytes            = uint64(1 << 20)
	MaxOutputBytes           = uint64(1 << 20)
	MaxRequestBytes          = uint64(8 << 20)
	MaxResponseBytes         = uint64(8 << 20)
	MaxConnectionsPerAdapter = 32
	MaxConnectionsTotal      = 256
	activationConnections    = 2
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
	defaultActivationTimeout = int64((5 * time.Minute) / time.Millisecond)
	defaultCleanupTimeout    = int64((30 * time.Second) / time.Millisecond)
	defaultMaxModelBytes     = uint64(64 << 30)
	maxTLSPrivateKeyBytes    = int64(256 << 10)
)

type fileConfig struct {
	Version    int               `json:"version"`
	Activation *activationConfig `json:"activation,omitempty"`
	Adapters   []adapterConfig   `json:"adapters"`
}

type activationConfig struct {
	StateDir               string `json:"stateDir"`
	OperationTimeoutMillis int64  `json:"operationTimeoutMillis,omitempty"`
	CleanupTimeoutMillis   int64  `json:"cleanupTimeoutMillis,omitempty"`
}

type adapterConfig struct {
	Type                   string          `json:"type"`
	BaseURL                string          `json:"baseUrl"`
	APIKeyFile             string          `json:"apiKeyFile,omitempty"`
	Model                  string          `json:"model"`
	ModelDigest            string          `json:"modelDigest"`
	RuntimeRevision        string          `json:"runtimeRevision,omitempty"`
	MaxInputBytes          uint64          `json:"maxInputBytes,omitempty"`
	MaxOutputBytes         uint64          `json:"maxOutputBytes,omitempty"`
	MaxRequestBytes        uint64          `json:"maxRequestBytes,omitempty"`
	MaxResponseBytes       uint64          `json:"maxResponseBytes,omitempty"`
	MaxConnections         int             `json:"maxConnections,omitempty"`
	MaxResponseHeaderBytes int64           `json:"maxResponseHeaderBytes,omitempty"`
	TimeoutMillis          int64           `json:"timeoutMillis,omitempty"`
	ConnectTimeoutMillis   int64           `json:"connectTimeoutMillis,omitempty"`
	AllowedPlaintextCIDRs  []string        `json:"allowedPlaintextCidrs,omitempty"`
	TLS                    *runtimeTLSFile `json:"tls,omitempty"`
	Admission              resourceConfig  `json:"admission,omitempty"`
	Activation             *slotConfig     `json:"activation,omitempty"`
}

type runtimeTLSFile struct {
	CAFile         string `json:"caFile,omitempty"`
	ClientCertFile string `json:"clientCertFile,omitempty"`
	ClientKeyFile  string `json:"clientKeyFile,omitempty"`
	ServerName     string `json:"serverName,omitempty"`
}

type runtimeTLSMaterial struct {
	rootCAs           *x509.CertPool
	clientCertificate *tls.Certificate
	serverName        string
}

type slotConfig struct {
	SlotID        string `json:"slotId"`
	MaxModelBytes uint64 `json:"maxModelBytes,omitempty"`
}

type resourceConfig struct {
	RAMBytes        uint64 `json:"ramBytes,omitempty"`
	VRAMBytes       uint64 `json:"vramBytes,omitempty"`
	KVCacheBytes    uint64 `json:"kvCacheBytes,omitempty"`
	ContextTokens   uint64 `json:"contextTokens,omitempty"`
	BatchSize       uint32 `json:"batchSize,omitempty"`
	ExecutionMillis int64  `json:"executionMillis,omitempty"`
}

type DesiredActivation struct {
	SlotID string
	Digest string
}

type ActivationConfiguration struct {
	Controller modelactivation.Config
	Desired    []DesiredActivation
	closers    []interface{ Close() error }
}

type Configuration struct {
	Adapters            []airuntime.Adapter
	Activation          *ActivationConfiguration
	profileCapabilities []airuntime.Capability
}

// TextGenerationProfilePlan constructs the exact immutable Edge deployment
// plan supported by this loaded runtime configuration. The plan binds the
// installed mapper to the one selector this deployment intends to advertise;
// it does not expose the Worker, start a listener, or grant authority.
func (c *Configuration) TextGenerationProfilePlan() (
	*edge.ProfileInvocationPlan,
	error,
) {
	if c == nil || len(c.profileCapabilities) == 0 ||
		len(c.profileCapabilities) > MaxAdapters {
		return nil, errors.New("invalid runtime configuration for profile routing")
	}
	return TextGenerationProfilePlanForCapabilities(c.profileCapabilities)
}

// TextGenerationProfilePlanForCapabilities constructs the same immutable Edge
// routing plan for an explicitly composed runtime, including the isolated
// containerd adapter. Invocation payloads cannot add routes to this plan.
func TextGenerationProfilePlanForCapabilities(
	capabilities []airuntime.Capability,
) (*edge.ProfileInvocationPlan, error) {
	if len(capabilities) == 0 || len(capabilities) > MaxAdapters {
		return nil, errors.New("invalid capabilities for profile routing")
	}
	mapper, err := textgeneration.NewMapperFromCapabilities(
		cloneCapabilities(capabilities),
	)
	if err != nil {
		return nil, err
	}
	registration, err := mapper.Registration()
	if err != nil {
		return nil, err
	}
	plan, err := edge.NewProfileInvocationPlan(
		[]edge.ProfileInvocationRegistration{registration},
		[]edge.ProfileInvocationRequirement{{
			ProfileID:      textgeneration.ProfileID,
			ProfileVersion: textgeneration.ProfileVersion,
			Operation:      textgeneration.Operation,
		}},
	)
	if err != nil {
		return nil, err
	}
	if !plan.Supports(
		textgeneration.ProfileID,
		textgeneration.ProfileVersion,
		nil,
		textgeneration.Operation,
	) {
		return nil, errors.New("text-generation profile mapper is not enabled")
	}
	return plan, nil
}

func (c *Configuration) CloseBackends() error {
	if c == nil || c.Activation == nil {
		return nil
	}
	var closeFailed bool
	for _, closer := range c.Activation.closers {
		if err := closer.Close(); err != nil {
			closeFailed = true
		}
	}
	if closeFailed {
		return errors.New("close activation backends")
	}
	return nil
}

// Load reads a private regular JSON file and constructs only the runtime
// adapters explicitly approved in that file.
func Load(path string) (Configuration, error) {
	data, err := readPrivateFile(path, MaxConfigBytes, false)
	if err != nil {
		return Configuration{}, errors.New("load operator runtime configuration")
	}
	if err := validateJSON(data); err != nil {
		return Configuration{}, errors.New("invalid operator runtime configuration")
	}
	var config fileConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Configuration{}, errors.New("invalid operator runtime configuration")
	}
	if (config.Version != Version && config.Version != previousVersion) ||
		len(config.Adapters) == 0 ||
		len(config.Adapters) > MaxAdapters {
		return Configuration{},
			errors.New("operator runtime configuration exceeds hard limits")
	}
	if err := applyActivationDefaults(config.Activation); err != nil {
		return Configuration{}, err
	}
	adapters := make([]airuntime.Adapter, 0, len(config.Adapters))
	slots := make([]modelactivation.Slot, 0, len(config.Adapters))
	desired := make([]DesiredActivation, 0, len(config.Adapters))
	closers := make([]interface{ Close() error }, 0, len(config.Adapters))
	seenSlots := make(map[string]struct{}, len(config.Adapters))
	seenDigests := make(map[string]struct{}, len(config.Adapters))
	totalConnections := 0
	for index, value := range config.Adapters {
		if config.Version == previousVersion && value.TLS != nil {
			closeAdapters(adapters)
			closeBackendClosers(closers)
			return Configuration{}, errors.New(
				"runtime TLS identity requires configuration version 2",
			)
		}
		applyDefaults(&value)
		connections := value.MaxConnections
		if value.Activation != nil {
			connections += activationConnections
		}
		if value.MaxConnections > MaxConnectionsPerAdapter ||
			connections <= 0 ||
			totalConnections > MaxConnectionsTotal-connections {
			closeAdapters(adapters)
			closeBackendClosers(closers)
			return Configuration{},
				errors.New("operator runtime configuration exceeds connection limits")
		}
		adapter, activated, err := buildAdapter(value, config.Activation)
		if err != nil {
			closeAdapters(adapters)
			closeBackendClosers(closers)
			return Configuration{},
				fmt.Errorf("runtime adapter %d is invalid", index)
		}
		totalConnections += connections
		adapters = append(adapters, adapter)
		if activated == nil {
			continue
		}
		if _, duplicate := seenSlots[activated.desired.SlotID]; duplicate {
			closeAdapters(adapters)
			_ = activated.closer.Close()
			closeBackendClosers(closers)
			return Configuration{}, errors.New("duplicate activation slot")
		}
		if _, duplicate := seenDigests[activated.desired.Digest]; duplicate {
			closeAdapters(adapters)
			_ = activated.closer.Close()
			closeBackendClosers(closers)
			return Configuration{}, errors.New("duplicate activation digest")
		}
		seenSlots[activated.desired.SlotID] = struct{}{}
		seenDigests[activated.desired.Digest] = struct{}{}
		slots = append(slots, activated.slot)
		desired = append(desired, activated.desired)
		closers = append(closers, activated.closer)
	}
	if config.Activation != nil && len(slots) == 0 {
		closeAdapters(adapters)
		return Configuration{},
			errors.New("activation configuration has no slots")
	}
	profileCapabilities := make(
		[]airuntime.Capability,
		0,
		len(adapters),
	)
	for _, adapter := range adapters {
		profileCapabilities = append(
			profileCapabilities,
			cloneCapability(adapter.Capability()),
		)
	}
	result := Configuration{
		Adapters: adapters, profileCapabilities: profileCapabilities,
	}
	if len(slots) > 0 {
		result.Activation = &ActivationConfiguration{
			Controller: modelactivation.Config{
				StateDir: config.Activation.StateDir,
				OperationTimeout: time.Duration(
					config.Activation.OperationTimeoutMillis,
				) * time.Millisecond,
				CleanupTimeout: time.Duration(
					config.Activation.CleanupTimeoutMillis,
				) * time.Millisecond,
				Slots: slots,
			},
			Desired: desired,
			closers: closers,
		}
	}
	return result, nil
}

func cloneCapabilities(
	capabilities []airuntime.Capability,
) []airuntime.Capability {
	output := make([]airuntime.Capability, len(capabilities))
	for index, capability := range capabilities {
		output[index] = cloneCapability(capability)
	}
	return output
}

func cloneCapability(capability airuntime.Capability) airuntime.Capability {
	capability.AcceptedPriorities = append(
		[]airuntime.Priority(nil),
		capability.AcceptedPriorities...,
	)
	return capability
}

func closeAdapters(adapters []airuntime.Adapter) {
	for _, adapter := range adapters {
		if closer, ok := adapter.(airuntime.AdapterCloser); ok {
			_ = closer.Close()
		}
	}
}

func closeBackendClosers(closers []interface{ Close() error }) {
	for _, closer := range closers {
		_ = closer.Close()
	}
}

type activatedAdapter struct {
	slot    modelactivation.Slot
	desired DesiredActivation
	closer  interface{ Close() error }
}

func buildAdapter(
	config adapterConfig,
	activation *activationConfig,
) (airuntime.Adapter, *activatedAdapter, error) {
	applyDefaults(&config)
	if config.MaxInputBytes > MaxInputBytes || config.MaxOutputBytes > MaxOutputBytes ||
		config.MaxRequestBytes > MaxRequestBytes || config.MaxResponseBytes > MaxResponseBytes {
		return nil, nil, errors.New("runtime body bound exceeds worker limit")
	}
	if config.TimeoutMillis <= 0 ||
		config.TimeoutMillis > int64(time.Hour/time.Millisecond) ||
		config.ConnectTimeoutMillis <= 0 ||
		config.ConnectTimeoutMillis > int64(time.Minute/time.Millisecond) ||
		config.Admission.ExecutionMillis <= 0 ||
		config.Admission.ExecutionMillis > int64(time.Hour/time.Millisecond) ||
		config.TimeoutMillis > config.Admission.ExecutionMillis {
		return nil, nil, errors.New("invalid duration")
	}
	timeout := time.Duration(config.TimeoutMillis) * time.Millisecond
	connectTimeout := time.Duration(config.ConnectTimeoutMillis) * time.Millisecond
	executionTime := time.Duration(config.Admission.ExecutionMillis) * time.Millisecond
	tlsMaterial, err := loadRuntimeTLS(config.TLS)
	if err != nil {
		return nil, nil, errors.New("invalid runtime TLS identity")
	}
	resources := admission.Resources{
		RAMBytes: config.Admission.RAMBytes, VRAMBytes: config.Admission.VRAMBytes,
		KVCacheBytes: config.Admission.KVCacheBytes, ContextTokens: config.Admission.ContextTokens,
		BatchSize: config.Admission.BatchSize, OutputBytes: config.MaxOutputBytes,
		ExecutionTime: executionTime,
	}
	switch config.Type {
	case "ollama":
		if config.APIKeyFile != "" || config.RuntimeRevision != "" {
			return nil, nil, errors.New("unsupported Ollama option")
		}
		runtimeModel := ""
		sourceDigest := ""
		if config.Activation != nil {
			if activation == nil {
				return nil, nil, errors.New(
					"activation slot requires global activation configuration",
				)
			}
			if config.Activation.MaxModelBytes == 0 {
				config.Activation.MaxModelBytes = defaultMaxModelBytes
			}
			if !ollamabinding.ValidSlotID(config.Activation.SlotID) ||
				config.Activation.MaxModelBytes == 0 ||
				config.Activation.MaxModelBytes >
					modelactivation.MaxModelBytesHard {
				return nil, nil, errors.New("invalid Ollama activation slot")
			}
			var err error
			runtimeModel, err = ollamabinding.RuntimeModel(
				config.Activation.SlotID, config.ModelDigest,
			)
			if err != nil {
				return nil, nil, errors.New("invalid activated model digest")
			}
			sourceDigest = config.ModelDigest
		}
		adapter, err := ollama.New(ollama.Config{
			BaseURL: config.BaseURL, Model: config.Model, ModelDigest: config.ModelDigest,
			RuntimeModel: runtimeModel, SourceDigest: sourceDigest,
			MaxInputBytes: config.MaxInputBytes, MaxOutputBytes: config.MaxOutputBytes,
			MaxRequestBytes: config.MaxRequestBytes, MaxResponseBytes: config.MaxResponseBytes,
			MaxConnections:         config.MaxConnections,
			MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
			Timeout:                timeout, ConnectTimeout: connectTimeout,
			AllowedPlaintextCIDRs: config.AllowedPlaintextCIDRs, Admission: resources,
			RootCAs:           tlsMaterial.rootCAs,
			ClientCertificate: tlsMaterial.clientCertificate,
			TLSServerName:     tlsMaterial.serverName,
		})
		if err != nil || config.Activation == nil {
			return adapter, nil, err
		}
		backend, err := activationollama.New(activationollama.Config{
			BaseURL: config.BaseURL, SlotID: config.Activation.SlotID,
			Model: config.Model,
			Timeout: time.Duration(
				activation.OperationTimeoutMillis,
			) * time.Millisecond,
			ConnectTimeout: connectTimeout,
			CleanupTimeout: time.Duration(
				activation.CleanupTimeoutMillis,
			) * time.Millisecond,
			MaxConnections:         activationConnections,
			MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
			MaxResponseBytes:       activationollama.MaxResponseBytesHard,
			AllowedPlaintextCIDRs:  config.AllowedPlaintextCIDRs,
			RootCAs:                tlsMaterial.rootCAs,
			ClientCertificate:      tlsMaterial.clientCertificate,
			TLSServerName:          tlsMaterial.serverName,
		})
		if err != nil {
			_ = adapter.Close()
			return nil, nil, err
		}
		return adapter, &activatedAdapter{
			slot: modelactivation.Slot{
				Policy: modelactivation.SlotPolicy{
					ID: config.Activation.SlotID, Model: config.Model,
					Runtime:       "ollama",
					MaxModelBytes: config.Activation.MaxModelBytes,
				},
				Backend: backend,
			},
			desired: DesiredActivation{
				SlotID: config.Activation.SlotID,
				Digest: config.ModelDigest,
			},
			closer: backend,
		}, nil
	case "openai-compatible", "vllm", "llama.cpp", "localai":
		if config.Activation != nil {
			return nil, nil, errors.New(
				"OpenAI-compatible activation is unavailable",
			)
		}
		var apiKey string
		if config.APIKeyFile != "" {
			value, err := readPrivateFile(config.APIKeyFile, openai.MaxCredentialBytes, true)
			if err != nil {
				return nil, nil, errors.New("load runtime credential")
			}
			apiKey = string(value)
		}
		adapter, err := openai.New(openai.Config{
			BaseURL: config.BaseURL, APIKey: apiKey, Model: config.Model,
			ModelDigest: config.ModelDigest, RuntimeRevision: config.RuntimeRevision,
			Runtime:       config.Type,
			MaxInputBytes: config.MaxInputBytes, MaxOutputBytes: config.MaxOutputBytes,
			MaxRequestBytes: config.MaxRequestBytes, MaxResponseBytes: config.MaxResponseBytes,
			MaxConnections:         config.MaxConnections,
			MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
			Timeout:                timeout, ConnectTimeout: connectTimeout,
			AllowedPlaintextCIDRs: config.AllowedPlaintextCIDRs, Admission: resources,
			RootCAs:           tlsMaterial.rootCAs,
			ClientCertificate: tlsMaterial.clientCertificate,
			TLSServerName:     tlsMaterial.serverName,
		})
		return adapter, nil, err
	default:
		return nil, nil, errors.New("unsupported runtime adapter")
	}
}

func loadRuntimeTLS(config *runtimeTLSFile) (runtimeTLSMaterial, error) {
	if config == nil {
		return runtimeTLSMaterial{}, nil
	}
	hasClientCert := config.ClientCertFile != ""
	hasClientKey := config.ClientKeyFile != ""
	if (config.CAFile == "" && !hasClientCert && !hasClientKey &&
		config.ServerName == "") || hasClientCert != hasClientKey {
		return runtimeTLSMaterial{}, errors.New("incomplete runtime TLS identity")
	}
	result := runtimeTLSMaterial{serverName: config.ServerName}
	if config.CAFile != "" {
		data, err := readPrivateFile(
			config.CAFile, int64(runtimehttp.MaxTLSMaterialBytes), false,
		)
		if err != nil {
			return runtimeTLSMaterial{}, errors.New("load runtime TLS roots")
		}
		roots, parseErr := parseRootCertificates(data)
		clear(data)
		if parseErr != nil {
			return runtimeTLSMaterial{}, errors.New("invalid runtime TLS roots")
		}
		result.rootCAs = roots
	}
	if hasClientCert {
		certificatePEM, err := readPrivateFile(
			config.ClientCertFile,
			int64(runtimehttp.MaxTLSMaterialBytes), false,
		)
		if err != nil {
			return runtimeTLSMaterial{}, errors.New(
				"load runtime TLS client certificate",
			)
		}
		defer clear(certificatePEM)
		if err := validateClientCertificatePEM(certificatePEM); err != nil {
			return runtimeTLSMaterial{}, err
		}
		keyPEM, err := readPrivateFile(
			config.ClientKeyFile, maxTLSPrivateKeyBytes, false,
		)
		if err != nil {
			return runtimeTLSMaterial{}, errors.New(
				"load runtime TLS client key",
			)
		}
		defer clear(keyPEM)
		if err := validatePrivateKeyPEM(keyPEM); err != nil {
			return runtimeTLSMaterial{}, err
		}
		certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
		if err != nil {
			return runtimeTLSMaterial{}, errors.New(
				"invalid runtime TLS client identity",
			)
		}
		result.clientCertificate = &certificate
	}
	return result, nil
}

func parseRootCertificates(data []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	seen := make(map[[sha256.Size]byte]struct{}, runtimehttp.MaxTLSRootsHard)
	remaining := data
	for count := 0; ; count++ {
		remaining = bytes.TrimSpace(remaining)
		if len(remaining) == 0 {
			if count == 0 {
				return nil, errors.New("empty runtime TLS roots")
			}
			return pool, nil
		}
		if count >= runtimehttp.MaxTLSRootsHard ||
			!bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errors.New("runtime TLS roots exceed hard limits")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errors.New("invalid runtime TLS root PEM")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.BasicConstraintsValid || !certificate.IsCA {
			return nil, errors.New("invalid runtime TLS root certificate")
		}
		digest := sha256.Sum256(certificate.Raw)
		if _, duplicate := seen[digest]; duplicate {
			return nil, errors.New("duplicate runtime TLS root certificate")
		}
		seen[digest] = struct{}{}
		pool.AddCert(certificate)
		remaining = rest
	}
}

func validateClientCertificatePEM(data []byte) error {
	remaining := data
	for count := 0; ; count++ {
		remaining = bytes.TrimSpace(remaining)
		if len(remaining) == 0 {
			if count == 0 {
				return errors.New("empty runtime TLS client certificate")
			}
			return nil
		}
		if count >= runtimehttp.MaxTLSChainHard ||
			!bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return errors.New("runtime TLS client chain exceeds hard limits")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return errors.New("invalid runtime TLS client certificate PEM")
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return errors.New("invalid runtime TLS client certificate")
		}
		remaining = rest
	}
}

func validatePrivateKeyPEM(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if !bytes.HasPrefix(trimmed, []byte("-----BEGIN ")) {
		return errors.New("invalid runtime TLS client key PEM")
	}
	block, rest := pem.Decode(trimmed)
	if block == nil {
		return errors.New("invalid runtime TLS client key PEM")
	}
	defer clear(block.Bytes)
	validType := block.Type == "PRIVATE KEY" ||
		block.Type == "RSA PRIVATE KEY" || block.Type == "EC PRIVATE KEY"
	if len(block.Headers) != 0 || !validType || len(block.Bytes) == 0 ||
		len(bytes.TrimSpace(rest)) != 0 {
		return errors.New("invalid runtime TLS client key PEM")
	}
	return nil
}

func applyActivationDefaults(config *activationConfig) error {
	if config == nil {
		return nil
	}
	if config.OperationTimeoutMillis == 0 {
		config.OperationTimeoutMillis = defaultActivationTimeout
	}
	if config.CleanupTimeoutMillis == 0 {
		config.CleanupTimeoutMillis = defaultCleanupTimeout
	}
	if !filepath.IsAbs(config.StateDir) ||
		config.OperationTimeoutMillis <= 0 ||
		config.OperationTimeoutMillis >
			int64(modelactivation.MaxOperationTimeout/time.Millisecond) ||
		config.CleanupTimeoutMillis <= 0 ||
		config.CleanupTimeoutMillis >
			int64(modelactivation.MaxCleanupTimeoutHard/time.Millisecond) {
		return errors.New("invalid activation configuration")
	}
	return nil
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
