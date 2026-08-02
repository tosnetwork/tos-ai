package operatorconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"tags.cncf.io/container-device-interface/pkg/parser"
)

const (
	IsolatedRuntimeConfigVersion = 1
	MaxIsolatedArguments         = 64
	MaxIsolatedEnvironment       = 64
	MaxIsolatedHosts             = 64
	MaxIsolatedStringBytes       = 4096
	MaxIsolatedExecutionMillis   = int64(time.Hour / time.Millisecond)
	MaxIsolatedRAMBytes          = uint64(1 << 40)
	MaxIsolatedVRAMBytes         = uint64(1 << 40)
	MaxIsolatedKVCacheBytes      = uint64(1 << 38)
	MaxIsolatedContextTokens     = uint64(1 << 30)
	MaxIsolatedBatchSize         = uint32(1 << 20)
	MaxIsolatedCPUMillis         = uint64(24 * time.Hour / time.Millisecond)
	MaxIsolatedDiskBytes         = uint64(16 << 40)
	MaxIsolatedPIDs              = uint32(1 << 20)
)

var namespacePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,75}$`)

type IsolatedBackendConfig struct {
	Type           string
	SocketPath     string
	Namespace      string
	Snapshotter    string
	Runtime        string
	FIFODir        string
	MaxActive      int
	PermitGPU      bool
	PermitNetwork  bool
	Limits         executor.Limits
	ImageReference string
	ImageDigest    string
	GPUDevices     map[string]string
}

// IsolatedBackend is the narrow lifecycle owned by a future audited backend.
// Close must stop accepting work and release its local runtime connection.
type IsolatedBackend interface {
	executor.ContainerdClient
	executor.RuntimeReadiness
	io.Closer
}

type IsolatedBackendFactory interface {
	Open(context.Context, IsolatedBackendConfig) (IsolatedBackend, error)
}

// IsolatedRuntime owns exactly one immutable adapter/backend pair.
type IsolatedRuntime struct {
	Adapter airuntime.Adapter
	backend IsolatedBackend
	close   sync.Once
	err     error
}

func (r *IsolatedRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.close.Do(func() {
		if !nilIsolatedBackend(r.backend) {
			r.err = closeIsolatedBackend(r.backend)
		}
	})
	return r.err
}

type isolatedRuntimeFile struct {
	Version    int                    `json:"version"`
	Backend    isolatedBackendFile    `json:"backend"`
	Capability isolatedCapabilityFile `json:"capability"`
	Container  isolatedContainerFile  `json:"container"`
}

type isolatedBackendFile struct {
	Type        string                  `json:"type"`
	SocketPath  string                  `json:"socketPath"`
	Namespace   string                  `json:"namespace"`
	Snapshotter string                  `json:"snapshotter"`
	Runtime     string                  `json:"runtime"`
	FIFODir     string                  `json:"fifoDir"`
	MaxActive   int                     `json:"maxActive"`
	GPUDevices  []isolatedGPUDeviceFile `json:"gpuDevices,omitempty"`
}

type isolatedGPUDeviceFile struct {
	Alias     string `json:"alias"`
	CDIDevice string `json:"cdiDevice"`
}

type isolatedCapabilityFile struct {
	ServiceID          string                `json:"serviceId"`
	Operation          string                `json:"operation"`
	Model              string                `json:"model"`
	ModelDigest        string                `json:"modelDigest"`
	Runtime            string                `json:"runtime"`
	RuntimeRevision    string                `json:"runtimeRevision"`
	MaxInputBytes      uint64                `json:"maxInputBytes"`
	MaxOutputBytes     uint64                `json:"maxOutputBytes"`
	AcceptedPriorities []airuntime.Priority  `json:"acceptedPriorities"`
	Admission          isolatedAdmissionFile `json:"admission"`
}

type isolatedAdmissionFile struct {
	RAMBytes       uint64 `json:"ramBytes"`
	VRAMBytes      uint64 `json:"vramBytes,omitempty"`
	KVCacheBytes   uint64 `json:"kvCacheBytes,omitempty"`
	ContextTokens  uint64 `json:"contextTokens"`
	BatchSize      uint32 `json:"batchSize"`
	ExecutionMilli int64  `json:"executionMillis"`
}

type isolatedContainerFile struct {
	ImageReference string               `json:"imageReference"`
	ImageDigest    string               `json:"imageDigest"`
	Entrypoint     []string             `json:"entrypoint"`
	Environment    map[string]string    `json:"environment,omitempty"`
	UserID         uint32               `json:"userId"`
	GroupID        uint32               `json:"groupId"`
	Network        executor.NetworkMode `json:"network"`
	AllowedHosts   []string             `json:"allowedHosts,omitempty"`
	AllowGPU       bool                 `json:"allowGpu,omitempty"`
	CPUMillis      uint64               `json:"cpuMillis"`
	DiskBytes      uint64               `json:"diskBytes"`
	PIDs           uint32               `json:"pids"`
	GPUDevices     uint32               `json:"gpuDevices,omitempty"`
}

// LoadIsolatedRuntime parses a private, strict operator file and asks only the
// locally supplied factory to open its fixed backend. Merely naming a backend
// in JSON cannot enable one that the binary did not compile and inject.
func LoadIsolatedRuntime(
	ctx context.Context,
	path string,
	factory IsolatedBackendFactory,
) (*IsolatedRuntime, error) {
	if ctx == nil || nilIsolatedFactory(factory) {
		return nil, errors.New("invalid isolated runtime loader")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := readPrivateFile(path, MaxConfigBytes, false)
	if err != nil || validateJSON(data) != nil {
		return nil, errors.New("load isolated runtime configuration")
	}
	var file isolatedRuntimeFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, errors.New("invalid isolated runtime configuration")
	}
	backendConfig, capability, spec, policy, err := buildIsolatedConfiguration(file)
	if err != nil {
		return nil, err
	}
	driver, err := openIsolatedBackend(ctx, factory, backendConfig)
	if err != nil || nilIsolatedBackend(driver) {
		return nil, errors.New("open isolated runtime backend")
	}
	if err := ctx.Err(); err != nil {
		_ = closeIsolatedBackend(driver)
		return nil, err
	}
	backend, err := executor.NewSupervisedBackend(driver, backendConfig.MaxActive)
	if err != nil {
		_ = closeIsolatedBackend(driver)
		return nil, errors.New("configure isolated runtime supervisor")
	}
	execution, err := executor.NewPolicyExecutor(policy, backend)
	if err != nil {
		_ = closeIsolatedBackend(backend)
		return nil, errors.New("configure isolated runtime policy")
	}
	adapter, err := executor.NewRuntimeAdapter(executor.RuntimeAdapterConfig{
		Capability: capability,
		Preflight: airuntime.Preflight{
			Model: capability.Model, ModelDigest: capability.ModelDigest,
			DigestEvidence: airuntime.BindingDeclared,
		},
		Spec: spec, Executor: execution, Readiness: backend,
	})
	if err != nil {
		_ = closeIsolatedBackend(backend)
		return nil, errors.New("configure isolated runtime adapter")
	}
	return &IsolatedRuntime{Adapter: adapter, backend: backend}, nil
}

func buildIsolatedConfiguration(file isolatedRuntimeFile) (
	IsolatedBackendConfig, airuntime.Capability, executor.Spec, executor.Policy, error,
) {
	if file.Version != IsolatedRuntimeConfigVersion ||
		file.Backend.Type != "containerd" ||
		!filepath.IsAbs(file.Backend.SocketPath) ||
		filepath.Clean(file.Backend.SocketPath) != file.Backend.SocketPath ||
		!filepath.IsAbs(file.Backend.FIFODir) ||
		filepath.Clean(file.Backend.FIFODir) != file.Backend.FIFODir ||
		file.Backend.Snapshotter != "overlayfs" ||
		file.Backend.Runtime != "io.containerd.runc.v2" ||
		!namespacePattern.MatchString(file.Backend.Namespace) ||
		file.Backend.MaxActive <= 0 ||
		file.Backend.MaxActive > executor.MaxSupervisedActiveHard ||
		file.Capability.Runtime != file.Backend.Type ||
		file.Capability.Admission.ExecutionMilli <= 0 ||
		file.Capability.Admission.ExecutionMilli > MaxIsolatedExecutionMillis ||
		file.Capability.Admission.RAMBytes > MaxIsolatedRAMBytes ||
		file.Capability.Admission.VRAMBytes > MaxIsolatedVRAMBytes ||
		file.Capability.Admission.KVCacheBytes > MaxIsolatedKVCacheBytes ||
		file.Capability.Admission.ContextTokens > MaxIsolatedContextTokens ||
		file.Capability.Admission.BatchSize > MaxIsolatedBatchSize ||
		file.Container.CPUMillis > MaxIsolatedCPUMillis ||
		file.Container.DiskBytes > MaxIsolatedDiskBytes ||
		file.Container.PIDs > MaxIsolatedPIDs ||
		len(file.Container.ImageReference) == 0 ||
		len(file.Container.ImageReference) > MaxIsolatedStringBytes ||
		strings.IndexByte(file.Container.ImageReference, 0) >= 0 ||
		!strings.HasSuffix(
			file.Container.ImageReference, "@"+file.Container.ImageDigest,
		) ||
		len(file.Container.Entrypoint) == 0 ||
		len(file.Container.Entrypoint) > MaxIsolatedArguments ||
		len(file.Container.Environment) > MaxIsolatedEnvironment ||
		len(file.Container.AllowedHosts) > MaxIsolatedHosts ||
		(file.Capability.Admission.VRAMBytes > 0 &&
			(!file.Container.AllowGPU || file.Container.GPUDevices == 0 ||
				len(file.Backend.GPUDevices) == 0 ||
				file.Container.GPUDevices > uint32(len(file.Backend.GPUDevices)))) ||
		(file.Capability.Admission.VRAMBytes == 0 &&
			(file.Container.AllowGPU || file.Container.GPUDevices != 0 ||
				len(file.Backend.GPUDevices) != 0)) {
		return IsolatedBackendConfig{}, airuntime.Capability{}, executor.Spec{}, executor.Policy{},
			errors.New("isolated runtime configuration exceeds hard limits")
	}
	executionTime := time.Duration(file.Capability.Admission.ExecutionMilli) * time.Millisecond
	capability := airuntime.Capability{
		ServiceID: file.Capability.ServiceID, Operation: file.Capability.Operation,
		Model: file.Capability.Model, ModelDigest: file.Capability.ModelDigest,
		Runtime: file.Capability.Runtime, RuntimeRevision: file.Capability.RuntimeRevision,
		MaxInputBytes:      file.Capability.MaxInputBytes,
		MaxOutputBytes:     file.Capability.MaxOutputBytes,
		AcceptedPriorities: append([]airuntime.Priority(nil), file.Capability.AcceptedPriorities...),
		Admission: admission.Resources{
			RAMBytes:      file.Capability.Admission.RAMBytes,
			VRAMBytes:     file.Capability.Admission.VRAMBytes,
			KVCacheBytes:  file.Capability.Admission.KVCacheBytes,
			ContextTokens: file.Capability.Admission.ContextTokens,
			BatchSize:     file.Capability.Admission.BatchSize,
			OutputBytes:   file.Capability.MaxOutputBytes,
			ExecutionTime: executionTime,
		},
	}
	if err := airuntime.ValidateCapability(capability); err != nil {
		return IsolatedBackendConfig{}, airuntime.Capability{}, executor.Spec{}, executor.Policy{},
			errors.New("invalid isolated runtime capability")
	}
	limits := executor.Limits{
		CPUMillis:   file.Container.CPUMillis,
		MemoryBytes: capability.Admission.RAMBytes,
		DiskBytes:   file.Container.DiskBytes, PIDs: file.Container.PIDs,
		ExecutionTime: executionTime, OutputBytes: capability.MaxOutputBytes,
		GPUDeviceCount: file.Container.GPUDevices,
	}
	spec := executor.Spec{
		ImageDigest:  file.Container.ImageDigest,
		Entrypoint:   append([]string(nil), file.Container.Entrypoint...),
		Environment:  cloneStringMap(file.Container.Environment),
		ReadOnlyRoot: true, Network: file.Container.Network,
		AllowedHosts: append([]string(nil), file.Container.AllowedHosts...),
		AllowGPU:     file.Container.AllowGPU, UserID: file.Container.UserID,
		GroupID: file.Container.GroupID, NoNewPrivileges: true, Limits: limits,
	}
	policy := executor.Policy{
		AllowedImages:       map[string]struct{}{spec.ImageDigest: {}},
		AllowedNetworkHosts: append([]string(nil), spec.AllowedHosts...),
		MaxAllowedImages:    1, MaxEnvironment: len(spec.Environment),
		MaxArguments: len(spec.Entrypoint), MaxAllowedHosts: len(spec.AllowedHosts),
		MaxStringBytes: MaxIsolatedStringBytes, MaxInputBytes: capability.MaxInputBytes,
		Ceiling: limits, PermitGPU: spec.AllowGPU,
		PermitNetwork:       spec.Network == executor.NetworkAllowlist,
		RequireReadOnlyRoot: true,
	}
	if err := policy.Validate(spec); err != nil {
		return IsolatedBackendConfig{}, airuntime.Capability{}, executor.Spec{}, executor.Policy{},
			errors.New("invalid isolated runtime container policy")
	}
	gpuDevices, err := validateGPUDeviceBindings(file.Backend.GPUDevices)
	if err != nil {
		return IsolatedBackendConfig{}, airuntime.Capability{}, executor.Spec{}, executor.Policy{}, err
	}
	return IsolatedBackendConfig{
		Type: file.Backend.Type, SocketPath: file.Backend.SocketPath,
		Namespace: file.Backend.Namespace, Snapshotter: file.Backend.Snapshotter,
		Runtime: file.Backend.Runtime, FIFODir: file.Backend.FIFODir,
		MaxActive: file.Backend.MaxActive, PermitGPU: file.Container.AllowGPU,
		PermitNetwork: file.Container.Network != executor.NetworkNone,
		Limits:        limits, ImageReference: file.Container.ImageReference,
		ImageDigest: file.Container.ImageDigest, GPUDevices: gpuDevices,
	}, capability, spec, policy, nil
}

func validateGPUDeviceBindings(
	bindings []isolatedGPUDeviceFile,
) (map[string]string, error) {
	if len(bindings) > 64 {
		return nil, errors.New("too many GPU device bindings")
	}
	if len(bindings) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(bindings))
	devices := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if !validGPUAlias(binding.Alias) {
			return nil, errors.New("invalid GPU device alias")
		}
		_, _, deviceName, err := parser.ParseQualifiedName(binding.CDIDevice)
		if err != nil || strings.EqualFold(deviceName, "all") {
			return nil, errors.New("invalid CDI device identifier")
		}
		if _, duplicate := result[binding.Alias]; duplicate {
			return nil, errors.New("duplicate GPU device alias")
		}
		if _, duplicate := devices[binding.CDIDevice]; duplicate {
			return nil, errors.New("duplicate CDI device identifier")
		}
		result[binding.Alias] = binding.CDIDevice
		devices[binding.CDIDevice] = struct{}{}
	}
	return result, nil
}

func validGPUAlias(value string) bool {
	if len(value) < 3 || len(value) > 64 || strings.Contains(value, "..") {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') &&
			!(character >= '0' && character <= '9') && character != '-' {
			return false
		}
	}
	return true
}

func cloneStringMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func nilIsolatedFactory(factory IsolatedBackendFactory) bool {
	return nilcheck.IsNil(factory)
}

func nilIsolatedBackend(backend IsolatedBackend) bool {
	return nilcheck.IsNil(backend)
}

func openIsolatedBackend(
	ctx context.Context,
	factory IsolatedBackendFactory,
	config IsolatedBackendConfig,
) (backend IsolatedBackend, err error) {
	defer func() {
		if recover() != nil {
			backend = nil
			err = errors.New("isolated runtime backend factory panicked")
		}
	}()
	return factory.Open(ctx, config)
}

func closeIsolatedBackend(backend IsolatedBackend) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("close isolated runtime backend")
		}
	}()
	if err := backend.Close(); err != nil {
		return errors.New("close isolated runtime backend")
	}
	return nil
}
