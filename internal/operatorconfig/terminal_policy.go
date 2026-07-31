package operatorconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/tosnetwork/tos-ai/internal/resourceguard"
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/internal/worker"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
)

const (
	TerminalPolicyVersion         = 3
	terminalPolicyPreviousVersion = 2
	terminalPolicyLegacyVersion   = 1
	MaxTerminalPolicyConfigBytes  = int64(64 << 10)
)

type terminalPolicyFile struct {
	Version              int                            `json:"version"`
	Workers              int                            `json:"workers"`
	OwnerReservedWorkers *int                           `json:"ownerReservedWorkers"`
	MaxQueue             int                            `json:"maxQueue"`
	MaxConnections       int                            `json:"maxConnections"`
	QuoteTTLMillis       int64                          `json:"quoteTtlMillis"`
	MaxQuotes            int                            `json:"maxQuotes"`
	MaxInvocations       int                            `json:"maxInvocations"`
	MaxDeadlineMillis    int64                          `json:"maxDeadlineMillis"`
	Preflight            terminalPreflightPolicy        `json:"preflight"`
	ResourceMonitor      *terminalResourceMonitorPolicy `json:"resourceMonitor"`
	Admission            terminalAdmissionPolicy        `json:"admission"`
}

type terminalPreflightPolicy struct {
	TimeoutMillis    int64 `json:"timeoutMillis"`
	SuccessTTLMillis int64 `json:"successTtlMillis"`
	FailureTTLMillis int64 `json:"failureTtlMillis"`
	RefreshMillis    int64 `json:"refreshMillis"`
	Workers          int   `json:"workers"`
}

type terminalResourceMonitorPolicy struct {
	IntervalMillis    int64 `json:"intervalMillis"`
	TimeoutMillis     int64 `json:"timeoutMillis"`
	FailureThreshold  int   `json:"failureThreshold"`
	RecoveryThreshold int   `json:"recoveryThreshold"`
}

type terminalAdmissionPolicy struct {
	Capacity      terminalResourcePolicy `json:"capacity"`
	OwnerReserved terminalResourcePolicy `json:"ownerReserved"`
	PerRequestMax terminalResourcePolicy `json:"perRequestMax"`
}

type terminalResourcePolicy struct {
	RAMBytes        uint64 `json:"ramBytes"`
	VRAMBytes       uint64 `json:"vramBytes"`
	KVCacheBytes    uint64 `json:"kvCacheBytes"`
	ContextTokens   uint64 `json:"contextTokens"`
	BatchSize       uint32 `json:"batchSize"`
	OutputBytes     uint64 `json:"outputBytes"`
	ExecutionMillis int64  `json:"executionMillis"`
}

// TerminalPolicy is the immutable, administrator-owned local scheduling and
// resource authority loaded before the worker allocates runtime capacity.
type TerminalPolicy struct {
	Workers              int
	OwnerReservedWorkers int
	MaxQueue             int
	MaxConnections       int
	QuoteTTL             time.Duration
	MaxQuotes            int
	MaxInvocations       int
	MaxDeadline          time.Duration
	PreflightTimeout     time.Duration
	PreflightTTL         time.Duration
	FailureTTL           time.Duration
	RefreshInterval      time.Duration
	PreflightWorkers     int
	ResourceMonitor      ResourceMonitorPolicy
	Admission            admission.Config
}

type ResourceMonitorPolicy struct {
	Interval          time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	RecoveryThreshold int
}

// LoadTerminalPolicy reads a strict private JSON file. It validates absolute
// hard limits here; startup separately constrains RAM and VRAM capacity to the
// locally observed host before creating the AdmissionController.
func LoadTerminalPolicy(path string) (TerminalPolicy, error) {
	data, err := readPrivateFile(path, MaxTerminalPolicyConfigBytes, false)
	if err != nil {
		return TerminalPolicy{}, errors.New("load terminal policy configuration")
	}
	if err := validateJSON(data); err != nil {
		return TerminalPolicy{}, errors.New("invalid terminal policy configuration")
	}
	var config terminalPolicyFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return TerminalPolicy{}, errors.New("invalid terminal policy configuration")
	}
	ownerReservedWorkers, monitor, ok := terminalVersionedFields(config)
	if !ok || !validTerminalPolicyScalars(config, ownerReservedWorkers, monitor) {
		return TerminalPolicy{}, errors.New("terminal policy exceeds hard limits")
	}
	capacity, ok := terminalResources(config.Admission.Capacity, true)
	if !ok {
		return TerminalPolicy{}, errors.New("invalid terminal capacity policy")
	}
	ownerReserved, ok := terminalResources(
		config.Admission.OwnerReserved, false,
	)
	if !ok || ownerReserved.RAMBytes == 0 ||
		ownerReserved.ContextTokens == 0 || ownerReserved.BatchSize == 0 ||
		ownerReserved.OutputBytes == 0 ||
		capacity.VRAMBytes > 0 && ownerReserved.VRAMBytes == 0 {
		return TerminalPolicy{}, errors.New("invalid terminal owner reserve")
	}
	perRequest, ok := terminalResources(config.Admission.PerRequestMax, true)
	if !ok {
		return TerminalPolicy{}, errors.New("invalid terminal request policy")
	}
	admissionConfig := admission.Config{
		MaxConcurrent: config.Workers,
		MaxQueue:      config.MaxQueue,
		Capacity:      capacity,
		OwnerReserved: ownerReserved,
		PerRequestMax: perRequest,
	}
	if _, err := admission.New(admissionConfig); err != nil {
		return TerminalPolicy{}, errors.New("invalid terminal admission policy")
	}
	return TerminalPolicy{
		Workers:              config.Workers,
		OwnerReservedWorkers: ownerReservedWorkers,
		MaxQueue:             config.MaxQueue,
		MaxConnections:       config.MaxConnections,
		QuoteTTL:             time.Duration(config.QuoteTTLMillis) * time.Millisecond,
		MaxQuotes:            config.MaxQuotes, MaxInvocations: config.MaxInvocations,
		MaxDeadline: time.Duration(config.MaxDeadlineMillis) * time.Millisecond,
		PreflightTimeout: time.Duration(
			config.Preflight.TimeoutMillis,
		) * time.Millisecond,
		PreflightTTL: time.Duration(
			config.Preflight.SuccessTTLMillis,
		) * time.Millisecond,
		FailureTTL: time.Duration(
			config.Preflight.FailureTTLMillis,
		) * time.Millisecond,
		RefreshInterval: time.Duration(
			config.Preflight.RefreshMillis,
		) * time.Millisecond,
		PreflightWorkers: config.Preflight.Workers,
		ResourceMonitor:  monitor,
		Admission:        admissionConfig,
	}, nil
}

func terminalVersionedFields(
	config terminalPolicyFile,
) (int, ResourceMonitorPolicy, bool) {
	legacyMonitor := ResourceMonitorPolicy{
		Interval: 10 * time.Second, Timeout: 5 * time.Second,
		FailureThreshold: 2, RecoveryThreshold: 2,
	}
	switch config.Version {
	case terminalPolicyLegacyVersion:
		return 0, legacyMonitor,
			config.OwnerReservedWorkers == nil && config.ResourceMonitor == nil
	case terminalPolicyPreviousVersion:
		if config.OwnerReservedWorkers == nil || config.ResourceMonitor != nil {
			return 0, ResourceMonitorPolicy{}, false
		}
		return *config.OwnerReservedWorkers, legacyMonitor, true
	case TerminalPolicyVersion:
		if config.OwnerReservedWorkers == nil || config.ResourceMonitor == nil {
			return 0, ResourceMonitorPolicy{}, false
		}
		return *config.OwnerReservedWorkers, ResourceMonitorPolicy{
			Interval:          time.Duration(config.ResourceMonitor.IntervalMillis) * time.Millisecond,
			Timeout:           time.Duration(config.ResourceMonitor.TimeoutMillis) * time.Millisecond,
			FailureThreshold:  config.ResourceMonitor.FailureThreshold,
			RecoveryThreshold: config.ResourceMonitor.RecoveryThreshold,
		}, true
	default:
		return 0, ResourceMonitorPolicy{}, false
	}
}

func validTerminalPolicyScalars(
	config terminalPolicyFile,
	ownerReservedWorkers int,
	monitor ResourceMonitorPolicy,
) bool {
	preflight := config.Preflight
	return config.Workers > 0 && config.Workers <= scheduler.MaxWorkersHard &&
		ownerReservedWorkers >= 0 && ownerReservedWorkers < config.Workers &&
		config.MaxQueue > 0 && config.MaxQueue <= scheduler.MaxQueueHard &&
		config.MaxConnections > 0 &&
		config.MaxConnections <= unixserver.MaxConnectionsHard &&
		validMillis(config.QuoteTTLMillis, worker.MaxQuoteTTLHard) &&
		config.MaxQuotes > 0 && config.MaxQuotes <= worker.MaxQuotesHard &&
		config.MaxInvocations > 0 &&
		config.MaxInvocations <= worker.MaxInvocationsHard &&
		validMillis(config.MaxDeadlineMillis, worker.MaxDeadlineHard) &&
		validMillis(preflight.TimeoutMillis, worker.MaxPreflightTimeoutHard) &&
		validMillis(preflight.SuccessTTLMillis, worker.MaxPreflightTTLHard) &&
		validMillis(preflight.FailureTTLMillis, worker.MaxFailureTTLHard) &&
		preflight.FailureTTLMillis <= preflight.SuccessTTLMillis &&
		preflight.RefreshMillis >= int64(worker.MinPreflightRefresh/time.Millisecond) &&
		preflight.RefreshMillis <= int64(worker.MaxPreflightRefreshHard/time.Millisecond) &&
		preflight.Workers > 0 &&
		preflight.Workers <= worker.MaxPreflightWorkersHard &&
		monitor.Interval >= resourceguard.MinInterval &&
		monitor.Interval <= resourceguard.MaxIntervalHard &&
		monitor.Timeout >= resourceguard.MinTimeout &&
		monitor.Timeout <= resourceguard.MaxTimeoutHard &&
		monitor.Timeout <= monitor.Interval &&
		monitor.FailureThreshold > 0 &&
		monitor.FailureThreshold <= resourceguard.MaxThresholdHard &&
		monitor.RecoveryThreshold > 0 &&
		monitor.RecoveryThreshold <= resourceguard.MaxThresholdHard
}

func validMillis(value int64, maximum time.Duration) bool {
	return value > 0 && value <= int64(maximum/time.Millisecond)
}

func terminalResources(
	value terminalResourcePolicy,
	requireExecution bool,
) (admission.Resources, bool) {
	if value.ExecutionMillis < 0 || requireExecution && value.ExecutionMillis == 0 ||
		value.ExecutionMillis > int64(worker.MaxDeadlineHard/time.Millisecond) {
		return admission.Resources{}, false
	}
	return admission.Resources{
		RAMBytes: value.RAMBytes, VRAMBytes: value.VRAMBytes,
		KVCacheBytes: value.KVCacheBytes, ContextTokens: value.ContextTokens,
		BatchSize: value.BatchSize, OutputBytes: value.OutputBytes,
		ExecutionTime: time.Duration(value.ExecutionMillis) * time.Millisecond,
	}, true
}
