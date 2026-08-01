package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/probe"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"google.golang.org/protobuf/proto"
)

const (
	MaxQuotesHard           = 65536
	MaxInvocationsHard      = 65536
	MaxAdaptersHard         = 64
	MaxPreflightWaitersHard = 256
	MaxQuoteTTLHard         = 5 * time.Minute
	MaxDeadlineHard         = time.Hour
	MaxPreflightTimeoutHard = 30 * time.Second
	MaxPreflightTTLHard     = 5 * time.Minute
	MaxFailureTTLHard       = 30 * time.Second
	MinPreflightRefresh     = 250 * time.Millisecond
	MaxPreflightRefreshHard = 5 * time.Minute
	MaxPreflightWorkersHard = 16
)

var (
	ErrShutdownIncomplete   = errors.New("worker shutdown is incomplete")
	errResourcesUnavailable = errors.New("local resources are unavailable")
	workerServiceIDPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{2,127}$`)
)

type Config struct {
	Version             string
	QuoteTTL            time.Duration
	MaxQuotes           int
	MaxInvocations      int
	MaxDeadline         time.Duration
	PreflightTimeout    time.Duration
	PreflightTTL        time.Duration
	PreflightFailureTTL time.Duration
	MaxPreflightWaiters int
	PreflightRefresh    time.Duration
	PreflightWorkers    int
	PriceNanoTOS        uint64
	Now                 func() time.Time
	GPUStatus           string
	ResourceHealth      probe.ResourceHealthProvider
	TaskStore           *localrpc.WorkerTaskStore
}

type Service struct {
	config          Config
	scheduler       *scheduler.Scheduler
	adapters        map[string]airuntime.Adapter
	runtimeSlots    map[string]*runtimeSlot
	capabilities    []airuntime.Capability
	quotes          *quoteStore
	invocations     *invocationStore
	admission       *admission.Controller
	resourceHealth  probe.ResourceHealthProvider
	taskStore       *localrpc.WorkerTaskStore
	startupRecovery startupTaskRecovery
	lifecycleMu     sync.Mutex
	draining        atomic.Bool
	runtimeCtx      context.Context
	runtimeStop     context.CancelFunc
	runtimeWG       sync.WaitGroup
	resultWG        sync.WaitGroup
	stopOnce        sync.Once
	resultWaitOnce  sync.Once
	runtimeDone     chan struct{}
	resultDone      chan struct{}
	closeOnce       sync.Once
	closeErr        error
}

func NewService(config Config, taskScheduler *scheduler.Scheduler, admissionController *admission.Controller, adapters []airuntime.Adapter) (*Service, error) {
	if config.Version == "" || len(config.Version) > 64 ||
		strings.IndexFunc(config.Version, unicode.IsControl) >= 0 ||
		!validGPUStatus(config.GPUStatus) ||
		config.QuoteTTL <= 0 || config.MaxQuotes <= 0 ||
		config.MaxInvocations <= 0 || config.MaxDeadline <= 0 || taskScheduler == nil ||
		admissionController == nil || len(adapters) == 0 ||
		config.QuoteTTL > MaxQuoteTTLHard || config.MaxQuotes > MaxQuotesHard ||
		config.MaxInvocations > MaxInvocationsHard || config.MaxDeadline > MaxDeadlineHard ||
		len(adapters) > MaxAdaptersHard ||
		config.PreflightTimeout <= 0 || config.PreflightTimeout > MaxPreflightTimeoutHard ||
		config.PreflightTTL <= 0 || config.PreflightTTL > MaxPreflightTTLHard ||
		config.PreflightFailureTTL <= 0 || config.PreflightFailureTTL > MaxFailureTTLHard ||
		config.PreflightFailureTTL > config.PreflightTTL ||
		config.MaxPreflightWaiters <= 0 ||
		config.MaxPreflightWaiters > MaxPreflightWaitersHard ||
		config.PreflightRefresh < MinPreflightRefresh ||
		config.PreflightRefresh > MaxPreflightRefreshHard ||
		config.PreflightWorkers <= 0 ||
		config.PreflightWorkers > MaxPreflightWorkersHard ||
		config.TaskStore == nil ||
		!validInitialResourceHealth(config) {
		return nil, errors.New("invalid worker configuration")
	}
	if config.PreflightRefresh+preflightScanLimit(
		len(adapters), config.PreflightWorkers, config.PreflightTimeout,
	) > config.PreflightTTL {
		return nil, errors.New("invalid worker configuration")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	service := &Service{
		config:         config,
		scheduler:      taskScheduler,
		adapters:       make(map[string]airuntime.Adapter, len(adapters)),
		runtimeSlots:   make(map[string]*runtimeSlot, len(adapters)),
		quotes:         newQuoteStore(config.MaxQuotes),
		invocations:    newInvocationStore(config.MaxInvocations),
		admission:      admissionController,
		resourceHealth: config.ResourceHealth,
		taskStore:      config.TaskStore,
		runtimeDone:    make(chan struct{}),
		resultDone:     make(chan struct{}),
	}
	preflight := preflightConfig{
		timeout: config.PreflightTimeout, successTTL: config.PreflightTTL,
		failureTTL: config.PreflightFailureTTL, maxWaiters: config.MaxPreflightWaiters,
		now: config.Now,
	}
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, errors.New("nil runtime adapter")
		}
		capability := adapter.Capability()
		capability.AcceptedPriorities = append(
			[]airuntime.Priority(nil), capability.AcceptedPriorities...,
		)
		if err := airuntime.ValidateCapability(capability); err != nil {
			return nil, errors.New("runtime adapter has invalid capability")
		}
		if err := validateSelector(
			capability.ServiceID,
			capability.Operation,
			capability.Model,
		); err != nil {
			return nil, errors.New("runtime adapter capability is not protocol-safe")
		}
		for _, priority := range capability.AcceptedPriorities {
			class, err := admissionClass(priority)
			if err != nil {
				return nil, errors.New("runtime adapter has invalid priority")
			}
			resources := capability.Admission
			resources.OutputBytes = capability.MaxOutputBytes
			if err := admissionController.Check(admission.Request{
				ID: "startup-check", Class: class, Resources: resources,
			}); err != nil {
				return nil, errors.New("runtime adapter exceeds admission configuration")
			}
		}
		key := adapterKey(capability.ServiceID, capability.Operation, capability.Model)
		if _, exists := service.adapters[key]; exists {
			return nil, fmt.Errorf("duplicate adapter capability %q", key)
		}
		service.adapters[key] = adapter
		service.runtimeSlots[key] = newRuntimeSlot(adapter, capability, preflight)
		service.capabilities = append(service.capabilities, capability)
	}
	sort.Slice(service.capabilities, func(a, b int) bool {
		left, right := service.capabilities[a], service.capabilities[b]
		return adapterKey(left.ServiceID, left.Operation, left.Model) <
			adapterKey(right.ServiceID, right.Operation, right.Model)
	})
	startupRecovery, err := prepareTaskStoreForStartup(
		config.TaskStore, config.Now().UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("reconcile Worker task store at startup: %w", err)
	}
	service.startupRecovery = startupRecovery
	if err := taskScheduler.Start(); err != nil {
		return nil, err
	}
	service.runtimeCtx, service.runtimeStop = context.WithCancel(context.Background())
	for _, slot := range service.runtimeSlots {
		slot.configureLifecycle(service.runtimeCtx, &service.runtimeWG)
	}
	service.runtimeWG.Add(1)
	go service.monitorRuntimes()
	return service, nil
}

func preflightScanLimit(
	adapters int,
	workers int,
	timeout time.Duration,
) time.Duration {
	batches := (adapters + workers - 1) / workers
	return time.Duration(batches) * timeout
}

func (s *Service) Health(_ context.Context, _ *connect.Request[edgev1.HealthRequest]) (*connect.Response[edgev1.HealthResponse], error) {
	snapshot := s.admission.Snapshot()
	taskCapacity := s.currentTaskStoreCapacity()
	readiness := s.readiness(snapshot, taskCapacity)
	now := s.config.Now().UTC()
	expires := now.Add(s.config.PreflightTTL)
	status := fmt.Sprintf("%s;admission=%s;resources=%s;runtimes=%d/%d;binding=%s;gpu=%s;tasks=%d/%d;running=%d;reserved=%d",
		readiness.Status, readiness.Admission, readiness.Resources, readiness.RuntimeReady,
		readiness.RuntimeTotal, readiness.BindingEvidence, readiness.GPU,
		readiness.TaskSlots, readiness.TaskCapacity,
		readiness.Running, readiness.Reserved)
	return connect.NewResponse(&edgev1.HealthResponse{
		Status: status, Version: s.config.Version,
		Readiness: wireReadiness(
			readiness,
			s.capacityRevisionFor(snapshot, taskCapacity),
			now,
			expires,
			"tos-ai-worker",
		),
	}), nil
}

type Readiness struct {
	Status                string
	Admission             string
	Resources             string
	RuntimeReady          int
	RuntimeTotal          int
	BindingEvidence       string
	GPU                   string
	TaskStore             string
	TaskStoreReason       string
	TaskSlots             uint64
	TaskCapacity          uint64
	TaskOwnerReserved     uint64
	TaskOwnerTasks        uint64
	TaskExternalTasks     uint64
	TaskAvailableExternal uint64
	Running               int
	Reserved              int
}

func (s *Service) Readiness() Readiness {
	snapshot := s.admission.Snapshot()
	return s.readiness(snapshot, s.currentTaskStoreCapacity())
}

// readiness derives the complete readiness view from one admission snapshot.
// Callers that expose multiple admission gauges can therefore avoid mixing
// values observed at different points in a reservation lifecycle.
func (s *Service) readiness(
	snapshot admission.Snapshot,
	tasks taskStoreCapacity,
) Readiness {
	status, admissionStatus := "ready", "ready"
	if s.draining.Load() || !snapshot.Accepting {
		status, admissionStatus = "draining", "draining"
	}
	runtimeReady, checked := 0, 0
	declared, observed := false, false
	for _, slot := range s.runtimeSlots {
		state := slot.snapshot()
		if state.checked {
			checked++
		}
		if !state.ready {
			continue
		}
		runtimeReady++
		switch state.evidence {
		case airuntime.BindingDeclared:
			declared = true
		case airuntime.BindingLocallyObserved:
			observed = true
		}
	}
	if status != "draining" && runtimeReady != len(s.runtimeSlots) {
		status = "degraded"
		if checked == 0 {
			status = "starting"
		}
	}
	resourceHealth := s.currentResourceHealth()
	if status != "draining" && !resourceHealth.Ready {
		status, admissionStatus = "degraded", "blocked"
	}
	if status != "draining" && !tasks.Ready {
		status, admissionStatus = "degraded", "blocked"
	}
	bindingEvidence := "unknown"
	switch {
	case declared && observed:
		bindingEvidence = "mixed"
	case declared:
		bindingEvidence = string(airuntime.BindingDeclared)
	case observed:
		bindingEvidence = string(airuntime.BindingLocallyObserved)
	}
	gpu := resourceHealth.GPU
	if gpu == "" {
		gpu = "unknown"
	}
	return Readiness{
		Status: status, Admission: admissionStatus,
		Resources: resourceHealth.Status, RuntimeReady: runtimeReady,
		RuntimeTotal: len(s.runtimeSlots), BindingEvidence: bindingEvidence,
		GPU: gpu, TaskStore: tasks.Status, TaskStoreReason: tasks.Reason,
		TaskSlots: tasks.Tasks, TaskCapacity: tasks.Capacity,
		TaskOwnerReserved:     tasks.OwnerReserved,
		TaskOwnerTasks:        tasks.OwnerTasks,
		TaskExternalTasks:     tasks.ExternalTasks,
		TaskAvailableExternal: tasks.AvailableExternal,
		Running:               snapshot.Running, Reserved: snapshot.Reserved,
	}
}

type taskStoreCapacity struct {
	Tasks             uint64
	Capacity          uint64
	Available         uint64
	OwnerReserved     uint64
	OwnerTasks        uint64
	ExternalTasks     uint64
	AvailableExternal uint64
	Ready             bool
	Status            string
	Reason            string
}

func (s *Service) currentTaskStoreCapacity() taskStoreCapacity {
	output := taskStoreCapacity{
		Status: "unavailable", Reason: "store_unavailable",
	}
	stats, err := s.taskStore.Stats()
	expectedExternal := uint64(0)
	if err == nil && stats.OwnerReserved <= stats.Capacity &&
		stats.OwnerTasks <= stats.Tasks {
		externalCapacity := stats.Capacity - stats.OwnerReserved
		externalTasks := stats.Tasks - stats.OwnerTasks
		if stats.Tasks < stats.Capacity && externalTasks < externalCapacity {
			expectedExternal = min(
				stats.Capacity-stats.Tasks,
				externalCapacity-externalTasks,
			)
		}
	}
	if err != nil || stats.Capacity == 0 ||
		stats.Tasks > stats.Capacity ||
		stats.Available != stats.Capacity-stats.Tasks ||
		stats.OwnerReserved > stats.Capacity ||
		stats.OwnerTasks > stats.Tasks ||
		stats.ExternalTasks != stats.Tasks-stats.OwnerTasks ||
		stats.AvailableExternal != expectedExternal {
		return output
	}
	output.Tasks = stats.Tasks
	output.Capacity = stats.Capacity
	output.Available = stats.Available
	output.OwnerReserved = stats.OwnerReserved
	output.OwnerTasks = stats.OwnerTasks
	output.ExternalTasks = stats.ExternalTasks
	output.AvailableExternal = stats.AvailableExternal
	if output.Available == 0 {
		output.Status = "unavailable"
		output.Reason = "capacity_exhausted"
		return output
	}
	output.Ready = true
	output.Status = "ready"
	output.Reason = "capacity_available"
	return output
}

func (capacity taskStoreCapacity) availableFor(priority edgev1.Priority) bool {
	if !capacity.Ready {
		return false
	}
	if priority == edgev1.Priority_PRIORITY_LOCAL_ASYNC {
		return capacity.Available != 0
	}
	return capacity.AvailableExternal != 0
}

// RefreshRuntimes performs an explicitly bounded refresh. Failures update
// readiness but do not prevent the private worker from starting, so an
// operator can diagnose and repair a local runtime through the Unix socket.
func (s *Service) RefreshRuntimes(ctx context.Context) Readiness {
	s.refreshRuntimes(ctx, true)
	return s.Readiness()
}

func (s *Service) refreshRuntimes(ctx context.Context, force bool) {
	if force && s.config.PreflightWorkers > 1 {
		s.refreshRuntimesConcurrent(ctx)
		return
	}
	for _, capability := range s.capabilities {
		if ctx.Err() != nil {
			return
		}
		key := adapterKey(capability.ServiceID, capability.Operation, capability.Model)
		if slot := s.runtimeSlots[key]; slot != nil {
			_, _ = slot.ensure(ctx, force)
		}
	}
}

func (s *Service) refreshRuntimesConcurrent(ctx context.Context) {
	workers := s.config.PreflightWorkers
	if workers > len(s.capabilities) {
		workers = len(s.capabilities)
	}
	if workers <= 0 {
		return
	}
	jobs := make(chan *runtimeSlot)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for slot := range jobs {
				if ctx.Err() != nil {
					return
				}
				_, _ = slot.ensure(ctx, true)
			}
		}()
	}
	for _, capability := range s.capabilities {
		key := adapterKey(capability.ServiceID, capability.Operation, capability.Model)
		slot := s.runtimeSlots[key]
		if slot == nil {
			continue
		}
		select {
		case jobs <- slot:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return
		}
	}
	close(jobs)
	wait.Wait()
}

func (s *Service) monitorRuntimes() {
	defer s.runtimeWG.Done()
	ticker := time.NewTicker(s.config.PreflightRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-s.runtimeCtx.Done():
			return
		case <-ticker.C:
			s.refreshRuntimes(s.runtimeCtx, true)
			_, _, _ = s.taskStore.Cleanup(
				s.config.Now().UTC(), localrpc.DefaultWorkerMaxPrunePerWrite,
			)
		}
	}
}

func (s *Service) GetCapabilities(ctx context.Context, _ *connect.Request[edgev1.GetCapabilitiesRequest]) (*connect.Response[edgev1.GetCapabilitiesResponse], error) {
	response, accepting := s.capabilitiesSnapshot()
	if !accepting {
		return connect.NewResponse(response), nil
	}
	s.refreshRuntimes(ctx, false)
	response, accepting = s.capabilitiesSnapshot()
	if !accepting {
		return connect.NewResponse(response), nil
	}
	for _, capability := range s.capabilities {
		slot := s.runtimeSlots[adapterKey(capability.ServiceID, capability.Operation, capability.Model)]
		if slot == nil || !slot.snapshot().ready {
			continue
		}
		capabilityResources := capability.Admission
		capabilityResources.OutputBytes = capability.MaxOutputBytes
		wire := &edgev1.Capability{
			ServiceId:       capability.ServiceID,
			Operation:       capability.Operation,
			Model:           capability.Model,
			ModelDigest:     capability.ModelDigest,
			Runtime:         capability.Runtime,
			RuntimeRevision: capability.RuntimeRevision,
			MaxInputBytes:   capability.MaxInputBytes,
			MaxOutputBytes:  capability.MaxOutputBytes,
			AdmissionLimits: wireCommittedLimits(capabilityResources),
		}
		for _, priority := range capability.AcceptedPriorities {
			wire.AcceptedPriorities = append(wire.AcceptedPriorities, toWirePriority(priority))
		}
		response.Capabilities = append(response.Capabilities, wire)
	}
	return connect.NewResponse(response), nil
}

func (s *Service) capabilitiesSnapshot() (
	*edgev1.GetCapabilitiesResponse,
	bool,
) {
	now := s.config.Now().UTC()
	expires := now.Add(s.config.PreflightTTL)
	snapshot := s.admission.Snapshot()
	tasks := s.currentTaskStoreCapacity()
	resourcesReady := s.currentResourceHealth().Ready
	if !resourcesReady || !tasks.availableFor(
		edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	) {
		snapshot.Accepting = false
	}
	revision := s.capacityRevisionFor(snapshot, tasks)
	return &edgev1.GetCapabilitiesResponse{
			CapacityRevision: revision,
			Resources: wireResourceClaims(
				snapshot, tasks, revision, now, expires, "tos-ai-worker",
			),
			TerminalRevision:    s.config.Version,
			CollectedUnixMillis: now.UnixMilli(),
			ExpiresUnixMillis:   expires.UnixMilli(),
		}, resourcesReady && tasks.availableFor(
			edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		) && snapshot.Accepting
}

func (s *Service) Quote(ctx context.Context, request *connect.Request[edgev1.QuoteRequest]) (*connect.Response[edgev1.QuoteResponse], error) {
	input := request.Msg
	if err := validateID(input.RequestId); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateSelector(input.ServiceId, input.Operation, input.Model); err != nil {
		return nil, invalidArgument(err)
	}
	key := adapterKey(input.ServiceId, input.Operation, input.Model)
	adapter := s.adapters[key]
	if adapter == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("capability not found"))
	}
	slot := s.runtimeSlots[key]
	if slot == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("runtime is unavailable"))
	}
	capability := slot.capability
	priority, err := fromWirePriority(input.Priority)
	if err != nil || !acceptsPriority(capability, priority) {
		return nil, invalidArgument(errors.New("priority is not accepted by this capability"))
	}
	now := s.config.Now()
	deadline := time.UnixMilli(input.DeadlineUnixMillis)
	if !deadline.After(now) || deadline.After(now.Add(s.config.MaxDeadline)) {
		return nil, invalidArgument(errors.New("deadline is outside the allowed window"))
	}
	if input.InputBytes > capability.MaxInputBytes || input.MaxOutputBytes == 0 ||
		input.MaxOutputBytes > capability.MaxOutputBytes {
		return nil, invalidArgument(errors.New("requested input or output exceeds capability"))
	}
	fingerprint, err := quoteFingerprint(input)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("fingerprint quote"))
	}
	if existing, found, findErr := s.quotes.findRequest(input.RequestId, fingerprint, now); findErr != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, findErr)
	} else if found {
		return connect.NewResponse(existing), nil
	}
	if !s.currentResourceHealth().Ready {
		return nil, connect.NewError(
			connect.CodeUnavailable, errResourcesUnavailable,
		)
	}
	taskCapacity := s.currentTaskStoreCapacity()
	if !taskCapacity.availableFor(input.Priority) {
		if taskCapacity.Reason == "capacity_exhausted" {
			return nil, connect.NewError(
				connect.CodeResourceExhausted,
				errors.New("durable task capacity unavailable"),
			)
		}
		if taskCapacity.Ready {
			return nil, connect.NewError(
				connect.CodeResourceExhausted,
				errors.New("durable task capacity reserved for owner-local work"),
			)
		}
		return nil, connect.NewError(connect.CodeUnavailable,
			errors.New("durable task store unavailable"))
	}
	if _, err := slot.ensure(ctx, false); err != nil {
		return nil, normalizePreflightError(err)
	}
	if !s.currentResourceHealth().Ready {
		return nil, connect.NewError(
			connect.CodeUnavailable, errResourcesUnavailable,
		)
	}
	class, err := admissionClass(priority)
	if err != nil {
		return nil, invalidArgument(err)
	}
	resources := admissionResources(capability, input.MaxOutputBytes, deadline.Sub(now))
	if err := validateRequestedLimits(input.RequestedLimits, resources); err != nil {
		return nil, invalidArgument(err)
	}
	if err := s.admission.Check(admission.Request{
		ID: input.RequestId, Fingerprint: fingerprint, Class: class, Resources: resources,
	}); err != nil {
		return nil, normalizeAdmissionError(err)
	}
	expires := now.Add(s.config.QuoteTTL)
	if deadline.Before(expires) {
		expires = deadline
	}
	quoteID, err := randomID()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("generate quote ID"))
	}
	response := &edgev1.QuoteResponse{
		QuoteId:           quoteID,
		RequestId:         input.RequestId,
		ExpiresUnixMillis: expires.UnixMilli(),
		PriceNanoTos:      s.config.PriceNanoTOS,
		CapacityRevision:  s.capacityRevision(),
		ModelRevision:     capability.ModelDigest,
		RuntimeRevision:   capability.RuntimeRevision,
		CommittedLimits:   wireCommittedLimits(resources),
	}
	s.quotes.add(quoteID, input.RequestId, quoteBinding{
		response:       response,
		serviceID:      input.ServiceId,
		operation:      input.Operation,
		model:          input.Model,
		inputBytes:     input.InputBytes,
		maxOutputBytes: input.MaxOutputBytes,
		deadlineMillis: input.DeadlineUnixMillis,
		priority:       input.Priority,
		resources:      resources,
		fingerprint:    fingerprint,
	})
	return connect.NewResponse(response), nil
}

func (s *Service) Invoke(ctx context.Context, request *connect.Request[edgev1.InvokeRequest]) (*connect.Response[edgev1.InvokeResponse], error) {
	input := request.Msg
	bound, digest, err := localrpc.BindInvocationRequest(input)
	if err != nil || input == nil || input.RequestDigest == "" ||
		input.RequestDigest != digest {
		return nil, invalidArgument(errors.New("invalid invocation request digest"))
	}
	input = bound
	if err := validateID(input.RequestId); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateSelector(input.ServiceId, input.Operation, input.Model); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateID(input.QuoteId); err != nil {
		return nil, invalidArgument(errors.New("invalid quote ID"))
	}
	if err := validateID(input.TaskId); err != nil {
		return nil, invalidArgument(errors.New("invalid task ID"))
	}
	fingerprint, err := invocationFingerprint(input)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("fingerprint invocation"))
	}
	if existing, found, findErr := s.invocations.find(input.RequestId, fingerprint); findErr != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, findErr)
	} else if found {
		return s.awaitInvocation(ctx, input.RequestId, existing, false)
	}
	if !s.currentResourceHealth().Ready {
		return nil, connect.NewError(
			connect.CodeUnavailable, errResourcesUnavailable,
		)
	}
	binding, err := s.quotes.get(input.QuoteId, s.config.Now())
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if err := validateBinding(binding, input); err != nil {
		return nil, invalidArgument(err)
	}
	key := adapterKey(input.ServiceId, input.Operation, input.Model)
	adapter := s.adapters[key]
	if adapter == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("capability not found"))
	}
	slot := s.runtimeSlots[key]
	if slot == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("runtime is unavailable"))
	}
	call, owner, err := s.invocations.begin(
		input.RequestId, input.TaskId, input.RequestDigest, fingerprint,
	)
	if err != nil {
		if errors.Is(err, errInvocationConflict) {
			return nil, connect.NewError(connect.CodeAlreadyExists, err)
		}
		return nil, connect.NewError(connect.CodeResourceExhausted, err)
	}
	if owner {
		if !s.beginInvocationLifecycle() {
			s.invocations.finish(call, nil, admission.ErrStopped)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		lifecycleOwned := true
		defer func() {
			if lifecycleOwned {
				s.resultWG.Done()
			}
		}()
		stored, disposition, claimErr := s.taskStore.ClaimTask(input, s.config.Now().UTC())
		if claimErr != nil {
			s.invocations.finish(call, nil, claimErr)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		identity, identityErr := stored.Identity()
		if identityErr != nil {
			s.invocations.finish(call, nil, identityErr)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		if disposition == localrpc.TaskReplay {
			s.finishTaskReplay(call, stored)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		if _, err := slot.ensure(ctx, true); err != nil {
			s.finishClaimedFailure(call, identity, input, newPreflightFailure(err))
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		if !s.currentResourceHealth().Ready {
			s.finishClaimedFailure(call, identity, input, errResourcesUnavailable)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		priority, _ := fromWirePriority(input.Priority)
		class, classErr := admissionClass(priority)
		if classErr != nil {
			s.finishClaimedFailure(call, identity, input, classErr)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		capability := slot.capability
		resources := binding.resources
		reservation, reservationOwner, reserveErr := s.admission.Reserve(admission.Request{
			ID: input.RequestId, Fingerprint: fingerprint, Class: class, Resources: resources,
		})
		if reserveErr != nil {
			s.finishClaimedFailure(call, identity, input, reserveErr)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		if !reservationOwner {
			s.finishClaimedFailure(call, identity, input, admission.ErrConflict)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		result, submitErr := s.scheduler.Submit(scheduler.Item{
			ID:       input.RequestId,
			Priority: priority,
			Deadline: time.UnixMilli(input.DeadlineUnixMillis),
			Context:  ctx,
			Work: func(runContext context.Context) (airuntime.Response, error) {
				if _, _, err := s.taskStore.MarkTaskRunning(
					identity, s.config.Now().UTC(),
				); err != nil {
					return airuntime.Response{}, err
				}
				if err := reservation.Start(); err != nil {
					return airuntime.Response{}, err
				}
				defer reservation.Release()
				return executeAdapter(adapter, capability, runContext, airuntime.Request{
					RequestID:      input.RequestId,
					Operation:      input.Operation,
					Model:          input.Model,
					Payload:        append([]byte(nil), input.Payload...),
					MaxOutputBytes: input.MaxOutputBytes,
				})
			},
		})
		if submitErr != nil {
			reservation.Release()
			s.finishClaimedFailure(call, identity, input, submitErr)
		} else {
			lifecycleOwned = false
			go func() {
				defer s.resultWG.Done()
				defer reservation.Release()
				outcome := <-result
				if outcome.Err != nil {
					s.finishClaimedFailure(call, identity, input, outcome.Err)
					return
				}
				response := &edgev1.InvokeResponse{
					RequestId: input.RequestId,
					Output:    append([]byte(nil), outcome.Response.Output...),
					Usage: &edgev1.Usage{
						InputBytes:      outcome.Response.Usage.InputBytes,
						OutputBytes:     outcome.Response.Usage.OutputBytes,
						InputTokens:     outcome.Response.Usage.InputTokens,
						OutputTokens:    outcome.Response.Usage.OutputTokens,
						ExecutionMillis: outcome.Response.Usage.ExecutionMillis,
					},
					ModelRevision:   outcome.Response.ModelRevision,
					RuntimeRevision: outcome.Response.RuntimeRevision,
				}
				completedAt := s.config.Now().UTC()
				if completedAt.After(time.UnixMilli(input.DeadlineUnixMillis)) {
					s.finishClaimedFailure(
						call, identity, input, context.DeadlineExceeded,
					)
					return
				}
				if _, _, err := s.taskStore.CompleteTaskSuccess(
					identity, response, completedAt, completedAt,
				); err != nil {
					s.invocations.finish(call, nil, err)
					return
				}
				s.invocations.finish(call, response, nil)
			}()
		}
	}
	return s.awaitInvocation(ctx, input.RequestId, call, owner)
}

func (s *Service) finishTaskReplay(call *invocation, task localrpc.StoredWorkerTask) {
	switch task.Status {
	case edgev1.TaskStatus_TASK_STATUS_SUCCEEDED:
		s.invocations.finish(call, task.Result, nil)
	case edgev1.TaskStatus_TASK_STATUS_CANCELED:
		s.invocations.finish(call, nil, context.Canceled)
	case edgev1.TaskStatus_TASK_STATUS_TIMED_OUT:
		s.invocations.finish(call, nil, context.DeadlineExceeded)
	case edgev1.TaskStatus_TASK_STATUS_FAILED:
		s.invocations.finish(call, nil, airuntime.NewError(airuntime.ErrorInternal, nil))
	default:
		s.invocations.finish(call, nil, errors.New("task is already active; use GetTask"))
	}
}

func (s *Service) finishClaimedFailure(
	call *invocation,
	identity localrpc.WorkerTaskIdentity,
	request *edgev1.InvokeRequest,
	cause error,
) {
	now := s.config.Now().UTC()
	status := edgev1.TaskStatus_TASK_STATUS_FAILED
	switch {
	case errors.Is(cause, context.Canceled), errors.Is(cause, scheduler.ErrCanceled):
		status = edgev1.TaskStatus_TASK_STATUS_CANCELED
	case errors.Is(cause, context.DeadlineExceeded) &&
		!now.Before(time.UnixMilli(request.DeadlineUnixMillis)):
		status = edgev1.TaskStatus_TASK_STATUS_TIMED_OUT
	}
	if _, _, err := s.taskStore.CompleteTaskFailure(
		identity, status, now, now,
	); err != nil {
		s.invocations.finish(call, nil, err)
		return
	}
	s.invocations.finish(call, nil, cause)
}

func (s *Service) awaitInvocation(ctx context.Context, requestID string, call *invocation, owner bool) (*connect.Response[edgev1.InvokeResponse], error) {
	select {
	case <-ctx.Done():
		if owner {
			s.scheduler.Cancel(requestID)
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
		}
		return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
	case <-call.done:
		response, resultErr := s.invocations.result(call)
		if resultErr != nil {
			return nil, normalizeExecutionError(resultErr)
		}
		return connect.NewResponse(response), nil
	}
}

func invocationFingerprint(request *edgev1.InvokeRequest) ([sha256.Size]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func quoteFingerprint(request *edgev1.QuoteRequest) ([sha256.Size]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (s *Service) GetTask(
	_ context.Context,
	request *connect.Request[edgev1.GetTaskRequest],
) (*connect.Response[edgev1.GetTaskResponse], error) {
	response, err := s.taskStore.GetTask(request.Msg, s.config.Now().UTC())
	if err != nil {
		if errors.Is(err, localrpc.ErrTaskConflict) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, invalidArgument(err)
	}
	return connect.NewResponse(response), nil
}

func (s *Service) Cancel(_ context.Context, request *connect.Request[edgev1.CancelRequest]) (*connect.Response[edgev1.CancelResponse], error) {
	if request.Msg == nil {
		return nil, invalidArgument(errors.New("empty cancellation"))
	}
	if err := validateID(request.Msg.RequestId); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateID(request.Msg.TaskId); err != nil ||
		!validRequestDigest(request.Msg.RequestDigest) {
		return nil, invalidArgument(errors.New("invalid cancellation identity"))
	}
	accepted := s.invocations.activeIdentity(
		request.Msg.RequestId, request.Msg.TaskId, request.Msg.RequestDigest,
	) && s.scheduler.Cancel(request.Msg.RequestId)
	return connect.NewResponse(&edgev1.CancelResponse{
		RequestId: request.Msg.RequestId, TaskId: request.Msg.TaskId,
		RequestDigest: request.Msg.RequestDigest, Accepted: accepted,
	}), nil
}

func validateBinding(binding quoteBinding, request *edgev1.InvokeRequest) error {
	if request.RequestId != binding.response.RequestId || request.ServiceId != binding.serviceID ||
		request.Operation != binding.operation || request.Model != binding.model ||
		request.Priority != binding.priority {
		return errors.New("invocation does not match quote")
	}
	if uint64(len(request.Payload)) > binding.inputBytes || request.MaxOutputBytes == 0 ||
		request.MaxOutputBytes > binding.maxOutputBytes ||
		request.DeadlineUnixMillis > binding.deadlineMillis {
		return errors.New("invocation exceeds quoted bounds")
	}
	return nil
}

func acceptsPriority(capability airuntime.Capability, priority airuntime.Priority) bool {
	for _, accepted := range capability.AcceptedPriorities {
		if priority == accepted {
			return true
		}
	}
	return false
}

func adapterKey(serviceID, operation, model string) string {
	return serviceID + "\x00" + operation + "\x00" + model
}

func validateID(id string) error {
	if len(id) < 8 || len(id) > 128 {
		return errors.New("request ID must contain 8..128 safe bytes")
	}
	for _, value := range id {
		if !(value >= 'a' && value <= 'z') && !(value >= 'A' && value <= 'Z') &&
			!(value >= '0' && value <= '9') && value != '-' && value != '_' &&
			value != '.' && value != ':' {
			return errors.New("request ID must contain 8..128 safe bytes")
		}
	}
	return nil
}

func validRequestDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 ||
		!strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validateSelector(serviceID, operation, model string) error {
	if !workerServiceIDPattern.MatchString(serviceID) {
		return errors.New("invalid capability selector")
	}
	for _, value := range []string{operation, model} {
		if value == "" || len(value) > airuntime.MaxCapabilityStringBytes ||
			strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return errors.New("invalid capability selector")
		}
	}
	return nil
}

func validGPUStatus(value string) bool {
	switch value {
	case "", "available", "degraded", "unavailable", "no-devices", "unknown":
		return true
	default:
		return false
	}
}

func validInitialResourceHealth(config Config) bool {
	if config.ResourceHealth == nil {
		return true
	}
	_, valid := observedResourceHealth(config.ResourceHealth)
	return valid
}

func validResourceHealth(health probe.ResourceHealth) bool {
	return (health.Status == "ready" || health.Status == "degraded") &&
		health.Ready == (health.Status == "ready") &&
		validGPUStatus(health.GPU)
}

func observedResourceHealth(
	provider probe.ResourceHealthProvider,
) (health probe.ResourceHealth, valid bool) {
	defer func() {
		if recover() != nil {
			health, valid = probe.ResourceHealth{}, false
		}
	}()
	health = provider.Health()
	return health, validResourceHealth(health)
}

func safeResourceHealth(provider probe.ResourceHealthProvider) probe.ResourceHealth {
	health, valid := observedResourceHealth(provider)
	if !valid {
		return probe.ResourceHealth{
			Ready: false, Status: "degraded", GPU: "unknown",
		}
	}
	return health
}

func (s *Service) currentResourceHealth() probe.ResourceHealth {
	if s.resourceHealth != nil {
		return safeResourceHealth(s.resourceHealth)
	}
	gpu := s.config.GPUStatus
	if gpu == "" {
		gpu = "unknown"
	}
	return probe.ResourceHealth{Ready: true, Status: "ready", GPU: gpu}
}

func randomID() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func (s *Service) capacityRevision() string {
	return s.capacityRevisionFor(
		s.admission.Snapshot(),
		s.currentTaskStoreCapacity(),
	)
}

func (s *Service) capacityRevisionFor(
	snapshot admission.Snapshot,
	tasks taskStoreCapacity,
) string {
	ready := 0
	for _, slot := range s.runtimeSlots {
		if slot.snapshot().ready {
			ready++
		}
	}
	resourcesReady := 0
	if s.currentResourceHealth().Ready {
		resourcesReady = 1
	}
	return fmt.Sprintf(
		"tier1-%d-%d-%d-%d-%d-%d-%d-%d-%d",
		resourcesReady, ready, snapshot.Running, snapshot.Reserved,
		tasks.Tasks, tasks.Capacity, tasks.OwnerReserved,
		tasks.OwnerTasks, tasks.AvailableExternal,
	)
}

func toWirePriority(priority airuntime.Priority) edgev1.Priority {
	return edgev1.Priority(priority)
}

func fromWirePriority(priority edgev1.Priority) (airuntime.Priority, error) {
	converted := airuntime.Priority(priority)
	if converted < airuntime.PriorityEmergency || converted > airuntime.PriorityBackground {
		return 0, errors.New("invalid priority")
	}
	return converted, nil
}

func invalidArgument(err error) *connect.Error {
	return connect.NewError(connect.CodeInvalidArgument, err)
}

func normalizeExecutionError(err error) *connect.Error {
	var runtimeError *airuntime.Error
	var preflightError *preflightFailure
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, scheduler.ErrCanceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, errResourcesUnavailable):
		return connect.NewError(connect.CodeUnavailable, errResourcesUnavailable)
	case errors.Is(err, scheduler.ErrQueueFull):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, admission.ErrQueueFull), errors.Is(err, admission.ErrCapacity),
		errors.Is(err, admission.ErrConcurrency),
		errors.Is(err, localrpc.ErrTaskCapacity):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("local capacity unavailable"))
	case errors.Is(err, localrpc.ErrTaskClosed),
		errors.Is(err, localrpc.ErrTaskCorrupt):
		return connect.NewError(connect.CodeUnavailable, errors.New("durable task store unavailable"))
	case errors.Is(err, admission.ErrStopped), errors.Is(err, scheduler.ErrStopped):
		return connect.NewError(connect.CodeUnavailable, errors.New("worker is draining"))
	case errors.As(err, &preflightError):
		return normalizePreflightError(airuntime.NewError(preflightError.kind, nil))
	case errors.As(err, &runtimeError):
		switch runtimeError.Kind {
		case airuntime.ErrorCanceled:
			return connect.NewError(connect.CodeCanceled, errors.New("runtime execution canceled"))
		case airuntime.ErrorTimeout:
			return connect.NewError(connect.CodeDeadlineExceeded, errors.New("runtime execution timed out"))
		case airuntime.ErrorInvalid:
			return connect.NewError(connect.CodeInvalidArgument, errors.New("runtime request rejected"))
		case airuntime.ErrorLimit:
			return connect.NewError(connect.CodeResourceExhausted, errors.New("runtime limit exceeded"))
		case airuntime.ErrorUnavailable:
			return connect.NewError(connect.CodeUnavailable, errors.New("runtime unavailable"))
		default:
			return connect.NewError(connect.CodeInternal, errors.New("runtime execution failed"))
		}
	default:
		return connect.NewError(connect.CodeInternal, errors.New("runtime execution failed"))
	}
}

func normalizeAdmissionError(err error) *connect.Error {
	switch {
	case errors.Is(err, admission.ErrLimit), errors.Is(err, admission.ErrPriority):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, admission.ErrStopped):
		return connect.NewError(connect.CodeUnavailable, errors.New("worker is draining"))
	case errors.Is(err, admission.ErrConflict):
		return connect.NewError(connect.CodeAlreadyExists, err)
	default:
		return connect.NewError(connect.CodeResourceExhausted, errors.New("local capacity unavailable"))
	}
}

func normalizePreflightError(err error) *connect.Error {
	switch airuntime.ErrorKindOf(err) {
	case airuntime.ErrorCanceled:
		return connect.NewError(connect.CodeCanceled, errors.New("runtime preflight canceled"))
	case airuntime.ErrorTimeout:
		return connect.NewError(connect.CodeUnavailable, errors.New("runtime preflight timed out"))
	case airuntime.ErrorLimit:
		return connect.NewError(connect.CodeResourceExhausted, errors.New("runtime preflight limit exceeded"))
	case airuntime.ErrorProtocol:
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("runtime model binding rejected"))
	case airuntime.ErrorUnavailable, airuntime.ErrorRemote:
		return connect.NewError(connect.CodeUnavailable, errors.New("runtime model is unavailable"))
	default:
		return connect.NewError(connect.CodeInternal, errors.New("runtime preflight failed"))
	}
}

func admissionClass(priority airuntime.Priority) (admission.Class, error) {
	switch priority {
	case airuntime.PriorityLocalAsync:
		return admission.ClassLocalAsync, nil
	case airuntime.PriorityExternalService:
		return admission.ClassExternalService, nil
	case airuntime.PriorityBackground:
		return admission.ClassBackground, nil
	default:
		return 0, errors.New("network invocation cannot claim emergency, control, or real-time priority")
	}
}

func admissionResources(capability airuntime.Capability, outputBytes uint64, available time.Duration) admission.Resources {
	resources := capability.Admission
	resources.OutputBytes = outputBytes
	if available > 0 && available < resources.ExecutionTime {
		resources.ExecutionTime = available
	}
	return resources
}

func executeAdapter(
	adapter airuntime.Adapter,
	capability airuntime.Capability,
	ctx context.Context,
	request airuntime.Request,
) (response airuntime.Response, err error) {
	defer func() {
		if recover() != nil {
			response = airuntime.Response{}
			err = airuntime.NewError(airuntime.ErrorInternal, nil)
		}
	}()
	response, err = adapter.Execute(ctx, request)
	if err != nil {
		return airuntime.Response{}, err
	}
	if uint64(len(response.Output)) > request.MaxOutputBytes ||
		response.Usage.OutputBytes > request.MaxOutputBytes ||
		response.ModelRevision != capability.ModelDigest ||
		response.RuntimeRevision != capability.RuntimeRevision {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	return response, nil
}

func (s *Service) BeginDrain() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.draining.CompareAndSwap(false, true) {
		s.admission.BeginDrain()
	}
}

func (s *Service) beginInvocationLifecycle() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.draining.Load() {
		return false
	}
	s.resultWG.Add(1)
	return true
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.BeginDrain()
	s.beginRuntimeStop()
	var resourceErr error
	if s.resourceHealth != nil {
		resourceErr = s.resourceHealth.Shutdown(ctx)
	}
	schedulerErr := s.scheduler.Shutdown(ctx)
	resultErr := s.waitInvocationResults(ctx)
	runtimeErr := s.waitRuntimeStop(ctx)
	s.admission.Shutdown()
	if resourceErr != nil || schedulerErr != nil || resultErr != nil ||
		runtimeErr != nil {
		return errors.Join(
			ErrShutdownIncomplete, resourceErr, schedulerErr, resultErr,
			runtimeErr,
		)
	}
	s.closeOnce.Do(func() {
		var closeErrors []error
		for _, adapter := range s.adapters {
			if closer, ok := adapter.(airuntime.AdapterCloser); ok {
				if err := closer.Close(); err != nil {
					closeErrors = append(closeErrors, errors.New("close runtime adapter"))
				}
			}
		}
		if err := s.taskStore.Close(); err != nil {
			closeErrors = append(closeErrors, errors.New("close Worker task store"))
		}
		s.closeErr = errors.Join(closeErrors...)
	})
	return s.closeErr
}

func (s *Service) waitInvocationResults(ctx context.Context) error {
	s.resultWaitOnce.Do(func() {
		go func() {
			s.resultWG.Wait()
			close(s.resultDone)
		}()
	})
	select {
	case <-s.resultDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) beginRuntimeStop() {
	s.stopOnce.Do(func() {
		for _, slot := range s.runtimeSlots {
			slot.stop()
		}
		if s.runtimeStop != nil {
			s.runtimeStop()
		}
		go func() {
			s.runtimeWG.Wait()
			close(s.runtimeDone)
		}()
	})
}

func (s *Service) waitRuntimeStop(ctx context.Context) error {
	select {
	case <-s.runtimeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
