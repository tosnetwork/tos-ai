package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"google.golang.org/protobuf/proto"
)

const (
	MaxQuotesHard      = 65536
	MaxInvocationsHard = 65536
	MaxAdaptersHard    = 64
	MaxQuoteTTLHard    = 5 * time.Minute
	MaxDeadlineHard    = time.Hour
)

type Config struct {
	Version        string
	QuoteTTL       time.Duration
	MaxQuotes      int
	MaxInvocations int
	MaxDeadline    time.Duration
	PriceNanoTOS   uint64
	Now            func() time.Time
	GPUStatus      string
}

type Service struct {
	config       Config
	scheduler    *scheduler.Scheduler
	adapters     map[string]airuntime.Adapter
	capabilities []airuntime.Capability
	quotes       *quoteStore
	invocations  *invocationStore
	admission    *admission.Controller
	draining     atomic.Bool
	closeOnce    sync.Once
	closeErr     error
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
		len(adapters) > MaxAdaptersHard {
		return nil, errors.New("invalid worker configuration")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	service := &Service{
		config:      config,
		scheduler:   taskScheduler,
		adapters:    make(map[string]airuntime.Adapter, len(adapters)),
		quotes:      newQuoteStore(config.MaxQuotes),
		invocations: newInvocationStore(config.MaxInvocations),
		admission:   admissionController,
	}
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, errors.New("nil runtime adapter")
		}
		capability := adapter.Capability()
		if err := airuntime.ValidateCapability(capability); err != nil {
			return nil, errors.New("runtime adapter has invalid capability")
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
		service.capabilities = append(service.capabilities, capability)
	}
	sort.Slice(service.capabilities, func(a, b int) bool {
		left, right := service.capabilities[a], service.capabilities[b]
		return adapterKey(left.ServiceID, left.Operation, left.Model) <
			adapterKey(right.ServiceID, right.Operation, right.Model)
	})
	if err := taskScheduler.Start(); err != nil {
		return nil, err
	}
	return service, nil
}

func (s *Service) Health(_ context.Context, _ *connect.Request[edgev1.HealthRequest]) (*connect.Response[edgev1.HealthResponse], error) {
	readiness := s.Readiness()
	status := fmt.Sprintf("%s;admission=%s;runtimes=%d;gpu=%s;running=%d;reserved=%d",
		readiness.Status, readiness.Admission, readiness.Runtimes, readiness.GPU,
		readiness.Running, readiness.Reserved)
	return connect.NewResponse(&edgev1.HealthResponse{Status: status, Version: s.config.Version}), nil
}

type Readiness struct {
	Status    string
	Admission string
	Runtimes  int
	GPU       string
	Running   int
	Reserved  int
}

func (s *Service) Readiness() Readiness {
	snapshot := s.admission.Snapshot()
	status, admissionStatus := "ready", "ready"
	if s.draining.Load() || !snapshot.Accepting {
		status, admissionStatus = "draining", "draining"
	}
	gpu := s.config.GPUStatus
	if gpu == "" {
		gpu = "unknown"
	}
	return Readiness{
		Status: status, Admission: admissionStatus, Runtimes: len(s.adapters),
		GPU: gpu, Running: snapshot.Running, Reserved: snapshot.Reserved,
	}
}

func (s *Service) GetCapabilities(_ context.Context, _ *connect.Request[edgev1.GetCapabilitiesRequest]) (*connect.Response[edgev1.GetCapabilitiesResponse], error) {
	response := &edgev1.GetCapabilitiesResponse{CapacityRevision: s.capacityRevision()}
	for _, capability := range s.capabilities {
		wire := &edgev1.Capability{
			ServiceId:       capability.ServiceID,
			Operation:       capability.Operation,
			Model:           capability.Model,
			ModelDigest:     capability.ModelDigest,
			Runtime:         capability.Runtime,
			RuntimeRevision: capability.RuntimeRevision,
			MaxInputBytes:   capability.MaxInputBytes,
			MaxOutputBytes:  capability.MaxOutputBytes,
		}
		for _, priority := range capability.AcceptedPriorities {
			wire.AcceptedPriorities = append(wire.AcceptedPriorities, toWirePriority(priority))
		}
		response.Capabilities = append(response.Capabilities, wire)
	}
	return connect.NewResponse(response), nil
}

func (s *Service) Quote(_ context.Context, request *connect.Request[edgev1.QuoteRequest]) (*connect.Response[edgev1.QuoteResponse], error) {
	input := request.Msg
	if err := validateID(input.RequestId); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateSelector(input.ServiceId, input.Operation, input.Model); err != nil {
		return nil, invalidArgument(err)
	}
	adapter := s.adapters[adapterKey(input.ServiceId, input.Operation, input.Model)]
	if adapter == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("capability not found"))
	}
	capability := adapter.Capability()
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
	class, err := admissionClass(priority)
	if err != nil {
		return nil, invalidArgument(err)
	}
	resources := admissionResources(capability, input.MaxOutputBytes, deadline.Sub(now))
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
		fingerprint:    fingerprint,
	})
	return connect.NewResponse(response), nil
}

func (s *Service) Invoke(ctx context.Context, request *connect.Request[edgev1.InvokeRequest]) (*connect.Response[edgev1.InvokeResponse], error) {
	input := request.Msg
	if err := validateID(input.RequestId); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateSelector(input.ServiceId, input.Operation, input.Model); err != nil {
		return nil, invalidArgument(err)
	}
	if err := validateID(input.QuoteId); err != nil {
		return nil, invalidArgument(errors.New("invalid quote ID"))
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
	binding, err := s.quotes.get(input.QuoteId, s.config.Now())
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if err := validateBinding(binding, input); err != nil {
		return nil, invalidArgument(err)
	}
	adapter := s.adapters[adapterKey(input.ServiceId, input.Operation, input.Model)]
	if adapter == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("capability not found"))
	}
	call, owner, err := s.invocations.begin(input.RequestId, fingerprint)
	if err != nil {
		if errors.Is(err, errInvocationConflict) {
			return nil, connect.NewError(connect.CodeAlreadyExists, err)
		}
		return nil, connect.NewError(connect.CodeResourceExhausted, err)
	}
	if owner {
		priority, _ := fromWirePriority(input.Priority)
		class, classErr := admissionClass(priority)
		if classErr != nil {
			s.invocations.finish(call, nil, classErr)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		capability := adapter.Capability()
		resources := admissionResources(capability, input.MaxOutputBytes, time.Until(time.UnixMilli(input.DeadlineUnixMillis)))
		reservation, reservationOwner, reserveErr := s.admission.Reserve(admission.Request{
			ID: input.RequestId, Fingerprint: fingerprint, Class: class, Resources: resources,
		})
		if reserveErr != nil {
			s.invocations.finish(call, nil, reserveErr)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		if !reservationOwner {
			s.invocations.finish(call, nil, admission.ErrConflict)
			return s.awaitInvocation(ctx, input.RequestId, call, true)
		}
		result, submitErr := s.scheduler.Submit(scheduler.Item{
			ID:       input.RequestId,
			Priority: priority,
			Deadline: time.UnixMilli(input.DeadlineUnixMillis),
			Context:  ctx,
			Work: func(runContext context.Context) (airuntime.Response, error) {
				if err := reservation.Start(); err != nil {
					return airuntime.Response{}, err
				}
				defer reservation.Release()
				return executeAdapter(adapter, runContext, airuntime.Request{
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
			s.invocations.finish(call, nil, submitErr)
		} else {
			go func() {
				defer reservation.Release()
				outcome := <-result
				if outcome.Err != nil {
					s.invocations.finish(call, nil, outcome.Err)
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
				s.invocations.finish(call, response, nil)
			}()
		}
	}
	return s.awaitInvocation(ctx, input.RequestId, call, owner)
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

func (s *Service) Cancel(_ context.Context, request *connect.Request[edgev1.CancelRequest]) (*connect.Response[edgev1.CancelResponse], error) {
	if err := validateID(request.Msg.RequestId); err != nil {
		return nil, invalidArgument(err)
	}
	return connect.NewResponse(&edgev1.CancelResponse{
		Accepted: s.scheduler.Cancel(request.Msg.RequestId),
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

func validateSelector(values ...string) error {
	for _, value := range values {
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

func randomID() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func (s *Service) capacityRevision() string {
	snapshot := s.admission.Snapshot()
	return fmt.Sprintf("tier1-%d-%d-%d", len(s.capabilities), snapshot.Running, snapshot.Reserved)
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
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, scheduler.ErrCanceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, scheduler.ErrQueueFull):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, admission.ErrQueueFull), errors.Is(err, admission.ErrCapacity),
		errors.Is(err, admission.ErrConcurrency):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("local capacity unavailable"))
	case errors.Is(err, admission.ErrStopped), errors.Is(err, scheduler.ErrStopped):
		return connect.NewError(connect.CodeUnavailable, errors.New("worker is draining"))
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

func executeAdapter(adapter airuntime.Adapter, ctx context.Context, request airuntime.Request) (response airuntime.Response, err error) {
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
	capability := adapter.Capability()
	if uint64(len(response.Output)) > request.MaxOutputBytes ||
		response.Usage.OutputBytes > request.MaxOutputBytes ||
		response.ModelRevision != capability.ModelDigest ||
		response.RuntimeRevision != capability.RuntimeRevision {
		return airuntime.Response{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	return response, nil
}

func (s *Service) BeginDrain() {
	if s.draining.CompareAndSwap(false, true) {
		s.admission.BeginDrain()
	}
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.BeginDrain()
	schedulerErr := s.scheduler.Shutdown(ctx)
	s.admission.Shutdown()
	s.closeOnce.Do(func() {
		for _, adapter := range s.adapters {
			if closer, ok := adapter.(airuntime.AdapterCloser); ok {
				if err := closer.Close(); err != nil {
					s.closeErr = errors.New("close runtime adapter")
				}
			}
		}
	})
	return errors.Join(schedulerErr, s.closeErr)
}
