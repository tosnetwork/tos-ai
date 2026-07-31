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
	"time"

	"connectrpc.com/connect"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Version        string
	QuoteTTL       time.Duration
	MaxQuotes      int
	MaxInvocations int
	MaxDeadline    time.Duration
	PriceNanoTOS   uint64
	Now            func() time.Time
}

type Service struct {
	config       Config
	scheduler    *scheduler.Scheduler
	adapters     map[string]airuntime.Adapter
	capabilities []airuntime.Capability
	quotes       *quoteStore
	invocations  *invocationStore
}

func NewService(config Config, taskScheduler *scheduler.Scheduler, adapters []airuntime.Adapter) (*Service, error) {
	if config.Version == "" || config.QuoteTTL <= 0 || config.MaxQuotes <= 0 ||
		config.MaxInvocations <= 0 || config.MaxDeadline <= 0 || taskScheduler == nil ||
		len(adapters) == 0 {
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
	}
	for _, adapter := range adapters {
		capability := adapter.Capability()
		key := adapterKey(capability.ServiceID, capability.Operation, capability.Model)
		if _, exists := service.adapters[key]; exists {
			return nil, fmt.Errorf("duplicate adapter capability %q", key)
		}
		if capability.MaxInputBytes == 0 || capability.MaxOutputBytes == 0 {
			return nil, fmt.Errorf("adapter %q has invalid bounds", key)
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
	return connect.NewResponse(&edgev1.HealthResponse{Status: "ok", Version: s.config.Version}), nil
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
	s.quotes.add(quoteID, quoteBinding{
		response:       response,
		serviceID:      input.ServiceId,
		operation:      input.Operation,
		model:          input.Model,
		inputBytes:     input.InputBytes,
		maxOutputBytes: input.MaxOutputBytes,
		deadlineMillis: input.DeadlineUnixMillis,
		priority:       input.Priority,
	})
	return connect.NewResponse(response), nil
}

func (s *Service) Invoke(ctx context.Context, request *connect.Request[edgev1.InvokeRequest]) (*connect.Response[edgev1.InvokeResponse], error) {
	input := request.Msg
	if err := validateID(input.RequestId); err != nil {
		return nil, invalidArgument(err)
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
		result, submitErr := s.scheduler.Submit(scheduler.Item{
			ID:       input.RequestId,
			Priority: priority,
			Deadline: time.UnixMilli(input.DeadlineUnixMillis),
			Context:  ctx,
			Work: func(runContext context.Context) (airuntime.Response, error) {
				return adapter.Execute(runContext, airuntime.Request{
					RequestID:      input.RequestId,
					Operation:      input.Operation,
					Model:          input.Model,
					Payload:        append([]byte(nil), input.Payload...),
					MaxOutputBytes: input.MaxOutputBytes,
				})
			},
		})
		if submitErr != nil {
			s.invocations.finish(call, nil, submitErr)
		} else {
			go func() {
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
	if len(id) < 8 || len(id) > 128 || strings.ContainsRune(id, '\x00') {
		return errors.New("request ID must contain 8..128 safe bytes")
	}
	return nil
}

func randomID() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func (s *Service) capacityRevision() string {
	return fmt.Sprintf("bootstrap-%d", len(s.capabilities))
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
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, scheduler.ErrCanceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, scheduler.ErrQueueFull):
		return connect.NewError(connect.CodeResourceExhausted, err)
	default:
		return connect.NewError(connect.CodeInternal, errors.New("runtime execution failed"))
	}
}
