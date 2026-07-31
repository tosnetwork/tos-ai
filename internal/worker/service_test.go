package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	taskScheduler, err := scheduler.New(scheduler.Config{Workers: 1, MaxQueue: 4})
	if err != nil {
		t.Fatal(err)
	}
	admissionController, err := admission.New(admission.Config{
		MaxConcurrent: 1, MaxQueue: 4,
		Capacity: admission.Resources{
			RAMBytes: 1 << 30, VRAMBytes: 1 << 30, KVCacheBytes: 1 << 30,
			ContextTokens: 1 << 20, BatchSize: 64, OutputBytes: 1 << 30,
			ExecutionTime: time.Hour,
		},
		OwnerReserved: admission.Resources{},
		PerRequestMax: admission.Resources{
			RAMBytes: 1 << 30, VRAMBytes: 1 << 30, KVCacheBytes: 1 << 30,
			ContextTokens: 1 << 20, BatchSize: 64, OutputBytes: 1 << 30,
			ExecutionTime: time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(Config{
		Version:        "test",
		QuoteTTL:       time.Minute,
		MaxQuotes:      4,
		MaxInvocations: 4,
		MaxDeadline:    time.Hour,
		Now:            time.Now,
		GPUStatus:      "unavailable",
	}, taskScheduler, admissionController, []airuntime.Adapter{mock.New(0)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taskScheduler.Shutdown(context.Background()) })
	return service
}

func TestQuoteInvokeAndReplay(t *testing.T) {
	service := newTestService(t)
	deadline := time.Now().Add(time.Minute).UnixMilli()
	quote, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          "request-0001",
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         5,
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		t.Fatal(err)
	}
	invocation := &edgev1.InvokeRequest{
		RequestId:          "request-0001",
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            []byte("hello"),
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
	first, err := service.Invoke(context.Background(), connect.NewRequest(invocation))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Invoke(context.Background(), connect.NewRequest(invocation))
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Msg.Output) != "hello" || string(second.Msg.Output) != "hello" {
		t.Fatal("unexpected output")
	}
	first.Msg.Output[0] = 'X'
	if string(second.Msg.Output) != "hello" {
		t.Fatal("replay response was aliased")
	}
	service.config.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	third, err := service.Invoke(context.Background(), connect.NewRequest(invocation))
	if err != nil || string(third.Msg.Output) != "hello" {
		t.Fatalf("completed replay after quote expiry failed: response=%v err=%v", third, err)
	}
	assertNoReservations(t, service)
}

func TestQuoteRejectsRealtimePriorityForExternalAdapter(t *testing.T) {
	service := newTestService(t)
	_, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          "request-0002",
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         1,
		MaxOutputBytes:     1,
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_REALTIME_PERCEPTION,
	}))
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("realtime priority error = %v", err)
	}
}

func TestInvokeRejectsRequestIDReuseWithDifferentPayload(t *testing.T) {
	service := newTestService(t)
	deadline := time.Now().Add(time.Minute).UnixMilli()
	quote, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          "request-0003",
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         8,
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		t.Fatal(err)
	}
	invocation := &edgev1.InvokeRequest{
		RequestId:          "request-0003",
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            []byte("first"),
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
	if _, err := service.Invoke(context.Background(), connect.NewRequest(invocation)); err != nil {
		t.Fatal(err)
	}
	invocation.Payload = []byte("second")
	if _, err := service.Invoke(context.Background(), connect.NewRequest(invocation)); err == nil ||
		connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("request ID content reuse error = %v", err)
	}
}

type faultAdapter struct {
	capability airuntime.Capability
	execute    func(context.Context, airuntime.Request) (airuntime.Response, error)
	closeCount atomic.Int32
	closeErr   error
}

func (a *faultAdapter) Capability() airuntime.Capability { return a.capability }
func (a *faultAdapter) Execute(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
	return a.execute(ctx, request)
}
func (a *faultAdapter) Close() error {
	a.closeCount.Add(1)
	return a.closeErr
}

func newFaultService(t *testing.T, execute func(context.Context, airuntime.Request) (airuntime.Response, error)) *Service {
	t.Helper()
	capability := mock.New(0).Capability()
	adapter := &faultAdapter{capability: capability, execute: execute}
	taskScheduler, err := scheduler.New(scheduler.Config{Workers: 1, MaxQueue: 2})
	if err != nil {
		t.Fatal(err)
	}
	admissionController, err := admission.New(admission.Config{
		MaxConcurrent: 1, MaxQueue: 2,
		Capacity: admission.Resources{
			RAMBytes: 1 << 30, VRAMBytes: 1 << 30, KVCacheBytes: 1 << 30,
			ContextTokens: 1 << 20, BatchSize: 64, OutputBytes: 1 << 30,
			ExecutionTime: time.Hour,
		},
		PerRequestMax: admission.Resources{
			RAMBytes: 1 << 30, VRAMBytes: 1 << 30, KVCacheBytes: 1 << 30,
			ContextTokens: 1 << 20, BatchSize: 64, OutputBytes: 1 << 30,
			ExecutionTime: time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(Config{
		Version: "test", QuoteTTL: time.Minute, MaxQuotes: 8, MaxInvocations: 8,
		MaxDeadline: time.Hour, GPUStatus: "unavailable",
	}, taskScheduler, admissionController, []airuntime.Adapter{adapter})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Shutdown(shutdownContext)
	})
	return service
}

func quotedInvocation(t *testing.T, service *Service, requestID string, deadline time.Time) *edgev1.InvokeRequest {
	t.Helper()
	quote, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId: requestID, ServiceId: "tos.ai.mock", Operation: "generate",
		Model: "deterministic-echo", InputBytes: 5, MaxOutputBytes: 16,
		DeadlineUnixMillis: deadline.UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return &edgev1.InvokeRequest{
		RequestId: requestID, QuoteId: quote.Msg.QuoteId, ServiceId: "tos.ai.mock",
		Operation: "generate", Model: "deterministic-echo", Payload: []byte("hello"),
		MaxOutputBytes: 16, DeadlineUnixMillis: deadline.UnixMilli(),
		Priority: edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
}

func assertNoReservations(t *testing.T, service *Service) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := service.admission.Snapshot()
		if snapshot.Reserved == 0 && snapshot.Running == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reservation leaked: %#v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReservationReleasedAfterAdapterFailureAndPanic(t *testing.T) {
	tests := []struct {
		name    string
		execute func(context.Context, airuntime.Request) (airuntime.Response, error)
	}{
		{"adapter error", func(context.Context, airuntime.Request) (airuntime.Response, error) {
			return airuntime.Response{}, errors.New("adapter internal path /secret")
		}},
		{"panic", func(context.Context, airuntime.Request) (airuntime.Response, error) {
			panic("credential")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newFaultService(t, test.execute)
			requestID := "lifecycle-adapter-error"
			if test.name == "panic" {
				requestID = "lifecycle-panic"
			}
			request := quotedInvocation(t, service, requestID, time.Now().Add(time.Minute))
			_, err := service.Invoke(context.Background(), connect.NewRequest(request))
			if err == nil || connect.CodeOf(err) != connect.CodeInternal ||
				err.Error() == "adapter internal path /secret" {
				t.Fatalf("execution error = %v", err)
			}
			assertNoReservations(t, service)
		})
	}
}

func TestReservationReleasedAfterTimeoutCancelAndDisconnect(t *testing.T) {
	blocking := func(started chan<- struct{}) func(context.Context, airuntime.Request) (airuntime.Response, error) {
		var once sync.Once
		return func(ctx context.Context, _ airuntime.Request) (airuntime.Response, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return airuntime.Response{}, ctx.Err()
		}
	}

	timeoutStarted := make(chan struct{})
	timeoutService := newFaultService(t, blocking(timeoutStarted))
	timeoutRequest := quotedInvocation(t, timeoutService, "timeout-request", time.Now().Add(25*time.Millisecond))
	_, timeoutErr := timeoutService.Invoke(context.Background(), connect.NewRequest(timeoutRequest))
	if timeoutErr == nil || connect.CodeOf(timeoutErr) != connect.CodeDeadlineExceeded {
		t.Fatalf("timeout error = %v", timeoutErr)
	}
	assertNoReservations(t, timeoutService)

	cancelStarted := make(chan struct{})
	cancelService := newFaultService(t, blocking(cancelStarted))
	cancelRequest := quotedInvocation(t, cancelService, "cancel-request", time.Now().Add(time.Minute))
	cancelResult := make(chan error, 1)
	go func() {
		_, err := cancelService.Invoke(context.Background(), connect.NewRequest(cancelRequest))
		cancelResult <- err
	}()
	<-cancelStarted
	response, err := cancelService.Cancel(context.Background(), connect.NewRequest(&edgev1.CancelRequest{RequestId: cancelRequest.RequestId}))
	if err != nil || !response.Msg.Accepted {
		t.Fatalf("cancel response=%v err=%v", response, err)
	}
	if err := <-cancelResult; err == nil || connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("cancel invoke error = %v", err)
	}
	assertNoReservations(t, cancelService)

	disconnectStarted := make(chan struct{})
	disconnectService := newFaultService(t, blocking(disconnectStarted))
	disconnectRequest := quotedInvocation(t, disconnectService, "disconnect-request", time.Now().Add(time.Minute))
	ctx, disconnect := context.WithCancel(context.Background())
	disconnectResult := make(chan error, 1)
	go func() {
		_, err := disconnectService.Invoke(ctx, connect.NewRequest(disconnectRequest))
		disconnectResult <- err
	}()
	<-disconnectStarted
	disconnect()
	if err := <-disconnectResult; err == nil || connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("disconnect error = %v", err)
	}
	assertNoReservations(t, disconnectService)
}

func TestShutdownDrainsAndReleasesReservation(t *testing.T) {
	started := make(chan struct{})
	service := newFaultService(t, func(ctx context.Context, _ airuntime.Request) (airuntime.Response, error) {
		close(started)
		<-ctx.Done()
		return airuntime.Response{}, ctx.Err()
	})
	request := quotedInvocation(t, service, "shutdown-request", time.Now().Add(time.Minute))
	result := make(chan error, 1)
	go func() {
		_, err := service.Invoke(context.Background(), connect.NewRequest(request))
		result <- err
	}()
	<-started
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil || connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("shutdown invoke error = %v", err)
	}
	assertNoReservations(t, service)
	if service.Readiness().Status != "draining" {
		t.Fatalf("readiness = %#v", service.Readiness())
	}
}

func TestShutdownClosesRuntimeOnceAndRedactsCloseError(t *testing.T) {
	service := newFaultService(t, func(context.Context, airuntime.Request) (airuntime.Response, error) {
		return airuntime.Response{}, nil
	})
	adapter := service.adapters[adapterKey("tos.ai.mock", "generate", "deterministic-echo")].(*faultAdapter)
	adapter.closeErr = errors.New("credential and endpoint must not leak")
	if err := service.Shutdown(context.Background()); err == nil ||
		strings.Contains(err.Error(), "credential") {
		t.Fatalf("shutdown error = %v", err)
	}
	if err := service.Shutdown(context.Background()); err == nil {
		t.Fatal("stable adapter close error was lost")
	}
	if adapter.closeCount.Load() != 1 {
		t.Fatalf("adapter close count = %d", adapter.closeCount.Load())
	}
}

func TestQuoteRetryIsIdempotentAndConflictsOnChangedContent(t *testing.T) {
	service := newTestService(t)
	request := &edgev1.QuoteRequest{
		RequestId: "quote-retry", ServiceId: "tos.ai.mock", Operation: "generate",
		Model: "deterministic-echo", InputBytes: 5, MaxOutputBytes: 16,
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
	first, err := service.Quote(context.Background(), connect.NewRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Quote(context.Background(), connect.NewRequest(request))
	if err != nil || first.Msg.QuoteId != second.Msg.QuoteId {
		t.Fatalf("quote retry first=%v second=%v err=%v", first, second, err)
	}
	request.MaxOutputBytes--
	if _, err := service.Quote(context.Background(), connect.NewRequest(request)); err == nil ||
		connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("quote conflict error = %v", err)
	}
}

func TestReplayAndQuoteMapsRemainBounded(t *testing.T) {
	service := newTestService(t)
	for index := 0; index < 20; index++ {
		requestID := fmt.Sprintf("bounded-%03d", index)
		request := quotedInvocation(t, service, requestID, time.Now().Add(time.Minute))
		if _, err := service.Invoke(context.Background(), connect.NewRequest(request)); err != nil {
			t.Fatal(err)
		}
	}
	service.invocations.mu.Lock()
	invocations := len(service.invocations.records)
	service.invocations.mu.Unlock()
	service.quotes.mu.Lock()
	quotes := len(service.quotes.records)
	requests := len(service.quotes.requests)
	service.quotes.mu.Unlock()
	if invocations > 4 || quotes > 4 || requests > 4 {
		t.Fatalf("bounded stores grew: invocations=%d quotes=%d requests=%d", invocations, quotes, requests)
	}
}
