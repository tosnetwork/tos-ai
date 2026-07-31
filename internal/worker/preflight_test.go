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
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

func TestRuntimeReadinessStartsUnknownAndRefreshes(t *testing.T) {
	service := newTestService(t)
	key := adapterKey("tos.ai.mock", "generate", "deterministic-echo")
	slot := service.runtimeSlots[key]
	now := time.Now().UTC()
	service.config.Now = func() time.Time { return now }
	slot.config.now = service.config.Now
	slot.config.successTTL = time.Second
	if readiness := service.Readiness(); readiness.Status != "starting" ||
		readiness.RuntimeReady != 0 || readiness.RuntimeTotal != 1 ||
		readiness.BindingEvidence != "unknown" {
		t.Fatalf("initial readiness = %#v", readiness)
	}
	readiness := service.RefreshRuntimes(context.Background())
	if readiness.Status != "ready" || readiness.RuntimeReady != 1 ||
		readiness.BindingEvidence != string(airuntime.BindingLocallyObserved) {
		t.Fatalf("refreshed readiness = %#v", readiness)
	}
	health, err := service.Health(context.Background(), connect.NewRequest(&edgev1.HealthRequest{}))
	if err != nil || !strings.Contains(health.Msg.Status, "runtimes=1/1") ||
		!strings.Contains(health.Msg.Status, "binding=locally-observed") {
		t.Fatalf("health=%v err=%v", health, err)
	}
	now = now.Add(time.Second)
	if stale := service.Readiness(); stale.Status != "degraded" ||
		stale.RuntimeReady != 0 || stale.BindingEvidence != "unknown" {
		t.Fatalf("stale readiness = %#v", stale)
	}
}

func TestRuntimeMonitorDetectsFailureAndRecoveryWithoutRPC(t *testing.T) {
	var available atomic.Bool
	var calls atomic.Int32
	service := newConfiguredFaultService(
		t, successfulFaultExecution,
		func(config *Config) {
			config.PreflightTimeout = 100 * time.Millisecond
			config.PreflightTTL = time.Second
			config.PreflightFailureTTL = 100 * time.Millisecond
			config.PreflightRefresh = MinPreflightRefresh
			config.PreflightWorkers = 1
		},
		func(adapter *faultAdapter) {
			adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
				calls.Add(1)
				if !available.Load() {
					return airuntime.Preflight{}, airuntime.NewError(
						airuntime.ErrorUnavailable, nil,
					)
				}
				return matchingPreflight(adapter.capability), nil
			}
		},
	)

	waitForRuntimeState(t, 2*time.Second, func() bool {
		return calls.Load() > 0 && service.Readiness().Status == "degraded"
	})
	available.Store(true)
	waitForRuntimeState(t, 2*time.Second, func() bool {
		return service.Readiness().Status == "ready"
	})
	available.Store(false)
	waitForRuntimeState(t, 2*time.Second, func() bool {
		return service.Readiness().Status == "degraded"
	})

	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := service.Shutdown(shutdownContext); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	stoppedAt := calls.Load()
	time.Sleep(MinPreflightRefresh + 100*time.Millisecond)
	if calls.Load() != stoppedAt {
		t.Fatalf("runtime monitor continued after shutdown: before=%d after=%d",
			stoppedAt, calls.Load())
	}
}

func TestForcedRuntimeRefreshConcurrencyIsBounded(t *testing.T) {
	const adapterCount = 5
	started := make(chan struct{}, adapterCount)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	var calls atomic.Int32
	adapters := make([]airuntime.Adapter, 0, adapterCount)
	for index := range adapterCount {
		capability := mock.New(0).Capability()
		capability.Model = fmt.Sprintf("bounded-model-%d", index)
		capability.ModelDigest = fmt.Sprintf("sha256:%064x", index+1)
		adapter := &faultAdapter{
			capability: capability, execute: successfulFaultExecution,
		}
		adapter.preflight = func(ctx context.Context) (airuntime.Preflight, error) {
			calls.Add(1)
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
				return matchingPreflight(capability), nil
			case <-ctx.Done():
				return airuntime.Preflight{}, ctx.Err()
			}
		}
		adapters = append(adapters, adapter)
	}
	taskScheduler, admissionController := newTestDependencies(t, adapterCount)
	config := testServiceConfig()
	config.PreflightWorkers = 2
	service, err := NewService(
		config, taskScheduler, admissionController, adapters,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeIfOpen(release)
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Shutdown(shutdownContext)
	})
	refreshDone := make(chan struct{})
	go func() {
		service.RefreshRuntimes(context.Background())
		close(refreshDone)
	}()
	for range config.PreflightWorkers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("bounded refresh workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("forced refresh exceeded configured concurrency")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("forced refresh did not complete")
	}
	if calls.Load() != adapterCount || maximum.Load() > int32(config.PreflightWorkers) {
		t.Fatalf("refresh calls=%d maximum concurrency=%d",
			calls.Load(), maximum.Load())
	}
}

func TestShutdownDoesNotCloseAdapterBeforePreflightStops(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, slot := faultRuntime(t, service)
	started := make(chan struct{})
	release := make(chan struct{})
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		close(started)
		<-release
		return matchingPreflight(adapter.capability), nil
	}
	refreshDone := make(chan struct{})
	go func() {
		service.RefreshRuntimes(context.Background())
		close(refreshDone)
	}()
	<-started
	shutdownContext, cancel := context.WithTimeout(
		context.Background(), 20*time.Millisecond,
	)
	err := service.Shutdown(shutdownContext)
	cancel()
	if !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("shutdown error=%v", err)
	}
	if adapter.closeCount.Load() != 0 {
		t.Fatal("adapter was closed while preflight was still running")
	}
	close(release)
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("late preflight did not finish")
	}
	shutdownContext, cancel = context.WithTimeout(context.Background(), time.Second)
	if err := service.Shutdown(shutdownContext); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if adapter.closeCount.Load() != 1 {
		t.Fatalf("adapter close count=%d", adapter.closeCount.Load())
	}
	if slot.snapshot().ready {
		t.Fatal("late success after lifecycle cancellation restored readiness")
	}
}

func waitForRuntimeState(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("runtime state did not converge before deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func closeIfOpen(channel chan struct{}) {
	select {
	case <-channel:
	default:
		close(channel)
	}
}

func TestQuoteFailsClosedOnModelBindingMismatchWithoutReservation(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, _ := faultRuntime(t, service)
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		return airuntime.Preflight{
			Model:          adapter.capability.Model,
			ModelDigest:    "sha256:" + strings.Repeat("f", 64),
			DigestEvidence: airuntime.BindingLocallyObserved,
		}, nil
	}
	if _, err := service.Quote(context.Background(), connect.NewRequest(
		preflightQuote(service, "binding-mismatch"),
	)); err == nil || connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("binding mismatch error = %v", err)
	}
	assertNoReservations(t, service)
}

func TestPreflightFailureCacheRecoversAfterBoundedTTL(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, slot := faultRuntime(t, service)
	now := time.Now().UTC()
	service.config.Now = func() time.Time { return now }
	slot.config.now = service.config.Now
	slot.config.failureTTL = time.Second
	var calls atomic.Int32
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		if calls.Add(1) == 1 {
			return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorUnavailable, nil)
		}
		return matchingPreflight(adapter.capability), nil
	}
	if _, err := service.Quote(context.Background(), connect.NewRequest(
		preflightQuote(service, "failure-cache-1"),
	)); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("first preflight error = %v", err)
	}
	if _, err := service.Quote(context.Background(), connect.NewRequest(
		preflightQuote(service, "failure-cache-2"),
	)); err == nil || calls.Load() != 1 {
		t.Fatalf("cached preflight error=%v calls=%d", err, calls.Load())
	}
	now = now.Add(time.Second + time.Millisecond)
	if _, err := service.Quote(context.Background(), connect.NewRequest(
		preflightQuote(service, "failure-cache-3"),
	)); err != nil || calls.Load() != 2 {
		t.Fatalf("recovered preflight error=%v calls=%d", err, calls.Load())
	}
}

func TestCapabilitiesHideUnavailableRuntimeAndRecover(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, slot := faultRuntime(t, service)
	now := time.Now().UTC()
	service.config.Now = func() time.Time { return now }
	slot.config.now = service.config.Now
	slot.config.failureTTL = time.Second
	available := false
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		if !available {
			return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorUnavailable, nil)
		}
		return matchingPreflight(adapter.capability), nil
	}
	first, err := service.GetCapabilities(context.Background(), connect.NewRequest(
		&edgev1.GetCapabilitiesRequest{},
	))
	if err != nil || len(first.Msg.Capabilities) != 0 ||
		service.Readiness().Status != "degraded" ||
		!strings.HasPrefix(first.Msg.CapacityRevision, "tier1-0-") {
		t.Fatalf("unavailable capabilities=%v readiness=%#v err=%v",
			first, service.Readiness(), err)
	}
	available = true
	now = now.Add(time.Second + time.Millisecond)
	second, err := service.GetCapabilities(context.Background(), connect.NewRequest(
		&edgev1.GetCapabilitiesRequest{},
	))
	if err != nil || len(second.Msg.Capabilities) != 1 ||
		service.Readiness().Status != "ready" ||
		!strings.HasPrefix(second.Msg.CapacityRevision, "tier1-1-") {
		t.Fatalf("recovered capabilities=%v readiness=%#v err=%v",
			second, service.Readiness(), err)
	}
}

func TestPreflightSingleflightAndWaitersStayBounded(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, slot := faultRuntime(t, service)
	slot.config.maxWaiters = 2
	slot.waiters = make(chan struct{}, 2)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return matchingPreflight(adapter.capability), nil
	}
	ownerResult := make(chan error, 1)
	go func() {
		_, err := slot.ensure(context.Background(), false)
		ownerResult <- err
	}()
	<-started

	const callers = 8
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := slot.ensure(context.Background(), false)
			results <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for len(slot.waiters) != cap(slot.waiters) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	expectedLimited := callers - cap(slot.waiters)
	for len(results) < expectedLimited && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(slot.waiters) != cap(slot.waiters) || len(results) < expectedLimited {
		t.Fatalf("waiters did not saturate: waiting=%d completed=%d",
			len(slot.waiters), len(results))
	}
	close(release)
	wait.Wait()
	close(results)
	if err := <-ownerResult; err != nil {
		t.Fatal(err)
	}
	limited, succeeded := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if airuntime.ErrorKindOf(err) == airuntime.ErrorLimit {
			limited++
		} else {
			t.Fatalf("unexpected waiter error = %v", err)
		}
	}
	if calls.Load() != 1 || succeeded > cap(slot.waiters) || limited == 0 {
		t.Fatalf("calls=%d succeeded=%d limited=%d", calls.Load(), succeeded, limited)
	}
}

func TestInvokeAuthoritativelyRechecksBindingBeforeAdmission(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, _ := faultRuntime(t, service)
	mismatch := false
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		result := matchingPreflight(adapter.capability)
		if mismatch {
			result.ModelDigest = "sha256:" + strings.Repeat("e", 64)
		}
		return result, nil
	}
	request := quotedInvocation(t, service, "invoke-binding-change", time.Now().Add(time.Minute))
	mismatch = true
	if _, err := service.Invoke(context.Background(), connect.NewRequest(request)); err == nil ||
		connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("invoke binding change error = %v", err)
	}
	request.Payload = []byte("changed")
	if _, err := service.Invoke(context.Background(), connect.NewRequest(request)); err == nil ||
		connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("failed-preflight request ID conflict = %v", err)
	}
	assertNoReservations(t, service)
}

func TestPreflightPanicIsRedactedAndCached(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, _ := faultRuntime(t, service)
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		panic("runtime endpoint and credential")
	}
	_, err := service.Quote(context.Background(), connect.NewRequest(
		preflightQuote(service, "preflight-panic"),
	))
	if err == nil || connect.CodeOf(err) != connect.CodeInternal ||
		strings.Contains(err.Error(), "credential") {
		t.Fatalf("preflight panic error = %v", err)
	}
}

func TestPreflightDeadlineIsBoundedAndCanceledWaitDoesNotMutateState(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, slot := faultRuntime(t, service)
	slot.config.timeout = 20 * time.Millisecond
	adapter.preflight = func(ctx context.Context) (airuntime.Preflight, error) {
		<-ctx.Done()
		return airuntime.Preflight{}, ctx.Err()
	}
	start := time.Now()
	if _, err := slot.ensure(context.Background(), false); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("preflight timeout error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("preflight exceeded hard timeout: %v", elapsed)
	}
	slot.mu.Lock()
	slot.checked = false
	slot.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := slot.ensure(ctx, false); airuntime.ErrorKindOf(err) != airuntime.ErrorCanceled {
		t.Fatalf("canceled preflight error = %v", err)
	}
	if slot.snapshot().checked {
		t.Fatal("caller cancellation mutated cached runtime state")
	}
}

func TestQuoteDisconnectDuringPreflightCreatesNoQuote(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, _ := faultRuntime(t, service)
	started := make(chan struct{})
	release := make(chan struct{})
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		close(started)
		<-release
		return matchingPreflight(adapter.capability), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := service.Quote(ctx, connect.NewRequest(
			preflightQuote(service, "preflight-disconnect"),
		))
		result <- err
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if err == nil || connect.CodeOf(err) != connect.CodeCanceled {
			t.Fatalf("disconnected quote error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected preflight did not return promptly")
	}
	close(release)
	service.quotes.mu.Lock()
	count := len(service.quotes.records)
	service.quotes.mu.Unlock()
	if count != 0 {
		t.Fatalf("disconnected preflight created %d quotes", count)
	}
}

func TestQuoteIdempotentReplayDoesNotDependOnLaterPreflight(t *testing.T) {
	service := newFaultService(t, successfulFaultExecution)
	adapter, slot := faultRuntime(t, service)
	request := preflightQuote(service, "preflight-replay")
	first, err := service.Quote(context.Background(), connect.NewRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorUnavailable, nil)
	}
	slot.mu.Lock()
	slot.checked = false
	slot.mu.Unlock()
	second, err := service.Quote(context.Background(), connect.NewRequest(request))
	if err != nil || second.Msg.QuoteId != first.Msg.QuoteId {
		t.Fatalf("replayed quote=%v err=%v", second, err)
	}
}

func successfulFaultExecution(
	_ context.Context,
	request airuntime.Request,
) (airuntime.Response, error) {
	return airuntime.Response{
		Output: append([]byte(nil), request.Payload...),
		Usage: airuntime.Usage{
			InputBytes: uint64(len(request.Payload)), OutputBytes: uint64(len(request.Payload)),
		},
		ModelRevision:   "sha256:c2ac77294809b3bc1093474bc5d552e6304ec76f03ae3c3e7c07f38e7528f197",
		RuntimeRevision: "mock-v1",
	}, nil
}

func faultRuntime(t *testing.T, service *Service) (*faultAdapter, *runtimeSlot) {
	t.Helper()
	key := adapterKey("tos.ai.mock", "generate", "deterministic-echo")
	adapter, ok := service.adapters[key].(*faultAdapter)
	if !ok {
		t.Fatal("fault adapter is unavailable")
	}
	slot := service.runtimeSlots[key]
	if slot == nil {
		t.Fatal("runtime slot is unavailable")
	}
	return adapter, slot
}

func matchingPreflight(capability airuntime.Capability) airuntime.Preflight {
	return airuntime.Preflight{
		Model: capability.Model, ModelDigest: capability.ModelDigest,
		DigestEvidence: airuntime.BindingLocallyObserved,
	}
}

func preflightQuote(service *Service, requestID string) *edgev1.QuoteRequest {
	return &edgev1.QuoteRequest{
		RequestId: requestID, ServiceId: "tos.ai.mock", Operation: "generate",
		Model: "deterministic-echo", InputBytes: 5, MaxOutputBytes: 16,
		DeadlineUnixMillis: service.config.Now().Add(time.Minute).UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
}

func TestPreflightContextErrorClassification(t *testing.T) {
	if airuntime.ErrorKindOf(preflightContextError(context.Canceled)) != airuntime.ErrorCanceled {
		t.Fatal("cancellation was not classified")
	}
	if airuntime.ErrorKindOf(preflightContextError(context.DeadlineExceeded)) != airuntime.ErrorTimeout {
		t.Fatal("deadline was not classified")
	}
	if !errors.Is(preflightContextError(context.Canceled), context.Canceled) {
		t.Fatal("cancellation cause was lost")
	}
}
