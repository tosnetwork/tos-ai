package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/probe"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"google.golang.org/protobuf/proto"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	taskScheduler, admissionController := newTestDependencies(t, 4)
	service, err := NewService(testServiceConfig(t), taskScheduler, admissionController, []airuntime.Adapter{mock.New(0)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(
			context.Background(), time.Second,
		)
		defer cancel()
		_ = service.Shutdown(shutdownContext)
	})
	return service
}

func newTestDependencies(
	t *testing.T,
	maxQueue int,
) (*scheduler.Scheduler, *admission.Controller) {
	t.Helper()
	taskScheduler, err := scheduler.New(scheduler.Config{
		Workers: 1, MaxQueue: maxQueue,
	})
	if err != nil {
		t.Fatal(err)
	}
	admissionController, err := admission.New(admission.Config{
		MaxConcurrent: 1, MaxQueue: maxQueue,
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
	return taskScheduler, admissionController
}

func TestNewServiceRejectsUnsafeRuntimeMonitorConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
	}{
		{"refresh too frequent", func(config *Config) {
			config.PreflightRefresh = MinPreflightRefresh - time.Nanosecond
		}},
		{"refresh too slow", func(config *Config) {
			config.PreflightRefresh = MaxPreflightRefreshHard + time.Nanosecond
		}},
		{"no refresh workers", func(config *Config) {
			config.PreflightWorkers = 0
		}},
		{"too many refresh workers", func(config *Config) {
			config.PreflightWorkers = MaxPreflightWorkersHard + 1
		}},
		{"freshness gap", func(config *Config) {
			config.PreflightRefresh = config.PreflightTTL
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testServiceConfig(t)
			test.configure(&config)
			taskScheduler, admissionController := newTestDependencies(t, 2)
			if _, err := NewService(
				config, taskScheduler, admissionController,
				[]airuntime.Adapter{mock.New(0)},
			); err == nil {
				t.Fatal("unsafe runtime monitor configuration was accepted")
			}
		})
	}
}

func TestNewServiceRejectsTypedNilAndPanickingDependencies(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*Config)
		adapter   airuntime.Adapter
	}{
		{
			name: "typed-nil runtime adapter",
			adapter: func() airuntime.Adapter {
				var adapter *faultAdapter
				return adapter
			}(),
		},
		{
			name: "panicking runtime capability",
			adapter: &faultAdapter{
				panicCapability: true,
			},
		},
		{
			name: "typed-nil resource health",
			configure: func(config *Config) {
				var resources *mutableResourceHealth
				config.ResourceHealth = resources
			},
			adapter: mock.New(0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testServiceConfig(t)
			if test.configure != nil {
				test.configure(&config)
			}
			taskScheduler, admissionController := newTestDependencies(t, 2)
			service, err := NewService(
				config, taskScheduler, admissionController,
				[]airuntime.Adapter{test.adapter},
			)
			if err == nil || service != nil {
				t.Fatal("unsafe Worker dependency accepted")
			}
			shutdownContext, cancel := context.WithTimeout(
				context.Background(), time.Second,
			)
			defer cancel()
			_ = taskScheduler.Shutdown(shutdownContext)
			admissionController.Shutdown()
		})
	}
}

func TestPreflightScanLimitAccountsForEveryBoundedBatch(t *testing.T) {
	if got := preflightScanLimit(5, 2, time.Second); got != 3*time.Second {
		t.Fatalf("five adapters with two workers scan limit=%v", got)
	}
	if got := preflightScanLimit(
		MaxAdaptersHard, 4, 5*time.Second,
	); got != 80*time.Second {
		t.Fatalf("maximum adapter scan limit=%v", got)
	}
}

func testServiceConfig(t *testing.T) Config {
	t.Helper()
	return testServiceConfigAt(
		t, filepath.Join(t.TempDir(), "worker-tasks.db"),
	)
}

func testServiceConfigAt(t *testing.T, path string) Config {
	return testServiceConfigAtLimit(t, path, 64)
}

func testServiceConfigAtLimit(
	t *testing.T,
	path string,
	maxTasks int,
) Config {
	return testServiceConfigAtTaskCapacity(t, path, maxTasks, 0)
}

func testServiceConfigAtTaskCapacity(
	t *testing.T,
	path string,
	maxTasks int,
	ownerReserved int,
) Config {
	return testServiceConfigAtStorageCapacity(
		t, path, maxTasks, ownerReserved,
		localrpc.DefaultWorkerMaxRetainedBytes,
	)
}

func testServiceConfigAtStorageCapacity(
	t *testing.T,
	path string,
	maxTasks int,
	ownerReserved int,
	maxRetainedBytes uint64,
) Config {
	t.Helper()
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	storeConfig := localrpc.DefaultWorkerTaskStoreConfig(
		path,
	)
	storeConfig.MaxTasks = maxTasks
	storeConfig.OwnerReservedTasks = ownerReserved
	storeConfig.MaxRetainedBytes = maxRetainedBytes
	storeConfig.MaxInvocationDuration = time.Hour
	storeConfig.AllowedPriorities = []edgev1.Priority{
		edgev1.Priority_PRIORITY_LOCAL_ASYNC,
		edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		edgev1.Priority_PRIORITY_BACKGROUND,
	}
	store, err := localrpc.OpenWorkerTaskStore(storeConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return Config{
		Version:             "test",
		QuoteTTL:            time.Minute,
		MaxQuotes:           4,
		MaxInvocations:      4,
		MaxDeadline:         time.Hour,
		PreflightTimeout:    time.Second,
		PreflightTTL:        MaxPreflightTTLHard,
		PreflightFailureTTL: time.Second,
		MaxPreflightWaiters: 4,
		PreflightRefresh:    4 * time.Minute,
		PreflightWorkers:    2,
		Now:                 time.Now,
		GPUStatus:           "unavailable",
		TaskStore:           store,
	}
}

func bindTestInvocation(
	t *testing.T,
	request *edgev1.InvokeRequest,
) *edgev1.InvokeRequest {
	t.Helper()
	if request.TaskId == "" {
		request.TaskId = "task-" + request.RequestId
	}
	if request.RetainUntilUnixMillis == 0 {
		request.RetainUntilUnixMillis = time.UnixMilli(
			request.DeadlineUnixMillis,
		).Add(time.Hour).UnixMilli()
	}
	bound, _, err := localrpc.BindInvocationRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return bound
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
	invocation := bindTestInvocation(t, &edgev1.InvokeRequest{
		RequestId:          "request-0001",
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            []byte("hello"),
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	})
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
	invocation := bindTestInvocation(t, &edgev1.InvokeRequest{
		RequestId:          "request-0003",
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            []byte("first"),
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	})
	if _, err := service.Invoke(context.Background(), connect.NewRequest(invocation)); err != nil {
		t.Fatal(err)
	}
	invocation.Payload = []byte("second")
	invocation.RequestDigest = ""
	invocation = bindTestInvocation(t, invocation)
	if _, err := service.Invoke(context.Background(), connect.NewRequest(invocation)); err == nil ||
		connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("request ID content reuse error = %v", err)
	}
}

type faultAdapter struct {
	capability      airuntime.Capability
	preflight       func(context.Context) (airuntime.Preflight, error)
	execute         func(context.Context, airuntime.Request) (airuntime.Response, error)
	closeCount      atomic.Int32
	closeErr        error
	panicCapability bool
	panicClose      bool
}

func (a *faultAdapter) Capability() airuntime.Capability {
	if a.panicCapability {
		panic("runtime capability detail")
	}
	return a.capability
}
func (a *faultAdapter) Preflight(ctx context.Context) (airuntime.Preflight, error) {
	if a.preflight != nil {
		return a.preflight(ctx)
	}
	return airuntime.Preflight{
		Model: a.capability.Model, ModelDigest: a.capability.ModelDigest,
		DigestEvidence: airuntime.BindingLocallyObserved,
	}, nil
}
func (a *faultAdapter) Execute(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
	return a.execute(ctx, request)
}
func (a *faultAdapter) Close() error {
	a.closeCount.Add(1)
	if a.panicClose {
		panic("runtime close detail")
	}
	return a.closeErr
}

func newFaultService(t *testing.T, execute func(context.Context, airuntime.Request) (airuntime.Response, error)) *Service {
	return newConfiguredFaultService(t, execute, nil)
}

func newConfiguredFaultService(
	t *testing.T,
	execute func(context.Context, airuntime.Request) (airuntime.Response, error),
	configure func(*Config),
	configureAdapters ...func(*faultAdapter),
) *Service {
	t.Helper()
	capability := mock.New(0).Capability()
	adapter := &faultAdapter{capability: capability, execute: execute}
	for _, configureAdapter := range configureAdapters {
		if configureAdapter != nil {
			configureAdapter(adapter)
		}
	}
	taskScheduler, admissionController := newTestDependencies(t, 2)
	config := testServiceConfig(t)
	config.MaxQuotes = 8
	config.MaxInvocations = 8
	if configure != nil {
		configure(&config)
	}
	service, err := NewService(
		config, taskScheduler, admissionController,
		[]airuntime.Adapter{adapter},
	)
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
	return bindTestInvocation(t, &edgev1.InvokeRequest{
		RequestId: requestID, QuoteId: quote.Msg.QuoteId, ServiceId: "tos.ai.mock",
		Operation: "generate", Model: "deterministic-echo", Payload: []byte("hello"),
		MaxOutputBytes: 16, DeadlineUnixMillis: deadline.UnixMilli(),
		Priority: edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	})
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
	response, err := cancelService.Cancel(context.Background(), connect.NewRequest(
		&edgev1.CancelRequest{
			RequestId: cancelRequest.RequestId, TaskId: cancelRequest.TaskId,
			RequestDigest: cancelRequest.RequestDigest,
		},
	))
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

func TestOwnerWorkerRemainsAvailableThroughInvokeFlow(t *testing.T) {
	capability := mock.New(0).Capability()
	started := make(chan string, 3)
	externalRelease := make(chan struct{})
	localRelease := make(chan struct{})
	adapter := &faultAdapter{capability: capability}
	adapter.execute = func(
		ctx context.Context,
		request airuntime.Request,
	) (airuntime.Response, error) {
		started <- request.RequestID
		release := externalRelease
		if strings.HasPrefix(request.RequestID, "owner-local") {
			release = localRelease
		}
		select {
		case <-release:
			return airuntime.Response{
				Output: append([]byte(nil), request.Payload...),
				Usage: airuntime.Usage{
					InputBytes:  uint64(len(request.Payload)),
					OutputBytes: uint64(len(request.Payload)),
				},
				ModelRevision:   capability.ModelDigest,
				RuntimeRevision: capability.RuntimeRevision,
			}, nil
		case <-ctx.Done():
			return airuntime.Response{}, ctx.Err()
		}
	}
	taskScheduler, err := scheduler.New(scheduler.Config{
		Workers: 2, MaxQueue: 4, OwnerReservedWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	admissionController, err := admission.New(admission.Config{
		MaxConcurrent: 2, MaxQueue: 4,
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
	config := testServiceConfig(t)
	config.MaxQuotes = 8
	config.MaxInvocations = 8
	service, err := NewService(
		config, taskScheduler, admissionController, []airuntime.Adapter{adapter},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Shutdown(shutdownContext)
	})
	invocation := func(id string, priority edgev1.Priority) *edgev1.InvokeRequest {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		quote, quoteErr := service.Quote(
			context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
				RequestId: id, ServiceId: capability.ServiceID,
				Operation: capability.Operation, Model: capability.Model,
				InputBytes: 5, MaxOutputBytes: 16,
				DeadlineUnixMillis: deadline.UnixMilli(), Priority: priority,
			}),
		)
		if quoteErr != nil {
			t.Fatal(quoteErr)
		}
		return bindTestInvocation(t, &edgev1.InvokeRequest{
			RequestId: id, QuoteId: quote.Msg.QuoteId,
			ServiceId: capability.ServiceID, Operation: capability.Operation,
			Model: capability.Model, Payload: []byte("hello"),
			MaxOutputBytes: 16, DeadlineUnixMillis: deadline.UnixMilli(),
			Priority: priority,
		})
	}
	externalOne := invocation(
		"owner-external-one", edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	)
	externalTwo := invocation(
		"owner-external-two", edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	)
	results := make(chan error, 3)
	invoke := func(request *edgev1.InvokeRequest) {
		_, invokeErr := service.Invoke(
			context.Background(), connect.NewRequest(request),
		)
		results <- invokeErr
	}
	go invoke(externalOne)
	go invoke(externalTwo)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("external invocation did not start")
	}
	select {
	case id := <-started:
		t.Fatalf("external invocation %q consumed the owner worker", id)
	case <-time.After(50 * time.Millisecond):
	}
	local := invocation(
		"owner-local-task", edgev1.Priority_PRIORITY_LOCAL_ASYNC,
	)
	go invoke(local)
	select {
	case id := <-started:
		if id != local.RequestId {
			t.Fatalf("unexpected invocation started: %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("local invocation was starved by external saturation")
	}
	close(localRelease)
	if err := <-results; err != nil {
		t.Fatalf("local invocation error=%v", err)
	}
	close(externalRelease)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("external invocation error=%v", err)
		}
	}
	assertNoReservations(t, service)
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

func TestShutdownContainsMOCKDependencyPanics(t *testing.T) {
	t.Run("runtime close", func(t *testing.T) {
		service := newFaultService(t, func(
			context.Context, airuntime.Request,
		) (airuntime.Response, error) {
			return airuntime.Response{}, nil
		})
		adapter := service.adapters[adapterKey(
			"tos.ai.mock", "generate", "deterministic-echo",
		)].(*faultAdapter)
		adapter.panicClose = true
		if err := service.Shutdown(context.Background()); err == nil ||
			strings.Contains(err.Error(), "runtime close detail") {
			t.Fatalf("shutdown error=%v", err)
		}
		if adapter.closeCount.Load() != 1 {
			t.Fatalf("adapter close count=%d", adapter.closeCount.Load())
		}
	})

	t.Run("resource shutdown", func(t *testing.T) {
		resources := &mutableResourceHealth{
			health: probe.ResourceHealth{
				Ready: true, Status: "ready", GPU: "no-devices",
			},
			panicShutdown: true,
		}
		taskScheduler, admissionController := newTestDependencies(t, 2)
		config := testServiceConfig(t)
		config.ResourceHealth = resources
		service, err := NewService(
			config, taskScheduler, admissionController,
			[]airuntime.Adapter{mock.New(0)},
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Shutdown(context.Background()); !errors.Is(err, ErrShutdownIncomplete) ||
			strings.Contains(err.Error(), "resource shutdown detail") {
			t.Fatalf("first shutdown error=%v", err)
		}
		resources.panicShutdown = false
		if err := service.Shutdown(context.Background()); err != nil {
			t.Fatalf("retry shutdown error=%v", err)
		}
		if resources.shutdowns.Load() != 2 {
			t.Fatalf("resource shutdown count=%d", resources.shutdowns.Load())
		}
	})
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

type mutableResourceHealth struct {
	mu            sync.Mutex
	health        probe.ResourceHealth
	panicOnHealth bool
	panicShutdown bool
	shutdowns     atomic.Int32
}

func (h *mutableResourceHealth) Health() probe.ResourceHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.panicOnHealth {
		panic("resource backend failure")
	}
	return h.health
}

func (h *mutableResourceHealth) Shutdown(context.Context) error {
	h.shutdowns.Add(1)
	if h.panicShutdown {
		panic("resource shutdown detail")
	}
	return nil
}

func (h *mutableResourceHealth) set(value probe.ResourceHealth) {
	h.mu.Lock()
	h.health = value
	h.mu.Unlock()
}

func TestDynamicResourceHealthGatesNewWorkAndRecovers(t *testing.T) {
	resources := &mutableResourceHealth{health: probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "no-devices",
	}}
	taskScheduler, admissionController := newTestDependencies(t, 4)
	config := testServiceConfig(t)
	config.ResourceHealth = resources
	service, err := NewService(
		config, taskScheduler, admissionController,
		[]airuntime.Adapter{mock.New(0)},
	)
	if err != nil {
		t.Fatal(err)
	}
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			shutdownContext, cancel := context.WithTimeout(
				context.Background(), time.Second,
			)
			defer cancel()
			if err := service.Shutdown(shutdownContext); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Cleanup(shutdown)
	service.RefreshRuntimes(context.Background())

	deadline := time.Now().Add(time.Minute).UnixMilli()
	quoteRequest := &edgev1.QuoteRequest{
		RequestId: "resource-existing", ServiceId: "tos.ai.mock",
		Operation: "generate", Model: "deterministic-echo", InputBytes: 5,
		MaxOutputBytes: 16, DeadlineUnixMillis: deadline,
		Priority: edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
	quote, err := service.Quote(
		context.Background(), connect.NewRequest(quoteRequest),
	)
	if err != nil {
		t.Fatal(err)
	}
	resources.set(probe.ResourceHealth{
		Ready: false, Status: "degraded", GPU: "unavailable",
	})
	readiness := service.Readiness()
	if readiness.Status != "degraded" || readiness.Admission != "blocked" ||
		readiness.Resources != "degraded" || readiness.GPU != "unavailable" {
		t.Fatalf("degraded readiness=%#v", readiness)
	}
	health, err := service.Health(
		context.Background(), connect.NewRequest(&edgev1.HealthRequest{}),
	)
	if err != nil || !strings.Contains(health.Msg.Status, "resources=degraded") {
		t.Fatalf("degraded health=%v err=%v", health, err)
	}
	capabilities, err := service.GetCapabilities(
		context.Background(), connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
	)
	if err != nil || len(capabilities.Msg.Capabilities) != 0 ||
		!strings.HasPrefix(capabilities.Msg.CapacityRevision, "tier1-0-") {
		t.Fatalf("degraded capabilities=%v err=%v", capabilities, err)
	}
	replayedQuote, err := service.Quote(
		context.Background(), connect.NewRequest(quoteRequest),
	)
	if err != nil || replayedQuote.Msg.QuoteId != quote.Msg.QuoteId {
		t.Fatalf("idempotent quote replay=%v err=%v", replayedQuote, err)
	}
	newQuote := proto.Clone(quoteRequest).(*edgev1.QuoteRequest)
	newQuote.RequestId = "resource-new-quote"
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(newQuote),
	); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("new quote while degraded error=%v", err)
	}
	invocation := bindTestInvocation(t, &edgev1.InvokeRequest{
		RequestId: quoteRequest.RequestId, QuoteId: quote.Msg.QuoteId,
		ServiceId: quoteRequest.ServiceId, Operation: quoteRequest.Operation,
		Model: quoteRequest.Model, Payload: []byte("hello"), MaxOutputBytes: 16,
		DeadlineUnixMillis: deadline, Priority: quoteRequest.Priority,
	})
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(invocation),
	); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("new invocation while degraded error=%v", err)
	}
	assertNoReservations(t, service)

	resources.set(probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "no-devices",
	})
	response, err := service.Invoke(
		context.Background(), connect.NewRequest(invocation),
	)
	if err != nil || string(response.Msg.Output) != "hello" {
		t.Fatalf("recovered invocation=%v err=%v", response, err)
	}
	resources.set(probe.ResourceHealth{
		Ready: false, Status: "degraded", GPU: "unavailable",
	})
	replay, err := service.Invoke(
		context.Background(), connect.NewRequest(invocation),
	)
	if err != nil || string(replay.Msg.Output) != "hello" {
		t.Fatalf("completed replay while degraded=%v err=%v", replay, err)
	}
	assertNoReservations(t, service)
	shutdown()
	if resources.shutdowns.Load() != 1 {
		t.Fatalf("resource provider shutdowns=%d", resources.shutdowns.Load())
	}
}

func TestDynamicResourceDegradationDoesNotPreemptRunningWork(t *testing.T) {
	resources := &mutableResourceHealth{health: probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "no-devices",
	}}
	started := make(chan struct{})
	release := make(chan struct{})
	service := newConfiguredFaultService(
		t,
		func(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
			close(started)
			select {
			case <-release:
				return successfulFaultExecution(ctx, request)
			case <-ctx.Done():
				return airuntime.Response{}, ctx.Err()
			}
		},
		func(config *Config) { config.ResourceHealth = resources },
	)
	request := quotedInvocation(
		t, service, "resource-inflight", time.Now().Add(time.Minute),
	)
	type invocationResult struct {
		response *connect.Response[edgev1.InvokeResponse]
		err      error
	}
	resultChannel := make(chan invocationResult, 1)
	go func() {
		response, err := service.Invoke(
			context.Background(), connect.NewRequest(request),
		)
		resultChannel <- invocationResult{response: response, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("invocation did not start")
	}
	resources.set(probe.ResourceHealth{
		Ready: false, Status: "degraded", GPU: "unavailable",
	})
	if readiness := service.Readiness(); readiness.Admission != "blocked" ||
		readiness.Running != 1 {
		t.Fatalf("degraded in-flight readiness=%#v", readiness)
	}
	newQuote := &edgev1.QuoteRequest{
		RequestId: "resource-after-loss", ServiceId: request.ServiceId,
		Operation: request.Operation, Model: request.Model, InputBytes: 1,
		MaxOutputBytes: 1, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
		Priority: request.Priority,
	}
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(newQuote),
	); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("new quote during in-flight degradation error=%v", err)
	}
	select {
	case result := <-resultChannel:
		t.Fatalf("in-flight invocation was preempted: %#v", result)
	default:
	}
	close(release)
	result := <-resultChannel
	if result.err != nil || string(result.response.Msg.Output) != "hello" {
		t.Fatalf("in-flight completion=%v err=%v", result.response, result.err)
	}
	assertNoReservations(t, service)
}

func TestResourceHealthIsRecheckedAfterRuntimePreflight(t *testing.T) {
	resources := &mutableResourceHealth{health: probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "no-devices",
	}}
	var executions atomic.Int32
	service := newConfiguredFaultService(
		t,
		func(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
			executions.Add(1)
			return successfulFaultExecution(ctx, request)
		},
		func(config *Config) { config.ResourceHealth = resources },
	)
	adapter, slot := faultRuntime(t, service)
	degradeDuringPreflight := func(context.Context) (airuntime.Preflight, error) {
		resources.set(probe.ResourceHealth{
			Ready: false, Status: "degraded", GPU: "unavailable",
		})
		return matchingPreflight(adapter.capability), nil
	}
	adapter.preflight = degradeDuringPreflight
	slot.mu.Lock()
	slot.checked = false
	slot.mu.Unlock()
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "resource-quote-race", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo", InputBytes: 5,
			MaxOutputBytes:     16,
			DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("quote after resource loss error=%v", err)
	}

	resources.set(probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "no-devices",
	})
	adapter.preflight = func(context.Context) (airuntime.Preflight, error) {
		return matchingPreflight(adapter.capability), nil
	}
	slot.mu.Lock()
	slot.checked = false
	slot.mu.Unlock()
	request := quotedInvocation(
		t, service, "resource-invoke-race", time.Now().Add(time.Minute),
	)
	adapter.preflight = degradeDuringPreflight
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(request),
	); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("invoke after resource loss error=%v", err)
	}
	if executions.Load() != 0 {
		t.Fatalf("adapter executed %d times after resource loss", executions.Load())
	}
	assertNoReservations(t, service)
}

func TestResourceHealthProviderFailsClosed(t *testing.T) {
	resources := &mutableResourceHealth{health: probe.ResourceHealth{
		Ready: true, Status: "degraded", GPU: "unknown",
	}}
	taskScheduler, admissionController := newTestDependencies(t, 2)
	config := testServiceConfig(t)
	config.ResourceHealth = resources
	if _, err := NewService(
		config, taskScheduler, admissionController,
		[]airuntime.Adapter{mock.New(0)},
	); err == nil {
		t.Fatal("contradictory initial resource health was accepted")
	}
	resources.set(probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "unknown",
	})
	taskScheduler, admissionController = newTestDependencies(t, 2)
	service, err := NewService(
		config, taskScheduler, admissionController,
		[]airuntime.Adapter{mock.New(0)},
	)
	if err != nil {
		t.Fatal(err)
	}
	resources.set(probe.ResourceHealth{
		Ready: true, Status: "degraded", GPU: "unknown",
	})
	if service.Readiness().Admission != "blocked" {
		t.Fatalf("invalid provider readiness=%#v", service.Readiness())
	}
	resources.mu.Lock()
	resources.panicOnHealth = true
	resources.mu.Unlock()
	if service.Readiness().Admission != "blocked" {
		t.Fatalf("panicking provider readiness=%#v", service.Readiness())
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
