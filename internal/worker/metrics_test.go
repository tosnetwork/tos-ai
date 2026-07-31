package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	edgev1connect "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
)

func TestOperationalMetricsAreBoundedAndLowCardinality(t *testing.T) {
	service := newTestService(t)
	service.RefreshRuntimes(context.Background())
	metrics := NewOperationalMetrics()
	client := metricsTestClient(t, service, metrics)

	const calls = 64
	errorsFound := make(chan error, calls)
	var wait sync.WaitGroup
	for range calls {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := client.Health(
				context.Background(), connect.NewRequest(&edgev1.HealthRequest{}),
			)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{}),
	); err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid quote error=%v", err)
	}

	response, body := scrapeMetrics(t, metrics.Handler(service))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", response.Code, body)
	}
	if len(body) > MaxMetricsResponseBytes ||
		response.Header().Get("Content-Length") != strconv.Itoa(len(body)) ||
		response.Header().Get("Cache-Control") != "no-store" ||
		!strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain;") {
		t.Fatalf("invalid bounded metrics response: headers=%v bytes=%d",
			response.Header(), len(body))
	}
	for _, expected := range []string{
		"tos_ai_worker_ready 1\n",
		`tos_ai_worker_runtimes{state="ready"} 1` + "\n",
		`tos_ai_worker_admission_resource_capacity{resource="ram_bytes"} 1073741824` + "\n",
		`tos_ai_worker_rpc_requests_total{method="health",code="ok"} 64` + "\n",
		`tos_ai_worker_rpc_requests_total{method="quote",code="invalid_argument"} 1` + "\n",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing metric %q", expected)
		}
	}
	if got, want := strings.Count(
		body, "tos_ai_worker_rpc_requests_total{",
	), metricMethodCount*metricOutcomeCount; got != want {
		t.Fatalf("RPC series=%d want=%d", got, want)
	}
	for _, secretOrUnboundedLabel := range []string{
		"deterministic-echo", "sha256:", "request_id", "endpoint", "credential",
	} {
		if strings.Contains(body, secretOrUnboundedLabel) {
			t.Fatalf("metrics exposed forbidden value %q", secretOrUnboundedLabel)
		}
	}
	for _, line := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, `\"`) || !strings.Contains(line, " ") {
			t.Fatalf("invalid metrics line %q", line)
		}
	}
}

func TestOperationalMetricsTrackInFlightCancellationAndRelease(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	service := newFaultService(t, func(
		ctx context.Context,
		_ airuntime.Request,
	) (airuntime.Response, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return airuntime.Response{}, ctx.Err()
	})
	service.RefreshRuntimes(context.Background())
	metrics := NewOperationalMetrics()
	client := metricsTestClient(t, service, metrics)
	request := quotedInvocation(
		t, service, "metrics-cancel-request", time.Now().Add(time.Minute),
	)
	invokeContext, stopInvoke := context.WithCancel(context.Background())
	defer stopInvoke()
	invokeResult := make(chan error, 1)
	go func() {
		_, err := client.Invoke(invokeContext, connect.NewRequest(request))
		invokeResult <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("metrics test invocation did not start")
	}

	_, activeBody := scrapeMetrics(t, metrics.Handler(service))
	for _, expected := range []string{
		`tos_ai_worker_admission_tasks{state="running"} 1` + "\n",
		`tos_ai_worker_admission_tasks{state="waiting"} 0` + "\n",
		`tos_ai_worker_admission_tasks{state="reserved"} 1` + "\n",
		`tos_ai_worker_admission_resource_used{resource="ram_bytes"} 1048576` + "\n",
		`tos_ai_worker_rpc_in_flight{method="invoke"} 1` + "\n",
	} {
		if !strings.Contains(activeBody, expected) {
			t.Fatalf("active metrics missing %q", expected)
		}
	}
	cancelResponse, err := client.Cancel(
		context.Background(), connect.NewRequest(&edgev1.CancelRequest{
			RequestId: request.RequestId, TaskId: request.TaskId,
			RequestDigest: request.RequestDigest,
		}),
	)
	if err != nil || !cancelResponse.Msg.Accepted {
		t.Fatalf("cancel response=%v err=%v", cancelResponse, err)
	}
	if err := <-invokeResult; err == nil ||
		connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("invoke cancellation error=%v", err)
	}
	assertNoReservations(t, service)

	_, releasedBody := scrapeMetrics(t, metrics.Handler(service))
	for _, expected := range []string{
		`tos_ai_worker_admission_tasks{state="running"} 0` + "\n",
		`tos_ai_worker_admission_tasks{state="reserved"} 0` + "\n",
		`tos_ai_worker_admission_resource_used{resource="ram_bytes"} 0` + "\n",
		`tos_ai_worker_rpc_in_flight{method="invoke"} 0` + "\n",
		`tos_ai_worker_rpc_requests_total{method="invoke",code="canceled"} 1` + "\n",
		`tos_ai_worker_rpc_requests_total{method="cancel",code="ok"} 1` + "\n",
	} {
		if !strings.Contains(releasedBody, expected) {
			t.Fatalf("released metrics missing %q", expected)
		}
	}
}

func TestOperationalMetricsRejectAmbiguousScrapesAndSaturate(t *testing.T) {
	service := newTestService(t)
	metrics := NewOperationalMetrics()
	handler := metrics.Handler(service)
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, MetricsPath, strings.NewReader("x")),
		httptest.NewRequest(http.MethodGet, MetricsPath+"?labels=request-id", nil),
		httptest.NewRequest(http.MethodGet, MetricsPath, strings.NewReader("x")),
		func() *http.Request {
			request := httptest.NewRequest(http.MethodGet, MetricsPath, nil)
			request.TransferEncoding = []string{"chunked"}
			return request
		}(),
		httptest.NewRequest(http.MethodGet, "/other", nil),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed ||
			response.Header().Get("Allow") != http.MethodGet {
			t.Fatalf("ambiguous scrape status=%d headers=%v",
				response.Code, response.Header())
		}
	}
	response, _ := scrapeMetrics(t, metrics.Handler(nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil service status=%d", response.Code)
	}
	var unavailableMetrics *OperationalMetrics
	response, _ = scrapeMetrics(t, unavailableMetrics.Handler(service))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil metrics status=%d", response.Code)
	}

	counter := &metrics.rpc[metricMethodHealth].requests[0]
	counter.Store(^uint64(0))
	saturatingIncrement(counter)
	if value := counter.Load(); value != ^uint64(0) {
		t.Fatalf("saturated counter wrapped to %d", value)
	}
}

func TestMetricResourcesFailClosedOutsideCapacity(t *testing.T) {
	limit := admission.Resources{
		RAMBytes: 1, VRAMBytes: 1, KVCacheBytes: 1, ContextTokens: 1,
		BatchSize: 1, OutputBytes: 1, ExecutionTime: time.Nanosecond,
	}
	if !metricResourcesFit(limit, limit) {
		t.Fatal("valid resource boundary rejected")
	}
	invalid := []admission.Resources{
		{RAMBytes: 2},
		{VRAMBytes: 2},
		{KVCacheBytes: 2},
		{ContextTokens: 2},
		{BatchSize: 2},
		{OutputBytes: 2},
		{ExecutionTime: 2 * time.Nanosecond},
		{ExecutionTime: -time.Nanosecond},
	}
	for _, value := range invalid {
		if metricResourcesFit(value, limit) {
			t.Fatalf("out-of-capacity resources accepted: %+v", value)
		}
	}
}

func metricsTestClient(
	t *testing.T,
	service *Service,
	metrics *OperationalMetrics,
) edgev1connect.WorkerServiceClient {
	t.Helper()
	path, handler := edgev1connect.NewWorkerServiceHandler(
		service, connect.WithInterceptors(metrics.Interceptor()),
	)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return edgev1connect.NewWorkerServiceClient(server.Client(), server.URL)
}

func scrapeMetrics(
	t *testing.T,
	handler http.Handler,
) (*httptest.ResponseRecorder, string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, MetricsPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	result := response.Result()
	defer result.Body.Close()
	body, err := io.ReadAll(io.LimitReader(
		result.Body, MaxMetricsResponseBytes+1,
	))
	if err != nil {
		t.Fatal(err)
	}
	return response, string(body)
}
