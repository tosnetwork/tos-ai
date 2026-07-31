package worker

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	edgev1connect "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
)

const (
	// MetricsPath is served only by the worker's private Unix-socket HTTP server.
	MetricsPath = "/metrics"
	// MaxMetricsResponseBytes is a hard bound on one encoded metrics snapshot.
	MaxMetricsResponseBytes = 16 << 10
)

const (
	metricMethodHealth = iota
	metricMethodCapabilities
	metricMethodQuote
	metricMethodInvoke
	metricMethodGetTask
	metricMethodCancel
	metricMethodCount
)

const metricOutcomeCount = int(connect.CodeUnauthenticated) + 1

var metricMethodNames = [metricMethodCount]string{
	"health", "capabilities", "quote", "invoke", "get_task", "cancel",
}

var metricOutcomeNames = [metricOutcomeCount]string{
	"ok",
	"canceled",
	"unknown",
	"invalid_argument",
	"deadline_exceeded",
	"not_found",
	"already_exists",
	"permission_denied",
	"resource_exhausted",
	"failed_precondition",
	"aborted",
	"out_of_range",
	"unimplemented",
	"internal",
	"unavailable",
	"data_loss",
	"unauthenticated",
}

type rpcMetric struct {
	inFlight atomic.Int64
	requests [metricOutcomeCount]atomic.Uint64
}

// OperationalMetrics records a fixed set of low-cardinality worker RPC
// counters. It never retains request IDs, selectors, endpoints, or errors.
type OperationalMetrics struct {
	rpc [metricMethodCount]rpcMetric
}

func NewOperationalMetrics() *OperationalMetrics {
	return &OperationalMetrics{}
}

// Interceptor records only the six protocol-defined unary worker methods.
// Unknown procedures pass through without creating a new metric series.
func (m *OperationalMetrics) Interceptor() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(
			ctx context.Context,
			request connect.AnyRequest,
		) (response connect.AnyResponse, err error) {
			method, observed := metricMethod(request.Spec().Procedure)
			if m == nil || !observed {
				return next(ctx, request)
			}
			metric := &m.rpc[method]
			metric.inFlight.Add(1)
			defer func() {
				metric.inFlight.Add(-1)
				if recovered := recover(); recovered != nil {
					saturatingIncrement(
						&metric.requests[int(connect.CodeInternal)],
					)
					panic(recovered)
				}
				saturatingIncrement(&metric.requests[metricOutcome(err)])
			}()
			return next(ctx, request)
		}
	})
}

func metricMethod(procedure string) (int, bool) {
	switch procedure {
	case edgev1connect.WorkerServiceHealthProcedure:
		return metricMethodHealth, true
	case edgev1connect.WorkerServiceGetCapabilitiesProcedure:
		return metricMethodCapabilities, true
	case edgev1connect.WorkerServiceQuoteProcedure:
		return metricMethodQuote, true
	case edgev1connect.WorkerServiceInvokeProcedure:
		return metricMethodInvoke, true
	case edgev1connect.WorkerServiceGetTaskProcedure:
		return metricMethodGetTask, true
	case edgev1connect.WorkerServiceCancelProcedure:
		return metricMethodCancel, true
	default:
		return 0, false
	}
}

func metricOutcome(err error) int {
	if err == nil {
		return 0
	}
	code := int(connect.CodeOf(err))
	if code <= 0 || code >= metricOutcomeCount {
		return int(connect.CodeUnknown)
	}
	return code
}

func saturatingIncrement(value *atomic.Uint64) {
	const maximum = ^uint64(0)
	for {
		current := value.Load()
		if current == maximum || value.CompareAndSwap(current, current+1) {
			return
		}
	}
}

// Handler returns a bounded Prometheus text endpoint. The caller must mount it
// only on the worker's private Unix-socket server.
func (m *OperationalMetrics) Handler(service *Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodGet || request.URL.Path != MetricsPath ||
			request.URL.RawQuery != "" || request.ContentLength != 0 ||
			len(request.TransferEncoding) != 0 {
			writer.Header().Set("Allow", http.MethodGet)
			http.Error(writer, "metrics request rejected", http.StatusMethodNotAllowed)
			return
		}
		readiness, admissionSnapshot, ok := safeMetricsSnapshot(service)
		if m == nil || !ok {
			http.Error(writer, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		encoded := m.render(readiness, admissionSnapshot)
		if len(encoded) == 0 || len(encoded) > MaxMetricsResponseBytes {
			http.Error(writer, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set(
			"Content-Type", "text/plain; version=0.0.4; charset=utf-8",
		)
		writer.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(encoded)
	})
}

func safeMetricsSnapshot(
	service *Service,
) (readiness Readiness, snapshot admission.Snapshot, valid bool) {
	defer func() {
		if recover() != nil {
			readiness, snapshot, valid = Readiness{}, admission.Snapshot{}, false
		}
	}()
	if service == nil {
		return Readiness{}, admission.Snapshot{}, false
	}
	snapshot = service.admission.Snapshot()
	readiness = service.readiness(
		snapshot,
		service.currentTaskStoreCapacity(),
	)
	valid = readiness.RuntimeReady >= 0 && readiness.RuntimeTotal >= 0 &&
		readiness.RuntimeReady <= readiness.RuntimeTotal && snapshot.Running >= 0 &&
		snapshot.Reserved >= snapshot.Running && snapshot.MaxRunning > 0 &&
		snapshot.MaxQueue >= 0 && snapshot.Running <= snapshot.MaxRunning &&
		snapshot.Reserved-snapshot.Running <= snapshot.MaxQueue &&
		metricResourcesFit(snapshot.Used, snapshot.Capacity) &&
		metricResourcesFit(snapshot.OwnerReserved, snapshot.Capacity) &&
		readiness.TaskSlots <= readiness.TaskCapacity &&
		readiness.TaskCapacity > 0
	return readiness, snapshot, valid
}

func metricResourcesFit(value, limit admission.Resources) bool {
	return value.RAMBytes <= limit.RAMBytes &&
		value.VRAMBytes <= limit.VRAMBytes &&
		value.KVCacheBytes <= limit.KVCacheBytes &&
		value.ContextTokens <= limit.ContextTokens &&
		value.BatchSize <= limit.BatchSize &&
		value.OutputBytes <= limit.OutputBytes &&
		value.ExecutionTime >= 0 && value.ExecutionTime <= limit.ExecutionTime
}

func (m *OperationalMetrics) render(
	readiness Readiness,
	snapshot admission.Snapshot,
) []byte {
	encoded := make([]byte, 0, 12<<10)
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_ready", "gauge",
		"Whether the worker is ready to accept and execute new work.",
	)
	encoded = appendMetric(encoded, "tos_ai_worker_ready", boolMetric(
		readiness.Status == "ready",
	))
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_draining", "gauge",
		"Whether the worker is draining and rejects new work.",
	)
	encoded = appendMetric(encoded, "tos_ai_worker_draining", boolMetric(
		readiness.Status == "draining",
	))
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_resources_ready", "gauge",
		"Whether the bounded local resource health guard is ready.",
	)
	encoded = appendMetric(encoded, "tos_ai_worker_resources_ready", boolMetric(
		readiness.Resources == "ready",
	))
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_admission_ready", "gauge",
		"Whether local admission currently accepts new work.",
	)
	encoded = appendMetric(encoded, "tos_ai_worker_admission_ready", boolMetric(
		readiness.Admission == "ready",
	))
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_task_store_ready", "gauge",
		"Whether the durable task store has capacity for a new task.",
	)
	encoded = appendMetric(encoded, "tos_ai_worker_task_store_ready", boolMetric(
		readiness.TaskStore == "ready",
	))
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_task_store_tasks", "gauge",
		"Durable task identities currently retained.",
	)
	encoded = appendMetric(
		encoded, "tos_ai_worker_task_store_tasks", readiness.TaskSlots,
	)
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_task_store_capacity", "gauge",
		"Maximum durable task identities retained by policy.",
	)
	encoded = appendMetric(
		encoded, "tos_ai_worker_task_store_capacity", readiness.TaskCapacity,
	)
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_task_store_available", "gauge",
		"Durable task slots currently available.",
	)
	encoded = appendMetric(
		encoded,
		"tos_ai_worker_task_store_available",
		readiness.TaskCapacity-readiness.TaskSlots,
	)
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_runtimes", "gauge",
		"Configured model runtimes by fixed readiness state.",
	)
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_runtimes", `state="ready"`,
		uint64(readiness.RuntimeReady),
	)
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_runtimes", `state="total"`,
		uint64(readiness.RuntimeTotal),
	)

	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_admission_tasks", "gauge",
		"Locally admitted tasks by fixed lifecycle state.",
	)
	waiting := snapshot.Reserved - snapshot.Running
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_admission_tasks", `state="running"`,
		uint64(snapshot.Running),
	)
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_admission_tasks", `state="waiting"`,
		uint64(waiting),
	)
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_admission_tasks", `state="reserved"`,
		uint64(snapshot.Reserved),
	)
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_admission_limit", "gauge",
		"Administrator-configured admission task limits.",
	)
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_admission_limit", `kind="running"`,
		uint64(snapshot.MaxRunning),
	)
	encoded = appendLabeledMetric(
		encoded, "tos_ai_worker_admission_limit", `kind="queue"`,
		uint64(snapshot.MaxQueue),
	)
	encoded = appendAdmissionResources(encoded, snapshot)

	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_rpc_in_flight", "gauge",
		"Worker RPC handlers currently in flight by fixed method.",
	)
	for method, name := range metricMethodNames {
		inFlight := m.rpc[method].inFlight.Load()
		if inFlight < 0 {
			inFlight = 0
		}
		encoded = appendLabeledMetric(
			encoded, "tos_ai_worker_rpc_in_flight", `method="`+name+`"`,
			uint64(inFlight),
		)
	}
	encoded = appendMetricHeader(
		encoded, "tos_ai_worker_rpc_requests_total", "counter",
		"Completed worker RPC calls by fixed method and Connect outcome.",
	)
	for method, methodName := range metricMethodNames {
		for outcome, outcomeName := range metricOutcomeNames {
			encoded = appendLabeledMetric(
				encoded, "tos_ai_worker_rpc_requests_total",
				`method="`+methodName+`",code="`+outcomeName+`"`,
				m.rpc[method].requests[outcome].Load(),
			)
		}
	}
	return encoded
}

type admissionResourceMetric struct {
	name          string
	used          uint64
	capacity      uint64
	ownerReserved uint64
}

func appendAdmissionResources(
	encoded []byte,
	snapshot admission.Snapshot,
) []byte {
	resources := [...]admissionResourceMetric{
		{"ram_bytes", snapshot.Used.RAMBytes, snapshot.Capacity.RAMBytes,
			snapshot.OwnerReserved.RAMBytes},
		{"vram_bytes", snapshot.Used.VRAMBytes, snapshot.Capacity.VRAMBytes,
			snapshot.OwnerReserved.VRAMBytes},
		{"kv_cache_bytes", snapshot.Used.KVCacheBytes, snapshot.Capacity.KVCacheBytes,
			snapshot.OwnerReserved.KVCacheBytes},
		{"context_tokens", snapshot.Used.ContextTokens, snapshot.Capacity.ContextTokens,
			snapshot.OwnerReserved.ContextTokens},
		{"batch_size", uint64(snapshot.Used.BatchSize), uint64(snapshot.Capacity.BatchSize),
			uint64(snapshot.OwnerReserved.BatchSize)},
		{"output_bytes", snapshot.Used.OutputBytes, snapshot.Capacity.OutputBytes,
			snapshot.OwnerReserved.OutputBytes},
	}
	for _, family := range [...]struct {
		name string
		help string
		get  func(admissionResourceMetric) uint64
	}{
		{
			"tos_ai_worker_admission_resource_used",
			"Resources held by active local reservations.",
			func(value admissionResourceMetric) uint64 { return value.used },
		},
		{
			"tos_ai_worker_admission_resource_capacity",
			"Administrator-configured aggregate admission resource capacity.",
			func(value admissionResourceMetric) uint64 { return value.capacity },
		},
		{
			"tos_ai_worker_admission_resource_owner_reserved",
			"Admission resources unavailable to external and background work.",
			func(value admissionResourceMetric) uint64 { return value.ownerReserved },
		},
	} {
		encoded = appendMetricHeader(encoded, family.name, "gauge", family.help)
		for _, resource := range resources {
			encoded = appendLabeledMetric(
				encoded, family.name, `resource="`+resource.name+`"`,
				family.get(resource),
			)
		}
	}
	return encoded
}

func appendMetricHeader(
	encoded []byte,
	name string,
	typeName string,
	help string,
) []byte {
	encoded = append(encoded, "# HELP "...)
	encoded = append(encoded, name...)
	encoded = append(encoded, ' ')
	encoded = append(encoded, help...)
	encoded = append(encoded, '\n')
	encoded = append(encoded, "# TYPE "...)
	encoded = append(encoded, name...)
	encoded = append(encoded, ' ')
	encoded = append(encoded, typeName...)
	return append(encoded, '\n')
}

func appendMetric(encoded []byte, name string, value uint64) []byte {
	encoded = append(encoded, name...)
	encoded = append(encoded, ' ')
	encoded = strconv.AppendUint(encoded, value, 10)
	return append(encoded, '\n')
}

func appendLabeledMetric(
	encoded []byte,
	name string,
	labels string,
	value uint64,
) []byte {
	encoded = append(encoded, name...)
	encoded = append(encoded, '{')
	encoded = append(encoded, labels...)
	encoded = append(encoded, '}', ' ')
	encoded = strconv.AppendUint(encoded, value, 10)
	return append(encoded, '\n')
}

func boolMetric(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}
