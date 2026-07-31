package worker

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestAIResourceDimensionsAndRequestedUpperBounds(t *testing.T) {
	resources := admission.Resources{
		RAMBytes: 1, VRAMBytes: 2, KVCacheBytes: 3,
		ContextTokens: 4, BatchSize: 5, OutputBytes: 6,
		ExecutionTime: 7 * time.Millisecond,
	}
	limits := wireResourceLimits(resources)
	if len(limits) != len(aiResourceDimensions) {
		t.Fatalf("resource limits=%v", limits)
	}
	for index, dimension := range aiResourceDimensions {
		if limits[index].Id != dimension.id ||
			limits[index].Unit != dimension.unit ||
			limits[index].Quantity != dimension.quantity(resources) {
			t.Fatalf("resource limit %d=%v", index, limits[index])
		}
	}
	if err := validateRequestedLimits([]*edgev1.ResourceLimit{{
		Id: resourceRAM, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
		Quantity: 2,
	}}, resources); err != nil {
		t.Fatal(err)
	}
	for name, requested := range map[string][]*edgev1.ResourceLimit{
		"below requirement": {{
			Id: resourceRAM, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
			Quantity: 0,
		}},
		"wrong unit": {{
			Id: resourceRAM, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
			Quantity: 2,
		}},
		"unknown": {{
			Id: "device.serial", Unit: edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
			Quantity: 1,
		}},
		"duplicate": {
			{Id: resourceBatch, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_COUNT, Quantity: 5},
			{Id: resourceBatch, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_COUNT, Quantity: 5},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRequestedLimits(requested, resources); err == nil {
				t.Fatal("invalid requested resource limits were accepted")
			}
		})
	}
}

func TestWorkerSelectorMatchesProtocolBoundary(t *testing.T) {
	if err := validateSelector("tos.ai.mock", "generate", "model-v1"); err != nil {
		t.Fatal(err)
	}
	for _, serviceID := range []string{
		"AI.Mock", "ab", "tos/ai/mock", strings.Repeat("a", 129),
	} {
		if err := validateSelector(serviceID, "generate", "model-v1"); err == nil {
			t.Fatalf("unsafe service ID %q was accepted", serviceID)
		}
	}
	if err := validateSelector(
		"tos.ai.mock", strings.Repeat("x", 257), "model-v1",
	); err == nil {
		t.Fatal("overlong operation was accepted")
	}
}

func TestStructuredReadinessResourcesAndQuoteCommitment(t *testing.T) {
	service := newTestService(t)
	service.RefreshRuntimes(context.Background())
	health, err := service.Health(
		context.Background(), connect.NewRequest(&edgev1.HealthRequest{}),
	)
	if err != nil || len(health.Msg.Readiness) != 6 {
		t.Fatalf("health=%v err=%v", health, err)
	}
	wantComponents := []string{
		"worker", "admission", "resources", "runtimes", "model-binding", "gpu",
	}
	for index, id := range wantComponents {
		if health.Msg.Readiness[index].Id != id ||
			health.Msg.Readiness[index].Evidence == nil {
			t.Fatalf("readiness %d=%v", index, health.Msg.Readiness[index])
		}
	}
	if health.Msg.Readiness[0].Evidence.Level !=
		edgev1.EvidenceLevel_EVIDENCE_LEVEL_DECLARED ||
		health.Msg.Readiness[3].Evidence.Level !=
			edgev1.EvidenceLevel_EVIDENCE_LEVEL_OBSERVED ||
		health.Msg.Readiness[4].Evidence.Level !=
			edgev1.EvidenceLevel_EVIDENCE_LEVEL_OBSERVED {
		t.Fatalf("incorrect readiness evidence levels: %v", health.Msg.Readiness)
	}
	capabilities, err := service.GetCapabilities(
		context.Background(), connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
	)
	if err != nil || capabilities.Msg.TerminalRevision != "test" ||
		capabilities.Msg.CollectedUnixMillis <= 0 ||
		capabilities.Msg.ExpiresUnixMillis <= capabilities.Msg.CollectedUnixMillis ||
		len(capabilities.Msg.Capabilities) != 1 ||
		len(capabilities.Msg.Resources) != len(aiResourceDimensions) {
		t.Fatalf("capabilities=%v err=%v", capabilities, err)
	}
	for _, claim := range capabilities.Msg.Resources {
		if len(claim.Attributes) != 0 || claim.Evidence == nil ||
			claim.AvailableExternal > claim.Total-claim.OwnerReserved {
			t.Fatalf("unsafe resource claim=%v", claim)
		}
	}
	encoded, err := protojson.Marshal(capabilities.Msg)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(encoded))
	for _, forbidden := range []string{
		"serial", "uuid", "hostname", "pci", "mac_address", "ip_address",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("private hardware identity appeared in capabilities: %s", lower)
		}
	}

	deadline := time.Now().Add(time.Minute)
	quote, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "resource-quote-0001", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: deadline.UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
			RequestedLimits: []*edgev1.ResourceLimit{{
				Id: resourceRAM, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
				Quantity: 2 << 20,
			}},
		}),
	)
	if err != nil || len(quote.Msg.CommittedLimits) == 0 {
		t.Fatalf("quote=%v err=%v", quote, err)
	}
	if limitByID(quote.Msg.CommittedLimits, resourceRAM).Quantity != 1<<20 ||
		limitByID(quote.Msg.CommittedLimits, resourceOutput).Quantity != 16 {
		t.Fatalf("committed limits=%v", quote.Msg.CommittedLimits)
	}
	invalid := &edgev1.QuoteRequest{
		RequestId: "resource-quote-0002", ServiceId: "tos.ai.mock",
		Operation: "generate", Model: "deterministic-echo",
		InputBytes: 5, MaxOutputBytes: 16,
		DeadlineUnixMillis: deadline.UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		RequestedLimits: []*edgev1.ResourceLimit{{
			Id: resourceRAM, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
			Quantity: 1,
		}},
	}
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(invalid),
	); err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("undersized resource request error=%v", err)
	}
}

func TestDurableTaskResultSurvivesWorkerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-tasks.db")
	first := newTestServiceAt(t, path)
	request := quotedInvocation(
		t, first, "durable-restart-0001", time.Now().Add(time.Minute),
	)
	response, err := first.Invoke(
		context.Background(), connect.NewRequest(request),
	)
	if err != nil || string(response.Msg.Output) != "hello" {
		t.Fatalf("invoke=%v err=%v", response, err)
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := first.Shutdown(shutdownContext); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()

	second := newTestServiceAt(t, path)
	recovered, err := second.GetTask(
		context.Background(), connect.NewRequest(&edgev1.GetTaskRequest{
			RequestId: request.RequestId, TaskId: request.TaskId,
			RequestDigest:         request.RequestDigest,
			RetainUntilUnixMillis: request.RetainUntilUnixMillis,
		}),
	)
	if err != nil || recovered.Msg.Status != edgev1.TaskStatus_TASK_STATUS_SUCCEEDED ||
		recovered.Msg.Result == nil || string(recovered.Msg.Result.Output) != "hello" {
		t.Fatalf("recovered=%v err=%v", recovered, err)
	}
	wrong := &edgev1.GetTaskRequest{
		RequestId: request.RequestId, TaskId: request.TaskId,
		RequestDigest:         "sha256:" + strings.Repeat("0", 64),
		RetainUntilUnixMillis: request.RetainUntilUnixMillis,
	}
	if _, err := second.GetTask(
		context.Background(), connect.NewRequest(wrong),
	); err == nil {
		t.Fatal("mismatched retained task identity was accepted")
	}
}

func TestTosProtocolWorkerClientCompatibility(t *testing.T) {
	service := newTestService(t)
	service.RefreshRuntimes(context.Background())
	directory := filepath.Join(t.TempDir(), "private")
	if err := unixserver.PreparePrivateParent(
		filepath.Join(directory, "worker.sock"),
	); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "worker.sock")
	listener, err := unixserver.ListenLimited(socket, 16)
	if err != nil {
		t.Fatal(err)
	}
	path, handler := edgev1connect.NewWorkerServiceHandler(service)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		<-serverDone
	})

	client, err := localrpc.NewWorkerClient(localrpc.DefaultWorkerClientConfig(socket))
	if err != nil {
		t.Fatal(err)
	}
	if health, err := client.Health(context.Background()); err != nil ||
		len(health.Readiness) != 6 {
		t.Fatalf("validated Health=%v err=%v", health, err)
	}
	capabilities, err := client.GetCapabilities(context.Background())
	if err != nil || len(capabilities.Capabilities) != 1 ||
		len(capabilities.Resources) != len(aiResourceDimensions) {
		t.Fatalf("validated capabilities=%v err=%v", capabilities, err)
	}
	now := time.Now().UTC()
	quoteRequest := &edgev1.QuoteRequest{
		RequestId: "compat-request-0001", ServiceId: "tos.ai.mock",
		Operation: "generate", Model: "deterministic-echo",
		InputBytes: 5, MaxOutputBytes: 16,
		DeadlineUnixMillis: now.Add(time.Minute).UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		RequestedLimits: []*edgev1.ResourceLimit{{
			Id: resourceRAM, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
			Quantity: 2 << 20,
		}},
	}
	quote, err := client.Quote(context.Background(), quoteRequest)
	if err != nil {
		t.Fatal(err)
	}
	invocation := &edgev1.InvokeRequest{
		RequestId: quoteRequest.RequestId, QuoteId: quote.QuoteId,
		TaskId: "compat-task-0001", ServiceId: quoteRequest.ServiceId,
		Operation: quoteRequest.Operation, Model: quoteRequest.Model,
		Payload: []byte("hello"), MaxOutputBytes: quoteRequest.MaxOutputBytes,
		DeadlineUnixMillis:    quoteRequest.DeadlineUnixMillis,
		RetainUntilUnixMillis: now.Add(time.Hour).UnixMilli(),
		Priority:              quoteRequest.Priority,
	}
	validated, err := client.Invoke(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	binding := localrpc.InvocationBinding{
		RequestID: invocation.RequestId, QuoteID: invocation.QuoteId,
		ServiceID: invocation.ServiceId, Operation: invocation.Operation,
	}
	completion, err := validated.Completion(binding)
	if err != nil || string(completion.Output) != "hello" {
		t.Fatalf("completion=%v err=%v", completion, err)
	}
	recovered, err := client.GetTask(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	status, err := recovered.Status()
	if err != nil || status != edgev1.TaskStatus_TASK_STATUS_SUCCEEDED {
		t.Fatalf("recovered status=%v err=%v", status, err)
	}
	if accepted, err := client.Cancel(
		context.Background(), invocation,
	); err != nil || accepted {
		t.Fatalf("terminal Cancel accepted=%v err=%v", accepted, err)
	}
}

func newTestServiceAt(t *testing.T, path string) *Service {
	t.Helper()
	scheduler, controller := newTestDependencies(t, 4)
	service, err := NewService(
		testServiceConfigAt(t, path), scheduler, controller,
		[]airuntime.Adapter{mock.New(0)},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	})
	return service
}

func limitByID(
	limits []*edgev1.ResourceLimit,
	id string,
) *edgev1.ResourceLimit {
	for _, limit := range limits {
		if limit.Id == id {
			return limit
		}
	}
	return &edgev1.ResourceLimit{}
}
