package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/edgeintegration"
	"github.com/tosnetwork/tos-ai/pkg/profile/textgeneration"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/ard"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"github.com/tosnetwork/tos-protocol/pkg/registry"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestAIResourceDimensionsAndRequestedUpperBounds(t *testing.T) {
	const maximumTaskBytes = 100
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
	}}, resources, maximumTaskBytes); err != nil {
		t.Fatal(err)
	}
	if err := validateRequestedLimits([]*edgev1.ResourceLimit{{
		Id:       resourceTaskSlots,
		Unit:     edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
		Quantity: 1,
	}}, resources, maximumTaskBytes); err != nil {
		t.Fatal(err)
	}
	if err := validateRequestedLimits([]*edgev1.ResourceLimit{{
		Id: resourceTaskBytes, Unit: edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
		Quantity: maximumTaskBytes,
	}}, resources, maximumTaskBytes); err != nil {
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
			if err := validateRequestedLimits(
				requested, resources, maximumTaskBytes,
			); err == nil {
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
	if err != nil || len(health.Msg.Readiness) != 7 {
		t.Fatalf("health=%v err=%v", health, err)
	}
	wantComponents := []string{
		"worker", "admission", "resources", "runtimes", "model-binding", "gpu",
		"task-store",
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
		len(capabilities.Msg.Resources) != len(aiResourceDimensions)+2 {
		t.Fatalf("capabilities=%v err=%v", capabilities, err)
	}
	for _, claim := range capabilities.Msg.Resources {
		if len(claim.Attributes) != 0 || claim.Evidence == nil ||
			claim.AvailableExternal > claim.Total-claim.OwnerReserved {
			t.Fatalf("unsafe resource claim=%v", claim)
		}
	}
	taskSlots := resourceClaimByID(capabilities.Msg.Resources, resourceTaskSlots)
	taskBytes := resourceClaimByID(capabilities.Msg.Resources, resourceTaskBytes)
	if taskSlots.Total != 64 || taskSlots.AvailableExternal != 64 ||
		taskBytes.Total != localrpc.DefaultWorkerMaxRetainedBytes ||
		taskBytes.AvailableExternal != localrpc.DefaultWorkerMaxRetainedBytes ||
		taskBytes.Unit != edgev1.ResourceUnit_RESOURCE_UNIT_BYTES ||
		taskSlots.Evidence.Level !=
			edgev1.EvidenceLevel_EVIDENCE_LEVEL_DECLARED ||
		limitByID(
			capabilities.Msg.Capabilities[0].AdmissionLimits,
			resourceTaskSlots,
		).Quantity != 1 ||
		limitByID(
			capabilities.Msg.Capabilities[0].AdmissionLimits,
			resourceTaskBytes,
		).Quantity != taskBytesForTest(t) ||
		limitByID(
			capabilities.Msg.Capabilities[0].AdmissionLimits,
			resourceOutput,
		).Quantity != capabilities.Msg.Capabilities[0].MaxOutputBytes {
		t.Fatalf("invalid durable capability accounting: %v", capabilities.Msg)
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
	catalog, err := ard.BuildWorkerCatalog(ard.WorkerCatalogConfig{
		ServiceIdentifier:  "urn:air:edge.example:tos:ai-terminal",
		ServiceDisplayName: "Example TOS AI Edge Terminal",
		HostDisplayName:    "Example Edge Operator", HostIdentifier: "did:web:edge.example",
		ServiceURL:   "https://edge.example/.well-known/tos-service.json",
		EntryVersion: "0.1.0",
	}, capabilities.Msg, time.Now().UTC())
	if err != nil || len(catalog.Entries) != 1 ||
		catalog.Entries[0].Identifier != "urn:air:edge.example:tos:ai-terminal" {
		t.Fatalf("ARD catalog=%#v err=%v", catalog, err)
	}
	catalogJSON, err := json.Marshal(catalog)
	if err != nil || strings.Contains(string(catalogJSON), "storage.task") ||
		strings.Contains(string(catalogJSON), "availableExternal") ||
		!strings.Contains(string(catalogJSON), "deterministic-echo") {
		t.Fatalf("ARD catalog leaked dynamic capacity: %s err=%v", catalogJSON, err)
	}
	index, err := registry.NewIndex(registry.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(t.TempDir(), "tos-ai-catalog.json")
	if err := ard.WriteCatalogFile(catalogPath, catalog, ard.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	loadedCatalog, err := ard.ReadCatalogFile(catalogPath, ard.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := index.ReplaceCatalogs([]registry.CatalogInput{{
		Source: "file:///tos-ai-catalog.json", Catalog: loadedCatalog,
	}}); err != nil {
		t.Fatal(err)
	}
	registryHandler, err := registry.NewHandler(index, "https://registry.edge.example/search")
	if err != nil {
		t.Fatal(err)
	}
	registryServer := httptest.NewServer(registryHandler.Routes())
	defer registryServer.Close()
	searchJSON, err := json.Marshal(registry.SearchRequest{
		Query: registry.QueryModel{
			Text: "deterministic-echo",
			Filter: map[string]interface{}{
				registry.WorkerFilterServiceID: capabilities.Msg.Capabilities[0].ServiceId,
				registry.WorkerFilterOperation: capabilities.Msg.Capabilities[0].Operation,
				registry.WorkerFilterModel:     capabilities.Msg.Capabilities[0].Model,
				registry.WorkerFilterRuntime:   capabilities.Msg.Capabilities[0].Runtime,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	searchContext, cancelSearch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSearch()
	searchRequest, err := http.NewRequestWithContext(
		searchContext, http.MethodPost, registryServer.URL+"/search",
		bytes.NewReader(searchJSON),
	)
	if err != nil {
		t.Fatal(err)
	}
	searchRequest.Header.Set("Content-Type", "application/json")
	searchResponse, err := http.DefaultClient.Do(searchRequest)
	if err != nil {
		t.Fatal(err)
	}
	var discovered registry.SearchResponse
	decodeErr := json.NewDecoder(io.LimitReader(searchResponse.Body, 1<<20)).Decode(&discovered)
	closeErr := searchResponse.Body.Close()
	if searchResponse.StatusCode != http.StatusOK || decodeErr != nil || closeErr != nil ||
		len(discovered.Results) != 1 ||
		discovered.Results[0].Identifier != catalog.Entries[0].Identifier {
		t.Fatalf(
			"ARD Worker HTTP discovery status=%d response=%#v decode_err=%v close_err=%v",
			searchResponse.StatusCode, discovered, decodeErr, closeErr,
		)
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
		limitByID(quote.Msg.CommittedLimits, resourceOutput).Quantity != 16 ||
		limitByID(quote.Msg.CommittedLimits, resourceTaskSlots).Quantity != 1 ||
		limitByID(quote.Msg.CommittedLimits, resourceTaskBytes).Quantity !=
			taskBytesForTest(t) {
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

func TestTaskStoreSaturationBlocksRoutingAndRecoversAfterCleanup(t *testing.T) {
	service := newTestServiceAtWithCapacity(
		t,
		filepath.Join(t.TempDir(), "worker-tasks.db"),
		1,
	)
	service.RefreshRuntimes(context.Background())
	deadline := time.Now().Add(time.Minute)
	first := quotedInvocation(t, service, "task-capacity-0001", deadline)
	raced := quotedInvocation(t, service, "task-capacity-race", deadline)
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(first),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(first),
	); err != nil {
		t.Fatalf("exact Invoke replay was blocked by saturation: %v", err)
	}
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(raced),
	); err == nil || connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("authoritative capacity race error=%v", err)
	}
	replayedQuote, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "task-capacity-0001", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: deadline.UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	)
	if err != nil || replayedQuote.Msg.QuoteId != first.QuoteId {
		t.Fatalf("exact Quote replay was blocked by saturation: %v %v", replayedQuote, err)
	}

	health, err := service.Health(
		context.Background(), connect.NewRequest(&edgev1.HealthRequest{}),
	)
	if err != nil || health.Msg.Readiness[6].Id != "task-store" ||
		health.Msg.Readiness[6].Status !=
			edgev1.ReadinessStatus_READINESS_STATUS_UNAVAILABLE ||
		health.Msg.Readiness[6].ReasonCode != "capacity_exhausted" {
		t.Fatalf("saturated health=%v err=%v", health, err)
	}
	capabilities, err := service.GetCapabilities(
		context.Background(), connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
	)
	taskSlots := resourceClaimByID(capabilities.Msg.Resources, resourceTaskSlots)
	if err != nil || len(capabilities.Msg.Capabilities) != 0 ||
		taskSlots.Total != 1 || taskSlots.AvailableExternal != 0 {
		t.Fatalf("saturated capabilities=%v err=%v", capabilities, err)
	}
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "task-capacity-0002", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	); err == nil || connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("saturated Quote error=%v", err)
	}

	future := time.Now().Add(2 * time.Hour)
	service.config.Now = func() time.Time { return future }
	removed, _, err := service.taskStore.Cleanup(
		future,
		localrpc.DefaultWorkerMaxPrunePerWrite,
	)
	if err != nil || removed != 1 {
		t.Fatalf("cleanup removed=%d err=%v", removed, err)
	}
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "task-capacity-0003", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: future.Add(time.Minute).UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	); err != nil {
		t.Fatalf("Quote did not recover after bounded cleanup: %v", err)
	}
}

func TestTaskStoreOwnerReserveKeepsLocalQuoteAvailable(t *testing.T) {
	service := newTestServiceAtTaskCapacity(
		t,
		filepath.Join(t.TempDir(), "worker-tasks.db"),
		3,
		1,
	)
	service.RefreshRuntimes(context.Background())
	deadline := time.Now().Add(time.Minute)
	for index := range 2 {
		invocation := quotedInvocation(
			t, service, fmt.Sprintf("owner-reserve-external-%d", index), deadline,
		)
		if _, err := service.Invoke(
			context.Background(), connect.NewRequest(invocation),
		); err != nil {
			t.Fatal(err)
		}
	}
	capabilities, err := service.GetCapabilities(
		context.Background(), connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
	)
	taskSlots := resourceClaimByID(capabilities.Msg.Resources, resourceTaskSlots)
	taskBytes := resourceClaimByID(capabilities.Msg.Resources, resourceTaskBytes)
	if err != nil || len(capabilities.Msg.Capabilities) != 0 ||
		taskSlots.Total != 3 || taskSlots.OwnerReserved != 1 ||
		taskSlots.AvailableExternal != 0 ||
		taskBytes.OwnerReserved != taskBytesForTest(t) ||
		taskBytes.AvailableExternal != 0 {
		t.Fatalf("owner-reserved capabilities=%v err=%v", capabilities, err)
	}
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "owner-reserve-external-blocked", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: deadline.UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	); err == nil || connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("external Quote consumed owner reserve: %v", err)
	}
	localQuote, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "owner-reserve-local", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: deadline.UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_LOCAL_ASYNC,
		}),
	)
	if err != nil {
		t.Fatalf("owner-local Quote was blocked: %v", err)
	}
	localInvocation := bindTestInvocation(t, &edgev1.InvokeRequest{
		RequestId: "owner-reserve-local", QuoteId: localQuote.Msg.QuoteId,
		ServiceId: "tos.ai.mock", Operation: "generate",
		Model: "deterministic-echo", Payload: []byte("hello"),
		MaxOutputBytes: 16, DeadlineUnixMillis: deadline.UnixMilli(),
		Priority: edgev1.Priority_PRIORITY_LOCAL_ASYNC,
	})
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(localInvocation),
	); err != nil {
		t.Fatalf("owner-local Invoke was blocked: %v", err)
	}
	stats, err := service.taskStore.Stats()
	if err != nil || stats.Tasks != 3 || stats.OwnerTasks != 1 ||
		stats.ExternalTasks != 2 || stats.Available != 0 {
		t.Fatalf("owner-local store stats=%#v err=%v", stats, err)
	}
}

func TestTaskStoreByteBudgetSuppressesRoutingBeforeSlotCapacity(t *testing.T) {
	maximumTaskBytes := taskBytesForTest(t)
	service := newTestServiceAtStorageCapacity(
		t,
		filepath.Join(t.TempDir(), "worker-tasks.db"),
		3,
		0,
		maximumTaskBytes,
	)
	service.RefreshRuntimes(context.Background())
	deadline := time.Now().Add(time.Minute)
	invocation := quotedInvocation(
		t, service, "byte-budget-first", deadline,
	)
	if _, err := service.Invoke(
		context.Background(), connect.NewRequest(invocation),
	); err != nil {
		t.Fatal(err)
	}
	capabilities, err := service.GetCapabilities(
		context.Background(), connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	taskSlots := resourceClaimByID(capabilities.Msg.Resources, resourceTaskSlots)
	taskBytes := resourceClaimByID(capabilities.Msg.Resources, resourceTaskBytes)
	if len(capabilities.Msg.Capabilities) != 0 || taskSlots.Total != 3 ||
		taskSlots.AvailableExternal != 0 || taskBytes.Total != maximumTaskBytes ||
		taskBytes.AvailableExternal != 0 {
		t.Fatalf("byte-exhausted capabilities=%v", capabilities.Msg)
	}
	stats, err := service.taskStore.Stats()
	if err != nil || stats.Tasks != 1 || stats.Available != 2 ||
		stats.AvailableBytes >= stats.MaximumTaskBytes {
		t.Fatalf("byte-exhausted stats=%#v err=%v", stats, err)
	}
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "byte-budget-blocked", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo",
			InputBytes: 5, MaxOutputBytes: 16,
			DeadlineUnixMillis: deadline.UnixMilli(),
			Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	); err == nil || connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("byte-exhausted Quote error=%v", err)
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
	streamPath, streamHandler := edgev1connect.NewWorkerStreamServiceHandler(service)
	mux.Handle(streamPath, streamHandler)
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
	health, healthErr := client.Health(context.Background())
	if healthErr != nil || len(health.Readiness) != 7 {
		t.Fatalf("validated Health=%v err=%v", health, healthErr)
	}
	capabilities, err := client.GetCapabilities(context.Background())
	if err != nil || len(capabilities.Capabilities) != 1 ||
		len(capabilities.Resources) != len(aiResourceDimensions)+2 {
		t.Fatalf("validated capabilities=%v err=%v", capabilities, err)
	}
	deployment, err := edgeintegration.New(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	installedWorker, err := deployment.Worker()
	if err != nil || installedWorker != client {
		t.Fatalf("installed Worker=%p want=%p err=%v", installedWorker, client, err)
	}
	profilePlan, err := deployment.ProfilePlan()
	if err != nil || !profilePlan.Supports(
		textgeneration.ProfileID, textgeneration.ProfileVersion, nil,
		textgeneration.Operation,
	) {
		t.Fatalf("live Worker profile plan=%v err=%v", profilePlan, err)
	}
	if err := deployment.CheckReady(context.Background()); err != nil {
		t.Fatalf("live Edge deployment readiness: %v health=%v", err, health)
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

	streamQuoteRequest := proto.Clone(quoteRequest).(*edgev1.QuoteRequest)
	streamQuoteRequest.RequestId = "compat-stream-request-0001"
	streamQuote, err := client.Quote(context.Background(), streamQuoteRequest)
	if err != nil {
		t.Fatal(err)
	}
	streamInvocation, _, err := localrpc.BindInvocationRequest(&edgev1.InvokeRequest{
		RequestId: streamQuoteRequest.RequestId, QuoteId: streamQuote.QuoteId,
		TaskId: "compat-stream-task-0001", ServiceId: streamQuoteRequest.ServiceId,
		Operation: streamQuoteRequest.Operation, Model: streamQuoteRequest.Model,
		Payload: []byte("hello"), MaxOutputBytes: streamQuoteRequest.MaxOutputBytes,
		DeadlineUnixMillis:    streamQuoteRequest.DeadlineUnixMillis,
		RetainUntilUnixMillis: now.Add(time.Hour).UnixMilli(),
		Priority:              streamQuoteRequest.Priority,
	})
	if err != nil {
		t.Fatal(err)
	}
	streamed, err := client.InvokeStream(context.Background(), streamInvocation, 2)
	if err != nil {
		t.Fatal(err)
	}
	streamCompletion, err := streamed.Completion(localrpc.InvocationBinding{
		RequestID: streamInvocation.RequestId, QuoteID: streamInvocation.QuoteId,
		ServiceID: streamInvocation.ServiceId, Operation: streamInvocation.Operation,
	})
	if err != nil || string(streamCompletion.Output) != "hello" {
		t.Fatalf("stream completion=%v err=%v", streamCompletion, err)
	}
	digest := sha256.Sum256([]byte("hello"))
	resumed, err := client.ResumeStream(
		context.Background(), streamInvocation, 1, []byte("he"),
		"sha256:"+hex.EncodeToString(digest[:]), 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	resumeCompletion, err := resumed.Completion(localrpc.InvocationBinding{
		RequestID: streamInvocation.RequestId, QuoteID: streamInvocation.QuoteId,
		ServiceID: streamInvocation.ServiceId, Operation: streamInvocation.Operation,
	})
	if err != nil || string(resumeCompletion.Output) != "hello" {
		t.Fatalf("resume completion=%v err=%v", resumeCompletion, err)
	}
}

func newTestServiceAt(t *testing.T, path string) *Service {
	return newTestServiceAtWithCapacity(t, path, 64)
}

func newTestServiceAtWithCapacity(
	t *testing.T,
	path string,
	maxTasks int,
) *Service {
	t.Helper()
	scheduler, controller := newTestDependencies(t, 4)
	service, err := NewService(
		testServiceConfigAtLimit(t, path, maxTasks), scheduler, controller,
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

func newTestServiceAtTaskCapacity(
	t *testing.T,
	path string,
	maxTasks int,
	ownerReserved int,
) *Service {
	t.Helper()
	scheduler, controller := newTestDependencies(t, 4)
	service, err := NewService(
		testServiceConfigAtTaskCapacity(
			t, path, maxTasks, ownerReserved,
		),
		scheduler,
		controller,
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

func newTestServiceAtStorageCapacity(
	t *testing.T,
	path string,
	maxTasks int,
	ownerReserved int,
	maxRetainedBytes uint64,
) *Service {
	t.Helper()
	scheduler, controller := newTestDependencies(t, 4)
	service, err := NewService(
		testServiceConfigAtStorageCapacity(
			t, path, maxTasks, ownerReserved, maxRetainedBytes,
		),
		scheduler,
		controller,
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

func resourceClaimByID(
	claims []*edgev1.ResourceClaim,
	id string,
) *edgev1.ResourceClaim {
	for _, claim := range claims {
		if claim.Id == id {
			return claim
		}
	}
	return &edgev1.ResourceClaim{}
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

func taskBytesForTest(t *testing.T) uint64 {
	t.Helper()
	value, err := localrpc.WorkerTaskMaximumReservationBytes(
		localrpc.DefaultWorkerMaxMessageBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
