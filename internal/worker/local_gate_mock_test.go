package worker

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/probe"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

func TestLocalGateMockTier1GPUAdmissionDegradationAndRecovery(t *testing.T) {
	resources := &mutableResourceHealth{health: probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "available",
	}}
	var capability airuntime.Capability
	service := newConfiguredFaultService(
		t,
		func(_ context.Context, request airuntime.Request) (airuntime.Response, error) {
			return airuntime.Response{
				Output: append([]byte(nil), request.Payload...),
				Usage: airuntime.Usage{
					InputBytes:  uint64(len(request.Payload)),
					OutputBytes: uint64(len(request.Payload)),
				},
				ModelRevision:   capability.ModelDigest,
				RuntimeRevision: capability.RuntimeRevision,
			}, nil
		},
		func(config *Config) { config.ResourceHealth = resources },
		func(adapter *faultAdapter) {
			adapter.capability.Runtime = "mock-cuda"
			adapter.capability.RuntimeRevision = "mock-cuda-v1"
			adapter.capability.Admission.VRAMBytes = 512 << 20
			capability = adapter.capability
		},
	)
	service.RefreshRuntimes(context.Background())
	if readiness := service.Readiness(); readiness.GPU != "available" ||
		readiness.Status != "ready" {
		t.Fatalf("simulated GPU readiness=%#v", readiness)
	}
	capabilities, err := service.GetCapabilities(
		context.Background(), connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
	)
	if err != nil || len(capabilities.Msg.Capabilities) != 1 {
		t.Fatalf("simulated GPU capabilities=%v err=%v", capabilities, err)
	}
	foundVRAM := false
	for _, limit := range capabilities.Msg.Capabilities[0].AdmissionLimits {
		if limit.Id == resourceVRAM && limit.Quantity == 512<<20 {
			foundVRAM = true
		}
	}
	if !foundVRAM {
		t.Fatal("simulated GPU capability omitted its VRAM admission limit")
	}

	first := quotedInvocation(t, service, "mock-gpu-first", time.Now().Add(time.Minute))
	firstResult, err := service.Invoke(
		context.Background(), connect.NewRequest(first),
	)
	if err != nil || string(firstResult.Msg.Output) != "hello" {
		t.Fatalf("simulated GPU invocation=%v err=%v", firstResult, err)
	}
	resources.set(probe.ResourceHealth{
		Ready: false, Status: "degraded", GPU: "unavailable",
	})
	if _, err := service.Quote(
		context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
			RequestId: "mock-gpu-blocked", ServiceId: "tos.ai.mock",
			Operation: "generate", Model: "deterministic-echo", InputBytes: 5,
			MaxOutputBytes: 16, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
			Priority: edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		}),
	); err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("degraded simulated GPU accepted new work: %v", err)
	}
	replay, err := service.Invoke(context.Background(), connect.NewRequest(first))
	if err != nil || string(replay.Msg.Output) != "hello" {
		t.Fatalf("degraded simulated GPU lost durable replay=%v err=%v", replay, err)
	}
	resources.set(probe.ResourceHealth{
		Ready: true, Status: "ready", GPU: "available",
	})
	second := quotedInvocation(t, service, "mock-gpu-recovered", time.Now().Add(time.Minute))
	secondResult, err := service.Invoke(
		context.Background(), connect.NewRequest(second),
	)
	if err != nil || string(secondResult.Msg.Output) != "hello" {
		t.Fatalf("recovered simulated GPU invocation=%v err=%v", secondResult, err)
	}
	if used := service.admission.Snapshot().Used; used != (admission.Resources{}) {
		t.Fatalf("simulated GPU resources leaked after execution: %#v", used)
	}
}
