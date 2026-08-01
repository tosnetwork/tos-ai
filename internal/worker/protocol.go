package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

const (
	resourceRAM       = "memory.ram"
	resourceVRAM      = "memory.vram"
	resourceKVCache   = "memory.kv_cache"
	resourceContext   = "runtime.context_tokens"
	resourceBatch     = "runtime.batch"
	resourceOutput    = "runtime.output"
	resourceExecution = "runtime.execution"
	resourceTaskSlots = "storage.task_slots"
)

type resourceDimension struct {
	id            string
	class         edgev1.ResourceClass
	unit          edgev1.ResourceUnit
	quantity      func(admission.Resources) uint64
	ownerReserved func(admission.Resources) uint64
}

var aiResourceDimensions = [...]resourceDimension{
	{resourceRAM, edgev1.ResourceClass_RESOURCE_CLASS_MEMORY,
		edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
		func(value admission.Resources) uint64 { return value.RAMBytes },
		func(value admission.Resources) uint64 { return value.RAMBytes }},
	{resourceVRAM, edgev1.ResourceClass_RESOURCE_CLASS_ACCELERATOR,
		edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
		func(value admission.Resources) uint64 { return value.VRAMBytes },
		func(value admission.Resources) uint64 { return value.VRAMBytes }},
	{resourceKVCache, edgev1.ResourceClass_RESOURCE_CLASS_MEMORY,
		edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
		func(value admission.Resources) uint64 { return value.KVCacheBytes },
		func(value admission.Resources) uint64 { return value.KVCacheBytes }},
	{resourceContext, edgev1.ResourceClass_RESOURCE_CLASS_RUNTIME,
		edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
		func(value admission.Resources) uint64 { return value.ContextTokens },
		func(value admission.Resources) uint64 { return value.ContextTokens }},
	{resourceBatch, edgev1.ResourceClass_RESOURCE_CLASS_RUNTIME,
		edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
		func(value admission.Resources) uint64 { return uint64(value.BatchSize) },
		func(value admission.Resources) uint64 { return uint64(value.BatchSize) }},
	{resourceOutput, edgev1.ResourceClass_RESOURCE_CLASS_RUNTIME,
		edgev1.ResourceUnit_RESOURCE_UNIT_BYTES,
		func(value admission.Resources) uint64 { return value.OutputBytes },
		func(value admission.Resources) uint64 { return value.OutputBytes }},
	{resourceExecution, edgev1.ResourceClass_RESOURCE_CLASS_RUNTIME,
		edgev1.ResourceUnit_RESOURCE_UNIT_MILLISECONDS,
		func(value admission.Resources) uint64 {
			return durationMilliseconds(value.ExecutionTime)
		},
		func(admission.Resources) uint64 { return 0 }},
}

func durationMilliseconds(value time.Duration) uint64 {
	if value <= 0 {
		return 0
	}
	// Round up so a positive sub-millisecond internal deadline never becomes
	// an invalid zero-valued wire limit.
	return uint64((value + time.Millisecond - 1) / time.Millisecond)
}

func wireResourceLimits(resources admission.Resources) []*edgev1.ResourceLimit {
	limits := make([]*edgev1.ResourceLimit, 0, len(aiResourceDimensions))
	for _, dimension := range aiResourceDimensions {
		quantity := dimension.quantity(resources)
		if quantity == 0 {
			continue
		}
		limits = append(limits, &edgev1.ResourceLimit{
			Id: dimension.id, Unit: dimension.unit, Quantity: quantity,
		})
	}
	return limits
}

func wireCommittedLimits(resources admission.Resources) []*edgev1.ResourceLimit {
	limits := wireResourceLimits(resources)
	return append(limits, &edgev1.ResourceLimit{
		Id:       resourceTaskSlots,
		Unit:     edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
		Quantity: 1,
	})
}

// validateRequestedLimits treats caller limits as upper bounds. The worker
// still derives the actual admission profile from private configuration and
// rejects unknown dimensions rather than accepting payload-based overrides.
func validateRequestedLimits(
	requested []*edgev1.ResourceLimit,
	actual admission.Resources,
) error {
	if len(requested) == 0 {
		return nil
	}
	if len(requested) > len(aiResourceDimensions)+1 {
		return errors.New("too many requested resource limits")
	}
	actualLimits := make(map[string]*edgev1.ResourceLimit, len(aiResourceDimensions))
	for _, limit := range wireResourceLimits(actual) {
		actualLimits[limit.Id] = limit
	}
	actualLimits[resourceTaskSlots] = &edgev1.ResourceLimit{
		Id:       resourceTaskSlots,
		Unit:     edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
		Quantity: 1,
	}
	seen := make(map[string]struct{}, len(requested))
	for _, limit := range requested {
		if limit == nil || limit.Quantity == 0 {
			return errors.New("invalid requested resource limit")
		}
		if _, duplicate := seen[limit.Id]; duplicate {
			return errors.New("duplicate requested resource limit")
		}
		seen[limit.Id] = struct{}{}
		required := actualLimits[limit.Id]
		if required == nil || required.Unit != limit.Unit {
			return fmt.Errorf("unsupported requested resource limit %q", limit.Id)
		}
		if limit.Quantity < required.Quantity {
			return fmt.Errorf("requested resource limit %q is below runtime requirement", limit.Id)
		}
	}
	return nil
}

func wireResourceClaims(
	snapshot admission.Snapshot,
	tasks taskStoreCapacity,
	revision string,
	now time.Time,
	expires time.Time,
	issuer string,
) []*edgev1.ResourceClaim {
	claims := make([]*edgev1.ResourceClaim, 0, len(aiResourceDimensions)+1)
	for _, dimension := range aiResourceDimensions {
		total := dimension.quantity(snapshot.Capacity)
		if total == 0 {
			continue
		}
		reserved := dimension.ownerReserved(snapshot.OwnerReserved)
		used := dimension.quantity(snapshot.Used)
		available := uint64(0)
		if snapshot.Accepting && total > reserved && total-reserved > used {
			available = total - reserved - used
		}
		claims = append(claims, &edgev1.ResourceClaim{
			Id: dimension.id, ResourceClass: dimension.class,
			Unit: dimension.unit, Total: total, OwnerReserved: reserved,
			AvailableExternal: available, Revision: revision,
			Evidence: declaredEvidence(now, expires, issuer),
		})
	}
	taskAvailability := tasks.AvailableExternal
	if !snapshot.Accepting {
		taskAvailability = 0
	}
	claims = append(claims, &edgev1.ResourceClaim{
		Id:            resourceTaskSlots,
		ResourceClass: edgev1.ResourceClass_RESOURCE_CLASS_STORAGE,
		Unit:          edgev1.ResourceUnit_RESOURCE_UNIT_COUNT,
		Total:         tasks.Capacity, OwnerReserved: tasks.OwnerReserved,
		AvailableExternal: taskAvailability,
		Revision:          revision,
		Evidence: readinessEvidence(
			edgev1.EvidenceLevel_EVIDENCE_LEVEL_DECLARED,
			now,
			expires,
			issuer,
		),
	})
	return claims
}

func declaredEvidence(now, expires time.Time, issuer string) *edgev1.ClaimEvidence {
	return readinessEvidence(
		edgev1.EvidenceLevel_EVIDENCE_LEVEL_DECLARED,
		now,
		expires,
		issuer,
	)
}

func readinessEvidence(
	level edgev1.EvidenceLevel,
	now, expires time.Time,
	issuer string,
) *edgev1.ClaimEvidence {
	return &edgev1.ClaimEvidence{
		Level:  level,
		Issuer: issuer, CollectedUnixMillis: now.UnixMilli(),
		ExpiresUnixMillis: expires.UnixMilli(),
	}
}

func wireReadinessStatus(value string) edgev1.ReadinessStatus {
	switch value {
	case "ready":
		return edgev1.ReadinessStatus_READINESS_STATUS_READY
	case "draining":
		return edgev1.ReadinessStatus_READINESS_STATUS_DRAINING
	case "starting", "unknown":
		return edgev1.ReadinessStatus_READINESS_STATUS_UNKNOWN
	case "blocked", "degraded", "mixed":
		return edgev1.ReadinessStatus_READINESS_STATUS_DEGRADED
	default:
		return edgev1.ReadinessStatus_READINESS_STATUS_UNAVAILABLE
	}
}

func wireReadiness(
	readiness Readiness,
	revision string,
	now time.Time,
	expires time.Time,
	issuer string,
) []*edgev1.ReadinessComponent {
	component := func(
		id string,
		status edgev1.ReadinessStatus,
		reason string,
		level edgev1.EvidenceLevel,
	) *edgev1.ReadinessComponent {
		return &edgev1.ReadinessComponent{
			Id: id, Status: status, Revision: revision, ReasonCode: reason,
			Evidence: readinessEvidence(level, now, expires, issuer),
		}
	}
	declared := edgev1.EvidenceLevel_EVIDENCE_LEVEL_DECLARED
	observed := edgev1.EvidenceLevel_EVIDENCE_LEVEL_OBSERVED
	runtimeStatus := edgev1.ReadinessStatus_READINESS_STATUS_READY
	runtimeReason := "all_ready"
	if readiness.RuntimeReady != readiness.RuntimeTotal {
		runtimeStatus = edgev1.ReadinessStatus_READINESS_STATUS_DEGRADED
		runtimeReason = "runtime_unready"
		if readiness.Status == "starting" {
			runtimeStatus = edgev1.ReadinessStatus_READINESS_STATUS_UNKNOWN
			runtimeReason = "preflight_pending"
		}
	}
	bindingStatus := edgev1.ReadinessStatus_READINESS_STATUS_READY
	bindingReason := "binding_observed"
	bindingEvidence := observed
	switch readiness.BindingEvidence {
	case "declared":
		bindingReason = "binding_declared"
		bindingEvidence = declared
	case "mixed":
		bindingStatus = edgev1.ReadinessStatus_READINESS_STATUS_DEGRADED
		bindingReason = "mixed_evidence"
		bindingEvidence = declared
	case "unknown":
		bindingStatus = edgev1.ReadinessStatus_READINESS_STATUS_UNKNOWN
		bindingReason = "binding_unknown"
		bindingEvidence = declared
	}
	gpuStatus := edgev1.ReadinessStatus_READINESS_STATUS_UNKNOWN
	gpuReason := "gpu_unknown"
	switch readiness.GPU {
	case "available":
		gpuStatus, gpuReason = edgev1.ReadinessStatus_READINESS_STATUS_READY, "gpu_available"
	case "no-devices":
		gpuStatus, gpuReason = edgev1.ReadinessStatus_READINESS_STATUS_READY, "cpu_only"
	case "degraded":
		gpuStatus, gpuReason = edgev1.ReadinessStatus_READINESS_STATUS_DEGRADED, "gpu_degraded"
	case "unavailable":
		gpuReason = "gpu_unavailable"
	}
	return []*edgev1.ReadinessComponent{
		component("worker", wireReadinessStatus(readiness.Status), readiness.Status, declared),
		component("admission", wireReadinessStatus(readiness.Admission), readiness.Admission, observed),
		component("resources", wireReadinessStatus(readiness.Resources), readiness.Resources, observed),
		component("runtimes", runtimeStatus, runtimeReason, observed),
		component("model-binding", bindingStatus, bindingReason, bindingEvidence),
		component("gpu", gpuStatus, gpuReason, observed),
		component(
			"task-store",
			wireReadinessStatus(readiness.TaskStore),
			readiness.TaskStoreReason,
			observed,
		),
	}
}
