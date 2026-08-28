package commercegate

import (
	"errors"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
	"github.com/tosnetwork/tos-service-protocol/pkg/codec"
)

const ExecutionResourceMeterEvidenceProfileV1 = "tos.ai.execution-resource-meter.v1"

// ExecutionResourceMeterEvidenceV1 records physical usage. It is not itself a
// monetary cost: OpenFox must apply a separately authorized rate, accounting
// policy and economic perimeter before emitting a CostObservationPayloadV1.
type ExecutionResourceMeterEvidenceV1 struct {
	SchemaVersion             uint16 `json:"schema_version"`
	ExecutionID               string `json:"execution_id"`
	CPUMillis                 uint64 `json:"cpu_millis"`
	PeakMemoryBytes           uint64 `json:"peak_memory_bytes"`
	DiskWrittenBytes          uint64 `json:"disk_written_bytes"`
	DurationNanoseconds       uint64 `json:"duration_nanoseconds"`
	MeterImplementationDigest string `json:"meter_implementation_digest"`
	ObservedAtUnix            uint64 `json:"observed_at_unix"`
}

func BuildExecutionResourceMeterEvidenceV1(executionID string, usage executor.Usage, meterImplementationDigest,
	issuerDescriptor, visibility, audienceDigest, retentionPolicyDigest, retrievalPolicyDigest,
	authorityTimeProofDigest, issuerQualificationProofDigest string, observedAt time.Time) (OutcomeEvidenceDescriptorV1, error) {
	if executionID == "" || usage.Duration <= 0 || uint64(usage.Duration) == 0 || !validDigest(meterImplementationDigest) || observedAt.IsZero() {
		return OutcomeEvidenceDescriptorV1{}, errors.New("execution resource-meter evidence is invalid")
	}
	evidence := ExecutionResourceMeterEvidenceV1{SchemaVersion: 1, ExecutionID: executionID, CPUMillis: usage.CPUMillis,
		PeakMemoryBytes: usage.PeakMemory, DiskWrittenBytes: usage.DiskWritten, DurationNanoseconds: uint64(usage.Duration),
		MeterImplementationDigest: meterImplementationDigest, ObservedAtUnix: uint64(observedAt.UTC().Unix())}
	canonical, err := codec.Marshal(evidence)
	if err != nil {
		return OutcomeEvidenceDescriptorV1{}, err
	}
	digest, err := codec.Digest("tos.ai.execution-resource-meter-evidence.v1", evidence)
	if err != nil {
		return OutcomeEvidenceDescriptorV1{}, err
	}
	item := commerce.OutcomeEvidenceItemV1{EvidenceRole: "cost_source", EvidenceProfileURI: ExecutionResourceMeterEvidenceProfileV1,
		SourceObjectProfileURI: "tos.ai.executor-usage.v1", SourceObjectDigest: digest, ObjectDigest: digest,
		CanonicalSize: uint64(len(canonical)), MediaType: "application/cbor", IssuerDescriptor: issuerDescriptor,
		SubjectDescriptor: executionID, ClaimedObservationTimeUnix: uint64(observedAt.UTC().Unix()),
		AuthorityTimeProofDigest: authorityTimeProofDigest, IssuerQualificationProofDigest: issuerQualificationProofDigest,
		Visibility: visibility, AudienceDigest: audienceDigest, RetentionPolicyDigest: retentionPolicyDigest,
		RetrievalPolicyDigest: retrievalPolicyDigest}
	return OutcomeEvidenceDescriptorV1{CanonicalEvidence: canonical, EvidenceItem: item}, nil
}
