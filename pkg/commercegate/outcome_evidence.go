package commercegate

import (
	"errors"
	"strings"
	"time"

	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
	"github.com/tosnetwork/tos-service-protocol/pkg/codec"
)

const ExecutionOutcomeEvidenceProfileV1 = "tos.ai.execution-gate-evidence.v1"

type ExecutionOutcomeEvidenceV1 struct {
	SchemaVersion        uint16 `json:"schema_version"`
	ExecutionID          string `json:"execution_id"`
	AgreementBodyDigest  string `json:"agreement_body_digest"`
	ObligationID         string `json:"obligation_id"`
	PlanDigest           string `json:"plan_digest"`
	State                State  `json:"state"`
	StateRevision        uint64 `json:"state_revision"`
	OutcomeDigest        string `json:"outcome_digest,omitempty"`
	PrepareActionID      string `json:"prepare_action_id"`
	PrepareRequestDigest string `json:"prepare_request_digest"`
	StartActionID        string `json:"start_action_id,omitempty"`
	StartRequestDigest   string `json:"start_request_digest,omitempty"`
	ObservedAtUnix       uint64 `json:"observed_at_unix"`
}

// BuildOperationOutcomeArtifacts exports the exact durable Gate record as a
// released Gate-execution assertion. The caller still signs the outer Agent
// Operation; this function neither publishes nor grants execution authority.
func (gate *Gate) BuildOperationOutcomeArtifacts(executionID, issuerDescriptor, visibility,
	audienceDigest, retentionPolicyDigest, retrievalPolicyDigest, gatePolicyDigest string,
	authorityProofs []commerce.OutcomeAuthorityProofMaterialV1, observedAt time.Time) (
	commerce.OperationOutcomeEventBodyV1, commerce.OperationOutcomeArtifactBundleV1, error) {
	if gate == nil || !validDigest(gatePolicyDigest) || len(authorityProofs) != 2 {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, errors.New("Gate outcome artifact request is incomplete")
	}
	proofs := append([]commerce.OutcomeAuthorityProofMaterialV1(nil), authorityProofs...)
	if err := commerce.SortOutcomeAuthorityProofMaterialsV1(proofs); err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	refs := make([]commerce.OutcomeAuthorityProofRefV1, len(proofs))
	var authorityTimeDigest, qualificationDigest string
	for index, material := range proofs {
		digest, err := commerce.OutcomeAuthorityProofObjectDigestV1(material)
		if err != nil {
			return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
		}
		refs[index] = commerce.OutcomeAuthorityProofRefV1{ProofProfileURI: material.ProofProfileURI, ObjectDigest: digest, CanonicalSize: uint64(len(material.CanonicalObject))}
		switch material.ProofProfileURI {
		case commerce.OutcomeAuthorityTimeProofProfileV1:
			authorityTimeDigest = digest
		case commerce.OutcomeIssuerQualificationProofProfileV1:
			qualificationDigest = digest
		default:
			return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, errors.New("Gate outcome authority proof profile is unsupported")
		}
	}
	if authorityTimeDigest == "" || qualificationDigest == "" || commerce.SortOutcomeAuthorityProofRefsV1(refs) != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, errors.New("Gate outcome requires one proof of each authority profile")
	}
	descriptor, err := gate.ExportOutcomeEvidence(executionID, issuerDescriptor, executionID, visibility, audienceDigest,
		retentionPolicyDigest, retrievalPolicyDigest, authorityTimeDigest, qualificationDigest,
		proofSize(refs, authorityTimeDigest), proofSize(refs, qualificationDigest), observedAt)
	if err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	gate.mu.Lock()
	entry, readErr := readRecord(gate.path(executionID))
	gate.mu.Unlock()
	if readErr != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, readErr
	}
	resourceDigest, err := codec.Digest("tos.ai.execution-resource-set.v1", struct {
		Files   []FileBinding    `json:"files"`
		Network []NetworkBinding `json:"network"`
	}{entry.Plan.Files, entry.Plan.NetworkBindings})
	if err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	credentialDigest, err := codec.Digest("tos.ai.execution-credential-set.v1", entry.Plan.CredentialBindings)
	if err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	effectDigest, err := codec.Digest("tos.ai.execution-effect-set.v1", entry.Plan.EffectBindings)
	if err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	assertion := commerce.GateExecutionObservationV1{ExecutionID: executionID, AgreementBodyDigest: entry.Plan.AgreementBodyDigest,
		ObligationID: entry.Plan.ExecutionObligationID, PlanDigest: entry.PlanDigest, GatePolicyDigest: gatePolicyDigest,
		InputSetDigest: entry.Plan.AcceptedInputManifestDigest, ResourceSetDigest: resourceDigest, CredentialSetDigest: credentialDigest,
		EffectSetDigest: effectDigest, State: strings.ToLower(string(entry.State)), StateRevision: entry.StateRevision,
		StartActionID: entry.StartActionID, StartRequestDigest: entry.StartRequestDigest, AuthoritativeRecord: descriptor.EvidenceItem.ObjectDigest,
		ObservedAtUnix: uint64(observedAt.UTC().Unix())}
	assertionPayload, err := codec.Marshal(assertion)
	if err != nil || commerce.ValidateGateExecutionObservationV1(assertion) != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, errors.New("Gate outcome assertion is invalid")
	}
	manifest := commerce.OutcomeEvidenceManifestV1{SchemaVersion: 1, ManifestPurpose: "gate_execution_assertion",
		AuthorityProofRefs: refs, EvidenceItems: []commerce.OutcomeEvidenceItemV1{descriptor.EvidenceItem}}
	event, err := commerce.BuildOperationOutcomeEventV1(commerce.OutcomeTransitionObservation,
		commerce.OutcomeSubjectRefV1{SubjectProfileURI: "tos.subject.execution.v1", SubjectID: executionID}, nil,
		commerce.OutcomeProfileGateExecution, assertionPayload, manifest, commerce.EmptyOutcomeExtensionSetV1())
	if err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	bundle := commerce.OperationOutcomeArtifactBundleV1{AssertionPayload: assertionPayload, EvidenceManifest: manifest,
		ExtensionSet: commerce.EmptyOutcomeExtensionSetV1(), AuthorityProofs: proofs}
	if err := commerce.VerifyOperationOutcomeArtifactBundleV1(event, bundle); err != nil {
		return commerce.OperationOutcomeEventBodyV1{}, commerce.OperationOutcomeArtifactBundleV1{}, err
	}
	return event, bundle, nil
}

func proofSize(refs []commerce.OutcomeAuthorityProofRefV1, digest string) uint64 {
	for _, ref := range refs {
		if ref.ObjectDigest == digest {
			return ref.CanonicalSize
		}
	}
	return 0
}

type OutcomeEvidenceDescriptorV1 struct {
	CanonicalEvidence []byte                         `json:"canonical_evidence"`
	EvidenceItem      commerce.OutcomeEvidenceItemV1 `json:"evidence_item"`
}

// ExportOutcomeEvidence exposes exact Gate state as a content-addressed
// source object. It does not sign or qualify itself: the caller must provide
// independently verified authority-time and issuer-qualification proof
// digests, both of which the final event manifest must retain.
func (gate *Gate) ExportOutcomeEvidence(executionID, issuerDescriptor, subjectDescriptor, visibility,
	audienceDigest, retentionPolicyDigest, retrievalPolicyDigest, authorityTimeProofDigest,
	issuerQualificationProofDigest string, authorityTimeProofSize, issuerQualificationProofSize uint64,
	observedAt time.Time) (OutcomeEvidenceDescriptorV1, error) {
	if gate == nil || gate.lock == nil || executionID == "" || observedAt.IsZero() {
		return OutcomeEvidenceDescriptorV1{}, errors.New("execution outcome evidence request is invalid")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	entry, err := readRecord(gate.path(executionID))
	if err != nil {
		return OutcomeEvidenceDescriptorV1{}, err
	}
	if entry.StateRevision == 0 || entry.Plan.ExecutionID != executionID {
		return OutcomeEvidenceDescriptorV1{}, errors.New("execution Gate record is inconsistent")
	}
	evidence := ExecutionOutcomeEvidenceV1{SchemaVersion: 1, ExecutionID: executionID,
		AgreementBodyDigest: entry.Plan.AgreementBodyDigest, ObligationID: entry.Plan.ExecutionObligationID,
		PlanDigest: entry.PlanDigest, State: entry.State, StateRevision: entry.StateRevision,
		OutcomeDigest: canonicalOutcomeDigest(entry.OutcomeDigest), PrepareActionID: entry.PrepareActionID,
		PrepareRequestDigest: entry.PrepareRequestDigest, StartActionID: canonicalOutcomeDigest(entry.StartActionID),
		StartRequestDigest: canonicalOutcomeDigest(entry.StartRequestDigest), ObservedAtUnix: uint64(observedAt.UTC().Unix())}
	canonical, err := codec.Marshal(evidence)
	if err != nil {
		return OutcomeEvidenceDescriptorV1{}, err
	}
	digest, err := codec.Digest("tos.ai.execution-gate-evidence.v1", evidence)
	if err != nil {
		return OutcomeEvidenceDescriptorV1{}, err
	}
	item := commerce.OutcomeEvidenceItemV1{EvidenceRole: "authoritative_resolution", EvidenceProfileURI: ExecutionOutcomeEvidenceProfileV1,
		SourceObjectProfileURI: "tos.ai.local-execution-gate.v1", SourceObjectDigest: digest, ObjectDigest: digest,
		CanonicalSize: uint64(len(canonical)), MediaType: "application/cbor", IssuerDescriptor: issuerDescriptor,
		SubjectDescriptor: subjectDescriptor, ClaimedObservationTimeUnix: uint64(observedAt.UTC().Unix()),
		AuthorityTimeProofDigest: authorityTimeProofDigest, IssuerQualificationProofDigest: issuerQualificationProofDigest,
		Visibility: visibility, AudienceDigest: audienceDigest, RetentionPolicyDigest: retentionPolicyDigest,
		RetrievalPolicyDigest: retrievalPolicyDigest}
	manifest := commerce.OutcomeEvidenceManifestV1{SchemaVersion: 1, ManifestPurpose: "gate_export_validation",
		AuthorityProofRefs: []commerce.OutcomeAuthorityProofRefV1{
			{ProofProfileURI: commerce.OutcomeAuthorityTimeProofProfileV1, ObjectDigest: authorityTimeProofDigest, CanonicalSize: authorityTimeProofSize},
			{ProofProfileURI: commerce.OutcomeIssuerQualificationProofProfileV1, ObjectDigest: issuerQualificationProofDigest, CanonicalSize: issuerQualificationProofSize}},
		EvidenceItems: []commerce.OutcomeEvidenceItemV1{item}}
	_ = commerce.SortOutcomeAuthorityProofRefsV1(manifest.AuthorityProofRefs)
	if err := commerce.ValidateOutcomeEvidenceManifestV1(manifest); err != nil {
		return OutcomeEvidenceDescriptorV1{}, err
	}
	return OutcomeEvidenceDescriptorV1{CanonicalEvidence: canonical, EvidenceItem: item}, nil
}

func canonicalOutcomeDigest(value string) string {
	if len(value) == 71 && len(value) > 7 && value[:7] == "sha256:" {
		return value
	}
	return ""
}
