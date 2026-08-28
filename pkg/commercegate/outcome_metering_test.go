package commercegate

import (
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
)

func TestBuildExecutionResourceMeterEvidenceDoesNotClaimMonetaryCost(t *testing.T) {
	digest := func(fill string) string { return "sha256:" + strings.Repeat(fill, 64) }
	now := time.Unix(2_000_000_000, 0).UTC()
	result, err := BuildExecutionResourceMeterEvidenceV1(digest("1"), executor.Usage{CPUMillis: 2, PeakMemory: 4096,
		DiskWritten: 30, Duration: 5 * time.Millisecond}, digest("2"), "issuer:executor", "local_private", digest("3"),
		digest("4"), digest("5"), digest("6"), digest("7"), now)
	if err != nil {
		t.Fatal(err)
	}
	manifest := commerce.OutcomeEvidenceManifestV1{SchemaVersion: 1, ManifestPurpose: "meter_test",
		AuthorityProofRefs: []commerce.OutcomeAuthorityProofRefV1{
			{ProofProfileURI: commerce.OutcomeAuthorityTimeProofProfileV1, ObjectDigest: digest("6"), CanonicalSize: 1},
			{ProofProfileURI: commerce.OutcomeIssuerQualificationProofProfileV1, ObjectDigest: digest("7"), CanonicalSize: 1}},
		EvidenceItems: []commerce.OutcomeEvidenceItemV1{result.EvidenceItem}}
	if err := commerce.SortOutcomeAuthorityProofRefsV1(manifest.AuthorityProofRefs); err != nil {
		t.Fatal(err)
	}
	if err := commerce.ValidateOutcomeEvidenceManifestV1(manifest); err != nil {
		t.Fatal(err)
	}
	if result.EvidenceItem.EvidenceRole != "cost_source" || result.EvidenceItem.ObjectDigest == "" || len(result.CanonicalEvidence) == 0 {
		t.Fatalf("invalid meter evidence: %+v", result)
	}
	if _, err = BuildExecutionResourceMeterEvidenceV1(digest("1"), executor.Usage{Duration: 0}, digest("2"),
		"issuer", "local_private", digest("3"), digest("4"), digest("5"), digest("6"), digest("7"), now); err == nil {
		t.Fatal("zero-duration usage was accepted")
	}
}
