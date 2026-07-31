package runtime

import (
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
)

func TestValidatePreflightRequiresExactBindingAndEvidence(t *testing.T) {
	capability := Capability{
		ServiceID: "service", Operation: "generate", Model: "model",
		ModelDigest: "sha256:" + strings.Repeat("a", 64),
		Runtime:     "test", RuntimeRevision: "v1",
		MaxInputBytes: 1, MaxOutputBytes: 1,
		AcceptedPriorities: []Priority{PriorityExternalService},
		Admission: admission.Resources{
			RAMBytes: 1, ContextTokens: 1, BatchSize: 1,
			ExecutionTime: time.Second,
		},
	}
	valid := Preflight{
		Model: capability.Model, ModelDigest: capability.ModelDigest,
		DigestEvidence: BindingLocallyObserved,
	}
	if err := ValidatePreflight(capability, valid); err != nil {
		t.Fatal(err)
	}
	declared := valid
	declared.DigestEvidence = BindingDeclared
	if err := ValidatePreflight(capability, declared); err != nil {
		t.Fatal(err)
	}
	tests := []Preflight{
		{Model: "other", ModelDigest: valid.ModelDigest, DigestEvidence: valid.DigestEvidence},
		{Model: valid.Model, ModelDigest: "sha256:" + strings.Repeat("b", 64), DigestEvidence: valid.DigestEvidence},
		{Model: valid.Model, ModelDigest: valid.ModelDigest, DigestEvidence: "attested"},
	}
	for _, invalid := range tests {
		if err := ValidatePreflight(capability, invalid); err == nil {
			t.Fatalf("invalid preflight accepted: %#v", invalid)
		}
	}
}
