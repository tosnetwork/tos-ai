package edgeintegration

import (
	"context"
	"errors"
	"strings"
	"testing"

	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

func TestDeploymentRejectsInvalidComposition(t *testing.T) {
	if _, err := New(nil, nil); err == nil {
		t.Fatal("nil composition context was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled composition error=%v", err)
	}
	if _, err := New(context.Background(), nil); err == nil {
		t.Fatal("nil Worker client was accepted")
	}
	var deployment *Deployment
	if _, err := deployment.Worker(); err == nil {
		t.Fatal("nil deployment returned a Worker")
	}
	if _, err := deployment.ProfilePlan(); err == nil {
		t.Fatal("nil deployment returned a profile plan")
	}
}

func TestExternalRouteFingerprintIsOrderIndependentAndBindsIdentity(t *testing.T) {
	capability := func(service, revision string) *edgev1.Capability {
		return &edgev1.Capability{
			ServiceId: service, Operation: "generate", Model: "model-a",
			ModelDigest: "sha256:" + strings.Repeat("a", 64),
			Runtime:     "mock", RuntimeRevision: revision,
			MaxInputBytes: 1024, MaxOutputBytes: 1024,
			AcceptedPriorities: []edgev1.Priority{
				edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
			},
		}
	}
	left := []*edgev1.Capability{
		capability("tos.ai.one", "runtime-v1"),
		capability("tos.ai.two", "runtime-v1"),
	}
	right := []*edgev1.Capability{left[1], left[0]}
	leftFingerprint, err := fingerprintExternalRoutes(left)
	if err != nil {
		t.Fatal(err)
	}
	rightFingerprint, err := fingerprintExternalRoutes(right)
	if err != nil || leftFingerprint != rightFingerprint {
		t.Fatalf("route order changed fingerprint: %x %x err=%v", leftFingerprint, rightFingerprint, err)
	}
	changed := []*edgev1.Capability{
		capability("tos.ai.one", "runtime-v2"), left[1],
	}
	changedFingerprint, err := fingerprintExternalRoutes(changed)
	if err != nil || changedFingerprint == leftFingerprint {
		t.Fatalf("runtime identity drift was not committed: %x err=%v", changedFingerprint, err)
	}
}
