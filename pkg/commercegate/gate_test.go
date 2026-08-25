package commercegate

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
)

type testWriterAuthority struct {
	key        ed25519.PublicKey
	generation uint64
	lease      string
}

func (authority *testWriterAuthority) AuthorizeFenceKey(_ string, key ed25519.PublicKey, _ time.Time) error {
	if !key.Equal(authority.key) {
		return errors.New("wrong authority key")
	}
	return nil
}
func (authority *testWriterAuthority) ConfirmCurrentWriterFence(fence commerce.WriterFence, _ time.Time) error {
	if fence.Body.WriterGeneration != authority.generation || fence.Body.LeaseID != authority.lease {
		return errors.New("stale writer")
	}
	return nil
}

func TestGateStartsExactImmutableResourcesOnce(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	fence := signedFence(t, key, now, 1, "lease:one")
	authority := &testWriterAuthority{key: key.Public().(ed25519.PublicKey), generation: 1, lease: "lease:one"}
	directory := filepath.Join(t.TempDir(), "gate")
	gate, err := Open(directory, authority)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	gate.now = func() time.Time { return now }
	inputPath := filepath.Join(t.TempDir(), "input.txt")
	original := []byte("immutable input")
	if err := os.WriteFile(inputPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := testPlan(inputPath, original, "work:one")
	prepared, prepareAction := authorizedPrepare(t, plan, fence, key, now)
	ticket, err := gate.Prepare(context.Background(), plan, fence, time.Minute, prepareAction)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := gate.Prepare(context.Background(), plan, fence, time.Minute, prepareAction); err != nil || retry != ticket {
		t.Fatalf("prepare retry=%+v err=%v", retry, err)
	}
	if err := os.WriteFile(inputPath, []byte("tampered input!"), 0o600); err != nil {
		t.Fatal(err)
	}
	startAction := authorizedStart(t, prepared, ticket, fence, key, now)
	if _, err := gate.Start(context.Background(), ticket, fence, startAction); err == nil {
		t.Fatal("mutated input started")
	}
	if err := os.WriteFile(inputPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	launch, err := gate.Start(context.Background(), ticket, fence, startAction)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFiles(launch.Files)
	if err := os.Rename(inputPath, inputPath+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(launch.Files[0])
	if err != nil || string(raw) != string(original) {
		t.Fatalf("launched handle=%q err=%v", raw, err)
	}
	if err := gate.MarkRunning(launch); err != nil {
		t.Fatal(err)
	}
	if err := gate.Complete(launch, StateSucceeded, digest([]byte("outcome"))); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Start(context.Background(), ticket, fence, startAction); err == nil {
		t.Fatal("terminal execution started twice")
	}
}

func TestGateRejectsStaleTakeoverAndRecoversStartingAsAmbiguous(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	fence1 := signedFence(t, key, now, 1, "lease:one")
	authority := &testWriterAuthority{key: key.Public().(ed25519.PublicKey), generation: 1, lease: "lease:one"}
	directory := filepath.Join(t.TempDir(), "gate")
	gate, _ := Open(directory, authority)
	gate.now = func() time.Time { return now }
	inputPath := filepath.Join(t.TempDir(), "input.txt")
	raw := []byte("input")
	_ = os.WriteFile(inputPath, raw, 0o600)
	plan := testPlan(inputPath, raw, "work:ambiguous")
	prepared, prepareAction := authorizedPrepare(t, plan, fence1, key, now)
	ticket, err := gate.Prepare(context.Background(), plan, fence1, time.Minute, prepareAction)
	if err != nil {
		t.Fatal(err)
	}
	authority.generation, authority.lease = 2, "lease:two"
	startAction := authorizedStart(t, prepared, ticket, fence1, key, now)
	if _, err := gate.Start(context.Background(), ticket, fence1, startAction); err == nil {
		t.Fatal("stale writer started after takeover")
	}
	fence2 := signedFence(t, key, now, 2, "lease:two")
	if _, err := gate.Start(context.Background(), ticket, fence2, startAction); err == nil {
		t.Fatal("new writer substituted itself into old prepared slot")
	}
	// No takeover: start reaches the durable STARTING point and then the process
	// disappears before MarkRunning. Recovery must report ambiguity, not retry.
	authority.generation, authority.lease = 1, "lease:one"
	launch, err := gate.Start(context.Background(), ticket, fence1, startAction)
	if err != nil {
		t.Fatal(err)
	}
	closeFiles(launch.Files)
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(directory, authority)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	recovered.now = func() time.Time { return now }
	state, _, err := recovered.Resolve(ticket.ExecutionID)
	if err != nil || state != StateAmbiguousStart {
		t.Fatalf("recovered state=%s err=%v", state, err)
	}
	if _, err := recovered.Start(context.Background(), ticket, fence1, startAction); err == nil {
		t.Fatal("ambiguous STARTING slot was launched again")
	}
	_ = fence2
}

func TestExecutionIdentitySurvivesWriterTakeoverButBoundPlanDoesNot(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	fence1 := signedFence(t, key, now, 1, "lease:one")
	fence2 := signedFence(t, key, now, 2, "lease:two")
	path := filepath.Join(t.TempDir(), "input")
	raw := []byte("input")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := testPlan(path, raw, "work:stable-takeover")
	first, _, _, err := PrepareAuthorizationMaterial(plan, fence1)
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, err := PrepareAuthorizationMaterial(plan, fence2)
	if err != nil {
		t.Fatal(err)
	}
	if first.ExecutionID != second.ExecutionID || first.CanonicalPlanDigest != second.CanonicalPlanDigest {
		t.Fatalf("takeover changed semantic execution identity: first=%+v second=%+v", first, second)
	}
	firstBound, firstDigest, _ := preparePlan(first, fence1)
	secondBound, secondDigest, _ := preparePlan(second, fence2)
	if firstBound.WriterGeneration == secondBound.WriterGeneration || firstDigest == secondDigest {
		t.Fatal("writer-bound plan did not retain the fencing generation")
	}
}

func TestGateRejectsPrivateOrRedirectableNetworkCapabilities(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	fence := signedFence(t, key, now, 1, "lease:network")
	authority := &testWriterAuthority{key: key.Public().(ed25519.PublicKey), generation: 1, lease: "lease:network"}
	gate, err := Open(filepath.Join(t.TempDir(), "gate"), authority)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	gate.now = func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "input")
	raw := []byte("input")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := testPlan(path, raw, "work:network")
	plan.NetworkBindings = []NetworkBinding{{BindingID: "source", Scheme: "https", Host: "example.com", Port: 443,
		ResolvedIPs: []string{"127.0.0.1"}, TLSServerName: "example.com", MaxRequestBytes: 1024, MaxResponseBytes: 1024}}
	_, action := authorizedPrepare(t, plan, fence, key, now)
	if _, err := gate.Prepare(context.Background(), plan, fence, time.Minute, action); err == nil {
		t.Fatal("private target entered execution plan")
	}
	plan.NetworkBindings[0].ResolvedIPs = []string{"1.1.1.1"}
	plan.NetworkBindings[0].AllowRedirects = true
	_, action = authorizedPrepare(t, plan, fence, key, now)
	if _, err := gate.Prepare(context.Background(), plan, fence, time.Minute, action); err == nil {
		t.Fatal("redirectable target entered execution plan")
	}
	plan.NetworkBindings[0].AllowRedirects = false
	_, action = authorizedPrepare(t, plan, fence, key, now)
	if _, err := gate.Prepare(context.Background(), plan, fence, time.Minute, action); err != nil {
		t.Fatal(err)
	}
}

func TestGateRecoversRunningExecutionAsAmbiguousWithoutRestart(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	fence := signedFence(t, key, now, 1, "lease:running")
	authority := &testWriterAuthority{key: key.Public().(ed25519.PublicKey), generation: 1, lease: "lease:running"}
	directory := filepath.Join(t.TempDir(), "gate")
	gate, err := Open(directory, authority)
	if err != nil {
		t.Fatal(err)
	}
	gate.now = func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "input")
	raw := []byte("input")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := testPlan(path, raw, "work:running")
	prepared, prepareAction := authorizedPrepare(t, plan, fence, key, now)
	ticket, err := gate.Prepare(context.Background(), plan, fence, time.Minute, prepareAction)
	if err != nil {
		t.Fatal(err)
	}
	launch, err := gate.Start(context.Background(), ticket, fence, authorizedStart(t, prepared, ticket, fence, key, now))
	if err != nil {
		t.Fatal(err)
	}
	closeFiles(launch.Files)
	if err := gate.MarkRunning(launch); err != nil {
		t.Fatal(err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(directory, authority)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	state, _, err := recovered.Resolve(ticket.ExecutionID)
	if err != nil || state != StateAmbiguousRun {
		t.Fatalf("recovered state=%s err=%v", state, err)
	}
}

func testPlan(path string, raw []byte, obligation string) Plan {
	return Plan{OwnerID: "owner", AgentID: "agent", AgreementBodyDigest: digest([]byte("agreement")), ExecutionObligationID: obligation,
		AcceptedInputManifestDigest: digest([]byte("manifest")), AttemptIndex: 0,
		PredecessorTerminalResolutionDigest: "sha256:" + strings.Repeat("0", 64), ReservationID: digest([]byte(obligation)),
		PolicyRevision: 1, LeaseLossPolicy: LeaseLossKill, Files: []FileBinding{{Path: path, Digest: digest(raw), Size: uint64(len(raw))}}}
}

func signedFence(t *testing.T, key ed25519.PrivateKey, now time.Time, generation uint64, lease string) commerce.WriterFence {
	t.Helper()
	fence, err := commerce.SignWriterFence(commerce.WriterFenceBody{SchemaVersion: 1, OwnerID: "owner", AgentID: "agent", InstanceID: "runtime",
		LeaseID: lease, WriterGeneration: generation, IssuedAtUnix: uint64(now.Unix()), ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()),
		AuthorityID: "authority", Scope: []string{"execution.prepare", "execution.start"}}, key)
	if err != nil {
		t.Fatal(err)
	}
	return fence
}

func authorizedPrepare(t *testing.T, plan Plan, fence commerce.WriterFence, key ed25519.PrivateKey, now time.Time) (Plan, commerce.AuthorizedAction) {
	t.Helper()
	prepared, request, fields, err := PrepareAuthorizationMaterial(plan, fence)
	if err != nil {
		return Plan{}, commerce.AuthorizedAction{}
	}
	action, err := commerce.BuildAuthorizedAction(plan.OwnerID, plan.AgentID, "execution.prepare", fields, request, fence, plan.PolicyRevision,
		digest([]byte("mandate")), "", "absent", uint64(now.Add(30*time.Minute).Unix()))
	if err != nil {
		t.Fatal(err)
	}
	action, err = commerce.SignAuthorizedAction(action, key)
	if err != nil {
		t.Fatal(err)
	}
	return prepared, action
}

func authorizedStart(t *testing.T, plan Plan, ticket Ticket, fence commerce.WriterFence, key ed25519.PrivateKey, now time.Time) commerce.AuthorizedAction {
	t.Helper()
	request, fields, err := StartAuthorizationMaterial(plan, ticket)
	if err != nil {
		t.Fatal(err)
	}
	action, err := commerce.BuildAuthorizedAction(plan.OwnerID, plan.AgentID, "execution.start", fields, request, fence, plan.PolicyRevision,
		digest([]byte("mandate")), "", "prepared", uint64(now.Add(30*time.Minute).Unix()))
	if err != nil {
		t.Fatal(err)
	}
	action, err = commerce.SignAuthorizedAction(action, key)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
