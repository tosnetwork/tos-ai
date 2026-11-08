package commercegate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/internal/actionauthoritytest"
	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
	"github.com/tosnetwork/tos-service-protocol/pkg/mldsa44"
)

type realAuthorityFixture struct {
	custody   *actionauthoritytest.Custody
	key       authorityKey
	authority *testWriterAuthority
	gate      *Gate
	directory string
	plan      Plan
	fence     commerce.WriterFence
	now       time.Time
}

// newRealAuthorityFixture opens a Gate whose authority lists two real
// ML-DSA-44 controllers and a third controller under a different authority.
func newRealAuthorityFixture(t *testing.T, obligation string) *realAuthorityFixture {
	t.Helper()
	custody := actionauthoritytest.NewCustody(t, actionauthoritytest.Controller, "second-key", "foreign")
	key := custodyKey(t, custody, actionauthoritytest.Controller)
	authority := newTestWriterAuthority(key, 1, "lease:one")
	authority.Keys["authority"] = append(authority.Keys["authority"], custody.PublicKeys["second-key"])
	authority.Keys["authority:foreign"] = [][]byte{custody.PublicKeys["foreign"]}
	now := time.Unix(2_000_000_000, 0).UTC()
	directory := filepath.Join(t.TempDir(), "gate")
	gate, err := Open(directory, authority)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	gate.now = func() time.Time { return now }
	inputPath := filepath.Join(t.TempDir(), "input.txt")
	raw := []byte("immutable input")
	if err := os.WriteFile(inputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return &realAuthorityFixture{custody: custody, key: key, authority: authority, gate: gate, directory: directory,
		plan: testPlan(inputPath, raw, obligation), fence: signedFence(t, key, now, 1, "lease:one"), now: now}
}

// gateState digests every file the Gate holds except its lock, so a refusal
// can be shown to have written nothing.
func gateState(t *testing.T, directory string) string {
	t.Helper()
	var names []string
	contents := map[string][]byte{}
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() == ".commerce-gate.lock" {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		names = append(names, path)
		contents[path] = raw
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		_, _ = hash.Write([]byte(name))
		_, _ = hash.Write(contents[name])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func TestGateAdmitsRealMLDSA44ActionAuthority(t *testing.T) {
	fixture := newRealAuthorityFixture(t, "work:real")
	prepared, prepareAction := authorizedPrepare(t, fixture.plan, fixture.fence, fixture.key, fixture.now)
	ticket, err := fixture.gate.Prepare(context.Background(), fixture.plan, fixture.fence, time.Minute, prepareAction)
	if err != nil {
		t.Fatal(err)
	}
	launch, err := fixture.gate.Start(context.Background(), ticket, fixture.fence,
		authorizedStart(t, prepared, ticket, fixture.fence, fixture.key, fixture.now))
	if err != nil {
		t.Fatal(err)
	}
	closeFiles(launch.Files)
}

// keyNamed returns the custody controller whose key ID is keyID.
func (fixture *realAuthorityFixture) keyNamed(t *testing.T, keyID string) authorityKey {
	t.Helper()
	for _, name := range []string{actionauthoritytest.Controller, "second-key", "foreign"} {
		if fixture.custody.KeyID(t, name) == keyID {
			return custodyKey(t, fixture.custody, name)
		}
	}
	t.Fatalf("no custody controller has key ID %s", keyID)
	return authorityKey{}
}

// candidateFences returns each refusal variant of the fixture fence. When a
// variant still encodes canonically, the returned action is freshly bound to
// that fence envelope and validly signed by the controller holding its key
// ID, so only the fence signature can cause a refusal. A variant whose key ID
// or proof does not encode canonically has no consistent action; the
// original action is returned and the refusal comes from canonical parsing.
func (fixture *realAuthorityFixture) candidateFences(t *testing.T) []actionauthoritytest.Variant {
	t.Helper()
	message, err := commerce.WriterFenceSignatureMessage(fixture.fence.Body)
	if err != nil {
		t.Fatal(err)
	}
	return actionauthoritytest.Variants(t, fixture.custody, fixture.key.controller, "second-key",
		fixture.fence.KeyID, fixture.fence.Proof, message)
}

func withVariant(fence commerce.WriterFence, variant actionauthoritytest.Variant) (commerce.WriterFence, bool) {
	fence.KeyID, fence.Proof = variant.KeyID, variant.Signature
	_, err := commerce.WriterFenceDigest(fence)
	return fence, err == nil
}

func TestGateRefusesEveryNonActionAuthorityFenceAndAction(t *testing.T) {
	fixture := newRealAuthorityFixture(t, "work:refusals")
	ctx := context.Background()
	prepared, prepareAction := authorizedPrepare(t, fixture.plan, fixture.fence, fixture.key, fixture.now)
	empty := gateState(t, fixture.directory)
	fenceVariants := fixture.candidateFences(t)
	for _, variant := range fenceVariants {
		bad, consistent := withVariant(fixture.fence, variant)
		action := prepareAction
		if consistent {
			_, action = authorizedPrepare(t, fixture.plan, bad, fixture.keyNamed(t, bad.KeyID), fixture.now)
		}
		_, err := fixture.gate.Prepare(ctx, fixture.plan, bad, time.Minute, action)
		if err == nil {
			t.Fatalf("Prepare admitted a fence with %s", variant.Name)
		}
		if variant.WantEd25519Err && !errors.Is(err, commerce.ErrEd25519ActionAuthority) {
			t.Fatalf("Prepare refused a fence with %s without the Ed25519 diagnostic: %v", variant.Name, err)
		}
		if gateState(t, fixture.directory) != empty {
			t.Fatalf("Prepare wrote state for a fence with %s", variant.Name)
		}
	}
	prepareMessage, err := commerce.AuthorizedActionSignatureMessage(prepareAction)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range actionauthoritytest.Variants(t, fixture.custody, fixture.key.controller, "second-key",
		prepareAction.AuthorityKeyID, prepareAction.AuthorizationProof, prepareMessage) {
		bad := prepareAction
		bad.AuthorityKeyID, bad.AuthorizationProof = variant.KeyID, variant.Signature
		if _, err := fixture.gate.Prepare(ctx, fixture.plan, fixture.fence, time.Minute, bad); err == nil {
			t.Fatalf("Prepare admitted an action with %s", variant.Name)
		}
		if gateState(t, fixture.directory) != empty {
			t.Fatalf("Prepare wrote state for an action with %s", variant.Name)
		}
	}
	ticket, err := fixture.gate.Prepare(ctx, fixture.plan, fixture.fence, time.Minute, prepareAction)
	if err != nil {
		t.Fatal(err)
	}
	startAction := authorizedStart(t, prepared, ticket, fixture.fence, fixture.key, fixture.now)
	startMessage, err := commerce.AuthorizedActionSignatureMessage(startAction)
	if err != nil {
		t.Fatal(err)
	}
	preparedState := gateState(t, fixture.directory)
	for _, variant := range actionauthoritytest.Variants(t, fixture.custody, fixture.key.controller, "second-key",
		startAction.AuthorityKeyID, startAction.AuthorizationProof, startMessage) {
		bad := startAction
		bad.AuthorityKeyID, bad.AuthorizationProof = variant.KeyID, variant.Signature
		if _, err := fixture.gate.Start(ctx, ticket, fixture.fence, bad); err == nil {
			t.Fatalf("Start admitted an action with %s", variant.Name)
		}
		if gateState(t, fixture.directory) != preparedState {
			t.Fatalf("Start wrote state for an action with %s", variant.Name)
		}
	}
	for _, variant := range fenceVariants {
		bad, consistent := withVariant(fixture.fence, variant)
		action := startAction
		if consistent {
			action = authorizedStart(t, prepared, ticket, bad, fixture.keyNamed(t, bad.KeyID), fixture.now)
		}
		_, err := fixture.gate.Start(ctx, ticket, bad, action)
		if err == nil {
			t.Fatalf("Start admitted a fence with %s", variant.Name)
		}
		if variant.WantEd25519Err && !errors.Is(err, commerce.ErrEd25519ActionAuthority) {
			t.Fatalf("Start refused a fence with %s without the Ed25519 diagnostic: %v", variant.Name, err)
		}
		if gateState(t, fixture.directory) != preparedState {
			t.Fatalf("Start wrote state for a fence with %s", variant.Name)
		}
	}
	launch, err := fixture.gate.Start(ctx, ticket, fixture.fence, startAction)
	if err != nil {
		t.Fatal(err)
	}
	closeFiles(launch.Files)
}

// TestGateRefusesAKeyListedForAnotherAuthority checks that a key is
// authorized only for the authority it is listed under.
func TestGateRefusesAKeyListedForAnotherAuthority(t *testing.T) {
	fixture := newRealAuthorityFixture(t, "work:foreign")
	foreign := custodyKey(t, fixture.custody, "foreign")
	fence := signedFence(t, foreign, fixture.now, 1, "lease:one")
	_, action := authorizedPrepare(t, fixture.plan, fence, foreign, fixture.now)
	empty := gateState(t, fixture.directory)
	if _, err := fixture.gate.Prepare(context.Background(), fixture.plan, fence, time.Minute, action); err == nil {
		t.Fatal("Prepare admitted a fence signed by a key listed for another authority")
	}
	if gateState(t, fixture.directory) != empty {
		t.Fatal("Prepare wrote state for a foreign authority key")
	}
}

type substitutingAuthority struct {
	*testWriterAuthority
	key []byte
}

func (authority substitutingAuthority) ResolveActionAuthorityKey(string, [mldsa44.KeyIDSize]byte, time.Time) ([]byte, error) {
	return append([]byte(nil), authority.key...), nil
}

// TestGateRefusesAResolverKeyThatDoesNotDeriveTheNamedKeyID presents a fence
// and action that name key A's ID but carry valid signatures by key B, to a
// resolver that answers every lookup with key B. Every signature verifies
// under the key returned, so only the re-derivation of the named key ID from
// the resolved key can refuse them.
func TestGateRefusesAResolverKeyThatDoesNotDeriveTheNamedKeyID(t *testing.T) {
	fixture := newRealAuthorityFixture(t, "work:substituted")
	substitute := custodyKey(t, fixture.custody, "second-key")
	fence := fixture.fence
	fenceMessage, err := commerce.WriterFenceSignatureMessage(fence.Body)
	if err != nil {
		t.Fatal(err)
	}
	fence.Proof = fixture.custody.SignFor(t, substitute.controller, "action-authority", fenceMessage)
	_, unsigned := authorizedPrepare(t, fixture.plan, fence, fixture.key, fixture.now)
	action := unsigned
	action.AuthorizationProof = ""
	actionMessage, err := commerce.AuthorizedActionSignatureMessage(action)
	if err != nil {
		t.Fatal(err)
	}
	action.AuthorizationProof = fixture.custody.SignFor(t, substitute.controller, "action-authority", actionMessage)
	if err := fixture.gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate, err := Open(fixture.directory, substitutingAuthority{testWriterAuthority: fixture.authority, key: substitute.publicKey})
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	gate.now = func() time.Time { return fixture.now }
	if _, err := gate.Prepare(context.Background(), fixture.plan, fence, time.Minute, action); err == nil {
		t.Fatal("Prepare admitted a fence whose resolved key does not derive its key ID")
	}
}
