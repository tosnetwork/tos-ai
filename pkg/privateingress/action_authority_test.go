package privateingress

import (
	"context"
	"crypto/ed25519"
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

type uploadFixture struct {
	custody       *actionauthoritytest.Custody
	owner         ownerKey
	authority     testAuthority
	store         *Store
	directory     string
	fence         commerce.WriterFence
	authorization commerce.SignedPrivateHandoffAuthorization
	ciphertext    []byte
	action        commerce.AuthorizedAction
	canonical     []byte
	fields        map[string]commerce.SemanticValue
	now           time.Time
}

func deterministicEd25519(fill byte) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = fill
	}
	return ed25519.NewKeyFromSeed(seed)
}

func custodyOwner(t *testing.T, custody *actionauthoritytest.Custody, controller string) ownerKey {
	t.Helper()
	return ownerKey{signer: custody.Signer, verifier: custody.Signer, controller: controller,
		keyID: custody.KeyID(t, controller), publicKey: custody.PublicKeys[controller]}
}

func signUploadFence(t *testing.T, owner ownerKey, now time.Time) commerce.WriterFence {
	t.Helper()
	fence, err := commerce.SignWriterFence(context.Background(), commerce.WriterFenceBody{SchemaVersion: 1, OwnerID: "owner:test",
		AgentID: "agent:receiver", InstanceID: "instance:test", LeaseID: "lease:test", WriterGeneration: 1,
		IssuedAtUnix: uint64(now.Unix()), ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()), AuthorityID: "authority:test",
		Scope: []string{"content.delete", "content.upload"}}, owner.signer, owner.controller)
	if err != nil {
		t.Fatal(err)
	}
	return fence
}

func signUploadAction(t *testing.T, fixture *uploadFixture, owner ownerKey, fence commerce.WriterFence) commerce.AuthorizedAction {
	t.Helper()
	mandate := sha256.Sum256([]byte("mandate"))
	action, err := commerce.BuildAuthorizedAction("owner:test", "agent:receiver", "content.upload", fixture.fields, fixture.canonical,
		fence, 1, "sha256:"+hex.EncodeToString(mandate[:]), "", "challenge_issued", uint64(fixture.now.Add(time.Hour).Unix()))
	if err != nil {
		t.Fatal(err)
	}
	action, err = commerce.SignAuthorizedAction(context.Background(), action, owner.keyID, owner.signer, owner.controller)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

// newUploadFixture opens an ingress store whose authority lists two real
// ML-DSA-44 controllers and a third controller under another authority,
// issues one challenge and seals one upload against it.
func newUploadFixture(t *testing.T) *uploadFixture {
	t.Helper()
	custody := actionauthoritytest.NewCustody(t, actionauthoritytest.Controller, "second-key", "foreign")
	owner := custodyOwner(t, custody, actionauthoritytest.Controller)
	now := time.Unix(2_000_000_000, 0).UTC()
	fence := signUploadFence(t, owner, now)
	authority := newTestAuthority(owner, fence)
	authority.Keys["authority:test"] = append(authority.Keys["authority:test"], custody.PublicKeys["second-key"])
	authority.Keys["authority:foreign"] = [][]byte{custody.PublicKeys["foreign"]}
	receiver, sender := deterministicEd25519(0x21), deterministicEd25519(0x22)
	directory := filepath.Join(t.TempDir(), "ingress")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := Open(directory, "agent:receiver", receiver, testHandoffResolver{
		"agent:receiver": receiver.Public().(ed25519.PublicKey), "agent:sender": sender.Public().(ed25519.PublicKey)}, authority)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	digest := func(value string) string {
		hash := sha256.Sum256([]byte(value))
		return "sha256:" + hex.EncodeToString(hash[:])
	}
	challenge, err := store.IssueChallenge(commerce.PrivateHandoffChallengeBody{SchemaVersion: 1, HandoffID: "handoff:pq",
		AgreementBodyDigest: digest("agreement"), ObligationID: "obligation:input", SenderAgentID: "agent:sender",
		ReceiverAgentID: "agent:receiver", Direction: "input", PurposeDigest: digest("purpose"),
		IngressProfileURI: "tos.private-ingress.v1", IngressInstanceID: "ingress:test", MaximumPlaintextBytes: 1024,
		MaximumCiphertextBytes: 1040, MaximumFiles: 1, AcceptedMediaTypes: []string{"application/octet-stream"},
		RetentionPolicyDigest: digest("retention"), IssuedAtUnix: uint64(now.Unix()),
		ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()), DeleteNotAfterUnix: uint64(now.Add(24 * time.Hour).Unix())})
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte("immutable private input")
	plainHash := sha256.Sum256(plaintext)
	manifest := commerce.PrivateContentManifest{ContentDigest: "sha256:" + hex.EncodeToString(plainHash[:]),
		MediaType: "application/octet-stream", FileCount: 1, CanonicalPaths: []string{"input.bin"},
		PlaintextBytes: uint64(len(plaintext)), MaximumExpandedBytes: uint64(len(plaintext)),
		CompressionProfileURI: "tos.compression.none.v1"}
	senderFields := map[string]commerce.SemanticValue{"owner_id": commerce.ID("owner:sender"), "agent_id": commerce.ID("agent:sender"),
		"agreement_body_digest": commerce.Digest32(challenge.Body.AgreementBodyDigest), "obligation_id": commerce.ID(challenge.Body.ObligationID),
		"recipient_id": commerce.ID(challenge.Body.ReceiverAgentID), "content_digest": commerce.Digest32(manifest.ContentDigest),
		"purpose_digest": commerce.Digest32(challenge.Body.PurposeDigest)}
	senderActionID, _, err := commerce.DeriveStableActionID("disclosure.release", senderFields)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, authorization, err := commerce.SealPrivateContent(challenge, manifest, plaintext, senderActionID, sender)
	if err != nil {
		t.Fatal(err)
	}
	canonical, fields, err := UploadAuthorizationMaterial("owner:test", "agent:receiver", challenge, authorization)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &uploadFixture{custody: custody, owner: owner, authority: authority, store: store, directory: directory, fence: fence,
		authorization: authorization, ciphertext: ciphertext, canonical: canonical, fields: fields, now: now}
	fixture.action = signUploadAction(t, fixture, owner, fence)
	return fixture
}

func (fixture *uploadFixture) accept(action commerce.AuthorizedAction, fence commerce.WriterFence) error {
	_, err := fixture.store.Accept(context.Background(), fixture.authorization.Body.ChallengeDigest, fixture.authorization,
		fixture.ciphertext, action, fence)
	return err
}

// ingressState digests every file the store holds except its lock.
func ingressState(t *testing.T, directory string) string {
	t.Helper()
	var names []string
	contents := map[string][]byte{}
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(entry.Name()) == ".lock" {
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

func TestIngressRefusesEveryNonActionAuthorityFenceAndAction(t *testing.T) {
	fixture := newUploadFixture(t)
	before := ingressState(t, fixture.directory)
	fenceMessage, err := commerce.WriterFenceSignatureMessage(fixture.fence.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range actionauthoritytest.Variants(t, fixture.custody, fixture.owner.controller, "second-key",
		fixture.fence.KeyID, fixture.fence.Proof, fenceMessage) {
		// A variant that still encodes canonically is made the current writer
		// and gets an action freshly bound to it and validly signed by the
		// controller holding its key ID, so only the fence signature can
		// refuse it. A non-canonical variant has no consistent action and is
		// refused by canonical parsing.
		bad := fixture.fence
		bad.KeyID, bad.Proof = variant.KeyID, variant.Signature
		action := fixture.action
		if _, digestErr := commerce.WriterFenceDigest(bad); digestErr == nil {
			action = signUploadAction(t, fixture, fixture.ownerNamed(t, bad.KeyID), bad)
			*fixture.authority.current = bad
		}
		err := fixture.accept(action, bad)
		*fixture.authority.current = fixture.fence
		if err == nil {
			t.Fatalf("upload admitted a fence with %s", variant.Name)
		}
		if variant.WantEd25519Err && !errors.Is(err, commerce.ErrEd25519ActionAuthority) {
			t.Fatalf("upload refused a fence with %s without the Ed25519 diagnostic: %v", variant.Name, err)
		}
		if ingressState(t, fixture.directory) != before {
			t.Fatalf("upload wrote state for a fence with %s", variant.Name)
		}
	}
	actionMessage, err := commerce.AuthorizedActionSignatureMessage(fixture.action)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range actionauthoritytest.Variants(t, fixture.custody, fixture.owner.controller, "second-key",
		fixture.action.AuthorityKeyID, fixture.action.AuthorizationProof, actionMessage) {
		bad := fixture.action
		bad.AuthorityKeyID, bad.AuthorizationProof = variant.KeyID, variant.Signature
		if err := fixture.accept(bad, fixture.fence); err == nil {
			t.Fatalf("upload admitted an action with %s", variant.Name)
		}
		if ingressState(t, fixture.directory) != before {
			t.Fatalf("upload wrote state for an action with %s", variant.Name)
		}
	}
	if err := fixture.accept(fixture.action, fixture.fence); err != nil {
		t.Fatalf("the real ML-DSA-44 upload was refused: %v", err)
	}
}

// ownerNamed returns the custody controller whose key ID is keyID.
func (fixture *uploadFixture) ownerNamed(t *testing.T, keyID string) ownerKey {
	t.Helper()
	for _, name := range []string{actionauthoritytest.Controller, "second-key", "foreign"} {
		if fixture.custody.KeyID(t, name) == keyID {
			return custodyOwner(t, fixture.custody, name)
		}
	}
	t.Fatalf("no custody controller has key ID %s", keyID)
	return ownerKey{}
}

type substitutingAuthority struct {
	testAuthority
	key []byte
}

func (authority substitutingAuthority) ResolveActionAuthorityKey(string, [mldsa44.KeyIDSize]byte, time.Time) ([]byte, error) {
	return append([]byte(nil), authority.key...), nil
}

// TestIngressRefusesAResolverKeyThatDoesNotDeriveTheNamedKeyID presents a
// fence and action that name the owner key's ID but carry valid signatures by
// a second key, to a resolver that answers every lookup with the second key.
// Every signature verifies under the key returned, so only the re-derivation
// of the named key ID from the resolved key can refuse them.
func TestIngressRefusesAResolverKeyThatDoesNotDeriveTheNamedKeyID(t *testing.T) {
	fixture := newUploadFixture(t)
	substitute := custodyOwner(t, fixture.custody, "second-key")
	fence := fixture.fence
	fenceMessage, err := commerce.WriterFenceSignatureMessage(fence.Body)
	if err != nil {
		t.Fatal(err)
	}
	fence.Proof = fixture.custody.SignFor(t, substitute.controller, "action-authority", fenceMessage)
	action := signUploadAction(t, fixture, fixture.owner, fence)
	action.AuthorizationProof = ""
	actionMessage, err := commerce.AuthorizedActionSignatureMessage(action)
	if err != nil {
		t.Fatal(err)
	}
	action.AuthorizationProof = fixture.custody.SignFor(t, substitute.controller, "action-authority", actionMessage)
	*fixture.authority.current = fence
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, sender := deterministicEd25519(0x21), deterministicEd25519(0x22)
	store, err := Open(fixture.directory, "agent:receiver", receiver, testHandoffResolver{
		"agent:receiver": receiver.Public().(ed25519.PublicKey), "agent:sender": sender.Public().(ed25519.PublicKey)},
		substitutingAuthority{testAuthority: fixture.authority, key: substitute.publicKey})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.now = func() time.Time { return fixture.now }
	before := ingressState(t, fixture.directory)
	if _, err := store.Accept(context.Background(), fixture.authorization.Body.ChallengeDigest, fixture.authorization,
		fixture.ciphertext, action, fence); err == nil {
		t.Fatal("upload admitted a fence whose resolved key does not derive its key ID")
	}
	if ingressState(t, fixture.directory) != before {
		t.Fatal("upload wrote state for a substituted resolver key")
	}
}

// TestIngressRefusesAKeyListedForAnotherAuthority checks that a key is
// authorized only for the authority it is listed under.
func TestIngressRefusesAKeyListedForAnotherAuthority(t *testing.T) {
	fixture := newUploadFixture(t)
	foreign := custodyOwner(t, fixture.custody, "foreign")
	fence := signUploadFence(t, foreign, fixture.now)
	action := signUploadAction(t, fixture, foreign, fence)
	// The foreign fence is made the current writer so that only the
	// authority scoping of its key can refuse it.
	*fixture.authority.current = fence
	before := ingressState(t, fixture.directory)
	if err := fixture.accept(action, fence); err == nil {
		t.Fatal("upload admitted a fence signed by a key listed for another authority")
	}
	if ingressState(t, fixture.directory) != before {
		t.Fatal("upload wrote state for a foreign authority key")
	}
}
