package privateingress

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
	"sync"
	"testing"
	"time"

	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
	"github.com/tosnetwork/tos-service-protocol/pkg/codec"
)

type testAuthority struct {
	authorityID string
	public      ed25519.PublicKey
	fence       commerce.WriterFence
}

func TestConcurrentChallengeConflictHasOneDurableWinner(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	ownerPublic, ownerPrivate, _ := ed25519.GenerateKey(rand.Reader)
	receiverPublic, receiverPrivate, _ := ed25519.GenerateKey(rand.Reader)
	senderPublic, _, _ := ed25519.GenerateKey(rand.Reader)
	fence, err := commerce.SignWriterFence(commerce.WriterFenceBody{SchemaVersion: 1, OwnerID: "owner:test", AgentID: "agent:receiver",
		InstanceID: "instance:test", LeaseID: "lease:test", WriterGeneration: 1, IssuedAtUnix: uint64(now.Unix()),
		ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()), AuthorityID: "authority:test", Scope: []string{"content.upload"}}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "ingress")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := Open(directory, "agent:receiver", receiverPrivate,
		testHandoffResolver{"agent:receiver": receiverPublic, "agent:sender": senderPublic},
		testAuthority{authorityID: "authority:test", public: ownerPublic, fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.now = func() time.Time { return now }
	base := commerce.PrivateHandoffChallengeBody{SchemaVersion: 1, HandoffID: "handoff:race", AgreementBodyDigest: "sha256:" + hex.EncodeToString(make([]byte, 32)),
		ObligationID: "obligation:input", SenderAgentID: "agent:sender", ReceiverAgentID: "agent:receiver", Direction: "input",
		PurposeDigest: "sha256:" + hex.EncodeToString(make([]byte, 32)), IngressProfileURI: "tos.private-ingress.v1", IngressInstanceID: "ingress:test",
		MaximumPlaintextBytes: 1024, MaximumCiphertextBytes: 1040, MaximumFiles: 1, AcceptedMediaTypes: []string{"application/octet-stream"},
		RetentionPolicyDigest: "sha256:" + hex.EncodeToString(make([]byte, 32)), IssuedAtUnix: uint64(now.Unix()),
		ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()), DeleteNotAfterUnix: uint64(now.Add(24 * time.Hour).Unix())}
	var wait sync.WaitGroup
	wait.Add(2)
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		candidate := base
		candidate.MaximumPlaintextBytes += uint64(index)
		go func() { defer wait.Done(); _, issueErr := store.IssueChallenge(candidate); results <- issueErr }()
	}
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for issueErr := range results {
		if issueErr == nil {
			successes++
		} else {
			conflicts++
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	if _, err := readChallenge(store.challengePath(base.HandoffID)); err != nil {
		t.Fatalf("durable winner: %v", err)
	}
}

func (authority testAuthority) AuthorizeFenceKey(authorityID string, key ed25519.PublicKey, _ time.Time) error {
	if authorityID != authority.authorityID || !authority.public.Equal(key) {
		return errors.New("unknown")
	}
	return nil
}

func (authority testAuthority) ConfirmCurrentWriterFence(fence commerce.WriterFence, _ time.Time) error {
	if fence.Proof != authority.fence.Proof {
		return errors.New("stale")
	}
	return nil
}

type testHandoffResolver map[string]ed25519.PublicKey

func (resolver testHandoffResolver) AuthorizeHandoffKey(agentID string, key ed25519.PublicKey, _ time.Time) error {
	if !resolver[agentID].Equal(key) {
		return errors.New("unauthorized")
	}
	return nil
}

func TestIngressAcceptsOneExactWriterFencedUpload(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	ownerPublic, ownerPrivate, _ := ed25519.GenerateKey(rand.Reader)
	receiverPublic, receiverPrivate, _ := ed25519.GenerateKey(rand.Reader)
	senderPublic, senderPrivate, _ := ed25519.GenerateKey(rand.Reader)
	digest := func(value string) string {
		hash := sha256.Sum256([]byte(value))
		return "sha256:" + hex.EncodeToString(hash[:])
	}
	fence, err := commerce.SignWriterFence(commerce.WriterFenceBody{SchemaVersion: 1, OwnerID: "owner:test", AgentID: "agent:receiver",
		InstanceID: "instance:test", LeaseID: "lease:test", WriterGeneration: 1, IssuedAtUnix: uint64(now.Unix()),
		ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()), AuthorityID: "authority:test", Scope: []string{"content.delete", "content.upload"}}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	authority := testAuthority{authorityID: "authority:test", public: ownerPublic, fence: fence}
	directory := filepath.Join(t.TempDir(), "ingress")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := Open(directory, "agent:receiver", receiverPrivate,
		testHandoffResolver{"agent:receiver": receiverPublic, "agent:sender": senderPublic}, authority)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.now = func() time.Time { return now }
	challenge, err := store.IssueChallenge(commerce.PrivateHandoffChallengeBody{SchemaVersion: 1, HandoffID: "handoff:test",
		AgreementBodyDigest: digest("agreement"), ObligationID: "obligation:input", SenderAgentID: "agent:sender", ReceiverAgentID: "agent:receiver",
		Direction: "input", PurposeDigest: digest("purpose"), IngressProfileURI: "tos.private-ingress.v1", IngressInstanceID: "ingress:test",
		MaximumPlaintextBytes: 1024, MaximumCiphertextBytes: 1040, MaximumFiles: 1,
		AcceptedMediaTypes: []string{"application/octet-stream"}, RetentionPolicyDigest: digest("retention"),
		IssuedAtUnix: uint64(now.Unix()), ExpiresAtUnix: uint64(now.Add(time.Hour).Unix()),
		DeleteNotAfterUnix: uint64(now.Add(24 * time.Hour).Unix())})
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte("immutable private input")
	plainHash := sha256.Sum256(plaintext)
	manifest := commerce.PrivateContentManifest{ContentDigest: "sha256:" + hex.EncodeToString(plainHash[:]), MediaType: "application/octet-stream",
		FileCount: 1, CanonicalPaths: []string{"input.bin"}, PlaintextBytes: uint64(len(plaintext)), MaximumExpandedBytes: uint64(len(plaintext)),
		CompressionProfileURI: "tos.compression.none.v1"}
	manifestDigest, _ := codec.Digest("tos.private-content-manifest.v1", manifest)
	senderFields := map[string]commerce.SemanticValue{"owner_id": commerce.ID("owner:sender"), "agent_id": commerce.ID("agent:sender"),
		"agreement_body_digest": commerce.Digest32(challenge.Body.AgreementBodyDigest), "obligation_id": commerce.ID(challenge.Body.ObligationID),
		"recipient_id": commerce.ID(challenge.Body.ReceiverAgentID), "content_digest": commerce.Digest32(manifest.ContentDigest),
		"purpose_digest": commerce.Digest32(challenge.Body.PurposeDigest)}
	senderActionID, _, err := commerce.DeriveStableActionID("disclosure.release", senderFields)
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]commerce.SemanticValue{"owner_id": commerce.ID("owner:test"), "agent_id": commerce.ID("agent:receiver"),
		"agreement_body_digest": commerce.Digest32(challenge.Body.AgreementBodyDigest), "obligation_id": commerce.ID(challenge.Body.ObligationID),
		"handoff_id": commerce.ID(challenge.Body.HandoffID), "sender_id": commerce.ID(challenge.Body.SenderAgentID),
		"receiver_id": commerce.ID(challenge.Body.ReceiverAgentID), "content_manifest_digest": commerce.Digest32(manifestDigest)}
	uploadActionID, _, err := commerce.DeriveStableActionID("content.upload", fields)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, authorization, err := commerce.SealPrivateContent(challenge, manifest, plaintext, senderActionID, senderPrivate)
	if err != nil {
		t.Fatal(err)
	}
	canonical, materialFields, err := UploadAuthorizationMaterial("owner:test", "agent:receiver", challenge, authorization)
	if err != nil {
		t.Fatal(err)
	}
	action, err := commerce.BuildAuthorizedAction("owner:test", "agent:receiver", "content.upload", materialFields, canonical, fence, 1,
		digest("mandate"), "", "challenge_issued", uint64(now.Add(time.Hour).Unix()))
	if err != nil {
		t.Fatal(err)
	}
	action, err = commerce.SignAuthorizedAction(action, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if action.StableActionID != uploadActionID {
		t.Fatal("receiver upload action identity changed")
	}
	record, err := store.Accept(context.Background(), authorization.Body.ChallengeDigest, authorization, ciphertext, action, fence)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := store.Accept(context.Background(), authorization.Body.ChallengeDigest, authorization, ciphertext, action, fence); err != nil || retry != record {
		t.Fatalf("exact retry=%+v err=%v", retry, err)
	}
	file, err := store.OpenAccepted(record)
	if err != nil {
		t.Fatal(err)
	}
	opened, _ := io.ReadAll(file)
	_ = file.Close()
	if string(opened) != string(plaintext) {
		t.Fatalf("opened %q", opened)
	}
	conflict := authorization
	conflict.Body.CiphertextDigest = digest("changed")
	if _, err := store.Accept(context.Background(), authorization.Body.ChallengeDigest, conflict, ciphertext, action, fence); err == nil {
		t.Fatal("conflicting retry was accepted")
	}
	deleteCanonical, deleteFields, err := DeleteAuthorizationMaterial("owner:test", "agent:receiver", challenge, record)
	if err != nil {
		t.Fatal(err)
	}
	deleteAction, err := commerce.BuildAuthorizedAction("owner:test", "agent:receiver", "content.delete", deleteFields, deleteCanonical,
		fence, 1, digest("mandate"), "", "accepted", uint64(now.Add(time.Hour).Unix()))
	if err != nil {
		t.Fatal(err)
	}
	deleteAction, err = commerce.SignAuthorizedAction(deleteAction, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	resolution, err := store.Delete(context.Background(), authorization.Body.ChallengeDigest, deleteAction, fence)
	if err != nil || resolution.State != commerce.ActionTerminal {
		t.Fatalf("deletion=%+v err=%v", resolution, err)
	}
	if retry, err := store.Delete(context.Background(), authorization.Body.ChallengeDigest, deleteAction, fence); err != nil || retry.StableActionID != resolution.StableActionID || retry.State != resolution.State {
		t.Fatalf("delete retry=%+v err=%v", retry, err)
	}
	if _, err := store.OpenAccepted(record); err == nil {
		t.Fatal("deleted private object remained readable")
	}
}
