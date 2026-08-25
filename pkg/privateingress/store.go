// Package privateingress implements the receiver-selected, Agreement-bound
// private byte ingress used before a commerce execution starts.
package privateingress

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
	"github.com/tosnetwork/tos-service-protocol/pkg/codec"
)

type WriterAuthority interface {
	commerce.FenceAuthorityResolver
	ConfirmCurrentWriterFence(commerce.WriterFence, time.Time) error
}

type Store struct {
	mu            sync.Mutex
	directory     string
	lock          *dirlock.Lock
	receiverAgent string
	signingKey    ed25519.PrivateKey
	resolver      commerce.HandoffAuthorityResolver
	writer        WriterAuthority
	now           func() time.Time
}

type challengeRecord struct {
	Schema                string                                 `json:"schema"`
	Challenge             commerce.SignedPrivateHandoffChallenge `json:"challenge"`
	EncryptionPrivateKey  string                                 `json:"encryption_private_key"`
	Accepted              *commerce.AcceptedPrivateContentRecord `json:"accepted,omitempty"`
	AuthorizationDigest   string                                 `json:"authorization_digest,omitempty"`
	DeletionActionID      string                                 `json:"deletion_action_id,omitempty"`
	DeletionRequestDigest string                                 `json:"deletion_request_digest,omitempty"`
	DeletionState         string                                 `json:"deletion_state,omitempty"`
	DeletedAtUnix         uint64                                 `json:"deleted_at_unix,omitempty"`
}

type UploadRequest struct {
	ChallengeDigest  string                                     `json:"challenge_digest"`
	Authorization    commerce.SignedPrivateHandoffAuthorization `json:"authorization"`
	CiphertextDigest string                                     `json:"ciphertext_digest"`
	CiphertextBytes  uint64                                     `json:"ciphertext_bytes"`
}

type DeleteRequest struct {
	HandoffID             string `json:"handoff_id"`
	AgreementBodyDigest   string `json:"agreement_body_digest"`
	ObligationID          string `json:"obligation_id"`
	ContentManifestDigest string `json:"content_manifest_digest"`
	RetentionPolicyDigest string `json:"retention_policy_digest"`
}

func Open(directory, receiverAgentID string, signingKey ed25519.PrivateKey,
	resolver commerce.HandoffAuthorityResolver, writer WriterAuthority) (*Store, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || receiverAgentID == "" ||
		len(signingKey) != ed25519.PrivateKeySize || resolver == nil || writer == nil {
		return nil, errors.New("private ingress configuration is invalid")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("private ingress directory must be owner-private")
	}
	lock, err := dirlock.Acquire(directory, ".private-ingress.lock")
	if err != nil {
		return nil, errors.New("private ingress is already owned")
	}
	return &Store{directory: directory, lock: lock, receiverAgent: receiverAgentID,
		signingKey: append(ed25519.PrivateKey(nil), signingKey...), resolver: resolver, writer: writer, now: time.Now}, nil
}

func (store *Store) Close() error {
	if store == nil || store.lock == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for index := range store.signingKey {
		store.signingKey[index] = 0
	}
	err := store.lock.Close()
	store.lock = nil
	return err
}

func (store *Store) IssueChallenge(body commerce.PrivateHandoffChallengeBody) (commerce.SignedPrivateHandoffChallenge, error) {
	if store == nil || store.lock == nil || body.ReceiverAgentID != store.receiverAgent || body.IngressInstanceID == "" {
		return commerce.SignedPrivateHandoffChallenge{}, errors.New("private ingress challenge is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	path := store.challengePath(body.HandoffID)
	if existing, err := readChallenge(path); err == nil {
		body.ReceiverEncryptionPublicKey = existing.Challenge.Body.ReceiverEncryptionPublicKey
		candidate, signErr := commerce.SignPrivateHandoffChallenge(body, store.signingKey)
		if signErr == nil {
			candidateDigest, _ := commerce.PrivateHandoffChallengeDigest(candidate.Body)
			existingDigest, _ := commerce.PrivateHandoffChallengeDigest(existing.Challenge.Body)
			if candidateDigest == existingDigest {
				return existing.Challenge, nil
			}
		}
		return commerce.SignedPrivateHandoffChallenge{}, errors.New("handoff ID conflicts with an existing challenge")
	} else if !errors.Is(err, os.ErrNotExist) {
		return commerce.SignedPrivateHandoffChallenge{}, err
	}
	encryptionKey, err := commerce.GenerateHandoffEncryptionKey()
	if err != nil {
		return commerce.SignedPrivateHandoffChallenge{}, err
	}
	body.ReceiverEncryptionPublicKey = base64.RawURLEncoding.EncodeToString(encryptionKey.PublicKey().Bytes())
	challenge, err := commerce.SignPrivateHandoffChallenge(body, store.signingKey)
	if err != nil {
		return commerce.SignedPrivateHandoffChallenge{}, err
	}
	digest, err := commerce.PrivateHandoffChallengeDigest(body)
	if err != nil {
		return commerce.SignedPrivateHandoffChallenge{}, err
	}
	record := challengeRecord{Schema: "tos.ai.private-ingress-challenge.v1", Challenge: challenge,
		EncryptionPrivateKey: base64.RawURLEncoding.EncodeToString(encryptionKey.Bytes())}
	raw, err := json.Marshal(record)
	if err != nil {
		return commerce.SignedPrivateHandoffChallenge{}, err
	}
	if err := createPrivate(path, raw); err != nil {
		if existing, readErr := readChallenge(path); readErr == nil {
			existingDigest, _ := commerce.PrivateHandoffChallengeDigest(existing.Challenge.Body)
			if existingDigest == digest {
				return existing.Challenge, nil
			}
		}
		return commerce.SignedPrivateHandoffChallenge{}, err
	}
	return challenge, nil
}

func UploadAuthorizationMaterial(ownerID, agentID string, challenge commerce.SignedPrivateHandoffChallenge,
	authorization commerce.SignedPrivateHandoffAuthorization) ([]byte, map[string]commerce.SemanticValue, error) {
	challengeDigest, err := commerce.PrivateHandoffChallengeDigest(challenge.Body)
	if err != nil {
		return nil, nil, err
	}
	manifestDigest, err := codec.Digest("tos.private-content-manifest.v1", authorization.Body.Manifest)
	if err != nil {
		return nil, nil, err
	}
	request := UploadRequest{ChallengeDigest: challengeDigest, Authorization: authorization,
		CiphertextDigest: authorization.Body.CiphertextDigest, CiphertextBytes: authorization.Body.CiphertextBytes}
	canonical, err := codec.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	fields := map[string]commerce.SemanticValue{
		"owner_id":                commerce.ID(ownerID),
		"agent_id":                commerce.ID(agentID),
		"agreement_body_digest":   commerce.Digest32(challenge.Body.AgreementBodyDigest),
		"obligation_id":           commerce.ID(challenge.Body.ObligationID),
		"handoff_id":              commerce.ID(challenge.Body.HandoffID),
		"sender_id":               commerce.ID(challenge.Body.SenderAgentID),
		"receiver_id":             commerce.ID(challenge.Body.ReceiverAgentID),
		"content_manifest_digest": commerce.Digest32(manifestDigest),
	}
	return canonical, fields, nil
}

func (store *Store) Accept(_ context.Context, challengeDigest string,
	authorization commerce.SignedPrivateHandoffAuthorization, ciphertext []byte,
	action commerce.AuthorizedAction, fence commerce.WriterFence) (commerce.AcceptedPrivateContentRecord, error) {
	if store == nil || store.lock == nil || action.ActionKind != "content.upload" || !canonicalDigest(challengeDigest) {
		return commerce.AcceptedPrivateContentRecord{}, errors.New("private ingress upload is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now().UTC()
	if err := commerce.VerifyWriterFence(fence, store.writer, now, "content.upload"); err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	if err := store.writer.ConfirmCurrentWriterFence(fence, now); err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	challengeRecordPath, record, err := store.findChallenge(challengeDigest)
	if err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	canonical, fields, err := UploadAuthorizationMaterial(action.OwnerID, action.AgentID, record.Challenge, authorization)
	if err != nil || authorization.Body.ChallengeDigest != challengeDigest ||
		commerce.VerifyAuthorizedAction(action, fields, canonical, fence, store.writer, now) != nil {
		return commerce.AcceptedPrivateContentRecord{}, errors.New("private upload is not the exact authorized action")
	}
	authorizationDigest, err := commerce.PrivateHandoffAuthorizationDigest(authorization.Body)
	if err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	if record.Accepted != nil {
		if record.AuthorizationDigest == authorizationDigest && record.Accepted.UploadActionID == action.StableActionID {
			return *record.Accepted, nil
		}
		return commerce.AcceptedPrivateContentRecord{}, errors.New("private ingress challenge already accepted conflicting bytes")
	}
	privateRaw, err := base64.RawURLEncoding.DecodeString(record.EncryptionPrivateKey)
	if err != nil || len(privateRaw) != 32 {
		return commerce.AcceptedPrivateContentRecord{}, errors.New("private ingress challenge key is invalid")
	}
	privateKey, err := ecdh.X25519().NewPrivateKey(privateRaw)
	if err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	plaintext, err := commerce.OpenPrivateContent(record.Challenge, authorization, ciphertext, privateKey, store.resolver, now)
	if err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	objectDigest := authorization.Body.Manifest.ContentDigest
	objectPath := store.objectPath(objectDigest)
	if err := createPrivate(objectPath, plaintext); err != nil {
		existing, readErr := os.ReadFile(objectPath)
		if readErr != nil || !bytes.Equal(existing, plaintext) {
			return commerce.AcceptedPrivateContentRecord{}, errors.New("immutable private object conflicts with accepted bytes")
		}
	}
	manifestDigest, err := codec.Digest("tos.private-content-manifest.v1", authorization.Body.Manifest)
	if err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	accepted := commerce.AcceptedPrivateContentRecord{SchemaVersion: 1, HandoffID: record.Challenge.Body.HandoffID,
		ChallengeDigest: challengeDigest, AuthorizationDigest: authorizationDigest, UploadActionID: action.StableActionID,
		SenderDisclosureActionID: authorization.Body.SenderDisclosureActionID,
		ContentDigest:            authorization.Body.Manifest.ContentDigest, ContentManifestDigest: manifestDigest, PlaintextBytes: uint64(len(plaintext)),
		ImmutableObjectDigest: objectDigest, RetentionPolicyDigest: record.Challenge.Body.RetentionPolicyDigest,
		AcceptedAtUnix: uint64(now.Unix()), DeleteNotAfterUnix: record.Challenge.Body.DeleteNotAfterUnix}
	record.Accepted = &accepted
	record.AuthorizationDigest = authorizationDigest
	record.EncryptionPrivateKey = ""
	if err := replacePrivate(challengeRecordPath, record); err != nil {
		return commerce.AcceptedPrivateContentRecord{}, err
	}
	return accepted, nil
}

func (store *Store) OpenAccepted(record commerce.AcceptedPrivateContentRecord) (*os.File, error) {
	if store != nil {
		store.mu.Lock()
		defer store.mu.Unlock()
	}
	return store.openAccepted(record)
}

func (store *Store) openAccepted(record commerce.AcceptedPrivateContentRecord) (*os.File, error) {
	if store == nil || !canonicalDigest(record.ContentDigest) || record.ImmutableObjectDigest != record.ContentDigest {
		return nil, errors.New("accepted private content record is invalid")
	}
	path := store.objectPath(record.ContentDigest)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || uint64(info.Size()) != record.PlaintextBytes {
		return nil, errors.New("accepted private content object is unavailable")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, errors.New("accepted private content object changed during open")
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, io.LimitReader(file, int64(record.PlaintextBytes)+1)); err != nil {
		_ = file.Close()
		return nil, err
	}
	if "sha256:"+hex.EncodeToString(hasher.Sum(nil)) != record.ContentDigest {
		_ = file.Close()
		return nil, errors.New("accepted private content digest changed")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (store *Store) Acknowledge(record commerce.AcceptedPrivateContentRecord) (commerce.SignedPrivateHandoffAcknowledgement, error) {
	if store == nil || store.lock == nil {
		return commerce.SignedPrivateHandoffAcknowledgement{}, errors.New("private ingress is unavailable")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if file, err := store.openAccepted(record); err != nil {
		return commerce.SignedPrivateHandoffAcknowledgement{}, err
	} else {
		_ = file.Close()
	}
	return commerce.SignPrivateHandoffAcknowledgement(record, store.receiverAgent, store.signingKey)
}

func DeleteAuthorizationMaterial(ownerID, agentID string, challenge commerce.SignedPrivateHandoffChallenge,
	accepted commerce.AcceptedPrivateContentRecord) ([]byte, map[string]commerce.SemanticValue, error) {
	request := DeleteRequest{HandoffID: challenge.Body.HandoffID, AgreementBodyDigest: challenge.Body.AgreementBodyDigest,
		ObligationID: challenge.Body.ObligationID, ContentManifestDigest: accepted.ContentManifestDigest,
		RetentionPolicyDigest: accepted.RetentionPolicyDigest}
	canonical, err := codec.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	fields := map[string]commerce.SemanticValue{"owner_id": commerce.ID(ownerID), "agent_id": commerce.ID(agentID),
		"agreement_body_digest": commerce.Digest32(challenge.Body.AgreementBodyDigest), "obligation_id": commerce.ID(challenge.Body.ObligationID),
		"handoff_id": commerce.ID(challenge.Body.HandoffID), "content_manifest_digest": commerce.Digest32(accepted.ContentManifestDigest),
		"retention_policy_digest": commerce.Digest32(accepted.RetentionPolicyDigest)}
	return canonical, fields, nil
}

// Delete first journals an exact deletion as in-progress. A crash can only be
// recovered by replaying this same action and request digest.
func (store *Store) Delete(_ context.Context, challengeDigest string, action commerce.AuthorizedAction,
	fence commerce.WriterFence) (commerce.ActionResolution, error) {
	if store == nil || store.lock == nil || action.ActionKind != "content.delete" || !canonicalDigest(challengeDigest) {
		return commerce.ActionResolution{}, errors.New("private content deletion is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now().UTC()
	if err := commerce.VerifyWriterFence(fence, store.writer, now, "content.delete"); err != nil {
		return commerce.ActionResolution{}, err
	}
	if err := store.writer.ConfirmCurrentWriterFence(fence, now); err != nil {
		return commerce.ActionResolution{}, err
	}
	path, record, err := store.findChallenge(challengeDigest)
	if err != nil || record.Accepted == nil {
		return commerce.ActionResolution{}, errors.New("accepted private content was not found")
	}
	canonical, fields, err := DeleteAuthorizationMaterial(action.OwnerID, action.AgentID, record.Challenge, *record.Accepted)
	if err != nil || commerce.VerifyAuthorizedAction(action, fields, canonical, fence, store.writer, now) != nil {
		return commerce.ActionResolution{}, errors.New("private deletion is not the exact authorized action")
	}
	requestDigest, _ := commerce.ExactRequestDigest(canonical)
	if record.DeletionActionID != "" && (record.DeletionActionID != action.StableActionID || record.DeletionRequestDigest != requestDigest) {
		return commerce.ActionResolution{}, errors.New("private content has a conflicting deletion action")
	}
	if record.DeletionState == "deleted" {
		return commerce.ActionResolution{StableActionID: action.StableActionID, ExactRequestDigest: requestDigest,
			State: commerce.ActionTerminal, SinkReference: "private-ingress:" + challengeDigest,
			EvidenceRefs: []string{record.Accepted.ContentDigest}, StateRevision: 2}, nil
	}
	if record.DeletionState == "" {
		record.DeletionActionID = action.StableActionID
		record.DeletionRequestDigest = requestDigest
		record.DeletionState = "deleting"
		if err := replacePrivate(path, record); err != nil {
			return commerce.ActionResolution{}, err
		}
	}
	objectPath := store.objectPath(record.Accepted.ContentDigest)
	if err := os.Remove(objectPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return commerce.ActionResolution{}, err
	}
	directory, err := os.Open(store.directory)
	if err != nil {
		return commerce.ActionResolution{}, err
	}
	err = directory.Sync()
	_ = directory.Close()
	if err != nil {
		return commerce.ActionResolution{}, err
	}
	record.DeletionState = "deleted"
	record.DeletedAtUnix = uint64(now.Unix())
	if err := replacePrivate(path, record); err != nil {
		return commerce.ActionResolution{}, err
	}
	return commerce.ActionResolution{StableActionID: action.StableActionID, ExactRequestDigest: requestDigest,
		State: commerce.ActionTerminal, SinkReference: "private-ingress:" + challengeDigest,
		EvidenceRefs: []string{record.Accepted.ContentDigest}, StateRevision: 2}, nil
}

func (store *Store) challengePath(handoffID string) string {
	digest := sha256.Sum256([]byte(handoffID))
	return filepath.Join(store.directory, "challenge-"+hex.EncodeToString(digest[:])+".json")
}

func (store *Store) findChallenge(challengeDigest string) (string, challengeRecord, error) {
	entries, err := os.ReadDir(store.directory)
	if err != nil {
		return "", challengeRecord{}, err
	}
	if len(entries) > 10_000 {
		return "", challengeRecord{}, errors.New("private ingress challenge index exceeds bound")
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() && len(entry.Name()) == len("challenge-")+64+len(".json") &&
			entry.Name()[:len("challenge-")] == "challenge-" && filepath.Ext(entry.Name()) == ".json" {
			path := filepath.Join(store.directory, entry.Name())
			record, readErr := readChallenge(path)
			if readErr != nil {
				continue
			}
			digest, digestErr := commerce.PrivateHandoffChallengeDigest(record.Challenge.Body)
			if digestErr == nil && digest == challengeDigest {
				return path, record, nil
			}
		}
	}
	return "", challengeRecord{}, os.ErrNotExist
}

func (store *Store) objectPath(digest string) string {
	return filepath.Join(store.directory, "object-"+digest[len("sha256:"):]+".bin")
}

func readChallenge(path string) (challengeRecord, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return challengeRecord{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > 1<<20 {
		return challengeRecord{}, errors.New("private ingress challenge record is invalid")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return challengeRecord{}, err
	}
	var record challengeRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil || decoder.Decode(&struct{}{}) != io.EOF || record.Schema != "tos.ai.private-ingress-challenge.v1" {
		return challengeRecord{}, errors.New("private ingress challenge record is malformed")
	}
	return record, nil
}

func createPrivate(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syncPrivateDirectory(filepath.Dir(path))
}

func replacePrivate(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	random := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return err
	}
	temporary := path + "." + hex.EncodeToString(random) + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if closeErr != nil {
		_ = os.Remove(temporary)
		return closeErr
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	return syncPrivateDirectory(filepath.Dir(path))
}

func syncPrivateDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	err = handle.Sync()
	closeErr := handle.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func canonicalDigest(value string) bool {
	if len(value) != len("sha256:")+64 || value[:len("sha256:")] != "sha256:" {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
