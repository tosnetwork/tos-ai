package update

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestManifestSignatureArtifactAndAntiRollback(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	artifact := []byte("model")
	digest := sha256.Sum256(artifact)
	now := time.Unix(1_800_000_000, 0)
	manifest, err := Sign(Manifest{
		Version:          ManifestVersion,
		Artifact:         "model.gguf",
		Digest:           "sha256:" + hex.EncodeToString(digest[:]),
		SizeBytes:        uint64(len(artifact)),
		Target:           "linux/amd64/cuda",
		SecurityRevision: 5,
		IssuedAt:         now.UnixMilli(),
		ExpiresAt:        now.Add(time.Hour).UnixMilli(),
		KeyID:            "model-release-key-1",
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.Verify(publicKey, "linux/amd64/cuda", 5, now); err != nil {
		t.Fatal(err)
	}
	if err := manifest.VerifyArtifact(bytes.NewReader(artifact)); err != nil {
		t.Fatal(err)
	}
	if err := manifest.Verify(publicKey, "linux/amd64/cuda", 6, now); err == nil {
		t.Fatal("security rollback accepted")
	}
	if err := manifest.VerifyArtifact(bytes.NewReader([]byte("other"))); err == nil {
		t.Fatal("wrong artifact accepted")
	}
	if err := manifest.Verify(publicKey, "linux/amd64/cuda", 5, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired manifest accepted for a new import")
	}
	if err := manifest.VerifyInstalled(publicKey, "linux/amd64/cuda", 5); err != nil {
		t.Fatalf("installed artifact failed authenticity revalidation: %v", err)
	}
	if err := manifest.VerifyInstalled(publicKey, "linux/amd64/cuda", 6); err == nil {
		t.Fatal("installed artifact security rollback accepted")
	}
}
