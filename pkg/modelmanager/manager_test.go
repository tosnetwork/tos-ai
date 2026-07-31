package modelmanager

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/update"
)

type modelFixture struct {
	manager    *Manager
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
	now        time.Time
	root       string
}

func fixture(t *testing.T, maxModels int, maxBytes uint64) modelFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "models")
	manager, err := New(Config{
		RootDir: root, Target: "linux/amd64/cuda", MaxModels: maxModels,
		MaxTotalBytes: maxBytes, CurrentSecurityRevision: 5,
		Signers: map[string]ed25519.PublicKey{"model-key": publicKey},
		Now:     func() time.Time { return time.Unix(1_800_000_000, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return modelFixture{
		manager: manager, publicKey: publicKey, privateKey: privateKey,
		now: time.Unix(1_800_000_000, 0), root: root,
	}
}

func (f modelFixture) manifest(t *testing.T, name string, data []byte, revision uint64) update.Manifest {
	t.Helper()
	digest := sha256.Sum256(data)
	value, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: name,
		Digest: "sha256:" + hex.EncodeToString(digest[:]), SizeBytes: uint64(len(data)),
		Target: "linux/amd64/cuda", SecurityRevision: revision,
		IssuedAt: f.now.Add(-time.Minute).UnixMilli(), ExpiresAt: f.now.Add(time.Hour).UnixMilli(),
		KeyID: "model-key",
	}, f.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestImportHashSizeSignatureAndRollback(t *testing.T) {
	f := fixture(t, 4, 64)
	data := []byte("model-a")
	manifest := f.manifest(t, "a.gguf", data, 5)
	model, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data))
	if err != nil || model.State != StateReady {
		t.Fatalf("import model=%#v err=%v", model, err)
	}
	if _, err := os.Stat(filepath.Join(f.root, stringsTrimDigest(manifest.Digest)+".model")); err != nil {
		t.Fatal(err)
	}

	wrongSize := f.manifest(t, "size.gguf", []byte("123456"), 5)
	if _, err := f.manager.Import(context.Background(), wrongSize, bytes.NewReader([]byte("1"))); err == nil {
		t.Fatal("wrong size accepted")
	}
	wrongHash := f.manifest(t, "hash.gguf", []byte("abcdef"), 5)
	if _, err := f.manager.Import(context.Background(), wrongHash, bytes.NewReader([]byte("ghijkl"))); err == nil {
		t.Fatal("wrong hash accepted")
	}
	tampered := f.manifest(t, "signed.gguf", []byte("signed"), 5)
	tampered.Target = "other"
	if _, err := f.manager.Import(context.Background(), tampered, bytes.NewReader([]byte("signed"))); err == nil {
		t.Fatal("tampered target accepted")
	}
	rollback := f.manifest(t, "old.gguf", []byte("old"), 4)
	if _, err := f.manager.Import(context.Background(), rollback, bytes.NewReader([]byte("old"))); err == nil {
		t.Fatal("security rollback accepted")
	}
	unapproved := f.manifest(t, "unapproved.gguf", []byte("unapproved"), 5)
	unapproved.KeyID = "unknown-key"
	if _, err := f.manager.Import(context.Background(), unapproved, bytes.NewReader([]byte("unapproved"))); err == nil {
		t.Fatal("unapproved signer accepted")
	}
	conflicting := manifest
	conflicting.Artifact = "different-name"
	conflicting, err = update.Sign(conflicting, f.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Import(context.Background(), conflicting, bytes.NewReader(data)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting digest metadata error = %v", err)
	}
	assertNoTemporaryFiles(t, f.root)
}

func TestLRUEvictionProtectsActivePinnedAndInUse(t *testing.T) {
	f := fixture(t, 2, 12)
	firstData, secondData, thirdData := []byte("1111"), []byte("2222"), []byte("3333")
	first := f.manifest(t, "first", firstData, 5)
	second := f.manifest(t, "second", secondData, 5)
	third := f.manifest(t, "third", thirdData, 5)
	if _, err := f.manager.Import(context.Background(), first, bytes.NewReader(firstData)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Import(context.Background(), second, bytes.NewReader(secondData)); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Activate(first.Digest); err != nil {
		t.Fatal(err)
	}
	_, release, err := f.manager.Acquire(second.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Import(context.Background(), third, bytes.NewReader(thirdData)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("protected cache error = %v", err)
	}
	release()
	if _, err := f.manager.Import(context.Background(), third, bytes.NewReader(thirdData)); err != nil {
		t.Fatal(err)
	}
	if f.manager.Status(first.Digest).State != StateActive ||
		f.manager.Status(second.Digest).State != StateAbsent ||
		f.manager.Status(third.Digest).State != StateReady {
		t.Fatalf("unexpected states: %#v", f.manager.List())
	}
	if err := f.manager.SetPinned(third.Digest, true); err != nil {
		t.Fatal(err)
	}
	fourthData := []byte("4444")
	fourth := f.manifest(t, "fourth", fourthData, 5)
	if _, err := f.manager.Import(context.Background(), fourth, bytes.NewReader(fourthData)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("active and pinned entries were evicted: %v", err)
	}
	if err := f.manager.Drain(first.Digest); err != nil ||
		f.manager.Status(first.Digest).State != StateDraining {
		t.Fatalf("drain state error=%v model=%#v", err, f.manager.Status(first.Digest))
	}
}

func TestCancellationCleansTemporaryFile(t *testing.T) {
	f := fixture(t, 2, 64)
	data := []byte("cancel-me")
	manifest := f.manifest(t, "cancel", data, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.manager.Import(ctx, manifest, bytes.NewReader(data)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	if f.manager.Status(manifest.Digest).State != StateFailed {
		t.Fatal("canceled model not marked failed")
	}
	assertNoTemporaryFiles(t, f.root)
}

func assertNoTemporaryFiles(t *testing.T, root string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, ".model-stage-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files leaked: %v", matches)
	}
}

func stringsTrimDigest(digest string) string {
	return digest[len("sha256:"):]
}
