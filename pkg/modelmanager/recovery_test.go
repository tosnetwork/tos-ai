package modelmanager

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/update"
)

func TestRecoveryRevalidatesAndResetsVolatileState(t *testing.T) {
	f := fixture(t, 4, 64)
	data := []byte("persistent-model")
	manifest := f.manifest(t, "persistent.gguf", data, 5)
	if _, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Activate(manifest.Digest); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.SetPinned(manifest.Digest, true); err != nil {
		t.Fatal(err)
	}
	_, release, err := f.manager.Acquire(manifest.Digest)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	recovered := reopen(t, f, 4, 64, 5, f.now.Add(2*time.Hour), f.publicKey)
	model := recovered.Status(manifest.Digest)
	if model.State != StateReady || model.Pinned || model.InUse != 0 ||
		model.Artifact != manifest.Artifact || model.SecurityRevision != 5 {
		t.Fatalf("recovered model = %#v", model)
	}
	metadataInfo, err := os.Stat(recovered.metadataPath(manifest.Digest))
	if err != nil || metadataInfo.Mode().Perm() != 0o600 {
		t.Fatalf("metadata info=%v err=%v", metadataInfo, err)
	}
	if recovered.totalBytes != uint64(len(data)) || recovered.reservedBytes != 0 {
		t.Fatalf("recovered bytes total=%d reserved=%d", recovered.totalBytes, recovered.reservedBytes)
	}
}

func TestRecoveryRejectsArtifactMetadataSignerAndRevisionTampering(t *testing.T) {
	t.Run("artifact", func(t *testing.T) {
		f, manifest, data := importedFixture(t)
		tampered := append([]byte(nil), data...)
		tampered[0] ^= 0xff
		if err := os.WriteFile(f.manager.artifactPath(manifest.Digest), tampered, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(recoveryConfig(f, 4, 64, 5, f.now, f.publicKey)); err == nil {
			t.Fatal("tampered recovered artifact accepted")
		}
	})

	t.Run("permissions", func(t *testing.T) {
		f, manifest, _ := importedFixture(t)
		if err := os.Chmod(f.manager.artifactPath(manifest.Digest), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := New(recoveryConfig(f, 4, 64, 5, f.now, f.publicKey)); err == nil {
			t.Fatal("insecure recovered artifact permissions accepted")
		}
	})

	t.Run("metadata", func(t *testing.T) {
		f, manifest, _ := importedFixture(t)
		path := f.manager.metadataPath(manifest.Digest)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, ' ')
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(recoveryConfig(f, 4, 64, 5, f.now, f.publicKey)); err == nil {
			t.Fatal("non-canonical recovered metadata accepted")
		}
	})

	t.Run("acceptance time", func(t *testing.T) {
		f, manifest, _ := importedFixture(t)
		path := f.manager.metadataPath(manifest.Digest)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var metadata persistedMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			t.Fatal(err)
		}
		metadata.AcceptedAtMillis = manifest.ExpiresAt
		data, err = json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(recoveryConfig(f, 4, 64, 5, f.now.Add(2*time.Hour), f.publicKey)); err == nil {
			t.Fatal("invalid persisted acceptance time accepted")
		}
	})

	t.Run("signer", func(t *testing.T) {
		f, _, _ := importedFixture(t)
		otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := New(recoveryConfig(f, 4, 64, 5, f.now, otherPublic)); err == nil {
			t.Fatal("unapproved recovered signer accepted")
		}
	})

	t.Run("revision", func(t *testing.T) {
		f, _, _ := importedFixture(t)
		if _, err := New(recoveryConfig(f, 4, 64, 6, f.now, f.publicKey)); err == nil {
			t.Fatal("recovered security rollback accepted")
		}
	})
}

func TestImportRechecksExpiryAndRollsBackPartialActivation(t *testing.T) {
	t.Run("expiry during import", func(t *testing.T) {
		f := fixture(t, 2, 64)
		data := []byte("expires")
		manifest := f.manifest(t, "expires.gguf", data, 5)
		calls := 0
		f.manager.config.Now = func() time.Time {
			calls++
			if calls == 1 {
				return f.now
			}
			return f.now.Add(2 * time.Hour)
		}
		if _, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data)); err == nil {
			t.Fatal("manifest that expired during import was activated")
		}
		if _, err := os.Stat(f.manager.artifactPath(manifest.Digest)); !os.IsNotExist(err) {
			t.Fatalf("expired import artifact remains: %v", err)
		}
		assertNoTemporaryFiles(t, f.root)
	})

	t.Run("metadata activation failure", func(t *testing.T) {
		f := fixture(t, 2, 64)
		data := []byte("activation")
		manifest := f.manifest(t, "activation.gguf", data, 5)
		if err := os.Mkdir(f.manager.metadataPath(manifest.Digest), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data)); err == nil {
			t.Fatal("metadata activation failure was accepted")
		}
		if _, err := os.Stat(f.manager.artifactPath(manifest.Digest)); !os.IsNotExist(err) {
			t.Fatalf("partially activated artifact remains: %v", err)
		}
		if f.manager.Status(manifest.Digest).State != StateFailed {
			t.Fatalf("partial activation state = %#v", f.manager.Status(manifest.Digest))
		}
		assertNoTemporaryFiles(t, f.root)
	})
}

func TestRecoveryCleansCrashResidueAndEvictsOldest(t *testing.T) {
	f := fixture(t, 3, 12)
	type imported struct {
		digest string
		data   []byte
	}
	var models []imported
	for index, value := range []string{"1111", "2222", "3333"} {
		data := []byte(value)
		manifest := f.manifest(t, fmt.Sprintf("model-%d", index), data, 5)
		if _, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		timestamp := f.now.Add(time.Duration(index) * time.Minute)
		if err := os.Chtimes(f.manager.metadataPath(manifest.Digest), timestamp, timestamp); err != nil {
			t.Fatal(err)
		}
		models = append(models, imported{digest: manifest.Digest, data: data})
	}
	stageArtifact := filepath.Join(f.root, ".model-stage-crash")
	stageMetadata := filepath.Join(f.root, ".manifest-stage-crash")
	orphanArtifact := filepath.Join(f.root, fmt.Sprintf("%064x.model", 999))
	orphanMetadata := filepath.Join(f.root, fmt.Sprintf("%064x.manifest", 1000))
	for _, path := range []string{stageArtifact, stageMetadata, orphanArtifact, orphanMetadata} {
		if err := os.WriteFile(path, []byte("residue"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	recovered := reopen(t, f, 2, 8, 5, f.now.Add(2*time.Hour), f.publicKey)
	if recovered.Status(models[0].digest).State != StateAbsent ||
		recovered.Status(models[1].digest).State != StateReady ||
		recovered.Status(models[2].digest).State != StateReady {
		t.Fatalf("recovered LRU states = %#v", recovered.List())
	}
	for _, path := range []string{
		stageArtifact, stageMetadata, orphanArtifact, orphanMetadata,
		f.manager.artifactPath(models[0].digest), f.manager.metadataPath(models[0].digest),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("recovery residue remains at %s: %v", filepath.Base(path), err)
		}
	}
}

func TestRecoveryDoesNotEvictBeforeRetainedArtifactsVerify(t *testing.T) {
	f := fixture(t, 2, 64)
	var manifests []update.Manifest
	for index, value := range []string{"older", "newest"} {
		data := []byte(value)
		manifest := f.manifest(t, value, data, 5)
		if _, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		timestamp := f.now.Add(time.Duration(index) * time.Minute)
		if err := os.Chtimes(f.manager.metadataPath(manifest.Digest), timestamp, timestamp); err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, manifest)
	}
	newestPath := f.manager.artifactPath(manifests[1].Digest)
	if err := os.WriteFile(newestPath, []byte("damage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(recoveryConfig(f, 1, 64, 5, f.now, f.publicKey)); err == nil {
		t.Fatal("corrupt retained artifact accepted")
	}
	for _, manifest := range manifests {
		for _, path := range []string{
			f.manager.artifactPath(manifest.Digest),
			f.manager.metadataPath(manifest.Digest),
		} {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("recovery failure evicted %s: %v", filepath.Base(path), err)
			}
		}
	}
}

func TestRecoveryDirectoryScanAndConcurrentReadersAreBounded(t *testing.T) {
	t.Run("directory limit", func(t *testing.T) {
		f := fixture(t, 1, 64)
		for index := 0; index <= MaxDirectoryEntriesHard; index++ {
			path := filepath.Join(f.root, fmt.Sprintf("%064x.model", index))
			if err := os.WriteFile(path, []byte{1}, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := New(recoveryConfig(f, 1, 64, 5, f.now, f.publicKey)); err == nil {
			t.Fatal("unbounded model cache directory accepted")
		}
	})

	t.Run("unknown entry", func(t *testing.T) {
		f := fixture(t, 1, 64)
		if err := os.WriteFile(filepath.Join(f.root, "unexpected"), []byte{1}, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(recoveryConfig(f, 1, 64, 5, f.now, f.publicKey)); err == nil {
			t.Fatal("unknown model cache entry accepted")
		}
	})

	t.Run("concurrent recovery", func(t *testing.T) {
		f, _, _ := importedFixture(t)
		const readers = 8
		errorsChannel := make(chan error, readers)
		var wait sync.WaitGroup
		for range readers {
			wait.Add(1)
			go func() {
				defer wait.Done()
				_, err := New(recoveryConfig(f, 4, 64, 5, f.now, f.publicKey))
				errorsChannel <- err
			}()
		}
		wait.Wait()
		close(errorsChannel)
		for err := range errorsChannel {
			if err != nil {
				t.Fatalf("concurrent recovery failed: %v", err)
			}
		}
	})
}

func importedFixture(t *testing.T) (modelFixture, update.Manifest, []byte) {
	t.Helper()
	f := fixture(t, 4, 64)
	data := []byte("recover-me")
	manifest := f.manifest(t, "recover.gguf", data, 5)
	if _, err := f.manager.Import(context.Background(), manifest, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	return f, manifest, data
}

func recoveryConfig(
	f modelFixture,
	maxModels int,
	maxBytes uint64,
	revision uint64,
	now time.Time,
	publicKey ed25519.PublicKey,
) Config {
	return Config{
		RootDir: f.root, Target: "linux/amd64/cuda", MaxModels: maxModels,
		MaxTotalBytes: maxBytes, CurrentSecurityRevision: revision,
		Signers: map[string]ed25519.PublicKey{"model-key": publicKey},
		Now:     func() time.Time { return now },
	}
}

func reopen(
	t *testing.T,
	f modelFixture,
	maxModels int,
	maxBytes uint64,
	revision uint64,
	now time.Time,
	publicKey ed25519.PublicKey,
) *Manager {
	t.Helper()
	manager, err := New(recoveryConfig(f, maxModels, maxBytes, revision, now, publicKey))
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
