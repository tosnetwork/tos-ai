package softwareupdate

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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/update"
)

type updateFixture struct {
	public  ed25519.PublicKey
	private ed25519.PrivateKey
	now     time.Time
	target  string
}

func newUpdateFixture(t *testing.T) updateFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return updateFixture{
		public: publicKey, private: privateKey,
		now: time.Unix(1_800_000_000, 0).UTC(), target: "linux/amd64/tos-ai",
	}
}

func (f updateFixture) manifest(
	t *testing.T,
	artifact []byte,
	revision uint64,
) update.Manifest {
	t.Helper()
	digest := sha256.Sum256(artifact)
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "tos-ai-release.tar.gz",
		Digest:    "sha256:" + hex.EncodeToString(digest[:]),
		SizeBytes: uint64(len(artifact)), Target: f.target,
		SecurityRevision: revision, IssuedAt: f.now.UnixMilli(),
		ExpiresAt: f.now.Add(time.Hour).UnixMilli(), KeyID: "release-key-1",
	}, f.private)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func (f updateFixture) config(root string) Config {
	return Config{Root: root, Target: f.target, PublicKeys: map[string]ed25519.PublicKey{
		"release-key-1": f.public,
	}}
}

func TestTwoSlotActivationHealthRollbackAndAntiRollback(t *testing.T) {
	fixture := newUpdateFixture(t)
	root := filepath.Join(t.TempDir(), "updates")
	manager, err := Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	first := []byte("release-one")
	if slot, err := manager.Stage(
		context.Background(), fixture.manifest(t, first, 1),
		bytes.NewReader(first), fixture.now,
	); err != nil || slot != "a" {
		t.Fatalf("first stage slot=%q err=%v", slot, err)
	}
	if err := manager.ActivatePending(); err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfirmHealthy(); err == nil {
		t.Fatal("pre-restart process confirmed its own candidate")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err = Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	status, err := manager.Status()
	if err != nil || !status.AwaitingHealth || !status.BootAttempted || status.ActiveSlot != "a" {
		t.Fatalf("first candidate boot status=%#v err=%v", status, err)
	}
	if err := manager.ConfirmHealthy(); err != nil {
		t.Fatal(err)
	}
	status, err = manager.Status()
	if err != nil || status.ActiveSlot != "a" || status.KnownGoodSlot != "a" ||
		status.SecurityRevision != 1 || status.AwaitingHealth {
		t.Fatalf("confirmed first status=%#v err=%v", status, err)
	}

	second := []byte("release-two")
	secondManifest := fixture.manifest(t, second, 2)
	if slot, err := manager.Stage(
		context.Background(), secondManifest, bytes.NewReader(second), fixture.now,
	); err != nil || slot != "b" {
		t.Fatalf("second stage slot=%q err=%v", slot, err)
	}
	if err := manager.ActivatePending(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	// The first reopen is the intended candidate boot. Losing power before its
	// health confirmation makes the following reopen roll back.
	manager, err = Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	status, err = manager.Status()
	if err != nil || status.ActiveSlot != "b" || status.KnownGoodSlot != "a" ||
		!status.AwaitingHealth || !status.BootAttempted || status.SecurityRevision != 1 {
		t.Fatalf("candidate boot status=%#v err=%v", status, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err = Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	status, err = manager.Status()
	if err != nil || status.ActiveSlot != "a" || status.KnownGoodSlot != "a" ||
		status.AwaitingHealth || status.BootAttempted || status.SecurityRevision != 1 {
		t.Fatalf("power-loss rollback status=%#v err=%v", status, err)
	}
	if slot, err := manager.Stage(
		context.Background(), secondManifest, bytes.NewReader(second), fixture.now,
	); err != nil || slot != "b" {
		t.Fatalf("restaged second slot=%q err=%v", slot, err)
	}
	if err := manager.ActivatePending(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err = Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfirmHealthy(); err != nil {
		t.Fatal(err)
	}
	status, _ = manager.Status()
	if status.ActiveSlot != "b" || status.KnownGoodSlot != "b" ||
		status.SecurityRevision != 2 {
		t.Fatalf("confirmed second status=%#v", status)
	}
	if _, err := manager.Stage(
		context.Background(), fixture.manifest(t, first, 1),
		bytes.NewReader(first), fixture.now,
	); err == nil {
		t.Fatal("security revision rollback was staged")
	}

	third := []byte("release-three")
	if slot, err := manager.Stage(
		context.Background(), fixture.manifest(t, third, 3),
		bytes.NewReader(third), fixture.now,
	); err != nil || slot != "a" {
		t.Fatalf("third stage slot=%q err=%v", slot, err)
	}
	if err := manager.ActivatePending(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Rollback(); err != nil {
		t.Fatal(err)
	}
	status, _ = manager.Status()
	if status.ActiveSlot != "b" || status.KnownGoodSlot != "b" ||
		status.AwaitingHealth || status.SecurityRevision != 2 {
		t.Fatalf("manual rollback status=%#v", status)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStageIsBoundedCancelableAndSingleOwner(t *testing.T) {
	fixture := newUpdateFixture(t)
	root := filepath.Join(t.TempDir(), "updates")
	manager, err := Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	ownedTemporary := filepath.Join(root, "a", artifactName+".tmp")
	if err := os.WriteFile(ownedTemporary, []byte("owner staging"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(fixture.config(root)); err == nil {
		t.Fatal("second software update owner was accepted")
	}
	if data, err := os.ReadFile(ownedTemporary); err != nil || string(data) != "owner staging" {
		t.Fatalf("losing opener changed owner staging file: %q err=%v", data, err)
	}
	if err := os.Remove(ownedTemporary); err != nil {
		t.Fatal(err)
	}

	artifact := []byte("candidate")
	manifest := fixture.manifest(t, artifact, 1)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.Stage(
		canceled, manifest, bytes.NewReader(artifact), fixture.now,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stage error=%v", err)
	}

	var wait sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, stageErr := manager.Stage(
				context.Background(), manifest, bytes.NewReader(artifact), fixture.now,
			)
			results <- stageErr
		}()
	}
	wait.Wait()
	close(results)
	successes := 0
	for stageErr := range results {
		if stageErr == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent stage successes=%d", successes)
	}

	for _, slot := range []string{"a", "b"} {
		entries, err := os.ReadDir(filepath.Join(root, slot))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 2 {
			t.Fatalf("slot %s retained %d files", slot, len(entries))
		}
	}
}

func TestRecoveryCleansInterruptedFilesAndRejectsTampering(t *testing.T) {
	fixture := newUpdateFixture(t)
	root := filepath.Join(t.TempDir(), "updates")
	manager, err := Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	artifact := []byte("release")
	if _, err := manager.Stage(
		context.Background(), fixture.manifest(t, artifact, 1),
		bytes.NewReader(artifact), fixture.now,
	); err != nil {
		t.Fatal(err)
	}
	if err := manager.ActivatePending(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err = Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfirmHealthy(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a", artifactName+".tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, stateName+".tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err = Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "a", artifactName+".tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("interrupted artifact residue was not removed")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(root, "a", artifactName)
	if err := os.Chmod(artifactPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifactPath, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(fixture.config(root)); err == nil {
		t.Fatal("tampered installed artifact was accepted")
	}
}

func TestMOCKDiskFullDoesNotAdvanceSoftwareUpdateState(t *testing.T) {
	fixture := newUpdateFixture(t)
	root := filepath.Join(t.TempDir(), "updates")
	manager, err := Open(fixture.config(root))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	artifact := []byte("release-with-disk-faults")
	manifest := fixture.manifest(t, artifact, 1)
	originalWrite := manager.writeFile

	for _, failedFile := range []string{manifestName, stateName} {
		manager.writeFile = func(
			path string, data []byte, mode os.FileMode,
		) error {
			if filepath.Base(path) == failedFile {
				return syscall.ENOSPC
			}
			return originalWrite(path, data, mode)
		}
		if _, err := manager.Stage(
			context.Background(), manifest, bytes.NewReader(artifact), fixture.now,
		); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("%s disk-full error=%v", failedFile, err)
		}
		status, err := manager.Status()
		if err != nil || status.PendingSlot != "" || status.ActiveSlot != "" ||
			status.AwaitingHealth {
			t.Fatalf("%s disk-full advanced state: %#v err=%v", failedFile, status, err)
		}
		for _, temporary := range []string{
			filepath.Join(root, "a", artifactName+".tmp"),
			filepath.Join(root, "a", manifestName+".tmp"),
			filepath.Join(root, stateName+".tmp"),
		} {
			if _, err := os.Lstat(temporary); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s retained temporary file %s: %v", failedFile, temporary, err)
			}
		}
	}

	manager.writeFile = originalWrite
	if slot, err := manager.Stage(
		context.Background(), manifest, bytes.NewReader(artifact), fixture.now,
	); err != nil || slot != "a" {
		t.Fatalf("retry after disk-full slot=%q err=%v", slot, err)
	}
}
