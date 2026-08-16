package artifactstore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreRoundTripIsContentAddressedAndIdempotent(t *testing.T) {
	store, err := Open(filepath.Join(resolvedTempDir(t), "artifacts"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("immutable artifact")
	first, err := store.Put(context.Background(), "application/octet-stream", bytes.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(context.Background(), "application/octet-stream", bytes.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Digest != "sha256:eca6f2c7063ef1bf0c7a3ee5beab0e50fb58b13e205106677b8a2470ad8e00ab" {
		t.Fatalf("descriptors = %#v %#v", first, second)
	}
	got, err := store.Get(context.Background(), first.Digest)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("Get() = %q, %v", got, err)
	}
}

func TestStoreRejectsOverflowTraversalSymlinkAndTampering(t *testing.T) {
	root := filepath.Join(resolvedTempDir(t), "artifacts")
	store, err := Open(root, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("123456789")); err == nil {
		t.Fatal("oversized artifact accepted")
	}
	for _, digest := range []string{"../etc/passwd", "sha256:" + strings.Repeat("A", 64), "sha256:" + strings.Repeat("0", 63)} {
		if _, err := store.Get(context.Background(), digest); err == nil {
			t.Fatalf("unsafe digest %q accepted", digest)
		}
	}
	descriptor, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("valid"))
	if err != nil {
		t.Fatal(err)
	}
	object := filepath.Join(root, "objects", strings.TrimPrefix(descriptor.Digest, "sha256:"))
	if err := os.Chmod(object, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(object, []byte("evil!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), descriptor.Digest); err == nil {
		t.Fatal("tampered artifact accepted")
	}
	if err := os.Remove(object); err != nil {
		t.Fatal(err)
	}
	outside := resolvedTempDir(t)
	if err := os.Symlink(filepath.Join(outside, "missing"), object); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), descriptor.Digest); err == nil {
		t.Fatal("symlink artifact accepted")
	}

	link := filepath.Join(resolvedTempDir(t), "linked-store")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(link, 8); err == nil {
		t.Fatal("symlink store root accepted")
	}
}

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}
