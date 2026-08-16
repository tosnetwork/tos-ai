//go:build linux || darwin

package dirlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireIsExclusiveReusableAndIdempotent(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := Acquire(directory, ".owner.lock")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(
		directory, ".owner.lock",
	); !errors.Is(err, ErrHeld) {
		t.Fatalf("concurrent ownership error=%v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(directory, ".owner.lock")
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(directory, ".owner.lock"))
	if err != nil || !info.Mode().IsRegular() ||
		info.Mode().Perm() != 0o600 {
		t.Fatalf("lock file info=%v err=%v", info, err)
	}
}

func TestAcquireRejectsUnsafeFilesAndNames(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../lock", "a/b"} {
		if _, err := Acquire(t.TempDir(), name); err == nil {
			t.Fatalf("unsafe lock name %q accepted", name)
		}
	}
	t.Run("symlink", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(
			target, filepath.Join(directory, ".owner.lock"),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := Acquire(directory, ".owner.lock"); err == nil {
			t.Fatal("symlink ownership file accepted")
		}
	})
	t.Run("permissions", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.WriteFile(
			filepath.Join(directory, ".owner.lock"), nil, 0o644,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := Acquire(directory, ".owner.lock"); err == nil {
			t.Fatal("public ownership file accepted")
		}
	})
}
