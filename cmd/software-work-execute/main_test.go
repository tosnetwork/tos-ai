package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadPrivateSource(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "source.tar")
	if err := os.WriteFile(path, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readPrivateSource(path)
	if err != nil || string(got) != "archive" {
		t.Fatalf("readPrivateSource() = %q, %v", got, err)
	}
}

func TestReadPrivateSourceRejectsUnsafePaths(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(directory, "private")
	if err := os.WriteFile(private, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	public := filepath.Join(directory, "public")
	if err := os.WriteFile(public, []byte("archive"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	large := filepath.Join(directory, "large")
	file, err := os.OpenFile(large, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxSourceBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	for name, path := range map[string]string{
		"relative": "source.tar", "public": public, "symlink": link, "large": large,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := readPrivateSource(path); err == nil {
				t.Fatal("unsafe source archive accepted")
			}
		})
	}
}

func TestRequirePrivateOwnedDirectory(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := requirePrivateOwnedDirectory(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := requirePrivateOwnedDirectory(directory); err == nil {
		t.Fatal("group-accessible directory accepted")
	}
}
