package artifacthttp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
)

func TestLocatorServesVerifiedObjectOverHTTPS(t *testing.T) {
	root := filepath.Join(resolvedTempDir(t), "artifacts")
	store, err := artifactstore.Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("verified paid-work artifact")
	descriptor, err := store.Put(context.Background(), "application/octet-stream", bytes.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}

	var locator *Locator
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		locator.Handler().ServeHTTP(writer, request)
	}))
	defer server.Close()
	locator, err = New(store, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	artifactURL, err := locator.ArtifactURL(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Get(artifactURL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(value)
	if response.StatusCode != http.StatusOK || !bytes.Equal(got, value) ||
		response.Header.Get("Content-Type") != descriptor.MediaType ||
		response.Header.Get("Digest") != "sha-256="+base64.StdEncoding.EncodeToString(sum[:]) ||
		response.Header.Get("ETag") != `"`+strings.TrimPrefix(descriptor.Digest, "sha256:")+`"` {
		t.Fatalf("unexpected artifact response: status=%d headers=%v body=%q", response.StatusCode, response.Header, got)
	}
}

func TestLocatorRejectsUnregisteredAndTamperedObjects(t *testing.T) {
	root := filepath.Join(resolvedTempDir(t), "artifacts")
	store, err := artifactstore.Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("valid"))
	if err != nil {
		t.Fatal(err)
	}
	locator, err := New(store, "https://artifacts.example")
	if err != nil {
		t.Fatal(err)
	}
	unregistered := httptest.NewRecorder()
	locator.Handler().ServeHTTP(unregistered, httptest.NewRequest(http.MethodGet,
		"https://artifacts.example/v1/artifacts/sha256/"+strings.Repeat("0", 64), nil))
	if unregistered.Code != http.StatusNotFound {
		t.Fatalf("unregistered status=%d", unregistered.Code)
	}
	artifactURL, err := locator.ArtifactURL(descriptor)
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
	tampered := httptest.NewRecorder()
	locator.Handler().ServeHTTP(tampered, httptest.NewRequest(http.MethodGet, artifactURL, nil))
	if tampered.Code != http.StatusGone {
		t.Fatalf("tampered status=%d body=%q", tampered.Code, tampered.Body.String())
	}
}

func TestLocatorRejectsAmbiguousOriginsAndDescriptors(t *testing.T) {
	store := &memoryStore{value: []byte("x")}
	for _, origin := range []string{"http://artifacts.example", "https://user@artifacts.example", "https://artifacts.example/path", "https://artifacts.example/"} {
		if _, err := New(store, origin); err == nil {
			t.Fatalf("unsafe origin accepted: %s", origin)
		}
	}
	locator, err := New(store, "https://artifacts.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := locator.ArtifactURL(artifactstore.Descriptor{
		Digest: "sha256:" + strings.Repeat("0", 64), MediaType: "application/octet-stream", SizeBytes: 2,
	}); err == nil {
		t.Fatal("descriptor with conflicting size was published")
	}
}

func TestPersistentLocatorSurvivesRestart(t *testing.T) {
	root := resolvedTempDir(t)
	store, err := artifactstore.Open(filepath.Join(root, "artifacts"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("durable"))
	if err != nil {
		t.Fatal(err)
	}
	indexRoot := filepath.Join(root, "private-index")
	if err := os.Mkdir(indexRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(indexRoot, "publications.json")
	first, err := OpenPersistent(store, "https://artifacts.example", index)
	if err != nil {
		t.Fatal(err)
	}
	artifactURL, err := first.ArtifactURL(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(index)
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("publication index mode = %v, %v", info, err)
	}

	restarted, err := OpenPersistent(store, "https://artifacts.example", index)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	restarted.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, artifactURL, nil))
	if response.Code != http.StatusOK || response.Body.String() != "durable" {
		t.Fatalf("restarted locator response = %d %q", response.Code, response.Body.String())
	}
}

type memoryStore struct{ value []byte }

func (s *memoryStore) Get(context.Context, string) ([]byte, error) {
	return append([]byte(nil), s.value...), nil
}

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}
