// Package artifacthttp publishes verified artifactstore objects through a
// stable HTTPS content-addressed URL. The URL is only a locator: Store.Get
// revalidates the bytes against the requested digest on every response.
package artifacthttp

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
)

const (
	pathPrefix             = "/v1/artifacts/sha256/"
	publicationIndexSchema = "tos.service.artifact-publications.v1"
	maximumIndexBytes      = 8 << 20
)

type verifiedStore interface {
	Get(context.Context, string) ([]byte, error)
}

// Locator registers descriptors emitted by the runner and serves their bytes.
// Metadata is learned only after the descriptor has been checked against the
// underlying content-addressed store.
type Locator struct {
	store  verifiedStore
	origin string
	index  string

	mutex       sync.RWMutex
	descriptors map[string]artifactstore.Descriptor
}

type publicationIndex struct {
	Schema      string                  `json:"schema"`
	Descriptors []publicationDescriptor `json:"descriptors"`
}

type publicationDescriptor struct {
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	SizeBytes uint64 `json:"size_bytes"`
}

// New returns an HTTPS-only locator. Origin must be an origin URL without
// credentials, path, query, or fragment so generated URLs are unambiguous.
func New(store verifiedStore, origin string) (*Locator, error) {
	parsed, err := url.Parse(origin)
	if err != nil || parsed == nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" || strings.HasSuffix(origin, "/") {
		return nil, errors.New("invalid artifact HTTPS origin")
	}
	if store == nil {
		return nil, errors.New("artifact store is required")
	}
	return &Locator{store: store, origin: origin, descriptors: make(map[string]artifactstore.Descriptor)}, nil
}

// OpenPersistent returns a locator whose published descriptor index survives
// process restarts. The index is private, atomically replaced, and every entry
// is revalidated against the content-addressed store before it becomes public.
func OpenPersistent(store verifiedStore, origin, indexPath string) (*Locator, error) {
	locator, err := New(store, origin)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(indexPath) || filepath.Clean(indexPath) != indexPath {
		return nil, errors.New("artifact publication index path must be canonical and absolute")
	}
	parent := filepath.Dir(indexPath)
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil || resolved != parent {
		return nil, errors.New("artifact publication index directory contains a symlink")
	}
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("artifact publication index directory must be private")
	}
	locator.index = indexPath
	descriptors, err := readPublicationIndex(indexPath)
	if err != nil {
		return nil, err
	}
	for _, descriptor := range descriptors {
		value, getErr := store.Get(context.Background(), descriptor.Digest)
		if getErr != nil || uint64(len(value)) != descriptor.SizeBytes {
			return nil, errors.New("artifact publication index does not match stored object")
		}
		locator.descriptors[descriptor.Digest] = descriptor
	}
	return locator, nil
}

// ArtifactURL verifies and registers one exact descriptor before publishing a
// locator for it. A caller cannot advertise arbitrary bytes by inventing a
// digest or media type.
func (l *Locator) ArtifactURL(descriptor artifactstore.Descriptor) (string, error) {
	if l == nil || l.store == nil || !validDescriptor(descriptor) {
		return "", errors.New("invalid artifact descriptor")
	}
	value, err := l.store.Get(context.Background(), descriptor.Digest)
	if err != nil || uint64(len(value)) != descriptor.SizeBytes {
		return "", errors.New("artifact descriptor does not match stored object")
	}
	l.mutex.Lock()
	if existing, found := l.descriptors[descriptor.Digest]; found && existing != descriptor {
		l.mutex.Unlock()
		return "", errors.New("artifact descriptor conflicts with published metadata")
	}
	if _, found := l.descriptors[descriptor.Digest]; !found && l.index != "" {
		if err := l.writePublicationIndexLocked(descriptor); err != nil {
			l.mutex.Unlock()
			return "", err
		}
	}
	l.descriptors[descriptor.Digest] = descriptor
	l.mutex.Unlock()
	return l.origin + pathPrefix + strings.TrimPrefix(descriptor.Digest, "sha256:"), nil
}

func readPublicationIndex(path string) ([]artifactstore.Descriptor, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || info.Size() < 0 || info.Size() > maximumIndexBytes {
		return nil, errors.New("invalid artifact publication index")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open artifact publication index")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 {
		return nil, errors.New("artifact publication index changed while opening")
	}
	decoder := json.NewDecoder(io.LimitReader(file, maximumIndexBytes+1))
	decoder.DisallowUnknownFields()
	var document publicationIndex
	if err := decoder.Decode(&document); err != nil || document.Schema != publicationIndexSchema {
		return nil, errors.New("decode artifact publication index")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("artifact publication index has trailing data")
	}
	descriptors := make([]artifactstore.Descriptor, 0, len(document.Descriptors))
	previous := ""
	for _, entry := range document.Descriptors {
		descriptor := artifactstore.Descriptor{Digest: entry.Digest, MediaType: entry.MediaType, SizeBytes: entry.SizeBytes}
		if !validDescriptor(descriptor) || entry.Digest <= previous {
			return nil, errors.New("invalid artifact publication descriptor")
		}
		previous = entry.Digest
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

func (l *Locator) writePublicationIndexLocked(add artifactstore.Descriptor) error {
	descriptors := make([]artifactstore.Descriptor, 0, len(l.descriptors)+1)
	for _, descriptor := range l.descriptors {
		descriptors = append(descriptors, descriptor)
	}
	descriptors = append(descriptors, add)
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].Digest < descriptors[right].Digest })
	document := publicationIndex{Schema: publicationIndexSchema, Descriptors: make([]publicationDescriptor, 0, len(descriptors))}
	for _, descriptor := range descriptors {
		document.Descriptors = append(document.Descriptors, publicationDescriptor{
			Digest: descriptor.Digest, MediaType: descriptor.MediaType, SizeBytes: descriptor.SizeBytes,
		})
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil || len(encoded)+1 > maximumIndexBytes {
		return errors.New("encode artifact publication index")
	}
	encoded = append(encoded, '\n')
	parent := filepath.Dir(l.index)
	temporary, err := os.CreateTemp(parent, ".artifact-publications-")
	if err != nil {
		return errors.New("create artifact publication index")
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(encoded)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return errors.New("write artifact publication index")
	}
	if err := os.Rename(temporaryPath, l.index); err != nil {
		return errors.New("commit artifact publication index")
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("open artifact publication index directory")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("sync artifact publication index directory")
	}
	return nil
}

// Handler serves only registered content-addressed artifacts. It deliberately
// does not support ranges, directory listings, redirects, or caller-selected
// filesystem paths.
func (l *Locator) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if l == nil || request == nil || (request.Method != http.MethodGet && request.Method != http.MethodHead) ||
			request.URL.RawQuery != "" || request.URL.Fragment != "" {
			http.Error(writer, "not found", http.StatusNotFound)
			return
		}
		hexDigest, found := strings.CutPrefix(request.URL.EscapedPath(), pathPrefix)
		if !found || len(hexDigest) != 64 || strings.Contains(hexDigest, "/") || strings.ToLower(hexDigest) != hexDigest {
			http.Error(writer, "not found", http.StatusNotFound)
			return
		}
		digest := "sha256:" + hexDigest
		l.mutex.RLock()
		descriptor, registered := l.descriptors[digest]
		l.mutex.RUnlock()
		if !registered {
			http.Error(writer, "not found", http.StatusNotFound)
			return
		}
		value, err := l.store.Get(request.Context(), digest)
		if err != nil || uint64(len(value)) != descriptor.SizeBytes {
			http.Error(writer, "artifact unavailable", http.StatusGone)
			return
		}
		sum := sha256.Sum256(value)
		if subtle.ConstantTimeCompare([]byte(digest), []byte(fmt.Sprintf("sha256:%x", sum[:]))) != 1 {
			http.Error(writer, "artifact unavailable", http.StatusGone)
			return
		}
		writer.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		writer.Header().Set("Content-Type", descriptor.MediaType)
		writer.Header().Set("Content-Length", strconv.FormatUint(descriptor.SizeBytes, 10))
		writer.Header().Set("Digest", "sha-256="+base64.StdEncoding.EncodeToString(sum[:]))
		writer.Header().Set("ETag", `"`+hexDigest+`"`)
		if request.Method == http.MethodHead {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(value)
	})
}

func validDescriptor(descriptor artifactstore.Descriptor) bool {
	if len(descriptor.Digest) != len("sha256:")+64 || !strings.HasPrefix(descriptor.Digest, "sha256:") ||
		descriptor.Digest != strings.ToLower(descriptor.Digest) || descriptor.SizeBytes == 0 ||
		descriptor.SizeBytes > artifactstore.MaxObjectBytesHard || descriptor.MediaType == "" ||
		strings.ContainsAny(descriptor.MediaType, "\r\n\x00") {
		return false
	}
	for _, character := range strings.TrimPrefix(descriptor.Digest, "sha256:") {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
