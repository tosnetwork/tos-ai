// Package artifactstore provides a bounded, immutable local content-addressed
// store for software-work outputs. Artifact bytes, not gateway metadata, are
// the address authority.
package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const MaxObjectBytesHard = uint64(64 << 20)

var (
	digestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	mediaTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9!#$&^_.+-]{0,126}/[a-z0-9][a-z0-9!#$&^_.+-]{0,126}$`)
)

type Descriptor struct {
	Digest    string
	MediaType string
	SizeBytes uint64
}

type Store struct {
	objects string
	maximum uint64
}

func Open(root string, maximum uint64) (*Store, error) {
	if !filepath.IsAbs(root) || maximum == 0 || maximum > MaxObjectBytesHard {
		return nil, errors.New("invalid artifact store configuration")
	}
	clean := filepath.Clean(root)
	if clean != root {
		return nil, errors.New("artifact store root is not canonical")
	}
	if err := os.MkdirAll(clean, 0o700); err != nil {
		return nil, errors.New("create artifact store root")
	}
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil || resolved != clean {
		return nil, errors.New("artifact store root contains a symlink")
	}
	if err := requirePrivateDirectory(clean); err != nil {
		return nil, err
	}
	objects := filepath.Join(clean, "objects")
	if err := os.Mkdir(objects, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.New("create artifact object directory")
	}
	if err := requirePrivateDirectory(objects); err != nil {
		return nil, err
	}
	return &Store{objects: objects, maximum: maximum}, nil
}

func (s *Store) Put(ctx context.Context, mediaType string, reader io.Reader) (Descriptor, error) {
	if s == nil || s.objects == "" || ctx == nil || reader == nil || !mediaTypePattern.MatchString(mediaType) {
		return Descriptor{}, errors.New("invalid artifact put request")
	}
	if err := ctx.Err(); err != nil {
		return Descriptor{}, err
	}
	temporary, err := os.CreateTemp(s.objects, ".put-")
	if err != nil {
		return Descriptor{}, errors.New("create artifact temporary object")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	written, copyErr := copyBounded(ctx, io.MultiWriter(temporary, hash), reader, s.maximum)
	if syncErr := temporary.Sync(); copyErr == nil && syncErr != nil {
		copyErr = syncErr
	}
	if closeErr := temporary.Close(); copyErr == nil && closeErr != nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return Descriptor{}, copyErr
	}
	if err := os.Chmod(temporaryName, 0o400); err != nil {
		return Descriptor{}, errors.New("protect artifact temporary object")
	}
	digest := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	target := s.path(digest)
	if err := os.Link(temporaryName, target); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return Descriptor{}, errors.New("commit artifact object")
		}
		stored, err := s.Get(ctx, digest)
		if err != nil || uint64(len(stored)) != written {
			return Descriptor{}, errors.New("existing artifact object failed verification")
		}
	} else if err := syncDirectory(s.objects); err != nil {
		return Descriptor{}, err
	}
	return Descriptor{Digest: digest, MediaType: mediaType, SizeBytes: written}, nil
}

func (s *Store) Get(ctx context.Context, digest string) ([]byte, error) {
	if s == nil || s.objects == "" || ctx == nil || !digestPattern.MatchString(digest) {
		return nil, errors.New("invalid artifact get request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	objectPath := s.path(digest)
	info, err := os.Lstat(objectPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o400 || info.Size() < 0 || uint64(info.Size()) > s.maximum {
		return nil, errors.New("invalid artifact object")
	}
	file, err := os.Open(objectPath)
	if err != nil {
		return nil, errors.New("open artifact object")
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(info, openedInfo) || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm() != 0o400 {
		return nil, errors.New("artifact object changed while opening")
	}
	value, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: file}, int64(s.maximum)+1))
	if err != nil || uint64(len(value)) > s.maximum || int64(len(value)) != info.Size() {
		return nil, errors.New("read artifact object")
	}
	hash := sha256.Sum256(value)
	if "sha256:"+hex.EncodeToString(hash[:]) != digest {
		return nil, errors.New("artifact object digest mismatch")
	}
	return value, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("open artifact object directory")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("sync artifact object directory")
	}
	return nil
}

func (s *Store) path(digest string) string {
	return filepath.Join(s.objects, strings.TrimPrefix(digest, "sha256:"))
}

func requirePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("artifact store directory must be private")
	}
	return nil
}

func copyBounded(ctx context.Context, destination io.Writer, source io.Reader, maximum uint64) (uint64, error) {
	written, err := io.Copy(destination, io.LimitReader(&contextReader{ctx: ctx, reader: source}, int64(maximum)+1))
	if err != nil {
		return 0, errors.New("write artifact object")
	}
	if uint64(written) > maximum {
		return 0, errors.New("artifact object exceeds size limit")
	}
	return uint64(written), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(value []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(value)
}
