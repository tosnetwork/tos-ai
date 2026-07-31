package modelmanager

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/update"
)

const (
	metadataVersion     = 1
	artifactSuffix      = ".model"
	metadataSuffix      = ".manifest"
	artifactStagePrefix = ".model-stage-*"
	metadataStagePrefix = ".manifest-stage-*"
	directoryReadBatch  = 128
)

type persistedMetadata struct {
	Version          uint8           `json:"version"`
	AcceptedAtMillis int64           `json:"acceptedAtMillis"`
	Manifest         update.Manifest `json:"manifest"`
}

type recoveryPair struct {
	digestHex    string
	artifactPath string
	metadataPath string
}

type recoveryCandidate struct {
	pair     recoveryPair
	manifest update.Manifest
	modified int64
}

func (m *Manager) artifactPath(digest string) string {
	return filepath.Join(m.config.RootDir, strings.TrimPrefix(digest, "sha256:")+artifactSuffix)
}

func (m *Manager) metadataPath(digest string) string {
	return filepath.Join(m.config.RootDir, strings.TrimPrefix(digest, "sha256:")+metadataSuffix)
}

func (m *Manager) writeMetadataTemp(manifest update.Manifest, acceptedAt time.Time) (string, error) {
	data, err := json.Marshal(persistedMetadata{
		Version: metadataVersion, AcceptedAtMillis: acceptedAt.UnixMilli(), Manifest: manifest,
	})
	if err != nil || len(data) == 0 || int64(len(data)) > MaxMetadataBytes {
		return "", errors.New("model metadata exceeds hard limits")
	}
	file, err := os.CreateTemp(m.config.RootDir, metadataStagePrefix)
	if err != nil {
		return "", errors.New("stage model metadata")
	}
	path := file.Name()
	success := false
	defer func() {
		_ = file.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return "", errors.New("secure model metadata")
	}
	written, err := file.Write(data)
	if err != nil || written != len(data) {
		return "", errors.New("write model metadata")
	}
	if err := file.Sync(); err != nil {
		return "", errors.New("sync model metadata")
	}
	if err := file.Close(); err != nil {
		return "", errors.New("close model metadata")
	}
	success = true
	return path, nil
}

func (m *Manager) activateFiles(artifactTemp, metadataTemp, artifactFinal, metadataFinal string) error {
	if err := os.Rename(artifactTemp, artifactFinal); err != nil {
		return errors.New("activate model artifact")
	}
	if err := os.Rename(metadataTemp, metadataFinal); err != nil {
		_ = os.Remove(artifactFinal)
		return errors.New("activate model metadata")
	}
	if err := syncDirectory(m.config.RootDir); err != nil {
		_ = os.Remove(metadataFinal)
		_ = os.Remove(artifactFinal)
		_ = syncDirectory(m.config.RootDir)
		return errors.New("sync model activation")
	}
	return nil
}

func (m *Manager) recover() error {
	pairs, cleaned, err := m.scanRecoveryPairs()
	if err != nil {
		return err
	}
	candidates := make([]recoveryCandidate, 0, len(pairs))
	recoveryNow := m.config.Now()
	for _, pair := range pairs {
		if pair.artifactPath == "" || pair.metadataPath == "" {
			if err := removePair(pair); err != nil {
				return errors.New("clean incomplete model cache entry")
			}
			cleaned = true
			continue
		}
		manifest, modified, err := m.readRecoveryMetadata(pair, recoveryNow)
		if err != nil {
			return err
		}
		candidates = append(candidates, recoveryCandidate{
			pair: pair, manifest: manifest, modified: modified,
		})
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].modified == candidates[right].modified {
			return candidates[left].pair.digestHex < candidates[right].pair.digestHex
		}
		return candidates[left].modified < candidates[right].modified
	})

	retained := make([]recoveryCandidate, 0, min(len(candidates), m.config.MaxModels))
	evicted := make([]recoveryPair, 0, len(candidates))
	var retainedBytes uint64
	for index := len(candidates) - 1; index >= 0; index-- {
		candidate := candidates[index]
		size := candidate.manifest.SizeBytes
		if len(retained) < m.config.MaxModels && size <= m.config.MaxTotalBytes-retainedBytes {
			retained = append(retained, candidate)
			retainedBytes += size
			continue
		}
		evicted = append(evicted, candidate.pair)
	}
	sort.Slice(retained, func(left, right int) bool {
		if retained[left].modified == retained[right].modified {
			return retained[left].pair.digestHex < retained[right].pair.digestHex
		}
		return retained[left].modified < retained[right].modified
	})
	for _, candidate := range retained {
		if err := verifyRecoveryArtifact(candidate.pair.artifactPath, candidate.manifest); err != nil {
			return errors.New("recovered model artifact verification failed")
		}
	}
	for _, pair := range evicted {
		if err := removePair(pair); err != nil {
			return errors.New("evict recovered model")
		}
		cleaned = true
	}
	if cleaned {
		if err := syncDirectory(m.config.RootDir); err != nil {
			return errors.New("sync recovered model cache")
		}
	}
	for _, candidate := range retained {
		current := &entry{
			model: Model{
				Artifact: candidate.manifest.Artifact, Digest: candidate.manifest.Digest,
				SizeBytes:        candidate.manifest.SizeBytes,
				SecurityRevision: candidate.manifest.SecurityRevision, State: StateReady,
			},
			path: candidate.pair.artifactPath, metadataPath: candidate.pair.metadataPath,
		}
		current.element = m.lru.PushBack(current)
		m.entries[current.model.Digest] = current
		m.totalBytes += current.model.SizeBytes
	}
	return nil
}

func (m *Manager) scanRecoveryPairs() (map[string]recoveryPair, bool, error) {
	directory, err := os.Open(m.config.RootDir)
	if err != nil {
		return nil, false, errors.New("open model cache")
	}
	defer directory.Close()
	pairs := make(map[string]recoveryPair, m.config.MaxModels)
	cleaned := false
	entryCount := 0
	for {
		entries, readErr := directory.ReadDir(directoryReadBatch)
		for _, item := range entries {
			entryCount++
			if entryCount > MaxDirectoryEntriesHard {
				return nil, false, errors.New("model cache directory exceeds hard entry limit")
			}
			name := item.Name()
			path := filepath.Join(m.config.RootDir, name)
			if name == managerLockFile {
				continue
			}
			if strings.HasPrefix(name, strings.TrimSuffix(artifactStagePrefix, "*")) ||
				strings.HasPrefix(name, strings.TrimSuffix(metadataStagePrefix, "*")) {
				if err := os.Remove(path); err != nil {
					return nil, false, errors.New("clean staged model file")
				}
				cleaned = true
				continue
			}
			digestHex, kind, ok := parseCacheName(name)
			if !ok {
				return nil, false, errors.New("model cache contains an unrecognized entry")
			}
			pair := pairs[digestHex]
			pair.digestHex = digestHex
			if kind == artifactSuffix {
				pair.artifactPath = path
			} else {
				pair.metadataPath = path
			}
			pairs[digestHex] = pair
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, false, errors.New("scan model cache")
		}
	}
	return pairs, cleaned, nil
}

func parseCacheName(name string) (string, string, bool) {
	var suffix string
	switch {
	case strings.HasSuffix(name, artifactSuffix):
		suffix = artifactSuffix
	case strings.HasSuffix(name, metadataSuffix):
		suffix = metadataSuffix
	default:
		return "", "", false
	}
	digestHex := strings.TrimSuffix(name, suffix)
	if len(digestHex) != 64 || digestHex != strings.ToLower(digestHex) {
		return "", "", false
	}
	if _, err := hex.DecodeString(digestHex); err != nil {
		return "", "", false
	}
	return digestHex, suffix, true
}

func (m *Manager) readRecoveryMetadata(pair recoveryPair, recoveryNow time.Time) (update.Manifest, int64, error) {
	data, info, err := readPrivateRegular(pair.metadataPath, MaxMetadataBytes)
	if err != nil {
		return update.Manifest{}, 0, errors.New("read recovered model metadata")
	}
	var metadata persistedMetadata
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return update.Manifest{}, 0, errors.New("decode recovered model metadata")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return update.Manifest{}, 0, errors.New("decode recovered model metadata")
	}
	canonical, err := json.Marshal(metadata)
	if err != nil || !bytes.Equal(data, canonical) || metadata.Version != metadataVersion {
		return update.Manifest{}, 0, errors.New("recovered model metadata is not canonical")
	}
	publicKey := m.config.Signers[metadata.Manifest.KeyID]
	if publicKey == nil {
		return update.Manifest{}, 0, errors.New("recovered model signer is not approved")
	}
	if err := metadata.Manifest.VerifyInstalled(
		publicKey, m.config.Target, m.config.CurrentSecurityRevision,
	); err != nil {
		return update.Manifest{}, 0, errors.New("recovered model manifest rejected")
	}
	acceptedAt := time.UnixMilli(metadata.AcceptedAtMillis)
	if time.UnixMilli(metadata.Manifest.IssuedAt).After(acceptedAt.Add(2*time.Minute)) ||
		!time.UnixMilli(metadata.Manifest.ExpiresAt).After(acceptedAt) ||
		acceptedAt.After(recoveryNow.Add(2*time.Minute)) {
		return update.Manifest{}, 0, errors.New("recovered model acceptance time rejected")
	}
	if strings.TrimPrefix(metadata.Manifest.Digest, "sha256:") != pair.digestHex {
		return update.Manifest{}, 0, errors.New("recovered model digest does not match its path")
	}
	artifactInfo, err := os.Lstat(pair.artifactPath)
	if err != nil || !privateRegular(artifactInfo) ||
		uint64(artifactInfo.Size()) != metadata.Manifest.SizeBytes {
		return update.Manifest{}, 0, errors.New("recovered model artifact metadata mismatch")
	}
	return metadata.Manifest, info.ModTime().UnixNano(), nil
}

func verifyRecoveryArtifact(path string, manifest update.Manifest) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !privateRegular(info) || uint64(info.Size()) != manifest.SizeBytes {
		return errors.New("invalid recovered model file")
	}
	return manifest.VerifyArtifact(file)
}

func readPrivateRegular(path string, maximum int64) ([]byte, os.FileInfo, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil || !privateRegular(pathInfo) || pathInfo.Size() <= 0 ||
		pathInfo.Size() > maximum {
		return nil, nil, errors.New("invalid private file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !os.SameFile(pathInfo, info) || !privateRegular(info) ||
		info.Size() <= 0 || info.Size() > maximum {
		return nil, nil, errors.New("private file changed")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(data)) != info.Size() {
		return nil, nil, errors.New("read private file")
	}
	return data, info, nil
}

func privateRegular(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() &&
		info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == 0o600
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}

func removePair(pair recoveryPair) error {
	if pair.metadataPath != "" {
		if err := os.Remove(pair.metadataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if pair.artifactPath != "" {
		if err := os.Remove(pair.artifactPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
