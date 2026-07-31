// Package modelmanager manages only operator-approved, signed,
// content-addressed model artifacts. It does not download arbitrary URLs.
package modelmanager

import (
	"container/list"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/update"
)

const (
	MaxModelsHard           = 1024
	MaxTotalBytesHard       = uint64(1 << 40)
	MaxSignersHard          = 64
	MaxMetadataBytes        = int64(16 << 10)
	MaxDirectoryEntriesHard = 4096
)

var (
	ErrCapacity = errors.New("model cache capacity is unavailable")
	ErrBusy     = errors.New("model is already being verified")
	ErrNotReady = errors.New("model is not ready")
	ErrInUse    = errors.New("model is active, pinned, or in use")
	ErrConflict = errors.New("model digest metadata conflicts with cached entry")
)

type State string

const (
	StateAbsent    State = "absent"
	StateVerifying State = "verifying"
	StateReady     State = "ready"
	StateActive    State = "active"
	StateDraining  State = "draining"
	StateFailed    State = "failed"
)

type Config struct {
	RootDir                 string
	Target                  string
	CurrentSecurityRevision uint64
	MaxModels               int
	MaxTotalBytes           uint64
	Signers                 map[string]ed25519.PublicKey
	Now                     func() time.Time
}

type Model struct {
	Artifact         string
	Digest           string
	SizeBytes        uint64
	SecurityRevision uint64
	State            State
	Pinned           bool
	InUse            int
	ErrorCategory    string
}

type entry struct {
	model        Model
	path         string
	metadataPath string
	element      *list.Element
}

type Manager struct {
	mu            sync.Mutex
	config        Config
	entries       map[string]*entry
	lru           *list.List
	totalBytes    uint64
	reservedBytes uint64
}

func New(config Config) (*Manager, error) {
	if config.RootDir == "" || !filepath.IsAbs(config.RootDir) ||
		config.Target == "" || len(config.Target) > 256 ||
		config.MaxModels <= 0 || config.MaxModels > MaxModelsHard ||
		config.MaxTotalBytes == 0 || config.MaxTotalBytes > MaxTotalBytesHard ||
		len(config.Signers) == 0 || len(config.Signers) > MaxSignersHard {
		return nil, errors.New("invalid model manager configuration")
	}
	for keyID, key := range config.Signers {
		if keyID == "" || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("invalid model signer configuration")
		}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if err := os.MkdirAll(config.RootDir, 0o700); err != nil {
		return nil, fmt.Errorf("create model cache: %w", err)
	}
	info, err := os.Lstat(config.RootDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 {
		return nil, errors.New("model cache must be a private non-symlink directory")
	}
	manager := &Manager{
		config:  config,
		entries: make(map[string]*entry, config.MaxModels),
		lru:     list.New(),
	}
	if err := manager.recover(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) Import(ctx context.Context, manifest update.Manifest, source io.Reader) (Model, error) {
	if ctx == nil || source == nil {
		return Model{}, errors.New("invalid model import")
	}
	publicKey := m.config.Signers[manifest.KeyID]
	if publicKey == nil {
		return Model{}, errors.New("model signer is not approved")
	}
	if err := manifest.Verify(publicKey, m.config.Target, m.config.CurrentSecurityRevision, m.config.Now()); err != nil {
		return Model{}, fmt.Errorf("model manifest rejected: %w", err)
	}
	if manifest.SizeBytes > m.config.MaxTotalBytes {
		return Model{}, ErrCapacity
	}

	m.mu.Lock()
	if existing := m.entries[manifest.Digest]; existing != nil {
		m.touchLocked(existing)
		if existing.model.Artifact != manifest.Artifact ||
			existing.model.SizeBytes != manifest.SizeBytes ||
			existing.model.SecurityRevision != manifest.SecurityRevision {
			m.mu.Unlock()
			return Model{}, ErrConflict
		}
		switch existing.model.State {
		case StateVerifying:
			m.mu.Unlock()
			return Model{}, ErrBusy
		case StateReady, StateActive, StateDraining:
			result := existing.model
			m.mu.Unlock()
			return result, nil
		case StateFailed:
			if err := m.removeLocked(existing); err != nil {
				m.mu.Unlock()
				return Model{}, ErrCapacity
			}
		}
	}
	if err := m.ensureCapacityLocked(manifest.SizeBytes); err != nil {
		m.mu.Unlock()
		return Model{}, err
	}
	current := &entry{model: Model{
		Artifact:         manifest.Artifact,
		Digest:           manifest.Digest,
		SizeBytes:        manifest.SizeBytes,
		SecurityRevision: manifest.SecurityRevision,
		State:            StateVerifying,
	}}
	current.element = m.lru.PushBack(current)
	m.entries[manifest.Digest] = current
	m.reservedBytes += manifest.SizeBytes
	m.mu.Unlock()

	temp, err := os.CreateTemp(m.config.RootDir, artifactStagePrefix)
	if err != nil {
		return Model{}, m.failImport(current, "", "staging", err)
	}
	tempPath := temp.Name()
	metadataTempPath := ""
	success := false
	defer func() {
		_ = temp.Close()
		if !success {
			_ = os.Remove(tempPath)
			if metadataTempPath != "" {
				_ = os.Remove(metadataTempPath)
			}
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return Model{}, m.failImport(current, tempPath, "staging", err)
	}
	written, err := io.Copy(temp, io.LimitReader(&contextReader{ctx: ctx, reader: source}, int64(manifest.SizeBytes)+1))
	if err != nil {
		return Model{}, m.failImport(current, tempPath, errorCategory(err), err)
	}
	if written != int64(manifest.SizeBytes) {
		return Model{}, m.failImport(current, tempPath, "size", errors.New("model artifact size mismatch"))
	}
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return Model{}, m.failImport(current, tempPath, "staging", err)
	}
	if err := manifest.VerifyArtifact(temp); err != nil {
		return Model{}, m.failImport(current, tempPath, "integrity", err)
	}
	if err := temp.Sync(); err != nil {
		return Model{}, m.failImport(current, tempPath, "staging", err)
	}
	if err := temp.Close(); err != nil {
		return Model{}, m.failImport(current, tempPath, "staging", err)
	}
	acceptedAt := m.config.Now()
	if err := manifest.Verify(publicKey, m.config.Target, m.config.CurrentSecurityRevision, acceptedAt); err != nil {
		return Model{}, m.failImport(current, tempPath, "validity", err)
	}
	metadataTempPath, err = m.writeMetadataTemp(manifest, acceptedAt)
	if err != nil {
		return Model{}, m.failImport(current, tempPath, "staging", err)
	}
	finalPath := m.artifactPath(manifest.Digest)
	metadataPath := m.metadataPath(manifest.Digest)
	if err := m.activateFiles(tempPath, metadataTempPath, finalPath, metadataPath); err != nil {
		return Model{}, m.failImport(current, tempPath, "activation", err)
	}
	success = true

	m.mu.Lock()
	current.path = finalPath
	current.metadataPath = metadataPath
	current.model.State = StateReady
	current.model.ErrorCategory = ""
	m.reservedBytes -= manifest.SizeBytes
	m.totalBytes += manifest.SizeBytes
	m.touchLocked(current)
	result := current.model
	m.mu.Unlock()
	return result, nil
}

func (m *Manager) failImport(current *entry, _ string, category string, cause error) error {
	m.mu.Lock()
	if current.model.State == StateVerifying {
		m.reservedBytes -= current.model.SizeBytes
		current.model.State = StateFailed
		current.model.ErrorCategory = category
		m.touchLocked(current)
	}
	m.mu.Unlock()
	return cause
}

func (m *Manager) Activate(digest string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.entries[digest]
	if current == nil || (current.model.State != StateReady && current.model.State != StateDraining) {
		return ErrNotReady
	}
	current.model.State = StateActive
	m.touchLocked(current)
	return nil
}

func (m *Manager) Drain(digest string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.entries[digest]
	if current == nil || current.model.State != StateActive {
		return ErrNotReady
	}
	current.model.State = StateDraining
	m.touchLocked(current)
	return nil
}

func (m *Manager) SetPinned(digest string, pinned bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.entries[digest]
	if current == nil || current.model.State == StateFailed || current.model.State == StateVerifying {
		return ErrNotReady
	}
	current.model.Pinned = pinned
	m.touchLocked(current)
	return nil
}

func (m *Manager) Acquire(digest string) (Model, func(), error) {
	m.mu.Lock()
	current := m.entries[digest]
	if current == nil || (current.model.State != StateReady && current.model.State != StateActive &&
		current.model.State != StateDraining) {
		m.mu.Unlock()
		return Model{}, nil, ErrNotReady
	}
	current.model.InUse++
	m.touchLocked(current)
	result := current.model
	m.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.mu.Lock()
			if latest := m.entries[digest]; latest == current && latest.model.InUse > 0 {
				latest.model.InUse--
				m.touchLocked(latest)
			}
			m.mu.Unlock()
		})
	}
	return result, release, nil
}

func (m *Manager) Status(digest string) Model {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.entries[digest]; current != nil {
		return current.model
	}
	return Model{Digest: digest, State: StateAbsent}
}

func (m *Manager) List() []Model {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]Model, 0, len(m.entries))
	for element := m.lru.Front(); element != nil; element = element.Next() {
		result = append(result, element.Value.(*entry).model)
	}
	return result
}

func (m *Manager) ensureCapacityLocked(incoming uint64) error {
	for len(m.entries) >= m.config.MaxModels ||
		m.totalBytes+m.reservedBytes+incoming > m.config.MaxTotalBytes {
		var victim *entry
		for element := m.lru.Front(); element != nil; element = element.Next() {
			candidate := element.Value.(*entry)
			if !candidate.model.Pinned && candidate.model.InUse == 0 &&
				(candidate.model.State == StateReady || candidate.model.State == StateFailed) {
				victim = candidate
				break
			}
		}
		if victim == nil {
			return ErrCapacity
		}
		if err := m.removeLocked(victim); err != nil {
			return ErrCapacity
		}
	}
	return nil
}

func (m *Manager) removeLocked(current *entry) error {
	if current.metadataPath != "" {
		if err := os.Remove(current.metadataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if current.path != "" {
		if err := os.Remove(current.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		m.totalBytes -= current.model.SizeBytes
	}
	delete(m.entries, current.model.Digest)
	m.lru.Remove(current.element)
	return nil
}

func (m *Manager) touchLocked(current *entry) {
	m.lru.MoveToBack(current.element)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(value []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(value)
	}
}

func errorCategory(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled"
	}
	return "read"
}
