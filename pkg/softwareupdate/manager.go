// Package softwareupdate manages two crash-safe software release slots. It
// verifies signed update manifests from pkg/update and never downloads an
// artifact or restarts a process; those operator-owned actions remain outside
// this local state machine.
package softwareupdate

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
	"github.com/tosnetwork/tos-ai/pkg/update"
)

const (
	stateVersion     = 1
	stateName        = "state.json"
	lockName         = ".software-update.lock"
	artifactName     = "artifact.bin"
	manifestName     = "manifest.json"
	maxStateBytes    = 4 << 10
	maxManifestBytes = 16 << 10
)

var validSlots = map[string]bool{"a": true, "b": true}

type Config struct {
	Root       string
	Target     string
	PublicKeys map[string]ed25519.PublicKey
}

type Status struct {
	ActiveSlot       string
	KnownGoodSlot    string
	PendingSlot      string
	AwaitingHealth   bool
	BootAttempted    bool
	SecurityRevision uint64
}

type persistentState struct {
	Version          int    `json:"version"`
	Active           string `json:"active,omitempty"`
	KnownGood        string `json:"knownGood,omitempty"`
	Pending          string `json:"pending,omitempty"`
	Previous         string `json:"previous,omitempty"`
	AwaitingHealth   bool   `json:"awaitingHealth,omitempty"`
	BootAttempted    bool   `json:"bootAttempted,omitempty"`
	SecurityRevision uint64 `json:"securityRevision"`
}

type Manager struct {
	root       string
	target     string
	publicKeys map[string]ed25519.PublicKey
	ownership  *dirlock.Lock
	writeFile  func(string, []byte, os.FileMode) error

	mu        sync.Mutex
	state     persistentState
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func Open(config Config) (*Manager, error) {
	if !filepath.IsAbs(config.Root) || filepath.Clean(config.Root) != config.Root ||
		config.Target == "" || len(config.Target) > 256 || len(config.PublicKeys) == 0 {
		return nil, errors.New("invalid software update configuration")
	}
	keys := make(map[string]ed25519.PublicKey, len(config.PublicKeys))
	for keyID, key := range config.PublicKeys {
		if keyID == "" || len(keyID) > 512 || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("invalid software update trust root")
		}
		keys[keyID] = append(ed25519.PublicKey(nil), key...)
	}
	if err := os.MkdirAll(config.Root, 0o700); err != nil {
		return nil, errors.New("create software update root")
	}
	if err := requirePrivateDirectory(config.Root); err != nil {
		return nil, err
	}
	// Ownership must precede residue cleanup. Otherwise a second opener could
	// remove the first owner's in-progress staging files before failing its
	// eventual lock attempt.
	ownership, err := dirlock.Acquire(config.Root, lockName)
	if err != nil {
		return nil, errors.New("software update root is already owned")
	}
	opened := false
	defer func() {
		if !opened {
			_ = ownership.Close()
		}
	}()
	_ = os.Remove(filepath.Join(config.Root, stateName+".tmp"))
	for _, slot := range []string{"a", "b"} {
		if err := os.MkdirAll(filepath.Join(config.Root, slot), 0o700); err != nil {
			return nil, errors.New("create software update slot")
		}
		if err := requirePrivateDirectory(filepath.Join(config.Root, slot)); err != nil {
			return nil, err
		}
		cleanInterruptedFiles(filepath.Join(config.Root, slot))
	}
	manager := &Manager{
		root: config.Root, target: config.Target,
		publicKeys: keys, ownership: ownership,
		state: persistentState{Version: stateVersion}, writeFile: writeAtomic,
	}
	if err := manager.loadAndRecover(); err != nil {
		return nil, err
	}
	opened = true
	return manager, nil
}

func (m *Manager) Stage(
	ctx context.Context,
	manifest update.Manifest,
	artifact io.Reader,
	now time.Time,
) (string, error) {
	if ctx == nil || artifact == nil {
		return "", errors.New("invalid software update stage request")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return "", errors.New("software update manager is closed")
	}
	if m.state.Pending != "" || m.state.AwaitingHealth {
		return "", errors.New("software update transition already pending")
	}
	key := m.publicKeys[manifest.KeyID]
	if len(key) != ed25519.PublicKeySize {
		return "", errors.New("software update signing key is not trusted")
	}
	if err := manifest.Verify(key, m.target, m.state.SecurityRevision, now.UTC()); err != nil {
		return "", fmt.Errorf("verify software update manifest: %w", err)
	}
	slot := "a"
	if m.state.Active == "a" {
		slot = "b"
	}
	slotDirectory := filepath.Join(m.root, slot)
	cleanInterruptedFiles(slotDirectory)
	temporaryArtifact := filepath.Join(slotDirectory, artifactName+".tmp")
	file, err := os.OpenFile(temporaryArtifact, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", errors.New("create staged software artifact")
	}
	verifiedReader := &contextReader{ctx: ctx, reader: io.TeeReader(artifact, file)}
	verifyErr := manifest.VerifyArtifact(verifiedReader)
	syncErr := file.Sync()
	closeErr := file.Close()
	if verifyErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(temporaryArtifact)
		return "", errors.Join(verifyErr, syncErr, closeErr)
	}
	if err := ctx.Err(); err != nil {
		_ = os.Remove(temporaryArtifact)
		return "", err
	}
	artifactPath := filepath.Join(slotDirectory, artifactName)
	if err := os.Rename(temporaryArtifact, artifactPath); err != nil {
		_ = os.Remove(temporaryArtifact)
		return "", errors.New("commit staged software artifact")
	}
	if err := os.Chmod(artifactPath, 0o500); err != nil {
		return "", errors.New("protect staged software artifact")
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return "", errors.New("encode staged software manifest")
	}
	if err := m.writeFile(filepath.Join(slotDirectory, manifestName), manifestJSON, 0o600); err != nil {
		return "", err
	}
	if err := syncDirectory(slotDirectory); err != nil {
		return "", err
	}
	next := m.state
	next.Pending = slot
	if err := m.storeState(next); err != nil {
		return "", err
	}
	m.state = next
	return slot, nil
}

func (m *Manager) ActivatePending() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !validSlots[m.state.Pending] || m.state.AwaitingHealth {
		return errors.New("no software update is ready for activation")
	}
	next := m.state
	next.Previous = next.Active
	next.Active = next.Pending
	next.Pending = ""
	next.AwaitingHealth = true
	next.BootAttempted = false
	if err := m.storeState(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Manager) ConfirmHealthy() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !m.state.AwaitingHealth || !m.state.BootAttempted ||
		!validSlots[m.state.Active] {
		return errors.New("no activated software update awaits health confirmation")
	}
	manifest, err := m.readManifest(m.state.Active)
	if err != nil {
		return err
	}
	next := m.state
	next.KnownGood = next.Active
	next.Previous = ""
	next.AwaitingHealth = false
	next.BootAttempted = false
	if manifest.SecurityRevision > next.SecurityRevision {
		next.SecurityRevision = manifest.SecurityRevision
	}
	if err := m.storeState(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Manager) Rollback() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !validSlots[m.state.KnownGood] ||
		(!m.state.AwaitingHealth && m.state.Active == m.state.KnownGood) {
		return errors.New("no known-good software rollback is available")
	}
	next := m.state
	next.Active = next.KnownGood
	next.Pending = ""
	next.Previous = ""
	next.AwaitingHealth = false
	next.BootAttempted = false
	if err := m.storeState(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Manager) Status() (Status, error) {
	if m == nil {
		return Status{}, errors.New("invalid software update manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Status{}, errors.New("software update manager is closed")
	}
	return Status{
		ActiveSlot: m.state.Active, KnownGoodSlot: m.state.KnownGood,
		PendingSlot: m.state.Pending, AwaitingHealth: m.state.AwaitingHealth,
		BootAttempted:    m.state.BootAttempted,
		SecurityRevision: m.state.SecurityRevision,
	}, nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		if m.ownership != nil {
			m.closeErr = m.ownership.Close()
		}
	})
	return m.closeErr
}

func (m *Manager) loadAndRecover() error {
	statePath := filepath.Join(m.root, stateName)
	data, err := readPrivateFile(statePath, maxStateBytes, 0o600)
	if errors.Is(err, os.ErrNotExist) {
		return m.storeState(m.state)
	}
	if err != nil {
		return errors.New("read software update state")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state persistentState
	if err := decoder.Decode(&state); err != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		validateState(state) != nil {
		return errors.New("invalid software update state")
	}
	m.state = state
	for _, slot := range uniqueReferencedSlots(state) {
		if err := m.verifyInstalledSlot(slot, state.SecurityRevision); err != nil {
			return err
		}
	}
	if state.AwaitingHealth && !state.BootAttempted {
		// The first opener after activation is the candidate boot. Persist the
		// attempt before returning so a crash or power loss can be distinguished
		// from the intentional activation restart.
		state.BootAttempted = true
		if err := m.storeState(state); err != nil {
			return err
		}
		m.state = state
	} else if state.AwaitingHealth {
		// A second opener proves that the candidate process exited without
		// confirming health. Return to the last known-good slot.
		if validSlots[state.KnownGood] {
			state.Active = state.KnownGood
		} else {
			state.Active = ""
		}
		state.Previous = ""
		state.AwaitingHealth = false
		state.BootAttempted = false
		if err := m.storeState(state); err != nil {
			return err
		}
		m.state = state
	}
	return nil
}

func (m *Manager) verifyInstalledSlot(slot string, revision uint64) error {
	manifest, err := m.readManifest(slot)
	if err != nil {
		return err
	}
	key := m.publicKeys[manifest.KeyID]
	if err := manifest.VerifyInstalled(key, m.target, revision); err != nil {
		return errors.New("installed software manifest failed verification")
	}
	artifactPath := filepath.Join(m.root, slot, artifactName)
	artifactLinkInfo, err := os.Lstat(artifactPath)
	if err != nil || artifactLinkInfo.Mode()&os.ModeSymlink != 0 ||
		!artifactLinkInfo.Mode().IsRegular() {
		return errors.New("invalid installed software artifact")
	}
	artifact, err := os.Open(artifactPath)
	if err != nil {
		return errors.New("open installed software artifact")
	}
	defer artifact.Close()
	artifactInfo, err := artifact.Stat()
	if err != nil || !artifactInfo.Mode().IsRegular() ||
		artifactInfo.Mode().Perm() != 0o500 || !ownedByCurrentUser(artifactInfo) ||
		uint64(artifactInfo.Size()) != manifest.SizeBytes {
		return errors.New("invalid installed software artifact")
	}
	if err := manifest.VerifyArtifact(artifact); err != nil {
		return errors.New("installed software artifact failed verification")
	}
	return nil
}

func (m *Manager) readManifest(slot string) (update.Manifest, error) {
	if !validSlots[slot] {
		return update.Manifest{}, errors.New("invalid software update slot")
	}
	data, err := readPrivateFile(
		filepath.Join(m.root, slot, manifestName), maxManifestBytes, 0o600,
	)
	if err != nil {
		return update.Manifest{}, errors.New("read installed software manifest")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest update.Manifest
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return update.Manifest{}, errors.New("decode installed software manifest")
	}
	return manifest, nil
}

func (m *Manager) storeState(state persistentState) error {
	if err := validateState(state); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return errors.New("encode software update state")
	}
	if m.writeFile == nil {
		return errors.New("software update persistence is unavailable")
	}
	if err := m.writeFile(filepath.Join(m.root, stateName), data, 0o600); err != nil {
		return err
	}
	return syncDirectory(m.root)
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

func validateState(state persistentState) error {
	if state.Version != stateVersion {
		return errors.New("invalid software update state version")
	}
	for _, slot := range []string{state.Active, state.KnownGood, state.Pending, state.Previous} {
		if slot != "" && !validSlots[slot] {
			return errors.New("invalid software update slot state")
		}
	}
	if state.Pending != "" && state.Pending == state.Active ||
		state.KnownGood != "" && state.Active == "" ||
		state.AwaitingHealth && state.Active == "" ||
		!state.AwaitingHealth && (state.Previous != "" || state.BootAttempted) {
		return errors.New("inconsistent software update transition")
	}
	return nil
}

func uniqueReferencedSlots(state persistentState) []string {
	seen := make(map[string]bool, 2)
	result := make([]string, 0, 2)
	for _, slot := range []string{state.Active, state.KnownGood, state.Pending, state.Previous} {
		if validSlots[slot] && !seen[slot] {
			seen[slot] = true
			result = append(result, slot)
		}
	}
	return result
}

func requirePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 ||
		info.Mode()&os.ModeSymlink != 0 || !ownedByCurrentUser(info) {
		return errors.New("software update directory is not private")
	}
	return nil
}

func readPrivateFile(path string, maximum int64, mode os.FileMode) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != mode ||
		!ownedByCurrentUser(info) || info.Size() < 0 || info.Size() > maximum {
		return nil, errors.New("invalid private software update file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(data)) > maximum {
		return nil, errors.New("read bounded software update file")
	}
	return data, nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func cleanInterruptedFiles(directory string) {
	_ = os.Remove(filepath.Join(directory, artifactName+".tmp"))
	_ = os.Remove(filepath.Join(directory, manifestName+".tmp"))
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return errors.New("create atomic software update file")
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(temporary)
		return errors.Join(writeErr, syncErr, closeErr)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return errors.New("commit atomic software update file")
	}
	if err := os.Chmod(path, mode); err != nil {
		return errors.New("protect software update file")
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("open software update directory")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("sync software update directory")
	}
	return nil
}
