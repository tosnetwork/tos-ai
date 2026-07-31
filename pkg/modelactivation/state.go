package modelactivation

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
)

const (
	activationStateVersion      = 1
	activationStateFile         = "activation-state.json"
	activationStateStagePrefix  = ".activation-state-"
	activationStateLockFile     = ".activation-owner.lock"
	MaxActivationStateBytesHard = 64 << 10
	MaxActivationStateFilesHard = 128
)

type persistedActivationState struct {
	Version    uint8                 `json:"version"`
	Generation uint64                `json:"generation"`
	Slots      []persistedActiveSlot `json:"slots"`
}

type persistedActiveSlot struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

type activationStateStore struct {
	closeMu    sync.Mutex
	dir        string
	generation uint64
	writeState func([]byte) error
	ownership  *dirlock.Lock
	closed     bool
}

func openActivationStateStore(
	dir string,
) (*activationStateStore, []persistedActiveSlot, error) {
	if dir == "" || !filepath.IsAbs(dir) {
		return nil, nil, errors.New("invalid activation state directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 ||
		info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errors.New("activation state directory is not private")
	}
	ownership, err := dirlock.Acquire(dir, activationStateLockFile)
	if err != nil {
		return nil, nil, errors.New("activation state is already managed")
	}
	store := &activationStateStore{dir: dir, ownership: ownership}
	store.writeState = store.writeAtomic
	state, exists, err := store.read()
	if err != nil {
		_ = ownership.Close()
		return nil, nil, err
	}
	if !exists {
		return store, nil, nil
	}
	store.generation = state.Generation
	return store, state.Slots, nil
}

func (s *activationStateStore) read() (persistedActivationState, bool, error) {
	directory, err := os.Open(s.dir)
	if err != nil {
		return persistedActivationState{}, false, err
	}
	entries, readErr := directory.Readdir(MaxActivationStateFilesHard + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return persistedActivationState{}, false, readErr
	}
	if closeErr != nil {
		return persistedActivationState{}, false, closeErr
	}
	if len(entries) > MaxActivationStateFilesHard {
		return persistedActivationState{}, false, errors.New("activation state directory limit")
	}

	found := false
	cleaned := false
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case name == activationStateLockFile:
			continue
		case name == activationStateFile:
			if found {
				return persistedActivationState{}, false, errors.New("duplicate activation state")
			}
			found = true
		case len(name) > len(activationStateStagePrefix) &&
			name[:len(activationStateStagePrefix)] == activationStateStagePrefix:
			path := filepath.Join(s.dir, name)
			info, statErr := os.Lstat(path)
			if statErr != nil {
				return persistedActivationState{}, false, statErr
			}
			if info.IsDir() {
				return persistedActivationState{}, false, errors.New("invalid activation staging entry")
			}
			if err := os.Remove(path); err != nil {
				return persistedActivationState{}, false, err
			}
			cleaned = true
		default:
			return persistedActivationState{}, false, errors.New("unknown activation state entry")
		}
	}
	if cleaned {
		if err := syncActivationDirectory(s.dir); err != nil {
			return persistedActivationState{}, false, err
		}
	}
	if !found {
		return persistedActivationState{}, false, nil
	}

	path := filepath.Join(s.dir, activationStateFile)
	pathInfo, err := os.Lstat(path)
	if err != nil || !privateActivationFile(pathInfo) {
		return persistedActivationState{}, false, errors.New("invalid activation state file")
	}
	file, err := os.Open(path)
	if err != nil {
		return persistedActivationState{}, false, err
	}
	info, statErr := file.Stat()
	if statErr != nil || !os.SameFile(pathInfo, info) || !privateActivationFile(info) {
		_ = file.Close()
		return persistedActivationState{}, false, errors.New("substituted activation state file")
	}
	data, readErr := io.ReadAll(io.LimitReader(file, MaxActivationStateBytesHard+1))
	closeErr = file.Close()
	if readErr != nil {
		return persistedActivationState{}, false, readErr
	}
	if closeErr != nil {
		return persistedActivationState{}, false, closeErr
	}
	if len(data) == 0 || len(data) > MaxActivationStateBytesHard {
		return persistedActivationState{}, false, errors.New("activation state size limit")
	}

	var state persistedActivationState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return persistedActivationState{}, false, errors.New("invalid activation state")
	}
	if err := ensureActivationJSONEOF(decoder); err != nil {
		return persistedActivationState{}, false, err
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(data, canonical) {
		return persistedActivationState{}, false, errors.New("non-canonical activation state")
	}
	if err := validatePersistedActivationState(state); err != nil {
		return persistedActivationState{}, false, err
	}
	return state, true, nil
}

func (s *activationStateStore) save(slots []persistedActiveSlot) error {
	if s == nil {
		return errors.New("activation state store is closed")
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return errors.New("activation state store is closed")
	}
	if s.generation == math.MaxUint64 {
		return errors.New("activation state generation limit")
	}
	state := persistedActivationState{
		Version: activationStateVersion, Generation: s.generation + 1,
		Slots: append([]persistedActiveSlot(nil), slots...),
	}
	sort.Slice(state.Slots, func(i, j int) bool {
		return state.Slots[i].ID < state.Slots[j].ID
	})
	if err := validatePersistedActivationState(state); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil || len(data) > MaxActivationStateBytesHard {
		return errors.New("activation state size limit")
	}
	if err := s.writeState(data); err != nil {
		return err
	}
	s.generation = state.Generation
	return nil
}

func (s *activationStateStore) close() error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	ownership := s.ownership
	s.ownership = nil
	if ownership == nil {
		return errors.New("invalid activation state ownership")
	}
	if err := ownership.Close(); err != nil {
		return errors.New("release activation state ownership")
	}
	return nil
}

func (s *activationStateStore) writeAtomic(data []byte) (resultErr error) {
	temp, err := os.CreateTemp(s.dir, activationStateStagePrefix)
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		if resultErr != nil {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temp.Write(data); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	finalPath := filepath.Join(s.dir, activationStateFile)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return err
	}
	if err := syncActivationDirectory(s.dir); err != nil {
		return err
	}
	return nil
}

func validatePersistedActivationState(state persistedActivationState) error {
	if state.Version != activationStateVersion || state.Generation == 0 ||
		len(state.Slots) > MaxSlotsHard {
		return errors.New("invalid activation state")
	}
	previous := ""
	for _, slot := range state.Slots {
		if !validIdentity(slot.ID) || !validDigest(slot.Digest) ||
			(previous != "" && slot.ID <= previous) {
			return errors.New("invalid activation slot state")
		}
		previous = slot.ID
	}
	return nil
}

func ensureActivationJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("invalid trailing activation state")
	}
	return nil
}

func privateActivationFile(info os.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode().Perm() == 0o600
}

func syncActivationDirectory(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
