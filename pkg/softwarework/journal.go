package softwarework

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
)

var executionIDPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type journalRecord struct {
	Schema      string  `json:"schema"`
	Fingerprint string  `json:"fingerprint"`
	State       string  `json:"state"`
	Outcome     Outcome `json:"outcome,omitempty"`
}

type Journal struct {
	root  string
	lock  *dirlock.Lock
	mutex sync.Mutex
}

func OpenJournal(root string) (*Journal, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("invalid software-work journal root")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, errors.New("create software-work journal root")
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("software-work journal root must be private")
	}
	ownership, err := dirlock.Acquire(root, ".software-work-journal.lock")
	if err != nil {
		return nil, errors.New("software-work journal is already owned")
	}
	return &Journal{root: root, lock: ownership}, nil
}

func (j *Journal) Claim(executionID, fingerprint string) (bool, journalRecord, error) {
	if j == nil || j.lock == nil || !executionIDPattern.MatchString(executionID) || !executionIDPattern.MatchString(fingerprint) {
		return false, journalRecord{}, errors.New("invalid software-work journal claim")
	}
	j.mutex.Lock()
	defer j.mutex.Unlock()
	path := j.path(executionID)
	record := journalRecord{Schema: "tos.service.software-work-journal.v1", Fingerprint: fingerprint, State: "running"}
	encoded, err := json.Marshal(record)
	if err != nil {
		return false, journalRecord{}, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if _, writeErr := file.Write(append(encoded, '\n')); writeErr != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return false, journalRecord{}, errors.New("write software-work claim")
		}
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return false, journalRecord{}, errors.New("sync software-work claim")
		}
		if closeErr := file.Close(); closeErr != nil {
			_ = os.Remove(path)
			return false, journalRecord{}, errors.New("close software-work claim")
		}
		if err := syncDirectory(j.root); err != nil {
			_ = os.Remove(path)
			return false, journalRecord{}, err
		}
		return true, record, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return false, journalRecord{}, errors.New("create software-work claim")
	}
	existing, err := readJournalRecord(path)
	if err != nil {
		return false, journalRecord{}, err
	}
	if existing.Fingerprint != fingerprint {
		return false, journalRecord{}, errors.New("execution identity conflicts with another request")
	}
	return false, existing, nil
}

func (j *Journal) Complete(executionID, fingerprint string, outcome Outcome) error {
	if j == nil || j.lock == nil {
		return errors.New("invalid software-work journal completion")
	}
	j.mutex.Lock()
	defer j.mutex.Unlock()
	path := j.path(executionID)
	existing, err := readJournalRecord(path)
	if err != nil || existing.Fingerprint != fingerprint || existing.State != "running" {
		return errors.New("software-work claim cannot be completed")
	}
	record := journalRecord{Schema: existing.Schema, Fingerprint: fingerprint, State: "complete", Outcome: outcome}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(j.root, ".complete-")
	if err != nil {
		return errors.New("create software-work completion")
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errors.New("protect software-work completion")
	}
	if _, err := temporary.Write(append(encoded, '\n')); err != nil {
		_ = temporary.Close()
		return errors.New("write software-work completion")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errors.New("sync software-work completion")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("close software-work completion")
	}
	if err := os.Rename(name, path); err != nil {
		return errors.New("commit software-work completion")
	}
	return syncDirectory(j.root)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("open software-work journal directory")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("sync software-work journal directory")
	}
	return nil
}

func (j *Journal) Close() error {
	if j == nil || j.lock == nil {
		return nil
	}
	return j.lock.Close()
}

func (j *Journal) path(executionID string) string {
	return filepath.Join(j.root, executionID[len("sha256:"):]+".json")
}

func readJournalRecord(path string) (journalRecord, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > 32<<10 {
		return journalRecord{}, errors.New("invalid software-work journal record")
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return journalRecord{}, errors.New("read software-work journal record")
	}
	var record journalRecord
	if err := json.Unmarshal(value, &record); err != nil || record.Schema != "tos.service.software-work-journal.v1" || !executionIDPattern.MatchString(record.Fingerprint) || (record.State != "running" && record.State != "complete") {
		return journalRecord{}, errors.New("invalid software-work journal record")
	}
	return record, nil
}
