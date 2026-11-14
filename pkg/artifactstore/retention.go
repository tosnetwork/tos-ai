package artifactstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
)

// Retention binds stored objects to the job that produced them until a
// deadline. A record lives in retention/<job hex>.json. Sweep deletes an
// expired job in three durable steps: it renames the record to a tombstone
// (<job hex>.expired), removes the job's objects that no surviving record
// references, and only then removes the tombstone. A crash at any point
// leaves either a live record whose objects are all present or a tombstone
// the next sweep finishes; no surviving record ever names a deleted object.
//
// The store keeps objects that no record names: they may belong to a job
// whose retention is not recorded yet, so deleting them could break a
// result that is about to be bound.
const (
	retentionSchema = "tos.ai.artifact-retention.v1"
	// MaxRetentionRecords bounds the records a sweep reads, so one call has
	// bounded work and memory whatever the directory holds.
	MaxRetentionRecords = 65536
	// MaxSweepJobs bounds the expired jobs one sweep deletes.
	MaxSweepJobs = 1024
	// MaxObjectsPerJob bounds the objects one job may retain.
	MaxObjectsPerJob = 16

	maxRetentionRecordBytes = 4 << 10
	retentionLockName       = ".retention.lock"
	recordSuffix            = ".json"
	tombstoneSuffix         = ".expired"
	retentionTempPrefix     = ".retain-"
)

// ErrRetentionBusy reports that another process is retaining or sweeping
// this store; the caller retries later.
var ErrRetentionBusy = errors.New("artifact retention is held by another process")

type retentionRecord struct {
	Schema          string   `json:"schema"`
	Job             string   `json:"job"`
	RetainUntilUnix int64    `json:"retain_until_unix"`
	Objects         []string `json:"objects"`
}

func (r retentionRecord) valid() bool {
	if r.Schema != retentionSchema || !digestPattern.MatchString(r.Job) || r.RetainUntilUnix <= 0 ||
		len(r.Objects) == 0 || len(r.Objects) > MaxObjectsPerJob {
		return false
	}
	for index, object := range r.Objects {
		if !digestPattern.MatchString(object) || (index > 0 && r.Objects[index-1] >= object) {
			return false
		}
	}
	return true
}

// SweepReport counts what one sweep did.
type SweepReport struct {
	DeletedJobs    int `json:"deleted_jobs"`
	DeletedObjects int `json:"deleted_objects"`
	// KeptShared counts objects of deleted jobs kept because a surviving
	// record still references them.
	KeptShared int `json:"kept_shared"`
	// Deferred counts expired jobs left for a later sweep by the limit.
	Deferred int `json:"deferred"`
	// Live counts unexpired jobs.
	Live int `json:"live"`
}

// Retain records that job keeps objects until retainUntil (inclusive). The
// job and every object are sha256:<64 lowercase hex> digests, and every
// object must already be stored. Retaining a job again may add objects and
// extend its deadline but never shortens it, so a retried completion cannot
// expire a result early.
func (s *Store) Retain(ctx context.Context, job string, retainUntil time.Time, objects []string) error {
	if s == nil || s.root == "" || ctx == nil || !digestPattern.MatchString(job) || retainUntil.Unix() <= 0 ||
		len(objects) == 0 || len(objects) > MaxObjectsPerJob {
		return errors.New("invalid artifact retention request")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, object := range objects {
		if !digestPattern.MatchString(object) {
			return errors.New("invalid artifact retention object")
		}
	}
	lock, retention, err := s.lockRetention()
	if err != nil {
		return err
	}
	defer lock.Close()
	name := strings.TrimPrefix(job, "sha256:")
	if _, err := os.Lstat(filepath.Join(retention, name+tombstoneSuffix)); err == nil {
		return errors.New("artifact retention job is being deleted")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("stat artifact retention tombstone")
	}
	record := retentionRecord{Schema: retentionSchema, Job: job, RetainUntilUnix: retainUntil.Unix()}
	existing, err := readRetentionRecord(retention, name+recordSuffix)
	switch {
	case err == nil:
		record.RetainUntilUnix = max(record.RetainUntilUnix, existing.RetainUntilUnix)
		record.Objects = append(record.Objects, existing.Objects...)
	case errors.Is(err, os.ErrNotExist):
	default:
		return err
	}
	record.Objects = append(record.Objects, objects...)
	slices.Sort(record.Objects)
	record.Objects = slices.Compact(record.Objects)
	if !record.valid() {
		return errors.New("artifact retention record exceeds its bounds")
	}
	// Every retained object must be present now, under the lock that keeps
	// a sweep from deleting it, so a record never names a missing object.
	for _, object := range record.Objects {
		if err := s.requireObject(object); err != nil {
			return err
		}
	}
	if existing.valid() && existing.RetainUntilUnix == record.RetainUntilUnix && slices.Equal(existing.Objects, record.Objects) {
		return nil
	}
	return writeRetentionRecord(retention, name+recordSuffix, record)
}

// Sweep deletes at most limit jobs whose retention ended before now, and
// their objects that no other surviving job references. A malformed or
// unexpected entry stops the sweep before anything is deleted, because it
// might be the only reference keeping an object alive. Rerunning a sweep,
// including after a crash, finishes interrupted deletions and is otherwise
// a no-op.
func (s *Store) Sweep(ctx context.Context, now time.Time, limit int) (SweepReport, error) {
	if s == nil || s.root == "" || ctx == nil || now.Unix() <= 0 || limit <= 0 || limit > MaxSweepJobs {
		return SweepReport{}, errors.New("invalid artifact sweep request")
	}
	if err := ctx.Err(); err != nil {
		return SweepReport{}, err
	}
	lock, retention, err := s.lockRetention()
	if err != nil {
		return SweepReport{}, err
	}
	defer lock.Close()
	directory, err := os.Open(retention)
	if err != nil {
		return SweepReport{}, errors.New("open artifact retention directory")
	}
	entries, err := directory.ReadDir(MaxRetentionRecords + 1)
	_ = directory.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return SweepReport{}, errors.New("read artifact retention directory")
	}
	if len(entries) > MaxRetentionRecords {
		return SweepReport{}, errors.New("artifact retention directory exceeds its record bound")
	}
	var tombstones, expired, surviving []retentionRecord
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, retentionTempPrefix):
			// An interrupted write that never committed; no one else can
			// be writing while the lock is held.
			if err := removeRegular(filepath.Join(retention, name)); err != nil {
				return SweepReport{}, err
			}
		case strings.HasSuffix(name, tombstoneSuffix):
			record, err := readRetentionRecord(retention, name)
			if err != nil {
				return SweepReport{}, err
			}
			tombstones = append(tombstones, record)
		case strings.HasSuffix(name, recordSuffix):
			record, err := readRetentionRecord(retention, name)
			if err != nil {
				return SweepReport{}, err
			}
			if record.RetainUntilUnix < now.Unix() {
				expired = append(expired, record)
			} else {
				surviving = append(surviving, record)
			}
		default:
			return SweepReport{}, fmt.Errorf("unexpected entry %q in the artifact retention directory", name)
		}
	}
	// Records are read in directory order; sort so a bounded sweep picks
	// the oldest deadlines first and is reproducible.
	byDeadline := func(a, b retentionRecord) int {
		if c := compareInt64(a.RetainUntilUnix, b.RetainUntilUnix); c != 0 {
			return c
		}
		return strings.Compare(a.Job, b.Job)
	}
	slices.SortFunc(expired, byDeadline)
	slices.SortFunc(tombstones, byDeadline)
	report := SweepReport{Live: len(surviving)}
	// Interrupted deletions finish first; they count against the limit.
	if len(tombstones) > limit {
		report.Deferred = len(tombstones) - limit + len(expired)
		surviving = append(surviving, expired...)
		tombstones, expired = tombstones[:limit], nil
	} else if remaining := limit - len(tombstones); len(expired) > remaining {
		report.Deferred = len(expired) - remaining
		surviving = append(surviving, expired[remaining:]...)
		expired = expired[:remaining]
	}
	// An object stays while any record that is not deleted in this call
	// names it, including an expired record deferred to a later sweep.
	referenced := map[string]struct{}{}
	for _, record := range surviving {
		for _, object := range record.Objects {
			referenced[object] = struct{}{}
		}
	}
	for _, record := range expired {
		name := strings.TrimPrefix(record.Job, "sha256:")
		if err := os.Rename(filepath.Join(retention, name+recordSuffix), filepath.Join(retention, name+tombstoneSuffix)); err != nil {
			return report, errors.New("mark artifact retention record expired")
		}
		tombstones = append(tombstones, record)
	}
	if len(expired) > 0 {
		if err := syncDirectory(retention); err != nil {
			return report, err
		}
	}
	for _, record := range tombstones {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		for _, object := range record.Objects {
			if _, kept := referenced[object]; kept {
				report.KeptShared++
				continue
			}
			removed, err := s.removeObject(object)
			if err != nil {
				return report, err
			}
			if removed {
				report.DeletedObjects++
			}
		}
		// The objects' removal must be durable before the tombstone that
		// lets a rerun finish it disappears.
		if err := syncDirectory(s.objects); err != nil {
			return report, err
		}
		name := strings.TrimPrefix(record.Job, "sha256:")
		if err := removeRegular(filepath.Join(retention, name+tombstoneSuffix)); err != nil {
			return report, err
		}
		if err := syncDirectory(retention); err != nil {
			return report, err
		}
		report.DeletedJobs++
	}
	return report, nil
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// lockRetention takes the store's retention lock and returns the private
// retention directory, creating it on first use.
func (s *Store) lockRetention() (*dirlock.Lock, string, error) {
	if err := requirePrivateDirectory(s.root); err != nil {
		return nil, "", err
	}
	lock, err := dirlock.Acquire(s.root, retentionLockName)
	if err != nil {
		return nil, "", ErrRetentionBusy
	}
	retention := filepath.Join(s.root, "retention")
	if err := os.Mkdir(retention, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		_ = lock.Close()
		return nil, "", errors.New("create artifact retention directory")
	}
	if err := requirePrivateDirectory(retention); err != nil {
		_ = lock.Close()
		return nil, "", err
	}
	return lock, retention, nil
}

// requireObject checks that an object is a stored, read-only regular file
// without reading it.
func (s *Store) requireObject(digest string) error {
	info, err := os.Lstat(s.path(digest))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 || info.Size() < 0 || uint64(info.Size()) > s.maximum {
		return fmt.Errorf("artifact object %s is not stored", digest)
	}
	return nil
}

// removeObject deletes one object. An object already gone is not an error,
// so an interrupted sweep can be rerun; anything other than a regular file
// is refused rather than followed or removed.
func (s *Store) removeObject(digest string) (bool, error) {
	if !digestPattern.MatchString(digest) {
		return false, errors.New("invalid artifact object digest")
	}
	path := s.path(digest)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.New("stat artifact object")
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("artifact object %s is not a regular file; refusing to delete it", digest)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, errors.New("delete artifact object")
	}
	return true, nil
}

func removeRegular(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("artifact retention entry %s is not a regular file", filepath.Base(path))
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("remove artifact retention entry")
	}
	return nil
}

func readRetentionRecord(directory, name string) (retentionRecord, error) {
	path := filepath.Join(directory, name)
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return retentionRecord{}, err
		}
		return retentionRecord{}, errors.New("stat artifact retention record")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > maxRetentionRecordBytes {
		return retentionRecord{}, fmt.Errorf("artifact retention record %s is not an owner-only bounded regular file", name)
	}
	file, err := os.Open(path)
	if err != nil {
		return retentionRecord{}, errors.New("open artifact retention record")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return retentionRecord{}, errors.New("artifact retention record changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxRetentionRecordBytes+1))
	if err != nil || len(raw) > maxRetentionRecordBytes {
		return retentionRecord{}, errors.New("read artifact retention record")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var record retentionRecord
	if err := decoder.Decode(&record); err != nil {
		return retentionRecord{}, fmt.Errorf("artifact retention record %s does not decode", name)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return retentionRecord{}, fmt.Errorf("artifact retention record %s has trailing data", name)
	}
	stem := strings.TrimSuffix(strings.TrimSuffix(name, recordSuffix), tombstoneSuffix)
	if !record.valid() || "sha256:"+stem != record.Job {
		return retentionRecord{}, fmt.Errorf("artifact retention record %s is invalid", name)
	}
	return record, nil
}

func writeRetentionRecord(directory, name string, record retentionRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return errors.New("encode artifact retention record")
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxRetentionRecordBytes {
		return errors.New("artifact retention record exceeds its size bound")
	}
	temporary, err := os.CreateTemp(directory, retentionTempPrefix)
	if err != nil {
		return errors.New("create artifact retention record")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errors.New("protect artifact retention record")
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return errors.New("write artifact retention record")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errors.New("sync artifact retention record")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("close artifact retention record")
	}
	if err := os.Rename(temporaryName, filepath.Join(directory, name)); err != nil {
		return errors.New("commit artifact retention record")
	}
	return syncDirectory(directory)
}
