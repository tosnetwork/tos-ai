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
// deadline. A job with a deadline has a record in retention/<job hex>.json.
// Sweep deletes an expired job in three durable steps: it renames the record
// to a tombstone (<job hex>.expired), removes the job's objects that no
// surviving record references, and only then removes the tombstone. A crash
// at any point leaves either a live record whose objects are all present or
// a tombstone the next sweep finishes; no surviving record ever names a
// deleted object.
//
// Objects are deduplicated by content, so one object may belong to several
// jobs. Every commit binds the object under the retention lock: PutFor adds
// it to its job's pending record (<job hex>.pending) until Retain gives the
// job a deadline, and Put leaves a permanent hold (<object hex>.hold). A
// sweep keeps any object a pending, unexpired or deferred record or a hold
// names, so it never deletes an object a job that is still running or
// retained depends on.
const (
	retentionSchema = "tos.ai.artifact-retention.v1"
	// MaxRetentionRecords bounds the deadline-bearing job records and
	// tombstones a store keeps. Giving a new job a deadline is refused at the
	// bound (jobs that already have one may still add objects and be
	// extended), and once expired jobs are swept new ones are admitted again.
	// Pending records and holds are references that must never be broken, so
	// they are never refused and do not count; a store whose jobs never get a
	// deadline grows the way an unswept store always did.
	MaxRetentionRecords = 65536
	// MaxSweepJobs bounds the expired jobs one sweep deletes.
	MaxSweepJobs = 1024
	// MaxObjectsPerJob bounds the objects one job may retain.
	MaxObjectsPerJob = 16

	maxRetentionRecordBytes = 4 << 10
	retentionLockName       = ".retention.lock"
	recordSuffix            = ".json"
	pendingSuffix           = ".pending"
	tombstoneSuffix         = ".expired"
	holdSuffix              = ".hold"
	retentionTempPrefix     = ".retain-"
	retentionRetryInterval  = 20 * time.Millisecond

	// DefaultRetentionWait bounds how long a commit, Retain or Sweep waits
	// for another process to release the retention lock. A sweep's work is
	// bounded per call, so a waiter normally gets the lock well within it.
	DefaultRetentionWait = 60 * time.Second
)

// ErrRetentionBusy reports that another process held the retention lock for
// longer than the bounded wait; the caller retries later.
var ErrRetentionBusy = errors.New("artifact retention is held by another process")

// ErrRetentionFull reports that the store holds MaxRetentionRecords
// deadline-bearing records and tombstones; a sweep of expired jobs makes
// room.
var ErrRetentionFull = errors.New("artifact retention is at its record bound; sweep expired jobs")

// retentionRecord binds objects to a job. A pending record has no deadline
// yet and keeps its objects until Retain sets one.
type retentionRecord struct {
	Schema          string   `json:"schema"`
	Job             string   `json:"job"`
	Pending         bool     `json:"pending"`
	RetainUntilUnix int64    `json:"retain_until_unix"`
	Objects         []string `json:"objects"`
}

func (r retentionRecord) valid() bool {
	if r.Schema != retentionSchema || !digestPattern.MatchString(r.Job) || r.Pending != (r.RetainUntilUnix == 0) ||
		r.RetainUntilUnix < 0 || len(r.Objects) == 0 || len(r.Objects) > MaxObjectsPerJob {
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
	// Live counts retained jobs whose deadline has not passed.
	Live int `json:"live"`
	// Pending counts jobs without a deadline yet; they keep their objects.
	Pending int `json:"pending"`
	// Held counts objects kept by a permanent hold.
	Held int `json:"held"`
}

// Retain records that job keeps objects until retainUntil (inclusive),
// turning a pending record final. The job and every object are
// sha256:<64 lowercase hex> digests, and every object must already be
// stored. Retaining a job again may add objects and extend its deadline but
// never shortens it, so a retried completion cannot expire a result early.
// It waits a bounded time for a concurrent sweep to finish.
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
	lock, retention, err := s.acquireRetention(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	return s.bindLocked(retention, job, retainUntil.Unix(), objects)
}

// bindLocked adds objects to job's record while the retention lock is held.
// retainUntil zero keeps the job's current state (a job without a deadline
// stays pending); a positive value gives a pending job its deadline or
// extends an existing one, never shortening it. Only a job's first deadline
// creates a new deadline-bearing record, so only that counts against the
// record bound.
func (s *Store) bindLocked(retention, job string, retainUntil int64, objects []string) error {
	name := strings.TrimPrefix(job, "sha256:")
	if _, err := os.Lstat(filepath.Join(retention, name+tombstoneSuffix)); err == nil {
		return errors.New("artifact retention job is being deleted")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("stat artifact retention tombstone")
	}
	final, finalErr := readRetentionRecord(retention, name+recordSuffix)
	if finalErr != nil && !errors.Is(finalErr, os.ErrNotExist) {
		return finalErr
	}
	pending, pendingErr := readRetentionRecord(retention, name+pendingSuffix)
	if pendingErr != nil && !errors.Is(pendingErr, os.ErrNotExist) {
		return pendingErr
	}
	hasFinal, hasPending := finalErr == nil, pendingErr == nil
	record := retentionRecord{Schema: retentionSchema, Job: job}
	switch {
	case hasFinal:
		record.RetainUntilUnix = max(retainUntil, final.RetainUntilUnix)
		record.Objects = append(record.Objects, final.Objects...)
	case retainUntil == 0:
		record.Pending = true
	default:
		if err := s.requireRoomLocked(retention, 1); err != nil {
			return err
		}
		record.RetainUntilUnix = retainUntil
	}
	if hasPending {
		record.Objects = append(record.Objects, pending.Objects...)
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
	if record.Pending {
		if hasPending && slices.Equal(pending.Objects, record.Objects) {
			return nil
		}
		return writeRetentionRecord(retention, name+pendingSuffix, record)
	}
	if hasFinal && !hasPending && final.RetainUntilUnix == record.RetainUntilUnix && slices.Equal(final.Objects, record.Objects) {
		return nil
	}
	if err := writeRetentionRecord(retention, name+recordSuffix, record); err != nil {
		return err
	}
	if !hasPending {
		return nil
	}
	// The deadline-bearing record now names every pending object, so the
	// pending record is redundant. A crash before this removal leaves both;
	// the next bind or sweep removes the pending one.
	if err := removeRegular(filepath.Join(retention, name+pendingSuffix)); err != nil {
		return err
	}
	return syncDirectory(retention)
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
	lock, retention, err := s.acquireRetention(ctx)
	if err != nil {
		return SweepReport{}, err
	}
	defer lock.Close()
	// Every entry is read whatever the directory holds: refusing a large
	// store would leave it unsweepable. Creation is capped at the record
	// bound, so this work stays bounded in normal operation.
	entries, err := readRetentionEntries(retention)
	if err != nil {
		return SweepReport{}, err
	}
	var tombstones, expired, surviving, pendingRecords []retentionRecord
	held := map[string]struct{}{}
	deadlineBearing := map[string][]string{}
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, retentionTempPrefix):
			// An interrupted write that never committed; no one else can
			// be writing while the lock is held.
			if err := removeRegular(filepath.Join(retention, name)); err != nil {
				return SweepReport{}, err
			}
		case strings.HasSuffix(name, holdSuffix):
			digest, err := readHold(retention, name)
			if err != nil {
				return SweepReport{}, err
			}
			held[digest] = struct{}{}
		case strings.HasSuffix(name, tombstoneSuffix):
			record, err := readRetentionRecord(retention, name)
			if err != nil {
				return SweepReport{}, err
			}
			tombstones = append(tombstones, record)
			deadlineBearing[record.Job] = record.Objects
		case strings.HasSuffix(name, pendingSuffix):
			record, err := readRetentionRecord(retention, name)
			if err != nil {
				return SweepReport{}, err
			}
			pendingRecords = append(pendingRecords, record)
		case strings.HasSuffix(name, recordSuffix):
			record, err := readRetentionRecord(retention, name)
			if err != nil {
				return SweepReport{}, err
			}
			deadlineBearing[record.Job] = record.Objects
			if record.RetainUntilUnix < now.Unix() {
				expired = append(expired, record)
			} else {
				surviving = append(surviving, record)
			}
		default:
			return SweepReport{}, fmt.Errorf("unexpected entry %q in the artifact retention directory", name)
		}
	}
	// A pending record left beside its job's deadline-bearing record by an
	// interrupted Retain is redundant once that record names all its
	// objects; anything else keeps the pending record and its objects.
	pending := 0
	for _, record := range pendingRecords {
		objects, retained := deadlineBearing[record.Job]
		if !retained {
			pending++
			surviving = append(surviving, record)
			continue
		}
		for _, object := range record.Objects {
			if !slices.Contains(objects, object) {
				return SweepReport{}, fmt.Errorf("pending record of %s names an object its retained record lacks", record.Job)
			}
		}
		if err := removeRegular(filepath.Join(retention, strings.TrimPrefix(record.Job, "sha256:")+pendingSuffix)); err != nil {
			return SweepReport{}, err
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
	report := SweepReport{Live: len(surviving) - pending, Pending: pending, Held: len(held)}
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
	// names it, including an expired record deferred to a later sweep, or
	// while a hold keeps it.
	referenced := held
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

// acquireRetention takes the retention lock, waiting up to the store's
// bounded wait while another process holds it.
func (s *Store) acquireRetention(ctx context.Context) (*dirlock.Lock, string, error) {
	deadline := time.Now().Add(s.lockWait)
	for {
		lock, retention, err := s.lockRetention()
		if !errors.Is(err, ErrRetentionBusy) {
			return lock, retention, err
		}
		if !time.Now().Before(deadline) {
			return nil, "", ErrRetentionBusy
		}
		timer := time.NewTimer(retentionRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, "", ctx.Err()
		case <-timer.C:
		}
	}
}

// CheckCapacity reports ErrRetentionFull unless jobs more jobs can be given
// a deadline under the record bound. A caller that will retain its results
// runs it before accepting the work, so a full store refuses up front.
func (s *Store) CheckCapacity(ctx context.Context, jobs int) error {
	if s == nil || s.root == "" || ctx == nil || jobs <= 0 || jobs > MaxRetentionRecords {
		return errors.New("invalid artifact capacity request")
	}
	lock, retention, err := s.acquireRetention(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	return s.requireRoomLocked(retention, jobs)
}

// requireRoomLocked refuses unless entries more deadline-bearing records fit
// under the store's record bound.
func (s *Store) requireRoomLocked(retention string, entries int) error {
	directory, err := os.Open(retention)
	if err != nil {
		return errors.New("open artifact retention directory")
	}
	defer directory.Close()
	count := 0
	for {
		batch, err := directory.ReadDir(1024)
		for _, entry := range batch {
			// Only deadline-bearing records and their tombstones count;
			// pending records and holds never break a reference.
			if name := entry.Name(); !strings.HasPrefix(name, retentionTempPrefix) &&
				(strings.HasSuffix(name, recordSuffix) || strings.HasSuffix(name, tombstoneSuffix)) {
				count++
			}
		}
		// Stop as soon as the answer is known.
		if count > s.maxRecords-entries {
			return ErrRetentionFull
		}
		if errors.Is(err, io.EOF) || (err == nil && len(batch) == 0) {
			return nil
		}
		if err != nil {
			return errors.New("read artifact retention directory")
		}
	}
}

// readRetentionEntries lists the whole retention directory in batches.
func readRetentionEntries(retention string) ([]os.DirEntry, error) {
	directory, err := os.Open(retention)
	if err != nil {
		return nil, errors.New("open artifact retention directory")
	}
	defer directory.Close()
	var entries []os.DirEntry
	for {
		batch, err := directory.ReadDir(1024)
		entries = append(entries, batch...)
		if errors.Is(err, io.EOF) || (err == nil && len(batch) == 0) {
			return entries, nil
		}
		if err != nil {
			return nil, errors.New("read artifact retention directory")
		}
	}
}

// writeHold durably records that an object committed without a job is kept
// permanently.
func (s *Store) writeHold(retention, digest string) error {
	path := filepath.Join(retention, strings.TrimPrefix(digest, "sha256:")+holdSuffix)
	if _, err := os.Lstat(path); err == nil {
		_, err := readHold(retention, filepath.Base(path))
		return err
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("stat artifact hold")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("create artifact hold")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync artifact hold")
	}
	if err := file.Close(); err != nil {
		return errors.New("close artifact hold")
	}
	return syncDirectory(retention)
}

// readHold validates a hold: an empty owner-only regular file named by the
// object it keeps.
func readHold(retention, name string) (string, error) {
	digest := "sha256:" + strings.TrimSuffix(name, holdSuffix)
	info, err := os.Lstat(filepath.Join(retention, name))
	if !digestPattern.MatchString(digest) || err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() != 0 {
		return "", fmt.Errorf("artifact hold %s is invalid", name)
	}
	return digest, nil
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
	// The file name states the record's kind: only a .pending record may be
	// pending, and it must be.
	stem := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, recordSuffix), tombstoneSuffix), pendingSuffix)
	if !record.valid() || "sha256:"+stem != record.Job || record.Pending != strings.HasSuffix(name, pendingSuffix) {
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
