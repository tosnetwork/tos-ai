package artifactstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var sweepNow = time.Unix(2_000_000_000, 0)

func job(b string) string { return "sha256:" + strings.Repeat(b, 64) }

func retentionStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := filepath.Join(resolvedTempDir(t), "artifacts")
	store, err := Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return store, root
}

// put stores value on behalf of jobID; the job's record is pending until
// it is retained.
func put(t *testing.T, store *Store, jobID, value string) string {
	t.Helper()
	descriptor, err := store.PutFor(context.Background(), jobID, "application/octet-stream", strings.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	return descriptor.Digest
}

func present(t *testing.T, store *Store, digest string) bool {
	t.Helper()
	_, err := os.Lstat(store.path(digest))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

func retain(t *testing.T, store *Store, jobID string, until time.Time, objects ...string) {
	t.Helper()
	if err := store.Retain(context.Background(), jobID, until, objects); err != nil {
		t.Fatal(err)
	}
}

func sweep(t *testing.T, store *Store, limit int) SweepReport {
	t.Helper()
	report, err := store.Sweep(context.Background(), sweepNow, limit)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func recordPath(root, b string) string {
	return filepath.Join(root, "retention", strings.Repeat(b, 64)+recordSuffix)
}

func TestSweepDeletesExpiredAndKeepsUnexpired(t *testing.T) {
	store, root := retentionStore(t)
	old, fresh, boundary := put(t, store, job("a"), "old"), put(t, store, job("b"), "fresh"), put(t, store, job("c"), "boundary")
	retain(t, store, job("a"), sweepNow.Add(-time.Second), old)
	retain(t, store, job("b"), sweepNow.Add(time.Hour), fresh)
	// The deadline is inclusive: a job retained until now is still live.
	retain(t, store, job("c"), sweepNow, boundary)
	report := sweep(t, store, 10)
	if report.DeletedJobs != 1 || report.DeletedObjects != 1 || report.Live != 2 || report.Deferred != 0 {
		t.Fatalf("report = %+v", report)
	}
	if present(t, store, old) || !present(t, store, fresh) || !present(t, store, boundary) {
		t.Fatal("sweep deleted the wrong objects")
	}
	if _, err := os.Lstat(recordPath(root, "a")); !os.IsNotExist(err) {
		t.Fatalf("expired record survived: %v", err)
	}
	if _, err := store.Get(context.Background(), fresh); err != nil {
		t.Fatalf("live object unreadable: %v", err)
	}
}

func TestSweepKeepsASharedObjectWhileAnyReferrerIsUnexpired(t *testing.T) {
	store, _ := retentionStore(t)
	shared, own := put(t, store, job("a"), "shared"), put(t, store, job("a"), "own")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), shared, own)
	put(t, store, job("b"), "shared")
	retain(t, store, job("b"), sweepNow.Add(time.Hour), shared)
	report := sweep(t, store, 10)
	if report.DeletedJobs != 1 || report.DeletedObjects != 1 || report.KeptShared != 1 {
		t.Fatalf("report = %+v", report)
	}
	if !present(t, store, shared) || present(t, store, own) {
		t.Fatal("a shared object was deleted while a referrer is unexpired")
	}
	later, err := store.Sweep(context.Background(), sweepNow.Add(2*time.Hour), 10)
	if err != nil || later.DeletedObjects != 1 || present(t, store, shared) {
		t.Fatalf("shared object outlived its last referrer: %+v %v", later, err)
	}
}

// A job that committed a deduplicated object but has not been retained yet
// (still running, or never given a deadline) keeps it alive.
func TestSweepKeepsAnObjectAPendingJobShares(t *testing.T) {
	store, root := retentionStore(t)
	shared := put(t, store, job("a"), "shared")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), shared)
	if again := put(t, store, job("b"), "shared"); again != shared {
		t.Fatal("identical bytes were not deduplicated")
	}
	record, err := readRetentionRecord(filepath.Join(root, "retention"), strings.Repeat("b", 64)+recordSuffix)
	if err != nil || !record.Pending || record.RetainUntilUnix != 0 {
		t.Fatalf("pending record = %+v %v", record, err)
	}
	report := sweep(t, store, 10)
	if report.DeletedJobs != 1 || report.DeletedObjects != 0 || report.KeptShared != 1 || report.Live != 1 || !present(t, store, shared) {
		t.Fatalf("an object a pending job committed was deleted: %+v", report)
	}
	// Once the pending job is retained and expires, the object goes.
	retain(t, store, job("b"), sweepNow.Add(-time.Minute), shared)
	if report := sweep(t, store, 10); report.DeletedObjects != 1 || present(t, store, shared) {
		t.Fatalf("after the last referrer expired: %+v", report)
	}
}

func TestSweepNeverDeletesAnObjectPutWithoutAJob(t *testing.T) {
	store, _ := retentionStore(t)
	shared := put(t, store, job("a"), "shared")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), shared)
	descriptor, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("shared"))
	if err != nil || descriptor.Digest != shared {
		t.Fatalf("Put = %+v %v", descriptor, err)
	}
	orphan, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	report := sweep(t, store, 10)
	if report.DeletedJobs != 1 || report.DeletedObjects != 0 || report.Held != 2 || !present(t, store, shared) || !present(t, store, orphan.Digest) {
		t.Fatalf("a held object was deleted: %+v", report)
	}
}

func TestSweepKeepsObjectsOfExpiredJobsDeferredByTheLimit(t *testing.T) {
	store, _ := retentionStore(t)
	shared := put(t, store, job("a"), "shared")
	put(t, store, job("b"), "shared")
	retain(t, store, job("a"), sweepNow.Add(-2*time.Hour), shared)
	retain(t, store, job("b"), sweepNow.Add(-time.Hour), shared)
	report := sweep(t, store, 1)
	if report.DeletedJobs != 1 || report.Deferred != 1 || report.KeptShared != 1 || !present(t, store, shared) {
		t.Fatalf("a deferred record lost its object: %+v", report)
	}
	report = sweep(t, store, 1)
	if report.DeletedJobs != 1 || report.Deferred != 0 || present(t, store, shared) {
		t.Fatalf("second sweep = %+v", report)
	}
}

func TestSweepRerunIsIdempotentAndFinishesAnInterruptedDeletion(t *testing.T) {
	store, root := retentionStore(t)
	first, second := put(t, store, job("a"), "first"), put(t, store, job("a"), "second")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), first, second)
	// Simulate a crash after the tombstone and one object removal.
	name := strings.Repeat("a", 64)
	if err := os.Rename(recordPath(root, "a"), filepath.Join(root, "retention", name+tombstoneSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.path(first)); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(root, "retention", retentionTempPrefix+"123")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	report := sweep(t, store, 10)
	if report.DeletedJobs != 1 || report.DeletedObjects != 1 || present(t, store, second) {
		t.Fatalf("interrupted deletion not finished: %+v", report)
	}
	if _, err := os.Lstat(stale); !os.IsNotExist(err) {
		t.Fatal("stale temporary record survived")
	}
	again := sweep(t, store, 10)
	if again != (SweepReport{}) {
		t.Fatalf("rerun was not a no-op: %+v", again)
	}
}

func TestSweepRejectsMalformedMetadataBeforeDeletingAnything(t *testing.T) {
	record := func(fields string) string {
		return `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `",` + fields + `}`
	}
	objects := `"objects":["` + job("c") + `"]`
	for name, body := range map[string]string{
		"not json":             "{",
		"unknown field":        record(`"pending":false,"retain_until_unix":1,` + objects + `,"x":1`),
		"wrong schema":         strings.Replace(record(`"pending":false,"retain_until_unix":1,`+objects), "retention.v1", "other", 1),
		"other job":            strings.Replace(record(`"pending":false,"retain_until_unix":1,`+objects), job("b"), job("d"), 1),
		"no objects":           record(`"pending":false,"retain_until_unix":1,"objects":[]`),
		"unsorted":             record(`"pending":false,"retain_until_unix":1,"objects":["` + job("d") + `","` + job("c") + `"]`),
		"final without a date": record(`"pending":false,"retain_until_unix":0,` + objects),
		"pending with a date":  record(`"pending":true,"retain_until_unix":5,` + objects),
		"negative deadline":    record(`"pending":false,"retain_until_unix":-1,` + objects),
		"trailing data":        record(`"pending":false,"retain_until_unix":1,`+objects) + " {}",
		"traversal":            record(`"pending":false,"retain_until_unix":1,"objects":["../../etc/passwd"]`),
		"upper-case hex":       record(`"pending":false,"retain_until_unix":1,"objects":["sha256:` + strings.Repeat("C", 64) + `"]`),
	} {
		t.Run(name, func(t *testing.T) {
			store, root := retentionStore(t)
			expired := put(t, store, job("a"), "expired")
			retain(t, store, job("a"), sweepNow.Add(-time.Hour), expired)
			if err := os.WriteFile(recordPath(root, "b"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil {
				t.Fatal("malformed retention metadata accepted")
			}
			if !present(t, store, expired) {
				t.Fatal("a sweep that refused metadata still deleted objects")
			}
		})
	}
}

func TestSweepRejectsMalformedHolds(t *testing.T) {
	for name, write := range map[string]func(string) error{
		"non-empty": func(dir string) error {
			return os.WriteFile(filepath.Join(dir, strings.Repeat("c", 64)+holdSuffix), []byte("x"), 0o600)
		},
		"bad name": func(dir string) error {
			return os.WriteFile(filepath.Join(dir, strings.Repeat("z", 64)+holdSuffix), nil, 0o600)
		},
		"short name": func(dir string) error { return os.WriteFile(filepath.Join(dir, "abc"+holdSuffix), nil, 0o600) },
		"wide mode": func(dir string) error {
			path := filepath.Join(dir, strings.Repeat("c", 64)+holdSuffix)
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				return err
			}
			return os.Chmod(path, 0o644)
		},
		"symlink": func(dir string) error {
			target := filepath.Join(dir, "..", "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				return err
			}
			return os.Symlink(target, filepath.Join(dir, strings.Repeat("c", 64)+holdSuffix))
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, root := retentionStore(t)
			expired := put(t, store, job("a"), "expired")
			retain(t, store, job("a"), sweepNow.Add(-time.Hour), expired)
			if err := write(filepath.Join(root, "retention")); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil || !present(t, store, expired) {
				t.Fatalf("malformed hold accepted: %v", err)
			}
		})
	}
}

func TestSweepRefusesUnexpectedEntriesSymlinksAndUnsafeObjects(t *testing.T) {
	t.Run("unknown entry", func(t *testing.T) {
		store, root := retentionStore(t)
		retain(t, store, job("a"), sweepNow.Add(-time.Hour), put(t, store, job("a"), "x"))
		if err := os.WriteFile(filepath.Join(root, "retention", "notes.txt"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil {
			t.Fatal("unexpected entry accepted")
		}
	})
	t.Run("symlinked record", func(t *testing.T) {
		store, root := retentionStore(t)
		object := put(t, store, job("a"), "x")
		retain(t, store, job("a"), sweepNow.Add(time.Hour), object)
		target := recordPath(root, "a")
		moved := filepath.Join(resolvedTempDir(t), "record.json")
		if err := os.Rename(target, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(moved, target); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil {
			t.Fatal("symlinked record accepted")
		}
	})
	t.Run("symlinked object", func(t *testing.T) {
		store, _ := retentionStore(t)
		object := put(t, store, job("a"), "x")
		retain(t, store, job("a"), sweepNow.Add(-time.Hour), object)
		outside := filepath.Join(resolvedTempDir(t), "victim")
		if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(store.path(object)); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, store.path(object)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil {
			t.Fatal("swept through a symlinked object")
		}
		if _, err := os.Stat(outside); err != nil {
			t.Fatalf("symlink target was touched: %v", err)
		}
		if _, err := os.Lstat(store.path(object)); err != nil {
			t.Fatal("refused object was removed anyway")
		}
	})
	t.Run("symlinked retention directory", func(t *testing.T) {
		store, root := retentionStore(t)
		if err := os.Symlink(resolvedTempDir(t), filepath.Join(root, "retention")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil {
			t.Fatal("symlinked retention directory accepted")
		}
	})
}

func TestRetainValidatesAndNeverShortens(t *testing.T) {
	store, root := retentionStore(t)
	first, second := put(t, store, job("a"), "first"), put(t, store, job("f"), "second")
	retain(t, store, job("a"), sweepNow.Add(time.Hour), first)
	// A retry with an earlier deadline keeps the later one and adds objects.
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), second, first)
	record, err := readRetentionRecord(filepath.Join(root, "retention"), strings.Repeat("a", 64)+recordSuffix)
	if err != nil || record.Pending || record.RetainUntilUnix != sweepNow.Add(time.Hour).Unix() || len(record.Objects) != 2 {
		t.Fatalf("record = %+v %v", record, err)
	}
	// A later commit for a retained job adds its object without touching
	// the deadline.
	third := put(t, store, job("a"), "third")
	record, err = readRetentionRecord(filepath.Join(root, "retention"), strings.Repeat("a", 64)+recordSuffix)
	if err != nil || record.Pending || record.RetainUntilUnix != sweepNow.Add(time.Hour).Unix() || len(record.Objects) != 3 {
		t.Fatalf("record after commit = %+v %v", record, err)
	}
	if report := sweep(t, store, 10); report.DeletedJobs != 0 || !present(t, store, first) || !present(t, store, second) || !present(t, store, third) {
		t.Fatalf("a retry shortened retention: %+v", report)
	}
	missing := "sha256:" + strings.Repeat("9", 64)
	tooMany := make([]string, MaxObjectsPerJob+1)
	for index := range tooMany {
		tooMany[index] = first
	}
	for name, call := range map[string]func() error{
		"missing object": func() error { return store.Retain(context.Background(), job("b"), sweepNow, []string{missing}) },
		"bad job":        func() error { return store.Retain(context.Background(), "../x", sweepNow, []string{first}) },
		"bad object":     func() error { return store.Retain(context.Background(), job("b"), sweepNow, []string{"sha256:../x"}) },
		"no objects":     func() error { return store.Retain(context.Background(), job("b"), sweepNow, nil) },
		"too many":       func() error { return store.Retain(context.Background(), job("b"), sweepNow, tooMany) },
		"zero deadline":  func() error { return store.Retain(context.Background(), job("b"), time.Unix(0, 0), []string{first}) },
		"bad put job": func() error {
			_, err := store.PutFor(context.Background(), "job-1", "application/octet-stream", strings.NewReader("x"))
			return err
		},
	} {
		if err := call(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, limit := range []int{0, -1, MaxSweepJobs + 1} {
		if _, err := store.Sweep(context.Background(), sweepNow, limit); err == nil {
			t.Errorf("limit %d accepted", limit)
		}
	}
}

func TestRetainRefusesAJobBeingDeleted(t *testing.T) {
	store, root := retentionStore(t)
	object := put(t, store, job("a"), "x")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), object)
	name := strings.Repeat("a", 64)
	if err := os.Rename(recordPath(root, "a"), filepath.Join(root, "retention", name+tombstoneSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := store.Retain(context.Background(), job("a"), sweepNow.Add(time.Hour), []string{object}); err == nil {
		t.Fatal("revived a job whose deletion is in progress")
	}
	if _, err := store.PutFor(context.Background(), job("a"), "application/octet-stream", strings.NewReader("y")); err == nil {
		t.Fatal("committed into a job whose deletion is in progress")
	}
}

func TestRetentionWaitsABoundedTimeForAnotherHolder(t *testing.T) {
	store, _ := retentionStore(t)
	object := put(t, store, job("a"), "x")
	lock, _, err := store.lockRetention()
	if err != nil {
		t.Fatal(err)
	}
	store.lockWait = 30 * time.Millisecond
	if _, err := store.Sweep(context.Background(), sweepNow, 1); !errors.Is(err, ErrRetentionBusy) {
		t.Fatalf("sweep past the wait = %v", err)
	}
	if err := store.Retain(context.Background(), job("a"), sweepNow, []string{object}); !errors.Is(err, ErrRetentionBusy) {
		t.Fatalf("retain past the wait = %v", err)
	}
	store.lockWait = 10 * time.Second
	released := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = lock.Close()
		close(released)
	}()
	if err := store.Retain(context.Background(), job("a"), sweepNow, []string{object}); err != nil {
		t.Fatalf("retain did not wait for the holder: %v", err)
	}
	<-released
	// A commit waiting for the lock stops when its context ends, well
	// before the bounded wait.
	again, _, err := store.lockRetention()
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := store.PutFor(ctx, job("b"), "application/octet-stream", strings.NewReader("y")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a cancelled commit = %v", err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Fatalf("a cancelled commit kept waiting for %v", waited)
	}
}

// Commits, retention and sweeps race freely; every object a live job names
// must stay readable throughout.
func TestRetentionIsConsistentUnderConcurrentSweeps(t *testing.T) {
	store, _ := retentionStore(t)
	expired := put(t, store, job("e"), "shared payload")
	retain(t, store, job("e"), sweepNow.Add(-time.Hour), expired)
	ctx, stop := context.WithCancel(context.Background())
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		for ctx.Err() == nil {
			if _, err := store.Sweep(context.Background(), sweepNow, 4); err != nil {
				t.Errorf("sweep: %v", err)
				return
			}
		}
	}()
	errs := make(chan error, 8)
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			jobID := "sha256:" + fmt.Sprintf("%064x", worker+1)
			descriptor, err := store.PutFor(context.Background(), jobID, "application/octet-stream", strings.NewReader("shared payload"))
			if err == nil {
				err = store.Retain(context.Background(), jobID, sweepNow.Add(time.Hour), []string{descriptor.Digest})
			}
			if err == nil {
				_, err = store.Get(context.Background(), descriptor.Digest)
			}
			errs <- err
		}()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	stop()
	group.Wait()
	if _, err := store.Get(context.Background(), expired); err != nil {
		t.Fatalf("an object live jobs share was deleted: %v", err)
	}
}
