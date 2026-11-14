package artifactstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

func put(t *testing.T, store *Store, value string) string {
	t.Helper()
	descriptor, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader(value))
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

func TestSweepDeletesExpiredAndKeepsUnexpired(t *testing.T) {
	store, root := retentionStore(t)
	old, fresh, boundary := put(t, store, "old"), put(t, store, "fresh"), put(t, store, "boundary")
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
	if _, err := os.Lstat(filepath.Join(root, "retention", strings.Repeat("a", 64)+recordSuffix)); !os.IsNotExist(err) {
		t.Fatalf("expired record survived: %v", err)
	}
	if _, err := store.Get(context.Background(), fresh); err != nil {
		t.Fatalf("live object unreadable: %v", err)
	}
}

func TestSweepKeepsASharedObjectWhileAnyReferrerIsUnexpired(t *testing.T) {
	store, _ := retentionStore(t)
	shared, own := put(t, store, "shared"), put(t, store, "own")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), shared, own)
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

func TestSweepKeepsObjectsOfExpiredJobsDeferredByTheLimit(t *testing.T) {
	store, _ := retentionStore(t)
	shared := put(t, store, "shared")
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
	first, second := put(t, store, "first"), put(t, store, "second")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), first, second)
	// Simulate a crash after the tombstone and one object removal.
	name := strings.Repeat("a", 64)
	if err := os.Rename(filepath.Join(root, "retention", name+recordSuffix), filepath.Join(root, "retention", name+tombstoneSuffix)); err != nil {
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
	for name, body := range map[string]string{
		"not json":       "{",
		"unknown field":  `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":1,"objects":["` + job("c") + `"],"x":1}`,
		"wrong schema":   `{"schema":"other","job":"` + job("b") + `","retain_until_unix":1,"objects":["` + job("c") + `"]}`,
		"other job":      `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("d") + `","retain_until_unix":1,"objects":["` + job("c") + `"]}`,
		"no objects":     `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":1,"objects":[]}`,
		"unsorted":       `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":1,"objects":["` + job("d") + `","` + job("c") + `"]}`,
		"zero deadline":  `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":0,"objects":["` + job("c") + `"]}`,
		"trailing data":  `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":1,"objects":["` + job("c") + `"]} {}`,
		"traversal":      `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":1,"objects":["../../etc/passwd"]}`,
		"upper-case hex": `{"schema":"tos.ai.artifact-retention.v1","job":"` + job("b") + `","retain_until_unix":1,"objects":["sha256:` + strings.Repeat("C", 64) + `"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			store, root := retentionStore(t)
			expired := put(t, store, "expired")
			retain(t, store, job("a"), sweepNow.Add(-time.Hour), expired)
			if err := os.WriteFile(filepath.Join(root, "retention", strings.Repeat("b", 64)+recordSuffix), []byte(body), 0o600); err != nil {
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

func TestSweepRefusesUnexpectedEntriesSymlinksAndUnsafeObjects(t *testing.T) {
	t.Run("unknown entry", func(t *testing.T) {
		store, root := retentionStore(t)
		retain(t, store, job("a"), sweepNow.Add(-time.Hour), put(t, store, "x"))
		if err := os.WriteFile(filepath.Join(root, "retention", "notes.txt"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Sweep(context.Background(), sweepNow, 10); err == nil {
			t.Fatal("unexpected entry accepted")
		}
	})
	t.Run("symlinked record", func(t *testing.T) {
		store, root := retentionStore(t)
		object := put(t, store, "x")
		retain(t, store, job("a"), sweepNow.Add(time.Hour), object)
		target := filepath.Join(root, "retention", strings.Repeat("a", 64)+recordSuffix)
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
		object := put(t, store, "x")
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
	first, second := put(t, store, "first"), put(t, store, "second")
	retain(t, store, job("a"), sweepNow.Add(time.Hour), first)
	// A retry with an earlier deadline keeps the later one and adds objects.
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), second, first)
	record, err := readRetentionRecord(filepath.Join(root, "retention"), strings.Repeat("a", 64)+recordSuffix)
	if err != nil || record.RetainUntilUnix != sweepNow.Add(time.Hour).Unix() || len(record.Objects) != 2 {
		t.Fatalf("record = %+v %v", record, err)
	}
	if report := sweep(t, store, 10); report.DeletedJobs != 0 || !present(t, store, first) || !present(t, store, second) {
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
	object := put(t, store, "x")
	retain(t, store, job("a"), sweepNow.Add(-time.Hour), object)
	name := strings.Repeat("a", 64)
	if err := os.Rename(filepath.Join(root, "retention", name+recordSuffix), filepath.Join(root, "retention", name+tombstoneSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := store.Retain(context.Background(), job("a"), sweepNow.Add(time.Hour), []string{object}); err == nil {
		t.Fatal("revived a job whose deletion is in progress")
	}
}

func TestRetentionIsHeldByOneProcessAtATime(t *testing.T) {
	store, _ := retentionStore(t)
	lock, _, err := store.lockRetention()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := store.Sweep(context.Background(), sweepNow, 1); err != ErrRetentionBusy {
		t.Fatalf("concurrent sweep = %v", err)
	}
}

func TestObjectsWithoutRetentionAreKept(t *testing.T) {
	store, _ := retentionStore(t)
	orphan := put(t, store, "orphan")
	if report := sweep(t, store, 10); report != (SweepReport{}) || !present(t, store, orphan) {
		t.Fatalf("an unretained object was deleted: %+v", report)
	}
}
