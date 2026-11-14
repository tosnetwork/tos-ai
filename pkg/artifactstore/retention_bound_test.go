package artifactstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func numberedJob(index int) string { return "sha256:" + fmt.Sprintf("%064x", index+1) }

// A store holding more entries than the record bound (for example after the
// bound was lowered) is still swept: successive bounded sweeps collect every
// expired job.
func TestSweepCollectsAStoreOverTheRecordBound(t *testing.T) {
	store, _ := retentionStore(t)
	var objects []string
	for index := range 12 {
		object := put(t, store, numberedJob(index), fmt.Sprintf("payload %d", index))
		retain(t, store, numberedJob(index), sweepNow.Add(-time.Duration(index+1)*time.Minute), object)
		objects = append(objects, object)
	}
	store.maxRecords = 8
	deleted := 0
	for round := 0; round < 5 && deleted < 12; round++ {
		report, err := store.Sweep(context.Background(), sweepNow, 5)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if report.DeletedJobs == 0 {
			t.Fatalf("round %d made no progress: %+v", round, report)
		}
		deleted += report.DeletedJobs
		if report.Deferred != 12-deleted {
			t.Fatalf("round %d deferred %d, want %d", round, report.Deferred, 12-deleted)
		}
	}
	if deleted != 12 {
		t.Fatalf("collected %d of 12 expired jobs", deleted)
	}
	for _, object := range objects {
		if present(t, store, object) {
			t.Fatalf("object %s survived its expired job", object)
		}
	}
}

// Only a job's first deadline counts against the bound: deadline-bearing
// jobs are refused at it, pending jobs and holds never are.
func TestOnlyNewDeadlinesAreRefusedAtTheRecordBound(t *testing.T) {
	store, _ := retentionStore(t)
	store.maxRecords = 2
	first := put(t, store, numberedJob(0), "first")
	second := put(t, store, numberedJob(1), "second")
	retain(t, store, numberedJob(0), sweepNow.Add(-time.Hour), first)
	retain(t, store, numberedJob(1), sweepNow.Add(time.Hour), second)
	if err := store.CheckCapacity(context.Background(), 1); !errors.Is(err, ErrRetentionFull) {
		t.Fatalf("capacity at the bound = %v", err)
	}
	// Pending jobs and holds keep being accepted far past the bound, as a
	// provider runtime that never sets deadlines needs.
	for index := 2; index < 12; index++ {
		put(t, store, numberedJob(index), fmt.Sprintf("pending %d", index))
		if _, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader(fmt.Sprintf("held %d", index))); err != nil {
			t.Fatalf("hold %d past the bound: %v", index, err)
		}
	}
	// They do not count: the bound is still exactly the two deadlines.
	if err := store.Retain(context.Background(), numberedJob(2), sweepNow.Add(time.Hour), []string{put(t, store, numberedJob(2), "pending 2")}); !errors.Is(err, ErrRetentionFull) {
		t.Fatalf("a new deadline at the bound = %v", err)
	}
	// Jobs that already have a deadline keep working at the bound.
	extra := put(t, store, numberedJob(0), "first, more")
	retain(t, store, numberedJob(0), sweepNow.Add(-time.Minute), first, extra)
	report := sweep(t, store, 8)
	if report.DeletedJobs != 1 || report.Live != 1 || report.Pending != 10 || report.Held != 10 {
		t.Fatalf("report = %+v", report)
	}
	// The sweep made room again for one deadline.
	if err := store.CheckCapacity(context.Background(), 1); err != nil {
		t.Fatalf("capacity after the sweep = %v", err)
	}
	retain(t, store, numberedJob(2), sweepNow.Add(time.Hour), put(t, store, numberedJob(2), "pending 2"))
	if err := store.CheckCapacity(context.Background(), 1); !errors.Is(err, ErrRetentionFull) {
		t.Fatalf("capacity back at the bound = %v", err)
	}
	for _, jobs := range []int{0, -1, MaxRetentionRecords + 1} {
		if err := store.CheckCapacity(context.Background(), jobs); err == nil {
			t.Errorf("capacity for %d jobs accepted", jobs)
		}
	}
}

func TestWithRecordBoundRefusesValuesOutsideTheBound(t *testing.T) {
	root := resolvedTempDir(t)
	for _, records := range []int{0, -1, MaxRetentionRecords + 1} {
		if _, err := Open(root+"/artifacts", 1024, WithRecordBound(records)); err == nil {
			t.Errorf("record bound %d accepted", records)
		}
	}
	if _, err := Open(root+"/artifacts", 1024, nil); err == nil {
		t.Error("a nil option was accepted")
	}
	store, err := Open(root+"/artifacts", 1024, WithRecordBound(7))
	if err != nil || store.maxRecords != 7 {
		t.Fatalf("store = %+v %v", store, err)
	}
}

// Retain replaces a job's pending record with its deadline-bearing one; a
// crash between the two leaves both, which the next sweep or bind resolves
// without losing an object.
func TestAnInterruptedRetainLeavesNoStalePendingRecord(t *testing.T) {
	store, root := retentionStore(t)
	object := put(t, store, numberedJob(0), "payload")
	name := fmt.Sprintf("%064x", 1)
	pendingPath := filepath.Join(root, "retention", name+pendingSuffix)
	saved, err := os.ReadFile(pendingPath)
	if err != nil {
		t.Fatal(err)
	}
	retain(t, store, numberedJob(0), sweepNow.Add(-time.Hour), object)
	if _, err := os.Lstat(pendingPath); !os.IsNotExist(err) {
		t.Fatalf("Retain left the pending record: %v", err)
	}
	// Recreate the state a crash before the removal leaves.
	if err := os.WriteFile(pendingPath, saved, 0o600); err != nil {
		t.Fatal(err)
	}
	report := sweep(t, store, 8)
	if report.DeletedJobs != 1 || report.Pending != 0 || present(t, store, object) {
		t.Fatalf("a stale pending record kept an expired job's object: %+v", report)
	}
	if _, err := os.Lstat(pendingPath); !os.IsNotExist(err) {
		t.Fatalf("the sweep left the stale pending record: %v", err)
	}
}
