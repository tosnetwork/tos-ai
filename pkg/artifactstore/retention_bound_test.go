package artifactstore

import (
	"context"
	"errors"
	"fmt"
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

func TestNewJobsAndHoldsAreRefusedAtTheRecordBound(t *testing.T) {
	store, _ := retentionStore(t)
	store.maxRecords = 3
	first := put(t, store, numberedJob(0), "first")
	put(t, store, numberedJob(1), "second")
	if _, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("held")); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckCapacity(context.Background(), 1); !errors.Is(err, ErrRetentionFull) {
		t.Fatalf("capacity at the bound = %v", err)
	}
	if _, err := store.PutFor(context.Background(), numberedJob(2), "application/octet-stream", strings.NewReader("third")); !errors.Is(err, ErrRetentionFull) {
		t.Fatalf("a new job at the bound = %v", err)
	}
	if _, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("another hold")); !errors.Is(err, ErrRetentionFull) {
		t.Fatalf("a new hold at the bound = %v", err)
	}
	// Existing jobs and holds keep working at the bound.
	extra := put(t, store, numberedJob(0), "first, more")
	retain(t, store, numberedJob(0), sweepNow.Add(-time.Hour), first, extra)
	if _, err := store.Put(context.Background(), "application/octet-stream", strings.NewReader("held")); err != nil {
		t.Fatalf("re-holding an existing object at the bound: %v", err)
	}
	report := sweep(t, store, 8)
	if report.DeletedJobs != 1 || report.Pending != 1 || report.Held != 1 || report.Live != 0 {
		t.Fatalf("report = %+v", report)
	}
	// The sweep made room again.
	if err := store.CheckCapacity(context.Background(), 1); err != nil {
		t.Fatalf("capacity after the sweep = %v", err)
	}
	put(t, store, numberedJob(2), "third")
	for _, jobs := range []int{0, -1, MaxRetentionRecords + 1} {
		if err := store.CheckCapacity(context.Background(), jobs); err == nil {
			t.Errorf("capacity for %d jobs accepted", jobs)
		}
	}
}
