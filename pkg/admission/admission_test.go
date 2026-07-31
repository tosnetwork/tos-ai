package admission

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func testController(t *testing.T, queue int) *Controller {
	t.Helper()
	controller, err := New(Config{
		MaxConcurrent: 1,
		MaxQueue:      queue,
		Capacity: Resources{
			RAMBytes: 100, VRAMBytes: 100, KVCacheBytes: 100,
			ContextTokens: 100, BatchSize: 10, OutputBytes: 100,
			ExecutionTime: time.Minute,
		},
		OwnerReserved: Resources{
			RAMBytes: 40, VRAMBytes: 40, KVCacheBytes: 40,
			ContextTokens: 40, BatchSize: 4, OutputBytes: 40,
		},
		PerRequestMax: Resources{
			RAMBytes: 100, VRAMBytes: 100, KVCacheBytes: 100,
			ContextTokens: 100, BatchSize: 10, OutputBytes: 100,
			ExecutionTime: time.Minute,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func request(id string, class Class, amount uint64) Request {
	return Request{
		ID: id, Fingerprint: sha256.Sum256([]byte(id)), Class: class,
		Resources: Resources{
			RAMBytes: amount, VRAMBytes: amount, KVCacheBytes: amount,
			ContextTokens: amount, BatchSize: 1, OutputBytes: amount,
			ExecutionTime: time.Second,
		},
	}
}

func TestOwnerReserveAndPriorityClasses(t *testing.T) {
	controller := testController(t, 2)
	if _, _, err := controller.Reserve(request("external-large", ClassExternalService, 61)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("external owner-reserve error = %v", err)
	}
	local, owner, err := controller.Reserve(request("local-large", ClassLocalAsync, 90))
	if err != nil || !owner {
		t.Fatalf("local reservation owner=%v err=%v", owner, err)
	}
	local.Release()
	bad := request("bad-priority", 0, 1)
	if err := controller.Check(bad); !errors.Is(err, ErrPriority) {
		t.Fatalf("priority error = %v", err)
	}
}

func TestQueueSaturationAndRelease(t *testing.T) {
	controller := testController(t, 1)
	first, _, err := controller.Reserve(request("first", ClassLocalAsync, 1))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := controller.Reserve(request("second", ClassLocalAsync, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := controller.Reserve(request("third", ClassLocalAsync, 1)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue saturation error = %v", err)
	}
	first.Release()
	if _, _, err := controller.Reserve(request("third", ClassLocalAsync, 1)); err != nil {
		t.Fatalf("reservation after release = %v", err)
	}
	second.Release()
}

func TestReservationIdempotencyAndConflict(t *testing.T) {
	controller := testController(t, 1)
	value := request("same-request", ClassExternalService, 1)
	first, owner, err := controller.Reserve(value)
	if err != nil || !owner {
		t.Fatalf("first owner=%v err=%v", owner, err)
	}
	retry, owner, err := controller.Reserve(value)
	if err != nil || owner {
		t.Fatalf("retry owner=%v err=%v", owner, err)
	}
	retry.Release()
	if controller.Snapshot().Reserved != 1 {
		t.Fatal("retry released owner reservation")
	}
	conflict := value
	conflict.Fingerprint = sha256.Sum256([]byte("different"))
	if _, _, err := controller.Reserve(conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	first.Release()
	if controller.Snapshot().Reserved != 0 {
		t.Fatal("reservation leaked")
	}
}

func TestStartCancelTimeoutAndShutdownRelease(t *testing.T) {
	controller := testController(t, 2)
	first, _, _ := controller.Reserve(request("running", ClassLocalAsync, 1))
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	second, _, _ := controller.Reserve(request("waiting", ClassLocalAsync, 1))
	if err := second.Start(); !errors.Is(err, ErrConcurrency) {
		t.Fatalf("concurrency error = %v", err)
	}
	first.Release()
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	second.Release()
	if snapshot := controller.Snapshot(); snapshot.Running != 0 || snapshot.Reserved != 0 {
		t.Fatalf("resources leaked: %#v", snapshot)
	}

	pending, _, _ := controller.Reserve(request("shutdown", ClassLocalAsync, 1))
	controller.BeginDrain()
	if err := controller.Check(request("rejected", ClassLocalAsync, 1)); !errors.Is(err, ErrStopped) {
		t.Fatalf("drain error = %v", err)
	}
	pending.Release()
	controller.Shutdown()
}

func TestConcurrentWaitersRemainBounded(t *testing.T) {
	controller := testController(t, 4)
	value := request("shared-waiter", ClassExternalService, 1)
	first, _, err := controller.Reserve(value)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 32)
	for range 32 {
		go func() {
			_, owner, reserveErr := controller.Reserve(value)
			if owner {
				done <- errors.New("retry became owner")
				return
			}
			done <- reserveErr
		}()
	}
	for range 32 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if controller.Snapshot().Reserved != 1 {
		t.Fatal("idempotent retries grew reservation map")
	}
	first.Release()
}
