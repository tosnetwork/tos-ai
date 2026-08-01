package executor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/executor/backendtest"
)

type supervisedTestDriver struct {
	mutex      sync.Mutex
	active     int
	containers int
	tasks      int
	snapshots  int
	closed     bool
	closeOnce  sync.Once
	closeCh    chan struct{}
}

type blockingReadyDriver struct {
	*supervisedTestDriver
	started chan struct{}
	release chan struct{}
}

func (d *blockingReadyDriver) CheckReady(ctx context.Context) error {
	select {
	case d.started <- struct{}{}:
	default:
	}
	select {
	case <-d.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newSupervisedTestDriver() *supervisedTestDriver {
	return &supervisedTestDriver{closeCh: make(chan struct{})}
}

func (d *supervisedTestDriver) CheckReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.closed {
		return errors.New("closed")
	}
	return nil
}

func (d *supervisedTestDriver) RunIsolated(
	ctx context.Context,
	_ executor.ContainerRequest,
	input []byte,
) (executor.Result, error) {
	d.mutex.Lock()
	if d.closed {
		d.mutex.Unlock()
		return executor.Result{}, errors.New("closed")
	}
	d.active++
	d.containers++
	d.tasks++
	d.snapshots++
	d.mutex.Unlock()
	defer func() {
		d.mutex.Lock()
		d.active--
		d.containers--
		d.tasks--
		d.snapshots--
		d.mutex.Unlock()
	}()
	if string(input) == "block" {
		select {
		case <-ctx.Done():
			return executor.Result{}, ctx.Err()
		case <-d.closeCh:
			return executor.Result{}, errors.New("closed")
		}
	}
	return executor.Result{
		Output: []byte("ok"), Usage: executor.Usage{Duration: time.Millisecond},
	}, nil
}

func (d *supervisedTestDriver) Close() error {
	d.closeOnce.Do(func() {
		d.mutex.Lock()
		d.closed = true
		d.mutex.Unlock()
		close(d.closeCh)
	})
	return nil
}

func (d *supervisedTestDriver) Snapshot(
	context.Context,
) (backendtest.Snapshot, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return backendtest.Snapshot{
		ActiveWorkloads: d.active, Containers: d.containers,
		Tasks: d.tasks, Snapshots: d.snapshots,
	}, nil
}

func TestSupervisedBackendPassesLifecycleConformance(t *testing.T) {
	successDigest, err := executor.ExecutionDigest("supervisor-success")
	if err != nil {
		t.Fatal(err)
	}
	cancelDigest, err := executor.ExecutionDigest("supervisor-cancel")
	if err != nil {
		t.Fatal(err)
	}
	limits := executor.Limits{
		CPUMillis: 1000, MemoryBytes: 1 << 20, DiskBytes: 1 << 20,
		PIDs: 8, ExecutionTime: time.Second, OutputBytes: 1024,
	}
	backendtest.Run(t, backendtest.Suite{
		New: func(context.Context) (backendtest.Backend, backendtest.Inspector, error) {
			driver := newSupervisedTestDriver()
			backend, err := executor.NewSupervisedBackend(driver, 32)
			return backend, driver, err
		},
		SuccessRequest: executor.ContainerRequest{
			ExecutionDigest: successDigest, Limits: limits,
		},
		SuccessInput: []byte("success"), ExpectedOutput: []byte("ok"),
		CancellationRequest: executor.ContainerRequest{
			ExecutionDigest: cancelDigest, Limits: limits,
		},
		CancellationInput: []byte("block"), StartTimeout: time.Second,
		ReturnTimeout: time.Second, InspectTimeout: time.Second,
		Concurrency: 8,
	})
}

func TestSupervisedBackendBoundsCapacityAndShutdown(t *testing.T) {
	driver := newSupervisedTestDriver()
	backend, err := executor.NewSupervisedBackend(driver, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstDigest, _ := executor.ExecutionDigest("first")
	secondDigest, _ := executor.ExecutionDigest("second")
	firstDone := make(chan error, 1)
	go func() {
		_, err := backend.RunIsolated(
			context.Background(),
			executor.ContainerRequest{ExecutionDigest: firstDigest},
			[]byte("block"),
		)
		firstDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, _ := driver.Snapshot(context.Background())
		if snapshot.ActiveWorkloads == 1 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("first workload did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := backend.RunIsolated(
		context.Background(),
		executor.ContainerRequest{ExecutionDigest: firstDigest}, nil,
	); err == nil {
		t.Fatal("duplicate active execution was accepted")
	}
	if _, err := backend.RunIsolated(
		context.Background(),
		executor.ContainerRequest{ExecutionDigest: secondDigest}, nil,
	); err == nil {
		t.Fatal("execution above fixed capacity was accepted")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err == nil {
		t.Fatal("driver shutdown returned an active workload as success")
	}
	if err := backend.CheckReady(context.Background()); err == nil {
		t.Fatal("closed supervisor remained ready")
	}
	if _, err := backend.RunIsolated(
		context.Background(),
		executor.ContainerRequest{ExecutionDigest: secondDigest}, nil,
	); err == nil {
		t.Fatal("closed supervisor accepted work")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisedBackendRejectsInvalidConfiguration(t *testing.T) {
	var typedNil *supervisedTestDriver
	for _, test := range []struct {
		driver executor.BackendDriver
		limit  int
	}{
		{driver: nil, limit: 1},
		{driver: typedNil, limit: 1},
		{driver: newSupervisedTestDriver(), limit: 0},
		{driver: newSupervisedTestDriver(), limit: executor.MaxSupervisedActiveHard + 1},
	} {
		if backend, err := executor.NewSupervisedBackend(
			test.driver, test.limit,
		); err == nil || backend != nil {
			t.Fatal("invalid supervisor configuration accepted")
		}
	}
}

func TestSupervisedBackendCloseWaitsForRegisteredReadiness(t *testing.T) {
	driver := &blockingReadyDriver{
		supervisedTestDriver: newSupervisedTestDriver(),
		started:              make(chan struct{}, 1), release: make(chan struct{}),
	}
	backend, err := executor.NewSupervisedBackend(driver, 1)
	if err != nil {
		t.Fatal(err)
	}
	readyDone := make(chan error, 1)
	go func() { readyDone <- backend.CheckReady(context.Background()) }()
	<-driver.started
	closeDone := make(chan error, 1)
	go func() { closeDone <- backend.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("close raced an active readiness probe: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(driver.release)
	if err := <-readyDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}
