package backendtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
)

type referenceBackend struct {
	mutex  sync.Mutex
	active map[string]struct{}
	closed bool
}

func (b *referenceBackend) CheckReady(ctx context.Context) error {
	return ctx.Err()
}

func (b *referenceBackend) RunIsolated(
	ctx context.Context,
	request executor.ContainerRequest,
	input []byte,
) (executor.Result, error) {
	b.mutex.Lock()
	if b.closed {
		b.mutex.Unlock()
		return executor.Result{}, errors.New("closed")
	}
	if _, exists := b.active[request.ExecutionDigest]; exists {
		b.mutex.Unlock()
		return executor.Result{}, errors.New("duplicate execution identity")
	}
	b.active[request.ExecutionDigest] = struct{}{}
	b.mutex.Unlock()
	defer func() {
		b.mutex.Lock()
		delete(b.active, request.ExecutionDigest)
		b.mutex.Unlock()
	}()
	if string(input) == "block" {
		<-ctx.Done()
		return executor.Result{}, ctx.Err()
	}
	return executor.Result{
		Output: []byte("ok"),
		Usage:  executor.Usage{Duration: time.Millisecond},
	}, nil
}

func (b *referenceBackend) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.closed = true
	return nil
}

func (b *referenceBackend) Snapshot(context.Context) (Snapshot, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return Snapshot{ActiveWorkloads: len(b.active)}, nil
}

func TestReferenceBackendPassesHarness(t *testing.T) {
	executionDigest, err := executor.ExecutionDigest("backend-conformance")
	if err != nil {
		t.Fatal(err)
	}
	request := executor.ContainerRequest{ExecutionDigest: executionDigest, Limits: executor.Limits{
		CPUMillis: 1000, MemoryBytes: 1 << 20, DiskBytes: 1 << 20,
		PIDs: 8, ExecutionTime: time.Second, OutputBytes: 1024,
	}}
	Run(t, Suite{
		New: func(context.Context) (Backend, Inspector, error) {
			backend := &referenceBackend{active: make(map[string]struct{})}
			return backend, backend, nil
		},
		SuccessRequest: request, SuccessInput: []byte("success"),
		ExpectedOutput: []byte("ok"), CancellationRequest: request,
		CancellationInput: []byte("block"), StartTimeout: time.Second,
		ReturnTimeout: time.Second, InspectTimeout: time.Second,
		Concurrency: 8,
	})
}

func TestValidateSuiteRejectsIncompleteResourceBounds(t *testing.T) {
	executionDigest, err := executor.ExecutionDigest("backend-conformance-validation")
	if err != nil {
		t.Fatal(err)
	}
	request := executor.ContainerRequest{ExecutionDigest: executionDigest, Limits: executor.Limits{
		CPUMillis: 1000, MemoryBytes: 1 << 20, DiskBytes: 1 << 20,
		PIDs: 8, ExecutionTime: time.Second, OutputBytes: 1,
	}}
	suite := Suite{
		New: func(context.Context) (Backend, Inspector, error) {
			backend := &referenceBackend{active: make(map[string]struct{})}
			return backend, backend, nil
		},
		SuccessRequest: request, ExpectedOutput: []byte("x"),
		CancellationRequest: request, StartTimeout: time.Second,
		ReturnTimeout: time.Second, InspectTimeout: time.Second,
		Concurrency: 1,
	}
	if err := validateSuite(suite); err != nil {
		t.Fatalf("valid suite rejected: %v", err)
	}

	suite.SuccessRequest.Limits.MemoryBytes = 0
	if err := validateSuite(suite); err == nil {
		t.Fatal("suite accepted a missing memory limit")
	}
	suite.SuccessRequest = request
	suite.SuccessRequest.ExecutionDigest = "raw/request/id"
	if err := validateSuite(suite); err == nil {
		t.Fatal("suite accepted a non-digest execution identity")
	}
	suite.SuccessRequest = request
	suite.ExpectedOutput = []byte("too large")
	if err := validateSuite(suite); err == nil {
		t.Fatal("suite accepted an expected output above its limit")
	}
}

type panicBackend struct{ referenceBackend }

func (*panicBackend) CheckReady(context.Context) error { panic("readiness") }
func (*panicBackend) Close() error                     { panic("close") }

func TestHarnessHelpersContainBackendPanics(t *testing.T) {
	backend := &panicBackend{}
	if err := safeReady(context.Background(), backend); err == nil {
		t.Fatal("readiness panic was not contained")
	}
	if err := safeClose(backend); err == nil {
		t.Fatal("close panic was not contained")
	}
	opened, inspector, err := safeOpen(
		context.Background(),
		func(context.Context) (Backend, Inspector, error) { panic("factory") },
	)
	if err == nil || opened != nil || inspector != nil {
		t.Fatal("factory panic was not contained")
	}
}

func TestValidateResultBoundsRejectsEveryReportedOverage(t *testing.T) {
	limits := executor.Limits{
		CPUMillis: 1, MemoryBytes: 1, DiskBytes: 1, PIDs: 1,
		ExecutionTime: time.Nanosecond, OutputBytes: 1,
	}
	cases := []executor.Result{
		{Output: []byte("xx")},
		{Usage: executor.Usage{CPUMillis: 2}},
		{Usage: executor.Usage{PeakMemory: 2}},
		{Usage: executor.Usage{DiskWritten: 2}},
		{Usage: executor.Usage{Duration: -1}},
		{Usage: executor.Usage{Duration: 2 * time.Nanosecond}},
	}
	for index, result := range cases {
		if err := validateResultBounds(limits, result); err == nil {
			t.Fatalf("case %d accepted an out-of-bounds result", index)
		}
	}
}
