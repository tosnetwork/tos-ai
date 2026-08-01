// Package backendtest provides a reusable black-box lifecycle suite for
// audited isolated-execution backend implementations. Passing this suite is
// necessary but does not by itself prove kernel, network, GPU, or filesystem
// isolation; those properties still require privileged platform tests.
package backendtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const (
	MaxConformanceConcurrency = 32
	MaxConformanceTimeout     = 30 * time.Second
)

type Backend interface {
	executor.ContainerdClient
	executor.RuntimeReadiness
	io.Closer
}

// Snapshot is a backend-namespace inventory taken through an independently
// reviewed administrative test adapter. Residual counts include both active
// and stopped objects retained by the tested execution.
type Snapshot struct {
	ActiveWorkloads int
	Containers      int
	Tasks           int
	Snapshots       int
}

type Inspector interface {
	Snapshot(context.Context) (Snapshot, error)
}

type Factory func(context.Context) (Backend, Inspector, error)

type Suite struct {
	New                 Factory
	SuccessRequest      executor.ContainerRequest
	SuccessInput        []byte
	ExpectedOutput      []byte
	CancellationRequest executor.ContainerRequest
	CancellationInput   []byte
	StartTimeout        time.Duration
	ReturnTimeout       time.Duration
	InspectTimeout      time.Duration
	Concurrency         int
}

// Run executes readiness, successful cleanup, cancellation cleanup, and
// concurrent cleanup checks against fresh backend instances. A cancellation
// fixture must remain active until its context is canceled.
func Run(t *testing.T, suite Suite) {
	t.Helper()
	if err := validateSuite(suite); err != nil {
		t.Fatalf("invalid backend conformance suite: %v", err)
	}
	t.Run("readiness", func(t *testing.T) {
		backend, inspector := open(t, suite)
		defer closeBackend(t, backend)
		ctx, cancel := context.WithTimeout(context.Background(), suite.ReturnTimeout)
		defer cancel()
		if err := safeReady(ctx, backend); err != nil {
			t.Fatalf("backend readiness failed: %v", err)
		}
		canceled, stop := context.WithCancel(context.Background())
		stop()
		if err := safeReady(canceled, backend); err == nil {
			t.Fatal("backend readiness ignored a canceled context")
		}
		assertClean(t, inspector, suite.InspectTimeout)
	})
	t.Run("success-cleanup", func(t *testing.T) {
		backend, inspector := open(t, suite)
		defer closeBackend(t, backend)
		ctx, cancel := context.WithTimeout(context.Background(), suite.ReturnTimeout)
		defer cancel()
		result, err := safeRun(
			ctx, backend, cloneRequest(suite.SuccessRequest),
			append([]byte(nil), suite.SuccessInput...),
		)
		if err != nil {
			t.Fatalf("successful fixture failed: %v", err)
		}
		if result.ExitCode != 0 || !bytes.Equal(result.Output, suite.ExpectedOutput) {
			t.Fatalf("unexpected successful result: exit=%d output=%q", result.ExitCode, result.Output)
		}
		assertResultBounds(t, suite.SuccessRequest.Limits, result)
		assertClean(t, inspector, suite.InspectTimeout)
	})
	t.Run("cancellation-cleanup", func(t *testing.T) {
		backend, inspector := open(t, suite)
		defer closeBackend(t, backend)
		ctx, cancel := context.WithCancel(context.Background())
		resultChannel := make(chan error, 1)
		go func() {
			_, err := safeRun(
				ctx, backend, cloneRequest(suite.CancellationRequest),
				append([]byte(nil), suite.CancellationInput...),
			)
			resultChannel <- err
		}()
		waitForActive(t, inspector, suite.StartTimeout)
		cancel()
		select {
		case err := <-resultChannel:
			if err == nil {
				t.Fatal("canceled backend execution returned success")
			}
		case <-time.After(suite.ReturnTimeout):
			t.Fatal("backend did not return after cancellation")
		}
		assertClean(t, inspector, suite.InspectTimeout)
	})
	t.Run("duplicate-identity", func(t *testing.T) {
		backend, inspector := open(t, suite)
		defer closeBackend(t, backend)
		ctx, cancel := context.WithCancel(context.Background())
		firstResult := make(chan error, 1)
		go func() {
			_, err := safeRun(
				ctx, backend, cloneRequest(suite.CancellationRequest),
				append([]byte(nil), suite.CancellationInput...),
			)
			firstResult <- err
		}()
		waitForActive(t, inspector, suite.StartTimeout)

		duplicateRequest := cloneRequest(suite.SuccessRequest)
		duplicateRequest.ExecutionDigest = suite.CancellationRequest.ExecutionDigest
		duplicateContext, stopDuplicate := context.WithTimeout(
			context.Background(), suite.ReturnTimeout,
		)
		_, duplicateErr := safeRun(
			duplicateContext, backend, duplicateRequest,
			append([]byte(nil), suite.SuccessInput...),
		)
		stopDuplicate()
		if duplicateErr == nil {
			t.Fatal("backend accepted two active workloads with one execution digest")
		}
		assertActiveCount(t, inspector, suite.InspectTimeout, 1)

		cancel()
		select {
		case err := <-firstResult:
			if err == nil {
				t.Fatal("canceled primary backend execution returned success")
			}
		case <-time.After(suite.ReturnTimeout):
			t.Fatal("primary backend execution did not return after cancellation")
		}
		assertClean(t, inspector, suite.InspectTimeout)
	})
	t.Run("concurrent-cleanup", func(t *testing.T) {
		backend, inspector := open(t, suite)
		defer closeBackend(t, backend)
		ctx, cancel := context.WithTimeout(context.Background(), suite.ReturnTimeout)
		defer cancel()
		failures := make(chan error, suite.Concurrency)
		var wait sync.WaitGroup
		for index := range suite.Concurrency {
			request := cloneRequest(suite.SuccessRequest)
			executionDigest, err := executor.ExecutionDigest(
				suite.SuccessRequest.ExecutionDigest + "#" + strconv.Itoa(index),
			)
			if err != nil {
				t.Fatalf("derive concurrent execution digest: %v", err)
			}
			request.ExecutionDigest = executionDigest
			wait.Add(1)
			go func(request executor.ContainerRequest) {
				defer wait.Done()
				result, err := safeRun(
					ctx, backend, request,
					append([]byte(nil), suite.SuccessInput...),
				)
				if err == nil && (result.ExitCode != 0 ||
					!bytes.Equal(result.Output, suite.ExpectedOutput)) {
					err = errors.New("concurrent result mismatch")
				}
				if err == nil {
					err = validateResultBounds(suite.SuccessRequest.Limits, result)
				}
				if err != nil {
					failures <- err
				}
			}(request)
		}
		wait.Wait()
		close(failures)
		for err := range failures {
			t.Errorf("concurrent backend execution: %v", err)
		}
		assertClean(t, inspector, suite.InspectTimeout)
	})
}

func validateSuite(suite Suite) error {
	if suite.New == nil || suite.StartTimeout <= 0 ||
		suite.StartTimeout > MaxConformanceTimeout ||
		suite.ReturnTimeout <= 0 || suite.ReturnTimeout > MaxConformanceTimeout ||
		suite.InspectTimeout <= 0 || suite.InspectTimeout > MaxConformanceTimeout ||
		suite.Concurrency <= 0 || suite.Concurrency > MaxConformanceConcurrency ||
		!validRequest(suite.SuccessRequest) ||
		!validRequest(suite.CancellationRequest) ||
		uint64(len(suite.ExpectedOutput)) >
			suite.SuccessRequest.Limits.OutputBytes {
		return errors.New("missing factory or invalid hard bounds")
	}
	return nil
}

func validRequest(request executor.ContainerRequest) bool {
	return executor.ValidateExecutionDigest(request.ExecutionDigest) == nil &&
		validLimits(request.Limits)
}

func validLimits(limits executor.Limits) bool {
	return limits.CPUMillis > 0 && limits.MemoryBytes > 0 &&
		limits.DiskBytes > 0 && limits.PIDs > 0 &&
		limits.ExecutionTime > 0 && limits.OutputBytes > 0
}

func open(t *testing.T, suite Suite) (Backend, Inspector) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suite.ReturnTimeout)
	defer cancel()
	backend, inspector, err := safeOpen(ctx, suite.New)
	if err != nil || backend == nil || inspector == nil {
		t.Fatalf("open conformance backend: %v", err)
	}
	return backend, inspector
}

func closeBackend(t *testing.T, backend Backend) {
	t.Helper()
	if err := safeClose(backend); err != nil {
		t.Errorf("close conformance backend: %v", err)
	}
}

func waitForActive(t *testing.T, inspector Inspector, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		snapshot, err := inspector.Snapshot(ctx)
		cancel()
		if err != nil {
			t.Fatalf("inspect active backend workload: %v", err)
		}
		if snapshot.ActiveWorkloads > 0 {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatal("cancellation fixture did not become active")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertClean(t *testing.T, inspector Inspector, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	snapshot, err := inspector.Snapshot(ctx)
	if err != nil {
		t.Fatalf("inspect backend cleanup: %v", err)
	}
	if snapshot.ActiveWorkloads != 0 || snapshot.Containers != 0 ||
		snapshot.Tasks != 0 || snapshot.Snapshots != 0 {
		t.Fatalf("backend retained execution residue: %#v", snapshot)
	}
}

func assertActiveCount(
	t *testing.T,
	inspector Inspector,
	timeout time.Duration,
	want int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	snapshot, err := inspector.Snapshot(ctx)
	if err != nil {
		t.Fatalf("inspect backend active count: %v", err)
	}
	if snapshot.ActiveWorkloads != want {
		t.Fatalf("active backend workloads=%d want=%d", snapshot.ActiveWorkloads, want)
	}
}

func assertResultBounds(t *testing.T, limits executor.Limits, result executor.Result) {
	t.Helper()
	if err := validateResultBounds(limits, result); err != nil {
		t.Fatalf("backend result exceeded requested limits: %#v", result)
	}
}

func validateResultBounds(limits executor.Limits, result executor.Result) error {
	if uint64(len(result.Output)) > limits.OutputBytes ||
		result.Usage.CPUMillis > limits.CPUMillis ||
		result.Usage.PeakMemory > limits.MemoryBytes ||
		result.Usage.DiskWritten > limits.DiskBytes ||
		result.Usage.Duration < 0 || result.Usage.Duration > limits.ExecutionTime {
		return errors.New("backend result exceeded requested limits")
	}
	return nil
}

func safeReady(ctx context.Context, backend Backend) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.New("backend readiness panicked")
		}
	}()
	return backend.CheckReady(ctx)
}

func safeOpen(
	ctx context.Context,
	factory Factory,
) (backend Backend, inspector Inspector, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			backend = nil
			inspector = nil
			err = errors.New("backend factory panicked")
		}
	}()
	return factory(ctx)
}

func safeClose(backend Backend) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.New("backend close panicked")
		}
	}()
	return backend.Close()
}

func safeRun(
	ctx context.Context,
	backend Backend,
	request executor.ContainerRequest,
	input []byte,
) (result executor.Result, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = executor.Result{}
			err = errors.New("backend execution panicked")
		}
	}()
	return backend.RunIsolated(ctx, request, input)
}

func cloneRequest(request executor.ContainerRequest) executor.ContainerRequest {
	request.Entrypoint = append([]string(nil), request.Entrypoint...)
	request.AllowedHosts = append([]string(nil), request.AllowedHosts...)
	if request.Environment != nil {
		cloned := make(map[string]string, len(request.Environment))
		for key, value := range request.Environment {
			cloned[key] = value
		}
		request.Environment = cloned
	}
	return request
}
