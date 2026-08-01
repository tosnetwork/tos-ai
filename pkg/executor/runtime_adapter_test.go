package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

type recordingContainerClient struct {
	mu       sync.Mutex
	requests []ContainerRequest
	inputs   [][]byte
	result   Result
	err      error
}

type runtimeReadinessFunc func(context.Context) error

func (f runtimeReadinessFunc) CheckReady(ctx context.Context) error {
	return f(ctx)
}

func (c *recordingContainerClient) RunIsolated(
	_ context.Context,
	request ContainerRequest,
	input []byte,
) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, request)
	c.inputs = append(c.inputs, append([]byte(nil), input...))
	result := c.result
	result.Output = append([]byte(nil), result.Output...)
	return result, c.err
}

func TestRuntimeAdapterMapsFixedSpecAndResult(t *testing.T) {
	client := &recordingContainerClient{result: Result{
		Output: []byte("generated"),
		Usage: Usage{
			CPUMillis: 20, PeakMemory: 1024, DiskWritten: 10,
			Duration: 1250 * time.Millisecond,
		},
	}}
	adapter, config := validRuntimeAdapter(t, client)
	config.Spec.Entrypoint[0] = "/attacker"
	config.Spec.Environment["MODEL"] = "attacker"
	config.Capability.AcceptedPriorities[0] = airuntime.PriorityBackground

	request := airuntime.Request{
		RequestID: "request-0001", Operation: "generate", Model: "model-a",
		Payload: []byte("prompt"), MaxOutputBytes: 512,
	}
	response, err := adapter.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Payload[0] = 'X'
	if string(response.Output) != "generated" ||
		response.Usage.InputBytes != uint64(len("prompt")) ||
		response.Usage.OutputBytes != uint64(len("generated")) ||
		response.Usage.ExecutionMillis != 1250 ||
		response.ModelRevision != config.Preflight.ModelDigest ||
		response.RuntimeRevision != "runtime-r1" {
		t.Fatalf("unexpected isolated response: %#v", response)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 1 ||
		client.requests[0].ExecutionDigest == "" ||
		client.requests[0].Entrypoint[0] != "/worker" ||
		client.requests[0].Environment["MODEL"] != "model-a" ||
		client.requests[0].Limits.OutputBytes != 512 ||
		string(client.inputs[0]) != "prompt" {
		t.Fatalf("operator spec was not preserved: %#v %#v", client.requests, client.inputs)
	}
	capability := adapter.Capability()
	if capability.AcceptedPriorities[0] != airuntime.PriorityExternalService {
		t.Fatal("runtime capability aliases caller configuration")
	}
	capability.AcceptedPriorities[0] = airuntime.PriorityBackground
	if adapter.Capability().AcceptedPriorities[0] != airuntime.PriorityExternalService {
		t.Fatal("returned runtime capability aliases adapter state")
	}
}

func TestRuntimeAdapterRejectsUnsafeOrMismatchedConfiguration(t *testing.T) {
	client := &recordingContainerClient{}
	_, base := validRuntimeAdapter(t, client)
	tests := []struct {
		name string
		edit func(*RuntimeAdapterConfig)
	}{
		{"nil executor", func(value *RuntimeAdapterConfig) { value.Executor = nil }},
		{"nil readiness", func(value *RuntimeAdapterConfig) { value.Readiness = nil }},
		{"memory mismatch", func(value *RuntimeAdapterConfig) { value.Spec.Limits.MemoryBytes++ }},
		{"time mismatch", func(value *RuntimeAdapterConfig) { value.Spec.Limits.ExecutionTime-- }},
		{"output mismatch", func(value *RuntimeAdapterConfig) { value.Spec.Limits.OutputBytes-- }},
		{"GPU mismatch", func(value *RuntimeAdapterConfig) { value.Spec.AllowGPU = true; value.Spec.Limits.GPUDeviceCount = 1 }},
		{"root identity", func(value *RuntimeAdapterConfig) { value.Spec.UserID = 0 }},
		{"unapproved image", func(value *RuntimeAdapterConfig) { value.Spec.ImageDigest = "sha256:" + repeatHex("b") }},
		{"bad preflight", func(value *RuntimeAdapterConfig) { value.Preflight.ModelDigest = "sha256:" + repeatHex("c") }},
		{"unproven observed evidence", func(value *RuntimeAdapterConfig) { value.Preflight.DigestEvidence = airuntime.BindingLocallyObserved }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			value.Capability = cloneRuntimeCapability(base.Capability)
			value.Spec = cloneSpec(base.Spec)
			test.edit(&value)
			if _, err := NewRuntimeAdapter(value); err == nil {
				t.Fatal("unsafe isolated runtime configuration accepted")
			}
		})
	}
}

func TestRuntimeAdapterPreflightChecksBackendAndContainsFailures(t *testing.T) {
	client := &recordingContainerClient{}
	adapter, config := validRuntimeAdapter(t, client)
	preflight, err := adapter.Preflight(context.Background())
	if err != nil || preflight.Model != config.Capability.Model {
		t.Fatalf("healthy preflight=%#v err=%v", preflight, err)
	}
	privateErr := errors.New("private readiness detail")
	config.Readiness = runtimeReadinessFunc(func(context.Context) error {
		return privateErr
	})
	adapter, err = NewRuntimeAdapter(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorUnavailable ||
		errors.Is(err, privateErr) {
		t.Fatalf("readiness detail escaped: %v", err)
	}
	config.Readiness = runtimeReadinessFunc(func(context.Context) error {
		panic("private readiness panic")
	})
	adapter, err = NewRuntimeAdapter(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorUnavailable {
		t.Fatalf("readiness panic escaped: %v", err)
	}
	config.Readiness = runtimeReadinessFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return errors.New("private canceled detail")
	})
	adapter, err = NewRuntimeAdapter(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := adapter.Preflight(ctx); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("readiness cancellation mapping=%v", err)
	}
	config.Readiness = runtimeReadinessFunc(func(context.Context) error {
		time.Sleep(15 * time.Millisecond)
		return nil
	})
	adapter, err = NewRuntimeAdapter(config)
	if err != nil {
		t.Fatal(err)
	}
	lateContext, stopLate := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stopLate()
	if _, err := adapter.Preflight(lateContext); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("late readiness result accepted: %v", err)
	}
}

func TestRuntimeAdapterMapsErrorsAndBoundsRequests(t *testing.T) {
	client := &recordingContainerClient{}
	adapter, _ := validRuntimeAdapter(t, client)
	invalid := airuntime.Request{
		RequestID: "request-0001", Operation: "generate", Model: "other",
		Payload: []byte("prompt"), MaxOutputBytes: 512,
	}
	if _, err := adapter.Execute(context.Background(), invalid); airuntime.ErrorKindOf(err) != airuntime.ErrorInvalid {
		t.Fatalf("invalid request error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Execute(canceled, validRuntimeRequest()); airuntime.ErrorKindOf(err) != airuntime.ErrorCanceled {
		t.Fatalf("canceled request error = %v", err)
	}

	expired, expire := context.WithDeadline(
		context.Background(), time.Now().Add(-time.Second),
	)
	defer expire()
	if _, err := adapter.Execute(expired, validRuntimeRequest()); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("deadline error = %v", err)
	}
	blockingAdapter, _ := validRuntimeAdapter(
		t,
		containerdClientFunc(func(
			ctx context.Context,
			_ ContainerRequest,
			_ []byte,
		) (Result, error) {
			<-ctx.Done()
			return Result{}, errors.New("private cancellation detail")
		}),
	)
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := blockingAdapter.Execute(deadline, validRuntimeRequest()); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("in-flight deadline error = %v", err)
	}

	client.mu.Lock()
	client.err = errors.New("private backend detail")
	client.mu.Unlock()
	if _, err := adapter.Execute(context.Background(), validRuntimeRequest()); airuntime.ErrorKindOf(err) != airuntime.ErrorInternal ||
		errors.Is(err, client.err) {
		t.Fatalf("backend detail escaped runtime boundary: %v", err)
	}

	client.mu.Lock()
	client.err = nil
	client.result = Result{ExitCode: 7}
	client.mu.Unlock()
	if _, err := adapter.Execute(context.Background(), validRuntimeRequest()); airuntime.ErrorKindOf(err) != airuntime.ErrorRemote {
		t.Fatalf("nonzero exit error = %v", err)
	}
}

func TestRuntimeAdapterSupportsConcurrentStatelessExecution(t *testing.T) {
	client := &recordingContainerClient{result: Result{Output: []byte("ok")}}
	adapter, _ := validRuntimeAdapter(t, client)
	const workers = 64
	var wait sync.WaitGroup
	failures := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := adapter.Execute(
				context.Background(), validRuntimeRequest(),
			)
			if err == nil && string(response.Output) != "ok" {
				err = errors.New("unexpected isolated output")
			}
			if err != nil {
				failures <- err
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != workers {
		t.Fatalf("isolated calls = %d, want %d", len(client.requests), workers)
	}
}

func validRuntimeAdapter(
	t *testing.T,
	client ContainerdClient,
) (*RuntimeAdapter, RuntimeAdapterConfig) {
	t.Helper()
	digest := "sha256:" + repeatHex("a")
	capability := airuntime.Capability{
		ServiceID: "tos.ai.isolated", Operation: "generate", Model: "model-a",
		ModelDigest: digest, Runtime: "containerd", RuntimeRevision: "runtime-r1",
		MaxInputBytes: 1024, MaxOutputBytes: 1024,
		AcceptedPriorities: []airuntime.Priority{airuntime.PriorityExternalService},
		Admission: admission.Resources{
			RAMBytes: 4096, ContextTokens: 2048, BatchSize: 1,
			ExecutionTime: time.Minute,
		},
	}
	spec := Spec{
		ImageDigest: digest, Entrypoint: []string{"/worker"},
		Environment:  map[string]string{"MODEL": "model-a"},
		ReadOnlyRoot: true, Network: NetworkNone,
		UserID: 1000, GroupID: 1000, NoNewPrivileges: true,
		Limits: Limits{
			CPUMillis: 60_000, MemoryBytes: capability.Admission.RAMBytes,
			DiskBytes: 1 << 20, PIDs: 32,
			ExecutionTime: capability.Admission.ExecutionTime,
			OutputBytes:   capability.MaxOutputBytes,
		},
	}
	policy, err := NewPolicyExecutor(Policy{
		AllowedImages: map[string]struct{}{digest: {}}, MaxAllowedImages: 1,
		MaxEnvironment: 4, MaxArguments: 4, MaxAllowedHosts: 0,
		MaxStringBytes: 256, MaxInputBytes: capability.MaxInputBytes,
		Ceiling: spec.Limits, RequireReadOnlyRoot: true,
	}, client)
	if err != nil {
		t.Fatal(err)
	}
	config := RuntimeAdapterConfig{
		Capability: capability,
		Preflight: airuntime.Preflight{
			Model: capability.Model, ModelDigest: capability.ModelDigest,
			DigestEvidence: airuntime.BindingDeclared,
		},
		Spec: spec, Executor: policy,
		Readiness: runtimeReadinessFunc(func(context.Context) error { return nil }),
	}
	adapter, err := NewRuntimeAdapter(config)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, config
}

func validRuntimeRequest() airuntime.Request {
	return airuntime.Request{
		RequestID: "request-0001", Operation: "generate", Model: "model-a",
		Payload: []byte("prompt"), MaxOutputBytes: 512,
	}
}

func repeatHex(value string) string {
	output := ""
	for range 64 {
		output += value
	}
	return output
}
