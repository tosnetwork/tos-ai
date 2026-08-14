package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testExecutionID = "executor-test-task"

type containerdClientFunc func(
	context.Context,
	ContainerRequest,
	[]byte,
) (Result, error)

func (f containerdClientFunc) RunIsolated(
	ctx context.Context,
	request ContainerRequest,
	input []byte,
) (Result, error) {
	return f(ctx, request, input)
}

type typedNilContainerdClient struct{}

func (*typedNilContainerdClient) RunIsolated(
	context.Context,
	ContainerRequest,
	[]byte,
) (Result, error) {
	return Result{}, nil
}

func TestPolicyRequiresPinnedAllowlistedBoundedWorkload(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	ceiling := Limits{
		CPUMillis:      1000,
		MemoryBytes:    1 << 30,
		DiskBytes:      1 << 30,
		PIDs:           64,
		ExecutionTime:  time.Minute,
		OutputBytes:    1 << 20,
		GPUDeviceCount: 1,
	}
	policy := Policy{
		AllowedImages:       map[string]struct{}{digest: {}},
		MaxAllowedImages:    8,
		MaxEnvironment:      8,
		MaxArguments:        8,
		MaxAllowedHosts:     4,
		MaxStringBytes:      256,
		MaxInputBytes:       1024,
		Ceiling:             ceiling,
		PermitGPU:           true,
		RequireReadOnlyRoot: true,
	}
	spec := Spec{
		ImageDigest:     digest,
		Entrypoint:      []string{"/worker"},
		ReadOnlyRoot:    true,
		Network:         NetworkNone,
		AllowGPU:        true,
		UserID:          65532,
		GroupID:         65532,
		NoNewPrivileges: true,
		Limits:          ceiling,
	}
	if err := policy.Validate(spec); err != nil {
		t.Fatal(err)
	}
	spec.ReadOnlyRoot = false
	if err := policy.Validate(spec); err == nil {
		t.Fatal("writable root accepted")
	}
	spec.ReadOnlyRoot = true
	spec.Limits.MemoryBytes++
	if err := policy.Validate(spec); err == nil {
		t.Fatal("memory above policy accepted")
	}
}

func TestPolicyRejectsUnsafeContainerCapabilities(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	ceiling := Limits{
		CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 1 << 30,
		PIDs: 32, ExecutionTime: time.Minute, OutputBytes: 1 << 20,
	}
	policy := Policy{
		AllowedImages: map[string]struct{}{digest: {}}, MaxEnvironment: 4,
		MaxAllowedImages: 8,
		MaxArguments:     4, MaxAllowedHosts: 2, MaxStringBytes: 128,
		MaxInputBytes: 1024, Ceiling: ceiling, RequireReadOnlyRoot: true,
	}
	base := Spec{
		ImageDigest: digest, Entrypoint: []string{"/worker"}, ReadOnlyRoot: true,
		Network: NetworkNone, UserID: 1000, GroupID: 1000,
		NoNewPrivileges: true, Limits: ceiling,
	}
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"privileged", func(value *Spec) { value.Privileged = true }},
		{"host mount", func(value *Spec) { value.HostMounts = []Mount{{Source: "/etc", Target: "/host"}} }},
		{"network", func(value *Spec) { value.Network = NetworkAllowlist; value.AllowedHosts = []string{"example.com"} }},
		{"writable root", func(value *Spec) { value.ReadOnlyRoot = false }},
		{"unpinned image", func(value *Spec) { value.ImageDigest = "latest" }},
		{"root user", func(value *Spec) { value.UserID = 0 }},
		{"runtime socket", func(value *Spec) { value.ExposeRuntimeSocket = true }},
		{"new privileges", func(value *Spec) { value.NoNewPrivileges = false }},
		{"resource overflow", func(value *Spec) { value.Limits.MemoryBytes++ }},
		{"workspace traversal", func(value *Spec) {
			value.WorkspaceArchive = true
			value.WorkingDirectory = "/workspace/source/../escape"
		}},
		{"workspace without archive", func(value *Spec) { value.WorkingDirectory = "/workspace/source" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.edit(&value)
			if err := policy.Validate(value); err == nil {
				t.Fatal("unsafe spec accepted")
			}
		})
	}
}

func TestPolicyRejectsMalformedContainerStringsAndInvalidCeilings(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("f")
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"argument NUL", func(value *Spec) { value.Entrypoint = []string{"/worker\x00arg"} }},
		{"environment key NUL", func(value *Spec) { value.Environment = map[string]string{"BAD\x00KEY": "value"} }},
		{"environment key equals", func(value *Spec) { value.Environment = map[string]string{"BAD=KEY": "value"} }},
		{"environment value NUL", func(value *Spec) { value.Environment = map[string]string{"KEY": "bad\x00value"} }},
		{"allowed host NUL", func(value *Spec) {
			value.Network = NetworkAllowlist
			value.AllowedHosts = []string{"example.com\x00.invalid"}
		}},
	}
	policy.PermitNetwork = true
	policy.AllowedNetworkHosts = []string{"example.com"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := spec
			test.edit(&value)
			if err := policy.Validate(value); err == nil {
				t.Fatal("malformed container string accepted")
			}
		})
	}

	invalidPolicies := []Policy{
		func() Policy { value := policy; value.Ceiling.ExecutionTime = 0; return value }(),
		func() Policy { value := policy; value.Ceiling.ExecutionTime = -time.Second; return value }(),
		func() Policy { value := policy; value.Ceiling.MemoryBytes = 0; return value }(),
		func() Policy { value := policy; value.PermitGPU = true; value.Ceiling.GPUDeviceCount = 0; return value }(),
		func() Policy { value := policy; value.PermitNetwork = true; value.MaxAllowedHosts = 0; return value }(),
	}
	client := containerdClientFunc(func(
		context.Context,
		ContainerRequest,
		[]byte,
	) (Result, error) {
		return Result{}, nil
	})
	for index, invalid := range invalidPolicies {
		if _, err := NewPolicyExecutor(invalid, client); err == nil {
			t.Fatalf("invalid policy %d accepted", index)
		}
	}
}

func TestPolicyNetworkHostsUseExactOperatorAllowlist(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("3")
	policy.PermitNetwork = true
	policy.AllowedNetworkHosts = []string{
		"api.example.com:443",
		"10.0.0.8",
	}
	spec.Network = NetworkAllowlist
	spec.AllowedHosts = []string{"api.example.com:443"}
	if err := policy.Validate(spec); err != nil {
		t.Fatal(err)
	}

	unauthorized := spec
	unauthorized.AllowedHosts = []string{"API.example.com:443"}
	if err := policy.Validate(unauthorized); err == nil {
		t.Fatal("non-exact host variant accepted")
	}
	unauthorized.AllowedHosts = []string{"attacker.example:443"}
	if err := policy.Validate(unauthorized); err == nil {
		t.Fatal("host absent from operator allowlist accepted")
	}

	duplicate := spec
	duplicate.AllowedHosts = []string{
		"api.example.com:443",
		"api.example.com:443",
	}
	if err := policy.Validate(duplicate); err == nil {
		t.Fatal("duplicate requested host accepted")
	}

	invalidOperatorPolicies := []Policy{
		func() Policy {
			value := policy
			value.AllowedNetworkHosts = []string{"api.example.com:443", "api.example.com:443"}
			return value
		}(),
		func() Policy {
			value := policy
			value.AllowedNetworkHosts = []string{"https://api.example.com"}
			return value
		}(),
		func() Policy {
			value := policy
			value.AllowedNetworkHosts = []string{"*.example.com"}
			return value
		}(),
		func() Policy {
			value := policy
			value.AllowedNetworkHosts = []string{"api.example.com:70000"}
			return value
		}(),
		func() Policy {
			value := policy
			value.AllowedNetworkHosts = nil
			return value
		}(),
		func() Policy {
			value := policy
			value.PermitNetwork = false
			return value
		}(),
	}
	client := containerdClientFunc(func(
		context.Context,
		ContainerRequest,
		[]byte,
	) (Result, error) {
		return Result{}, nil
	})
	for index, invalid := range invalidOperatorPolicies {
		if _, err := NewPolicyExecutor(invalid, client); err == nil {
			t.Fatalf("invalid network policy %d accepted", index)
		}
	}

	overBound := policy
	overBound.AllowedNetworkHosts = make(
		[]string,
		MaxAllowedNetworkHostsHard+1,
	)
	for index := range overBound.AllowedNetworkHosts {
		overBound.AllowedNetworkHosts[index] = fmt.Sprintf("host-%d.example", index)
	}
	if _, err := NewPolicyExecutor(overBound, client); err == nil {
		t.Fatal("operator network allowlist above hard bound accepted")
	}
}

func TestPolicyExecutorClonesNetworkAllowlistAndSupportsConcurrency(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("4")
	operatorHosts := []string{"api.example.com:443", "10.0.0.8"}
	policy.PermitNetwork = true
	policy.AllowedNetworkHosts = operatorHosts
	spec.Network = NetworkAllowlist
	spec.AllowedHosts = []string{"api.example.com:443"}

	var calls atomic.Int32
	var malformed atomic.Int32
	executor, err := NewPolicyExecutor(
		policy,
		containerdClientFunc(func(
			_ context.Context,
			request ContainerRequest,
			_ []byte,
		) (Result, error) {
			calls.Add(1)
			if len(request.AllowedHosts) != 1 ||
				request.AllowedHosts[0] != "api.example.com:443" {
				malformed.Add(1)
			}
			return Result{ExitCode: 0}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Mutating caller-owned policy storage after construction must not change
	// the executor's operator-owned authorization snapshot.
	operatorHosts[0] = "attacker.example:443"
	policy.AllowedNetworkHosts[1] = "other.example:443"

	const workers = 64
	var wait sync.WaitGroup
	failures := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := executor.Execute(
				context.Background(),
				testExecutionID,
				spec,
				nil,
			); err != nil {
				failures <- err
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if calls.Load() != workers || malformed.Load() != 0 {
		t.Fatalf(
			"network backend calls=%d malformed=%d",
			calls.Load(),
			malformed.Load(),
		)
	}

	attacker := spec
	attacker.AllowedHosts = []string{"attacker.example:443"}
	if _, err := executor.Execute(
		context.Background(),
		testExecutionID,
		attacker,
		nil,
	); err == nil || calls.Load() != workers {
		t.Fatal("post-construction policy mutation changed authorization")
	}
}

func TestPolicyExecutorAppliesExecutionDeadlineAndPreservesCancellation(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("1")
	policy.Ceiling.ExecutionTime = 40 * time.Millisecond
	spec.Limits.ExecutionTime = policy.Ceiling.ExecutionTime

	deadlineSeen := make(chan time.Time, 1)
	executor, err := NewPolicyExecutor(
		policy,
		containerdClientFunc(func(
			ctx context.Context,
			_ ContainerRequest,
			_ []byte,
		) (Result, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				return Result{}, errors.New("missing execution deadline")
			}
			deadlineSeen <- deadline
			<-ctx.Done()
			return Result{}, errors.New("backend wrapped cancellation")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := executor.Execute(context.Background(), testExecutionID, spec, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("execution error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("execution deadline took too long: %v", elapsed)
	}
	if deadline := <-deadlineSeen; deadline.Sub(started) > spec.Limits.ExecutionTime+10*time.Millisecond {
		t.Fatalf("backend deadline exceeds execution limit: %v", deadline.Sub(started))
	}

	parent, cancel := context.WithCancel(context.Background())
	cancelExecutor, err := NewPolicyExecutor(
		policy,
		containerdClientFunc(func(
			ctx context.Context,
			_ ContainerRequest,
			_ []byte,
		) (Result, error) {
			cancel()
			<-ctx.Done()
			return Result{}, errors.New("backend cancellation detail")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cancelExecutor.Execute(parent, testExecutionID, spec, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("execution error = %v, want canceled", err)
	}
}

func TestPolicyExecutorRejectsLateResultFromCancellationIgnoringBackend(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("2")
	policy.Ceiling.ExecutionTime = 10 * time.Millisecond
	spec.Limits.ExecutionTime = policy.Ceiling.ExecutionTime
	executor, err := NewPolicyExecutor(
		policy,
		containerdClientFunc(func(
			context.Context,
			ContainerRequest,
			[]byte,
		) (Result, error) {
			time.Sleep(30 * time.Millisecond)
			return Result{ExitCode: 0}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := executor.Execute(context.Background(), testExecutionID, spec, nil); !errors.Is(err, context.DeadlineExceeded) || len(result.Output) != 0 {
		t.Fatalf("late backend result escaped boundary: %#v, %v", result, err)
	}
}

func TestPolicyExecutorClonesPolicyRequestInputAndResult(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("c")
	expectedExecutionDigest, err := ExecutionDigest(testExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	spec.Entrypoint = []string{"/worker", "serve"}
	spec.Environment = map[string]string{"MODE": "safe"}
	input := []byte("request")
	backendOutput := []byte("response")
	client := containerdClientFunc(func(
		ctx context.Context,
		request ContainerRequest,
		backendInput []byte,
	) (Result, error) {
		if ctx == nil || request.ExecutionDigest != expectedExecutionDigest ||
			request.ImageDigest != spec.ImageDigest ||
			request.Entrypoint[1] != "serve" ||
			request.Environment["MODE"] != "safe" ||
			string(backendInput) != "request" {
			t.Fatalf("unexpected container request: %#v", request)
		}
		request.Entrypoint[0] = "changed"
		request.Environment["MODE"] = "changed"
		backendInput[0] = 'X'
		return Result{
			ExitCode: 0,
			Output:   backendOutput,
			Usage: Usage{
				CPUMillis: 10, PeakMemory: 1024,
				DiskWritten: 2, Duration: time.Millisecond,
			},
		}, nil
	})
	executor, err := NewPolicyExecutor(policy, client)
	if err != nil {
		t.Fatal(err)
	}
	delete(policy.AllowedImages, spec.ImageDigest)
	result, err := executor.Execute(context.Background(), testExecutionID, spec, input)
	if err != nil {
		t.Fatal(err)
	}
	backendOutput[0] = 'X'
	if string(result.Output) != "response" ||
		spec.Entrypoint[0] != "/worker" ||
		spec.Environment["MODE"] != "safe" || string(input) != "request" {
		t.Fatalf(
			"execution aliases mutable state: result=%#v spec=%#v input=%q",
			result,
			spec,
			input,
		)
	}
}

func TestPolicyExecutorRejectsBeforeAndAfterBackendBoundary(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("d")
	var calls atomic.Int32
	validResult := Result{
		ExitCode: 0, Output: []byte("ok"),
		Usage: Usage{
			CPUMillis: 1, PeakMemory: 1,
			DiskWritten: 1, Duration: time.Millisecond,
		},
	}
	client := containerdClientFunc(func(
		context.Context,
		ContainerRequest,
		[]byte,
	) (Result, error) {
		calls.Add(1)
		return validResult, nil
	})
	executor, err := NewPolicyExecutor(policy, client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(
		context.Background(), "", spec, nil,
	); err == nil || calls.Load() != 0 {
		t.Fatal("missing execution identity reached the container runtime client")
	}
	if _, err := executor.Execute(
		context.Background(), strings.Repeat("x", MaxExecutionIDBytes+1), spec, nil,
	); err == nil || calls.Load() != 0 {
		t.Fatal("oversized execution identity reached the container runtime client")
	}
	unsafe := spec
	unsafe.Privileged = true
	if _, err := executor.Execute(
		context.Background(), testExecutionID, unsafe, nil,
	); err == nil || calls.Load() != 0 {
		t.Fatal("unsafe spec reached the container runtime client")
	}
	if _, err := executor.Execute(
		context.Background(), testExecutionID, spec,
		make([]byte, policy.MaxInputBytes+1),
	); err == nil || calls.Load() != 0 {
		t.Fatal("oversized input reached the container runtime client")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executor.Execute(canceled, testExecutionID, spec, nil); err == nil ||
		calls.Load() != 0 {
		t.Fatal("canceled request reached the container runtime client")
	}

	invalidResults := []Result{
		{ExitCode: -1},
		{ExitCode: 256},
		{ExitCode: 0, Output: make([]byte, spec.Limits.OutputBytes+1)},
		{ExitCode: 0, Usage: Usage{CPUMillis: spec.Limits.CPUMillis + 1}},
		{ExitCode: 0, Usage: Usage{PeakMemory: spec.Limits.MemoryBytes + 1}},
		{ExitCode: 0, Usage: Usage{DiskWritten: spec.Limits.DiskBytes + 1}},
		{ExitCode: 0, Usage: Usage{Duration: spec.Limits.ExecutionTime + 1}},
		{ExitCode: 0, Usage: Usage{Duration: -1}},
	}
	for index, result := range invalidResults {
		bad, err := NewPolicyExecutor(
			policy,
			containerdClientFunc(func(
				context.Context,
				ContainerRequest,
				[]byte,
			) (Result, error) {
				return result, nil
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bad.Execute(
			context.Background(), testExecutionID, spec, nil,
		); err == nil {
			t.Fatalf("invalid backend result %d was accepted", index)
		}
	}
}

func TestExecutionDigestIsDeterministicAndPathSafe(t *testing.T) {
	first, err := ExecutionDigest("task/../../with\x00controls")
	if err != nil {
		t.Fatal(err)
	}
	again, err := ExecutionDigest("task/../../with\x00controls")
	if err != nil {
		t.Fatal(err)
	}
	other, err := ExecutionDigest("other-task")
	if err != nil {
		t.Fatal(err)
	}
	if first != again || first == other || ValidateExecutionDigest(first) != nil ||
		strings.ContainsAny(first, "/\\\x00") {
		t.Fatalf("unsafe execution digests: first=%q again=%q other=%q", first, again, other)
	}
	for _, invalid := range []string{"", "sha256:ABC", "raw-task-id"} {
		if err := ValidateExecutionDigest(invalid); err == nil {
			t.Fatalf("accepted invalid execution digest %q", invalid)
		}
	}
}

func TestPolicyExecutorContainsBackendFailureAndSupportsConcurrency(t *testing.T) {
	policy, spec := validExecutionPolicyAndSpec("e")
	for name, client := range map[string]ContainerdClient{
		"error": containerdClientFunc(func(
			context.Context,
			ContainerRequest,
			[]byte,
		) (Result, error) {
			return Result{Output: []byte("secret")}, errors.New("secret backend error")
		}),
		"panic": containerdClientFunc(func(
			context.Context,
			ContainerRequest,
			[]byte,
		) (Result, error) {
			panic("secret panic")
		}),
	} {
		t.Run(name, func(t *testing.T) {
			executor, err := NewPolicyExecutor(policy, client)
			if err != nil {
				t.Fatal(err)
			}
			if result, err := executor.Execute(
				context.Background(), testExecutionID, spec, nil,
			); err == nil || len(result.Output) != 0 ||
				strings.Contains(err.Error(), "secret") {
				t.Fatalf("backend failure escaped boundary: %#v, %v", result, err)
			}
		})
	}
	var typedNil *typedNilContainerdClient
	if _, err := NewPolicyExecutor(policy, typedNil); err == nil {
		t.Fatal("typed nil container runtime client accepted")
	}

	var calls atomic.Int32
	executor, err := NewPolicyExecutor(
		policy,
		containerdClientFunc(func(
			context.Context,
			ContainerRequest,
			[]byte,
		) (Result, error) {
			calls.Add(1)
			return Result{ExitCode: 0}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 64
	var wait sync.WaitGroup
	failures := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := executor.Execute(
				context.Background(), testExecutionID, spec, nil,
			); err != nil {
				failures <- err
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if calls.Load() != workers {
		t.Fatalf("container runtime calls = %d", calls.Load())
	}
}

func validExecutionPolicyAndSpec(suffix string) (Policy, Spec) {
	digest := "sha256:" + strings.Repeat(suffix, 64)
	limits := Limits{
		CPUMillis: 1000, MemoryBytes: 1 << 20, DiskBytes: 1 << 20,
		PIDs: 16, ExecutionTime: time.Minute, OutputBytes: 1024,
	}
	policy := Policy{
		AllowedImages: map[string]struct{}{digest: {}}, MaxAllowedImages: 8,
		MaxEnvironment: 8, MaxArguments: 8, MaxAllowedHosts: 4,
		MaxStringBytes: 256, MaxInputBytes: 1024, Ceiling: limits,
		RequireReadOnlyRoot: true,
	}
	return policy, Spec{
		ImageDigest: digest, Entrypoint: []string{"/worker"},
		ReadOnlyRoot: true, Network: NetworkNone,
		UserID: 65532, GroupID: 65532, NoNewPrivileges: true,
		Limits: limits,
	}
}
