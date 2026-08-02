package operatorconfig

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

type testIsolatedBackend struct {
	mutex          sync.Mutex
	request        executor.ContainerRequest
	input          []byte
	runs           int
	closes         int
	result         executor.Result
	runtimeErr     error
	readinessErr   error
	readinessCalls int
}

func (b *testIsolatedBackend) CheckReady(context.Context) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.readinessCalls++
	return b.readinessErr
}

func (b *testIsolatedBackend) RunIsolated(
	_ context.Context,
	request executor.ContainerRequest,
	input []byte,
) (executor.Result, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.request = request
	b.input = append([]byte(nil), input...)
	b.runs++
	return b.result, b.runtimeErr
}

func (b *testIsolatedBackend) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.closes++
	return nil
}

type testIsolatedFactory struct {
	backend IsolatedBackend
	config  IsolatedBackendConfig
	calls   int
	err     error
}

type panicIsolatedFactory struct{}

func (panicIsolatedFactory) Open(
	context.Context, IsolatedBackendConfig,
) (IsolatedBackend, error) {
	panic("private factory panic")
}

type panicCloseBackend struct{ testIsolatedBackend }

func (*panicCloseBackend) Close() error { panic("private close panic") }

func (f *testIsolatedFactory) Open(
	_ context.Context,
	config IsolatedBackendConfig,
) (IsolatedBackend, error) {
	f.calls++
	f.config = config
	if f.err != nil {
		return nil, f.err
	}
	return f.backend, nil
}

func validIsolatedConfig() string {
	digest := "sha256:" + strings.Repeat("1", 64)
	image := "sha256:" + strings.Repeat("2", 64)
	return `{
  "version": 1,
  "backend": {"type":"containerd","socketPath":"/run/tos-ai/containerd.sock","namespace":"tos-ai","snapshotter":"overlayfs","runtime":"io.containerd.runc.v2","fifoDir":"/run/tos-ai/containerd-fifos","maxActive":8},
  "capability": {
    "serviceId":"tos.ai.isolated","operation":"infer","model":"fixed-model",
    "modelDigest":"` + digest + `","runtime":"containerd","runtimeRevision":"containerd-2",
    "maxInputBytes":1024,"maxOutputBytes":2048,"acceptedPriorities":[4,5],
    "admission":{"ramBytes":1048576,"contextTokens":4096,"batchSize":1,"executionMillis":5000}
  },
  "container": {
    "imageReference":"registry.example/tos/infer@` + image + `",
    "imageDigest":"` + image + `","entrypoint":["/opt/tos/infer","--stdio"],
    "environment":{"MODEL_PATH":"/models/fixed"},"userId":65532,"groupId":65532,
    "network":"none","cpuMillis":2000,"diskBytes":1048576,"pids":32
  }
}`
}

func validGPUIsolatedConfig() string {
	value := validIsolatedConfig()
	value = strings.Replace(
		value,
		`"maxActive":8}`,
		`"maxActive":8,"gpuDevices":[{"alias":"gpu-a","cdiDevice":"nvidia.com/gpu=0"}]}`,
		1,
	)
	value = strings.Replace(
		value, `"ramBytes":1048576,`,
		`"ramBytes":1048576,"vramBytes":1073741824,`, 1,
	)
	value = strings.Replace(
		value, `"network":"none","cpuMillis":2000`,
		`"network":"none","allowGpu":true,"cpuMillis":2000`, 1,
	)
	return strings.Replace(value, `"pids":32`, `"pids":32,"gpuDevices":1`, 1)
}

func writeIsolatedConfig(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "isolated.json")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadIsolatedRuntimeBindsFixedPolicy(t *testing.T) {
	image := "sha256:" + strings.Repeat("2", 64)
	backend := &testIsolatedBackend{result: executor.Result{
		Output: []byte("result"),
		Usage:  executor.Usage{CPUMillis: 10, PeakMemory: 100, Duration: time.Millisecond},
	}}
	factory := &testIsolatedFactory{backend: backend}
	runtime, err := LoadIsolatedRuntime(
		context.Background(), writeIsolatedConfig(t, validIsolatedConfig()), factory,
	)
	if err != nil {
		t.Fatal(err)
	}
	if factory.calls != 1 || factory.config.Type != "containerd" ||
		factory.config.SocketPath != "/run/tos-ai/containerd.sock" ||
		factory.config.Namespace != "tos-ai" ||
		factory.config.Snapshotter != "overlayfs" ||
		factory.config.Runtime != "io.containerd.runc.v2" ||
		factory.config.FIFODir != "/run/tos-ai/containerd-fifos" ||
		factory.config.ImageReference != "registry.example/tos/infer@"+image ||
		factory.config.ImageDigest != image ||
		factory.config.MaxActive != 8 {
		t.Fatalf("unexpected backend factory call: %#v", factory)
	}
	capability := runtime.Adapter.Capability()
	if _, err := runtime.Adapter.Preflight(context.Background()); err != nil {
		t.Fatalf("isolated backend preflight: %v", err)
	}
	response, err := runtime.Adapter.Execute(context.Background(), airuntime.Request{
		RequestID: "request-1", Operation: capability.Operation,
		Model: capability.Model, Payload: []byte("input"), MaxOutputBytes: 1024,
	})
	if err != nil || string(response.Output) != "result" {
		t.Fatalf("execute: response=%#v err=%v", response, err)
	}
	backend.mutex.Lock()
	request := backend.request
	input := string(backend.input)
	readinessCalls := backend.readinessCalls
	backend.mutex.Unlock()
	if input != "input" || request.ImageDigest != "sha256:"+strings.Repeat("2", 64) ||
		!request.ReadOnlyRoot || !request.NoNewPrivileges || request.UserID == 0 ||
		request.Network != executor.NetworkNone || request.AllowGPU ||
		request.Limits.OutputBytes != 1024 || readinessCalls != 1 {
		t.Fatalf("fixed sandbox contract was not preserved: %#v input=%q", request, input)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	backend.mutex.Lock()
	closes := backend.closes
	backend.mutex.Unlock()
	if closes != 1 {
		t.Fatalf("backend close count=%d", closes)
	}
}

func TestLoadGPUIsolatedRuntimeBindsFixedOperatorCDIDevice(t *testing.T) {
	backend := &testIsolatedBackend{result: executor.Result{
		Output: []byte("gpu-result"),
		Usage: executor.Usage{
			CPUMillis: 10, PeakMemory: 100, Duration: time.Millisecond,
		},
	}}
	factory := &testIsolatedFactory{backend: backend}
	loaded, err := LoadIsolatedRuntime(
		context.Background(),
		writeIsolatedConfig(t, validGPUIsolatedConfig()), factory,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if !factory.config.PermitGPU || factory.config.Limits.GPUDeviceCount != 1 ||
		factory.config.GPUDevices["gpu-a"] != "nvidia.com/gpu=0" {
		t.Fatalf("GPU backend config=%#v", factory.config)
	}
	capability := loaded.Adapter.Capability()
	if capability.Admission.VRAMBytes != 1<<30 {
		t.Fatalf("GPU capability=%#v", capability)
	}
	response, err := loaded.Adapter.Execute(
		context.Background(), airuntime.Request{
			RequestID: "gpu-request", Operation: capability.Operation,
			Model: capability.Model, Payload: []byte("input"),
			MaxOutputBytes: 1024,
		},
	)
	if err != nil || string(response.Output) != "gpu-result" {
		t.Fatalf("GPU execute response=%#v err=%v", response, err)
	}
	backend.mutex.Lock()
	request := backend.request
	backend.mutex.Unlock()
	if !request.AllowGPU || request.Limits.GPUDeviceCount != 1 {
		t.Fatalf("GPU request=%#v", request)
	}
}

func TestLoadIsolatedRuntimeRejectsAuthorityExpansion(t *testing.T) {
	valid := validIsolatedConfig()
	tests := map[string]string{
		"unknown":               strings.Replace(valid, `"version": 1`, `"version": 1,"domain":"evil"`, 1),
		"duplicate":             strings.Replace(valid, `"version": 1`, `"version": 1,"version":1`, 1),
		"remote socket":         strings.Replace(valid, "/run/tos-ai/containerd.sock", "tcp://host:1", 1),
		"relative fifo":         strings.Replace(valid, "/run/tos-ai/containerd-fifos", "relative-fifos", 1),
		"unknown snapshotter":   strings.Replace(valid, `"snapshotter":"overlayfs"`, `"snapshotter":"native"`, 1),
		"unknown OCI runtime":   strings.Replace(valid, `"runtime":"io.containerd.runc.v2"`, `"runtime":"io.containerd.kata.v2"`, 1),
		"unpinned image ref":    strings.Replace(valid, "registry.example/tos/infer@sha256:", "registry.example/tos/infer:latest#sha256:", 1),
		"unknown backend":       strings.Replace(valid, `"type":"containerd"`, `"type":"shell"`, 1),
		"zero active limit":     strings.Replace(valid, `"maxActive":8`, `"maxActive":0`, 1),
		"excess active limit":   strings.Replace(valid, `"maxActive":8`, `"maxActive":257`, 1),
		"runtime mismatch":      strings.Replace(valid, `"runtime":"containerd"`, `"runtime":"other"`, 1),
		"root user":             strings.Replace(valid, `"userId":65532`, `"userId":0`, 1),
		"network without hosts": strings.Replace(valid, `"network":"none"`, `"network":"allowlist"`, 1),
		"gpu without vram":      strings.Replace(valid, `"network":"none"`, `"network":"none","allowGpu":true,"gpuDevices":1`, 1),
		"excessive duration":    strings.Replace(valid, `"executionMillis":5000`, `"executionMillis":3600001`, 1),
		"excessive ram":         strings.Replace(valid, `"ramBytes":1048576`, `"ramBytes":1099511627777`, 1),
		"host mount field":      strings.Replace(valid, `"network":"none"`, `"network":"none","hostMounts":[{"source":"/","target":"/host"}]`, 1),
	}
	gpu := validGPUIsolatedConfig()
	tests["duplicate gpu alias"] = strings.Replace(
		gpu, `{"alias":"gpu-a","cdiDevice":"nvidia.com/gpu=0"}`,
		`{"alias":"gpu-a","cdiDevice":"nvidia.com/gpu=0"},{"alias":"gpu-a","cdiDevice":"nvidia.com/gpu=1"}`, 1,
	)
	tests["duplicate cdi device"] = strings.Replace(
		gpu, `{"alias":"gpu-a","cdiDevice":"nvidia.com/gpu=0"}`,
		`{"alias":"gpu-a","cdiDevice":"nvidia.com/gpu=0"},{"alias":"gpu-b","cdiDevice":"nvidia.com/gpu=0"}`, 1,
	)
	tests["request exceeds gpu pool"] = strings.Replace(
		gpu, `"pids":32,"gpuDevices":1`, `"pids":32,"gpuDevices":2`, 1,
	)
	tests["unsafe cdi identifier"] = strings.Replace(
		gpu, `nvidia.com/gpu=0`, `../../dev/nvidia0`, 1,
	)
	tests["aggregate cdi selector"] = strings.Replace(
		gpu, `nvidia.com/gpu=0`, `nvidia.com/gpu=all`, 1,
	)
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			factory := &testIsolatedFactory{backend: &testIsolatedBackend{}}
			if runtime, err := LoadIsolatedRuntime(
				context.Background(), writeIsolatedConfig(t, data), factory,
			); err == nil || runtime != nil {
				t.Fatal("accepted unsafe isolated runtime configuration")
			}
			if factory.calls != 0 {
				t.Fatal("opened backend before validating all policy")
			}
		})
	}
}

func TestLoadIsolatedRuntimeContainsFactoryFailures(t *testing.T) {
	path := writeIsolatedConfig(t, validIsolatedConfig())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if runtime, err := LoadIsolatedRuntime(
		canceled, path, &testIsolatedFactory{backend: &testIsolatedBackend{}},
	); !errors.Is(err, context.Canceled) || runtime != nil {
		t.Fatalf("canceled load: runtime=%v err=%v", runtime, err)
	}
	factory := &testIsolatedFactory{err: errors.New("private backend detail")}
	if runtime, err := LoadIsolatedRuntime(context.Background(), path, factory); err == nil ||
		runtime != nil || strings.Contains(err.Error(), "private backend detail") {
		t.Fatalf("factory failure leaked or succeeded: runtime=%v err=%v", runtime, err)
	}
	if runtime, err := LoadIsolatedRuntime(context.Background(), path, nil); err == nil || runtime != nil {
		t.Fatal("accepted nil factory")
	}
	if runtime, err := LoadIsolatedRuntime(
		context.Background(), path, panicIsolatedFactory{},
	); err == nil || runtime != nil || strings.Contains(err.Error(), "private factory panic") {
		t.Fatalf("factory panic escaped: runtime=%v err=%v", runtime, err)
	}
	panicBackend := &panicCloseBackend{}
	loaded, err := LoadIsolatedRuntime(
		context.Background(), path,
		&testIsolatedFactory{backend: panicBackend},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := loaded.Close(); err == nil || strings.Contains(err.Error(), "private close panic") {
		t.Fatalf("close panic escaped or leaked: %v", err)
	}
}
