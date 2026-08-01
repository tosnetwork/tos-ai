package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/internal/worker"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/modelactivation"
	"github.com/tosnetwork/tos-ai/pkg/modelapproval"
	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	"github.com/tosnetwork/tos-ai/pkg/probe"
	"github.com/tosnetwork/tos-ai/pkg/profile/textgeneration"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/update"
)

func TestConfiguredAdaptersRequireExplicitModeAndRejectMixing(t *testing.T) {
	if _, err := configuredRuntimes("", "", false, 0); err == nil {
		t.Fatal("missing production configuration failed open to mock")
	}
	runtimes, err := configuredRuntimes("", "", true, 0)
	if err != nil || len(runtimes.adapters) != 1 ||
		runtimes.adapters[0].Capability().Runtime != "mock" {
		t.Fatalf("explicit development runtimes=%v err=%v", runtimes, err)
	}
	if _, err := configuredRuntimes(
		"/private/runtime.json", "", true, 0,
	); err == nil {
		t.Fatal("mock and production runtime configuration accepted together")
	}
	if _, err := configuredRuntimes(
		"", "/private/trust.json", false, 0,
	); err == nil {
		t.Fatal("model trust without a production runtime was accepted")
	}
	if _, err := configuredRuntimes(
		"", "", false, time.Millisecond,
	); err == nil {
		t.Fatal("mock delay enabled the development runtime implicitly")
	}
	if _, err := configuredRuntimes(
		"/private/runtime.json", "", false, time.Millisecond,
	); err == nil {
		t.Fatal("mock delay was mixed with production configuration")
	}
	for _, delay := range []time.Duration{-time.Nanosecond, time.Minute + 1} {
		if _, err := configuredRuntimes("", "", true, delay); err == nil {
			t.Fatalf("out-of-bounds development mock delay accepted: %v", delay)
		}
	}
}

func TestConfiguredAdaptersCanRequireSignedCachedModel(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(t.TempDir(), "models")
	manager, err := modelmanager.New(modelmanager.Config{
		RootDir: cacheDir, Target: "linux/amd64/cuda",
		CurrentSecurityRevision: 9, MaxModels: 4, MaxTotalBytes: 1 << 20,
		Signers: map[string]ed25519.PublicKey{"models": publicKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("worker-approved-model")
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	now := time.Now()
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "approved.gguf",
		Digest: digest, SizeBytes: uint64(len(data)),
		Target: "linux/amd64/cuda", SecurityRevision: 9,
		IssuedAt:  now.Add(-time.Minute).UnixMilli(),
		ExpiresAt: now.Add(time.Hour).UnixMilli(), KeyID: "models",
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Import(
		context.Background(), manifest, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	runtimePath := writeWorkerPrivate(t, "runtime.json", fmt.Sprintf(`{
		"version":1,
		"adapters":[{
			"type":"ollama",
			"baseUrl":"http://127.0.0.1:11434",
			"model":"approved",
			"modelDigest":%q
		}]
	}`, digest))
	trustPath := writeWorkerPrivate(t, "trust.json", fmt.Sprintf(`{
		"version":1,
		"cacheDir":%q,
		"target":"linux/amd64/cuda",
		"currentSecurityRevision":9,
		"maxModels":4,
		"maxTotalBytes":1048576,
		"verifyTimeoutMillis":1000,
		"signers":[{"keyId":"models","publicKey":%q}]
	}`, cacheDir, base64.StdEncoding.EncodeToString(publicKey)))
	runtimes, err := configuredRuntimes(runtimePath, trustPath, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtimes.adapters) != 1 {
		t.Fatalf("approved adapter count=%d", len(runtimes.adapters))
	}
	if runtimes.profilePlan == nil || runtimes.profilePlan.Len() != 1 ||
		!runtimes.profilePlan.Supports(
			textgeneration.ProfileID,
			textgeneration.ProfileVersion,
			nil,
			textgeneration.Operation,
		) {
		t.Fatal("production runtime did not retain its validated profile plan")
	}
	if _, ok := runtimes.adapters[0].(*modelapproval.Adapter); !ok {
		t.Fatalf(
			"runtime adapter is not model guarded: %T",
			runtimes.adapters[0],
		)
	}
	closeRuntimeAdapters(runtimes.adapters)
	if err := runtimes.closeRuntimeState(context.Background()); err != nil {
		t.Fatal(err)
	}

	missingRuntime := strings.Replace(
		string(mustRead(t, runtimePath)), digest,
		"sha256:"+strings.Repeat("f", 64), 1,
	)
	missingPath := writeWorkerPrivate(t, "missing-runtime.json", missingRuntime)
	if _, err := configuredRuntimes(
		missingPath, trustPath, false, 0,
	); err == nil || strings.Contains(err.Error(), cacheDir) {
		t.Fatalf("missing approved digest error=%v", err)
	}
}

func TestConfiguredRuntimesActivatesRecoversAndCleansApprovedOllamaModel(
	t *testing.T,
) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(t.TempDir(), "models")
	data := []byte("worker-activated-approved-gguf")
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	manager, err := modelmanager.New(modelmanager.Config{
		RootDir: cacheDir, Target: "linux/amd64/cuda",
		CurrentSecurityRevision: 11, MaxModels: 4, MaxTotalBytes: 1 << 20,
		Signers: map[string]ed25519.PublicKey{"models": publicKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "approved.gguf",
		Digest: digest, SizeBytes: uint64(len(data)),
		Target: "linux/amd64/cuda", SecurityRevision: 11,
		IssuedAt:  now.Add(-time.Minute).UnixMilli(),
		ExpiresAt: now.Add(time.Hour).UnixMilli(), KeyID: "models",
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Import(
		context.Background(), manifest, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	fake := newWorkerOllamaRuntime()
	server := httptest.NewServer(fake)
	defer server.Close()
	stateDir := filepath.Join(t.TempDir(), "activation")
	runtimePath := writeWorkerPrivate(t, "activation-runtime.json", fmt.Sprintf(`{
		"version":1,
		"activation":{
			"stateDir":%q,
			"operationTimeoutMillis":2000,
			"cleanupTimeoutMillis":1000
		},
		"adapters":[{
			"type":"ollama",
			"baseUrl":%q,
			"model":"approved",
			"modelDigest":%q,
			"timeoutMillis":2000,
			"connectTimeoutMillis":500,
			"activation":{"slotId":"primary","maxModelBytes":1048576}
		}]
	}`, stateDir, server.URL, digest))
	trustPath := writeWorkerPrivate(t, "activation-trust.json", fmt.Sprintf(`{
		"version":1,
		"cacheDir":%q,
		"target":"linux/amd64/cuda",
		"currentSecurityRevision":11,
		"maxModels":4,
		"maxTotalBytes":1048576,
		"verifyTimeoutMillis":1000,
		"signers":[{"keyId":"models","publicKey":%q}]
	}`, cacheDir, base64.StdEncoding.EncodeToString(publicKey)))

	start := func() *runtimeResources {
		t.Helper()
		resources, err := configuredRuntimes(
			runtimePath, trustPath, false, 0,
		)
		if err != nil {
			t.Fatal(err)
		}
		status, err := resources.activation.Status("primary")
		if err != nil || status.State != modelactivation.StateActive ||
			status.ModelDigest != digest ||
			status.DigestEvidence != airuntime.BindingLocallyObserved {
			t.Fatalf("activation status=%#v err=%v", status, err)
		}
		preflight, err := resources.adapters[0].Preflight(context.Background())
		if err != nil || preflight.Model != "approved" ||
			preflight.ModelDigest != digest ||
			preflight.DigestEvidence != airuntime.BindingLocallyObserved {
			t.Fatalf("preflight=%#v err=%v", preflight, err)
		}
		return resources
	}
	stop := func(resources *runtimeResources) {
		t.Helper()
		closeRuntimeAdapters(resources.adapters)
		if err := resources.closeRuntimeState(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	first := start()
	stop(first)
	fake.mu.Lock()
	if fake.uploads != 1 || fake.creates != 1 || fake.deletes != 1 ||
		len(fake.models) != 0 {
		t.Fatalf(
			"first lifecycle uploads=%d creates=%d deletes=%d models=%v",
			fake.uploads, fake.creates, fake.deletes, fake.models,
		)
	}
	fake.mu.Unlock()

	second := start()
	stop(second)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.uploads != 1 || fake.creates != 2 || fake.deletes != 2 ||
		len(fake.models) != 0 {
		t.Fatalf(
			"recovery lifecycle uploads=%d creates=%d deletes=%d models=%v",
			fake.uploads, fake.creates, fake.deletes, fake.models,
		)
	}
}

func TestConfiguredRuntimesRequiresTrustAndSeparateActivationState(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	cacheDir := filepath.Join(t.TempDir(), "models")
	stateDir := filepath.Join(cacheDir, "activation")
	runtimePath := writeWorkerPrivate(t, "runtime.json", fmt.Sprintf(`{
		"version":1,
		"activation":{"stateDir":%q},
		"adapters":[{
			"type":"ollama","baseUrl":"http://127.0.0.1:11434",
			"model":"approved","modelDigest":%q,
			"activation":{"slotId":"primary"}
		}]
	}`, stateDir, digest))
	if _, err := configuredRuntimes(
		runtimePath, "", false, 0,
	); err == nil {
		t.Fatal("activation without signed model trust succeeded")
	}
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trustPath := writeWorkerPrivate(t, "trust.json", fmt.Sprintf(`{
		"version":1,"cacheDir":%q,"target":"linux/amd64/cuda",
		"currentSecurityRevision":1,"maxModels":4,
		"maxTotalBytes":1048576,
		"signers":[{"keyId":"models","publicKey":%q}]
	}`, cacheDir, base64.StdEncoding.EncodeToString(publicKey)))
	if _, err := configuredRuntimes(
		runtimePath, trustPath, false, 0,
	); err == nil {
		t.Fatal("overlapping model cache and activation state succeeded")
	}
	trust, err := operatorconfig.LoadModelTrust(trustPath)
	if err != nil {
		t.Fatalf("failed startup retained model cache ownership: %v", err)
	}
	if err := trust.Manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreflightWaitersAreBoundedByConnectionsAndHardLimit(t *testing.T) {
	if got := preflightWaiters(8); got != 8 {
		t.Fatalf("preflight waiters = %d", got)
	}
	if got := preflightWaiters(worker.MaxPreflightWaitersHard + 1); got != worker.MaxPreflightWaitersHard {
		t.Fatalf("hard-bounded preflight waiters = %d", got)
	}
	if got := preflightWaiters(0); got != 0 {
		t.Fatalf("invalid connection count was hidden: %d", got)
	}
}

func TestActivationStartupAndDirectoryBounds(t *testing.T) {
	activation := &operatorconfig.ActivationConfiguration{
		Controller: modelactivation.Config{
			OperationTimeout: 2 * time.Second,
		},
		Desired: make([]operatorconfig.DesiredActivation, 2),
	}
	if timeout := activationStartupTimeout(activation); timeout != 26*time.Second {
		t.Fatalf("activation startup timeout=%v", timeout)
	}
	resources := &runtimeResources{
		configuration: &operatorconfig.Configuration{
			Activation: activation,
		},
	}
	activation.Controller.CleanupTimeout = 3 * time.Second
	if timeout := resources.activationCleanupTimeout(); timeout != 6*time.Second {
		t.Fatalf("activation cleanup timeout=%v", timeout)
	}
	activation.Controller.OperationTimeout = 10 * time.Minute
	activation.Controller.CleanupTimeout = time.Minute
	activation.Desired = make(
		[]operatorconfig.DesiredActivation, modelactivation.MaxSlotsHard,
	)
	if timeout := activationStartupTimeout(activation); timeout != 30*time.Minute {
		t.Fatalf("activation startup hard timeout=%v", timeout)
	}
	if timeout := resources.activationCleanupTimeout(); timeout != 30*time.Minute {
		t.Fatalf("activation cleanup hard timeout=%v", timeout)
	}
	root := filepath.Join(t.TempDir(), "models")
	if !directoriesOverlap(root, root) ||
		!directoriesOverlap(root, filepath.Join(root, "state")) ||
		directoriesOverlap(root, root+"-state") {
		t.Fatal("activation/cache directory overlap classification failed")
	}
}

func TestDefaultAdmissionConfigSupportsNoGPUAndHasHardBounds(t *testing.T) {
	report := probe.Report{
		Host:   probe.Host{MemoryBytes: 16 << 30},
		NVIDIA: probe.NVIDIAReport{Status: "unavailable"},
	}
	config, err := defaultAdmissionConfig(report, 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if config.Capacity.VRAMBytes != 0 || config.MaxConcurrent != 2 ||
		config.OwnerReserved.RAMBytes == 0 {
		t.Fatalf("unexpected config: %#v", config)
	}
	if _, err := admission.New(config); err != nil {
		t.Fatal(err)
	}
	if _, err := defaultAdmissionConfig(report, admission.MaxConcurrentHard+1, 8); err == nil {
		t.Fatal("worker hard limit accepted")
	}
}

func TestDefaultAdmissionConfigRejectsInsufficientRAM(t *testing.T) {
	_, err := defaultAdmissionConfig(probe.Report{
		Host: probe.Host{MemoryBytes: 32 << 20},
	}, 1, 1)
	if err == nil {
		t.Fatal("insufficient RAM accepted")
	}
	_, err = defaultAdmissionConfig(probe.Report{
		Host: probe.Host{MemoryBytes: 16 << 30},
		NVIDIA: probe.NVIDIAReport{Devices: []probe.NVIDIADevice{{
			VRAMBytes: 1, VRAMUsedBytes: 2,
		}}},
	}, 1, 1)
	if err == nil {
		t.Fatal("invalid observed VRAM accepted")
	}
}

func TestConfiguredTerminalPolicyRequiresProductionConfig(t *testing.T) {
	report := probe.Report{
		Host:   probe.Host{MemoryBytes: 16 << 30},
		NVIDIA: probe.NVIDIAReport{Status: "unavailable"},
	}
	flags := defaultTerminalPolicyFlags()
	if _, err := configuredTerminalPolicy("", false, report, flags); err == nil {
		t.Fatal("production mode accepted a missing terminal policy")
	}
	policy, err := configuredTerminalPolicy("", true, report, flags)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Workers != defaultWorkers ||
		policy.MaxConnections != defaultMaxConnections ||
		policy.Admission.Capacity.VRAMBytes != 0 ||
		policy.ResourceMonitor.Interval != 10*time.Second ||
		policy.ResourceMonitor.Timeout != 5*time.Second {
		t.Fatalf("development policy=%#v", policy)
	}
	if _, err := admission.New(policy.Admission); err != nil {
		t.Fatal(err)
	}
	invalid := []terminalPolicyFlags{
		func() terminalPolicyFlags {
			value := flags
			value.maxConnections = 0
			return value
		}(),
		func() terminalPolicyFlags {
			value := flags
			value.preflightRefresh = worker.MinPreflightRefresh - time.Nanosecond
			return value
		}(),
		func() terminalPolicyFlags {
			value := flags
			value.preflightWorkers = worker.MaxPreflightWorkersHard + 1
			return value
		}(),
	}
	for _, value := range invalid {
		if _, err := configuredTerminalPolicy("", true, report, value); err == nil {
			t.Fatalf("invalid development terminal settings accepted: %#v", value)
		}
	}
}

func TestConfiguredTerminalPolicyValidatesObservedCapacity(t *testing.T) {
	report := probe.Report{
		Host: probe.Host{MemoryBytes: 16 << 30},
		NVIDIA: probe.NVIDIAReport{
			Status: "available",
			Devices: []probe.NVIDIADevice{{
				VRAMBytes: 8 << 30, VRAMUsedBytes: 2 << 30,
			}},
		},
	}
	path := writeWorkerPrivate(
		t, "terminal-policy.json", workerTerminalPolicyJSON(8<<30, 4<<30),
	)
	policy, err := configuredTerminalPolicy(
		path, false, report, defaultTerminalPolicyFlags(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Admission.Capacity.RAMBytes != 8<<30 ||
		policy.Admission.Capacity.VRAMBytes != 4<<30 ||
		policy.OwnerReservedWorkers != 1 {
		t.Fatalf("loaded admission=%#v", policy.Admission)
	}

	tests := []struct {
		name   string
		path   string
		report probe.Report
	}{
		{
			name: "RAM headroom",
			path: writeWorkerPrivate(
				t, "excessive-ram.json",
				workerTerminalPolicyJSON(16<<30, 0),
			),
			report: probe.Report{Host: probe.Host{MemoryBytes: 16 << 30}},
		},
		{
			name:   "missing GPU",
			path:   path,
			report: probe.Report{Host: probe.Host{MemoryBytes: 16 << 30}},
		},
		{
			name: "invalid observed GPU",
			path: path,
			report: probe.Report{
				Host: probe.Host{MemoryBytes: 16 << 30},
				NVIDIA: probe.NVIDIAReport{Devices: []probe.NVIDIADevice{{
					VRAMBytes: 2 << 30, VRAMUsedBytes: 3 << 30,
				}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := configuredTerminalPolicy(
				test.path, false, test.report, defaultTerminalPolicyFlags(),
			)
			if err == nil || strings.Contains(err.Error(), test.path) {
				t.Fatalf("unsafe observed capacity error=%v", err)
			}
		})
	}
}

func TestConfiguredTerminalPolicyRejectsDevelopmentFlagMixing(t *testing.T) {
	path := writeWorkerPrivate(
		t, "terminal-policy.json", workerTerminalPolicyJSON(8<<30, 0),
	)
	flags := defaultTerminalPolicyFlags()
	flags.workers++
	_, err := configuredTerminalPolicy(path, true, probe.Report{
		Host: probe.Host{MemoryBytes: 16 << 30},
	}, flags)
	if err == nil {
		t.Fatal("terminal policy was mixed with development resource flags")
	}
}

func workerTerminalPolicyJSON(ramBytes, vramBytes uint64) string {
	ownerVRAM := vramBytes / 4
	return fmt.Sprintf(`{
  "version":3,
  "workers":2,
  "ownerReservedWorkers":1,
  "maxQueue":8,
  "maxConnections":32,
  "quoteTtlMillis":30000,
  "maxQuotes":128,
  "maxInvocations":64,
  "maxDeadlineMillis":900000,
  "preflight":{
    "timeoutMillis":5000,
    "successTtlMillis":120000,
    "failureTtlMillis":2000,
    "refreshMillis":5000,
    "workers":4
  },
  "resourceMonitor":{
    "intervalMillis":10000,
    "timeoutMillis":5000,
    "failureThreshold":2,
    "recoveryThreshold":2
  },
  "admission":{
    "capacity":{
      "ramBytes":%d,
      "vramBytes":%d,
      "kvCacheBytes":4294967296,
      "contextTokens":262144,
      "batchSize":64,
      "outputBytes":67108864,
      "executionMillis":900000
    },
    "ownerReserved":{
      "ramBytes":1073741824,
      "vramBytes":%d,
      "kvCacheBytes":1073741824,
      "contextTokens":32768,
      "batchSize":8,
      "outputBytes":8388608,
      "executionMillis":0
    },
    "perRequestMax":{
      "ramBytes":2147483648,
      "vramBytes":%d,
      "kvCacheBytes":2147483648,
      "contextTokens":32768,
      "batchSize":8,
      "outputBytes":8388608,
      "executionMillis":900000
    }
  }
}`, ramBytes, vramBytes, ownerVRAM, vramBytes)
}

func writeWorkerPrivate(t *testing.T, name string, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type workerOllamaRuntime struct {
	mu sync.Mutex

	blobs  map[string][]byte
	models map[string]string

	uploads int
	creates int
	deletes int
}

func newWorkerOllamaRuntime() *workerOllamaRuntime {
	return &workerOllamaRuntime{
		blobs: make(map[string][]byte), models: make(map[string]string),
	}
}

func (f *workerOllamaRuntime) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	switch {
	case request.Method == http.MethodHead &&
		strings.HasPrefix(request.URL.Path, "/api/blobs/"):
		digest := strings.TrimPrefix(request.URL.Path, "/api/blobs/")
		f.mu.Lock()
		_, exists := f.blobs[digest]
		f.mu.Unlock()
		if !exists {
			writer.WriteHeader(http.StatusNotFound)
		}
	case request.Method == http.MethodPost &&
		strings.HasPrefix(request.URL.Path, "/api/blobs/"):
		digest := strings.TrimPrefix(request.URL.Path, "/api/blobs/")
		data, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(data)
		if digest != "sha256:"+hex.EncodeToString(sum[:]) {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.blobs[digest] = data
		f.uploads++
		f.mu.Unlock()
		writer.WriteHeader(http.StatusCreated)
	case request.Method == http.MethodGet && request.URL.Path == "/api/tags":
		f.mu.Lock()
		names := make([]string, 0, len(f.models))
		for name := range f.models {
			names = append(names, name)
		}
		f.mu.Unlock()
		sort.Strings(names)
		models := make([]map[string]string, 0, len(names))
		for _, name := range names {
			models = append(models, map[string]string{
				"name": name, "model": name,
			})
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"models": models})
	case request.Method == http.MethodPost && request.URL.Path == "/api/create":
		var value struct {
			Model string            `json:"model"`
			Files map[string]string `json:"files"`
		}
		if json.NewDecoder(request.Body).Decode(&value) != nil ||
			len(value.Files) != 1 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var digest string
		for _, candidate := range value.Files {
			digest = candidate
		}
		f.mu.Lock()
		if f.blobs[digest] == nil {
			f.mu.Unlock()
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		f.models[value.Model] = digest
		f.creates++
		f.mu.Unlock()
		_, _ = writer.Write([]byte(`{"status":"success"}`))
	case request.Method == http.MethodPost && request.URL.Path == "/api/show":
		var value struct {
			Model string `json:"model"`
		}
		if json.NewDecoder(request.Body).Decode(&value) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		digest := f.models[value.Model]
		f.mu.Unlock()
		if digest == "" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = writer.Write([]byte(
			`{"modelfile":"FROM /models/blobs/sha256-` +
				strings.TrimPrefix(digest, "sha256:") +
				`","details":{"format":"gguf"}}`,
		))
	case request.Method == http.MethodPost && request.URL.Path == "/api/generate":
		var value struct {
			Model string `json:"model"`
		}
		if json.NewDecoder(request.Body).Decode(&value) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		exists := f.models[value.Model] != ""
		f.mu.Unlock()
		if !exists {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = writer.Write([]byte(
			`{"model":` + fmt.Sprintf("%q", value.Model) + `,"done":true}`,
		))
	case request.Method == http.MethodDelete && request.URL.Path == "/api/delete":
		var value struct {
			Model string `json:"model"`
		}
		if json.NewDecoder(request.Body).Decode(&value) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.deletes++
		if f.models[value.Model] == "" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		delete(f.models, value.Model)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func TestResourceProbeSubprocessIsStrictBoundedAndCancelable(t *testing.T) {
	t.Setenv("TOS_AI_RESOURCE_PROBE_HELPER", "1")
	command := func(ctx context.Context, mode string) (probe.Report, error) {
		return runResourceProbeCommand(
			ctx, os.Args[0], "-test.run=TestResourceProbeHelperProcess", "--", mode,
		)
	}
	report, err := command(context.Background(), "valid")
	if err != nil || report.Host.MemoryBytes != 16<<30 ||
		report.NVIDIA.Status != "unavailable" {
		t.Fatalf("valid subprocess report=%#v err=%v", report, err)
	}
	for _, mode := range []string{"malformed", "unknown", "oversized", "failure"} {
		if _, err := command(context.Background(), mode); err == nil {
			t.Fatalf("resource subprocess mode %q failed open", mode)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := command(ctx, "hang"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hung subprocess error=%v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("hung resource subprocess was not killed promptly")
	}

	bounded := &boundedProbeOutput{maximum: 8}
	if _, err := bounded.Write([]byte("0123456789abcdef")); err != nil ||
		bounded.buffer.Len() != 8 || !bounded.exceeded {
		t.Fatalf("bounded output=%q exceeded=%v err=%v", bounded.buffer.Bytes(), bounded.exceeded, err)
	}
	if err := runInternalResourceProbe([]string{"unexpected"}, io.Discard); err == nil {
		t.Fatal("internal resource probe accepted trailing arguments")
	}
}

func TestResourceProbeHelperProcess(t *testing.T) {
	if os.Getenv("TOS_AI_RESOURCE_PROBE_HELPER") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	switch mode {
	case "valid":
		now := time.Now().UTC()
		_ = json.NewEncoder(os.Stdout).Encode(probe.Report{
			Host: probe.Host{
				OS: "linux", Architecture: "amd64", LogicalCPUs: 8,
				MemoryBytes: 16 << 30, CollectedAt: now,
				Evidence: probe.EvidenceLocallyObserved,
			},
			NVIDIA: probe.NVIDIAReport{
				Status: "unavailable", Devices: []probe.NVIDIADevice{},
				CollectedAt: now, Evidence: probe.EvidenceLocallyObserved,
			},
		})
		os.Exit(0)
	case "malformed":
		_, _ = io.WriteString(os.Stdout, `{"host":{}}`)
		os.Exit(0)
	case "unknown":
		_, _ = io.WriteString(os.Stdout, `{"unknown":true}`)
		os.Exit(0)
	case "oversized":
		_, _ = io.WriteString(
			os.Stdout, strings.Repeat("x", maxResourceProbeOutput+1),
		)
		os.Exit(0)
	case "failure":
		os.Exit(7)
	case "hang":
		time.Sleep(time.Hour)
		os.Exit(0)
	default:
		os.Exit(9)
	}
}
