package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/internal/worker"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/modelapproval"
	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	"github.com/tosnetwork/tos-ai/pkg/probe"
	"github.com/tosnetwork/tos-ai/pkg/update"
)

func TestConfiguredAdaptersDefaultsToMockAndRejectsMixedMode(t *testing.T) {
	adapters, err := configuredAdapters("", "", 0)
	if err != nil || len(adapters) != 1 || adapters[0].Capability().Runtime != "mock" {
		t.Fatalf("default adapters=%v err=%v", adapters, err)
	}
	if _, err := configuredAdapters(
		"/private/runtime.json", "", time.Millisecond,
	); err == nil {
		t.Fatal("mock and production runtime configuration accepted together")
	}
	if _, err := configuredAdapters("", "/private/trust.json", 0); err == nil {
		t.Fatal("model trust without a production runtime was accepted")
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
	adapters, err := configuredAdapters(runtimePath, trustPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(adapters) != 1 {
		t.Fatalf("approved adapter count=%d", len(adapters))
	}
	if _, ok := adapters[0].(*modelapproval.Adapter); !ok {
		t.Fatalf("runtime adapter is not model guarded: %T", adapters[0])
	}
	closeRuntimeAdapters(adapters)

	missingRuntime := strings.Replace(
		string(mustRead(t, runtimePath)), digest,
		"sha256:"+strings.Repeat("f", 64), 1,
	)
	missingPath := writeWorkerPrivate(t, "missing-runtime.json", missingRuntime)
	if _, err := configuredAdapters(
		missingPath, trustPath, 0,
	); err == nil || strings.Contains(err.Error(), cacheDir) {
		t.Fatalf("missing approved digest error=%v", err)
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
