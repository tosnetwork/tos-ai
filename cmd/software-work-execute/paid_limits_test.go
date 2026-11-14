package main

import (
	"archive/tar"
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
)

func tightLimits() nativecore.SoftwareWorkLimitsV1 {
	return nativecore.SoftwareWorkLimitsV1{CPUMillis: 30_000, MemoryBytes: 256 << 20, ScratchBytes: 512 << 20,
		OutputBytes: 1 << 20, WallClockMillis: 30_000}
}

func TestManifestContractRunsUnderExactlyTheManifestLimits(t *testing.T) {
	contract, err := manifestContract(testManifest, tightLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := softwarework.FrozenV1Limits()
	want.CPUMillis, want.MemoryBytes, want.DiskBytes, want.OutputBytes, want.ExecutionTime =
		30_000, 256<<20, 512<<20, 1<<20, 30*time.Second
	if contract.Limits != want || contract.ManifestDigest != testManifest || contract.SandboxDigest != testManifest {
		t.Fatalf("contract = %+v, want limits %+v", contract, want)
	}
	// The preset's own bounds are accepted as they are.
	frozen := softwarework.FrozenV1Limits()
	exact := nativecore.SoftwareWorkLimitsV1{CPUMillis: frozen.CPUMillis, MemoryBytes: frozen.MemoryBytes,
		ScratchBytes: frozen.DiskBytes, OutputBytes: frozen.OutputBytes, WallClockMillis: uint64(frozen.ExecutionTime / time.Millisecond)}
	if contract, err := manifestContract(testManifest, exact); err != nil || contract.Limits != frozen {
		t.Fatalf("preset bounds = %+v %v", contract.Limits, err)
	}
	for name, edit := range map[string]func(*nativecore.SoftwareWorkLimitsV1){
		"cpu above preset":        func(l *nativecore.SoftwareWorkLimitsV1) { l.CPUMillis = frozen.CPUMillis + 1 },
		"memory above preset":     func(l *nativecore.SoftwareWorkLimitsV1) { l.MemoryBytes = frozen.MemoryBytes + 1 },
		"scratch above preset":    func(l *nativecore.SoftwareWorkLimitsV1) { l.ScratchBytes = frozen.DiskBytes + 1 },
		"output above preset":     func(l *nativecore.SoftwareWorkLimitsV1) { l.OutputBytes = frozen.OutputBytes + 1 },
		"wall clock above preset": func(l *nativecore.SoftwareWorkLimitsV1) { l.WallClockMillis = 180_001 },
		"huge wall clock":         func(l *nativecore.SoftwareWorkLimitsV1) { l.WallClockMillis = 1 << 63 },
		"zero cpu":                func(l *nativecore.SoftwareWorkLimitsV1) { l.CPUMillis = 0 },
		"zero memory":             func(l *nativecore.SoftwareWorkLimitsV1) { l.MemoryBytes = 0 },
		"zero scratch":            func(l *nativecore.SoftwareWorkLimitsV1) { l.ScratchBytes = 0 },
		"zero output":             func(l *nativecore.SoftwareWorkLimitsV1) { l.OutputBytes = 0 },
		"zero wall clock":         func(l *nativecore.SoftwareWorkLimitsV1) { l.WallClockMillis = 0 },
	} {
		limits := tightLimits()
		edit(&limits)
		if _, err := manifestContract(testManifest, limits); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := manifestContract("", tightLimits()); err == nil {
		t.Fatal("a paid contract without a manifest digest was accepted")
	}
}

// blockingClient stands in for the container runtime: it records the
// request and blocks until the executor's deadline ends the run.
type blockingClient struct {
	mutex   sync.Mutex
	request executor.ContainerRequest
}

func (c *blockingClient) RunIsolated(ctx context.Context, request executor.ContainerRequest, _ []byte) (executor.Result, error) {
	c.mutex.Lock()
	c.request = request
	c.mutex.Unlock()
	<-ctx.Done()
	return executor.Result{}, ctx.Err()
}

func goModArchive(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	body := []byte("module example.test/work\n\ngo 1.26.5\n")
	if err := writer.WriteHeader(&tar.Header{Name: "go.mod", Mode: 0o444, Size: int64(len(body)), Format: tar.FormatUSTAR}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// The manifest's wall clock, not the preset's 180 seconds, stops the run,
// and the sandbox request carries every manifest limit.
func TestPaidContractStopsARunAtTheManifestWallClock(t *testing.T) {
	limits := tightLimits()
	limits.WallClockMillis = 200
	contract, err := manifestContract(testManifest, limits)
	if err != nil {
		t.Fatal(err)
	}
	client := &blockingClient{}
	bound, err := executor.NewPolicyExecutor(softwarework.FrozenV1Policy(), client)
	if err != nil {
		t.Fatal(err)
	}
	root := privateDir(t)
	store, err := artifactstore.Open(filepath.Join(root, "artifacts"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := softwarework.OpenJournal(filepath.Join(root, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	runner, err := softwarework.NewRunner(bound, store, journal, contract)
	if err != nil {
		t.Fatal(err)
	}
	archive := goModArchive(t)
	started := time.Now()
	_, err = runner.Execute(context.Background(), softwarework.Request{QuoteCommitment: testQuote,
		ExecutionID: "sha256:" + strings.Repeat("01", 32), InputDigest: "sha256:" + strings.Repeat("02", 32),
		SourceDigest: sha256Digest(archive), SourceArchive: archive})
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("a run past the manifest wall clock succeeded")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("the run was stopped after %v, not at the manifest's 200 ms", elapsed)
	}
	client.mutex.Lock()
	got := client.request.Limits
	client.mutex.Unlock()
	if got != contract.Limits || got.ExecutionTime != 200*time.Millisecond || got.CPUMillis != limits.CPUMillis ||
		got.MemoryBytes != limits.MemoryBytes || got.DiskBytes != limits.ScratchBytes || got.OutputBytes != limits.OutputBytes {
		t.Fatalf("sandbox limits = %+v, want %+v", got, contract.Limits)
	}
}
