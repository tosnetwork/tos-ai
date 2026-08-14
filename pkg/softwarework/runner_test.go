package softwarework

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

type fakeExecutor struct {
	calls int
	spec  executor.Spec
	input []byte
	err   error
}

func (f *fakeExecutor) Execute(_ context.Context, _ string, spec executor.Spec, input []byte) (executor.Result, error) {
	f.calls++
	f.spec = spec
	f.input = append([]byte(nil), input...)
	if f.err != nil {
		return executor.Result{}, f.err
	}
	return executor.Result{ExitCode: 0, Output: []byte("ok\n"), Usage: executor.Usage{CPUMillis: 1, PeakMemory: 1024, DiskWritten: 10, Duration: time.Millisecond}}, nil
}

func fixture(t *testing.T) (*Runner, *fakeExecutor, *Journal, Request) {
	t.Helper()
	root := t.TempDir()
	store, err := artifactstore.Open(filepath.Join(root, "artifacts"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := OpenJournal(filepath.Join(root, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeExecutor{}
	contract := Contract{
		ManifestDigest: digest([]byte("manifest")), ToolchainDigest: digest([]byte("toolchain")), SandboxDigest: digest([]byte("sandbox")),
		Executable: "/usr/local/bin/go", Arguments: []string{"test", "./...", "-count=1"}, WorkingDirectory: "/workspace/source",
		Limits: executor.Limits{CPUMillis: 1000, MemoryBytes: 1 << 20, DiskBytes: 1 << 20, PIDs: 32, ExecutionTime: time.Second, OutputBytes: 1 << 16},
		UserID: 65532, GroupID: 65532,
	}
	archive := testArchive(t)
	request := Request{QuoteCommitment: digest([]byte("quote")), ExecutionID: digest([]byte("execution")), InputDigest: digest([]byte("input")), SourceDigest: digest(archive), SourceArchive: archive}
	runner, err := NewRunner(backend, store, journal, contract)
	if err != nil {
		t.Fatal(err)
	}
	runner.now = func() time.Time { return time.Unix(1_776_000_000, 0) }
	return runner, backend, journal, request
}

func TestRunnerBindsExactContractAndReturnsImmutableArtifacts(t *testing.T) {
	runner, backend, journal, request := fixture(t)
	defer journal.Close()
	outcome, err := runner.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if backend.calls != 1 || !backend.spec.WorkspaceArchive || backend.spec.Network != executor.NetworkNone ||
		backend.spec.WorkingDirectory != "/workspace/source" || backend.spec.Entrypoint[0] != "/usr/local/bin/go" ||
		backend.spec.Environment["GOROOT"] != "/usr/local/go" || backend.spec.Environment["CGO_ENABLED"] != "0" ||
		!bytes.Equal(backend.input, request.SourceArchive) || outcome.Artifact.MediaType != ArtifactMediaType || outcome.Report.MediaType != ReportMediaType {
		t.Fatalf("contract mapping or outcome mismatch: %#v %#v", backend.spec, outcome)
	}
	replayed, err := runner.Execute(context.Background(), request)
	if err != nil || replayed.Artifact.Digest != outcome.Artifact.Digest || backend.calls != 1 {
		t.Fatalf("completed replay = %#v, %v, calls=%d", replayed, err, backend.calls)
	}
}

func TestRunnerRejectsConflictingExecutionIdentity(t *testing.T) {
	runner, backend, journal, request := fixture(t)
	defer journal.Close()
	if _, err := runner.Execute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.InputDigest = digest([]byte("other input"))
	if _, err := runner.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "conflicts") || backend.calls != 1 {
		t.Fatalf("conflict err=%v calls=%d", err, backend.calls)
	}
}

func TestRunnerRejectsCommitmentMismatchBeforeExecution(t *testing.T) {
	runner, backend, journal, request := fixture(t)
	defer journal.Close()
	request.SourceDigest = digest([]byte("different"))
	if _, err := runner.Execute(context.Background(), request); err == nil || backend.calls != 0 {
		t.Fatalf("mismatch err=%v calls=%d", err, backend.calls)
	}
}

func TestRunnerCrashBoundaryFailsClosedWithoutSecondExecution(t *testing.T) {
	runner, backend, journal, request := fixture(t)
	backend.err = errors.New("runtime connection lost")
	if _, err := runner.Execute(context.Background(), request); err == nil || backend.calls != 1 {
		t.Fatalf("first execution err=%v calls=%d", err, backend.calls)
	}
	backend.err = nil
	if _, err := runner.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "ambiguous") || backend.calls != 1 {
		t.Fatalf("ambiguous replay err=%v calls=%d", err, backend.calls)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenJournal(journal.root)
	if err != nil {
		t.Fatal(err)
	}
	runner.journal = reopened
	defer reopened.Close()
	if _, err := runner.Execute(context.Background(), request); err == nil || backend.calls != 1 {
		t.Fatalf("restart replay err=%v calls=%d", err, backend.calls)
	}
}

func testArchive(t *testing.T) []byte {
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
