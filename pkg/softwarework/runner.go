// Package softwarework binds the frozen software-work contract to the bounded
// executor and immutable artifact store. It deliberately does not decide
// whether a Quote or escrow is canonical; that authority remains finalized TOS
// state at the future public worker boundary.
package softwarework

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const (
	ArtifactMediaType = "application/vnd.atos.software-artifact.v1+tar"
	ReportMediaType   = "application/vnd.atos.test-report.v1+json"
)

type Contract struct {
	ManifestDigest   string
	ToolchainDigest  string
	SandboxDigest    string
	Executable       string
	Arguments        []string
	WorkingDirectory string
	Limits           executor.Limits
	UserID           uint32
	GroupID          uint32
}

type Request struct {
	QuoteCommitment string
	ExecutionID     string
	InputDigest     string
	SourceDigest    string
	SourceArchive   []byte
}

type Outcome struct {
	QuoteCommitment string                   `json:"quote_commitment"`
	ExecutionID     string                   `json:"execution_id"`
	InputDigest     string                   `json:"input_digest"`
	ResultDigest    string                   `json:"result_digest"`
	Artifact        artifactstore.Descriptor `json:"artifact"`
	Report          artifactstore.Descriptor `json:"report"`
	SourceDigest    string                   `json:"source_digest"`
	ToolchainDigest string                   `json:"toolchain_digest"`
	SandboxDigest   string                   `json:"sandbox_digest"`
	ExitCode        int                      `json:"exit_code"`
	CompletedAtUnix uint64                   `json:"completed_at_unix"`
}

type Runner struct {
	executor executor.Executor
	store    *artifactstore.Store
	journal  *Journal
	contract Contract
	now      func() time.Time
}

func NewRunner(boundExecutor executor.Executor, store *artifactstore.Store, journal *Journal, contract Contract) (*Runner, error) {
	if boundExecutor == nil || store == nil || journal == nil || validateContract(contract) != nil {
		return nil, errors.New("invalid software-work runner configuration")
	}
	contract.Arguments = append([]string(nil), contract.Arguments...)
	return &Runner{executor: boundExecutor, store: store, journal: journal, contract: contract, now: time.Now}, nil
}

func (r *Runner) Execute(ctx context.Context, request Request) (Outcome, error) {
	if r == nil || r.executor == nil || r.store == nil || r.journal == nil || ctx == nil {
		return Outcome{}, errors.New("invalid software-work execution request")
	}
	if err := validateRequest(r.contract, request); err != nil {
		return Outcome{}, err
	}
	fingerprint := requestFingerprint(r.contract, request)
	lease, record, err := r.journal.Claim(request.ExecutionID, fingerprint)
	if err != nil {
		return Outcome{}, err
	}
	if !lease {
		if record.State == "complete" {
			return record.Outcome, nil
		}
		return Outcome{}, errors.New("software-work execution outcome is ambiguous")
	}
	spec := executor.Spec{
		ImageDigest: r.contract.ToolchainDigest, Entrypoint: append([]string{r.contract.Executable}, r.contract.Arguments...),
		WorkingDirectory: r.contract.WorkingDirectory, WorkspaceArchive: true,
		Environment: map[string]string{
			"GOROOT": "/usr/local/go", "HOME": "/workspace",
			"GOCACHE": "/workspace/go-cache", "GOTMPDIR": "/workspace",
			"TMPDIR": "/workspace", "CGO_ENABLED": "0", "GOMAXPROCS": "2",
		},
		ReadOnlyRoot: true, Network: executor.NetworkNone, UserID: r.contract.UserID, GroupID: r.contract.GroupID,
		NoNewPrivileges: true, Limits: r.contract.Limits,
	}
	result, err := r.executor.Execute(ctx, request.ExecutionID, spec, request.SourceArchive)
	if err != nil {
		return Outcome{}, err
	}
	if result.ExitCode != 0 {
		return Outcome{}, errors.New("software-work objective success condition failed")
	}
	completed := r.now().UTC()
	resultDigest := digest(result.Output)
	reportBytes, err := json.Marshal(struct {
		Schema          string         `json:"schema"`
		ExecutionID     string         `json:"execution_id"`
		ResultDigest    string         `json:"result_digest"`
		ExitCode        int            `json:"exit_code"`
		Usage           executor.Usage `json:"usage"`
		CompletedAtUnix uint64         `json:"completed_at_unix"`
	}{"atos.software-work-report.v1", request.ExecutionID, resultDigest, result.ExitCode, result.Usage, uint64(completed.Unix())})
	if err != nil {
		return Outcome{}, errors.New("encode software-work report")
	}
	report, err := r.store.Put(ctx, ReportMediaType, bytes.NewReader(reportBytes))
	if err != nil {
		return Outcome{}, err
	}
	artifactBytes, err := buildArtifact(result.Output, reportBytes)
	if err != nil {
		return Outcome{}, err
	}
	artifact, err := r.store.Put(ctx, ArtifactMediaType, bytes.NewReader(artifactBytes))
	if err != nil {
		return Outcome{}, err
	}
	outcome := Outcome{
		QuoteCommitment: request.QuoteCommitment, ExecutionID: request.ExecutionID, InputDigest: request.InputDigest,
		ResultDigest: resultDigest, Artifact: artifact, Report: report, SourceDigest: request.SourceDigest,
		ToolchainDigest: r.contract.ToolchainDigest, SandboxDigest: r.contract.SandboxDigest,
		ExitCode: result.ExitCode, CompletedAtUnix: uint64(completed.Unix()),
	}
	if err := r.journal.Complete(request.ExecutionID, fingerprint, outcome); err != nil {
		return Outcome{}, err
	}
	return outcome, nil
}

func validateContract(value Contract) error {
	if !executionIDPattern.MatchString(value.ManifestDigest) || !executionIDPattern.MatchString(value.ToolchainDigest) ||
		value.SandboxDigest != value.ManifestDigest || value.Executable == "" || len(value.Arguments) == 0 ||
		len(value.Arguments) > 64 || !strings.HasPrefix(value.Executable, "/") || path.Clean(value.Executable) != value.Executable ||
		forbiddenExecutable(value.Executable) || value.WorkingDirectory != "/workspace/source" || value.UserID == 0 || value.GroupID == 0 ||
		value.Limits.CPUMillis == 0 || value.Limits.MemoryBytes == 0 || value.Limits.DiskBytes == 0 || value.Limits.PIDs == 0 ||
		value.Limits.ExecutionTime <= 0 || value.Limits.OutputBytes == 0 || value.Limits.GPUDeviceCount != 0 {
		return errors.New("invalid software-work contract")
	}
	for _, argument := range value.Arguments {
		if argument == "" || len(argument) > 512 || strings.IndexByte(argument, 0) >= 0 {
			return errors.New("invalid software-work contract")
		}
	}
	return nil
}

func forbiddenExecutable(value string) bool {
	switch path.Base(value) {
	case "sh", "bash", "dash", "zsh", "fish", "ksh", "cmd", "powershell", "pwsh":
		return true
	default:
		return false
	}
}

func validateRequest(contract Contract, value Request) error {
	if !executionIDPattern.MatchString(value.QuoteCommitment) || !executionIDPattern.MatchString(value.ExecutionID) ||
		!executionIDPattern.MatchString(value.InputDigest) || !executionIDPattern.MatchString(value.SourceDigest) ||
		digest(value.SourceArchive) != value.SourceDigest || uint64(len(value.SourceArchive)) > contract.Limits.DiskBytes {
		return errors.New("software-work request does not match its commitments")
	}
	return nil
}

func requestFingerprint(contract Contract, request Request) string {
	encoded, _ := json.Marshal(struct {
		Manifest  string `json:"manifest"`
		Quote     string `json:"quote"`
		Execution string `json:"execution"`
		Input     string `json:"input"`
		Source    string `json:"source"`
	}{contract.ManifestDigest, request.QuoteCommitment, request.ExecutionID, request.InputDigest, request.SourceDigest})
	return digest(encoded)
}

func digest(value []byte) string {
	hash := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func buildArtifact(output, report []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, entry := range []struct {
		name string
		body []byte
	}{{"output.log", output}, {"report.json", report}} {
		header := &tar.Header{Name: entry.name, Mode: 0o444, Size: int64(len(entry.body)), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}
		if err := writer.WriteHeader(header); err != nil {
			return nil, errors.New("encode software-work artifact")
		}
		if _, err := writer.Write(entry.body); err != nil {
			return nil, errors.New("encode software-work artifact")
		}
	}
	if err := writer.Close(); err != nil {
		return nil, errors.New("encode software-work artifact")
	}
	return buffer.Bytes(), nil
}
