// Command software-work-execute runs one V1 software-work job through the
// production bounded executor and prints its content-addressed outcome.
//
// The default mode runs a job whose admission its caller already proved. The
// "paid" subcommand is the standalone paid path: it admits a finalized
// pending Service Actor request at most once before executing it. The
// "artifacts-gc" subcommand deletes artifacts whose retention ended.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/internal/osguard"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/executor/containerdbackend"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
)

const (
	maxSourceBytes = 16 << 20
)

// Exit codes shared by every mode. The paid mode adds the statuses in
// paid.go.
const (
	exitOK    = 0
	exitError = 1
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run dispatches on an optional leading subcommand. Without one the command
// runs a job whose admission its caller already proved (the default mode);
// "paid" admits a Service Actor request itself before executing it, and
// "artifacts-gc" sweeps expired artifact retention.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "paid":
			return runPaid(args[1:], stdout, stderr)
		case "artifacts-gc":
			return runArtifactsGC(args[1:], stdout, stderr)
		}
	}
	return runLocal(args, stdout, stderr)
}

func runLocal(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("software-work-execute", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socket := flags.String("containerd-socket", "", "private containerd socket")
	fifo := flags.String("fifo-dir", "", "private FIFO directory")
	root := flags.String("state-dir", "", "private execution state directory")
	source := flags.String("source", "", "deterministic USTAR source archive")
	quote := flags.String("quote", "", "Accepted Quote commitment")
	executionID := flags.String("execution-id", "", "execution identity")
	inputDigest := flags.String("input-digest", "", "canonical job input digest")
	manifestDigest := flags.String("manifest-digest", "", "published manifest digest the sandbox binds (default: the frozen manifest)")
	outputBytes := flags.Uint64("output-bytes", 0, "output bound the published manifest states (default: the frozen bound)")
	retainUntil := flags.Int64("retain-until", 0, "unix second until which the job's artifacts are retained (default: no retention record; kept until removed by hand)")
	if err := flags.Parse(args); err != nil {
		return exitError
	}
	if flags.NArg() != 0 {
		return failLocal(stderr, fmt.Errorf("unexpected argument %q", flags.Arg(0)))
	}
	if *retainUntil < 0 {
		return failLocal(stderr, errors.New("retain-until must be a positive unix second"))
	}
	contract, err := publishedContract(*manifestDigest, *outputBytes)
	if err != nil {
		return failLocal(stderr, err)
	}
	if *socket == "" || *fifo == "" || *root == "" || *source == "" {
		return failLocal(stderr, fmt.Errorf("all path flags are required"))
	}
	if err := requirePrivateOwnedDirectory(*root); err != nil {
		return failLocal(stderr, err)
	}
	archive, err := readPrivateSource(*source)
	if err != nil {
		return failLocal(stderr, err)
	}
	execution, err := openExecution(*socket, *fifo, *root, contract)
	if err != nil {
		return failLocal(stderr, err)
	}
	defer execution.Close()
	outcome, err := execution.runner.Execute(context.Background(), softwarework.Request{QuoteCommitment: *quote,
		ExecutionID: *executionID, InputDigest: *inputDigest, SourceDigest: sha256Digest(archive), SourceArchive: archive})
	if err != nil {
		return failLocal(stderr, err)
	}
	if *retainUntil > 0 {
		if err := execution.store.Retain(context.Background(), outcome.ExecutionID, time.Unix(*retainUntil, 0),
			[]string{outcome.Artifact.Digest, outcome.Report.Digest}); err != nil {
			return failLocal(stderr, fmt.Errorf("record artifact retention: %w", err))
		}
	}
	encoded, err := json.MarshalIndent(outcome, "", "  ")
	if err != nil {
		return failLocal(stderr, errors.New("encode outcome"))
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}

// execution is the bounded executor, artifact store and journal of one
// private state directory.
type execution struct {
	backend *containerdbackend.Backend
	store   *artifactstore.Store
	journal *softwarework.Journal
	runner  *softwarework.Runner
}

func (e *execution) Close() error {
	var failures []error
	if e.journal != nil {
		failures = append(failures, e.journal.Close())
	}
	if e.backend != nil {
		failures = append(failures, e.backend.Close())
	}
	return errors.Join(failures...)
}

func openExecution(socket, fifo, root string, contract softwarework.Contract) (*execution, error) {
	limits := softwarework.FrozenV1Limits()
	backend, err := containerdbackend.Open(context.Background(), containerdbackend.Config{
		SocketPath: socket, Namespace: "tos-service-paid-work", Snapshotter: "overlayfs", Runtime: "io.containerd.runc.v2",
		FIFODir: fifo, MaxActive: 1, PolicyLimits: limits,
		ImageReference: softwarework.FrozenV1ImageReference, ImageDigest: softwarework.FrozenV1ImageDigest,
		ImagePlatform: "linux/amd64",
	})
	if err != nil {
		return nil, err
	}
	result := &execution{backend: backend}
	bound, err := executor.NewPolicyExecutor(softwarework.FrozenV1Policy(), backend)
	if err != nil {
		return nil, errors.Join(err, result.Close())
	}
	result.store, err = artifactstore.Open(filepath.Join(root, "artifacts"), artifactstore.MaxObjectBytesHard)
	if err != nil {
		return nil, errors.Join(err, result.Close())
	}
	result.journal, err = softwarework.OpenJournal(filepath.Join(root, "journal"))
	if err != nil {
		return nil, errors.Join(err, result.Close())
	}
	result.runner, err = softwarework.NewRunner(bound, result.store, result.journal, contract)
	if err != nil {
		return nil, errors.Join(err, result.Close())
	}
	return result, nil
}

// publishedContract is the frozen contract bound to a provider's published
// manifest: its digest names the sandbox, and its output bound may only be
// tighter than the frozen one.
func publishedContract(manifestDigest string, outputBytes uint64) (softwarework.Contract, error) {
	contract := softwarework.FrozenV1Contract()
	if manifestDigest != "" {
		if len(manifestDigest) != 71 || manifestDigest[:7] != "sha256:" {
			return softwarework.Contract{}, errors.New("manifest digest must be sha256:<64 hex>")
		}
		if _, err := hex.DecodeString(manifestDigest[7:]); err != nil || strings.ToLower(manifestDigest) != manifestDigest {
			return softwarework.Contract{}, errors.New("manifest digest must be sha256:<64 lowercase hex>")
		}
		contract.ManifestDigest, contract.SandboxDigest = manifestDigest, manifestDigest
	}
	if outputBytes != 0 {
		if outputBytes > contract.Limits.OutputBytes {
			return softwarework.Contract{}, errors.New("output bound may not exceed the frozen bound")
		}
		contract.Limits.OutputBytes = outputBytes
	}
	return contract, nil
}

func sha256Digest(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// The executor may own a privileged containerd socket. Never let a less
// privileged caller turn its path flags into a confused-deputy file read or
// state write. Operators must stage both inputs under the executor identity.
func readPrivateSource(path string) ([]byte, error) {
	return readPrivateFile(path, maxSourceBytes, "source archive")
}

// readPrivateFile reads a bounded regular file owned by the executor
// identity, not readable by others, inside a private owned directory.
func readPrivateFile(path string, maximum int64, what string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("%s path must be canonical and absolute", what)
	}
	if err := requirePrivateOwnedDirectory(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("%s parent must be private and owned", what)
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 ||
		before.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(before) ||
		before.Size() < 0 || before.Size() > maximum {
		return nil, fmt.Errorf("%s must be a bounded private owned regular file", what)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s", what)
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() ||
		after.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(after) {
		return nil, fmt.Errorf("%s changed while opening", what)
	}
	value, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(value)) > maximum || int64(len(value)) != after.Size() {
		return nil, fmt.Errorf("read bounded %s", what)
	}
	return value, nil
}

func requirePrivateOwnedDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("private directory path must be canonical and absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return errors.New("directory must be private and owned")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("private directory path contains a symlink")
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	return osguard.CurrentUserOwns(info)
}

func failLocal(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, err)
	return exitError
}
