// Command software-work-execute runs one already-authorized V1 job through the
// production bounded executor and prints its content-addressed outcome.
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
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/executor/containerdbackend"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
)

const (
	manifest       = "sha256:9d39a2d3f5c34a4bfeb63324681e0f457437b756ffb79da8a1681aa79bf9f3e5"
	image          = "sha256:9624bca74096f810c5b24e489521dde124fadcfa1808581648b38bdc1ba1b105"
	maxSourceBytes = 16 << 20
)

func main() {
	socket := flag.String("containerd-socket", "", "private containerd socket")
	fifo := flag.String("fifo-dir", "", "private FIFO directory")
	root := flag.String("state-dir", "", "private execution state directory")
	source := flag.String("source", "", "deterministic USTAR source archive")
	quote := flag.String("quote", "", "Accepted Quote commitment")
	executionID := flag.String("execution-id", "", "execution identity")
	inputDigest := flag.String("input-digest", "", "canonical job input digest")
	flag.Parse()
	if *socket == "" || *fifo == "" || *root == "" || *source == "" {
		fail(fmt.Errorf("all path flags are required"))
	}
	if err := requirePrivateOwnedDirectory(*root); err != nil {
		fail(err)
	}
	archive, err := readPrivateSource(*source)
	if err != nil {
		fail(err)
	}
	limits := executor.Limits{CPUMillis: 120_000, MemoryBytes: 1 << 30, DiskBytes: 2 << 30,
		PIDs: 64, ExecutionTime: 180 * time.Second, OutputBytes: 16 << 20}
	backend, err := containerdbackend.Open(context.Background(), containerdbackend.Config{
		SocketPath: *socket, Namespace: "tos-service-paid-work", Snapshotter: "overlayfs", Runtime: "io.containerd.runc.v2",
		FIFODir: *fifo, MaxActive: 1, PolicyLimits: limits,
		ImageReference: "docker.io/tosnetwork/software-work-go:1.26.5@" + image, ImageDigest: image,
	})
	if err != nil {
		fail(err)
	}
	defer backend.Close()
	bound, err := executor.NewPolicyExecutor(executor.Policy{AllowedImages: map[string]struct{}{image: {}},
		MaxAllowedImages: 1, MaxEnvironment: 8, MaxArguments: 8, MaxAllowedHosts: 0, MaxStringBytes: 4096,
		MaxInputBytes: 16 << 20, Ceiling: limits, RequireReadOnlyRoot: true}, backend)
	if err != nil {
		fail(err)
	}
	store, err := artifactstore.Open(filepath.Join(*root, "artifacts"), 64<<20)
	if err != nil {
		fail(err)
	}
	journal, err := softwarework.OpenJournal(filepath.Join(*root, "journal"))
	if err != nil {
		fail(err)
	}
	defer journal.Close()
	runner, err := softwarework.NewRunner(bound, store, journal, softwarework.Contract{ManifestDigest: manifest,
		ToolchainDigest: image, SandboxDigest: manifest, Executable: "/usr/local/bin/go",
		Arguments: []string{"test", "./...", "-count=1"}, WorkingDirectory: "/workspace/source",
		Limits: limits, UserID: 65532, GroupID: 65532})
	if err != nil {
		fail(err)
	}
	outcome, err := runner.Execute(context.Background(), softwarework.Request{QuoteCommitment: *quote,
		ExecutionID: *executionID, InputDigest: *inputDigest, SourceDigest: sha256Digest(archive), SourceArchive: archive})
	if err != nil {
		fail(err)
	}
	encoded, _ := json.MarshalIndent(outcome, "", "  ")
	fmt.Println(string(encoded))
}

func sha256Digest(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// The executor may own a privileged containerd socket. Never let a less
// privileged caller turn its path flags into a confused-deputy file read or
// state write. Operators must stage both inputs under the executor identity.
func readPrivateSource(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("source archive path must be canonical and absolute")
	}
	if err := requirePrivateOwnedDirectory(filepath.Dir(path)); err != nil {
		return nil, errors.New("source archive parent must be private and owned")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 ||
		before.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(before) ||
		before.Size() < 0 || before.Size() > maxSourceBytes {
		return nil, errors.New("source archive must be a bounded private owned regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open source archive")
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() ||
		after.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(after) {
		return nil, errors.New("source archive changed while opening")
	}
	archive, err := io.ReadAll(io.LimitReader(file, maxSourceBytes+1))
	if err != nil || len(archive) > maxSourceBytes || int64(len(archive)) != after.Size() {
		return nil, errors.New("read bounded source archive")
	}
	return archive, nil
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
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
