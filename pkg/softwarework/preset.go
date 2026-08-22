package softwarework

import (
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const (
	FrozenV1ManifestDigest = "sha256:e4db0138ca2a4d5ad8f3c7ec458304927344e341ae610ee0a682b9cc5b00594e"
	FrozenV1ImageDigest    = "sha256:9624bca74096f810c5b24e489521dde124fadcfa1808581648b38bdc1ba1b105"
	FrozenV1ImageReference = "docker.io/tosnetwork/software-work-go:1.26.5@" + FrozenV1ImageDigest
)

// FrozenV1Limits, FrozenV1Policy and FrozenV1Contract are the single local
// source for both the one-shot executor and long-running Messenger worker.
// Each call returns fresh mutable collections so callers cannot alter another
// runtime's authority.
func FrozenV1Limits() executor.Limits {
	return executor.Limits{CPUMillis: 120_000, MemoryBytes: 1 << 30, DiskBytes: 2 << 30,
		PIDs: 64, ExecutionTime: 180 * time.Second, OutputBytes: 16 << 20}
}

func FrozenV1Policy() executor.Policy {
	return executor.Policy{AllowedImages: map[string]struct{}{FrozenV1ImageDigest: {}},
		MaxAllowedImages: 1, MaxEnvironment: 8, MaxArguments: 8, MaxAllowedHosts: 0,
		MaxStringBytes: 4096, MaxInputBytes: 16 << 20, Ceiling: FrozenV1Limits(), RequireReadOnlyRoot: true}
}

func FrozenV1Contract() Contract {
	return Contract{ManifestDigest: FrozenV1ManifestDigest, ToolchainDigest: FrozenV1ImageDigest,
		SandboxDigest: FrozenV1ManifestDigest, Executable: "/usr/local/bin/go",
		Arguments: []string{"test", "./...", "-count=1"}, WorkingDirectory: "/workspace/source",
		Limits: FrozenV1Limits(), UserID: 65532, GroupID: 65532}
}
