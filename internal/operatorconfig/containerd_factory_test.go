package operatorconfig

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/executor"
)

func TestContainerdFactoryRejectsUnimplementedAuthorityBeforeOpeningSocket(t *testing.T) {
	base := IsolatedBackendConfig{
		Type: "containerd", SocketPath: "/missing/private/containerd.sock",
		Namespace: "tos-ai", Snapshotter: "overlayfs",
		Runtime: "io.containerd.runc.v2", FIFODir: "/missing/private/fifos",
		MaxActive: 1,
		Limits: executor.Limits{
			CPUMillis: 1000, MemoryBytes: 64 << 20, DiskBytes: 12 << 20,
			PIDs: 16, ExecutionTime: time.Second, OutputBytes: 1024,
		},
		ImageReference: "registry.example/tos/infer@sha256:1111111111111111111111111111111111111111111111111111111111111111",
		ImageDigest:    "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	for _, mutate := range []func(*IsolatedBackendConfig){
		func(config *IsolatedBackendConfig) { config.PermitGPU = true },
		func(config *IsolatedBackendConfig) { config.PermitNetwork = true },
		func(config *IsolatedBackendConfig) { config.Limits.DiskBytes = 1 },
	} {
		config := base
		mutate(&config)
		if backend, err := (ContainerdBackendFactory{}).Open(
			context.Background(), config,
		); err == nil || backend != nil {
			t.Fatal("unsupported containerd authority reached runtime open")
		}
	}
}
