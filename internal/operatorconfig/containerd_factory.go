package operatorconfig

import (
	"context"
	"errors"
	"sort"

	"github.com/tosnetwork/tos-ai/executor/gpuisolation"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/executor/containerdbackend"
)

// ContainerdBackendFactory is the only compiled-in production factory for
// isolated-runtime configuration. GPU execution is available only through an
// operator-fixed alias-to-CDI map and the exclusive gpuisolation lease layer.
type ContainerdBackendFactory struct{}

type gpuContainerdBackend struct {
	base   *containerdbackend.Backend
	leased *gpuisolation.Client
}

func (b *gpuContainerdBackend) RunIsolated(
	ctx context.Context,
	request executor.ContainerRequest,
	input []byte,
) (executor.Result, error) {
	return b.leased.RunIsolated(ctx, request, input)
}

func (b *gpuContainerdBackend) CheckReady(ctx context.Context) error {
	return b.base.CheckReady(ctx)
}

func (b *gpuContainerdBackend) Close() error { return b.base.Close() }

func (ContainerdBackendFactory) Open(
	ctx context.Context,
	config IsolatedBackendConfig,
) (IsolatedBackend, error) {
	if config.Type != "containerd" {
		return nil, errors.New("unsupported isolated backend type")
	}
	backend, err := containerdbackend.Open(ctx, containerdbackend.Config{
		SocketPath: config.SocketPath, Namespace: config.Namespace,
		Snapshotter: config.Snapshotter, Runtime: config.Runtime,
		FIFODir: config.FIFODir, MaxActive: config.MaxActive,
		PermitGPU: config.PermitGPU, PermitNetwork: config.PermitNetwork,
		GPUDevices:   config.GPUDevices,
		PolicyLimits: config.Limits, ImageReference: config.ImageReference,
		ImageDigest: config.ImageDigest,
	})
	if err != nil {
		return nil, err
	}
	if !config.PermitGPU {
		return backend, nil
	}
	aliases := make([]string, 0, len(config.GPUDevices))
	for alias := range config.GPUDevices {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	leased, err := gpuisolation.New(aliases, backend)
	if err != nil {
		_ = backend.Close()
		return nil, errors.New("configure exclusive GPU devices")
	}
	return &gpuContainerdBackend{base: backend, leased: leased}, nil
}

var _ IsolatedBackendFactory = ContainerdBackendFactory{}
