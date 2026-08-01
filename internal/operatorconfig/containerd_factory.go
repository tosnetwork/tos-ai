package operatorconfig

import (
	"context"
	"errors"

	"github.com/tosnetwork/tos-ai/pkg/executor/containerdbackend"
)

// ContainerdBackendFactory is the only compiled-in production factory for
// isolated-runtime configuration. The backend itself deliberately supports
// only CPU execution with network=none in v0.1.
type ContainerdBackendFactory struct{}

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
		PolicyLimits: config.Limits, ImageReference: config.ImageReference,
		ImageDigest: config.ImageDigest,
	})
	if err != nil {
		return nil, err
	}
	return backend, nil
}

var _ IsolatedBackendFactory = ContainerdBackendFactory{}
