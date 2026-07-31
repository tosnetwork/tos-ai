// Package mock provides a deterministic development adapter. It is not a
// production inference runtime.
package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

type Adapter struct {
	capability airuntime.Capability
	delay      time.Duration
}

func New(delay time.Duration) *Adapter {
	digest := sha256.Sum256([]byte("tos-ai-deterministic-mock-v1"))
	return &Adapter{
		capability: airuntime.Capability{
			ServiceID:       "tos.ai.mock",
			Operation:       "generate",
			Model:           "deterministic-echo",
			ModelDigest:     "sha256:" + hex.EncodeToString(digest[:]),
			Runtime:         "mock",
			RuntimeRevision: "mock-v1",
			MaxInputBytes:   1 << 20,
			MaxOutputBytes:  1 << 20,
			AcceptedPriorities: []airuntime.Priority{
				airuntime.PriorityLocalAsync,
				airuntime.PriorityExternalService,
				airuntime.PriorityBackground,
			},
			Admission: admission.Resources{
				RAMBytes: 1 << 20, ContextTokens: 4096, BatchSize: 1,
				OutputBytes: 1 << 20, ExecutionTime: 15 * time.Minute,
			},
		},
		delay: delay,
	}
}

func (a *Adapter) Capability() airuntime.Capability {
	return a.capability
}

func (a *Adapter) Execute(ctx context.Context, request airuntime.Request) (airuntime.Response, error) {
	if err := airuntime.ValidateRequest(a.capability, request); err != nil {
		return airuntime.Response{}, err
	}
	if a.delay > 0 {
		timer := time.NewTimer(a.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return airuntime.Response{}, ctx.Err()
		case <-timer.C:
		}
	}
	if uint64(len(request.Payload)) > request.MaxOutputBytes {
		return airuntime.Response{}, errors.New("mock output exceeds requested limit")
	}
	start := time.Now()
	output := append([]byte(nil), request.Payload...)
	return airuntime.Response{
		Output: output,
		Usage: airuntime.Usage{
			InputBytes:      uint64(len(request.Payload)),
			OutputBytes:     uint64(len(output)),
			ExecutionMillis: airuntime.MillisecondsSince(start),
		},
		ModelRevision:   a.capability.ModelDigest,
		RuntimeRevision: a.capability.RuntimeRevision,
	}, nil
}
