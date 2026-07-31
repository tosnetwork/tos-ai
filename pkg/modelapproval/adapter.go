// Package modelapproval binds runtime adapters to operator-approved, signed
// modelmanager artifacts. It does not load a model into a runtime or claim
// that a generic runtime's configured digest is locally observed.
package modelapproval

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

const (
	MaxAdaptersHard            = 64
	MaxVerificationTimeoutHard = 10 * time.Minute
)

// Adapter retains one path-free artifact lease and rehashes it before every
// runtime preflight. Worker Invoke performs a forced preflight before local
// admission, so an unavailable or changed approval artifact fails before
// runtime execution and resource reservation.
type Adapter struct {
	inner   airuntime.Adapter
	lease   *modelmanager.ArtifactLease
	timeout time.Duration
	gate    chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// New validates and acquires the exact capability digest. The caller retains
// ownership of inner when New fails.
func New(
	ctx context.Context,
	manager *modelmanager.Manager,
	inner airuntime.Adapter,
	verificationTimeout time.Duration,
) (*Adapter, error) {
	if ctx == nil || manager == nil || inner == nil ||
		verificationTimeout <= 0 ||
		verificationTimeout > MaxVerificationTimeoutHard {
		return nil, errors.New("invalid model approval configuration")
	}
	capability := inner.Capability()
	if err := airuntime.ValidateCapability(capability); err != nil {
		return nil, errors.New("invalid approved runtime capability")
	}
	lease, err := manager.AcquireArtifact(capability.ModelDigest)
	if err != nil {
		return nil, errors.New("approved model artifact is unavailable")
	}
	adapter := &Adapter{
		inner: inner, lease: lease, timeout: verificationTimeout,
		gate: make(chan struct{}, 1),
	}
	adapter.gate <- struct{}{}
	if err := adapter.verify(ctx); err != nil {
		_ = lease.Close()
		return nil, errors.New("approved model artifact verification failed")
	}
	return adapter, nil
}

// WrapAll atomically transfers ownership of every input adapter. On failure,
// all already-created wrappers and remaining input adapters are closed.
func WrapAll(
	ctx context.Context,
	manager *modelmanager.Manager,
	adapters []airuntime.Adapter,
	verificationTimeout time.Duration,
) ([]airuntime.Adapter, error) {
	if len(adapters) == 0 || len(adapters) > MaxAdaptersHard {
		closeAdapters(adapters)
		return nil, errors.New("invalid model approval adapter set")
	}
	result := make([]airuntime.Adapter, 0, len(adapters))
	for index, inner := range adapters {
		guarded, err := New(ctx, manager, inner, verificationTimeout)
		if err != nil {
			closeAdapters(result)
			closeAdapters(adapters[index:])
			return nil, err
		}
		result = append(result, guarded)
	}
	return result, nil
}

func (a *Adapter) Capability() airuntime.Capability {
	if a == nil || a.inner == nil {
		return airuntime.Capability{}
	}
	capability := a.inner.Capability()
	capability.AcceptedPriorities = append(
		[]airuntime.Priority(nil), capability.AcceptedPriorities...,
	)
	return capability
}

func (a *Adapter) Preflight(ctx context.Context) (airuntime.Preflight, error) {
	if !a.valid() || ctx == nil || a.closed.Load() {
		return airuntime.Preflight{}, airuntime.NewError(
			airuntime.ErrorUnavailable, nil,
		)
	}
	if err := a.lock(ctx); err != nil {
		return airuntime.Preflight{}, approvalRuntimeError(ctx, err)
	}
	defer a.unlock()
	if a.closed.Load() {
		return airuntime.Preflight{}, airuntime.NewError(
			airuntime.ErrorUnavailable, nil,
		)
	}
	if err := a.verifyLocked(ctx); err != nil {
		return airuntime.Preflight{}, approvalRuntimeError(ctx, err)
	}
	if a.closed.Load() {
		return airuntime.Preflight{}, airuntime.NewError(
			airuntime.ErrorUnavailable, nil,
		)
	}
	return a.inner.Preflight(ctx)
}

func (a *Adapter) Execute(
	ctx context.Context,
	request airuntime.Request,
) (airuntime.Response, error) {
	if !a.valid() || ctx == nil || a.closed.Load() {
		return airuntime.Response{}, airuntime.NewError(
			airuntime.ErrorUnavailable, nil,
		)
	}
	return a.inner.Execute(ctx, request)
}

func (a *Adapter) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		a.closed.Store(true)
		if !a.valid() {
			a.closeErr = errors.New("close invalid model approval adapter")
			return
		}
		<-a.gate
		defer func() { a.gate <- struct{}{} }()
		if closer, ok := a.inner.(airuntime.AdapterCloser); ok {
			if err := closer.Close(); err != nil {
				a.closeErr = errors.New("close approved runtime adapter")
			}
		}
		if err := a.lease.Close(); err != nil && a.closeErr == nil {
			a.closeErr = errors.New("close approved model artifact")
		}
	})
	return a.closeErr
}

func (a *Adapter) verify(ctx context.Context) error {
	if !a.valid() || ctx == nil {
		return errors.New("invalid model approval adapter")
	}
	if err := a.lock(ctx); err != nil {
		return err
	}
	defer a.unlock()
	if a.closed.Load() {
		return errors.New("model approval adapter is closed")
	}
	return a.verifyLocked(ctx)
}

func (a *Adapter) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.gate:
		return nil
	}
}

func (a *Adapter) unlock() {
	a.gate <- struct{}{}
}

func (a *Adapter) verifyLocked(ctx context.Context) error {
	verifyContext, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.lease.Verify(verifyContext)
}

func (a *Adapter) valid() bool {
	return a != nil && a.inner != nil && a.lease != nil && a.gate != nil
}

func closeAdapters(adapters []airuntime.Adapter) {
	for _, adapter := range adapters {
		if closer, ok := adapter.(airuntime.AdapterCloser); ok {
			_ = closer.Close()
		}
	}
}

func approvalRuntimeError(ctx context.Context, err error) error {
	switch {
	case errors.Is(ctx.Err(), context.Canceled) ||
		errors.Is(err, context.Canceled):
		return airuntime.NewError(airuntime.ErrorCanceled, nil)
	case errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(err, context.DeadlineExceeded):
		return airuntime.NewError(airuntime.ErrorTimeout, nil)
	default:
		return airuntime.NewError(airuntime.ErrorUnavailable, nil)
	}
}
