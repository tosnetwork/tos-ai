package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

type preflightConfig struct {
	timeout    time.Duration
	successTTL time.Duration
	failureTTL time.Duration
	maxWaiters int
	now        func() time.Time
}

type runtimeSlot struct {
	adapter    airuntime.Adapter
	capability airuntime.Capability
	config     preflightConfig
	waiters    chan struct{}

	mu        sync.Mutex
	checking  bool
	done      chan struct{}
	checked   bool
	checkedAt time.Time
	ready     bool
	result    airuntime.Preflight
	errorKind airuntime.ErrorKind
}

type runtimeSlotSnapshot struct {
	checked  bool
	ready    bool
	evidence airuntime.BindingEvidence
}

type preflightFailure struct {
	kind airuntime.ErrorKind
}

func (e *preflightFailure) Error() string {
	return "runtime preflight failed"
}

func newPreflightFailure(err error) error {
	return &preflightFailure{kind: airuntime.ErrorKindOf(err)}
}

func newRuntimeSlot(
	adapter airuntime.Adapter,
	capability airuntime.Capability,
	config preflightConfig,
) *runtimeSlot {
	return &runtimeSlot{
		adapter: adapter, capability: capability, config: config,
		waiters: make(chan struct{}, config.maxWaiters),
	}
}

func (s *runtimeSlot) ensure(ctx context.Context, force bool) (airuntime.Preflight, error) {
	if err := ctx.Err(); err != nil {
		return airuntime.Preflight{}, preflightContextError(err)
	}
	now := s.config.now()
	s.mu.Lock()
	if !force && s.cacheFreshLocked(now) {
		result, err := s.cachedResultLocked()
		s.mu.Unlock()
		return result, err
	}
	if s.checking {
		done := s.done
		s.mu.Unlock()
		select {
		case s.waiters <- struct{}{}:
			defer func() { <-s.waiters }()
		default:
			return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorLimit, nil)
		}
		select {
		case <-ctx.Done():
			return airuntime.Preflight{}, preflightContextError(ctx.Err())
		case <-done:
		}
		s.mu.Lock()
		result, err := s.cachedResultLocked()
		s.mu.Unlock()
		return result, err
	}
	s.checking = true
	s.done = make(chan struct{})
	done := s.done
	s.mu.Unlock()

	go s.runCheck(ctx, done)
	select {
	case <-ctx.Done():
		return airuntime.Preflight{}, preflightContextError(ctx.Err())
	case <-done:
	}
	s.mu.Lock()
	result, err := s.cachedResultLocked()
	s.mu.Unlock()
	return result, err
}

func (s *runtimeSlot) runCheck(parent context.Context, done chan struct{}) {
	timeout := s.config.timeout
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			s.finishCheck(done, airuntime.Preflight{}, airuntime.ErrorTimeout)
			return
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	probeContext, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	result, err := safePreflight(s.adapter, s.capability, probeContext)
	cancel()
	kind := airuntime.ErrorKind("")
	if err != nil {
		kind = airuntime.ErrorKindOf(err)
	}
	s.finishCheck(done, result, kind)
}

func (s *runtimeSlot) finishCheck(
	done chan struct{},
	result airuntime.Preflight,
	kind airuntime.ErrorKind,
) {
	s.mu.Lock()
	s.checked = true
	s.checkedAt = s.config.now()
	s.ready = kind == ""
	s.result = result
	s.errorKind = kind
	s.checking = false
	close(done)
	s.mu.Unlock()
}

func (s *runtimeSlot) cacheFreshLocked(now time.Time) bool {
	if !s.checked || now.Before(s.checkedAt) {
		return false
	}
	ttl := s.config.failureTTL
	if s.ready {
		ttl = s.config.successTTL
	}
	return now.Sub(s.checkedAt) < ttl
}

func (s *runtimeSlot) cachedResultLocked() (airuntime.Preflight, error) {
	if s.ready {
		return s.result, nil
	}
	kind := s.errorKind
	if kind == "" {
		kind = airuntime.ErrorUnavailable
	}
	return airuntime.Preflight{}, airuntime.NewError(kind, nil)
}

func (s *runtimeSlot) snapshot() runtimeSlotSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	ready := s.ready && s.cacheFreshLocked(s.config.now())
	evidence := s.result.DigestEvidence
	if !ready {
		evidence = ""
	}
	return runtimeSlotSnapshot{
		checked: s.checked, ready: ready,
		evidence: evidence,
	}
}

func safePreflight(
	adapter airuntime.Adapter,
	capability airuntime.Capability,
	ctx context.Context,
) (result airuntime.Preflight, err error) {
	defer func() {
		if recover() != nil {
			result = airuntime.Preflight{}
			err = airuntime.NewError(airuntime.ErrorInternal, nil)
		}
	}()
	result, err = adapter.Preflight(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return airuntime.Preflight{}, preflightContextError(err)
		}
		var runtimeError *airuntime.Error
		if errors.As(err, &runtimeError) {
			return airuntime.Preflight{}, err
		}
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorInternal, nil)
	}
	if err := airuntime.ValidatePreflight(capability, result); err != nil {
		return airuntime.Preflight{}, airuntime.NewError(airuntime.ErrorProtocol, nil)
	}
	return result, nil
}

func preflightContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
	}
	return airuntime.NewError(airuntime.ErrorCanceled, context.Canceled)
}
