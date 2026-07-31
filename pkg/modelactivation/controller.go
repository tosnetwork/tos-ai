// Package modelactivation coordinates operator-approved model artifacts with
// a narrowly scoped runtime loading backend. It does not accept task-selected
// paths, URLs, models, runtime endpoints, or executable payloads.
package modelactivation

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

const (
	MaxSlotsHard            = 64
	MaxIdentityBytes        = 256
	MaxHandleBytes          = 256
	MaxModelBytesHard       = uint64(1 << 40)
	MaxOperationTimeout     = 10 * time.Minute
	MaxCleanupTimeoutHard   = time.Minute
	MaxRecoveryBindingsHard = 2
)

type State string

const (
	StateInactive   State = "inactive"
	StateRecovering State = "recovering"
	StateLoading    State = "loading"
	StateActive     State = "active"
	StateDraining   State = "draining"
	StateFailed     State = "failed"
	StateClosed     State = "closed"
)

type ErrorKind string

const (
	ErrorInvalid     ErrorKind = "invalid"
	ErrorNotFound    ErrorKind = "not_found"
	ErrorBusy        ErrorKind = "busy"
	ErrorConflict    ErrorKind = "conflict"
	ErrorLimit       ErrorKind = "limit"
	ErrorCanceled    ErrorKind = "canceled"
	ErrorTimeout     ErrorKind = "timeout"
	ErrorArtifact    ErrorKind = "artifact"
	ErrorBackend     ErrorKind = "backend"
	ErrorBinding     ErrorKind = "binding"
	ErrorHealth      ErrorKind = "health"
	ErrorCleanup     ErrorKind = "cleanup"
	ErrorPersistence ErrorKind = "persistence"
	ErrorRecovery    ErrorKind = "recovery"
	ErrorClosed      ErrorKind = "closed"
	ErrorInternal    ErrorKind = "internal"
)

type Error struct {
	Kind  ErrorKind
	cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "model activation failed"
	}
	return "model activation " + string(e.Kind)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func ErrorKindOf(err error) ErrorKind {
	var activationError *Error
	if errors.As(err, &activationError) {
		return activationError.Kind
	}
	return ErrorInternal
}

func newError(kind ErrorKind, cause error) error {
	return &Error{Kind: kind, cause: cause}
}

type SlotPolicy struct {
	ID            string
	Model         string
	Runtime       string
	MaxModelBytes uint64
}

type LoadRequest struct {
	SlotID      string
	Model       string
	ModelDigest string
	SizeBytes   uint64
	Artifact    io.ReaderAt
}

// Binding is private controller/backend state. Handle must be an opaque,
// bounded backend identifier; it is deliberately omitted from Status.
type Binding struct {
	SlotID          string
	Model           string
	ModelDigest     string
	Runtime         string
	RuntimeRevision string
	Handle          string
	DigestEvidence  airuntime.BindingEvidence
}

// Backend is the narrow contract for a future audited Ollama, LocalAI, vLLM,
// llama.cpp, or vendor runtime loader.
//
// Load must not replace the currently active slot binding. It must create a
// separately unloadable candidate and attempt to clean partial state before
// returning an error. If cleanup cannot be confirmed after a create attempt,
// it must return the exact valid candidate Binding together with the error;
// the controller then retries cleanup and retains the lease if that retry also
// fails. Every method must honor cancellation and deadlines. Backend
// implementations are administrator configured and never receive task
// payloads, owner wallet keys, host paths, or arbitrary URLs. A backend shared
// by multiple configured slots must be concurrency-safe.
type Backend interface {
	Load(context.Context, LoadRequest) (Binding, error)
	Health(context.Context, Binding) error
	Unload(context.Context, Binding) error
}

type RecoveryBindings struct {
	Count    int
	Bindings [MaxRecoveryBindingsHard]Binding
}

// RecoveryBackend is optional for volatile controllers and mandatory when
// Config.StateDir is set. Inspect returns all bindings currently owned by one
// fixed administrator slot through a fixed-size value. Count outside
// [0, MaxRecoveryBindingsHard] fails closed. It must not discover or return
// unrelated runtime state.
type RecoveryBackend interface {
	Backend
	Inspect(context.Context, string) (RecoveryBindings, error)
}

type Slot struct {
	Policy  SlotPolicy
	Backend Backend
}

type Config struct {
	OperationTimeout time.Duration
	CleanupTimeout   time.Duration
	// StateDir enables crash-safe active/known-good intent. It must be an
	// absolute private directory separate from the model cache. A controller
	// with persistent state rejects lifecycle operations until Recover
	// succeeds.
	StateDir string
	Slots    []Slot
}

type Status struct {
	SlotID          string
	State           State
	Model           string
	ModelDigest     string
	Runtime         string
	RuntimeRevision string
	DigestEvidence  airuntime.BindingEvidence
	CleanupPending  bool
	ErrorCategory   ErrorKind
}

type loadedModel struct {
	binding Binding
	lease   *modelmanager.ArtifactLease
}

type runtimeSlot struct {
	policy  SlotPolicy
	backend Backend

	busy      bool
	state     State
	active    *loadedModel
	orphan    *loadedModel
	pending   string
	desired   string
	uncertain bool
	lastError ErrorKind
}

type Controller struct {
	mu             sync.Mutex
	closeMu        sync.Mutex
	recoveryMu     sync.Mutex
	stateMu        sync.Mutex
	manager        *modelmanager.Manager
	slots          map[string]*runtimeSlot
	orderedIDs     []string
	stateStore     *activationStateStore
	recoveryNeeded bool
	operationLimit time.Duration
	cleanupLimit   time.Duration
	closed         bool
}

func New(manager *modelmanager.Manager, config Config) (*Controller, error) {
	if manager == nil || config.OperationTimeout <= 0 ||
		config.OperationTimeout > MaxOperationTimeout ||
		config.CleanupTimeout <= 0 ||
		config.CleanupTimeout > MaxCleanupTimeoutHard ||
		len(config.Slots) == 0 || len(config.Slots) > MaxSlotsHard {
		return nil, newError(ErrorInvalid, nil)
	}
	controller := &Controller{
		manager: manager, slots: make(map[string]*runtimeSlot, len(config.Slots)),
		orderedIDs:     make([]string, 0, len(config.Slots)),
		operationLimit: config.OperationTimeout, cleanupLimit: config.CleanupTimeout,
	}
	for _, configured := range config.Slots {
		if configured.Backend == nil || validatePolicy(configured.Policy) != nil {
			return nil, newError(ErrorInvalid, nil)
		}
		if _, exists := controller.slots[configured.Policy.ID]; exists {
			return nil, newError(ErrorInvalid, nil)
		}
		controller.slots[configured.Policy.ID] = &runtimeSlot{
			policy: configured.Policy, backend: configured.Backend, state: StateInactive,
		}
		controller.orderedIDs = append(controller.orderedIDs, configured.Policy.ID)
	}
	sort.Strings(controller.orderedIDs)
	if config.StateDir != "" {
		store, persisted, err := openActivationStateStore(config.StateDir)
		if err != nil {
			return nil, newError(ErrorPersistence, err)
		}
		seenDigests := make(map[string]struct{}, len(persisted))
		for _, record := range persisted {
			slot := controller.slots[record.ID]
			if slot == nil {
				_ = store.close()
				return nil, newError(ErrorPersistence, nil)
			}
			if _, exists := seenDigests[record.Digest]; exists {
				_ = store.close()
				return nil, newError(ErrorPersistence, nil)
			}
			seenDigests[record.Digest] = struct{}{}
			slot.desired = record.Digest
		}
		controller.stateStore = store
		controller.recoveryNeeded = true
		for _, slot := range controller.slots {
			slot.state = StateRecovering
		}
	}
	return controller, nil
}

// Recover reconciles the private persisted active/known-good intent with the
// bounded runtime state reported by RecoveryBackend. It removes at most one
// unexpected candidate beside the desired binding, revalidates the complete
// artifact, health-checks an adopted binding, or reloads the desired artifact
// when the runtime no longer has it. Persistent controllers fail closed until
// this synchronous operation succeeds.
func (c *Controller) Recover(ctx context.Context) error {
	if ctx == nil {
		return newError(ErrorInvalid, nil)
	}
	if c.stateStore == nil {
		return nil
	}
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return newError(ErrorClosed, nil)
	}
	if !c.recoveryNeeded {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if err := c.reloadPersistedIntent(); err != nil {
		c.finishRecoveryAttempt(ErrorPersistence)
		return newError(ErrorPersistence, err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return newError(ErrorClosed, nil)
	}
	if !c.recoveryNeeded {
		c.mu.Unlock()
		return nil
	}
	for _, slot := range c.slots {
		if slot.busy {
			c.mu.Unlock()
			return newError(ErrorBusy, nil)
		}
	}
	for _, slot := range c.slots {
		slot.busy, slot.state, slot.lastError = true, StateRecovering, ""
	}
	c.mu.Unlock()

	for _, slotID := range c.orderedIDs {
		if err := c.recoverSlot(ctx, c.slots[slotID]); err != nil {
			c.finishRecoveryAttempt(ErrorRecovery)
			kind := contextErrorKind(ctx, err, ErrorRecovery)
			return newError(kind, err)
		}
	}
	c.mu.Lock()
	c.recoveryNeeded = false
	for _, slot := range c.slots {
		slot.busy, slot.lastError = false, ""
		if slot.active != nil {
			slot.state = StateActive
		} else {
			slot.state = StateInactive
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Controller) reloadPersistedIntent() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	state, exists, err := c.stateStore.read()
	if err != nil {
		return err
	}
	if !exists {
		c.mu.Lock()
		for _, slot := range c.slots {
			slot.desired = ""
		}
		c.stateStore.generation = 0
		c.mu.Unlock()
		return nil
	}
	seenDigests := make(map[string]struct{}, len(state.Slots))
	desired := make(map[string]string, len(state.Slots))
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range state.Slots {
		if c.slots[record.ID] == nil {
			return errors.New("unknown persisted activation slot")
		}
		if _, duplicate := seenDigests[record.Digest]; duplicate {
			return errors.New("duplicate persisted activation digest")
		}
		seenDigests[record.Digest] = struct{}{}
		desired[record.ID] = record.Digest
	}
	for id, slot := range c.slots {
		slot.desired = desired[id]
	}
	c.stateStore.generation = state.Generation
	return nil
}

func (c *Controller) recoverSlot(ctx context.Context, slot *runtimeSlot) error {
	backend, ok := slot.backend.(RecoveryBackend)
	if !ok {
		return errors.New("backend recovery is unavailable")
	}
	inspectContext, cancelInspect := context.WithTimeout(ctx, c.operationLimit)
	inspected, inspectErr, panicked := safeInspect(
		backend, inspectContext, slot.policy.ID,
	)
	cancelInspect()
	if panicked || inspectErr != nil {
		return errors.New("runtime inspection failed")
	}
	if inspected.Count < 0 || inspected.Count > MaxRecoveryBindingsHard {
		return errors.New("runtime recovery binding limit")
	}
	bindings := inspected.Bindings[:inspected.Count]
	seenDigests := make(map[string]struct{}, MaxRecoveryBindingsHard)
	seenHandles := make(map[string]struct{}, MaxRecoveryBindingsHard)
	var desiredBinding *Binding
	for index := range bindings {
		binding := bindings[index]
		if !validDigest(binding.ModelDigest) ||
			validateBinding(slot.policy, binding.ModelDigest, binding) != nil {
			return errors.New("invalid recovered binding")
		}
		if _, exists := seenDigests[binding.ModelDigest]; exists {
			return errors.New("duplicate recovered digest")
		}
		if _, exists := seenHandles[binding.Handle]; exists {
			return errors.New("duplicate recovered handle")
		}
		seenDigests[binding.ModelDigest] = struct{}{}
		seenHandles[binding.Handle] = struct{}{}
		if binding.ModelDigest == slot.desired {
			copy := binding
			desiredBinding = &copy
		}
	}

	if slot.desired == "" {
		for _, binding := range bindings {
			if err := c.unloadForRecovery(ctx, slot.backend, binding); err != nil {
				return err
			}
		}
		c.releaseRecoveredSlot(slot, nil)
		return nil
	}
	if desiredBinding == nil && len(bindings) >= MaxRecoveryBindingsHard {
		return errors.New("no bounded recovery candidate capacity")
	}

	lease, acquired, err := c.recoveryLease(slot, slot.desired)
	if err != nil {
		return err
	}
	keepLease := false
	defer func() {
		if acquired && !keepLease {
			_ = lease.Close()
		}
	}()
	model := lease.Model()
	if model.SizeBytes == 0 || model.SizeBytes > slot.policy.MaxModelBytes {
		return errors.New("recovered artifact exceeds policy")
	}
	verifyContext, cancelVerify := context.WithTimeout(ctx, c.operationLimit)
	verifyErr := lease.Verify(verifyContext)
	cancelVerify()
	if verifyErr != nil {
		return errors.New("recovered artifact verification failed")
	}

	var binding Binding
	loadedCandidate := false
	if desiredBinding != nil {
		binding = *desiredBinding
		healthContext, cancelHealth := context.WithTimeout(ctx, c.operationLimit)
		healthErr, _ := safeBackendCall(func() error {
			return slot.backend.Health(healthContext, binding)
		})
		cancelHealth()
		if healthErr != nil {
			keepLease = true
			c.retainRecoveryCandidate(
				slot, &loadedModel{binding: binding, lease: lease}, false,
			)
			return errors.New("recovered binding health failed")
		}
	} else {
		loadContext, cancelLoad := context.WithTimeout(ctx, c.operationLimit)
		loaded, loadErr, loadPanicked := safeLoad(slot.backend, loadContext, LoadRequest{
			SlotID: slot.policy.ID, Model: slot.policy.Model,
			ModelDigest: slot.desired, SizeBytes: model.SizeBytes, Artifact: lease,
		})
		cancelLoad()
		if loadPanicked || loadErr != nil {
			keepLease = true
			c.retainRecoveryCandidate(
				slot, &loadedModel{binding: loaded, lease: lease}, loadPanicked,
			)
			return errors.New("recovery load failed")
		}
		binding = loaded
		loadedCandidate = true
		if validateBinding(slot.policy, slot.desired, binding) != nil {
			_ = c.unloadForRecovery(ctx, slot.backend, binding)
			keepLease = true
			c.retainRecoveryCandidate(
				slot, &loadedModel{binding: binding, lease: lease}, false,
			)
			return errors.New("invalid recovery load binding")
		}
		healthContext, cancelHealth := context.WithTimeout(ctx, c.operationLimit)
		healthErr, _ := safeBackendCall(func() error {
			return slot.backend.Health(healthContext, binding)
		})
		cancelHealth()
		if healthErr != nil {
			_ = c.unloadForRecovery(ctx, slot.backend, binding)
			keepLease = true
			c.retainRecoveryCandidate(
				slot, &loadedModel{binding: binding, lease: lease}, false,
			)
			return errors.New("recovery load health failed")
		}
	}
	if err := c.manager.Activate(slot.desired); err != nil {
		if loadedCandidate {
			_ = c.unloadForRecovery(ctx, slot.backend, binding)
		}
		keepLease = true
		c.retainRecoveryCandidate(
			slot, &loadedModel{binding: binding, lease: lease}, false,
		)
		return errors.New("recovered artifact activation failed")
	}
	active := &loadedModel{binding: binding, lease: lease}
	for _, unexpected := range bindings {
		if unexpected.ModelDigest == slot.desired {
			continue
		}
		if err := c.unloadForRecovery(ctx, slot.backend, unexpected); err != nil {
			keepLease = true
			c.retainRecoveryCandidate(slot, active, false)
			return err
		}
	}
	keepLease = true
	c.releaseRecoveredSlot(slot, active)
	return nil
}

func (c *Controller) retainRecoveryCandidate(
	slot *runtimeSlot,
	retained *loadedModel,
	uncertain bool,
) {
	c.mu.Lock()
	previousOrphan := slot.orphan
	if slot.active != nil && slot.active.lease == retained.lease {
		slot.orphan = nil
	} else {
		slot.orphan = retained
	}
	slot.uncertain = slot.uncertain || uncertain
	c.mu.Unlock()
	if previousOrphan != nil && previousOrphan.lease != retained.lease {
		_ = c.manager.Deactivate(loadedDigest(previousOrphan))
		_ = previousOrphan.lease.Close()
	}
}

func (c *Controller) recoveryLease(
	slot *runtimeSlot,
	digest string,
) (*modelmanager.ArtifactLease, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot.active != nil && loadedDigest(slot.active) == digest &&
		slot.active.lease != nil {
		return slot.active.lease, false, nil
	}
	if slot.orphan != nil && loadedDigest(slot.orphan) == digest &&
		slot.orphan.lease != nil {
		return slot.orphan.lease, false, nil
	}
	lease, err := c.manager.AcquireArtifact(digest)
	return lease, true, err
}

func (c *Controller) releaseRecoveredSlot(
	slot *runtimeSlot,
	active *loadedModel,
) {
	c.mu.Lock()
	previousActive, previousOrphan := slot.active, slot.orphan
	slot.active, slot.orphan, slot.uncertain = active, nil, false
	c.mu.Unlock()
	for _, previous := range []*loadedModel{previousActive, previousOrphan} {
		if previous == nil || (active != nil && previous.lease == active.lease) {
			continue
		}
		_ = c.manager.Deactivate(loadedDigest(previous))
		_ = previous.lease.Close()
	}
}

func (c *Controller) unloadForRecovery(
	ctx context.Context,
	backend Backend,
	binding Binding,
) error {
	cleanupContext, cancelCleanup := context.WithTimeout(ctx, c.cleanupLimit)
	defer cancelCleanup()
	err, _ := safeBackendCall(func() error {
		return backend.Unload(cleanupContext, binding)
	})
	return err
}

func (c *Controller) finishRecoveryAttempt(kind ErrorKind) {
	c.mu.Lock()
	c.recoveryNeeded = true
	for _, slot := range c.slots {
		slot.busy, slot.state, slot.lastError = false, StateRecovering, kind
	}
	c.mu.Unlock()
}

func (c *Controller) Activate(
	ctx context.Context,
	slotID string,
	digest string,
) (Status, error) {
	if ctx == nil || !validDigest(digest) {
		return Status{}, newError(ErrorInvalid, nil)
	}
	slot, old, status, err := c.beginActivation(slotID, digest)
	if err != nil || slot == nil {
		return status, err
	}

	lease, acquireErr := c.manager.AcquireArtifact(digest)
	if acquireErr != nil {
		kind := ErrorArtifact
		if errors.Is(acquireErr, modelmanager.ErrNotReady) {
			kind = ErrorNotFound
		}
		return c.finishFailure(slot, old, nil, kind), newError(kind, acquireErr)
	}
	candidate := &loadedModel{lease: lease}
	model := lease.Model()
	if model.SizeBytes == 0 || model.SizeBytes > slot.policy.MaxModelBytes {
		_ = lease.Close()
		return c.finishFailure(slot, old, nil, ErrorLimit), newError(ErrorLimit, nil)
	}

	operationContext, cancelOperation := context.WithTimeout(ctx, c.operationLimit)
	if verifyErr := lease.Verify(operationContext); verifyErr != nil {
		cancelOperation()
		_ = lease.Close()
		kind := contextErrorKind(ctx, verifyErr, ErrorArtifact)
		return c.finishFailure(slot, old, nil, kind), newError(kind, nil)
	}
	binding, backendErr, panicked := safeLoad(slot.backend, operationContext, LoadRequest{
		SlotID: slot.policy.ID, Model: slot.policy.Model, ModelDigest: digest,
		SizeBytes: model.SizeBytes, Artifact: lease,
	})
	cancelOperation()
	candidate.binding = binding
	if panicked {
		return c.finishFailure(slot, old, candidate, ErrorInternal),
			newError(ErrorInternal, nil)
	}
	if backendErr != nil {
		if validateBinding(slot.policy, digest, candidate.binding) == nil {
			if cleanupErr := c.cleanupCandidate(slot, candidate); cleanupErr != nil {
				return c.finishFailure(
						slot, old, candidate, ErrorCleanup,
					),
					newError(ErrorCleanup, nil)
			}
		}
		_ = lease.Close()
		kind := contextErrorKind(ctx, backendErr, ErrorBackend)
		return c.finishFailure(slot, old, nil, kind), newError(kind, nil)
	}
	if err := validateBinding(slot.policy, digest, binding); err != nil {
		if cleanupErr := c.cleanupCandidate(slot, candidate); cleanupErr != nil {
			return c.finishFailure(slot, old, candidate, ErrorCleanup),
				newError(ErrorCleanup, nil)
		}
		_ = lease.Close()
		return c.finishFailure(slot, old, nil, ErrorBinding), newError(ErrorBinding, nil)
	}

	healthContext, cancelHealth := context.WithTimeout(ctx, c.operationLimit)
	healthErr, _ := safeBackendCall(func() error {
		return slot.backend.Health(healthContext, binding)
	})
	cancelHealth()
	if healthErr != nil {
		if cleanupErr := c.cleanupCandidate(slot, candidate); cleanupErr != nil {
			return c.finishFailure(slot, old, candidate, ErrorCleanup),
				newError(ErrorCleanup, nil)
		}
		_ = lease.Close()
		kind := contextErrorKind(ctx, healthErr, ErrorHealth)
		return c.finishFailure(slot, old, nil, kind), newError(kind, nil)
	}
	if err := ctx.Err(); err != nil {
		if cleanupErr := c.cleanupCandidate(slot, candidate); cleanupErr != nil {
			return c.finishFailure(slot, old, candidate, ErrorCleanup),
				newError(ErrorCleanup, nil)
		}
		_ = lease.Close()
		kind := contextErrorKind(ctx, err, ErrorCanceled)
		return c.finishFailure(slot, old, nil, kind), newError(kind, nil)
	}

	if err := c.persistDesired(slot.policy.ID, digest); err != nil {
		_ = c.cleanupCandidate(slot, candidate)
		return c.finishRecoveryFailure(slot, old, candidate),
			newError(ErrorPersistence, err)
	}

	if err := c.manager.Activate(digest); err != nil {
		return c.rollbackPersistedCandidate(
			slot, old, candidate, ErrorArtifact,
		)
	}

	if old != nil {
		if err := c.manager.Drain(old.binding.ModelDigest); err != nil {
			return c.rollbackPersistedCandidate(
				slot, old, candidate, ErrorInternal,
			)
		}
		unloadContext, cancelUnload := context.WithTimeout(ctx, c.operationLimit)
		unloadErr, _ := safeBackendCall(func() error {
			return slot.backend.Unload(unloadContext, old.binding)
		})
		cancelUnload()
		if unloadErr != nil {
			_ = c.manager.Activate(old.binding.ModelDigest)
			return c.rollbackPersistedCandidate(
				slot, old, candidate, contextErrorKind(ctx, unloadErr, ErrorCleanup),
			)
		}
		_ = c.manager.Deactivate(old.binding.ModelDigest)
		_ = old.lease.Close()
	}
	return c.finishSuccess(slot, candidate), nil
}

func (c *Controller) Deactivate(ctx context.Context, slotID string) (Status, error) {
	if ctx == nil {
		return Status{}, newError(ErrorInvalid, nil)
	}
	c.mu.Lock()
	slot := c.slots[slotID]
	if slot == nil {
		c.mu.Unlock()
		return Status{}, newError(ErrorNotFound, nil)
	}
	if c.closed {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorClosed, nil)
	}
	if c.recoveryNeeded {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorRecovery, nil)
	}
	if slot.busy {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorBusy, nil)
	}
	if slot.orphan != nil {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorCleanup, nil)
	}
	if slot.active == nil {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, nil
	}
	active := slot.active
	slot.busy, slot.state, slot.lastError = true, StateDraining, ""
	c.mu.Unlock()

	if err := c.persistDesired(slot.policy.ID, ""); err != nil {
		return c.finishRecoveryFailure(slot, active, nil),
			newError(ErrorPersistence, err)
	}
	if err := c.manager.Drain(active.binding.ModelDigest); err != nil {
		if persistErr := c.persistDesired(
			slot.policy.ID, active.binding.ModelDigest,
		); persistErr != nil {
			return c.finishRecoveryFailure(slot, active, nil),
				newError(ErrorPersistence, persistErr)
		}
		return c.finishDeactivateFailure(slot, ErrorArtifact),
			newError(ErrorArtifact, err)
	}
	operationContext, cancelOperation := context.WithTimeout(ctx, c.operationLimit)
	unloadErr, _ := safeBackendCall(func() error {
		return slot.backend.Unload(operationContext, active.binding)
	})
	cancelOperation()
	if unloadErr != nil {
		_ = c.manager.Activate(active.binding.ModelDigest)
		if persistErr := c.persistDesired(
			slot.policy.ID, active.binding.ModelDigest,
		); persistErr != nil {
			return c.finishRecoveryFailure(slot, active, nil),
				newError(ErrorPersistence, persistErr)
		}
		kind := contextErrorKind(ctx, unloadErr, ErrorBackend)
		return c.finishDeactivateFailure(slot, kind), newError(kind, nil)
	}
	_ = c.manager.Deactivate(active.binding.ModelDigest)
	_ = active.lease.Close()
	c.mu.Lock()
	slot.active, slot.busy, slot.state, slot.lastError = nil, false, StateInactive, ""
	if c.recoveryNeeded {
		slot.state, slot.lastError = StateRecovering, ErrorRecovery
	}
	status := statusLocked(slot)
	c.mu.Unlock()
	return status, nil
}

func (c *Controller) RetryCleanup(ctx context.Context, slotID string) (Status, error) {
	if ctx == nil {
		return Status{}, newError(ErrorInvalid, nil)
	}
	c.mu.Lock()
	slot := c.slots[slotID]
	if slot == nil {
		c.mu.Unlock()
		return Status{}, newError(ErrorNotFound, nil)
	}
	if c.closed {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorClosed, nil)
	}
	if c.recoveryNeeded {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorRecovery, nil)
	}
	if slot.busy {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorBusy, nil)
	}
	orphan := slot.orphan
	if orphan == nil {
		uncertain := slot.uncertain
		status := statusLocked(slot)
		c.mu.Unlock()
		if uncertain {
			return status, newError(ErrorCleanup, nil)
		}
		return status, nil
	}
	if orphan.binding.Handle == "" {
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorCleanup, nil)
	}
	slot.busy = true
	c.mu.Unlock()

	cleanupContext, cancelCleanup := context.WithTimeout(ctx, c.cleanupLimit)
	cleanupErr, _ := safeBackendCall(func() error {
		return slot.backend.Unload(cleanupContext, orphan.binding)
	})
	cancelCleanup()
	if cleanupErr != nil {
		c.mu.Lock()
		slot.busy, slot.lastError = false, ErrorCleanup
		status := statusLocked(slot)
		c.mu.Unlock()
		return status, newError(ErrorCleanup, nil)
	}
	_ = c.manager.Deactivate(orphan.binding.ModelDigest)
	_ = orphan.lease.Close()
	c.mu.Lock()
	slot.orphan, slot.busy, slot.lastError = nil, false, ""
	if slot.active != nil {
		slot.state = StateActive
	} else {
		slot.state = StateInactive
	}
	if c.recoveryNeeded {
		slot.state, slot.lastError = StateRecovering, ErrorRecovery
	}
	status := statusLocked(slot)
	c.mu.Unlock()
	return status, nil
}

func (c *Controller) Status(slotID string) (Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	slot := c.slots[slotID]
	if slot == nil {
		return Status{}, newError(ErrorNotFound, nil)
	}
	return statusLocked(slot), nil
}

func (c *Controller) List() []Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]Status, 0, len(c.orderedIDs))
	for _, slotID := range c.orderedIDs {
		result = append(result, statusLocked(c.slots[slotID]))
	}
	return result
}

// Close unloads fixed slot state synchronously. It creates no watcher or
// background goroutine. A failed unload remains pinned and can be retried by a
// later Close call; this deliberately favors artifact safety over eviction.
func (c *Controller) Close(ctx context.Context) error {
	if ctx == nil {
		return newError(ErrorInvalid, nil)
	}
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	c.mu.Lock()
	for _, slot := range c.slots {
		if slot.busy {
			c.mu.Unlock()
			return newError(ErrorBusy, nil)
		}
	}
	c.closed = true
	c.mu.Unlock()

	var closeErr error
	for _, slotID := range c.orderedIDs {
		c.mu.Lock()
		slot := c.slots[slotID]
		orphan, active, uncertain := slot.orphan, slot.active, slot.uncertain
		slot.busy, slot.state = true, StateClosed
		c.mu.Unlock()
		slotFailure := false

		if uncertain {
			closeErr = newError(ErrorCleanup, nil)
			slotFailure = true
		}
		if orphan != nil && orphan.binding.Handle != "" {
			if err := c.unloadForClose(ctx, slot, orphan); err == nil {
				_ = c.manager.Deactivate(orphan.binding.ModelDigest)
				_ = orphan.lease.Close()
				orphan = nil
			} else {
				closeErr = newError(ErrorCleanup, nil)
				slotFailure = true
			}
		} else if orphan != nil {
			_ = orphan.lease.Close()
			orphan = nil
			uncertain = true
			closeErr = newError(ErrorCleanup, nil)
			slotFailure = true
		}
		if active != nil {
			_ = c.manager.Drain(active.binding.ModelDigest)
			if err := c.unloadForClose(ctx, slot, active); err == nil {
				_ = c.manager.Deactivate(active.binding.ModelDigest)
				_ = active.lease.Close()
				active = nil
			} else {
				_ = c.manager.Activate(active.binding.ModelDigest)
				closeErr = newError(ErrorCleanup, nil)
				slotFailure = true
			}
		}
		c.mu.Lock()
		slot.orphan, slot.active, slot.pending = orphan, active, ""
		slot.uncertain, slot.busy = uncertain, false
		if slotFailure {
			slot.lastError = ErrorCleanup
		} else {
			slot.lastError = ""
		}
		c.mu.Unlock()
	}
	if closeErr == nil && c.stateStore != nil {
		if err := c.stateStore.close(); err != nil {
			closeErr = newError(ErrorPersistence, err)
		}
	}
	return closeErr
}

func (c *Controller) beginActivation(
	slotID string,
	digest string,
) (*runtimeSlot, *loadedModel, Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	slot := c.slots[slotID]
	if slot == nil {
		return nil, nil, Status{}, newError(ErrorNotFound, nil)
	}
	if c.closed {
		return nil, nil, statusLocked(slot), newError(ErrorClosed, nil)
	}
	if c.recoveryNeeded {
		return nil, nil, statusLocked(slot), newError(ErrorRecovery, nil)
	}
	if slot.busy {
		return nil, nil, statusLocked(slot), newError(ErrorBusy, nil)
	}
	if slot.orphan != nil || slot.uncertain {
		return nil, nil, statusLocked(slot), newError(ErrorCleanup, nil)
	}
	if slot.active != nil && slot.active.binding.ModelDigest == digest {
		return nil, nil, statusLocked(slot), nil
	}
	for _, candidate := range c.slots {
		if candidate == slot {
			continue
		}
		if (candidate.active != nil && loadedDigest(candidate.active) == digest) ||
			(candidate.orphan != nil && loadedDigest(candidate.orphan) == digest) ||
			candidate.pending == digest {
			return nil, nil, statusLocked(slot), newError(ErrorConflict, nil)
		}
	}
	old := slot.active
	slot.busy, slot.state, slot.pending, slot.lastError = true, StateLoading, digest, ""
	return slot, old, Status{}, nil
}

func (c *Controller) rollbackPersistedCandidate(
	slot *runtimeSlot,
	old *loadedModel,
	candidate *loadedModel,
	kind ErrorKind,
) (Status, error) {
	cleanupErr := c.cleanupCandidate(slot, candidate)
	oldDigest := ""
	if old != nil {
		oldDigest = old.binding.ModelDigest
	}
	persistErr := c.persistDesired(slot.policy.ID, oldDigest)
	if persistErr != nil {
		if cleanupErr == nil {
			_ = c.manager.Deactivate(candidate.binding.ModelDigest)
		}
		return c.finishRecoveryFailure(slot, old, candidate),
			newError(ErrorPersistence, persistErr)
	}
	if cleanupErr == nil {
		_ = c.manager.Deactivate(candidate.binding.ModelDigest)
		_ = candidate.lease.Close()
		return c.finishFailure(slot, old, nil, kind), newError(kind, nil)
	}
	return c.finishFailure(slot, old, candidate, ErrorCleanup),
		newError(ErrorCleanup, nil)
}

func (c *Controller) finishRecoveryFailure(
	slot *runtimeSlot,
	old *loadedModel,
	retained *loadedModel,
) Status {
	c.mu.Lock()
	c.recoveryNeeded = true
	for _, candidate := range c.slots {
		if candidate.state != StateClosed {
			candidate.state, candidate.lastError = StateRecovering, ErrorRecovery
		}
	}
	slot.active, slot.orphan, slot.pending = old, retained, ""
	slot.busy, slot.state, slot.lastError = false, StateRecovering, ErrorPersistence
	status := statusLocked(slot)
	c.mu.Unlock()
	return status
}

func (c *Controller) persistDesired(slotID string, digest string) error {
	if c.stateStore == nil {
		return nil
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	c.mu.Lock()
	if c.slots[slotID] == nil {
		c.mu.Unlock()
		return errors.New("unknown activation slot")
	}
	if c.recoveryNeeded {
		c.mu.Unlock()
		return errors.New("activation recovery is required")
	}
	records := make([]persistedActiveSlot, 0, len(c.slots))
	for _, id := range c.orderedIDs {
		current := c.slots[id].desired
		if id == slotID {
			current = digest
		}
		if current != "" {
			records = append(records, persistedActiveSlot{ID: id, Digest: current})
		}
	}
	c.mu.Unlock()

	if err := c.stateStore.save(records); err != nil {
		c.mu.Lock()
		c.recoveryNeeded = true
		for _, slot := range c.slots {
			if slot.state != StateClosed {
				slot.state, slot.lastError = StateRecovering, ErrorRecovery
			}
		}
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	c.slots[slotID].desired = digest
	c.mu.Unlock()
	return nil
}

func (c *Controller) cleanupCandidate(slot *runtimeSlot, candidate *loadedModel) error {
	cleanupContext, cancelCleanup := context.WithTimeout(
		context.Background(), c.cleanupLimit,
	)
	defer cancelCleanup()
	err, _ := safeBackendCall(func() error {
		return slot.backend.Unload(cleanupContext, candidate.binding)
	})
	return err
}

func (c *Controller) unloadForClose(
	ctx context.Context,
	slot *runtimeSlot,
	loaded *loadedModel,
) error {
	cleanupContext, cancelCleanup := context.WithTimeout(ctx, c.cleanupLimit)
	defer cancelCleanup()
	err, _ := safeBackendCall(func() error {
		return slot.backend.Unload(cleanupContext, loaded.binding)
	})
	return err
}

func (c *Controller) finishSuccess(slot *runtimeSlot, active *loadedModel) Status {
	c.mu.Lock()
	slot.active, slot.orphan, slot.pending = active, nil, ""
	slot.busy, slot.state, slot.lastError = false, StateActive, ""
	if c.recoveryNeeded {
		slot.state, slot.lastError = StateRecovering, ErrorRecovery
	}
	status := statusLocked(slot)
	c.mu.Unlock()
	return status
}

func (c *Controller) finishFailure(
	slot *runtimeSlot,
	old *loadedModel,
	orphan *loadedModel,
	kind ErrorKind,
) Status {
	c.mu.Lock()
	slot.active, slot.orphan, slot.pending = old, orphan, ""
	slot.busy, slot.lastError = false, kind
	if c.recoveryNeeded {
		slot.state, slot.lastError = StateRecovering, ErrorRecovery
	} else if old != nil {
		slot.state = StateActive
	} else {
		slot.state = StateFailed
	}
	status := statusLocked(slot)
	c.mu.Unlock()
	return status
}

func (c *Controller) finishDeactivateFailure(slot *runtimeSlot, kind ErrorKind) Status {
	c.mu.Lock()
	slot.busy, slot.state, slot.lastError = false, StateActive, kind
	if c.recoveryNeeded {
		slot.state, slot.lastError = StateRecovering, ErrorRecovery
	}
	status := statusLocked(slot)
	c.mu.Unlock()
	return status
}

func statusLocked(slot *runtimeSlot) Status {
	status := Status{
		SlotID: slot.policy.ID, State: slot.state,
		Model: slot.policy.Model, Runtime: slot.policy.Runtime,
		CleanupPending: slot.orphan != nil || slot.uncertain,
		ErrorCategory:  slot.lastError,
	}
	if slot.active != nil {
		status.ModelDigest = slot.active.binding.ModelDigest
		status.RuntimeRevision = slot.active.binding.RuntimeRevision
		status.DigestEvidence = slot.active.binding.DigestEvidence
	} else if slot.state == StateRecovering {
		status.ModelDigest = slot.desired
	}
	return status
}

func loadedDigest(loaded *loadedModel) string {
	if loaded == nil {
		return ""
	}
	if loaded.binding.ModelDigest != "" {
		return loaded.binding.ModelDigest
	}
	if loaded.lease != nil {
		return loaded.lease.Model().Digest
	}
	return ""
}

func validatePolicy(policy SlotPolicy) error {
	if !validIdentity(policy.ID) || !validIdentity(policy.Model) ||
		!validIdentity(policy.Runtime) || policy.MaxModelBytes == 0 ||
		policy.MaxModelBytes > MaxModelBytesHard {
		return errors.New("invalid slot policy")
	}
	return nil
}

func validateBinding(policy SlotPolicy, digest string, binding Binding) error {
	if binding.SlotID != policy.ID || binding.Model != policy.Model ||
		binding.ModelDigest != digest || binding.Runtime != policy.Runtime ||
		!validIdentity(binding.RuntimeRevision) || !validHandle(binding.Handle) ||
		binding.DigestEvidence != airuntime.BindingLocallyObserved {
		return errors.New("invalid runtime binding")
	}
	return nil
}

func validIdentity(value string) bool {
	return value != "" && len(value) <= MaxIdentityBytes &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func validHandle(value string) bool {
	return value != "" && len(value) <= MaxHandleBytes &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	digest := strings.TrimPrefix(value, "sha256:")
	if digest != strings.ToLower(digest) {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func safeLoad(
	backend Backend,
	ctx context.Context,
	request LoadRequest,
) (binding Binding, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			binding, err, panicked = Binding{}, nil, true
		}
	}()
	binding, err = backend.Load(ctx, request)
	return binding, err, false
}

func safeInspect(
	backend RecoveryBackend,
	ctx context.Context,
	slotID string,
) (bindings RecoveryBindings, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			bindings, err, panicked = RecoveryBindings{}, nil, true
		}
	}()
	bindings, err = backend.Inspect(ctx, slotID)
	return bindings, err, false
}

func safeBackendCall(call func() error) (err error, panicked bool) {
	defer func() {
		if recover() != nil {
			err, panicked = newError(ErrorInternal, nil), true
		}
	}()
	return call(), false
}

func contextErrorKind(ctx context.Context, err error, fallback ErrorKind) ErrorKind {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return ErrorCanceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(err, context.DeadlineExceeded) {
		return ErrorTimeout
	}
	return fallback
}
