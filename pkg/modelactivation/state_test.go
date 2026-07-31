package modelactivation

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
)

func newPersistentController(
	t *testing.T,
	fixture activationFixture,
	stateDir string,
	slots []Slot,
) *Controller {
	t.Helper()
	controller, err := New(fixture.manager, Config{
		OperationTimeout: time.Second,
		CleanupTimeout:   time.Second,
		StateDir:         stateDir,
		Slots:            slots,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func primarySlot(backend Backend) Slot {
	return Slot{
		Policy: SlotPolicy{
			ID: "primary", Model: "approved-model", Runtime: "fake",
			MaxModelBytes: 1 << 20,
		},
		Backend: backend,
	}
}

func readActivationState(t *testing.T, stateDir string) persistedActivationState {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(stateDir, activationStateFile))
	if err != nil {
		t.Fatal(err)
	}
	var state persistedActivationState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func abandonController(c *Controller) {
	c.mu.Lock()
	loaded := make([]*loadedModel, 0, len(c.slots)*2)
	for _, slot := range c.slots {
		if slot.active != nil {
			loaded = append(loaded, slot.active)
			slot.active = nil
		}
		if slot.orphan != nil {
			loaded = append(loaded, slot.orphan)
			slot.orphan = nil
		}
	}
	c.mu.Unlock()
	for _, item := range loaded {
		_ = c.manager.Deactivate(loadedDigest(item))
		_ = item.lease.Close()
	}
}

func loadRuntimeResidue(
	t *testing.T,
	fixture activationFixture,
	backend *fakeBackend,
	slot SlotPolicy,
	digest string,
) Binding {
	t.Helper()
	lease, err := fixture.manager.AcquireArtifact(digest)
	if err != nil {
		t.Fatal(err)
	}
	model := lease.Model()
	binding, err := backend.Load(context.Background(), LoadRequest{
		SlotID: slot.ID, Model: slot.Model, ModelDigest: digest,
		SizeBytes: model.SizeBytes, Artifact: lease,
	})
	_ = lease.Close()
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func TestPersistentActivationRequiresRecoveryAndRestoresDesired(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	status, err := controller.Status("primary")
	if err != nil || status.State != StateRecovering || status.ModelDigest != "" {
		t.Fatalf("initial persistent status=%#v err=%v", status, err)
	}
	digest := f.importModel(t, "model.gguf", []byte("persistent-model"))
	if _, err := controller.Activate(
		context.Background(), "primary", digest,
	); ErrorKindOf(err) != ErrorRecovery {
		t.Fatalf("activation bypassed recovery: %v", err)
	}
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(context.Background(), "primary", digest); err != nil {
		t.Fatal(err)
	}
	state := readActivationState(t, stateDir)
	if state.Generation != 1 || len(state.Slots) != 1 ||
		state.Slots[0] != (persistedActiveSlot{ID: "primary", Digest: digest}) {
		t.Fatalf("persisted activation state=%#v", state)
	}
	info, err := os.Lstat(filepath.Join(stateDir, activationStateFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("activation state mode=%v", info.Mode())
	}
	if err := controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if status, err := restarted.Status("primary"); err != nil ||
		status.State != StateRecovering || status.ModelDigest != digest {
		t.Fatalf("restart intent status=%#v err=%v", status, err)
	}
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err = restarted.Status("primary")
	if err != nil || status.State != StateActive || status.ModelDigest != digest {
		t.Fatalf("recovered status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.InUse != 1 {
		t.Fatalf("recovered artifact lease=%#v", model)
	}
	if _, err := restarted.Deactivate(
		context.Background(), "primary",
	); err != nil {
		t.Fatal(err)
	}
	state = readActivationState(t, stateDir)
	if state.Generation != 2 || len(state.Slots) != 0 {
		t.Fatalf("persisted inactive state=%#v", state)
	}
	if err := restarted.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	inactiveRestart := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := inactiveRestart.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status, err := inactiveRestart.Status("primary"); err != nil ||
		status.State != StateInactive || status.ModelDigest != "" {
		t.Fatalf("persisted inactive restart status=%#v err=%v", status, err)
	}
}

func TestRecoveryRollsBackCrashResidueToPersistedKnownGood(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	knownGood := f.importModel(t, "known.gguf", []byte("known-good"))
	candidate := f.importModel(t, "candidate.gguf", []byte("crash-candidate"))
	if _, err := controller.Activate(
		context.Background(), "primary", knownGood,
	); err != nil {
		t.Fatal(err)
	}
	loadRuntimeResidue(t, f, f.backend, primarySlot(f.backend).Policy, candidate)
	abandonController(controller)

	restarted := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := restarted.Status("primary")
	if err != nil || status.State != StateActive || status.ModelDigest != knownGood {
		t.Fatalf("known-good recovery status=%#v err=%v", status, err)
	}
	f.backend.mu.Lock()
	_, candidatePresent := f.backend.current["candidate-"+candidate[len("sha256:"):len("sha256:")+12]]
	f.backend.mu.Unlock()
	if candidatePresent {
		t.Fatal("crash candidate remained loaded")
	}
	if model := f.manager.Status(candidate); model.InUse != 0 {
		t.Fatalf("crash candidate lease=%#v", model)
	}
}

func TestRecoveryRemovesUncommittedFirstActivationResidue(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "candidate.gguf", []byte("uncommitted"))
	loadRuntimeResidue(t, f, f.backend, primarySlot(f.backend).Policy, digest)

	restarted := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := restarted.Status("primary")
	if err != nil || status.State != StateInactive || status.ModelDigest != "" {
		t.Fatalf("inactive recovery status=%#v err=%v", status, err)
	}
	f.backend.mu.Lock()
	current := len(f.backend.current)
	f.backend.mu.Unlock()
	if current != 0 {
		t.Fatalf("uncommitted runtime bindings=%d", current)
	}
}

func TestRecoveryVerifiesDesiredBeforeUnloadingRuntimeKnownGood(t *testing.T) {
	f := newActivationFixture(t)
	knownGood := f.importModel(t, "known.gguf", []byte("known-good"))
	candidate := f.importModel(t, "candidate.gguf", []byte("candidate"))
	loadRuntimeResidue(t, f, f.backend, primarySlot(f.backend).Policy, knownGood)

	stateDir := filepath.Join(t.TempDir(), "activation")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(persistedActivationState{
		Version: activationStateVersion, Generation: 1,
		Slots: []persistedActiveSlot{{ID: "primary", Digest: candidate}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(stateDir, activationStateFile), data, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	candidatePath := filepath.Join(
		f.root, strings.TrimPrefix(candidate, "sha256:")+".model",
	)
	if err := os.WriteFile(candidatePath, []byte("tampered!"), 0o600); err != nil {
		t.Fatal(err)
	}

	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(
		context.Background(),
	); ErrorKindOf(err) != ErrorRecovery {
		t.Fatalf("tampered desired recovery=%v", err)
	}
	f.backend.mu.Lock()
	current := make([]Binding, 0, len(f.backend.current))
	for _, binding := range f.backend.current {
		current = append(current, binding)
	}
	f.backend.mu.Unlock()
	if len(current) != 1 || current[0].ModelDigest != knownGood {
		t.Fatalf("runtime known-good was disturbed: %#v", current)
	}
	if model := f.manager.Status(candidate); model.InUse != 0 {
		t.Fatalf("tampered desired lease=%#v", model)
	}
}

func TestRecoveryDoesNotCreateThirdTransientBinding(t *testing.T) {
	f := newActivationFixture(t)
	desired := f.importModel(t, "desired.gguf", []byte("desired"))
	first := f.importModel(t, "first.gguf", []byte("first-residue"))
	second := f.importModel(t, "second.gguf", []byte("second-residue"))
	policy := primarySlot(f.backend).Policy
	loadRuntimeResidue(t, f, f.backend, policy, first)
	loadRuntimeResidue(t, f, f.backend, policy, second)
	beforeLoads, _, _ := f.backend.counts()

	stateDir := filepath.Join(t.TempDir(), "activation")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(persistedActivationState{
		Version: activationStateVersion, Generation: 1,
		Slots: []persistedActiveSlot{{ID: "primary", Digest: desired}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(stateDir, activationStateFile), data, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(
		context.Background(),
	); ErrorKindOf(err) != ErrorRecovery {
		t.Fatalf("full recovery slot error=%v", err)
	}
	afterLoads, _, _ := f.backend.counts()
	if afterLoads != beforeLoads {
		t.Fatalf("recovery created a third binding: loads %d -> %d",
			beforeLoads, afterLoads)
	}
	f.backend.mu.Lock()
	current := len(f.backend.current)
	f.backend.mu.Unlock()
	if current != MaxRecoveryBindingsHard {
		t.Fatalf("runtime binding count=%d", current)
	}
}

func TestPersistenceFailureFailsClosedAndRecoveryReleasesLease(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "model.gguf", []byte("write-failure"))
	controller.stateStore.writeState = func([]byte) error {
		return errors.New("private disk detail")
	}
	status, err := controller.Activate(context.Background(), "primary", digest)
	if ErrorKindOf(err) != ErrorPersistence || status.State != StateRecovering ||
		!status.CleanupPending || strings.Contains(err.Error(), "disk") {
		t.Fatalf("persistence failure status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.InUse != 1 {
		t.Fatalf("persistence failure lease=%#v", model)
	}
	controller.stateStore.writeState = controller.stateStore.writeAtomic
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if model := f.manager.Status(digest); model.InUse != 0 {
		t.Fatalf("recovery leaked retained lease=%#v", model)
	}
	status, err = controller.Status("primary")
	if err != nil || status.State != StateInactive || status.CleanupPending {
		t.Fatalf("post-failure recovery status=%#v err=%v", status, err)
	}
}

func TestDeactivatePersistenceFailurePreservesActiveBinding(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "model.gguf", []byte("active-write-failure"))
	if _, err := controller.Activate(
		context.Background(), "primary", digest,
	); err != nil {
		t.Fatal(err)
	}
	controller.stateStore.writeState = func([]byte) error {
		return errors.New("deactivate state failure")
	}
	status, err := controller.Deactivate(context.Background(), "primary")
	if ErrorKindOf(err) != ErrorPersistence || status.State != StateRecovering ||
		status.ModelDigest != digest {
		t.Fatalf("deactivate persistence status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.State != modelmanager.StateActive ||
		model.InUse != 1 {
		t.Fatalf("active model after state failure=%#v", model)
	}
	f.backend.mu.Lock()
	current := len(f.backend.current)
	f.backend.mu.Unlock()
	if current != 1 {
		t.Fatalf("active runtime binding count=%d", current)
	}
	controller.stateStore.writeState = controller.stateStore.writeAtomic
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err = controller.Status("primary")
	if err != nil || status.State != StateActive || status.ModelDigest != digest {
		t.Fatalf("reconciled active status=%#v err=%v", status, err)
	}
}

func TestRecoveryHealthFailureRetainsOneLeaseForRetry(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "model.gguf", []byte("recovery-health"))
	if _, err := controller.Activate(
		context.Background(), "primary", digest,
	); err != nil {
		t.Fatal(err)
	}
	if err := controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	f.backend.healthFn = func(context.Context, Binding) error {
		return errors.New("private unhealthy detail")
	}
	if err := restarted.Recover(
		context.Background(),
	); ErrorKindOf(err) != ErrorRecovery {
		t.Fatalf("health recovery error=%v", err)
	}
	status, err := restarted.Status("primary")
	if err != nil || status.State != StateRecovering || !status.CleanupPending ||
		status.ErrorCategory != ErrorRecovery {
		t.Fatalf("health recovery status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.InUse != 1 {
		t.Fatalf("health recovery retained lease=%#v", model)
	}
	f.backend.healthFn = nil
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err = restarted.Status("primary")
	if err != nil || status.State != StateActive || status.CleanupPending {
		t.Fatalf("health retry status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.InUse != 1 {
		t.Fatalf("health retry lease count=%#v", model)
	}
}

func TestFailedRollbackStateWriteRecoversCommittedCandidate(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	knownGood := f.importModel(t, "known.gguf", []byte("known"))
	candidate := f.importModel(t, "candidate.gguf", []byte("candidate"))
	if _, err := controller.Activate(
		context.Background(), "primary", knownGood,
	); err != nil {
		t.Fatal(err)
	}
	f.backend.unloadFn = func(_ context.Context, binding Binding) error {
		if binding.ModelDigest == knownGood {
			return errors.New("old unload failure")
		}
		return nil
	}
	writeAtomic := controller.stateStore.writeAtomic
	writeCount := 0
	controller.stateStore.writeState = func(data []byte) error {
		writeCount++
		if writeCount == 2 {
			return errors.New("rollback state failure")
		}
		return writeAtomic(data)
	}
	status, err := controller.Activate(context.Background(), "primary", candidate)
	if ErrorKindOf(err) != ErrorPersistence || status.State != StateRecovering ||
		status.ModelDigest != knownGood {
		t.Fatalf("failed rollback status=%#v err=%v", status, err)
	}
	state := readActivationState(t, stateDir)
	if len(state.Slots) != 1 || state.Slots[0].Digest != candidate {
		t.Fatalf("committed candidate intent=%#v", state)
	}
	controller.stateStore.writeState = writeAtomic
	f.backend.unloadFn = nil
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err = controller.Status("primary")
	if err != nil || status.State != StateActive || status.ModelDigest != candidate {
		t.Fatalf("candidate recovery status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(knownGood); model.InUse != 0 {
		t.Fatalf("known-good lease after committed recovery=%#v", model)
	}
}

func TestConcurrentSlotPersistenceDoesNotLoseUpdates(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	firstBackend, secondBackend := &fakeBackend{}, &fakeBackend{}
	slots := []Slot{
		{Policy: SlotPolicy{
			ID: "a-slot", Model: "approved-model", Runtime: "fake",
			MaxModelBytes: 1 << 20,
		}, Backend: firstBackend},
		{Policy: SlotPolicy{
			ID: "b-slot", Model: "approved-model", Runtime: "fake",
			MaxModelBytes: 1 << 20,
		}, Backend: secondBackend},
	}
	controller := newPersistentController(t, f, stateDir, slots)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := f.importModel(t, "first.gguf", []byte("first"))
	second := f.importModel(t, "second.gguf", []byte("second"))
	var wait sync.WaitGroup
	errorsFound := make(chan error, 2)
	for _, request := range []struct {
		slot   string
		digest string
	}{{"a-slot", first}, {"b-slot", second}} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := controller.Activate(
				context.Background(), request.slot, request.digest,
			)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	state := readActivationState(t, stateDir)
	if state.Generation != 2 || len(state.Slots) != 2 ||
		state.Slots[0].ID != "a-slot" || state.Slots[0].Digest != first ||
		state.Slots[1].ID != "b-slot" || state.Slots[1].Digest != second {
		t.Fatalf("concurrent persisted state=%#v", state)
	}
}

func TestPersistenceFailureStopsConcurrentSlotCommit(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	firstBackend, secondBackend := &fakeBackend{}, &fakeBackend{}
	slots := []Slot{
		{Policy: SlotPolicy{
			ID: "a-slot", Model: "approved-model", Runtime: "fake",
			MaxModelBytes: 1 << 20,
		}, Backend: firstBackend},
		{Policy: SlotPolicy{
			ID: "b-slot", Model: "approved-model", Runtime: "fake",
			MaxModelBytes: 1 << 20,
		}, Backend: secondBackend},
	}
	controller := newPersistentController(t, f, stateDir, slots)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := f.importModel(t, "first.gguf", []byte("first-failure"))
	second := f.importModel(t, "second.gguf", []byte("second-blocked"))
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	secondHealthy := make(chan struct{})
	secondBackend.healthFn = func(context.Context, Binding) error {
		close(secondHealthy)
		return nil
	}
	controller.stateStore.writeState = func([]byte) error {
		close(writeStarted)
		<-releaseWrite
		return errors.New("injected persistence failure")
	}
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	go func() {
		_, err := controller.Activate(context.Background(), "a-slot", first)
		firstResult <- err
	}()
	<-writeStarted
	go func() {
		_, err := controller.Activate(context.Background(), "b-slot", second)
		secondResult <- err
	}()
	<-secondHealthy
	close(releaseWrite)
	if err := <-firstResult; ErrorKindOf(err) != ErrorPersistence {
		t.Fatalf("first persistence error=%v", err)
	}
	if err := <-secondResult; ErrorKindOf(err) != ErrorPersistence {
		t.Fatalf("concurrent persistence error=%v", err)
	}
	for _, status := range controller.List() {
		if status.State != StateRecovering || !status.CleanupPending {
			t.Fatalf("concurrent recovery status=%#v", status)
		}
	}
	if _, err := os.Lstat(
		filepath.Join(stateDir, activationStateFile),
	); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed writes unexpectedly committed state: %v", err)
	}
	controller.stateStore.writeState = controller.stateStore.writeAtomic
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, digest := range []string{first, second} {
		if model := f.manager.Status(digest); model.InUse != 0 {
			t.Fatalf("concurrent retained lease=%#v", model)
		}
	}
}

func TestRepeatedPersistentLifecycleRemainsBounded(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "model.gguf", []byte("repeated-lifecycle"))
	const cycles = 32
	for index := 0; index < cycles; index++ {
		if _, err := controller.Activate(
			context.Background(), "primary", digest,
		); err != nil {
			t.Fatalf("activate cycle %d: %v", index, err)
		}
		if _, err := controller.Deactivate(
			context.Background(), "primary",
		); err != nil {
			t.Fatalf("deactivate cycle %d: %v", index, err)
		}
	}
	state := readActivationState(t, stateDir)
	if state.Generation != cycles*2 || len(state.Slots) != 0 {
		t.Fatalf("repeated lifecycle state=%#v", state)
	}
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != activationStateFile {
		t.Fatalf("activation state directory grew: %#v", entries)
	}
	if model := f.manager.Status(digest); model.InUse != 0 ||
		model.State != modelmanager.StateReady {
		t.Fatalf("repeated lifecycle model=%#v", model)
	}
	f.backend.mu.Lock()
	current := len(f.backend.current)
	f.backend.mu.Unlock()
	if current != 0 {
		t.Fatalf("repeated lifecycle runtime bindings=%d", current)
	}
}

func TestActivationStateTamperingAndBoundsFailClosed(t *testing.T) {
	f := newActivationFixture(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{
			name: "unknown-field",
			data: []byte(`{"version":1,"generation":1,"slots":[],"extra":true}`),
			mode: 0o600,
		},
		{name: "non-canonical", data: []byte(
			`{"version": 1,"generation":1,"slots":[]}`,
		), mode: 0o600},
		{name: "zero-generation", data: []byte(
			`{"version":1,"generation":0,"slots":[]}`,
		), mode: 0o600},
		{name: "unknown-slot", data: []byte(
			`{"version":1,"generation":1,"slots":[{"id":"other","digest":"` +
				digest + `"}]}`,
		), mode: 0o600},
		{name: "public-mode", data: []byte(
			`{"version":1,"generation":1,"slots":[]}`,
		), mode: 0o644},
		{name: "oversized", data: []byte(
			`{"version":1,"generation":1,"slots":[],"padding":"` +
				strings.Repeat("x", MaxActivationStateBytesHard) + `"}`,
		), mode: 0o600},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "activation")
			if err := os.MkdirAll(stateDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(
				filepath.Join(stateDir, activationStateFile), test.data, test.mode,
			); err != nil {
				t.Fatal(err)
			}
			_, err := New(f.manager, Config{
				OperationTimeout: time.Second, CleanupTimeout: time.Second,
				StateDir: stateDir, Slots: []Slot{primarySlot(f.backend)},
			})
			if ErrorKindOf(err) != ErrorPersistence {
				t.Fatalf("tampered state accepted: %v", err)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		parent := t.TempDir()
		stateDir := filepath.Join(parent, "activation")
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(parent, "target")
		if err := os.WriteFile(
			target, []byte(`{"version":1,"generation":1,"slots":[]}`), 0o600,
		); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(
			target, filepath.Join(stateDir, activationStateFile),
		); err != nil {
			t.Fatal(err)
		}
		_, err := New(f.manager, Config{
			OperationTimeout: time.Second, CleanupTimeout: time.Second,
			StateDir: stateDir, Slots: []Slot{primarySlot(f.backend)},
		})
		if ErrorKindOf(err) != ErrorPersistence {
			t.Fatalf("symlink state accepted: %v", err)
		}
	})
	t.Run("public-directory", func(t *testing.T) {
		stateDir := filepath.Join(t.TempDir(), "activation")
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(stateDir, 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := New(f.manager, Config{
			OperationTimeout: time.Second, CleanupTimeout: time.Second,
			StateDir: stateDir, Slots: []Slot{primarySlot(f.backend)},
		})
		if ErrorKindOf(err) != ErrorPersistence {
			t.Fatalf("public state directory accepted: %v", err)
		}
	})
}

func TestActivationStateCleansBoundedStageResidue(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(stateDir, activationStateStagePrefix+"crash")
	if err := os.WriteFile(stage, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging residue not removed: %v", err)
	}
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	tooMany := filepath.Join(t.TempDir(), "too-many")
	if err := os.MkdirAll(tooMany, 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= MaxActivationStateFilesHard; index++ {
		name := filepath.Join(
			tooMany, activationStateStagePrefix+strings.Repeat("x", index+1),
		)
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, err := New(f.manager, Config{
		OperationTimeout: time.Second, CleanupTimeout: time.Second,
		StateDir: tooMany, Slots: []Slot{primarySlot(f.backend)},
	})
	if ErrorKindOf(err) != ErrorPersistence {
		t.Fatalf("directory entry limit accepted: %v", err)
	}
}

func TestRecoverReloadsAndRejectsStateChangedAfterConstruction(t *testing.T) {
	f := newActivationFixture(t)
	stateDir := filepath.Join(t.TempDir(), "activation")
	controller := newPersistentController(
		t, f, stateDir, []Slot{primarySlot(f.backend)},
	)
	if err := os.WriteFile(
		filepath.Join(stateDir, "unexpected"), []byte("tampered"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := controller.Recover(
		context.Background(),
	); ErrorKindOf(err) != ErrorPersistence {
		t.Fatalf("post-construction tampering accepted: %v", err)
	}
	if _, err := controller.Activate(
		context.Background(), "primary", "sha256:"+strings.Repeat("a", 64),
	); ErrorKindOf(err) != ErrorRecovery {
		t.Fatalf("controller did not remain recovery-blocked: %v", err)
	}
}

type backendWithoutRecovery struct {
	delegate *fakeBackend
}

func (b backendWithoutRecovery) Load(
	ctx context.Context,
	request LoadRequest,
) (Binding, error) {
	return b.delegate.Load(ctx, request)
}

func (b backendWithoutRecovery) Health(
	ctx context.Context,
	binding Binding,
) error {
	return b.delegate.Health(ctx, binding)
}

func (b backendWithoutRecovery) Unload(
	ctx context.Context,
	binding Binding,
) error {
	return b.delegate.Unload(ctx, binding)
}

func TestRecoveryBackendAndBindingLimitsFailClosed(t *testing.T) {
	t.Run("backend unavailable", func(t *testing.T) {
		f := newActivationFixture(t)
		controller := newPersistentController(t, f,
			filepath.Join(t.TempDir(), "activation"),
			[]Slot{primarySlot(backendWithoutRecovery{delegate: f.backend})},
		)
		if err := controller.Recover(
			context.Background(),
		); ErrorKindOf(err) != ErrorRecovery {
			t.Fatalf("missing recovery backend=%v", err)
		}
	})
	t.Run("too many bindings", func(t *testing.T) {
		f := newActivationFixture(t)
		f.backend.inspectFn = func(
			context.Context,
			string,
		) (RecoveryBindings, error) {
			return RecoveryBindings{Count: MaxRecoveryBindingsHard + 1}, nil
		}
		controller := newPersistentController(t, f,
			filepath.Join(t.TempDir(), "activation"),
			[]Slot{primarySlot(f.backend)},
		)
		if err := controller.Recover(
			context.Background(),
		); ErrorKindOf(err) != ErrorRecovery {
			t.Fatalf("binding limit recovery=%v", err)
		}
		if status, _ := controller.Status("primary"); status.State != StateRecovering {
			t.Fatalf("binding limit status=%#v", status)
		}
	})
}
