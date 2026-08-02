package modelactivation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/update"
)

type fakeBackend struct {
	mu sync.Mutex

	loadCalls   int
	healthCalls int
	unloadCalls int
	loaded      []Binding
	unloaded    []Binding
	current     map[string]Binding

	loadFn    func(context.Context, LoadRequest) (Binding, error)
	healthFn  func(context.Context, Binding) error
	unloadFn  func(context.Context, Binding) error
	inspectFn func(context.Context, string) (RecoveryBindings, error)
}

func (f *fakeBackend) Load(ctx context.Context, request LoadRequest) (Binding, error) {
	f.mu.Lock()
	f.loadCalls++
	loadFn := f.loadFn
	f.mu.Unlock()
	var binding Binding
	var err error
	if loadFn != nil {
		binding, err = loadFn(ctx, request)
	} else {
		var data []byte
		data, err = io.ReadAll(
			io.NewSectionReader(request.Artifact, 0, int64(request.SizeBytes)),
		)
		if err == nil && uint64(len(data)) != request.SizeBytes {
			err = errors.New("fake artifact read")
		}
		binding = matchingBinding(request)
	}
	if err != nil {
		return binding, err
	}
	f.mu.Lock()
	f.loaded = append(f.loaded, binding)
	if f.current == nil {
		f.current = make(map[string]Binding)
	}
	if binding.Handle != "" {
		f.current[binding.Handle] = binding
	}
	f.mu.Unlock()
	return binding, nil
}

func (f *fakeBackend) Health(ctx context.Context, binding Binding) error {
	f.mu.Lock()
	f.healthCalls++
	healthFn := f.healthFn
	f.mu.Unlock()
	if healthFn != nil {
		return healthFn(ctx, binding)
	}
	return nil
}

func (f *fakeBackend) Unload(ctx context.Context, binding Binding) error {
	f.mu.Lock()
	f.unloadCalls++
	unloadFn := f.unloadFn
	f.mu.Unlock()
	if unloadFn != nil {
		if err := unloadFn(ctx, binding); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.unloaded = append(f.unloaded, binding)
	delete(f.current, binding.Handle)
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) Inspect(
	ctx context.Context,
	slotID string,
) (RecoveryBindings, error) {
	f.mu.Lock()
	inspectFn := f.inspectFn
	f.mu.Unlock()
	if inspectFn != nil {
		return inspectFn(ctx, slotID)
	}
	if err := ctx.Err(); err != nil {
		return RecoveryBindings{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	result := RecoveryBindings{}
	for _, binding := range f.current {
		if binding.SlotID == slotID {
			if result.Count < MaxRecoveryBindingsHard {
				result.Bindings[result.Count] = binding
			}
			result.Count++
		}
	}
	if result.Count <= MaxRecoveryBindingsHard {
		sort.Slice(result.Bindings[:result.Count], func(i, j int) bool {
			return result.Bindings[i].Handle < result.Bindings[j].Handle
		})
	}
	return result, nil
}

func (f *fakeBackend) counts() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadCalls, f.healthCalls, f.unloadCalls
}

type activationFixture struct {
	manager    *modelmanager.Manager
	controller *Controller
	backend    *fakeBackend
	privateKey ed25519.PrivateKey
	now        time.Time
	root       string
}

func newActivationFixture(t *testing.T) activationFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	root := filepath.Join(t.TempDir(), "models")
	manager, err := modelmanager.New(modelmanager.Config{
		RootDir: root, Target: "linux/amd64/cuda",
		CurrentSecurityRevision: 7, MaxModels: 8, MaxTotalBytes: 1 << 20,
		Signers: map[string]ed25519.PublicKey{"models": publicKey},
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeBackend{}
	controller, err := New(manager, Config{
		OperationTimeout: time.Second, CleanupTimeout: time.Second,
		Slots: []Slot{{
			Policy: SlotPolicy{
				ID: "primary", Model: "approved-model", Runtime: "fake",
				MaxModelBytes: 1 << 20,
			},
			Backend: backend,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return activationFixture{
		manager: manager, controller: controller, backend: backend,
		privateKey: privateKey, now: now, root: root,
	}
}

func (f activationFixture) importModel(t *testing.T, name string, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: name, Digest: digest,
		SizeBytes: uint64(len(data)), Target: "linux/amd64/cuda",
		SecurityRevision: 7, IssuedAt: f.now.Add(-time.Minute).UnixMilli(),
		ExpiresAt: f.now.Add(time.Hour).UnixMilli(), KeyID: "models",
	}, f.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Import(
		context.Background(), manifest, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	return digest
}

func matchingBinding(request LoadRequest) Binding {
	return Binding{
		SlotID: request.SlotID, Model: request.Model, ModelDigest: request.ModelDigest,
		Runtime: "fake", RuntimeRevision: "fake-v1",
		Handle:         "candidate-" + request.ModelDigest[len("sha256:"):len("sha256:")+12],
		DigestEvidence: airuntime.BindingLocallyObserved,
	}
}

func TestActivationReplacementAndDeactivationLifecycle(t *testing.T) {
	f := newActivationFixture(t)
	first := f.importModel(t, "first.gguf", []byte("first-model"))
	second := f.importModel(t, "second.gguf", []byte("second-model"))

	status, err := f.controller.Activate(context.Background(), "primary", first)
	if err != nil || status.State != StateActive || status.ModelDigest != first ||
		status.RuntimeRevision != "fake-v1" ||
		status.DigestEvidence != airuntime.BindingLocallyObserved {
		t.Fatalf("first activation status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(first); model.State != modelmanager.StateActive ||
		model.InUse != 1 {
		t.Fatalf("first model = %#v", model)
	}
	if _, err := f.controller.Activate(context.Background(), "primary", first); err != nil {
		t.Fatal(err)
	}
	if loads, _, _ := f.backend.counts(); loads != 1 {
		t.Fatalf("idempotent activation loaded %d times", loads)
	}

	status, err = f.controller.Activate(context.Background(), "primary", second)
	if err != nil || status.State != StateActive || status.ModelDigest != second {
		t.Fatalf("replacement status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(first); model.State != modelmanager.StateReady ||
		model.InUse != 0 {
		t.Fatalf("replaced model = %#v", model)
	}
	if model := f.manager.Status(second); model.State != modelmanager.StateActive ||
		model.InUse != 1 {
		t.Fatalf("replacement model = %#v", model)
	}
	if loads, health, unloads := f.backend.counts(); loads != 2 ||
		health != 2 || unloads != 1 {
		t.Fatalf("backend counts load=%d health=%d unload=%d", loads, health, unloads)
	}

	status, err = f.controller.Deactivate(context.Background(), "primary")
	if err != nil || status.State != StateInactive || status.ModelDigest != "" {
		t.Fatalf("deactivation status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(second); model.State != modelmanager.StateReady ||
		model.InUse != 0 {
		t.Fatalf("deactivated model = %#v", model)
	}
	if status, err := f.controller.Deactivate(
		context.Background(), "primary",
	); err != nil || status.State != StateInactive {
		t.Fatalf("idempotent deactivate status=%#v err=%v", status, err)
	}
}

func TestLoadBindingAndHealthFailuresReleaseArtifact(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(*fakeBackend)
		kind          ErrorKind
		expectUnloads int
	}{
		{
			name: "load",
			configure: func(backend *fakeBackend) {
				backend.loadFn = func(context.Context, LoadRequest) (Binding, error) {
					return Binding{}, errors.New("endpoint and credential")
				}
			},
			kind: ErrorBackend,
		},
		{
			name: "binding",
			configure: func(backend *fakeBackend) {
				backend.loadFn = func(_ context.Context, request LoadRequest) (Binding, error) {
					binding := matchingBinding(request)
					binding.ModelDigest = "sha256:" + strings.Repeat("f", 64)
					return binding, nil
				}
			},
			kind: ErrorBinding, expectUnloads: 1,
		},
		{
			name: "health",
			configure: func(backend *fakeBackend) {
				backend.healthFn = func(context.Context, Binding) error {
					return errors.New("private health detail")
				}
			},
			kind: ErrorHealth, expectUnloads: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newActivationFixture(t)
			digest := f.importModel(t, "model.gguf", []byte("model"))
			test.configure(f.backend)
			status, err := f.controller.Activate(context.Background(), "primary", digest)
			if ErrorKindOf(err) != test.kind || status.State != StateFailed ||
				status.CleanupPending || strings.Contains(err.Error(), "credential") ||
				strings.Contains(err.Error(), "private") {
				t.Fatalf("failure status=%#v err=%v", status, err)
			}
			if model := f.manager.Status(digest); model.State != modelmanager.StateReady ||
				model.InUse != 0 {
				t.Fatalf("failed model lease = %#v", model)
			}
			if _, _, unloads := f.backend.counts(); unloads != test.expectUnloads {
				t.Fatalf("unloads=%d want=%d", unloads, test.expectUnloads)
			}
		})
	}
}

func TestFailedReplacementRollsBackKnownGood(t *testing.T) {
	f := newActivationFixture(t)
	first := f.importModel(t, "first.gguf", []byte("known-good"))
	second := f.importModel(t, "second.gguf", []byte("candidate"))
	if _, err := f.controller.Activate(context.Background(), "primary", first); err != nil {
		t.Fatal(err)
	}
	f.backend.unloadFn = func(_ context.Context, binding Binding) error {
		if binding.ModelDigest == first {
			return errors.New("known-good unload failed")
		}
		return nil
	}
	status, err := f.controller.Activate(context.Background(), "primary", second)
	if ErrorKindOf(err) != ErrorCleanup || status.State != StateActive ||
		status.ModelDigest != first || status.CleanupPending {
		t.Fatalf("rollback status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(first); model.State != modelmanager.StateActive ||
		model.InUse != 1 {
		t.Fatalf("known-good state = %#v", model)
	}
	if model := f.manager.Status(second); model.State != modelmanager.StateReady ||
		model.InUse != 0 {
		t.Fatalf("candidate state = %#v", model)
	}
}

func TestCleanupFailureStaysBoundedAndCanRetry(t *testing.T) {
	f := newActivationFixture(t)
	digest := f.importModel(t, "candidate.gguf", []byte("candidate"))
	f.backend.healthFn = func(context.Context, Binding) error {
		return errors.New("health failed")
	}
	cleanupFails := true
	f.backend.unloadFn = func(context.Context, Binding) error {
		if cleanupFails {
			return errors.New("cleanup failed")
		}
		return nil
	}
	status, err := f.controller.Activate(context.Background(), "primary", digest)
	if ErrorKindOf(err) != ErrorCleanup || status.State != StateFailed ||
		!status.CleanupPending {
		t.Fatalf("cleanup failure status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.InUse != 1 {
		t.Fatalf("cleanup failure did not retain lease: %#v", model)
	}
	if _, err := f.controller.Activate(
		context.Background(), "primary", digest,
	); ErrorKindOf(err) != ErrorCleanup {
		t.Fatalf("activation bypassed cleanup = %v", err)
	}
	cleanupFails = false
	status, err = f.controller.RetryCleanup(context.Background(), "primary")
	if err != nil || status.State != StateInactive || status.CleanupPending {
		t.Fatalf("cleanup retry status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.InUse != 0 ||
		model.State != modelmanager.StateReady {
		t.Fatalf("cleanup retry model = %#v", model)
	}
}

func TestLoadPartialBindingCleanupFailureRetainsLease(t *testing.T) {
	f := newActivationFixture(t)
	digest := f.importModel(t, "candidate.gguf", []byte("partial"))
	f.backend.loadFn = func(
		_ context.Context,
		request LoadRequest,
	) (Binding, error) {
		return matchingBinding(request), errors.New("load response failed")
	}
	cleanupFails := true
	f.backend.unloadFn = func(context.Context, Binding) error {
		if cleanupFails {
			return errors.New("cleanup failed")
		}
		return nil
	}
	status, err := f.controller.Activate(
		context.Background(), "primary", digest,
	)
	if ErrorKindOf(err) != ErrorCleanup || !status.CleanupPending ||
		f.manager.Status(digest).InUse != 1 {
		t.Fatalf("partial load status=%#v err=%v", status, err)
	}
	cleanupFails = false
	status, err = f.controller.RetryCleanup(context.Background(), "primary")
	if err != nil || status.CleanupPending ||
		f.manager.Status(digest).InUse != 0 {
		t.Fatalf("partial cleanup status=%#v err=%v", status, err)
	}
}

func TestCancellationTimeoutAndPanicAreStable(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		f := newActivationFixture(t)
		f.controller.operationLimit = 20 * time.Millisecond
		digest := f.importModel(t, "timeout.gguf", []byte("timeout"))
		f.backend.loadFn = func(ctx context.Context, _ LoadRequest) (Binding, error) {
			<-ctx.Done()
			return Binding{}, ctx.Err()
		}
		if _, err := f.controller.Activate(
			context.Background(), "primary", digest,
		); ErrorKindOf(err) != ErrorTimeout {
			t.Fatalf("timeout error = %v", err)
		}
		if f.manager.Status(digest).InUse != 0 {
			t.Fatal("timeout leaked artifact lease")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		f := newActivationFixture(t)
		digest := f.importModel(t, "cancel.gguf", []byte("cancel"))
		f.backend.loadFn = func(ctx context.Context, _ LoadRequest) (Binding, error) {
			<-ctx.Done()
			return Binding{}, ctx.Err()
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := f.controller.Activate(
			ctx, "primary", digest,
		); ErrorKindOf(err) != ErrorCanceled {
			t.Fatalf("cancel error = %v", err)
		}
	})
	t.Run("cancel after health", func(t *testing.T) {
		f := newActivationFixture(t)
		digest := f.importModel(t, "cancel-health.gguf", []byte("cancel-health"))
		ctx, cancel := context.WithCancel(context.Background())
		f.backend.healthFn = func(context.Context, Binding) error {
			cancel()
			return nil
		}
		status, err := f.controller.Activate(ctx, "primary", digest)
		if ErrorKindOf(err) != ErrorCanceled || status.State != StateFailed {
			t.Fatalf("post-health cancel status=%#v err=%v", status, err)
		}
		if model := f.manager.Status(digest); model.State != modelmanager.StateReady ||
			model.InUse != 0 {
			t.Fatalf("post-health cancel model = %#v", model)
		}
		if _, _, unloads := f.backend.counts(); unloads != 1 {
			t.Fatalf("post-health cancel unloads = %d", unloads)
		}
	})
	t.Run("panic", func(t *testing.T) {
		f := newActivationFixture(t)
		digest := f.importModel(t, "panic.gguf", []byte("panic"))
		f.backend.loadFn = func(context.Context, LoadRequest) (Binding, error) {
			panic("secret runtime URL and path")
		}
		status, err := f.controller.Activate(context.Background(), "primary", digest)
		if ErrorKindOf(err) != ErrorInternal || !status.CleanupPending ||
			strings.Contains(err.Error(), "secret") {
			t.Fatalf("panic status=%#v err=%v", status, err)
		}
		if f.manager.Status(digest).InUse != 1 {
			t.Fatal("panic did not fail closed with a bounded retained lease")
		}
		// A panic provides no backend handle to unload. Shutdown still releases
		// the bounded artifact lease while reporting that cleanup was incomplete.
		if err := f.controller.Close(context.Background()); ErrorKindOf(err) != ErrorCleanup {
			t.Fatalf("panic close error = %v", err)
		}
		if f.manager.Status(digest).InUse != 0 {
			t.Fatal("shutdown leaked panic artifact lease")
		}
		if status, err := f.controller.Status("primary"); err != nil ||
			!status.CleanupPending {
			t.Fatalf("uncertain panic cleanup status=%#v err=%v", status, err)
		}
		if err := f.controller.Close(context.Background()); ErrorKindOf(err) != ErrorCleanup {
			t.Fatalf("uncertain cleanup was forgotten: %v", err)
		}
	})
}

func TestConcurrentActivationIsSingleSlotBounded(t *testing.T) {
	f := newActivationFixture(t)
	digest := f.importModel(t, "model.gguf", []byte("concurrent"))
	started := make(chan struct{})
	release := make(chan struct{})
	f.backend.loadFn = func(_ context.Context, request LoadRequest) (Binding, error) {
		close(started)
		<-release
		return matchingBinding(request), nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := f.controller.Activate(context.Background(), "primary", digest)
		result <- err
	}()
	<-started
	if status, err := f.controller.Activate(
		context.Background(), "primary", digest,
	); ErrorKindOf(err) != ErrorBusy || status.State != StateLoading {
		t.Fatalf("concurrent status=%#v err=%v", status, err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if loads, _, _ := f.backend.counts(); loads != 1 {
		t.Fatalf("concurrent activation load count = %d", loads)
	}
}

func TestCrossSlotDigestConflictAndSortedStatus(t *testing.T) {
	f := newActivationFixture(t)
	secondBackend := &fakeBackend{}
	controller, err := New(f.manager, Config{
		OperationTimeout: time.Second, CleanupTimeout: time.Second,
		Slots: []Slot{
			{Policy: SlotPolicy{
				ID: "z-slot", Model: "approved-model", Runtime: "fake",
				MaxModelBytes: 1 << 20,
			}, Backend: f.backend},
			{Policy: SlotPolicy{
				ID: "a-slot", Model: "approved-model", Runtime: "fake",
				MaxModelBytes: 1 << 20,
			}, Backend: secondBackend},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "model.gguf", []byte("shared"))
	if _, err := controller.Activate(context.Background(), "a-slot", digest); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(
		context.Background(), "z-slot", digest,
	); ErrorKindOf(err) != ErrorConflict {
		t.Fatalf("cross-slot conflict = %v", err)
	}
	statuses := controller.List()
	if len(statuses) != 2 || statuses[0].SlotID != "a-slot" ||
		statuses[1].SlotID != "z-slot" {
		t.Fatalf("sorted statuses = %#v", statuses)
	}
}

func TestCrossSlotDigestConflictIncludesLoadingCandidate(t *testing.T) {
	f := newActivationFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	firstBackend := &fakeBackend{
		loadFn: func(_ context.Context, request LoadRequest) (Binding, error) {
			close(started)
			<-release
			return matchingBinding(request), nil
		},
	}
	secondBackend := &fakeBackend{}
	controller, err := New(f.manager, Config{
		OperationTimeout: time.Second, CleanupTimeout: time.Second,
		Slots: []Slot{
			{Policy: SlotPolicy{
				ID: "first", Model: "approved-model", Runtime: "fake",
				MaxModelBytes: 1 << 20,
			}, Backend: firstBackend},
			{Policy: SlotPolicy{
				ID: "second", Model: "approved-model", Runtime: "fake",
				MaxModelBytes: 1 << 20,
			}, Backend: secondBackend},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := f.importModel(t, "model.gguf", []byte("pending"))
	result := make(chan error, 1)
	go func() {
		_, err := controller.Activate(context.Background(), "first", digest)
		result <- err
	}()
	<-started
	if _, err := controller.Activate(
		context.Background(), "second", digest,
	); ErrorKindOf(err) != ErrorConflict {
		t.Fatalf("loading digest conflict = %v", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if loads, _, _ := secondBackend.counts(); loads != 0 {
		t.Fatalf("conflicting slot loaded %d candidates", loads)
	}
}

func TestDeactivateFailureAndCloseReleaseActiveLease(t *testing.T) {
	f := newActivationFixture(t)
	digest := f.importModel(t, "model.gguf", []byte("active"))
	if _, err := f.controller.Activate(context.Background(), "primary", digest); err != nil {
		t.Fatal(err)
	}
	failUnload := true
	f.backend.unloadFn = func(context.Context, Binding) error {
		if failUnload {
			return errors.New("unload failed")
		}
		return nil
	}
	status, err := f.controller.Deactivate(context.Background(), "primary")
	if ErrorKindOf(err) != ErrorBackend || status.State != StateActive ||
		f.manager.Status(digest).State != modelmanager.StateActive {
		t.Fatalf("failed deactivate status=%#v model=%#v err=%v",
			status, f.manager.Status(digest), err)
	}
	failUnload = false
	if err := f.controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err = f.controller.Status("primary")
	if err != nil || status.State != StateClosed || status.ModelDigest != "" {
		t.Fatalf("closed status=%#v err=%v", status, err)
	}
	if model := f.manager.Status(digest); model.State != modelmanager.StateReady ||
		model.InUse != 0 {
		t.Fatalf("closed model = %#v", model)
	}
	if _, err := f.controller.Activate(
		context.Background(), "primary", digest,
	); ErrorKindOf(err) != ErrorClosed {
		t.Fatalf("closed activation = %v", err)
	}
}

func TestConfigurationAndBindingBoundsFailClosed(t *testing.T) {
	f := newActivationFixture(t)
	invalidConfigs := []Config{
		{},
		{OperationTimeout: -1, CleanupTimeout: time.Second, Slots: []Slot{{
			Policy:  SlotPolicy{ID: "slot", Model: "model", Runtime: "fake", MaxModelBytes: 1},
			Backend: f.backend,
		}}},
		{OperationTimeout: time.Second, CleanupTimeout: time.Second, Slots: []Slot{{
			Policy:  SlotPolicy{ID: "slot\n", Model: "model", Runtime: "fake", MaxModelBytes: 1},
			Backend: f.backend,
		}}},
		{OperationTimeout: time.Second, CleanupTimeout: time.Second, Slots: []Slot{
			{Policy: SlotPolicy{ID: "slot", Model: "model", Runtime: "fake", MaxModelBytes: 1}, Backend: f.backend},
			{Policy: SlotPolicy{ID: "slot", Model: "model", Runtime: "fake", MaxModelBytes: 1}, Backend: f.backend},
		}},
		{OperationTimeout: time.Second, CleanupTimeout: time.Second, Slots: []Slot{{
			Policy:  SlotPolicy{ID: "typed-nil", Model: "model", Runtime: "fake", MaxModelBytes: 1},
			Backend: (*fakeBackend)(nil),
		}}},
	}
	for _, config := range invalidConfigs {
		if _, err := New(f.manager, config); ErrorKindOf(err) != ErrorInvalid {
			t.Fatalf("invalid config accepted: %#v err=%v", config, err)
		}
	}
	digest := f.importModel(t, "model.gguf", []byte("binding"))
	f.backend.loadFn = func(_ context.Context, request LoadRequest) (Binding, error) {
		binding := matchingBinding(request)
		binding.Handle = strings.Repeat("x", MaxHandleBytes+1)
		return binding, nil
	}
	if _, err := f.controller.Activate(
		context.Background(), "primary", digest,
	); ErrorKindOf(err) != ErrorBinding {
		t.Fatalf("oversized binding handle = %v", err)
	}
}

func TestActivationRehashesArtifactBeforeBackendLoad(t *testing.T) {
	f := newActivationFixture(t)
	digest := f.importModel(t, "model.gguf", []byte("original"))
	path := filepath.Join(
		f.root, strings.TrimPrefix(digest, "sha256:")+".model",
	)
	if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	status, err := f.controller.Activate(context.Background(), "primary", digest)
	if ErrorKindOf(err) != ErrorArtifact || status.State != StateFailed {
		t.Fatalf("tampered activation status=%#v err=%v", status, err)
	}
	if loads, _, _ := f.backend.counts(); loads != 0 {
		t.Fatalf("tampered artifact reached backend %d times", loads)
	}
	if f.manager.Status(digest).InUse != 0 {
		t.Fatal("tampered activation leaked artifact lease")
	}
}
