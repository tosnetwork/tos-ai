package modelapproval

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/update"
)

type fakeAdapter struct {
	capability      airuntime.Capability
	preflightCalls  atomic.Int32
	executeCalls    atomic.Int32
	closeCalls      atomic.Int32
	closeErr        error
	panicCapability bool
	panicPreflight  bool
	panicExecute    bool
	panicClose      bool
	preflightStart  chan struct{}
	preflightDone   chan struct{}
}

func (f *fakeAdapter) Capability() airuntime.Capability {
	if f.panicCapability {
		panic("runtime detail")
	}
	return f.capability
}

func (f *fakeAdapter) Preflight(
	context.Context,
) (airuntime.Preflight, error) {
	if f.panicPreflight {
		panic("runtime detail")
	}
	f.preflightCalls.Add(1)
	if f.preflightStart != nil {
		close(f.preflightStart)
		<-f.preflightDone
	}
	return airuntime.Preflight{
		Model: f.capability.Model, ModelDigest: f.capability.ModelDigest,
		DigestEvidence: airuntime.BindingLocallyObserved,
	}, nil
}

func (f *fakeAdapter) Execute(
	context.Context,
	airuntime.Request,
) (airuntime.Response, error) {
	if f.panicExecute {
		panic("runtime detail")
	}
	f.executeCalls.Add(1)
	return airuntime.Response{Output: []byte("ok")}, nil
}

func (f *fakeAdapter) Close() error {
	if f.panicClose {
		panic("runtime detail")
	}
	f.closeCalls.Add(1)
	return f.closeErr
}

type approvalFixture struct {
	manager *modelmanager.Manager
	root    string
	digest  string
}

func newApprovalFixture(t *testing.T, data []byte) approvalFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "models")
	manager, err := modelmanager.New(modelmanager.Config{
		RootDir: root, Target: "linux/amd64/cuda",
		CurrentSecurityRevision: 4, MaxModels: 4, MaxTotalBytes: 1 << 20,
		Signers: map[string]ed25519.PublicKey{"models": publicKey},
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "approved.gguf",
		Digest: digest, SizeBytes: uint64(len(data)),
		Target: "linux/amd64/cuda", SecurityRevision: 4,
		IssuedAt:  now.Add(-time.Minute).UnixMilli(),
		ExpiresAt: now.Add(time.Hour).UnixMilli(), KeyID: "models",
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Import(
		context.Background(), manifest, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	return approvalFixture{manager: manager, root: root, digest: digest}
}

func approvedFake(digest string) *fakeAdapter {
	return &fakeAdapter{capability: airuntime.Capability{
		ServiceID: "tos.ai.test", Operation: "generate", Model: "approved",
		ModelDigest: digest, Runtime: "fake", RuntimeRevision: "fake-v1",
		MaxInputBytes: 1024, MaxOutputBytes: 1024,
		AcceptedPriorities: []airuntime.Priority{airuntime.PriorityExternalService},
		Admission: admission.Resources{
			RAMBytes: 1, ContextTokens: 1, BatchSize: 1,
			ExecutionTime: time.Second,
		},
	}}
}

func TestNewRejectsTypedNilAndMOCKCapabilityPanic(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	var typedNil *fakeAdapter
	for name, inner := range map[string]airuntime.Adapter{
		"typed-nil": typedNil,
		"panic":     &fakeAdapter{panicCapability: true},
	} {
		t.Run(name, func(t *testing.T) {
			adapter, err := New(context.Background(), fixture.manager, inner, time.Second)
			if err == nil || adapter != nil {
				t.Fatal("unsafe approved runtime accepted")
			}
		})
	}
}

func TestApprovedAdapterContainsMOCKRuntimePanics(t *testing.T) {
	for _, operation := range []string{"preflight", "execute", "close"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newApprovalFixture(t, []byte("approved-model"))
			inner := approvedFake(fixture.digest)
			guarded, err := New(context.Background(), fixture.manager, inner, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "preflight":
				inner.panicPreflight = true
				_, err = guarded.Preflight(context.Background())
			case "execute":
				inner.panicExecute = true
				_, err = guarded.Execute(context.Background(), airuntime.Request{})
			case "close":
				inner.panicClose = true
				err = guarded.Close()
			}
			if err == nil {
				t.Fatal("runtime panic was accepted")
			}
			if operation != "close" {
				if closeErr := guarded.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if model := fixture.manager.Status(fixture.digest); model.InUse != 0 {
				t.Fatalf("panic leaked approved model lease: %#v", model)
			}
		})
	}
}

func TestApprovedAdapterVerifiesBeforePreflightAndReleasesLease(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	inner := approvedFake(fixture.digest)
	guarded, err := New(
		context.Background(), fixture.manager, inner, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if model := fixture.manager.Status(fixture.digest); model.InUse != 1 {
		t.Fatalf("approval lease=%#v", model)
	}
	if _, err := guarded.Preflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	if inner.preflightCalls.Load() != 1 {
		t.Fatalf("inner preflight calls=%d", inner.preflightCalls.Load())
	}
	path := filepath.Join(
		fixture.root, strings.TrimPrefix(fixture.digest, "sha256:")+".model",
	)
	if err := os.WriteFile(path, []byte("tampered-model"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := guarded.Preflight(
		context.Background(),
	); airuntime.ErrorKindOf(err) != airuntime.ErrorUnavailable {
		t.Fatalf("tampered approval preflight=%v", err)
	}
	if inner.preflightCalls.Load() != 1 {
		t.Fatal("tampered approval reached runtime preflight")
	}
	if err := guarded.Close(); err != nil {
		t.Fatal(err)
	}
	if err := guarded.Close(); err != nil {
		t.Fatal(err)
	}
	if inner.closeCalls.Load() != 1 {
		t.Fatalf("inner close calls=%d", inner.closeCalls.Load())
	}
	if model := fixture.manager.Status(fixture.digest); model.InUse != 0 {
		t.Fatalf("approval lease after close=%#v", model)
	}
	if _, err := guarded.Execute(
		context.Background(), airuntime.Request{},
	); airuntime.ErrorKindOf(err) != airuntime.ErrorUnavailable {
		t.Fatalf("closed approval execution=%v", err)
	}
}

func TestApprovedAdapterWaiterCancellationIsBounded(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	inner := approvedFake(fixture.digest)
	guarded, err := New(
		context.Background(), fixture.manager, inner, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	<-guarded.gate
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := guarded.Preflight(ctx); airuntime.ErrorKindOf(err) !=
		airuntime.ErrorCanceled {
		t.Fatalf("canceled approval waiter=%v", err)
	}
	guarded.gate <- struct{}{}
	if inner.preflightCalls.Load() != 0 {
		t.Fatal("canceled approval waiter reached inner adapter")
	}
	timeoutContext, cancelTimeout := context.WithTimeout(
		context.Background(), time.Nanosecond,
	)
	defer cancelTimeout()
	<-timeoutContext.Done()
	if _, err := guarded.Preflight(timeoutContext); airuntime.ErrorKindOf(err) !=
		airuntime.ErrorTimeout {
		t.Fatalf("timed out approval waiter=%v", err)
	}
	if err := guarded.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApprovedAdapterConcurrentPreflightsKeepOneLease(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	inner := approvedFake(fixture.digest)
	guarded, err := New(
		context.Background(), fixture.manager, inner, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	const callers = 32
	start := make(chan struct{})
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := guarded.Preflight(context.Background())
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if inner.preflightCalls.Load() != callers {
		t.Fatalf("concurrent inner preflight calls=%d",
			inner.preflightCalls.Load())
	}
	if model := fixture.manager.Status(fixture.digest); model.InUse != 1 {
		t.Fatalf("concurrent approval leases=%#v", model)
	}
	if err := guarded.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWrapAllFailureClosesAdaptersAndLeases(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	first := approvedFake(fixture.digest)
	second := approvedFake("sha256:" + strings.Repeat("f", 64))
	if _, err := WrapAll(
		context.Background(), fixture.manager,
		[]airuntime.Adapter{first, second}, time.Second,
	); err == nil {
		t.Fatal("missing approved artifact was accepted")
	}
	if first.closeCalls.Load() != 1 || second.closeCalls.Load() != 1 {
		t.Fatalf("close calls first=%d second=%d",
			first.closeCalls.Load(), second.closeCalls.Load())
	}
	if model := fixture.manager.Status(fixture.digest); model.InUse != 0 {
		t.Fatalf("failed wrap leaked approval lease=%#v", model)
	}
}

func TestApprovedAdapterCloseErrorIsStable(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	inner := approvedFake(fixture.digest)
	inner.closeErr = errors.New("credential /private/path")
	guarded, err := New(
		context.Background(), fixture.manager, inner, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = guarded.Close()
	if err == nil || strings.Contains(err.Error(), "credential") ||
		strings.Contains(err.Error(), "/private") {
		t.Fatalf("unstable close error=%v", err)
	}
	if fixture.manager.Status(fixture.digest).InUse != 0 {
		t.Fatal("close error leaked approval lease")
	}
}

func TestApprovedAdapterCloseWaitsForPreflightAndReleases(t *testing.T) {
	fixture := newApprovalFixture(t, []byte("approved-model"))
	inner := approvedFake(fixture.digest)
	inner.preflightStart = make(chan struct{})
	inner.preflightDone = make(chan struct{})
	guarded, err := New(
		context.Background(), fixture.manager, inner, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	preflightResult := make(chan error, 1)
	go func() {
		_, err := guarded.Preflight(context.Background())
		preflightResult <- err
	}()
	<-inner.preflightStart
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- guarded.Close()
	}()
	deadline := time.Now().Add(time.Second)
	for !guarded.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !guarded.closed.Load() {
		t.Fatal("close did not begin")
	}
	select {
	case err := <-closeResult:
		t.Fatalf("close returned during preflight: %v", err)
	default:
	}
	if inner.closeCalls.Load() != 0 {
		t.Fatal("inner adapter closed during preflight")
	}
	if fixture.manager.Status(fixture.digest).InUse != 1 {
		t.Fatal("approval lease released during preflight")
	}
	close(inner.preflightDone)
	if err := <-preflightResult; err != nil {
		t.Fatal(err)
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	if inner.closeCalls.Load() != 1 ||
		fixture.manager.Status(fixture.digest).InUse != 0 {
		t.Fatal("close did not release runtime and approval lease")
	}
}

func TestApprovedAdapterZeroValueFailsWithoutBlocking(t *testing.T) {
	var guarded Adapter
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := guarded.Preflight(ctx); airuntime.ErrorKindOf(err) !=
		airuntime.ErrorUnavailable {
		t.Fatalf("zero preflight=%v", err)
	}
	if _, err := guarded.Execute(
		ctx, airuntime.Request{},
	); airuntime.ErrorKindOf(err) != airuntime.ErrorUnavailable {
		t.Fatalf("zero execute=%v", err)
	}
	if err := guarded.Close(); err == nil {
		t.Fatal("zero close succeeded")
	}
}
