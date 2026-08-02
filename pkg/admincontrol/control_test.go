package admincontrol

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
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/softwareupdate"
	"github.com/tosnetwork/tos-ai/pkg/update"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
)

type fakeLifecycle struct {
	mu       sync.Mutex
	status   softwareupdate.Status
	calls    int
	fail     bool
	panicNow bool
}

func (f *fakeLifecycle) Status() (softwareupdate.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status, nil
}

func (f *fakeLifecycle) ActivatePending() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.panicNow {
		panic("synthetic lifecycle panic")
	}
	if f.fail {
		return errors.New("synthetic lifecycle failure")
	}
	f.status.ActiveSlot = "a"
	f.status.AwaitingHealth = true
	return nil
}

func (f *fakeLifecycle) ConfirmHealthy() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail {
		return errors.New("synthetic lifecycle failure")
	}
	f.status.KnownGoodSlot = f.status.ActiveSlot
	f.status.AwaitingHealth = false
	return nil
}

func (f *fakeLifecycle) Rollback() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail {
		return errors.New("synthetic lifecycle failure")
	}
	f.status.ActiveSlot = f.status.KnownGoodSlot
	f.status.AwaitingHealth = false
	return nil
}

func (f *fakeLifecycle) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type controllerFixture struct {
	public  ed25519.PublicKey
	private ed25519.PrivateKey
	now     time.Time
}

func newControllerFixture(t *testing.T) controllerFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return controllerFixture{
		public: publicKey, private: privateKey,
		now: time.Unix(1_800_000_000, 0).UTC(),
	}
}

func (f controllerFixture) config(root string, lifecycle Lifecycle) Config {
	return Config{
		DatabasePath: filepath.Join(root, "admin", "commands.db"),
		TerminalID:   "terminal-1",
		AdministratorKeys: map[string]ed25519.PublicKey{
			"admin-1": f.public,
		},
		Lifecycle: lifecycle,
	}
}

func TestOpenRejectsTypedNilMOCKLifecycle(t *testing.T) {
	fixture := newControllerFixture(t)
	var lifecycle *fakeLifecycle
	controller, err := Open(fixture.config(t.TempDir(), lifecycle))
	if err == nil || controller != nil {
		t.Fatal("typed-nil administrator lifecycle accepted")
	}
}

func (f controllerFixture) sign(
	t *testing.T,
	command Command,
	issuedAt time.Time,
	expiresAt time.Time,
) identity.Envelope {
	t.Helper()
	envelope, err := identity.SignCanonical(
		f.private, CommandDomain, "admin-1", command, issuedAt, expiresAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func command(id, action, expected string) Command {
	return Command{
		Version: CommandVersion, CommandID: id, TerminalID: "terminal-1",
		Action: action, ExpectedActiveSlot: expected,
	}
}

func TestSignedCommandExactReplayConflictAndRestart(t *testing.T) {
	fixture := newControllerFixture(t)
	lifecycle := &fakeLifecycle{}
	config := fixture.config(t.TempDir(), lifecycle)
	controller, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	envelope := fixture.sign(
		t, command("command-1", ActionActivate, ""),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	result, err := controller.Execute(context.Background(), envelope, fixture.now)
	if err != nil || !result.Succeeded || result.Replay || result.ActiveSlot != "a" {
		t.Fatalf("first result=%#v err=%v", result, err)
	}
	replay, err := controller.Execute(context.Background(), envelope, fixture.now)
	if err != nil || !replay.Succeeded || !replay.Replay || lifecycle.callCount() != 1 {
		t.Fatalf("replay=%#v calls=%d err=%v", replay, lifecycle.callCount(), err)
	}

	conflict := fixture.sign(
		t, command("command-1", ActionConfirm, "a"),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	if _, err := controller.Execute(context.Background(), conflict, fixture.now); err == nil {
		t.Fatal("conflicting command identifier was accepted")
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	controller, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	replay, err = controller.Execute(context.Background(), envelope, fixture.now)
	if err != nil || !replay.Replay || lifecycle.callCount() != 1 {
		t.Fatalf("restart replay=%#v calls=%d err=%v", replay, lifecycle.callCount(), err)
	}
}

func TestAuthenticationBindingPreconditionAndCancellation(t *testing.T) {
	fixture := newControllerFixture(t)
	lifecycle := &fakeLifecycle{status: softwareupdate.Status{ActiveSlot: "a"}}
	config := fixture.config(t.TempDir(), lifecycle)
	controller, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()

	tests := []struct {
		name     string
		envelope identity.Envelope
		now      time.Time
	}{
		{
			name: "wrong terminal",
			envelope: fixture.sign(t, Command{
				Version: CommandVersion, CommandID: "wrong-terminal",
				TerminalID: "terminal-2", Action: ActionConfirm,
			}, fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour)),
			now: fixture.now,
		},
		{
			name: "expired",
			envelope: fixture.sign(
				t, command("expired", ActionConfirm, "a"),
				fixture.now.Add(-time.Hour), fixture.now.Add(-time.Minute),
			),
			now: fixture.now,
		},
		{
			name: "wrong precondition",
			envelope: fixture.sign(
				t, command("wrong-slot", ActionConfirm, "b"),
				fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
			),
			now: fixture.now,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := controller.Execute(context.Background(), test.envelope, test.now); err == nil {
				t.Fatal("invalid command was accepted")
			}
		})
	}

	tampered := fixture.sign(
		t, command("tampered", ActionConfirm, "a"),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	if tampered.Signature[0] == 'A' {
		tampered.Signature = "B" + tampered.Signature[1:]
	} else {
		tampered.Signature = "A" + tampered.Signature[1:]
	}
	if _, err := controller.Execute(context.Background(), tampered, fixture.now); err == nil {
		t.Fatal("tampered command was accepted")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	valid := fixture.sign(
		t, command("canceled", ActionConfirm, "a"),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	if _, err := controller.Execute(canceled, valid, fixture.now); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error=%v", err)
	}
	if lifecycle.callCount() != 0 {
		// Wrong-slot reaches the lifecycle precondition path but does not
		// execute a lifecycle action.
		t.Fatalf("lifecycle action calls=%d", lifecycle.callCount())
	}
}

func TestFailedPanicAndUncertainOutcomesNeverReexecute(t *testing.T) {
	fixture := newControllerFixture(t)
	for _, test := range []struct {
		name      string
		lifecycle *fakeLifecycle
	}{
		{name: "failure", lifecycle: &fakeLifecycle{fail: true}},
		{name: "panic", lifecycle: &fakeLifecycle{panicNow: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller, err := Open(fixture.config(t.TempDir(), test.lifecycle))
			if err != nil {
				t.Fatal(err)
			}
			defer controller.Close()
			envelope := fixture.sign(
				t, command("failed-command", ActionActivate, ""),
				fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
			)
			result, err := controller.Execute(context.Background(), envelope, fixture.now)
			if err == nil || result.Succeeded || result.Replay {
				t.Fatalf("first result=%#v err=%v", result, err)
			}
			result, err = controller.Execute(context.Background(), envelope, fixture.now)
			if err == nil || result.Succeeded || !result.Replay || test.lifecycle.callCount() != 1 {
				t.Fatalf("replay result=%#v calls=%d err=%v", result, test.lifecycle.callCount(), err)
			}
		})
	}

	lifecycle := &fakeLifecycle{}
	config := fixture.config(t.TempDir(), lifecycle)
	controller, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	envelope := fixture.sign(
		t, command("uncertain-command", ActionActivate, ""),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	fingerprint, err := envelope.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := controller.claim(
		command("uncertain-command", ActionActivate, ""), fingerprint,
		envelope.ExpiresAt, fixture.now,
	); err != nil || replay != nil {
		t.Fatalf("manual claim replay=%#v err=%v", replay, err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	controller, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Execute(context.Background(), envelope, fixture.now); !errors.Is(err, ErrUncertain) {
		t.Fatalf("uncertain replay error=%v", err)
	}
	if lifecycle.callCount() != 0 {
		t.Fatalf("uncertain command executed %d times", lifecycle.callCount())
	}
}

func TestConcurrentReplayRetentionBoundsOwnershipAndPermissions(t *testing.T) {
	fixture := newControllerFixture(t)
	lifecycle := &fakeLifecycle{}
	root := t.TempDir()
	config := fixture.config(root, lifecycle)
	config.MaxRecords = 1
	config.Retention = time.Millisecond
	controller, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(config); err == nil {
		t.Fatal("second journal owner was accepted")
	}
	envelope := fixture.sign(
		t, command("a-command", ActionActivate, ""),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, executeErr := controller.Execute(context.Background(), envelope, fixture.now)
			errorsSeen <- executeErr
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for executeErr := range errorsSeen {
		if executeErr != nil {
			t.Fatalf("concurrent exact retry error=%v", executeErr)
		}
	}
	if lifecycle.callCount() != 1 {
		t.Fatalf("concurrent lifecycle calls=%d", lifecycle.callCount())
	}
	history, err := controller.History(1)
	if err != nil || len(history) != 1 || history[0].CommandID != "a-command" ||
		history[0].Action != ActionActivate || history[0].State != "completed" ||
		!history[0].Succeeded || history[0].Sequence == 0 {
		t.Fatalf("history=%#v err=%v", history, err)
	}
	if _, err := controller.History(MaxHistoryEntries + 1); err == nil {
		t.Fatal("unbounded history request was accepted")
	}

	parentInfo, err := os.Stat(filepath.Dir(config.DatabasePath))
	if err != nil || parentInfo.Mode().Perm() != 0o700 {
		t.Fatalf("parent mode=%v err=%v", parentInfo.Mode().Perm(), err)
	}
	databaseInfo, err := os.Stat(config.DatabasePath)
	if err != nil || databaseInfo.Mode().Perm() != 0o600 {
		t.Fatalf("database mode=%v err=%v", databaseInfo.Mode().Perm(), err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	// A command remains replay-protected through envelope expiry plus the
	// configured retention. Once that bounded interval passes, capacity can
	// be reclaimed by a fresh valid command.
	lifecycle.status = softwareupdate.Status{ActiveSlot: "a"}
	controller, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	secondNow := fixture.now.Add(time.Hour + 2*time.Millisecond)
	second := fixture.sign(
		t, command("z-command", ActionConfirm, "a"),
		secondNow.Add(-time.Millisecond), secondNow.Add(time.Hour),
	)
	if _, err := controller.Execute(context.Background(), second, secondNow); err != nil {
		t.Fatalf("bounded retention did not reclaim capacity: %v", err)
	}
	history, err = controller.History(2)
	if err != nil || len(history) != 1 || history[0].CommandID != "z-command" ||
		history[0].Sequence <= 1 {
		t.Fatalf("pruned history=%#v err=%v", history, err)
	}
}

func TestOpenRejectsUnsafeConfigurationAndTinyByteLimit(t *testing.T) {
	fixture := newControllerFixture(t)
	lifecycle := &fakeLifecycle{}
	config := fixture.config(t.TempDir(), lifecycle)
	config.DatabasePath = "relative.db"
	if _, err := Open(config); err == nil {
		t.Fatal("relative database path was accepted")
	}
	config = fixture.config(t.TempDir(), lifecycle)
	config.MaxDatabaseBytes = 1
	if _, err := Open(config); err == nil {
		t.Fatal("impossible database byte limit was accepted")
	}
}

func TestExpiryIndexReclaimsLaterShorterEnvelopeWithoutFullScan(t *testing.T) {
	fixture := newControllerFixture(t)
	lifecycle := &fakeLifecycle{}
	config := fixture.config(t.TempDir(), lifecycle)
	config.MaxRecords = 2
	config.Retention = time.Millisecond
	controller, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	long := fixture.sign(
		t, command("a-long", ActionConfirm, ""),
		fixture.now.Add(-time.Minute), fixture.now.Add(2*time.Hour),
	)
	short := fixture.sign(
		t, command("b-short", ActionConfirm, ""),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Minute),
	)
	if _, err := controller.Execute(context.Background(), long, fixture.now); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Execute(context.Background(), short, fixture.now); err != nil {
		t.Fatal(err)
	}
	later := fixture.now.Add(time.Minute + 2*time.Millisecond)
	fresh := fixture.sign(
		t, command("c-fresh", ActionConfirm, ""),
		later.Add(-time.Millisecond), later.Add(time.Hour),
	)
	if _, err := controller.Execute(context.Background(), fresh, later); err != nil {
		t.Fatalf("shorter later expiry was not reclaimed: %v", err)
	}
	history, err := controller.History(3)
	if err != nil || len(history) != 2 || history[0].CommandID != "c-fresh" ||
		history[1].CommandID != "a-long" {
		t.Fatalf("history after expiry-index prune=%#v err=%v", history, err)
	}
}

func TestControllerDrivesRealTwoSlotCandidateLifecycle(t *testing.T) {
	fixture := newControllerFixture(t)
	releasePublic, releasePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	updateConfig := softwareupdate.Config{
		Root: filepath.Join(root, "updates"), Target: "linux/amd64/tos-ai",
		PublicKeys: map[string]ed25519.PublicKey{"release-1": releasePublic},
	}
	manager, err := softwareupdate.Open(updateConfig)
	if err != nil {
		t.Fatal(err)
	}
	artifact := []byte("signed release candidate")
	digest := sha256.Sum256(artifact)
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "tos-ai-release.tar.gz",
		Digest: "sha256:" + hex.EncodeToString(digest[:]), SizeBytes: uint64(len(artifact)),
		Target: updateConfig.Target, SecurityRevision: 1,
		IssuedAt: fixture.now.UnixMilli(), ExpiresAt: fixture.now.Add(time.Hour).UnixMilli(),
		KeyID: "release-1",
	}, releasePrivate)
	if err != nil {
		t.Fatal(err)
	}
	if slot, err := manager.Stage(
		context.Background(), manifest, bytes.NewReader(artifact), fixture.now,
	); err != nil || slot != "a" {
		t.Fatalf("stage slot=%q err=%v", slot, err)
	}

	adminConfig := fixture.config(root, manager)
	controller, err := Open(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	activate := fixture.sign(
		t, command("activate-real", ActionActivate, ""),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	if result, err := controller.Execute(context.Background(), activate, fixture.now); err != nil ||
		!result.Succeeded || result.ActiveSlot != "a" {
		t.Fatalf("activate result=%#v err=%v", result, err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening is the candidate boot; only this instance may confirm health.
	manager, err = softwareupdate.Open(updateConfig)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.Lifecycle = manager
	controller, err = Open(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	defer manager.Close()
	confirm := fixture.sign(
		t, command("confirm-real", ActionConfirm, "a"),
		fixture.now.Add(-time.Minute), fixture.now.Add(time.Hour),
	)
	if result, err := controller.Execute(context.Background(), confirm, fixture.now); err != nil ||
		!result.Succeeded || result.ActiveSlot != "a" {
		t.Fatalf("confirm result=%#v err=%v", result, err)
	}
	status, err := manager.Status()
	if err != nil || status.KnownGoodSlot != "a" || status.AwaitingHealth ||
		status.BootAttempted || status.SecurityRevision != 1 {
		t.Fatalf("confirmed status=%#v err=%v", status, err)
	}
}
