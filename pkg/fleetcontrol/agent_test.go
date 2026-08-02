package fleetcontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/identity"
	bolt "go.etcd.io/bbolt"
)

type mockExecutor struct {
	mu          sync.Mutex
	actions     []string
	failAction  string
	panicAction string
}

func (m *mockExecutor) Apply(_ context.Context, command Command) error {
	if command.Action == m.panicAction {
		panic("injected executor panic")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.actions = append(m.actions, command.Action)
	if command.Action == m.failAction {
		return errors.New("injected failure")
	}
	return nil
}

type cancelingFleetExecutor struct {
	cancel context.CancelFunc
}

func (executor cancelingFleetExecutor) Apply(context.Context, Command) error {
	executor.cancel()
	return nil
}

func fleetKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return publicKey, privateKey
}

func signedFleetCommand(t *testing.T, key ed25519.PrivateKey, terminal, id, action string, generation uint64, now time.Time) identity.Envelope {
	t.Helper()
	envelope, err := identity.SignCanonical(key, CommandDomain, "controller-1", Command{
		Version: CommandVersion, CommandID: id, FleetID: "fleet-one", TerminalID: terminal,
		Generation: generation, Action: action, ReleaseDigest: "sha256:" + strings.Repeat("a", 64),
	}, now.Add(-time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func openTestAgent(t *testing.T, terminal string, publicKey ed25519.PublicKey, executor *mockExecutor, online, busy *bool) *Agent {
	t.Helper()
	agent, err := Open(Config{
		DatabasePath: filepath.Join(t.TempDir(), "private", "fleet.db"), FleetID: "fleet-one", TerminalID: terminal,
		ControllerKeys: map[string]ed25519.PublicKey{"controller-1": publicKey}, Executor: executor,
		Online: func() bool { return *online }, RealtimeBusy: func() bool { return *busy }, MaxQueued: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agent.Close() })
	return agent
}

func TestOpenRejectsTypedNilMOCKExecutor(t *testing.T) {
	publicKey, _ := fleetKey(t)
	var executor *mockExecutor
	agent, err := Open(Config{
		DatabasePath: filepath.Join(t.TempDir(), "private", "fleet.db"),
		FleetID:      "fleet-one", TerminalID: "terminal-one",
		ControllerKeys: map[string]ed25519.PublicKey{"controller-1": publicKey},
		Executor:       executor, Online: func() bool { return true },
		RealtimeBusy: func() bool { return false }, MaxQueued: 2,
	})
	if err == nil || agent != nil {
		t.Fatal("typed-nil fleet executor accepted")
	}
}

func TestAgentOfflineQueueRealtimePriorityReplayAndBounds(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	online, busy := false, false
	executor := &mockExecutor{}
	agent := openTestAgent(t, "terminal-one", publicKey, executor, &online, &busy)
	now := time.Unix(1_800_000_000, 0)
	first := signedFleetCommand(t, privateKey, "terminal-one", "command-one", "install-release", 1, now)
	result, err := agent.Submit(context.Background(), first, now)
	if err != nil || result.State != "queued" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	replay, err := agent.Submit(context.Background(), first, now)
	if err != nil || !replay.Replay || replay.State != "queued" {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	_, _ = agent.Submit(context.Background(), signedFleetCommand(t, privateKey, "terminal-one", "command-two", "drain", 2, now), now)
	if _, err := agent.Submit(context.Background(), signedFleetCommand(t, privateKey, "terminal-one", "command-three", "resume", 3, now), now); err == nil {
		t.Fatal("bounded offline queue accepted overflow")
	}
	online, busy = true, true
	if results, err := agent.Drain(context.Background(), now, 2); !errors.Is(err, ErrRealtimeBusy) || len(results) != 0 {
		t.Fatalf("real-time gate results=%#v err=%v", results, err)
	}
	busy = false
	results, err := agent.Drain(context.Background(), now, 2)
	if err != nil || len(results) != 2 || len(executor.actions) != 2 {
		t.Fatalf("drain=%#v err=%v actions=%#v", results, err, executor.actions)
	}
}

func TestAgentRejectsWrongTerminalAndStaleGeneration(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	online, busy := true, false
	agent := openTestAgent(t, "terminal-one", publicKey, &mockExecutor{}, &online, &busy)
	now := time.Unix(1_800_000_000, 0)
	if _, err := agent.Submit(context.Background(), signedFleetCommand(t, privateKey, "terminal-two", "wrong-terminal", "drain", 1, now), now); err == nil {
		t.Fatal("wrong terminal accepted")
	}
	if _, err := agent.Submit(context.Background(), signedFleetCommand(t, privateKey, "terminal-one", "new-generation", "drain", 2, now), now); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Submit(context.Background(), signedFleetCommand(t, privateKey, "terminal-one", "stale-generation", "resume", 1, now), now); err == nil {
		t.Fatal("stale generation accepted")
	}
}

func TestAgentAppliesSignedPolicyCommandsAndRejectsDigestConfusion(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	online, busy := true, false
	executor := &mockExecutor{}
	agent := openTestAgent(t, "terminal-one", publicKey, executor, &online, &busy)
	now := time.Unix(1_800_000_000, 0)
	command := Command{
		Version: CommandVersion, CommandID: "policy-one", FleetID: "fleet-one", TerminalID: "terminal-one",
		Generation: 1, Action: "apply-policy", PolicyDigest: "sha256:" + strings.Repeat("b", 64),
	}
	envelope, err := identity.SignCanonical(privateKey, CommandDomain, "controller-1", command, now.Add(-time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := agent.Submit(context.Background(), envelope, now); err != nil || result.State != "succeeded" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	command.CommandID = "policy-confused"
	command.Generation = 2
	command.ReleaseDigest = "sha256:" + strings.Repeat("a", 64)
	envelope, _ = identity.SignCanonical(privateKey, CommandDomain, "controller-1", command, now.Add(-time.Second), now.Add(time.Hour))
	if _, err := agent.Submit(context.Background(), envelope, now); err == nil {
		t.Fatal("command carrying both release and policy digests accepted")
	}
}

func TestOpenRejectsSymlinkJournal(t *testing.T) {
	publicKey, _ := fleetKey(t)
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(directory, "target.db")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "fleet.db")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	online, busy := true, false
	if _, err := Open(Config{
		DatabasePath: path, FleetID: "fleet-one", TerminalID: "terminal-one",
		ControllerKeys: map[string]ed25519.PublicKey{"controller-1": publicKey}, Executor: &mockExecutor{},
		Online: func() bool { return online }, RealtimeBusy: func() bool { return busy },
	}); err == nil {
		t.Fatal("symlink fleet journal accepted")
	}
}

func TestAgentRestartPreservesQueueAndPreventsOvertake(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	now := time.Unix(1_800_000_000, 0)
	online, busy := false, false
	executor := &mockExecutor{}
	directory := filepath.Join(t.TempDir(), "private")
	path := filepath.Join(directory, "fleet.db")
	config := Config{
		DatabasePath: path, FleetID: "fleet-one", TerminalID: "terminal-one",
		ControllerKeys: map[string]ed25519.PublicKey{"controller-1": publicKey}, Executor: executor,
		Online: func() bool { return online }, RealtimeBusy: func() bool { return busy }, MaxQueued: 4,
	}
	agent, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	first := signedFleetCommand(t, privateKey, "terminal-one", "restart-command-one", "drain", 1, now)
	if result, err := agent.Submit(context.Background(), first, now); err != nil || result.State != "queued" {
		t.Fatalf("queue result=%#v err=%v", result, err)
	}
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	agent, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	online = true
	second := signedFleetCommand(t, privateKey, "terminal-one", "restart-command-two", "resume", 2, now)
	if result, err := agent.Submit(context.Background(), second, now); err != nil || result.State != "queued" {
		t.Fatalf("overtake result=%#v err=%v", result, err)
	}
	results, err := agent.Drain(context.Background(), now, 4)
	if err != nil || len(results) != 2 || len(executor.actions) != 2 || executor.actions[0] != "drain" || executor.actions[1] != "resume" {
		t.Fatalf("restart drain=%#v actions=%#v err=%v", results, executor.actions, err)
	}
}

func TestQueuedCommandCrashWindowRecoversUncertainWithoutReexecution(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	now := time.Unix(1_800_000_000, 0)
	online, busy := false, false
	executor := &mockExecutor{}
	path := filepath.Join(t.TempDir(), "private", "fleet.db")
	config := Config{
		DatabasePath: path, FleetID: "fleet-one", TerminalID: "terminal-one",
		ControllerKeys: map[string]ed25519.PublicKey{"controller-1": publicKey}, Executor: executor,
		Online: func() bool { return online }, RealtimeBusy: func() bool { return busy }, MaxQueued: 4,
	}
	agent, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	envelope := signedFleetCommand(t, privateKey, "terminal-one", "crash-window-command", "install-release", 1, now)
	if result, err := agent.Submit(context.Background(), envelope, now); err != nil || result.State != "queued" {
		t.Fatalf("queue result=%#v err=%v", result, err)
	}
	var queued record
	if err := agent.db.View(func(tx *bolt.Tx) error {
		_, value := tx.Bucket(queueBucket).Cursor().First()
		return json.Unmarshal(value, &queued)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.claimQueued(queued); err != nil {
		t.Fatal(err)
	}
	// Simulate a process loss after durable claim and before a result write.
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	agent, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	online = true
	replay, err := agent.Submit(context.Background(), envelope, now)
	if !errors.Is(err, ErrUncertain) || !replay.Replay || replay.State != "uncertain" {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	if drained, err := agent.Drain(context.Background(), now, 4); err != nil || len(drained) != 0 {
		t.Fatalf("drained=%#v err=%v", drained, err)
	}
	if len(executor.actions) != 0 {
		t.Fatalf("uncertain action re-executed: %v", executor.actions)
	}
}

func TestAgentContainsExecutorPanicAndCancellationLateSuccess(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	now := time.Unix(1_800_000_000, 0)
	for name, setup := range map[string]struct {
		executor func(context.CancelFunc) Executor
	}{
		"panic": {executor: func(context.CancelFunc) Executor {
			return &mockExecutor{panicAction: "drain"}
		}},
		"cancellation": {executor: func(cancel context.CancelFunc) Executor {
			return cancelingFleetExecutor{cancel: cancel}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			online, busy := true, false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			agent, err := Open(Config{
				DatabasePath: filepath.Join(t.TempDir(), "private", "fleet.db"), FleetID: "fleet-one", TerminalID: "terminal-one",
				ControllerKeys: map[string]ed25519.PublicKey{"controller-1": publicKey}, Executor: setup.executor(cancel),
				Online: func() bool { return online }, RealtimeBusy: func() bool { return busy },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer agent.Close()
			envelope := signedFleetCommand(t, privateKey, "terminal-one", name+"-uncertain", "drain", 1, now)
			result, err := agent.Submit(ctx, envelope, now)
			if !errors.Is(err, ErrUncertain) || result.State != "uncertain" {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			replay, err := agent.Submit(context.Background(), envelope, now)
			if !errors.Is(err, ErrUncertain) || !replay.Replay || replay.State != "uncertain" {
				t.Fatalf("replay=%#v err=%v", replay, err)
			}
		})
	}
}

func TestRolloutCanaryFailureRollsBackTouchedTerminals(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	now := time.Unix(1_800_000_000, 0)
	var targets []RolloutTarget
	for index, terminal := range []string{"terminal-a", "terminal-b", "terminal-c"} {
		online, busy := true, false
		executor := &mockExecutor{}
		if terminal == "terminal-b" {
			executor.failAction = "install-release"
		}
		agent := openTestAgent(t, terminal, publicKey, executor, &online, &busy)
		targets = append(targets, RolloutTarget{
			TerminalID: terminal, Agent: agent,
			Apply:    signedFleetCommand(t, privateKey, terminal, terminal+"-apply", "install-release", uint64(index*2+1), now),
			Rollback: signedFleetCommand(t, privateKey, terminal, terminal+"-rollback", "rollback", uint64(index*2+2), now),
		})
	}
	report, err := Rollout(context.Background(), targets, 2, now)
	if err == nil || report.Failed != "terminal-b" || len(report.Succeeded) != 1 || len(report.RolledBack) != 1 || report.RolledBack[0] != "terminal-a" {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}
