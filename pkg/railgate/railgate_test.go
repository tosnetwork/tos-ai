package railgate

import (
	"context"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
)

func openGate(t *testing.T, f *fixture) (*Gate, string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "slots")
	gate, err := OpenGate(f.verifier, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	return gate, directory
}

func TestVerifyProvesAPendingRequest(t *testing.T) {
	f := newFixture(t)
	verified, err := f.verifier.Verify(context.Background(), f.claim)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Actor != fxActor || verified.RequestID != 0 || verified.Price.Cmp(big.NewInt(300_000_000)) != 0 ||
		verified.ResponseDeadline != f.snapshot.ResponseDeadline || verified.ProviderAgentID != fxAgent ||
		verified.Transport.SecurityMode != nativecore.TransportAgentPacket ||
		!strings.HasPrefix(verified.QuoteCommitment, "tvm-cell-sha256:") || verified.BuyerWallet != fxBuyer {
		t.Fatalf("verified = %+v", verified)
	}
}

func TestVerifyRefusesEveryRequestThatIsNotPendingNow(t *testing.T) {
	refund := []toschain.ActorPayout{{Destination: fxBuyer, AmountNano: "300000000", EmptyBody: true}}
	for name, edit := range map[string]func(*testing.T, *fixture){
		"responded": func(t *testing.T, f *fixture) {
			f.transition(t, toschain.ActorTxRespond, uint32(fxNow.Unix())-10, fxOwner, true, nil, nil, nil)
		},
		"expired": func(t *testing.T, f *fixture) {
			f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)+10, 0)
			f.transition(t, toschain.ActorTxExpire, uint32(f.snapshot.ResponseDeadline)+1, fxOwner, false, nil,
				map[uint64]nativecore.ServiceActorRequestSnapshotV1{0: f.snapshot}, nil)
		},
		"refunded": func(t *testing.T, f *fixture) {
			f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)+20, 0)
			f.transition(t, toschain.ActorTxExpire, uint32(f.snapshot.ResponseDeadline)+1, fxOwner, false, nil,
				map[uint64]nativecore.ServiceActorRequestSnapshotV1{0: f.snapshot}, nil)
			f.transition(t, toschain.ActorTxClaim, uint32(f.snapshot.ResponseDeadline)+2, fxBuyer, false, nil, nil, refund)
		},
		"swept": func(t *testing.T, f *fixture) {
			f.chain.chainTime = time.Unix(int64(f.snapshot.RefundClaimDeadline)+20, 0)
			f.transition(t, toschain.ActorTxSweep, uint32(f.snapshot.RefundClaimDeadline)+1, fxOwner, false, nil, nil, refund)
		},
		"pending but past its response deadline": func(t *testing.T, f *fixture) {
			f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)+1, 0)
		},
		"pending without room to execute and respond": func(t *testing.T, f *fixture) {
			f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)-299, 0)
		},
		"getter differs from the decoded state": func(t *testing.T, f *fixture) { f.chain.getterLie = true },
		"history incomplete":                    func(t *testing.T, f *fixture) { f.chain.incomplete = true },
		"history does not reconcile": func(t *testing.T, f *fixture) {
			f.chain.data = fxData(t, f.policy, 1, nil, nil)
		},
		"call not finalized": func(t *testing.T, f *fixture) {
			f.chain.history, f.chain.data = nil, fxData(t, f.policy, 0, nil, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			edit(t, f)
			if _, err := f.verifier.Verify(context.Background(), f.claim); err == nil {
				t.Fatal("verified a request that does not admit execution now")
			}
		})
	}
}

func TestVerifyAdmitsTheLastSecondWithRoomToRespond(t *testing.T) {
	f := newFixture(t)
	f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)-301, 0)
	if _, err := f.verifier.Verify(context.Background(), f.claim); err != nil {
		t.Fatalf("one second beyond both budgets was refused: %v", err)
	}
	f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)-300, 0)
	if _, err := f.verifier.Verify(context.Background(), f.claim); err == nil {
		t.Fatal("a deadline exactly at both budgets was admitted")
	}
}

func TestVerifyRefusesAClaimTheChainDoesNotProve(t *testing.T) {
	for name, edit := range map[string]func(*fixture){
		"another execution id": func(f *fixture) { f.claim.ExecutionID = digest32(9) },
		"another input digest": func(f *fixture) { f.claim.InputDigest = digest32(9) },
		"another request id":   func(f *fixture) { f.claim.RequestID = 1 },
		"another call lt":      func(f *fixture) { f.claim.Call.LT++ },
		"another call hash":    func(f *fixture) { f.claim.Call.Hash = strings.Repeat("ab", 32) },
		"another buyer":        func(f *fixture) { f.claim.BuyerWallet = fxOwner },
		"another provider": func(f *fixture) {
			f.pkg.Proposal.ProviderAgentId = "agent_" + strings.Repeat("45", 32)
		},
		"another capability": func(f *fixture) { f.pkg.Proposal.CapabilityId = "cap_" + strings.Repeat("34", 32) },
		"zero execution id":  func(f *fixture) { f.claim.ExecutionID = [32]byte{} },
		"missing package":    func(f *fixture) { f.claim.Package = nil },
		"tampered manifest":  func(f *fixture) { f.pkg.CanonicalManifestCbor[3] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			edit(f)
			if _, err := f.verifier.Verify(context.Background(), f.claim); err == nil {
				t.Fatal("verified a claim the finalized state does not prove")
			}
		})
	}
}

func TestVerifyRefusesTwoCallsCarryingOneRequestHash(t *testing.T) {
	f := newFixture(t)
	second := f.call
	second.LT, second.Hash, second.Kind, second.Snapshot, second.ExitCode = 11, txHash(11), toschain.ActorTxFailed, nil, 1926
	f.chain.history = append([]toschain.ActorTransaction{second}, f.chain.history...)
	if _, err := f.verifier.Verify(context.Background(), f.claim); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("two calls with one request hash: %v", err)
	}
}

func TestNewVerifierRefusesUnboundedBudgets(t *testing.T) {
	f := newFixture(t)
	base := f.verifier.config
	for name, edit := range map[string]func(*VerifierConfig){
		"no execution budget":   func(c *VerifierConfig) { c.ExecutionBudget = 0 },
		"short respond budget":  func(c *VerifierConfig) { c.RespondBudget = MinRespondBudget - time.Second },
		"huge execution budget": func(c *VerifierConfig) { c.ExecutionBudget = MaxBudget + time.Second },
		"unpinned code hash":    func(c *VerifierConfig) { c.ActorCodeHash = "sha256:" + strings.Repeat("88", 32) },
		"no provider":           func(c *VerifierConfig) { c.ProviderAgentID = "" },
	} {
		config := base
		edit(&config)
		if _, err := NewVerifier(config); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	short := base
	short.ExecutionBudget = 179 * time.Second
	verifier, err := NewVerifier(short)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(context.Background(), f.claim); err == nil {
		t.Fatal("a manifest wall clock above the execution budget was admitted")
	}
}

func TestGateAdmitsARequestOnceAndRunsItAfterAFreshCheck(t *testing.T) {
	f := newFixture(t)
	gate, _ := openGate(t, f)
	ctx := context.Background()
	slot, verified, err := gate.Admit(ctx, f.claim)
	if err != nil || slot.State != SlotAdmitted || verified == nil {
		t.Fatalf("admit: %+v %v", slot, err)
	}
	if again, _, err := gate.Admit(ctx, f.claim); !errors.Is(err, ErrSlotTaken) || again.State != SlotAdmitted {
		t.Fatalf("second admit: %+v %v", again, err)
	}
	before := f.chain.sessions
	ran := 0
	err = gate.Run(ctx, slot, f.claim, func(_ context.Context, fresh *Verified) error {
		ran++
		if fresh.Checkpoint <= verified.Checkpoint {
			t.Error("the run reused the admission checkpoint")
		}
		return nil
	})
	if err != nil || ran != 1 || f.chain.sessions != before+1 {
		t.Fatalf("run: ran=%d sessions=%d->%d err=%v", ran, before, f.chain.sessions, err)
	}
	if current, _, _ := gate.Lookup(slot.Key()); current.State != SlotStarted {
		t.Fatalf("after run the slot is %s", current.State)
	}
	if err := gate.Run(ctx, slot, f.claim, func(context.Context, *Verified) error { ran++; return nil }); err == nil || ran != 1 {
		t.Fatal("a started slot ran again")
	}
	if err := gate.Complete(slot.Key(), strings.Repeat("cd", 32)); err != nil {
		t.Fatal(err)
	}
	if err := gate.Recheck(ctx, slot, f.claim, verified); err != nil {
		t.Fatalf("recheck of a still pending completed request: %v", err)
	}
	if err := gate.Fail(slot.Key()); err == nil {
		t.Fatal("a completed slot failed")
	}
	if again, _, err := gate.Admit(ctx, f.claim); !errors.Is(err, ErrSlotTaken) || again.State != SlotCompleted {
		t.Fatalf("admit after completion: %+v %v", again, err)
	}
}

func TestGateAbandonsARequestThatStoppedBeingPendingBeforeExecution(t *testing.T) {
	f := newFixture(t)
	gate, _ := openGate(t, f)
	ctx := context.Background()
	slot, _, err := gate.Admit(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)+5, 0)
	f.transition(t, toschain.ActorTxExpire, uint32(f.snapshot.ResponseDeadline)+1, fxOwner, false, nil,
		map[uint64]nativecore.ServiceActorRequestSnapshotV1{0: f.snapshot}, nil)
	ran := false
	err = gate.Run(ctx, slot, f.claim, func(context.Context, *Verified) error { ran = true; return nil })
	if !errors.Is(err, ErrNotPendingNow) || ran {
		t.Fatalf("run of an expired request: ran=%v err=%v", ran, err)
	}
	if current, _, _ := gate.Lookup(slot.Key()); current.State != SlotAbandoned {
		t.Fatalf("slot is %s", current.State)
	}
}

func TestGateRecheckRefusesARespondedOrUnfinishedRequest(t *testing.T) {
	f := newFixture(t)
	gate, _ := openGate(t, f)
	ctx := context.Background()
	slot, verified, err := gate.Admit(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Recheck(ctx, slot, f.claim, verified); err == nil {
		t.Fatal("an admitted, never executed request was rechecked for respond")
	}
	if err := gate.Run(ctx, slot, f.claim, func(context.Context, *Verified) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := gate.Complete(slot.Key(), strings.Repeat("cd", 32)); err != nil {
		t.Fatal(err)
	}
	f.transition(t, toschain.ActorTxRespond, uint32(fxNow.Unix())-10, fxOwner, true, nil, nil, nil)
	if err := gate.Recheck(ctx, slot, f.claim, verified); err == nil {
		t.Fatal("a responded request passed the recheck")
	}
	// With only the respond budget left the recheck still passes on a
	// pending request; with less it refuses.
	g := newFixture(t)
	gate2, _ := openGate(t, g)
	slot2, verified2, err := gate2.Admit(ctx, g.claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate2.Run(ctx, slot2, g.claim, func(context.Context, *Verified) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := gate2.Complete(slot2.Key(), strings.Repeat("cd", 32)); err != nil {
		t.Fatal(err)
	}
	g.chain.chainTime = time.Unix(int64(g.snapshot.ResponseDeadline)-121, 0)
	if err := gate2.Recheck(ctx, slot2, g.claim, verified2); err != nil {
		t.Fatalf("recheck with the respond budget left: %v", err)
	}
	g.chain.chainTime = time.Unix(int64(g.snapshot.ResponseDeadline)-120, 0)
	if err := gate2.Recheck(ctx, slot2, g.claim, verified2); err == nil {
		t.Fatal("recheck without the respond budget left passed")
	}
}

func TestGateTurnsAStartedSlotAmbiguousAfterARestart(t *testing.T) {
	f := newFixture(t)
	directory := filepath.Join(t.TempDir(), "slots")
	gate, err := OpenGate(f.verifier, directory)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	slot, _, err := gate.Admit(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Run(ctx, slot, f.claim, func(context.Context, *Verified) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenGate(f.verifier, directory); err == nil {
		t.Fatal("a second gate opened an owned directory")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenGate(f.verifier, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current, found, err := reopened.Lookup(slot.Key())
	if err != nil || !found || current.State != SlotAmbiguous {
		t.Fatalf("after restart: %+v %v %v", current, found, err)
	}
	if err := reopened.Complete(slot.Key(), strings.Repeat("cd", 32)); err == nil {
		t.Fatal("an ambiguous execution was completed")
	}
	if _, _, err := reopened.Admit(ctx, f.claim); !errors.Is(err, ErrSlotTaken) {
		t.Fatalf("an ambiguous request was admitted again: %v", err)
	}
}

func TestGateRefusesTamperedOrForeignSlotFiles(t *testing.T) {
	f := newFixture(t)
	gate, directory := openGate(t, f)
	slot, _, err := gate.Admit(context.Background(), f.claim)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, slot.Key().fileName())
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), `"admitted"`, `"completed"`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := gate.Lookup(slot.Key()); err == nil {
		t.Fatal("a completed slot without a response hash was read")
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := gate.Lookup(slot.Key()); err == nil {
		t.Fatal("a world-readable slot was read")
	}
}

func TestGateFailRecordsAnExecutionThatDidNotSucceed(t *testing.T) {
	f := newFixture(t)
	gate, _ := openGate(t, f)
	ctx := context.Background()
	slot, _, err := gate.Admit(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Fail(slot.Key()); err == nil {
		t.Fatal("a never started slot failed")
	}
	if err := gate.Run(ctx, slot, f.claim, func(context.Context, *Verified) error { return errors.New("exit 1") }); err == nil {
		t.Fatal("the execution error was lost")
	}
	if err := gate.Fail(slot.Key()); err != nil {
		t.Fatal(err)
	}
	if err := gate.Complete(slot.Key(), strings.Repeat("cd", 32)); err == nil {
		t.Fatal("a failed execution was completed")
	}
}
