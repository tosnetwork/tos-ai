package railgate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
)

// sessionHook runs before the n-th session (1-based) is opened, so a test
// can change the chain between admission and the pre-execution check.
type sessionHook struct {
	inner  Reader
	opened int
	before func(n int)
}

func (h *sessionHook) BeginSession(ctx context.Context) (Session, error) {
	h.opened++
	if h.before != nil {
		h.before(h.opened)
	}
	return h.inner.BeginSession(ctx)
}

func hookedGate(t *testing.T, f *fixture, before func(n int)) *Gate {
	t.Helper()
	config := f.verifier.config
	config.Reader = &sessionHook{inner: f.chain, before: before}
	verifier, err := NewVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := OpenGate(verifier, filepath.Join(t.TempDir(), "slots"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	return gate
}

var goodHash = strings.Repeat("cd", 32)

func TestExecuteOnceCompletesOnceAndNeverRunsAgain(t *testing.T) {
	f := newFixture(t)
	gate, _ := openGate(t, f)
	ran := 0
	execute := func(_ context.Context, verified *Verified) (string, error) {
		ran++
		if verified == nil || verified.RequestID != 0 || verified.Actor != fxActor {
			t.Errorf("execute got %+v", verified)
		}
		return goodHash, nil
	}
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, execute)
	if result.Status != StatusCompleted || result.Err != nil || result.ResponseHash != goodHash ||
		result.Slot.State != SlotCompleted || result.Slot.ResponseHash != goodHash || ran != 1 || result.Verified == nil {
		t.Fatalf("first run = %+v ran=%d", result, ran)
	}
	again := gate.ExecuteOnce(context.Background(), f.claim, nil, execute)
	if again.Status != StatusAlreadyTaken || !errors.Is(again.Err, ErrSlotTaken) || again.Slot.State != SlotCompleted || ran != 1 {
		t.Fatalf("second run = %+v ran=%d", again, ran)
	}
}

func TestExecuteOnceAbandonsARequestThatStoppedBeingPendingAfterAdmission(t *testing.T) {
	f := newFixture(t)
	gate := hookedGate(t, f, func(n int) {
		if n == 2 {
			f.transition(t, toschain.ActorTxRespond, uint32(fxNow.Unix())-10, fxOwner, true, nil, nil, nil)
		}
	})
	ran := false
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		ran = true
		return goodHash, nil
	})
	if result.Status != StatusNotPendingNow || !errors.Is(result.Err, ErrNotPendingNow) || ran || result.Slot.State != SlotAbandoned {
		t.Fatalf("result = %+v ran=%v", result, ran)
	}
}

func TestExecuteOnceReportsAnAmbiguousSlotAfterACrash(t *testing.T) {
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
	// The process dies after the slot is started and before an outcome.
	if err := gate.Run(ctx, slot, f.claim, func(context.Context, *Verified) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ran := false
	execute := func(context.Context, *Verified) (string, error) { ran = true; return goodHash, nil }
	if started := gate.ExecuteOnce(ctx, f.claim, nil, execute); started.Status != StatusAmbiguous || started.Slot.State != SlotStarted || ran {
		t.Fatalf("started slot = %+v ran=%v", started, ran)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenGate(f.verifier, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	result := reopened.ExecuteOnce(ctx, f.claim, nil, execute)
	if result.Status != StatusAmbiguous || result.Slot.State != SlotAmbiguous || ran {
		t.Fatalf("after restart = %+v ran=%v", result, ran)
	}
}

func TestExecuteOnceDoesNotAdmitAnUnverifiableClaim(t *testing.T) {
	f := newFixture(t)
	gate, directory := openGate(t, f)
	f.chain.incomplete = true
	ran := false
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		ran = true
		return goodHash, nil
	})
	if result.Status != StatusNotAdmitted || result.Err == nil || ran || result.Verified != nil {
		t.Fatalf("result = %+v ran=%v", result, ran)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			t.Fatalf("an unverified claim left slot %s", entry.Name())
		}
	}
	if invalid := gate.ExecuteOnce(context.Background(), f.claim, nil, nil); invalid.Status != StatusNotAdmitted {
		t.Fatalf("nil execute = %+v", invalid)
	}
}

func TestExecuteOnceRecordsAFailedExecution(t *testing.T) {
	for name, execute := range map[string]func(context.Context, *Verified) (string, error){
		"error":          func(context.Context, *Verified) (string, error) { return "", errors.New("tests failed") },
		"malformed hash": func(context.Context, *Verified) (string, error) { return strings.Repeat("CD", 32), nil },
		"empty hash":     func(context.Context, *Verified) (string, error) { return "", nil },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			gate, _ := openGate(t, f)
			result := gate.ExecuteOnce(context.Background(), f.claim, nil, execute)
			if result.Status != StatusExecutionFailed || result.Err == nil || result.Slot.State != SlotFailed || result.ResponseHash != "" {
				t.Fatalf("result = %+v", result)
			}
			if again := gate.ExecuteOnce(context.Background(), f.claim, nil, execute); again.Status != StatusAlreadyTaken {
				t.Fatalf("a failed request was admitted again: %+v", again)
			}
		})
	}
}

func TestExecuteOnceReportsAnOutcomeItCouldNotRecordAsAmbiguous(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not restrict root")
	}
	f := newFixture(t)
	gate, directory := openGate(t, f)
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		// The slot directory stops accepting writes after the execution.
		if err := os.Chmod(directory, 0o500); err != nil {
			t.Fatal(err)
		}
		return goodHash, nil
	})
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusAmbiguous || result.Err == nil || result.Slot.State != SlotStarted {
		t.Fatalf("result = %+v", result)
	}
}

type unavailableReader struct{}

func (unavailableReader) BeginSession(context.Context) (Session, error) {
	return nil, errors.New("quorum not reached")
}

func TestExecuteOnceReportsAnUnavailableResolverAsRetryable(t *testing.T) {
	f := newFixture(t)
	config := f.verifier.config
	config.Reader = unavailableReader{}
	verifier, err := NewVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := OpenGate(verifier, filepath.Join(t.TempDir(), "slots"))
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	ran := false
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		ran = true
		return goodHash, nil
	})
	if result.Status != StatusUnavailable || !errors.Is(result.Err, ErrResolverUnavailable) || ran {
		t.Fatalf("result = %+v ran=%v", result, ran)
	}
}

func TestExecuteOnceReportsASlotStoreFailureBeforeASlotAsRetryable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not restrict root")
	}
	f := newFixture(t)
	gate, directory := openGate(t, f)
	if err := os.Chmod(directory, 0o500); err != nil {
		t.Fatal(err)
	}
	ran := false
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		ran = true
		return goodHash, nil
	})
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusUnavailable || result.Err == nil || result.Verified == nil || ran {
		t.Fatalf("result = %+v ran=%v", result, ran)
	}
	if _, found, err := gate.Lookup(result.Verified.SlotKey()); err != nil || found {
		t.Fatalf("a slot exists after the store failed: %v %v", found, err)
	}
	// Nothing was admitted, so the claim can be retried.
	if retry := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		return goodHash, nil
	}); retry.Status != StatusCompleted {
		t.Fatalf("retry = %+v", retry)
	}
}

func TestExecuteOnceAbandonsAVerifiedRequestTheCheckRefuses(t *testing.T) {
	f := newFixture(t)
	gate, _ := openGate(t, f)
	ran := false
	execute := func(context.Context, *Verified) (string, error) { ran = true; return goodHash, nil }
	checked := 0
	result := gate.ExecuteOnce(context.Background(), f.claim, func(verified *Verified) error {
		checked++
		if verified == nil || verified.RequestID != 0 {
			t.Errorf("check got %+v", verified)
		}
		return errors.New("policy refuses this request")
	}, execute)
	if result.Status != StatusRefusedBeforeExecution || result.Err == nil || result.Slot.State != SlotAbandoned || ran || checked != 1 {
		t.Fatalf("result = %+v ran=%v checked=%d", result, ran, checked)
	}
	if again := gate.ExecuteOnce(context.Background(), f.claim, nil, execute); again.Status != StatusAlreadyTaken || ran {
		t.Fatalf("a refused request was admitted again: %+v", again)
	}
}

func TestExecuteOnceRechecksTheDeadlineRightBeforeExecution(t *testing.T) {
	f := newFixture(t)
	gate := hookedGate(t, f, func(n int) {
		if n == 2 {
			// Still pending, but no longer room to execute and respond.
			f.chain.chainTime = time.Unix(int64(f.snapshot.ResponseDeadline)-299, 0)
		}
	})
	ran := false
	result := gate.ExecuteOnce(context.Background(), f.claim, nil, func(context.Context, *Verified) (string, error) {
		ran = true
		return goodHash, nil
	})
	if result.Status != StatusNotPendingNow || ran {
		t.Fatalf("result = %+v ran=%v", result, ran)
	}
}
