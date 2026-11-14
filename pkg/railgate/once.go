package railgate

import (
	"context"
	"errors"
	"fmt"
)

// Status is the operator-facing result of one ExecuteOnce call.
type Status string

const (
	// StatusCompleted: admitted, executed once, and the response hash is
	// recorded in the completed slot.
	StatusCompleted Status = "completed"
	// StatusAlreadyTaken: the request already has a slot that is not
	// ambiguous; it is never executed again.
	StatusAlreadyTaken Status = "already_taken"
	// StatusNotPendingNow: admitted, but the fresh pre-execution check
	// refused; the slot is abandoned and nothing ran.
	StatusNotPendingNow Status = "not_pending_now"
	// StatusRefusedBeforeExecution: admitted, but the caller's check of the
	// verified request refused it; the slot is abandoned and nothing ran.
	StatusRefusedBeforeExecution Status = "refused_before_execution"
	// StatusAmbiguous: an execution may have run without a recorded
	// outcome, or the slot could not be updated. It is never re-run or
	// attested; the buyer's refund path applies.
	StatusAmbiguous Status = "ambiguous"
	// StatusNotAdmitted: verification at a fresh checkpoint refused the
	// claim; no slot was created and nothing ran.
	StatusNotAdmitted Status = "not_admitted"
	// StatusUnavailable: no finalized checkpoint could be opened, or the
	// local slot store failed before a slot existed. Nothing was admitted
	// or run and the claim may be retried.
	StatusUnavailable Status = "unavailable"
	// StatusExecutionFailed: the execution ran and did not succeed; the
	// slot is failed and is never attested.
	StatusExecutionFailed Status = "execution_failed"
)

// Result is the outcome of ExecuteOnce. Slot is the latest slot record
// known, Verified is set once a verification succeeded, and Err explains
// every status other than StatusCompleted.
type Result struct {
	Status       Status
	Slot         Slot
	Verified     *Verified
	ResponseHash string
	Err          error
}

// ExecuteOnce drives one claim through the whole at-most-once lifecycle:
// Admit at a fresh checkpoint, check (when not nil) the verified request
// against the caller's own policy, Run re-verifies right before execution,
// and the outcome is recorded with Complete or Fail. A refusing check
// abandons the slot without running anything. execute returns the response
// hash (64 lowercase hex) of a successful execution; an error, or a
// malformed hash, records the slot failed. execute is called at most once
// and only after both verifications and the check passed and the slot is
// durably started.
func (g *Gate) ExecuteOnce(ctx context.Context, claim Claim, check func(*Verified) error,
	execute func(context.Context, *Verified) (string, error)) Result {
	if g == nil || ctx == nil || execute == nil {
		return Result{Status: StatusNotAdmitted, Err: errors.New("railgate: invalid execution")}
	}
	slot, verified, err := g.Admit(ctx, claim)
	switch {
	case errors.Is(err, ErrSlotTaken):
		result := Result{Status: StatusAlreadyTaken, Slot: slot, Verified: verified, Err: err}
		if slot.State == SlotStarted || slot.State == SlotAmbiguous {
			result.Status = StatusAmbiguous
		}
		return result
	case errors.Is(err, ErrResolverUnavailable):
		return Result{Status: StatusUnavailable, Err: err}
	case err != nil && verified != nil:
		// The claim verified but the slot store failed. A slot that exists
		// anyway may never run again; without one nothing was admitted.
		storeErr := fmt.Errorf("railgate: the slot store failed: %w", err)
		existing, found, lookupErr := g.Lookup(verified.SlotKey())
		switch {
		case lookupErr == nil && !found:
			return Result{Status: StatusUnavailable, Verified: verified, Err: storeErr}
		case lookupErr == nil:
			return Result{Status: StatusAmbiguous, Slot: existing, Verified: verified, Err: storeErr}
		default:
			return Result{Status: StatusAmbiguous, Verified: verified, Err: errors.Join(storeErr, lookupErr)}
		}
	case err != nil:
		return Result{Status: StatusNotAdmitted, Err: err}
	}
	key := slot.Key()
	result := Result{Slot: slot, Verified: verified}
	if check != nil {
		if checkErr := check(verified); checkErr != nil {
			if transitionErr := g.transition(key, SlotAdmitted, SlotAbandoned, ""); transitionErr != nil {
				result.Status, result.Err = StatusAmbiguous, errors.Join(checkErr, transitionErr)
			} else {
				result.Status, result.Err = StatusRefusedBeforeExecution, checkErr
			}
			return g.withCurrentSlot(result)
		}
	}
	executed := false
	var responseHash string
	runErr := g.Run(ctx, slot, claim, func(runCtx context.Context, current *Verified) error {
		executed = true
		hash, executeErr := execute(runCtx, current)
		if executeErr != nil {
			return executeErr
		}
		if !responseHashPattern.MatchString(hash) {
			return errors.New("railgate: the execution returned an invalid response hash")
		}
		responseHash = hash
		return nil
	})
	switch {
	case !executed && errors.Is(runErr, ErrNotPendingNow):
		result.Status, result.Err = StatusNotPendingNow, runErr
	case !executed:
		// The slot is admitted or started without an execution: either
		// way it may never run again, and an operator must look at it.
		result.Status, result.Err = StatusAmbiguous, fmt.Errorf("railgate: the slot could not be started: %w", runErr)
	case runErr != nil:
		if failErr := g.Fail(key); failErr != nil {
			result.Status, result.Err = StatusAmbiguous, errors.Join(runErr, failErr)
		} else {
			result.Status, result.Err = StatusExecutionFailed, runErr
		}
	default:
		if completeErr := g.Complete(key, responseHash); completeErr != nil {
			result.Status, result.Err = StatusAmbiguous, fmt.Errorf("railgate: the execution ran but its outcome was not recorded: %w", completeErr)
		} else {
			result.Status, result.ResponseHash = StatusCompleted, responseHash
		}
	}
	return g.withCurrentSlot(result)
}

// withCurrentSlot reports the slot as recorded; a failed read leaves the
// last known one.
func (g *Gate) withCurrentSlot(result Result) Result {
	if current, found, err := g.Lookup(result.Slot.Key()); err == nil && found {
		result.Slot = current
	}
	return result
}
