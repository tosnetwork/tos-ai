package fleetcontrol

import (
	"context"
	"errors"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/identity"
)

type RolloutTarget struct {
	TerminalID string
	Agent      *Agent
	Apply      identity.Envelope
	Rollback   identity.Envelope
}

type RolloutReport struct {
	Succeeded  []string
	RolledBack []string
	Failed     string
}

// Rollout applies a deterministic canary ring before the remaining group. A
// failed or merely queued command stops promotion and invokes independently
// signed rollback commands on every terminal already touched.
func Rollout(ctx context.Context, targets []RolloutTarget, canaries int, now time.Time) (RolloutReport, error) {
	if ctx == nil || now.IsZero() || len(targets) == 0 || len(targets) > 4096 || canaries <= 0 || canaries > len(targets) {
		return RolloutReport{}, errors.New("invalid fleet rollout")
	}
	ids := make([]string, len(targets))
	byID := make(map[string]RolloutTarget, len(targets))
	for index, target := range targets {
		if target.Agent == nil {
			return RolloutReport{}, errors.New("nil fleet rollout target")
		}
		ids[index] = target.TerminalID
		byID[target.TerminalID] = target
	}
	ordered, err := SortTerminalIDs(ids)
	if err != nil || len(byID) != len(targets) {
		return RolloutReport{}, errors.New("invalid fleet rollout targets")
	}
	report := RolloutReport{}
	for index, terminalID := range ordered {
		if index == canaries {
			// The completed canary ring is the health gate: all commands must have
			// reached a terminal success, never merely an offline queue.
			if len(report.Succeeded) != canaries {
				break
			}
		}
		target := byID[terminalID]
		result, submitErr := target.Agent.Submit(ctx, target.Apply, now)
		if submitErr != nil || result.State != "succeeded" {
			report.Failed = terminalID
			for rollbackIndex := len(report.Succeeded) - 1; rollbackIndex >= 0; rollbackIndex-- {
				rollbackID := report.Succeeded[rollbackIndex]
				rollbackTarget := byID[rollbackID]
				rollbackResult, rollbackErr := rollbackTarget.Agent.Submit(ctx, rollbackTarget.Rollback, now)
				if rollbackErr == nil && rollbackResult.State == "succeeded" {
					report.RolledBack = append(report.RolledBack, rollbackID)
				}
			}
			return report, errors.New("fleet rollout health gate failed")
		}
		report.Succeeded = append(report.Succeeded, terminalID)
	}
	return report, nil
}
