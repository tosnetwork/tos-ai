package fleetcontrol

import (
	"context"
	"errors"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
)

// ReleaseController, PolicyController and AvailabilityController are narrow
// operator-owned bridges. They deliberately receive only a previously signed,
// validated digest or a fixed lifecycle action, never a path, URL, unit name,
// shell command, runtime endpoint or credential.
type ReleaseController interface {
	InstallRelease(context.Context, string) error
	RollbackRelease(context.Context, string) error
}

type PolicyController interface {
	ApplyPolicy(context.Context, string) error
	RollbackPolicy(context.Context, string) error
}

type AvailabilityController interface {
	Drain(context.Context) error
	Resume(context.Context) error
}

type ActionExecutor struct {
	releases     ReleaseController
	policies     PolicyController
	availability AvailabilityController
}

func NewActionExecutor(releases ReleaseController, policies PolicyController, availability AvailabilityController) (*ActionExecutor, error) {
	if nilcheck.IsNil(releases) || nilcheck.IsNil(policies) || nilcheck.IsNil(availability) {
		return nil, errors.New("invalid fleet action executor")
	}
	return &ActionExecutor{releases: releases, policies: policies, availability: availability}, nil
}

func (e *ActionExecutor) Apply(ctx context.Context, command Command) (resultErr error) {
	if e == nil || ctx == nil || !validAction(command.Action) || !validCommandDigests(command) {
		return errors.New("invalid fleet action")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if recover() != nil {
			resultErr = errors.New("fleet action controller failed")
		}
	}()
	var err error
	switch command.Action {
	case "install-release":
		err = e.releases.InstallRelease(ctx, command.ReleaseDigest)
	case "rollback":
		err = e.releases.RollbackRelease(ctx, command.ReleaseDigest)
	case "apply-policy":
		err = e.policies.ApplyPolicy(ctx, command.PolicyDigest)
	case "rollback-policy":
		err = e.policies.RollbackPolicy(ctx, command.PolicyDigest)
	case "drain":
		err = e.availability.Drain(ctx)
	case "resume":
		err = e.availability.Resume(ctx)
	default:
		return errors.New("invalid fleet action")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if err != nil {
		return errors.New("fleet action controller failed")
	}
	return nil
}

var _ Executor = (*ActionExecutor)(nil)
