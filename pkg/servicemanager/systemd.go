// Package servicemanager provides a fixed-unit systemd adapter for operator
// composition. It never invokes a shell and no remote command can select the
// executable, unit, verb or arguments.
package servicemanager

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"regexp"
	"time"
)

const MaxOperationTimeout = 2 * time.Minute

var unitPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]{0,126}\.service$`)

type Runner interface {
	Run(context.Context, string, ...string) error
}

type Systemd struct {
	unit    string
	binary  string
	timeout time.Duration
	runner  Runner
}

func NewSystemd(unit string, timeout time.Duration) (*Systemd, error) {
	return NewSystemdWithRunner(unit, "/usr/bin/systemctl", timeout, commandRunner{})
}

// NewSystemdWithRunner exists for deterministic tests and reviewed platform
// adapters. Production callers should use NewSystemd.
func NewSystemdWithRunner(unit, binary string, timeout time.Duration, runner Runner) (*Systemd, error) {
	if !unitPattern.MatchString(unit) || binary != "/usr/bin/systemctl" || timeout <= 0 || timeout > MaxOperationTimeout || nilRunner(runner) {
		return nil, errors.New("invalid systemd service configuration")
	}
	return &Systemd{unit: unit, binary: binary, timeout: timeout, runner: runner}, nil
}

func (s *Systemd) Restart(ctx context.Context) error { return s.run(ctx, "restart") }

func (s *Systemd) Reload(ctx context.Context) error { return s.run(ctx, "reload") }

func (s *Systemd) CheckReady(ctx context.Context) error { return s.run(ctx, "is-active", "--quiet") }

func (s *Systemd) run(ctx context.Context, verb string, suffix ...string) (resultErr error) {
	if s == nil || ctx == nil || s.runner == nil {
		return errors.New("invalid systemd service operation")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	operationContext, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	arguments := []string{"--no-ask-password", verb}
	arguments = append(arguments, suffix...)
	arguments = append(arguments, s.unit)
	defer func() {
		if recover() != nil {
			resultErr = errors.New("systemd service operation failed")
		}
	}()
	if err := s.runner.Run(operationContext, s.binary, arguments...); err != nil {
		if contextErr := operationContext.Err(); contextErr != nil {
			return contextErr
		}
		return errors.New("systemd service operation failed")
	}
	if err := operationContext.Err(); err != nil {
		return err
	}
	return nil
}

type commandRunner struct{}

func (commandRunner) Run(ctx context.Context, binary string, arguments ...string) error {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Stdin = nil
	command.Stdout = nil
	command.Stderr = nil
	return command.Run()
}

func nilRunner(runner Runner) bool {
	if runner == nil {
		return true
	}
	value := reflect.ValueOf(runner)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
