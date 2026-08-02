package fleetcontrol

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type mockActions struct {
	calls    []string
	fail     bool
	panicNow bool
}

func (m *mockActions) call(value string) error {
	if m.panicNow {
		panic("injected")
	}
	m.calls = append(m.calls, value)
	if m.fail {
		return errors.New("injected")
	}
	return nil
}
func (m *mockActions) InstallRelease(_ context.Context, digest string) error {
	return m.call("install:" + digest)
}
func (m *mockActions) RollbackRelease(_ context.Context, digest string) error {
	return m.call("rollback:" + digest)
}
func (m *mockActions) ApplyPolicy(_ context.Context, digest string) error {
	return m.call("policy:" + digest)
}
func (m *mockActions) RollbackPolicy(_ context.Context, digest string) error {
	return m.call("policy-rollback:" + digest)
}
func (m *mockActions) Drain(context.Context) error  { return m.call("drain") }
func (m *mockActions) Resume(context.Context) error { return m.call("resume") }

func TestActionExecutorRoutesOnlyFixedActionsAndDigests(t *testing.T) {
	mock := &mockActions{}
	executor, err := NewActionExecutor(mock, mock, mock)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	commands := []Command{
		{Action: "install-release", ReleaseDigest: digest}, {Action: "rollback", ReleaseDigest: digest},
		{Action: "apply-policy", PolicyDigest: digest}, {Action: "rollback-policy", PolicyDigest: digest},
		{Action: "drain"}, {Action: "resume"},
	}
	for _, command := range commands {
		if err := executor.Apply(context.Background(), command); err != nil {
			t.Fatalf("%s: %v", command.Action, err)
		}
	}
	if len(mock.calls) != len(commands) {
		t.Fatalf("calls=%v", mock.calls)
	}
	if err := executor.Apply(context.Background(), Command{Action: "install-release", ReleaseDigest: "../release"}); err == nil {
		t.Fatal("untrusted release selector accepted")
	}
}

func TestActionExecutorContainsMOCKFailuresAndPanics(t *testing.T) {
	for name, mock := range map[string]*mockActions{"failure": {fail: true}, "panic": {panicNow: true}} {
		t.Run(name, func(t *testing.T) {
			executor, err := NewActionExecutor(mock, mock, mock)
			if err != nil {
				t.Fatal(err)
			}
			if err := executor.Apply(context.Background(), Command{Action: "drain"}); err == nil {
				t.Fatal("failure accepted")
			}
		})
	}
}
