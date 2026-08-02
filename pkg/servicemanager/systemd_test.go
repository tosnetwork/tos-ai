package servicemanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type mockRunner struct {
	binary    string
	arguments []string
	block     bool
	panicNow  bool
}

func (m *mockRunner) Run(ctx context.Context, binary string, arguments ...string) error {
	if m.panicNow {
		panic("injected")
	}
	m.binary = binary
	m.arguments = append([]string(nil), arguments...)
	if m.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func TestFixedSystemdOperationsWithMOCKRunner(t *testing.T) {
	runner := &mockRunner{}
	manager, err := NewSystemdWithRunner("tos-ai-worker.service", "/usr/bin/systemctl", time.Second, runner)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Restart(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runner.binary != "/usr/bin/systemctl" || !reflect.DeepEqual(runner.arguments,
		[]string{"--no-ask-password", "restart", "tos-ai-worker.service"}) {
		t.Fatalf("command=%q %v", runner.binary, runner.arguments)
	}
	if err := manager.CheckReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(runner.arguments,
		[]string{"--no-ask-password", "is-active", "--quiet", "tos-ai-worker.service"}) {
		t.Fatalf("health command=%v", runner.arguments)
	}
}

func TestSystemdRejectsInjectionAndContainsMOCKFailure(t *testing.T) {
	for _, unit := range []string{"tos-ai-worker", "../worker.service", "worker.service --now", ";reboot.service"} {
		if _, err := NewSystemdWithRunner(unit, "/usr/bin/systemctl", time.Second, &mockRunner{}); err == nil {
			t.Fatalf("unsafe unit accepted: %q", unit)
		}
	}
	if _, err := NewSystemdWithRunner("worker.service", "/tmp/systemctl", time.Second, &mockRunner{}); err == nil {
		t.Fatal("request-selected executable accepted")
	}
	var typedNil *mockRunner
	if _, err := NewSystemdWithRunner("worker.service", "/usr/bin/systemctl", time.Second, typedNil); err == nil {
		t.Fatal("typed-nil runner accepted")
	}
	for name, runner := range map[string]*mockRunner{"timeout": {block: true}, "panic": {panicNow: true}} {
		t.Run(name, func(t *testing.T) {
			manager, err := NewSystemdWithRunner("worker.service", "/usr/bin/systemctl", time.Millisecond, runner)
			if err != nil {
				t.Fatal(err)
			}
			err = manager.Restart(context.Background())
			if err == nil {
				t.Fatal("injected failure accepted")
			}
			if name == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
