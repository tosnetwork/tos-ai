package benchmark

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/protocol"
)

type mockRunner struct {
	fail     bool
	panicNow bool
}

func TestNewIssuerRejectsTypedNilMOCKRunner(t *testing.T) {
	_, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	var runner *mockRunner
	if issuer, err := NewIssuer("key", "issuer", privateKey, runner); err == nil || issuer != nil {
		t.Fatal("typed-nil benchmark runner accepted")
	}
}

func (m mockRunner) Run(_ context.Context, value Case) (Sample, error) {
	if m.panicNow {
		panic("injected")
	}
	if m.fail {
		return Sample{}, errors.New("injected")
	}
	return Sample{CaseID: value.ID, Iterations: value.Iterations, Successes: value.Iterations,
		TotalMillis: uint64(value.Iterations), OutputDigest: value.ExpectedDigest}, nil
}

func TestIssuerProducesVerifiableBenchmarkEvidenceWithMOCKRunner(t *testing.T) {
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewIssuer("benchmark-key", "operator-benchmark", privateKey, mockRunner{})
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	envelope, err := issuer.Run(context.Background(), "bundle-0001", "terminal-class-a", []Case{
		{ID: "text-generation", Iterations: 3, Deadline: time.Second, ExpectedDigest: digest},
	}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var bundle protocol.EvidenceBundle
	err = envelope.VerifyCanonical(publicKey, evidenceDomain, now, &bundle)
	if err != nil || len(bundle.Claims) != 1 || bundle.Validate(now) != nil ||
		bundle.Claims[0].Type != "ai.benchmark.text-generation" || bundle.Claims[0].Level != protocol.EvidenceBenchmarked {
		t.Fatalf("bundle=%#v err=%v", bundle, err)
	}
	if envelope.Domain != evidenceDomain {
		t.Fatal("wrong evidence domain")
	}
}

func TestIssuerFailsClosedOnMOCKFailureAndPanic(t *testing.T) {
	_, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().UTC()
	testCase := []Case{{ID: "text-generation", Iterations: 1, Deadline: time.Second,
		ExpectedDigest: "sha256:" + strings.Repeat("a", 64)}}
	for name, runner := range map[string]Runner{"error": mockRunner{fail: true}, "panic": mockRunner{panicNow: true}} {
		t.Run(name, func(t *testing.T) {
			issuer, err := NewIssuer("key", "issuer", privateKey, runner)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := issuer.Run(context.Background(), "bundle-1", "terminal", testCase, now, now.Add(time.Hour)); err == nil {
				t.Fatal("failure injection was accepted")
			}
		})
	}
}
