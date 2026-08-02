// Package benchmark produces privacy-minimized, signed benchmark evidence.
// The runner is injected: production may use a reviewed local runtime while
// tests use the deterministic MOCK runner. Hardware serials, hostnames and raw
// runtime output are deliberately absent from the evidence format.
package benchmark

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
	"github.com/tosnetwork/tos-protocol/pkg/codec"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	"github.com/tosnetwork/tos-protocol/pkg/protocol"
)

const (
	MaxCases       = 32
	MaxIterations  = 100
	MaxCaseIDBytes = 115
	evidenceDomain = "tos.evidence-bundle.v1"
)

type Case struct {
	ID             string
	Iterations     uint32
	Deadline       time.Duration
	ExpectedDigest string
}

type Sample struct {
	CaseID       string `json:"caseId" cbor:"caseId"`
	Iterations   uint32 `json:"iterations" cbor:"iterations"`
	Successes    uint32 `json:"successes" cbor:"successes"`
	TotalMillis  uint64 `json:"totalMillis" cbor:"totalMillis"`
	OutputDigest string `json:"outputDigest" cbor:"outputDigest"`
}

type Runner interface {
	Run(context.Context, Case) (Sample, error)
}

type Issuer struct {
	keyID      string
	issuer     string
	privateKey ed25519.PrivateKey
	runner     Runner
}

func NewIssuer(keyID, issuer string, privateKey ed25519.PrivateKey, runner Runner) (*Issuer, error) {
	if !validID(keyID, 512) || !validID(issuer, 512) || len(privateKey) != ed25519.PrivateKeySize || nilcheck.IsNil(runner) {
		return nil, errors.New("invalid benchmark issuer")
	}
	return &Issuer{keyID: keyID, issuer: issuer, privateKey: append(ed25519.PrivateKey(nil), privateKey...), runner: runner}, nil
}

func (i *Issuer) Run(ctx context.Context, bundleID, subject string, cases []Case, now, expiresAt time.Time) (identity.Envelope, error) {
	if i == nil || ctx == nil || !validID(bundleID, 128) || !validID(subject, 512) || len(cases) == 0 || len(cases) > MaxCases ||
		now.IsZero() || !expiresAt.After(now) || expiresAt.Sub(now) > identity.MaxLifetime {
		return identity.Envelope{}, errors.New("invalid benchmark request")
	}
	ordered := append([]Case(nil), cases...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].ID < ordered[right].ID })
	claims := make([]protocol.EvidenceClaim, 0, len(ordered))
	for index, benchmarkCase := range ordered {
		if validateCase(benchmarkCase) != nil || index > 0 && benchmarkCase.ID == ordered[index-1].ID {
			return identity.Envelope{}, errors.New("invalid benchmark request")
		}
		caseContext, cancel := context.WithTimeout(ctx, benchmarkCase.Deadline)
		sample, err := callRunner(caseContext, i.runner, benchmarkCase)
		contextErr := caseContext.Err()
		cancel()
		if contextErr != nil {
			return identity.Envelope{}, contextErr
		}
		if err != nil || validateSample(benchmarkCase, sample) != nil {
			return identity.Envelope{}, errors.New("benchmark execution failed")
		}
		encoded, err := codec.Marshal(sample)
		if err != nil {
			return identity.Envelope{}, errors.New("benchmark encoding failed")
		}
		digest := sha256.Sum256(encoded)
		claims = append(claims, protocol.EvidenceClaim{
			Type: "ai.benchmark." + benchmarkCase.ID, Level: protocol.EvidenceBenchmarked,
			Subject: subject, Issuer: i.issuer, CollectedAt: now.UTC(), ExpiresAt: expiresAt.UTC(),
			Digest: "sha256:" + hex.EncodeToString(digest[:]),
		})
	}
	bundle := protocol.EvidenceBundle{Version: protocol.BaseEnvelopeVersion, BundleID: bundleID, Claims: claims}
	if err := bundle.Validate(now.UTC()); err != nil {
		return identity.Envelope{}, errors.New("benchmark evidence rejected")
	}
	return identity.SignCanonical(i.privateKey, evidenceDomain, i.keyID, bundle, now.UTC(), expiresAt.UTC())
}

func validateCase(value Case) error {
	if !validCaseID(value.ID) || value.Iterations == 0 || value.Iterations > MaxIterations ||
		value.Deadline < time.Millisecond || value.Deadline > time.Hour || !validDigest(value.ExpectedDigest) {
		return errors.New("invalid benchmark case")
	}
	return nil
}

func validateSample(expected Case, value Sample) error {
	if value.CaseID != expected.ID || value.Iterations != expected.Iterations || value.Successes != expected.Iterations ||
		value.TotalMillis == 0 || value.TotalMillis > uint64(expected.Deadline/time.Millisecond) ||
		value.OutputDigest != expected.ExpectedDigest {
		return errors.New("invalid benchmark sample")
	}
	return nil
}

func callRunner(ctx context.Context, runner Runner, value Case) (sample Sample, err error) {
	defer func() {
		if recover() != nil {
			sample = Sample{}
			err = errors.New("benchmark runner panicked")
		}
	}()
	return runner.Run(ctx, value)
}

func validCaseID(value string) bool {
	if len(value) == 0 || len(value) > MaxCaseIDBytes || value[0] < 'a' || value[0] > 'z' ||
		value[len(value)-1] == '.' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') && character != '.' && character != '-' {
			return false
		}
	}
	return true
}

func validID(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character < ' ' || character == '\x7f' {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}
