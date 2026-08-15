// Package agentpacketadapter admits Agent Packet software-work tasks through
// the same finalized Native Execution Gate used by A2A and MCP.
package agentpacketadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-protocol/pkg/agentpacket"
	"github.com/tosnetwork/tos-protocol/pkg/executiongate"
)

const PayloadSchema = "atos.native.agent-packet-work.v1"

type Gate interface {
	ClaimExecution(context.Context, executiongate.Request) (executiongate.Evidence, error)
}

type Runner interface {
	Execute(context.Context, softwarework.Request) (softwarework.Outcome, error)
}

type Adapter struct {
	gate   Gate
	runner Runner
}

// Receive implements agentpacket.Receiver, allowing the protocol HTTP
// verifier to deliver purchase-bound packets directly to this adapter. The
// packet verifier remains responsible for identity and replay; this method
// adds the finalized commercial Gate before the runner is reached.
func (a *Adapter) Receive(ctx context.Context, packet agentpacket.Packet) error {
	_, _, err := a.Execute(ctx, packet)
	return err
}

type payload struct {
	Schema              string `json:"schema"`
	EscrowAddress       string `json:"escrow_address"`
	QuoteCommitment     string `json:"quote_commitment"`
	ExecutionID         string `json:"execution_id"`
	InputDigest         string `json:"input_digest"`
	SourceDigest        string `json:"source_digest"`
	SourceArchiveBase64 string `json:"source_archive_base64"`
}

func New(gate Gate, runner Runner) (*Adapter, error) {
	if gate == nil || runner == nil {
		return nil, errors.New("invalid Agent Packet adapter configuration")
	}
	return &Adapter{gate: gate, runner: runner}, nil
}

// Execute admits only a purchase-bound work packet. The packet itself is an
// authenticated transport envelope; finalized commercial authority comes only
// from ClaimExecution.
func (a *Adapter) Execute(ctx context.Context, packet agentpacket.Packet) (softwarework.Outcome, executiongate.Evidence, error) {
	if a == nil || ctx == nil {
		return softwarework.Outcome{}, executiongate.Evidence{}, errors.New("invalid Agent Packet execution context")
	}
	work, claim, err := decode(packet)
	if err != nil {
		return softwarework.Outcome{}, executiongate.Evidence{}, err
	}
	evidence, err := a.gate.ClaimExecution(ctx, claim)
	if err != nil {
		return softwarework.Outcome{}, executiongate.Evidence{}, errors.New("Agent Packet lacks finalized ATOS authorization")
	}
	if evidence.QuoteCommitment != claim.QuoteCommitment || evidence.EscrowAddress != claim.EscrowAddress || evidence.CapabilityID != packet.CapabilityID {
		return softwarework.Outcome{}, executiongate.Evidence{}, errors.New("execution evidence does not match Agent Packet")
	}
	outcome, err := a.runner.Execute(ctx, work)
	if err != nil {
		return softwarework.Outcome{}, evidence, err
	}
	if outcome.QuoteCommitment != work.QuoteCommitment || outcome.ExecutionID != work.ExecutionID || outcome.InputDigest != work.InputDigest || outcome.SourceDigest != work.SourceDigest {
		return softwarework.Outcome{}, evidence, errors.New("software-work runner returned a conflicting outcome")
	}
	return outcome, evidence, nil
}

func decode(packet agentpacket.Packet) (softwarework.Request, executiongate.Request, error) {
	if packet.QuoteCommitment == "" {
		return softwarework.Request{}, executiongate.Request{}, errors.New("Agent Packet is not purchase-bound")
	}
	dec := json.NewDecoder(bytes.NewReader(packet.Payload))
	dec.DisallowUnknownFields()
	var p payload
	if err := dec.Decode(&p); err != nil || p.Schema != PayloadSchema {
		return softwarework.Request{}, executiongate.Request{}, errors.New("invalid Agent Packet work payload")
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return softwarework.Request{}, executiongate.Request{}, errors.New("Agent Packet work payload has trailing JSON")
	}
	if p.QuoteCommitment != packet.QuoteCommitment || p.SourceArchiveBase64 == "" {
		return softwarework.Request{}, executiongate.Request{}, errors.New("Agent Packet Quote binding mismatch")
	}
	source, err := base64.StdEncoding.Strict().DecodeString(p.SourceArchiveBase64)
	if err != nil || len(source) == 0 {
		return softwarework.Request{}, executiongate.Request{}, errors.New("invalid Agent Packet source archive")
	}
	if digest(source) != p.SourceDigest {
		return softwarework.Request{}, executiongate.Request{}, errors.New("Agent Packet source digest mismatch")
	}
	work := softwarework.Request{QuoteCommitment: p.QuoteCommitment, ExecutionID: p.ExecutionID, InputDigest: p.InputDigest, SourceDigest: p.SourceDigest, SourceArchive: source}
	if !validDigest(work.ExecutionID, "sha256:") || !validDigest(work.InputDigest, "sha256:") || !validDigest(work.SourceDigest, "sha256:") {
		return softwarework.Request{}, executiongate.Request{}, errors.New("invalid Agent Packet work digest")
	}
	return work, executiongate.Request{EscrowAddress: p.EscrowAddress, QuoteCommitment: p.QuoteCommitment, ExecutionID: p.ExecutionID, InputDigest: p.InputDigest, SourceDigest: p.SourceDigest}, nil
}

func digest(v []byte) string { h := sha256.Sum256(v); return "sha256:" + hex.EncodeToString(h[:]) }
func validDigest(v, prefix string) bool {
	return len(v) == len(prefix)+64 && strings.HasPrefix(v, prefix) && v == strings.ToLower(v)
}
