package agentpacketadapter

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/agentpacket"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
)

type gateFake struct{ calls int }

func (g *gateFake) ClaimExecution(_ context.Context, r executiongate.Request) (executiongate.Evidence, error) {
	g.calls++
	return executiongate.Evidence{QuoteCommitment: r.QuoteCommitment, EscrowAddress: r.EscrowAddress, CapabilityID: "cap_" + repeat("ab", 32)}, nil
}

type runnerFake struct{ calls int }

func (r *runnerFake) Execute(_ context.Context, w softwarework.Request) (softwarework.Outcome, error) {
	r.calls++
	return softwarework.Outcome{QuoteCommitment: w.QuoteCommitment, ExecutionID: w.ExecutionID, InputDigest: w.InputDigest, SourceDigest: w.SourceDigest}, nil
}
func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func TestExecuteClaimsSharedGateBeforeRunner(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	source := []byte("source")
	execID := digest([]byte("exec"))
	input := digest([]byte("input"))
	sourceDigest := digest(source)
	// The payload is built through the same strict wire shape as production.
	p := payload{Schema: PayloadSchema, EscrowAddress: "0:" + repeat("11", 32), QuoteCommitment: "tvm-cell-sha256:" + repeat("22", 32), ExecutionID: execID, InputDigest: input, SourceDigest: sourceDigest, SourceArchiveBase64: "c291cmNl"}
	raw, _ := marshalPayload(p)
	packet, err := agentpacket.Sign(agentpacket.Packet{SenderAgentID: "agent_" + repeat("aa", 32), RecipientAgentID: "agent_" + repeat("bb", 32), CapabilityID: "cap_" + repeat("ab", 32), QuoteCommitment: p.QuoteCommitment, Sequence: 1, CreatedAtUnix: 1, Payload: raw, SenderPublicKey: pub}, priv)
	if err != nil {
		t.Fatal(err)
	}
	g, r := &gateFake{}, &runnerFake{}
	a, _ := New(g, r)
	if _, _, err = a.Execute(context.Background(), packet); err != nil {
		t.Fatal(err)
	}
	if g.calls != 1 || r.calls != 1 {
		t.Fatalf("gate=%d runner=%d", g.calls, r.calls)
	}
}

func marshalPayload(p payload) ([]byte, error) { return json.Marshal(p) }

type locatorFake struct {
	descriptors []artifactstore.Descriptor
	fail        bool
}

func (l *locatorFake) ArtifactURL(descriptor artifactstore.Descriptor) (string, error) {
	if l.fail {
		return "", errors.New("publication failed")
	}
	l.descriptors = append(l.descriptors, descriptor)
	return "https://provider.example/v1/artifacts/sha256/" + descriptor.Digest[7:], nil
}

type settlerFake struct{ calls int }

func (s *settlerFake) Settle(context.Context, executiongate.Evidence, softwarework.Outcome) error {
	s.calls++
	return nil
}

type publishingRunner struct{ runnerFake }

func (r *publishingRunner) Execute(ctx context.Context, work softwarework.Request) (softwarework.Outcome, error) {
	outcome, err := r.runnerFake.Execute(ctx, work)
	outcome.Artifact = artifactstore.Descriptor{Digest: digest([]byte("artifact")), MediaType: softwarework.ArtifactMediaType, SizeBytes: 8}
	outcome.Report = artifactstore.Descriptor{Digest: digest([]byte("report")), MediaType: softwarework.ReportMediaType, SizeBytes: 6}
	return outcome, err
}

func TestPublishingSettlingRegistersArtifactAndReport(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	source := []byte("source")
	p := payload{Schema: PayloadSchema, EscrowAddress: "0:" + repeat("11", 32), QuoteCommitment: "tvm-cell-sha256:" + repeat("22", 32), ExecutionID: digest([]byte("exec")), InputDigest: digest([]byte("input")), SourceDigest: digest(source), SourceArchiveBase64: "c291cmNl"}
	raw, _ := marshalPayload(p)
	packet, err := agentpacket.Sign(agentpacket.Packet{SenderAgentID: "agent_" + repeat("aa", 32), RecipientAgentID: "agent_" + repeat("bb", 32), CapabilityID: "cap_" + repeat("ab", 32), QuoteCommitment: p.QuoteCommitment, Sequence: 1, CreatedAtUnix: 1, Payload: raw, SenderPublicKey: pub}, priv)
	if err != nil {
		t.Fatal(err)
	}
	locator := &locatorFake{}
	adapter, err := NewPublishingSettling(&gateFake{}, &publishingRunner{}, locator, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.Execute(context.Background(), packet); err != nil {
		t.Fatal(err)
	}
	if len(locator.descriptors) != 2 || locator.descriptors[0].MediaType != softwarework.ArtifactMediaType || locator.descriptors[1].MediaType != softwarework.ReportMediaType {
		t.Fatalf("published descriptors = %#v", locator.descriptors)
	}
}

func TestPublishingSettlingDoesNotSettleBeforeArtifactPublication(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	source := []byte("source")
	p := payload{Schema: PayloadSchema, EscrowAddress: "0:" + repeat("11", 32), QuoteCommitment: "tvm-cell-sha256:" + repeat("22", 32), ExecutionID: digest([]byte("exec")), InputDigest: digest([]byte("input")), SourceDigest: digest(source), SourceArchiveBase64: "c291cmNl"}
	raw, _ := marshalPayload(p)
	packet, err := agentpacket.Sign(agentpacket.Packet{SenderAgentID: "agent_" + repeat("aa", 32), RecipientAgentID: "agent_" + repeat("bb", 32), CapabilityID: "cap_" + repeat("ab", 32), QuoteCommitment: p.QuoteCommitment, Sequence: 1, CreatedAtUnix: 1, Payload: raw, SenderPublicKey: pub}, priv)
	if err != nil {
		t.Fatal(err)
	}
	settler := &settlerFake{}
	adapter, err := NewPublishingSettling(&gateFake{}, &publishingRunner{}, &locatorFake{fail: true}, settler)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.Execute(context.Background(), packet); err == nil {
		t.Fatal("failed artifact publication was accepted")
	}
	if settler.calls != 0 {
		t.Fatal("escrow settled before artifact publication")
	}
}
