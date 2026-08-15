package agentpacketadapter

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"

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
