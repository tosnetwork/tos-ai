package mcpadapter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
)

type gateFake struct {
	calls int
	fail  bool
}

func (f *gateFake) ClaimExecution(_ context.Context, r executiongate.Request) (Evidence, error) {
	f.calls++
	if f.fail {
		return Evidence{}, errors.New("conflict")
	}
	return Evidence{NetworkID: "test", ProviderAgentID: "agent_" + strings.Repeat("10", 32),
		CapabilityID: "cap_" + strings.Repeat("11", 32), CapabilityVersion: "1",
		ManifestDigest: "sha256:" + strings.Repeat("22", 32), QuoteCommitment: r.QuoteCommitment,
		EscrowAddress: r.EscrowAddress, ProviderAddress: "0:" + strings.Repeat("34", 32),
		EscrowCodeHash:            "tvm-cell-sha256:" + strings.Repeat("35", 32),
		RegistryCodeHash:          "tvm-cell-sha256:" + strings.Repeat("36", 32),
		EscrowTransactionHash:     "sha256:" + strings.Repeat("37", 32),
		AgentTransactionHash:      "sha256:" + strings.Repeat("38", 32),
		CapabilityTransactionHash: "sha256:" + strings.Repeat("39", 32),
		EscrowFinalizedCheckpoint: 7, AgentFinalizedCheckpoint: 8, CapabilityFinalizedCheckpoint: 9}, nil
}

type runnerFake struct{ calls int }

func (f *runnerFake) Execute(_ context.Context, r softwarework.Request) (softwarework.Outcome, error) {
	f.calls++
	return softwarework.Outcome{QuoteCommitment: r.QuoteCommitment, ExecutionID: r.ExecutionID, InputDigest: r.InputDigest, SourceDigest: r.SourceDigest, ResultDigest: "sha256:" + strings.Repeat("44", 32), Artifact: artifactstore.Descriptor{Digest: "sha256:" + strings.Repeat("55", 32), MediaType: softwarework.ArtifactMediaType, SizeBytes: 10}, Report: artifactstore.Descriptor{Digest: "sha256:" + strings.Repeat("66", 32), MediaType: softwarework.ReportMediaType, SizeBytes: 5}, ToolchainDigest: "sha256:" + strings.Repeat("77", 32), SandboxDigest: "sha256:" + strings.Repeat("88", 32), CompletedAtUnix: 2_000_000_000}, nil
}

type locatorFake struct{}

func (locatorFake) URL(v artifactstore.Descriptor) (string, error) {
	return "https://provider.example/objects/" + v.Digest[7:], nil
}

type failingLocator struct{}

func (failingLocator) URL(artifactstore.Descriptor) (string, error) {
	return "", errors.New("publication failed")
}

type settlerFake struct{ calls int }

func (s *settlerFake) Settle(context.Context, executiongate.Evidence, softwarework.Outcome) error {
	s.calls++
	return nil
}

func TestMCPAdapterExecutesOnlyCommittedInput(t *testing.T) {
	gate, runner := &gateFake{}, &runnerFake{}
	adapter, err := New(gate, runner, locatorFake{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput("0:"+strings.Repeat("cc", 32), "tvm-cell-sha256:"+strings.Repeat("aa", 32),
		"sha256:"+strings.Repeat("bb", 32), []byte("source"))
	if err != nil {
		t.Fatal(err)
	}
	_, out, err := adapter.Call(context.Background(), nil, input)
	if err != nil {
		t.Fatal(err)
	}
	if out.Protocol != "tos_service_v1" || out.Evidence.EscrowFinalizedCheckpoint != 7 || out.Artifact.URL == "" || gate.calls != 1 || runner.calls != 1 {
		t.Fatal("MCP result lost Native evidence")
	}
	input.SourceArchiveBase64 = "Y2hhbmdlZA=="
	if _, _, err := adapter.Call(context.Background(), nil, input); err == nil {
		t.Fatal("changed MCP source accepted")
	}
	if gate.calls != 1 || runner.calls != 1 {
		t.Fatal("invalid MCP input reached authority or execution")
	}
}

func TestMCPDoesNotSettleBeforeArtifactPublication(t *testing.T) {
	settler := &settlerFake{}
	adapter, err := NewSettling(&gateFake{}, &runnerFake{}, failingLocator{}, settler)
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput("0:"+strings.Repeat("cc", 32), "tvm-cell-sha256:"+strings.Repeat("aa", 32),
		"sha256:"+strings.Repeat("bb", 32), []byte("source"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.Call(context.Background(), nil, input); err == nil {
		t.Fatal("failed artifact publication was accepted")
	}
	if settler.calls != 0 {
		t.Fatal("escrow settled before artifact publication")
	}
}

func TestMCPAdapterRequiresUniqueExecutionClaim(t *testing.T) {
	gate, runner := &gateFake{fail: true}, &runnerFake{}
	adapter, _ := New(gate, runner, locatorFake{})
	input, _ := PrepareInput("0:"+strings.Repeat("cc", 32), "tvm-cell-sha256:"+strings.Repeat("aa", 32),
		"sha256:"+strings.Repeat("bb", 32), []byte("source"))
	if _, _, err := adapter.Call(context.Background(), nil, input); err == nil {
		t.Fatal("unclaimed MCP execution accepted")
	}
	if runner.calls != 0 {
		t.Fatal("unclaimed MCP tool reached runner")
	}
}
