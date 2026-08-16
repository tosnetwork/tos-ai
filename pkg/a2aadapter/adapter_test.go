package a2aadapter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
)

type authorizerFake struct {
	calls int
	fail  bool
}

func (f *authorizerFake) ClaimExecution(_ context.Context, request executiongate.Request) (FinalizedEvidence, error) {
	f.calls++
	if f.fail {
		return FinalizedEvidence{}, errors.New("not funded")
	}
	return FinalizedEvidence{NetworkID: "test", ProviderAgentID: "agent_" + strings.Repeat("10", 32),
		CapabilityID:      "cap_" + strings.Repeat("11", 32),
		CapabilityVersion: "1.0.0", ManifestDigest: "sha256:" + strings.Repeat("22", 32),
		QuoteCommitment: request.QuoteCommitment, EscrowAddress: request.EscrowAddress,
		ProviderAddress: "0:" + strings.Repeat("34", 32), EscrowFinalizedCheckpoint: 42,
		EscrowCodeHash:            "tvm-cell-sha256:" + strings.Repeat("35", 32),
		RegistryCodeHash:          "tvm-cell-sha256:" + strings.Repeat("36", 32),
		EscrowTransactionHash:     "sha256:" + strings.Repeat("37", 32),
		AgentTransactionHash:      "sha256:" + strings.Repeat("38", 32),
		CapabilityTransactionHash: "sha256:" + strings.Repeat("39", 32),
		AgentFinalizedCheckpoint:  43, CapabilityFinalizedCheckpoint: 44}, nil
}

type runnerFake struct {
	calls       int
	fail        bool
	conflicting bool
}

func (f *runnerFake) Execute(_ context.Context, request softwarework.Request) (softwarework.Outcome, error) {
	f.calls++
	if f.fail {
		return softwarework.Outcome{}, errors.New("objective failure")
	}
	result := softwarework.Outcome{QuoteCommitment: request.QuoteCommitment, ExecutionID: request.ExecutionID,
		InputDigest: request.InputDigest, ResultDigest: "sha256:" + strings.Repeat("44", 32),
		Artifact:     artifactstore.Descriptor{Digest: "sha256:" + strings.Repeat("55", 32), MediaType: softwarework.ArtifactMediaType, SizeBytes: 100},
		Report:       artifactstore.Descriptor{Digest: "sha256:" + strings.Repeat("66", 32), MediaType: softwarework.ReportMediaType, SizeBytes: 50},
		SourceDigest: request.SourceDigest, ToolchainDigest: "sha256:" + strings.Repeat("77", 32),
		SandboxDigest: "sha256:" + strings.Repeat("88", 32), CompletedAtUnix: 2_000_000_000}
	if f.conflicting {
		result.ExecutionID = "sha256:" + strings.Repeat("99", 32)
	}
	return result, nil
}

type locatorFake struct{}

func (locatorFake) URL(value artifactstore.Descriptor) (a2a.URL, error) {
	return a2a.URL("https://provider.example/objects/" + value.Digest[7:]), nil
}

type insecureLocator struct{}

func (insecureLocator) URL(artifactstore.Descriptor) (a2a.URL, error) {
	return "http://provider.example/object", nil
}

type failingLocator struct{}

func (failingLocator) URL(artifactstore.Descriptor) (a2a.URL, error) {
	return "", errors.New("publication failed")
}

type settlerFake struct{ calls int }

func (s *settlerFake) Settle(context.Context, executiongate.Evidence, softwarework.Outcome) error {
	s.calls++
	return nil
}

func TestAdapterMapsExactTaskAndResult(t *testing.T) {
	authorizer, runner := &authorizerFake{}, &runnerFake{}
	adapter, err := New(authorizer, runner, locatorFake{})
	if err != nil {
		t.Fatal(err)
	}
	request := taskRequest(t)
	task, err := adapter.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status.State != a2a.TaskStateCompleted || len(task.Artifacts) != 1 ||
		task.Artifacts[0].Parts[0].MediaType != ResultMediaType || authorizer.calls != 1 || runner.calls != 1 {
		t.Fatal("A2A adapter did not produce the exact completed task")
	}
	result, ok := task.Artifacts[0].Parts[0].Data().(resultBinding)
	if !ok || result.Evidence.EscrowFinalizedCheckpoint != 42 || result.Artifact.Digest == "" || result.Artifact.URL == "" {
		t.Fatal("A2A result omitted finalized evidence or content commitments")
	}
}

func TestAdapterDoesNotSettleBeforeArtifactPublication(t *testing.T) {
	settler := &settlerFake{}
	adapter, err := NewSettling(&authorizerFake{}, &runnerFake{}, failingLocator{}, settler)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Execute(context.Background(), taskRequest(t)); err == nil {
		t.Fatal("failed artifact publication was accepted")
	}
	if settler.calls != 0 {
		t.Fatal("escrow settled before artifact publication")
	}
}

func TestAdapterRejectsChangedSourceBeforeAuthorityRead(t *testing.T) {
	authorizer, runner := &authorizerFake{}, &runnerFake{}
	adapter, _ := New(authorizer, runner, locatorFake{})
	request := taskRequest(t)
	request.Message.Parts[1].Content = a2a.Raw([]byte("changed"))
	if _, err := adapter.Execute(context.Background(), request); err == nil {
		t.Fatal("A2A adapter accepted source bytes outside the input commitment")
	}
	if authorizer.calls != 0 || runner.calls != 0 {
		t.Fatal("invalid A2A bytes reached authority or execution")
	}
}

func TestAdapterRequiresFinalizedAuthorizationBeforeExecution(t *testing.T) {
	authorizer, runner := &authorizerFake{fail: true}, &runnerFake{}
	adapter, _ := New(authorizer, runner, locatorFake{})
	if _, err := adapter.Execute(context.Background(), taskRequest(t)); err == nil {
		t.Fatal("unauthorized A2A task accepted")
	}
	if runner.calls != 0 {
		t.Fatal("unauthorized A2A task reached the executor")
	}
}

func TestAdapterMapsExecutionFailureToTerminalTask(t *testing.T) {
	adapter, _ := New(&authorizerFake{}, &runnerFake{fail: true}, locatorFake{})
	task, err := adapter.Execute(context.Background(), taskRequest(t))
	if err != nil || task.Status.State != a2a.TaskStateFailed || len(task.Artifacts) != 0 {
		t.Fatalf("failed execution mapping task=%+v err=%v", task, err)
	}
}

func TestAdapterRejectsConflictingRunnerOutcome(t *testing.T) {
	adapter, _ := New(&authorizerFake{}, &runnerFake{conflicting: true}, locatorFake{})
	if _, err := adapter.Execute(context.Background(), taskRequest(t)); err == nil {
		t.Fatal("conflicting executor outcome became an A2A result")
	}
}

func TestAdapterRejectsInsecureArtifactLocation(t *testing.T) {
	adapter, _ := New(&authorizerFake{}, &runnerFake{}, insecureLocator{})
	if _, err := adapter.Execute(context.Background(), taskRequest(t)); err == nil {
		t.Fatal("insecure artifact URL became an A2A result")
	}
}

func taskRequest(t *testing.T) *a2a.SendMessageRequest {
	t.Helper()
	request, err := NewTaskRequest("message-one", "context-one",
		"0:"+strings.Repeat("cc", 32), "tvm-cell-sha256:"+strings.Repeat("aa", 32),
		"sha256:"+strings.Repeat("bb", 32), []byte("source archive"))
	if err != nil {
		t.Fatal(err)
	}
	return request
}
