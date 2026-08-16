// Package a2aadapter maps the official A2A 1.0 Task model into the single
// finalized-authority TOS Service Protocol software-work lifecycle. It adds no payment or
// execution authority to A2A metadata.
package a2aadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
)

const (
	ExtensionURI    = "urn:tos:service:extension:native-software-work:v1"
	TaskMediaType   = "application/vnd.tos.service.a2a.software-work-task.v1+json"
	SourceMediaType = "application/vnd.tos.service.software-source.v1+tar"
	ResultMediaType = "application/vnd.tos.service.a2a.software-work-result.v1+json"
)

type FinalizedEvidence = executiongate.Evidence

type FinalizedExecutionGate interface {
	// ClaimExecution atomically binds one Quote/escrow to this execution and
	// input before returning finalized authorization evidence.
	ClaimExecution(context.Context, executiongate.Request) (executiongate.Evidence, error)
}

type Runner interface {
	Execute(context.Context, softwarework.Request) (softwarework.Outcome, error)
}

type ArtifactLocator interface {
	URL(artifactstore.Descriptor) (a2a.URL, error)
}

// Settler releases escrow for a completed execution from the finalized Evidence
// and the validated Outcome. Settlement is separate from execution; see the
// agentpacketadapter.Settler contract.
type Settler interface {
	Settle(context.Context, executiongate.Evidence, softwarework.Outcome) error
}

type Adapter struct {
	authorizer FinalizedExecutionGate
	runner     Runner
	locator    ArtifactLocator
	settler    Settler
	now        func() time.Time
}

type taskBinding struct {
	Protocol        string `json:"protocol"`
	EscrowAddress   string `json:"escrow_address"`
	QuoteCommitment string `json:"quote_commitment"`
	ExecutionID     string `json:"execution_id"`
	InputDigest     string `json:"input_digest"`
	SourceDigest    string `json:"source_digest"`
}

type resultBinding struct {
	Protocol        string            `json:"protocol"`
	Evidence        FinalizedEvidence `json:"finalized_evidence"`
	QuoteCommitment string            `json:"quote_commitment"`
	ExecutionID     string            `json:"execution_id"`
	InputDigest     string            `json:"input_digest"`
	ResultDigest    string            `json:"result_digest"`
	Artifact        objectBinding     `json:"artifact"`
	Report          objectBinding     `json:"report"`
	SourceDigest    string            `json:"source_digest"`
	ToolchainDigest string            `json:"toolchain_digest"`
	SandboxDigest   string            `json:"sandbox_digest"`
	ExitCode        int               `json:"exit_code"`
	CompletedAtUnix uint64            `json:"completed_at_unix"`
}

type objectBinding struct {
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	SizeBytes uint64 `json:"size_bytes"`
	URL       string `json:"url"`
}

func New(authorizer FinalizedExecutionGate, runner Runner, locator ArtifactLocator) (*Adapter, error) {
	return NewSettling(authorizer, runner, locator, nil)
}

// NewSettling builds an adapter that releases escrow through settler after each
// completed execution. A nil settler yields the execution-only behaviour of New.
func NewSettling(authorizer FinalizedExecutionGate, runner Runner, locator ArtifactLocator, settler Settler) (*Adapter, error) {
	if authorizer == nil || runner == nil || locator == nil {
		return nil, errors.New("invalid A2A software-work adapter configuration")
	}
	return &Adapter{authorizer: authorizer, runner: runner, locator: locator, settler: settler, now: time.Now}, nil
}

func NewTaskRequest(messageID, contextID, escrowAddress, quoteCommitment, executionID string, sourceArchive []byte) (*a2a.SendMessageRequest, error) {
	if messageID == "" || len(messageID) > 256 || !rawAddressValid(escrowAddress) || !cellDigestValid(quoteCommitment) ||
		!shaDigestValid(executionID) || len(sourceArchive) == 0 {
		return nil, errors.New("invalid A2A software-work task input")
	}
	binding := taskBinding{Protocol: "tos_service_v1", EscrowAddress: escrowAddress, QuoteCommitment: quoteCommitment,
		ExecutionID: executionID, SourceDigest: digest(sourceArchive)}
	binding.InputDigest = inputDigest(binding)
	bindingPart := a2a.NewDataPart(binding)
	bindingPart.MediaType = TaskMediaType
	sourcePart := a2a.NewRawPart(append([]byte(nil), sourceArchive...))
	sourcePart.MediaType, sourcePart.Filename = SourceMediaType, "source.tar"
	message := a2a.NewMessage(a2a.MessageRoleUser, bindingPart, sourcePart)
	message.ID, message.ContextID, message.Extensions = messageID, contextID, []string{ExtensionURI}
	return &a2a.SendMessageRequest{Message: message}, nil
}

// Execute handles the narrow synchronous SendMessage mapping. Transport-level
// validation or authorization failure returns an error before a Task exists;
// an execution failure returns a terminal failed Task.
func (a *Adapter) Execute(ctx context.Context, request *a2a.SendMessageRequest) (*a2a.Task, error) {
	if a == nil || ctx == nil {
		return nil, errors.New("invalid A2A software-work request")
	}
	work, claim, message, err := decodeRequest(request)
	if err != nil {
		return nil, err
	}
	evidence, err := a.authorizer.ClaimExecution(ctx, claim)
	if err != nil {
		return nil, errors.New("A2A execution lacks finalized TOS Service Protocol authorization")
	}
	if err := validateEvidence(evidence, claim); err != nil {
		return nil, err
	}
	task := submittedTask(message, work, a.now().UTC())
	outcome, err := a.runner.Execute(ctx, work)
	completed := a.now().UTC()
	if err != nil {
		task.Status = a2a.TaskStatus{State: a2a.TaskStateFailed, Timestamp: &completed}
		return task, nil
	}
	if err := validateOutcome(outcome, work); err != nil {
		return nil, errors.New("software-work runner returned a conflicting outcome")
	}
	artifactURL, err := a.locator.URL(outcome.Artifact)
	if err != nil || !artifactURLValid(artifactURL) {
		return nil, errors.New("invalid A2A artifact retrieval URL")
	}
	reportURL, err := a.locator.URL(outcome.Report)
	if err != nil || !artifactURLValid(reportURL) {
		return nil, errors.New("invalid A2A report retrieval URL")
	}
	if a.settler != nil {
		if err := a.settler.Settle(ctx, evidence, outcome); err != nil {
			return nil, errors.New("escrow settlement failed for a completed execution")
		}
	}
	result := resultBinding{Protocol: "tos_service_v1", Evidence: evidence,
		QuoteCommitment: outcome.QuoteCommitment, ExecutionID: outcome.ExecutionID,
		InputDigest: outcome.InputDigest, ResultDigest: outcome.ResultDigest, SourceDigest: outcome.SourceDigest,
		ToolchainDigest: outcome.ToolchainDigest, SandboxDigest: outcome.SandboxDigest,
		ExitCode: outcome.ExitCode, CompletedAtUnix: outcome.CompletedAtUnix,
		Artifact: objectBinding{Digest: outcome.Artifact.Digest, MediaType: outcome.Artifact.MediaType,
			SizeBytes: outcome.Artifact.SizeBytes, URL: string(artifactURL)},
		Report: objectBinding{Digest: outcome.Report.Digest, MediaType: outcome.Report.MediaType,
			SizeBytes: outcome.Report.SizeBytes, URL: string(reportURL)}}
	part := a2a.NewDataPart(result)
	part.MediaType = ResultMediaType
	artifactID := sha256.Sum256([]byte(work.ExecutionID + "\x00" + outcome.ResultDigest))
	task.Artifacts = []*a2a.Artifact{{ID: a2a.ArtifactID("tos-service-" + hex.EncodeToString(artifactID[:])),
		Name: "TOS Service Protocol software-work result", Extensions: []string{ExtensionURI}, Parts: a2a.ContentParts{part}}}
	task.Status = a2a.TaskStatus{State: a2a.TaskStateCompleted, Timestamp: &completed}
	return task, nil
}

func decodeRequest(request *a2a.SendMessageRequest) (softwarework.Request, executiongate.Request, *a2a.Message, error) {
	if request == nil || request.Message == nil || request.Message.Role != a2a.MessageRoleUser ||
		request.Message.ID == "" || len(request.Message.ID) > 256 || len(request.Message.Parts) != 2 ||
		len(request.Message.Extensions) != 1 || request.Message.Extensions[0] != ExtensionURI {
		return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A software-work message envelope")
	}
	var bindingPart, sourcePart *a2a.Part
	for _, part := range request.Message.Parts {
		if part == nil || len(part.Metadata) != 0 {
			return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A software-work part")
		}
		switch part.MediaType {
		case TaskMediaType:
			if bindingPart != nil || part.Data() == nil {
				return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A task binding part")
			}
			bindingPart = part
		case SourceMediaType:
			if sourcePart != nil || len(part.Raw()) == 0 || part.Filename != "source.tar" {
				return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A source part")
			}
			sourcePart = part
		default:
			return softwarework.Request{}, executiongate.Request{}, nil, errors.New("unsupported A2A software-work media type")
		}
	}
	if bindingPart == nil || sourcePart == nil {
		return softwarework.Request{}, executiongate.Request{}, nil, errors.New("incomplete A2A software-work message")
	}
	raw, err := json.Marshal(bindingPart.Data())
	if err != nil || len(raw) > 4096 {
		return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A task binding data")
	}
	var binding taskBinding
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&binding); err != nil {
		return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A task binding data")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) || binding.Protocol != "tos_service_v1" {
		return softwarework.Request{}, executiongate.Request{}, nil, errors.New("invalid A2A task binding protocol")
	}
	source := append([]byte(nil), sourcePart.Raw()...)
	work := softwarework.Request{QuoteCommitment: binding.QuoteCommitment, ExecutionID: binding.ExecutionID,
		InputDigest: binding.InputDigest, SourceDigest: binding.SourceDigest, SourceArchive: source}
	if !rawAddressValid(binding.EscrowAddress) || !cellDigestValid(work.QuoteCommitment) || !shaDigestValid(work.ExecutionID) ||
		!shaDigestValid(work.InputDigest) || !shaDigestValid(work.SourceDigest) ||
		digest(source) != work.SourceDigest || inputDigest(binding) != work.InputDigest {
		return softwarework.Request{}, executiongate.Request{}, nil, errors.New("A2A task bytes do not match their commitments")
	}
	claim := executiongate.Request{EscrowAddress: binding.EscrowAddress, QuoteCommitment: work.QuoteCommitment,
		ExecutionID: work.ExecutionID, InputDigest: work.InputDigest, SourceDigest: work.SourceDigest}
	return work, claim, request.Message, nil
}

func inputDigest(binding taskBinding) string {
	raw, _ := json.Marshal(struct {
		Protocol, EscrowAddress, QuoteCommitment, ExecutionID, SourceDigest string
	}{binding.Protocol, binding.EscrowAddress, binding.QuoteCommitment, binding.ExecutionID, binding.SourceDigest})
	return digest(append([]byte("tos.service.a2a.software-work-input.v1\x00"), raw...))
}

func submittedTask(message *a2a.Message, work softwarework.Request, now time.Time) *a2a.Task {
	id := sha256.Sum256([]byte("tos.service.a2a.task.v1\x00" + work.QuoteCommitment + "\x00" + work.ExecutionID))
	contextID := message.ContextID
	if contextID == "" {
		contextID = "tos-service-" + hex.EncodeToString(id[:])
	}
	return &a2a.Task{ID: a2a.TaskID("tos-service-" + hex.EncodeToString(id[:])), ContextID: contextID,
		History: []*a2a.Message{message}, Metadata: map[string]any{"tos_service_extension": ExtensionURI},
		Status: a2a.TaskStatus{State: a2a.TaskStateWorking, Timestamp: &now}}
}

func validateEvidence(value FinalizedEvidence, request executiongate.Request) error {
	if value.NetworkID == "" || len(value.NetworkID) > 64 || strings.TrimSpace(value.NetworkID) != value.NetworkID ||
		!agentIDValid(value.ProviderAgentID) || !capabilityIDValid(value.CapabilityID) ||
		value.CapabilityVersion == "" || len(value.CapabilityVersion) > 64 ||
		!shaDigestValid(value.ManifestDigest) || value.QuoteCommitment != request.QuoteCommitment ||
		value.EscrowAddress != request.EscrowAddress || !rawAddressValid(value.ProviderAddress) ||
		!cellDigestValid(value.EscrowCodeHash) || !cellDigestValid(value.RegistryCodeHash) ||
		!shaDigestValid(value.EscrowTransactionHash) || !shaDigestValid(value.AgentTransactionHash) ||
		!shaDigestValid(value.CapabilityTransactionHash) ||
		value.EscrowFinalizedCheckpoint == 0 || value.AgentFinalizedCheckpoint == 0 || value.CapabilityFinalizedCheckpoint == 0 {
		return errors.New("invalid finalized TOS Service Protocol execution evidence")
	}
	return nil
}

func agentIDValid(value string) bool {
	return len(value) == 70 && strings.HasPrefix(value, "agent_") && digestSuffixValid("sha256:"+value[6:], "sha256:")
}

func validateOutcome(value softwarework.Outcome, request softwarework.Request) error {
	if value.QuoteCommitment != request.QuoteCommitment || value.ExecutionID != request.ExecutionID ||
		value.InputDigest != request.InputDigest || value.SourceDigest != request.SourceDigest ||
		!shaDigestValid(value.ResultDigest) || !shaDigestValid(value.ToolchainDigest) ||
		!shaDigestValid(value.SandboxDigest) || value.ExitCode != 0 || value.CompletedAtUnix == 0 ||
		!shaDigestValid(value.Artifact.Digest) || value.Artifact.MediaType != softwarework.ArtifactMediaType || value.Artifact.SizeBytes == 0 ||
		!shaDigestValid(value.Report.Digest) || value.Report.MediaType != softwarework.ReportMediaType || value.Report.SizeBytes == 0 {
		return errors.New("invalid software-work outcome")
	}
	return nil
}

func digest(value []byte) string {
	hash := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func shaDigestValid(value string) bool {
	return digestSuffixValid(value, "sha256:")
}

func cellDigestValid(value string) bool {
	return digestSuffixValid(value, "tvm-cell-sha256:")
}

func digestSuffixValid(value, prefix string) bool {
	if len(value) != len(prefix)+64 || !strings.HasPrefix(value, prefix) || value != strings.ToLower(value) {
		return false
	}
	raw, err := hex.DecodeString(value[len(prefix):])
	return err == nil && !bytes.Equal(raw, make([]byte, 32))
}

func capabilityIDValid(value string) bool {
	return len(value) == 68 && strings.HasPrefix(value, "cap_") && digestSuffixValid("sha256:"+value[4:], "sha256:")
}

func rawAddressValid(value string) bool {
	workchain, account, found := strings.Cut(value, ":")
	parsed, err := strconv.ParseInt(workchain, 10, 32)
	if !found || err != nil || (parsed != 0 && parsed != -1) || len(account) != 64 || account != strings.ToLower(account) {
		return false
	}
	raw, err := hex.DecodeString(account)
	return err == nil && !bytes.Equal(raw, make([]byte, 32))
}

func artifactURLValid(value a2a.URL) bool {
	parsed, err := url.Parse(string(value))
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil &&
		parsed.Fragment == "" && len(value) <= 2048
}
