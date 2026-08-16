// Package mcpadapter exposes the single Native software-work operation through
// official MCP types without granting MCP any identity or payment authority.
package mcpadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
)

const ToolName = "tos_service_software_work"

type Input struct {
	EscrowAddress       string `json:"escrow_address" jsonschema:"exact funded escrow raw address"`
	QuoteCommitment     string `json:"quote_commitment" jsonschema:"exact finalized Accepted Quote commitment"`
	ExecutionID         string `json:"execution_id" jsonschema:"nonzero sha256 execution identity"`
	InputDigest         string `json:"input_digest" jsonschema:"TOS Service Protocol MCP input commitment"`
	SourceDigest        string `json:"source_digest" jsonschema:"sha256 of exact source archive bytes"`
	SourceArchiveBase64 string `json:"source_archive_base64" jsonschema:"canonical standard base64 uncompressed POSIX tar"`
}

type Evidence = executiongate.Evidence

type Object struct {
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	SizeBytes uint64 `json:"size_bytes"`
	URL       string `json:"url"`
}

type Output struct {
	Protocol        string   `json:"protocol"`
	Evidence        Evidence `json:"finalized_evidence"`
	QuoteCommitment string   `json:"quote_commitment"`
	ExecutionID     string   `json:"execution_id"`
	InputDigest     string   `json:"input_digest"`
	ResultDigest    string   `json:"result_digest"`
	Artifact        Object   `json:"artifact"`
	Report          Object   `json:"report"`
	SourceDigest    string   `json:"source_digest"`
	ToolchainDigest string   `json:"toolchain_digest"`
	SandboxDigest   string   `json:"sandbox_digest"`
	CompletedAtUnix uint64   `json:"completed_at_unix"`
}

type ExecutionGate interface {
	// ClaimExecution atomically binds the finalized Quote/escrow to exactly one
	// execution ID and input digest before provider work can begin.
	ClaimExecution(context.Context, executiongate.Request) (executiongate.Evidence, error)
}
type Runner interface {
	Execute(context.Context, softwarework.Request) (softwarework.Outcome, error)
}
type Locator interface {
	URL(artifactstore.Descriptor) (string, error)
}

// Settler releases escrow for a completed execution from the finalized Evidence
// and the validated Outcome. Settlement is separate from execution; see the
// agentpacketadapter.Settler contract.
type Settler interface {
	Settle(context.Context, executiongate.Evidence, softwarework.Outcome) error
}

type Adapter struct {
	gate    ExecutionGate
	runner  Runner
	locator Locator
	settler Settler
}

func New(gate ExecutionGate, runner Runner, locator Locator) (*Adapter, error) {
	return NewSettling(gate, runner, locator, nil)
}

// NewSettling builds an adapter that releases escrow through settler after each
// completed execution. A nil settler yields the execution-only behaviour of New.
func NewSettling(gate ExecutionGate, runner Runner, locator Locator, settler Settler) (*Adapter, error) {
	if gate == nil || runner == nil || locator == nil {
		return nil, errors.New("invalid MCP software-work adapter configuration")
	}
	return &Adapter{gate: gate, runner: runner, locator: locator, settler: settler}, nil
}

func Tool() *mcp.Tool {
	return &mcp.Tool{Name: ToolName, Title: "Purchase-bound Native software work",
		Description: "Executes one already-funded TOS Service Protocol software-work Quote. MCP metadata never authorizes payment or execution."}
}

func (a *Adapter) AddTo(server *mcp.Server) error {
	if a == nil || server == nil {
		return errors.New("invalid MCP server registration")
	}
	mcp.AddTool(server, Tool(), a.Call)
	return nil
}

func PrepareInput(escrow, quote, execution string, source []byte) (Input, error) {
	if !rawAddress(escrow) || !cellDigest(quote) || !shaDigest(execution) || len(source) == 0 {
		return Input{}, errors.New("invalid MCP software-work input")
	}
	value := Input{EscrowAddress: escrow, QuoteCommitment: quote, ExecutionID: execution, SourceDigest: digest(source),
		SourceArchiveBase64: base64.StdEncoding.EncodeToString(source)}
	value.InputDigest = inputDigest(value)
	return value, nil
}

func (a *Adapter) Call(ctx context.Context, _ *mcp.CallToolRequest, input Input) (*mcp.CallToolResult, Output, error) {
	if a == nil || ctx == nil {
		return nil, Output{}, errors.New("invalid MCP software-work call")
	}
	source, err := base64.StdEncoding.DecodeString(input.SourceArchiveBase64)
	if err != nil || base64.StdEncoding.EncodeToString(source) != input.SourceArchiveBase64 || len(source) == 0 ||
		!rawAddress(input.EscrowAddress) || !cellDigest(input.QuoteCommitment) || !shaDigest(input.ExecutionID) || !shaDigest(input.InputDigest) ||
		!shaDigest(input.SourceDigest) || digest(source) != input.SourceDigest || inputDigest(input) != input.InputDigest {
		return nil, Output{}, errors.New("MCP tool arguments do not match their commitments")
	}
	work := softwarework.Request{QuoteCommitment: input.QuoteCommitment, ExecutionID: input.ExecutionID,
		InputDigest: input.InputDigest, SourceDigest: input.SourceDigest, SourceArchive: source}
	claim := executiongate.Request{EscrowAddress: input.EscrowAddress, QuoteCommitment: work.QuoteCommitment,
		ExecutionID: work.ExecutionID, InputDigest: work.InputDigest, SourceDigest: work.SourceDigest}
	evidence, err := a.gate.ClaimExecution(ctx, claim)
	if err != nil || !validEvidence(evidence, claim) {
		return nil, Output{}, errors.New("MCP execution lacks a unique finalized TOS Service Protocol authorization")
	}
	outcome, err := a.runner.Execute(ctx, work)
	if err != nil {
		return nil, Output{}, errors.New("bounded software-work execution failed")
	}
	if !validOutcome(outcome, work) {
		return nil, Output{}, errors.New("software-work runner returned a conflicting outcome")
	}
	artifactURL, err := a.locator.URL(outcome.Artifact)
	if err != nil || !httpsURL(artifactURL) {
		return nil, Output{}, errors.New("invalid artifact retrieval URL")
	}
	reportURL, err := a.locator.URL(outcome.Report)
	if err != nil || !httpsURL(reportURL) {
		return nil, Output{}, errors.New("invalid report retrieval URL")
	}
	if a.settler != nil {
		if err := a.settler.Settle(ctx, evidence, outcome); err != nil {
			return nil, Output{}, errors.New("escrow settlement failed for a completed execution")
		}
	}
	return nil, Output{Protocol: "tos_service_v1", Evidence: evidence, QuoteCommitment: outcome.QuoteCommitment,
		ExecutionID: outcome.ExecutionID, InputDigest: outcome.InputDigest, ResultDigest: outcome.ResultDigest,
		Artifact:     Object{outcome.Artifact.Digest, outcome.Artifact.MediaType, outcome.Artifact.SizeBytes, artifactURL},
		Report:       Object{outcome.Report.Digest, outcome.Report.MediaType, outcome.Report.SizeBytes, reportURL},
		SourceDigest: outcome.SourceDigest, ToolchainDigest: outcome.ToolchainDigest, SandboxDigest: outcome.SandboxDigest,
		CompletedAtUnix: outcome.CompletedAtUnix}, nil
}

func inputDigest(v Input) string {
	raw, _ := json.Marshal(struct{ Protocol, EscrowAddress, QuoteCommitment, ExecutionID, SourceDigest string }{
		"tos_service_v1", v.EscrowAddress, v.QuoteCommitment, v.ExecutionID, v.SourceDigest})
	return digest(append([]byte("tos.service.mcp.software-work-input.v1\x00"), raw...))
}
func validEvidence(v Evidence, r executiongate.Request) bool {
	return v.NetworkID != "" && agentID(v.ProviderAgentID) && capabilityID(v.CapabilityID) &&
		v.CapabilityVersion != "" && shaDigest(v.ManifestDigest) && v.QuoteCommitment == r.QuoteCommitment &&
		v.EscrowAddress == r.EscrowAddress && rawAddress(v.ProviderAddress) && v.EscrowFinalizedCheckpoint > 0 &&
		cellDigest(v.EscrowCodeHash) && cellDigest(v.RegistryCodeHash) && shaDigest(v.EscrowTransactionHash) &&
		shaDigest(v.AgentTransactionHash) && shaDigest(v.CapabilityTransactionHash) &&
		v.AgentFinalizedCheckpoint > 0 && v.CapabilityFinalizedCheckpoint > 0
}
func validOutcome(v softwarework.Outcome, r softwarework.Request) bool {
	return v.QuoteCommitment == r.QuoteCommitment && v.ExecutionID == r.ExecutionID && v.InputDigest == r.InputDigest && v.SourceDigest == r.SourceDigest && shaDigest(v.ResultDigest) && shaDigest(v.ToolchainDigest) && shaDigest(v.SandboxDigest) && v.ExitCode == 0 && v.CompletedAtUnix > 0 && shaDigest(v.Artifact.Digest) && v.Artifact.MediaType == softwarework.ArtifactMediaType && v.Artifact.SizeBytes > 0 && shaDigest(v.Report.Digest) && v.Report.MediaType == softwarework.ReportMediaType && v.Report.SizeBytes > 0
}
func digest(v []byte) string   { h := sha256.Sum256(v); return "sha256:" + hex.EncodeToString(h[:]) }
func shaDigest(v string) bool  { return validDigest(v, "sha256:") }
func cellDigest(v string) bool { return validDigest(v, "tvm-cell-sha256:") }
func rawAddress(v string) bool {
	wc, id, ok := strings.Cut(v, ":")
	return ok && wc == "0" && len(id) == 64 && validDigest("sha256:"+id, "sha256:")
}
func agentID(v string) bool {
	return len(v) == 70 && strings.HasPrefix(v, "agent_") && shaDigest("sha256:"+v[6:])
}
func capabilityID(v string) bool {
	return len(v) == 68 && strings.HasPrefix(v, "cap_") && shaDigest("sha256:"+v[4:])
}
func validDigest(v, p string) bool {
	if len(v) != len(p)+64 || !strings.HasPrefix(v, p) || v != strings.ToLower(v) {
		return false
	}
	raw, e := hex.DecodeString(v[len(p):])
	return e == nil && !bytes.Equal(raw, make([]byte, 32))
}
func httpsURL(v string) bool {
	u, e := url.Parse(v)
	return e == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && len(v) <= 2048
}
