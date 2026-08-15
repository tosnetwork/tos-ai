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
)

const ToolName = "atos_native_software_work"

type Input struct {
	QuoteCommitment     string `json:"quote_commitment" jsonschema:"exact finalized Accepted Quote commitment"`
	ExecutionID         string `json:"execution_id" jsonschema:"nonzero sha256 execution identity"`
	InputDigest         string `json:"input_digest" jsonschema:"ATOS MCP input commitment"`
	SourceDigest        string `json:"source_digest" jsonschema:"sha256 of exact source archive bytes"`
	SourceArchiveBase64 string `json:"source_archive_base64" jsonschema:"canonical standard base64 uncompressed POSIX tar"`
}

type Evidence struct {
	NetworkID           string `json:"network_id"`
	CapabilityID        string `json:"capability_id"`
	CapabilityVersion   string `json:"capability_version"`
	ManifestDigest      string `json:"manifest_digest"`
	QuoteCommitment     string `json:"quote_commitment"`
	EscrowAddress       string `json:"escrow_address"`
	FinalizedCheckpoint uint64 `json:"finalized_checkpoint"`
}

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
	ClaimExecution(context.Context, softwarework.Request) (Evidence, error)
}
type Runner interface {
	Execute(context.Context, softwarework.Request) (softwarework.Outcome, error)
}
type Locator interface {
	URL(artifactstore.Descriptor) (string, error)
}

type Adapter struct {
	gate    ExecutionGate
	runner  Runner
	locator Locator
}

func New(gate ExecutionGate, runner Runner, locator Locator) (*Adapter, error) {
	if gate == nil || runner == nil || locator == nil {
		return nil, errors.New("invalid MCP software-work adapter configuration")
	}
	return &Adapter{gate: gate, runner: runner, locator: locator}, nil
}

func Tool() *mcp.Tool {
	return &mcp.Tool{Name: ToolName, Title: "Purchase-bound Native software work",
		Description: "Executes one already-funded ATOS Native software-work Quote. MCP metadata never authorizes payment or execution."}
}

func (a *Adapter) AddTo(server *mcp.Server) error {
	if a == nil || server == nil {
		return errors.New("invalid MCP server registration")
	}
	mcp.AddTool(server, Tool(), a.Call)
	return nil
}

func PrepareInput(quote, execution string, source []byte) (Input, error) {
	if !cellDigest(quote) || !shaDigest(execution) || len(source) == 0 {
		return Input{}, errors.New("invalid MCP software-work input")
	}
	value := Input{QuoteCommitment: quote, ExecutionID: execution, SourceDigest: digest(source),
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
		!cellDigest(input.QuoteCommitment) || !shaDigest(input.ExecutionID) || !shaDigest(input.InputDigest) ||
		!shaDigest(input.SourceDigest) || digest(source) != input.SourceDigest || inputDigest(input) != input.InputDigest {
		return nil, Output{}, errors.New("MCP tool arguments do not match their commitments")
	}
	work := softwarework.Request{QuoteCommitment: input.QuoteCommitment, ExecutionID: input.ExecutionID,
		InputDigest: input.InputDigest, SourceDigest: input.SourceDigest, SourceArchive: source}
	evidence, err := a.gate.ClaimExecution(ctx, work)
	if err != nil || !validEvidence(evidence, work) {
		return nil, Output{}, errors.New("MCP execution lacks a unique finalized ATOS authorization")
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
	return nil, Output{Protocol: "atos_native_v1", Evidence: evidence, QuoteCommitment: outcome.QuoteCommitment,
		ExecutionID: outcome.ExecutionID, InputDigest: outcome.InputDigest, ResultDigest: outcome.ResultDigest,
		Artifact:     Object{outcome.Artifact.Digest, outcome.Artifact.MediaType, outcome.Artifact.SizeBytes, artifactURL},
		Report:       Object{outcome.Report.Digest, outcome.Report.MediaType, outcome.Report.SizeBytes, reportURL},
		SourceDigest: outcome.SourceDigest, ToolchainDigest: outcome.ToolchainDigest, SandboxDigest: outcome.SandboxDigest,
		CompletedAtUnix: outcome.CompletedAtUnix}, nil
}

func inputDigest(v Input) string {
	raw, _ := json.Marshal(struct{ Protocol, QuoteCommitment, ExecutionID, SourceDigest string }{
		"atos_native_v1", v.QuoteCommitment, v.ExecutionID, v.SourceDigest})
	return digest(append([]byte("atos.mcp.software-work-input.v1\x00"), raw...))
}
func validEvidence(v Evidence, r softwarework.Request) bool {
	return v.NetworkID != "" && len(v.CapabilityID) == 68 && strings.HasPrefix(v.CapabilityID, "cap_") && v.CapabilityVersion != "" && shaDigest(v.ManifestDigest) && v.QuoteCommitment == r.QuoteCommitment && v.EscrowAddress != "" && v.FinalizedCheckpoint > 0
}
func validOutcome(v softwarework.Outcome, r softwarework.Request) bool {
	return v.QuoteCommitment == r.QuoteCommitment && v.ExecutionID == r.ExecutionID && v.InputDigest == r.InputDigest && v.SourceDigest == r.SourceDigest && shaDigest(v.ResultDigest) && shaDigest(v.ToolchainDigest) && shaDigest(v.SandboxDigest) && v.ExitCode == 0 && v.CompletedAtUnix > 0 && shaDigest(v.Artifact.Digest) && v.Artifact.MediaType == softwarework.ArtifactMediaType && v.Artifact.SizeBytes > 0 && shaDigest(v.Report.Digest) && v.Report.MediaType == softwarework.ReportMediaType && v.Report.SizeBytes > 0
}
func digest(v []byte) string   { h := sha256.Sum256(v); return "sha256:" + hex.EncodeToString(h[:]) }
func shaDigest(v string) bool  { return validDigest(v, "sha256:") }
func cellDigest(v string) bool { return validDigest(v, "tvm-cell-sha256:") }
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
