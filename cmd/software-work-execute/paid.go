package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/railgate"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
	"github.com/tosnetwork/tos-service-protocol/pkg/quoteexchange"
	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
	"google.golang.org/protobuf/proto"
)

// Paid-mode exit codes. Every paid run also prints one JSON status object
// on stdout whose "status" names the same outcome.
const (
	// exitRefused (1, exitError): the configuration or input is invalid, or
	// a dependency (finalized resolver, executor, slot store) is
	// unavailable; nothing was admitted or run and the run may be retried.
	exitRefused = exitError
	// exitUsage (2): the command line does not parse.
	exitUsage = 2
	// exitAlreadyTaken: the request already has an execution slot; it is
	// not executed again.
	exitAlreadyTaken = 3
	// exitNotPendingNow: admitted, but the fresh pre-execution check found
	// the request no longer pending with room to finish; nothing ran.
	exitNotPendingNow = 4
	// exitAmbiguous: an execution may have run without a recorded outcome
	// (a crash or an unrecordable result). Never re-run or respond.
	exitAmbiguous = 5
	// exitNotAdmitted: finalized state does not prove the claim; no slot
	// was created and nothing ran.
	exitNotAdmitted = 6
	// exitExecutionFailed: the execution ran and did not succeed; the slot
	// is failed and must never be attested.
	exitExecutionFailed = 7
	// exitRefusedBeforeExecution: admitted, but the verified request does
	// not fit this executor (actor, request, manifest, toolchain, price or
	// retention); the slot is abandoned and nothing ran.
	exitRefusedBeforeExecution = 8
)

const (
	railConfigSchema   = "tos.ai.railgate-config.v1"
	maxRailConfigBytes = 64 << 10
	maxManifestBytes   = 1 << 20
	maxRegistryBOC     = 1 << 20
	maxQuotePackage    = 2 << 20
)

var (
	sha256TextPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	hex64Pattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	rawAddressPattern = regexp.MustCompile(`^0:[0-9a-f]{64}$`)
)

// railConfig binds the paid mode to one provider offer and the finalized
// resolver it reads. Unknown fields are refused.
type railConfig struct {
	Schema  string `json:"schema"`
	Network struct {
		NetworkID       string `json:"network_id"`
		GenesisRootHash string `json:"genesis_root_hash"`
		GenesisFileHash string `json:"genesis_file_hash"`
	} `json:"network"`
	RPCEndpoints         []string `json:"rpc_endpoints"`
	Quorum               int      `json:"quorum"`
	RegistryCodeBOCFile  string   `json:"registry_code_boc_file"`
	RegistryCodeHash     string   `json:"registry_code_hash"`
	ActorCodeHash        string   `json:"actor_code_hash"`
	ProviderAgentID      string   `json:"provider_agent_id"`
	CapabilityID         string   `json:"capability_id"`
	Version              string   `json:"version"`
	ManifestFile         string   `json:"manifest_file"`
	RespondBudgetSeconds uint32   `json:"respond_budget_seconds"`
}

func loadRailConfig(path string) (railConfig, error) {
	raw, err := readPrivateFile(path, maxRailConfigBytes, "rail config")
	if err != nil {
		return railConfig{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config railConfig
	if err := decoder.Decode(&config); err != nil {
		return railConfig{}, fmt.Errorf("rail config does not decode: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return railConfig{}, errors.New("rail config has trailing data")
	}
	switch {
	case config.Schema != railConfigSchema:
		return railConfig{}, fmt.Errorf("rail config schema must be %s", railConfigSchema)
	case config.Network.NetworkID == "" || config.Network.GenesisRootHash == "" || config.Network.GenesisFileHash == "":
		return railConfig{}, errors.New("rail config needs the complete network domain")
	case len(config.RPCEndpoints) == 0:
		return railConfig{}, errors.New("rail config needs finalized RPC endpoints")
	case config.Quorum < 0 || config.Quorum > len(config.RPCEndpoints):
		return railConfig{}, errors.New("rail config quorum is outside the endpoint count")
	case !absoluteClean(config.RegistryCodeBOCFile) || !absoluteClean(config.ManifestFile):
		return railConfig{}, errors.New("rail config file paths must be absolute and canonical")
	case config.RegistryCodeHash == "" || config.ActorCodeHash == "":
		return railConfig{}, errors.New("rail config needs the pinned registry and actor code hashes")
	case config.ProviderAgentID == "" || config.CapabilityID == "" || config.Version == "":
		return railConfig{}, errors.New("rail config needs the provider Agent, Capability and version")
	case config.RespondBudgetSeconds == 0:
		return railConfig{}, errors.New("rail config needs an explicit respond budget")
	}
	return config, nil
}

func absoluteClean(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

// paidInputs are the per-request flags of the paid mode. Every one is
// required: a missing one refuses the run before anything is admitted.
type paidInputs struct {
	railConfig  string
	socket      string
	fifo        string
	stateDir    string
	source      string
	quote       string
	buyer       string
	actor       string
	requestID   uint64
	callLT      uint64
	callHash    string
	executionID string
	inputDigest string
}

func parsePaidInputs(args []string, stderr io.Writer) (paidInputs, error) {
	flags := flag.NewFlagSet("software-work-execute paid", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var in paidInputs
	var requestID, callLT string
	flags.StringVar(&in.railConfig, "rail-config", "", "private rail config JSON (network, resolver endpoints, provider offer)")
	flags.StringVar(&in.socket, "containerd-socket", "", "private containerd socket")
	flags.StringVar(&in.fifo, "fifo-dir", "", "private FIFO directory")
	flags.StringVar(&in.stateDir, "state-dir", "", "persistent private paid state directory (slots, checkpoints, journal, artifacts)")
	flags.StringVar(&in.source, "source", "", "deterministic USTAR source archive")
	flags.StringVar(&in.quote, "quote-package", "", "binary QuoteProposalPackageV1 the buyer accepted")
	flags.StringVar(&in.buyer, "buyer-wallet", "", "raw address of the paying wallet (0:<hex>)")
	flags.StringVar(&in.actor, "actor", "", "raw address of the Service Actor (0:<hex>)")
	flags.StringVar(&requestID, "request-id", "", "request id the actor assigned to the call")
	flags.StringVar(&callLT, "call-lt", "", "logical time of the finalized call transaction")
	flags.StringVar(&in.callHash, "call-hash", "", "hash of the finalized call transaction (64 lowercase hex)")
	flags.StringVar(&in.executionID, "execution-id", "", "execution id committed by the request (sha256:<hex>)")
	flags.StringVar(&in.inputDigest, "input-digest", "", "input digest committed by the request (sha256:<hex>)")
	if err := flags.Parse(args); err != nil {
		return paidInputs{}, usageError{err}
	}
	if flags.NArg() != 0 {
		return paidInputs{}, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	for name, value := range map[string]string{"rail-config": in.railConfig, "containerd-socket": in.socket,
		"fifo-dir": in.fifo, "state-dir": in.stateDir, "source": in.source, "quote-package": in.quote,
		"buyer-wallet": in.buyer, "actor": in.actor, "request-id": requestID, "call-lt": callLT,
		"call-hash": in.callHash, "execution-id": in.executionID, "input-digest": in.inputDigest} {
		if value == "" {
			return paidInputs{}, fmt.Errorf("--%s is required in paid mode", name)
		}
	}
	var err error
	if in.requestID, err = parseCanonicalUint(requestID); err != nil {
		return paidInputs{}, fmt.Errorf("--request-id: %w", err)
	}
	if in.callLT, err = parseCanonicalUint(callLT); err != nil || in.callLT == 0 {
		return paidInputs{}, errors.New("--call-lt must be a positive canonical integer")
	}
	switch {
	case !rawAddressPattern.MatchString(in.buyer) || !rawAddressPattern.MatchString(in.actor):
		return paidInputs{}, errors.New("--buyer-wallet and --actor must be raw addresses 0:<64 lowercase hex>")
	case !hex64Pattern.MatchString(in.callHash):
		return paidInputs{}, errors.New("--call-hash must be 64 lowercase hex")
	case !sha256TextPattern.MatchString(in.executionID) || !sha256TextPattern.MatchString(in.inputDigest):
		return paidInputs{}, errors.New("--execution-id and --input-digest must be sha256:<64 lowercase hex>")
	}
	return in, nil
}

// parseCanonicalUint accepts only the canonical decimal form, so one request
// cannot be named by two spellings.
func parseCanonicalUint(value string) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, errors.New("must be a canonical unsigned integer")
	}
	var result uint64
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, errors.New("must be a canonical unsigned integer")
		}
		if result > (math.MaxUint64-uint64(digit-'0'))/10 {
			return 0, errors.New("overflows 64 bits")
		}
		result = result*10 + uint64(digit-'0')
	}
	return result, nil
}

func digest32(text string) ([32]byte, error) {
	var value [32]byte
	if !sha256TextPattern.MatchString(text) {
		return value, errors.New("invalid sha256 digest")
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(text, "sha256:"))
	if err != nil || len(raw) != 32 {
		return value, errors.New("invalid sha256 digest")
	}
	copy(value[:], raw)
	if value == ([32]byte{}) {
		return value, errors.New("zero sha256 digest")
	}
	return value, nil
}

// paidReport is the machine-readable result of a paid run.
type paidReport struct {
	Status           string                `json:"status"`
	ExitCode         int                   `json:"exit_code"`
	Actor            string                `json:"actor,omitempty"`
	RequestID        uint64                `json:"request_id"`
	QuoteCommitment  string                `json:"quote_commitment,omitempty"`
	SlotState        string                `json:"slot_state,omitempty"`
	ResponseHash     string                `json:"response_hash,omitempty"`
	ReceiptBOCBase64 string                `json:"receipt_boc_base64,omitempty"`
	RetainUntilUnix  int64                 `json:"retain_until_unix,omitempty"`
	RespondCheck     string                `json:"respond_check,omitempty"`
	Outcome          *softwarework.Outcome `json:"outcome,omitempty"`
	Error            string                `json:"error,omitempty"`
}

const statusRefused = "refused"

func exitCodeFor(status railgate.Status) int {
	switch status {
	case railgate.StatusCompleted:
		return exitOK
	case railgate.StatusAlreadyTaken:
		return exitAlreadyTaken
	case railgate.StatusNotPendingNow:
		return exitNotPendingNow
	case railgate.StatusAmbiguous:
		return exitAmbiguous
	case railgate.StatusNotAdmitted:
		return exitNotAdmitted
	case railgate.StatusExecutionFailed:
		return exitExecutionFailed
	case railgate.StatusRefusedBeforeExecution:
		return exitRefusedBeforeExecution
	case railgate.StatusUnavailable:
		return exitRefused
	default:
		// An unknown status is never reported as success or as safe to
		// retry: treat it as ambiguous.
		return exitAmbiguous
	}
}

// paidJob is one paid request bound to an open gate and executor. The
// runner is reachable only through ExecuteOnce's callback, so nothing runs
// without both finalized verifications and a durably started slot.
type paidJob struct {
	gate           *railgate.Gate
	claim          railgate.Claim
	actor          string
	manifestDigest string
	contract       softwarework.Contract
	archive        []byte
	sourceDigest   string
	run            func(context.Context, softwarework.Request) (softwarework.Outcome, error)
	store          *artifactstore.Store

	outcome     *softwarework.Outcome
	receipt     []byte
	retainUntil int64
}

// check refuses a verified request this executor cannot serve. It runs after
// admission and before the slot is started, so a refusal abandons the slot
// with nothing run.
func (p *paidJob) check(verified *railgate.Verified) error {
	if verified == nil || verified.Price == nil || verified.Price.Sign() <= 0 {
		return errors.New("paid: the verified request has no price")
	}
	if verified.Actor != p.actor || verified.RequestID != p.claim.RequestID || verified.ManifestDigest != p.manifestDigest {
		return errors.New("paid: the verified request is not the configured actor, request and manifest")
	}
	if verified.Manifest.ToolchainDigest != p.contract.ToolchainDigest {
		return errors.New("paid: the quoted toolchain is not the executor's toolchain")
	}
	if _, err := retentionDeadline(verified.ResponseDeadline, verified.Manifest.RetentionSeconds); err != nil {
		return err
	}
	return nil
}

func (p *paidJob) execute(ctx context.Context, verified *railgate.Verified) (string, error) {
	// The pre-execution verification is a fresh one; it must still pass
	// the same check.
	if err := p.check(verified); err != nil {
		return "", err
	}
	retainUntil, err := retentionDeadline(verified.ResponseDeadline, verified.Manifest.RetentionSeconds)
	if err != nil {
		return "", err
	}
	request := softwarework.Request{QuoteCommitment: verified.QuoteCommitment,
		ExecutionID: sha256Text(verified.ExecutionID), InputDigest: sha256Text(verified.InputDigest),
		SourceDigest: p.sourceDigest, SourceArchive: p.archive}
	outcome, err := p.run(ctx, request)
	if err != nil {
		return "", err
	}
	// The runner bound its objects to this execution as pending; give them
	// the job's deadline first, so even an outcome refused below does not
	// stay pending forever.
	if err := p.store.Retain(ctx, request.ExecutionID, time.Unix(retainUntil, 0),
		[]string{outcome.Artifact.Digest, outcome.Report.Digest}); err != nil {
		return "", fmt.Errorf("paid: record artifact retention: %w", err)
	}
	if outcome.ExitCode != 0 || outcome.CompletedAtUnix == 0 || outcome.ExecutionID != request.ExecutionID ||
		outcome.QuoteCommitment != request.QuoteCommitment || outcome.InputDigest != request.InputDigest ||
		outcome.SourceDigest != p.sourceDigest || outcome.ToolchainDigest != p.contract.ToolchainDigest ||
		outcome.SandboxDigest != p.manifestDigest {
		return "", errors.New("paid: the execution outcome is not a successful run of this request")
	}
	// The deliverables must be stored and intact before a response hash can
	// be recorded for them.
	for _, digest := range []string{outcome.Artifact.Digest, outcome.Report.Digest} {
		if _, err := p.store.Get(ctx, digest); err != nil {
			return "", fmt.Errorf("paid: stored result %s: %w", digest, err)
		}
	}
	root, _, err := nativecore.BuildSoftwareWorkReceiptCellV1(nativecore.SoftwareWorkReceiptV1{
		QuoteCommitment: verified.QuoteCommitment, ExecutionID: request.ExecutionID, InputDigest: request.InputDigest,
		ResultDigest: outcome.ResultDigest, ArtifactDigest: outcome.Artifact.Digest, ReportDigest: outcome.Report.Digest,
		SourceDigest: p.sourceDigest, ToolchainDigest: verified.Manifest.ToolchainDigest, SandboxDigest: verified.ManifestDigest,
		ChargedAtomicAmount: verified.Price.String(), ProviderAgentID: verified.ProviderAgentID,
		CompletedAt: outcome.CompletedAtUnix, ExitCode: int32(outcome.ExitCode),
	})
	if err != nil {
		return "", fmt.Errorf("paid: build Receipt: %w", err)
	}
	p.outcome, p.receipt, p.retainUntil = &outcome, root.ToBOC(), retainUntil
	return hex.EncodeToString(root.Hash()), nil
}

// retentionDeadline is the last second results stay retained: the end of
// the response window plus the manifest's retention, so a request answered
// late keeps its full retention for delivery.
func retentionDeadline(responseDeadline, retentionSeconds uint64) (int64, error) {
	if retentionSeconds == 0 || responseDeadline == 0 {
		return 0, errors.New("paid: the manifest retains no result")
	}
	if responseDeadline > math.MaxInt64 || retentionSeconds > math.MaxInt64-responseDeadline {
		return 0, errors.New("paid: the result retention overflows")
	}
	return int64(responseDeadline + retentionSeconds), nil
}

func sha256Text(value [32]byte) string { return "sha256:" + hex.EncodeToString(value[:]) }

// executeOnce runs the job through the gate and reports the outcome.
func (p *paidJob) executeOnce(ctx context.Context) paidReport {
	result := p.gate.ExecuteOnce(ctx, p.claim, p.check, p.execute)
	report := paidReport{Status: string(result.Status), ExitCode: exitCodeFor(result.Status), Actor: p.actor,
		RequestID: p.claim.RequestID, SlotState: string(result.Slot.State), QuoteCommitment: result.Slot.QuoteCommitment,
		ResponseHash: result.ResponseHash}
	if result.Err != nil {
		report.Error = result.Err.Error()
	}
	if result.Status != railgate.StatusCompleted {
		return report
	}
	report.Outcome, report.RetainUntilUnix = p.outcome, p.retainUntil
	report.ReceiptBOCBase64 = base64.StdEncoding.EncodeToString(p.receipt)
	// The completed slot is recorded; whether respond may be sent now is a
	// separate fresh check the operator repeats before signing.
	if err := p.gate.Recheck(ctx, result.Slot, p.claim, result.Verified); err != nil {
		report.RespondCheck = "refused: " + err.Error()
	} else {
		report.RespondCheck = "passed"
	}
	return report
}

func writeReport(stdout io.Writer, report paidReport) int {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		// The status cannot be stated; never let it read as success.
		return exitAmbiguous
	}
	fmt.Fprintln(stdout, string(encoded))
	return report.ExitCode
}

func refuse(stdout io.Writer, err error) int {
	return writeReport(stdout, paidReport{Status: statusRefused, ExitCode: exitRefused, Error: err.Error()})
}

// usageError marks a command line that does not parse.
type usageError struct{ error }

func (e usageError) Unwrap() error { return e.error }

const statusUsage = "usage"

func runPaid(args []string, stdout, stderr io.Writer) int {
	in, err := parsePaidInputs(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	var usage usageError
	if errors.As(err, &usage) {
		return writeReport(stdout, paidReport{Status: statusUsage, ExitCode: exitUsage, Error: err.Error()})
	}
	if err != nil {
		return refuse(stdout, err)
	}
	job, closer, err := openPaid(in)
	if err != nil {
		return refuse(stdout, err)
	}
	report := job.executeOnce(context.Background())
	if closeErr := closer(); closeErr != nil && report.Error == "" {
		report.Error = "close: " + closeErr.Error()
	}
	return writeReport(stdout, report)
}

// openPaid validates every input and opens the finalized resolver, the slot
// gate and the executor before anything is admitted, so a missing piece
// refuses the run instead of admitting a request it cannot serve.
func openPaid(in paidInputs) (*paidJob, func() error, error) {
	if err := requirePrivateOwnedDirectory(in.stateDir); err != nil {
		return nil, nil, err
	}
	config, err := loadRailConfig(in.railConfig)
	if err != nil {
		return nil, nil, err
	}
	network := &nativev1.NetworkDomain{NetworkId: config.Network.NetworkID,
		GenesisRootHash: config.Network.GenesisRootHash, GenesisFileHash: config.Network.GenesisFileHash}
	manifestCBOR, err := readPrivateFile(config.ManifestFile, maxManifestBytes, "provider manifest")
	if err != nil {
		return nil, nil, err
	}
	manifest, err := nativecore.DecodeCanonicalSoftwareWorkManifestCBOR(manifestCBOR)
	if err != nil {
		return nil, nil, err
	}
	canonical, manifestDigest, err := nativecore.CanonicalSoftwareWorkManifest(manifest)
	if err != nil || !bytes.Equal(canonical, manifestCBOR) {
		return nil, nil, errors.New("the provider manifest is not canonical")
	}
	contract, err := publishedContract(manifestDigest, manifest.Limits.OutputBytes)
	if err != nil {
		return nil, nil, err
	}
	if manifest.ToolchainDigest != contract.ToolchainDigest {
		return nil, nil, errors.New("the provider manifest names another toolchain than the executor's")
	}
	if manifest.RetentionSeconds == 0 {
		return nil, nil, errors.New("the provider manifest retains no result; a paid result must be retained for delivery")
	}
	packageBytes, err := readPrivateFile(in.quote, maxQuotePackage, "Quote package")
	if err != nil {
		return nil, nil, err
	}
	pkg := &nativev1.QuoteProposalPackageV1{}
	if err := proto.Unmarshal(packageBytes, pkg); err != nil || pkg.GetProposal() == nil {
		return nil, nil, errors.New("the Quote package does not decode")
	}
	validated, err := quoteexchange.ValidatePortable(network, &nativev1.RequestQuoteProposalRequest{
		CapabilityId: config.CapabilityID, CapabilityVersion: config.Version, BuyerAddress: in.buyer}, pkg)
	if err != nil {
		return nil, nil, err
	}
	if validated.Rail.Actor != in.actor {
		return nil, nil, errors.New("--actor is not the Service Actor the Quote names")
	}
	archive, err := readPrivateSource(in.source)
	if err != nil {
		return nil, nil, err
	}
	claim := railgate.Claim{Package: pkg, BuyerWallet: in.buyer, RequestID: in.requestID,
		Call: toschain.ActorTxID{LT: in.callLT, Hash: in.callHash}}
	if claim.ExecutionID, err = digest32(in.executionID); err != nil {
		return nil, nil, fmt.Errorf("--execution-id: %w", err)
	}
	if claim.InputDigest, err = digest32(in.inputDigest); err != nil {
		return nil, nil, fmt.Errorf("--input-digest: %w", err)
	}
	reader, err := openFinalizedReader(config, network, in.stateDir)
	if err != nil {
		return nil, nil, err
	}
	// The budget covers the longer of the quoted wall clock and the
	// executor's own limit, so a run can never outlast what was checked.
	budget := max(time.Duration(manifest.Limits.WallClockMillis)*time.Millisecond, contract.Limits.ExecutionTime)
	manifests := staticManifest{digest: manifestDigest, value: manifestCBOR}
	verifier, err := railgate.NewVerifier(railgate.VerifierConfig{Reader: reader, Manifests: manifests, Network: network,
		ActorCodeHash: config.ActorCodeHash, ProviderAgentID: config.ProviderAgentID, CapabilityID: config.CapabilityID,
		Version: config.Version, ExecutionBudget: budget, RespondBudget: time.Duration(config.RespondBudgetSeconds) * time.Second})
	if err != nil {
		return nil, nil, err
	}
	environment, err := openExecution(in.socket, in.fifo, in.stateDir, contract)
	if err != nil {
		return nil, nil, err
	}
	gate, err := railgate.OpenGate(verifier, filepath.Join(in.stateDir, "slots"))
	if err != nil {
		return nil, nil, errors.Join(err, environment.Close())
	}
	job := &paidJob{gate: gate, claim: claim, actor: in.actor, manifestDigest: manifestDigest, contract: contract,
		archive: archive, sourceDigest: sha256Digest(archive), run: environment.runner.Execute, store: environment.store}
	return job, func() error { return errors.Join(gate.Close(), environment.Close()) }, nil
}

// openFinalizedReader builds the strict-majority finalized Service Actor
// reader. Its checkpoints live in the paid state directory so their
// high-water marks survive across runs.
func openFinalizedReader(config railConfig, network *nativev1.NetworkDomain, stateDir string) (railgate.Reader, error) {
	quorum := config.Quorum
	if quorum == 0 {
		quorum = len(config.RPCEndpoints)/2 + 1
	}
	chain, err := toschain.New(toschain.Config{Network: network.GetNetworkId(), Endpoints: config.RPCEndpoints, Quorum: quorum})
	if err != nil {
		return nil, err
	}
	code, err := readPrivateFile(config.RegistryCodeBOCFile, maxRegistryBOC, "registry code BOC")
	if err != nil || len(code) == 0 {
		return nil, errors.New("read the registry code BOC")
	}
	locator, err := nativecore.NewLocator(network, 0, strings.Join(strings.Fields(string(code)), ""), config.RegistryCodeHash)
	if err != nil {
		return nil, err
	}
	registry, err := toschain.NewSimplifiedNativeResolver(chain, locator, filepath.Join(stateDir, "registry.checkpoint"))
	if err != nil {
		return nil, err
	}
	reader, err := toschain.NewServiceActorReader(toschain.ServiceActorReaderConfig{Chain: chain, Network: network,
		ActorCodeHash: config.ActorCodeHash, Registry: registry, CheckpointPath: filepath.Join(stateDir, "service-actor.checkpoint")})
	if err != nil {
		return nil, err
	}
	return railgate.FromChain(reader)
}

// staticManifest serves only the configured manifest. Its bytes are still
// checked against the finalized digest by the verifier.
type staticManifest struct {
	digest string
	value  []byte
}

func (m staticManifest) Manifest(_ context.Context, digest string) ([]byte, error) {
	if digest != m.digest {
		return nil, errors.New("manifest not served by this provider")
	}
	return append([]byte(nil), m.value...), nil
}
