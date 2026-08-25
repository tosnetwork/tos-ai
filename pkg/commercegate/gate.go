// Package commercegate implements the local, Agreement-bound execution start
// boundary. It complements the chain-specific Native Execution Gate: trusted
// or externally settled work still needs an atomic, at-most-once local start.
package commercegate

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
	commerce "github.com/tosnetwork/tos-service-protocol/pkg/agentcommerce"
	"github.com/tosnetwork/tos-service-protocol/pkg/codec"
)

const (
	MaxPlanFiles           = 256
	MaxInputFileBytes      = 1 << 30
	MaxStartTicketLifetime = 5 * time.Minute
	MaxPlanNetworkTargets  = 64
	MaxPlanCredentials     = 64
	MaxPlanEffects         = 256
)

type LeaseLossPolicy string

const (
	LeaseLossContinue LeaseLossPolicy = "continue"
	LeaseLossDrain    LeaseLossPolicy = "drain"
	LeaseLossKill     LeaseLossPolicy = "kill"
)

type FileBinding struct {
	Path   string `json:"path"`
	Digest string `json:"digest"`
	Size   uint64 `json:"size"`
}

type NetworkBinding struct {
	BindingID        string   `json:"binding_id"`
	Scheme           string   `json:"scheme"`
	Host             string   `json:"host"`
	Port             uint16   `json:"port"`
	ResolvedIPs      []string `json:"resolved_ips"`
	TLSServerName    string   `json:"tls_server_name"`
	AllowRedirects   bool     `json:"allow_redirects"`
	MaxRequestBytes  uint64   `json:"max_request_bytes"`
	MaxResponseBytes uint64   `json:"max_response_bytes"`
}

type CredentialBinding struct {
	HandleID      string `json:"handle_id"`
	ScopeDigest   string `json:"scope_digest"`
	ExpiresAtUnix uint64 `json:"expires_at_unix"`
}

// EffectBinding is one externally visible operation frozen into the approved
// plan. A runner may invoke only these entries through EffectBroker.
type EffectBinding struct {
	PlanEffectID            string `json:"plan_effect_id"`
	EffectProfileDigest     string `json:"effect_profile_digest"`
	TargetDigest            string `json:"target_digest"`
	OperationKind           string `json:"operation_kind"`
	EffectSemanticKeyDigest string `json:"effect_semantic_key_digest"`
	NetworkBindingID        string `json:"network_binding_id,omitempty"`
	CredentialHandleID      string `json:"credential_handle_id,omitempty"`
	MaximumUses             uint32 `json:"maximum_uses"`
}

type Plan struct {
	OwnerID                             string              `json:"owner_id"`
	AgentID                             string              `json:"agent_id"`
	AgreementBodyDigest                 string              `json:"agreement_body_digest"`
	ExecutionObligationID               string              `json:"execution_obligation_id"`
	CanonicalPlanDigest                 string              `json:"canonical_plan_digest"`
	AcceptedInputManifestDigest         string              `json:"accepted_input_manifest_digest"`
	AttemptIndex                        uint64              `json:"attempt_index"`
	PredecessorTerminalResolutionDigest string              `json:"predecessor_terminal_resolution_digest"`
	ExecutionID                         string              `json:"execution_id"`
	ReservationID                       string              `json:"reservation_id"`
	PolicyRevision                      uint64              `json:"policy_revision"`
	WriterGeneration                    uint64              `json:"writer_generation"`
	LeaseLossPolicy                     LeaseLossPolicy     `json:"lease_loss_policy"`
	Files                               []FileBinding       `json:"files,omitempty"`
	NetworkBindings                     []NetworkBinding    `json:"network_bindings,omitempty"`
	CredentialBindings                  []CredentialBinding `json:"credential_bindings,omitempty"`
	EffectBindings                      []EffectBinding     `json:"effect_bindings,omitempty"`
}

type Ticket struct {
	ExecutionID       string `json:"execution_id"`
	Secret            string `json:"secret"`
	StartNotAfterUnix uint64 `json:"start_not_after_unix"`
	PlanDigest        string `json:"plan_digest"`
}

type State string

const (
	StatePrepared       State = "PREPARED"
	StateStarting       State = "STARTING"
	StateRunning        State = "RUNNING"
	StateAmbiguousStart State = "AMBIGUOUS_START"
	StateAmbiguousRun   State = "AMBIGUOUS_RUN"
	StateSucceeded      State = "SUCCEEDED"
	StateFailed         State = "FAILED"
	StateCancelled      State = "CANCELLED"
	StateKilled         State = "KILLED"
)

// Resolution is the durable, read-only result of one deterministic execution
// identity. It deliberately omits the ticket secret and bound resource handles.
type Resolution struct {
	State         State
	StateRevision uint64
	PlanDigest    string
	OutcomeDigest string
}

type record struct {
	Schema               string                  `json:"schema"`
	Plan                 Plan                    `json:"plan"`
	PlanDigest           string                  `json:"plan_digest"`
	TicketSecret         string                  `json:"ticket_secret"`
	StartNotAfterUnix    uint64                  `json:"start_not_after_unix"`
	State                State                   `json:"state"`
	StateRevision        uint64                  `json:"state_revision"`
	OutcomeDigest        string                  `json:"outcome_digest,omitempty"`
	PrepareActionID      string                  `json:"prepare_action_id"`
	PrepareRequestDigest string                  `json:"prepare_request_digest"`
	StartActionID        string                  `json:"start_action_id,omitempty"`
	StartRequestDigest   string                  `json:"start_request_digest,omitempty"`
	Effects              map[string]EffectRecord `json:"effects,omitempty"`
}

type EffectRecord struct {
	StableActionID     string `json:"stable_action_id"`
	ExactRequestDigest string `json:"exact_request_digest"`
	State              string `json:"state"`
	ResponseDigest     string `json:"response_digest,omitempty"`
	HTTPStatus         uint16 `json:"http_status,omitempty"`
	StateRevision      uint64 `json:"state_revision"`
}

type HTTPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type HTTPSRequest struct {
	PlanEffectID string       `json:"plan_effect_id"`
	Method       string       `json:"method"`
	PathAndQuery string       `json:"path_and_query"`
	Headers      []HTTPHeader `json:"headers,omitempty"`
	Body         []byte       `json:"body,omitempty"`
}

type HTTPSResponse struct {
	StatusCode int
	Body       []byte
	Digest     string
}

type CredentialResolver interface {
	ResolveCredential(handleID, scopeDigest, stableActionID string, now time.Time) ([]HTTPHeader, error)
}

type Gate struct {
	mu        sync.Mutex
	directory string
	lock      *dirlock.Lock
	resolver  WriterAuthority
	now       func() time.Time
}

// WriterAuthority is the owner-scoped high-water source. Signature validation
// alone is insufficient because an old but unexpired fence must stop admitting
// starts immediately after takeover.
type WriterAuthority interface {
	commerce.FenceAuthorityResolver
	ConfirmCurrentWriterFence(commerce.WriterFence, time.Time) error
}

type Launch struct {
	ExecutionID        string
	PlanDigest         string
	TicketSecret       string
	LeaseLossPolicy    LeaseLossPolicy
	Files              []*os.File
	NetworkBindings    []NetworkBinding
	CredentialBindings []CredentialBinding
	EffectBindings     []EffectBinding
}

func Open(directory string, resolver WriterAuthority) (*Gate, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || resolver == nil {
		return nil, errors.New("local execution Gate configuration is invalid")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("local execution Gate directory must be private")
	}
	lock, err := dirlock.Acquire(directory, ".commerce-gate.lock")
	if err != nil {
		return nil, errors.New("local execution Gate is already owned")
	}
	gate := &Gate{directory: directory, lock: lock, resolver: resolver, now: time.Now}
	if err := gate.recoverInterruptedStates(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return gate, nil
}

func (gate *Gate) Close() error {
	if gate == nil || gate.lock == nil {
		return nil
	}
	err := gate.lock.Close()
	gate.lock = nil
	return err
}

func (gate *Gate) Prepare(_ context.Context, plan Plan, fence commerce.WriterFence, lifetime time.Duration,
	action commerce.AuthorizedAction) (Ticket, error) {
	if gate == nil || gate.lock == nil || lifetime < time.Second || lifetime > MaxStartTicketLifetime {
		return Ticket{}, errors.New("local execution preparation is invalid")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	now := gate.now().UTC()
	if err := commerce.VerifyWriterFence(fence, gate.resolver, now, "execution.prepare"); err != nil {
		return Ticket{}, err
	}
	if err := gate.resolver.ConfirmCurrentWriterFence(fence, now); err != nil {
		return Ticket{}, err
	}
	prepared, digest, err := preparePlan(plan, fence)
	if err != nil {
		return Ticket{}, err
	}
	canonicalRequest, fields, err := prepareAuthorizationMaterial(prepared)
	if err != nil || action.ActionKind != "execution.prepare" ||
		commerce.VerifyAuthorizedAction(action, fields, canonicalRequest, fence, gate.resolver, now) != nil {
		return Ticket{}, errors.New("execution preparation is not an exact authorized action")
	}
	path := gate.path(prepared.ExecutionID)
	if existing, readErr := readRecord(path); readErr == nil {
		if existing.PlanDigest != digest || existing.PrepareActionID != action.StableActionID || existing.PrepareRequestDigest != action.ExactRequestDigest {
			return Ticket{}, errors.New("execution identity conflicts with another plan")
		}
		if existing.State != StatePrepared {
			return Ticket{}, errors.New("execution has already started or is ambiguous")
		}
		if uint64(now.Unix()) >= existing.StartNotAfterUnix {
			return Ticket{}, errors.New("execution start ticket expired")
		}
		return ticketFrom(existing), nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return Ticket{}, readErr
	}
	secret, err := randomSecret()
	if err != nil {
		return Ticket{}, err
	}
	deadline := uint64(now.Add(lifetime).Unix())
	if fence.Body.ExpiresAtUnix < deadline {
		deadline = fence.Body.ExpiresAtUnix
	}
	if action.ExpiresAtUnix < deadline {
		deadline = action.ExpiresAtUnix
	}
	entry := record{Schema: "tos.ai.local-execution-gate.v1", Plan: prepared, PlanDigest: digest, TicketSecret: secret,
		StartNotAfterUnix: deadline, State: StatePrepared, StateRevision: 1, PrepareActionID: action.StableActionID,
		PrepareRequestDigest: action.ExactRequestDigest, Effects: map[string]EffectRecord{}}
	if err := createRecord(path, entry); err != nil {
		return Ticket{}, err
	}
	return ticketFrom(entry), nil
}

func (gate *Gate) Start(_ context.Context, ticket Ticket, fence commerce.WriterFence,
	action commerce.AuthorizedAction) (Launch, error) {
	if gate == nil || gate.lock == nil || ticket.ExecutionID == "" || ticket.Secret == "" {
		return Launch{}, errors.New("local execution start is invalid")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	now := gate.now().UTC()
	if err := commerce.VerifyWriterFence(fence, gate.resolver, now, "execution.start"); err != nil {
		return Launch{}, err
	}
	if err := gate.resolver.ConfirmCurrentWriterFence(fence, now); err != nil {
		return Launch{}, err
	}
	path := gate.path(ticket.ExecutionID)
	entry, err := readRecord(path)
	if err != nil {
		return Launch{}, err
	}
	if entry.PlanDigest != ticket.PlanDigest || entry.TicketSecret != ticket.Secret || entry.StartNotAfterUnix != ticket.StartNotAfterUnix ||
		entry.Plan.WriterGeneration != fence.Body.WriterGeneration {
		return Launch{}, errors.New("start ticket, plan, or writer mismatch")
	}
	canonicalRequest, fields, err := startAuthorizationMaterial(entry.Plan, ticket)
	if err != nil || action.ActionKind != "execution.start" ||
		commerce.VerifyAuthorizedAction(action, fields, canonicalRequest, fence, gate.resolver, now) != nil {
		return Launch{}, errors.New("execution start is not an exact authorized action")
	}
	if entry.State == StateStarting || entry.State == StateRunning {
		return Launch{}, errors.New("execution start is ambiguous or already running")
	}
	if entry.State != StatePrepared || !now.Before(time.Unix(int64(entry.StartNotAfterUnix), 0)) {
		return Launch{}, errors.New("execution is not startable")
	}
	files, err := openBoundFiles(entry.Plan.Files)
	if err != nil {
		return Launch{}, err
	}
	entry.State = StateStarting
	entry.StateRevision++
	entry.StartActionID = action.StableActionID
	entry.StartRequestDigest = action.ExactRequestDigest
	if err := replaceRecord(path, entry); err != nil {
		closeFiles(files)
		return Launch{}, err
	}
	return Launch{ExecutionID: entry.Plan.ExecutionID, PlanDigest: entry.PlanDigest, TicketSecret: entry.TicketSecret,
		LeaseLossPolicy: entry.Plan.LeaseLossPolicy, Files: files, NetworkBindings: append([]NetworkBinding(nil), entry.Plan.NetworkBindings...),
		CredentialBindings: append([]CredentialBinding(nil), entry.Plan.CredentialBindings...),
		EffectBindings:     append([]EffectBinding(nil), entry.Plan.EffectBindings...)}, nil
}

// PrepareAuthorizationMaterial returns the exact request and semantic fields
// that the Owner Action Authority must authorize before Prepare. Callers may
// not alter the returned prepared plan.
func PrepareAuthorizationMaterial(plan Plan, fence commerce.WriterFence) (Plan, []byte, map[string]commerce.SemanticValue, error) {
	prepared, _, err := preparePlan(plan, fence)
	if err != nil {
		return Plan{}, nil, nil, err
	}
	request, fields, err := prepareAuthorizationMaterial(prepared)
	return prepared, request, fields, err
}

func prepareAuthorizationMaterial(prepared Plan) ([]byte, map[string]commerce.SemanticValue, error) {
	request, err := codec.Marshal(prepared)
	if err != nil {
		return nil, nil, err
	}
	fields := map[string]commerce.SemanticValue{"owner_id": commerce.ID(prepared.OwnerID), "agent_id": commerce.ID(prepared.AgentID),
		"execution_id": commerce.Digest32(prepared.ExecutionID)}
	return request, fields, nil
}

func StartAuthorizationMaterial(plan Plan, ticket Ticket) ([]byte, map[string]commerce.SemanticValue, error) {
	return startAuthorizationMaterial(plan, ticket)
}

func startAuthorizationMaterial(plan Plan, ticket Ticket) ([]byte, map[string]commerce.SemanticValue, error) {
	request, err := codec.Marshal(ticket)
	if err != nil {
		return nil, nil, err
	}
	fields := map[string]commerce.SemanticValue{"owner_id": commerce.ID(plan.OwnerID), "agent_id": commerce.ID(plan.AgentID),
		"execution_id": commerce.Digest32(plan.ExecutionID)}
	return request, fields, nil
}

func (gate *Gate) MarkRunning(launch Launch) error {
	return gate.transition(launch, StateStarting, StateRunning, "")
}

func (gate *Gate) Complete(launch Launch, terminal State, outcomeDigest string) error {
	if terminal != StateSucceeded && terminal != StateFailed && terminal != StateCancelled && terminal != StateKilled {
		return errors.New("execution completion is not terminal")
	}
	if terminal == StateSucceeded && !validDigest(outcomeDigest) {
		return errors.New("successful execution requires an outcome digest")
	}
	return gate.transition(launch, StateRunning, terminal, outcomeDigest)
}

func (gate *Gate) Resolve(executionID string) (State, uint64, error) {
	resolution, err := gate.Inspect(executionID)
	return resolution.State, resolution.StateRevision, err
}

func (gate *Gate) Inspect(executionID string) (Resolution, error) {
	if !validDigest(executionID) {
		return Resolution{}, errors.New("invalid execution identity")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	entry, err := readRecord(gate.path(executionID))
	if err != nil {
		return Resolution{}, err
	}
	return Resolution{State: entry.State, StateRevision: entry.StateRevision,
		PlanDigest: entry.PlanDigest, OutcomeDigest: entry.OutcomeDigest}, nil
}

func (gate *Gate) transition(launch Launch, expected, next State, outcome string) error {
	if gate == nil || gate.lock == nil {
		return errors.New("local execution Gate is unavailable")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	path := gate.path(launch.ExecutionID)
	entry, err := readRecord(path)
	if err != nil || entry.PlanDigest != launch.PlanDigest || entry.TicketSecret != launch.TicketSecret || entry.State != expected {
		return errors.New("execution transition does not match its one-shot launch")
	}
	entry.State = next
	entry.StateRevision++
	entry.OutcomeDigest = outcome
	return replaceRecord(path, entry)
}

// EffectAuthorizationMaterial returns the exact request and released semantic
// fields for one plan-bound effect. The same bytes are verified again by Gate.
func EffectAuthorizationMaterial(plan Plan, request HTTPSRequest) ([]byte, map[string]commerce.SemanticValue, error) {
	var effect *EffectBinding
	for index := range plan.EffectBindings {
		if plan.EffectBindings[index].PlanEffectID == request.PlanEffectID {
			effect = &plan.EffectBindings[index]
			break
		}
	}
	if effect == nil {
		return nil, nil, errors.New("effect is not present in the approved plan")
	}
	canonical, err := codec.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	fields := map[string]commerce.SemanticValue{"owner_id": commerce.ID(plan.OwnerID), "agent_id": commerce.ID(plan.AgentID),
		"agreement_body_digest": commerce.Digest32(plan.AgreementBodyDigest), "obligation_id": commerce.ID(plan.ExecutionObligationID),
		"execution_id": commerce.Digest32(plan.ExecutionID), "plan_effect_id": commerce.ID(effect.PlanEffectID),
		"effect_profile_digest": commerce.Digest32(effect.EffectProfileDigest), "target_digest": commerce.Digest32(effect.TargetDigest),
		"operation_kind": commerce.Kind(effect.OperationKind), "effect_semantic_key_digest": commerce.Digest32(effect.EffectSemanticKeyDigest)}
	return canonical, fields, nil
}

// PerformHTTPS is the task-scoped outbound broker. It durably linearizes an
// effect as submitted before opening the socket; a crash or timeout thereafter
// remains ambiguous and cannot be retried as a new semantic action.
func (gate *Gate) PerformHTTPS(ctx context.Context, launch Launch, request HTTPSRequest,
	action commerce.AuthorizedAction, fence commerce.WriterFence, credentials CredentialResolver) (HTTPSResponse, error) {
	if gate == nil || gate.lock == nil {
		return HTTPSResponse{}, errors.New("execution effect broker is unavailable")
	}
	now := gate.now().UTC()
	gate.mu.Lock()
	path := gate.path(launch.ExecutionID)
	entry, err := readRecord(path)
	if err != nil || entry.State != StateRunning || entry.PlanDigest != launch.PlanDigest || entry.TicketSecret != launch.TicketSecret {
		gate.mu.Unlock()
		return HTTPSResponse{}, errors.New("effect does not belong to the running one-shot launch")
	}
	if entry.Effects == nil {
		entry.Effects = map[string]EffectRecord{}
	}
	var effect *EffectBinding
	var network *NetworkBinding
	var credential *CredentialBinding
	for index := range entry.Plan.EffectBindings {
		if entry.Plan.EffectBindings[index].PlanEffectID == request.PlanEffectID {
			effect = &entry.Plan.EffectBindings[index]
			break
		}
	}
	if effect != nil {
		for index := range entry.Plan.NetworkBindings {
			if entry.Plan.NetworkBindings[index].BindingID == effect.NetworkBindingID {
				network = &entry.Plan.NetworkBindings[index]
				break
			}
		}
	}
	if effect != nil && effect.CredentialHandleID != "" {
		for index := range entry.Plan.CredentialBindings {
			if entry.Plan.CredentialBindings[index].HandleID == effect.CredentialHandleID {
				credential = &entry.Plan.CredentialBindings[index]
				break
			}
		}
	}
	canonical, fields, materialErr := EffectAuthorizationMaterial(entry.Plan, request)
	if materialErr != nil || effect == nil || network == nil || action.ActionKind != "executor.effect" ||
		commerce.VerifyAuthorizedAction(action, fields, canonical, fence, gate.resolver, now) != nil || gate.resolver.ConfirmCurrentWriterFence(fence, now) != nil ||
		!validHTTPSRequest(request, *effect, *network) {
		gate.mu.Unlock()
		return HTTPSResponse{}, errors.New("HTTPS effect is not an exact current authorized action")
	}
	if prior, found := entry.Effects[request.PlanEffectID]; found {
		gate.mu.Unlock()
		if prior.StableActionID != action.StableActionID || prior.ExactRequestDigest != action.ExactRequestDigest {
			return HTTPSResponse{}, errors.New("effect identity conflicts")
		}
		if prior.State != "terminal" {
			return HTTPSResponse{}, errors.New("effect submission is ambiguous; resolve externally without retry")
		}
		body, readErr := os.ReadFile(gate.effectResponsePath(action.StableActionID))
		if readErr != nil {
			return HTTPSResponse{}, errors.New("terminal effect response is unavailable")
		}
		return HTTPSResponse{StatusCode: int(prior.HTTPStatus), Body: body, Digest: prior.ResponseDigest}, nil
	}
	entry.Effects[request.PlanEffectID] = EffectRecord{StableActionID: action.StableActionID,
		ExactRequestDigest: action.ExactRequestDigest, State: "submitted", StateRevision: 1}
	entry.StateRevision++
	if err := replaceRecord(path, entry); err != nil {
		gate.mu.Unlock()
		return HTTPSResponse{}, err
	}
	gate.mu.Unlock()

	headers := append([]HTTPHeader(nil), request.Headers...)
	if credential != nil {
		if credentials == nil || !now.Before(time.Unix(int64(credential.ExpiresAtUnix), 0)) {
			return HTTPSResponse{}, errors.New("effect credential is unavailable or expired")
		}
		secretHeaders, resolveErr := credentials.ResolveCredential(credential.HandleID, credential.ScopeDigest, action.StableActionID, now)
		if resolveErr != nil {
			return HTTPSResponse{}, resolveErr
		}
		headers = append(headers, secretHeaders...)
	}
	response, callErr := performPinnedHTTPS(ctx, *network, request, headers, action.StableActionID)
	if callErr != nil {
		return HTTPSResponse{}, callErr
	}
	response.Digest, err = codec.Digest("tos.executor-effect-response.v1", struct {
		Status int    `json:"status"`
		Body   []byte `json:"body"`
	}{response.StatusCode, response.Body})
	if err != nil {
		return HTTPSResponse{}, err
	}
	if err := gate.writeEffectResponse(action.StableActionID, response.Body); err != nil {
		return HTTPSResponse{}, err
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	entry, err = readRecord(path)
	prior := entry.Effects[request.PlanEffectID]
	if err != nil || prior.State != "submitted" || prior.StableActionID != action.StableActionID {
		return HTTPSResponse{}, errors.New("effect terminalization lost its submitted predecessor")
	}
	prior.State, prior.ResponseDigest, prior.HTTPStatus, prior.StateRevision = "terminal", response.Digest, uint16(response.StatusCode), prior.StateRevision+1
	entry.Effects[request.PlanEffectID] = prior
	entry.StateRevision++
	if err := replaceRecord(path, entry); err != nil {
		return HTTPSResponse{}, err
	}
	return response, nil
}

func validHTTPSRequest(request HTTPSRequest, effect EffectBinding, binding NetworkBinding) bool {
	wantMethod := strings.ToUpper(strings.TrimPrefix(effect.OperationKind, "https."))
	if request.Method != wantMethod || request.PathAndQuery == "" || request.PathAndQuery[0] != '/' ||
		strings.ContainsAny(request.PathAndQuery, "\r\n") || uint64(len(request.Body)) > binding.MaxRequestBytes {
		return false
	}
	last := ""
	for _, header := range request.Headers {
		name := http.CanonicalHeaderKey(header.Name)
		if name == "" || name != header.Name || name <= last || header.Value == "" || strings.ContainsAny(header.Value, "\r\n") ||
			name == "Authorization" || name == "Proxy-Authorization" || name == "Host" || name == "Connection" || name == "Transfer-Encoding" || name == "Idempotency-Key" {
			return false
		}
		last = name
	}
	return true
}

func performPinnedHTTPS(ctx context.Context, binding NetworkBinding, request HTTPSRequest, headers []HTTPHeader, stableID string) (HTTPSResponse, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{Proxy: nil, DisableCompression: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: binding.TLSServerName}}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, net.JoinHostPort(binding.ResolvedIPs[0], fmt.Sprint(binding.Port)))
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer transport.CloseIdleConnections()
	url := "https://" + net.JoinHostPort(binding.Host, fmt.Sprint(binding.Port)) + request.PathAndQuery
	httpRequest, err := http.NewRequestWithContext(ctx, request.Method, url, bytes.NewReader(request.Body))
	if err != nil {
		return HTTPSResponse{}, err
	}
	for _, header := range headers {
		name := http.CanonicalHeaderKey(header.Name)
		if name == "" || strings.ContainsAny(header.Value, "\r\n") || name == "Host" || name == "Connection" || name == "Transfer-Encoding" || name == "Idempotency-Key" {
			return HTTPSResponse{}, errors.New("credential supplied an unsafe HTTP header")
		}
		httpRequest.Header.Add(name, header.Value)
	}
	httpRequest.Header.Set("Idempotency-Key", strings.TrimPrefix(stableID, "sha256:"))
	result, err := client.Do(httpRequest)
	if err != nil {
		return HTTPSResponse{}, err
	}
	defer result.Body.Close()
	body, err := io.ReadAll(io.LimitReader(result.Body, int64(binding.MaxResponseBytes)+1))
	if err != nil || uint64(len(body)) > binding.MaxResponseBytes {
		return HTTPSResponse{}, errors.New("HTTPS effect response exceeded its exact bound")
	}
	return HTTPSResponse{StatusCode: result.StatusCode, Body: body}, nil
}

func (gate *Gate) effectResponsePath(stableID string) string {
	return filepath.Join(gate.directory, "effect-"+strings.TrimPrefix(stableID, "sha256:")+".response")
}
func (gate *Gate) writeEffectResponse(stableID string, body []byte) error {
	path := gate.effectResponsePath(stableID)
	temporary, err := os.CreateTemp(gate.directory, ".effect-response-")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(body)
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncParentDirectory(path)
}

func preparePlan(plan Plan, fence commerce.WriterFence) (Plan, string, error) {
	if plan.OwnerID != fence.Body.OwnerID || plan.AgentID != fence.Body.AgentID || plan.AgreementBodyDigest == "" || plan.ExecutionObligationID == "" ||
		plan.AcceptedInputManifestDigest == "" || plan.ReservationID == "" || plan.PolicyRevision == 0 || len(plan.Files) > MaxPlanFiles ||
		len(plan.NetworkBindings) > MaxPlanNetworkTargets || len(plan.CredentialBindings) > MaxPlanCredentials ||
		len(plan.EffectBindings) > MaxPlanEffects ||
		plan.LeaseLossPolicy != LeaseLossContinue && plan.LeaseLossPolicy != LeaseLossDrain && plan.LeaseLossPolicy != LeaseLossKill {
		return Plan{}, "", errors.New("execution plan is incomplete")
	}
	plan.WriterGeneration = fence.Body.WriterGeneration
	plan.ExecutionID = ""
	plan.CanonicalPlanDigest = ""
	sort.Slice(plan.Files, func(i, j int) bool { return plan.Files[i].Path < plan.Files[j].Path })
	for index, file := range plan.Files {
		if !filepath.IsAbs(file.Path) || filepath.Clean(file.Path) != file.Path || !validDigest(file.Digest) || file.Size > MaxInputFileBytes ||
			index > 0 && plan.Files[index-1].Path == file.Path {
			return Plan{}, "", errors.New("execution file binding is invalid or duplicated")
		}
	}
	sort.Slice(plan.NetworkBindings, func(i, j int) bool { return plan.NetworkBindings[i].BindingID < plan.NetworkBindings[j].BindingID })
	for index, binding := range plan.NetworkBindings {
		if binding.BindingID == "" || binding.Scheme != "https" || binding.Host == "" || binding.Port == 0 ||
			binding.TLSServerName != binding.Host || binding.AllowRedirects || binding.MaxRequestBytes == 0 || binding.MaxRequestBytes > 16<<20 ||
			binding.MaxResponseBytes == 0 || binding.MaxResponseBytes > 64<<20 || len(binding.ResolvedIPs) == 0 || len(binding.ResolvedIPs) > 16 ||
			!sort.StringsAreSorted(binding.ResolvedIPs) || index > 0 && plan.NetworkBindings[index-1].BindingID == binding.BindingID {
			return Plan{}, "", errors.New("execution network binding is invalid or duplicated")
		}
		for ipIndex, text := range binding.ResolvedIPs {
			ip := net.ParseIP(text)
			if !publicIP(ip) || ipIndex > 0 && binding.ResolvedIPs[ipIndex-1] == text {
				return Plan{}, "", errors.New("execution network binding contains a non-public or duplicate address")
			}
		}
	}
	sort.Slice(plan.CredentialBindings, func(i, j int) bool { return plan.CredentialBindings[i].HandleID < plan.CredentialBindings[j].HandleID })
	for index, binding := range plan.CredentialBindings {
		if binding.HandleID == "" || !validDigest(binding.ScopeDigest) || binding.ExpiresAtUnix == 0 ||
			index > 0 && plan.CredentialBindings[index-1].HandleID == binding.HandleID {
			return Plan{}, "", errors.New("execution credential binding is invalid or duplicated")
		}
	}
	networks := make(map[string]NetworkBinding, len(plan.NetworkBindings))
	for _, binding := range plan.NetworkBindings {
		networks[binding.BindingID] = binding
	}
	credentials := make(map[string]struct{}, len(plan.CredentialBindings))
	for _, binding := range plan.CredentialBindings {
		credentials[binding.HandleID] = struct{}{}
	}
	sort.Slice(plan.EffectBindings, func(i, j int) bool { return plan.EffectBindings[i].PlanEffectID < plan.EffectBindings[j].PlanEffectID })
	for index, effect := range plan.EffectBindings {
		network, found := networks[effect.NetworkBindingID]
		targetDigest, digestErr := codec.Digest("tos.executor-network-target.v1", network)
		_, credentialFound := credentials[effect.CredentialHandleID]
		if effect.PlanEffectID == "" || !validDigest(effect.EffectProfileDigest) || !validDigest(effect.TargetDigest) ||
			!validDigest(effect.EffectSemanticKeyDigest) || effect.OperationKind != "https.get" && effect.OperationKind != "https.post" && effect.OperationKind != "https.put" && effect.OperationKind != "https.delete" ||
			!found || digestErr != nil || effect.TargetDigest != targetDigest || effect.MaximumUses != 1 ||
			effect.CredentialHandleID != "" && !credentialFound || index > 0 && plan.EffectBindings[index-1].PlanEffectID == effect.PlanEffectID {
			return Plan{}, "", errors.New("execution effect binding is invalid or duplicated")
		}
	}
	// Writer generation fences admission of a concrete start, but it is not
	// part of the semantic work identity. A takeover must resolve the same slot
	// instead of manufacturing a second execution ID.
	writerGeneration := plan.WriterGeneration
	plan.WriterGeneration = 0
	canonicalPlanDigest, err := codec.Digest("tos.local-execution-plan.v1", plan)
	plan.WriterGeneration = writerGeneration
	if err != nil {
		return Plan{}, "", err
	}
	plan.CanonicalPlanDigest = canonicalPlanDigest
	fields := map[string]commerce.SemanticValue{"owner_id": commerce.ID(plan.OwnerID), "agent_id": commerce.ID(plan.AgentID),
		"agreement_body_digest": commerce.Digest32(plan.AgreementBodyDigest), "execution_obligation_id": commerce.ID(plan.ExecutionObligationID),
		"canonical_plan_digest": commerce.Digest32(canonicalPlanDigest), "accepted_input_manifest_digest": commerce.Digest32(plan.AcceptedInputManifestDigest),
		"attempt_index": commerce.U64(plan.AttemptIndex), "predecessor_terminal_resolution_digest": commerce.Digest32(plan.PredecessorTerminalResolutionDigest)}
	executionID, _, err := commerce.DeriveStableActionID("execution.slot", fields)
	if err != nil {
		return Plan{}, "", err
	}
	plan.ExecutionID = executionID
	digest, err := codec.Digest("tos.local-execution-plan-bound.v1", plan)
	return plan, digest, err
}

func openBoundFiles(bindings []FileBinding) ([]*os.File, error) {
	files := make([]*os.File, 0, len(bindings))
	for _, binding := range bindings {
		file, err := openReadOnlyNoFollow(binding.Path)
		if err != nil {
			closeFiles(files)
			return nil, errors.New("open immutable execution input")
		}
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != binding.Size {
			file.Close()
			closeFiles(files)
			return nil, errors.New("execution input identity changed")
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, io.LimitReader(file, int64(MaxInputFileBytes)+1)); err != nil || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != binding.Digest {
			file.Close()
			closeFiles(files)
			return nil, errors.New("execution input digest changed")
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			file.Close()
			closeFiles(files)
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}
func (gate *Gate) path(id string) string {
	if !validDigest(id) {
		return filepath.Join(gate.directory, "invalid")
	}
	return filepath.Join(gate.directory, id[7:]+".json")
}
func ticketFrom(entry record) Ticket {
	return Ticket{entry.Plan.ExecutionID, entry.TicketSecret, entry.StartNotAfterUnix, entry.PlanDigest}
}
func validDigest(value string) bool {
	if len(value) != 71 || value[:7] != "sha256:" {
		return false
	}
	_, err := hex.DecodeString(value[7:])
	return err == nil
}
func publicIP(ip net.IP) bool {
	return ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast()
}
func randomSecret() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func createRecord(path string, entry record) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syncParentDirectory(path)
}
func replaceRecord(path string, entry record) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".gate-")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(raw)
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncParentDirectory(path)
}
func readRecord(path string) (record, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return record{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > 2<<20 {
		return record{}, errors.New("invalid local execution Gate record")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return record{}, err
	}
	var entry record
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&entry) != nil || decoder.Decode(&struct{}{}) != io.EOF || entry.Schema != "tos.ai.local-execution-gate.v1" || entry.StateRevision == 0 ||
		!validDigest(entry.PrepareActionID) || !validDigest(entry.PrepareRequestDigest) || !knownState(entry.State) ||
		(entry.StartActionID == "") != (entry.StartRequestDigest == "") || entry.StartActionID != "" && (!validDigest(entry.StartActionID) || !validDigest(entry.StartRequestDigest)) {
		return record{}, errors.New("invalid local execution Gate record")
	}
	if entry.Effects == nil {
		entry.Effects = map[string]EffectRecord{}
	}
	for effectID, effect := range entry.Effects {
		if effectID == "" || !validDigest(effect.StableActionID) || !validDigest(effect.ExactRequestDigest) || effect.StateRevision == 0 ||
			effect.State != "submitted" && effect.State != "terminal" || effect.State == "terminal" && (!validDigest(effect.ResponseDigest) || effect.HTTPStatus == 0) {
			return record{}, errors.New("invalid execution effect record")
		}
	}
	return entry, nil
}

func knownState(state State) bool {
	switch state {
	case StatePrepared, StateStarting, StateRunning, StateAmbiguousStart, StateAmbiguousRun,
		StateSucceeded, StateFailed, StateCancelled, StateKilled:
		return true
	default:
		return false
	}
}

// recoverInterruptedStates runs while holding the Gate process lock. A prior
// process may have crossed the irreversible start boundary, so restart makes
// that uncertainty explicit and never admits the same execution again.
func (gate *Gate) recoverInterruptedStates() error {
	entries, err := os.ReadDir(gate.directory)
	if err != nil {
		return err
	}
	for _, item := range entries {
		if !item.Type().IsRegular() || !strings.HasSuffix(item.Name(), ".json") || len(item.Name()) != 64+5 {
			continue
		}
		path := filepath.Join(gate.directory, item.Name())
		entry, readErr := readRecord(path)
		if readErr != nil {
			return readErr
		}
		switch entry.State {
		case StateStarting:
			entry.State = StateAmbiguousStart
		case StateRunning:
			entry.State = StateAmbiguousRun
		default:
			continue
		}
		entry.StateRevision++
		if err := replaceRecord(path, entry); err != nil {
			return err
		}
	}
	return nil
}
