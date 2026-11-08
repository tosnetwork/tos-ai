// Package railgate admits the execution of a paid Service Actor request at
// most once, and only while finalized TOS state shows the request pending
// with enough time left to answer it. Every decision is re-derived from one
// finalized checkpoint: the Quote preimages, the original call proven from
// its transaction and snapshot, the request's lifecycle replayed from the
// actor's complete history and reconciled with its state, and the getters.
// A packet, a gateway or a local record never stands in for that state.
package railgate

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"time"

	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
	"github.com/tosnetwork/tos-service-protocol/pkg/quoteexchange"
	"github.com/tosnetwork/tos-service-protocol/pkg/serviceactor"
	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
	"google.golang.org/protobuf/proto"
)

const (
	// MinRespondBudget is the least time a verifier may reserve for sending
	// and finalizing respond after an execution ends.
	MinRespondBudget = 30 * time.Second
	// DefaultRespondBudget is used when a configuration leaves it unset.
	DefaultRespondBudget = 2 * time.Minute
	// MaxBudget bounds either budget so a misconfiguration cannot make every
	// request look too late or overflow deadline arithmetic.
	MaxBudget = 24 * time.Hour
)

var (
	cellDigestPattern = regexp.MustCompile(`^tvm-cell-sha256:[0-9a-f]{64}$`)
	agentPattern      = regexp.MustCompile(`^agent_[0-9a-f]{64}$`)
	rawAddressPattern = regexp.MustCompile(`^0:[0-9a-f]{64}$`)
)

// Session is one finalized checkpoint with every read the gate needs.
type Session interface {
	serviceactor.Session
	serviceactor.TransitionSession
	History(ctx context.Context, actor string, anchor *toschain.ActorTxID) (*toschain.ActorHistory, error)
	GetRequest(ctx context.Context, actor string, requestID uint64) (*nativecore.ServiceActorRequestViewV1, error)
	GetRefund(ctx context.Context, actor string, requestID uint64) (*nativecore.ServiceActorRefundV1, error)
}

// Reader opens finalized sessions.
type Reader interface {
	BeginSession(ctx context.Context) (Session, error)
}

type chainReader struct{ reader *toschain.ServiceActorReader }

func (c chainReader) BeginSession(ctx context.Context) (Session, error) {
	session, err := c.reader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return session, nil
}

// FromChain adapts a quorum-finalized Service Actor reader.
func FromChain(reader *toschain.ServiceActorReader) (Reader, error) {
	if reader == nil {
		return nil, errors.New("railgate: missing Service Actor reader")
	}
	return chainReader{reader: reader}, nil
}

// VerifierConfig binds a verifier to one provider offer.
type VerifierConfig struct {
	Reader          Reader
	Manifests       serviceactor.ManifestSource
	Network         *nativev1.NetworkDomain
	ActorCodeHash   string
	ProviderAgentID string
	CapabilityID    string
	Version         string
	// ExecutionBudget is the longest an admitted execution may take; it is
	// at least the manifest wall clock limit.
	ExecutionBudget time.Duration
	// RespondBudget is reserved after execution for respond to finalize.
	RespondBudget time.Duration
	// Now is the wall clock the deadline slack is measured with; nil means
	// time.Now.
	Now func() time.Time
}

// Verifier checks a claim against finalized state.
type Verifier struct {
	config   VerifierConfig
	codeHash [32]byte
}

// NewVerifier refuses an incomplete or unbounded configuration.
func NewVerifier(config VerifierConfig) (*Verifier, error) {
	if config.Reader == nil || config.Manifests == nil || config.Network == nil || config.Network.NetworkId == "" ||
		!agentPattern.MatchString(config.ProviderAgentID) || config.CapabilityID == "" || config.Version == "" ||
		!cellDigestPattern.MatchString(config.ActorCodeHash) {
		return nil, errors.New("railgate: invalid verifier configuration")
	}
	if config.RespondBudget == 0 {
		config.RespondBudget = DefaultRespondBudget
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RespondBudget < MinRespondBudget || config.RespondBudget > MaxBudget ||
		config.ExecutionBudget <= 0 || config.ExecutionBudget > MaxBudget {
		return nil, errors.New("railgate: execution and respond budgets are outside their bounds")
	}
	raw, err := hex.DecodeString(config.ActorCodeHash[len("tvm-cell-sha256:"):])
	if err != nil || len(raw) != 32 {
		return nil, errors.New("railgate: invalid actor code hash")
	}
	verifier := &Verifier{config: config}
	copy(verifier.codeHash[:], raw)
	config.Network = proto.Clone(config.Network).(*nativev1.NetworkDomain)
	verifier.config.Network = config.Network
	return verifier, nil
}

// Claim names one request a buyer asks the provider to execute. Every field
// is untrusted until Verify proves it from finalized state.
type Claim struct {
	Package     *nativev1.QuoteProposalPackageV1
	BuyerWallet string
	ExecutionID [32]byte
	InputDigest [32]byte
	RequestID   uint64
	Call        toschain.ActorTxID
}

// Verified is a claim proven at one finalized checkpoint.
type Verified struct {
	Checkpoint       uint64
	ChainTime        time.Time
	Actor            string
	RequestID        uint64
	RequestHash      [32]byte
	QuoteCommitment  string
	ExecutionID      [32]byte
	InputDigest      [32]byte
	BuyerWallet      string
	CapabilityID     string
	Version          string
	ProviderAgentID  string
	ManifestDigest   string
	Manifest         nativecore.SoftwareWorkManifestV1
	Transport        nativecore.TransportBindingV1
	Price            *big.Int
	ResponseDeadline uint64
	Call             toschain.ActorTxID
}

// SlotKey is the at-most-once identity of an execution.
func (v *Verified) SlotKey() SlotKey {
	return SlotKey{QuoteCommitment: v.QuoteCommitment, Actor: v.Actor, RequestID: v.RequestID}
}

// Verify proves a claim at one fresh finalized checkpoint and requires the
// request to be pending with the execution and respond budgets still ahead
// of its response deadline.
func (v *Verifier) Verify(ctx context.Context, claim Claim) (*Verified, error) {
	if v == nil {
		return nil, errors.New("railgate: invalid verification")
	}
	return v.verify(ctx, claim, v.config.ExecutionBudget+v.config.RespondBudget)
}

// verify proves a claim and requires budget to stay strictly before the
// request's response deadline at the checkpoint's chain time.
func (v *Verifier) verify(ctx context.Context, claim Claim, budget time.Duration) (*Verified, error) {
	if v == nil || ctx == nil {
		return nil, errors.New("railgate: invalid verification")
	}
	started := v.config.Now()
	if claim.Package == nil || claim.Package.Proposal == nil || !rawAddressPattern.MatchString(claim.BuyerWallet) ||
		claim.ExecutionID == ([32]byte{}) || claim.InputDigest == ([32]byte{}) || claim.Call.Hash == "" {
		return nil, errors.New("railgate: the claim is incomplete")
	}
	proposal := claim.Package.Proposal
	if proposal.CapabilityId != v.config.CapabilityID || proposal.CapabilityVersion != v.config.Version ||
		proposal.ProviderAgentId != v.config.ProviderAgentID {
		return nil, errors.New("railgate: the Quote names another provider, Capability or version")
	}
	validated, err := quoteexchange.ValidatePortable(v.config.Network, &nativev1.RequestQuoteProposalRequest{
		CapabilityId: proposal.CapabilityId, CapabilityVersion: proposal.CapabilityVersion, BuyerAddress: claim.BuyerWallet}, claim.Package)
	if err != nil {
		return nil, fmt.Errorf("railgate: %w", err)
	}
	if limit := time.Duration(validated.Manifest.Limits.WallClockMillis) * time.Millisecond; limit <= 0 || limit > v.config.ExecutionBudget {
		return nil, errors.New("railgate: the manifest wall clock exceeds the execution budget")
	}
	quote, commitment, err := nativecore.BuildAcceptedQuoteCommitment(v.config.Network, validated.Proposal)
	if err != nil {
		return nil, fmt.Errorf("railgate: %w", err)
	}
	var quoteHash [32]byte
	copy(quoteHash[:], quote.Hash())
	requestCell, err := nativecore.BuildServiceRequestCellV1(nativecore.ServiceRequestV1{QuoteCommitment: quoteHash,
		ExecutionID: claim.ExecutionID, InputDigest: claim.InputDigest})
	if err != nil {
		return nil, fmt.Errorf("railgate: %w", err)
	}
	var requestHash [32]byte
	copy(requestHash[:], requestCell.Hash())

	session, err := v.config.Reader.BeginSession(ctx)
	if err != nil {
		return nil, err
	}
	evidence, err := serviceactor.Gather(ctx, session, v.config.Network, v.config.Manifests, proposal.CapabilityId, proposal.CapabilityVersion)
	if err != nil {
		return nil, err
	}
	if evidence.Actor == nil || evidence.Actor.Data == nil {
		return nil, errors.New("railgate: the Service Actor account is not active at the checkpoint")
	}
	actor := validated.Rail.Actor
	if evidence.Actor.Address != actor {
		return nil, errors.New("railgate: the checkpoint actor is not the quoted actor")
	}
	history, err := session.History(ctx, actor, nil)
	if err != nil {
		return nil, err
	}
	if history == nil || history.Result != toschain.HistoryComplete || history.Head != evidence.Actor.LastTransaction {
		return nil, errors.New("railgate: the Service Actor history is not complete at the checkpoint")
	}
	call, err := findCall(history.Transactions, claim, requestHash, validated.Rail.TermsHash)
	if err != nil {
		return nil, err
	}
	acceptance, err := serviceactor.ProveAcceptance(ctx, session, evidence, call)
	if err != nil {
		return nil, err
	}
	existing, err := serviceactor.VerifyExistingRequest(evidence, v.codeHash, &validated.Rail, validated.Proposal, actor, call,
		claim.BuyerWallet, requestHash, acceptance)
	if err != nil {
		return nil, err
	}
	if existing.Snapshot.RequestID != claim.RequestID {
		return nil, errors.New("railgate: the call assigned another request id")
	}
	requests, err := serviceactor.Replay(history.Transactions, evidence.Actor.Data)
	if err != nil {
		return nil, fmt.Errorf("railgate: the actor history does not reconcile with its state: %w", err)
	}
	request := requests[claim.RequestID]
	if request == nil || request.Call.Hash != call.Hash || request.Call.LT != call.LT {
		return nil, errors.New("railgate: the replayed history does not hold this call's request")
	}
	if request.State != serviceactor.StatePending {
		return nil, fmt.Errorf("railgate: request %d is %s, not pending", claim.RequestID, request.State)
	}
	if err := crossCheckPending(ctx, session, actor, claim.RequestID, evidence.Actor.Data); err != nil {
		return nil, err
	}
	// The slack is measured from the checkpoint's chain time advanced by the
	// time this verification took, or from the wall clock if that is later,
	// so slow reads cannot leave less than the budget before the deadline.
	if err := checkSlack(currentTime(session.ChainTime(), started, v.config.Now()), request.Snapshot.ResponseDeadline, budget); err != nil {
		return nil, err
	}
	return &Verified{Checkpoint: session.Checkpoint(), ChainTime: session.ChainTime(), Actor: actor, RequestID: claim.RequestID,
		RequestHash: requestHash, QuoteCommitment: commitment, ExecutionID: claim.ExecutionID, InputDigest: claim.InputDigest,
		BuyerWallet: claim.BuyerWallet, CapabilityID: proposal.CapabilityId, Version: proposal.CapabilityVersion,
		ProviderAgentID: proposal.ProviderAgentId, ManifestDigest: proposal.ManifestDigest, Manifest: validated.Manifest,
		Transport: validated.TransportBinding, Price: new(big.Int).Set(request.Snapshot.Terms.PricePerCall),
		ResponseDeadline: request.Snapshot.ResponseDeadline, Call: claim.Call}, nil
}

// findCall returns the claimed finalized call. It must be a successful call
// from the claimed buyer carrying the request hash the claim reproduces and
// the quoted terms hash, and no other successful call from that buyer may
// carry the same hash. Failed calls and calls from other senders create no
// request for this buyer, so a replay of the public request hash by anyone
// else cannot block the buyer's request.
func findCall(transactions []toschain.ActorTransaction, claim Claim, requestHash, termsHash [32]byte) (toschain.ActorTransaction, error) {
	requestText, termsText := hex.EncodeToString(requestHash[:]), hex.EncodeToString(termsHash[:])
	var found []toschain.ActorTransaction
	for _, transaction := range transactions {
		if transaction.Kind == toschain.ActorTxCall && transaction.Call != nil && transaction.Sender == claim.BuyerWallet &&
			transaction.Call.RequestHash == requestText {
			found = append(found, transaction)
		}
	}
	if len(found) != 1 {
		return toschain.ActorTransaction{}, fmt.Errorf("railgate: %d successful calls from the buyer carry this request hash, need exactly one", len(found))
	}
	call := found[0]
	if call.LT != claim.Call.LT || call.Hash != claim.Call.Hash || call.Call.ExpectedTermsHash != termsText {
		return toschain.ActorTransaction{}, errors.New("railgate: the finalized call is not the claimed call under the quoted terms")
	}
	return call, nil
}

// crossCheckPending requires the getters to agree with the decoded state:
// get_request returns exactly the pending entry and get_refund nothing.
func crossCheckPending(ctx context.Context, session Session, actor string, id uint64, data *nativecore.ServiceActorDataV1) error {
	pending, err := session.GetRequest(ctx, actor, id)
	if err != nil {
		return err
	}
	refund, err := session.GetRefund(ctx, actor, id)
	if err != nil {
		return err
	}
	entry, ok := data.Pending[id]
	if !ok || refund != nil || !nativecore.SameRequestView(entry, pending) {
		return errors.New("railgate: the getters differ from the decoded pending entry")
	}
	return nil
}

// currentTime is a lower bound on the time at which the caller acts: the
// checkpoint's chain time advanced by the time elapsed since started, or now
// if that is later. A zero chain time stays zero so checkSlack refuses it.
func currentTime(chainTime, started, now time.Time) time.Time {
	if chainTime.IsZero() {
		return chainTime
	}
	elapsed := now.Sub(started)
	if elapsed < 0 {
		elapsed = 0
	}
	current := chainTime.Add(elapsed)
	if now.After(current) {
		current = now
	}
	return current
}

// checkSlack requires chain time plus budget to stay strictly before the
// response deadline.
func checkSlack(chainTime time.Time, responseDeadline uint64, budget time.Duration) error {
	if chainTime.IsZero() || chainTime.Unix() <= 0 || budget <= 0 {
		return errors.New("railgate: the checkpoint has no chain time")
	}
	seconds := uint64((budget + time.Second - 1) / time.Second)
	now := uint64(chainTime.Unix())
	if responseDeadline <= now || responseDeadline-now <= seconds {
		return errors.New("railgate: the request's response deadline leaves no room to execute and respond")
	}
	return nil
}

// Recheck proves, at a fresh checkpoint, that a verified request is still
// pending with the respond budget ahead of its deadline and unchanged since
// it was verified. The attestor policy runs it right before respond.
func (v *Verifier) Recheck(ctx context.Context, claim Claim, expected *Verified) error {
	if v == nil || expected == nil || expected.Price == nil {
		return errors.New("railgate: missing verified request")
	}
	verified, err := v.verify(ctx, claim, v.config.RespondBudget)
	if err != nil {
		return err
	}
	if verified.SlotKey() != expected.SlotKey() || verified.RequestHash != expected.RequestHash ||
		verified.Price.Cmp(expected.Price) != 0 || verified.ResponseDeadline != expected.ResponseDeadline {
		return errors.New("railgate: the request changed since it was verified")
	}
	return nil
}
