package railgate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/pkg/mldsa44"
	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
	"github.com/tosnetwork/tosutils-go/address"
	"github.com/tosnetwork/tosutils-go/tvm/cell"
	"google.golang.org/protobuf/proto"
)

// The values below are synthetic shapes, not contract-produced vectors: the
// gate's logic is under test, with the protocol's own decoders and builders.
const (
	fxCapability = "cap_3333333333333333333333333333333333333333333333333333333333333333"
	fxAgent      = "agent_4444444444444444444444444444444444444444444444444444444444444444"
)

var (
	fxActor    = "0:" + strings.Repeat("77", 32)
	fxBuyer    = "0:" + strings.Repeat("21", 32)
	fxOwner    = "0:" + strings.Repeat("11", 32)
	fxCodeHash = "tvm-cell-sha256:" + strings.Repeat("88", 32)
	fxKey      = bytes.Repeat([]byte{0x5a}, mldsa44.PublicKeySize)
	fxKeyID    = func() [32]byte { id, _ := mldsa44.KeyID(fxKey); return id }()
	fxNow      = time.Unix(2_000_000_000, 0)
	fxNetwork  = &nativev1.NetworkDomain{NetworkId: "tos-testnet", GenesisRootHash: "sha256:" + strings.Repeat("11", 32),
		GenesisFileHash: "sha256:" + strings.Repeat("22", 32)}
)

type fxManifests map[string][]byte

func (m fxManifests) Manifest(_ context.Context, digest string) ([]byte, error) {
	if value, ok := m[digest]; ok {
		return value, nil
	}
	return nil, errors.New("unknown manifest")
}

// fxChain is a fake finalized chain for one Capability and its actor; each
// session is an immutable copy of the chain at that moment.
type fxChain struct {
	mu         sync.Mutex
	checkpoint uint64
	chainTime  time.Time
	capability *nativev1.NativeStateV1
	agent      *nativev1.NativeStateV1
	data       *nativecore.ServiceActorDataV1
	history    []toschain.ActorTransaction
	getterLie  bool
	incomplete bool
	sessions   int
}

func (c *fxChain) BeginSession(context.Context) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoint++
	c.sessions++
	return &fxSession{checkpoint: c.checkpoint, chainTime: c.chainTime, capability: proto.Clone(c.capability).(*nativev1.NativeStateV1),
		agent: proto.Clone(c.agent).(*nativev1.NativeStateV1), data: c.data, history: append([]toschain.ActorTransaction(nil), c.history...),
		getterLie: c.getterLie, incomplete: c.incomplete}, nil
}

type fxSession struct {
	checkpoint        uint64
	chainTime         time.Time
	capability, agent *nativev1.NativeStateV1
	data              *nativecore.ServiceActorDataV1
	history           []toschain.ActorTransaction
	getterLie         bool
	incomplete        bool
}

func (s *fxSession) RegistryTransition(context.Context, string, nativecore.RegistryTransitionKind, string) (*toschain.RegistryTransitionTx, error) {
	return nil, toschain.ErrRegistryTransitionUnproven
}

func (s *fxSession) Checkpoint() uint64   { return s.checkpoint }
func (s *fxSession) ChainTime() time.Time { return s.chainTime }

func (s *fxSession) ResolveRegistry(_ context.Context, id string) (*nativev1.NativeStateV1, bool, error) {
	var state *nativev1.NativeStateV1
	switch id {
	case fxCapability:
		state = s.capability
	case fxAgent:
		state = s.agent
	default:
		return nil, false, nil
	}
	state.Reference = &nativev1.ChainReference{FinalizedCheckpoint: s.checkpoint}
	return state, true, nil
}

func (s *fxSession) head() toschain.ActorTxID {
	if len(s.history) == 0 {
		return toschain.ActorTxID{}
	}
	return toschain.ActorTxID{LT: s.history[0].LT, Hash: s.history[0].Hash}
}

func (s *fxSession) ReadActor(_ context.Context, account string) (*toschain.ActorAccount, bool, error) {
	if account != fxActor {
		return nil, false, nil
	}
	return &toschain.ActorAccount{Address: account, CodeHash: fxCodeHash, Data: s.data, LastTransaction: s.head()}, true, nil
}

func (s *fxSession) History(_ context.Context, _ string, anchor *toschain.ActorTxID) (*toschain.ActorHistory, error) {
	if anchor != nil {
		return nil, errors.New("not used")
	}
	result := toschain.HistoryComplete
	if s.incomplete {
		result = toschain.HistoryAnchorReached
	}
	return &toschain.ActorHistory{Result: result, Head: s.head(), Transactions: s.history}, nil
}

func (s *fxSession) GetRequest(_ context.Context, _ string, id uint64) (*nativecore.ServiceActorRequestViewV1, error) {
	entry, ok := s.data.Pending[id]
	if !ok {
		return nil, nil
	}
	view := &nativecore.ServiceActorRequestViewV1{Caller: entry.Caller, Price: entry.Price, StorageFee: entry.StorageFee,
		CleanupBounty: entry.CleanupBounty, ResponseDeadline: entry.ResponseDeadline, RefundClaimDeadline: entry.RefundClaimDeadline,
		PolicyVersion: entry.PolicyVersion, RequestHash: entry.RequestHash, TermsHash: entry.TermsHash,
		HasAttestor: entry.HasAttestor, AttestorKeyID: entry.AttestorKeyID}
	if s.getterLie {
		view.ResponseDeadline++
	}
	return view, nil
}

func (s *fxSession) GetRefund(_ context.Context, _ string, id uint64) (*nativecore.ServiceActorRefundV1, error) {
	entry, ok := s.data.Refunds[id]
	if !ok {
		return nil, nil
	}
	return &entry, nil
}

func fxManifest() nativecore.SoftwareWorkManifestV1 {
	attestor := mldsa44.KeyIDText(fxKeyID)
	return nativecore.SoftwareWorkManifestV1{
		Protocol: nativecore.SoftwareWorkManifestProtocolV1, Version: "1.0.0", Name: "Deterministic Go test",
		Description: "Run the committed Go test suite without network access.", Operation: "test",
		AcceptedSourceKinds: []string{"content-addressed-archive"},
		InputSchemaDigest:   "sha256:" + strings.Repeat("11", 32), OutputSchemaDigest: "sha256:" + strings.Repeat("22", 32),
		ToolchainDigest: "sha256:" + strings.Repeat("33", 32),
		Invocation: nativecore.SoftwareWorkInvocationV1{Executable: "/usr/local/bin/go", Arguments: []string{"test", "./..."},
			WorkingDirectory: "/workspace/source"},
		NetworkPolicy: "none", Limits: nativecore.SoftwareWorkLimitsV1{CPUMillis: 120000, MemoryBytes: 1 << 30,
			ScratchBytes: 2 << 30, OutputBytes: 1 << 20, WallClockMillis: 180000},
		ArtifactMediaTypes: []string{"application/vnd.tos.service.software-artifact.v1+tar"},
		ReportMediaTypes:   []string{"application/vnd.tos.service.test-report.v1+json"},
		SuccessCondition:   "exit-code-zero-and-valid-reports", RefundConditions: []string{nativecore.NoAttestedResponseRefund},
		EndpointCommitment:           "sha256:" + strings.Repeat("44", 32),
		ExecutionSignerAuthorization: nativecore.SettlementAttestorAuthorization(attestor), RetentionSeconds: 86400,
		SupportedAssets: []nativecore.SoftwareWorkAssetV1{{Kind: nativecore.NativeTOSAsset}},
		Settlement: nativecore.SoftwareWorkSettlementV1{Rail: nativecore.ServiceActorRail, ActorWorkchain: 0,
			ActorAccountID: bytes.Repeat([]byte{0x77}, 32), ActorCodeHash: bytes.Repeat([]byte{0x88}, 32), AttestorKey: attestor},
	}
}

func fxTerms(metadata [32]byte) nativecore.ServiceActorTermsV1 {
	return nativecore.ServiceActorTermsV1{PolicyVersion: 2, PricePerCall: big.NewInt(300_000_000), StorageFee: big.NewInt(200_000_000),
		CleanupBounty: big.NewInt(100_000_000), ResponseSLA: 3600, RefundClaimWindow: 3600, MetadataHash: metadata,
		ProofSchemeHash: nativecore.ServiceActorSoftwareWorkProofScheme, HasAttestor: true, AttestorKeyID: fxKeyID}
}

func txHash(lt uint64) string {
	var hash [32]byte
	binary.BigEndian.PutUint64(hash[24:], lt)
	return hex.EncodeToString(hash[:])
}

func fxCall(t *testing.T, caller string, requestID uint64, requestHash [32]byte, now uint32, lt uint64,
	terms nativecore.ServiceActorTermsV1) toschain.ActorTransaction {
	t.Helper()
	termsCell, err := nativecore.BuildServiceActorTermsCell(terms)
	if err != nil {
		t.Fatal(err)
	}
	callerAddress, err := address.ParseRawAddr(caller)
	if err != nil {
		t.Fatal(err)
	}
	responseDeadline := uint64(now) + uint64(terms.ResponseSLA)
	snapshot := cell.BeginCell().MustStoreUInt(nativecore.ServiceActorRequestSnapshotTag, 32).MustStoreUInt(requestID, 64).
		MustStoreAddr(callerAddress).MustStoreSlice(requestHash[:], 256).MustStoreUInt(responseDeadline, 64).
		MustStoreUInt(responseDeadline+uint64(terms.RefundClaimWindow), 64).MustStoreRef(termsCell).EndCell()
	return toschain.ActorTransaction{LT: lt, Hash: txHash(lt), Now: now, Kind: toschain.ActorTxCall, Sender: caller,
		Opcode: nativecore.ServiceActorCallOpcode, QueryID: 9,
		Call: &toschain.ActorCallBody{QueryID: 9, RequestHash: hex.EncodeToString(requestHash[:]),
			ExpectedTermsHash: hex.EncodeToString(termsCell.Hash())},
		Snapshot: &toschain.ActorSAR1{BodyBOC: base64.StdEncoding.EncodeToString(snapshot.ToBOC())}}
}

func fxData(t *testing.T, policy nativecore.ServiceActorPolicyV1, next uint64,
	pending, refunds map[uint64]nativecore.ServiceActorRequestSnapshotV1) *nativecore.ServiceActorDataV1 {
	t.Helper()
	owner, err := nativecore.StandardAddress(fxOwner)
	if err != nil {
		t.Fatal(err)
	}
	data := &nativecore.ServiceActorDataV1{Owner: owner, Active: true, NextRequestID: next, Policy: policy,
		Accounting: nativecore.ServiceActorAccountingV1{WithdrawableRevenue: new(big.Int), LockedStorageFees: new(big.Int),
			PendingLiability: new(big.Int), RefundableLiability: new(big.Int)},
		Pending: map[uint64]nativecore.ServiceActorPendingRequestV1{}, Refunds: map[uint64]nativecore.ServiceActorRefundV1{},
		CallerLiveCounts: map[[32]byte]uint32{}}
	for id, s := range pending {
		data.Pending[id] = nativecore.ServiceActorPendingRequestV1{Caller: s.Caller, Price: s.Terms.PricePerCall,
			StorageFee: s.Terms.StorageFee, CleanupBounty: s.Terms.CleanupBounty, ResponseDeadline: s.ResponseDeadline,
			RefundClaimDeadline: s.RefundClaimDeadline, PolicyVersion: s.Terms.PolicyVersion, RequestHash: s.RequestHash,
			TermsHash: s.TermsHash, HasAttestor: s.Terms.HasAttestor, AttestorKeyID: s.Terms.AttestorKeyID}
		data.Accounting.PendingLiability.Add(data.Accounting.PendingLiability, s.Terms.PricePerCall)
		data.Accounting.LockedStorageFees.Add(data.Accounting.LockedStorageFees, s.Terms.StorageFee)
		key, err := s.Caller.SliceHash()
		if err != nil {
			t.Fatal(err)
		}
		data.CallerLiveCounts[key]++
	}
	for id, s := range refunds {
		data.Refunds[id] = nativecore.ServiceActorRefundV1{Caller: s.Caller, Price: s.Terms.PricePerCall, StorageFee: s.Terms.StorageFee,
			CleanupBounty: s.Terms.CleanupBounty, RefundClaimDeadline: s.RefundClaimDeadline}
		data.Accounting.RefundableLiability.Add(data.Accounting.RefundableLiability, s.Terms.PricePerCall)
		data.Accounting.LockedStorageFees.Add(data.Accounting.LockedStorageFees, s.Terms.StorageFee)
		key, err := s.Caller.SliceHash()
		if err != nil {
			t.Fatal(err)
		}
		data.CallerLiveCounts[key]++
	}
	data.PendingCount, data.LiveCount = uint32(len(pending)), uint32(len(pending)+len(refunds))
	return data
}

type fixture struct {
	chain    *fxChain
	verifier *Verifier
	pkg      *nativev1.QuoteProposalPackageV1
	policy   nativecore.ServiceActorPolicyV1
	terms    nativecore.ServiceActorTermsV1
	claim    Claim
	call     toschain.ActorTransaction
	snapshot nativecore.ServiceActorRequestSnapshotV1
}

func digest32(b byte) [32]byte {
	var v [32]byte
	copy(v[:], bytes.Repeat([]byte{b}, 32))
	return v
}

// newFixture returns a provider offer with one finalized pending request
// from the buyer, called 100 seconds before the chain time.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	manifest := fxManifest()
	manifestCBOR, digest, err := nativecore.CanonicalSoftwareWorkManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := nativecore.ServiceActorMetadataHash(nativecore.ServiceActorMetadataInputV1{Network: fxNetwork,
		CapabilityID: fxCapability, ProviderAgentID: fxAgent, CapabilityVersion: "1.0.0", ManifestDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	terms := fxTerms(metadata)
	policy := nativecore.ServiceActorPolicyV1{PolicyVersion: terms.PolicyVersion, PricePerCall: terms.PricePerCall,
		StorageFee: terms.StorageFee, CleanupBounty: terms.CleanupBounty, ResponseSLA: terms.ResponseSLA,
		RefundClaimWindow: terms.RefundClaimWindow, OpenAccess: true, MetadataHash: metadata, ProofSchemeHash: terms.ProofSchemeHash,
		HasAttestor: true, AttestorKeyID: fxKeyID, AttestorKey: fxKey}
	binding, err := nativecore.AgentPacketTransportBinding(fxAgent, nativecore.MaxAgentPacketTransportRequestBytes)
	if err != nil {
		t.Fatal(err)
	}
	transport, transportDigest, err := nativecore.BuildTransportBindingCellV1(binding)
	if err != nil {
		t.Fatal(err)
	}
	termsHash, err := nativecore.ServiceActorTermsHash(terms)
	if err != nil {
		t.Fatal(err)
	}
	proposal := &nativev1.QuoteProposalV1{ProposalId: "provider-1", CapabilityId: fxCapability, CapabilityVersion: "1.0.0",
		ProviderAgentId: fxAgent, ManifestDigest: digest, TransportBindingDigest: transportDigest,
		ExpiresAtUnixSeconds: uint64(fxNow.Add(30 * time.Minute).Unix()),
		Actor:                &nativev1.TOSAccountAddressV1{AccountId: bytes.Repeat([]byte{0x77}, 32)}, ActorCodeHash: fxCodeHash,
		PolicyVersion: terms.PolicyVersion, TermsHash: hex.EncodeToString(termsHash[:]), PriceNanotos: "300000000",
		StorageFeeNanotos: "200000000", AttestorKeyId: mldsa44.KeyIDText(fxKeyID)}
	pkg := &nativev1.QuoteProposalPackageV1{Proposal: proposal, CanonicalManifestCbor: manifestCBOR, TransportBindingBoc: transport.ToBOC()}
	quote, _, err := nativecore.BuildAcceptedQuoteCommitment(fxNetwork, proposal)
	if err != nil {
		t.Fatal(err)
	}
	var quoteHash [32]byte
	copy(quoteHash[:], quote.Hash())
	requestCell, err := nativecore.BuildServiceRequestCellV1(nativecore.ServiceRequestV1{QuoteCommitment: quoteHash,
		ExecutionID: digest32(1), InputDigest: digest32(2)})
	if err != nil {
		t.Fatal(err)
	}
	var requestHash [32]byte
	copy(requestHash[:], requestCell.Hash())
	call := fxCall(t, fxBuyer, 0, requestHash, uint32(fxNow.Unix())-100, 10, terms)
	snapshot, err := call.Snapshot.Decode()
	if err != nil {
		t.Fatal(err)
	}
	chain := &fxChain{checkpoint: 10, chainTime: fxNow,
		data:    fxData(t, policy, 1, map[uint64]nativecore.ServiceActorRequestSnapshotV1{0: snapshot}, nil),
		history: []toschain.ActorTransaction{call},
		capability: &nativev1.NativeStateV1{Network: fxNetwork, State: &nativev1.NativeStateV1_Capability{Capability: &nativev1.CapabilityStateV1{
			CapabilityId: fxCapability, OwnerAgentId: fxAgent, Versions: []*nativev1.CapabilityVersionV1{{Version: "1.0.0", ManifestDigest: digest}}}}},
		agent: &nativev1.NativeStateV1{Network: fxNetwork, State: &nativev1.NativeStateV1_Agent{Agent: &nativev1.AgentStateV1{AgentId: fxAgent}}}}
	verifier, err := NewVerifier(VerifierConfig{Reader: chain, Manifests: fxManifests{digest: manifestCBOR}, Network: fxNetwork,
		ActorCodeHash: fxCodeHash, ProviderAgentID: fxAgent, CapabilityID: fxCapability, Version: "1.0.0",
		ExecutionBudget: 180 * time.Second, RespondBudget: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{chain: chain, verifier: verifier, pkg: pkg, policy: policy, terms: terms, call: call, snapshot: snapshot,
		claim: Claim{Package: pkg, BuyerWallet: fxBuyer, ExecutionID: digest32(1), InputDigest: digest32(2), RequestID: 0,
			Call: toschain.ActorTxID{LT: call.LT, Hash: call.Hash}}}
}

// transition appends a lifecycle transaction and replaces the actor data.
func (f *fixture) transition(t *testing.T, kind string, now uint32, sender string, signed bool,
	pending, refunds map[uint64]nativecore.ServiceActorRequestSnapshotV1, payouts []toschain.ActorPayout) {
	t.Helper()
	f.chain.mu.Lock()
	defer f.chain.mu.Unlock()
	lt := f.chain.history[0].LT + 1
	f.chain.history = append([]toschain.ActorTransaction{{LT: lt, Hash: txHash(lt), Now: now, Kind: kind, RequestID: 0,
		Sender: sender, Signed: signed, Destination: fxBuyer, Payouts: payouts}}, f.chain.history...)
	f.chain.data = fxData(t, f.policy, 1, pending, refunds)
}
