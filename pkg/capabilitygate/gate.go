// Package capabilitygate enforces Trusted Capability V1 at the executor sink.
package capabilitygate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	trusted "github.com/tosnetwork/tos-service-protocol/pkg/trustedcapability"
)

type AuthorityHeads struct {
	AuthorityEpoch, PolicyRevision, AdmissionRevocationGeneration, PromotionRevocationGeneration, ControlScopeGeneration uint64
	AdmissionRevision, PromotionRevision, InstallationRevision, InventoryRevision                                        uint64
	PolicyDigest, AdmissionEnvelopeDigest, PromotionEnvelopeDigest, PermissionManifestDigest                             []byte
	AdmittedPermissionManifest                                                                                           trusted.CapabilityPermissionManifestV1
	LeaseIssuerSubject                                                                                                   trusted.TypedAuthoritySubjectV1
	LeaseAuthorityID                                                                                                     []byte
	LeaseProofProfileURI                                                                                                 string
	OwnerID, AgentID                                                                                                     []byte
}
type HeadResolver interface {
	ResolveCapabilityHeads(context.Context, []byte, []byte, []byte) (AuthorityHeads, error)
}

// SinkJournal must atomically enforce monotonic heads and one-shot execution
// identity in rollback-resistant storage. Returning nil is the linearization
// point; an in-memory implementation is not production-conforming.
type SinkJournal interface {
	LinearizeCapabilityStart(context.Context, []byte, AuthorityHeads, []byte, []byte, []byte, TrustedTimeObservation) error
}

type TrustedTimeObservation struct {
	UnixSeconds    uint64
	Epoch          uint64
	EvidenceDigest []byte
}

type TrustedTimeSource interface {
	ObserveTrustedTime(context.Context) (TrustedTimeObservation, error)
}
type Request struct {
	Binding                trusted.CapabilityUseBindingV1
	LeaseObject            trusted.ProfileObjectV1
	LeaseEnvelope          trusted.ProfileAuthorizationEnvelopeV1
	PermissionSubsetObject trusted.ProfileObjectV1
	Remote                 bool
	Observed               ObservedUseContext
}

// ObservedUseContext is measured or resolved by the sink. It must never be
// copied from the caller's binding.
type ObservedUseContext struct {
	LoadedObjectDigest                     []byte
	InstallationRevision                   uint64
	RuntimeAndSandboxDigest                []byte
	EffectiveEnvironmentDigest             []byte
	CredentialCapabilityReferenceSetDigest []byte
	FilesystemHandleSetDigest              []byte
	NetworkBrokerPolicyDigest              []byte
	RemoteSessionHandshakeDigest           *[]byte
}

type Gate struct {
	domainKind uint8
	domainID   []byte
	sinkID     []byte
	heads      HeadResolver
	clock      TrustedTimeSource
	journal    SinkJournal
}

func New(domainKind trusted.DomainKind, domainID, sinkID []byte, heads HeadResolver, journal SinkJournal, clock TrustedTimeSource) (*Gate, error) {
	if (domainKind != trusted.DomainTOSNetwork && domainKind != trusted.DomainOwnerLocal) || len(domainID) == 0 || len(sinkID) == 0 || heads == nil || journal == nil || clock == nil {
		return nil, errors.New("capability Gate requires sink identity, authority resolver, rollback-resistant journal, and trusted time")
	}
	return &Gate{uint8(domainKind), append([]byte(nil), domainID...), append([]byte(nil), sinkID...), heads, clock, journal}, nil
}

func (gate *Gate) Admit(ctx context.Context, request Request) error {
	if gate == nil || ctx == nil {
		return errors.New("capability Gate is unavailable")
	}
	if err := trusted.ValidateUseBindingShape(request.Binding, request.Remote); err != nil {
		return err
	}
	heads, err := gate.heads.ResolveCapabilityHeads(ctx, request.Binding.OwnerID, request.Binding.AgentID, request.Binding.ArtifactVersionDigest)
	if err != nil {
		return err
	}
	timeObservation, err := gate.clock.ObserveTrustedTime(ctx)
	if err != nil || timeObservation.UnixSeconds == 0 || timeObservation.Epoch == 0 || len(timeObservation.EvidenceDigest) != sha256.Size {
		return errors.New("trusted execution time is unavailable")
	}
	now := timeObservation.UnixSeconds
	var lease trusted.CapabilityUseLeaseV1
	if err := trusted.DecodeBody(request.LeaseObject, "use-lease", &lease); err != nil {
		return err
	}
	if err := trusted.ValidateUseLease(lease, now, heads.AuthorityEpoch, heads.AdmissionRevocationGeneration, heads.PromotionRevocationGeneration); err != nil {
		return err
	}
	if err := trusted.VerifyAuthorization(request.LeaseEnvelope, request.LeaseObject, now, heads.AuthorityEpoch); err != nil {
		return err
	}
	envelope := request.LeaseEnvelope.Body
	if envelope.AuthorityKind != "use-lease" || !bytes.Equal(envelope.AuthorityID, heads.LeaseAuthorityID) ||
		envelope.ProofProfileURI != heads.LeaseProofProfileURI || !sameSubject(envelope.IssuerSubject, heads.LeaseIssuerSubject) ||
		!bytes.Equal(envelope.OwnerID, heads.OwnerID) || envelope.AgentID == nil || !bytes.Equal(*envelope.AgentID, heads.AgentID) ||
		!bytes.Equal(lease.OwnerID, heads.OwnerID) || !bytes.Equal(lease.AgentID, heads.AgentID) ||
		envelope.PolicyRevision != heads.PolicyRevision || !bytes.Equal(envelope.PolicyDigest, heads.PolicyDigest) ||
		envelope.AuthorityEpoch != heads.AuthorityEpoch {
		return errors.New("capability Gate rejected self-selected lease authority")
	}
	var selectedPermissions trusted.CapabilityPermissionManifestV1
	if err := trusted.DecodeBody(request.PermissionSubsetObject, "permission-manifest", &selectedPermissions); err != nil {
		return err
	}
	selectedDigest, err := trusted.ObjectDigest(request.PermissionSubsetObject)
	if err != nil || !bytes.Equal(selectedDigest, request.Binding.PermissionSubsetDigest) ||
		trusted.PermissionSubsetOf(selectedPermissions, heads.AdmittedPermissionManifest) != nil {
		return errors.New("capability Gate rejected an unbound or expanded permission subset")
	}
	if request.LeaseObject.DomainKind != gate.domainKind || !bytes.Equal(request.LeaseObject.DomainID, gate.domainID) {
		return errors.New("capability Gate rejected cross-domain lease")
	}
	leaseDigest, err := trusted.ObjectDigest(request.LeaseObject)
	if err != nil {
		return err
	}
	if trusted.ValidateLeaseBinding(lease, request.Binding, gate.sinkID) != nil || !bytes.Equal(request.Binding.UseLeaseDigest, leaseDigest) || request.Binding.AuthorityEpoch != heads.AuthorityEpoch ||
		request.Binding.PolicyRevision != heads.PolicyRevision || !bytes.Equal(request.Binding.PolicyDigest, heads.PolicyDigest) || request.Binding.ControlScopeGeneration != heads.ControlScopeGeneration ||
		request.Binding.AdmissionRevision != heads.AdmissionRevision || !bytes.Equal(request.Binding.AdmissionEnvelopeDigest, heads.AdmissionEnvelopeDigest) ||
		request.Binding.AdmissionRevocationGeneration != heads.AdmissionRevocationGeneration || value(request.Binding.PromotionRevision) != heads.PromotionRevision ||
		value(request.Binding.PromotionRevocationGeneration) != heads.PromotionRevocationGeneration || !optionalEqual(request.Binding.PromotionEnvelopeDigest, heads.PromotionEnvelopeDigest) ||
		request.Binding.InstallationRevision != heads.InstallationRevision || request.Binding.InventoryRevision != heads.InventoryRevision {
		return errors.New("capability Gate rejected stale authority or sink binding")
	}
	observed := request.Observed
	if !bytes.Equal(observed.LoadedObjectDigest, request.Binding.LoadedObjectDigest) || observed.InstallationRevision != request.Binding.InstallationRevision ||
		!bytes.Equal(observed.RuntimeAndSandboxDigest, request.Binding.RuntimeAndSandboxDigest) ||
		!bytes.Equal(observed.EffectiveEnvironmentDigest, request.Binding.EffectiveEnvironmentDigest) ||
		!bytes.Equal(observed.CredentialCapabilityReferenceSetDigest, request.Binding.CredentialCapabilityReferenceSetDigest) ||
		!bytes.Equal(observed.FilesystemHandleSetDigest, request.Binding.FilesystemHandleSetDigest) ||
		!bytes.Equal(observed.NetworkBrokerPolicyDigest, request.Binding.NetworkBrokerPolicyDigest) ||
		!optionalObservedEqual(observed.RemoteSessionHandshakeDigest, request.Binding.RemoteSessionHandshakeDigest) {
		return errors.New("capability Gate rejected caller-selected or changed runtime resources")
	}
	scopeBytes, err := trusted.MarshalBody(struct {
		DomainKind uint8  `cbor:"1,keyasint"`
		DomainID   []byte `cbor:"2,keyasint"`
		SinkID     []byte `cbor:"3,keyasint"`
		OwnerID    []byte `cbor:"4,keyasint"`
		AgentID    []byte `cbor:"5,keyasint"`
	}{gate.domainKind, gate.domainID, gate.sinkID, request.Binding.OwnerID, request.Binding.AgentID})
	if err != nil {
		return err
	}
	scopeSum := sha256.Sum256(append([]byte("tos.capability-start-scope.v1\x00"), scopeBytes...))
	scope := scopeSum[:]
	bindingObject, err := trusted.NewObject(trusted.DomainKind(gate.domainKind), gate.domainID, "capability-use-binding", request.Binding)
	if err != nil {
		return err
	}
	exactRequestDigest, err := trusted.ObjectDigest(bindingObject)
	if err != nil {
		return err
	}
	return gate.journal.LinearizeCapabilityStart(ctx, scope, heads, request.Binding.ExecutionID, request.Binding.ActionID, exactRequestDigest, timeObservation)
}

func sameSubject(left, right trusted.TypedAuthoritySubjectV1) bool {
	return left.Kind == right.Kind && left.Namespace == right.Namespace && bytes.Equal(left.Identifier, right.Identifier)
}

func optionalEqual(binding *[]byte, resolved []byte) bool {
	return binding == nil && len(resolved) == 0 || binding != nil && bytes.Equal(*binding, resolved)
}

func optionalObservedEqual(left, right *[]byte) bool {
	return left == nil && right == nil || left != nil && right != nil && bytes.Equal(*left, *right)
}

func value(input *uint64) uint64 {
	if input == nil {
		return 0
	}
	return *input
}
