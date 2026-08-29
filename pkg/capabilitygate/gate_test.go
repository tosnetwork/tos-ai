package capabilitygate

import (
	"context"
	"errors"
	"testing"
	"time"

	trusted "github.com/tosnetwork/tos-service-protocol/pkg/trustedcapability"
)

type resolverFunc func(context.Context, []byte, []byte, []byte) (AuthorityHeads, error)
type journalFunc func(context.Context, []byte, AuthorityHeads, []byte, []byte, []byte, TrustedTimeObservation) error
type clockFunc func(context.Context) (TrustedTimeObservation, error)

func (function journalFunc) LinearizeCapabilityStart(ctx context.Context, s []byte, h AuthorityHeads, e, a, d []byte, observed TrustedTimeObservation) error {
	return function(ctx, s, h, e, a, d, observed)
}

func (function clockFunc) ObserveTrustedTime(ctx context.Context) (TrustedTimeObservation, error) {
	return function(ctx)
}

func (function resolverFunc) ResolveCapabilityHeads(ctx context.Context, o, a, d []byte) (AuthorityHeads, error) {
	return function(ctx, o, a, d)
}

func TestGateFailsClosedWhenAuthorityCannotBeResolved(t *testing.T) {
	gate, err := New(trusted.DomainOwnerLocal, []byte("domain"), []byte("sink"), resolverFunc(func(context.Context, []byte, []byte, []byte) (AuthorityHeads, error) {
		return AuthorityHeads{}, errors.New("partition")
	}), journalFunc(func(context.Context, []byte, AuthorityHeads, []byte, []byte, []byte, TrustedTimeObservation) error {
		return nil
	}), clockFunc(func(context.Context) (TrustedTimeObservation, error) {
		return TrustedTimeObservation{UnixSeconds: uint64(time.Unix(100, 0).Unix()), Epoch: 1, EvidenceDigest: make([]byte, 32)}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Admit(context.Background(), Request{}); err == nil {
		t.Fatal("authority outage admitted execution")
	}
}
