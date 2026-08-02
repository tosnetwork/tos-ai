// Package edgegateway composes the generic TOS Edge Core with one private
// tos-ai Worker. It owns no wallet, chain key, runtime endpoint, or signing
// key; those authorities remain behind the supplied production boundaries.
package edgegateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
	"github.com/tosnetwork/tos-ai/pkg/edgeintegration"
	"github.com/tosnetwork/tos-protocol/pkg/ard"
	"github.com/tosnetwork/tos-protocol/pkg/authorization"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"github.com/tosnetwork/tos-protocol/pkg/payment"
	"github.com/tosnetwork/tos-protocol/pkg/protocol"
)

const (
	DefaultPaidActionRetention  = 24 * time.Hour
	DefaultReceiptLifetime      = 5 * time.Minute
	DefaultPaidActionConcurrent = 4
)

// Config contains only immutable, startup-reviewed composition policy. The
// manifest envelope is operator-loaded and must match current chain authority;
// no request or discovery document can replace it.
type Config struct {
	Descriptor              protocol.ServiceDescriptor
	Catalog                 ard.Catalog
	ManifestEnvelope        identity.Envelope
	Reference               authorization.Reference
	RequiredDelegationScope string
	CoreConfig              edge.CoreConfig
	PaidActionRetention     time.Duration
	ReceiptLifetime         time.Duration
	PaidActionMaxConcurrent int
}

// Dependencies are already constructed trust boundaries. Keeping them as
// interfaces makes the composition testable without weakening the production
// command, which supplies quorum-backed TOS adapters and private Unix clients.
type Dependencies struct {
	AuthorityResolver       authorization.Resolver
	ClientKeyResolver       authorization.ClientKeyResolver
	PaymentObserver         *payment.Observer
	ChainReadiness          edge.ReadinessChecker
	Worker                  *localrpc.WorkerClient
	ReceiptSigner           authorization.ReceiptSigner
	ReceiptReadiness        edge.ReadinessChecker
	ReceiptAuthorizer       edge.ReceiptDeliveryAuthorizer
	ActionStatusAuthorizer  edge.ActionStatusAuthorizer
	PaidActionErrorReporter edge.PaidActionErrorReporter
}

// Gateway is a fully composed non-streaming public handler plus its durable
// Edge Core. It owns and closes only the Core it opened; Worker, signer, and
// chain clients have independent process lifecycles.
type Gateway struct {
	handler   http.Handler
	core      *edge.Core
	closeOnce sync.Once
	closeErr  error
}

// Open verifies all cross-component bindings before returning a public
// handler. Partial paid-action configuration is rejected by construction.
func Open(ctx context.Context, config Config, dependencies Dependencies) (*Gateway, error) {
	if ctx == nil {
		return nil, errors.New("nil AI Edge gateway startup context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, dependency := range []any{
		dependencies.AuthorityResolver, dependencies.ClientKeyResolver,
		dependencies.ChainReadiness, dependencies.ReceiptSigner,
		dependencies.ReceiptReadiness, dependencies.ReceiptAuthorizer,
		dependencies.ActionStatusAuthorizer, dependencies.PaidActionErrorReporter,
	} {
		if dependency != nil && nilcheck.IsNil(dependency) {
			return nil, errors.New("typed-nil AI Edge gateway dependency")
		}
	}
	if nilcheck.IsNil(dependencies.AuthorityResolver) ||
		nilcheck.IsNil(dependencies.ClientKeyResolver) ||
		dependencies.PaymentObserver == nil || nilcheck.IsNil(dependencies.ChainReadiness) ||
		dependencies.Worker == nil || nilcheck.IsNil(dependencies.ReceiptSigner) ||
		nilcheck.IsNil(dependencies.ReceiptReadiness) {
		return nil, errors.New("incomplete AI Edge gateway trust dependencies")
	}
	if config.Reference.Network != config.Descriptor.Network ||
		config.Reference.ServiceID != config.Descriptor.ServiceID ||
		config.Reference.Address != config.Descriptor.Controller {
		return nil, errors.New("AI Edge gateway chain reference does not match descriptor")
	}
	if config.PaidActionRetention == 0 {
		config.PaidActionRetention = DefaultPaidActionRetention
	}
	if config.ReceiptLifetime == 0 {
		config.ReceiptLifetime = DefaultReceiptLifetime
	}
	if config.PaidActionMaxConcurrent == 0 {
		config.PaidActionMaxConcurrent = DefaultPaidActionConcurrent
	}
	if config.CoreConfig.RequestJournalPath == "" {
		return nil, errors.New("AI Edge gateway requires a durable request journal")
	}
	config.CoreConfig.PaymentObserver = dependencies.PaymentObserver
	if config.CoreConfig.PaymentReconciliationInterval == 0 {
		config.CoreConfig.PaymentReconciliationInterval = edge.DefaultPaymentReconciliationInterval
	}
	if config.CoreConfig.PaymentReconciliationMaxInterval == 0 {
		config.CoreConfig.PaymentReconciliationMaxInterval = edge.DefaultPaymentReconciliationMaxInterval
	}
	if config.CoreConfig.PaymentReconciliationTimeout == 0 {
		config.CoreConfig.PaymentReconciliationTimeout = edge.DefaultPaymentReconciliationTimeout
	}
	if config.CoreConfig.PaymentReconciliationBatch == 0 {
		config.CoreConfig.PaymentReconciliationBatch = edge.DefaultPaymentReconciliationBatch
	}

	workerDeployment, err := edgeintegration.New(ctx, dependencies.Worker)
	if err != nil {
		return nil, fmt.Errorf("compose AI Worker profile: %w", err)
	}
	profilePlan, err := workerDeployment.ProfilePlan()
	if err != nil {
		return nil, fmt.Errorf("load AI Worker profile plan: %w", err)
	}
	verifier, err := authorization.NewVerifier(authorization.DefaultPolicy())
	if err != nil {
		return nil, fmt.Errorf("configure protocol verifier: %w", err)
	}
	actionAuthorizer, err := authorization.NewPaidActionAuthorizer(
		authorization.PaidActionAuthorizerConfig{
			Verifier: verifier, AuthorityResolver: dependencies.AuthorityResolver,
			ClientKeyResolver:  dependencies.ClientKeyResolver,
			Reference:          config.Reference,
			ManifestEnvelope:   config.ManifestEnvelope,
			RequiredScope:      config.RequiredDelegationScope,
			InitialMasterSeqno: config.Reference.MinimumMasterSeqno,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("configure paid-action authority: %w", err)
	}
	httpAuthorizer, err := edge.NewJSONPaidActionAuthorizer(actionAuthorizer)
	if err != nil {
		return nil, fmt.Errorf("configure paid-action ingress: %w", err)
	}
	core, err := edge.OpenCore(config.CoreConfig)
	if err != nil {
		return nil, fmt.Errorf("open AI Edge journal: %w", err)
	}
	fail := func(startupErr error) (*Gateway, error) {
		_ = core.Close()
		return nil, startupErr
	}
	server, err := edge.NewServerWithDependencies(
		config.Descriptor, config.Catalog, time.Now().UTC(),
		edge.ServerDependencies{
			Core: core, ChainReadiness: dependencies.ChainReadiness,
			ReceiptSignerReadiness: dependencies.ReceiptReadiness,
			ProfileReadiness:       workerDeployment,
			ReceiptAuthorizer:      dependencies.ReceiptAuthorizer,
			ReceiptSource:          receiptSourceFor(dependencies.ReceiptAuthorizer, core),
			ActionStatusAuthorizer: dependencies.ActionStatusAuthorizer,
			PaidActionAuthorizer:   httpAuthorizer,
			PaymentObserver:        dependencies.PaymentObserver,
			ProfilePlan:            profilePlan, Worker: dependencies.Worker,
			ReceiptSigner:           dependencies.ReceiptSigner,
			PaidActionRetention:     config.PaidActionRetention,
			ReceiptLifetime:         config.ReceiptLifetime,
			PaidActionMaxConcurrent: config.PaidActionMaxConcurrent,
			PaidActionErrorReporter: dependencies.PaidActionErrorReporter,
		},
	)
	if err != nil {
		return fail(fmt.Errorf("configure AI Edge HTTP server: %w", err))
	}
	return &Gateway{handler: server.Routes(), core: core}, nil
}

func receiptSourceFor(authorizer edge.ReceiptDeliveryAuthorizer, core *edge.Core) edge.ReceiptSource {
	if authorizer == nil {
		return nil
	}
	return core
}

func (g *Gateway) Handler() (http.Handler, error) {
	if g == nil || g.handler == nil || g.core == nil {
		return nil, errors.New("invalid AI Edge gateway")
	}
	return g.handler, nil
}

func (g *Gateway) Core() (*edge.Core, error) {
	if g == nil || g.core == nil {
		return nil, errors.New("invalid AI Edge gateway")
	}
	return g.core, nil
}

func (g *Gateway) Close() error {
	if g == nil {
		return nil
	}
	g.closeOnce.Do(func() {
		if g.core != nil {
			g.closeErr = g.core.Close()
		}
	})
	return g.closeErr
}
