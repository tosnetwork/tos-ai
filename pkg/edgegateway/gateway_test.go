package edgegateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/ard"
	"github.com/tosnetwork/tos-protocol/pkg/authorization"
	"github.com/tosnetwork/tos-protocol/pkg/chain"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"github.com/tosnetwork/tos-protocol/pkg/payment"
	"github.com/tosnetwork/tos-protocol/pkg/protocol"
)

type capabilityWorker struct {
	edgev1connect.UnimplementedWorkerServiceHandler
}

func (capabilityWorker) GetCapabilities(
	_ context.Context,
	_ *connect.Request[edgev1.GetCapabilitiesRequest],
) (*connect.Response[edgev1.GetCapabilitiesResponse], error) {
	now := time.Now().UTC()
	return connect.NewResponse(&edgev1.GetCapabilitiesResponse{
		CapacityRevision: "capacity-1", TerminalRevision: "terminal-1",
		CollectedUnixMillis: now.UnixMilli(),
		ExpiresUnixMillis:   now.Add(time.Minute).UnixMilli(),
		Capabilities: []*edgev1.Capability{{
			ServiceId: "tos.ai.mock", Operation: "generate", Model: "model-a",
			ModelDigest: "sha256:" + strings.Repeat("a", 64),
			Runtime:     "mock", RuntimeRevision: "runtime-1",
			MaxInputBytes: 1024, MaxOutputBytes: 1024,
			AcceptedPriorities: []edgev1.Priority{
				edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
			},
		}},
	}), nil
}

type inertAuthorityResolver struct{}

func (inertAuthorityResolver) ResolveAuthority(
	context.Context, authorization.Reference,
) (authorization.AuthoritySnapshot, error) {
	return authorization.AuthoritySnapshot{}, errors.New("not called during startup")
}

type inertClientKeyResolver struct{}

func (inertClientKeyResolver) ResolveClientKey(
	context.Context, authorization.ClientKeyReference,
) (authorization.ClientKeySnapshot, error) {
	return authorization.ClientKeySnapshot{}, errors.New("not called during startup")
}

type inertPaymentResolver struct{}

func (inertPaymentResolver) ObservePayment(
	context.Context, chain.PaymentReference,
) (chain.PaymentState, error) {
	return chain.PaymentState{}, errors.New("not called during startup")
}

type readyDependency struct{}

func (readyDependency) CheckReady(context.Context) error { return nil }

type inertReceiptSigner struct{}

func (inertReceiptSigner) SignReceipt(
	context.Context, []byte, time.Time, time.Time,
) (identity.Envelope, error) {
	return identity.Envelope{}, errors.New("not called during startup")
}

func TestOpenRequiresCompleteTrustComposition(t *testing.T) {
	client, closeWorker := newCapabilityWorkerClient(t)
	defer closeWorker()
	config := validGatewayConfig(t)
	observer, err := payment.NewObserver(inertPaymentResolver{}, payment.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{
		AuthorityResolver: inertAuthorityResolver{},
		ClientKeyResolver: inertClientKeyResolver{}, PaymentObserver: observer,
		ChainReadiness: readyDependency{}, Worker: client,
		ReceiptSigner: inertReceiptSigner{}, ReceiptReadiness: readyDependency{},
	}
	gateway, err := Open(context.Background(), config, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	handler, err := gateway.Handler()
	if err != nil || handler == nil {
		t.Fatalf("handler=%v err=%v", handler, err)
	}
	if _, err := gateway.Core(); err != nil {
		t.Fatal(err)
	}

	partial := dependencies
	partial.ReceiptReadiness = nil
	if _, err := Open(context.Background(), config, partial); err == nil {
		t.Fatal("partial signer readiness was accepted")
	}
	wrongReference := config
	wrongReference.Reference.ServiceID = "different.service"
	if _, err := Open(context.Background(), wrongReference, dependencies); err == nil {
		t.Fatal("descriptor/reference mismatch was accepted")
	}
}

// TestLocalGateTLSMalformedInputSoak exercises the complete public gateway
// composition over TLS with a bounded concurrent anonymous-input sample. It
// deliberately uses malformed requests so no chain, payment, signer, or
// Worker mock can turn the test into a false successful-service claim. The
// important invariant is that every request is rejected before durable state
// is created and the gateway remains live after the load subsides. Full
// readiness is dependency-sensitive and is covered separately.
func TestLocalGateTLSMalformedInputSoak(t *testing.T) {
	client, closeWorker := newCapabilityWorkerClient(t)
	defer closeWorker()
	config := validGatewayConfig(t)
	journalPath := config.CoreConfig.RequestJournalPath
	observer, err := payment.NewObserver(inertPaymentResolver{}, payment.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := Open(context.Background(), config, Dependencies{
		AuthorityResolver: inertAuthorityResolver{},
		ClientKeyResolver: inertClientKeyResolver{},
		PaymentObserver:   observer, ChainReadiness: readyDependency{},
		Worker: client, ReceiptSigner: inertReceiptSigner{},
		ReceiptReadiness: readyDependency{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	handler, err := gateway.Handler()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()

	const requestCount = 2_048
	const concurrency = 32
	jobs := make(chan struct{})
	errorsSeen := make(chan error, requestCount)
	var workers sync.WaitGroup
	var rejected atomic.Uint64
	var saturated atomic.Uint64
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range jobs {
				request, requestErr := http.NewRequest(
					http.MethodPost, server.URL+"/tos/v1/actions",
					bytes.NewBufferString("{"),
				)
				if requestErr != nil {
					errorsSeen <- requestErr
					continue
				}
				request.Header.Set("Content-Type", "application/json")
				response, requestErr := server.Client().Do(request)
				if requestErr != nil {
					errorsSeen <- requestErr
					continue
				}
				if response.TLS == nil || response.TLS.Version != tls.VersionTLS13 {
					errorsSeen <- errors.New("request did not use TLS 1.3")
				}
				switch response.StatusCode {
				case http.StatusUnauthorized:
					rejected.Add(1)
				case http.StatusServiceUnavailable:
					// The paid-action admission bound is intentionally lower
					// than this test's concurrency. Saturation must shed load
					// without allocating durable request state.
					saturated.Add(1)
				default:
					errorsSeen <- fmt.Errorf(
						"malformed request returned unexpected HTTP %d",
						response.StatusCode,
					)
				}
				_ = response.Body.Close()
			}
		}()
	}
	for range requestCount {
		jobs <- struct{}{}
	}
	close(jobs)
	workers.Wait()
	close(errorsSeen)
	for requestErr := range errorsSeen {
		t.Fatal(requestErr)
	}
	if rejected.Load() == 0 || rejected.Load()+saturated.Load() != requestCount {
		t.Fatalf(
			"malformed-input outcomes rejected=%d saturated=%d total=%d",
			rejected.Load(), saturated.Load(), requestCount,
		)
	}

	live, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = live.Body.Close()
	if live.StatusCode != http.StatusOK {
		t.Fatalf("gateway not live after malformed-input soak: %s", live.Status)
	}
	info, err := os.Stat(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 64<<10 {
		t.Fatalf("malformed input unexpectedly grew journal to %d bytes", info.Size())
	}
}

func validGatewayConfig(t *testing.T) Config {
	t.Helper()
	now := time.Now().UTC()
	controller := "0:" + strings.Repeat("1", 64)
	return Config{
		Descriptor: protocol.ServiceDescriptor{
			ProtocolVersion: protocol.DescriptorVersion,
			ServiceID:       "tos.ai.mock", DisplayName: "TOS AI Test",
			Controller: controller, Network: "tos-local", Revision: "revision-1",
			ExpiresAt: now.Add(time.Hour),
			Profiles: []protocol.ProfileReference{{
				ID: "tos.ai.text-generation", Version: "0.1.0",
				MediaType: "application/json", URL: "https://edge.example/profile.json",
				Digest: "sha256:" + strings.Repeat("2", 64),
			}},
		},
		Catalog: ard.Catalog{SpecVersion: ard.SpecVersion},
		ManifestEnvelope: identity.Envelope{
			Domain: protocol.ServiceManifestDomain, KeyID: "controller",
			Payload: []byte{1}, Signature: "AA",
		},
		Reference: authorization.Reference{
			Network: "tos-local", Address: controller, ServiceID: "tos.ai.mock",
		},
		CoreConfig: edge.DefaultCoreConfig(filepath.Join(t.TempDir(), "edge.db")),
	}
}

func newCapabilityWorkerClient(t *testing.T) (*localrpc.WorkerClient, func()) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "worker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	path, handler := edgev1connect.NewWorkerServiceHandler(capabilityWorker{})
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	client, err := localrpc.NewWorkerClient(localrpc.DefaultWorkerClientConfig(socket))
	if err != nil {
		server.Close()
		<-done
		t.Fatal(err)
	}
	closeAll := func() {
		_ = server.Close()
		<-done
	}
	return client, closeAll
}

var (
	_ authorization.Resolver          = inertAuthorityResolver{}
	_ authorization.ClientKeyResolver = inertClientKeyResolver{}
	_ payment.Resolver                = inertPaymentResolver{}
	_ edge.ReadinessChecker           = readyDependency{}
	_ authorization.ReceiptSigner     = inertReceiptSigner{}
)
