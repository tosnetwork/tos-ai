package edgegateway_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/authorization"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"github.com/tosnetwork/tos-protocol/pkg/protocol"
	"github.com/tosnetwork/tos-protocol/pkg/receiptsigner"
	"github.com/tosnetwork/tos-protocol/pkg/toschain"
)

// TestLiveThreeNodeDiscoveryToReceipt is opt-in deployment evidence. It uses
// the production tos-ai-edge process, private Worker and signer sockets, two
// real Agent Accounts, and one exact finalized payment on the local TOS chain.
func TestLiveThreeNodeDiscoveryToReceipt(t *testing.T) {
	configPath := os.Getenv("TOS_AI_LIVE_EDGE_CONFIG")
	if configPath == "" {
		t.Skip("live AI Edge deployment is not configured")
	}
	required := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("%s is required", name)
		}
		return value
	}
	baseURL := required("TOS_AI_LIVE_BASE_URL")
	clientAccount := required("TOS_AI_LIVE_CLIENT_ACCOUNT")
	clientSeedPath := required("TOS_AI_LIVE_CLIENT_SEED")
	paymentReference := required("TOS_AI_LIVE_PAYMENT_REFERENCE")
	payee := required("TOS_AI_LIVE_PAYEE")
	runID := os.Getenv("TOS_AI_LIVE_RUN_ID")
	if runID == "" {
		runID = "0001"
	}
	sessionID := fmt.Sprintf("session-m1-live-%s", runID)
	requestID := fmt.Sprintf("request-m1-live-%s", runID)
	authorizationID := fmt.Sprintf("authorization-m1-live-%s", runID)

	config, err := operatorconfig.LoadEdgeGatewayConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	documents, err := config.LoadDocuments(now)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := documents.Chain.BuildRuntime()
	if err != nil {
		t.Fatal(err)
	}
	reference := config.Reference(documents.Descriptor)
	verifier, err := authorization.NewVerifier(authorization.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manifest, err := verifier.ResolveAndVerifyManifest(
		ctx, runtime.Authority, reference, documents.ManifestEnvelope, now,
	)
	if err != nil {
		t.Fatal(err)
	}

	sessionConfig := localrpc.DefaultSessionSignerClientConfig(
		required("TOS_AI_LIVE_SESSION_SOCKET"),
	)
	sessionConfig.ExpectedKeyID = required("TOS_AI_LIVE_SESSION_KEY_ID")
	sessionConfig.ExpectedPublicKey = required("TOS_AI_LIVE_SESSION_PUBLIC_KEY")
	sessionSigner, err := localrpc.NewSessionSignerClient(sessionConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer sessionSigner.Close()
	if err := sessionSigner.CheckReady(ctx); err != nil {
		t.Fatal(err)
	}
	quoteConfig := localrpc.DefaultQuoteSignerClientConfig(
		required("TOS_AI_LIVE_QUOTE_SOCKET"),
	)
	quoteConfig.ExpectedKeyID = required("TOS_AI_LIVE_QUOTE_KEY_ID")
	quoteConfig.ExpectedPublicKey = required("TOS_AI_LIVE_QUOTE_PUBLIC_KEY")
	quoteSigner, err := localrpc.NewQuoteSignerClient(quoteConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer quoteSigner.Close()
	if err := quoteSigner.CheckReady(ctx); err != nil {
		t.Fatal(err)
	}
	workerConfig := localrpc.DefaultWorkerClientConfig(
		required("TOS_AI_LIVE_WORKER_SOCKET"),
	)
	worker, err := localrpc.NewWorkerClient(workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.CheckReady(ctx); err != nil {
		t.Fatal(err)
	}
	clientPrivate, err := receiptsigner.LoadPrivateKey(clientSeedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer zeroPrivateKey(clientPrivate)
	clientKeyID, err := toschain.FormatAgentClientKeyID(
		clientAccount, clientPrivate.Public().(ed25519.PublicKey),
	)
	if err != nil {
		t.Fatal(err)
	}

	sessionExpires := now.Add(15 * time.Minute)
	_, sessionEnvelope, err := manifest.IssueSessionGrant(
		ctx,
		authorization.SessionDraft{
			SessionID: sessionID, ProfileID: "tos.ai.text-generation",
			ProfileVersion: "0.1.0", Client: clientKeyID,
			RuntimeKeyID: sessionConfig.ExpectedKeyID, Operations: []string{"generate"},
			MaxRequests: 4, MaxNanoTOS: 4_000_000_000, ExpiresAt: sessionExpires,
		},
		sessionSigner, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	expectedOutput := []byte("hello from the real local TOS chain")
	intent := []byte(`{"model":"deterministic-echo","prompt":"hello from the real local TOS chain"}`)
	intentDigest, err := protocol.RequestIntentDigest(
		"tos.ai.text-generation", "0.1.0", nil, "generate", intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Chain-backed manifest/session authority is intentionally leased for only
	// five minutes, so a live Quote must fit inside that fresh observation.
	invocationDeadline := now.Add(4 * time.Minute)
	workerQuote, err := worker.Quote(ctx, &edgev1.QuoteRequest{
		RequestId: requestID, ServiceId: documents.Descriptor.ServiceID,
		Operation: "generate", Model: "deterministic-echo",
		InputBytes: uint64(len(expectedOutput)), MaxOutputBytes: 4096,
		DeadlineUnixMillis: invocationDeadline.UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	})
	if err != nil {
		t.Fatal(err)
	}
	quoteExpires := time.UnixMilli(workerQuote.ExpiresUnixMillis).UTC()
	verifiedSession, err := manifest.VerifySessionGrant(sessionEnvelope, now)
	if err != nil {
		t.Fatal(err)
	}
	verifiedQuote, err := manifest.IssueQuote(
		ctx, verifiedSession,
		authorization.QuoteDraft{
			QuoteID: workerQuote.QuoteId, RequestID: requestID,
			Operation: "generate", IntentDigest: intentDigest,
			ResourceRevision: workerQuote.CapacityRevision, Payee: payee,
			Settlement: paymentReference, PriceNanoTOS: 1_000_000_000,
			MaxInputBytes: 4096, MaxOutputBytes: 4096,
			ResourceLimits: protocolResourceLimits(t, workerQuote.CommittedLimits),
			Deadline:       invocationDeadline, ExpiresAt: quoteExpires,
		},
		quoteSigner, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	quoteEnvelope, err := verifiedQuote.SignedEnvelope(now)
	if err != nil {
		t.Fatal(err)
	}
	paymentEnvelope, err := identity.SignCanonical(
		clientPrivate, protocol.PaymentAuthorizationDomain, clientKeyID,
		protocol.PaymentAuthorization{
			Version:         protocol.BaseEnvelopeVersion,
			AuthorizationID: authorizationID,
			QuoteID:         workerQuote.QuoteId, RequestID: requestID,
			Network: documents.Descriptor.Network, Payer: clientAccount, Payee: payee,
			MaxNanoTOS: 1_000_000_000, Reference: paymentReference,
			ExpiresAt: quoteExpires,
		},
		now, quoteExpires,
	)
	if err != nil {
		t.Fatal(err)
	}
	authorizedPayment, err := verifiedQuote.AuthorizePayment(
		ctx, verifiedSession, runtime.ClientKeys, reference.MinimumMasterSeqno,
		nil, paymentEnvelope, config.RequiredDelegationScope, now,
	)
	if err != nil {
		t.Fatalf("authorize live payment: %v", err)
	}
	if _, err := runtime.Payments.Observe(
		ctx, authorizedPayment, reference.MinimumMasterSeqno, now,
	); err != nil {
		t.Fatalf("observe live payment: %v", err)
	}
	document := struct {
		Version              string            `json:"version"`
		Intent               []byte            `json:"intent"`
		SessionGrant         identity.Envelope `json:"sessionGrant"`
		Quote                identity.Envelope `json:"quote"`
		PaymentAuthorization identity.Envelope `json:"paymentAuthorization"`
	}{
		Version: protocol.BaseEnvelopeVersion, Intent: intent,
		SessionGrant: sessionEnvelope, Quote: quoteEnvelope,
		PaymentAuthorization: paymentEnvelope,
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if capture := os.Getenv("TOS_AI_LIVE_REQUEST_CAPTURE"); capture != "" {
		if err := os.WriteFile(capture, body, 0o600); err != nil {
			t.Fatalf("capture exact live request: %v", err)
		}
	}

	for _, path := range []string{"/.well-known/tos-service.json", "/.well-known/ai-catalog.json", "/readyz"} {
		response, err := http.Get(baseURL + path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s returned %s", path, response.Status)
		}
	}
	first := invokeLiveAction(t, baseURL, body)
	t.Logf("first paid action result: status=%s receipt=%t", first.Status, first.Receipt != nil)
	second := invokeLiveAction(t, baseURL, body)
	t.Logf("replayed paid action result: status=%s receipt=%t", second.Status, second.Receipt != nil)
	if first.Status != "succeeded" || second.Status != first.Status ||
		!bytes.Equal(first.Output, expectedOutput) ||
		!bytes.Equal(second.Output, first.Output) || first.Receipt == nil || second.Receipt == nil {
		t.Fatalf("unexpected action results: first=%+v second=%+v", first, second)
	}
	firstFingerprint, err := first.Receipt.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := second.Receipt.Fingerprint()
	if err != nil || secondFingerprint != firstFingerprint {
		t.Fatal("exact action replay did not return the durable signed receipt")
	}
	t.Logf("three-node discovery-to-receipt completed: receipt=%s", firstFingerprint)
}

type liveActionResult struct {
	Version string             `json:"version"`
	Status  string             `json:"status"`
	Output  []byte             `json:"output"`
	Receipt *identity.Envelope `json:"receipt"`
}

func invokeLiveAction(t *testing.T, baseURL string, body []byte) liveActionResult {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodPost, baseURL+"/tos/v1/actions", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 24<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("paid action returned %s: %s", response.Status, data)
	}
	var result liveActionResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func protocolResourceLimits(
	t *testing.T,
	limits []*edgev1.ResourceLimit,
) []protocol.ResourceLimit {
	t.Helper()
	result := make([]protocol.ResourceLimit, 0, len(limits))
	for _, limit := range limits {
		if limit == nil {
			t.Fatal("worker Quote returned a nil resource limit")
		}
		var unit protocol.ResourceUnit
		switch limit.Unit {
		case edgev1.ResourceUnit_RESOURCE_UNIT_COUNT:
			unit = protocol.ResourceUnitCount
		case edgev1.ResourceUnit_RESOURCE_UNIT_BYTES:
			unit = protocol.ResourceUnitBytes
		case edgev1.ResourceUnit_RESOURCE_UNIT_MILLISECONDS:
			unit = protocol.ResourceUnitMilliseconds
		default:
			t.Fatalf("worker Quote returned unsupported resource unit %s", limit.Unit)
		}
		result = append(result, protocol.ResourceLimit{
			ID: limit.Id, Unit: unit, Quantity: limit.Quantity,
		})
	}
	return result
}

func zeroPrivateKey(key ed25519.PrivateKey) {
	for index := range key {
		key[index] = 0
	}
}
