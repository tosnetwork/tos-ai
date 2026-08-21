package adapterinterop

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tosnetwork/tos-ai/pkg/a2aadapter"
	"github.com/tosnetwork/tos-ai/pkg/adapterhttp"
	"github.com/tosnetwork/tos-ai/pkg/agentpacketadapter"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-service-protocol/pkg/agentpacket"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
)

const bearerToken = "0123456789abcdef0123456789abcdef"

type sharedGate struct {
	mu      sync.Mutex
	claimed bool
	calls   int
}

func (g *sharedGate) ClaimExecution(_ context.Context, request executiongate.Request) (executiongate.Evidence, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls++
	if g.claimed {
		return executiongate.Evidence{}, errors.New("purchase slot already claimed")
	}
	g.claimed = true
	return executiongate.Evidence{NetworkID: "interop", ProviderAgentID: "agent_" + strings.Repeat("10", 32),
		CapabilityID: "cap_" + strings.Repeat("11", 32), CapabilityVersion: "1.0.0",
		ManifestDigest: "sha256:" + strings.Repeat("22", 32), QuoteCommitment: request.QuoteCommitment,
		EscrowAddress: request.EscrowAddress, ProviderAddress: "0:" + strings.Repeat("34", 32),
		EscrowCodeHash:            "tvm-cell-sha256:" + strings.Repeat("35", 32),
		RegistryCodeHash:          "tvm-cell-sha256:" + strings.Repeat("36", 32),
		EscrowTransactionHash:     "sha256:" + strings.Repeat("37", 32),
		AgentTransactionHash:      "sha256:" + strings.Repeat("38", 32),
		CapabilityTransactionHash: "sha256:" + strings.Repeat("39", 32),
		EscrowFinalizedCheckpoint: 100, AgentFinalizedCheckpoint: 101, CapabilityFinalizedCheckpoint: 102}, nil
}

type sharedRunner struct {
	mu    sync.Mutex
	calls int
}

func (r *sharedRunner) Execute(_ context.Context, request softwarework.Request) (softwarework.Outcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return softwarework.Outcome{QuoteCommitment: request.QuoteCommitment, ExecutionID: request.ExecutionID,
		InputDigest: request.InputDigest, SourceDigest: request.SourceDigest,
		ResultDigest: "sha256:" + strings.Repeat("44", 32),
		Artifact: artifactstore.Descriptor{Digest: "sha256:" + strings.Repeat("55", 32),
			MediaType: softwarework.ArtifactMediaType, SizeBytes: 10},
		Report: artifactstore.Descriptor{Digest: "sha256:" + strings.Repeat("66", 32),
			MediaType: softwarework.ReportMediaType, SizeBytes: 5},
		ToolchainDigest: "sha256:" + strings.Repeat("77", 32), SandboxDigest: "sha256:" + strings.Repeat("88", 32),
		CompletedAtUnix: 2_000_000_000}, nil
}

type a2aLocator struct{}

func (a2aLocator) URL(value artifactstore.Descriptor) (a2a.URL, error) {
	return a2a.URL("https://provider.example/objects/" + value.Digest[7:]), nil
}

type mcpLocator struct{}

func (mcpLocator) URL(value artifactstore.Descriptor) (string, error) {
	return "https://provider.example/objects/" + value.Digest[7:], nil
}

func TestTLSA2AThenMCPCannotExecuteOnePurchaseTwice(t *testing.T) {
	certificatePath, keyPath, roots := testCertificate(t)
	gate, runner := &sharedGate{}, &sharedRunner{}
	a2aAdapter, err := a2aadapter.New(gate, runner, a2aLocator{})
	if err != nil {
		t.Fatal(err)
	}
	a2aServer, err := a2aadapter.NewPublicServer(a2aAdapter, serverConfig(certificatePath, keyPath))
	if err != nil {
		t.Fatal(err)
	}
	a2aURL, stopA2A := serveTLS(t, a2aServer)
	defer stopA2A()
	client := authenticatedClient(roots)
	transport := a2aclient.NewJSONRPCTransport(a2aURL, client)
	defer transport.Destroy()
	request, err := a2aadapter.NewTaskRequest("message", "context", "0:"+strings.Repeat("cc", 32),
		"tvm-cell-sha256:"+strings.Repeat("aa", 32), "sha256:"+strings.Repeat("bb", 32), []byte("source archive"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := transport.SendMessage(context.Background(), nil, request)
	if err != nil {
		t.Fatal(err)
	}
	if task, ok := result.(*a2a.Task); !ok || task.Status.State != a2a.TaskStateCompleted {
		t.Fatal("A2A transport did not complete the first authorized execution")
	}

	mcpAdapter, err := mcpadapter.New(gate, runner, mcpLocator{})
	if err != nil {
		t.Fatal(err)
	}
	mcpServer, err := mcpadapter.NewPublicServer(mcpAdapter, serverConfig(certificatePath, keyPath))
	if err != nil {
		t.Fatal(err)
	}
	mcpURL, stopMCP := serveTLS(t, mcpServer)
	defer stopMCP()
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "tos-service-interop", Version: "1.0.0"}, nil)
	session, err := mcpClient.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint: mcpURL, HTTPClient: client, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	input, err := mcpadapter.PrepareInput("0:"+strings.Repeat("cc", 32),
		"tvm-cell-sha256:"+strings.Repeat("aa", 32), "sha256:"+strings.Repeat("bb", 32), []byte("source archive"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: mcpadapter.ToolName, Arguments: input})
	if err == nil && response != nil && !response.IsError {
		t.Fatal("MCP transport repeated a purchase already claimed through A2A")
	}
	if gate.calls != 2 || runner.calls != 1 {
		t.Fatalf("cross-transport calls gate=%d runner=%d, want 2/1", gate.calls, runner.calls)
	}
}

func TestConcurrentA2AMCPAgentPacketShareOneExecutionGate(t *testing.T) {
	gate, runner := &sharedGate{}, &sharedRunner{}
	a2aAdapter, err := a2aadapter.New(gate, runner, a2aLocator{})
	if err != nil {
		t.Fatal(err)
	}
	mcpAdapter, err := mcpadapter.New(gate, runner, mcpLocator{})
	if err != nil {
		t.Fatal(err)
	}
	packetAdapter, err := agentpacketadapter.New(gate, runner)
	if err != nil {
		t.Fatal(err)
	}
	escrow := "0:" + strings.Repeat("cc", 32)
	quote := "tvm-cell-sha256:" + strings.Repeat("aa", 32)
	execution := "sha256:" + strings.Repeat("bb", 32)
	source := []byte("one purchase over three transports")
	a2aRequest, err := a2aadapter.NewTaskRequest("message", "context", escrow, quote, execution, source)
	if err != nil {
		t.Fatal(err)
	}
	mcpInput, err := mcpadapter.PrepareInput(escrow, quote, execution, source)
	if err != nil {
		t.Fatal(err)
	}
	packet := signedAgentPacket(t, escrow, quote, execution, source)

	start := make(chan struct{})
	results := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		_, callErr := a2aAdapter.Execute(context.Background(), a2aRequest)
		results <- callErr
	}()
	go func() {
		defer workers.Done()
		<-start
		_, _, callErr := mcpAdapter.Call(context.Background(), nil, mcpInput)
		results <- callErr
	}()
	go func() {
		defer workers.Done()
		<-start
		_, _, callErr := packetAdapter.Execute(context.Background(), packet)
		results <- callErr
	}()
	close(start)
	workers.Wait()
	close(results)
	successes := 0
	for callErr := range results {
		if callErr == nil {
			successes++
		}
	}
	if successes != 1 || gate.calls != 3 || runner.calls != 1 {
		t.Fatalf("three-transport arbitration successes=%d gate=%d runner=%d, want 1/3/1",
			successes, gate.calls, runner.calls)
	}
}

func signedAgentPacket(t *testing.T, escrow, quote, execution string, source []byte) agentpacket.Packet {
	t.Helper()
	sourceHash := sha256.Sum256(source)
	payload, err := json.Marshal(map[string]string{
		"schema": "tos.service.agent-packet-work.v1", "escrow_address": escrow,
		"quote_commitment": quote, "execution_id": execution,
		"input_digest":          "sha256:" + strings.Repeat("dd", 32),
		"source_digest":         "sha256:" + hex.EncodeToString(sourceHash[:]),
		"source_archive_base64": base64.StdEncoding.EncodeToString(source),
	})
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := agentpacket.Sign(agentpacket.Packet{
		SenderAgentID:    "agent_" + strings.Repeat("20", 32),
		RecipientAgentID: "agent_" + strings.Repeat("10", 32),
		CapabilityID:     "cap_" + strings.Repeat("11", 32), QuoteCommitment: quote,
		Sequence: 1, CreatedAtUnix: 2_000_000_000, Payload: payload, SenderPublicKey: public,
	}, private)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func serverConfig(certificatePath, keyPath string) adapterhttp.ServerConfig {
	return adapterhttp.ServerConfig{Address: "127.0.0.1:0", CertificateFile: certificatePath,
		PrivateKeyFile: keyPath, Boundary: adapterhttp.BoundaryConfig{BearerToken: bearerToken,
			MaxRequestBytes: 1 << 20, MaxConcurrent: 4}}
}

func serveTLS(t *testing.T, server *http.Server) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	tlsListener := tls.NewListener(listener, server.TLSConfig)
	done := make(chan error, 1)
	go func() { done <- server.Serve(tlsListener) }()
	return "https://" + listener.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-done
	}
}

type bearerTransport struct{ base http.RoundTripper }

func (t bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header.Set("Authorization", "Bearer "+bearerToken)
	return t.base.RoundTrip(clone)
}

func authenticatedClient(roots *x509.CertPool) *http.Client {
	return &http.Client{Transport: bearerTransport{base: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: roots}}}}
}

func testCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "127.0.0.1"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true,
		BasicConstraintsValid: true}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certificatePath, keyPath := filepath.Join(directory, "server.crt"), filepath.Join(directory, "server.key")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("test certificate was not accepted")
	}
	return certificatePath, keyPath, roots
}
