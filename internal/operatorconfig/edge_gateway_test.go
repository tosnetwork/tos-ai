package operatorconfig

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/identity"
	"github.com/tosnetwork/tos-protocol/pkg/protocol"
)

func TestLoadEdgeGatewayConfigAndDocuments(t *testing.T) {
	directory := t.TempDir()
	now := time.Now().UTC().Truncate(time.Millisecond)
	descriptorPath := filepath.Join(directory, "service.json")
	catalogPath := filepath.Join(directory, "catalog.json")
	manifestPath := filepath.Join(directory, "manifest.json")
	chainPath := filepath.Join(directory, "chain.json")
	writePrivateTestFile(t, descriptorPath, []byte(fmt.Sprintf(`{
      "protocolVersion":"0.1","serviceId":"ai.edge.local","displayName":"Local AI",
      "controller":"0:%s","network":"tos-local","revision":"revision-0001",
      "expiresAt":%q,"profiles":[{"id":"tos.ai.text-generation","version":"0.1",
      "mediaType":"application/vnd.tos.ai.text-generation+json","url":"https://edge.local/profile",
      "digest":"sha256:%s"}]}`, strings.Repeat("a", 64), now.Add(time.Hour).Format(time.RFC3339Nano), strings.Repeat("b", 64))))
	writePrivateTestFile(t, catalogPath, []byte(`{
      "specVersion":"1.0","entries":[{"identifier":"urn:air:edge.local:tos:ai",
      "displayName":"Local AI","type":"application/vnd.tos.service+json",
      "url":"https://edge.local/.well-known/tos-service.json"}]}`))
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := identity.Sign(
		privateKey, protocol.ServiceManifestDomain, "controller-key-0001",
		[]byte(`{"manifest":"test"}`), now, now.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	manifestDocument, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	writePrivateTestFile(t, manifestPath, manifestDocument)
	writePrivateTestFile(t, chainPath, []byte(fmt.Sprintf(`{
      "version":"1","network":"tos-local","endpoints":["http://127.0.0.1:8011/"],
      "quorum":1,"allowedServiceCodeHashes":["%s"]}`, strings.Repeat("c", 64))))
	configPath := filepath.Join(directory, "edge.json")
	writePrivateTestFile(t, configPath, []byte(fmt.Sprintf(`{
      "version":1,"listenAddress":"127.0.0.1:8080","descriptorFile":%q,
      "catalogFile":%q,"manifestEnvelopeFile":%q,"chainConfigFile":%q,
      "workerSocket":%q,"requestJournalFile":%q,"receiptSignerSocket":%q,
      "receiptSignerKeyId":"receipt-key-0001","receiptSignerPublicKey":"%s",
      "requiredDelegationScope":"ai.edge.local"}`, descriptorPath, catalogPath,
		manifestPath, chainPath, filepath.Join(directory, "worker.sock"),
		filepath.Join(directory, "journal.db"), filepath.Join(directory, "receipt.sock"),
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")))

	config, err := LoadEdgeGatewayConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	documents, err := config.LoadDocuments(now)
	if err != nil {
		t.Fatal(err)
	}
	if documents.Descriptor.ServiceID != "ai.edge.local" ||
		documents.Chain.Network != documents.Descriptor.Network {
		t.Fatalf("unexpected deployment documents: %+v", documents)
	}
}

func TestLoadEdgeGatewayConfigRejectsUnsafeInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge.json")
	writePrivateTestFile(t, path, []byte(`{"version":1,"version":1}`))
	if _, err := LoadEdgeGatewayConfig(path); err == nil {
		t.Fatal("duplicate field was accepted")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadEdgeGatewayConfig(path); err == nil {
		t.Fatal("public operator configuration was accepted")
	}
}

func TestEdgeGatewayConfigRejectsPlaintextPublicListener(t *testing.T) {
	config := EdgeGatewayConfig{
		Version: EdgeGatewayConfigVersion, ListenAddress: "0.0.0.0:8080",
		DescriptorFile: "/private/descriptor.json", CatalogFile: "/private/catalog.json",
		ManifestEnvelopeFile: "/private/manifest.json", ChainConfigFile: "/private/chain.json",
		WorkerSocket: "/run/tos-ai/worker.sock", RequestJournalFile: "/private/journal.db",
		ReceiptSignerSocket: "/run/tos-ai/receipt.sock", ReceiptSignerKeyID: "receipt-key-0001",
		ReceiptSignerPublicKey:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		RequiredDelegationScope: "ai.edge.local",
	}
	if err := config.validate(); err == nil {
		t.Fatal("plaintext public Edge listener was accepted")
	}
	config.ListenAddress = "[::1]:8080"
	if err := config.validate(); err != nil {
		t.Fatalf("IPv6 loopback Edge listener was rejected: %v", err)
	}
}

func writePrivateTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
