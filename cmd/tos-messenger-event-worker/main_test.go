package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tosnetwork/tos-ai/pkg/messengereventbridge"
	"github.com/tosnetwork/tosutils-go/tvm/cell"
)

func TestValidateConfigRequiresCompleteExactResultRouting(t *testing.T) {
	value := validConfig(t)
	if err := validateConfig(value); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}
	value.ResultRoutes = nil
	if err := validateConfig(value); err == nil {
		t.Fatal("allowed sender without a result route was accepted")
	}
	value = validConfig(t)
	value.ResultRoutes[0].SenderAgentID = "agent_" + strings.Repeat("b", 64)
	if err := validateConfig(value); err == nil {
		t.Fatal("substituted sender result route was accepted")
	}
}

func TestValidateConfigFailsClosedOnOperatorBounds(t *testing.T) {
	for name, mutate := range map[string]func(*config){
		"aliased profile sockets": func(value *config) { value.MCPSocket = value.A2ASocket },
		"short result lifetime":   func(value *config) { value.ResultLifetimeSeconds = 59 },
		"fast publisher spin":     func(value *config) { value.PublishIntervalMilliseconds = 99 },
		"minority chain quorum":   func(value *config) { value.Chain.Quorum = 1 },
		"insecure remote RPC":     func(value *config) { value.Chain.Endpoints[0] = "http://rpc-a.example/v1" },
		"insecure artifact URL":   func(value *config) { value.ArtifactHTTPSOrigin = "http://artifacts.example" },
		"relative runtime socket": func(value *config) { value.MessengerRuntimeSocket = "messenger.sock" },
	} {
		t.Run(name, func(t *testing.T) {
			value := validConfig(t)
			mutate(&value)
			if err := validateConfig(value); err == nil {
				t.Fatal("unsafe operator configuration was accepted")
			}
		})
	}
}

func TestReadConfigRequiresStrictPrivateSingleLinkJSON(t *testing.T) {
	value := validConfig(t)
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	decoded, err := readConfig(path)
	if err != nil || decoded.Schema != configSchema {
		t.Fatalf("private configuration rejected: schema=%q err=%v", decoded.Schema, err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfig(path); err == nil {
		t.Fatal("group-readable configuration accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(filepath.Dir(path), "worker-hardlink.json")
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfig(path); err == nil {
		t.Fatal("multiply-linked configuration accepted")
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	unknown := append(raw[:len(raw)-1], []byte(`,"unknown":true}`)...)
	if err := os.WriteFile(path, unknown, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfig(path); err == nil {
		t.Fatal("unknown configuration field accepted")
	}
	duplicate := append(raw[:len(raw)-1], []byte(`,"schema":"tos.service.messenger-event-worker.v1"}`)...)
	if err := os.WriteFile(path, duplicate, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfig(path); err == nil {
		t.Fatal("duplicate configuration field accepted")
	}
}

func validConfig(t *testing.T) config {
	t.Helper()
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	runtime := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(runtime, 0o700); err != nil {
		t.Fatal(err)
	}
	sender := "agent_" + strings.Repeat("a", 64)
	code := cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()
	return config{
		Schema: configSchema, StateRoot: state,
		A2ASocket: filepath.Join(runtime, "a2a.sock"), MCPSocket: filepath.Join(runtime, "mcp.sock"),
		ArtifactSocket: filepath.Join(runtime, "artifacts.sock"), ArtifactHTTPSOrigin: "https://artifacts.example",
		MessengerRuntimeSocket: filepath.Join(runtime, "messenger.sock"), MessengerCallTimeoutSeconds: 10,
		PublishIntervalMilliseconds: 1000, ResultLifetimeSeconds: 3600,
		AllowedSenders: []string{sender}, AllowedConversations: []string{"conv_" + strings.Repeat("c", 64)},
		ResultRoutes: []messengereventbridge.ResultRoute{{SenderAgentID: sender,
			SessionID: "ses_" + strings.Repeat("d", 64), RecipientEndpointID: "mep_" + strings.Repeat("e", 64)}},
		Network: networkConfig{ID: "tos-testnet", GenesisRootHash: "sha256:" + strings.Repeat("1", 64), GenesisFileHash: "sha256:" + strings.Repeat("2", 64)},
		Chain: chainConfig{Endpoints: []string{"https://rpc-a.example/v1", "https://rpc-b.example/v1", "https://rpc-c.example/v1"},
			Quorum: 2, RegistryCodeBOCBase64: base64.StdEncoding.EncodeToString(code.ToBOC()),
			RegistryCodeHash: "tvm-cell-sha256:" + hex.EncodeToString(code.Hash()),
			EscrowCodeHash:   "tvm-cell-sha256:" + strings.Repeat("2", 64)},
		Provider: providerConfig{AgentID: sender, Address: "0:" + strings.Repeat("3", 64),
			TransportDigest: "sha256:" + strings.Repeat("4", 64), ExecutionSignerAuthorization: "sha256:" + strings.Repeat("5", 64)},
		ContainerdSocket: filepath.Join(runtime, "containerd.sock"), ContainerdFIFODirectory: filepath.Join(runtime, "fifo"),
	}
}
