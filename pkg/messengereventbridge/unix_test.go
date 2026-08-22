package messengereventbridge

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tosnetwork/tos-messenger/pkg/protocolbridge"
)

func TestUnixServiceCreatesPrivateDistinctSocketsAndCleansUp(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	a2aPath, mcpPath := filepath.Join(root, "a2a.sock"), filepath.Join(root, "mcp.sock")
	a2aConsumer, mcpConsumer := &a2aFake{}, &mcpFake{}
	server := testServer(t, a2aConsumer, mcpConsumer)
	service, err := ListenUnix(server, a2aPath, mcpPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{a2aPath, mcpPath} {
		info, statErr := os.Stat(path)
		if statErr != nil || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("unsafe Unix socket %s: mode=%v err=%v", path, info.Mode(), statErr)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	client := unixClient(a2aPath)
	request, err := http.NewRequest(http.MethodPost, "http://unix"+A2APath,
		bytes.NewReader(eventWire(t, "a2a.message", testSender, testConversation)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", EventContentType)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("Unix service returned %d", response.StatusCode)
	}
	crossRequest, err := http.NewRequest(http.MethodPost, "http://unix"+MCPPath,
		bytes.NewReader(eventWire(t, "mcp.call", testSender, testConversation)))
	if err != nil {
		t.Fatal(err)
	}
	crossRequest.Header.Set("Content-Type", EventContentType)
	crossResponse, err := client.Do(crossRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = crossResponse.Body.Close()
	if crossResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("A2A socket exposed MCP endpoint: %d", crossResponse.StatusCode)
	}
	a2aReceiver, err := protocolbridge.NewUnixReceiver(a2aPath, protocolbridge.ProfileA2A, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := a2aReceiver.Receive(context.Background(), decodedEvent(t, "a2a.message")); err != nil {
		t.Fatalf("Messenger A2A client was incompatible: %v", err)
	}
	mcpReceiver, err := protocolbridge.NewUnixReceiver(mcpPath, protocolbridge.ProfileMCP, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := mcpReceiver.Receive(context.Background(), decodedEvent(t, "mcp.call")); err != nil {
		t.Fatalf("Messenger MCP client was incompatible: %v", err)
	}
	if len(a2aConsumer.events) != 2 || len(mcpConsumer.calls) != 1 {
		t.Fatalf("unexpected cross-repository delivery: a2a=%d mcp=%d", len(a2aConsumer.events), len(mcpConsumer.calls))
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Unix service did not stop")
	}
	for _, path := range []string{a2aPath, mcpPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("owned socket was not removed: %s err=%v", path, err)
		}
	}
}

func TestUnixServiceRefusesAliasesUnsafeParentsAndExistingPaths(t *testing.T) {
	server := testServer(t, &a2aFake{}, &mcpFake{})
	private := t.TempDir()
	if err := os.Chmod(private, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(private, "same.sock")
	if _, err := ListenUnix(server, alias, alias); err == nil {
		t.Fatal("aliased A2A/MCP sockets were accepted")
	}
	unsafe := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(unsafe, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenUnix(server, filepath.Join(unsafe, "a2a.sock"), ""); err == nil {
		t.Fatal("world-visible socket parent was accepted")
	}
	existing := filepath.Join(private, "existing.sock")
	if err := os.WriteFile(existing, []byte("owned by someone else"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenUnix(server, existing, ""); err == nil {
		t.Fatal("existing path was replaced")
	}
	body, err := os.ReadFile(existing)
	if err != nil || string(body) != "owned by someone else" {
		t.Fatalf("existing path changed: body=%q err=%v", body, err)
	}
}

func unixClient(path string) *http.Client {
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}
}
