package operatorconfig

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

func TestLoadOpenAIConfigAndCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("credential was not loaded from the private file")
		}
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"configured"}}],"usage":{}}`))
	}))
	defer server.Close()
	credential := writePrivate(t, "credential", "private-token", 0o600)
	config := fmt.Sprintf(`{
		"version": 1,
		"adapters": [{
			"type": "openai-compatible",
			"baseUrl": %q,
			"apiKeyFile": %q,
			"model": "approved-model",
			"modelDigest": "sha256:%s",
			"runtimeRevision": "localai-v1",
			"maxInputBytes": 1024,
			"maxOutputBytes": 64,
			"maxRequestBytes": 2048,
			"maxResponseBytes": 2048,
			"admission": {"ramBytes": 1, "contextTokens": 128, "batchSize": 1}
		}]
	}`, server.URL, credential, strings.Repeat("a", 64))
	path := writePrivate(t, "runtime.json", config, 0o600)
	adapters, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(adapters) != 1 || adapters[0].Capability().Runtime != "openai-compatible" {
		t.Fatalf("unexpected adapters: %#v", adapters)
	}
	response, err := adapters[0].Execute(context.Background(), airuntime.Request{
		RequestID: "configured-request", Operation: "generate", Model: "approved-model",
		Payload: []byte("hello"), MaxOutputBytes: 64,
	})
	if err != nil || string(response.Output) != "configured" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
}

func TestLoadOllamaUsesBoundedDefaults(t *testing.T) {
	config := fmt.Sprintf(`{
		"version": 1,
		"adapters": [{
			"type": "ollama",
			"baseUrl": "http://127.0.0.1:11434",
			"model": "approved-model",
			"modelDigest": "sha256:%s"
		}]
	}`, strings.Repeat("b", 64))
	adapters, err := Load(writePrivate(t, "runtime.json", config, 0o600))
	if err != nil {
		t.Fatal(err)
	}
	capability := adapters[0].Capability()
	if capability.MaxInputBytes != defaultMaxInputBytes ||
		capability.MaxOutputBytes != defaultMaxOutputBytes ||
		capability.Admission.RAMBytes != defaultRAMBytes ||
		capability.Admission.ExecutionTime.Milliseconds() != defaultTimeoutMillis {
		t.Fatalf("defaults were not applied: %#v", capability)
	}
}

func TestLoadRejectsInsecureAmbiguousAndUnboundedFiles(t *testing.T) {
	valid := fmt.Sprintf(`{"version":1,"adapters":[{
		"type":"ollama","baseUrl":"http://127.0.0.1:11434",
		"model":"m","modelDigest":"sha256:%s"}]}`, strings.Repeat("c", 64))
	insecure := writePrivate(t, "insecure.json", valid, 0o644)
	if _, err := Load(insecure); err == nil {
		t.Fatal("world-readable configuration accepted")
	}
	target := writePrivate(t, "target.json", valid, 0o600)
	symlink := filepath.Join(t.TempDir(), "runtime.json")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(symlink); err == nil {
		t.Fatal("symlink configuration accepted")
	}
	unknown := strings.Replace(valid, `"version":1`, `"version":1,"endpointOverride":"bad"`, 1)
	if _, err := Load(writePrivate(t, "unknown.json", unknown, 0o600)); err == nil {
		t.Fatal("unknown field accepted")
	}
	duplicate := strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1)
	if _, err := Load(writePrivate(t, "duplicate.json", duplicate, 0o600)); err == nil {
		t.Fatal("duplicate field accepted")
	}
	oversized := strings.Repeat(" ", int(MaxConfigBytes)+1)
	if _, err := Load(writePrivate(t, "oversized.json", oversized, 0o600)); err == nil {
		t.Fatal("oversized configuration accepted")
	}
}

func TestLoadRejectsUnsafeEndpointDurationAndCredential(t *testing.T) {
	secret := "must-not-leak"
	credential := writePrivate(t, "credential", secret, 0o644)
	config := fmt.Sprintf(`{"version":1,"adapters":[{
		"type":"openai-compatible","baseUrl":"http://runtime.example",
		"apiKeyFile":%q,"model":"m","modelDigest":"sha256:%s",
		"runtimeRevision":"v1","timeoutMillis":9223372036854775807
	}]}`, credential, strings.Repeat("d", 64))
	path := writePrivate(t, "runtime.json", config, 0o600)
	_, err := Load(path)
	if err == nil {
		t.Fatal("unsafe runtime configuration accepted")
	}
	if strings.Contains(err.Error(), "runtime.example") ||
		strings.Contains(err.Error(), credential) || strings.Contains(err.Error(), secret) {
		t.Fatalf("configuration error leaked sensitive data: %v", err)
	}
}

func TestLoadRejectsInsecureCredentialFile(t *testing.T) {
	credential := writePrivate(t, "credential", "private-token", 0o644)
	config := fmt.Sprintf(`{"version":1,"adapters":[{
		"type":"openai-compatible","baseUrl":"https://runtime.example",
		"apiKeyFile":%q,"model":"m","modelDigest":"sha256:%s",
		"runtimeRevision":"v1"
	}]}`, credential, strings.Repeat("e", 64))
	if _, err := Load(writePrivate(t, "runtime.json", config, 0o600)); err == nil {
		t.Fatal("insecure credential file accepted")
	}
}

func TestLoadRejectsExecutionBudgetBelowRuntimeTimeout(t *testing.T) {
	config := fmt.Sprintf(`{"version":1,"adapters":[{
		"type":"ollama","baseUrl":"http://127.0.0.1:11434",
		"model":"m","modelDigest":"sha256:%s",
		"timeoutMillis":2000,"admission":{"executionMillis":1000}
	}]}`, strings.Repeat("e", 64))
	if _, err := Load(writePrivate(t, "runtime.json", config, 0o600)); err == nil {
		t.Fatal("runtime timeout longer than admission execution budget accepted")
	}
}

func TestLoadRejectsWorkerAndAggregateConnectionLimits(t *testing.T) {
	base := fmt.Sprintf(`{
		"type":"ollama","baseUrl":"http://127.0.0.1:11434",
		"model":"m","modelDigest":"sha256:%s",
		"maxConnections":%d,"maxInputBytes":%d
	}`, strings.Repeat("f", 64), MaxConnectionsPerAdapter+1, MaxInputBytes+1)
	config := `{"version":1,"adapters":[` + base + `]}`
	if _, err := Load(writePrivate(t, "runtime.json", config, 0o600)); err == nil {
		t.Fatal("worker or connection hard limit accepted")
	}

	adapter := fmt.Sprintf(`{
		"type":"ollama","baseUrl":"http://127.0.0.1:11434",
		"model":"m","modelDigest":"sha256:%s",
		"maxConnections":%d
	}`, strings.Repeat("f", 64), MaxConnectionsPerAdapter)
	var values []string
	for range MaxConnectionsTotal/MaxConnectionsPerAdapter + 1 {
		values = append(values, adapter)
	}
	config = `{"version":1,"adapters":[` + strings.Join(values, ",") + `]}`
	if _, err := Load(writePrivate(t, "connections.json", config, 0o600)); err == nil {
		t.Fatal("aggregate connection hard limit accepted")
	}
}

func writePrivate(t *testing.T, name, data string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(data), mode); err != nil {
		t.Fatal(err)
	}
	return path
}
