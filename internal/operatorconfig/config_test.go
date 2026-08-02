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

	"github.com/tosnetwork/tos-ai/pkg/profile/textgeneration"
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
	configuration, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(configuration.Adapters) != 1 ||
		configuration.Adapters[0].Capability().Runtime != "openai-compatible" {
		t.Fatalf("unexpected adapters: %#v", configuration.Adapters)
	}
	profilePlan, err := configuration.TextGenerationProfilePlan()
	if err != nil {
		t.Fatal(err)
	}
	if profilePlan.Len() != 1 || !profilePlan.Supports(
		textgeneration.ProfileID,
		textgeneration.ProfileVersion,
		nil,
		textgeneration.Operation,
	) {
		t.Fatal("loaded runtime did not expose its exact text-generation mapper")
	}
	originalAdapter := configuration.Adapters[0]
	configuration.Adapters[0] = nil
	profilePlan, err = configuration.TextGenerationProfilePlan()
	configuration.Adapters[0] = originalAdapter
	if err != nil || profilePlan.Len() != 1 || !profilePlan.Supports(
		textgeneration.ProfileID,
		textgeneration.ProfileVersion,
		nil,
		textgeneration.Operation,
	) {
		t.Fatal("caller mutation changed the private profile capability snapshot")
	}
	response, err := configuration.Adapters[0].Execute(
		context.Background(), airuntime.Request{
			RequestID: "configured-request", Operation: "generate", Model: "approved-model",
			Payload: []byte("hello"), MaxOutputBytes: 64,
		},
	)
	if err != nil || string(response.Output) != "configured" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
}

func TestLoadFixedOpenAICompatibleRuntimeKinds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/models" {
			_, _ = writer.Write([]byte(`{"object":"list","data":[{"id":"approved-model"}]}`))
			return
		}
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{}}`))
	}))
	defer server.Close()
	for _, kind := range []string{"vllm", "llama.cpp", "localai"} {
		config := fmt.Sprintf(`{
			"version": 2,
			"adapters": [{
				"type": %q, "baseUrl": %q, "model": "approved-model",
				"modelDigest": "sha256:%s", "runtimeRevision": %q,
				"maxInputBytes": 1024, "maxOutputBytes": 64,
				"maxRequestBytes": 2048, "maxResponseBytes": 2048,
				"admission": {"ramBytes": 1, "contextTokens": 128, "batchSize": 1}
			}]
		}`, kind, server.URL, strings.Repeat("a", 64), kind+"-v1")
		configuration, err := Load(writePrivate(t, kind+".json", config, 0o600))
		if err != nil {
			t.Fatalf("kind %q: %v", kind, err)
		}
		if got := configuration.Adapters[0].Capability().Runtime; got != kind {
			t.Fatalf("kind %q runtime=%q", kind, got)
		}
	}
}

func TestTextGenerationProfilePlanRejectsEmptyConfiguration(t *testing.T) {
	var configuration *Configuration
	if _, err := configuration.TextGenerationProfilePlan(); err == nil {
		t.Fatal("nil runtime configuration produced a profile plan")
	}
	configuration = &Configuration{}
	if _, err := configuration.TextGenerationProfilePlan(); err == nil {
		t.Fatal("empty runtime configuration produced a profile plan")
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
	configuration, err := Load(
		writePrivate(t, "runtime.json", config, 0o600),
	)
	if err != nil {
		t.Fatal(err)
	}
	capability := configuration.Adapters[0].Capability()
	if capability.MaxInputBytes != defaultMaxInputBytes ||
		capability.MaxOutputBytes != defaultMaxOutputBytes ||
		capability.Admission.RAMBytes != defaultRAMBytes ||
		capability.Admission.ExecutionTime.Milliseconds() != defaultTimeoutMillis {
		t.Fatalf("defaults were not applied: %#v", capability)
	}
}

func TestLoadBuildsBoundedOllamaActivation(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "activation")
	digest := "sha256:" + strings.Repeat("a", 64)
	config := fmt.Sprintf(`{
		"version":1,
		"activation":{
			"stateDir":%q,
			"operationTimeoutMillis":1000,
			"cleanupTimeoutMillis":500
		},
		"adapters":[{
			"type":"ollama",
			"baseUrl":"http://127.0.0.1:11434",
			"model":"approved-model",
			"modelDigest":%q,
			"activation":{"slotId":"primary","maxModelBytes":1048576}
		}]
	}`, stateDir, digest)
	configuration, err := Load(
		writePrivate(t, "activation.json", config, 0o600),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer configuration.CloseBackends()
	if len(configuration.Adapters) != 1 ||
		configuration.Activation == nil ||
		len(configuration.Activation.Controller.Slots) != 1 ||
		len(configuration.Activation.Desired) != 1 {
		t.Fatalf("activation configuration=%#v", configuration)
	}
	slot := configuration.Activation.Controller.Slots[0].Policy
	if slot.ID != "primary" || slot.Model != "approved-model" ||
		slot.Runtime != "ollama" || slot.MaxModelBytes != 1048576 ||
		configuration.Activation.Desired[0].Digest != digest {
		t.Fatalf(
			"slot=%#v desired=%#v",
			slot, configuration.Activation.Desired,
		)
	}
}

func TestLoadRejectsAmbiguousActivationConfiguration(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	adapter := fmt.Sprintf(`{
		"type":"ollama","baseUrl":"http://127.0.0.1:11434",
		"model":"approved","modelDigest":%q,
		"activation":{"slotId":"primary"}
	}`, digest)
	withoutGlobal := `{"version":1,"adapters":[` + adapter + `]}`
	if _, err := Load(writePrivate(
		t, "without-global.json", withoutGlobal, 0o600,
	)); err == nil {
		t.Fatal("activation slot without global policy accepted")
	}
	global := fmt.Sprintf(
		`{"version":1,"activation":{"stateDir":%q},"adapters":[%s,%s]}`,
		filepath.Join(t.TempDir(), "state"), adapter, adapter,
	)
	if _, err := Load(writePrivate(
		t, "duplicate-slot.json", global, 0o600,
	)); err == nil {
		t.Fatal("duplicate activation slot accepted")
	}
	openAI := fmt.Sprintf(`{
		"type":"openai-compatible","baseUrl":"https://runtime.example",
		"model":"approved","modelDigest":%q,"runtimeRevision":"v1",
		"activation":{"slotId":"primary"}
	}`, digest)
	unsupported := fmt.Sprintf(
		`{"version":1,"activation":{"stateDir":%q},"adapters":[%s]}`,
		filepath.Join(t.TempDir(), "state"), openAI,
	)
	if _, err := Load(writePrivate(
		t, "unsupported.json", unsupported, 0o600,
	)); err == nil {
		t.Fatal("OpenAI-compatible activation accepted")
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

func TestLoadRejectsPlaintextCIDRSpanningPublicAddresses(t *testing.T) {
	config := fmt.Sprintf(`{"version":1,"adapters":[{
		"type":"ollama","baseUrl":"http://10.0.0.5:11434",
		"allowedPlaintextCidrs":["10.0.0.0/7"],
		"model":"m","modelDigest":"sha256:%s"
	}]}`, strings.Repeat("d", 64))
	if _, err := Load(writePrivate(
		t, "broad-plaintext-cidr.json", config, 0o600,
	)); err == nil {
		t.Fatal("plaintext CIDR spanning public addresses was accepted")
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

func TestLoadCountsActivationConnectionsInAggregateLimit(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "activation")
	adapters := make([]string, 0, 9)
	for index := range 8 {
		adapters = append(adapters, fmt.Sprintf(`{
			"type":"ollama","baseUrl":"http://127.0.0.1:11434",
			"model":"model-%d","modelDigest":"sha256:%064x",
			"maxConnections":30,"activation":{"slotId":"slot-%d"}
		}`, index, index+1, index))
	}
	withinLimit := fmt.Sprintf(
		`{"version":1,"activation":{"stateDir":%q},"adapters":[%s]}`,
		stateDir, strings.Join(adapters, ","),
	)
	configuration, err := Load(writePrivate(
		t, "activation-connections.json", withinLimit, 0o600,
	))
	if err != nil {
		t.Fatal(err)
	}
	for _, adapter := range configuration.Adapters {
		if closer, ok := adapter.(airuntime.AdapterCloser); ok {
			_ = closer.Close()
		}
	}
	if err := configuration.CloseBackends(); err != nil {
		t.Fatal(err)
	}
	adapters = append(adapters, fmt.Sprintf(`{
		"type":"ollama","baseUrl":"http://127.0.0.1:11434",
		"model":"extra","modelDigest":"sha256:%064x",
		"maxConnections":1
	}`, 9))
	overLimit := fmt.Sprintf(
		`{"version":1,"activation":{"stateDir":%q},"adapters":[%s]}`,
		stateDir, strings.Join(adapters, ","),
	)
	if _, err := Load(writePrivate(
		t, "activation-connections-over.json", overLimit, 0o600,
	)); err == nil {
		t.Fatal("activation HTTP connections were omitted from aggregate limit")
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
