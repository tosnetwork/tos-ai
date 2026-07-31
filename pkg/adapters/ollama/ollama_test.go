package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

func config(baseURL string) Config {
	return Config{
		BaseURL: baseURL, Model: "qwen", ModelDigest: "sha256:" + strings.Repeat("a", 64),
		MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxResponseBytes: 1 << 20,
		MaxConnections: 2, MaxResponseHeaderBytes: 4096,
		Timeout: time.Second, ConnectTimeout: time.Second,
		Admission: admission.Resources{
			RAMBytes: 1, ContextTokens: 128, BatchSize: 1,
			ExecutionTime: time.Second,
		},
	}
}

func TestExecuteIsBoundedAndCapturesUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/generate" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"response":"answer","prompt_eval_count":2,"eval_count":1,"done":true}`))
	}))
	defer server.Close()
	adapter, err := New(config(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	response, err := adapter.Execute(context.Background(), airuntime.Request{
		RequestID:      "request-1",
		Operation:      "generate",
		Model:          "qwen",
		Payload:        []byte("question"),
		MaxOutputBytes: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Output) != "answer" || response.Usage.OutputTokens != 1 {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestNewRejectsRemotePlaintextHostname(t *testing.T) {
	value := config("http://runtime.example")
	_, err := New(value)
	if err == nil {
		t.Fatal("remote plaintext URL accepted")
	}
}
