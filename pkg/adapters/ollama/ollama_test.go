package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

func TestExecuteIsBoundedAndCapturesUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/generate" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"response":"answer","prompt_eval_count":2,"eval_count":1,"done":true}`))
	}))
	defer server.Close()
	adapter, err := New(Config{
		BaseURL:        server.URL,
		Model:          "qwen",
		ModelDigest:    "sha256:test",
		MaxInputBytes:  1024,
		MaxOutputBytes: 1024,
		Timeout:        time.Second,
	})
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
	_, err := New(Config{
		BaseURL:        "http://runtime.example",
		Model:          "qwen",
		ModelDigest:    "sha256:test",
		MaxInputBytes:  1,
		MaxOutputBytes: 1,
		Timeout:        time.Second,
	})
	if err == nil {
		t.Fatal("remote plaintext URL accepted")
	}
}
