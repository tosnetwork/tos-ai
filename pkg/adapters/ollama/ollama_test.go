package ollama

import (
	"context"
	"errors"
	"fmt"
	"io"
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
		MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxRequestBytes: 2048,
		MaxResponseBytes: 1 << 20,
		MaxConnections:   2, MaxResponseHeaderBytes: 4096,
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

func TestPreflightVerifiesObservedModelDigest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/tags" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		_, _ = writer.Write([]byte(fmt.Sprintf(
			`{"models":[{"name":"qwen","model":"qwen","digest":"%s","size":123}]}`,
			strings.Repeat("a", 64),
		)))
	}))
	defer server.Close()
	adapter, err := New(config(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Preflight(context.Background())
	if err != nil || result.ModelDigest != adapter.capability.ModelDigest ||
		result.DigestEvidence != airuntime.BindingLocallyObserved {
		t.Fatalf("preflight=%#v err=%v", result, err)
	}
}

func TestActivatedPreflightVerifiesApprovedSourceAndUsesPrivateModel(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	runtimeModel := "tos-ai/primary:" + strings.Repeat("a", 64)
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		switch request.URL.Path {
		case "/api/show":
			if request.Method != http.MethodPost {
				t.Fatalf("show method=%s", request.Method)
			}
			_, _ = writer.Write([]byte(fmt.Sprintf(
				`{"modelfile":"FROM /models/blobs/sha256-%s",`+
					`"details":{"format":"gguf"}}`,
				strings.TrimPrefix(digest, "sha256:"),
			)))
		case "/api/generate":
			data, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), runtimeModel) {
				t.Fatalf("private runtime model missing from request: %s", data)
			}
			_, _ = writer.Write([]byte(
				`{"response":"activated","done":true}`,
			))
		default:
			t.Fatalf("unexpected path=%s", request.URL.Path)
		}
	}))
	defer server.Close()
	value := config(server.URL)
	value.RuntimeModel = runtimeModel
	value.SourceDigest = digest
	adapter, err := New(value)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Preflight(context.Background())
	if err != nil || result.Model != "qwen" ||
		result.ModelDigest != digest ||
		result.DigestEvidence != airuntime.BindingLocallyObserved {
		t.Fatalf("activated preflight=%#v err=%v", result, err)
	}
	response, err := adapter.Execute(
		context.Background(),
		airuntime.Request{
			RequestID: "activated", Operation: "generate", Model: "qwen",
			Payload: []byte("hello"), MaxOutputBytes: 32,
		},
	)
	if err != nil || string(response.Output) != "activated" {
		t.Fatalf("activated response=%#v err=%v", response, err)
	}
}

func TestActivatedPreflightRejectsWrongOrOversizedSource(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name string
		body string
		kind airuntime.ErrorKind
	}{
		{
			name: "wrong digest",
			body: `{"modelfile":"FROM sha256-` + strings.Repeat("b", 64) +
				`","details":{"format":"gguf"}}`,
			kind: airuntime.ErrorProtocol,
		},
		{
			name: "duplicate",
			body: `{"modelfile":"FROM sha256-` + strings.Repeat("a", 64) +
				`","modelfile":"FROM sha256-` + strings.Repeat("a", 64) +
				`","details":{"format":"gguf"}}`,
			kind: airuntime.ErrorProtocol,
		},
		{
			name: "oversized",
			body: strings.Repeat(
				"x", int(airuntime.MaxPreflightResponseBytesHard)+1,
			),
			kind: airuntime.ErrorLimit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter,
				_ *http.Request,
			) {
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			value := config(server.URL)
			value.RuntimeModel = "tos-ai/primary:" + strings.Repeat("a", 64)
			value.SourceDigest = digest
			adapter, err := New(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.Preflight(
				context.Background(),
			); airuntime.ErrorKindOf(err) != test.kind {
				t.Fatalf("activated preflight error=%v", err)
			}
		})
	}
}

func TestPreflightRejectsMissingMismatchedAndAmbiguousModels(t *testing.T) {
	tests := []struct {
		name string
		body string
		kind airuntime.ErrorKind
	}{
		{"missing", `{"models":[{"name":"other","digest":"` + strings.Repeat("a", 64) + `"}]}`, airuntime.ErrorUnavailable},
		{"mismatch", `{"models":[{"name":"qwen","digest":"` + strings.Repeat("b", 64) + `"}]}`, airuntime.ErrorProtocol},
		{"invalid digest", `{"models":[{"name":"qwen","digest":"not-a-digest"}]}`, airuntime.ErrorProtocol},
		{"duplicate", `{"models":[{"name":"qwen","digest":"` + strings.Repeat("a", 64) + `"},{"model":"qwen","digest":"` + strings.Repeat("a", 64) + `"}]}`, airuntime.ErrorProtocol},
		{"trailing JSON", `{"models":[]}{"secret":"value"}`, airuntime.ErrorProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			adapter, err := New(config(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != test.kind {
				t.Fatalf("preflight error = %v", err)
			}
		})
	}
}

func TestPreflightBoundsInventoryAndRedactsHTTPFailure(t *testing.T) {
	t.Run("body bytes", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write([]byte(strings.Repeat("x", int(airuntime.MaxPreflightResponseBytesHard)+1)))
		}))
		defer server.Close()
		adapter, err := New(config(server.URL))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorLimit {
			t.Fatalf("body limit error = %v", err)
		}
	})

	t.Run("model count", func(t *testing.T) {
		body := `{"models":[`
		for index := 0; index <= airuntime.MaxPreflightModelsHard; index++ {
			if index > 0 {
				body += ","
			}
			body += `{"name":"other"}`
		}
		body += "]}"
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write([]byte(body))
		}))
		defer server.Close()
		adapter, err := New(config(server.URL))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorLimit {
			t.Fatalf("model count error = %v", err)
		}
	})

	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusUnauthorized)
			_, _ = writer.Write([]byte(`{"endpoint":"http://secret","token":"secret"}`))
		}))
		defer server.Close()
		adapter, err := New(config(server.URL))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorRemote ||
			strings.Contains(err.Error(), "secret") {
			t.Fatalf("HTTP error leaked details: %v", err)
		}
	})
}

func TestPreflightPropagatesTimeoutAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-time.After(time.Second):
			writer.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	timeoutConfig := config(server.URL)
	timeoutConfig.Timeout = 20 * time.Millisecond
	timeoutAdapter, err := New(timeoutConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeoutAdapter.Preflight(context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("preflight timeout = %v", err)
	}
	cancelAdapter, err := New(config(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cancelAdapter.Preflight(ctx); airuntime.ErrorKindOf(err) != airuntime.ErrorCanceled ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("preflight cancellation = %v", err)
	}
}

func TestNewRejectsRemotePlaintextHostname(t *testing.T) {
	value := config("http://runtime.example")
	_, err := New(value)
	if err == nil {
		t.Fatal("remote plaintext URL accepted")
	}
}

func TestExecuteRejectsOversizedRequestBody(t *testing.T) {
	value := config("http://127.0.0.1:11434")
	value.MaxRequestBytes = 8
	adapter, err := New(value)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Execute(context.Background(), airuntime.Request{
		RequestID: "request-oversize", Operation: "generate", Model: "qwen",
		Payload: []byte("payload"), MaxOutputBytes: 32,
	})
	if airuntime.ErrorKindOf(err) != airuntime.ErrorLimit {
		t.Fatalf("oversized request error = %v", err)
	}
}

func TestResponseLimitAccountsForJSONEscaping(t *testing.T) {
	if got := responseLimit(8<<20, 1<<20); got != 6*(1<<20)+responseOverhead {
		t.Fatalf("response limit = %d", got)
	}
	if got := responseLimit(128, 32); got != 128 {
		t.Fatalf("configured response cap = %d", got)
	}
}
