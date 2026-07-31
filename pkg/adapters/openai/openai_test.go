package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

func newAdapter(t *testing.T, handler http.Handler, timeout time.Duration, maxResponse uint64) (*Adapter, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	adapter, err := New(Config{
		BaseURL: server.URL, Model: "approved", ModelDigest: "sha256:" + strings.Repeat("a", 64),
		RuntimeRevision: "test-v1", MaxInputBytes: 1024, MaxOutputBytes: 32,
		MaxRequestBytes: 2048, MaxResponseBytes: maxResponse, MaxConnections: 2,
		MaxResponseHeaderBytes: 4096, Timeout: timeout, ConnectTimeout: time.Second,
		Admission: admission.Resources{
			RAMBytes: 1, ContextTokens: 128, BatchSize: 1,
			ExecutionTime: time.Second,
		},
	})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return adapter, server
}

func execute(adapter *Adapter, ctx context.Context) (airuntime.Response, error) {
	return adapter.Execute(ctx, airuntime.Request{
		RequestID: "request-openai", Operation: "generate", Model: "approved",
		Payload: []byte("hello"), MaxOutputBytes: 32,
	})
}

func TestExecuteSuccess(t *testing.T) {
	adapter, server := newAdapter(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"answer"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`))
	}), time.Second, 2048)
	defer server.Close()
	response, err := execute(adapter, context.Background())
	if err != nil || string(response.Output) != "answer" || response.Usage.OutputTokens != 1 {
		t.Fatalf("response=%#v err=%v", response, err)
	}
}

func TestOversizedResponseAndMaliciousJSON(t *testing.T) {
	adapter, server := newAdapter(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(strings.Repeat("x", 500)))
	}), time.Second, 128)
	defer server.Close()
	if _, err := execute(adapter, context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorLimit {
		t.Fatalf("oversize error = %v", err)
	}

	adapter2, server2 := newAdapter(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}{"credential":"leak"}`))
	}), time.Second, 2048)
	defer server2.Close()
	if _, err := execute(adapter2, context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorProtocol {
		t.Fatalf("malicious JSON error = %v", err)
	}
}

func TestTimeoutCancellationAndHTTPError(t *testing.T) {
	block := func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-time.After(time.Second):
			writer.WriteHeader(http.StatusOK)
		}
	}
	timeoutAdapter, timeoutServer := newAdapter(t, http.HandlerFunc(block), 20*time.Millisecond, 2048)
	defer timeoutServer.Close()
	if _, err := execute(timeoutAdapter, context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorTimeout {
		t.Fatalf("timeout error = %v", err)
	}

	cancelAdapter, cancelServer := newAdapter(t, http.HandlerFunc(block), time.Second, 2048)
	defer cancelServer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := execute(cancelAdapter, ctx); airuntime.ErrorKindOf(err) != airuntime.ErrorCanceled ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}

	errorAdapter, errorServer := newAdapter(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = writer.Write([]byte(`{"url":"http://secret","token":"secret"}`))
	}), time.Second, 2048)
	defer errorServer.Close()
	if _, err := execute(errorAdapter, context.Background()); airuntime.ErrorKindOf(err) != airuntime.ErrorRemote ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("HTTP error leaked details: %v", err)
	}
}

func TestPlaintextEndpointPolicy(t *testing.T) {
	config := Config{
		BaseURL: "http://192.0.2.10", Model: "m", ModelDigest: "sha256:" + strings.Repeat("a", 64), RuntimeRevision: "v1",
		MaxInputBytes: 1, MaxOutputBytes: 1, MaxRequestBytes: 128, MaxResponseBytes: 128,
		MaxConnections: 1, MaxResponseHeaderBytes: 1024, Timeout: time.Second,
		ConnectTimeout: time.Second,
		Admission:      admission.Resources{RAMBytes: 1, ContextTokens: 1, BatchSize: 1, ExecutionTime: time.Second},
	}
	if _, err := New(config); err == nil {
		t.Fatal("remote plaintext endpoint accepted")
	}
	config.BaseURL = "http://192.168.1.5"
	config.AllowedPlaintextCIDRs = []string{"192.168.1.0/24"}
	if _, err := New(config); err != nil {
		t.Fatalf("explicit local plaintext rejected: %v", err)
	}
}
