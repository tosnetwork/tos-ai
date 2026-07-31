package ollama

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/modelactivation"
	"github.com/tosnetwork/tos-ai/pkg/ollamabinding"
)

type fakeRuntime struct {
	mu sync.Mutex

	blobs  map[string][]byte
	models map[string]string
	loaded map[string]bool

	uploads      int
	creates      int
	deletes      int
	failCreate   bool
	failDelete   bool
	overrideShow string
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		blobs: make(map[string][]byte), models: make(map[string]string),
		loaded: make(map[string]bool),
	}
}

func (f *fakeRuntime) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	switch {
	case request.Method == http.MethodHead &&
		strings.HasPrefix(request.URL.Path, "/api/blobs/"):
		f.mu.Lock()
		_, exists := f.blobs[strings.TrimPrefix(
			request.URL.Path, "/api/blobs/",
		)]
		f.mu.Unlock()
		if !exists {
			writer.WriteHeader(http.StatusNotFound)
		}
	case request.Method == http.MethodPost &&
		strings.HasPrefix(request.URL.Path, "/api/blobs/"):
		f.pushBlob(writer, request)
	case request.Method == http.MethodPost && request.URL.Path == "/api/create":
		f.create(writer, request)
	case request.Method == http.MethodPost && request.URL.Path == "/api/show":
		f.show(writer, request)
	case request.Method == http.MethodGet && request.URL.Path == "/api/tags":
		f.tags(writer)
	case request.Method == http.MethodPost && request.URL.Path == "/api/generate":
		f.generate(writer, request)
	case request.Method == http.MethodDelete && request.URL.Path == "/api/delete":
		f.delete(writer, request)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeRuntime) pushBlob(
	writer http.ResponseWriter,
	request *http.Request,
) {
	digest := strings.TrimPrefix(request.URL.Path, "/api/blobs/")
	data, err := io.ReadAll(request.Body)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256(data)
	if digest != "sha256:"+hex.EncodeToString(sum[:]) {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.blobs[digest] = data
	f.uploads++
	f.mu.Unlock()
	writer.WriteHeader(http.StatusCreated)
}

func (f *fakeRuntime) create(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var value struct {
		Model string            `json:"model"`
		Files map[string]string `json:"files"`
	}
	if json.NewDecoder(request.Body).Decode(&value) != nil ||
		len(value.Files) != 1 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	digest := value.Files[artifactFilename]
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blobs[digest] == nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	f.models[value.Model] = digest
	f.creates++
	if f.failCreate {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, _ = writer.Write([]byte(`{"status":"success"}`))
}

func (f *fakeRuntime) show(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var value struct {
		Model string `json:"model"`
	}
	if json.NewDecoder(request.Body).Decode(&value) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	digest := f.models[value.Model]
	override := f.overrideShow
	f.mu.Unlock()
	if digest == "" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if override != "" {
		_, _ = writer.Write([]byte(override))
		return
	}
	_, _ = writer.Write([]byte(
		`{"modelfile":"FROM /models/blobs/sha256-` +
			strings.TrimPrefix(digest, "sha256:") +
			`","details":{"format":"gguf"}}`,
	))
}

func (f *fakeRuntime) tags(writer http.ResponseWriter) {
	f.mu.Lock()
	names := make([]string, 0, len(f.models))
	for name := range f.models {
		names = append(names, name)
	}
	f.mu.Unlock()
	sort.Strings(names)
	type model struct {
		Name  string `json:"name"`
		Model string `json:"model"`
	}
	response := struct {
		Models []model `json:"models"`
	}{Models: make([]model, 0, len(names))}
	for _, name := range names {
		response.Models = append(response.Models, model{Name: name, Model: name})
	}
	_ = json.NewEncoder(writer).Encode(response)
}

func (f *fakeRuntime) generate(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var value struct {
		Model     string          `json:"model"`
		KeepAlive json.RawMessage `json:"keep_alive"`
	}
	if json.NewDecoder(request.Body).Decode(&value) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.models[value.Model] == "" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if string(value.KeepAlive) == "0" {
		delete(f.loaded, value.Model)
	} else {
		f.loaded[value.Model] = true
	}
	_, _ = writer.Write([]byte(
		`{"model":` + quote(value.Model) + `,"done":true}`,
	))
}

func (f *fakeRuntime) delete(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var value struct {
		Model string `json:"model"`
	}
	if json.NewDecoder(request.Body).Decode(&value) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	if f.failDelete {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	if f.models[value.Model] == "" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	delete(f.models, value.Model)
	delete(f.loaded, value.Model)
}

func quote(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func backendConfig(baseURL string) Config {
	return Config{
		BaseURL: baseURL, SlotID: "primary", Model: "approved",
		Timeout: time.Second, ConnectTimeout: time.Second,
		CleanupTimeout: time.Second, MaxConnections: 2,
		MaxResponseHeaderBytes: 4096, MaxResponseBytes: 1 << 20,
	}
}

func loadRequest(data []byte) modelactivation.LoadRequest {
	sum := sha256.Sum256(data)
	return modelactivation.LoadRequest{
		SlotID: "primary", Model: "approved",
		ModelDigest: "sha256:" + hex.EncodeToString(sum[:]),
		SizeBytes:   uint64(len(data)), Artifact: bytes.NewReader(data),
	}
}

func TestBackendLifecycleAndRecoveryInspection(t *testing.T) {
	runtime := newFakeRuntime()
	server := httptest.NewServer(runtime)
	defer server.Close()
	backend, err := New(backendConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	request := loadRequest([]byte("small-approved-gguf"))
	binding, err := backend.Load(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	expectedHandle, _ := ollamabinding.RuntimeModel(
		"primary", request.ModelDigest,
	)
	if binding.Handle != expectedHandle ||
		binding.ModelDigest != request.ModelDigest {
		t.Fatalf("binding=%#v", binding)
	}
	if err := backend.Health(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	inspected, err := backend.Inspect(context.Background(), "primary")
	if err != nil || inspected.Count != 1 ||
		inspected.Bindings[0] != binding {
		t.Fatalf("inspected=%#v err=%v", inspected, err)
	}
	if _, err := backend.Load(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	if runtime.uploads != 1 || runtime.creates != 1 ||
		!runtime.loaded[binding.Handle] {
		t.Fatalf(
			"uploads=%d creates=%d loaded=%v",
			runtime.uploads, runtime.creates, runtime.loaded,
		)
	}
	runtime.mu.Unlock()
	if err := backend.Unload(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	inspected, err = backend.Inspect(context.Background(), "primary")
	if err != nil || inspected.Count != 0 {
		t.Fatalf("post-unload inspected=%#v err=%v", inspected, err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Health(
		context.Background(), binding,
	); err == nil {
		t.Fatal("closed backend accepted health request")
	}
}

func TestBackendCreateFailureCleansCandidate(t *testing.T) {
	runtime := newFakeRuntime()
	runtime.failCreate = true
	server := httptest.NewServer(runtime)
	defer server.Close()
	backend, err := New(backendConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	request := loadRequest([]byte("candidate"))
	binding, err := backend.Load(context.Background(), request)
	if err == nil || strings.Contains(err.Error(), server.URL) {
		t.Fatalf("create failure binding=%#v err=%v", binding, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.models) != 0 || runtime.deletes != 1 {
		t.Fatalf(
			"failed create models=%v deletes=%d",
			runtime.models, runtime.deletes,
		)
	}
}

func TestBackendRejectsChangedSourceAndCandidateOverflow(t *testing.T) {
	runtime := newFakeRuntime()
	server := httptest.NewServer(runtime)
	defer server.Close()
	backend, err := New(backendConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	request := loadRequest([]byte("approved"))
	binding, err := backend.Load(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	runtime.overrideShow = `{"modelfile":"FROM sha256-` +
		strings.Repeat("f", 64) + `","details":{"format":"gguf"}}`
	runtime.mu.Unlock()
	if err := backend.Health(context.Background(), binding); err == nil {
		t.Fatal("changed model source accepted")
	}
	runtime.mu.Lock()
	runtime.overrideShow = ""
	for index := 0; index < modelactivation.MaxRecoveryBindingsHard; index++ {
		digest := "sha256:" + strings.Repeat(
			string(rune('b'+index)), 64,
		)
		handle, nameErr := ollamabinding.RuntimeModel("primary", digest)
		if nameErr != nil {
			t.Fatal(nameErr)
		}
		runtime.models[handle] = digest
		runtime.blobs[digest] = []byte("other")
	}
	runtime.mu.Unlock()
	if _, err := backend.Inspect(
		context.Background(), "primary",
	); err == nil {
		t.Fatal("too many owned runtime candidates accepted")
	}
}

func TestBackendWaiterCancellationAndConfigurationBounds(t *testing.T) {
	if _, err := New(backendConfig("http://runtime.example")); err == nil {
		t.Fatal("remote plaintext activation endpoint accepted")
	}
	runtime := newFakeRuntime()
	server := httptest.NewServer(runtime)
	defer server.Close()
	backend, err := New(backendConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	<-backend.gate
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backend.Inspect(ctx, "primary"); err == nil {
		t.Fatal("canceled activation waiter succeeded")
	}
	backend.gate <- struct{}{}
}

func TestBackendRejectsOversizedTimeoutAndMaliciousJSON(t *testing.T) {
	t.Run("oversized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			_, _ = writer.Write([]byte(strings.Repeat("x", 65)))
		}))
		defer server.Close()
		config := backendConfig(server.URL)
		config.MaxResponseBytes = 64
		backend, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()
		if _, err := backend.Inspect(
			context.Background(), "primary",
		); err == nil {
			t.Fatal("oversized activation response accepted")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			time.Sleep(100 * time.Millisecond)
			_, _ = writer.Write([]byte(`{"models":[]}`))
		}))
		defer server.Close()
		config := backendConfig(server.URL)
		config.Timeout = 20 * time.Millisecond
		backend, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()
		if _, err := backend.Inspect(
			context.Background(), "primary",
		); err == nil {
			t.Fatal("timed-out activation request succeeded")
		}
	})
	t.Run("malicious JSON", func(t *testing.T) {
		values := []string{
			`{"models":[],"models":[]}`,
			`{"models":[]}{}`,
			`{"models":[` + strings.Repeat(`{"name":"x"},`, MaxInventoryModelsHard) +
				`{"name":"x"}]}`,
			strings.Repeat(`{"x":`, maxJSONDepth) + `0` +
				strings.Repeat(`}`, maxJSONDepth),
		}
		for index, value := range values {
			var target struct {
				Models []struct {
					Name string `json:"name"`
				} `json:"models"`
			}
			if err := decodeSingle([]byte(value), &target); err == nil {
				t.Fatalf("malicious JSON %d accepted", index)
			}
		}
	})
	t.Run("missing inventory", func(t *testing.T) {
		for index, body := range []string{`{}`, `{"models":null}`} {
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter,
				_ *http.Request,
			) {
				_, _ = writer.Write([]byte(body))
			}))
			config := backendConfig(server.URL)
			backend, err := New(config)
			if err != nil {
				server.Close()
				t.Fatal(err)
			}
			_, inspectErr := backend.Inspect(context.Background(), "primary")
			_ = backend.Close()
			server.Close()
			if inspectErr == nil {
				t.Fatalf("missing inventory %d accepted", index)
			}
		}
	})
}
