package textgeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
)

func TestMapperRegistersAndMapsNormativeVector(t *testing.T) {
	mapper, err := NewMapper([]Route{{ServiceID: "tos.ai.mock", Model: "deterministic-echo"}})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := mapper.Registration()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := edge.NewProfileInvocationRegistry(
		[]edge.ProfileInvocationRegistration{registration},
	)
	if err != nil {
		t.Fatal(err)
	}
	if registry.Len() != 1 {
		t.Fatal("profile registration was not installed")
	}

	vectors := loadVectors(t)
	if vectors.ProfileID != ProfileID || vectors.ProfileVersion != ProfileVersion ||
		vectors.Operation != Operation || len(vectors.Valid) == 0 {
		t.Fatalf("invalid normative vector header: %#v", vectors)
	}
	for _, vector := range vectors.Valid {
		t.Run(vector.Name, func(t *testing.T) {
			output, err := mapper.MapInvocation(context.Background(), edge.ProfileInvocationInput{
				ProfileID: ProfileID, ProfileVersion: ProfileVersion,
				Operation: Operation, ServiceID: vector.ServiceID,
				Intent: []byte(vector.Intent), MaxInputBytes: uint64(len(vector.Intent)),
				MaxOutputBytes: 4096,
			})
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(output.Payload)
			if output.Model != vector.ExpectedModel ||
				"sha256:"+hex.EncodeToString(digest[:]) != vector.ExpectedPayloadDigest {
				t.Fatalf("unexpected mapping: %#v", output)
			}
			output.Payload[0] ^= 0xff
			again, err := mapper.MapInvocation(context.Background(), edge.ProfileInvocationInput{
				ProfileID: ProfileID, ProfileVersion: ProfileVersion,
				Operation: Operation, ServiceID: vector.ServiceID,
				Intent: []byte(vector.Intent), MaxInputBytes: uint64(len(vector.Intent)),
				MaxOutputBytes: 4096,
			})
			if err != nil || string(again.Payload) != vector.ExpectedPayload {
				t.Fatal("mapping output aliased mutable state")
			}
		})
	}
}

func TestNormativeIntentSchemaIsValidJSON(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(
		"..", "..", "..", "spec", "profiles", "text-generation", "v0.1", "intent.schema.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" ||
		schema["type"] != "object" {
		t.Fatalf("unexpected normative schema header: %#v", schema)
	}
}

func TestMapperRejectsMalformedUnboundedOrUnregisteredIntent(t *testing.T) {
	mapper, err := NewMapper([]Route{{ServiceID: "tos.ai.mock", Model: "deterministic-echo"}})
	if err != nil {
		t.Fatal(err)
	}
	vectors := loadVectors(t)
	for _, vector := range vectors.Invalid {
		t.Run(vector.Name, func(t *testing.T) {
			if _, err := mapper.MapInvocation(context.Background(), edge.ProfileInvocationInput{
				ProfileID: ProfileID, ProfileVersion: ProfileVersion,
				Operation: Operation, ServiceID: "tos.ai.mock",
				Intent: []byte(vector.Intent), MaxInputBytes: uint64(max(1, len(vector.Intent))),
				MaxOutputBytes: 4096,
			}); err == nil {
				t.Fatal("invalid vector was accepted")
			}
		})
	}
	valid := []byte(`{"model":"deterministic-echo","prompt":"hello"}`)
	tests := []struct {
		name  string
		input edge.ProfileInvocationInput
	}{
		{"wrong version", edge.ProfileInvocationInput{ProfileID: ProfileID, ProfileVersion: "0.2.0", Operation: Operation, ServiceID: "tos.ai.mock", Intent: valid, MaxInputBytes: 1024, MaxOutputBytes: 1024}},
		{"extension", edge.ProfileInvocationInput{ProfileID: ProfileID, ProfileVersion: ProfileVersion, ProfileExtensions: []string{"x"}, Operation: Operation, ServiceID: "tos.ai.mock", Intent: valid, MaxInputBytes: 1024, MaxOutputBytes: 1024}},
		{"route", edge.ProfileInvocationInput{ProfileID: ProfileID, ProfileVersion: ProfileVersion, Operation: Operation, ServiceID: "tos.ai.other", Intent: valid, MaxInputBytes: 1024, MaxOutputBytes: 1024}},
		{"quoted input", edge.ProfileInvocationInput{ProfileID: ProfileID, ProfileVersion: ProfileVersion, Operation: Operation, ServiceID: "tos.ai.mock", Intent: valid, MaxInputBytes: 1, MaxOutputBytes: 1024}},
		{"zero output", edge.ProfileInvocationInput{ProfileID: ProfileID, ProfileVersion: ProfileVersion, Operation: Operation, ServiceID: "tos.ai.mock", Intent: valid, MaxInputBytes: 1024}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := mapper.MapInvocation(context.Background(), test.input); err == nil {
				t.Fatal("invalid mapping input was accepted")
			}
		})
	}
	invalidUTF8 := append(
		[]byte(`{"model":"deterministic-echo","prompt":"`),
		0xff, '"', '}',
	)
	if _, err := mapper.MapInvocation(context.Background(), edge.ProfileInvocationInput{
		ProfileID: ProfileID, ProfileVersion: ProfileVersion,
		Operation: Operation, ServiceID: "tos.ai.mock", Intent: invalidUTF8,
		MaxInputBytes: 1024, MaxOutputBytes: 1024,
	}); err == nil {
		t.Fatal("invalid UTF-8 intent was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mapper.MapInvocation(canceled, tests[0].input); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
}

func TestMapperConstructionIsBoundedAndImmutable(t *testing.T) {
	routes := []Route{{ServiceID: "tos.ai.mock", Model: "deterministic-echo"}}
	mapper, err := NewMapper(routes)
	if err != nil {
		t.Fatal(err)
	}
	routes[0].Model = "changed"
	valid := edge.ProfileInvocationInput{
		ProfileID: ProfileID, ProfileVersion: ProfileVersion,
		Operation: Operation, ServiceID: "tos.ai.mock",
		Intent:        []byte(`{"model":"deterministic-echo","prompt":"hello"}`),
		MaxInputBytes: 1024, MaxOutputBytes: 1024,
	}
	if _, err := mapper.MapInvocation(context.Background(), valid); err != nil {
		t.Fatal("caller mutation changed mapper routes")
	}
	const readers = 64
	var wait sync.WaitGroup
	errorsSeen := make(chan error, readers)
	for range readers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			output, err := mapper.MapInvocation(context.Background(), valid)
			if err != nil {
				errorsSeen <- err
				return
			}
			if output.Model != "deterministic-echo" || string(output.Payload) != "hello" {
				errorsSeen <- errors.New("concurrent mapping changed output")
			}
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	if _, err := NewMapper([]Route{routes[0], routes[0]}); err == nil {
		t.Fatal("duplicate route accepted")
	}
	if _, err := NewMapper(nil); err == nil {
		t.Fatal("empty route set accepted")
	}
	overLimit := make([]Route, MaxRoutes+1)
	for index := range overLimit {
		overLimit[index] = Route{
			ServiceID: "tos.ai.mock", Model: fmt.Sprintf("model-%03d", index),
		}
	}
	if _, err := NewMapper(overLimit); err == nil {
		t.Fatal("oversized route set accepted")
	}
	var nilMapper *Mapper
	if _, err := nilMapper.Registration(); err == nil {
		t.Fatal("nil mapper registered")
	}
}

func TestMapperDerivesOnlyExternallyCallableTextGenerationRoutes(t *testing.T) {
	external := validCapability("tos.ai.external", Operation, "model-a")
	otherOperation := validCapability("tos.ai.embedding", "embed", "model-b")
	ownerOnly := validCapability("tos.ai.owner", Operation, "model-c")
	ownerOnly.AcceptedPriorities = []airuntime.Priority{airuntime.PriorityLocalAsync}
	mapper, err := NewMapperFromCapabilities([]airuntime.Capability{
		external, otherOperation, ownerOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	mapModel := func(serviceID, model string) error {
		intent, err := json.Marshal(Intent{Model: model, Prompt: "hello"})
		if err != nil {
			return err
		}
		_, err = mapper.MapInvocation(context.Background(), edge.ProfileInvocationInput{
			ProfileID: ProfileID, ProfileVersion: ProfileVersion,
			Operation: Operation, ServiceID: serviceID, Intent: intent,
			MaxInputBytes: 1024, MaxOutputBytes: 1024,
		})
		return err
	}
	if err := mapModel(external.ServiceID, external.Model); err != nil {
		t.Fatal(err)
	}
	if err := mapModel(otherOperation.ServiceID, otherOperation.Model); err == nil {
		t.Fatal("different operation was routed as text generation")
	}
	if err := mapModel(ownerOnly.ServiceID, ownerOnly.Model); err == nil {
		t.Fatal("owner-only capability was exposed to external profile work")
	}

	invalid := external
	invalid.ModelDigest = "invalid"
	if _, err := NewMapperFromCapabilities(
		[]airuntime.Capability{invalid},
	); err == nil {
		t.Fatal("invalid capability was accepted for profile routing")
	}
	if _, err := NewMapperFromCapabilities(
		[]airuntime.Capability{external, external},
	); err == nil {
		t.Fatal("duplicate capability route was accepted")
	}
	if _, err := NewMapperFromCapabilities(
		[]airuntime.Capability{ownerOnly},
	); err == nil {
		t.Fatal("route set without external text generation was accepted")
	}
	if _, err := NewMapperFromCapabilities(make(
		[]airuntime.Capability,
		MaxRoutes+1,
	)); err == nil {
		t.Fatal("oversized capability set was accepted")
	}
}

func validCapability(
	serviceID string,
	operation string,
	model string,
) airuntime.Capability {
	return airuntime.Capability{
		ServiceID: serviceID, Operation: operation, Model: model,
		ModelDigest: "sha256:" + strings.Repeat("a", 64),
		Runtime:     "test", RuntimeRevision: "test-v1",
		MaxInputBytes: 1024, MaxOutputBytes: 1024,
		AcceptedPriorities: []airuntime.Priority{
			airuntime.PriorityExternalService,
		},
		Admission: admission.Resources{
			RAMBytes: 1, ContextTokens: 1, BatchSize: 1,
			ExecutionTime: time.Second,
		},
	}
}

type vectorFile struct {
	ProfileID      string `json:"profileId"`
	ProfileVersion string `json:"profileVersion"`
	Operation      string `json:"operation"`
	Valid          []struct {
		Name                  string `json:"name"`
		ServiceID             string `json:"serviceId"`
		Intent                string `json:"intent"`
		ExpectedModel         string `json:"expectedModel"`
		ExpectedPayload       string `json:"expectedPayload"`
		ExpectedPayloadDigest string `json:"expectedPayloadDigest"`
	} `json:"valid"`
	Invalid []struct {
		Name   string `json:"name"`
		Intent string `json:"intent"`
	} `json:"invalid"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		"..", "..", "..", "spec", "profiles", "text-generation", "v0.1", "vectors.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var vectors vectorFile
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}
