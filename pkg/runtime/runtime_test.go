package runtime

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
)

func validCapability() Capability {
	return Capability{
		ServiceID: "service", Operation: "generate", Model: "model",
		ModelDigest: "sha256:" + strings.Repeat("a", 64),
		Runtime:     "test", RuntimeRevision: "v1",
		MaxInputBytes: 1024, MaxOutputBytes: 2048,
		AcceptedPriorities: []Priority{PriorityExternalService},
		Admission: admission.Resources{
			RAMBytes: 1, ContextTokens: 1, BatchSize: 1,
			ExecutionTime: time.Second,
		},
	}
}

func TestValidatePreflightRequiresExactBindingAndEvidence(t *testing.T) {
	capability := validCapability()
	valid := Preflight{
		Model: capability.Model, ModelDigest: capability.ModelDigest,
		DigestEvidence: BindingLocallyObserved,
	}
	if err := ValidatePreflight(capability, valid); err != nil {
		t.Fatal(err)
	}
	declared := valid
	declared.DigestEvidence = BindingDeclared
	if err := ValidatePreflight(capability, declared); err != nil {
		t.Fatal(err)
	}
	tests := []Preflight{
		{Model: "other", ModelDigest: valid.ModelDigest, DigestEvidence: valid.DigestEvidence},
		{Model: valid.Model, ModelDigest: "sha256:" + strings.Repeat("b", 64), DigestEvidence: valid.DigestEvidence},
		{Model: valid.Model, ModelDigest: valid.ModelDigest, DigestEvidence: "attested"},
	}
	for _, invalid := range tests {
		if err := ValidatePreflight(capability, invalid); err == nil {
			t.Fatalf("invalid preflight accepted: %#v", invalid)
		}
	}
}

func TestValidateCapabilityRejectsEveryUntrustedBoundary(t *testing.T) {
	valid := validCapability()
	if err := ValidateCapability(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Capability)
	}{
		{"empty identity", func(v *Capability) { v.ServiceID = "" }},
		{"control identity", func(v *Capability) { v.Runtime = "bad\nvalue" }},
		{"long identity", func(v *Capability) { v.Model = strings.Repeat("x", MaxCapabilityStringBytes+1) }},
		{"digest length", func(v *Capability) { v.ModelDigest = "sha256:00" }},
		{"digest encoding", func(v *Capability) { v.ModelDigest = "sha256:" + strings.Repeat("z", 64) }},
		{"input zero", func(v *Capability) { v.MaxInputBytes = 0 }},
		{"input hard limit", func(v *Capability) { v.MaxInputBytes = MaxInputOutputBytesHard + 1 }},
		{"output zero", func(v *Capability) { v.MaxOutputBytes = 0 }},
		{"priorities empty", func(v *Capability) { v.AcceptedPriorities = nil }},
		{"priorities excess", func(v *Capability) {
			v.AcceptedPriorities = make([]Priority, MaxAcceptedPriorities+1)
		}},
		{"forbidden priority", func(v *Capability) { v.AcceptedPriorities = []Priority{PriorityEmergency} }},
		{"duplicate priority", func(v *Capability) {
			v.AcceptedPriorities = []Priority{PriorityExternalService, PriorityExternalService}
		}},
		{"RAM zero", func(v *Capability) { v.Admission.RAMBytes = 0 }},
		{"context zero", func(v *Capability) { v.Admission.ContextTokens = 0 }},
		{"batch zero", func(v *Capability) { v.Admission.BatchSize = 0 }},
		{"execution zero", func(v *Capability) { v.Admission.ExecutionTime = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			value.AcceptedPriorities = append([]Priority(nil), valid.AcceptedPriorities...)
			test.mutate(&value)
			if err := ValidateCapability(value); err == nil {
				t.Fatalf("invalid capability accepted: %#v", value)
			}
		})
	}
}

func TestValidateRequestBindsIdentityAndByteLimits(t *testing.T) {
	capability := validCapability()
	valid := Request{
		RequestID: "request-0001", Operation: capability.Operation,
		Model: capability.Model, Payload: []byte("input"),
		MaxOutputBytes: capability.MaxOutputBytes,
	}
	if err := ValidateRequest(capability, valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Request){
		"empty ID":    func(v *Request) { v.RequestID = "" },
		"long ID":     func(v *Request) { v.RequestID = strings.Repeat("x", 129) },
		"operation":   func(v *Request) { v.Operation = "embed" },
		"model":       func(v *Request) { v.Model = "other" },
		"input":       func(v *Request) { v.Payload = make([]byte, capability.MaxInputBytes+1) },
		"zero output": func(v *Request) { v.MaxOutputBytes = 0 },
		"output":      func(v *Request) { v.MaxOutputBytes = capability.MaxOutputBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if err := ValidateRequest(capability, value); err == nil {
				t.Fatalf("invalid request accepted: %#v", value)
			}
		})
	}
}

func TestRuntimeErrorsAndElapsedTimeAreBounded(t *testing.T) {
	cause := errors.New("private runtime detail")
	err := NewError(ErrorTimeout, cause)
	if ErrorKindOf(err) != ErrorTimeout || !errors.Is(err, cause) ||
		err.Error() != string(ErrorTimeout) {
		t.Fatalf("runtime error=%v kind=%q", err, ErrorKindOf(err))
	}
	if ErrorKindOf(errors.New("foreign")) != ErrorInternal ||
		ErrorKindOf(nil) != ErrorInternal {
		t.Fatal("foreign error was assigned a trusted runtime category")
	}
	var nilError *Error
	if nilError.Error() != "runtime failure" || nilError.Unwrap() != nil {
		t.Fatal("nil runtime error is unsafe")
	}
	if MillisecondsSince(time.Now().Add(time.Second)) != 0 {
		t.Fatal("future start produced elapsed runtime")
	}
	if elapsed := MillisecondsSince(time.Now().Add(-5 * time.Millisecond)); elapsed == 0 || elapsed > 100 {
		t.Fatalf("elapsed milliseconds=%d", elapsed)
	}
}
