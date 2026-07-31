// Package runtime defines the narrow contract between the TOS AI worker and a
// model-serving runtime. Adapters execute behind this interface and do not
// receive wallet or fleet owner credentials.
package runtime

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/tosnetwork/tos-ai/pkg/admission"
)

const (
	MaxCapabilityStringBytes      = 256
	MaxAcceptedPriorities         = 3
	MaxInputOutputBytesHard       = uint64(64 << 20)
	MaxPreflightResponseBytesHard = uint64(1 << 20)
	MaxPreflightModelsHard        = 256
)

type BindingEvidence string

const (
	BindingDeclared        BindingEvidence = "declared"
	BindingLocallyObserved BindingEvidence = "locally-observed"
)

type Priority uint8

const (
	PriorityEmergency Priority = iota + 1
	PriorityControl
	PriorityRealtimePerception
	PriorityLocalAsync
	PriorityExternalService
	PriorityBackground
)

type Capability struct {
	ServiceID          string
	Operation          string
	Model              string
	ModelDigest        string
	Runtime            string
	RuntimeRevision    string
	MaxInputBytes      uint64
	MaxOutputBytes     uint64
	AcceptedPriorities []Priority
	Admission          admission.Resources
}

type Request struct {
	RequestID      string
	Operation      string
	Model          string
	Payload        []byte
	MaxOutputBytes uint64
}

type Usage struct {
	InputBytes      uint64
	OutputBytes     uint64
	InputTokens     uint64
	OutputTokens    uint64
	ExecutionMillis uint64
}

type Response struct {
	Output          []byte
	Usage           Usage
	ModelRevision   string
	RuntimeRevision string
}

type Preflight struct {
	Model          string
	ModelDigest    string
	DigestEvidence BindingEvidence
}

type Adapter interface {
	Capability() Capability
	Preflight(context.Context) (Preflight, error)
	Execute(context.Context, Request) (Response, error)
}

// AdapterCloser is implemented by adapters with connection pools or other
// process-local resources that must be released during worker shutdown.
type AdapterCloser interface {
	Adapter
	Close() error
}

type ErrorKind string

const (
	ErrorInvalid     ErrorKind = "invalid_request"
	ErrorCanceled    ErrorKind = "canceled"
	ErrorTimeout     ErrorKind = "timeout"
	ErrorUnavailable ErrorKind = "unavailable"
	ErrorRemote      ErrorKind = "runtime_rejected"
	ErrorProtocol    ErrorKind = "invalid_runtime_response"
	ErrorLimit       ErrorKind = "resource_limit"
	ErrorInternal    ErrorKind = "runtime_failure"
)

type Error struct {
	Kind  ErrorKind
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "runtime failure"
	}
	return string(e.Kind)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewError(kind ErrorKind, cause error) error {
	return &Error{Kind: kind, Cause: cause}
}

func ErrorKindOf(err error) ErrorKind {
	var runtimeError *Error
	if errors.As(err, &runtimeError) {
		return runtimeError.Kind
	}
	return ErrorInternal
}

func ValidateRequest(capability Capability, request Request) error {
	if request.RequestID == "" || len(request.RequestID) > 128 {
		return errors.New("invalid request ID")
	}
	if request.Operation != capability.Operation || request.Model != capability.Model {
		return errors.New("operation or model mismatch")
	}
	if uint64(len(request.Payload)) > capability.MaxInputBytes {
		return errors.New("input exceeds adapter limit")
	}
	if request.MaxOutputBytes == 0 || request.MaxOutputBytes > capability.MaxOutputBytes {
		return errors.New("output limit is invalid")
	}
	return nil
}

func ValidateCapability(capability Capability) error {
	values := []string{
		capability.ServiceID, capability.Operation, capability.Model,
		capability.Runtime, capability.RuntimeRevision,
	}
	for _, value := range values {
		if value == "" || len(value) > MaxCapabilityStringBytes ||
			strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return errors.New("invalid capability identity")
		}
	}
	if len(capability.ModelDigest) != len("sha256:")+64 ||
		!strings.HasPrefix(capability.ModelDigest, "sha256:") {
		return errors.New("capability model digest must be SHA-256")
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(capability.ModelDigest, "sha256:")); err != nil {
		return errors.New("capability model digest must be SHA-256")
	}
	if capability.MaxInputBytes == 0 || capability.MaxInputBytes > MaxInputOutputBytesHard ||
		capability.MaxOutputBytes == 0 || capability.MaxOutputBytes > MaxInputOutputBytesHard ||
		len(capability.AcceptedPriorities) == 0 ||
		len(capability.AcceptedPriorities) > MaxAcceptedPriorities ||
		capability.Admission.RAMBytes == 0 || capability.Admission.ContextTokens == 0 ||
		capability.Admission.BatchSize == 0 || capability.Admission.ExecutionTime <= 0 {
		return errors.New("invalid capability bounds")
	}
	seen := make(map[Priority]struct{}, len(capability.AcceptedPriorities))
	for _, priority := range capability.AcceptedPriorities {
		if priority < PriorityLocalAsync || priority > PriorityBackground {
			return errors.New("capability contains a forbidden priority")
		}
		if _, exists := seen[priority]; exists {
			return errors.New("capability contains a duplicate priority")
		}
		seen[priority] = struct{}{}
	}
	return nil
}

func ValidatePreflight(capability Capability, preflight Preflight) error {
	if preflight.Model != capability.Model ||
		preflight.ModelDigest != capability.ModelDigest {
		return errors.New("runtime model binding does not match capability")
	}
	switch preflight.DigestEvidence {
	case BindingDeclared, BindingLocallyObserved:
		return nil
	default:
		return errors.New("runtime model binding has invalid evidence")
	}
}

func MillisecondsSince(start time.Time) uint64 {
	elapsed := time.Since(start)
	if elapsed < 0 {
		return 0
	}
	return uint64(elapsed.Milliseconds())
}
