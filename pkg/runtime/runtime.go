// Package runtime defines the narrow contract between the TOS AI worker and a
// model-serving runtime. Adapters execute behind this interface and do not
// receive wallet or fleet owner credentials.
package runtime

import (
	"context"
	"errors"
	"time"
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

type Adapter interface {
	Capability() Capability
	Execute(context.Context, Request) (Response, error)
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

func MillisecondsSince(start time.Time) uint64 {
	elapsed := time.Since(start)
	if elapsed < 0 {
		return 0
	}
	return uint64(elapsed.Milliseconds())
}
