package mock

import (
	"context"
	"testing"
)

func TestAdapterCopiesInput(t *testing.T) {
	adapter := New(0)
	response, err := adapter.Execute(context.Background(), request(adapter))
	if err != nil {
		t.Fatal(err)
	}
	response.Output[0] = 'X'
	if request(adapter).Payload[0] == 'X' {
		t.Fatal("adapter returned aliased input")
	}
}

func request(adapter *Adapter) (requestValue struct {
	RequestID      string
	Operation      string
	Model          string
	Payload        []byte
	MaxOutputBytes uint64
}) {
	capability := adapter.Capability()
	return struct {
		RequestID      string
		Operation      string
		Model          string
		Payload        []byte
		MaxOutputBytes uint64
	}{
		RequestID:      "request-1",
		Operation:      capability.Operation,
		Model:          capability.Model,
		Payload:        []byte("hello"),
		MaxOutputBytes: 16,
	}
}
