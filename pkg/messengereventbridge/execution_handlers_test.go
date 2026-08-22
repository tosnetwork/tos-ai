package messengereventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
	"github.com/tosnetwork/tos-messenger/pkg/envelope"
	"github.com/tosnetwork/tos-messenger/pkg/payload"
)

type a2aExecutorFake struct {
	calls int
	task  *a2a.Task
	fail  bool
}

func (e *a2aExecutorFake) Execute(context.Context, *a2a.SendMessageRequest) (*a2a.Task, error) {
	e.calls++
	if e.fail {
		return nil, errors.New("execute")
	}
	return e.task, nil
}

type a2aResultsFake struct {
	calls int
	fail  bool
}

func (r *a2aResultsFake) ReceiveA2AResult(context.Context, envelope.Event, *a2a.Task) error {
	r.calls++
	if r.fail {
		return errors.New("persist")
	}
	return nil
}

type mcpExecutorFake struct {
	calls  int
	output mcpadapter.Output
	fail   bool
}

func (e *mcpExecutorFake) Call(context.Context, *mcp.CallToolRequest, mcpadapter.Input) (*mcp.CallToolResult, mcpadapter.Output, error) {
	e.calls++
	if e.fail {
		return nil, mcpadapter.Output{}, errors.New("execute")
	}
	return nil, e.output, nil
}

type mcpResultsFake struct {
	values []mcpadapter.Output
	fail   bool
}

func (r *mcpResultsFake) ReceiveMCPResult(_ context.Context, _ envelope.Event, output mcpadapter.Output) error {
	r.values = append(r.values, output)
	if r.fail {
		return errors.New("persist")
	}
	return nil
}

func TestA2AExecutionHandlerDecodesExecutesAndCommitsResult(t *testing.T) {
	request := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("work"))}
	request.Message.ID = "message"
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	executor := &a2aExecutorFake{task: &a2a.Task{ID: "task", ContextID: "context"}}
	results := &a2aResultsFake{}
	handler, err := NewA2AExecutionHandler(executor, results)
	if err != nil {
		t.Fatal(err)
	}
	event := decodedEvent(t, "a2a.message")
	if err := handler.HandleA2A(context.Background(), event, payload.A2AMessage{Foreign: payload.Foreign{
		Protocol: "a2a", Version: "1", Body: raw,
	}}); err != nil {
		t.Fatal(err)
	}
	if executor.calls != 1 || results.calls != 1 {
		t.Fatalf("A2A handler did not execute and commit once: execute=%d result=%d", executor.calls, results.calls)
	}
	results.fail = true
	if err := handler.HandleA2A(context.Background(), event, payload.A2AMessage{Foreign: payload.Foreign{
		Protocol: "a2a", Version: "1", Body: raw,
	}}); err == nil {
		t.Fatal("result persistence failure was acknowledged")
	}
}

func TestMCPExecutionHandlerSeparatesCallsAndResults(t *testing.T) {
	output := mcpadapter.Output{Protocol: "tos_service_v1", ExecutionID: "sha256:test"}
	executor := &mcpExecutorFake{output: output}
	results := &mcpResultsFake{}
	handler, err := NewMCPExecutionHandler(executor, results)
	if err != nil {
		t.Fatal(err)
	}
	inputRaw, _ := json.Marshal(mcpadapter.Input{ExecutionID: "sha256:input"})
	if err := handler.HandleMCPCall(context.Background(), decodedEvent(t, "mcp.call"), payload.MCPCall{Foreign: payload.Foreign{
		Protocol: "mcp", Version: "1", Body: inputRaw,
	}}); err != nil {
		t.Fatal(err)
	}
	outputRaw, _ := json.Marshal(output)
	if err := handler.HandleMCPResult(context.Background(), decodedEvent(t, "mcp.result"), payload.MCPResult{Foreign: payload.Foreign{
		Protocol: "mcp", Version: "1", Body: outputRaw,
	}}); err != nil {
		t.Fatal(err)
	}
	if executor.calls != 1 || len(results.values) != 2 || results.values[0].ExecutionID != output.ExecutionID ||
		results.values[1].ExecutionID != output.ExecutionID {
		t.Fatalf("unexpected MCP execution/result flow: execute=%d results=%+v", executor.calls, results.values)
	}
}

func TestExecutionHandlersRejectProfileUnknownFieldsAndTrailingJSON(t *testing.T) {
	a2aHandler, _ := NewA2AExecutionHandler(&a2aExecutorFake{task: &a2a.Task{}}, &a2aResultsFake{})
	for _, body := range []payload.A2AMessage{
		{Foreign: payload.Foreign{Protocol: "mcp", Version: "1", Body: []byte(`{}`)}},
		{Foreign: payload.Foreign{Protocol: "a2a", Version: "2", Body: []byte(`{}`)}},
		{Foreign: payload.Foreign{Protocol: "a2a", Version: "1", Body: []byte(`{"unknown":true}`)}},
		{Foreign: payload.Foreign{Protocol: "a2a", Version: "1", Body: []byte(`{} {}`)}},
	} {
		if err := a2aHandler.HandleA2A(context.Background(), decodedEvent(t, "a2a.message"), body); err == nil {
			t.Fatalf("invalid A2A body accepted: %+v", body.Foreign)
		}
	}
	mcpHandler, _ := NewMCPExecutionHandler(&mcpExecutorFake{}, &mcpResultsFake{})
	if err := mcpHandler.HandleMCPCall(context.Background(), decodedEvent(t, "mcp.call"), payload.MCPCall{Foreign: payload.Foreign{
		Protocol: "mcp", Version: "1", Body: []byte(`{"unknown":true}`),
	}}); err == nil {
		t.Fatal("unknown MCP call field was accepted")
	}
}

func decodedEvent(t *testing.T, kind string) envelope.Event {
	t.Helper()
	event, err := envelope.DecodeEventJSON(eventWire(t, kind, testSender, testConversation))
	if err != nil {
		t.Fatal(err)
	}
	return event
}
