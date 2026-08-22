package messengereventbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tosnetwork/tos-ai/pkg/a2aadapter"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
	"github.com/tosnetwork/tos-messenger/pkg/envelope"
	"github.com/tosnetwork/tos-messenger/pkg/payload"
)

type A2AExecutor interface {
	Execute(context.Context, *a2a.SendMessageRequest) (*a2a.Task, error)
}

type A2AResultReceiver interface {
	ReceiveA2AResult(context.Context, envelope.Event, *a2a.Task) error
}

type A2AExecutionHandler struct {
	executor A2AExecutor
	results  A2AResultReceiver
}

func NewA2AExecutionHandler(executor A2AExecutor, results A2AResultReceiver) (*A2AExecutionHandler, error) {
	if executor == nil || results == nil {
		return nil, errors.New("invalid A2A Messenger execution handler")
	}
	return &A2AExecutionHandler{executor: executor, results: results}, nil
}

func (h *A2AExecutionHandler) HandleA2A(ctx context.Context, event envelope.Event, body payload.A2AMessage) error {
	if h == nil || h.executor == nil || h.results == nil || ctx == nil || body.Protocol != "a2a" || body.Version != "1" {
		return errors.New("invalid A2A Messenger execution event")
	}
	var request a2a.SendMessageRequest
	if err := decodeStrictJSON(body.Body, &request); err != nil {
		return errors.New("invalid A2A Messenger request body")
	}
	task, err := h.executor.Execute(ctx, &request)
	if err != nil || task == nil {
		return errors.New("A2A Messenger execution failed")
	}
	// Messenger completes its durable application lease only after the result
	// receiver has made the outcome durable or published it idempotently.
	if err := h.results.ReceiveA2AResult(ctx, event, task); err != nil {
		return errors.New("A2A Messenger result was not committed")
	}
	return nil
}

type MCPExecutor interface {
	Call(context.Context, *mcp.CallToolRequest, mcpadapter.Input) (*mcp.CallToolResult, mcpadapter.Output, error)
}

type MCPResultReceiver interface {
	ReceiveMCPResult(context.Context, envelope.Event, mcpadapter.Output) error
}

type MCPExecutionHandler struct {
	executor MCPExecutor
	results  MCPResultReceiver
}

func NewMCPExecutionHandler(executor MCPExecutor, results MCPResultReceiver) (*MCPExecutionHandler, error) {
	if executor == nil || results == nil {
		return nil, errors.New("invalid MCP Messenger execution handler")
	}
	return &MCPExecutionHandler{executor: executor, results: results}, nil
}

func (h *MCPExecutionHandler) HandleMCPCall(ctx context.Context, event envelope.Event, body payload.MCPCall) error {
	if h == nil || h.executor == nil || h.results == nil || ctx == nil || body.Protocol != "mcp" || body.Version != "1" {
		return errors.New("invalid MCP Messenger call event")
	}
	var input mcpadapter.Input
	if err := decodeStrictJSON(body.Body, &input); err != nil {
		return errors.New("invalid MCP Messenger call body")
	}
	_, output, err := h.executor.Call(ctx, nil, input)
	if err != nil {
		return errors.New("MCP Messenger execution failed")
	}
	if err := h.results.ReceiveMCPResult(ctx, event, output); err != nil {
		return errors.New("MCP Messenger result was not committed")
	}
	return nil
}

func (h *MCPExecutionHandler) HandleMCPResult(ctx context.Context, event envelope.Event, body payload.MCPResult) error {
	if h == nil || h.results == nil || ctx == nil || body.Protocol != "mcp" || body.Version != "1" {
		return errors.New("invalid MCP Messenger result event")
	}
	var output mcpadapter.Output
	if err := decodeStrictJSON(body.Body, &output); err != nil {
		return errors.New("invalid MCP Messenger result body")
	}
	if err := h.results.ReceiveMCPResult(ctx, event, output); err != nil {
		return errors.New("MCP Messenger result was not committed")
	}
	return nil
}

func decodeStrictJSON(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > envelope.MaxContentBytes {
		return errors.New("protocol JSON is outside its bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("protocol JSON has trailing content")
	}
	return nil
}

var _ A2AExecutor = (*a2aadapter.Adapter)(nil)
var _ MCPExecutor = (*mcpadapter.Adapter)(nil)
