package a2aadapter

import (
	"context"
	"errors"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// NewRequestHandler binds the deliberately narrow synchronous ATOS profile to
// the official transport-neutral A2A server boundary.
func NewRequestHandler(adapter *Adapter) (a2asrv.RequestHandler, error) {
	if adapter == nil {
		return nil, errors.New("missing A2A adapter")
	}
	return &requestHandler{adapter: adapter}, nil
}

// NewJSONRPCHandler exposes the narrow profile through the official A2A
// JSON-RPC transport. Authentication and public-listener policy remain the
// embedding provider's responsibility.
func NewJSONRPCHandler(adapter *Adapter, options ...a2asrv.TransportOption) (http.Handler, error) {
	handler, err := NewRequestHandler(adapter)
	if err != nil {
		return nil, err
	}
	return a2asrv.NewJSONRPCHandler(handler, options...), nil
}

type requestHandler struct{ adapter *Adapter }

func (h *requestHandler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	task, err := h.adapter.Execute(ctx, req)
	if err != nil {
		return nil, errors.Join(a2a.ErrInvalidParams, err)
	}
	return task, nil
}

func (*requestHandler) GetTask(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (*requestHandler) ListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (*requestHandler) CancelTask(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (*requestHandler) SubscribeToTask(context.Context, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedEvents()
}

func (*requestHandler) SendStreamingMessage(context.Context, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedEvents()
}

func (*requestHandler) GetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) ListTaskPushConfigs(context.Context, *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) DeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigRequest) error {
	return a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) GetExtendedAgentCard(context.Context, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return nil, a2a.ErrExtendedCardNotConfigured
}

func unsupportedEvents() iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(nil, a2a.ErrUnsupportedOperation)
	}
}
