package a2aadapter

import (
	"context"
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestOfficialRequestHandlerBindsOnlySynchronousSendMessage(t *testing.T) {
	adapter, err := New(&authorizerFake{}, &runnerFake{}, locatorFake{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRequestHandler(adapter)
	if err != nil {
		t.Fatal(err)
	}
	result, err := handler.SendMessage(context.Background(), taskRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if task, ok := result.(*a2a.Task); !ok || task.Status.State != a2a.TaskStateCompleted {
		t.Fatal("official A2A handler lost completed task")
	}
	if _, err := handler.GetTask(context.Background(), &a2a.GetTaskRequest{}); !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Fatal("narrow A2A profile unexpectedly exposed task storage")
	}
	if _, err := NewJSONRPCHandler(adapter); err != nil {
		t.Fatal(err)
	}
}
