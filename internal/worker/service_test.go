package worker

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	taskScheduler, err := scheduler.New(scheduler.Config{Workers: 1, MaxQueue: 4})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(Config{
		Version:        "test",
		QuoteTTL:       time.Minute,
		MaxQuotes:      4,
		MaxInvocations: 4,
		MaxDeadline:    time.Hour,
		Now:            time.Now,
	}, taskScheduler, []airuntime.Adapter{mock.New(0)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taskScheduler.Shutdown(context.Background()) })
	return service
}

func TestQuoteInvokeAndReplay(t *testing.T) {
	service := newTestService(t)
	deadline := time.Now().Add(time.Minute).UnixMilli()
	quote, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          "request-0001",
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         5,
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		t.Fatal(err)
	}
	invocation := &edgev1.InvokeRequest{
		RequestId:          "request-0001",
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            []byte("hello"),
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
	first, err := service.Invoke(context.Background(), connect.NewRequest(invocation))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Invoke(context.Background(), connect.NewRequest(invocation))
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Msg.Output) != "hello" || string(second.Msg.Output) != "hello" {
		t.Fatal("unexpected output")
	}
	first.Msg.Output[0] = 'X'
	if string(second.Msg.Output) != "hello" {
		t.Fatal("replay response was aliased")
	}
	service.config.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	third, err := service.Invoke(context.Background(), connect.NewRequest(invocation))
	if err != nil || string(third.Msg.Output) != "hello" {
		t.Fatalf("completed replay after quote expiry failed: response=%v err=%v", third, err)
	}
}

func TestQuoteRejectsRealtimePriorityForExternalAdapter(t *testing.T) {
	service := newTestService(t)
	_, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          "request-0002",
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         1,
		MaxOutputBytes:     1,
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
		Priority:           edgev1.Priority_PRIORITY_REALTIME_PERCEPTION,
	}))
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("realtime priority error = %v", err)
	}
}

func TestInvokeRejectsRequestIDReuseWithDifferentPayload(t *testing.T) {
	service := newTestService(t)
	deadline := time.Now().Add(time.Minute).UnixMilli()
	quote, err := service.Quote(context.Background(), connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          "request-0003",
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         8,
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		t.Fatal(err)
	}
	invocation := &edgev1.InvokeRequest{
		RequestId:          "request-0003",
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            []byte("first"),
		MaxOutputBytes:     16,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}
	if _, err := service.Invoke(context.Background(), connect.NewRequest(invocation)); err != nil {
		t.Fatal(err)
	}
	invocation.Payload = []byte("second")
	if _, err := service.Invoke(context.Background(), connect.NewRequest(invocation)); err == nil ||
		connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("request ID content reuse error = %v", err)
	}
}
