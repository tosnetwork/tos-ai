package messengereventbridge

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
	"github.com/tosnetwork/tos-messenger/pkg/dispatch"
	"github.com/tosnetwork/tos-messenger/pkg/envelope"
	"github.com/tosnetwork/tos-messenger/pkg/eventlog"
	"github.com/tosnetwork/tos-messenger/pkg/firewall"
	"github.com/tosnetwork/tos-messenger/pkg/localapi"
)

type localCallerFake struct {
	requests []localapi.Request
	fail     bool
	emptyID  bool
}

func (c *localCallerFake) Call(_ context.Context, request localapi.Request) (localapi.Response, error) {
	if err := localapi.ValidateRequest(request); err != nil {
		return localapi.Response{}, err
	}
	c.requests = append(c.requests, request)
	if c.fail {
		return localapi.Response{}, errors.New("daemon unavailable")
	}
	if c.emptyID {
		return localapi.Response{OK: true}, nil
	}
	return localapi.Response{OK: true, EventID: "evt_" + strings.Repeat("8", 64)}, nil
}

func TestResultPublisherQueuesExactDaemonOwnedResultThenCompletesOutbox(t *testing.T) {
	outbox, err := OpenResultOutbox(filepath.Join(t.TempDir(), "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	event := decodedEvent(t, "a2a.message")
	if err := outbox.ReceiveA2AResult(context.Background(), event, &a2a.Task{ID: "task", ContextID: "context"}); err != nil {
		t.Fatal(err)
	}
	client := &localCallerFake{}
	publisher := newTestPublisher(t, outbox, client)
	summary, err := publisher.PublishPending(context.Background())
	if err != nil || summary.Queued != 1 || summary.Retained != 0 || len(client.requests) != 1 {
		t.Fatalf("publish failed: summary=%+v calls=%d err=%v", summary, len(client.requests), err)
	}
	request := client.requests[0]
	if request.Op != localapi.OpComposeProtocolResult || request.ProtocolKind != "a2a.message" || request.Protocol != "a2a" ||
		request.ReplyToEventID != event.EventID || request.ConversationID != event.ConversationID ||
		request.ExpiresAtUnix != event.CreatedAtUnix+3600 || !strings.HasPrefix(request.IdempotencyKey, "idem_") {
		t.Fatalf("unexpected local API composition: %+v", request)
	}
	if pending, err := outbox.Pending(); err != nil || len(pending) != 0 {
		t.Fatalf("queued result remained pending: %+v err=%v", pending, err)
	}
	if summary, err := publisher.PublishPending(context.Background()); err != nil || summary != (PublishSummary{}) || len(client.requests) != 1 {
		t.Fatalf("completed result was published twice: summary=%+v calls=%d err=%v", summary, len(client.requests), err)
	}
}

func TestResultPublisherRetainsFailuresWithStableIntent(t *testing.T) {
	outbox, err := OpenResultOutbox(filepath.Join(t.TempDir(), "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	event := decodedEvent(t, "mcp.call")
	if err := outbox.ReceiveMCPResult(context.Background(), event, testMCPOutput()); err != nil {
		t.Fatal(err)
	}
	client := &localCallerFake{fail: true}
	publisher := newTestPublisher(t, outbox, client)
	summary, err := publisher.PublishPending(context.Background())
	if err == nil || summary.Retained != 1 || summary.Queued != 0 || len(client.requests) != 1 {
		t.Fatalf("daemon failure was not retained: summary=%+v calls=%d err=%v", summary, len(client.requests), err)
	}
	first := client.requests[0]
	client.fail = false
	summary, err = publisher.PublishPending(context.Background())
	if err != nil || summary.Queued != 1 || len(client.requests) != 2 {
		t.Fatalf("retained result did not retry: summary=%+v calls=%d err=%v", summary, len(client.requests), err)
	}
	second := client.requests[1]
	if first.IdempotencyKey != second.IdempotencyKey || first.ExpiresAtUnix != second.ExpiresAtUnix ||
		string(first.ProtocolBody) != string(second.ProtocolBody) || second.ProtocolKind != "mcp.result" || second.Protocol != "mcp" {
		t.Fatalf("retry intent changed: first=%+v second=%+v", first, second)
	}
}

func TestResultPublisherRefusesMissingRoutesAndInvalidConfiguration(t *testing.T) {
	outbox, err := OpenResultOutbox(filepath.Join(t.TempDir(), "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	event := decodedEvent(t, "a2a.message")
	if err := outbox.ReceiveA2AResult(context.Background(), event, &a2a.Task{ID: "task"}); err != nil {
		t.Fatal(err)
	}
	client := &localCallerFake{}
	publisher, err := NewResultPublisher(PublisherConfig{Outbox: outbox, Client: client, Lifetime: time.Hour,
		Routes: []ResultRoute{{SenderAgentID: "agent_" + strings.Repeat("9", 64), SessionID: testSession, RecipientEndpointID: testEndpoint}}})
	if err != nil {
		t.Fatal(err)
	}
	if summary, err := publisher.PublishPending(context.Background()); err == nil || summary.Retained != 1 || len(client.requests) != 0 {
		t.Fatalf("missing sender route was hidden: summary=%+v calls=%d err=%v", summary, len(client.requests), err)
	}
	if _, err := NewResultPublisher(PublisherConfig{Outbox: outbox, Client: client, Lifetime: time.Second,
		Routes: []ResultRoute{{SenderAgentID: testSender, SessionID: testSession, RecipientEndpointID: testEndpoint}}}); err == nil {
		t.Fatal("unsafe result lifetime was accepted")
	}
}

func TestResultPublisherQueuesThroughRealMessengerLocalAPIAndRestart(t *testing.T) {
	root := t.TempDir()
	journal, err := eventlog.Open(filepath.Join(root, "messenger-state"))
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(1_900_000_000, 0)
	dispatcher, err := dispatch.New(dispatch.Config{Journal: journal, Now: func() time.Time { return clock },
		Identity: dispatch.Identity{AgentID: "agent_" + strings.Repeat("7", 64), EndpointID: "mep_" + strings.Repeat("8", 64), DeviceID: "dev_" + strings.Repeat("9", 64)},
		Network:  testNetwork(), AllowedEventClasses: []string{"a2a", "mcp"}})
	if err != nil {
		t.Fatal(err)
	}
	owner := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	server, err := localapi.NewServer(localapi.Config{Journal: journal, Dispatcher: dispatcher, Policy: firewall.Default(),
		OwnerKey: owner, LocalEndpointID: "mep_" + strings.Repeat("8", 64), Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "run", "runtime.sock")
	listener, err := localapi.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener, localapi.PrincipalRuntime) }()
	client, err := localapi.NewClient(socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := OpenResultOutbox(filepath.Join(root, "result-outbox"))
	if err != nil {
		t.Fatal(err)
	}
	source := decodedEvent(t, "mcp.call")
	if err := outbox.ReceiveMCPResult(context.Background(), source, testMCPOutput()); err != nil {
		t.Fatal(err)
	}
	publisher := newTestPublisher(t, outbox, client)
	if summary, err := publisher.PublishPending(context.Background()); err != nil || summary.Queued != 1 {
		t.Fatalf("real Messenger queue failed: summary=%+v err=%v", summary, err)
	}
	due, err := journal.Due(clock)
	if err != nil || len(due) != 1 {
		t.Fatalf("result was not durable in Messenger: %+v err=%v", due, err)
	}
	raw, err := due[0].Payload()
	if err != nil {
		t.Fatal(err)
	}
	resultEvent, err := envelope.DecodeEventJSON(raw)
	if err != nil || resultEvent.Kind != "mcp.result" || resultEvent.ReplyToEventID != source.EventID ||
		resultEvent.SenderAgentID != dispatcher.LocalIdentity().AgentID {
		t.Fatalf("unexpected queued result Event: %+v err=%v", resultEvent, err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = listener.Close()
	<-done
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedOutbox, err := OpenResultOutbox(filepath.Join(root, "result-outbox"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedOutbox.Close()
	if pending, err := reopenedOutbox.Pending(); err != nil || len(pending) != 0 {
		t.Fatalf("published result returned after restart: %+v err=%v", pending, err)
	}
}

const (
	testSession  = "ses_5555555555555555555555555555555555555555555555555555555555555555"
	testEndpoint = "mep_6666666666666666666666666666666666666666666666666666666666666666"
)

func newTestPublisher(t *testing.T, outbox *ResultOutbox, client LocalAPICaller) *ResultPublisher {
	t.Helper()
	publisher, err := NewResultPublisher(PublisherConfig{Outbox: outbox, Client: client, Lifetime: time.Hour,
		Routes: []ResultRoute{{SenderAgentID: testSender, SessionID: testSession, RecipientEndpointID: testEndpoint}}})
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func testMCPOutput() mcpadapter.Output {
	return mcpadapter.Output{Protocol: "tos_service_v1", ExecutionID: "sha256:" + strings.Repeat("1", 64)}
}
