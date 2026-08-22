package messengereventbridge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
)

func TestResultOutboxIsIdempotentDurableAndRestartSafe(t *testing.T) {
	root := filepath.Join(t.TempDir(), "results")
	outbox, err := OpenResultOutbox(root)
	if err != nil {
		t.Fatal(err)
	}
	event := decodedEvent(t, "a2a.message")
	task := &a2a.Task{ID: "task", ContextID: "context"}
	if err := outbox.ReceiveA2AResult(context.Background(), event, task); err != nil {
		t.Fatal(err)
	}
	if err := outbox.ReceiveA2AResult(context.Background(), event, task); err != nil {
		t.Fatalf("exact result retry failed: %v", err)
	}
	if err := outbox.ReceiveA2AResult(context.Background(), event, &a2a.Task{ID: "substituted", ContextID: "context"}); err == nil {
		t.Fatal("result substitution under one source Event ID was accepted")
	}
	pending, err := outbox.Pending()
	if err != nil || len(pending) != 1 || pending[0].SourceEventID != event.EventID ||
		pending[0].ConversationID != event.ConversationID || pending[0].SenderAgentID != event.SenderAgentID ||
		pending[0].ResultKind != "a2a.message" {
		t.Fatalf("unexpected pending result: %+v err=%v", pending, err)
	}
	digest := pending[0].ResultDigest
	pending[0].ResultJSON[0] ^= 0xff
	again, err := outbox.Pending()
	if err != nil || len(again) != 1 || again[0].ResultDigest != digest || again[0].ResultJSON[0] == pending[0].ResultJSON[0] {
		t.Fatal("caller mutated durable result through returned slice")
	}
	if err := outbox.Complete(event.EventID, "sha256:"+strings.Repeat("9", 64)); err == nil {
		t.Fatal("wrong result digest completed the outbox entry")
	}
	if err := outbox.Complete(event.EventID, digest); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Complete(event.EventID, digest); err != nil {
		t.Fatalf("exact completion retry failed: %v", err)
	}
	if pending, err := outbox.Pending(); err != nil || len(pending) != 0 {
		t.Fatalf("completed result remained pending: %+v err=%v", pending, err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Pending(); err == nil {
		t.Fatal("closed outbox remained usable without ownership")
	}
	reopened, err := OpenResultOutbox(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if pending, err := reopened.Pending(); err != nil || len(pending) != 0 {
		t.Fatalf("completed result returned after restart: %+v err=%v", pending, err)
	}
	if err := reopened.ReceiveA2AResult(context.Background(), event, task); err != nil {
		t.Fatalf("completed exact retry was not idempotent: %v", err)
	}
}

func TestResultOutboxSeparatesMCPResultAndSourceIdentity(t *testing.T) {
	outbox, err := OpenResultOutbox(filepath.Join(t.TempDir(), "results"))
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	event := decodedEvent(t, "mcp.call")
	output := mcpadapter.Output{Protocol: "tos_service_v1", ExecutionID: "sha256:" + strings.Repeat("1", 64)}
	if err := outbox.ReceiveMCPResult(context.Background(), event, output); err != nil {
		t.Fatal(err)
	}
	if err := outbox.ReceiveA2AResult(context.Background(), event, &a2a.Task{ID: "cross-profile"}); err == nil {
		t.Fatal("source Event ID was reused across result profiles")
	}
	pending, err := outbox.Pending()
	if err != nil || len(pending) != 1 || pending[0].ResultKind != "mcp.result" {
		t.Fatalf("unexpected MCP pending result: %+v err=%v", pending, err)
	}
}

func TestMCPResultJournalCommitsInboundResultWithoutPublishing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "inbound-mcp-results")
	journal, err := OpenMCPResultJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	event := decodedEvent(t, "mcp.result")
	output := mcpadapter.Output{Protocol: "tos_service_v1", ExecutionID: "sha256:" + strings.Repeat("1", 64)}
	if err := journal.ReceiveMCPResult(context.Background(), event, output); err != nil {
		t.Fatal(err)
	}
	if err := journal.ReceiveMCPResult(context.Background(), event, output); err != nil {
		t.Fatalf("exact inbound result retry failed: %v", err)
	}
	substituted := output
	substituted.ExecutionID = "sha256:" + strings.Repeat("2", 64)
	if err := journal.ReceiveMCPResult(context.Background(), event, substituted); err == nil {
		t.Fatal("inbound MCP result substitution was accepted")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	inspect, err := OpenResultOutbox(root)
	if err != nil {
		t.Fatal(err)
	}
	if pending, err := inspect.Pending(); err != nil || len(pending) != 0 {
		t.Fatalf("inbound result leaked into publishable pending set: %+v err=%v", pending, err)
	}
	if err := inspect.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMCPResultJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ReceiveMCPResult(context.Background(), event, output); err != nil {
		t.Fatalf("inbound result did not survive restart: %v", err)
	}
}

func TestResultOutboxRefusesConcurrentOwnerAndCorruptRecord(t *testing.T) {
	root := filepath.Join(t.TempDir(), "results")
	outbox, err := OpenResultOutbox(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenResultOutbox(root); err == nil {
		t.Fatal("second result outbox owner was accepted")
	}
	event := decodedEvent(t, "mcp.result")
	if err := outbox.ReceiveMCPResult(context.Background(), event, mcpadapter.Output{Protocol: "tos_service_v1"}); err != nil {
		t.Fatal(err)
	}
	path := outbox.path(event.EventID)
	if err := os.WriteFile(path, []byte(`{"schema":"substituted"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Pending(); err == nil {
		t.Fatal("corrupt durable result was skipped")
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestResultOutboxRequiresPrivateCleanRoot(t *testing.T) {
	if _, err := OpenResultOutbox("relative/results"); err == nil {
		t.Fatal("relative outbox root was accepted")
	}
	root := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenResultOutbox(root); err == nil {
		t.Fatal("public outbox root was accepted")
	}
}
