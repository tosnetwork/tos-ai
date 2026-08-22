package messengereventbridge

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/tosnetwork/tos-messenger/pkg/envelope"
	"github.com/tosnetwork/tos-messenger/pkg/payload"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
)

type a2aFake struct {
	events []envelope.Event
	fail   bool
}

func (h *a2aFake) HandleA2A(_ context.Context, event envelope.Event, body payload.A2AMessage) error {
	if body.Protocol != "a2a" {
		return errors.New("wrong A2A profile")
	}
	h.events = append(h.events, event)
	if h.fail {
		return errors.New("unavailable")
	}
	return nil
}

type mcpFake struct {
	calls   []envelope.Event
	results []envelope.Event
	fail    bool
}

func (h *mcpFake) HandleMCPCall(_ context.Context, event envelope.Event, body payload.MCPCall) error {
	if body.Protocol != "mcp" {
		return errors.New("wrong MCP profile")
	}
	h.calls = append(h.calls, event)
	if h.fail {
		return errors.New("unavailable")
	}
	return nil
}

func (h *mcpFake) HandleMCPResult(_ context.Context, event envelope.Event, body payload.MCPResult) error {
	if body.Protocol != "mcp" {
		return errors.New("wrong MCP profile")
	}
	h.results = append(h.results, event)
	if h.fail {
		return errors.New("unavailable")
	}
	return nil
}

func TestServerIndependentlyVerifiesAndSeparatesProtocolEvents(t *testing.T) {
	a2a, mcp := &a2aFake{}, &mcpFake{}
	server := testServer(t, a2a, mcp)
	for _, trial := range []struct {
		path string
		kind string
		want int
	}{
		{A2APath, "a2a.message", http.StatusAccepted},
		{MCPPath, "mcp.call", http.StatusAccepted},
		{MCPPath, "mcp.result", http.StatusAccepted},
		{A2APath, "mcp.call", http.StatusUnprocessableEntity},
		{MCPPath, "a2a.message", http.StatusUnprocessableEntity},
	} {
		wire := eventWire(t, trial.kind, testSender, testConversation)
		response := request(t, server.handler(), trial.path, EventContentType, wire)
		if response.Code != trial.want {
			t.Fatalf("%s at %s returned %d, want %d: %s", trial.kind, trial.path, response.Code, trial.want, response.Body.String())
		}
	}
	if len(a2a.events) != 1 || len(mcp.calls) != 1 || len(mcp.results) != 1 {
		t.Fatalf("protocol dispatch crossed profiles: a2a=%d calls=%d results=%d", len(a2a.events), len(mcp.calls), len(mcp.results))
	}
}

func TestServerRejectsUnauthorizedNonCanonicalAndFailedConsumption(t *testing.T) {
	a2a, mcp := &a2aFake{}, &mcpFake{}
	server := testServer(t, a2a, mcp)
	valid := eventWire(t, "a2a.message", testSender, testConversation)
	trials := []struct {
		name        string
		path        string
		contentType string
		body        []byte
		want        int
	}{
		{"wrong content type", A2APath, "application/json", valid, http.StatusUnsupportedMediaType},
		{"noncanonical", A2APath, EventContentType, append(append([]byte(nil), valid...), '\n'), http.StatusUnprocessableEntity},
		{"foreign sender", A2APath, EventContentType, eventWire(t, "a2a.message", "agent_"+strings.Repeat("9", 64), testConversation), http.StatusForbidden},
		{"foreign conversation", A2APath, EventContentType, eventWire(t, "a2a.message", testSender, "conv_"+strings.Repeat("9", 64)), http.StatusForbidden},
	}
	for _, trial := range trials {
		t.Run(trial.name, func(t *testing.T) {
			response := request(t, server.handler(), trial.path, trial.contentType, trial.body)
			if response.Code != trial.want {
				t.Fatalf("status=%d want=%d: %s", response.Code, trial.want, response.Body.String())
			}
		})
	}
	a2a.fail = true
	if response := request(t, server.handler(), A2APath, EventContentType, valid); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("handler failure was acknowledged: %d", response.Code)
	}
	if len(a2a.events) != 1 {
		t.Fatalf("rejected requests reached handler: %d", len(a2a.events))
	}
}

func TestStaticPolicyRequiresExplicitSortedAuthority(t *testing.T) {
	network := testNetwork()
	for _, configured := range []struct {
		network       *nativev1.NetworkDomain
		senders       []string
		conversations []string
	}{
		{nil, []string{testSender}, []string{testConversation}},
		{network, nil, []string{testConversation}},
		{network, []string{testSender}, nil},
		{network, []string{testSender, testSender}, []string{testConversation}},
		{network, []string{"z", "a"}, []string{testConversation}},
	} {
		if _, err := NewStaticPolicy(configured.network, configured.senders, configured.conversations); err == nil {
			t.Fatalf("invalid static policy accepted: %+v", configured)
		}
	}
	senders := []string{testSender}
	conversations := []string{testConversation}
	sort.Strings(senders)
	sort.Strings(conversations)
	if _, err := NewStaticPolicy(network, senders, conversations); err != nil {
		t.Fatal(err)
	}
}

const (
	testSender       = "agent_1111111111111111111111111111111111111111111111111111111111111111"
	testConversation = "conv_2222222222222222222222222222222222222222222222222222222222222222"
)

func testServer(t *testing.T, a2a A2AHandler, mcp MCPHandler) *Server {
	t.Helper()
	policy, err := NewStaticPolicy(testNetwork(), []string{testSender}, []string{testConversation})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{Authorizer: policy, A2A: a2a, MCP: mcp})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func testNetwork() *nativev1.NetworkDomain {
	return &nativev1.NetworkDomain{NetworkId: "tos-local", GenesisRootHash: strings.Repeat("a", 64), GenesisFileHash: strings.Repeat("b", 64)}
}

func eventWire(t *testing.T, kind, sender, conversation string) []byte {
	t.Helper()
	foreign := payload.Foreign{Protocol: "mcp", Version: "1", Body: []byte(`{"work":true}`)}
	var body payload.Payload
	switch kind {
	case "a2a.message":
		foreign.Protocol = "a2a"
		body = payload.A2AMessage{Foreign: foreign}
	case "mcp.call":
		body = payload.MCPCall{Foreign: foreign}
	case "mcp.result":
		body = payload.MCPResult{Foreign: foreign}
	default:
		t.Fatalf("unsupported event kind %q", kind)
	}
	content, err := payload.Encode(body)
	if err != nil {
		t.Fatal(err)
	}
	event, err := envelope.NewEvent(envelope.Event{
		Network: testNetwork(), ConversationID: conversation, SenderAgentID: sender,
		SenderEndpointID: "mep_" + strings.Repeat("3", 64), SenderDeviceID: "dev_" + strings.Repeat("4", 64),
		CreatedAtUnix: 1_900_000_000, Kind: kind, Content: content,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := envelope.EncodeEventJSON(event)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func request(t *testing.T, handler http.Handler, path, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
