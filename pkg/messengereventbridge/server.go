// Package messengereventbridge independently verifies Messenger Event v2
// inputs before handing typed A2A or MCP bodies to execution-facing code.
package messengereventbridge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"

	"github.com/tosnetwork/tos-messenger/pkg/envelope"
	"github.com/tosnetwork/tos-messenger/pkg/payload"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
)

const (
	A2APath          = "/v1/a2a-event"
	MCPPath          = "/v1/mcp-event"
	EventContentType = "application/vnd.tos.messaging.event.v2+json"
	MaxRequestBytes  = envelope.MaxContentBytes + 32<<10
)

type A2AHandler interface {
	HandleA2A(context.Context, envelope.Event, payload.A2AMessage) error
}

type MCPHandler interface {
	HandleMCPCall(context.Context, envelope.Event, payload.MCPCall) error
	HandleMCPResult(context.Context, envelope.Event, payload.MCPResult) error
}

type Authorizer interface {
	AuthorizeMessengerEvent(context.Context, envelope.Event) error
}

type Config struct {
	Authorizer Authorizer
	A2A        A2AHandler
	MCP        MCPHandler
}

type Server struct {
	authorizer Authorizer
	a2a        A2AHandler
	mcp        MCPHandler
}

func New(config Config) (*Server, error) {
	if config.Authorizer == nil || (config.A2A == nil && config.MCP == nil) {
		return nil, errors.New("invalid Messenger protocol event consumer configuration")
	}
	return &Server{authorizer: config.Authorizer, a2a: config.A2A, mcp: config.MCP}, nil
}

// handler combines both paths for request-level tests. Production exposure is
// profile-separated by UnixService and is intentionally not exported.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(A2APath, s.receiveA2A)
	mux.HandleFunc(MCPPath, s.receiveMCP)
	return mux
}

func (s *Server) a2aHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(A2APath, s.receiveA2A)
	return mux
}

func (s *Server) mcpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(MCPPath, s.receiveMCP)
	return mux
}

func (s *Server) receiveA2A(response http.ResponseWriter, request *http.Request) {
	if s == nil || s.a2a == nil {
		http.Error(response, "A2A consumer unavailable", http.StatusServiceUnavailable)
		return
	}
	event, body, ok := s.decode(response, request, "a2a.message")
	if !ok {
		return
	}
	typed, ok := body.(payload.A2AMessage)
	if !ok {
		http.Error(response, "invalid A2A event", http.StatusUnprocessableEntity)
		return
	}
	if err := s.a2a.HandleA2A(request.Context(), event, typed); err != nil {
		http.Error(response, "A2A consumer failed", http.StatusServiceUnavailable)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

func (s *Server) receiveMCP(response http.ResponseWriter, request *http.Request) {
	if s == nil || s.mcp == nil {
		http.Error(response, "MCP consumer unavailable", http.StatusServiceUnavailable)
		return
	}
	event, body, ok := s.decode(response, request, "mcp.call", "mcp.result")
	if !ok {
		return
	}
	var err error
	switch typed := body.(type) {
	case payload.MCPCall:
		err = s.mcp.HandleMCPCall(request.Context(), event, typed)
	case payload.MCPResult:
		err = s.mcp.HandleMCPResult(request.Context(), event, typed)
	default:
		http.Error(response, "invalid MCP event", http.StatusUnprocessableEntity)
		return
	}
	if err != nil {
		http.Error(response, "MCP consumer failed", http.StatusServiceUnavailable)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

func (s *Server) decode(response http.ResponseWriter, request *http.Request, kinds ...string) (envelope.Event, payload.Payload, bool) {
	if request.Method != http.MethodPost || request.Header.Get("Content-Type") != EventContentType {
		http.Error(response, "invalid Messenger event request", http.StatusUnsupportedMediaType)
		return envelope.Event{}, nil, false
	}
	reader := http.MaxBytesReader(response, request.Body, MaxRequestBytes)
	raw, err := io.ReadAll(reader)
	if err != nil || len(raw) == 0 {
		http.Error(response, "invalid Messenger event body", http.StatusRequestEntityTooLarge)
		return envelope.Event{}, nil, false
	}
	event, err := envelope.DecodeEventJSON(raw)
	if err != nil || !contains(kinds, event.Kind) {
		http.Error(response, "invalid Messenger event", http.StatusUnprocessableEntity)
		return envelope.Event{}, nil, false
	}
	canonical, err := envelope.EncodeEventJSON(event)
	if err != nil || !bytes.Equal(raw, canonical) {
		http.Error(response, "non-canonical Messenger event", http.StatusUnprocessableEntity)
		return envelope.Event{}, nil, false
	}
	body, err := payload.Decode(event.Kind, event.Content)
	if err != nil {
		http.Error(response, "invalid Messenger protocol payload", http.StatusUnprocessableEntity)
		return envelope.Event{}, nil, false
	}
	if err := s.authorizer.AuthorizeMessengerEvent(request.Context(), event); err != nil {
		http.Error(response, "Messenger event unauthorized", http.StatusForbidden)
		return envelope.Event{}, nil, false
	}
	return event, body, true
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// StaticPolicy is a small fail-closed deployment policy. More dynamic
// finalized policy resolvers can implement Authorizer directly.
type StaticPolicy struct {
	networkID     string
	genesisRoot   string
	genesisFile   string
	senders       []string
	conversations []string
}

func NewStaticPolicy(network *nativev1.NetworkDomain, senders, conversations []string) (*StaticPolicy, error) {
	if network == nil || network.NetworkId == "" || network.GenesisRootHash == "" || network.GenesisFileHash == "" ||
		!strictSet(senders) || !strictSet(conversations) {
		return nil, errors.New("invalid Messenger event static policy")
	}
	return &StaticPolicy{networkID: network.NetworkId, genesisRoot: network.GenesisRootHash,
		genesisFile: network.GenesisFileHash, senders: append([]string(nil), senders...),
		conversations: append([]string(nil), conversations...)}, nil
}

func (p *StaticPolicy) AuthorizeMessengerEvent(_ context.Context, event envelope.Event) error {
	if p == nil || event.Network == nil || event.Network.NetworkId != p.networkID ||
		event.Network.GenesisRootHash != p.genesisRoot || event.Network.GenesisFileHash != p.genesisFile ||
		!sortedContains(p.senders, event.SenderAgentID) || !sortedContains(p.conversations, event.ConversationID) {
		return errors.New("Messenger Event is outside the static policy")
	}
	return nil
}

func strictSet(values []string) bool {
	if len(values) == 0 {
		return false
	}
	for index, value := range values {
		if value == "" || (index > 0 && values[index-1] >= value) {
			return false
		}
	}
	return sort.StringsAreSorted(values)
}

func sortedContains(values []string, value string) bool {
	index := sort.SearchStrings(values, value)
	return index < len(values) && values[index] == value
}
