package messengereventbridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"time"

	"github.com/tosnetwork/tos-messenger/pkg/localapi"
)

var (
	agentPattern    = regexp.MustCompile(`^agent_[0-9a-f]{64}$`)
	endpointPattern = regexp.MustCompile(`^mep_[0-9a-f]{64}$`)
	sessionPattern  = regexp.MustCompile(`^ses_[0-9a-f]{64}$`)
)

type LocalAPICaller interface {
	Call(context.Context, localapi.Request) (localapi.Response, error)
}

type ResultRoute struct {
	SenderAgentID       string `json:"sender_agent_id"`
	SessionID           string `json:"session_id"`
	RecipientEndpointID string `json:"recipient_endpoint_id"`
}

type PublisherConfig struct {
	Outbox   *ResultOutbox
	Client   LocalAPICaller
	Routes   []ResultRoute
	Lifetime time.Duration
}

type ResultPublisher struct {
	outbox   *ResultOutbox
	client   LocalAPICaller
	routes   map[string]ResultRoute
	lifetime time.Duration
}

type PublishSummary struct {
	Queued, Retained int
}

func NewResultPublisher(config PublisherConfig) (*ResultPublisher, error) {
	if config.Outbox == nil || config.Client == nil || config.Lifetime < time.Minute || config.Lifetime > 7*24*time.Hour || len(config.Routes) == 0 {
		return nil, errors.New("invalid Messenger result publisher configuration")
	}
	routes := make(map[string]ResultRoute, len(config.Routes))
	previous := ""
	for _, route := range config.Routes {
		if !agentPattern.MatchString(route.SenderAgentID) || !sessionPattern.MatchString(route.SessionID) ||
			!endpointPattern.MatchString(route.RecipientEndpointID) || route.SenderAgentID <= previous {
			return nil, errors.New("invalid or unsorted Messenger result route")
		}
		previous = route.SenderAgentID
		routes[route.SenderAgentID] = route
	}
	return &ResultPublisher{outbox: config.Outbox, client: config.Client, routes: routes, lifetime: config.Lifetime}, nil
}

func (p *ResultPublisher) PublishPending(ctx context.Context) (PublishSummary, error) {
	if p == nil || p.outbox == nil || p.client == nil || ctx == nil {
		return PublishSummary{}, errors.New("invalid Messenger result publisher")
	}
	pending, err := p.outbox.Pending()
	if err != nil {
		return PublishSummary{}, err
	}
	summary := PublishSummary{}
	errorsSeen := make([]error, 0)
	for _, result := range pending {
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}
		route, ok := p.routes[result.SenderAgentID]
		if !ok {
			summary.Retained++
			errorsSeen = append(errorsSeen, errors.New("no Messenger result route for sender"))
			continue
		}
		protocol := "mcp"
		if result.ResultKind == "a2a.message" {
			protocol = "a2a"
		}
		expires := result.SourceCreatedAtUnix + uint64(p.lifetime/time.Second)
		response, callErr := p.client.Call(ctx, localapi.Request{Op: localapi.OpComposeProtocolResult,
			ConversationID: result.ConversationID, ReplyToEventID: result.SourceEventID,
			ProtocolKind: result.ResultKind, Protocol: protocol, ProtocolVersion: "1", ProtocolBody: result.ResultJSON,
			IdempotencyKey: resultIdempotency(result.SourceEventID, result.ResultDigest),
			SessionID:      route.SessionID, RecipientEndpointID: route.RecipientEndpointID, ExpiresAtUnix: expires})
		if callErr != nil || response.EventID == "" {
			summary.Retained++
			if callErr == nil {
				callErr = errors.New("Messenger did not return a queued result Event ID")
			}
			errorsSeen = append(errorsSeen, callErr)
			continue
		}
		if err := p.outbox.Complete(result.SourceEventID, result.ResultDigest); err != nil {
			summary.Retained++
			errorsSeen = append(errorsSeen, err)
			continue
		}
		summary.Queued++
	}
	return summary, errors.Join(errorsSeen...)
}

func resultIdempotency(sourceEventID, digest string) string {
	hash := sha256.Sum256([]byte("tos.service.messenger-result-publish.v1\x00" + sourceEventID + "\x00" + digest))
	return "idem_" + hex.EncodeToString(hash[:])
}
