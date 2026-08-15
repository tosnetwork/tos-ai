package mcpadapter

import (
	"errors"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tosnetwork/tos-ai/pkg/adapterhttp"
)

// NewStreamableHTTPHandler binds the tool to the official MCP streamable HTTP
// transport. Authentication, TLS, and public-listener policy remain the
// embedding provider's responsibility.
func NewStreamableHTTPHandler(adapter *Adapter) (http.Handler, error) {
	if adapter == nil {
		return nil, errors.New("missing MCP adapter")
	}
	server := mcp.NewServer(&mcp.Implementation{
		Name: "tos-service-provider", Title: "TOS Service Provider", Version: "1.0.0",
	}, nil)
	if err := adapter.AddTo(server); err != nil {
		return nil, err
	}
	options := &mcp.StreamableHTTPOptions{
		Stateless: true, JSONResponse: true, PropagateRequestCancellation: true,
	}
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, options), nil
}

// NewPublicServer composes the official MCP transport with the mandatory TOS Service Protocol
// TLS, authentication, request-size, and concurrency boundary.
func NewPublicServer(adapter *Adapter, config adapterhttp.ServerConfig) (*http.Server, error) {
	handler, err := NewStreamableHTTPHandler(adapter)
	if err != nil {
		return nil, err
	}
	return adapterhttp.NewServer(handler, config)
}
