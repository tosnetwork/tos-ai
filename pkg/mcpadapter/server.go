package mcpadapter

import (
	"errors"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewStreamableHTTPHandler binds the tool to the official MCP streamable HTTP
// transport. Authentication, TLS, and public-listener policy remain the
// embedding provider's responsibility.
func NewStreamableHTTPHandler(adapter *Adapter) (http.Handler, error) {
	if adapter == nil {
		return nil, errors.New("missing MCP adapter")
	}
	server := mcp.NewServer(&mcp.Implementation{
		Name: "atos-native-provider", Title: "ATOS Native provider", Version: "1.0.0",
	}, nil)
	if err := adapter.AddTo(server); err != nil {
		return nil, err
	}
	options := &mcp.StreamableHTTPOptions{
		Stateless: true, JSONResponse: true, PropagateRequestCancellation: true,
	}
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, options), nil
}
