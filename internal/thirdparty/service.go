// Package thirdparty implements tos.edge.v1.ThirdPartyExecutionService: the
// private, operator-allowlisted boundary a tos-ai worker exposes for
// dispatching to a third-party HTTP/MCP/A2A provider endpoint instead of
// this worker's own model-serving runtime (pkg/runtime.Adapter).
//
// See atos-spec docs/THIRD_PARTY_EXECUTION_PLANE.md for the
// cross-repository design. The load-bearing invariant every RPC here
// enforces: a caller names a binding, it never grants one -- every request
// is checked against operatorconfig.ThirdPartyBindings before any outbound
// dial, and a binding not on that allowlist is rejected before any network
// call is made, exactly the "task payloads cannot select the endpoint"
// invariant this codebase's other adapters (see pkg/adapters/openai's own
// doc comment) already enforce.
package thirdparty

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

func transportName(t edgev1.ThirdPartyTransport) (string, error) {
	switch t {
	case edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_HTTP:
		return "http", nil
	case edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_MCP:
		return "mcp", nil
	case edgev1.ThirdPartyTransport_THIRD_PARTY_TRANSPORT_A2A:
		return "a2a", nil
	default:
		return "", errors.New("unspecified transport")
	}
}

// dialer is the narrow per-transport contract each of http.go/mcp.go/
// a2a.go's *Binding types satisfies.
type dialer interface {
	invoke(ctx context.Context, req *edgev1.ThirdPartyInvokeRequest) (*edgev1.ThirdPartyInvokeResponse, error)
	query(ctx context.Context, requestID string) (*edgev1.ThirdPartyQueryResponse, error)
	cancel(ctx context.Context, requestID, reason string) (*edgev1.ThirdPartyCancelResponse, error)
	health(ctx context.Context) *edgev1.ThirdPartyHealthResponse
	close()
}

// Service implements edgev1connect.ThirdPartyExecutionServiceHandler. Every
// dialer is built once at construction time from the operator's approved
// allowlist -- never per-request -- exactly like every other adapter in
// this codebase builds its transport once from operator config.
type Service struct {
	bindings operatorconfig.ThirdPartyBindings
	dialers  map[bindingKey]dialer
}

type bindingKey struct {
	transport, endpointRef, capabilityID, capabilityVersion string
}

// NewService builds bound dialers for every approved binding. A binding
// whose transport-specific construction fails (e.g. an invalid endpoint_ref
// URL) fails the whole call -- an operator's allowlist is not something
// this worker silently starts with entries missing from.
func NewService(bindings operatorconfig.ThirdPartyBindings) (*Service, error) {
	dialers := make(map[bindingKey]dialer, bindings.Len())
	for _, entry := range bindings.Entries() {
		var d dialer
		var err error
		switch entry.Transport {
		case "http":
			d, err = newHTTPBinding(entry)
		case "mcp":
			d, err = newMCPBinding(entry)
		case "a2a":
			d, err = newA2ABinding(entry)
		default:
			err = fmt.Errorf("unsupported transport %q", entry.Transport)
		}
		if err != nil {
			closeAll(dialers)
			return nil, fmt.Errorf("thirdparty: build binding (transport=%s endpoint_ref=%s capability_id=%s): %w",
				entry.Transport, entry.EndpointRef, entry.CapabilityID, err)
		}
		dialers[bindingKey{entry.Transport, entry.EndpointRef, entry.CapabilityID, entry.CapabilityVersion}] = d
	}
	return &Service{bindings: bindings, dialers: dialers}, nil
}

func closeAll(dialers map[bindingKey]dialer) {
	for _, d := range dialers {
		d.close()
	}
}

// Close releases every bound transport's idle connections.
func (s *Service) Close() {
	closeAll(s.dialers)
}

// resolve is the sole authorization gate: it looks up the allowlist entry
// matching ref, and only then the already-built dialer for it. A ref that
// matches no approved entry is rejected here, before touching the network.
func (s *Service) resolve(ref *edgev1.ThirdPartyBindingRef) (dialer, error) {
	if ref == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: binding is required"))
	}
	transport, err := transportName(ref.Transport)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("thirdparty: %w", err))
	}
	if _, allowed := s.bindings.Allowed(transport, ref.EndpointRef, ref.CapabilityId, ref.CapabilityVersion); !allowed {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("thirdparty: binding is not on this worker's operator-approved allowlist"))
	}
	d, ok := s.dialers[bindingKey{transport, ref.EndpointRef, ref.CapabilityId, ref.CapabilityVersion}]
	if !ok {
		// Allowed() matched but no dialer was built for it -- cannot
		// happen given NewService builds one dialer per allowlist entry,
		// but fail closed rather than panic on a future refactor bug.
		return nil, connect.NewError(connect.CodeInternal, errors.New("thirdparty: no transport built for an approved binding"))
	}
	return d, nil
}

func (s *Service) Health(
	ctx context.Context, req *connect.Request[edgev1.ThirdPartyHealthRequest],
) (*connect.Response[edgev1.ThirdPartyHealthResponse], error) {
	if req == nil || req.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request is required"))
	}
	d, err := s.resolve(req.Msg.Binding)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(d.health(ctx)), nil
}

func (s *Service) Invoke(
	ctx context.Context, req *connect.Request[edgev1.ThirdPartyInvokeRequest],
) (*connect.Response[edgev1.ThirdPartyInvokeResponse], error) {
	if req == nil || req.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request is required"))
	}
	if req.Msg.RequestId == "" || req.Msg.JobId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request_id and job_id are required"))
	}
	d, err := s.resolve(req.Msg.Binding)
	if err != nil {
		return nil, err
	}
	resp, err := d.invoke(ctx, req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) Query(
	ctx context.Context, req *connect.Request[edgev1.ThirdPartyQueryRequest],
) (*connect.Response[edgev1.ThirdPartyQueryResponse], error) {
	if req == nil || req.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request is required"))
	}
	if req.Msg.RequestId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request_id is required"))
	}
	d, err := s.resolve(req.Msg.Binding)
	if err != nil {
		return nil, err
	}
	resp, err := d.query(ctx, req.Msg.RequestId)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) Cancel(
	ctx context.Context, req *connect.Request[edgev1.ThirdPartyCancelRequest],
) (*connect.Response[edgev1.ThirdPartyCancelResponse], error) {
	if req == nil || req.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request is required"))
	}
	if req.Msg.RequestId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("thirdparty: request_id is required"))
	}
	d, err := s.resolve(req.Msg.Binding)
	if err != nil {
		return nil, err
	}
	resp, err := d.cancel(ctx, req.Msg.RequestId, req.Msg.Reason)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnimplemented, err)
	}
	return connect.NewResponse(resp), nil
}
