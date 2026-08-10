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
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

// defaultQueryObservedRetention bounds a completion record whose first
// observation came from Query rather than Invoke (see the Query RPC's own
// comment on why -- ThirdPartyQueryRequest carries no retain_until of its
// own), mirroring the native path's DefaultWorkerMaxTaskRetention.
const defaultQueryObservedRetention = 48 * time.Hour

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

	// completions durably records the first observed completion time per
	// request_id, bounded by each record's own retain_until_unix_millis --
	// worker.proto's own doc comment requires Invoke and Query to return
	// the SAME millisecond value for a given request so receipt replay is
	// independent of RPC latency, and unlike an in-memory-only cache this
	// survives a tos-ai worker restart between the first observation and
	// a later recovery Query. See completionStore's doc comment.
	completions *completionStore
}

type bindingKey struct {
	transport, endpointRef, capabilityID, capabilityVersion string
}

// NewService builds bound dialers for every approved binding, and opens a
// durable completion-time journal at completionStorePath (see
// completionStore's doc comment). A binding whose transport-specific
// construction fails (e.g. an invalid endpoint_ref URL) fails the whole
// call -- an operator's allowlist is not something this worker silently
// starts with entries missing from.
func NewService(bindings operatorconfig.ThirdPartyBindings, completionStorePath string) (*Service, error) {
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
	completions, err := openCompletionStore(completionStorePath)
	if err != nil {
		closeAll(dialers)
		return nil, err
	}
	return &Service{bindings: bindings, dialers: dialers, completions: completions}, nil
}

// stabilizeCompletedAt returns the first-ever observed completion time for
// requestID, durably recording observed as that value if none was stored
// yet. Called for every terminal (completed/failed) outcome from Invoke
// and Query alike, so a later Query recovering the same request_id --
// even after a tos-ai worker restart -- returns the identical millisecond
// value Invoke (or an earlier Query) already did. worker.proto's own
// doc comment describes this as worker-owned DURABLE state, not
// best-effort, so a store error (disk full, I/O failure, a bbolt
// transaction failure) here MUST fail the whole RPC rather than silently
// returning the freshly observed value as if it had been durably
// recorded -- returning success on a durability failure would let a
// signed Receipt's completed_unix_millis rest on a timestamp this worker
// never actually persisted, defeating the entire reason this store
// exists. The caller propagates this as CodeUnavailable, which routes
// tos-protocol into its existing UNCERTAIN/Query-recovery path -- no
// economic state machine changes needed on that side.
func (s *Service) stabilizeCompletedAt(requestID string, observed, retainUntil int64) (int64, error) {
	stabilized, err := s.completions.stabilize(requestID, observed, retainUntil)
	if err != nil {
		return 0, fmt.Errorf("thirdparty: durably record completion time: %w", err)
	}
	return stabilized, nil
}

// terminalThirdPartyStatus reports whether status is a final outcome worth
// stabilizing a completion time for -- pending/unspecified are not terminal
// and have no meaningful completion time yet.
func terminalThirdPartyStatus(status edgev1.ThirdPartyInvokeStatus) bool {
	switch status {
	case edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_COMPLETED,
		edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_FAILED:
		return true
	default:
		return false
	}
}

func closeAll(dialers map[bindingKey]dialer) {
	for _, d := range dialers {
		d.close()
	}
}

// Close releases every bound transport's idle connections and the
// completion-time journal's underlying file handle.
func (s *Service) Close() {
	closeAll(s.dialers)
	_ = s.completions.close()
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
	// The dialer lookup key MUST come from the matched allowlist entry, not
	// the request: Allowed() lets an entry's CapabilityVersion of "" or "*"
	// match any requested version, but NewService only ever built a dialer
	// under that entry's own literal (possibly wildcard) version. Building
	// the lookup key from ref.CapabilityVersion instead would miss every
	// wildcard entry's dialer even though Allowed() just approved it.
	matched, allowed := s.bindings.Allowed(transport, ref.EndpointRef, ref.CapabilityId, ref.CapabilityVersion)
	if !allowed {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("thirdparty: binding is not on this worker's operator-approved allowlist"))
	}
	d, ok := s.dialers[bindingKey{transport, matched.EndpointRef, matched.CapabilityID, matched.CapabilityVersion}]
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
	if resp != nil && terminalThirdPartyStatus(resp.Status) {
		completedAt, stabilizeErr := s.stabilizeCompletedAt(req.Msg.RequestId, resp.CompletedUnixMillis, req.Msg.RetainUntilUnixMillis)
		if stabilizeErr != nil {
			return nil, connect.NewError(connect.CodeUnavailable, stabilizeErr)
		}
		resp.CompletedUnixMillis = completedAt
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
	if resp != nil && resp.Found && resp.Result != nil && terminalThirdPartyStatus(resp.Result.Status) {
		// ThirdPartyQueryRequest carries no retain_until_unix_millis of its
		// own (unlike ThirdPartyInvokeRequest) -- a Query that observes a
		// terminal outcome before any Invoke on this worker ever recorded
		// one (e.g. tos-protocol crashed between dispatching Invoke and
		// receiving its response, then recovered via Query) has no
		// caller-supplied retention boundary to persist. Falling back to
		// defaultQueryObservedRetention still bounds the record instead of
		// retaining it forever.
		retainUntil := time.Now().Add(defaultQueryObservedRetention).UnixMilli()
		completedAt, stabilizeErr := s.stabilizeCompletedAt(req.Msg.RequestId, resp.Result.CompletedUnixMillis, retainUntil)
		if stabilizeErr != nil {
			return nil, connect.NewError(connect.CodeUnavailable, stabilizeErr)
		}
		resp.Result.CompletedUnixMillis = completedAt
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
