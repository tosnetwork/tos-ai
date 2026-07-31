# `tos-protocol` WorkerService alignment

Status: non-streaming v0.1 interface implemented

`tos-ai` pins `tos-protocol` revision `ce9653400793`, which contains the
WorkerService fields required by the terminal runtime. Protobuf definitions
remain owned by `tos-protocol`; this repository does not copy or fork them.

## Implemented coverage

- `Health` returns six structured readiness components in addition to its
  compact diagnostic status.
- `GetCapabilities` returns fresh, revisioned resource claims and
  per-capability admission limits.
- RAM, VRAM, KV cache, context, batch, output, and execution time use the exact
  identifiers and units defined by the protocol alignment contract.
- Quote treats `requested_limits` as caller-accepted upper bounds, rejects
  unknown or undersized dimensions, and returns the actual locally checked
  `committed_limits`.
- Invoke performs a second authoritative resource/runtime admission and uses
  the resource profile retained with the quote; payloads cannot override it.
- Invoke requires a task ID, exact protocol request digest, and bounded
  retention deadline before it atomically claims the durable task store.
- `GetTask` returns exact retained active or terminal state across worker
  restart.
- Cancel verifies and echoes request ID, task ID, and request digest. The
  request-ID-only legacy form is rejected.
- The diagnostic CLI can create the new Invoke identity and issue `get-task`
  or exact cancellation calls.

The worker emits declared evidence for administrator-owned admission policy
and does not upgrade it merely because it is serialized locally. Runtime model
preflight retains its separate declared or locally-observed evidence. Public
wire values omit GPU serial/UUID, PCI address, MAC address, hostname, IP
address, exact site location, credentials, and raw runtime errors. Resource
attributes are currently empty rather than filled from private inventory.

The real `tos-ai-worker` ConnectRPC handler is exercised through
`tos-protocol/pkg/localrpc.WorkerClient` over the private Unix transport. The
compatibility test covers structured Health/capabilities, resource Quote,
Invoke, retained GetTask success, opaque result consumption, and exact Cancel
validation. Separate tests cover resource limit failures, task identity
conflicts, cancellation, concurrent replay, and result recovery after closing
and reopening the Worker database.

## Streaming

WorkerService v0.1 remains deliberately unary. `tos-ai` does not emulate
streaming with repeated Invoke calls or an opaque byte field. Ordering,
partial-result semantics, bounded buffering and backpressure, cancellation,
terminal state, retry/resume, idempotency, total output, usage, and receipt
binding are specified as a separate `tos-protocol` streaming RFC targeted at
v0.2 unless explicitly accepted before the v0.1 release tag.
