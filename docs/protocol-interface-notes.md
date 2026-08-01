# `tos-protocol` WorkerService alignment

Status: non-streaming v0.1 interface implemented

`tos-ai` pins `tos-protocol` revision `f9de14fd79cc`, which includes the
priority-aware task-store statistics, atomic owner-reserved slot enforcement,
retained-byte counters, the `storage.task_bytes` contract, and the
privacy-minimized Worker-to-ARD projection and atomic local catalog handoff
used below. That revision also provides the strict Worker extension decoder,
bounded Registry search/filter projection, strict local catalog reader, atomic
complete-catalog-set reload, and bounded Registry request admission. Protobuf
definitions remain owned by `tos-protocol`; this repository does not copy or
fork them. These ARD additions do not change WorkerService v0.1 protobuf
fields.

## Implemented coverage

- `Health` returns seven structured readiness components in addition to its
  compact diagnostic status.
- `GetCapabilities` returns fresh, revisioned resource claims and
  per-capability admission limits.
- RAM, VRAM, KV cache, context, batch, output, execution time, and durable task
  slots use the exact identifiers and units defined by the protocol alignment
  contract.
- `storage.task_slots` reports bounded logical count/capacity, while
  `storage.task_bytes` reports conservative retained-byte reservations. Every
  capability and Quote commits one slot plus the maximum per-task byte charge.
  Owner-reserved slots imply maximum-sized owner byte reservations limited to
  `LOCAL_ASYNC`; external routing stops at either boundary while owner-local
  Quote/Invoke remains available and the atomic durable claim remains
  authoritative.
- Quote treats `requested_limits` as caller-accepted upper bounds, rejects
  unknown or undersized dimensions, and returns the actual locally checked
  `committed_limits`.
- Invoke performs a second authoritative resource/runtime admission and uses
  the resource profile retained with the quote; payloads cannot override it.
- Invoke requires a task ID, exact protocol request digest, and bounded
  retention deadline before it atomically claims the durable task store.
- `GetTask` returns exact retained active or terminal state across worker
  restart.
- Before opening its private listener, the synchronous worker performs
  bounded expired cleanup and payload-free active-task pagination. Retained
  interrupted tasks become `FAILED/RUNTIME_FAILED` and are never resubmitted;
  a durable executor supervisor is required before any future resume policy.
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

## First vertical profile mapper

`pkg/profile/textgeneration` now implements the externally maintained
`tos.ai.text-generation` profile v0.1.0 for operation `generate`. It parses
only the normative `{model, prompt}` intent and maps it to the Worker's model
selector and prompt payload. Its immutable route set binds each accepted
model to an operator-reviewed Worker service ID.

The mapper cannot provide any security-owned Worker field. Edge retains the
request, quote, payment, task, priority, deadline, output and retention
bindings and validates the mapped request before changing durable state.
The exact registration is introspectable through
`ProfileInvocationRegistry.Supports`, allowing future deployment composition
to fail before advertising a profile without its reviewed mapper. The schema,
semantic limits, privacy rules and fixed vectors live under
`spec/profiles/text-generation/v0.1/` in this repository.
