# Messenger A2A and MCP event boundary

`pkg/messengereventbridge` is the private consumer paired with Messenger daemon
configuration v9. It does not accept a bare opaque payload. It independently
decodes the complete canonical Event v2, recomputes its Event ID through the
Messenger codec, checks the exact network, sender and conversation policy, and
then decodes the typed carriage once more.

The service creates mode-`0600` Unix sockets only in an existing private
directory. It never removes an existing path. The sockets are distinct and
profile-specific:

- the A2A socket exposes only `POST /v1/a2a-event` and accepts only
  `a2a.message` with carriage protocol `a2a`, version `1`;
- the MCP socket exposes only `POST /v1/mcp-event` and accepts only `mcp.call`
  or `mcp.result` with carriage protocol `mcp`, version `1`.

Requests require `application/vnd.tos.messaging.event.v2+json`, have a fixed
bound, and reject non-canonical JSON, wrong kinds, network/sender/conversation
substitution, cross-socket paths and unknown protocol fields. There is no
proxy, TCP listener, browser path, text fallback or authority in rendering.

`A2AExecutionHandler` strictly maps the foreign body to the official
`a2a.SendMessageRequest` consumed by `a2aadapter.Adapter`.
`MCPExecutionHandler` maps calls to `mcpadapter.Input` and incoming results to
`mcpadapter.Output`. Execution remains behind the adapters' shared finalized
Execution Gate.

The result receiver is mandatory. A handler returns success only after the A2A
Task or MCP Output has been durably recorded or idempotently published. A
decode, authorization, execution or result-commit failure returns non-202, so
Messenger retains its application lease for retry. Deployments must key result
commit/republication by the source Event ID.

`ResultOutbox` supplies that durable boundary. It is a mode-`0700`, single-owner
directory of mode-`0600` records. The first A2A Task or MCP Output for a source
Event ID is create-exclusive and directory-fsynced; exact retries in pending or
complete state are idempotent, while profile, conversation, sender, digest or
body substitution is refused. Pending records are deterministic and immutable
to callers. Completion requires the exact result digest, uses fsync-before-
rename and survives restart. A publishing loop may therefore retry an outbound
Messenger result and mark this record complete only after the canonical result
Event is durably queued. The outbox does not itself choose a network route.
