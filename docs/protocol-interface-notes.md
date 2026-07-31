# `tos-protocol` interface notes

This PR keeps the pinned `tos-protocol` WorkerService unchanged and does not
copy its protobuf definitions into `tos-ai`.

The current RPC is sufficient for bounded non-streaming capabilities, Quote,
Invoke, Cancel, and a compact health string. A later, separately reviewed
`tos-protocol` change is needed before cross-process clients can express or
consume:

- structured terminal resources with per-field evidence level and freshness;
- structured readiness components rather than a compact status string;
- adapter/model admission dimensions for RAM, VRAM, KV cache, context, batch,
  and expected execution time;
- a quote commitment to those resource/profile dimensions;
- a protocol-defined streaming response with ordering, partial-result,
  cancellation, and terminal-state semantics.

Until those fields exist, `tos-ai` derives conservative resource requirements
from administrator-configured adapter capabilities and performs local
admission again at Invoke. It does not place local Go structs on the wire,
accept resource overrides from task payloads, or emulate streaming over the
unary RPC.
