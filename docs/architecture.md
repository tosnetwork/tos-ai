# TOS AI Tier 1 runtime foundation

The current release is a managed-inference worker foundation, not a bare GPU
rental daemon, hard real-time controller, payment service, or public runtime.

```text
tos-edge (future wallet/payment/auth boundary)
       |
       | ConnectRPC over a private mode-0600 Unix socket
       v
tos-ai-worker
       |
       +-- bounded connection and RPC body limits
       +-- idempotent quote/invocation replay stores
       +-- authoritative local admission reservations
       +-- bounded priority scheduler
       +-- approved runtime adapter
       +-- privacy-preserving host/NVML probe
       +-- signed model/update verification libraries
```

## Execution and reservation lifecycle

Quote validates an exact configured adapter and performs a current admission
check. It creates no durable capacity claim. Invoke validates the quote
binding and request fingerprint, then reserves all configured RAM, VRAM, KV
cache, context, batch, output, and execution-time budgets before scheduler
submission and before adapter execution.

```text
Invoke
  -> replay/conflict check
  -> quote binding check
  -> local reservation
  -> bounded priority queue
  -> mark running
  -> adapter
  -> release reservation
  -> bounded replay result
```

The reservation release is idempotent and runs from both the work lifecycle
and result cleanup. Queue cancellation, caller disconnect, deadline, adapter
error, recovered panic, scheduler shutdown, and normal completion therefore
converge on the same release operation. Shutdown first stops new admission,
then closes ingress, cancels or drains scheduler work, and clears terminal
reservation state.

The accepted inference priority order is:

1. local asynchronous
2. external service
3. background

The protocol enum also contains emergency, control, and real-time perception,
but the current public inference adapters reject them. Those priorities belong
only behind future site-local authorization and an independent safety
controller.

## Resource evidence

The probe layer separates a small backend interface from NVIDIA/go-nvml, so CI
uses fake devices and does not need a GPU. Host data includes OS,
architecture, bounded logical CPU count, and RAM. NVIDIA data includes a
bounded device list, device class, VRAM, driver, CUDA compute capability,
temperature, and power state.

Missing NVML, missing drivers, and zero devices are normal health states.
Invalid counts and telemetry are omitted or marked degraded. Reports never
contain GPU serial/UUID, PCI address, MAC, hostname, or an intentionally
stable hardware fingerprint. Evidence is field-source metadata, not proof that
a self-observation is true.

## Model lifecycle

`pkg/modelmanager` accepts only manifests signed by administrator-configured
Ed25519 public keys. It reuses `pkg/update` for signature, target, validity
window, security revision, size, and SHA-256 verification.

```text
absent -> verifying -> ready -> active -> draining
                    \-> failed
```

Imports write mode-0600 temporary files in a private cache directory, verify
the complete artifact, sync it, and atomically rename it to its SHA-256
address. Failure and cancellation remove temporary files. Entry count,
resident bytes, staging bytes, and LRU state are bounded; active, draining,
pinned, or in-use models cannot be evicted. The manager does not implement
Internet download or accept task-supplied model bytes.

## Runtime adapters

The adapter ABI binds service, operation, model digest, runtime revision,
request/output bounds, accepted priority classes, and local admission
requirements.

- `mock` is deterministic and development-only.
- `ollama` uses `/api/generate`.
- `openai` uses non-streaming `/v1/chat/completions` and targets
  administrator-configured LocalAI/vLLM-style endpoints.

HTTP adapters cap request/response/header sizes, connections, and timeouts;
disable redirects; propagate context cancellation; require HTTPS remotely;
and allow HTTP only for loopback or an explicitly configured private/local
CIDR. Returned errors are categorized without endpoint, filesystem, or
credential details.

## Execution isolation

`pkg/executor` remains fail closed. `DenyAll` is the default and there is no
production containerd backend. The future client contract accepts only a
validated request with a digest-pinned allowlisted image, non-root user/group,
read-only root filesystem, no-new-privileges, PID/CPU/RAM/disk/output/time
limits, and explicitly authorized GPU/network access. Policy rejects
privileged mode, host mounts, writable root, runtime sockets, root identity,
unpinned images, and resource overflow.

## Implemented versus planned

Implemented:

- private bounded Unix-socket worker and diagnostic CLI
- readiness/draining health summary without secret data
- bounded replay, scheduler, local admission, and owner reserve
- graceful cancellation and shutdown resource cleanup
- Linux host and NVIDIA NVML probes with fake backends
- deterministic, Ollama, and OpenAI-compatible adapters
- signed bounded model-manager and update-verification foundations
- container isolation contract and validation layer with `DenyAll`

Planned, not claimed by this release:

- operator configuration and lifecycle wiring for production runtime adapters
  and model slots
- audited containerd execution backend and packaging
- signed benchmark runner and external evidence issuers
- public authentication, payment, receipts, and settlement through Edge Core
- real streaming after a `tos-protocol` streaming RPC exists
- active/known-good software update slots and crash recovery
- ARD catalog/Registry, relay, offline journal, fleet, and physical-terminal
  profiles

KubeEdge, EdgeX, Ollama, LocalAI, vLLM, and containerd remain external
projects or design sources; this daemon does not fork or embed their control
planes.
