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
       +-- private administrator runtime configuration
       +-- bounded connection and RPC body limits
       +-- idempotent quote/invocation replay stores
       +-- authoritative local admission reservations
       +-- bounded priority scheduler
       +-- bounded runtime/model preflight
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
address alongside a canonical metadata sidecar containing the signed manifest
and its acceptance time. Both files and the cache directory are synced before
the model becomes ready. Failure and cancellation remove temporary files.
Entry count, resident bytes, staging bytes, metadata size, directory scan
count, and LRU state are bounded; active, draining, pinned, or in-use models
cannot be evicted.

On restart, the manager scans at most 4,096 directory entries, removes staging
files and incomplete crash pairs, and revalidates signer, target, security
revision, original acceptance window, size, file mode, path digest, and
SHA-256. Manifest expiry still rejects a new import, while an artifact that
was accepted during the signed validity window may restart after expiry.
Recovery verifies every retained artifact before applying capacity-driven
evictions. Volatile active, draining, pinned, and in-use state is deliberately
restored as `ready`; no model is automatically loaded into a runtime. The
cache root is an operator-owned single-manager boundary; concurrent processes
must not share it because cross-process locking is not implemented. The manager
does not implement Internet download or accept task-supplied model bytes.

## Runtime adapters

The adapter ABI binds service, operation, model digest, runtime revision,
request/output bounds, accepted priority classes, and local admission
requirements. Every adapter also provides a cancellation-aware preflight that
must return the exact configured model binding plus an explicit digest
evidence level.

- `mock` is deterministic and development-only.
- `ollama` uses `/api/generate`.
- `openai` uses non-streaming `/v1/chat/completions` and targets
  administrator-configured LocalAI/vLLM-style endpoints.

HTTP adapters cap request/response/header sizes, connections, and timeouts;
disable redirects; propagate context cancellation; require HTTPS remotely;
and allow HTTP only for loopback or an explicitly configured private/local
CIDR. Returned errors are categorized without endpoint, filesystem, or
credential details.

Ollama preflight reads `/api/tags`, accepts at most 1 MiB and 256 entries, and
requires one exact model-name and SHA-256 digest match. Its digest evidence is
`locally-observed`. OpenAI-compatible preflight reads `/v1/models` with the
same bounds and requires one exact model ID. Because that API provides no
portable content digest, its administrator-configured digest remains
`declared`; the worker does not upgrade that claim to observed or attested.

Each configured adapter has one fixed-size preflight slot. Concurrent checks
coalesce without spawning a watcher, waiters have an explicit cap, success
and failure have separate short TTLs, and adapter panics or errors are reduced
to stable categories. Startup performs a bounded refresh but remains
diagnosable when a local runtime is down. Stale or failed slots are excluded
from capabilities, rejected before Quote admission checks, and rechecked
authoritatively for every new owner inside the idempotent Invoke lifecycle
before reservation. Completed or in-flight request-ID replays retain their
original result and do not create duplicate checks or executions.

`tos-ai-worker -runtime-config` loads a bounded JSON document from a private,
current-user-owned, non-symlink regular file. Unknown fields, duplicate keys,
excessive nesting, multiple JSON values, more than 64 adapters, and insecure
file modes fail startup. An OpenAI-compatible API key may only be loaded from
a separate private file; credentials and endpoints are not task-controlled.
If the flag is omitted, the worker retains its deterministic development mock.
If the flag is present, the mock is not mixed into the configured production
capabilities. Adapter connection pools are closed once after scheduler drain.

The configured model digest remains an administrator assertion for generic
OpenAI-compatible runtimes. Ollama supplies a runtime digest for an exact
local observation, but neither interface provides hardware attestation or a
portable proof that the process loaded a particular `pkg/modelmanager`
artifact. Automatic activation of verified model-manager artifacts is still
planned.

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
- bounded runtime preflight, readiness filtering, and evidence-preserving
  model binding
- private bounded operator configuration and production adapter wiring
- signed bounded model-manager and update-verification foundations
- container isolation contract and validation layer with `DenyAll`

Planned, not claimed by this release:

- automatic activation of signed model slots into configured runtimes
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
