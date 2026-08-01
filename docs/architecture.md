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
       +-- immutable administrator terminal resource policy
       +-- exclusive private socket ownership and bounded connections
       +-- bounded RPC body limits
       +-- bounded low-cardinality operational metrics
       +-- idempotent quote cache
       +-- bounded durable Worker task/result store
       +-- authoritative local admission reservations
       +-- bounded priority scheduler
       +-- bounded runtime/model preflight
       +-- bounded subprocess resource-liveness guard
       +-- approved runtime adapter
       +-- privacy-preserving host/NVML probe
       +-- signed model/update verification libraries
```

## Execution and reservation lifecycle

Quote validates an exact configured adapter, requested resource upper bounds,
and current admission. It creates no durable capacity claim. Invoke validates
the quote binding and exact request digest, atomically claims the retained task
identity, then reserves all committed RAM, VRAM, KV cache, context, batch,
output, and execution-time budgets before scheduler submission and adapter
execution.

```text
Invoke
  -> quote binding check
  -> durable task claim/replay/conflict check
  -> runtime and resource recheck
  -> local reservation
  -> bounded priority queue
  -> durable RUNNING transition
  -> adapter
  -> release reservation
  -> durable terminal result
  -> exact Invoke replay or read-only GetTask
```

The task database is mode 0600 under a mode-0700 non-symlink directory. Its
count, conservative retained-byte reservations, per-message bytes, retention,
expiry cleanup, and transition work are bounded. Claim reserves request bytes,
a maximum result, maximum metadata, and fixed logical keys until cleanup, so a
terminal write cannot lose a capacity race. A configured subset of task
identities and maximum-sized byte reservations is atomically reserved for
`LOCAL_ASYNC`; external-service and background claims cannot consume it, and
resource discovery reports the reduced external availability. Physical bbolt
page allocation still requires a filesystem quota. The store contains no raw
failure diagnostics. An exact retained success is
replayed without executing the adapter. Before the private listener opens,
startup removes expired tasks and scans retained state through bounded pages
that contain only active identities. Since synchronous adapters have no
durable executor handle, crash-left accepted/running tasks fail closed as
`RUNTIME_FAILED`; they are never automatically resubmitted. A future durable
supervisor may recover them only by proving ownership of the exact task ID.
Cancellation must repeat the request ID, task ID, and request digest.

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

The scheduler can divide its fixed worker pool into general and
owner-reserved workers. General workers retain normal priority ordering and
may execute all accepted classes. Reserved workers execute only owner-side
priorities through `LOCAL_ASYNC`; they never execute `EXTERNAL_SERVICE` or
`BACKGROUND`. Consequently sustained external work cannot occupy every
execution slot, while an all-local workload can still use the whole pool.
The queue, worker count, and goroutine count remain fixed at startup.

The protocol enum also contains emergency, control, and real-time perception,
but the current public inference adapters reject them. Those priorities belong
only behind future site-local authorization and an independent safety
controller.

## Resource evidence

The probe layer separates a small backend interface from NVIDIA/go-nvml, so CI
uses fake devices and does not need a GPU. Host data includes OS,
architecture, bounded logical CPU count, and RAM. On Linux, CPU and RAM are
effective process resources: the probe takes the minimum of host resources,
process scheduler affinity, cgroup CPU quota/cpuset, and every applicable
cgroup ancestor memory limit. Both cgroup v2 and the memory, CPU, and cpuset
v1 controllers are supported. Membership files, mount metadata, scalar
values, controller and mount counts, paths, CPU indices, and hierarchy depth
are hard-bounded and ambiguous observations fail closed. Cgroup paths never
leave the probe. NVIDIA data includes a bounded device list, device class,
VRAM, driver, CUDA compute capability, temperature, and power state.

Missing NVML, missing drivers, and zero devices are normal health states.
Invalid counts and telemetry are omitted or marked degraded. Reports never
contain GPU serial/UUID, PCI address, MAC, hostname, or an intentionally
stable hardware fingerprint. Evidence is field-source metadata, not proof that
a self-observation is true.

Startup and continuous liveness collection run in a copy of the worker binary
with only an internal probe operation enabled. The parent accepts one strict
JSON report capped at 64 KiB and discards child stderr. A context deadline
kills the child, isolating the long-lived worker and its graceful shutdown
from a stuck NVML/driver call. One fixed monitor goroutine runs at most one
probe subprocess at a time; it retains only health, consecutive failure, and
consecutive recovery counters.

## Terminal policy authority

Production startup requires one private `-terminal-policy-config` document.
It is the immutable process authority for scheduler workers and queue slots,
Unix-socket connection count, quote and invocation replay bounds, maximum
deadline, runtime-preflight cache and refresh work, aggregate admission
capacity, owner resource reserve, owner-reserved workers, and per-request
limits. Task payloads and runtime endpoints cannot modify these values. The
development-only mock mode may omit the document and use conservative
probe-derived defaults with no reserved worker; its legacy resource flags
cannot be combined with an explicit policy.

The strict JSON document is capped at 64 KiB. Version 3 requires
`ownerReservedWorkers` and a `resourceMonitor` interval, timeout, failure
threshold, and recovery threshold. Upgrade-compatible version 2 retains its
worker reserve and receives fixed safe monitor defaults; version 1 maps to
zero reserved workers and the same defaults. Older schemas cannot carry newer
fields. The loader rejects duplicate or unknown fields, excessive nesting,
multiple JSON values, symlinks,
non-regular files, wrong ownership, or group/other file permissions. Central
package hard limits cap every scheduler, connection, replay, deadline,
preflight, and admission field. Owner reserve must be meaningful for RAM,
context, batch, and output, and for VRAM whenever GPU capacity is configured.
The worker reserve may be zero and must remain below the total worker count so
external work always has a bounded execution path.

Worker reservation assumes the caller has already been classified at the
trusted local Edge Core boundary. The inference protocol carries a priority
class but no worker-side wallet authority, and `tos-ai-worker` deliberately
does not load an owner key. Operators must restrict the mode-0600 socket to a
dedicated Unix identity and Edge Core must not assign `LOCAL_ASYNC` to remote
consumer work. This reservation is availability isolation, not caller
authentication or hard real-time preemption.

After NVML and host probing but before allocating runtime state or creating a
listener, startup also limits configured RAM to 75 percent of observed
effective RAM and configured VRAM to the sum of currently observed free
device memory. In a Linux container or systemd cgroup, the effective RAM
already includes all visible ancestor hard limits. Missing GPUs therefore
remain a normal CPU-only state but cannot satisfy a positive VRAM policy.

During operation, each isolated probe subprocess re-reads process affinity and
cgroup membership, mounts, CPU quota/cpuset, and memory limits. The resource
guard verifies the configured effective RAM backing and, for positive VRAM
policies, the presence of an available NVIDIA driver, devices, and sufficient
total VRAM. It intentionally checks total rather than free VRAM:
runtime/model allocations make free-memory telemetry unsuitable for resizing
admission underneath active reservations. Consecutive failures
close the new-work gate; consecutive healthy observations reopen it. While
closed, readiness reports `resources=degraded`, capabilities are empty, and
new Quote and Invoke owners fail unavailable. Exact Quote retries and
in-flight/completed Invoke replays retain idempotent behavior, and running
tasks are not preempted. The health transition is also reflected in the
capacity revision.

The policy is intentionally not hot-reloaded. This liveness gate does not
resize resource budgets, measure cgroup working-set consumption or pressure,
react to transient free-memory changes, or prevent independent worker
processes from overcommitting the same host. Intervals are
bounded from one second through five minutes, subprocess timeouts from 100
milliseconds through 30 seconds and no longer than the interval, and both
hysteresis thresholds from one through ten.

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
restored as `ready`; the model manager alone never loads a runtime. The cache
root is an operator-owned single-manager boundary. On Linux, `New` acquires a
non-blocking exclusive `flock` on a current-user, mode-0600 regular file in
the private cache. Concurrent owners fail immediately. `Close` refuses to
release ownership while an import, active/draining model, pin, or lease
remains, so startup handover cannot race live cache mutation. A process crash
releases the kernel lock; the persistent lock file itself is reused during
bounded recovery. The manager does not implement Internet download or accept
task-supplied model bytes.

`pkg/modelapproval` optionally connects this cache authority to production
runtime adapters without activating a model. The worker loads a private,
strict model-trust configuration containing only Ed25519 public keys and
bounded cache policy. Each configured adapter must acquire its exact digest
as a path-free lease and pass a complete SHA-256 rehash during startup. The
lease remains held, so the approved entry cannot be evicted, and a serialized,
cancellation-aware rehash runs before every adapter preflight. A missing,
unsigned, wrong-target, expired-at-import, or security-revision rollback
artifact fails worker startup. A cache artifact changed after startup prevents
advertisement and Invoke from reaching local admission. Shutdown closes both
the runtime adapter and approval lease once.

`pkg/modelactivation` adds the in-process coordination contract for audited
runtime loaders. It acquires an already-open, path-free artifact lease,
which keeps the content-addressed cache entry in use and non-evictable. Before
the backend sees the artifact, the controller recomputes its SHA-256 through a
1 MiB buffer under a hard operation timeout. Backends receive only a bounded
slot policy, exact model digest, size, and `io.ReaderAt`; they do not receive a
task-selected path, URL, endpoint, owner key, or executable payload.

Each controller has at most 64 fixed administrator slots, one synchronous
operation per slot, and no queue, watcher, or background goroutine. A
candidate must return an exact locally observed binding and pass a bounded
health gate before it can replace the current model. The current binding and
artifact lease remain the known-good state until replacement succeeds.
Failure rolls the candidate back; failed cleanup retains at most one bounded
candidate lease per slot, blocks further activation, and exposes an explicit
retry operation. Shutdown synchronously unloads bindings and releases leases.

An optional persistent mode records only sorted fixed slot IDs and desired
SHA-256 digests in a canonical mode-0600 state file under a mode-0700
operator-owned directory. The file is capped at 64 KiB, the directory scan at
128 entries, and the state at 64 slots. Writes use a new generation, a synced
temporary file, atomic rename, and directory sync. A write error is treated as
an ambiguous commit: every lifecycle operation is blocked until explicit
recovery reloads the file. This local generation is crash ordering, not a
tamper-proof or externally anchored rollback counter. On Linux, the state
directory is an enforced single-controller boundary using the same
non-blocking private-file `flock` discipline. A normal `Close` releases it
only after every binding and artifact lease is cleaned up; a cleanup failure
retains ownership for a bounded retry. A process crash releases the lock so a
new controller can perform explicit recovery.

Persistent controllers start in `recovering`. Their backend inspection result
is a fixed-size value containing at most two bindings per slot. Recovery
reloads and strictly validates the state file, obtains a path-free lease,
rehashes the full desired artifact, and health-checks an existing exact
binding or loads it as a separate candidate. Only after the desired binding is
healthy may recovery unload an older or uncommitted binding. Missing,
oversized, duplicate, mismatched, or non-locally-observed bindings fail
closed. Recovery also refuses to load a missing desired binding when both
fixed runtime binding positions are already occupied, so it never creates an
unbounded or third transient binding. One retained candidate lease per slot
keeps retry state bounded and non-evictable. A normal shutdown unloads runtime
bindings but retains desired intent so a later process can restore them.

`pkg/modelactivation/ollama` implements the first concrete backend for a fixed
administrator-owned Ollama endpoint. It accepts only a controller-provided
open GGUF artifact and exact SHA-256 digest. It checks or uploads the
content-addressed blob, creates a private `tos-ai/<slot>:<digest>` model from
one fixed `approved-model.gguf` file entry, then verifies `/api/show` reports
GGUF and resolves its first `FROM` instruction to the exact approved blob.
Health rechecks the model and blob and performs an empty bounded preload.
Unload requests memory eviction and deletes the private model. No pull API,
Internet URL, task-selected path, arbitrary Modelfile, or owner credential is
accepted.

The backend serializes operations through one cancellation-aware fixed gate,
has no watcher or background goroutine, and bounds connections, headers,
response bytes, JSON depth/fields/items, runtime inventory, recovery
candidates, and cleanup time. All errors crossing the package boundary are
stable and omit endpoints and paths. An uncertain create failure returns the
candidate binding so the controller can retry cleanup; a repeated cleanup
failure retains one lease and blocks the slot.

`tos-ai-worker` enables this path only when a private runtime configuration
contains both a global activation state policy and an Ollama slot and a
separate signed model-trust configuration is supplied. Model-cache and
activation-state directories may not contain one another. Startup performs
`Recover`, activates every fixed desired digest, wraps inference adapters with
the signed-cache approval guard, and only then creates the Unix listener.
Shutdown drains service work before synchronously unloading activation slots
and closing backend transports, then releases activation-state and model-cache
ownership. Desired intent remains on disk, so a restart can recreate a cleanly
removed private runtime model without downloading it.

Activation for LocalAI, vLLM, llama.cpp, and vendor runtimes remains
unimplemented. There is no live lifecycle RPC or arbitrary runtime/model
creation interface. The Ollama endpoint and each activation slot namespace
are a single-worker operator boundary; there is no distributed lock across
workers sharing one runtime. The local cache/state locks require a filesystem
with reliable Linux `flock` semantics and do not defend against a hostile
process running as the same Unix user.

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
support bounded private roots and optional mutual TLS; and allow HTTP only for
loopback or an explicitly configured private/local CIDR. Every plaintext CIDR
must be wholly contained inside a recognized loopback, RFC 1918 private,
link-local, or IPv6 ULA range; validating only the network address is
insufficient because a shorter prefix could also cover public addresses.
Invalid CIDRs fail startup even when attached to an HTTPS adapter. Returned
errors are categorized without endpoint, filesystem, or credential details.

Plaintext `localhost` is resolved inside the connection timeout through a
bounded path accepting at most 16 answers. Every answer must be loopback
before any connection is attempted, the dial target must retain the exact
configured host and port, and only verified numeric loopback addresses reach
the socket dialer. This avoids treating a poisoned or unexpectedly broad
`localhost` resolution as local transport authority. Other plaintext runtime
hosts must be literal addresses, so they do not acquire a DNS rebinding path.
All runtime transports additionally reject a dial whose host or port differs
from the administrator-configured endpoint, including HTTPS transports; the
HTTP client is not a general-purpose egress client.

Runtime configuration version 2 can replace the system trust store with an
administrator-owned CA bundle, present one client certificate identity, and
set a restricted DNS verification name for an IP-pinned endpoint. Version 1
remains compatible only without a TLS identity. TLS has a 1.2 minimum, never
sets `InsecureSkipVerify`, and rejects TLS material on plaintext endpoints.
Root bundles contain at most 64 unique CA certificates; client chains contain
at most eight certificates; CA and certificate files are capped at 1 MiB and
private keys at 256 KiB. All files are private, regular, non-symlink files owned
by the worker user. PEM parsing rejects non-certificate roots, non-CA roots,
duplicates, headers, extra blocks, partial identities, and mismatched keypairs.
Ollama activation and inference receive the same immutable TLS material, so a
model-management request cannot bypass the adapter transport boundary. TLS
keys authenticate the runtime connection only and are unrelated to TOS wallet
owner keys. Identity rotation currently requires a worker restart.

A separately managed Ollama model preflight reads `/api/tags`, accepts at most
1 MiB and 256 entries, and requires one exact model-name and SHA-256 digest
match. An activated slot uses its private digest-scoped runtime model and
validates `/api/show` against the exact source blob instead. Its public
capability continues to expose the stable administrator model name, not the
private handle. Both modes report `locally-observed` digest evidence.
OpenAI-compatible preflight reads `/v1/models` with the same bounds and
requires one exact model ID. Because that API provides no portable content
digest, its administrator-configured digest remains `declared`; the worker
does not upgrade that claim to observed or attested.

Each configured adapter has one fixed-size preflight slot. Concurrent checks
coalesce, waiters have an explicit cap, success and failure have separate
short TTLs, and adapter panics or errors are reduced to stable categories.
One process-owned monitor refreshes the fixed slots periodically using a
bounded worker pool; it does not create a watcher for each adapter, request,
retry, or failure. Startup performs the same bounded full refresh but remains
diagnosable when a local runtime is down. Stale or failed slots are excluded
from capabilities, rejected before Quote admission checks, and rechecked
authoritatively for every new owner inside the idempotent Invoke lifecycle
before reservation. Completed or in-flight request-ID replays retain their
original result and do not create duplicate checks or executions.

The monitor interval is bounded to 250 milliseconds through five minutes and
the full-refresh pool to 16 workers across at most 64 adapter slots. Startup
rejects a refresh interval plus the worst-case timeout for every adapter batch
that exceeds the readiness success TTL.
Every check is tied to a service-owned cancellation context and tracked until
completion. Shutdown prevents new checks before canceling that context, then
waits for both scheduler work and preflight activity before closing adapter
pools. If the caller's shutdown deadline expires, the service returns an
explicit incomplete-shutdown error and leaves adapters open; model activation
cleanup is skipped rather than racing a still-running adapter operation.

`tos-ai-worker -runtime-config` loads a bounded JSON document from a private,
current-user-owned, non-symlink regular file. Unknown fields, duplicate keys,
excessive nesting, multiple JSON values, more than 64 adapters, and insecure
file modes fail startup. An OpenAI-compatible API key may only be loaded from
a separate private file; credentials and endpoints are not task-controlled.
The worker requires this flag unless `-dev-mock` explicitly selects the
deterministic development adapter. Missing mode selection fails startup;
development mock flags cannot be mixed with runtime or model-trust
configuration. Adapter connection pools are closed once after scheduler
drain.

Outside development mock mode the worker independently requires
`-terminal-policy-config`; runtime configuration does not carry or override
terminal capacity. Production startup therefore has two deliberately narrow
authorities: runtime configuration selects fixed adapters and endpoints,
while terminal policy bounds local execution and memory pressure.

Optional `-model-trust-config` is accepted only with `-runtime-config`. It is
capped at 64 KiB and uses the same private-file, duplicate-key, nesting, and
unknown-field checks. Cache capacity remains bounded by `pkg/modelmanager`;
there are at most 64 signer public keys and one retained lease per configured
adapter. The worker loads no signing private key. Model approval failures use
stable unavailable categories and do not expose cache paths or signer data.

The configured model digest remains an administrator assertion for generic
OpenAI-compatible runtimes even when the same digest is approved in the local
signed cache. Ollama activation binds one private model to the exact approved
source blob, but neither interface provides hardware attestation or proves
how runtime memory is executed.

## Private operational metrics

The worker serves Prometheus text at `GET /metrics` on its existing
mode-0600 Unix socket. It creates no TCP listener, discovery service, exporter
goroutine, watcher, or remote egress path. The diagnostic CLI uses the same
bounded Unix transport and accepts at most 16 KiB of printable metrics text,
so an unexpected local response cannot stream without limit or inject terminal
control sequences.

Metric storage has a fixed shape: six protocol methods multiplied by the
fixed Connect outcome enum, plus one in-flight gauge per method. Readiness,
runtime counts, admission task state, configured limits, and the six fixed
admission resource dimensions are rendered from bounded values. Counters use
saturating atomics; no request creates a map entry or label value. In
particular, request IDs, service/model selectors, runtime endpoints, error
text, credentials, filesystem paths, hostnames, and stable hardware
identifiers are neither retained nor emitted. A scrape is GET-only, rejects a
query or body, returns `no-store`, and is capped at 16 KiB.

Readiness and admission gauges are derived from one admission snapshot, so a
single response does not combine pre- and post-reservation task counts. RPC
counters, including startup expired-removal and interrupted-failure counts,
are intentionally process-local and reset on restart. This endpoint
is private operational telemetry, not authenticated public monitoring, a
durable audit log, a billing record, or the planned offline journal. A
collector that needs remote access must run outside the worker and preserve
the Unix identity boundary.

## Execution isolation

`pkg/executor` remains fail closed. `DenyAll` is the default and there is no
production containerd backend. The future client contract accepts only a
validated request with a digest-pinned allowlisted image, non-root user/group,
read-only root filesystem, no-new-privileges, PID/CPU/RAM/disk/output/time
limits, and explicitly authorized GPU/network access. Policy rejects
privileged mode, host mounts, writable root, runtime sockets, root identity,
unpinned images, and resource overflow.

## Process ownership

The Unix listener creates a per-socket, current-user, mode-0600 advisory lock
inside an exact mode-0700 directory before inspecting the socket path. A
second compliant process therefore cannot unlink an active worker. For a
socket left by a version without the ownership lock, startup performs one
bounded local stream connection probe: a successful connection proves the
listener is active, `ECONNREFUSED` permits stale-socket removal, and every
other outcome fails closed. Listener cleanup and lock release execute exactly
once, so a repeated or concurrent close cannot remove a successor socket.
The lock is local operational coordination, not a distributed lock or a
defense against a hostile process with the same Unix identity.

## Implemented versus planned

Implemented:

- exclusively owned private bounded Unix-socket worker and diagnostic CLI
- fail-closed runtime selection with an explicit development mock mode
- structured readiness plus a compact health summary without secret data
- private bounded low-cardinality operational metrics on the existing Unix
  socket, with a bounded safe-text CLI diagnostic
- bounded durable task/replay state, read-only restart recovery, exact
  cancellation, scheduler, local admission, resource owner reserve, and
  owner-reserved execution workers
- task-store capacity readiness, `storage.task_slots` and
  `storage.task_bytes` capability/Quote commitments, atomic owner-local
  slot/byte reserves, priority-aware routing suppression, and fixed private
  capacity metrics
- deterministic privacy-minimized one-service ARD catalog generation from
  fresh validated externally callable Worker capabilities and
  operator-approved identity/routing metadata, without publication or a
  public listener; optional atomic mode-0600 local handoff rejects symlinks and
  is covered through the protocol Registry's fail-closed atomic reload path
- cross-repository discovery coverage proving the real Worker model selector
  is found through the bounded Registry HTTP search handler, extension index,
  and exact service, operation, model, and runtime filters
- bounded startup cleanup and payload-free pagination that resolves
  interrupted synchronous tasks to a retained zero-charge failure without
  resubmission
- graceful cancellation and shutdown resource cleanup
- Linux host, cgroup v1/v2 effective-resource, and NVIDIA NVML probes with
  fake backends
- timeout-isolated continuous host/GPU-class liveness gating with bounded
  failure and recovery hysteresis
- deterministic, Ollama, and OpenAI-compatible adapters
- bounded runtime preflight, readiness filtering, and evidence-preserving
  model binding, with active bounded health refresh and lifecycle cancellation
- private bounded operator configuration and production adapter wiring
- bounded runtime private-CA trust and optional mTLS for inference, preflight,
  and Ollama activation, with strict private-file and PEM validation
- mandatory private production terminal policy with observed RAM/free-VRAM
  startup validation and a single resource authority
- signed bounded model-manager and update-verification foundations
- fail-fast exclusive local ownership for model caches and persistent
  activation state, with cleanup-aware handover
- optional signed-cache model approval guards in worker preflight
- path-free model leases, bounded runtime-activation coordination, and
  crash-safe active/known-good intent with explicit recovery
- opt-in signed-cache GGUF activation for fixed Ollama endpoints, including
  startup recovery and synchronous shutdown cleanup
- container isolation contract and validation layer with `DenyAll`

Planned, not claimed by this release:

- activation backends for LocalAI, vLLM, llama.cpp, and vendor runtimes
- live administrator lifecycle controls for fixed activation slots
- authenticated policy rollout, hot reload, and dynamic capacity resizing or
  cross-process host coordination
- audited containerd execution backend and packaging
- signed benchmark runner and external evidence issuers
- public authentication, payment, receipts, and settlement through Edge Core
- real streaming after a `tos-protocol` streaming RPC exists
- active/known-good software update slots and crash recovery
- ARD publication/Registry hosting, relay, offline journal, fleet, and
  physical-terminal profiles
- authenticated remote metrics collection and durable operational history

KubeEdge, EdgeX, Ollama, LocalAI, vLLM, and containerd remain external
projects or design sources; this daemon does not fork or embed their control
planes.
