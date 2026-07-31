# TOS AI

`tos-ai` is the vertical AI Edge Computing Terminal implementation for TOS
Network. It turns owner-operated hardware into bounded AI services while
preserving local resource authority. It is not a validator, bare GPU rental
daemon, public shell, or wallet process.

## Implemented foundation

The Go 1.24 module currently contains:

- `tos-ai-worker`, a private ConnectRPC worker served only on an exclusively
  owned mode-0600 Unix socket, with bounded connections and no public
  listener;
- `tos-ai-cli`, with `health`, `capabilities`, `metrics`, `quote`, `invoke`,
  and `cancel` diagnostics;
- a private Prometheus text snapshot on the same Unix socket, with a 16 KiB
  response cap and only fixed method, outcome, state, limit, and resource
  labels. It does not emit request IDs, model names, endpoints, credentials,
  host identifiers, or hardware identifiers;
- CPU, RAM, OS, and architecture discovery plus NVIDIA/go-nvml GPU, VRAM,
  driver, CUDA capability, temperature, and power probes. On Linux, reported
  CPU is bounded by scheduler affinity, while CPU and RAM both honor strict,
  bounded cgroup v1 or v2 ancestor limits rather than blindly advertising
  host totals;
- graceful `unavailable` or `no-devices` NVIDIA states when NVML or a GPU is
  absent;
- a bounded continuous resource-liveness guard that runs host/NVML probes in
  deadline-killable subprocesses, applies failure/recovery hysteresis, and
  blocks new capabilities, quotes, and invocations if the configured host
  class disappears;
- a bounded local `AdmissionController` for concurrency, queue slots, RAM,
  VRAM, KV cache, context, batch, output, execution time, owner reserve, and
  the three accepted inference priorities;
- scheduler-level owner worker reservation, so external/background saturation
  cannot consume every execution slot while local asynchronous work may still
  use the full pool when capacity is idle;
- idempotent Quote/Invoke handling and reservation cleanup after success,
  failure, cancellation, deadline, disconnect, adapter failure, panic, and
  shutdown;
- a signed, SHA-256-addressed model-manager library with verifying, ready,
  active, draining, failed, and absent states, bounded LRU storage, protected
  active/pinned/in-use entries, atomic artifact/metadata activation,
  restart-time integrity recovery, crash-residue cleanup, and exclusive
  process ownership of each private cache;
- path-free model artifact leases plus a bounded runtime activation
  coordinator with activation-time SHA-256 verification, health gating,
  known-good preservation, rollback, retryable cleanup, and optional
  crash-safe activation intent with explicit startup recovery and exclusive
  process ownership of each persistent state directory;
- an opt-in Ollama GGUF activation backend that uploads only an already-open
  signed-cache artifact, creates a digest-scoped private runtime model,
  verifies its exact source blob, preloads it before readiness, and
  synchronously unloads and deletes it during shutdown;
- optional worker model-approval guards that bind configured runtime digests
  to recovered, signed local cache artifacts, retain bounded leases, and
  rehash them before every runtime preflight;
- deterministic mock, Ollama, and generic OpenAI-compatible HTTP adapters;
- bounded runtime/model preflight before advertisement, Quote, and Invoke,
  with a single process monitor, bounded refresh concurrency, fixed-size
  singleflight state, expiring success/failure caches, and stable redacted
  failures;
- strict administrator-owned JSON runtime configuration with bounded defaults,
  duplicate/unknown-field rejection, and private credential-file loading;
- a separate immutable terminal policy that is mandatory outside development
  mock mode and bounds scheduler capacity, socket connections, replay state,
  deadlines, runtime health work, admission resources, and owner reserve;
- bounded HTTP transports with administrator-selected endpoints, HTTPS for
  remote endpoints, strict private-CA and optional mTLS identities, whole-range
  validation for explicit local-CIDR plaintext exceptions, loopback-pinned
  `localhost` resolution, deadline propagation, and stable redacted runtime
  error categories;
- a fail-closed container execution policy and narrow future containerd client
  contract. No containerd backend is enabled;
- Ed25519-signed update manifest verification, SHA-256 artifact verification,
  expiry checks, target binding, and security-revision anti-rollback.

Resource reports deliberately omit GPU serials and UUIDs, PCI identifiers,
MAC addresses, hostnames, and other stable hardware fingerprints. Probe claims
carry explicit evidence levels such as `declared`, `locally-observed`, and
`benchmarked`; the current live probes are `locally-observed`.

## Development

The module pins an exact `tos-protocol` revision, so a standalone clone builds
normally:

```sh
make all
make test-race
```

Contributors changing both repositories together may optionally use a local Go
workspace with `go work init ./tos-protocol ./tos-ai`.

## Run the development worker

```sh
go run ./cmd/tos-ai-worker \
  -dev-mock \
  -socket /run/user/$(id -u)/tos-ai/worker.sock

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock health

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock capabilities

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock metrics

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock quote hello

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock invoke hello
```

The worker fails closed unless exactly one runtime mode is selected:
`-runtime-config /absolute/private/runtime.json` for configured Ollama and
OpenAI-compatible adapters, or explicit `-dev-mock` for the deterministic
development adapter. `-dev-mock`, `-mock-delay`, production runtime
configuration, and model trust cannot be ambiguously mixed. An invocation
payload can never select or override an endpoint. Model import similarly
accepts a bounded reader and an approved signed manifest, not an arbitrary
Internet URL.

Production adapters may additionally be constrained to the signed local model
cache:

```sh
go run ./cmd/tos-ai-worker \
  -socket /run/user/$(id -u)/tos-ai/worker.sock \
  -terminal-policy-config /etc/tos-ai/terminal-policy.json \
  -runtime-config /etc/tos-ai/runtime.json \
  -model-trust-config /etc/tos-ai/model-trust.json
```

Production mode requires `-terminal-policy-config`. This private, strict JSON
file is the single authority for worker and queue counts, connection and
replay bounds, deadlines, preflight cadence, aggregate admission capacity,
owner-reserved worker slots and resources, and per-request maxima. It is
capped at 64 KiB and uses the same regular-file, ownership, mode,
duplicate-key, nesting, and unknown-field checks as other operator
configuration. In version 3, `ownerReservedWorkers` must be explicit,
non-negative, and less than `workers`; setting it to zero is supported for
single-worker or best-effort installations but provides no execution-slot
isolation. Version 3 also requires a bounded `resourceMonitor` interval,
subprocess timeout, and failure/recovery thresholds. External and background
work runs only on general workers, while local asynchronous work can use both
general and owner-reserved workers. Startup rejects RAM
capacity above 75 percent of locally observed effective RAM or VRAM capacity
above the currently observed free VRAM, before creating the listener. On
Linux, effective RAM is the smaller of physical RAM and every applicable
cgroup ancestor hard limit. A nonzero
resource owner reserve is mandatory for RAM, context, batch, and output, and
for VRAM when VRAM capacity is enabled. The policy is immutable for the
process lifetime; changing it requires a restart. See
[docs/terminal-policy-config.example.json](docs/terminal-policy-config.example.json)
and size it for the actual host and configured adapters.

The current policy schema is version 3. Versions 1 and 2 remain accepted for
upgrade compatibility with fixed safe monitor defaults of a ten-second
interval, five-second timeout, and two-sample failure/recovery thresholds.
Version 1 maps to zero reserved workers. Older versions cannot carry newer
fields; operators should migrate deliberately to version 3.

The resource monitor owns exactly one goroutine and at most one probe
subprocess at a time. Probe output is strict and capped at 64 KiB; Linux
cgroup membership, mount metadata, scalar files, hierarchy depth, controller
count, and mount count all have independent hard bounds. A timeout kills the
subprocess, so a stuck NVML call cannot pin worker shutdown. Each fresh child
re-observes scheduler affinity plus cgroup v1/v2 memory, CPU-quota, and cpuset
constraints without returning cgroup paths. After
the configured failure threshold, readiness reports `resources=degraded`,
capabilities become empty, and new Quote/Invoke owners receive unavailable.
Exact Quote retries and in-flight or completed Invoke replays preserve their
idempotent result, and already-running work is not preempted. Admission
reopens only after the configured recovery threshold. CPU-only policies treat
a missing GPU as normal. A positive VRAM policy requires the configured GPU
class, driver, devices, and total VRAM to remain present.

The `-workers`, `-max-queue`, `-max-connections`,
`-runtime-health-interval`, and `-runtime-health-workers` flags are retained
only for explicit `-dev-mock` diagnostics without a terminal policy. They
cannot be mixed with a policy file, avoiding two competing resource
authorities.

The model-trust file is private, strict JSON containing the cache path, target,
security revision, capacity bounds, verification timeout, and canonical
base64 Ed25519 public keys. It never contains a private signing key. Every
configured adapter digest must already be present as an approved
`pkg/modelmanager` cache entry or startup fails. The worker retains one
path-free lease per adapter and rehashes the full artifact before each runtime
preflight; graceful shutdown releases all leases and then the cache ownership
lock. A second process using the same cache fails startup immediately. There
is no model upload or download RPC, and the default mock mode rejects
`-model-trust-config`. See
[docs/model-trust-config.example.json](docs/model-trust-config.example.json);
the example public key is a structural placeholder and must be replaced with
the operator's approved Ed25519 public key.

An Ollama adapter may additionally opt into controlled startup activation.
This requires both the global `activation` policy and an adapter-local fixed
slot, plus `-model-trust-config`. Startup synchronously recovers the bounded
private intent, rehashes the signed-cache GGUF, uploads that open artifact to
Ollama's content-addressed blob endpoint when absent, creates a private model
named from the slot and SHA-256 digest, verifies `/api/show` resolves to that
exact blob, and preloads it before the worker listens. It never calls a model
pull API, accepts a URL or host path, or exposes the private runtime model name
to task payloads. Shutdown drains inference, unloads and deletes the private
runtime model, releases all artifact leases, and leaves only the desired
slot/digest intent for bounded restart recovery. The activation state
directory must be private, absolute, and separate from the model cache. One
controller holds it exclusively; a concurrent controller fails closed. See
[docs/ollama-activation-config.example.json](docs/ollama-activation-config.example.json).
Each activation slot namespace and its Ollama endpoint are a single-worker
operator boundary; concurrently managed workers must use isolated runtimes or
distinct slot IDs.

The worker preflights configured runtimes at startup, refreshes them through
one process-owned periodic monitor, refreshes stale bindings for capabilities
and Quote, and performs an authoritative recheck for every new Invoke owner.
A separately managed Ollama model name and SHA-256 digest are matched against
its bounded `/api/tags` inventory. An activated Ollama slot instead verifies
that `/api/show` identifies the exact approved GGUF source blob. Both are
reported as `locally-observed`.
OpenAI-compatible `/v1/models` can prove only that the configured model ID is
present; its configured content digest remains `declared`. A failed or stale
runtime is omitted from capabilities and cannot reach admission or execution.
Preflight responses are capped at 1 MiB and 256 models, and per-adapter
concurrent waiters are capped at 256. Periodic full refresh uses at most 16
workers across at most 64 fixed adapter slots; it does not create an
unbounded watcher per request or retry.

The current runtime configuration schema is version 2; version 1 remains
accepted when no `tls` object is present. The configuration must be a regular,
non-symlink file owned by the worker user with no group or other permissions.
It is capped at 1 MiB and 64 adapters. OpenAI-compatible credentials are
referenced through `apiKeyFile`; that file is subject to the same ownership and
permission checks and is capped at 8 KiB. Credentials must contain only
printable non-space ASCII bytes, with no trailing newline. Inline credentials
are not accepted. See
[docs/runtime-config.example.json](docs/runtime-config.example.json).

Omitted adapter bounds default to 1 MiB input/output, 8 MiB encoded
request/response, eight connections, 16 KiB response headers, a two-minute
request/execution timeout, a five-second connect timeout, 1 GiB RAM, 8,192
context tokens, and batch size one. Configured worker adapters cannot exceed
1 MiB input/output, 8 MiB encoded request/response, 32 connections each or 256
connections in aggregate, 64 KiB headers, one-hour execution, or a one-minute
connect timeout. Admission validation may impose lower observed-capacity
bounds. Invalid or ambiguous configuration fails worker startup.

Remote runtime endpoints require HTTPS. Plaintext endpoints must use a
loopback literal, `localhost`, or a literal address inside one of at most 16
administrator-configured local CIDRs. Every configured CIDR must be wholly
contained in loopback, RFC 1918 private, link-local, or IPv6 ULA space; a wide
prefix that crosses into public address space fails startup. `localhost`
resolution accepts at most 16 addresses, requires every result to be
loopback, binds the configured port, and includes resolution plus connection
attempts in the configured connect timeout. Every runtime transport also pins
dials to the configured host and port; redirects remain disabled. HTTPS uses a
TLS 1.2 minimum and normal hostname verification. Version 2 can replace system
trust with up to 64 private CA certificates and can present one client identity
with a chain of at most eight certificates. The CA and client-certificate files
are each capped at 1 MiB; the client-key file is capped at 256 KiB. These files
use the same private regular-file policy as credentials, reject duplicate or
extraneous PEM material, and must be rotated by restarting the worker. A
restricted DNS `serverName` override supports an IP-pinned endpoint whose
certificate uses an administrator-selected internal name. TLS identity fields
on a plaintext endpoint and partial client identities fail worker startup. The
same TLS policy is used for Ollama activation and request traffic. A runtime
client key authenticates only to the configured model runtime; it is not and
must never be a wallet owner key.

Development mock defaults are one concurrent task, 64 queued tasks, 128
private socket connections, 1 MiB output per request, a 15-minute execution
deadline, zero reserved workers, and capacity derived conservatively from
locally observed RAM/VRAM. Production values come only from the terminal
policy. Hard limits reject more than 128 workers, a worker reserve equal to or
larger than the total pool, 4096 queued tasks, 4096 socket connections, a
one-hour admission deadline, or oversized resource configuration. The owner
resource reserve is removed from external/background capacity before it is
advertised or admitted, and reserved workers never execute external or
background work. The development health defaults are a five-second check
timeout, a two-minute success TTL, a two-second failure TTL, and a five-second
refresh with four workers. Policy validation rejects refresh intervals below
250 milliseconds or above five minutes, more than 16 health workers, or any
timeout/interval combination that could leave a freshness gap before the
success TTL after accounting for every configured adapter batch.
The resource-liveness defaults are a ten-second interval, five-second probe
timeout, and two consecutive observations for both failure and recovery.
Hard limits permit intervals from one second through five minutes, timeouts
from 100 milliseconds through 30 seconds and no longer than the interval, and
thresholds from one through ten.

## Security boundaries

- `tos-edge` in `tos-protocol` remains the future authentication, payment,
  receipt, and settlement control plane.
- The worker has no wallet owner key, public TCP listener, arbitrary model
  upload, arbitrary downloader, shell, host mount, privileged container, or
  raw accelerator API.
- External inference accepts only `EXTERNAL_SERVICE`; approved local callers
  may use `LOCAL_ASYNC`, and maintenance may use `BACKGROUND`. Network work
  cannot claim emergency, control, or real-time priority.
- Owner-reserved workers protect only correctly classified `LOCAL_ASYNC`
  work. The private socket is a trusted local boundary: Edge Core must never
  map an external request to this class, and deployments should isolate the
  worker and Edge Core under a dedicated Unix identity. The worker still
  loads no wallet or owner private key.
- Go scheduling is not a hard real-time or physical-safety loop.
- Quote is an expiring capability observation, not a permanent reservation.
  Invoke repeats local admission before adapter execution.
- Production admission and process-capacity values come only from a private
  startup policy and are checked against effective observed RAM/free VRAM
  before the socket is created. On Linux, host totals are reduced by
  applicable cgroup ancestor hard limits. Task payloads cannot increase or
  replace them.
- Host-class liveness is continuously re-probed in a bounded subprocess. A
  degraded observation gates new work without dynamically shrinking budgets
  underneath existing reservations or preempting in-flight work.
- Runtime errors exposed across RPC are stable categories and do not include
  internal endpoints, paths, or credentials.
- Operational metrics are available only as `GET /metrics` on the same
  mode-0600 Unix socket. The snapshot is capped at 16 KiB, uses a fixed set of
  low-cardinality series, and never uses request, model, endpoint, error, or
  hardware data as labels. Counters live only for the worker process lifetime;
  this endpoint is neither a public listener nor a durable audit journal.
- Cached runtime readiness expires; stale adapters are not advertised or
  admitted until a bounded preflight succeeds again.
- Runtime health checks are tied to the worker lifecycle. Shutdown stops new
  checks, cancels the monitor, drains scheduler work, and waits for in-flight
  checks before closing adapter pools exactly once. A timed-out drain is
  reported as incomplete and does not close an adapter still in use or run
  model cleanup concurrently with it.
- The private socket name has an adjacent current-user mode-0600 advisory lock.
  A second worker cannot unlink or replace a live socket. A pre-lock legacy
  socket is removed only when a bounded local connection probe proves it is
  stale; ambiguous probe errors fail closed. Repeated or concurrent listener
  close calls cannot remove a successor's socket.
- On Linux, model caches and persistent activation-state directories use
  non-blocking advisory `flock` ownership on private, current-user, mode-0600
  regular lock files. Locks survive for the manager/controller lifetime,
  remain held while cleanup is retryable, and are released by the kernel after
  a process crash. Deploy these directories on a local filesystem with
  reliable `flock` semantics; this is not a distributed lock or protection
  against a hostile process running as the same user.
- Optional activation state contains only fixed administrator slot IDs and
  SHA-256 digests in a private local file. It is crash-recovery intent, not a
  remote trust assertion or substitute for signed model verification.
- A signed local cache approval proves only that an administrator-approved
  artifact is available to this worker. Generic OpenAI-compatible runtime
  model IDs remain `declared`; the guard does not claim remote content
  attestation.
- Ollama activation is restricted to administrator-configured GGUF artifacts
  already present in the signed cache. It does not authorize Internet
  downloads, task-selected models, arbitrary Modelfiles, or arbitrary runtime
  creation.

## Not implemented

Controlled activation is currently implemented only for an
administrator-configured Ollama endpoint and a signed-cache GGUF artifact.
There is no activation backend for LocalAI, vLLM, llama.cpp, or vendor
runtimes, no live activation-management RPC, no arbitrary Modelfile support,
and no Internet model pull/download. Separately managed Ollama models remain
supported through bounded inventory preflight. Generic OpenAI-compatible
model-list APIs expose an ID but do not attest their configured content
digest.

This repository also does not yet provide public ingress, TOS payment
authorization, receipts, settlement, ARD publication/Registry, fleet
management, an offline journal, a remote metrics collector/exporter, streaming
RPC, a production containerd backend, physical-I/O control, or audited NVIDIA
runtime packaging. It does not support arbitrary consumer
containers/programs/models, unrestricted fine-tuning, training, token
issuance, or bare GPU rental.

Terminal policy reload and dynamic RAM/VRAM capacity rebalancing are not
implemented. The current monitor verifies that configured RAM backing,
including Linux cgroup hard limits, and any required
GPU/driver/device/total-VRAM class still exist; it intentionally does not
resize admission from current memory consumption, pressure signals,
fluctuating free memory, or coordinate capacity across multiple worker
processes.

Protocol fields needed for richer resource and admission exchange are recorded
in [docs/protocol-interface-notes.md](docs/protocol-interface-notes.md).

No license has been selected for this new repository yet. Add one before the
first public release.
