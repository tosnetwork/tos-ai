# TOS AI

`tos-ai` is the vertical AI Edge Computing Terminal implementation for TOS
Network. It turns owner-operated hardware into bounded AI services while
preserving local resource authority. It is not a validator, bare GPU rental
daemon, public shell, or wallet process.

## Implemented foundation

The Go 1.24 module currently contains:

- `tos-ai-worker`, a private ConnectRPC worker served only on a mode-0600 Unix
  socket, with bounded connections and no public listener;
- `tos-ai-cli`, with `health`, `capabilities`, `quote`, `invoke`, and `cancel`
  diagnostics;
- CPU, RAM, OS, and architecture discovery plus NVIDIA/go-nvml GPU, VRAM,
  driver, CUDA capability, temperature, and power probes;
- graceful `unavailable` or `no-devices` NVIDIA states when NVML or a GPU is
  absent;
- a bounded local `AdmissionController` for concurrency, queue slots, RAM,
  VRAM, KV cache, context, batch, output, execution time, owner reserve, and
  the three accepted inference priorities;
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
  with fixed-size singleflight state, expiring success/failure caches, and
  stable redacted failures;
- strict administrator-owned JSON runtime configuration with bounded defaults,
  duplicate/unknown-field rejection, and private credential-file loading;
- bounded HTTP transports with administrator-selected endpoints, HTTPS for
  remote endpoints, explicit local-CIDR exceptions for plaintext, deadline
  propagation, and stable redacted runtime error categories;
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
  -socket /run/user/$(id -u)/tos-ai/worker.sock

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock health

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock capabilities

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock quote hello

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock invoke hello
```

Without `-runtime-config`, the worker enables only the deterministic
development adapter. Passing `-runtime-config /absolute/private/runtime.json`
replaces the mock with the explicitly configured Ollama and OpenAI-compatible
adapters. An invocation payload can never select or override an endpoint.
Model import similarly accepts a bounded reader and an approved signed
manifest, not an arbitrary Internet URL.

Production adapters may additionally be constrained to the signed local model
cache:

```sh
go run ./cmd/tos-ai-worker \
  -socket /run/user/$(id -u)/tos-ai/worker.sock \
  -runtime-config /etc/tos-ai/runtime.json \
  -model-trust-config /etc/tos-ai/model-trust.json
```

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

The worker preflights configured runtimes at startup, refreshes stale bindings
for capabilities and Quote, and performs an authoritative recheck for every
new Invoke owner. A separately managed Ollama model name and SHA-256 digest
are matched against its bounded `/api/tags` inventory. An activated Ollama
slot instead verifies that `/api/show` identifies the exact approved GGUF
source blob. Both are reported as `locally-observed`.
OpenAI-compatible `/v1/models` can prove only that the configured model ID is
present; its configured content digest remains `declared`. A failed or stale
runtime is omitted from capabilities and cannot reach admission or execution.
Preflight responses are capped at 1 MiB and 256 models, and per-adapter
concurrent waiters are capped at 256.

The runtime configuration must be a regular, non-symlink file owned by the
worker user with no group or other permissions. It is capped at 1 MiB and 64
adapters. OpenAI-compatible credentials are referenced through `apiKeyFile`;
that file is subject to the same ownership and permission checks and is capped
at 8 KiB. Credentials must contain only printable non-space ASCII bytes, with
no trailing newline. Inline credentials are not accepted. See
[docs/runtime-config.example.json](docs/runtime-config.example.json).

Omitted adapter bounds default to 1 MiB input/output, 8 MiB encoded
request/response, eight connections, 16 KiB response headers, a two-minute
request/execution timeout, a five-second connect timeout, 1 GiB RAM, 8,192
context tokens, and batch size one. Configured worker adapters cannot exceed
1 MiB input/output, 8 MiB encoded request/response, 32 connections each or 256
connections in aggregate, 64 KiB headers, one-hour execution, or a one-minute
connect timeout. Admission validation may impose lower observed-capacity
bounds. Invalid or ambiguous configuration fails worker startup.

Default worker bounds are one concurrent task, 64 queued tasks, 128 private
socket connections, 1 MiB output per request, a 15-minute execution deadline,
and capacity derived conservatively from locally observed RAM/VRAM. Hard
limits reject more than 128 workers, 4096 queued tasks, 4096 socket
connections, a one-hour admission deadline, or oversized resource
configuration. The owner reserve is removed from external/background
capacity before it is advertised or admitted. Runtime preflight uses a
five-second check timeout, a 15-second success TTL, and a two-second failure
TTL; it creates no periodic watcher.

## Security boundaries

- `tos-edge` in `tos-protocol` remains the future authentication, payment,
  receipt, and settlement control plane.
- The worker has no wallet owner key, public TCP listener, arbitrary model
  upload, arbitrary downloader, shell, host mount, privileged container, or
  raw accelerator API.
- External inference accepts only `EXTERNAL_SERVICE`; approved local callers
  may use `LOCAL_ASYNC`, and maintenance may use `BACKGROUND`. Network work
  cannot claim emergency, control, or real-time priority.
- Go scheduling is not a hard real-time or physical-safety loop.
- Quote is an expiring capability observation, not a permanent reservation.
  Invoke repeats local admission before adapter execution.
- Runtime errors exposed across RPC are stable categories and do not include
  internal endpoints, paths, or credentials.
- Cached runtime readiness expires; stale adapters are not advertised or
  admitted until a bounded preflight succeeds again.
- Runtime adapter connection pools are closed exactly once during graceful
  shutdown.
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
management, an offline journal, streaming RPC, a production containerd
backend, physical-I/O control, or audited NVIDIA runtime packaging. It does
not support arbitrary consumer containers/programs/models, unrestricted
fine-tuning, training, token issuance, or bare GPU rental.

Protocol fields needed for richer resource and admission exchange are recorded
in [docs/protocol-interface-notes.md](docs/protocol-interface-notes.md).

No license has been selected for this new repository yet. Add one before the
first public release.
