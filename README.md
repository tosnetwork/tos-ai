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
  restart-time integrity recovery, and crash-residue cleanup;
- deterministic mock, Ollama, and generic OpenAI-compatible HTTP adapters;
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
capacity before it is advertised or admitted.

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
- Runtime adapter connection pools are closed exactly once during graceful
  shutdown.

## Not implemented

This repository does not yet provide automatic activation of imported model
files in Ollama, LocalAI, or vLLM. An operator must ensure that each configured
model name is bound to the declared digest in the separately managed runtime;
the generic HTTP APIs do not attest that binding.

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
