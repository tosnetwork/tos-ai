# tos-ai Roadmap

Status: non-streaming M1 composition and local integration complete candidate
Last reviewed: 2026-08-02

This is the repository-level delivery roadmap for the TOS AI Edge Computing
Terminal. The cross-repository program view lives in
[`tos/doc/tos-network-roadmap.md`](https://github.com/tosnetwork/tos/blob/main/doc/tos-network-roadmap.md),
and the generic protocol roadmap lives in
[`tos-protocol/ROADMAP.md`](https://github.com/tosnetwork/tos-protocol/blob/main/ROADMAP.md).

The product contract is a bounded AI service, not bare GPU rental, arbitrary
consumer execution, a public shell, validator work, or token issuance.

## Completed

- Private ConnectRPC Worker on an exclusively owned, bounded mode-0600 Unix
  socket plus a diagnostic CLI; no runtime or wallet is exposed publicly.
- Structured readiness, privacy-minimized capability/resource claims, fixed
  message limits, bounded control deadlines, and low-cardinality private
  operational metrics.
- CPU, RAM, cgroup v1/v2, NVIDIA GPU/VRAM, driver, temperature, and power probes
  with evidence levels, redaction, fake backends, and timeout-isolated liveness
  monitoring.
- Immutable terminal policy, priority scheduling, owner-reserved execution
  workers and resources, bounded queue/concurrency/RAM/VRAM/KV/context/batch/
  output/time admission, and fail-closed resource rechecks.
- Idempotent Quote plus bounded bbolt task storage with exact task ID/request
  digest/retention binding, atomic claims, retained terminal results, exact
  Cancel, read-only `GetTask`, startup reconciliation, count and byte quotas,
  owner reserves, and restart recovery.
- Deterministic mock, Ollama, and generic OpenAI-compatible runtime adapters
  with bounded HTTP transport, private CA/mTLS support, redacted failure
  categories, preflight, cancellation, timeout, and cleanup.
- Signed SHA-256 model/update verification, bounded cache, exclusive ownership,
  model leases, activation state, known-good rollback, anti-rollback, crash
  residue cleanup, and optional Ollama GGUF activation.
- Container isolation contract, immutable policy adapter, fixed task identity,
  duplicate-safe workload supervisor, reusable lifecycle conformance harness,
  and an opt-in CPU-only preloaded-image containerd driver with `network=none`.
- Immutable `tos.ai.text-generation` v0.1 mapper with canonical intent bytes,
  fixed vectors, exact service/model routing, and no request-selected runtime
  endpoint or resource policy.
- `pkg/edgeintegration` deployment bridge deriving an exact public profile plan
  from a fresh validated Worker capability snapshot and rejecting subsequent
  service/model/digest/runtime route drift.
- Real Worker compatibility tests through the pinned `tos-protocol`
  `WorkerClient`, including Health, capabilities, resource Quote, Invoke,
  retained GetTask, Cancel, ARD search, and profile-plan readiness.
- Independent full race tests, static analysis, repeated concurrency tests, CI,
  exact `tos-protocol` pinning, and GPL-3.0 licensing.
- Strict `tos-ai-edge` production composition and mode-`0600` configuration:
  descriptor/ARD publication, current TOS authority/client/payment adapters,
  private Worker and Receipt signer, bounded HTTP server, startup preflight,
  graceful shutdown and server-side redacted diagnostics.
- Real local three-node discovery-to-Receipt flow using service and client
  Agent Accounts and an exact finalized native payment, including same-process
  exact replay and byte-identical replay after both Worker and Edge restart.
- Durable Worker completion timestamps shared by Invoke and GetTask, closing a
  live replay conflict found by the deployment rehearsal.
- Production config/systemd templates plus local one-node quorum tolerance,
  two-node fail-closed startup, signer/Worker outage readiness and bounded
  anonymous malformed-input evidence.
- Independent-module, non-cached race/static gates; byte-identical command
  builds; mock NVIDIA telemetry and GPU admission degradation/recovery; signer
  rotation coverage; and concurrent TLS malformed-input rejection without
  durable-store growth.
- Deterministic complete release bundles with internal and external SHA-256
  manifests, detached Ed25519 verification, archive safety checks, service and
  configuration material, and byte-identical build gates in CI.
- Crash-safe two-slot software updates with signed artifact staging, an exact
  candidate-boot health window, automatic known-good rollback after an
  unconfirmed boot, monotonic security revisions, and bounded residue cleanup.
- Terminal-bound signed administrator activation/confirmation/rollback
  commands with durable exact replay, conflict and uncertain-outcome handling,
  exclusive bounded storage, and a privacy-minimized bounded history view.
- The CPU-only containerd lifecycle conformance suite has run successfully on
  this host against real containerd/runc, including success, cancellation,
  duplicate identity, concurrency, and synchronous object cleanup. This is
  local runtime evidence, not target-kernel or NVIDIA certification.
- WorkerStreamService v0.2 result streaming over the private Unix socket, with
  durable execute-once semantics, bounded chunks, backpressure, exact retained
  resume and cross-repository client validation.
- Bounded terminal-side fleet control with signed terminal/fleet-scoped
  commands, monotonic generations, exact replay, capped persistent offline
  queue and history, pre-execution durable claims, fail-closed `uncertain`
  restart recovery, real-time-work priority, reconnect drain, deterministic
  canary rings and signed rollback. The Agent itself contains executor panic
  and cancellation-late success. MOCK crash windows and executors cover failure
  injection without requiring a physical GPU or terminal fleet.
- Fixed operator-owned vLLM, llama.cpp, and LocalAI identities over the bounded
  OpenAI-compatible adapter, with MOCK server coverage and no request-selected
  endpoint.
- Separately authenticated bounded fleet HTTP transport, signed policy
  apply/rollback commands using the existing generation/replay/canary state
  machine, and a fixed-destination privacy-filtered metrics exporter. These are
  transport libraries; target TLS policy and physical interlocks remain
  deployment evidence.
- A fixed-action fleet execution bridge routes validated commands only to
  release, policy and availability controllers; it never accepts paths, URLs,
  unit names, shell text or runtime endpoints. MOCK failures and panics fail
  closed.
- An exclusive GPU alias lease layer for reviewed container backends, with
  capacity rejection and synchronous release after success, error,
  cancellation or panic. MOCK concurrency proves that one device is never
  shared; actual NVIDIA OCI injection remains a target certification item.
- Privacy-minimized signed benchmark evidence with deterministic MOCK runner
  fault injection, plus a bounded authenticated fleet metrics collector that
  retains only bearer-token digests and one fixed-size snapshot per configured
  terminal alias, rejects excess concurrency before reading another body, and
  expires data without a background queue.
- A fixed-unit, no-shell systemd service-manager adapter with bounded
  restart/reload/readiness operations and MOCK timeout/panic/injection tests.
  Selecting the production unit and privilege boundary remains deployment
  policy.
- Unified construction-time typed-nil rejection across runtime HTTP,
  containerd, GPU isolation, model activation/approval, benchmark, fleet,
  administrator, service-manager, Worker lifecycle and AI Edge dependency
  injection. Runtime capability/close, resource-provider shutdown and NVIDIA
  SDK panics are converted into bounded fail-closed errors or a
  privacy-minimized degraded observation, and strict fleet transport parsing
  has a persistent fuzz regression target.

## In Progress

The active milestone is A2, the Tier 1 production candidate. All identified
locally executable engineering and MOCK sub-gates are complete; the remaining
work is target-deployment evidence and operator policy:

- keep the exact immutable protocol pin, run independent CI for the compatible
  repository pair, and prepare release tagging;
- install the supplied service/config templates on one selected Linux/NVIDIA
  terminal without exposing runtime endpoints, credentials, hardware identity
  or raw errors;
- select the deployment-owned authentication/read policy and production
  session/Quote/Receipt custody;
- complete GPU/container isolation, model supply-chain, long-duration memory,
  public perimeter, controller/key rotation, revocation and settlement
  evidence on the actual target deployment;
- execute the offline release-key ceremony, sign the reproducible candidate,
  and rehearse the documented rollback procedure on the target terminal.

## Next

1. Bind the fixed fleet action interfaces to the selected policy loader and
   systemd unit, and audit the selected operator TLS identity. The local signed
   command, fixed-action, systemd, retry/replay and MOCK failure boundaries are
   complete; unit privileges and atomic policy reload semantics are
   installation policy.
2. Bind the exclusive GPU lease client to the selected NVIDIA OCI backend and
   certify exact device injection and cleanup on supported hardware. The local
   concurrency/capacity/panic contract is complete and exposes no raw device
   selector to callers.
3. Add vendor-specific artifact activation only when that runtime exposes a
   reviewed atomic activation/rollback API. Fixed vLLM, llama.cpp and LocalAI
   execution identities are complete; no arbitrary Internet model pull is
   implied.
4. Provision benchmark issuer trust and collect evidence on target hardware;
   deterministic signed evidence generation is complete, but MOCK results are
   never advertised as measured performance.
5. Integrate the completed fleet agent/control library and bounded reference
   operator transport with the selected TLS identity and physical safety controller. Actual
   actuator interlocks remain outside this Go process and require target-site
   evidence.
6. Deploy the completed fixed-destination exporter and bounded reference
   collector with operator-selected retention and monitoring infrastructure.

## External Certification

External certification covers the selected Tier 1 hardware, GPU/container
isolation, model/update supply chain, live TOS authority and settlement, key
custody, public perimeter, long-duration availability and memory, and release
operations. Offline physical-control and fleet claims remain deferred until
their later milestone and must not be inferred from A1.

The only mutable gate status, required evidence, evidence links and
last-verification dates are maintained in
[`tos-protocol/docs/non-streaming-v0.1-production-gates.md`](https://github.com/tosnetwork/tos-protocol/blob/main/docs/non-streaming-v0.1-production-gates.md).
This ROADMAP intentionally does not duplicate that ledger.

## Release milestones

| Milestone | Exit condition | State |
|---|---|---|
| A0: private terminal foundation | Worker, admission, persistence, runtime adapters, model trust, text profile, isolation foundation, protocol compatibility and race tests | Completed |
| A1: public non-streaming composition | `tos-ai-edge` completes the local discovery-to-receipt flow with complete fail-closed dependencies | Completed |
| A2: Tier 1 production candidate | Packaging plus required chain, key, isolation, model, memory, and network evidence | In Progress |
| A3: extended runtimes and streaming | Reviewed GPU/runtime backends and versioned streaming compatibility | Streaming and GPU lease MOCK gates complete; NVIDIA OCI certification remains |
| A4: physical terminal and fleet | Offline, real-time, update, device-safety, reconnect, and fleet acceptance | Fleet-control local gates complete; physical certification remains |

## Maintenance

Update this file in the same pull request whenever an implementation changes
category. Automated tests close code items; they do not certify target hardware,
privileged isolation, key custody, public networking, or long-duration memory
behavior. External gate status changes only in the canonical production-gate
ledger.
