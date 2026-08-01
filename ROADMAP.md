# tos-ai Roadmap

Status: non-streaming M1 composition and local integration complete candidate
Last reviewed: 2026-08-01

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

## In Progress

The active milestone is A2, the Tier 1 production candidate:

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
- produce signed reproducible artifacts and an audited rollback procedure.

## Next

1. Add signed production packaging, service management, upgrade/rollback, and
   operator diagnostics for the Tier 1 terminal.
2. Add a reviewed NVIDIA container runtime/device-isolation backend and prove
   its cleanup and exclusivity on supported hardware; do not expose raw device
   access to callers.
3. Add fixed activation backends for selected vLLM, llama.cpp, LocalAI, or
   vendor runtimes with exact artifact/runtime identity and the existing trust
   boundary. No arbitrary Internet model pull is implied.
4. Implement active/known-good software update slots with crash- and power-loss
   recovery, staged rollout, health gates, anti-rollback, and bounded retention.
5. Add authenticated administrator lifecycle controls and carefully bounded
   policy rollout. Dynamic capacity resizing or cross-process coordination must
   retain one authoritative admission state.
6. Consume `tos-protocol` streaming v0.2 only after ordering, backpressure,
   cancellation, resume, usage, total-output, and Receipt semantics are frozen.
7. Add signed benchmark evidence and external evidence issuers without
   treating self-reported TOPS or hardware identity as proof of service quality.
8. Implement the site-bound physical terminal track: real-time local priority,
   bounded offline journal, safe reconnect, signed updates, device/actuator
   isolation, independent safety interlocks, and fleet lifecycle.
9. Add remote metrics collection and durable operational history through a
   separate authenticated, bounded export path.

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
| A3: extended runtimes and streaming | Reviewed GPU/runtime backends and versioned streaming compatibility | Next |
| A4: physical terminal and fleet | Offline, real-time, update, device-safety, reconnect, and fleet acceptance | Next |

## Maintenance

Update this file in the same pull request whenever an implementation changes
category. Automated tests close code items; they do not certify target hardware,
privileged isolation, key custody, public networking, or long-duration memory
behavior. External gate status changes only in the canonical production-gate
ledger.
