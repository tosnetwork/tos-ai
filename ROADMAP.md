# tos-ai Roadmap

Status: private Worker and non-streaming profile foundation implemented  
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

## In Progress

The active milestone is the first deployable non-streaming AI Edge service:

- build `tos-ai-edge`, the public production composition that combines the
  implemented protocol Action server, text-generation profile, private Worker,
  current TOS chain adapters, Quote/Receipt custody, ARD catalog publication,
  and deployment-owned authentication policy;
- define one strict production configuration that cannot partially enable
  payment, signing, profile, readiness, or public ingress dependencies;
- package and exercise one Tier 1 Linux/NVIDIA terminal while keeping model
  runtime endpoints, credentials, hardware identity, and raw errors private;
- complete the local three-node flow from ARD discovery and TOS authority
  resolution through Quote, payment, execution/recovery, and signed Receipt;
- turn failure cases into repeatable acceptance tests: Worker/signer/chain
  outage, cancellation, adapter crash, timeout, restart, disk quota, payment
  reorganization, and route/model drift;
- prepare an immutable compatible release with `tos-protocol` after the
  deployment gates are evidenced.

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

The following require target hardware and deployment evidence:

- **Tier 1 hardware:** supported NVIDIA driver/runtime matrix, cold start,
  thermal behavior, power behavior, model load, sustained inference, and
  owner-priority operation on the selected terminal class.
- **Isolation:** exact kernel, cgroup v2, containerd, runc, seccomp, namespaces,
  filesystem, network-none, and any NVIDIA device configuration; prove zero
  residual runtime objects after success, failure, cancellation, and restart.
- **Model supply chain:** real trust roots and signed artifacts; corruption,
  incompatible runtime, interrupted activation, disk full, rollback,
  anti-rollback, power loss, and offline rehearsal.
- **Live TOS integration:** real controller/client authority, payment finality,
  key rotation/revocation, reorganization, settlement policy, and restart
  reconciliation on the local/test network.
- **Key custody and public perimeter:** production Quote/Receipt custody,
  private socket ownership, TLS ingress, authentication policy, rate limits,
  firewall, redaction, and no public runtime endpoint.
- **Availability and memory:** long-duration anonymous-load and fault-injection
  tests recording RSS, heap, goroutines, file descriptors, bbolt/task-store,
  disk, RAM, VRAM, queues, model cache, and container objects until steady
  state is demonstrated.
- **Offline/physical/fleet claims:** disconnected soak, bounded journal,
  reconnect idempotency, real-time deadline priority, safe update rollout,
  independent actuator safety, delegation/revocation, and bounded fleet fan-out
  before those product classes are advertised.
- **Release operations:** signed reproducible artifacts, rollback procedure,
  upgrade compatibility, independent security review, and testnet observation.

The shared deployment evidence requirements are maintained in
[`tos-protocol/docs/non-streaming-v0.1-production-gates.md`](https://github.com/tosnetwork/tos-protocol/blob/main/docs/non-streaming-v0.1-production-gates.md).

## Release milestones

| Milestone | Exit condition | State |
|---|---|---|
| A0: private terminal foundation | Worker, admission, persistence, runtime adapters, model trust, text profile, isolation foundation, protocol compatibility and race tests | Completed |
| A1: public non-streaming composition | `tos-ai-edge` completes the local discovery-to-receipt flow with complete fail-closed dependencies | In Progress |
| A2: Tier 1 production candidate | Packaging plus required chain, key, isolation, model, memory, and network evidence | Next |
| A3: extended runtimes and streaming | Reviewed GPU/runtime backends and versioned streaming compatibility | Next |
| A4: physical terminal and fleet | Offline, real-time, update, device-safety, reconnect, and fleet acceptance | Next |

## Maintenance

Update this file in the same pull request whenever an implementation changes
category. Automated tests close code items; they do not certify target hardware,
privileged isolation, key custody, public networking, or long-duration memory
behavior.
