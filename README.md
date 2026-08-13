# TOS AI Execution Foundation

`tos-ai` contains the reusable local execution substrate for the Native ATOS
software-work market. It is not a second registry, an authority over payments,
or a public A2A protocol implementation. Finalized TOS state remains canonical;
this module will execute work bound by a future Accepted Quote and produce
off-chain outputs and evidence for a TOS-committed receipt.

The normative product direction lives in
[`tosnetwork/atos-spec`](https://github.com/tosnetwork/atos-spec).

## Current scope

This cleanup intentionally leaves a library foundation, not a runnable worker
protocol. The retained code provides:

- a fail-closed container execution policy with digest-pinned images,
  non-root execution, read-only roots, no-new-privileges, bounded CPU/RAM/disk,
  PID, time and output limits, and network-none or exact host allowlists;
- a bounded supervisor that rejects duplicate execution identities, contains
  backend panics, propagates cancellation, and synchronously drains on close;
- a containerd backend with private socket/FIFO validation, cgroup v2 metrics,
  OCI hardening and cleanup checks;
- operator-fixed CDI GPU mappings behind exclusive local leases;
- a reusable black-box backend conformance harness and optional live
  containerd/NVIDIA tests;
- privacy-minimized host/GPU probes and a resource-liveness guard;
- private Unix listener, metrics export, signed update/rollback, and bounded
  systemd helper libraries suitable for a future worker process.

The previous text-generation Worker RPC, model adapters, old Edge gateway,
Managed payment path, legacy discovery integration and generalized third-party
execution adapters were removed. They encoded the retired protocol and must not
be treated as compatibility requirements for the new software-work schema.

## Build and test

The module uses Go 1.26.5.

```bash
go test ./...
```

No production command is currently shipped. A new worker entry point should be
added only after `atos-spec` freezes the software-work request, artifact,
execution, escrow and receipt contracts. That worker should adapt those frozen
types to `pkg/executor`; it should not resurrect the deleted inference RPC.

## Repository layout

```text
pkg/executor/                    policy, supervision and backend contract
pkg/executor/containerdbackend/ containerd implementation
pkg/executor/backendtest/       reusable lifecycle conformance suite
executor/gpuisolation/          exclusive operator-named GPU leases
pkg/probe/                      privacy-minimized local resource probes
internal/resourceguard/         continuous fail-closed resource gating
internal/unixserver/            bounded private Unix listeners
pkg/metricsexport/              bounded metrics delivery
pkg/update/                     signed artifact manifests
pkg/softwareupdate/             crash-safe two-slot update state machine
pkg/servicemanager/             fixed-unit bounded systemd operations
```

See [isolated backend conformance](docs/isolated-backend-conformance.md) for
the security and live-test gates. Passing unit or lifecycle tests is not a
hardware-isolation attestation.

Licensed under the [GNU General Public License v3.0](LICENSE).
