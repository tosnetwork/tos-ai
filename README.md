# TOS AI Execution Foundation

`tos-ai` contains the reusable local execution substrate for the Native ATOS
software-work market. It is not a second registry, an authority over payments,
or a public A2A server. Finalized TOS state remains canonical; this module
executes work bound by an Accepted Quote and produces off-chain outputs and
evidence for a TOS-committed Receipt.

The normative product direction lives in
[`tosnetwork/atos-spec`](https://github.com/tosnetwork/atos-spec).

## Current scope

This repository intentionally exposes a library foundation, not a public
worker protocol. The retained code provides:

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
- a source-archive workspace boundary that rejects links and traversal, mounts
  source read-only, and runs the manifest's exact working directory;
- an at-most-once software-work runner with a crash-safe execution journal;
- bounded, tamper-detecting content-addressed report and artifact storage;
- an official A2A 1.0 Task/result mapper that requires a finalized ATOS
  authorization adapter before execution and never treats A2A metadata as
  payment authority;
- an official MCP 2026-07-28 typed tool mapper with the same finalized,
  atomic single-execution claim boundary;
- privacy-minimized host/GPU probes and a resource-liveness guard;
- private Unix listener, metrics export, signed update/rollback, and bounded
  systemd helper libraries suitable for a future worker process.
- a dedicated private-containerd provider deployment template with explicit
  root-runtime and separate-signing-custody boundaries.

The previous text-generation Worker RPC, model adapters, old Edge gateway,
Managed payment path, legacy discovery integration and generalized third-party
execution adapters were removed. They encoded the retired protocol and must not
be treated as compatibility requirements for the new software-work schema.

## Build and test

The module uses Go 1.26.5.

```bash
go test ./...
```

The provider-local `software-work-execute` command composes the frozen V1 job
with the bounded executor; no public worker server is currently shipped. The
execution contract and A2A mapping are frozen in
`atos-spec/docs/SOFTWARE_WORK_EXECUTION_V1.md` and
`atos-spec/docs/A2A_ADAPTER_V1.md`. `pkg/a2aadapter` uses the official A2A Go
types, but a production entry point still requires the finalized-chain
authorizer and reviewed server binding. It must not resurrect the deleted
inference RPC.

The provider-local command is a privileged runtime boundary when it connects
to root-owned containerd. Do not grant a gateway user, buyer, or remote client
access to that raw socket: containerd access is equivalent to host-root
authority. The command therefore requires its source archive, source parent,
and pre-created state root to be private and owned by the executor identity,
and it refuses symlinks, non-canonical paths, broad permissions, and oversized
source files. The containerd socket and FIFO tree remain owner-private. Receipt
signing stays in the separate `tosctl` custody boundary; no mnemonic or private
key belongs in executor state.

## Repository layout

```text
pkg/executor/                    policy, supervision and backend contract
pkg/executor/containerdbackend/ containerd implementation
pkg/executor/backendtest/       reusable lifecycle conformance suite
executor/gpuisolation/          exclusive operator-named GPU leases
pkg/softwarework/               bound jobs and at-most-once outcome journal
pkg/artifactstore/              immutable content-addressed output storage
pkg/a2aadapter/                 authority-gated A2A Task/result mapping
pkg/mcpadapter/                 purchase-bound MCP tool mapping
pkg/probe/                      privacy-minimized local resource probes
internal/resourceguard/         continuous fail-closed resource gating
internal/unixserver/            bounded private Unix listeners
pkg/metricsexport/              bounded metrics delivery
pkg/update/                     signed artifact manifests
pkg/softwareupdate/             crash-safe two-slot update state machine
pkg/servicemanager/             fixed-unit bounded systemd operations
deploy/provider/                private containerd provider template and runbook
```

See [isolated backend conformance](docs/isolated-backend-conformance.md) for
the security and live-test gates. Passing unit or lifecycle tests is not a
hardware-isolation attestation.

Licensed under the [GNU General Public License v3.0](LICENSE).
