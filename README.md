# TOS AI Execution Foundation

`tos-ai` contains the reusable local execution substrate for the Native TOS Service Protocol
software-work market. It is not a second registry, an authority over payments,
or a public network authority. Finalized TOS state remains canonical; this module
executes work bound by an Accepted Quote and produces off-chain outputs and
evidence for a TOS-committed Receipt.

The normative product direction lives in
[`tosnetwork/tos-service-spec`](https://github.com/tosnetwork/tos-service-spec).

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
- a local commerce Gate and private ingress store that admit an execution or
  upload only under an ML-DSA-44 Action Authority Writer Fence and
  AuthorizedActionV1 resolved by key ID;
- privacy-minimized host/GPU probes and a resource-liveness guard;
- metrics export, signed update/rollback, and bounded systemd helper libraries
  used by operator-composed worker processes;
- a dedicated private-containerd provider deployment template with explicit
  root-runtime and separate-signing-custody boundaries.

The previous text-generation Worker RPC, model adapters, old Edge gateway,
Managed payment path, legacy discovery integration and generalized third-party
execution adapters were removed. They encoded the retired protocol and must not
be treated as compatibility requirements for the new software-work schema.
The escrow-gated A2A, MCP and Agent Packet adapters, their public HTTP
boundary and the Messenger event worker were removed with stablecoin escrow:
software work settles on the Service Actor rail. `pkg/railgate` is the
replacement gate: it admits a finalized pending Service Actor request at most
once. `software-work-execute paid` admits through it before executing; the
command's default mode is for work whose admission its caller already proved.

## Build and test

The module uses Go 1.26.11.

```bash
go test ./...
```

The provider-local `software-work-execute` command composes the frozen V1 job
with the bounded executor. The execution contract is frozen in
`tos-service-spec/docs/SOFTWARE_WORK_EXECUTION_V1.md`; settlement follows
`tos-service-spec/docs/SERVICE_ACTOR_TVM_V1.md`. It does not resurrect the
deleted inference RPC. Its `paid` subcommand is the standalone paid path: it
admits a finalized pending Service Actor request through `pkg/railgate`
before executing it at most once. The default mode runs a job whose admission
its caller already proved, and `artifacts-gc` deletes artifacts whose
retention ended. See [the provider runbook](deploy/provider/README.md).

Tests that check real ML-DSA-44 bytes run the pinned `tosctl` helper named by
`TOS_SERVICE_TOSCTL` and `TOS_SERVICE_TOSCTL_SHA256`; they fail, rather than
skip, without it.

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
pkg/railgate/                   finalized pending-request admission for paid Service Actor work
pkg/artifactstore/              immutable content-addressed output storage with bounded retention
pkg/commercegate/               Action Authority gated local execution admission
pkg/privateingress/             Action Authority gated private content ingress
pkg/probe/                      privacy-minimized local resource probes
internal/resourceguard/         continuous fail-closed resource gating
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
