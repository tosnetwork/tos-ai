# Isolated backend conformance

Status: reusable black-box lifecycle harness plus fixed-policy CPU and GPU-CDI
SDK driver candidate implemented with explicit production-binary composition.
The full lifecycle and MOCK-CDI device path have passed against real local
containerd/runc; no target NVIDIA or privileged isolation certification is
claimed.

`pkg/executor/backendtest` gives every future isolated-execution backend one
common minimum lifecycle test. A backend test supplies a `Factory`, an
administrative `Inspector`, one deterministic successful fixture, and one
fixture that remains active until cancellation.

The harness opens a fresh backend instance for each case and verifies:

- readiness succeeds without creating runtime objects and rejects an already
  canceled context;
- a successful execution returns the exact expected bounded result;
- caller cancellation terminates the workload within a fixed deadline;
- a second live workload cannot use the same execution digest;
- a bounded group of concurrent executions completes correctly; and
- no active workload, container, task, or snapshot remains after success or
  cancellation.

Factories, readiness, execution, and close panics are converted into test
failures. Test timeouts and concurrency have hard package ceilings. Request
and expected-result resource bounds are validated before any backend opens.
The suite clones mutable request and input data so a test implementation
cannot alter later cases through shared buffers.

## Backend test requirements

Each subtest must receive a private, initially empty runtime namespace. The
inspector must count all objects created in that namespace, including stopped
containers/tasks and retained snapshots, not only running processes. It must
use an independently reviewed administrative adapter rather than the exact
cleanup query used by the implementation under test; otherwise the same bug
can hide its own residue.

The cancellation fixture must signal activity through the inspector and then
remain alive until its context is canceled. A backend that detaches work from
the request context, returns before cleanup, or requires an unbounded polling
loop must fail the suite. `Close` must synchronously release test-owned
runtime resources.

Every fixture carries a precomputed lowercase `sha256:` execution digest. The
backend must use it as its sole runtime-object identity and reject a duplicate
while the first workload is live. It must not accept a raw Worker task ID,
derive object names from payload bytes, replace an existing object, or delete
unknown residue to make a collision disappear.

Typical use from a backend's external or privileged test package is:

```go
func TestContainerdBackendConformance(t *testing.T) {
    backendtest.Run(t, backendtest.Suite{
        New: newTestNamespaceBackend,
        SuccessRequest: successRequest,
        SuccessInput: []byte("success"),
        ExpectedOutput: []byte("ok"),
        CancellationRequest: cancellationRequest,
        CancellationInput: []byte("wait-for-cancel"),
        StartTimeout: 5 * time.Second,
        ReturnTimeout: 10 * time.Second,
        InspectTimeout: 5 * time.Second,
        Concurrency: 8,
    })
}
```

## What passing does not prove

This harness tests lifecycle behavior, bounded results, cancellation, and
observable cleanup. It cannot prove Linux namespace, cgroup, seccomp, LSM,
device, filesystem, or network isolation. A production backend still needs
privileged platform tests that independently verify at least:

- immutable digest-pinned, locally approved images and no runtime-socket or
  host-path exposure;
- non-root identity, read-only root filesystem, no-new-privileges, capability
  removal, seccomp/LSM policy, and PID limits;
- CPU, RAM, writable-disk, execution-time, and output enforcement under
  adversarial workloads;
- exact GPU device-count assignment and denial of unassigned devices;
- network-none behavior and allowlist enforcement resistant to DNS rebinding;
- cleanup after process kill, runtime restart, disk exhaustion, and host
  reboot; and
- bounded logs, metadata, runtime objects, and disk use during repeated
  failure and cancellation storms.

Passing this suite is therefore a required launch gate for a backend, not an
isolation attestation. The production Worker remains fail closed until an
audited backend is explicitly compiled in through `IsolatedBackendFactory`.
Factory results are wrapped by the fixed-capacity `executor.SupervisedBackend`
before use; conformance tests for a driver must still exercise the driver
directly so the wrapper cannot hide driver cleanup or collision defects.

`pkg/executor/containerdbackend` currently passes its non-privileged lifecycle,
configuration, bounded-output and generated-OCI-spec tests with a fake engine.
Those tests verify fixed-capacity cancellation/close behavior, panic
containment, digest-only identity, private socket/FIFO validation, capability
removal, non-root/read-only/no-new-privileges policy, non-empty default seccomp
generation, private networking, resource fields, exact operator alias-to-CDI
translation, duplicate rejection and exclusive device release. A CDI-library
test loads an isolated temporary specification and verifies its environment
and device edits. These tests do not prove that the host runtime enforces
seccomp and do not replace the private-daemon privileged suite above.

The SDK driver accepts only cgroup v2 task metrics. CPU and memory records are
mandatory; IO records have a 4,096-device ceiling and checked write-byte
addition. Unknown metric types, missing records, malformed payloads and
overflow fail closed. Privileged fixtures must additionally show that the
daemon reports final metrics after process exit and before task deletion, and
that the observed values cannot exceed policy without rejecting the result.

`tos-ai-worker -isolated-runtime-config <private-file>` is the only production
binary entry point for this driver. It is mutually exclusive with the HTTP
runtime, HTTP model-trust and development-mock modes. The binary opens and
validates the private socket, namespace, image binding and residue state before
starting its Worker listener, and closes the owned backend synchronously during
shutdown. Operators must not describe this composition as certified until the
privileged suite in this document passes on every supported deployment image.

## Live lifecycle suite

`TestContainerdBackendLiveConformance` connects the SDK driver to a real
private daemon and runs the reusable lifecycle harness. It is skipped unless
all five variables below are set; a partial environment fails instead of
silently skipping:

```text
TOS_AI_CONTAINERD_TEST_SOCKET
TOS_AI_CONTAINERD_TEST_NAMESPACE
TOS_AI_CONTAINERD_TEST_FIFO_DIR
TOS_AI_CONTAINERD_TEST_IMAGE_REFERENCE
TOS_AI_CONTAINERD_TEST_IMAGE_DIGEST
```

The socket and its parent must satisfy the production owner/mode checks. The
namespace must already contain the exact digest-qualified image record and the
image must be unpacked for `overlayfs`. The fixture image must provide
`/bin/sh`, `read`, `printf`, and `sleep` to an unprivileged numeric user with a
read-only root filesystem. The namespace must contain no managed 64-hex
container or snapshot identity before the run. The suite neither pulls an image
nor deletes uncertain pre-existing residue.

Run it explicitly with:

```sh
go test -race -count=1 \
  ./pkg/executor/containerdbackend \
  -run TestContainerdBackendLiveConformance -v
```

This closes the live lifecycle gate only. Independent privileged tests must
still verify namespace/cgroup/seccomp/LSM enforcement and adversarial resource
limits from outside the workload.

## GPU CDI execution and certification

GPU configuration is local operator authority, not task authority. Each
`backend.gpuDevices` entry binds a privacy-safe alias to one qualified CDI
device name. The immutable container policy fixes the required device count;
the Worker request contains neither alias nor CDI identity. The production
factory sorts the configured aliases, acquires an exclusive in-process lease,
translates only the leased aliases through the fixed map, refreshes the
operator-selected CDI registry without a background watcher, and injects the
result after the base isolation specification. Unknown aliases, duplicate
physical CDI names, capacity exhaustion, missing CDI specifications and
inconsistent GPU/VRAM policy fail closed.

`TestContainerdBackendLiveMockCDIConformance` exercises this path through real
containerd/runc with an isolated test CDI specification. The fixture adds one
environment marker and one harmless character device, then verifies the
workload sees both and that the lease is synchronously released. Set the five
base live-suite variables plus:

```text
TOS_AI_CONTAINERD_TEST_CDI_SPEC_DIR
```

The current host passed this test and the complete lifecycle suite on
2026-08-02 using an owner-private proxy socket and an isolated containerd
namespace. No managed container, task or snapshot remained. This is real OCI
device-injection and cleanup evidence, but the injected device is deliberately
a MOCK fixture; it is not NVIDIA performance or hardware-isolation evidence.

Target NVIDIA certification uses the same production path and the exact test
`TestContainerdBackendLiveNVIDIAConformance`. It additionally requires one
operator-selected qualified NVIDIA CDI device:

```text
TOS_AI_CONTAINERD_TEST_NVIDIA_CDI_DEVICE=nvidia.com/gpu=0
```

The digest-pinned certification image must contain `/bin/sh` and `nvidia-smi`
and the CDI device must expose exactly one GPU inside the container. Run:

```sh
make nvidia-certification
```

The script fails before testing when any environment value, `nvidia-smi`, or a
host-visible GPU is missing. A pass proves that this image and configured CDI
identity expose exactly one device and that execution/lease cleanup succeeds;
it does not by itself certify thermals, sustained inference, cross-process
ownership, kernel isolation, or physical power-loss recovery. The CPU-only
example remains `isolated-runtime-config.example.json`; the explicit GPU form
is `isolated-gpu-runtime-config.example.json`.
