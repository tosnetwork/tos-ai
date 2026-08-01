# Isolated backend conformance

Status: reusable black-box lifecycle harness implemented; no production
containerd backend or privileged isolation certification is claimed.

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
