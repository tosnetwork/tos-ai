# Provider deployment template

This template installs one dedicated, root-owned containerd boundary for the
fixed Native software-work executor. It is deliberately not a gateway service
and exposes no public port. Finalized TOS state, not this daemon, remains the
authority for Capability and payment state.

The systemd unit hides the distribution-level `/etc/containerd/conf.d` import
directory with an empty read-only mount before this daemon starts. The supplied
configuration also disables CRI, NRI, and restart-management plugins so
Kubernetes configuration, runtime mutation hooks, or host restart policy
cannot silently widen the fixed executor surface. Startup requires the content,
metadata, overlayfs, and runc task plugins used by the reviewed backend.

## Host prerequisites

- Linux with cgroup v2, containerd 2.x, runc v2, and overlayfs;
- the reviewed `software-work-execute` binary built from the pinned release;
- the exact digest-qualified OCI image committed by the Capability manifest;
- a separate unprivileged `tosctl` vault for the execution signer; and
- an operator with root access for installation and each privileged execution.

Do not reuse `/run/containerd/containerd.sock`, Docker's daemon, or a
Kubernetes containerd. Do not grant a gateway account access to the private
socket and do not add a general `sudo` rule for `software-work-execute`. Raw
containerd access is equivalent to host-root authority.

## Install the private daemon

Review both template files, then install them as root:

```bash
install -d -o root -g root -m 0755 /etc/tos-service
install -o root -g root -m 0600 deploy/provider/containerd.toml \
  /etc/tos-service/containerd.toml
install -o root -g root -m 0644 deploy/provider/tos-service-containerd.service \
  /etc/systemd/system/tos-service-containerd.service
systemctl daemon-reload
systemctl enable --now tos-service-containerd.service
```

Before proceeding, require all of these checks to pass:

```bash
systemctl is-active --quiet tos-service-containerd.service
test "$(stat -c '%U:%G:%a' /run/tos-service-containerd)" = root:root:700
test "$(stat -c '%U:%G:%a' /run/tos-service-containerd/containerd.sock)" = root:root:600
ctr --address /run/tos-service-containerd/containerd.sock plugins ls
```

The unit tightens the socket to `0600` after the daemon reports readiness;
containerd itself creates it `0660`, which the executor refuses. It runs with
`UMask=0022` because the overlayfs snapshotter creates each layer's root
directory itself: under a stricter umask those become `0700` and the
unprivileged sandbox user cannot traverse `/`. The daemon's root and state
directories remain `0700`, so this widens nothing on the host.

Build the reproducible OCI archive with
`build/software-work-go-1.26.5/build.sh` (it verifies the toolchain ZIP and the
resulting index digest). Import it into namespace `tos-service-paid-work`,
unpacked for `overlayfs`. The executor looks the image up by its exact
digest-qualified reference, so that reference must exist as an image name.
Never use a mutable tag without the manifest's exact `sha256:` digest.

```bash
socket=/run/tos-service-containerd/containerd.sock
name=docker.io/tosnetwork/software-work-go:1.26.5
digest=sha256:9624bca74096f810c5b24e489521dde124fadcfa1808581648b38bdc1ba1b105
test "sha256:$(tar -xOf software-work-go-1.26.5.oci.tar index.json | sha256sum | cut -d' ' -f1)" = "$digest"
ctr --address "$socket" -n tos-service-paid-work images import \
  --index-name "$name" --snapshotter overlayfs software-work-go-1.26.5.oci.tar
ctr --address "$socket" -n tos-service-paid-work images tag "$name" "$name@$digest"
ctr --address "$socket" -n tos-service-paid-work images check
```

Then run the live backend conformance suite against this daemon. Build the
test binary as an unprivileged user and run only the binary as root:

```bash
GOWORK=off go test -c -o containerdbackend.test ./pkg/executor/containerdbackend
install -d -o root -g root -m 0700 /run/tos-service-containerd/test-fifos
env TOS_AI_CONTAINERD_TEST_SOCKET="$socket" \
  TOS_AI_CONTAINERD_TEST_NAMESPACE=tos-service-paid-work \
  TOS_AI_CONTAINERD_TEST_FIFO_DIR=/run/tos-service-containerd/test-fifos \
  TOS_AI_CONTAINERD_TEST_IMAGE_REFERENCE="$name@$digest" \
  TOS_AI_CONTAINERD_TEST_IMAGE_DIGEST="$digest" \
  ./containerdbackend.test -test.run 'TestContainerdBackendLive(Conformance|Workspace)$' -test.v
```

Both tests must pass before the host serves paid work.

## Paid path: admit, then execute

`software-work-execute paid` is the only standalone way to execute paid work.
It refuses to run anything until finalized TOS state proves the request:

1. every flag and the rail config are validated, the Quote package is checked
   against the configured provider offer and `--actor`, and the containerd
   executor, journal, artifact store and slot directory are opened. The
   manifest must state a non-zero `retention_seconds`. Any missing or
   malformed piece refuses the run before admission;
2. `railgate` `Admit` verifies the call, the pending request, the Quote and
   input binding and the deadline slack at a fresh finalized checkpoint and
   creates an at-most-once slot keyed by `(quote_commitment, actor,
   request_id)`;
3. the verified request is checked against this executor (actor, request,
   manifest, toolchain, a positive price, a representable retention
   deadline); a refusal abandons the slot before anything runs;
4. `Run` re-verifies at another fresh checkpoint immediately before
   execution and durably marks the slot started;
5. the run's artifact and report are retained first, until the response
   deadline plus the manifest's `retention_seconds`; a successful run's
   objects are then re-read and bound into the `SoftwareWorkReceiptV1` whose
   cell hash is recorded with `Complete`; any other outcome is recorded with
   `Fail` and is never attested;
6. a final fresh `Recheck` reports whether respond may be sent now.

The paid state directory holds the slots, the resolver checkpoints, the job
journal and the artifacts. It must be **persistent and shared by every paid
run of the provider offer**: deleting or splitting it would forget which
requests were already admitted. One process owns it at a time; a concurrent
run fails to open it rather than share it.

**Run exactly one provider runtime per Service Actor and offer.** The
at-most-once guarantee holds only within one slot directory. Two paid state
directories for the same offer, or `software-work-execute paid` next to
another provider runtime that keeps its own slots for the same actor (for
example an agent runtime with its own state directory), can each admit and
execute the same request. Pick one runtime for an actor and route every
paid request for it through that runtime's single slot directory.

The rail config is a private JSON file owned by the executor identity:

```json
{
  "schema": "tos.ai.railgate-config.v1",
  "network": {
    "network_id": "tos-testnet",
    "genesis_root_hash": "sha256:...",
    "genesis_file_hash": "sha256:..."
  },
  "rpc_endpoints": ["https://rpc-a:443", "https://rpc-b:443", "https://rpc-c:443"],
  "quorum": 0,
  "registry_code_boc_file": "/etc/tos-service/registry-code.boc",
  "registry_code_hash": "tvm-cell-sha256:...",
  "actor_code_hash": "tvm-cell-sha256:...",
  "provider_agent_id": "agent_...",
  "capability_id": "cap_...",
  "version": "1.0.0",
  "manifest_file": "/etc/tos-service/manifest.cbor",
  "respond_budget_seconds": 120
}
```

`quorum: 0` means a strict majority of the endpoints. Agreement among
endpoints one operator runs is not independent finality; choosing
independent operators is a configuration responsibility. The manifest file
must be the canonical CBOR the Capability version publishes, and its
`toolchain_digest` must be the executor's pinned image. The sandbox runs
under exactly the manifest's `limits`: `cpu_millis`, `memory_bytes`,
`scratch_bytes` (the workspace disk bound), `output_bytes` and
`wall_clock_millis`, which is also the execution budget reserved before the
response deadline. Each must be non-zero and no larger than the frozen
preset (120 000 CPU ms, 1 GiB memory, 2 GiB disk, 16 MiB output, 180 s);
a manifest outside those bounds is refused before admission. The default
mode takes no manifest and runs under the frozen preset (with an optional
tighter `--output-bytes`).

```bash
/usr/local/libexec/tos-service/software-work-execute paid \
  --rail-config /etc/tos-service/rail.json \
  --containerd-socket /run/tos-service-containerd/containerd.sock \
  --fifo-dir /run/tos-service-containerd/paid-fifos \
  --state-dir /var/lib/tos-service-provider/paid \
  --source /var/lib/tos-service-provider/inbox/REQUEST/source.tar \
  --quote-package /var/lib/tos-service-provider/inbox/REQUEST/quote.pb \
  --buyer-wallet 0:BUYER_WALLET_HEX \
  --actor 0:SERVICE_ACTOR_HEX \
  --request-id REQUEST_ID \
  --call-lt CALL_LT --call-hash CALL_TX_HASH_HEX \
  --execution-id sha256:EXECUTION_ID --input-digest sha256:INPUT_DIGEST
```

The command prints one JSON object (`status`, `exit_code`, `actor`,
`request_id`, `quote_commitment`, `slot_state`, and on completion
`response_hash`, `receipt_boc_base64`, `retain_until_unix`, `respond_check`
and the `outcome`; `error` explains every other status) and exits with:

| Exit | `status` | Meaning and operator action |
|---|---|---|
| 0 | `completed` | Admitted, executed once, Receipt hash recorded. Sign respond through `tosctl` only while `respond_check` is `passed`; otherwise rerun the check before the response deadline. |
| 0 | (none) | `-h`: usage printed, nothing run. |
| 1 | `refused` | Invalid flags or config, or a local dependency (executor, journal, store) cannot be opened. Nothing was admitted or run; fix and retry. |
| 1 | `unavailable` | No finalized checkpoint could be opened (resolver or quorum outage), or the slot store failed before a slot existed. Nothing was admitted or run; retry. |
| 2 | `usage` | The command line does not parse. |
| 3 | `already_taken` | The request already has a slot (admitted, completed, failed or abandoned). It is not executed again. |
| 4 | `not_pending_now` | Admitted, but the pre-execution check found the request no longer pending with room to execute and respond. The slot is abandoned; nothing ran. |
| 5 | `ambiguous` | A run may have started without a recorded outcome (a crash, or a result that could not be recorded), or the slot store failed in a way that left a slot behind. Never rerun or respond; the buyer's refund path applies. |
| 6 | `not_admitted` | Finalized state at an open checkpoint does not prove the claim (unknown call, other terms, incomplete history, too close to the deadline). A read that fails after the checkpoint opened also lands here. No slot was created; nothing ran. |
| 7 | `execution_failed` | The execution ran and did not succeed. The slot is failed and is never attested. |
| 8 | `refused_before_execution` | Admitted, but the verified request does not fit this executor (actor, request, manifest, toolchain, price or retention). The slot is abandoned; nothing ran. |

A resolver outage during the pre-execution re-verification (step 4) cannot
be told apart from a request that stopped being pending; it abandons the
slot and reports `not_pending_now`.

The input digest is the value the paid call committed. How a transport
derives it from the source archive is that transport's rule; the Receipt
binds the digest of the archive actually executed, so stage only the archive
the buyer delivered for this request.

## Pre-admitted jobs (default mode)

Without a subcommand the executor runs a job whose admission its caller has
already proved, for example a provider process that owns its own `railgate`
slots. It performs no chain verification, so **never use it directly for paid
work**. For each job, create a new private directory and copy—not link—the
already content-verified source archive into it:

```bash
install -d -o root -g root -m 0700 /var/lib/tos-service-provider/job-UNIQUE
install -o root -g root -m 0600 SOURCE.tar \
  /var/lib/tos-service-provider/job-UNIQUE/source.tar
install -d -o root -g root -m 0700 \
  /var/lib/tos-service-provider/job-UNIQUE/state \
  /run/tos-service-containerd/job-UNIQUE-fifos

/usr/local/libexec/tos-service/software-work-execute \
  --containerd-socket /run/tos-service-containerd/containerd.sock \
  --fifo-dir /run/tos-service-containerd/job-UNIQUE-fifos \
  --state-dir /var/lib/tos-service-provider/job-UNIQUE/state \
  --source /var/lib/tos-service-provider/job-UNIQUE/source.tar \
  --quote tvm-cell-sha256:ACCEPTED_QUOTE_COMMITMENT \
  --execution-id sha256:UNIQUE_EXECUTION_ID \
  --input-digest sha256:CANONICAL_INPUT_DIGEST \
  --retain-until RETAIN_UNTIL_UNIX_SECOND
```

`--retain-until` gives the job's artifact and report a retention deadline;
without it the job's retention record stays pending and the objects are
never swept (see [Artifact retention](#artifact-retention)). The caller must first resolve the exact Capability
version, Accepted Quote, and the pending Service Actor request `(actor,
request_id)` that was called under the quoted `terms_hash` and
`request_hash`. A crash-ambiguous execution ID is never reused. The executor
output and content-addressed objects are public evidence; they contain no
signing credential.

## Artifact retention

Objects are deduplicated by content, so one object may belong to several
jobs. Every job has a record in `STATE/artifacts/retention/` naming its
objects. The record is written as each object is committed and stays
pending (`<job>.pending`), keeping its objects, until the job is given its
last retained unix second (`<job>.json`; the paid path does this itself, the
default mode with `--retain-until`). Objects stored without a job get a permanent hold
(`<object>.hold`). Sweep expired jobs with:

```bash
/usr/local/libexec/tos-service/software-work-execute artifacts-gc \
  --state-dir /var/lib/tos-service-provider/paid --limit 64
```

The sweep deletes at most `--limit` (1–1024) expired jobs per call and prints
a JSON report (`deleted_jobs`, `deleted_objects`, `kept_shared`, `deferred`,
`live` retained jobs not yet expired, `pending` jobs without a deadline, and
`held` objects); rerun it while `deferred` is non-zero, for example from a
systemd timer. An object stays while a pending, unexpired or deferred job
names it or a hold keeps it, so a sweep never deletes an object that a
running job just committed or deduplicated. Committing an object, retaining
a job and sweeping take one retention lock; each waits up to 60 seconds for
the others, so a timer-driven sweep does not fail a run that finishes at the
same moment.

A store keeps at most 65 536 deadline-bearing job records and tombstones.
At that bound, giving a new job its first deadline is refused (jobs that
already have one can still add objects and be extended), and `paid` refuses
to start a run, exiting 1 before anything is admitted or executed. A sweep
reads every entry however many there are, so it always makes progress, and
each expired job it deletes makes room again.

Pending records and holds are references that must never be broken, so they
are never refused and do not count toward the bound. The default mode does
not check capacity: without `--retain-until` it keeps working indefinitely,
and its storage grows the way it always did before retention existed,
because objects of jobs that never get a deadline are never collected. **A
provider runtime that drives the default mode should pass
`--retain-until`** (for example the end of the response window plus the
manifest's `retention_seconds`) so its objects become collectable. With
`--retain-until` the default mode needs room for a deadline after the run;
at the bound that run fails after executing, so keep sweeping.

In the paid path every retention lock wait (the runner's two object commits
and the final `Retain`) is also bounded by the time left before the response
deadline minus the respond budget, or by the 60 second cap if that is
shorter. A wait that runs out fails the execution (`execution_failed`, or
`ambiguous` if the outcome cannot be recorded); it never reads as success.

Each deletion is marked by a tombstone first, so an interrupted sweep leaves
no live record pointing at a deleted object and the next run finishes it.
Before deleting anything, the sweep refuses when it cannot get the lock in
time or when any record or hold is malformed or unexpected. An object that
turns out not to be a regular file stops the sweep at that job: jobs deleted
earlier in the same call stay deleted, the offending job keeps its tombstone
and object, and a rerun after the operator fixes it continues. The sweep
never follows symlinks. Jobs whose record stays pending (for example
default-mode runs without `--retain-until`, or a run that crashed before it
was retained) keep their objects until a deadline is set.

## Settlement and signing custody

Settlement is the actor's `respond`, authorized by the attestor's SRP2
signature. Review the response outside this root boundary and sign its exact
32-byte SRP2 hash through `tosctl`. Never copy a mnemonic, vault export,
private key, or vault master key into `/var/lib/tos-service-provider`,
`/run/tos-service-containerd`, the source archive, or executor environment.

## Acceptance checks

Run the live conformance suite from
`docs/isolated-backend-conformance.md` on the exact deployment image. A provider
template is ready only when the private socket checks, pinned-image workspace
test, residue cleanup, cgroup metrics, and a complete local Receipt flow pass.
The template is operational scaffolding, not an isolation certification.
