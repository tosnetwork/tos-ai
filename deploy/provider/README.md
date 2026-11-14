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

Import the reproducible OCI archive into namespace `tos-service-paid-work`, verify its
index digest independently, and unpack it for `overlayfs`. Never use a mutable
tag without the manifest's exact `sha256:` digest.

## Paid path: admit, then execute

`software-work-execute paid` is the only standalone way to execute paid work.
It refuses to run anything until finalized TOS state proves the request:

1. every flag and the rail config are validated, the Quote package is checked
   against the configured provider offer and `--actor`, and the containerd
   executor, journal, artifact store and slot directory are opened. Any
   missing or malformed piece refuses the run before admission;
2. `railgate` `Admit` verifies the call, the pending request, the Quote and
   input binding and the deadline slack at a fresh finalized checkpoint and
   creates an at-most-once slot keyed by `(quote_commitment, actor,
   request_id)`;
3. `Run` re-verifies at another fresh checkpoint immediately before
   execution and durably marks the slot started;
4. a successful run's artifact and report are re-read, retained until the
   response deadline plus the manifest's `retention_seconds`, and bound into
   the `SoftwareWorkReceiptV1` whose cell hash is recorded with `Complete`;
   any other outcome is recorded with `Fail` and is never attested;
5. a final fresh `Recheck` reports whether respond may be sent now.

The paid state directory holds the slots, the resolver checkpoints, the job
journal and the artifacts. It must be **persistent and shared by every paid
run of the provider offer**: deleting or splitting it would forget which
requests were already admitted. One process owns it at a time; a concurrent
run fails to open it rather than share it.

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
`toolchain_digest` must be the executor's pinned image.

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
| 1 | `refused` | Invalid flags or config, or a local dependency is unavailable. Nothing was admitted or run; fix and retry. |
| 3 | `already_taken` | The request already has a slot (admitted, completed, failed or abandoned). It is not executed again. |
| 4 | `not_pending_now` | Admitted, but the pre-execution check found the request no longer pending with room to execute and respond. The slot is abandoned; nothing ran. |
| 5 | `ambiguous` | A run may have started without a recorded outcome (a crash, or a result that could not be recorded). Never rerun or respond; the buyer's refund path applies. |
| 6 | `not_admitted` | Finalized state does not prove the claim (unknown call, other terms, incomplete history, too close to the deadline, resolver unavailable). No slot was created; nothing ran. |
| 7 | `execution_failed` | The execution ran and did not succeed. The slot is failed and is never attested. |

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

`--retain-until` records how long the job's artifact and report are
retained; without it no retention record is written and the objects stay
until removed by hand. The caller must first resolve the exact Capability
version, Accepted Quote, and the pending Service Actor request `(actor,
request_id)` that was called under the quoted `terms_hash` and
`request_hash`. A crash-ambiguous execution ID is never reused. The executor
output and content-addressed objects are public evidence; they contain no
signing credential.

## Artifact retention

Every retained job has a record in `STATE/artifacts/retention/` naming its
objects and the last unix second they are kept. Sweep expired jobs with:

```bash
/usr/local/libexec/tos-service/software-work-execute artifacts-gc \
  --state-dir /var/lib/tos-service-provider/paid --limit 64
```

The sweep deletes at most `--limit` (1–1024) expired jobs per call and prints
a JSON report (`deleted_jobs`, `deleted_objects`, `kept_shared`, `deferred`,
`live`); rerun it while `deferred` is non-zero, for example from a systemd
timer. An object stays while any unexpired or deferred job still names it.
Each deletion is marked by a tombstone first, so an interrupted sweep leaves
no live record pointing at a deleted object and the next run finishes it.
The sweep refuses, without deleting anything, when another process holds the
store's retention lock, when any record is malformed or unexpected, or when
an object is not a regular file; it never follows symlinks. Objects without a
retention record (for example from a run without `--retain-until`) are never
swept.

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
