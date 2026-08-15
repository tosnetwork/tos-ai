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
install -d -o root -g root -m 0755 /etc/atos
install -o root -g root -m 0600 deploy/provider/containerd.toml \
  /etc/atos/containerd.toml
install -o root -g root -m 0644 deploy/provider/atos-containerd.service \
  /etc/systemd/system/atos-containerd.service
systemctl daemon-reload
systemctl enable --now atos-containerd.service
```

Before proceeding, require all of these checks to pass:

```bash
systemctl is-active --quiet atos-containerd.service
test "$(stat -c '%U:%G:%a' /run/atos-containerd)" = root:root:700
test "$(stat -c '%U:%G:%a' /run/atos-containerd/containerd.sock)" = root:root:600
ctr --address /run/atos-containerd/containerd.sock plugins ls
```

Import the reproducible OCI archive into namespace `atos-paid-work`, verify its
index digest independently, and unpack it for `overlayfs`. Never use a mutable
tag without the manifest's exact `sha256:` digest.

## Stage and execute one accepted job

The root executor accepts only absolute, canonical, non-symlink paths owned by
its own identity. For each job, create a new private directory and copy—not
link—the already content-verified source archive into it:

```bash
install -d -o root -g root -m 0700 /var/lib/atos-provider/job-UNIQUE
install -o root -g root -m 0600 SOURCE.tar \
  /var/lib/atos-provider/job-UNIQUE/source.tar
install -d -o root -g root -m 0700 \
  /var/lib/atos-provider/job-UNIQUE/state \
  /run/atos-containerd/job-UNIQUE-fifos

/usr/local/libexec/atos/software-work-execute \
  --containerd-socket /run/atos-containerd/containerd.sock \
  --fifo-dir /run/atos-containerd/job-UNIQUE-fifos \
  --state-dir /var/lib/atos-provider/job-UNIQUE/state \
  --source /var/lib/atos-provider/job-UNIQUE/source.tar \
  --quote tvm-cell-sha256:ACCEPTED_QUOTE_COMMITMENT \
  --execution-id sha256:UNIQUE_EXECUTION_ID \
  --input-digest sha256:CANONICAL_INPUT_DIGEST
```

The operator must first resolve the exact Capability version, Accepted Quote,
and funded escrow. A crash-ambiguous execution ID is never reused. The
executor output and content-addressed objects are public evidence; they contain
no signing credential.

Review the settlement intent outside this root boundary and sign its exact
32-byte payload through `tosctl`. Never copy a mnemonic, vault export, private
key, or vault master key into `/var/lib/atos-provider`, `/run/atos-containerd`,
the source archive, or executor environment.

## Acceptance checks

Run the live conformance suite from
`docs/isolated-backend-conformance.md` on the exact deployment image. A provider
template is ready only when the private socket checks, pinned-image workspace
test, residue cleanup, cgroup metrics, and a complete local Receipt flow pass.
The template is operational scaffolding, not an isolation certification.
