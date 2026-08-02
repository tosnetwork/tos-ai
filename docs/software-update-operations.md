# Software update and rollback operations

This repository provides deterministic signed release bundles and a local,
crash-safe two-slot update state machine. It does not download software,
install a system service, restart a process, or authorize a remote operator by
itself. Those actions remain explicit deployment responsibilities.

## Release bundle

Build the same source revision twice and require byte-identical output:

```sh
make release-gates
```

For a release candidate, build into an empty operator-owned directory:

```sh
SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)" \
  ./scripts/build-release-bundle.sh /secure/release-output v0.1.0
```

Sign the complete compressed bundle with an offline Ed25519 release key. The
verification command checks the external digest when present, archive path and
entry safety, the complete internal file manifest, every file digest, and the
detached signature:

```sh
openssl pkeyutl -sign -inkey /offline/release-private.pem -rawin \
  -in /secure/release-output/tos-ai-v0.1.0-linux-amd64.tar.gz \
  -out /secure/release-output/tos-ai-v0.1.0-linux-amd64.tar.gz.sig

./scripts/verify-release-bundle.sh \
  /secure/release-output/tos-ai-v0.1.0-linux-amd64.tar.gz \
  /etc/tos-ai/release-public.pem \
  /secure/release-output/tos-ai-v0.1.0-linux-amd64.tar.gz.sig
```

Keep the private signing key off the terminal. Record the source commit,
bundle digest, signature, public-key identity, operator, approval, and rollout
time in the deployment change record.

## Two-slot lifecycle

`pkg/softwareupdate.Manager` owns exactly two private slots, `a` and `b`. A
signed `pkg/update.Manifest` binds the target, artifact size and SHA-256 digest,
validity interval, signing key, and monotonic security revision.

The safe sequence is:

1. stream and verify an operator-selected artifact into the inactive slot;
2. activate that pending slot;
3. restart only through the deployment's bounded service manager;
4. run startup, dependency, inference, and local Receipt health checks;
5. confirm healthy only after all selected checks pass;
6. otherwise invoke rollback and restart the prior known-good slot.

Activation deliberately enters `awaiting health`. The first opener after that
transition is recorded durably as the candidate boot and is allowed to run the
health gate. If it exits or loses power without confirmation, the following
opener automatically selects the last confirmed slot. The process that staged
the update cannot confirm its own candidate. The security revision advances
only after a successful candidate-boot confirmation, so an interrupted
activation cannot lock out the known-good version.

## Administrator command boundary

`pkg/admincontrol.Controller` is the local authorization boundary for
activation, health confirmation, and rollback. It accepts only canonical,
domain-separated Ed25519 command envelopes bound to the exact terminal, action,
command identifier, validity window, and expected active slot. Exact retries
return the durable result; command-ID conflicts and uncertain crash outcomes
never execute again automatically.

The journal is mode `0600`, exclusively locked, count- and byte-bounded, and
retained for a configured finite interval. Its bounded `History` view omits
signatures, payloads, keys, fingerprints, and raw error text. The package does
not expose a listener. A deployment that exports this interface must add a
private Unix or mutually authenticated transport, authorization, rate limits,
and an independent audit sink.

## Recovery rules

- Never delete the known-good slot during an activation or health window.
- Never bypass manifest signature, digest, target, or security-revision checks.
- Never retry an `uncertain` administrator command automatically; reconcile
  local state and require a new command identifier after operator review.
- Never place release keys, wallet keys, raw GPU identity, or runtime secrets
  in the bundle or command journal.
- Put the update root and journal on a filesystem with an administrator-enforced
  quota in addition to their application-level bounds.
- A production evidence record still requires the exact target hardware,
  service manager, filesystem, power-loss test, and rollback drill. MOCK tests
  prove deterministic state-machine behavior, not physical certification.
