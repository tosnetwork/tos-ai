# Software update and rollback foundation

`pkg/update` verifies a bounded Ed25519-signed manifest that pins target,
artifact size, SHA-256 digest, validity interval, signing key, and monotonic
security revision. `pkg/softwareupdate` provides a local crash-safe two-slot
state machine, and `pkg/servicemanager` provides a fixed-unit, no-shell systemd
adapter.

These packages do not download releases, authorize remote operators, expose a
listener, or restart a service by themselves. A future Native software-work
worker may compose them only through an explicit operator-controlled boundary.

The safe lifecycle is:

1. stream and verify an operator-selected artifact into the inactive slot;
2. activate the pending slot;
3. restart through the bounded service manager;
4. run deployment-selected startup and dependency health checks;
5. confirm only after the candidate boot passes every check;
6. otherwise roll back and restart the last known-good slot.

Activation enters `awaiting health`. If a candidate exits or loses power
without confirmation, the next opener selects the last confirmed slot. The
security revision advances only after candidate confirmation, so an interrupted
activation cannot lock out the known-good version.

Operational rules:

- keep release signing keys offline and outside worker hosts;
- never delete the known-good slot during an activation window;
- never bypass target, signature, digest, expiry or anti-rollback validation;
- place update state on a private, quota-enforced filesystem;
- record source revision, artifact digest, signature, key identity, operator
  approval and rollout time in an external audit log;
- test power-loss and rollback behavior on every supported deployment image.
