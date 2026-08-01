# Production non-streaming AI Edge deployment

`tos-ai-edge` is the only public application process in the v0.1 reference
composition. It serves bounded discovery and paid Action routes while the
Worker and purpose-specific signers remain on owner-only Unix sockets. The
Worker has no wallet authority; the Edge has no private signing seed.

## Required artifacts

- a current service descriptor and ARD catalog;
- a controller-signed service manifest envelope whose digest is committed by
  the deployed service Agent Account;
- a strict TOS chain configuration with at least three independently operated
  endpoints and a strict-majority quorum;
- private Worker, session, Quote and Receipt signer sockets;
- a mode-`0600` Edge configuration based on
  `edge-gateway-config.example.json`;
- bounded bbolt files on a filesystem with an administrator-enforced quota;
- a reviewed TLS/rate-limiting ingress in front of the loopback listener.

Generate public bootstrap material with `tos-service-material` from
`tos-protocol`. Manifest ID, deployment revision, profile digest and every key
ID are mandatory operator inputs; the command never emits a private seed. The
reported manifest digest must match the Agent Account before publication.

## Startup order

1. Start the private Worker and verify its structured readiness.
2. Start session, Quote and Receipt signers and verify the expected key ID,
   public key, signing domain and path on each private socket.
3. Start `tos-ai-edge`. Startup fails unless chain authority, service code,
   manifest commitment, Worker route identity and Receipt signer all agree.
4. Publish only `/.well-known/tos-service.json`,
   `/.well-known/ai-catalog.json`, health/readiness and the selected public
   Action routes through the reviewed ingress.

Session and Quote issuance are deployment-owned authentication policy. The
reference protocol supplies exact purpose-bound signing clients but does not
turn discovery metadata into authority. Action-status and Receipt read routes
must remain disabled unless a concrete audited authorizer is installed.

## Required rehearsal

The release record must show a real discovery-to-Receipt request using current
chain authority, a real client Agent Account key and an exact finalized native
payment. Repeat the identical signed request before and after restarting both
Edge and Worker; the terminal output and signed Receipt must be byte-identical.
Then demonstrate one-of-three RPC loss remains ready, two-of-three loss fails
closed, signer and Worker outages degrade readiness, and bounded anonymous
malformed load does not grow journals, task storage, file descriptors or RSS
without limit.

Development mock mode is acceptable only for protocol/integration rehearsal.
It is not evidence for NVIDIA performance, model quality, thermal behavior,
container/GPU isolation or production key custody.
