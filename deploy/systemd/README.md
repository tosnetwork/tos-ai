# systemd deployment templates

These units are reviewed starting points for the non-streaming AI Edge
composition. They deliberately use a dedicated unprivileged `tos-ai` account,
private runtime/state directories, bounded restart policy, and no writable
application tree.

Before enabling them, an operator must:

1. install immutable `tos-ai-worker` and `tos-ai-edge` binaries under
   `/usr/local/bin` and keep `/etc/tos-ai` root-owned;
2. install the purpose-fixed Receipt signer as `tos-receipt-signer.service` and
   keep its seed outside the Edge and Worker read paths;
3. replace every placeholder in the strict examples under `docs/`, set all
   private JSON files to mode `0600`, and validate public descriptor, catalog,
   manifest and on-chain commitments together;
4. place TLS, connection and request-rate enforcement in a reviewed ingress in
   front of the loopback Edge listener;
5. extend only the Worker's explicit read/write/device paths required by the
   selected model runtime. NVIDIA device access is not granted by this base
   unit and requires the separate hardware isolation certification;
6. run `systemd-analyze security` and the repository deployment rehearsal on
   the exact target host before advertising the service.

The units do not run development mock mode and do not expose the Worker or any
signer socket on a network interface.
