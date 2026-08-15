# Public A2A and MCP boundary

`pkg/adapterhttp` is the required network boundary for public TOS Service Protocol A2A and MCP
handlers. It grants transport access only. The shared execution Gate still
derives every commercial decision from finalized TOS state and its durable
purchase-intent journal.

Construct an A2A server with `a2aadapter.NewPublicServer`, or an MCP server with
`mcpadapter.NewPublicServer`:

```go
server, err := a2aadapter.NewPublicServer(adapter, adapterhttp.ServerConfig{
    Address: ":8443",
    CertificateFile: "/etc/tos-service/tls/provider.crt",
    PrivateKeyFile: "/etc/tos-service/tls/provider.key",
    // Set ClientCAFile to require and verify mTLS client certificates.
    ClientCAFile: "/etc/tos-service/tls/client-ca.crt",
    Boundary: adapterhttp.BoundaryConfig{
        BearerToken: os.Getenv("TOS_SERVICE_ADAPTER_BEARER_TOKEN"),
        MaxRequestBytes: 16 << 20,
        MaxConcurrent: 32,
    },
})
if err != nil { /* fail closed */ }
err = adapterhttp.ListenAndServe(server)
```

The boundary enforces:

- TLS 1.3 with an already loaded and validated key pair;
- optional mandatory client-certificate verification;
- a 32–512 byte whitespace-free bearer credential;
- constant-time credential comparison;
- rejection of browser-origin requests by default;
- bounded request body, header size, concurrent requests, read time, and idle
  connections;
- `429` plus `Retry-After` when concurrency is exhausted; and
- `no-store`, `nosniff`, and no-referrer response policy.

Certificate, key, and client-CA paths must be absolute, canonical, protected
regular files. Symlinks, writable credential files, oversized files, and files
that change while opening are rejected. Never place the bearer token in a
command-line argument, checked-in configuration, manifest, A2A message, MCP
tool input, Quote, or Receipt.

There is intentionally no permissive public constructor. The lower-level
transport handlers remain available for in-process tests and private embedding,
but an Internet listener that bypasses `pkg/adapterhttp` is unsupported.
