# Cross-transport acceptance

`pkg/adapterinterop` runs the official A2A JSON-RPC client and official MCP
streamable-HTTP client against separate real TLS 1.3 servers protected by the
same public adapter boundary.

The acceptance sequence is:

1. submit one purchase through A2A with a valid bearer credential;
2. acquire the shared `(Quote commitment, escrow)` execution claim;
3. complete exactly one bounded runner invocation;
4. submit the same purchase through MCP; and
5. require the shared Gate to reject it before a second runner invocation.

The test also uses a certificate chain trusted by the clients rather than
disabling TLS verification. Its invariant is `gate calls = 2` and
`runner calls = 1`.

Run it independently with:

```bash
go test -race ./pkg/adapterinterop -count=1 -v
```

This is deterministic same-process interoperability evidence. It proves the
public transports cannot bypass the shared purchase claim, but it is not the
fresh external provider/buyer session required to accept Gate E.
