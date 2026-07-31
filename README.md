# TOS AI

`tos-ai` is the vertical AI Edge Computing Terminal implementation for TOS
Network. It turns owner-operated AI edge hardware into callable services while
preserving local safety, policy, and real-time authority.

The bootstrap is written in Go 1.24 and contains:

- `tos-ai-worker`, a private ConnectRPC worker served only on a mode-0600 Unix
  socket;
- `tos-ai-cli`, a local diagnostics client;
- a bounded priority scheduler with explicit local and external task classes;
- runtime adapter contracts plus deterministic and Ollama adapters;
- bounded quote and invocation replay state;
- Linux host resource discovery;
- content-addressed, Ed25519-verified update manifests with anti-rollback
  checks.

The worker has no wallet owner key and no public listener. `tos-edge` in
`tos-protocol` remains the future authentication, quote/payment, receipt, and
settlement control plane.

## Development

The module pins the exact `tos-protocol` bootstrap revision, so a standalone
clone builds normally:

```sh
make all
make test-race
```

Contributors changing both repositories together may optionally use a local Go
workspace with `go work init ./tos-protocol ./tos-ai`.

## Run the deterministic development worker

```sh
go run ./cmd/tos-ai-worker \
  -socket /run/user/$(id -u)/tos-ai/worker.sock

go run ./cmd/tos-ai-cli \
  -socket /run/user/$(id -u)/tos-ai/worker.sock capabilities
```

The mock adapter is intentionally marked development-only. Ollama is connected
as an external runtime over its HTTP API; the worker does not embed or fork
the runtime.

## Security posture

- Public ingress, TOS payment authorization, and ARD discovery are outside the
  worker process.
- Queue, request, response, quote, replay, and execution time are bounded.
- Network-originated work uses `EXTERNAL_SERVICE`; it cannot claim emergency,
  control, or real-time priority.
- Go is not used as a hard real-time or physical safety loop. An independent
  site safety controller keeps final actuator authority.
- Arbitrary consumer containers, bare GPU rental, raw device APIs, and direct
  Docker/containerd socket exposure are non-goals.

## Planned adapters

- containerd isolation with a narrow task policy
- NVIDIA NVML and Jetson telemetry
- LocalAI, vLLM, llama.cpp, TensorRT, and OpenVINO
- OPA local admission policy
- ORAS/Cosign/TUF artifact delivery
- SPIFFE workload identity
- KubeEdge-inspired offline/fleet management without embedding the complete
  Kubernetes control plane

No license has been selected for this new repository yet. Add one before the
first public release.
