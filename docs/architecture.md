# TOS AI bootstrap architecture

The first release is an AI task worker, not a bare GPU rental daemon and not a
hard real-time controller.

```text
tos-edge (wallet/payment control boundary)
       |
       | ConnectRPC over a private Unix socket
       v
tos-ai-worker
       |
       +-- quote and invocation replay bounds
       +-- bounded priority scheduler
       +-- model/runtime adapter
       +-- host probe
       +-- signed update verifier
```

Priority is explicit:

1. emergency
2. control
3. real-time perception
4. local asynchronous
5. external service
6. background

The bootstrap inference adapters accept only local asynchronous, external
service, and background work. A caller cannot label Internet work as
emergency, control, or real-time. Physical terminal adapters will add those
classes only behind site-local authorization and an independent safety
controller.

## Foundation versus integrations

Implemented now:

- deterministic worker API and replay semantics
- queue and concurrent execution limits
- deadlines, cancellation, input/output bounds
- Ollama adapter over an operator-configured endpoint
- coarse privacy-preserving host probe
- signed, content-addressed update manifest verification and anti-rollback

Next milestones:

- containerd executor with an allowlisted OCI policy, read-only rootfs,
  seccomp, user namespace, no host devices by default, and strict cgroup bounds
- NVML/Jetson probes and benchmark evidence
- LocalAI, vLLM, llama.cpp, TensorRT, and OpenVINO adapters
- OPA admission policy
- active/known-good update slots, crash-safe activation, and ORAS/Cosign/TUF
- bounded offline journal and idempotent reconnect
- fleet enrollment, scoped delegation, rollout rings, health gates, and
  terminal retirement

KubeEdge and EdgeX are design and interoperability sources, not codebases to
embed wholesale in this daemon.
