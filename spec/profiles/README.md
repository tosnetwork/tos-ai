# TOS AI profile registry

This directory owns normative definitions in the `tos.ai.*` profile
namespace. A profile is a semantic contract layered on the generic TOS
Service Protocol; it does not expose a Worker socket or grant payment or
execution authority by itself.

Implemented candidates:

| Profile | Version | Operation | Extensions | Implementation |
| --- | --- | --- | --- | --- |
| `tos.ai.text-generation` | `0.1.0` | `generate` | none | `pkg/profile/textgeneration` |

Every profile version must carry bounded schemas, fixed vectors, deterministic
mapping rules, quote and receipt semantics, privacy constraints, and an
explicit compatibility policy. Runtime-specific endpoints, credentials and
unreviewed provider options never belong in public task intents.
