# `tos.ai.text-generation` profile v0.1.0

Status: implementation candidate for the non-streaming WorkerService v0.1

This profile defines one paid, unary text-generation action. Its only
operation is `generate`; it has no critical extensions. The exact public
intent bytes are committed by `tos.request-intent.v1` under the negotiated
profile selector before this mapper runs.

The intent is the RFC 8785 JSON Canonicalization Scheme (JCS) encoding of
exactly two UTF-8 JSON strings. Object members appear in lexicographic order,
so the only accepted shape is `{"model":<string>,"prompt":<string>}` without
insignificant whitespace. Strings use the shortest required JSON escaping and
otherwise preserve Unicode scalar values as UTF-8. This gives each semantic
v0.1 intent exactly one byte representation and therefore one
`tos.request-intent.v1` digest.

The two members are:

- `model`: a logical model name selected from the operator's immutable
  `serviceId + model` route set;
- `prompt`: the text passed as the private Worker payload.

Unknown and duplicate members, non-canonical JSON, trailing JSON, NUL,
unpaired UTF-16 surrogates, invalid UTF-8, empty values, unconfigured routes,
nonzero extension sets, and inputs outside the signed quote's byte bounds fail
closed. The implementation also caps the encoded intent and decoded prompt at
16 MiB and model names at 256 UTF-8 bytes. JSON Schema `maxLength` counts
Unicode scalar values, so the mapper's byte checks remain authoritative.

Sampling controls, tools, attachments, arbitrary runtime parameters, runtime
URLs, credentials, system prompts, and provider-specific JSON are not part of
v0.1. Adding any of them requires a new reviewed profile version or critical
extension with deterministic mapping and quote/receipt semantics. The mapper
returns only model and payload; Edge Core continues to derive request, quote,
payment, task, priority, deadline, output, and retention fields.

Success returns one bounded final result. Streaming and partial-work charging
remain outside this version. A failed, canceled, or timed-out execution has
the base protocol's zero-charge non-success receipt behavior.

Normative artifacts:

- `intent.schema.json`: Draft 2020-12 intent schema;
- `vectors.json`: exact intent and payload digests, valid mappings, and
  rejected syntax/semantic/canonicalization cases;
- `pkg/profile/textgeneration`: fail-closed Go mapper and conformance tests.
