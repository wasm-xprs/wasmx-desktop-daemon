# Wasmtime deployment and invocation isolation: bounded assurance and AttoLang candidate

## Scope and safety claim

`assurance/model.py` is a **source-free, bounded, executable reference model**, not a verification of the production implementation. It explores the entire reachable reference state space for two tenants, two keys per tenant, two payloads, independent authority/proof/budget denials, idempotency, conflicting retries and disjoint-tenant commutativity. It also exercises a small domain-specific safety check. Its bound is finite (four state-changing admissions); no unbounded liveness, OS security, or backend parity is claimed.

Run: `python3 assurance/model.py`. A successful host run prints `HOST_MODEL_PASS` and always declares `GUEST_EXECUTION=NOT_EXECUTED`. The workflow `.github/workflows/assurance-host-model.yml` checks only this host reference and presence of the source fixture. Source presence is not compilation.

## Runtime refinement obligations

Digest-bound immutable deployment identity, tenant separation, strict import ABI, no WASI, per-invocation fresh Store, fuel/epoch/memory isolation, crash and reload tamper rejection.

Production source/test points to connect through an adapter: src/; tests/security_boundaries.rs; ABI.md.

The model's booleans are **assumptions** about authority, proof and budget, not successful validation implementations. A future harness must extract real operations and observable effects from the running application, replay the same traces, and reject model/implementation disagreements. Add negative mutations so that a deliberately broken guard is caught. For persistence, fault-inject rollback, crash, partial outbox commit, and duplicate delivery, rather than relying on in-memory model success.

## AttoLang integration target

`assurance/attolang_guard.atto` contains a pure admission predicate and positive/negative candidate checks, written using AttoLang's `.atto` and snake_case conventions. This is **TARGET / NOT_EXECUTED**. The fixture is not wired to this repository's production runtime, compiled by CI, or certified across Java, LLVM, Wasm, Forge and .NET AOT.

To promote AttoLang support, pin the authoritative compiler+backend source/tree/blob SHAs, compile the exact `.atto` fixture using an installed compiler, invoke its test function through a real runner, compare positive/negative outputs to the host reference, and retain stepful job logs plus artifact digests. Add an adapter to the actual domain API only after verifying capability isolation and wire-format ownership. Do not auto-enable guest authority, filesystem, network or process imports.

AttoLang specification: https://github.com/ores-truffle-oreslang/attolang-language-spec.md
Evidence policy: https://github.com/ores-truffle-oreslang/attolang-language-spec.md/blob/main/spec/11-conformance.md

## What remains unverified

- Actual Rust/TypeScript/client-to-server behavior and live security boundaries
- Exact-head AttoLang compilation and guest execution
- Scheduler, process, crash, storage and network concurrency
- All five backend implementations and signed/immutable execution evidence

Existing domain-specific suites, conformance registries and prior proof artifacts remain authoritative and must not be replaced by this intentionally small reference gate.
