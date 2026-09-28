# wasmx-desktop-daemon

Local control plane and execution host for wasm-xprs.

This daemon deliberately embeds Wasmtime directly. It does not use Lunatic, WASI, a JavaScript engine, or an external wasmtime CLI process.

## Security model

Each invocation receives a fresh Wasmtime Store. Compiled Module objects may be cached because they are immutable engine artifacts, but guest memory, globals, tables, host state, fuel and output buffers are recreated per invocation.

Default limits:

- 128 MiB maximum linear memory per Store
- 50,000,000 fuel units per invocation
- wall-clock interruption via Wasmtime epochs
- 10 MiB input/output limit
- bounded parallel invocation semaphore
- immutable deployment ids (same bytes are idempotent; different bytes conflict)
- bounded guest log volume and per-hostcall transfer sizes
- route-specific HTTP request body limits
- loopback-only HTTP listener
- bearer token stored under `~/.wasm-xprs/daemon/token`
- no WASI filesystem, sockets, environment variables or process APIs

The only guest capabilities in ABI v1 are `input_len`, `input_read`, `output_write` and `log`. Deployment validation rejects unknown import modules/names and wrong hostcall signatures before the module is persisted.

Canonical guest identity is `wasm32-unknown-unknown`, `wasmx-v1`, with WASI disabled.

## ORES Stack integration

`POST /v1/deploy` accepts an optional `ores_adapter` object. When present, it is validated as an `ores.lambda.adapter/v1` descriptor for provider `wasm_xprs` and must match the daemon's no-WASI Wasmtime isolation contract. The response reports `ores_adapter_verified=true` only after that validation succeeds.

This preserves a clean boundary:

- ORES owns provider-neutral `lambda.rs` semantics and adapter source identity.
- wasm-xprs owns the guest ABI, Wasmtime hostcalls, resource limits, module admission and per-invocation isolation.
- internal isolate creation is host scheduling and is not equivalent to granting an ORES `spawn` capability to the guest.

## HTTP API

- `GET /healthz`
- `GET /v1/status`
- `POST /v1/deploy`
- `POST /v1/invoke`
- `DELETE /v1/deployments/{tenant_id}/{deployment_id}`

Deployment artifacts are persisted under `~/.wasm-xprs/artifacts/{tenant}/{deployment}/module.wasm` and compiled modules are cached in memory after validation.

## Environment

`WASMX_DESKTOP_ADDR` defaults to `127.0.0.1:8765`.
`WASMX_MAX_MEMORY_BYTES` defaults to `134217728`.
`WASMX_MAX_PARALLEL_INVOCATIONS` defaults to `8`.
`WASMX_DEFAULT_FUEL` defaults to `50000000`.
`WASMX_ARTIFACT_ROOT` overrides the artifact directory.
`WASMX_DESKTOP_TOKEN_FILE` overrides the bearer-token file.

See `ABI.md` for the guest contract.

## Persistent deployment integrity

Each deployment now has a `manifest.json` bound to its tenant/deployment identity, module SHA-256, module size, guest ABI, target triple and WASI policy. Cold loads verify the module against that manifest before caching it. Legacy validated artifacts are upgraded by writing a manifest on first cold load.

Per-tenant persistent storage is bounded by `WASMX_MAX_TENANT_DEPLOYMENTS` (default 64) and `WASMX_MAX_TENANT_STORAGE_BYTES` (default 536870912). The status endpoint reports both limits. `GET /v1/deployments/{tenant}/{deployment}` exposes deployment metadata, and `/readyz` reports whether the artifact store is usable.

The engine explicitly disables WebAssembly threads and memory64 for the v1 runtime surface.
