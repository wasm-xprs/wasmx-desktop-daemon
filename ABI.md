# wasmx-v1 guest ABI

wasm-xprs intentionally starts with a tiny capability surface rather than ambient WASI.

Canonical guest identity:

- Rust target: `wasm32-unknown-unknown`
- ABI: `wasmx-v1`
- WASI: disabled
- isolation: fresh Wasmtime `Store` per invocation

A guest module must export:

- `memory`
- `wasmx_main() -> i32`

A zero return value means success. Any non-zero return value fails the invocation.

The host provides imports from module name `wasmx`:

- `input_len() -> i32`
- `input_read(offset: i32, ptr: i32, len: i32) -> i32`
- `output_write(ptr: i32, len: i32) -> i32`
- `log(ptr: i32, len: i32) -> i32`

Deploy-time validation rejects every other import module, every unknown `wasmx` import, and any wrong hostcall signature. In particular, `wasi_snapshot_preview1` imports are rejected before an artifact is persisted.

The invocation input is UTF-8 JSON bytes. The guest must write UTF-8 JSON bytes to `output_write`. The daemon parses those bytes as JSON before returning the HTTP response.

No filesystem, network, clock, random, environment, process, secret or storage capability exists in v1. Future capabilities must be added as explicit, narrow imports rather than by enabling ambient WASI.

## ORES Stack adapter binding

An optional `ores_adapter` object on `POST /v1/deploy` is accepted for ORES Stack generated lambdas. When present it must be an `ores.lambda.adapter/v1` descriptor and must exactly bind to:

- provider/runtime stack `wasm_xprs`
- execution model `wasm_isolate`
- isolation boundary `wasmtime_store`
- artifact format `wasm_module`
- target `wasm32-unknown-unknown`
- guest ABI `wasmx-v1`
- `wasi_enabled=false`
- actor model and same-process multi-tenancy enabled

The provider-neutral `lambda.rs` source path and SHA-256 are validated as descriptor identity metadata. They do not grant guest capabilities.
