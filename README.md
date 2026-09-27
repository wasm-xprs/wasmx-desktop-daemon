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
- loopback-only HTTP listener
- bearer token stored under ~/.wasm-xprs/daemon/token
- no WASI filesystem, sockets, environment variables or process APIs

The only guest capabilities in ABI v1 are input_len, input_read, output_write and log.

## HTTP API

- GET /healthz
- GET /v1/status
- POST /v1/deploy
- POST /v1/invoke
- DELETE /v1/deployments/{tenant_id}/{deployment_id}

Deployment artifacts are persisted under ~/.wasm-xprs/artifacts/{tenant}/{deployment}/module.wasm and compiled modules are cached in memory after validation.

## Environment

WASMX_DESKTOP_ADDR defaults to 127.0.0.1:8765.
WASMX_MAX_MEMORY_BYTES defaults to 134217728.
WASMX_MAX_PARALLEL_INVOCATIONS defaults to 8.
WASMX_DEFAULT_FUEL defaults to 50000000.
WASMX_ARTIFACT_ROOT overrides the artifact directory.
WASMX_DESKTOP_TOKEN_FILE overrides the bearer-token file.

See ABI.md for the guest contract.
