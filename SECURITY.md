# Security model

wasm-xprs runs untrusted WebAssembly with Wasmtime directly. The security boundary is intentionally small:

- one fresh Wasmtime `Store` per invocation
- no ambient WASI
- no guest filesystem, sockets, process APIs, environment variables or host clock
- explicit memory, fuel, epoch timeout and I/O limits
- loopback-only daemon listener
- bearer-token authentication with constant-time comparison
- immutable compiled `Module` objects may be cached, but guest state is never reused

## Threat model

Guest modules are considered hostile. A guest may consume CPU, attempt memory growth, trap, emit malformed output, or deliberately exercise runtime edge cases. The host must treat every guest pointer and length as untrusted.

The desktop daemon is not a replacement for an OS sandbox or VM boundary. Operators that need defense in depth can run the daemon inside a hardened OS account, container, sandbox, or microVM.

## Capability policy

ABI v1 exposes only:

- `input_len`
- `input_read`
- `output_write`
- `log`

Future capabilities such as HTTP, KV, queues or secrets should remain narrow host imports with explicit per-deployment policy. Do not enable ambient WASI merely for convenience.

## Persistence

Only validated `.wasm` deployment artifacts are persisted. Invocation memory, globals, tables, host state, fuel and output buffers are recreated for each call.
