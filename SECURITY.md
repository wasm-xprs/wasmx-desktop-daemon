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

The direct Wasmtime runtime is deliberately **not** an actor runtime. ORES adapters for wasm-xprs must declare `actor_model=false`; actor/mailbox semantics belong in Lunatic Lorry rather than this host.

Deployment identifiers are immutable. Repeating an identical deployment is idempotent, but attempting to replace an existing deployment id with different Wasm is rejected.

## Persistence

Only validated `.wasm` deployment artifacts are persisted. Invocation memory, globals, tables, host state, fuel and output buffers are recreated for each call.

## Persistent artifact integrity

Deployment manifests bind immutable deployment IDs to the SHA-256 and byte length of the stored module plus the expected wasm-xprs ABI/runtime identity. Cold loads fail closed if the manifest is missing, invalid, or does not match the artifact. This detects accidental or malicious artifact tampering between daemon restarts and avoids trust-on-first-use repair.

Per-tenant deployment-count and byte quotas bound authenticated disk-exhaustion attempts. These quotas complement, rather than replace, filesystem quotas or an OS-level sandbox.

The v1 admission layer rejects shared-memory/threaded modules and memory64 modules before Wasmtime compilation. They can be admitted in a future ABI only after their resource and isolation implications are reviewed explicitly.

On Unix, daemon state/artifact directories are forced to mode `0700`, and newly-created bearer-token files are created with mode `0600` from the outset rather than relying on a later permission fix-up.
