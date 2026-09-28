# Security Policy

## Supported versions

The latest commit on `main` is the supported development version until tagged releases begin.

## Reporting vulnerabilities

Please do not publish exploit details in a public issue. Use GitHub's private vulnerability reporting for this repository when available, or contact the maintainers privately.

Useful reports include the Wasm module or WAT reproducer, host platform, Wasmtime version, resource-limit configuration, and whether the issue crosses the intended fresh-`Store` isolation boundary.

## Security invariants

- no ambient WASI
- loopback-only daemon listener
- fresh Wasmtime `Store` per invocation
- explicit hostcall allowlist
- bounded memory, fuel, wall time, I/O, logging, compilation concurrency, and compiled-module cache
- immutable deployment identifiers
