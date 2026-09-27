# wasmx-v1 guest ABI

wasm-xprs intentionally starts with a tiny capability surface rather than ambient WASI.

A guest module must export:

- memory
- wasmx_main() -> i32

A zero return value means success. Any non-zero return value fails the invocation.

The host provides imports from module name wasmx:

- input_len() -> i32
- input_read(offset: i32, ptr: i32, len: i32) -> i32
- output_write(ptr: i32, len: i32) -> i32
- log(ptr: i32, len: i32) -> i32

The invocation input is UTF-8 JSON bytes. The guest must write UTF-8 JSON bytes to output_write. The daemon parses those bytes as JSON before returning the HTTP response.

No filesystem, network, clock, random, environment, process, secret or storage capability exists in v1. Future capabilities should be added as explicit, narrow imports rather than by enabling ambient WASI.
