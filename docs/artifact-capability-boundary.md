# Artifact filesystem capability boundary

`wasmx-desktop-daemon` treats the configured artifact root as the ambient-to-capability transition point. After startup opens that root as a `cap_std::fs::Dir`, tenant and deployment filesystem operations must remain relative to retained directory capabilities rather than returning to ambient path lookup.

## Required invariants

- Tenant and deployment components are validated identifiers and opened with `open_dir_nofollow`.
- Newly created child directories receive restrictive permissions through the already-open parent capability before the child is reopened. This avoids relying on child handles that may be opened with `O_PATH`-style semantics and are unsuitable for `fchmod`-style mutation.
- Module and manifest writes use capability-relative create-new staging plus atomic hard-link no-replace publication.
- Module and manifest reads are bounded and opened without following the final component.
- Listing, quota accounting, deletion, immutable-manifest verification, and compiled-module cache admission all operate through the same retained capability chain.
- A compiled Wasmtime `Module` is only a cache optimization; persisted module bytes and manifest evidence remain durable authority and are revalidated before cache reuse.

The boundary is intentionally narrower than the runtime's full security model. Fresh `Store` + `Instance` isolation, explicit `wasmx-v1` imports, fuel/epoch/memory limits, ORES adapter validation, and ORES build-receipt verification remain independent gates.
