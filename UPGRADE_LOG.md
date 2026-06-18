# Dependency Upgrade Log

## 2026-06-17 — frankensqlite — third-party dependency bumps

Scope: third-party deps only. Franken-ecosystem inter-dependency pins
(fsqlite*, frankensqlite, asupersync, frankensearch*, ftui*, toon/tru)
were intentionally left untouched — releases are coordinated separately.

### Caret-compatible patch/minor bumps (via `cargo update -p`)

| Dependency          | From      | To        | Kind            |
|---------------------|-----------|-----------|-----------------|
| smallvec            | 1.15.1    | 1.15.2    | patch           |
| bumpalo             | 3.20.2    | 3.20.3    | patch           |
| jsonschema          | 0.46.2    | 0.46.5    | patch (dev-dep) |
| js-sys              | 0.3.95    | 0.3.102   | wasm            |
| wasm-bindgen        | 0.2.118   | 0.2.125   | wasm            |
| wasm-bindgen-test   | 0.3.68    | 0.3.75    | wasm dev-dep    |

The wasm-bindgen family is bound together by exact (`=`) version pins
(js-sys `=0.2.x` ↔ wasm-bindgen ↔ wasm-bindgen-test), so a plain
`cargo update -p` group did not move them. They were re-resolved by
raising the version floors in `crates/fsqlite-wasm/Cargo.toml`
(`js-sys = "0.3.102"`, `wasm-bindgen = "0.2.125"`,
`wasm-bindgen-test = "0.3.75"`) then running
`cargo update -p js-sys -p wasm-bindgen -p wasm-bindgen-test`.
This cascaded wasm-bindgen-{macro,macro-support,shared,futures},
web-sys, and wasm-bindgen-test-{macro,shared} to the matching versions,
and pruned the old wit-bindgen/wasm-encoder build tooling that the newer
wasm-bindgen no longer pulls.

### getrandom 0.2.x → 0.4.3 (wasm; major) — APPLIED (trivial, no code)

| Crate / declaration                              | From    | To      |
|--------------------------------------------------|---------|---------|
| fsqlite-wasm (wasm32 dev-dep)                     | 0.4.2   | 0.4.3   |
| fsqlite-ext-misc (wasm32 dep, feature `js`)       | 0.2.x   | 0.4.3   |

The `fsqlite-wasm` crate was already on getrandom 0.4.x with the
`wasm_js` feature, so the major migration was effectively already in
place there — only a patch bump (0.4.2 → 0.4.3) was needed.

For `fsqlite-ext-misc`, getrandom was declared `0.2` with the old `js`
feature but is **never directly `use`d** in any source file (verified:
the only `getrandom` reference in `*.rs` is a string literal in
`fsqlite-types/src/cx.rs`). The 0.2→0.4 jump (which renamed the
JS-backend feature `js` → `wasm_js` and changed the custom-register API)
therefore reduced to a one-line feature-name change in `Cargo.toml`
(`features = ["js"]` → `features = ["wasm_js"]`). No code migration was
required, so it was applied rather than deferred. Its wasm32 getrandom
edge now resolves to 0.4.3.

Note: getrandom 0.2.17 and 0.3.4 still appear in `Cargo.lock` purely as
transitive deps (rand_core 0.6.4 via aes-gcm/asupersync, and ahash); these
flow through franken inter-deps and were left untouched per scope.

### Franken inter-dependency pins — untouched (per scope)

fsqlite*, asupersync, and other franken-ecosystem pins were not modified.

### Validation

- `cargo check --workspace --all-targets` (native) — **clean** (Finished in 2m23s,
  no errors/warnings). Confirms the smallvec/bumpalo/jsonschema patch bumps and the
  re-resolved `Cargo.lock` build natively across the whole workspace.
- `cargo check -p fsqlite-wasm --target wasm32-unknown-unknown` — the wasm dependency
  graph (getrandom 0.4.3 `wasm_js`, wasm-bindgen 0.2.125, js-sys 0.3.102) **resolves
  and compiles up to fsqlite-core**. The build then fails on a **pre-existing,
  unrelated** signature mismatch in committed `crates/fsqlite-core/src/connection.rs`
  (E0061 around `preserve_existing_live_vtabs` / `pending_rootpage_zero_virtual_tables`,
  a vtable/rootpage code path under concurrent development). That file is **not** part
  of this dependency change (working tree shows only the two wasm `Cargo.toml` files +
  `Cargo.lock` + this log), so the wasm break exists with or without these bumps and is
  out of scope for the dependency pass. The dep changes themselves introduce no new
  wasm errors.
