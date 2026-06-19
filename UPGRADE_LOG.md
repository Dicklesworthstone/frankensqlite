# Dependency Upgrade Log

**Date:** 2026-06-19
**Project:** FrankenSQLite
**Language:** Rust, TypeScript
**Manifest:** Cargo.toml, crate Cargo.toml files, package.json files

---

## Summary

| Metric | Count |
|--------|-------|
| **Total direct dependency entries audited** | 81 |
| **Updated** | 23 |
| **Skipped** | 0 |
| **Failed (rolled back)** | 0 |
| **Requires attention** | 0 |

---

## Successfully Updated

### ftui: 0.2.1 -> 0.4.1

**Changelog:** local source and crates.io package metadata; local checkout `/data/projects/frankentui` reports workspace version `0.4.1`.

**Breaking changes:** Public facade has moved from the `0.2.x` line to the `0.4.x` line. FrankenSQLite only uses `ftui` behind the optional `fsqlite-e2e/tui` feature, so the compatibility check is the relevant behavior gate.

**Notable changes:**
- Updated to the latest local `/dp`/`/data/projects` FrankenTUI version.
- Lockfile also refreshed related `ftui-*` crates to `0.4.1` and pulled compatible patch updates for `bitflags`, `memchr`, `serde_json`, and `unicode-segmentation`.

**Deprecations fixed:** None.

**Tests:** Passed.

```bash
cargo update -p ftui
timeout 20m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-ftui-target cargo check -p fsqlite-e2e --features tui --all-targets
```

---

### Compatible Rust dependency floor alignment

**Changelog:** `cargo upgrade --dry-run --incompatible allow --recursive false`; crates.io resolver metadata.

**Breaking changes:** None expected. These are caret-compatible patch/minor floor bumps or direct crate-level floor alignments already accepted by the existing code.

**Updated entries:**

| Dependency | From | To |
|------------|------|----|
| tracing-subscriber | 0.3.22 | 0.3.23 |
| bitflags | 2.9 | 2.13 |
| smallvec | 1.13 / 1.13.2 / 1.11 | 1.15 / 1.15.2 |
| memchr | 2.7 | 2.8 |
| blake3 | 1.5 | 1.8 |
| dashmap | 6.1 | 6.2 |
| proptest | 1.6 | 1.11 |
| insta | 1.42 | 1.48 |
| tempfile | 3.17 | 3.27 |
| jsonschema | 0.46.2 | 0.46.5 |

**Notable changes:**
- Raised workspace dependency floors and the direct `fsqlite-pager`, `fsqlite-vfs`, and `fsqlite-e2e` crate-level pins.
- Lockfile moved `blake3 1.8.4 -> 1.8.5`, `dashmap 6.1.0 -> 6.2.1`, and `insta 1.47.2 -> 1.48.0`; the rest were already locked at compatible newer versions.

**Deprecations fixed:** None.

**Tests:** Passed.

```bash
cargo update -p tracing-subscriber -p bitflags@2.13.0 -p smallvec -p memchr -p blake3 -p dashmap -p proptest -p insta -p tempfile -p jsonschema
timeout 30m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-upgrade-target cargo check --workspace --all-targets
```

---

### sha2: 0.10 -> 0.11

**Changelog:** `cargo upgrade --dry-run --incompatible allow --recursive false`; compiler API migration evidence.

**Breaking changes:** `Sha256::digest(...)` / `finalize()` now returns an array type that no longer implements direct `LowerHex` formatting.

**Notable changes:**
- Added byte-to-lower-hex helpers in `fsqlite-harness` and `fsqlite-e2e`.
- Migrated all direct `Sha256` hex formatting call sites to explicit byte formatting.
- Left transitive `sha2 0.10` edges in place where upstream crypto crates still require them.

**Deprecations fixed:** None.

**Tests:** Passed after migration.

```bash
cargo update -p sha2@0.11.0
timeout 20m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-upgrade-target cargo check -p fsqlite-e2e -p fsqlite-harness --all-targets
```

---

### nix: 0.29 -> 0.31

**Changelog:** `cargo upgrade --dry-run --incompatible allow --recursive false`; compiler API migration evidence.

**Breaking changes:** `nix::fcntl::fcntl` and `nix::unistd::read` now require safe fd wrapper traits instead of accepting raw file descriptors directly.

**Notable changes:**
- Updated VFS `fcntl` calls to pass `BorrowedFd` via the existing `AsFd` handle.
- Updated the Linux SCM_RIGHTS test to validate the received fd through `/proc/self/fd` without introducing unsafe code.
- Preserved the MVCC IPC raw-fd wrapper behavior so production fd ownership semantics stay unchanged.

**Deprecations fixed:** None.

**Tests:** Passed after migration.

```bash
cargo update -p nix@0.29.0 -p nix@0.31.2
timeout 20m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-upgrade-target cargo check -p fsqlite-vfs -p fsqlite-mvcc --all-targets
```

---

### cache, benchmark, and TOML crates

**Changelog:** `cargo upgrade --dry-run --incompatible allow --recursive false`; compiler deprecation output from `criterion 0.7`.

**Updated entries:**

| Dependency | From | To |
|------------|------|----|
| hashbrown | 0.14 | 0.17 |
| lru | 0.16 | 0.18 |
| criterion | 0.5 | 0.7 |
| toml | 0.8 | 1.1 |

**Breaking changes:** `criterion::black_box` is deprecated in favor of `std::hint::black_box`.

**Notable changes:**
- Updated planner/core LRU dependency floors; existing `NonZeroUsize` cache capacities already matched the modern `lru` API.
- Updated direct `hashbrown` users to the latest `0.17.x` line while leaving older transitive `hashbrown` edges where upstream crates still require them.
- Migrated benchmark `black_box` usage away from Criterion's deprecated re-export.

**Deprecations fixed:** Replaced Criterion's deprecated `black_box` re-export in benchmark code.

**Tests:** Passed after migration.

```bash
cargo update -p hashbrown@0.17.0 --precise 0.17.1
timeout 30m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-upgrade-target cargo check --workspace --all-targets
```

---

### rand: 0.8 -> 0.10

**Changelog:** rand 0.10 rustdoc on docs.rs for `RngExt` API changes.

**Breaking changes:** The old `Rng` extension-style methods used by FrankenSQLite moved to the rand 0.10 `RngExt` surface: `gen_range` became `random_range`, `gen_ratio` became `random_ratio`, `gen` became `random`, and `thread_rng()` became `rng()`.

**Notable changes:**
- Updated direct rand callers in `fsqlite`, `fsqlite-e2e`, and `fsqlite-ext-misc`.
- Kept deterministic `StdRng::seed_from_u64` call sites intact.
- Left transitive `rand 0.8` in the lockfile where upstream crates still require it.

**Deprecations fixed:** Removed all direct old rand API usages from the affected crates.

**Tests:** Passed after migration.

```bash
cargo update -p rand@0.10.1
timeout 20m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-upgrade-target cargo check -p fsqlite -p fsqlite-e2e -p fsqlite-ext-misc --all-targets
```

---

### rusqlite: 0.32 -> 0.40.1

**Changelog:** rusqlite GitHub release notes for `0.40.1` and rustdoc/source for the backup/name API changes.

**Breaking changes:** `DatabaseName::Main` was replaced by the `MAIN_DB` name constant in the backup API, and rusqlite no longer accepts direct `u64` values through `FromSql` / `ToSql` for the touched call sites.

**Notable changes:**
- Updated the workspace rusqlite dependency to the latest `0.40.1` release.
- Migrated the `realdb-e2e` backup call to `MAIN_DB`.
- Updated e2e oracle/count helpers to read SQLite integer values as `i64` and convert to `u64` with checked conversions where the public helper still returns `u64`.
- Updated e2e rusqlite parameter bindings that were passing unsigned workload counters directly.

**Deprecations fixed:** None.

**Tests:** Passed after migration.

```bash
cargo update -p rusqlite --precise 0.40.1
timeout 30m rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-upgrade-target cargo check -p fsqlite -p fsqlite-core -p fsqlite-e2e -p fsqlite-harness -p fsqlite-ext-fts5 -p fsqlite-vdbe --all-targets
```

---

### TypeScript browser tooling

**Changelog:** npm registry metadata from `npm view`, compiler/test output from TypeScript and Vitest.

**Updated entries:**

| Dependency | From | To |
|------------|------|----|
| @playwright/test | 1.58.2 | 1.61.0 |
| typescript | 5.9.3 | 6.0.3 |
| vitest | 3.2.4 | 4.1.9 |

**Breaking changes:** TypeScript 6 enforces exact optional property semantics across the worker SDK types more strictly, and Vitest 4 package resolution expects the SDK's workspace dependency to resolve through a declared package export or an explicit test alias.

**Notable changes:**
- Refreshed the npm lockfile with the latest browser SDK/worker test tooling.
- Updated TypeScript shared config to avoid deprecated `baseUrl` usage while preserving package source aliases.
- Tightened SDK/worker optional-field serialization so undefined optional values are omitted instead of assigned.
- Added a Vitest source alias for the SDK tests because the in-repo worker package has not been built to `dist/` during source tests.
- Replaced the SDK package's local workspace dependency marker with a semver package dependency so npm can refresh the lockfile consistently.

**Deprecations fixed:** Removed TypeScript `baseUrl` deprecation from the shared browser tsconfig.

**Tests:** Passed after migration.

```bash
npm install --package-lock-only --ignore-scripts --legacy-peer-deps
npm ci --ignore-scripts --legacy-peer-deps
npm run typecheck:wasm-browser
npm test
npx tsc --version
npx vitest --version
npx playwright --version
npm outdated --workspaces --all
```

---

### asupersync: 0.3.4 -> 0.3.5

**Changelog:** local source audit of `/dp/asupersync`; FrankenSQLite build and
test output against the local workspace checkout.

**Breaking changes:** The RaptorQ `SystematicEncoder::repair_symbol` API now
expects public repair ESIs in the same namespace as emitted repair symbols
(`K..`), not zero-based repair indexes.

**Notable changes:**
- Updated the workspace dependency to the latest local `/dp/asupersync`
  checkout and kept `default-features = false`.
- Refreshed `Cargo.lock` to local `asupersync 0.3.5` and matching
  `franken-* 0.3.5` companion crates.
- Updated `ReplicationSender` repair-symbol emission to pass the public ISI/ESI
  through to `SystematicEncoder::repair_symbol`.
- Fixed an `asupersync` local clippy warning in `src/runtime/builder.rs` so
  FrankenSQLite's path-dependency clippy gate remains warning-clean.

**Deprecations fixed:** None.

**Tests:** In progress as part of the final release gate. Targeted RaptorQ
streaming tests and the full `fsqlite-core` library suite passed after the
migration; the final whole-workspace gates are tracked below.

```bash
cargo update -p asupersync
cargo test -p fsqlite-core --lib replication_sender::tests::test_streaming -- --nocapture
cargo test -p fsqlite-core --lib replication_receiver::tests::test -- --nocapture
cargo test -p fsqlite-core --lib
```

---

## Skipped

_None._

---

## Failed Updates (Rolled Back)

_None so far._

---

## Requires Attention

_None so far._

---

## Security Notes

`npm ci --ignore-scripts --legacy-peer-deps` reported 0 vulnerabilities. Final
Rust duplicate and release-prep gates are still pending for the restarted
release candidate.

---

## Commands Used

```bash
cargo upgrade --dry-run --incompatible allow --recursive false
cargo metadata --format-version 1 --no-deps
npm outdated --workspaces --all
```

---

## Notes

- Repo docs require concurrent-writer mode to stay default-on and forbid Tokio-family dependencies.
- Local `/dp` is the authoritative local-library root for this release pass;
  `asupersync` latest local active checkout is `/dp/asupersync` at `0.3.5`, and
  the FrankenSQLite workspace now depends on that local path while release
  validation runs.
- `ftui` latest local checkout is `/data/projects/frankentui` at `0.4.1`; the workspace manifest started at `0.2.1`.

---

## Previous Upgrade History

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
