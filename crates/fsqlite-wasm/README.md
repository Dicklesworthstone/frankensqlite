# `fsqlite-wasm`

`fsqlite-wasm` is the Rust crate that produces FrankenSQLite's browser-facing
WebAssembly package.

The intended npm artifact is published as `@frankensqlite/core` and exposes the
generated `wasm-bindgen` glue plus the `FrankenDB` / `FrankenPreparedStatement`
APIs implemented in [`src/lib.rs`](./src/lib.rs).

## Package Build

Build the primary browser ES module package into `target/fsqlite-wasm-pkg/`:

```bash
./scripts/build_fsqlite_wasm_package.sh
```

Choose a different output directory or `wasm-pack` target:

```bash
FSQLITE_WASM_TARGET=bundler ./scripts/build_fsqlite_wasm_package.sh target/fsqlite-wasm-bundler
FSQLITE_WASM_TARGET=nodejs ./scripts/build_fsqlite_wasm_package.sh target/fsqlite-wasm-node
```

The helper script:

- runs `wasm-pack build`
- uses the workspace size-optimized `release` profile (`opt-level = "z"`,
  LTO, one codegen unit, stripped symbols, aborting panics)
- runs `wasm-opt` explicitly after wasm-bindgen output, with Rust's
  bulk-memory and nontrapping-float feature flags enabled, then keeps the
  optimized output only when it is no larger after gzip, without leaving a
  rejected optimizer side artifact in the package directory
- normalizes the generated `package.json` to the `@frankensqlite/core` package name
- copies README/license files into the output package
- validates the generated `.wasm`, `.js`, and `.d.ts` artifacts exist
- can build a minimum-feature core package with
  `FSQLITE_WASM_NO_DEFAULT_FEATURES=1`
- can opt into SQLite image import/export bindings with
  `FSQLITE_WASM_FEATURES=backup`
- can enable opt-in diagnostics such as
  `FSQLITE_WASM_FEATURES=diagnostics,tracing,panic-hook`
- can make optimizer availability explicit with `FSQLITE_WASM_WASM_OPT`
  (`required`, `auto`, or `disabled`)
- can postprocess an existing wasm-bindgen output directory without invoking
  `wasm-pack` or cargo by setting `FSQLITE_WASM_PACKAGE_ONLY=1`
- can refuse local `wasm-pack` execution with
  `FSQLITE_WASM_FORBID_LOCAL_BUILD=1`, which is useful for agent runs where
  cargo-shaped work must happen through `rch`
- strips caller-location file/line/column detail from release/profiling builds
  by default with `-Zlocation-detail=none`
- emits `twiggy-top.txt` before enforcing the gzip budget when `twiggy` is
  available, or requires it with `FSQLITE_WASM_TWIGGY=required`, so over-budget
  failures retain a size-attribution artifact
- writes `frankensqlite_wasm_bg.wasm.gz` and enforces the 800 KB core gzip
  budget by default (`FSQLITE_WASM_MAX_GZIP_BYTES=0` disables the guard)
- writes `size-report.json` with the raw wasm bytes, gzipped wasm bytes,
  wasm-opt retention decision, size budgets, and final npm tarball bytes
- runs `npm pack` so the result is ready for registry or local install testing
- enforces a packed tarball size budget of 2 MiB by default (`FSQLITE_WASM_MAX_PACKED_BYTES=0` disables the guard)

The default crate feature set is intentionally empty so the core browser
package does not carry crash-reporting or diagnostics glue. Minimum-core release
wasm builds compile tracing at `error` level only and strip caller-location
detail to keep metadata out of the core download. The default browser package
leaves FrankenSQLite-specific observability PRAGMAs and browser-facing
introspection exports out of the core transfer. Enable the `diagnostics` feature
when a build needs `parseSql()`, `db.path`, `db.explain()`,
prepared-statement `explain()`, prepared-statement metadata getters (`stmt.sql`,
`stmt.columnCount`, `stmt.columnNames()`), `db.memoryStats()`,
`PRAGMA fsqlite.jit_stats`,
`PRAGMA fsqlite.cache_stats`, `PRAGMA fsqlite.txn_stats`, lineage, SSI,
JavaScript NaN coercion warnings, or diagnostic error recovery fields such as
`transient`, `userRecoverable`, and `suggestion`, query-result `changes`
placeholders, richer JavaScript value-type descriptions in error messages, or
other debug/advisor surfaces. Diagnostics builds also retain the expanded
out-of-memory advisory text with memory knob names; the default core package
uses a compact out-of-memory message. Default JavaScript errors still include `code`,
`sqliteCode`, `extendedCode`, and `message`; default prepared statements still
keep `execute()` and `query()` available. The `tracing` feature is also opt-in
because it restores warning-level tracing and pulls in extra browser logging
glue. The `panic-hook` feature is available for browser
crash reports when a larger diagnostic package is acceptable:

```bash
FSQLITE_WASM_FEATURES=diagnostics,tracing,panic-hook ./scripts/build_fsqlite_wasm_package.sh
```

The `backup` feature is separate from diagnostics. Enable it when the browser
package needs `FrankenDB.import()`, `FrankenDB.importWithOptions()`, or
`db.export()` for SQLite image round-trips. The minimum core package omits those
backup bindings and keeps the common in-memory `open`/`execute`/`query` surface.

## Size Budgets

All release packages must emit the raw `.wasm`, a gzipped `.wasm.gz`, and a
Twiggy top report in CI. The helper also writes `size-report.json` so CI and
manual runs preserve the exact wasm-opt decision, raw/gzip bytes, active
budgets, Twiggy report path, and packed archive size next to the package
artifacts. Twiggy runs before the gzip-budget failure path, so an over-budget
core package can still be diagnosed from the same output directory. The core
package budget is enforced against the gzipped WebAssembly artifact because that
is the browser transfer shape.

| Feature combo | Build command | Gzip budget |
| --- | --- | --- |
| Minimum core | `FSQLITE_WASM_NO_DEFAULT_FEATURES=1 FSQLITE_WASM_TWIGGY=required ./scripts/build_fsqlite_wasm_package.sh` | `800000` bytes |
| Default core | `FSQLITE_WASM_TWIGGY=required ./scripts/build_fsqlite_wasm_package.sh` | `800000` bytes |
| Diagnostics | `FSQLITE_WASM_FEATURES=diagnostics,tracing FSQLITE_WASM_TWIGGY=required ./scripts/build_fsqlite_wasm_package.sh` | `800000` bytes unless the release owner intentionally raises `FSQLITE_WASM_MAX_GZIP_BYTES` |
| Extension bundle | `FSQLITE_WASM_FEATURES=extensions FSQLITE_WASM_TWIGGY=required ./scripts/build_fsqlite_wasm_package.sh` | report-only until each extension has its own tracked budget; set `FSQLITE_WASM_MAX_GZIP_BYTES=0` for exploratory measurement |

Manual measurement should use the package helper so the post-bindgen `wasm-opt`
flags and gzip-based artifact selection match CI:

```bash
FSQLITE_WASM_TWIGGY=required ./scripts/build_fsqlite_wasm_package.sh target/fsqlite-wasm-pkg
wc -c target/fsqlite-wasm-pkg/frankensqlite_wasm_bg.wasm.gz
twiggy top target/fsqlite-wasm-pkg/frankensqlite_wasm_bg.wasm
```

When a remote `rch` build or CI job has already produced a wasm-bindgen package
directory, agents can run the package checks without re-entering cargo:

```bash
FSQLITE_WASM_FORBID_LOCAL_BUILD=1 \
FSQLITE_WASM_PACKAGE_ONLY=1 \
FSQLITE_WASM_WASM_OPT=disabled \
FSQLITE_WASM_TWIGGY=disabled \
FSQLITE_WASM_MAX_GZIP_BYTES=0 \
./scripts/build_fsqlite_wasm_package.sh /path/to/wasm-bindgen-output
```

Package-only mode refuses settings that affect the earlier cargo or wasm-pack
build, not postprocessing: `FSQLITE_WASM_TARGET`, `FSQLITE_WASM_MODE`,
`FSQLITE_WASM_SCOPE`, `FSQLITE_WASM_PROFILE`,
`FSQLITE_WASM_STRIP_LOCATION_DETAIL`, `FSQLITE_WASM_FEATURES`, and
`FSQLITE_WASM_NO_DEFAULT_FEATURES`. Build the desired target/profile/feature set
first, then point the helper at that output directory. Because package-only mode
cannot know which Rust flags produced the prebuilt `.wasm`, `size-report.json`
sets `stripLocationDetail` to `null` for package-only runs.

## Expected Package Contents

- `frankensqlite_wasm_bg.wasm`
- `frankensqlite_wasm.js`
- `frankensqlite_wasm.d.ts`
- `snippets/`
- `README.md`
- `LICENSE`

## Import Example

```ts
import init, { FrankenDB } from "@frankensqlite/core";

await init();

const db = new FrankenDB(":memory:");
db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)");
db.execute("INSERT INTO users(name) VALUES('Ada')");

const result = db.query("SELECT id, name FROM users ORDER BY id");
console.log(result.rows);
```

## WASM Memory Management

FrankenSQLite's WASM package runs inside the browser's WebAssembly linear
memory, so the hard upper bound remains 4 GiB for the whole module. The
database-specific knobs exposed by `FrankenDB.openWithOptions()` and
the backup-feature `FrankenDB.importWithOptions()` let you budget
FrankenSQLite's own heap usage inside that ceiling:

```ts
const db = FrankenDB.openWithOptions(":memory:", {
  pageBufferMax: 256,
  memory: {
    initialPages: 4,
    growthChunkPages: 1,
    maxPages: 512,
  },
});
```

- `pageBufferMax` caps the pager's page-buffer pool in pages.
- `memory.initialPages`, `memory.growthChunkPages`, and `memory.maxPages`
  express the same policy in WebAssembly pages (`64 KiB` each). The byte-level
  aliases `memory.initialReserveBytes`, `memory.growthChunkBytes`, and
  `memory.maxBytes` remain available when callers want exact byte counts.
- `memory.maxPages` / `memory.maxBytes` act as a hard cap for tracked
  `MemoryVfs` heap usage. When the engine crosses that cap, operations fail
  with a structured out-of-memory error instead of trapping through an
  `unreachable`.
- With `FSQLITE_WASM_FEATURES=diagnostics`, `memory.warnAtPercent` derives a
  warning threshold from the tracked max, `memory.warningThresholdBytes` accepts
  exact byte thresholds, and `memory.onWarning` fires once with the same
  byte-level and page-oriented payload as `db.memoryStats()`.
- Diagnostic builds also expose `db.memoryStats()` and emit
  page-cache pressure advisory fields:
  `pageCachePressureLevel`, `pageCachePressureBudgetBytes`,
  `recommendedPageBufferMaxPages`, `recommendedPageBufferMaxBytes`, and
  `trackedHeadroomBytes`. These let the JS side decide when to ratchet
  `pageBufferMax` down before the tracked heap reaches its hard cap.

Diagnostic builds can call `db.memoryStats()` at any point to inspect tracked
heap bytes, page-cache resident bytes, page-cache capacity, configured warning
thresholds, growth events, current linear-memory size/pages (when running under
`wasm32`), and the derived page-cache pressure recommendation.
