# v0.3.6 release notes — DRAFT (changelog input)

> **Status: DRAFT / prep only.** This is the curated `v0.3.5..HEAD` delta staged
> as input for `CHANGELOG.md`. **No version bump has been applied** — the
> workspace is still at `0.3.5`. Integrate the `## [0.3.6]` block below into
> `CHANGELOG.md`, then bump the version surface (see the appendix) as a separate
> step.
>
> Range: `v0.3.5` (`92a4e4e73`, 2026-08-17) .. `HEAD` (2026-08-19) — **539
> commits** (145 `fix`, 40 `feat`, 65 `test`, 6 `perf`, 241 `chore`/beads).
> **54 unique GitHub issues** referenced. Commit hashes cited are short SHAs on
> `main`; every headline change carries a keeper test and a tracking bead.
>
> ⚠️ **CHANGELOG gap to reconcile:** `CHANGELOG.md` currently documents through
> `[0.3.1]` even at the `v0.3.5` tag — releases **0.3.2, 0.3.3, 0.3.4, 0.3.5**
> were tagged without changelog entries. This draft covers only the
> `v0.3.5..HEAD` (0.3.6) delta the user scoped; the 0.3.2–0.3.5 backfill is a
> separate task.

---

## [0.3.6] -- 2026-08-19 (fresh-eyes correctness campaign; UTF-16 read/write completion; SSI livelock + eviction-race fixes; FTS5 lazy contentless lifecycle; printf conformance; asupersync 0.4.8)

Post-0.3.5 hardening wave. A multi-lane "fresh-eyes" review campaign audited the
recently-landed code and drove a batch of regression fixes — **2 P0s** (a
namespace-lease fail-open split-brain and an SSI writer livelock) and a series
of **P1** SQL-semantics and encoding regressions — all with keeper tests and
oracle differentials. The UTF-16 read/write family reached end-to-end parity
(writes enabled, every decode/serialize site threaded through the database text
encoding), the SSI committed-witness machinery closed a livelock and two
eviction races, FTS5 gained a lazy contentless lifecycle plus external-content
correctness, and `printf` conformance was brought to C-SQLite dtoa parity.
Dependency baseline moved to asupersync `0.4.8`.

### Added

- **UTF-16 database writes** are enabled end-to-end — guard flip, `PRAGMA
  encoding` setter, and VACUUM hydration decode (bd-bld9w.7 capstone,
  `83707829e`), on top of `TextEncoding::is_write_supported` and text-encoding
  threading through header-metadata writes (`1d8befd25`). Closes the
  RELEASE-P0 UTF-16 lifecycle epic (bd-bld9w).
- **WITHOUT ROWID, non-leading PRIMARY KEY**: on-disk and VACUUM file-format
  parity for tables whose PK is not the first column (bd-v6pjf, `331433a05`).
  (The read/write enablement itself landed as the `v0.3.5` tag commit and is
  therefore part of 0.3.5, not this delta.)
- **Forward-compat format guard**: `Connection::open` refuses to open a
  `.fsqlite` written by a newer build than it understands (`24e56a96b`),
  plus a one-time idempotent first-open repair pass for upgraders
  (bd-zywqc.5, `f32ce2aa6`).
- **`UPDATE ... FROM` with an OUTER JOIN source** (GH#250, `d7099fa61`).
- **FTS5 surface**: `fts5vocab` virtual-table module (GH#271, `6cf73719e`);
  doclist stitching across leaf-page continuations (GH#360, `fd0fed9e8`);
  lazy segment merge so segment count stays bounded (`654621ac5`); and the
  `integrity-check` / `flush` / `delete-all` maintenance commands for lazy
  contentless tables.
- **PRAGMA coverage**: `optimize` via ANALYZE (GH#251, `b9feb25fc`),
  `function_list` (GH#206), `pragma_compile_options()` table-valued function
  (GH#207), `reverse_unordered_selects` (GH#236), per-connection `cache_spill`
  (GH#275), process-global `soft_heap_limit`/`hard_heap_limit` (GH#280), and
  recognition of `journal_size_limit`.
- **Namespace lifecycle**: `quiesce()`/`guard_generation()` teardown with
  generation-bound sidecar release (bd-97kjm, `c1ab88b39`), wired into
  `validate_namespace_binding` (bd-9hp58, `2c5d14fca`).
- **Tooling / platform**: a `beads-doctor` crate with cross-platform periodic
  probes (bd-316l0); macOS + Windows process-liveness probes with reuse-safe
  birth tokens for crash cleanup (bd-4dr7g); and a CLI `.mode quote` matching
  the `sqlite3` SQL-literal output.

### Fixed

**UTF-16 read/write encoding family** — every decode/serialize site now threads
the database's storage encoding instead of hardcoding UTF-8:

- Wide (>64-column) records decode under the DB encoding (bd-7c6g7 #1,
  `41e712146`); spilled external-merge sorter keys (#5, `721ef918e`); the
  bounded integrity check's row decode + expected-key serialization (#6,
  `ec138b616`); and the unique-index blind-append comparison (#7,
  `e4ba0419b`).
- `sqlite_master` (type/name/tbl_name/sql) decodes under the DB encoding, so a
  UTF-16 database no longer garbles schema names and mis-fires a spurious
  first-open migration/repair (bd-1v1pl, `dd012ebe0`).
- UTF-16 index-key prefix scans and blind-append (bd-oqglk/bd-nmd19,
  `a7b29d683`); FK parent-key probes (bd-cquyy, `a6dce85c5`);
  expression-index `integrity_check` false-corruption (bd-7c6g7.6);
  `octet_length` honoring the DB encoding (bd-iubwb); write-path decode sites
  (bd-o3rz4); and cross-encoding `ATTACH` rejection to match stock (bd-6n5cy).

**SSI livelock + concurrency races**:

- **P0** — concurrent writers no longer livelock on a read-only SSI witness
  pileup: read-only commit witnesses no longer poison every later writer via a
  monotonic lost-below watermark (bd-stujd, `7c30f7af9`).
- Two committed-witness eviction races closed (bd-0iree, bd-9orb5,
  `2e4f62859`); SSI witness gaps and edge discovery on the direct
  `TransactionManager` commit path (bd-cht52, `b873781a4`; GH#189).
- **P0** — namespace generation-guard fails *closed* on a transient stat
  probe error instead of releasing the lease and admitting a second
  cross-process writer (split-brain) (bd-ep8y9, `8fa4b1c99`).
- CAS-safe savepoint rewind of the shared concurrent rowid allocator (GH#147);
  plus committed-freelist reclamation / double-grant and concurrent-writer EOF
  growth hardening.

**Fresh-eyes SQL-semantics regressions** (P1 unless noted):

- `INDEXED BY` partial-index cover check corrected in both directions —
  null-rejecting comparisons imply `IS NOT NULL`, OR branches recurse, outer
  `ON` conjuncts are excluded, and the check extends to UPDATE/DELETE
  (bd-wlo29, `350af08c3`); DELETE truncate-opt path no longer over-rejects a
  bare `DELETE ... INDEXED BY` on a partial index (bd-0fz9d).
- `min()`/`max()` with bare columns or subqueries in a mixed output expression
  now source from the extremum row and keep FILTER/COLLATE (bd-3radn H2/M5,
  `ab5104bcd`/`15d34d33d`; bd-0174u); the nested `min()`/`max()` VALUE is
  computed under its argument's collation, matching the extremum row
  (bd-89z48/bd-9vtbh, `1cfee0eb3`).
- Row-value comparison of subqueries in the interpreted (FROM-less/CTE-routed)
  evaluator (bd-t7oeo, `8e5e70558`); fail-open row-value width when a peer is
  `SELECT *` (bd-wpiq6 M8, `ae5285d8c`).
- A never-true `<const> AND E` filter fold no longer swallows the prepare-time
  errors stock raises (only a literal integer `0` folds before name/aggregate
  resolution) (bd-kcvra, `c04fca2b7`); NOT MATERIALIZED CTE pre-expansion
  honors lexical shadowing (bd-9tcne, `cb296ecf4`).

**`printf` conformance** (C-SQLite dtoa parity):

- Review-tail batch M9/M10/L1/L2/L3 (bd-9zzr0, `a33f9d935`); `%f`/`%e`/`%g`
  match SQLite's 16-significant-figure dtoa cap at high precision (bd-o8m86,
  `6e6c0d372`); `%.0c` + dynamic-precision `%.*d` (bd-77dkj); alt-form `#`/`!`
  on floats, field width on `%%`/`%c`/`%q`/`%Q`/`%w`, the `,` thousands flag,
  and round-ties-away-from-zero.

**Format / durability / FTS5 integrity**:

- `Connection::open` returns CANTOPEN (not CORRUPT) for a newer on-disk format
  (`19fa25042`); stale WAL-cert replay across a file replacement is rejected
  via `db_file_id` identity binding (bd-85x9y, GH#364).
- FTS5 external-content reload must not rebuild the index from the content
  table (bd-fts5-lazy-shadow-reads-itcc4.3, `d3ec0f1a9`); `delete-all` is
  allowed on external-content tables (bd-i3ldw, `f6f864a7c`).

### Performance

- Lock-free sharded MVCC version-arena: **+64% concurrent version-publish
  throughput at 8 threads** (bd-5kgie, `27d3501ae`).
- Restored the memdb schema-cookie fast-path on a dirty mirror (bd-ixf69,
  `db1cf5eb0`); shared wide-TEXT decode between the lazy cache and the register
  (`b42fb8b05`). See `docs/perf-*` and the bench-methodology docs for the
  measured workloads.

### Dependencies

- asupersync and the `franken-*` companion crates: `0.4.5` → **`0.4.8`**
  (`508c77f86`).

---

## Appendix A — version-bump surface (pre-check; DO NOT bump yet)

`rg '0\.3\.5' Cargo.toml crates/*/Cargo.toml` → **29 files, 55 lines**:

- **28** per-crate `version = "0.3.5"` declarations (line 3 of each
  `crates/*/Cargo.toml`). The workspace now has 28 crates under `crates/`,
  including the newer `beads-doctor` and `fsqlite-wasm`. Crates **hardcode**
  the version — there is no `version.workspace = true` inheritance — so each
  must be edited individually.
- **27** internal workspace-dependency pins in root `Cargo.toml` (lines
  127–153), each `version = "0.3.5"`. (`beads-doctor` is a standalone crate,
  not listed among the root workspace dependencies, hence 27 pins vs 28
  crates.)

Bump procedure at release time (28 crate `version` lines + 27 root dep pins →
`0.3.6`, then `cargo update -w` to refresh `Cargo.lock`). Also grep the tree for
any hardcoded `0.3.5` version strings in code/docs/tests before tagging.

## Appendix B — release checklist notes

- Integrate the `## [0.3.6]` block above into `CHANGELOG.md` (newest-first,
  under the `# Changelog` intro).
- Reconcile the CHANGELOG gap: 0.3.2–0.3.5 tags have no changelog entries.
- Update the "27-member Cargo workspace" phrasing in `CHANGELOG.md`'s intro and
  `AGENTS.md` if the crate count (now 28) is being corrected.
- Verify the full suite + parity gates on the release preflight target, then
  tag `v0.3.6` and push `main:master`.
