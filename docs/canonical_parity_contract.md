# Canonical Parity Contract

**Bead:** `bd-2yqp6.1`  
**Purpose:** define the canonical parity boundary between FrankenSQLite and C
SQLite so runtime work, harness work, and documentation all target the same
surface.

## Authority

This document is the human-readable interpretation of the Track A contract.
The machine-readable sources of truth are:

1. [`docs/contracts/sqlite_version_contract.toml`](contracts/sqlite_version_contract.toml)
   Version target and report contract.
2. [`docs/contracts/supported_surface_matrix.toml`](contracts/supported_surface_matrix.toml)
   Supported vs partial vs excluded surface.
3. [`docs/contracts/feature_universe_ledger.toml`](contracts/feature_universe_ledger.toml)
   Per-component feature decomposition, lifecycle, and evidence links.
4. [`docs/contracts/parity_taxonomy.toml`](contracts/parity_taxonomy.toml)
   Weighted feature taxonomy and intentional exclusions.
5. [`docs/contracts/corpus_manifest.toml`](contracts/corpus_manifest.toml)
   Canonical fixture roots, content hashes, and corpus cardinality floors.
6. [`docs/contracts/parity_score_contract.toml`](contracts/parity_score_contract.toml)
   Exact meaning of a "100% parity" claim.
7. [`crates/fsqlite-harness/src/canonical_parity_contract.rs`](../crates/fsqlite-harness/src/canonical_parity_contract.rs)
   Sole five-contract path registry, executable bundle loader, and authority
   drift validator. The parity-score contract remains an additional Track A
   input, but it is not one of the five formerly duplicated root contracts.

If this document and those files diverge, the repo is in contract drift and the
drift must be fixed. The correct response is to reconcile the files, not to
reinterpret the contract ad hoc.

### Root Copies Are Inert

The five same-named TOML files at the repository root are not contract mirrors
or fallback locations. Each is an `fsqlite.inert_contract_pointer.v1` document
whose historical payload is retained as an inert literal string. Its pointer
names the sole `docs/contracts/` authority and pins that file's SHA-256. A
consumer must never parse the historical payload as a live contract.

| Logical contract | Canonical authority | Inert root pointer |
| --- | --- | --- |
| Supported surface | `docs/contracts/supported_surface_matrix.toml` | `supported_surface_matrix.toml` |
| Feature universe | `docs/contracts/feature_universe_ledger.toml` | `feature_universe_ledger.toml` |
| Parity taxonomy | `docs/contracts/parity_taxonomy.toml` | `parity_taxonomy.toml` |
| SQLite version | `docs/contracts/sqlite_version_contract.toml` | `sqlite_version_contract.toml` |
| Corpus roots | `docs/contracts/corpus_manifest.toml` | `corpus_manifest.toml` |

Run `scripts/verify_canonical_contract_authority.sh --json` to validate the
registry, specialized constants, canonical TOML, root-pointer hashes, and all
bare references in code, tests, scripts, and workflows. The report classifies
the non-read references inventoried in
`docs/contracts/turso_test_adaptation_inventory.toml`, including synthetic
temporary-workspace paths, report artifact names, serialized fixtures, and
workflow triggers. An unregistered live root read fails the guard.

## Validation Surface

Track A contract drift is guarded by deterministic harness tests:

- [`crates/fsqlite-harness/tests/bd_2yqp6_1_1_supported_surface_matrix.rs`](../crates/fsqlite-harness/tests/bd_2yqp6_1_1_supported_surface_matrix.rs)
- [`crates/fsqlite-harness/tests/bd_2yqp6_1_2_feature_universe_ledger.rs`](../crates/fsqlite-harness/tests/bd_2yqp6_1_2_feature_universe_ledger.rs)
- [`crates/fsqlite-harness/tests/bd_2yqp6_1_3_sqlite_version_contract.rs`](../crates/fsqlite-harness/tests/bd_2yqp6_1_3_sqlite_version_contract.rs)
- [`crates/fsqlite-harness/tests/bd_2yqp6_1_4_parity_score_contract.rs`](../crates/fsqlite-harness/tests/bd_2yqp6_1_4_parity_score_contract.rs)

These tests are part of the contract itself. Any Track A doc or TOML update
must keep them green rather than relying on manual interpretation.

For the built-in SQL function surface, the runtime-authoritative inventory lives
in
[`crates/fsqlite-func/src/lib.rs`](../crates/fsqlite-func/src/lib.rs) via
`builtin_function_surface_inventory()`. Track E function parity work should
derive scalar/aggregate/window coverage from that inventory rather than from a
hand-maintained matrix copy.

## Boundary

This contract applies only to the current user-facing compatibility runtime:

- The pager-backed, SQLite-file-compatible execution path.
- Behavior compared against C SQLite through the native Rust differential
  harness and `rusqlite` oracle runs.
- The declared supported surface in the surface matrix.

This contract does **not** apply to:

- Native / ECS / content-addressed / time-travel design work.
- FrankenSQLite-only observability, JIT, conflict, checkpoint-advisor, or
  repair telemetry PRAGMAs.
- Parser-only support that has not been promoted into the supported surface
  matrix.
- Experimental or benchmark-only behavior.

## Scope States

The contract uses three states from
[`supported_surface_matrix.toml`](contracts/supported_surface_matrix.toml):

- `supported`: in scope for parity promises.
- `partial`: in scope for implementation and differential closure, but not yet
  eligible for a strict parity claim.
- `excluded`: out of scope until explicitly promoted.

`partial` and `excluded` are not harmless bookkeeping. Under
[`parity_score_contract.toml`](contracts/parity_score_contract.toml), partial features
lower the parity score and excluded features count as coverage debt, which means
they block a strict "100% parity" claim until closed out.

## Version Contract

The declared Track A target is SQLite `3.52.0`, as pinned in
[`sqlite_version_contract.toml`](contracts/sqlite_version_contract.toml) and mirrored
by `FRANKENSQLITE_SQLITE_VERSION`.

Contract rule:

- Any C SQLite oracle used for parity evidence must match the declared contract
  target, or the run is version-drifted.
- `Cargo.lock` is part of the audit surface for this rule because it pins the
  active `rusqlite` / `libsqlite3-sys` oracle dependency chain.
- A version-drifted oracle may still be useful for local debugging, but it does
  not justify a strict parity claim.
- Differential reports must carry the contract reference path, not just an
  engine version string.

## In-Scope Surface

These surfaces are currently inside the canonical parity contract.

| Feature ID | Surface | State | Contract Meaning |
| --- | --- | --- | --- |
| `SURF-SQL-CORE-001` | Core SQL DDL/DML | `supported` | `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, `SELECT` are first-class parity surface. |
| `SURF-SQL-COMPOUND-002` | Compound queries and nested subqueries | `supported` | `UNION`, `INTERSECT`, `EXCEPT`, and nested `SELECT` behavior are part of parity work. |
| `SURF-TXN-MVCC-CONCURRENT-006` | Page-level MVCC concurrent writers | `supported` | FrankenSQLite-specific default concurrency behavior is part of the contract and must remain on by default. |
| `SURF-WAL-CRASH-RECOVERY-008` | WAL crash recovery and checkpoint behavior | `supported` | Recovery semantics and WAL maintenance are parity-critical storage behavior. |
| `SURF-FUNC-CORE-011` | Core built-in scalar and aggregate functions | `supported` | Baseline SQL function behavior is in scope. |
| `SURF-CONFORMANCE-NATIVE-019` | Native Rust differential conformance harness | `supported` | This is the required proof mechanism for parity, not an optional extra. |
| `SURF-PRAGMA-CORE-009` | Core SQLite-compatible PRAGMA subset | `partial` | Only the explicitly named PRAGMAs below are in scope today. |
| `SURF-FUNC-WINDOW-012` | Window-function parity | `partial` | Tracked inside parity work, but not yet claim-clean. |
| `SURF-EXT-JSON1-013` | JSON1 extension | `partial` | Inside the contract, but not yet closure-complete. |
| `SURF-EXT-RTREE-016` | R-Tree extension | `partial` | Inside the contract, but still needs broader evidence. |
| `SURF-CLI-COMPAT-018` | sqlite3-like CLI behavior | `partial` | Tracked for parity, but not yet a full sqlite3 shell contract. |

## SQLite-Compatible PRAGMAs In Scope Today

Within the broader partial PRAGMA surface, the canonical SQLite-compatible
PRAGMAs currently in scope are:

- Configuration/state: `journal_mode`, `synchronous`, `cache_size`,
  `busy_timeout`, `foreign_keys`, `recursive_triggers`, `user_version`,
  `application_id`
- Integrity/maintenance: `integrity_check`, `quick_check`, `wal_checkpoint`,
  `page_count`, `freelist_count`, `schema_version`, `encoding`
- Schema introspection: `table_info`, `table_xinfo`, `index_list`,
  `index_info`, `index_xinfo`, `foreign_key_list`, `compile_options`,
  `database_list`, `table_list`, `collation_list`

Rules for PRAGMAs:

- Unlisted SQLite PRAGMAs are out of scope, even if they parse, stub, or
  silently no-op.
- FrankenSQLite-specific `fsqlite.*` PRAGMAs are product extension surface, not
  C SQLite parity surface.
- "PRAGMA compatibility" does not mean "all PRAGMAs". It means the explicit
  subset above plus any future additions promoted through the surface matrix.

## Explicitly Out of Scope

These surfaces are explicitly excluded from the canonical parity contract until
they are promoted in the surface matrix:

- `SURF-SQL-WITHOUT-ROWID-003`: `WITHOUT ROWID` tables
- `SURF-SQL-STRICT-004`: `STRICT` tables
- `SURF-SQL-GENERATED-COLUMNS-005`: Generated columns (`VIRTUAL` and `STORED`)
- `SURF-PRAGMA-LEGACY-010`: Full legacy and edge-case SQLite PRAGMA compatibility beyond the named subset
- `SURF-TXN-SERIALIZED-WRITER-LOCK-007`: SQLite-style single-writer serialized file-lock escalation
- `SURF-EXT-FTS3-014`: FTS3
- `SURF-EXT-FTS5-015`: FTS5
- `SURF-EXT-SESSION-017`: Session / changeset extension
- `SURF-CONFORMANCE-TCL-020`: Direct upstream TCL harness execution parity

Additional interpretation rules:

- Native-mode / ECS / time-travel / content-addressed durability work is not
  part of the current parity contract.
- Implemented code paths do not become parity scope automatically.
- Parser support, hidden feature flags, or README aspirations do not override
  the surface matrix.

## Behavioral Rules

The contract is not just a feature checklist. The following behavior rules are
part of scope lock:

- The stable parity runtime is the compatibility-mode, pager-backed path over
  SQLite `.db` files.
- `BEGIN` promoting to `BEGIN CONCURRENT` by default is part of FrankenSQLite's
  contract and must not be regressed to SQLite-style serialized writer locking.
- Intentional divergences must be explicitly documented, justified, and wired
  into the harness as intentional rather than left ambiguous.
- Evidence must come from reproducible tests and differential runs, not from
  parser acceptance or hand inspection.

## What Does Not Count As Proof

The following are insufficient on their own:

- A parser round-trip test
- A single unit test without oracle comparison
- A benchmark that happens to succeed
- A feature existing behind a Cargo flag
- A README claim not backed by the machine-readable contract files

## Downstream Contract Map

Track A is split into four contract artifacts. This document is the narrative
entry point; the files below carry the enforceable machine-readable details:

- `bd-2yqp6.1.1`: [`supported_surface_matrix.toml`](contracts/supported_surface_matrix.toml)
- `bd-2yqp6.1.2`: [`feature_universe_ledger.toml`](contracts/feature_universe_ledger.toml)
- `bd-2yqp6.1.3`: [`sqlite_version_contract.toml`](contracts/sqlite_version_contract.toml)
- `bd-2yqp6.1.4`: [`parity_score_contract.toml`](contracts/parity_score_contract.toml)

Any downstream parity, conformance, or release-gating work should cite these
artifacts rather than restating the scope from memory.

Track G certification policy, ratchets, and release-evidence requirements are
documented in
[`design/certification-gates-ratchets-release-evidence.md`](design/certification-gates-ratchets-release-evidence.md).
