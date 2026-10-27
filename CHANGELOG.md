# Changelog

All notable changes to FrankenSQLite are documented in this file.

FrankenSQLite is an independent ground-up Rust reimplementation of SQLite with
page-level MVCC concurrent writers and Serializable Snapshot Isolation (SSI).
RaptorQ erasure coding is durability research: the codecs and WAL repair
routines exist in the workspace, but the compatibility runtime's WAL commit and
recovery paths do not write or consult repair symbols. The project is organized
as a 28-member Cargo workspace under `crates/`.

> The project is pre-release. Crates are published to crates.io as `fsqlite`
> and the `fsqlite-*` workspace members. The historical entries below
> (0.1.0–0.1.2) are organized by capability area rather than raw diff order,
> covering all 2,520 commits from project inception (2026-02-06) through
> 2026-03-21; the 0.1.3 and 0.1.4 entries are point releases.

Repository: <https://github.com/Dicklesworthstone/frankensqlite>

Scope window: [v0.3.7](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.7) (2026-08-20) through [v0.4.12](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.12) (2026-10-13). v0.4.8 remains an unchanged, withdrawn tag without a GitHub Release or crates.io publication. v0.3.2–v0.3.4, v0.3.6–v0.3.18 and v0.4.4 are GitHub Releases; **v0.3.5 is a tag / crates.io snapshot with no GitHub Release**, and **0.4.0–0.4.3 are crates.io publishes with no tag and no GitHub Release** — their changes are documented in the v0.4.4 section. Every release in the window has a per-change section below (the v0.3.12/v0.3.13 sections were reconstructed from their tag ranges on 2026-09-01).

## Version Timeline

| Version | Kind | Date | Summary |
|---------|------|------|---------|
| [Unreleased](https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.12...main) | HEAD | 2026-10-27 | Development on `main` after v0.4.12, with **data-integrity fixes**: an unparenthesized column `DEFAULT` swallowed a following NOT NULL or COLLATE, also on stock-created schemas; an FTS5 external-content `rebuild` after a content-table rename erased the index, and such a table lost its postings on its first write after a reopen (GH#211); concurrent writers could commit duplicate implicit rowids (bd-ujp0h, bd-2ym3z); triggers fired by a cascade skipped FK checks, and an FK action skipped by `RAISE(IGNORE)` left orphan rows (bd-vcvn0); R-tree tables were not written in stock's file format. Plus GH#509, GH#511, GH#512, GH#448, GH#521, GH#523, GH#526 (measured), GH#528 and GH#529 fixes, `json_each` over NULL input and table-valued functions listed before the table they read, contended checkpoints that failed with BusyRecovery, read-only opens of a header-less WAL without `-shm` (GH#515) and of unwritable files, WAL resets by the autocheckpoint (GH#385), join memory bounds (GH#527), candidate fixes for GH#510, GH#519 and GH#506 (macOS) that were not run when committed and for GH#524 and GH#525 that were not fully built or run, large-commit memory bounds (GH#518), stable-Rust build work (GH#522; releases through v0.4.12 still need nightly), the source-incompatible `MigrationOutcome::Incomplete` and geopoly return types, and ordered batch replication in the TypeScript SDK. Many commits record no compile or test run |
| [v0.4.12](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.12) | Release | 2026-10-13 | Uniform release of all 26 public crates, with an **upgrade advisory for 0.4.10**: a join whose outer table was read through an index with a `DESC` column returned no rows (GH#502); `:memory:` with `autocommit_retain = OFF` could lose every row of a table (GH#503); `DELETE ... WHERE EXISTS` over the same table deleted the wrong rows (bd-ntt2b). Plus FROM-less row-value comparisons (GH#450), attached-database DML and name resolution, conflict-clause and constraint parity, a full filesystem during WAL-index growth reported as an I/O error instead of a SIGBUS on Linux (GH#506), WAL-FEC attach/close fixes, and index seeks for `IN` lists and `WHERE k = ? ORDER BY id` |
| [0.4.11](https://crates.io/crates/fsqlite-vfs/0.4.11) | crates.io only | 2026-10-09 | `fsqlite-vfs` alone: on macOS and other non-Linux Unix, a redundant main-DB descriptor opened while WAL lifetime locks were held is reused instead of parked forever, so long-running processes stop leaking about 2 fds per fresh connection (bd-l0rbc). Tag `fsqlite-vfs-v0.4.11`; no GitHub Release. The next uniform release must be >= 0.4.12 |
| [v0.4.10](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.10) | Release | 2026-10-07 | Uniform release of all 26 public crates, with a **data-integrity advisory**: inserts inside a transaction could silently corrupt a table b-tree (bd-obwsy); UPDATE ... FROM lost or repeated updates (bd-b5j26); non-integer rowid / INTEGER PRIMARY KEY values left malformed index entries (REINDEX); a REPLACE constraint could delete a row an IGNORE constraint then kept out; a retried fsync after a failed WAL sync could vouch for unwritten frames (bd-alyrn). Plus checkpoint source validation, max-rowid and rowid-reuse parity, `:memory:` freelist reuse, stock empty-schema opens, and DML/join/trigger performance work |
| [v0.4.9](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.9) | Release | 2026-10-03 | Published replacement for withdrawn v0.4.8, with frozen runtime source 23697702 and the CTAS wrong-table write fix. All 26 public crates use 0.4.9. The full gate records documented failures; signed assets and publication evidence accompany the release. |
| [v0.4.8](https://github.com/Dicklesworthstone/frankensqlite/tree/v0.4.8) | Withdrawn tag | 2026-10-02 | Withdrawn before publication: CREATE TABLE AS SELECT with a quoted table name could write its copied rows into another existing table. The tag remains unchanged and unreleased; published v0.4.9 supersedes it. Planned changes are retained below. |
| [v0.4.7](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.7) | Release | 2026-09-29 | Prompt fix release of all 26 public crates. Large multi-row INSERT wrote index entries without their rowid and skipped UNIQUE checks past register 65,535 (hfdt-dlkam3); serialized-DDL index race (bd-4iaoi); composite-key DML seek (GH#434) with residual fix; autocommit DDL starvation (bd-f5sh5); GH#435 changes(); streaming aggregate and correlated-subquery performance (GH#420, GH#432, GH#436) |
| [v0.4.6](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.6) | Release | 2026-09-27 | Uniform release of all 26 public crates (0.4.5 skipped: consumed by the fsqlite-pager backport). Page-referenced-twice fix (bd-b5vmw), lost-index-entry race fix (bd-11sz4), freelist/overflow hardening, WAL-index healing, per-statement busy_timeout (GH#423), UTF-16/collation and numeric parity |
| [0.4.5](https://crates.io/crates/fsqlite-pager/0.4.5) | crates.io only | 2026-09-25 | `fsqlite-pager` alone: bd-b5vmw backport onto v0.4.4, so a page can no longer end up referenced by both the freelist trunk and a live b-tree across a WAL generation. Tag `fsqlite-pager-v0.4.5`; no GitHub Release. The next uniform release must be >= 0.4.6 |
| [v0.4.4](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.4.4) | Release | 2026-09-17 | First uniform-version release: all 28 crates publish at 0.4.4 together. Page-accounting repair: abandoned pages reach the durable freelist again on the native WAL-index path, fixing orphans stock `integrity_check` reported as `page N is never used` (bd-u2kmg); pending freelist repair is reserved for real writers (GH#462). Shared-memory foundations for cross-process MVCC (mapped page lock table, Unix MVCC SHM with identity-bound headers, session-owned `BEGIN CONCURRENT` tokens) — infrastructure only; public `Connection` MVCC authority stays process-local (GH#329 open). Native WAL-index SHM with append ownership and reader leases, plus stock WAL-index codec parity (GH#19). Read-only openers join a live WAL; schema-only WAL-index recovery. Scoped WAL-FEC repair pipeline. A substantial new TypeScript SDK / WASM worker: managed transaction scopes, streaming imports, atomic bulk writes, pre-IPC backpressure, versioned IndexedDB snapshots, binary result transfer, snapshot pools, and committed-change streams with live SELECT. SQL fixes: no rowid burned on a discarded row (bd-55kh5), `UNIQUE` violations name the conflicting columns (bd-towj6), bounded `DROP` teardown (bd-fmhvo), VIRTUAL generated-column bounds/NOT NULL/collation (bd-fjieg) |
| [0.4.3](https://crates.io/crates/fsqlite-pager/0.4.3) | crates.io only | 2026-09-16 | `fsqlite-pager` alone (GH#462 freelist repair); no tag, no GitHub Release. Documented in the v0.4.4 section |
| [0.4.2](https://crates.io/crates/fsqlite/0.4.2) | crates.io only | 2026-09-16 | Partial bump of the crates whose APIs had changed; no tag, no GitHub Release |
| [0.4.1](https://crates.io/crates/fsqlite/0.4.1) | crates.io only | 2026-09-12 | Partial bump; no tag, no GitHub Release |
| [0.4.0](https://crates.io/crates/fsqlite/0.4.0) | crates.io only | 2026-09-12 | Asupersync 0.5 alignment (breaking); no tag, no GitHub Release |
| [v0.3.18](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.18) | Release | 2026-09-06 | Parameterized `rowid IN (?, …)` planned as rowid seeks instead of full scans (GH#415); namespace sidecars accept mount-imposed modes with a GID-aware exposure rule (beads_rust GH#491); read-only WAL readers register byte-neutrally and fail closed when they cannot (bd-ti3xe); CLI same-line statement batches; lazy grouped IN predicates in aggregates; WAL-to-DELETE visibility; autocommit acknowledgement/crash guards; io_uring buffers retained until kernel quiescence (bd-6hdwo.35); agent-swarm replay fallback and real commit evidence |
| [v0.3.17](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.17) | Release | 2026-09-06 | SQL/shell correctness, real metrics & diagnostics (reader gauge, WAL durability, MVCC conflict/commit counters, /metrics HTTP), PRAGMA cache_size as a real page budget, aoj0g journal-boundary savepoint (O(n^2)->flat insert ramp) + private-allocation ownership through rollback, printf/FTS5 3.53.2 parity, epoch/RaptorQ native-storage hardening |
| [v0.3.16](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.16) | Release | 2026-09-03 | FTS5 lazy read path made usable at scale — `MATCH`/`ORDER BY rank`/`bm25()` (incl. prefix) answer without hydrating the corpus, fixing the cass runaway (Fix A/B/C + prefix scoring); bd-9inpb EOF-growth double-grant closed under the reserved append lock; GH#405 row-level FTS5 savepoint undo log; GH#382 appended-tail index; GH#406 content-backed incremental insert; dependency lockfile refresh (asupersync 0.4.10) |
| [v0.3.15](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.15) | Release | 2026-09-02 | FTS5 `'optimize'` rewrites the index — the in-engine migration for pre-GH#404 contentless indexes (bd-aks56) + contentless empty-re-encode guard (bd-dqcf5) + legacy origin-poison self-heal (bd-kon3m) + macOS clippy `-D warnings` gate restored (bd-0v03x) |
| [v0.3.14](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.14) | Release | 2026-09-01 | FTS5 stock-compat writer fix (GH#404) + GH#402 checkpoint watermark (super-linear autocommit fix) + `PRAGMA wal_checkpoint` cumulative-nBackfill parity + legacy-automerge origin-poisoning fix |
| [v0.3.13](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.13) | Release | 2026-08-29 | GH#399 reader-slot release-failure follow-up (b1c4609d9) |
| [v0.3.12](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.12) | Release | 2026-08-29 | GH#399 cross-process WAL wave: reader-slot registration + checkpoint horizon gating (55c186682), abandoned-page pool drop at a peer WAL generation reset (5946b3b7c) |
| [v0.3.11](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.11) | Release | 2026-08-27 | Preserve and enforce compound CHECK constraints with nested grouping parentheses |
| [v0.3.10](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.10) | Release | 2026-08-26 | DDL source-span and shadowed-main ALTER correctness; checkpoint, recovery, planner, backup, compaction, and observability hardening |
| [v0.3.9](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.9) | Release | 2026-08-23 | **Expedited**: v0.3.8 prebuilt CLI could not open file databases (bd-slgya feature-pin regression) + Windows read-only open regression fix (GH#438) + async-api implies native (GH#379) + C API `sqlite3_bind_*` + UPSERT/trigger/parity continuation |
| [v0.3.8](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.8) | Release | 2026-08-22 | UPSERT clobber P1 fix + INSERT-ABORT atomicity + stock-error-message parity campaign + JSON5 + differential oracle keeper suite |
| [v0.3.7](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.7) | Release | 2026-08-20 | GH#345/#366/#368/#369/#370/#371/#244 + bd-xv5cm 7-facet concurrency hardening + REVIEW3 |
| [v0.3.6](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.6) | Release | 2026-08-19 | FTS5 reopen/hydration + binary release |
| [v0.3.5](https://github.com/Dicklesworthstone/frankensqlite/tree/v0.3.5) | Tag | 2026-08-17 | crates.io snapshot; non-leading PK WITHOUT ROWID (no GitHub Release) |
| [v0.3.4](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.4) | Release | 2026-08-16 | GH#353 WITHOUT ROWID composite-UNIQUE auto-index |
| [v0.3.3](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.3) | Release | 2026-08-16 | asupersync 0.4.5 |
| [v0.3.2](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.2) | Release | 2026-08-16 | Post-0.3.1 correctness swarm tag |

---

## [Unreleased] -- development on `main` after v0.4.12

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.12...main>

**Verification status.** GitHub Actions is off, and many commits in this range state in their own
messages that Cargo was unavailable or that their Rust tests were not run; the entries below repeat
those caveats. Where the tracker (`.beads/issues.jsonl`) later records a run of a named test, the
entry says so; that run is not part of the commit.
[1cab6896d](https://github.com/Dicklesworthstone/frankensqlite/commit/1cab6896d) made the
`fsqlite-vfs`, `fsqlite-wal` and `fsqlite-core` targets compile and pass `clippy -D warnings` again
on 2026-10-22, and the tracker records no full-workspace test run of `main` since 2026-10-03
(bd-reality-check-1022-llk6g.12). From 2026-10-23 most commits record their own runs: unless an
entry says otherwise, each commit from that date that it cites records a keeper that failed before
the change and passes with it on Linux x86_64 build hosts (trj, mostly through rch). Entries name
the runs made on macOS arm64 and the commits that record no run or only part of one.

### Data integrity

- **An unparenthesized column `DEFAULT` swallowed the constraints that followed it, on new and
  stock-created schemas alike
  ([770fe3048](https://github.com/Dicklesworthstone/frankensqlite/commit/770fe3048),
  bd-reality-check-1022-llk6g.1).** `x INTEGER DEFAULT 0 NOT NULL` became the default
  `0 IS NOT NULL` (that is, 1) with no NOT NULL check, and `DEFAULT 'a' COLLATE NOCASE` lost its
  collation, so files written by earlier versions may hold wrong defaults or NULLs in NOT NULL
  columns, which stock `PRAGMA integrity_check` reports; the default is now one signed term as in
  stock, which makes `DEFAULT 1 + 2` a syntax error.
- **An FTS5 `'rebuild'` on an external-content table whose content table had been renamed erased the
  index ([#211](https://github.com/Dicklesworthstone/frankensqlite/issues/211),
  [183f44fc1](https://github.com/Dicklesworthstone/frankensqlite/commit/183f44fc1)).** Stock fails
  that rebuild with `no such table`, but FrankenSQLite rebuilt from its in-memory documents, which
  hold no external content, and dropped every posting; it now returns the same error and leaves the
  index and source rows unchanged.
- **An external-content FTS5 table lost its postings on its first write after a reopen
  ([#211](https://github.com/Dicklesworthstone/frankensqlite/issues/211),
  [ca48dd641](https://github.com/Dicklesworthstone/frankensqlite/commit/ca48dd641)).** When a
  connection bound an index it had not built itself (a reopened file, a stock-created file, or the
  segments a `'rebuild'` had just written), the first INSERT or `'delete'` rebuilt the in-memory
  index from the table's `%_content` rows, which an external-content table does not have, and wrote
  that index back, so `MATCH` then found only the rows written since. The index is now loaded from
  its persisted segments before the first change. `'rebuild'` on an external-content table now
  rebuilds from the source rows alone, where a row deleted from the content table without a
  matching FTS5 `'delete'` kept its postings, and
  [6f0818e63](https://github.com/Dicklesworthstone/frankensqlite/commit/6f0818e63) makes it read
  the source columns by name, where it paired them by position and could index the wrong columns.
  An index damaged this way can be regenerated with `INSERT INTO ft(ft) VALUES('rebuild')`. The
  GH#211 e2e keeper that was red on `main` passes with ca48dd641.
- **Several connections inserting implicit rowids into one WAL database could commit the same rowid
  (bd-ujp0h, bd-2ym3z;
  [76e57eeac](https://github.com/Dicklesworthstone/frankensqlite/commit/76e57eeac),
  [ce314ef4e](https://github.com/Dicklesworthstone/frankensqlite/commit/ce314ef4e)), the known issue
  listed for v0.4.12.** A right-edge split now takes a write-conflict claim on the old leaf even
  when its bytes are unchanged, a passive checkpoint no longer double-counts backfilled commits in
  the visible commit sequence, and a reused VDBE engine drops its cached concurrent rowid allocator;
  neither commit was built or run when committed, and the tracker records the 4- and 8-writer
  `concurrent_implicit_rowid_append_with_autocheckpoint_and_large_rows_bd_ujp0h_bd_2ym3z` keeper
  passing with stock `integrity_check` afterwards
  ([34e882226](https://github.com/Dicklesworthstone/frankensqlite/commit/34e882226)).
- **Statements run by a user trigger that an `ON DELETE` or `ON UPDATE CASCADE` fired skipped
  foreign-key checks, so they could insert orphan rows and let deferred violations pass COMMIT
  ([0bede8f87](https://github.com/Dicklesworthstone/frankensqlite/commit/0bede8f87), bd-vcvn0).**
  Enforcement is now restored on entry to every trigger frame; the fix was verified with stable
  rustc 1.99.1 by an external program running the new oracle cases, because builds of the
  repository's own test target ran out of memory.
- **With `foreign_keys` on, a CASCADE or SET NULL action that a child table's BEFORE trigger
  skipped with `RAISE(IGNORE)` left an orphan row, and the statement succeeded
  ([ad5d47395](https://github.com/Dicklesworthstone/frankensqlite/commit/ad5d47395), bd-vcvn0).**
  Stock fails such a statement with `FOREIGN KEY constraint failed` unless the parent key exists
  again by the statement's end (by COMMIT for a deferred key), and FrankenSQLite now does the same.
  One difference remains on purpose: inside an explicit transaction stock keeps the failed
  statement's changes, orphan included, while FrankenSQLite rolls the statement back. Orphans left
  by earlier versions show up in `PRAGMA foreign_key_check`.
- **R-tree tables were not stored in stock's file format
  ([80ac75664](https://github.com/Dicklesworthstone/frankensqlite/commit/80ac75664),
  bd-reality-check-1022-llk6g.3).** FrankenSQLite kept one row per entry in the table's own b-tree
  and wrote no `%_node`, `%_rowid` or `%_parent` shadow tables, so stock could not read the file
  (`no such table: main.rt_node`, also from `PRAGMA integrity_check`), and FrankenSQLite failed on
  stock-written R-trees with `OpenRead failed: could not open storage cursor on root page 0`.
  `rtree` and `rtree_i32` tables created now use stock's shadow tables and node layout and get the
  same shadow rows as stock for the same writes, and stock-written R-trees can be read and
  extended. Tables created by earlier versions keep the old layout, which FrankenSQLite still reads
  and updates and stock still cannot read (conversion is bd-reality-check-1022-llk6g.34). A
  duplicate id now fails with stock's `UNIQUE constraint failed: rt.id`, and `CREATE VIRTUAL TABLE`
  passes module arguments as their source text, so an auxiliary column `+tag0` no longer reaches
  the module as `+ tag0`, which stock read back as a syntax error.

### Fixed

SQL results and schema:

- STRICT `ANY` columns keep numeric-looking text such as `'000123'` as text, including defaults that
  `ALTER TABLE ADD COLUMN` backfills
  ([#509](https://github.com/Dicklesworthstone/frankensqlite/issues/509),
  [e55d78c96](https://github.com/Dicklesworthstone/frankensqlite/commit/e55d78c96),
  [78dc5e39a](https://github.com/Dicklesworthstone/frankensqlite/commit/78dc5e39a)). Neither commit
  was built or run when committed; the tracker records
  `strict_any_column_preserves_zero_padded_text_and_all_storage_classes_gh509`
  ([97618e7da](https://github.com/Dicklesworthstone/frankensqlite/commit/97618e7da)) passing against
  rusqlite afterwards.
- `WITH c AS (...) INSERT INTO attached.t SELECT ... FROM c` no longer fails with `no such table: c`
  ([#511](https://github.com/Dicklesworthstone/frankensqlite/issues/511),
  [4b6919f14](https://github.com/Dicklesworthstone/frankensqlite/commit/4b6919f14)).
- [#512](https://github.com/Dicklesworthstone/frankensqlite/issues/512): `PRAGMA table_info` reports
  the right `pk` ordinal for a quoted or parenthesized composite-key column, and the next implicit
  rowid after a rejected or ignored insert matches stock
  ([77b6544f9](https://github.com/Dicklesworthstone/frankensqlite/commit/77b6544f9),
  [4b6919f14](https://github.com/Dicklesworthstone/frankensqlite/commit/4b6919f14)). For the tied
  `min()`/`max()` storage class this range adds only a regression test; 77b6544f9 was not built or
  run, and the tracker records the three GH#512 keepers passing afterwards.
- `changes()` and `total_changes()` after a trigger `RAISE(FAIL)` follow stock: a statement whose
  first row fails in a BEFORE trigger reports 0 instead of the previous statement's count, prepared
  statements keep the rows written before the failure, and DML in a failing trigger step still
  counts ([#448](https://github.com/Dicklesworthstone/frankensqlite/issues/448);
  [79b4cb85a](https://github.com/Dicklesworthstone/frankensqlite/commit/79b4cb85a),
  [93770efe1](https://github.com/Dicklesworthstone/frankensqlite/commit/93770efe1),
  [8b99ba764](https://github.com/Dicklesworthstone/frankensqlite/commit/8b99ba764),
  [02a76463d](https://github.com/Dicklesworthstone/frankensqlite/commit/02a76463d)). None of these
  commits was compiled when committed; the tracker closed GH#448 citing
  `gh448_raise_fail_updates_changes_and_total_changes`
  ([87a919c1c](https://github.com/Dicklesworthstone/frankensqlite/commit/87a919c1c)) passing against
  rusqlite, while a later tracker note (2026-10-22) still lists them as unverified.
- `RAISE(ROLLBACK, 'msg')` fails with just `msg`, as in stock, instead of
  `transaction rolled back: msg`, and in autocommit it acts as ABORT instead of rolling back
  whatever transaction handle was open. A failed statement leaves `last_insert_rowid()` at the last
  row it inserted, even one that ABORT rolled back, as stock does, instead of restoring the value
  from before the statement
  ([#448](https://github.com/Dicklesworthstone/frankensqlite/issues/448),
  [7d09e6417](https://github.com/Dicklesworthstone/frankensqlite/commit/7d09e6417)). Of the
  commit's 256 trigger cases and 72 conflict cases compared with stock, 88 and 18 differed before.
- `PRAGMA integrity_check` inside `BEGIN` on a read-only connection reported every free page of a
  valid file as `page N is never used`; it now walks the committed freelist
  ([58ddae0de](https://github.com/Dicklesworthstone/frankensqlite/commit/58ddae0de), bd-vgw6e).
- A materialized CTE named like a table with an INTEGER PRIMARY KEY no longer returns that table's
  values through the rowid fast paths (joins, keyset limits, correlated scans, GROUP BY)
  ([292b08c34](https://github.com/Dicklesworthstone/frankensqlite/commit/292b08c34)), and
  materialized CTE tables stay installed while the statement that reads them runs
  ([77b6544f9](https://github.com/Dicklesworthstone/frankensqlite/commit/77b6544f9)). The oracle
  tests of 292b08c34 could not run when committed; the tracker records
  `bd_cte_composition_edges_oracle_probe` passing later (bd-dy2h3).
- `[+|-]HH:MM[:SS[.FFF]]` and `(+|-)YYYY-MM-DD` date/time modifiers and BCE dates such as
  `date('-0001-01-01')` returned NULL; they now follow stock, and BCE dates are no longer one day
  late ([0701d799a](https://github.com/Dicklesworthstone/frankensqlite/commit/0701d799a)).
- `CREATE TABLE` accepts adjacent table constraints without a separating comma, as stock's grammar
  does ([0c709404d](https://github.com/Dicklesworthstone/frankensqlite/commit/0c709404d)). The
  commit adds no test and was not compiled.
- Date and time functions follow stock's `date.c`
  ([d5524d3fa](https://github.com/Dicklesworthstone/frankensqlite/commit/d5524d3fa),
  bd-reality-check-1022-llk6g.17): `'localtime'` outside 1970-01-01..2038-01-18 uses the same
  wall-clock time in year 2000 + Y%4 instead of the zone's historical offsets (1800 in New York
  came out at -4:56:02); `'utc'`, and a second `'localtime'`, leave a value already in that zone
  alone, including text ending in `Z` or `+HH:MM`; a time zone is only `+HH:MM` or `-HH:MM` with
  hours 0-14, or `Z`, with optional spaces around it; any run of spaces and `T` (or none) may
  separate date and time; hour 24 is accepted; fractions are truncated at .999, so `59.9995` with
  `'subsec'` is 59.999, not 60.000; `'floor'` follows stock's state; date and time fields print as
  written while no modifier recomputes them (`datetime('2024-01-01 24:00')` is
  `2024-01-01 24:00:00`); and `9999-12-31 23:59:59.999` is in range.
  [4ba02c822](https://github.com/Dicklesworthstone/frankensqlite/commit/4ba02c822) makes
  `'start of day'`, `'start of month'` and `'start of year'` on an hour-24 input start from the
  date as written, as stock does (bd-a20bp); it was not compiled or run when committed, and no
  later run of its unit cases is recorded.
- `printf()` and `format()` accept the `l` and `ll` length modifiers and ignore them, as stock
  does: `printf('%lld', x)`, `%ld`, `%lu`, `%llx` and `format('%lld rows', 3)` returned NULL. An
  unknown conversion such as `%a` now keeps the text formatted before it (`printf('x%a', 1)` is
  `'x'`) and still returns NULL when there is none
  ([c86030ce7](https://github.com/Dicklesworthstone/frankensqlite/commit/c86030ce7),
  bd-reality-check-1022-llk6g.16).
- `iif()` and its alias `if()` take any number of arguments, as stock 3.48 and later do:
  `iif(c1, v1, c2, v2, ..., else)` is a searched `CASE`, a missing else is NULL, and only the
  selected branch is evaluated, so an unselected `abs(-9223372036854775808)` no longer fails with
  `integer overflow`
  ([e0a15abb8](https://github.com/Dicklesworthstone/frankensqlite/commit/e0a15abb8),
  bd-reality-check-1022-llk6g.28).
- A recursive CTE's `ORDER BY` orders its recursion queue as in stock, so `ORDER BY lvl DESC`
  walks a tree depth-first where FrankenSQLite always went breadth-first, and the CTE's own
  `LIMIT` and `OFFSET` apply in every form: `LIMIT 5 OFFSET 2` returned every row, and with an
  `OFFSET` or a non-literal `LIMIT` an otherwise infinite recursion never ended
  ([db5e15482](https://github.com/Dicklesworthstone/frankensqlite/commit/db5e15482),
  bd-reality-check-1022-llk6g.15).
- JSON values keep stock's JSON subtype from one JSON function to the next
  ([a75995f59](https://github.com/Dicklesworthstone/frankensqlite/commit/a75995f59),
  bd-reality-check-1022-llk6g.14): `json_quote(json('{"a":1}'))` returns `{"a":1}` instead of a
  quoted string; results of `json_extract` (an array or object), `json_patch`, `json_remove`,
  `json_group_array` and `json_group_object` are embedded by an outer `json_array` or
  `json_object` instead of quoted; a JSONB blob passed to `json_quote`, `json_array`,
  `json_object` or `json_set` is read as the value it encodes instead of failing with
  `JSON cannot hold BLOB values`; and joins, `GROUP BY` and `HAVING` pass subtypes as a FROM-less
  SELECT does (`json_array(json('[1]'))` over a join gave `["[1]"]`). Duplicate object keys still
  differ from stock: `json_extract` returns the last one.
- `pragma_table_list` works as a table-valued function (bare, `()` and `('t')`) instead of failing
  with `no such table: pragma_table_list`, and `PRAGMA table_list` reports a view's column count
  in `ncol` when the view has no column list, where it reported 0
  ([3ef73bc95](https://github.com/Dicklesworthstone/frankensqlite/commit/3ef73bc95)).
- `PRAGMA table_info`, `table_xinfo` and `pragma_table_info()` resolve a view first, as stock
  does: a circular view fails with `view c is circularly defined` instead of returning one column
  named `*`, and a view whose table was dropped fails with `no such table: main.t` instead of
  returning its old columns
  ([44ca9d261](https://github.com/Dicklesworthstone/frankensqlite/commit/44ca9d261), bd-mw9m3).
- `RIGHT` and `FULL JOIN` with `USING` or `NATURAL` match stock on rows that exist only on the
  right ([e0ca46a17](https://github.com/Dicklesworthstone/frankensqlite/commit/e0ca46a17),
  bd-reality-check-1022-llk6g.13, bd-reality-check-1022-llk6g.31): `SELECT *` and `tbl.*` return
  the coalesced join key where they returned NULL, and a `USING` after such a join compares the
  coalesced key of every left table that has the column, so
  `a FULL JOIN b USING(id) FULL JOIN c USING(id)` returns one row for an id that only b and c
  hold, not two. Stock's errors `ambiguous reference to X in USING()` and
  `ambiguous column name: X` are raised for the shapes stock rejects.
- A table-valued function listed first in FROM with a subquery among its arguments returned no
  rows and no error: `SELECT value FROM json_each((SELECT '[1,2,2]'))` was empty and
  `SELECT sum(DISTINCT value) FROM generate_series(1, (SELECT 3))` was NULL instead of 6
  ([653391c3c](https://github.com/Dicklesworthstone/frankensqlite/commit/653391c3c), bd-oamvm).
  Plain `count`, `sum`, `avg`, `min` and `max` and `GROUP BY` projections were not affected, and
  v0.4.9 behaves the same. A function listed before the table its argument reads
  (`FROM json_each(t.tags) AS j, t`) still returns no rows (bd-4t938). The commit's keeper failed
  on 0f338e1d8 and passes with the change on macOS arm64.
- After `ALTER TABLE ... DROP COLUMN`, statements that read the in-memory row mirror, such as those
  run through `query_with_params`, still returned the dropped column (`1|2|3` where stock returns
  `1|3`); the mirror is now reloaded from storage after the drop
  ([10ca0776a](https://github.com/Dicklesworthstone/frankensqlite/commit/10ca0776a)).
- `ALTER TABLE ... DROP COLUMN` is allowed when a trigger only assigns the column
  (`UPDATE t SET c = ...`, `INSERT INTO t(a, c) ...`, an upsert's `SET c`), as in stock; the
  trigger then fails when it fires. A trigger that reads the column still blocks the drop. An
  INSERT into a table with an INSERT trigger reports a VALUES width mismatch in stock's words
  (`table t has 2 columns but 1 values were supplied`) instead of an internal error
  ([4f37506fc](https://github.com/Dicklesworthstone/frankensqlite/commit/4f37506fc),
  bd-reality-check-1022-llk6g.28).

Constraints, foreign keys and triggers:

- With `foreign_keys` on, an UPDATE checks only the foreign keys whose child columns it assigns,
  as stock does. A row that already violated a key, for example one written while `foreign_keys`
  was off, failed any UPDATE with `FOREIGN KEY constraint failed`, even of an unrelated column
  ([#451](https://github.com/Dicklesworthstone/frankensqlite/issues/451),
  [7efb85c25](https://github.com/Dicklesworthstone/frankensqlite/commit/7efb85c25),
  bd-reality-check-1022-llk6g.4).
- After a failed INSERT the next automatic rowid is `max(rowid) + 1`, as in stock. A UNIQUE or
  PRIMARY KEY failure on a table first written in the failing transaction, and any failure of an
  `OR FAIL` statement, left a one-rowid gap. An AUTOINCREMENT table gets the rowid back only from a
  failed statement, and a successful `OR IGNORE` still uses one up, as in stock
  ([c3f68438d](https://github.com/Dicklesworthstone/frankensqlite/commit/c3f68438d),
  bd-reality-check-1022-llk6g.33).
- Schema and constraint parity
  ([4a60d9f71](https://github.com/Dicklesworthstone/frankensqlite/commit/4a60d9f71)):
  `ALTER TABLE ADD COLUMN` with a CHECK checks the existing rows (bd-whs71); VACUUM rebuilds a
  partial index with the columns' declared collations (bd-h09hh); CHECK error text keeps grouped
  expressions as written, and later column constraints take the preceding `CONSTRAINT` name, as in
  stock (bd-3syf5); `CREATE TRIGGER ... ON schema.table` parses (bd-3j81a); rowid aliases in an
  UPSERT `SET` and in `RETURNING` resolve as in stock (bd-xrzce); generated columns that name
  `rowid` or a qualified column, and CHECK constraints on unknown columns or on a WITHOUT ROWID
  table's `rowid`, are refused at `CREATE TABLE` and `ADD COLUMN`, where they were accepted into a
  schema stock cannot read (bd-kn6ah, bd-vtbcf, bd-wt3mn); RESTRICT and NO ACTION keys on
  self-referencing rows no longer reject valid DELETEs and UPDATEs (bd-01rla); `CREATE INDEX` on
  an FTS5 table reports `virtual tables may not be indexed` (bd-kld68); repeated result column
  names of a derived table or view are renamed `x:1`, as in stock (bd-073qp, bd-84b60); an
  unqualified `rowid` in a multi-table FROM resolves to a table's declared `rowid` column instead
  of being ambiguous (bd-257yl); and the bare columns that come with `min(DISTINCT ...)` and
  `max(DISTINCT ...)` are taken from the row stock picks (bd-d4lv6). The commit records
  `trigger_fk_upsert_oracle_keepers` (37 tests) passing against rusqlite on host hz3, and no run
  before the change.

Extensions and virtual tables:

- `ORDER BY bm25(docs)` and other FTS5 auxiliary calls in `ORDER BY` failed with
  `internal error: column not found: docs` unless the same call was also a result column; they
  now order the matches
  ([52e2dc66c](https://github.com/Dicklesworthstone/frankensqlite/commit/52e2dc66c),
  bd-reality-check-1022-llk6g.18). Still different from stock: `WHERE ft = 'q'` as a MATCH, the
  table-valued form `FROM ft('q')`, and the `rank` configuration command.
- An external-content FTS5 table returned NULL or empty text for its columns, both in a scan and
  for `MATCH` hits, so `highlight()` and `snippet()` returned nothing
  ([#211](https://github.com/Dicklesworthstone/frankensqlite/issues/211),
  [6f0818e63](https://github.com/Dicklesworthstone/frankensqlite/commit/6f0818e63), bd-c6jre).
  Columns are now read from the content table by name, as stock does: a full scan is a scan of the
  content table, so `count(*)` counts its rows, and a missing content column fails with
  `no such column: T.body`. After the content table is renamed or dropped, a full scan fails with
  stock's `no such table`, but a `MATCH` that reads a column returns NULL for it where stock
  fails, and a hit whose content row is gone also reads as NULL.
- A virtual table whose module was registered again after reopening the file failed with
  `no such table`; the schema reload that `register_module` arms now runs
  ([#483](https://github.com/Dicklesworthstone/frankensqlite/issues/483),
  [ef2b6bc96](https://github.com/Dicklesworthstone/frankensqlite/commit/ef2b6bc96),
  bd-reality-check-1022-llk6g.10).
- The geopoly functions are ported from `geopoly.c`
  ([4d3a3fc02](https://github.com/Dicklesworthstone/frankensqlite/commit/4d3a3fc02),
  bd-reality-check-1022-llk6g.28); 179 of the 196 statements in the commit's recorded stock run
  differed before, and all match now. `geopoly_blob()` writes stock's blob header, so stock and
  FrankenSQLite read each other's polygons; JSON input needs the closing vertex, and input that
  is not a polygon gives NULL instead of an error; `geopoly_json()` and `geopoly_svg()` produce
  stock's text; `geopoly_area()` is signed; `geopoly_overlap()` and `geopoly_within()` return
  stock's codes instead of 0 or 1; and `geopoly_bbox`, `geopoly_contains_point`, `geopoly_xform`,
  `geopoly_regular`, `geopoly_ccw`, `geopoly_debug` and the `geopoly_group_bbox` aggregate are
  registered. Blobs that earlier versions' `geopoly_blob()` wrote carry the old header and now
  read as not a polygon. The geopoly virtual table is not provided.
- `json_each` and `json_tree` over a NULL input return no rows, as in stock, instead of failing
  with `json table-valued input must be TEXT or BLOB JSON`
  ([d21ba7baa](https://github.com/Dicklesworthstone/frankensqlite/commit/d21ba7baa), bd-oui0e).
  So `FROM t, json_each(t.tags)` failed on any table with a NULL in `tags`; it now skips those
  rows, and `t LEFT JOIN json_each(t.tags)` keeps them with NULL columns. A NULL PATH also
  returns no rows (it scanned from `$`), and `json_each()` with no argument returns no rows
  instead of an error. The new keeper compares 16 queries with rusqlite's bundled SQLite: 15
  differed at 46192f4be, none after.
- A table-valued function listed before the FROM source its arguments read, as in
  `FROM json_each(t.tags) AS j, t`, failed with `internal error: column not found: tags`. Like
  stock, FrankenSQLite now runs the function after that source
  ([4f584689a](https://github.com/Dicklesworthstone/frankensqlite/commit/4f584689a), bd-4t938),
  for comma joins and inner joins with `ON`, including chains of functions. When a LEFT JOIN pins
  the source after the function, the function returns no rows, as `json_each` does in stock. The
  new keeper compares 19 queries with rusqlite's bundled SQLite: all 19 differed at 46192f4be,
  none after. Still different from stock: `CROSS JOIN` is reordered too, where stock keeps its
  order and `json_each` returns no rows; `USING`, `NATURAL`, and a FROM with an outer join
  elsewhere still fail.

B-tree:

- Descending inserts into an index, including a `DESC` column of a composite index, stranded
  one-cell leaves: 12,000 descending integers produced 5,384 index leaves and a 22 MB file where
  stock needs 56 leaves and 344 KB
  ([#521](https://github.com/Dicklesworthstone/frankensqlite/issues/521);
  [00e907b52](https://github.com/Dicklesworthstone/frankensqlite/commit/00e907b52),
  [6fb125a3d](https://github.com/Dicklesworthstone/frankensqlite/commit/6fb125a3d),
  [cdad39841](https://github.com/Dicklesworthstone/frankensqlite/commit/cdad39841)). Index and
  interior pages are now equalized after left-packing and an overflowing root is split by bytes,
  which also removes a `split child does not fit` error when large keys cluster on one side;
  6fb125a3d was not run when committed, and cdad39841 reports native validation of the result
  against stock page geometry.

Files, WAL and durability:

- **A read-only open of a WAL database whose `-wal` has no header and that has no `-shm` reads the
  main file again ([#515](https://github.com/Dicklesworthstone/frankensqlite/issues/515);
  [5083bd687](https://github.com/Dicklesworthstone/frankensqlite/commit/5083bd687),
  [8f3f91644](https://github.com/Dicklesworthstone/frankensqlite/commit/8f3f91644),
  bd-reality-check-1022-llk6g.30, bd-reality-check-1022-llk6g.20).** The contract for embedders,
  unchanged since 0.4.12 (bd-yxqs9,
  [8cc4796da](https://github.com/Dicklesworthstone/frankensqlite/commit/8cc4796da)): a `-wal`
  shorter than its 32-byte header (zero bytes, as stock's `wal_checkpoint(TRUNCATE)` leaves it, or
  a torn short file) is header-less and has no frames. Nothing can be appended to it until a
  writable connection binds a new WAL generation, which writes the header and so changes the
  `-wal` file; until then an append through a handle on it answers `BusyRecovery`
  (`refuse_append_while_headerless`). A read-only open with a `-shm` present still binds such a
  WAL header-less, for example under a live stock writer after `wal_checkpoint(TRUNCATE)`, and
  sees that writer's later commits. What changed is only the read-only open
  (`SQLITE_OPEN_READ_ONLY`) with no `-shm`, on native Unix with a file-backed VFS: it answered
  `BusyRecovery` on every read, and it now reads the main file, as 0.4.10 and stock do, refuses
  writes as read-only, creates no `-shm`, and leaves the main file and `-wal` unchanged
  (5083bd687; its keeper failed on a018602fb with `database is busy (recovery in progress)`).
  8f3f91644 opens the same shape when its files are mode 0444, where every open failed with
  `Permission denied`. An embedder that refuses any change to the `-wal` during recovery, as
  beads_rust does, still has no way to admit the header bind deliberately (the issue's third ask),
  and GH#515 stays open.
- A `-shm` that stock left uninitialized, either truncated to 3 bytes by a first attacher that
  died before rebuilding it or extended but zero, made every plain read-only open answer
  `BusyRecovery` forever; beads_rust read-only commands hit this
  ([#528](https://github.com/Dicklesworthstone/frankensqlite/issues/528),
  [0f338e1d8](https://github.com/Dicklesworthstone/frankensqlite/commit/0f338e1d8)). A read-only
  open now rebuilds the WAL index in that `-shm` from the WAL, only while the index is
  uninitialized and only under every recovery lock; it never creates a missing `-shm` and never
  writes the WAL or the main file. If a peer publishes a valid index first it uses that one, and
  while a live peer holds the WAL locks the open fails with the usual retryable busy error and
  leaves the `-shm` alone. An initialized index that fails validation still answers
  `BusyRecovery`. While a rebuild waits for the other connections of its process to finish (at
  most 2 s), their new opens and transactions answer `Busy`. The commit's suites failed before the
  change (6 of 8 core tests, on macOS arm64) and pass with it in three runs on macOS arm64 and on
  Linux (trj).
- A WAL-FEC sidecar mutation could report `Busy` although nothing was contending for the sidecar
  (GH#453). The mutation guard was released only by closing its lock descriptor, and a child
  process forked by another thread keeps a copy of that descriptor until its exec, so the lock
  outlived the guard for a moment. The guard now unlocks explicitly when dropped. This is what
  made the `wal_fec_sidecar` suite fail intermittently (162 of 200 standalone runs on Linux before
  the fix).
- On macOS, `-shm` growth tries `F_PREALLOCATE` before extending the file and reports a real storage
  failure such as `ENOSPC` instead of mapping sparse space
  ([#506](https://github.com/Dicklesworthstone/frankensqlite/issues/506),
  [43bc4f21e](https://github.com/Dicklesworthstone/frankensqlite/commit/43bc4f21e)). The commit was
  not compiled and nothing ran on macOS; the tracker records only its injected-call policy test
  passing, on Linux.
- Cross-process lock ordering
  ([#519](https://github.com/Dicklesworthstone/frankensqlite/issues/519);
  [3dc78166e](https://github.com/Dicklesworthstone/frankensqlite/commit/3dc78166e),
  [96bc40e36](https://github.com/Dicklesworthstone/frankensqlite/commit/96bc40e36),
  [ff5f170dc](https://github.com/Dicklesworthstone/frankensqlite/commit/ff5f170dc),
  [48b26924c](https://github.com/Dicklesworthstone/frankensqlite/commit/48b26924c)): WAL append,
  checkpoint and recovery take the main-file lock before the WAL WRITE/CKPT slots on Unix, Windows
  and the in-memory VFS, a `BEGIN IMMEDIATE` that waited for a peer takes RESERVED before binding
  its WAL snapshot instead of starting from the peer's old one, and one busy-wait budget spans
  BEGIN, checkpoint, group-commit and reader-slot waits (PASSIVE checkpoints never wait). None of
  these commits ran native tests;
  [1cab6896d](https://github.com/Dicklesworthstone/frankensqlite/commit/1cab6896d) later ran
  `gh519_native_lock_progress` (6 passed) and the `fsqlite-vfs` library (508 passed, 2 failed in
  `wal_fence_cancelled_*`, which fail the same way on its base).
- [#510](https://github.com/Dicklesworthstone/frankensqlite/issues/510) (on a WAL database, after a
  bulk `INSERT ... SELECT` was rolled back to a savepoint and released, every later INSERT failed
  with `database is busy`):
  [d9837a6ec](https://github.com/Dicklesworthstone/frankensqlite/commit/d9837a6ec) and
  [1642d9b05](https://github.com/Dicklesworthstone/frankensqlite/commit/1642d9b05) keep the freelist
  origin of committed-free pages through savepoint rollback and through a dropped transaction's
  deferred cleanup. Neither was compiled or run, the savepoint regressions added for it
  ([497db384a](https://github.com/Dicklesworthstone/frankensqlite/commit/497db384a),
  [5c83039c5](https://github.com/Dicklesworthstone/frankensqlite/commit/5c83039c5),
  [472084581](https://github.com/Dicklesworthstone/frankensqlite/commit/472084581)) have no recorded
  run, and the tracker closed GH#510 citing the multi-connection INSERT keeper instead.
- The `shm_interop` failure listed for v0.4.12
  ([#513](https://github.com/Dicklesworthstone/frankensqlite/issues/513)) is fixed in the test only:
  its child process gets its own `TMPDIR` and a `SIGXFSZ` exit under `ulimit -f 64` is accepted
  ([4b6919f14](https://github.com/Dicklesworthstone/frankensqlite/commit/4b6919f14)). No library
  code changed for it.
- Checkpoints sync the WAL at full durability before their first database write, validate the WAL
  header even when there is nothing to copy, leave a zero-byte WAL alone in every mode, refuse a WAL
  that gained a commit after the plan was made, and refuse a backfill endpoint inside a multi-frame
  transaction ([ddbddc667](https://github.com/Dicklesworthstone/frankensqlite/commit/ddbddc667),
  [5f7da1542](https://github.com/Dicklesworthstone/frankensqlite/commit/5f7da1542),
  [421c4dd6a](https://github.com/Dicklesworthstone/frankensqlite/commit/421c4dd6a)). These commits
  were checked only against Python models; their Rust tests were not run.
- The first-open migration marker and its backups are created owner-only through exclusive staging
  and an atomic rename
  ([98f90882f](https://github.com/Dicklesworthstone/frankensqlite/commit/98f90882f)), which the
  tracker records as the fix for
  [#517](https://github.com/Dicklesworthstone/frankensqlite/issues/517). The commit was not
  compiled, and the `-wal`, `-shm` and WAL-certificate sidecars used the ambient umask until
  [97f4ae1b5](https://github.com/Dicklesworthstone/frankensqlite/commit/97f4ae1b5) (below;
  bd-reality-check-1022-llk6g.29).
- First-open migration repair requires its main and WAL backups and stops before any repair if they
  fail, repairs only on an explicit corruption verdict, reports a new `Incomplete` outcome instead
  of success when a stage fails, keeps checksummed (BLAKE3) backup generations across retries, and
  copies the source under the native write and checkpoint fences, pausing physical commits and
  checkpoints while it copies
  ([0a949c188](https://github.com/Dicklesworthstone/frankensqlite/commit/0a949c188),
  [75d334d75](https://github.com/Dicklesworthstone/frankensqlite/commit/75d334d75),
  [bad47e5a3](https://github.com/Dicklesworthstone/frankensqlite/commit/bad47e5a3),
  [719aa32a7](https://github.com/Dicklesworthstone/frankensqlite/commit/719aa32a7),
  [c9be23175](https://github.com/Dicklesworthstone/frankensqlite/commit/c9be23175),
  [8fab351ea](https://github.com/Dicklesworthstone/frankensqlite/commit/8fab351ea)). None of these
  commits was compiled when committed; their evidence is SQLite 3.46.1/POSIX reference probes.
- A database file the process cannot write (mode 0444, another user's file, a read-only mount)
  opens read-only, as with stock's `unixOpen`, instead of failing with
  `I/O error: Permission denied`; reads work and writes fail with `SQLITE_READONLY`
  ([5e188deea](https://github.com/Dicklesworthstone/frankensqlite/commit/5e188deea),
  bd-reality-check-1022-llk6g.20). A WAL database with frames and no `-shm` opens this way too,
  because rebuilding its WAL index no longer takes a write lock on the main file. Native Unix only.
  A read-only WAL database with frames and a 0444 `-shm` still fails to open
  (bd-reality-check-1022-llk6g.35).
- `-wal`, `-journal` and `-shm` files, and the `-wal-cert` pair, are created with the database
  file's mode and, for a root process, its owner, as stock's unix VFS does; they took the process
  umask, so a 0600 database could get a 0644 WAL holding copies of its pages
  ([97f4ae1b5](https://github.com/Dicklesworthstone/frankensqlite/commit/97f4ae1b5),
  bd-reality-check-1022-llk6g.29). Delete-on-close files are created 0600.
- On Linux and macOS the WAL peer probe opened and closed a temporary descriptor on the database
  or its `-shm`, and closing it dropped every POSIX lock the process held on that file, including
  the locks of a stock SQLite connection in the same process
  ([#455](https://github.com/Dicklesworthstone/frankensqlite/issues/455),
  [3eed2882f](https://github.com/Dicklesworthstone/frankensqlite/commit/3eed2882f),
  bd-reality-check-1022-llk6g.9). The probe runs whenever another process holds the namespace
  lease. It now keeps one descriptor per probed file open for the life of the process.
- The `-wal-cert` sidecar gained one 156-byte record per WAL commit and never shrank (31.4 KB
  after 200 commits, kept since the database was created), and past 4,096 records WAL-FEC replay
  dropped every certificate. The first append after a WAL reset now truncates it, so it holds only
  the current WAL generation's records
  ([d2a752116](https://github.com/Dicklesworthstone/frankensqlite/commit/d2a752116),
  bd-reality-check-1022-llk6g.32). A database switched to `journal_mode=DELETE` keeps one
  generation's records until WAL mode is used again.
- With default settings a file-backed autocommit loop never reset the WAL: the autocheckpoint was
  PASSIVE, and past the 4,000-frame urgent threshold every commit ran a FULL checkpoint over the
  whole WAL without resetting it, at about 10 ms per commit (353x stock at 10,000 inserts), while
  the WAL kept growing (12.4 MB after 3,000 inserts)
  ([#385](https://github.com/Dicklesworthstone/frankensqlite/issues/385),
  [ea44754a4](https://github.com/Dicklesworthstone/frankensqlite/commit/ea44754a4),
  bd-reality-check-1022-llk6g.7). The autocheckpoint now picks RESTART whenever few transactions
  are active, waits for more frames after an attempt that left the WAL unreset, and never waits on
  readers or writers. The commit's 9,000-insert keeper failed after 2,134 s with a 32.2 MB WAL
  high-water mark before and passes in 7.3 s with 3 WAL resets.

Connections, busy handling and cancellation:

- With the default `busy_timeout` of 0, an autocommit SELECT on a WAL database could fail with
  `database is busy` while another connection committed (139 of 33,091 reads with one writer and
  four readers); stock's readers retry these races without the busy handler
  ([#457](https://github.com/Dicklesworthstone/frankensqlite/issues/457),
  [93dd2b5fe](https://github.com/Dicklesworthstone/frankensqlite/commit/93dd2b5fe),
  bd-reality-check-1022-llk6g.23). The schema and row-mirror refreshes now rebind a newer
  publication, up to 16 times, and WAL-index reader admission retries a torn header, busy slots
  and moved marks at stock's pace, up to 100 times. The keeper saw no BUSY result in two runs
  afterwards.
- A `PRAGMA wal_checkpoint(TRUNCATE)` (or PASSIVE/RESTART) that raced another process's WAL reset
  failed with `BusyRecovery`, so processes that checkpoint at close, as `br` does, exited with
  `truncate checkpoint: BusyRecovery`. It now returns the `busy = 1` row the PRAGMA already
  returns for a plain busy checkpoint (GH#399), since stock's checkpoint never returns
  SQLITE_BUSY_RECOVERY
  ([3716efbb2](https://github.com/Dicklesworthstone/frankensqlite/commit/3716efbb2), bd-s4lco).
  The cross-process churn keeper failed 4 and 3 of 10 runs at 46192f4be on macOS arm64; with the
  change it passed 20 of 20 there and 7 of 7 completed runs on Linux x86_64. No deterministic test
  drives the busy row.
- `Connection::query`, which the CLI uses, did not retry an autocommit INSERT, UPDATE or DELETE
  that conflicted with a writer in another process, so with `busy_timeout=5000` it failed in about
  90 ms with `database is busy (snapshot conflict on pages: 2)` (265 of 400 inserts from four
  processes committed); it now retries within `busy_timeout`, as `execute()` already did
  ([2433e21f6](https://github.com/Dicklesworthstone/frankensqlite/commit/2433e21f6),
  bd-reality-check-1022-llk6g.5). Cross-process stalls remain: later runs of the same test file on
  loaded Linux build hosts failed once after 211 s and once after 1,245 s
  ([c15d0734a](https://github.com/Dicklesworthstone/frankensqlite/commit/c15d0734a)), and the
  tracker records the fresh-connection shape committing 372 of 400 on `main` at 80ac75664
  (bd-reality-check-1022-llk6g.5, open).
- On a connection opened the way the `fsqlite` CLI opens it (a `RuntimeContext` built inside a
  runtime), `ATTACH` and then `DETACH` made every later statement, and close, fail with
  `callback requested query abort`
  ([081606a58](https://github.com/Dicklesworthstone/frankensqlite/commit/081606a58),
  bd-reality-check-1022-llk6g.2). Closing any connection cancelled the I/O context it shared with
  the other connections on that context, and with `RuntimeContext::new_with_root_cx` it cancelled
  the caller's own native context; neither happens now. DETACH closes the detached connection
  properly, and a DETACH of a database that the open transaction uses fails with
  `database aux is locked` and leaves it attached, as in stock.
- Cancelling a busy wait
  ([#524](https://github.com/Dicklesworthstone/frankensqlite/issues/524)): a test added in
  [24499460e](https://github.com/Dicklesworthstone/frankensqlite/commit/24499460e) shows that on
  v0.4.12 a cancelled `BEGIN IMMEDIATE`, autocommit INSERT or COMMIT waiting for a writer in
  another process waited out its whole `busy_timeout` (10 s), and that `main` at ad5d47395
  already ends all three promptly (not bisected). Later commits close further gaps: native and
  namespace lock admission and every Unix WAL reader-slot retry observe the operation's
  cancellation ([77e42b879](https://github.com/Dicklesworthstone/frankensqlite/commit/77e42b879));
  bootstrap, transaction, prepare-refresh and autocommit retries do too, without giving up commit
  settlement or rollback cleanup
  ([d4e814928](https://github.com/Dicklesworthstone/frankensqlite/commit/d4e814928)); a cancelled
  open, including an async open, stops in pager admission, schema initialization, first-open
  migration or a nested ATTACH
  ([d6492cbdf](https://github.com/Dicklesworthstone/frankensqlite/commit/d6492cbdf),
  [b657719db](https://github.com/Dicklesworthstone/frankensqlite/commit/b657719db)); and the WAL
  reader retry backoff is cancellable
  ([6e8916b16](https://github.com/Dicklesworthstone/frankensqlite/commit/6e8916b16)).
  Verification is partial: 77e42b879 and d4e814928 record local Linux runs (the WAL reader
  regression returned `Busy` after 10 s before the fix; afterwards `migration_tests` passed 14
  tests and the GH#524 test 6), but d4e814928's six new core unit tests did not run, b657719db was
  not compiled natively, and d6492cbdf and 6e8916b16 ran only a few tests in isolated or non-native
  builds. [0f338e1d8](https://github.com/Dicklesworthstone/frankensqlite/commit/0f338e1d8) later
  built this tree on macOS arm64 and Linux for its own tests. GH#524 stays open.

CLI:

- A `CASE ... END;` inside a trigger body ended `CREATE TRIGGER` early, so the trigger was not
  created and the leftover `END;` ran as a `COMMIT`; only an `END` that follows a `;` closes the
  body now, as in `sqlite3_complete()`
  ([53a4ed3ef](https://github.com/Dicklesworthstone/frankensqlite/commit/53a4ed3ef)).
- `fsqlite app.db "SELECT ..."` runs each SQL argument after the database path in order and stops
  with exit status 1 at the first failure, as `sqlite3 FILENAME SQL...` does; it used to fail with
  `too many positional arguments`
  ([5bd1fdc23](https://github.com/Dicklesworthstone/frankensqlite/commit/5bd1fdc23)).
- The `fsqlite` shell draws `EXPLAIN QUERY PLAN` output as the stock shell does, as a tree under
  `QUERY PLAN` in every output mode, where it printed the rows in the current mode (csv, line,
  column, ...) ([3ef73bc95](https://github.com/Dicklesworthstone/frankensqlite/commit/3ef73bc95)).
  The plan's content still comes from FrankenSQLite's own planner. The new shell tests compare
  the output with the system `sqlite3` and pass; no run before the change is recorded.

Build and platforms:

- [#522](https://github.com/Dicklesworthstone/frankensqlite/issues/522): `main` aims to build on
  stable Rust; releases through v0.4.12 still need nightly, and `rust-toolchain.toml` still pins
  `nightly-2026-08-31`. The nightly `prefetch_read_data` calls are replaced by a stable
  `_mm_prefetch` wrapper in `fsqlite-vfs`, the Windows `windows_by_handle` feature by
  `GetFileInformationByHandle`, and the `-Z threads=4` flag is opt-in
  ([4c0a98dea](https://github.com/Dicklesworthstone/frankensqlite/commit/4c0a98dea),
  [00261bb5d](https://github.com/Dicklesworthstone/frankensqlite/commit/00261bb5d)); on rustc 1.99.1
  the `fsqlite-pager`, `fsqlite-btree` and `fsqlite` library checks and the prefetch and cleanup
  tests passed and the Windows GNU target cross-compiled, but Windows tests were not executed and
  the full test matrix was not built.

### Performance

- Composite index equality scans with bound parameters, such as `a = ?1 AND k = ?2 AND v > 0` on
  `(a, k)`, seek the whole pinned prefix and stop at its end
  ([72b0806c3](https://github.com/Dicklesworthstone/frankensqlite/commit/72b0806c3), bd-gtjlv
  follow-up). Measured by the commit through FLDT's `fldt-fsqlite-compat` wrapper (dev build, 60k
  rows, against 0.4.10), a 4-column bound prefix went from 4,098 ms to 1.6 ms and a 3-column one
  from 3,745 ms to 87 ms; full scans and point lookups were unchanged.
- Large single-transaction WAL commits keep fewer copies: VFS writes take ownership of frame
  buffers, large appends are written in 1 MiB chunks, retained WAL scratch is capped at 4 MiB,
  post-commit cache admission stays within the cache budget, and an idle page-buffer pool keeps at
  most 8 MiB ([#518](https://github.com/Dicklesworthstone/frankensqlite/issues/518);
  [4b6919f14](https://github.com/Dicklesworthstone/frankensqlite/commit/4b6919f14),
  [b1263081c](https://github.com/Dicklesworthstone/frankensqlite/commit/b1263081c),
  [9f0b1429e](https://github.com/Dicklesworthstone/frankensqlite/commit/9f0b1429e)). No RSS change
  is recorded; b1263081c and 9f0b1429e were not compiled when committed, and the tracker records the
  `fsqlite-wal` `wal::tests` (200 passed) and the borrowed-page test passing for 4b6919f14.
- A `WITH` statement on a schema-only open no longer loads every row into the MemDatabase mirror
  ([#493](https://github.com/Dicklesworthstone/frankensqlite/issues/493),
  [b729ccf1d](https://github.com/Dicklesworthstone/frankensqlite/commit/b729ccf1d)), and prepared
  point reads on file databases hydrate the mirror only for logical images of at most 8 MiB
  ([#492](https://github.com/Dicklesworthstone/frankensqlite/issues/492),
  [91df8b6b9](https://github.com/Dicklesworthstone/frankensqlite/commit/91df8b6b9)). 91df8b6b9 was
  not compiled when committed; the tracker records the GH#492 keeper (5 passed) and the GH#493
  targets passing afterwards (bd-w7qsf, bd-dy2h3).
- `PageData::clone` no longer copies through an intermediate `Vec`, and the in-memory page store
  shares cached page snapshots
  ([#520](https://github.com/Dicklesworthstone/frankensqlite/issues/520),
  [6fb125a3d](https://github.com/Dicklesworthstone/frankensqlite/commit/6fb125a3d)). The commit was
  not run; the tracker records its two tests passing.
- A join that leaves the streaming paths (a third source, a subquery source, a WHERE or ON
  predicate, a computed aggregate) held every column of every source row; columns the statement
  cannot name are now read as NULL, so an unreferenced wide column is no longer copied per row
  ([#527](https://github.com/Dicklesworthstone/frankensqlite/issues/527),
  [226bf9f03](https://github.com/Dicklesworthstone/frankensqlite/commit/226bf9f03)). Measured by
  the commit with a release CLI on a 2.26 GB cass archive, peak RSS went from 6,832 to 4,491 MiB
  for `total_messages` and from 4,082 to 1,184 MiB for `message_metrics JOIN messages` with
  `SUM(CASE ...)`. Two-table joins with a WHERE or ON predicate are unchanged (3.9-4.5 GB on that
  archive), because the rest is the in-memory row mirror, and GH#527 stays open. The commit
  records its new test and 23 join, subquery, view and CTE suites passing through rch (host hz4),
  and no run before the change.
- DISTINCT aggregates over a table-valued source compared each value with every earlier distinct
  value, so `select sum(distinct round(value/1.5)) from generate_series(1,50000)` took 2.8 s on
  0.4.12 and four times as long for twice the rows; they now sort, keeping the same equality
  classes and the first value of each in scan order
  ([#529](https://github.com/Dicklesworthstone/frankensqlite/issues/529),
  [07bc8b79e](https://github.com/Dicklesworthstone/frankensqlite/commit/07bc8b79e), bd-r0jkl). In
  the commit's growth test on macOS arm64 debug builds, 5,000 and 40,000 rows took 0.93 s and
  58.5 s before and 10.9 ms and 90.1 ms after;
  [ccb0b05d5](https://github.com/Dicklesworthstone/frankensqlite/commit/ccb0b05d5) adds the
  reported query itself to that test (growth ratio 51.9 on 6e8916b16, 8.0 after).
  `min`/`max(DISTINCT)` bare-column tracking is still linear per row (bd-ma0e1).
- `ORDER BY <index columns>, <rowid>` reads the index in order instead of scanning the table and
  sorting it: `SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY i, id
  LIMIT ?2` with `UNIQUE(c, i)` and `id INTEGER PRIMARY KEY` read the whole table for every page
  (cass measured 80.7 ms per 25-row page against 1.34 ms for `ORDER BY i`)
  ([#523](https://github.com/Dicklesworthstone/frankensqlite/issues/523),
  [fd1ef9c70](https://github.com/Dicklesworthstone/frankensqlite/commit/fd1ef9c70),
  [bf64160f8](https://github.com/Dicklesworthstone/frankensqlite/commit/bf64160f8), bd-lolg7).
  Every index entry of a rowid table ends with the rowid, ascending, so a last ORDER BY term naming
  the rowid or its INTEGER PRIMARY KEY alias right after the index's key terms needs no sort, in
  either walk direction, hinted or not. fd1ef9c70 recognised only a hidden rowid name and its
  keeper failed on `main`; bf64160f8 adds the alias. bf64160f8 also makes an `INDEXED BY` index
  that cannot give the ORDER BY still seek the WHERE equality and sort the matches, as stock does,
  where the hint was dropped for a table scan. Its keeper failed its plan checks on f87ca8ff2 and
  passes with the change on macOS arm64. The missing-index half of #523 already failed as stock
  does.
- An explicit COMMIT on a `:memory:` connection got slower as the database grew, even when it
  changed one row, because the time-travel capture after it read every page; the capture now
  reuses the previous image and re-reads only the commit's pages
  ([#494](https://github.com/Dicklesworthstone/frankensqlite/issues/494),
  [bf490d0f3](https://github.com/Dicklesworthstone/frankensqlite/commit/bf490d0f3)). In the
  commit's release timing probe the mean commit stays at 28-63 µs through 1,400 rows, where it
  reached 5.2 ms before, and with 8,192 unrelated rows the commit's p50 went from 34 ms to 0.1 ms.
  A ROLLBACK (about 41 ms) and the first read after a COMMIT (about 42 ms) with 8,192 resident
  rows are unchanged (bd-reality-check-1022-llk6g.24).
- A multi-row `INSERT ... VALUES` into a table with a BEFORE or AFTER INSERT trigger replays its
  rows through one compiled row program instead of dispatching each row as a separate single-row
  statement ([#516](https://github.com/Dicklesworthstone/frankensqlite/issues/516),
  [4a60d9f71](https://github.com/Dicklesworthstone/frankensqlite/commit/4a60d9f71)). No timing is
  recorded. The commit records `gh516_returning_conformance` (4 tests, including the previously
  ignored `returning_compiled_candidate_avoids_row_redispatch`) passing on host vmi1152480, and no
  run before the change.
- A non-UNIQUE index insert whose key sorts after the index's last key appends at the right edge
  instead of seeking from the root, as UNIQUE inserts already did (BINARY collations and keys
  without NULL only)
  ([b226c8633](https://github.com/Dicklesworthstone/frankensqlite/commit/b226c8633),
  bd-reality-check-1022-llk6g.8). Measured by the commit with a release CLI on a 1M-row table,
  `CREATE INDEX` on a column in rowid order went from 4.57 s to 2.73 s and `INSERT ... SELECT` into
  a table indexed on a rising column from 8.89 s to 7.45 s (stock: 0.26 s and 0.45 s); random,
  duplicate-heavy and text keys are unchanged at 5.6-6.3 s. Its oracle keeper passes before and
  after the change: it guards the append rather than reproducing a bug.
- Read transactions kept copies of the pages they read up to the default 262,144-page allocation
  pool rather than the cache budget, so read-only connections held page buffers far above
  `cache_size` (GH#525 reports 676 MiB for four 64 MiB readers); retention is now capped at the
  smaller of the cache budget and the page-buffer capacity, and a read-only transaction on a lone
  connection retains none
  ([#525](https://github.com/Dicklesworthstone/frankensqlite/issues/525),
  [d39a460b6](https://github.com/Dicklesworthstone/frankensqlite/commit/d39a460b6)). Not compiled
  when committed; no run of its five pager regressions or of the GH#525 oracles and memory
  profiles added in
  [82dcce30f](https://github.com/Dicklesworthstone/frankensqlite/commit/82dcce30f) and
  [29e1c3090](https://github.com/Dicklesworthstone/frankensqlite/commit/29e1c3090) is recorded,
  so there is no measured RSS change, and GH#525 stays open.
- A BEFORE UPDATE trigger whose `WHEN` holds a correlated multi-column `EXISTS` no longer walks
  the duplicates of the composite index's first column on every UPDATE
  ([#526](https://github.com/Dicklesworthstone/frankensqlite/issues/526)). The fix is the
  bound-parameter prefix seek above
  ([72b0806c3](https://github.com/Dicklesworthstone/frankensqlite/commit/72b0806c3)), which
  `WHEN`'s numbered OLD/NEW parameters already reach. The `gh526_trigger_when_composite_exists`
  keeper counts 9,309 opcodes per UPDATE at 2,048 rows at 72b0806c3's parent and 260 at
  72b0806c3. [14c7f7a19](https://github.com/Dicklesworthstone/frankensqlite/commit/14c7f7a19),
  committed for #526 without a build, is not needed for the issue's shape: its keepers pass at
  its parent too. Measured 2026-10-27 on macOS arm64 (release, stock 3.53.2 in the same run;
  median of 3), the `WHEN` arm's milliseconds per UPDATE at 2k / 8k / 32k rows were 2.31 / 11.20
  / 45.50 on v0.4.12 and 0.114 / 0.147 / 0.305 on `main` (46192f4be), against stock's
  0.006 / 0.006 / 0.012. FrankenSQLite is still 19-26x slower than stock per UPDATE here, and
  7-16x without the trigger.

### TypeScript SDK and worker

These commits were executed with Node 22.16.0 against native SQLite 3.49.1, not against the
FrankenSQLite engine; they state that no full SDK, worker, browser or WASM build was run.

Fixed:

- **A synchronous adapter hook could start an owned child savepoint before an admitted parent write
  was registered, so a handled child rollback erased the parent's row while the outer transaction
  committed ([af5503f5c](https://github.com/Dicklesworthstone/frankensqlite/commit/af5503f5c)).**
  Changeset capture and `withDeferredForeignKeys` now reserve ownership before calling the adapter
  and reject the overlapping child with `BUSY`.
- A `Uint8Array` BLOB with a shadowed `length` property was encoded from that property, so a 4-byte
  BLOB could become an empty one that SQLite then applied; the session encoders read the internal
  slots and reject detached, shared, resizable and proxied buffers
  ([e027b9544](https://github.com/Dicklesworthstone/frankensqlite/commit/e027b9544)).
- `ChangesetOutbox` `record` and `recordSnapshot` could exceed `maxEntries` and `maxPayloadBytes`
  when their callback published through another outbox; capacity is rechecked before publication
  ([c5231f1ef](https://github.com/Dicklesworthstone/frankensqlite/commit/c5231f1ef)).
- `ChangesetDeliveryPump` ignored a source port's `receiverId`, so a source bound to one replica
  could accept another replica's receipt and reclaim payloads the first replica never applied
  ([fe8a7c40c](https://github.com/Dicklesworthstone/frankensqlite/commit/fe8a7c40c)).
- The single-message ordered receiver committed an order receipt whose counts did not match what was
  applied; counts are now checked before the sequence is published
  ([a4f8a7557](https://github.com/Dicklesworthstone/frankensqlite/commit/a4f8a7557)).
- Nested capture savepoints: a callback rejection after a confirmed child rollback lets the parent
  continue, and an uncertain child cleanup fences the whole scope
  ([d05352025](https://github.com/Dicklesworthstone/frankensqlite/commit/d05352025),
  [d1f71461d](https://github.com/Dicklesworthstone/frankensqlite/commit/d1f71461d)).
- Worker: a stolen or released OPFS publication lock let a detached writer overwrite a newer
  checkpoint ([84362e955](https://github.com/Dicklesworthstone/frankensqlite/commit/84362e955));
  snapshot stores trusted shadowable `byteLength`/`buffer` properties and could persist an oversized
  image ([d8ea80ffe](https://github.com/Dicklesworthstone/frankensqlite/commit/d8ea80ffe)); and a
  lost Web Locks lease is now reported and fences new and queued work
  ([8d0ad32dc](https://github.com/Dicklesworthstone/frankensqlite/commit/8d0ad32dc),
  [4567e4c6c](https://github.com/Dicklesworthstone/frankensqlite/commit/4567e4c6c)). The browser
  tests could not run; these were executed on Node with lock and storage models.

Added:

- `constraintConflicts: "isolate"` applies each changeset or patchset row in its own child
  transaction so a constraint failure can be omitted instead of aborting the scope; it composes with
  `foreignKeys: "defer"` and is available through `ChangesetRebaseJournal.apply`, delivery receivers
  and `ChangesetOrder`
  ([4a35a41cb](https://github.com/Dicklesworthstone/frankensqlite/commit/4a35a41cb),
  [cda35504a](https://github.com/Dicklesworthstone/frankensqlite/commit/cda35504a),
  [ba77d8f20](https://github.com/Dicklesworthstone/frankensqlite/commit/ba77d8f20),
  [9adb235d4](https://github.com/Dicklesworthstone/frankensqlite/commit/9adb235d4),
  [d737512d5](https://github.com/Dicklesworthstone/frankensqlite/commit/d737512d5)). Default
  application stays atomic with no per-row savepoints.
- Conflict policies receive the receiver's current row as `ChangesetConflict.current`
  ([60a16c7b1](https://github.com/Dicklesworthstone/frankensqlite/commit/60a16c7b1));
  `PatchsetGroup` and `concatPatchsets` combine patchsets
  ([afe4d875c](https://github.com/Dicklesworthstone/frankensqlite/commit/afe4d875c)); capture
  executors expose nested `transaction`, and `onRebase` hooks can write through owned savepoints in
  the same transaction
  ([f27e107bb](https://github.com/Dicklesworthstone/frankensqlite/commit/f27e107bb),
  [b03595a8f](https://github.com/Dicklesworthstone/frankensqlite/commit/b03595a8f)).
- `prepareChangesetReader` and `preparePatchsetReader` validate a whole message and then decode one
  record at a time, and application, receivers, the pump, the outbox and the encoder use them
  instead of a full decoded copy
  ([842eebcc7](https://github.com/Dicklesworthstone/frankensqlite/commit/842eebcc7),
  [dcca36d6c](https://github.com/Dicklesworthstone/frankensqlite/commit/dcca36d6c),
  [78e32d085](https://github.com/Dicklesworthstone/frankensqlite/commit/78e32d085),
  [d79f0e98b](https://github.com/Dicklesworthstone/frankensqlite/commit/d79f0e98b)). For a
  6,000-row, 32-column payload of 8.6 MB, the extra JS heap at application admission went from 14.3
  MB to 73 KB plus the retained wire bytes, and at outbox load from 14.3 MB to 86 KB.
- Ordered replication can move 1 to 256 contiguous messages as one unit: `ChangesetOrder.applyBatch`
  and `receiveBatch` apply them in one SQL transaction, `createOrderedChangesetBatchTransport` and
  `createOrderedChangesetBatchHttpTransport`/`createOrderedChangesetBatchHttpHandler` carry them
  (one binary POST, documented in `packages/sdk/docs/ordered-batch-http.md`), and
  `ChangesetDeliveryPump.runBatch` drives them
  ([936052eb2](https://github.com/Dicklesworthstone/frankensqlite/commit/936052eb2),
  [eba352d09](https://github.com/Dicklesworthstone/frankensqlite/commit/eba352d09),
  [7f503f701](https://github.com/Dicklesworthstone/frankensqlite/commit/7f503f701),
  [c9346deb1](https://github.com/Dicklesworthstone/frankensqlite/commit/c9346deb1),
  [23084f9ee](https://github.com/Dicklesworthstone/frankensqlite/commit/23084f9ee)). Sources can
  read a window from one snapshot with `readBatch` and, with `atomicAcknowledgements: true`,
  acknowledge it in one transaction with `acknowledgeBatch`
  ([859041c33](https://github.com/Dicklesworthstone/frankensqlite/commit/859041c33),
  [829b4906a](https://github.com/Dicklesworthstone/frankensqlite/commit/829b4906a),
  [725830d5c](https://github.com/Dicklesworthstone/frankensqlite/commit/725830d5c),
  [965b6c463](https://github.com/Dicklesworthstone/frankensqlite/commit/965b6c463)); source and
  receiver remain separate transactions.
- `WorkerConnectionHost` accepts optional row, cell and byte limits for query results, and
  `FrankenDB.open({ resultLimits })` negotiates them with the worker before exposing the database
  ([0aa03a7e9](https://github.com/Dicklesworthstone/frankensqlite/commit/0aa03a7e9),
  [d1b9dec8a](https://github.com/Dicklesworthstone/frankensqlite/commit/d1b9dec8a)). The limits are
  checked after the SQL ran, so a refused result can follow committed writes.

### API changes

- **Source-incompatible:** `fsqlite_core::migration::MigrationOutcome` gains an
  `Incomplete { stage, repairs, error }` variant, with the new `MigrationFailureStage` enum
  ([75d334d75](https://github.com/Dicklesworthstone/frankensqlite/commit/75d334d75)). The enum is
  not `#[non_exhaustive]`, so an exhaustive `match` on it needs a new arm.
- SQL: an unparenthesized `DEFAULT` expression with an operator, such as `DEFAULT 1 + 2`, is now a
  syntax error, as in stock
  ([770fe3048](https://github.com/Dicklesworthstone/frankensqlite/commit/770fe3048)).
- **Source-incompatible:** in `fsqlite-ext-rtree`, `geopoly_contains_point`, `geopoly_overlap` and
  `geopoly_within` return stock's `i64` codes instead of `bool`, and `geopoly_area` returns a
  signed area ([4d3a3fc02](https://github.com/Dicklesworthstone/frankensqlite/commit/4d3a3fc02)).
- **Source-incompatible:** `fsqlite_ast::CreateTriggerStatement` gains a public
  `table_schema: Option<String>` field for `ON schema.table`; the struct is not
  `#[non_exhaustive]`, so a struct literal needs the new field
  ([4a60d9f71](https://github.com/Dicklesworthstone/frankensqlite/commit/4a60d9f71)).
- The parser turns an `iif()` or `if()` call with two or more plain arguments into a searched
  `CASE` expression, so an AST consumer sees `CASE` there instead of a function call
  ([e0a15abb8](https://github.com/Dicklesworthstone/frankensqlite/commit/e0a15abb8)).
- New Rust items: `fsqlite::wal_recovery` (feature `native`) re-exports the read-only frame-free WAL
  witnesses `inspect_frame_free_wal` and `verify_frame_free_wal_transition` for
  [#515](https://github.com/Dicklesworthstone/frankensqlite/issues/515)
  ([57bd4079d](https://github.com/Dicklesworthstone/frankensqlite/commit/57bd4079d),
  [0af6f244a](https://github.com/Dicklesworthstone/frankensqlite/commit/0af6f244a));
  `capture_migration_backup`, `MigrationBackup`, `read_migration_backup` and
  `migration_backup_receipt_path` in `fsqlite_core::migration`, with
  `MigrationBackup::restore_to_new_directory` and async variants
  ([bad47e5a3](https://github.com/Dicklesworthstone/frankensqlite/commit/bad47e5a3),
  [d05fbd145](https://github.com/Dicklesworthstone/frankensqlite/commit/d05fbd145),
  [cf3f43ec7](https://github.com/Dicklesworthstone/frankensqlite/commit/cf3f43ec7),
  [121b28e26](https://github.com/Dicklesworthstone/frankensqlite/commit/121b28e26));
  `fsqlite_types::cx::BusyWaitBudget` with `Cx::with_busy_wait_budget`
  ([48b26924c](https://github.com/Dicklesworthstone/frankensqlite/commit/48b26924c));
  `VfsFile::write_owned` and `write_owned_tracked`, both with default implementations
  ([4b6919f14](https://github.com/Dicklesworthstone/frankensqlite/commit/4b6919f14));
  `fsqlite_vfs::cache_hint`
  ([4c0a98dea](https://github.com/Dicklesworthstone/frankensqlite/commit/4c0a98dea)); and
  `fsqlite_vfs::host_fs::write_private_atomic` and `copy_private_atomic_if_exists`
  ([98f90882f](https://github.com/Dicklesworthstone/frankensqlite/commit/98f90882f)). The witness
  commits were not run when committed;
  [1cab6896d](https://github.com/Dicklesworthstone/frankensqlite/commit/1cab6896d) later ran the
  `frame_free` tests (6 passed).
- More new Rust items: `Cx::set_shared_native_cx`
  ([081606a58](https://github.com/Dicklesworthstone/frankensqlite/commit/081606a58)) and
  `Cx::create_child_preserving_mask`, `Cx::local_cancel_relay` and `LocalCancelRelay`
  ([d6492cbdf](https://github.com/Dicklesworthstone/frankensqlite/commit/d6492cbdf)) in
  `fsqlite_types::cx`; `ConnectionEnv::with_bootstrap_cx` and `ConnectionEnv::bootstrap_cx` in
  `fsqlite-core` ([b657719db](https://github.com/Dicklesworthstone/frankensqlite/commit/b657719db));
  `ScalarFunction::result_subtype_varies`, `ScalarFunction::invoke_with_result_subtype` and
  `AggregateFunction::result_subtype` in `fsqlite-func`, all with default implementations
  ([a75995f59](https://github.com/Dicklesworthstone/frankensqlite/commit/a75995f59));
  `Fts5Table::is_external_content`
  ([ca48dd641](https://github.com/Dicklesworthstone/frankensqlite/commit/ca48dd641)); in
  `fsqlite-ext-rtree`, seven new `Geopoly*Func` types, `geopoly_bbox_polygon` and
  `geopoly_svg_with_attributes`
  ([4d3a3fc02](https://github.com/Dicklesworthstone/frankensqlite/commit/4d3a3fc02)) and the
  R-tree shadow-storage API (`RtreeShadowWrites`, `RtreeShadowRowChanges` and new
  `RtreeVirtualTable` methods such as `take_shadow_writes`)
  ([80ac75664](https://github.com/Dicklesworthstone/frankensqlite/commit/80ac75664));
  `shared_wal_index_header_is_torn` and `shared_wal_index_header_is_uninitialized` in
  `fsqlite-wal` ([93dd2b5fe](https://github.com/Dicklesworthstone/frankensqlite/commit/93dd2b5fe),
  [0f338e1d8](https://github.com/Dicklesworthstone/frankensqlite/commit/0f338e1d8)); and
  `BtCursor::last_index_insert_was_past_rightmost` in `fsqlite-btree`
  ([b226c8633](https://github.com/Dicklesworthstone/frankensqlite/commit/b226c8633)).
- `next_pending_batch` on the `fsqlite` compat changeset `OrderedDelivery` and `FanoutDelivery`
  types reads a bounded contiguous batch from one source snapshot
  ([bdb4db9db](https://github.com/Dicklesworthstone/frankensqlite/commit/bdb4db9db),
  [67d7d93f1](https://github.com/Dicklesworthstone/frankensqlite/commit/67d7d93f1)). These commits
  have no message body and record no test run.
- TypeScript: the SDK root exports `prepareChangesetReader`, `preparePatchsetReader`,
  `PatchsetGroup`, `concatPatchsets`, `createOrderedChangesetBatchTransport`,
  `createOrderedChangesetBatchHttpTransport`, `createOrderedChangesetBatchHttpHandler` and their
  option types; `ChangesetApplyError` gains the `ERR_FSQLITE_CHANGESET_BUSY` and
  `ERR_FSQLITE_CHANGESET_STATE` codes and `ChangesetOrderError` gains `ERR_FSQLITE_ORDER_STATE`,
  both with a `cleanupErrors` field
  ([b03595a8f](https://github.com/Dicklesworthstone/frankensqlite/commit/b03595a8f),
  [d737512d5](https://github.com/Dicklesworthstone/frankensqlite/commit/d737512d5)). The worker
  package exports `ResultLimits`, `resolveResultLimits`, `DEFAULT_RESULT_LIMITS`,
  `ResultBudgetError` and `captureResultLimitsAcknowledgement`, and `WorkerConnectionHost` takes an
  optional fourth constructor argument
  ([0aa03a7e9](https://github.com/Dicklesworthstone/frankensqlite/commit/0aa03a7e9)).

### Experimental (not reachable from `Connection`)

The `fsqlite-wal`/`fsqlite-pager` native stack gained caller-owned recovery that rewrites before it
syncs, recovered-prefix copy with RaptorQ redundancy regeneration, coordinator and page-store
rebuild from a sealed recovery handoff, live transfer of a failed publisher into recovery,
commit-outcome resolution, historical and online page snapshots that the B-tree/VDBE dispatcher can
read, snapshot forks, and local and transport-independent replica catch-up with authenticated
selective object requests (for example
[fe4093876](https://github.com/Dicklesworthstone/frankensqlite/commit/fe4093876),
[43191f697](https://github.com/Dicklesworthstone/frankensqlite/commit/43191f697),
[3e434118d](https://github.com/Dicklesworthstone/frankensqlite/commit/3e434118d),
[31511e61a](https://github.com/Dicklesworthstone/frankensqlite/commit/31511e61a),
[333b4a57b](https://github.com/Dicklesworthstone/frankensqlite/commit/333b4a57b),
[6bdbd8418](https://github.com/Dicklesworthstone/frankensqlite/commit/6bdbd8418),
[e1aab9726](https://github.com/Dicklesworthstone/frankensqlite/commit/e1aab9726),
[9f5681c99](https://github.com/Dicklesworthstone/frankensqlite/commit/9f5681c99),
[08d00338f](https://github.com/Dicklesworthstone/frankensqlite/commit/08d00338f),
[1e6aa4f9f](https://github.com/Dicklesworthstone/frankensqlite/commit/1e6aa4f9f)). Its commit
service wakes idle workers and blocked producers on cancellation and shutdown and keeps group commit
while isolating rejected writers
([f356ee822](https://github.com/Dicklesworthstone/frankensqlite/commit/f356ee822),
[37d5cf509](https://github.com/Dicklesworthstone/frankensqlite/commit/37d5cf509),
[3eb53bc46](https://github.com/Dicklesworthstone/frankensqlite/commit/3eb53bc46)), and the pager's
[#494](https://github.com/Dicklesworthstone/frankensqlite/issues/494) memory-snapshot owner retries
a refused commit without replaying writes
([db5f7ba19](https://github.com/Dicklesworthstone/frankensqlite/commit/db5f7ba19)). `fsqlite-core`
gained checkpoint-anchored commit-marker validation and bounded native-index caches
([d9210956b](https://github.com/Dicklesworthstone/frankensqlite/commit/d9210956b),
[f4386c7c2](https://github.com/Dicklesworthstone/frankensqlite/commit/f4386c7c2),
[4991f5d92](https://github.com/Dicklesworthstone/frankensqlite/commit/4991f5d92),
[64a666993](https://github.com/Dicklesworthstone/frankensqlite/commit/64a666993)), and the
`fsqlite-vdbe` vectorized hash-join operator, which the SQL planner does not use, gained a streaming
probe and stock numeric key equality
([cd0844456](https://github.com/Dicklesworthstone/frankensqlite/commit/cd0844456),
[1570f3389](https://github.com/Dicklesworthstone/frankensqlite/commit/1570f3389)). Almost all of
these commits say Cargo was unavailable and their Rust tests were not run;
[1cab6896d](https://github.com/Dicklesworthstone/frankensqlite/commit/1cab6896d) later made the
affected targets compile and ran `commit_marker` (47 passed) and `native_pages::snapshot` (9
passed). The [#508](https://github.com/Dicklesworthstone/frankensqlite/issues/508) native harness
tests were corrected to target the real sync barriers, also without being run.

### Documentation

- The README drops the CI badge, says page-lock writers wait within the busy budget with deadlock
  detection instead of never waiting, names rusqlite's bundled SQLite 3.53.2 as the conformance
  oracle, stops claiming the SQLite test corpus is run, lists the companion files a database uses,
  and says `main` aims to build on stable while releases through v0.4.12 need nightly
  ([652c76cff](https://github.com/Dicklesworthstone/frankensqlite/commit/652c76cff)).

### Committed as unapplied candidates

- The [#494](https://github.com/Dicklesworthstone/frankensqlite/issues/494) explicit-COMMIT
  `Connection` integration
  ([61b0b81b3](https://github.com/Dicklesworthstone/frankensqlite/commit/61b0b81b3)) and the
  [#516](https://github.com/Dicklesworthstone/frankensqlite/issues/516) compiled INSERT row executor
  ([757179e79](https://github.com/Dicklesworthstone/frankensqlite/commit/757179e79),
  [4c099d558](https://github.com/Dicklesworthstone/frankensqlite/commit/4c099d558),
  [f7fe9f331](https://github.com/Dicklesworthstone/frankensqlite/commit/f7fe9f331)) are stored under
  `artifacts/` with their validation scripts and are not built into the engine; neither fixes its
  issue. [4a60d9f71](https://github.com/Dicklesworthstone/frankensqlite/commit/4a60d9f71) later
  built a compiled INSERT row executor for #516 into the engine (see Performance); the #494 patch
  under `artifacts/gh494` stays unapplied, and
  [bf490d0f3](https://github.com/Dicklesworthstone/frankensqlite/commit/bf490d0f3) addressed the
  `:memory:` COMMIT cost instead.

---

## [0.4.12] -- 2026-10-13 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.10...v0.4.12>

**What this is.** A uniform release of all 26 public crates, cut from `main`. It fixes two defects
that v0.4.10 introduced (a join that returned no rows, and row loss on `:memory:` databases with
`autocommit_retain = OFF`), the wrong-row DELETE that the v0.4.10 notes listed as a known issue,
and a set of SQL-parity, constraint and durability fixes. Upgrading from 0.4.10 is recommended.
There is no uniform 0.4.11: that number went to the crates.io-only `fsqlite-vfs` patch below,
whose fix is included here, and the backport release once planned from v0.4.10 is superseded by
this one.

### Upgrade advisory

- **A join whose outer table was read through an index with a `DESC` column returned no rows
  (GH#502, bd-w5736).** v0.4.10 only; v0.4.9 and stock SQLite return the rows. Inner and LEFT
  joins were both affected, with or without `GROUP BY`, and no error was raised. `main` was
  already correct when this was reported; the fixing commit was not bisected. A stock-oracle
  keeper pins the shape
  ([4a500546b](https://github.com/Dicklesworthstone/frankensqlite/commit/4a500546b)): it fails on
  v0.4.10 and passes here.
- **`:memory:` with `PRAGMA fsqlite.autocommit_retain = OFF` could lose every row of a table
  (GH#503).** v0.4.10 only. A `DROP INDEX` that freed a page, then a row large enough for an
  overflow chain, then a `CREATE TABLE`, handed the freed page out while the first table still
  used it; `integrity_check` then reported `page N is never used`. File-backed databases and the
  default `autocommit_retain` were not affected. Fixed by
  [be9ae2526](https://github.com/Dicklesworthstone/frankensqlite/commit/be9ae2526) and
  [b1f576179](https://github.com/Dicklesworthstone/frankensqlite/commit/b1f576179), with
  follow-ups [2db2ec2c5](https://github.com/Dicklesworthstone/frankensqlite/commit/2db2ec2c5),
  [458325345](https://github.com/Dicklesworthstone/frankensqlite/commit/458325345) and
  [563bdc0b1](https://github.com/Dicklesworthstone/frankensqlite/commit/563bdc0b1).
- **`DELETE ... WHERE EXISTS (a subquery over the same table)` deleted the wrong rows (bd-ntt2b,
  [79ecdae28](https://github.com/Dicklesworthstone/frankensqlite/commit/79ecdae28)).** v0.4.10
  is affected and named it as a known issue. The statement took the first column of each matching row as
  its rowid, so it deleted whichever rows had those rowids. It now freezes the matching rows by
  their real locator (rowid, or the full primary key on a WITHOUT ROWID table). In the same family:
  UPDATE / DELETE with a correlated `IN` on a WITHOUT ROWID table
  ([4fcad6d3c](https://github.com/Dicklesworthstone/frankensqlite/commit/4fcad6d3c), bd-1jnfu), and a
  `:memory:` DELETE whose nested SELECT read a stale mirror after a WITHOUT ROWID write
  ([cc61ac1ab](https://github.com/Dicklesworthstone/frankensqlite/commit/cc61ac1ab), bd-pxn52). The
  file shows no trace of such a DELETE afterwards; review data that these statement shapes changed.

### Fixed

SQL results and name resolution:

- A FROM-less `SELECT` with a row-value comparison in its `WHERE`, such as
  `SELECT 1 WHERE (1,2) = (1,2)`, raised `unsupported expression type: RowValue` instead of
  answering (GH#450, [9f6bc6839](https://github.com/Dicklesworthstone/frankensqlite/commit/9f6bc6839)).
  `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS` and `IS NOT` now compare element-wise, as stock does. A bind
  parameter inside such a row value is still refused with an explicit error.
- INSERT, UPDATE and DELETE read each attached database's same-named table, and RETURNING
  subqueries resolve qualified tables (bd-tjvwc, bd-8sawz,
  [b56c594a4](https://github.com/Dicklesworthstone/frankensqlite/commit/b56c594a4)). `name.*`
  expands every item addressed as `name`, whatever its database (bd-x4g7x,
  [fd820a6fb](https://github.com/Dicklesworthstone/frankensqlite/commit/fd820a6fb)), and
  `schema.table.column` binds to that database's FROM item (bd-at0bx,
  [5a5d008b3](https://github.com/Dicklesworthstone/frankensqlite/commit/5a5d008b3)).
- A parenthesized join as the first FROM source is an ordinary join list (bd-1ht9p,
  [b16ee15d3](https://github.com/Dicklesworthstone/frankensqlite/commit/b16ee15d3)).
- A damaged index root reports `SQLITE_CORRUPT` instead of answering its seeks with no rows
  (bd-pvv2k, [42b34f3f7](https://github.com/Dicklesworthstone/frankensqlite/commit/42b34f3f7)).
- View chains expand without a native stack frame per level (bd-5haia,
  [8fcbb47ae](https://github.com/Dicklesworthstone/frankensqlite/commit/8fcbb47ae)).

Constraints and conflict handling:

- A NULL for a NOT NULL column resolves `REPLACE` and `IGNORE` as stock does (bd-m3jt4,
  [3c292cd75](https://github.com/Dicklesworthstone/frankensqlite/commit/3c292cd75)). A failed
  statement is resolved by the conflict algorithm of the check that failed (bd-nn29x,
  [04d598b6a](https://github.com/Dicklesworthstone/frankensqlite/commit/04d598b6a)), and FK and STRICT
  datatype errors always resolve with ABORT (bd-axr5h,
  [e044094f3](https://github.com/Dicklesworthstone/frankensqlite/commit/e044094f3)). A WITHOUT ROWID
  UPDATE that fails with FAIL keeps the rows it already rewrote
  ([b2cbee3dc](https://github.com/Dicklesworthstone/frankensqlite/commit/b2cbee3dc)).
- UPSERT probes partial UNIQUE indexes, evaluating their predicate under the table name (bd-45puh,
  bd-r4g3y, [fe89def05](https://github.com/Dicklesworthstone/frankensqlite/commit/fe89def05)).
- The implicit NOT NULL of a STRICT table's primary key is enforced (bd-9y1gr,
  [63fdec396](https://github.com/Dicklesworthstone/frankensqlite/commit/63fdec396)).
- CHECK constraints and partial-index predicates see the written row's rowid (bd-8cs1s,
  [9135821f6](https://github.com/Dicklesworthstone/frankensqlite/commit/9135821f6); bd-q6sdy,
  [78709c64b](https://github.com/Dicklesworthstone/frankensqlite/commit/78709c64b)), including during
  VACUUM (bd-5i5md, [d7db58671](https://github.com/Dicklesworthstone/frankensqlite/commit/d7db58671)).
  A reloaded CHECK constraint keeps its name and source text
  ([acc175c69](https://github.com/Dicklesworthstone/frankensqlite/commit/acc175c69)).
- A full ROLLBACK gives back the rowids its transaction reserved, so AUTOINCREMENT resumes like
  stock (bd-gwoit, [9989f8a3a](https://github.com/Dicklesworthstone/frankensqlite/commit/9989f8a3a)).
- `PRAGMA integrity_check` detects stored STRICT datatype violations
  ([e475bed43](https://github.com/Dicklesworthstone/frankensqlite/commit/e475bed43)), inspects actual
  NULL storage ([c59f03686](https://github.com/Dicklesworthstone/frankensqlite/commit/c59f03686)), and
  names the hidden rowid after an unshadowed alias (bd-uyxzy,
  [90600b0cc](https://github.com/Dicklesworthstone/frankensqlite/commit/90600b0cc)).

Files, WAL and durability:

- **A full filesystem while the WAL index grows is an I/O error, not a SIGBUS, on Linux and
  Android (GH#506).** The `-shm` file and the MVCC companion are allocated before they are mapped
  ([874fb59a5](https://github.com/Dicklesworthstone/frankensqlite/commit/874fb59a5)), and ENOSPC,
  EDQUOT, EIO or EFBIG fail the open cleanly. Where preallocation is not available, the mapping
  is sparse, as in 0.4.10, with one warning per process: filesystems without `fallocate` (NFS before
  4.2, 9p, many FUSE mounts), btrfs extents shared by a snapshot or a reflink copy, macOS, and the
  other non-Linux Unixes
  ([144103f98](https://github.com/Dicklesworthstone/frankensqlite/commit/144103f98); the musl
  build was fixed in [8bcc19af3](https://github.com/Dicklesworthstone/frankensqlite/commit/8bcc19af3)).
  There is no protection on macOS yet.
- On macOS and other non-Linux Unix, each new connection in WAL mode leaked about two main-database
  descriptors (bd-l0rbc, [23079d4e8](https://github.com/Dicklesworthstone/frankensqlite/commit/23079d4e8),
  [0a342d809](https://github.com/Dicklesworthstone/frankensqlite/commit/0a342d809)). This is the fix
  shipped alone as `fsqlite-vfs` 0.4.11.
- A WAL that a stock writer truncated to zero bytes reads as a WAL with no frames, not as corrupt or
  as a recovery in progress (bd-yxqs9,
  [8cc4796da](https://github.com/Dicklesworthstone/frankensqlite/commit/8cc4796da)). Such a
  header-less WAL needs a writable connection to bind a new header before anything can be appended
  to it, and appends answer `BusyRecovery` until then; a read-only open of it with no `-shm`
  answered `BusyRecovery` on every read in this release
  ([#515](https://github.com/Dicklesworthstone/frankensqlite/issues/515); fixed on `main`, see the
  Unreleased section).
- Attaching WAL-FEC to a fully backfilled WAL creates no repair artifacts, so a read-only session
  no longer writes `-wal-fec` files (bd-xsxdz, GH#294,
  [0a87aedd2](https://github.com/Dicklesworthstone/frankensqlite/commit/0a87aedd2)). Close succeeds
  after leaving WAL mode, and a best-effort close finishes when the WAL-FEC detach fails
  ([3490d82a5](https://github.com/Dicklesworthstone/frankensqlite/commit/3490d82a5),
  [ca57387f3](https://github.com/Dicklesworthstone/frankensqlite/commit/ca57387f3)).
- The io-wq worker threads behind FrankenSQLite's io_uring rings are capped
  ([7ac8ed437](https://github.com/Dicklesworthstone/frankensqlite/commit/7ac8ed437)).

### Performance

- `col IN ('a', 'b')` and `key IN (?1, ?2, ...)` on an indexed TEXT or INTEGER column seek the index
  once per value (GH#497, bd-5qu8n,
  [ceb5a7830](https://github.com/Dicklesworthstone/frankensqlite/commit/ceb5a7830),
  [8ce50345f](https://github.com/Dicklesworthstone/frankensqlite/commit/8ce50345f)).
- `WHERE k = ? ORDER BY id` seeks the `k` index and sorts the matches (GH#498, bd-kyh7x,
  [b991b3752](https://github.com/Dicklesworthstone/frankensqlite/commit/b991b3752)).
- Live B-tree encoding no longer loops over short varints (GH#491,
  [bd70b47cb](https://github.com/Dicklesworthstone/frankensqlite/commit/bd70b47cb)).
- `:memory:` commits share an unchanged page directory instead of copying it (GH#494,
  [89b79b717](https://github.com/Dicklesworthstone/frankensqlite/commit/89b79b717),
  [ed831afdd](https://github.com/Dicklesworthstone/frankensqlite/commit/ed831afdd)).
- Single-use CTE dependency forests and schema-only projection CTEs read through the pager instead
  of hydrating rows (GH#493, partial;
  [b1e95e7ea](https://github.com/Dicklesworthstone/frankensqlite/commit/b1e95e7ea),
  [84085123e](https://github.com/Dicklesworthstone/frankensqlite/commit/84085123e)).
- The in-memory fallback warns once per connection, and the row-by-row trigger and FK replays not
  at all (GH#490, [4b83e21b0](https://github.com/Dicklesworthstone/frankensqlite/commit/4b83e21b0)).

Measured against v0.4.10 (see Qualification): 11 of 13 CLI workloads within 2%, and multi-row
`INSERT ... VALUES` 3% faster. Two are slower: 20,000 single-row transactions on `:memory:` by
3.7%, and 2,000 primary-key point SELECTs by 3.9%. Both come from changes between 0.4.10 and this
release, not from its release preparation; a bisect is tracked in
[GH#507](https://github.com/Dicklesworthstone/frankensqlite/issues/507).

### Library surface

The experimental native storage stack gained commit groups with shared durability barriers,
snapshot journal restore, and a replication outbox. Public `Connection` does not select it, and
it carries no claim of production durability.

### Dependencies

`asupersync` stays at 0.5.0. 23 locked packages moved to their latest compatible versions
(including `smallvec` 1.16.3, `serde_json` 1.0.154, `libc` 0.2.190, `zerocopy` 0.8.62). The
`wasm-bindgen` 0.2.128 family is held because 0.2.129's `wasm-bindgen-futures` would put tokio in
`Cargo.lock`, which this project does not allow, and `proptest` stays at 1.11. `cargo-audit`
reports no vulnerabilities in the lock.

### Not in this release (known issues)

The first two were found by this release's gate. They are not new: v0.4.10 behaves the same.

- **In a STRICT table, an `ANY` column stores numeric-looking text as an integer
  ([GH#509](https://github.com/Dicklesworthstone/frankensqlite/issues/509)).** `'000123'` is
  stored as `123`, where stock SQLite keeps the text. The stored value changes without an error.
  Until it is fixed, declare such columns `TEXT`.
- **On a WAL database, a connection that rolls a bulk `INSERT ... SELECT` back to a savepoint and
  releases it then fails every later INSERT
  ([GH#510](https://github.com/Dicklesworthstone/frankensqlite/issues/510))** with
  `database is busy (snapshot conflict on pages: N)`. No data is lost and `integrity_check` stays
  `ok`. A new connection to the same file inserts normally, so reopen the connection to continue.
- **Known corruption under concurrency (bd-ujp0h, bd-2ym3z), reduced but not gone.** Several
  connections on one file inserting rows with implicit rowids can still commit the same rowid
  silently; stock `integrity_check` then reports `Rowid N out of order`.
  [863ecbb50](https://github.com/Dicklesworthstone/frankensqlite/commit/863ecbb50) fixes the
  8-writer quick-balance race that v0.4.10 named (0 of 60 rounds afterwards, against 14 of 40
  before), but the repository's verifier still reproduced it with 4 writers (2 of 60 rounds) and
  with large variable-width rows at 8 writers (5 of 14). The cause is tracked in bd-2ym3z. Until
  it is fixed, a workload whose rowids must be unique across writers should insert through a
  single writer connection.
- GH#506 on macOS and other non-Linux Unix: no SHM preallocation, as in 0.4.10.
- GH#211 (FTS5 external-content rename then `rebuild` can drop a posting), GH#448 (`changes()`
  after `RAISE(FAIL)`) and the GH#492, GH#493 and GH#496 performance items are still open.
- The statements reported in GH#436, GH#438, GH#439, GH#489, GH#491 and GH#504 take about as long
  as on 0.4.10 and remain several times slower than stock SQLite (re-timed on one host; numbers in
  [GH#507](https://github.com/Dicklesworthstone/frankensqlite/issues/507)).
- `WITH c AS (...) INSERT INTO attached.t SELECT ... FROM c` fails with `no such table: c`
  ([GH#511](https://github.com/Dicklesworthstone/frankensqlite/issues/511)); as in v0.4.10.
- Three small stock-parity differences, as in v0.4.10: the storage class of a tied `max()` through
  an ordered subquery, the `table_info` pk ordinal of a key column named through a quoted
  identifier, and the rowids assigned after a rejected `INSERT ... SELECT`
  ([GH#512](https://github.com/Dicklesworthstone/frankensqlite/issues/512)).
- The experimental native commit stack fails three of its own harness tests on injected sync
  failures ([GH#508](https://github.com/Dicklesworthstone/frankensqlite/issues/508)).
- The qualification-test failures inherited from earlier releases (GH#453 through GH#488) are
  not addressed here, except GH#453, whose fix landed on `main` right after the tag. The gate
  results below list the failures that reproduced.

### Qualification

GitHub Actions is off, so the gate is the set of runs below. Linux runs were native x86_64 with
the pinned toolchain (nightly-2026-08-31), on 2026-10-12/13; the test suite, the reproducers and
the benchmarks ran on trj (AMD Threadripper PRO 5995WX, btrfs) from trees exported at the stated
commits.

- **Release gate, on the release commit
  [26aa19afc](https://github.com/Dicklesworthstone/frankensqlite/commit/26aa19afc)** (this section
  was completed on `main` after the tag, when the results were in):
  - `cargo fmt --all --check`, `cargo check --workspace --all-targets --locked` and
    `cargo clippy --workspace --all-targets --locked -- -D warnings` pass. Clippy ran on Linux
    x86_64 and on macOS aarch64.
  - Tests, Linux x86_64 on trj: the unit-test suites, plus every integration target added or
    changed since v0.4.10.

    | Scope | Passed | Failed | Ignored |
    |---|---|---|---|
    | `fsqlite-core`: unit tests (4007 passed, 0 failed) and the 40 changed integration targets | 4154 | 8 | 11 |
    | `fsqlite`: unit tests and `gh519_stock_truncated_wal_reader` | 734 | 0 | 2 |
    | `fsqlite-cli`, `fsqlite-c-api`, `fsqlite-wasm`, `beads-doctor`: complete | 171 | 0 | 0 |
    | `fsqlite-harness`: unit tests and the 3 changed integration targets | 1818 | 4 | 1 |
    | `fsqlite-e2e`: unit tests and `row_value_oracle_e2e` | 719 | 0 | 1 |
    | `fsqlite-types`, `fsqlite-vfs`, `fsqlite-pager`: complete | 2123 | 0 | 17 |
  - Every keeper for the advisory passes: `join_composite_index_lookup_oracle` (GH#502),
    `gh503_multitrunk_freelist_oracle` (GH#503), `bd_ntt2b_self_referencing_exists_delete_oracle`,
    and `bd_row_values_oracle_probe` with `row_value_oracle_e2e` (GH#450).
- **Complete suites, with integration tests and doctests, on
  [144103f98](https://github.com/Dicklesworthstone/frankensqlite/commit/144103f98)** (Linux x86_64
  on trj). The release commit differs from that tree only by lint fixes with no behavior change
  ([d2491f2be](https://github.com/Dicklesworthstone/frankensqlite/commit/d2491f2be),
  [3fb9afcad](https://github.com/Dicklesworthstone/frankensqlite/commit/3fb9afcad),
  [75742f964](https://github.com/Dicklesworthstone/frankensqlite/commit/75742f964): one range
  spelling in a test helper, three `#[allow]` attributes, and two nested `if`s collapsed in a
  harness test) and by this changelog.
  - Storage crates (types, error, vfs, pager, wal, mvcc, btree): 5502 passed, 1 failed, 49 ignored.
  - SQL crates (ast, parser, planner, vdbe, func, the extensions, observability): 4043 passed,
    0 failed, 20 ignored.
- **The 13 failures, each tracked:**
  - 8 in four `fsqlite-core` oracle targets that were added after v0.4.10 without having been run.
    Replayed on the CLIs against stock SQLite, the published v0.4.10 binary gives the same answer
    as this release in every case, or a worse one: GH#509, GH#510, GH#511, GH#512, and the
    hydration bound of GH#493.
  - 3 in the experimental native commit harness tests (GH#508); identical before and after this
    release's own commits.
  - 1 test-fixture defect in `shm_interop`
    ([GH#513](https://github.com/Dicklesworthstone/frankensqlite/issues/513)): a child process with
    a 32 KiB file-size limit inherits the test log as its stdout. With output not sent to a large
    file, the target passed 900 of 900 runs.
  - 1 intermittent `Busy` in the WAL-FEC sidecar suite (GH#453). Its fix landed on `main` after the
    tag.
- **Not run:** the integration targets that are unchanged since v0.4.10 (v0.4.10's gate did not
  run the full suite either), and the all-features matrix.
- **Issue reproducers**, on musl release CLIs with stock sqlite3 3.46.1 as the oracle: GH#502,
  GH#503, GH#450 and the bd-ntt2b DELETE each give the wrong answer on v0.4.10 and stock's answer
  on this release.
- **GH#506 and the SHM fallback**, on the same binaries:
  - `fallocate` refused with EOPNOTSUPP (strace fault injection), with FIEMAP refused as well:
    WAL create and reopen succeed.
  - `fallocate` refused with ENOSPC: a clean `No space left on device` error, never a SIGBUS.
  - A `cp --reflink` copy of a WAL database family on btrfs, made by either version, opens and
    passes `integrity_check`.
  - A v0.4.10 process and this release sharing one WAL database, each as holder and as joiner:
    results identical to the same-version controls, and stock sqlite3 reports `ok` afterwards.
- **macOS** (Apple Silicon, macOS 26.5, APFS): `cargo test -p fsqlite-vfs` 436 passed / 0
  failed. The release CLI creates, reopens and grows a WAL database past its first index region
  (20,000 frames). Interop with the published v0.4.10 macOS binary behaves like the
  same-version controls.
- **Performance:** musl release CLIs of v0.4.10 and of this release, built on trj with the
  release recipe. 13 workloads, 9 alternating rounds each, pinned to 8 cores; median CPU time
  this release / v0.4.10:

  | Workload | Ratio |
  |---|---|
  | 100k INSERTs in one transaction | 0.99 |
  | 100k rows as 200-row multi-VALUES | 0.97 |
  | 5k autocommit INSERTs, WAL | 1.00 |
  | 20k single-row transactions, `:memory:` | 1.04 |
  | full scan + aggregates, 300k rows | 1.00 |
  | GROUP BY, 300k rows | 1.00 |
  | range UPDATE, 100k rows | 1.01 |
  | range DELETE, 100k rows | 1.00 |
  | CREATE INDEX, 300k rows | 1.01 |
  | 2,000 primary-key point SELECTs | 1.04 |
  | 200 indexed range queries | 1.00 |
  | 100k-row indexed join | 1.02 |
  | 200 cold WAL opens (separate processes) | 1.01 |

  Only the two 1.04 rows are distinguishable from noise (rank test p = 0.03 and 0.02). A
  21-round run against `main` as it was before this release's own commits (formatting, lockfile,
  version, GH#450, the SHM fallback) puts those commits at 0.96 for the `:memory:` transactions
  and 1.01 (not significant) for the point SELECTs. The remaining drift comes from the work
  between v0.4.10 and `main`.

---

## [0.4.11] -- 2026-10-09 (crates.io only: `fsqlite-vfs`)

This is a VFS-only patch, published from tag
[`fsqlite-vfs-v0.4.11`](https://github.com/Dicklesworthstone/frankensqlite/tree/fsqlite-vfs-v0.4.11)
(commit `3bb28e731`). It is the v0.4.10 release tree plus one fix, and it follows the 0.4.3 / 0.4.5 precedent.

- **Other crates.** Every other crate stays at 0.4.10 and requires `fsqlite-vfs` by caret, so consumers pick the
  fix up with `cargo update -p fsqlite-vfs --precise 0.4.11`.
- **Not shipped here.** There is no GitHub Release and no binaries. `main`'s other changes since v0.4.10 are not
  included.
- **Next release.** `main`'s `fsqlite-vfs` manifest still reads 0.4.10, so the next uniform release must use 0.4.12
  or later.

### Fixed

- **macOS and other non-Linux Unix leaked main-database descriptors.** A main-DB file opened while the inode held
  lock claims was parked in `deferred_close_files` and closed only once no claim remained. In WAL mode every live
  connection holds a lifetime claim, so that list never drained, and each fresh connection leaked about 2
  descriptors (bd-l0rbc; [`23079d4e8`](https://github.com/Dicklesworthstone/frankensqlite/commit/23079d4e8)).
  A live Agent Mail server on macOS held 4,425 `storage.sqlite3` descriptors after 20.6 hours. The non-Linux open
  path now reuses the retained canonical descriptor when the path still names the same `(dev, ino)` and the
  requested access is allowed (`faccessat` with `AT_EACCESS`, then a second `stat`), as Linux has done since 0.4.9
  with an `O_PATH` witness. Linux behavior is unchanged.
- **Android keeps the ordinary open path** ([`0a342d809`](https://github.com/Dicklesworthstone/frankensqlite/commit/0a342d809)):
  nix does not define `AT_EACCESS` there, so the reuse is not compiled for Android and behavior matches 0.4.10.

Verified on the tag tree: Linux x86_64, non-root, 468 passed / 0 failed / 1 ignored; macOS arm64 427 passed /
0 failed; `cargo publish --locked` verified the package against the published 0.4.10 crates. The crates.io archive
checksum matches the index, its `.cargo_vcs_info.json` names `3bb28e731`, and its `src/unix.rs` is byte-identical
to the tag.

---

## [0.4.10] -- 2026-10-07 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.9...v0.4.10>
(v0.4.9 was cut from a side branch at frozen runtime `23697702`; everything `main` landed after
that commit ships here.)

**What this is.** A uniform release of all 26 public crates. It carries a set of silent
data-integrity fixes that affect every earlier release, so upgrading is recommended for all users.
It also includes the SQL-parity, durability and performance work `main` landed after the v0.4.9
freeze.

### Upgrade advisory: data integrity

The first three issues can leave a file that stock SQLite's `PRAGMA integrity_check` reports as
damaged. After upgrading, run `PRAGMA integrity_check;` on databases written by an earlier version.
If it names an index, run `REINDEX;` (or `REINDEX <index>`). The lost or repeated UPDATE ... FROM
updates and the REPLACE/IGNORE row loss leave no trace in the file, so no check can find them
afterwards; review data written by those statement shapes. The last two items are durability and
checkpoint-safety fixes that prevent losses rather than repair them.

- **Inserts inside an explicit transaction could corrupt a table b-tree (bd-obwsy,
  [9f7534c5c](https://github.com/Dicklesworthstone/frankensqlite/commit/9f7534c5c), hardened by
  d55f379da).**
  0.4.6, 0.4.7 and 0.4.9 are affected (reproduced on their release binaries). A transaction that
  appends many rows of varying width to a table with no secondary index could write a later row
  into a leaf that an earlier split had already rewritten, using that leaf's old header. The row
  overwrote live bytes of another row, on a leaf that was no longer the table's right edge. Inserts
  report success; stock `integrity_check` reports `Rowid N out of order`, `Multiple uses for byte
  N` or a cell offset of 0; `count(*)` can gain a phantom row; and full scans or a later
  `CREATE INDEX` fail as malformed. The owner's report used multi-row `INSERT ... VALUES` through
  the CLI, which corrupts from 64 rows per statement. Through `Connection::execute`, single-row
  INSERTs in one transaction hit the same path. The insert path now replaces its cached leaf
  image whenever the ordinary insert placed a row, and uses it only while it names the current
  right edge. The b-tree refuses an append whose cached header no longer matches the page, and a
  quick balance whose leaf is no longer the parent's right child.
  **Repair:** run `PRAGMA integrity_check` on databases loaded this way. If it reports errors,
  reload the affected tables from their source data. If no source exists, extract what stock
  SQLite can still read (`sqlite3 damaged.db .recover > recovered.sql`), load that into a new
  database, and verify row counts. In the reported file, one row on each damaged leaf was
  unreadable.
- **UPDATE ... FROM lost or repeated updates (bd-b5j26,
  [033d49ac0](https://github.com/Dicklesworthstone/frankensqlite/commit/033d49ac0),
  [826037063](https://github.com/Dicklesworthstone/frankensqlite/commit/826037063)).** Every release
  back to at least 0.3.6 is affected. On a rowid table the statement rewrote the target while
  scanning it:
  - File-backed databases updated only the first matching row. `:memory:` databases updated only
    the rows on the first leaf page.
  - A target row matched by several FROM rows was rewritten once per match, so `SET c = c + d.n`
    compounded and `changes()` over-counted.
  - Assigning the INTEGER PRIMARY KEY moved rows ahead of the scan.

  On a WITHOUT ROWID table, a row matched several times left a duplicate secondary-index entry
  (`wrong # of entries in index ...`; repair with `REINDEX`). UPDATE ... FROM now collects its
  matches first and applies each target row once, using the last matching FROM row as stock does.
  The trigger/foreign-key path gets the same last-match rule.
- **A non-integer rowid or INTEGER PRIMARY KEY value was stored truncated, with malformed index
  entries ([2894fc314](https://github.com/Dicklesworthstone/frankensqlite/commit/2894fc314)).** This
  has shipped since at least 0.3.9. INSERT, UPDATE, UPDATE ... FROM and UPSERT copied the key
  unconverted. The row landed at a truncated rowid (`1.5` became 1, `'abc'` became 0, `'1e1'`
  became 1), and every secondary-index entry kept the raw REAL or TEXT value as its rowid
  (`index key record missing trailing integer rowid`; repair with `REINDEX`). The key now passes SQLite's
  `MustBeInt`: `'7'`, `8.0` and `'1e1'` convert, while `1.5`, `'abc'` and blobs fail with
  `datatype mismatch`.
- **A REPLACE constraint could delete a row that a later IGNORE constraint then kept out
  ([541ca1b6b](https://github.com/Dicklesworthstone/frankensqlite/commit/541ca1b6b)).** UNIQUE
  constraints were checked in creation order. Take a table with `b UNIQUE ON CONFLICT REPLACE` and
  `d UNIQUE ON CONFLICT IGNORE`: a new row conflicting on both deleted the `b` victim and was then
  ignored, losing a row stock keeps. Indexes are now checked in stock's order, so a row that breaks
  two ABORT constraints is reported against the constraint stock names, and an upsert with no
  conflict target acts on the row stock picks.
- **A failed WAL fsync could be followed by a "successful" one that wrote nothing (bd-alyrn,
  [00104ff64](https://github.com/Dicklesworthstone/frankensqlite/commit/00104ff64)).** On Linux a
  failed fsync can leave the pages clean in the page cache. In-doubt commit reconciliation re-read
  them, re-synced and published the commit as durable, so a power loss could drop a commit the
  application had been told succeeded. The WAL now rewrites every frame a failed sync covered before
  any later sync can vouch for them.
- **Checkpoints validate their source frames and refuse an uncommitted WAL tail
  ([b5aa7af07](https://github.com/Dicklesworthstone/frankensqlite/commit/b5aa7af07),
  [0f74043c1](https://github.com/Dicklesworthstone/frankensqlite/commit/0f74043c1)).** Before any
  database write, a checkpoint verifies generation, salts, page numbers and the checksum chain of
  every frame it copies, and the copy pass re-checks them. A WAL that does not end at a commit
  marker is not backfilled or reset.

### Storage and durability

- `:memory:` databases reuse their freelist, and a rewound EOF never re-grants parked pages. Before,
  freed pages were never reused and `integrity_check` could report `page N is never used`
  ([ea552c9ab](https://github.com/Dicklesworthstone/frankensqlite/commit/ea552c9ab)).
- Raw cell decoders stop at the page's usable size, so a pointer into the reserved trailer reads as
  corruption (GH#426, bd-i2pad, [540740799](https://github.com/Dicklesworthstone/frankensqlite/commit/540740799)).
- Stock databases whose empty schema left the header's schema format and text encoding at 0 now open.
  Typical sources are a file that only saw `PRAGMA user_version`, or a VACUUMed empty schema
  (bd-25au0, [b40125341](https://github.com/Dicklesworthstone/frankensqlite/commit/b40125341)).
- GH#443 follow-up: when a stock read-only connection leaves a header-only WAL and a `-shm` holding
  stock's unindexed-empty header with its own salts, a writer binds that header instead of failing
  every commit with `BusyRecovery` (v0.4.9 fixed the 0-byte-WAL shape;
  [d2a6d655c](https://github.com/Dicklesworthstone/frankensqlite/commit/d2a6d655c)).
- VACUUM INTO reads through a WAL a reading peer pins (bd-5unvm,
  [5dc1a17e1](https://github.com/Dicklesworthstone/frankensqlite/commit/5dc1a17e1)).
- A full WAL-FEC repair queue, or a stopped repair worker, defers repair admission instead of
  refusing the write. A commit larger than one RaptorQ source block gets no repair group instead of
  aborting the process (bd-jyeus, [e82a0a9c2](https://github.com/Dicklesworthstone/frankensqlite/commit/e82a0a9c2),
  [0035a8ff9](https://github.com/Dicklesworthstone/frankensqlite/commit/0035a8ff9),
  [e1b5f0fd1](https://github.com/Dicklesworthstone/frankensqlite/commit/e1b5f0fd1)).
- `synchronous=FULL` commits fsync only the WAL, as stock SQLite does. The `-wal-cert` sidecar
  record is no longer fsynced per commit, because recovery does not depend on it (bd-qyekq,
  [3fdad335a](https://github.com/Dicklesworthstone/frankensqlite/commit/3fdad335a)).
- 0.3.x/0.4.x guard follow-up: a namespace joiner holds its WAL-lifetime claim before releasing the
  gate, so a slow joiner is never refused as a 0.3.x peer
  ([0e755afaf](https://github.com/Dicklesworthstone/frankensqlite/commit/0e755afaf)). The guard
  remains one-way: never let a 0.3.x engine share a WAL database with 0.4.x+ (see README).
- Shared WAL-index words are copied through their atomic access paths
  ([27c629f14](https://github.com/Dicklesworthstone/frankensqlite/commit/27c629f14)). Mapped
  write-completion relays publish before waking waiters
  ([fed3e0676](https://github.com/Dicklesworthstone/frankensqlite/commit/fed3e0676)).
- `PRAGMA integrity_check` also validates NOT NULL/CHECK constraints in attached databases
  ([11cf3e7f6](https://github.com/Dicklesworthstone/frankensqlite/commit/11cf3e7f6)).
- fsqlite-vfs builds on macOS again (`getgroups` on Apple targets,
  [56ff324eb](https://github.com/Dicklesworthstone/frankensqlite/commit/56ff324eb)).

### SQL correctness (stock-SQLite parity)

- Rowid edge cases:
  - Once a table holds rowid 9223372036854775807, an automatic rowid is a random unused one, and
    AUTOINCREMENT fails `SQLITE_FULL` (bd-6i9c5, [864f31114](https://github.com/Dicklesworthstone/frankensqlite/commit/864f31114)).
  - Explicit rowids keep working in a table that holds the largest rowid
    ([9308c4bb6](https://github.com/Dicklesworthstone/frankensqlite/commit/9308c4bb6)).
  - A statement rolled back inside a transaction gives its implicit rowids back (bd-8a8pr,
    [fc7e91333](https://github.com/Dicklesworthstone/frankensqlite/commit/fc7e91333)).
- Rowid range seeks with a TEXT, REAL or BLOB key follow stock instead of truncating the key
  ([cb4a0e0d4](https://github.com/Dicklesworthstone/frankensqlite/commit/cb4a0e0d4)). An index
  equality pins a seek only under its key's collation, and ORDER BY index scans seek their equality
  prefix under the comparison's affinity
  ([2ca04510c](https://github.com/Dicklesworthstone/frankensqlite/commit/2ca04510c),
  [68e4e4221](https://github.com/Dicklesworthstone/frankensqlite/commit/68e4e4221)).
- Triggers:
  - A trigger's `SELECT RAISE(...)` fires for every branch shape.
  - NEW rows compute generated columns.
  - Multi-row UPDATE/DELETE fire triggers in rowid/primary-key order.
  - Trigger subqueries resolve bare ORDER BY/LIMIT names and non-TEMP tables like stock.

  (bd-u7sv8, bd-yb70u, bd-f5s4w, [1711fdd43](https://github.com/Dicklesworthstone/frankensqlite/commit/1711fdd43),
  [369627058](https://github.com/Dicklesworthstone/frankensqlite/commit/369627058),
  [cfebcdd66](https://github.com/Dicklesworthstone/frankensqlite/commit/cfebcdd66)).
- Name resolution:
  - A subquery's ORDER BY/GROUP BY resolves outer columns, as in SQLite 3.53 (bd-r9dhv).
  - ORDER BY expressions resolve result aliases after FROM columns (bd-z0qqu).
  - An UPDATE ... FROM subquery alias no longer shadows a table (bd-hhh47).
  - A DQS string beside a table-valued source is a literal (bd-clpji).
- Aggregates and joins:
  - With several min()/max() aggregates, the last one picks the bare-column row (bd-6lijo).
  - Aggregates over a join keep scan order (bd-54sjx).
  - The in-memory EXISTS probe compares typeless columns without conversion.
  - IN-subquery affinity follows declared operands.
- Schema and ALTER:
  - Stored schema text is the statement's own text (bd-i95tk).
  - ALTER TABLE ADD COLUMN accepts every constant default stock folds.
  - ALTER on an attached schema runs there (bd-az48n).
  - A re-prepared statement replaces the stale one in its handle (bd-tj811).
  - A transaction begun by SAVEPOINT commits its DDL.
  - INSERT ... SELECT into a missing table says `no such table`.
- Time travel and attached databases:
  - `FOR SYSTEM_TIME` queries answer as stock frozen at that commit (bd-zjocc).
  - Mixed main + attached SELECTs route FROM-less bodies and aggregates correctly (bd-gjlhh).
  - RELEASE-as-COMMIT commits attached participants.
- The first-open migration pass also runs on stock-created databases FrankenSQLite wrote last
  (bd-6jf9o review, [f4bfc92df](https://github.com/Dicklesworthstone/frankensqlite/commit/f4bfc92df)).
- The shell supports `.print` (bd-5heqk).

### Performance

The numbers below are the measurements recorded in the cited commits; each commit message gives
its method and the stock `sqlite3` comparison.

- UPDATE ... FROM pass 1 seeks the target row by rowid instead of scanning per FROM row. It had been
  quadratic: 10k rows took 39.8 s, against stock's 0.05 s (bd-7orjo,
  [f3754e71e](https://github.com/Dicklesworthstone/frankensqlite/commit/f3754e71e)).
- A join on several ON equalities seeks the composite index they pin: over 120 s (killed) -> 0.06 s
  ([07cf6c5a6](https://github.com/Dicklesworthstone/frankensqlite/commit/07cf6c5a6)). Related changes:
  - A composite equality scan seeks every pinned key term (bd-gtjlv).
  - A numeric probe into a typeless index seeks a map of its numeric TEXT keys (bd-k8ebx).
  - Index seeks compare stored keys in place (e8cdc0530).
- Trigger bodies bind OLD/NEW as parameters and reuse one compiled program (bd-l9j27). Trigger guard
  probes seek their outer table (bd-8c68u). Row-by-row DML replay reuses one compiled program
  (bd-so2el).
- Correlated subqueries reuse their read cursors across outer rows (bd-a3g0m). Whole-table
  min()/max() bare columns are tracked in bytecode: 1.10 -> 0.60 s (bd-6hoc8,
  [28049bf87](https://github.com/Dicklesworthstone/frankensqlite/commit/28049bf87)).
- `:memory:` time-travel snapshots share committed pages. On a 3M-row database a one-row COMMIT went
  from 0.56-0.62 s to 0.021-0.023 s (bd-jdjee,
  [c52c2f112](https://github.com/Dicklesworthstone/frankensqlite/commit/c52c2f112)).
- WAL-FEC keeps its sidecar group map between commits and encodes small groups through source
  coefficients (bd-sid3o, bd-wqmil).
- Futex wakes are skipped when nothing waits (bd-ih8ak). The first-open migration pass skips databases
  another SQLite library wrote last (bd-6jf9o). UPDATE no longer pays per row for a witness copy
  (bd-yb70u, GH#439).

### Experimental (not reachable from `Connection`)

The `fsqlite-wal`/`fsqlite-pager` native snapshot-transaction stack grew substantially. It covers
VFS-backed two-barrier commit publication, RaptorQ-authenticated recovery, page history and
reclamation, native schema catalogs, and engine dispatch of native transactions. Public
`Connection` does not select it. It ships as library surface for integration work, with no claim
of production durability. Its commits landed without a compile. The release gate made them build
and pass `clippy -D warnings` without behaviour changes
([311e156d0](https://github.com/Dicklesworthstone/frankensqlite/commit/311e156d0)), but several of
its `fsqlite-harness` integration tests still fail at runtime. One example: VDBE execution over a
native transaction errors, because the engine-side binding is not implemented yet.

### Not in this release

- GH#492 (prepared-read hydration cost) and GH#493 (schema-only CTE hydration) are **not fixed**.
  Main carries only unapplied candidate patches under `artifacts/gh492` and `artifacts/gh493`, plus
  their regression tests. The GH#492 growth case is `#[ignore]`d. Its non-ignored
  `small_ordinary_database_retains_memdb_acceleration` fails at this release: it expects a point
  read on a fresh small-database open to hydrate rows into MemDatabase, but current reads use
  storage cursors without hydration. The test was committed unrun and encodes the candidate
  patch's intended behaviour.
- Still open from the v0.4.9 known issues:
  - **known data loss (GH#211):** renaming an explicitly declared FTS5 external-content table
    and then requesting `rebuild` can silently drop a posting. Content rows survive and ordinary
    integrity checks pass;
  - table-free constant row-value equality returns an unsupported-expression error (GH#450);
  - the issues filed from the v0.4.9 full-workspace gate, GH#453–GH#488 (mostly qualification
    harness and timing keepers; see the 0.4.9 section).
- Still open, newer reports:
  - **known corruption under concurrency (bd-ujp0h):** with several connections on one file each
    inserting rows with implicit rowids, two connections can commit the same rowid, silently:
    every INSERT reports success. Stock `integrity_check` then reports `Rowid N out of order`.
    Eight writers looping autocommit INSERTs hit it in 1 to 4 of every 12 runs on the Mac, and on
    Linux, and it predates this release. The prepared-INSERT fast path takes implicit rowids from
    connection-local state instead of the shared concurrent rowid allocator. Two writers can then
    take the same `max(rowid) + 1` and land it on different leaves, so the page-level conflict
    check sees no overlap. A fix is in progress for 0.4.11. Until then, a workload whose rowids
    must be unique across writers should insert through a single writer connection;
  - per-statement WARN flood on the trigger/FK INSERT fallback (GH#490);
  - slow expression-index creation and primary-key inserts (GH#489, GH#491).

### Qualification

All runs used Linux x86_64 rch remote workers, driven from trj with the pinned toolchain
(nightly-2026-08-31), on 2026-10-06/07. GitHub Actions is off.

The release tree is 311e156d0 plus the bd-obwsy keeper, its fix (9f7534c5c) and the follow-up
guard (d55f379da), plus the version bump.

- **Release tree:**
  - `cargo fmt --check`, `cargo check --workspace --all-targets --locked` and `cargo clippy
    --workspace --all-targets --locked -D warnings` pass.
  - bd-obwsy keeper: 8/8.
  - `fsqlite-btree` lib: 525/0.
  - Insert-path keepers, all pass: `ipk_value_must_be_int_oracle`,
    `unique_conflict_check_order_oracle`, `rowid_max_explicit_insert_oracle`,
    `bd_8a8pr_rowid_reuse_after_statement_rollback_oracle`, `bd_b5j26_update_from_rowid_oracle`,
    `btree_page_geometry_stress_oracle`, `index_seek_random_differential`,
    `memory_mirror_snapshot_differential`, `gh441_vacuum_overflow_freelist`; facade
    `concurrent_freelist_churn`.
  - `fsqlite-core` lib: 3979/0.
  - The owner's CLI repro (`minimal_repro.py`, run against a debug CLI freshly built from this
    tree): all 7 cases OK.
- **Library suites at 1abcf186f/311e156d0, 0 failures each:**

  | Crate | Passed |
  |---|---|
  | types | 571 |
  | vfs | 468 |
  | wal | 1034 |
  | pager | 1016 |
  | mvcc | 1667 |
  | parser | 613 |
  | planner | 457 |
  | vdbe | 1182 |
  | core | 3979 |
  | fsqlite | 732 |
  | c-api | 55 |
  | btree | 523 |
  | wasm | 15 |
- **Keepers at 311e156d0:**
  - All 92 `fsqlite-core` integration targets on the release keeper list pass except one:
    `gh492_bounded_post_commit_hydration`'s small-database case (see "Not in this release").
  - The fault-injection failed-commit keeper passes.
  - Facade: `compat_tests` 88, `gh444`, `single_writer_ddl_batch_freelist_republication`,
    `concurrent_freelist_churn`.
  - CLI: `gh442` VACUUM INTO beside an idle peer.
  - e2e: SSI serialization, transactions, golden bytecode shapes, savepoint and isolation
    oracles, q37ep index-churn corruption, WAL-corruption recovery, crash-recovery matrix.
  - VDBE golden snapshots: 8.
- `wal_fec_commit_pipeline`: three tests exceeded their 30 s wall-clock budget for the WAL-FEC
  sidecar lock on a worker at load 25 on 10 cores. A rerun on another worker passed 16/16 in
  6 s.
- **Performance of the bd-obwsy fix:** the peer's callgrind run of 200k small rows measured the
  whole program at -0.3% instructions and `execute_morsel_insert` at +0.44%. Here, same-profile
  release CLIs (LTO off, 16 codegen units, built from 311e156d0 and this tree) ran 9 alternating
  rounds each, pinned to 8 cores on trj on 2026-10-07. Median CPU fix/base:
  - 100k single-row INSERTs: 0.956;
  - 100k rows in 200-row statements: 1.012;
  - 6k wide rows in 200-row statements: 0.999.

  The pre-fix binary corrupted the last two workloads (stock `integrity_check` failed); the fixed
  binary passed every run.
- **Not run:** the full workspace test suite and the all-features matrix. The experimental
  native-stack harness tests pass 59/74 (bd-qmadh).

---

## [0.4.9] -- 2026-10-03 (replacement for withdrawn v0.4.8)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.7...v0.4.9>

Published on 2026-10-03 at 21:10 UTC from release commit
[`1eacdbe0`](https://github.com/Dicklesworthstone/frankensqlite/commit/1eacdbe0d4bd1d864b106c096c904d2a3933ab46).
The qualification and known-issue notes below preserve the frozen release's evidence
and limitations; they are not a new qualification of later `main` commits.

A uniform release of all 26 public crates, superseding the unchanged, unreleased v0.4.8 tag.
It carries the fixes and performance improvements described in the withdrawn candidate section below,
plus subsequent changes through frozen source `23697702`. Later main commits are excluded
and will ride in 0.4.10, including WAL-FEC full-queue admission deferral (e82a0a9c2)
and reserved-trailer B-tree decoder bounds (540740799).

### Correctness and availability

- CREATE TABLE AS SELECT quotes the destination identifier before replaying rows into one prepared
  INSERT. Embedded quotes cannot redirect copied rows into an existing table (GH#447,
  [c23ca7fb4](https://github.com/Dicklesworthstone/frankensqlite/commit/c23ca7fb4)). The stock-SQLite
  regression checks copied rows, preservation of the original table, transactions and rollback.
- CTAS preserves changes(), total_changes() and last_insert_rowid(), matching SQLite (GH#439).
- CREATE TEMP TABLE AS SELECT uses the TEMP namespace; TEMP and main CTAS under a TEMP
  shadow retain inferred affinities for later INSERTs. STRICT ANY preserves numeric-looking text
  in BEFORE trigger frames; row-level DELETE triggers observe each preceding row deletion
  (bd-y26jy, bd-oh34i, bd-5pt42, 7ab6b88a9).
- A first write after stock SQLite's empty, unindexed WAL establishes the WAL generation instead of
  refusing BusyRecovery (GH#443, 4b48c95d8).
- VACUUM INTO can complete beside an idle peer process (GH#442, cf0621d0c).
- Append fit checks happen before allocating overflow chains (GH#441, 157826d97).
- A rejected UPDATE restores the exact index entries removed by that row, including
  expression and partial indexes, and leaves unchanged indexes alone (GH#449, bd-c1vth,
  574c26216, 36b2bbf09). Skipped UPDATE rows do not run parent foreign-key actions or
  AFTER UPDATE triggers; completed trigger steps remain in total_changes(). The regression
  closes the connection, then requires stock SQLite first reopen to report one indexed row
  and a clean integrity check. This prevents new duplicates; it does not repair affected files.
- Autocommit settles an in-doubt physical commit before answering the caller. A write
  that is durably committed reports success, preventing a retry of an already committed row
  (bd-no6zz, 4802106f4).
- Prepared statements re-prepare after another connection changes the schema, matching
  sqlite3_prepare_v2 (bd-9zuif, a5bc1e3c4). UPDATE FROM resolves source tables when
  UPDATE triggers or foreign-key work select the replay path (bd-2jbl5, 2b6215390).
- Join keys apply SQLite comparison-affinity rules across lookup, nested and hash paths,
  including numeric-looking TEXT in typeless columns and three-table TEXT-affinity
  chains (GH#445/GH#446, bd-kr6hf, bd-y5mc8, 23697702a). Join ORDER BY honors
  ordinal references and DESC.
- Bare columns beside one min()/max() come from the extremum row on general source and
  join paths, including HAVING that repeats the aggregate. Unaliased result expressions
  are named by their source text (bd-xik4y, 14d607a51).
- FROM-subquery flattening preserves an inner ORDER BY and keeps mixed-star expression projections
  materialized, preserving aggregate tie values and the first exposed duplicate column name. Ordinary
  aggregate and expression projections can still flatten (bd-5ap77).

### Performance

- Repeated prepared VDBE reads on unchanged file databases avoid a whole MemDatabase
  reload per execution; publication/schema checks still guard peer changes. EXPLAIN
  QUERY PLAN names every equality-bound key column (bd-xml0z, 43ec7360d).

- UPDATE rewrites rows in place when the rowid cannot change, with position-revalidated
  fallback for resized records; rowid-changing assignments retain the existing path
  (bd-9ag5r, GH#439, a31225e8b).
- Cell-slot entries are reused across row rewrites, reducing UPDATE/DELETE cache churn. CTAS retains
  rows at their actual width and uses prepared row replay (GH#438/GH#439, c23ca7fb4).
- Short-record DEFAULT values are cached per schema generation instead of re-parsed per execution
  (GH#440, 5c18621ef).
- Exact zero-match seeks on implied partial/composite indexes avoid fallback full scans
  (GH#444, 71556d531).
- Trigger WHEN subqueries compile once and bind per row (bd-ry6x7, 36f13401a).
- FROM-subqueries beneath outer aggregate/expression projections can flatten; table-function sources
  and deterministic aliased expressions are included when their name scope is unambiguous (bd-5ap77).

### Qualification

Pinned-nightly remote formatting and workspace/all-target Clippy pass. The complete frozen
workspace gate executes 74 bounded jobs over all 28 members (26 public crates), including
all 1,136 default integration targets, units, binaries, doctests and declared required-feature
supplements. Reported results are 25,211 passed, 68 failed and 208 ignored; these are result
rows rather than a unique-test count. All original failures are retained, independently reviewed
and classified below under the release ruling. An optional test=false pager benchmark is
compiled only, without a timing claim. This qualification does not claim that every assertion
passes. Locked Cargo metadata and workspace check pass after the metadata-only version bump.

The release uses a direct child of frozen runtime 23697702. Only Cargo manifests, the lockfile
and this release's changelog differ; runtime source blobs and modes are preserved.
Native build, signed-asset and published installer/upgrade evidence
is recorded separately with the release. Later main changes are reserved for 0.4.10.

### Known issues

Concurrent-DDL qualification expects an old-schema writer to commit after a
peer publishes a schema change, but both versions refuse with BusySnapshot
(GH#476); later row/integrity assertions do not run. The fresh-reader checkpoint
campaign also exceeds its idle progress budget on both versions (GH#477),
without any successful nonempty TRUNCATE. The old run additionally reports
four reader Busy errors absent from frozen source; campaign equivalence and
corruption are not established. A local-DDL prepared-statement keeper rejects
the same invalid-arity error on both versions (GH#478), separately from the
peer-DDL reprepare change. Original assertions and failures remain recorded.

Table-free constant row-value equality predicates return an explicit unsupported
expression error instead of SQLite results (GH#450). The full stock-oracle
target reproduces exactly on v0.4.7 and frozen source, with four other tests
passing. This known SQL limitation remains unfixed; the repro performs no
persistent writes and does not demonstrate corruption.

Four Turso provenance/report qualification checks require Git custody absent
from remote source copies (GH#474); all four failures reproduce on v0.4.7,
and their contract assertions remain uncompleted. Three synthetic SSI timing
budgets also fail on both versions (GH#475). The unchanged workload exercises
in-memory MVCC transactions and simulated CPU work; it does not qualify SQL
throughput or file durability. Its false-positive counters are heuristic.
No performance equivalence or satisfaction of those budgets is claimed.

The forced-index range opcode keeper conflates any `IdxRowid` traversal with
a range-bound seek (GH#473). Both v0.4.7 and frozen source reproduce the same
assertion for an explicitly forced unrelated index; the seven stock-SQLite
result cases pass, including that query. The original keeper is retained.

Three contention/publication qualification failures also reproduce on v0.4.7
(GH#470–472). The SSI workload requires a local publication sequence after every
COMMIT, including read-only paths; the failed operation is unlogged, so that
specific explanation remains a source-review hypothesis. The WAL mode fixture
counts four JSON publication events against seven durable certificates; all
seven certificate epochs appear on the console, and stock-row/integrity checks
pass before its count assertion. Its thread-local logger misses main-thread JSON
events. A two-second cross-table progress keeper can expire before worker
connections finish opening. These inherited failures remain recorded with the
original expectations unchanged; they do not establish equivalent performance,
indefinite starvation or database corruption.

The two-second four-writer fairness keeper can fall below its existing Jain-index
bound (GH#469). A paired same-host comparison of unchanged compiled full targets
reproduces this failure on both v0.4.7 and frozen source. This establishes a
previous-release bounded-measurement failure, without demonstrating equivalent
performance, persistent starvation, data correctness or durability. The release
keeps the keeper and its original failure recorded.

The reference-index target expects 15 files from an uninitialized pinned SQLite
submodule and flags three unchanged explanatory comments as direct imports (GH#467).
An actual v0.4.7 run reproduces all four failures with identical reported paths
and markers. The RaptorQ debug decode timing keeper also exceeds its existing
computed bound on both revisions (GH#468); decoder correctness checks pass.
These verified previous-release qualification failures remain recorded without
weakening the source audit or timing assertion.

The corpus hash lock and runtime-stub source inventory also fail on v0.4.7
(GH#466). Both revisions observe the same hash mismatch across the same 35
conformance files. Stub complaints include inherited gaps plus intentional
function movement and a new aggregate-JOIN optimization decline that retains
the general fallback. These source/fixture contract failures do not report wrong
SQL rows. The frozen release preserves the contracts and records their follow-up.

The direct in-memory SSI witness scenario refuses both writers after forming an
active dangerous dependency structure, while its fixture expects the first writer
to commit (GH#463). The test and MVCC source are unchanged since v0.4.7, where an
actual comparison run reproduces the same three failures. This synthetic direct-API target
does not execute Connection SQL or persist a database, and demonstrates no wrong
committed state. Follow-up must retain both commit errors and validate the resulting
state rather than weaken SSI checks.

Two synthetic performance-harness assertions fail under parallel load (GH#464):
swizzle traversal result checks pass, but relative timing/depth checks fail; an
isolated arena GC fixture exceeds its existing 5 ms tail limit once. The relevant
source and fixtures are unchanged since v0.4.7; exact previous-release runtime
comparisons pass both targets and do not reproduce the current timing failures.
No correctness or persistence failure is reported by
these targets, and this release makes no performance claim from those measurements.

The core SQL checksum manifest has 24 parser and four execution mismatches
(GH#465). Actual v0.4.7 and frozen-source runs produce exactly the same mismatch
records, including all four execution hashes. This verified pre-existing failure
remains recorded; hash-only output does not establish stock-SQLite parity, and
the frozen release leaves the golden manifest unchanged.

Two autocommit prepared-schema unit tests still require `SchemaChanged` after
unrelated peer DDL, while the intended sqlite3_prepare_v2 behavior now re-prepares
and succeeds (GH#462). The complete core target reports 3,967 passes, these two
expectation failures and four ignored tests; exact filtered reruns receive the
correct alpha row and one inserted row. The peer-DDL stock oracle and a held
prepared-statement stock oracle pass, including explicit-transaction boundaries.
Both old unit tests pass on v0.4.7; these new expectation mismatches follow the
intentional compatibility change rather than a wrong SQL result.
The old zero-refresh assertions describe the superseded refusal path and need
fixture alignment; the frozen release leaves them recorded without weakening them.

Four golden bytecode records are stale after the intended rowid affinity,
UPDATE conflict/row-rewrite and three-table DESC fixes (GH#461). The snapshot target
passes on v0.4.7 and reports these four expectation mismatches on frozen source;
independent review checks the changed opcode flags and sorter key/direction fields.
The release keeps the frozen source and records the fixture follow-up.

Under heavy parallel load, the Linux cancellation keeper missed its 5 ms wall-clock
bound once; isolated current and v0.4.7 keepers both pass the existing bound (GH#459). The
cancellation implementation is unchanged. A pager mock fault keeper also missed its
coalesced-batch setup, then passed in isolation (GH#460). Original failures remain
recorded; thresholds and durability assertions are unchanged.

In an explicitly embedded process that also links stock SQLite, the Unix WAL peer
probe can release a live stock reader's POSIX locks when namespace contention causes
a temporary main/SHM descriptor probe (GH#455). A refused open reproduced this with
a separate legacy namespace holder: TRUNCATE changed from Busy to success while the
stock reader stayed open. The reproducer proves lost lock protection, without checking
reader results or persistent corruption. The default engine/CLI graph does not link
stock SQLite; this mixed-library interoperability limitation needs a robust fix.

Two auxiliary harness failures also reproduce on released v0.4.7: e2e-runner reports
RawIdentical with logical_match=false (GH#456), and the mt-oltp tiny-run reader encounters
a snapshot-binding Busy refusal (GH#457). Their binary source is unchanged. These
remain follow-ups; neither failure alone establishes physical corruption.

The optional verification-contract scorecard fixture sets an invalid release threshold
of 0.0 below its 0.70 floor (GH#458), so its base-gate assertion fails before contract
evaluation. The fixture and validation code are unchanged since v0.4.7.

The source-policy check for direct Cx constructors flags compound-cfg test modules
plus the separate recovery utility (GH#452). The scanner and reported source files are
unchanged since v0.4.7, where the same test fails with the same reported locations.
This pre-existing source-policy failure remains a follow-up.

The optional strict parity-certification inventory is missing the DELETE row-replay and
SELECT short-circuit/subquery decision reasons (GH#454). Its source-coverage check
fails; this is an inventory follow-up and does not establish a normal SQL-result failure.

The WAL-FEC sidecar integration suite can intermittently report Busy during parallel
fixture mutations (GH#453); the same target also fails on v0.4.7 with unchanged source
and tests. The isolated v0.4.7 reclaim test passes. The checkpoint caller defers sidecar
reclamation errors after a durable reset and retries later; this test failure does not
establish primary-WAL corruption.

A BEFORE DELETE trigger that raises FAIL can leave `changes()` at zero instead of the
number of rows deleted before the error (GH#448). The counter mismatch also reproduces
on released v0.4.7; row-level FAIL/ABORT effects on current main pass the stock oracle.

GH#445/GH#446 are addressed by the frozen source and its focused stock-SQLite join
oracles pass. Published-release checks remain pending. Cross-process WAL
compatibility changes also remain under qualification. Windows VACUUM INTO and
WAL-FEC recovery remain as described in the prior-release section.

- Auxiliary source/tracker audits remain red: [#479](https://github.com/Dicklesworthstone/frankensqlite/issues/479) reproduces the previous release's missing Git-custody failures; [#480](https://github.com/Dicklesworthstone/frankensqlite/issues/480) reports 31 inherited missing acceptance criteria plus six additional task records. These are documented metadata gaps, with original failing receipts retained.

- Workspace-layering fixtures retain eight previous-release member/feature checks and two stale native `opt-level="z"` checks after the intentional switch to optimization level 3 ([#481](https://github.com/Dicklesworthstone/frankensqlite/issues/481)). The wasm release script retains its size optimization override. This classification makes no measured performance or size claim.

- **Unfixed inherited FTS posting loss** ([#211](https://github.com/Dicklesworthstone/frankensqlite/issues/211)): renaming an explicitly declared external-content table and requesting rebuild succeeds instead of refusing the missing source. A known posting disappears from live queries and stock SQLite's first reopen after an awaited close, on both v0.4.7 and frozen source. Primary content rows survive and ordinary integrity checks pass; those checks do not establish FTS consistency. FTS5 is built by default. This release records the defect under JE's pre-existing-defect ruling and does not claim a fix, whole-index preservation or successful recovery.
- JSON5 read-function differences also reproduce exactly on v0.4.7 ([#482](https://github.com/Dicklesworthstone/frankensqlite/issues/482)): two constant SELECT expressions return results where the reference records an error, and a returned JSON TEXT array uses different escape spelling despite equivalent decoded strings. Original oracle failures are retained; no persistent table writes are involved.

- A custom R-tree module is unavailable to its first SELECT after awaited close, reopen and re-registration ([#483](https://github.com/Dicklesworthstone/frankensqlite/issues/483)). The full v0.4.7 target reproduces the same refusal after the live UPDATE/rollback/delete-refusal assertions pass. This is an inherited module lifecycle defect; persisted row erasure is not established. Built-in R-tree update cases pass.
- The snapshot sender fixture with a 65,536-byte page and one-byte symbols refuses during prepare() with TooBig on both versions ([#484](https://github.com/Dicklesworthstone/frankensqlite/issues/484)). Its intended oversized repair-symbol assertions are not reached; no snapshot recovery claim is made.

- Two unchanged debug timing keepers missed their original limits in the full frozen gate ([#485](https://github.com/Dicklesworthstone/frankensqlite/issues/485)): covering-index probes averaged 125,311 ns against 100,000 ns, and FK insert growth was 2.51x against 2.5x. Both complete targets pass on v0.4.7 and frozen source in the same-host comparison (39 tests passed, four ignored); frozen probes average 65,997 ns and FK growth is 0.95x. These are intermittent test-profile qualification failures, not established inherited failed thresholds or a persistent performance regression. FK inserts exercise an ordinary SQL path. Original failures and limits remain intact; performance equivalence is not claimed.

- Three inherited durability/concurrency qualification failures reproduce in the complete v0.4.7 targets ([#486](https://github.com/Dicklesworthstone/frankensqlite/issues/486), [#487](https://github.com/Dicklesworthstone/frankensqlite/issues/487), [#488](https://github.com/Dicklesworthstone/frankensqlite/issues/488)). The crash fixture incorrectly expects two acknowledged FULL-synchronous autocommit mutations to disappear; stock first reopen preserves exactly their 5,000 surviving rows and passes integrity checking on both versions. A concurrent schema/DML keeper excludes SchemaChanged from its retry class and stops before final index/row checks. A file-backed keeper reads unattributed global profiling counters and fails before durability checks (old parks=1, frozen parks=2); its complete unchanged frozen case passes in isolation with stock-peer rows before close and both-engine reopen checks. Original failures remain recorded; no broad durability, campaign-integrity or performance-equivalence claim follows.

---

## [0.4.8] -- 2026-10-02 (withdrawn; unreleased tag)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.7...v0.4.8>

**Withdrawn before publication.** Release qualification found that `CREATE TABLE ... AS SELECT`
with an embedded quote in its table name could write the copied rows into a different existing table,
leaving the requested copy empty. The optimized v0.4.8 candidate reproduced the wrong-table write
against stock SQLite. The v0.4.8 tag stays unchanged and unreleased. The fix already on `main`
([c23ca7fb4](https://github.com/Dicklesworthstone/frankensqlite/commit/c23ca7fb4)) escapes the table name
before prepared row replay; published v0.4.9 carries that fix and its stock-SQLite regression test.

**Planned scope, retained for history.** A uniform release of all 26 public crates. It restores 0.4.6's prompt refusal for strict
read-only opens, which 0.4.7 regressed (consumers that pinned 0.4.6 for that reason can move up). It also
fixes the rare double-granted page that 0.4.7 listed as a known issue (bd-sx5w2) and wrong join results
under mixed column affinities, and it carries the fixes and performance work landed on `main` since v0.4.7.

### Data integrity

- **Concurrent writers with TRUNCATE checkpoints could grant one page to two b-trees (bd-sx5w2,
  [566494d57](https://github.com/Dicklesworthstone/frankensqlite/commit/566494d57)).** A commit folded
  abandoned end-of-file pages into the freelist after judging them against a WAL generation that a
  checkpoint had already ended, so a page a peer had made live was published as free and granted again
  (stock `integrity_check`: "wrong # of entries in index"). The fold now drops candidates judged in an
  older generation instead of freeing them; at worst that leaves an unreferenced page for orphan repair.
  This was the P0 known issue in 0.4.7.
- **A failed autocommit commit no longer leaves uncommitted schema in the connection (bd-q1b7z,
  [549d83e8e](https://github.com/Dicklesworthstone/frankensqlite/commit/549d83e8e)).** A `CREATE INDEX`
  whose commit lost a race could fail with "index already exists" when retried, and a later `DROP INDEX`
  with "no such index". The database file was always correct.
- **Bounded `DROP` teardown counts overflow pages (bd-fmhvo,
  [685eedbf8](https://github.com/Dicklesworthstone/frankensqlite/commit/685eedbf8)).**

### Availability

- **Strict read-only opens refuse a stale WAL index promptly again
  ([48c6cf6b2](https://github.com/Dicklesworthstone/frankensqlite/commit/48c6cf6b2)).** 0.4.7 retried
  every schema-only open for at least 30 s on `BusyRecovery`. A read-only open never performs WAL-index
  recovery, so a strict open beside a stale index waited 30 s per attempt instead of failing at once
  (cass's GH477 status/health probes took ~744 s or failed). Only writable opens retry the whole bootstrap
  now.

### SQL correctness

- **Join lookups honor comparison affinity (found in the 0.4.8 release review,
  [dc4926c30](https://github.com/Dicklesworthstone/frankensqlite/commit/dc4926c30), [8f3f4271d](https://github.com/Dicklesworthstone/frankensqlite/commit/8f3f4271d)).**
  `t.a = u.b` with an INTEGER `t.a` and a TEXT `u.b` holding `'1'` is true in SQLite, but the direct
  rowid/index lookup lanes probed with the raw value: an index probe of `'1'` missed the key `1` (a join of
  a TEXT code column to an `INTEGER UNIQUE` key returned no rows at all), and a rowid probe truncated `1.5`
  to rowid 1. Rows were dropped or added in row output, grouped joins and multi-join chains, and, through
  the 0.4.8 single-lookup aggregate lane (1aee0b458), in `count`/`sum`/`min`/`max` that 0.4.7 still
  answered correctly. Rowid probes now go through `MustBeInt`; an index lookup coerces a TEXT or typeless
  probe to the indexed column's numeric affinity (SQLite's `sqlite3IndexAffinityOk`) and keeps the seek; a
  numeric probe into a TEXT index uses the general join.
- `GROUP BY` over a join emits groups in key order, as SQLite does
  ([c1b7f7b51](https://github.com/Dicklesworthstone/frankensqlite/commit/c1b7f7b51)).
- Rows whose INTEGER PRIMARY KEY slot holds the rowid read correctly after `ALTER TABLE ADD COLUMN`
  ([a2398978a](https://github.com/Dicklesworthstone/frankensqlite/commit/a2398978a)). This repairs files
  written by 0.3.x. A row written by a pre-March-2026 build that omitted the slot and whose first stored
  value equals its rowid can now read shifted.
- An unqualified `USING` column resolves inside a correlated subquery (bd-7s1zz,
  [a915766b0](https://github.com/Dicklesworthstone/frankensqlite/commit/a915766b0)).
- An aggregate over a join naming an unknown column reports "no such column" (bd-24pa2,
  [5975efd76](https://github.com/Dicklesworthstone/frankensqlite/commit/5975efd76)).

### Performance

- Native release binaries are built at `opt-level = 3` instead of `"z"`; the wasm package keeps `"z"`
  ([86945ae73](https://github.com/Dicklesworthstone/frankensqlite/commit/86945ae73)).
- `count`/`sum`/`min`/`max` over a single-lookup join compile to VDBE instead of materializing and sorting
  every joined row (bd-41o3t, [1aee0b458](https://github.com/Dicklesworthstone/frankensqlite/commit/1aee0b458)).
- Implicit-join `WHERE` equalities and expression join keys use hash joins and seeks
  ([9a42732a4](https://github.com/Dicklesworthstone/frankensqlite/commit/9a42732a4)).
- `INSERT ... SELECT` from table-valued functions is linear again (GH#438,
  [e51cc5c7c](https://github.com/Dicklesworthstone/frankensqlite/commit/e51cc5c7c)).
- Correlated `EXISTS` scan probes reuse one substituted template per query (GH#419,
  [160d930e3](https://github.com/Dicklesworthstone/frankensqlite/commit/160d930e3)).
- A DML statement's savepoint snapshot no longer copies the whole schema (bd-ry6x7,
  [1f724e542](https://github.com/Dicklesworthstone/frankensqlite/commit/1f724e542)).
- Index scans stop paying quadratic cell-slot cache costs
  ([26d959970](https://github.com/Dicklesworthstone/frankensqlite/commit/26d959970)); join-expression
  bindings hash with foldhash ([939972af2](https://github.com/Dicklesworthstone/frankensqlite/commit/939972af2));
  `generate_series` checks for cancellation every 256 rows
  ([95eda7139](https://github.com/Dicklesworthstone/frankensqlite/commit/95eda7139)).

### TypeScript SDK

Durable workflows: atomic recoverable workflow submissions, verified parent outputs fed into handlers, and
bounded prerequisite results read in one lease-checked snapshot.

### Dependencies

Semver-compatible lockfile updates (cc, clap, syn, thiserror, zerocopy, rand, rustix, smallvec, xxhash-rust,
zlib-rs and others). The wasm-bindgen family stays at 0.2.128: wasm-bindgen-futures 0.4.79 would add
tokio to the lockfile.

### Release gate

Run through rch against exact `git archive` exports of the release tree (origin/main plus the version bump and
lockfile updates), with the toolchain pinned (`nightly-2026-08-31`).

- **Build and lint:** `cargo fmt --all --check`, `cargo check --workspace --all-targets --locked` and
  `cargo clippy --workspace --all-targets --locked -- -D warnings` pass.
- **Library tests:** fsqlite-pager 1007/0, fsqlite-btree 517/0 (`--test-threads=1`), fsqlite-wal 973/0,
  fsqlite-vdbe 1180/0, fsqlite-func 512/0, fsqlite-ext-misc 110/0, fsqlite-core 3968/0.
- **Integration tests (fsqlite-core), 224 passed / 0 failed across 19 targets:** the 0.4.7 set
  (`hfdt_dlkam3_multirow_insert_indexes`, `gh434_keyed_dml`, `gh435_multirow_replace_changes`,
  `gh436_streaming_function_calls`, `bd_4iaoi_serialized_ddl_misses_peer_rows`, `bd_cp3yd_jit_temp_scan`,
  `bd_orwh0_statement_shape_busy_timeout`, `gh410_lock_byte_freelist`), every test file added or changed since
  0.4.7, the new `join_lookup_affinity_oracle`, and `wal_reset_with_foreign_reader` (13/13), whose
  `cass_gh477_explicit_index_recovery_preserves_database_and_wal` guards the strict read-only fix (it took
  30.07 s on 0.4.7 against a 5 s bound).
- **Facade (`fsqlite` crate) test suite:** 1037 passed, 0 failed (113 test targets plus unit tests).
- The GH#434 ABORT keeper added after 0.4.7 expected an error variant `fsqlite-core` never returns; it now
  checks stock's message ([b897c8566](https://github.com/Dicklesworthstone/frankensqlite/commit/b897c8566)).
- The library, integration and facade suites ran on the release tree as it stood before 8f3f4271d's helper
  changed from `Option<Option<char>>` to an enum for clippy (a type-only change with the same behavior). The
  final tree then passed fmt, check, clippy, the fsqlite-vdbe library suite and the join, GH#434 and
  `wal_reset_with_foreign_reader` integration targets.
- **Not covered:** the full workspace test suite and all-features validation (bd-dybn7), as in 0.4.7.

### Known issues

- **Three-table join, typeless column against a TEXT key (bd-y5mc8):** `FROM u JOIN v ON v.k = u.d JOIN
  t ON t.id = v.m`, where `v.k` is TEXT UNIQUE and `u.d` is typeless, matches integer 2 to `'2'`. SQLite
  applies no affinity there. Present in 0.4.7; the two-table form is correct. Joins of three or more tables
  run through the hash join, whose key comparison applies TEXT affinity to the typeless side.
- **Numeric probe into a typeless-column index (bd-kr6hf):** `FROM parent JOIN child ON child.parent_id =
  parent.id` with an untyped, indexed `parent_id` keeps 0.4.7's index lookup, which misses a `parent_id`
  stored as numeric-looking TEXT (`'2'`); SQLite would match it. Values stored as numbers are found. The
  exact alternative is a nested loop until the join can be reordered to a rowid lookup as SQLite does.
- **#443:** a read-write COMMIT returns `BusyRecovery` and a retrying process stalls when a stock
  read-only connection left `-shm` beside a 0-byte `-wal` (since 0.4.6).
- **#442:** `VACUUM INTO` fails busy while another process has the WAL database open, even idle (since
  0.4.4; fails closed).
- Carried from 0.4.7: trigger WHEN clauses with a subquery are re-parsed per row; bd-dd24l (Windows
  `VACUUM INTO`), bd-sz9j5 (no cross-process protocol version guard: do not mix engine versions on one
  database), bd-1hi.11 (WAL-FEC recovery unfinished), and the long-open multi-writer RELEASE-P0 beads.

---

## [0.4.7] -- 2026-09-29 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.6...v0.4.7>

**What this is.** A prompt fix release, uniform across all 26 public crates. It carries a silent-corruption
fix that affects every consumer doing large multi-row `INSERT ... VALUES` (hfdt-dlkam3), the bd-4iaoi fix
that 0.4.6 listed as a known issue, and the fixes and performance work landed on `main` since v0.4.6.

### Data integrity

- **Large multi-row INSERT wrote index entries without their rowid (hfdt-dlkam3, bd-2eebi,
  [1f53ff965](https://github.com/Dicklesworthstone/frankensqlite/commit/1f53ff965)).**
  - **Symptom:** after one `INSERT ... VALUES (...), (...), ...` of enough rows, `PRAGMA integrity_check`
    reported `index key record missing trailing integer rowid`, and reads through an index failed with
    `database disk image is malformed`.
  - **Cause:** the VDBE silently dropped every register write above register 65,535. It has done so since
    February 2026, so 0.4.1, 0.4.4 and 0.4.6 are affected. A multi-row VALUES statement allocates fresh
    registers per row. Once `base + rows x R` passed 65,535, where R is the registers per row and grows
    with columns, CHECK constraints and indexes, later rows got index records without the rowid, and
    UNIQUE/PRIMARY KEY conflict detection was skipped for them. Table values, CHECK and foreign-key
    enforcement were not affected. A table with a BEFORE INSERT trigger takes a per-row path and was not
    affected.
  - **Example:** an 11-column table with 8 CHECKs, 4 foreign keys and 7 indexes (R = 46) was whole at
    1,424 rows and corrupt at 1,425.
  - **Fix:** every register a compiled program declares is writable. Only undeclared registers past
    65,535 are still refused.
  - **Repairing a database written by an affected version:** check each UNIQUE and PRIMARY KEY column
    set for duplicates (`SELECT ... FROM t NOT INDEXED GROUP BY <key> HAVING count(*) > 1`), resolve any,
    then `REINDEX`. Stock `integrity_check` names the affected indexes.
- **A serialized schema change raced concurrent writers into a corrupt index (bd-4iaoi,
  [4b1593ea2](https://github.com/Dicklesworthstone/frankensqlite/commit/4b1593ea2)).** This was the
  0.4.6 known issue. `BEGIN IMMEDIATE; CREATE INDEX ...; COMMIT` beside a connection that autocommits
  INSERTs in concurrent mode could leave that connection's rows out of the new index. A serialized write
  commit now holds the concurrent-commit registry guard through its generation bump and schema-cookie
  publish. A serialized schema change is refused (`BusySnapshot`, retried) if a peer committed since it
  began.
- **Composite-key UPDATE/DELETE with an index that repeats a column (GH#434 follow-up).** A WHERE term
  beyond the key (for example `gate = ?` with an index on `(a, a)`) is enforced as a residual. It could
  previously be dropped, so the statement touched rows it should have skipped. This was found by the
  GH#434 differential matrix before any release shipped the seek.

### Concurrency and availability

- **Autocommit DDL no longer starves against a busy writer (bd-f5sh5,
  [89e7a08dc](https://github.com/Dicklesworthstone/frankensqlite/commit/89e7a08dc)).** After three
  consecutive lost attempts, the next autocommit schema change runs `IMMEDIATE`. DML stays concurrent. In
  a 72-worker stress run the DDL starved in 79% of runs before this change and 0% after.
- **A schema-only open retries its whole bootstrap on Busy/BusyRecovery**, as an ordinary open does
  ([0853f5e8e](https://github.com/Dicklesworthstone/frankensqlite/commit/0853f5e8e)).

### SQL correctness

- Multi-row INSERT of 64+ rows reports every row in `changes()` (GH#435,
  [1beb3cc8a](https://github.com/Dicklesworthstone/frankensqlite/commit/1beb3cc8a)).
- JIT templates leave TEMP-table cursors to the interpreter (bd-cp3yd,
  [b17451708](https://github.com/Dicklesworthstone/frankensqlite/commit/b17451708)).
- `round()` follows SQLite 3.53: the N = 0 path adds ±0.5 and truncates, and N is read as int64 before
  clamping (GH#436, [68a42ed40](https://github.com/Dicklesworthstone/frankensqlite/commit/68a42ed40)).
- `SELECT *` leaves out generate_series's HIDDEN columns (bd-cpa8b,
  [cfcf0deeb](https://github.com/Dicklesworthstone/frankensqlite/commit/cfcf0deeb)).
- `HAVING` on a non-aggregate query is rejected at every nesting level (GH#427,
  [777394ea1](https://github.com/Dicklesworthstone/frankensqlite/commit/777394ea1)).
- The deferred-FTS5 repair open binds a missing FTS5 shadow autoindex (cass GH#503,
  [cbd0b98dc](https://github.com/Dicklesworthstone/frankensqlite/commit/cbd0b98dc)).

### Performance and memory

- Keyed UPDATE/DELETE on a composite key seeks the index instead of scanning: 22.7 ms to 0.20 ms per
  statement at 20,000 rows (GH#434, [c049374eb](https://github.com/Dicklesworthstone/frankensqlite/commit/c049374eb)).
- A lone table-valued function streams into simple aggregates, including expression arguments, instead of
  materializing every row (GH#420, [d8fbd0100](https://github.com/Dicklesworthstone/frankensqlite/commit/d8fbd0100),
  [d1fefde18](https://github.com/Dicklesworthstone/frankensqlite/commit/d1fefde18)). `sum(value)` over a
  million-row `generate_series` drops from ~680 MB to ~13 MB peak.
- Streaming-aggregate function calls are resolved once per query: `GROUP BY round(log10(value))` over 1M
  rows went from 2.85 s to 1.00 s of CPU (GH#436, [68a42ed40](https://github.com/Dicklesworthstone/frankensqlite/commit/68a42ed40)).
- A correlated EXISTS or scalar subquery on a non-rowid key seeks an index instead of running the per-row
  fallback (GH#432, [f8fb308f6](https://github.com/Dicklesworthstone/frankensqlite/commit/f8fb308f6),
  [a215656ea](https://github.com/Dicklesworthstone/frankensqlite/commit/a215656ea)).
- A `sqlite_master` query on a schema-only connection no longer reads the whole file
  ([ec4956420](https://github.com/Dicklesworthstone/frankensqlite/commit/ec4956420)).
- The published-page overflow plane is bounded, so resident memory stops tracking bytes read
  ([ad3f23ca9](https://github.com/Dicklesworthstone/frankensqlite/commit/ad3f23ca9)).

### TypeScript SDK

Durable workflow graphs and job continuations publish atomically, with dependency and lease fencing
enforced in SQL. Replica installation defers foreign keys and replicates generated-column schemas. Rebase
history retires without repeating published work.

### Release gate

Run on trj against an exact `git archive` export of the release tree (ee86939cd plus the version bump), with
the toolchain pinned (`nightly-2026-08-31`) and a fresh target directory.

- **Build and lint:** `cargo check --workspace --all-targets --locked` and
  `cargo clippy --workspace --all-targets --locked -- -D warnings` passed. `cargo fmt --all --check` flagged one
  test in `fsqlite-func`, fixed in c692a80e9 (whitespace only), and passes on the tagged tree.
- **Library tests:** fsqlite-pager 1006/0, fsqlite-btree 517/0, fsqlite-wal 973/0, fsqlite-vdbe 1180/0,
  fsqlite-func 512/0, fsqlite-core 3966/0.
- **Integration tests:** `hfdt_dlkam3_multirow_insert_indexes`, `gh434_keyed_dml`,
  `gh435_multirow_replace_changes`, `gh436_streaming_function_calls`, `bd_4iaoi_serialized_ddl_misses_peer_rows`,
  `bd_cp3yd_jit_temp_scan`, `bd_orwh0_statement_shape_busy_timeout` and `gh410_lock_byte_freelist`: all pass.
- **Facade (`fsqlite` crate) test suite:** 1036 passed, 0 failed.

### Known issues

- **bd-sx5w2 (P0, rare, under investigation):** an autocommit `CREATE`/`DROP INDEX` that completes
  concurrently with another connection's INSERTs and `PRAGMA wal_checkpoint(TRUNCATE)` can, under heavy
  host load, leave one page granted to two b-trees (a duplicate index entry or an orphaned page). It was
  seen in about 1 of 150-500 stress runs at load ~200 and has not reproduced at normal load. The code path
  predates 0.4.6.
- **Trigger WHEN clauses containing a subquery** are re-parsed per row inside BEFORE-trigger firing, which
  can look like a hang on large batches. Workaround: move the subquery into the trigger body
  (`BEGIN SELECT RAISE(ABORT, ...) WHERE (subquery); END`).
- **bd-q1b7z:** under concurrent writers with TRUNCATE checkpoints, a connection's in-memory schema can
  briefly lose an index it just created (a following `DROP INDEX` reports `no such index`). The index is
  on disk.
- **Test-suite reds:** three GH#399/GH#294 checkpoint keeper tests in
  `bulk_import_upsert_text_pk_phantom_pk.rs` (bd-n2a7i, pre-existing) and, on macOS only,
  `test_retained_autocommit_flush_propagates_nontransient_post_commit_checkpoint_failure` (bd-wpqyn; passes
  on Linux).
- Carried from 0.4.6: bd-dd24l (Windows `VACUUM INTO`), bd-sz9j5 (no cross-process protocol version
  guard), bd-ih8ak/bd-jyeus (per-statement refresh cost; Apple-silicon throughput gap), bd-1hi.11
  (WAL-FEC recovery unfinished), and the long-open multi-writer RELEASE-P0 beads.

---

## [0.4.6] -- 2026-09-27 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.4.4...v0.4.6>

**What this is.** A uniform-version release: all 26 public workspace crates publish at 0.4.6 together (the two private members, `fsqlite-e2e` and `fsqlite-harness`, are `publish = false`), and the
tag, the crates.io set and this section describe the same commit. It covers the whole `v0.4.4..v0.4.6`
range: 370 non-merge commits, 2026-09-17 through 2026-09-26.

**Why not 0.4.5.** `main` was bumped to a uniform 0.4.5 but never published at that version. Earlier the
same day, `fsqlite-pager` 0.4.5 had gone out as a crates.io-only backport of bd-b5vmw onto v0.4.4 (see the
0.4.5 entry), and crates.io versions are immutable. 0.4.6 therefore supersedes both.

### Page accounting and on-disk integrity

- **Page referenced twice across a WAL generation (bd-b5vmw).** A live B-tree page could be freed and
  granted again, so `integrity_check` reported `page N is referenced multiple times` (freelist trunk plus
  index, or two index paths). This is the same fix as the 0.4.5 pager backport
  ([a712b8ea4](https://github.com/Dicklesworthstone/frankensqlite/commit/a712b8ea4)).
- **Freelist sizing.** Freelist trunks are sized by usable size rather than page size, which matters for
  databases with reserved bytes. A rebuilt image's freelist is sized by the header it publishes
  ([d13743008](https://github.com/Dicklesworthstone/frankensqlite/commit/d13743008),
  [e1306af2f](https://github.com/Dicklesworthstone/frankensqlite/commit/e1306af2f)).
- **B-tree readers reject corrupt structure instead of propagating it.**
  - Corrupt freelist references and duplicate free pages are refused
    ([0645b9c3b](https://github.com/Dicklesworthstone/frankensqlite/commit/0645b9c3b)).
  - Invalid overflow links and corrupt overflow chains are rejected before a row payload is reconstructed
    ([6a3cfda20](https://github.com/Dicklesworthstone/frankensqlite/commit/6a3cfda20),
    [9be5de33b](https://github.com/Dicklesworthstone/frankensqlite/commit/9be5de33b)).
  - Failed overflow reservations are compensated, and allocation identities are validated
    ([a44068249](https://github.com/Dicklesworthstone/frankensqlite/commit/a44068249)).
  - Reserved bytes are excluded from cell headers and copy spans
    ([a2997f963](https://github.com/Dicklesworthstone/frankensqlite/commit/a2997f963)).
- **Collated indexes.** Collated-index corruption and lossy key comparisons are fixed; UTF-16 read/order
  parity is restored; and collated UNIQUE probes seek instead of scanning
  ([8086a0cbe](https://github.com/Dicklesworthstone/frankensqlite/commit/8086a0cbe)).
- **`integrity_check` reporting.** It reports stored NOT NULL and CHECK violations the way stock SQLite
  does, and its vectorized scan decodes UTF-16 databases
  ([76b38991a](https://github.com/Dicklesworthstone/frankensqlite/commit/76b38991a)).

### WAL, recovery and repair

- **WAL-index and companion lifecycle.**
  - A stale WAL-index is healed on journal-mode re-entry, and a commit is never acknowledged into an
    unlinked WAL ([363b4dd4d](https://github.com/Dicklesworthstone/frankensqlite/commit/363b4dd4d)).
  - A missing WAL companion is treated as an ordinary state rather than a dead end, bd-7zs8a
    ([2fc898296](https://github.com/Dicklesworthstone/frankensqlite/commit/2fc898296)).
  - The native path no longer creates a companion file
    ([e0c23cc78](https://github.com/Dicklesworthstone/frankensqlite/commit/e0c23cc78)).
- **Fail-closed recovery.**
  - Native recovery and compaction fail closed
    ([49890c89c](https://github.com/Dicklesworthstone/frankensqlite/commit/49890c89c)).
  - Stale WAL pairings are rejected for rollback-mode recovery sources
    ([6b6acfff4](https://github.com/Dicklesworthstone/frankensqlite/commit/6b6acfff4)).
  - Page bodies are verified before checkpoint checksum evidence is accepted
    ([15fd3c042](https://github.com/Dicklesworthstone/frankensqlite/commit/15fd3c042)).
  - Recovery exports can no longer poison the source database's companions
    ([e6af81d72](https://github.com/Dicklesworthstone/frankensqlite/commit/e6af81d72)).
- **Readers and waiters.** Sidecar readers wait out an in-flight mutation, and fence waiters back off
  starting at 1 ms ([e2eb7afdc](https://github.com/Dicklesworthstone/frankensqlite/commit/e2eb7afdc)).
  Passive checkpoints report their progress and post-truncate counts accurately
  ([0449e2487](https://github.com/Dicklesworthstone/frankensqlite/commit/0449e2487)).
- **WAL-FEC recovery building blocks (bd-1hi.11).**
  - Verified WAL images are reconstructed from FEC repair groups.
  - Complete recovered snapshots are materialized.
  - A guarded, non-destructive recovery export command is added.
  - Recovery is anchored to surviving checksums and identity-bound commit certificates.
  - A source WAL can be repaired with a verified backup and a native index rebuild.
  - Commits: [8e28f4844](https://github.com/Dicklesworthstone/frankensqlite/commit/8e28f4844),
    [4b00a78bc](https://github.com/Dicklesworthstone/frankensqlite/commit/4b00a78bc),
    [6e40b6063](https://github.com/Dicklesworthstone/frankensqlite/commit/6e40b6063),
    [7226e6db5](https://github.com/Dicklesworthstone/frankensqlite/commit/7226e6db5),
    [5131151e5](https://github.com/Dicklesworthstone/frankensqlite/commit/5131151e5).
  - Torn terminal payloads are recovered, and native repairs are settled with exact-length rollback
    ([750e8a259](https://github.com/Dicklesworthstone/frankensqlite/commit/750e8a259),
    [255ec1366](https://github.com/Dicklesworthstone/frankensqlite/commit/255ec1366)).
  - **Scope:** the complete WAL-FEC recovery algorithm (bd-1hi.11) remains open. The compatibility
    runtime's ordinary commit path still does not write repair symbols.
- **Stale reader marks.** Stale WAL reader marks are recovered or fail fast, and stock SQLite's
  unindexed empty WAL index is accepted (GH#430, GH#431; [e85717e0a](https://github.com/Dicklesworthstone/frankensqlite/commit/e85717e0a)).

### Concurrency, busy handling and MVCC

- **`busy_timeout` semantics.**
  - Each statement now gets one `busy_timeout` budget (GH#423), shared across its retries through
    poll-scoped machinery ([0280838fb](https://github.com/Dicklesworthstone/frankensqlite/commit/0280838fb),
    [e550fe45c](https://github.com/Dicklesworthstone/frankensqlite/commit/e550fe45c)).
  - DDL, SAVEPOINT and ANALYZE honor `busy_timeout` (bd-orwh0), and so does BEGIN under checkpoint
    contention (bd-udetu). Commits:
    [4c018b93b](https://github.com/Dicklesworthstone/frankensqlite/commit/4c018b93b),
    [ea6430afe](https://github.com/Dicklesworthstone/frankensqlite/commit/ea6430afe),
    [2cce5f3d8](https://github.com/Dicklesworthstone/frankensqlite/commit/2cce5f3d8).
  - VACUUM is no longer armed for retry
    ([61a81b141](https://github.com/Dicklesworthstone/frankensqlite/commit/61a81b141)).
- **Opt-in whole-transaction retry.** MVCC conflicts can now trigger a whole-transaction retry, but only
  when the caller opts in ([232554eaa](https://github.com/Dicklesworthstone/frankensqlite/commit/232554eaa)).
- **MVCC correctness.**
  - Rowid exhaustion is preserved across durable refresh and savepoints
    ([0b8318e1d](https://github.com/Dicklesworthstone/frankensqlite/commit/0b8318e1d)).
  - Retire-batch deadlocks and repeated epoch scans are avoided
    ([a6174d840](https://github.com/Dicklesworthstone/frankensqlite/commit/a6174d840)).
  - Timestamp caches are isolated across allocator lifetimes, and ID wraparound is rejected
    ([88d58831e](https://github.com/Dicklesworthstone/frankensqlite/commit/88d58831e)).
  - Rebase preserves SQLite predicate and numeric semantics
    ([d6ee573d3](https://github.com/Dicklesworthstone/frankensqlite/commit/d6ee573d3)).
  - Intent proofs bind exact raw TEXT bytes
    ([830cb705a](https://github.com/Dicklesworthstone/frankensqlite/commit/830cb705a)).
- **Ownership under uncertainty.** When process liveness is inconclusive, snapshot-publisher and rebuild
  ownership are retained. Hidden Linux processes are distinguished from dead owners. File and SHM ownership
  survive a failed admission. Commits:
  [4b8a956af](https://github.com/Dicklesworthstone/frankensqlite/commit/4b8a956af),
  [91f5aacb7](https://github.com/Dicklesworthstone/frankensqlite/commit/91f5aacb7),
  [c7cda06ee](https://github.com/Dicklesworthstone/frankensqlite/commit/c7cda06ee),
  [430ccd3ba](https://github.com/Dicklesworthstone/frankensqlite/commit/430ccd3ba).
- **Write coordinator shutdown.** The coordinator stops cooperatively before the last close, and closing
  regions drain cooperatively ([6cf1407d6](https://github.com/Dicklesworthstone/frankensqlite/commit/6cf1407d6),
  [2bba1e7c6](https://github.com/Dicklesworthstone/frankensqlite/commit/2bba1e7c6)).
- **Concurrent schema changes.** A concurrent schema change aborts after any peer write commit since its
  BEGIN, whatever that commit's sequence number (bd-11sz4; [7c81a5cdd](https://github.com/Dicklesworthstone/frankensqlite/commit/7c81a5cdd); see the release gate).
- **Read-only concurrent COMMIT.** A file-backed read-only concurrent COMMIT no longer adopts the global
  commit clock (GH#429; [fd8c16a94](https://github.com/Dicklesworthstone/frankensqlite/commit/fd8c16a94)).
- **`integrity_check` under contention.** A transient conflict is reported as `SQLITE_BUSY`, not as a
  verdict row (bd-svwm7; [eef5999da](https://github.com/Dicklesworthstone/frankensqlite/commit/eef5999da)).

### SQL and function parity with stock SQLite

- **Query semantics.**
  - EXISTS over an aggregate, and ORDER BY on a selected literal
    ([3aff98db3](https://github.com/Dicklesworthstone/frankensqlite/commit/3aff98db3)).
  - INSERT ... SELECT ... UPSERT keeps its parameter bindings
    ([746983217](https://github.com/Dicklesworthstone/frankensqlite/commit/746983217)).
  - Correlated EXISTS seeks treat an explicit `main.table` as MAIN
    ([83d65ab7e](https://github.com/Dicklesworthstone/frankensqlite/commit/83d65ab7e)).
- **Numeric semantics.**
  - Exact integer conversion and SQLite's numeric boundaries are preserved
    ([4d16c4ff1](https://github.com/Dicklesworthstone/frankensqlite/commit/4d16c4ff1)).
  - Math coercion and infinities match SQLite
    ([f176f7135](https://github.com/Dicklesworthstone/frankensqlite/commit/f176f7135)).
  - `ceil`/`floor`/`trunc` keep exact integer text
    ([448d37cbb](https://github.com/Dicklesworthstone/frankensqlite/commit/448d37cbb)).
  - Numeric aggregates are exact, and IEEE special values are preserved
    ([c407ea7d4](https://github.com/Dicklesworthstone/frankensqlite/commit/c407ea7d4)).
  - Percentile validation and ranking match SQLite
    ([78fe1b1c0](https://github.com/Dicklesworthstone/frankensqlite/commit/78fe1b1c0)).
  - Decimal accepts scientific input and converts exactly to binary64
    ([ce6936ce9](https://github.com/Dicklesworthstone/frankensqlite/commit/ce6936ce9)).
- **Text and collation.**
  - NOCASE comparison, equality keys and ordering (including embedded NULs) match SQLite
    ([ac4cbf1ad](https://github.com/Dicklesworthstone/frankensqlite/commit/ac4cbf1ad),
    [6ede4b51c](https://github.com/Dicklesworthstone/frankensqlite/commit/6ede4b51c)).
  - JSON5 infinities survive typed reads and JSONB
    ([60140ce39](https://github.com/Dicklesworthstone/frankensqlite/commit/60140ce39)).
  - The engine and system tables use the database's text encoding
    ([8926f4b2f](https://github.com/Dicklesworthstone/frankensqlite/commit/8926f4b2f),
    [54206492c](https://github.com/Dicklesworthstone/frankensqlite/commit/54206492c)).
- **Schema, TEMP and CTEs.**
  - WITHOUT ROWID index keys omit overlapping PK columns
    ([0a75c20dc](https://github.com/Dicklesworthstone/frankensqlite/commit/0a75c20dc)).
  - TEMP virtual-column UNIQUE and stock rowids are handled, CTE-only WITH skips whole-image hydration, and
    retried DDL keeps its verbatim text
    ([971d7115c](https://github.com/Dicklesworthstone/frankensqlite/commit/971d7115c)).
  - The arbitrary recursive-CTE limit is removed
    ([7fea195d2](https://github.com/Dicklesworthstone/frankensqlite/commit/7fea195d2)).
  - Named `generate_series` instances declare their columns, and bounded/ordered series scans are planned
    ([5c232a4f7](https://github.com/Dicklesworthstone/frankensqlite/commit/5c232a4f7),
    [38a448f13](https://github.com/Dicklesworthstone/frankensqlite/commit/38a448f13)).
- **In-memory TEMP lane (MemDatabase).** Failed statements are atomic, the statement boundary is
  nesting-aware, and explicit ROLLBACK is restored (bd-5bq6u, bd-ndm28). Commits:
  [e1d15a527](https://github.com/Dicklesworthstone/frankensqlite/commit/e1d15a527),
  [9e56b36e9](https://github.com/Dicklesworthstone/frankensqlite/commit/9e56b36e9),
  [e6f106d2e](https://github.com/Dicklesworthstone/frankensqlite/commit/e6f106d2e).
- **Planner.** Equality indexes are chosen from `sqlite_stat1` rows-per-key
  ([8258b1a8d](https://github.com/Dicklesworthstone/frankensqlite/commit/8258b1a8d)).
- **WITHOUT ROWID probes.** Complete UNIQUE keys on WITHOUT ROWID tables are point-probed
  ([17a2f522f](https://github.com/Dicklesworthstone/frankensqlite/commit/17a2f522f)).
- **Opening databases.** SQLite URI open modes and filenames are honored
  ([b009f0267](https://github.com/Dicklesworthstone/frankensqlite/commit/b009f0267)).
- **EXISTS.** EXISTS result-list semantics and correlated comparison operand rules match stock SQLite
  ([375e2c060](https://github.com/Dicklesworthstone/frankensqlite/commit/375e2c060)).

### Performance

No throughput numbers are claimed in this entry. Performance claims stay tracked on bd-uh1fv and bd-jyeus.

- **Committed state is re-derived less often.**
  - Committed state is no longer re-derived per statement, and the WAL is not re-proved inside one held
    append gate. These address bd-ih8ak and remain partial
    ([c7402ebf8](https://github.com/Dicklesworthstone/frankensqlite/commit/c7402ebf8),
    [5c3f0e9a3](https://github.com/Dicklesworthstone/frankensqlite/commit/5c3f0e9a3)).
  - Begin-path proofs are reused while the WAL header is unchanged, and the refresh signature is carried
    forward ([81484c140](https://github.com/Dicklesworthstone/frankensqlite/commit/81484c140),
    [3f8a9dc5f](https://github.com/Dicklesworthstone/frankensqlite/commit/3f8a9dc5f),
    [cab30cbef](https://github.com/Dicklesworthstone/frankensqlite/commit/cab30cbef)).
- **Less work per statement.**
  - Storage-free SELECTs run without an autocommit transaction
    ([bedbb0ee7](https://github.com/Dicklesworthstone/frankensqlite/commit/bedbb0ee7)).
  - Page I/O is inline, and the WAL path is proved by stat
    ([643163ffe](https://github.com/Dicklesworthstone/frankensqlite/commit/643163ffe)).
  - Small correlated EXISTS probes are scanned rather than compiled per row
    ([1f09b51fd](https://github.com/Dicklesworthstone/frankensqlite/commit/1f09b51fd)).
  - Primary join rows are moved rather than copied
    ([1e6dd92a5](https://github.com/Dicklesworthstone/frankensqlite/commit/1e6dd92a5)).
- **WAL and B-tree internals.** Checkpoint frame headers are read in 64 KiB runs; the WAL-index hash scan is
  skipped when the page tail is clean; and B-tree overflow tracking is inline. Commits:
  [ba73ab0c9](https://github.com/Dicklesworthstone/frankensqlite/commit/ba73ab0c9),
  [bfeb7a028](https://github.com/Dicklesworthstone/frankensqlite/commit/bfeb7a028),
  [accb746f9](https://github.com/Dicklesworthstone/frankensqlite/commit/accb746f9).
- **hfdt-driven work (hfdt-gbou9l).** Seeks of composite literal existence prefixes, direct scans of proven
  WITHOUT ROWID prefixes, cached integer JSON-key anti-joins, and text-parent seeks during correlated
  foreign-key checks. Commits:
  [8a7af2ca8](https://github.com/Dicklesworthstone/frankensqlite/commit/8a7af2ca8),
  [873bfeb23](https://github.com/Dicklesworthstone/frankensqlite/commit/873bfeb23),
  [39e48fc43](https://github.com/Dicklesworthstone/frankensqlite/commit/39e48fc43),
  [b6b392e73](https://github.com/Dicklesworthstone/frankensqlite/commit/b6b392e73).

### Sessions, changesets, replication and snapshots

- **Session changesets** (Rust `fsqlite-ext-session`, plus `(sdk)` items in the TypeScript SDK).
  - SDK: interoperable SQLite session changeset encoding and inversion
    ([2f14c089a](https://github.com/Dicklesworthstone/frankensqlite/commit/2f14c089a)).
  - Changesets and patchsets are applied atomically, with row-local conflict rollback
    ([011ac4f76](https://github.com/Dicklesworthstone/frankensqlite/commit/011ac4f76),
    [72fc7e0f8](https://github.com/Dicklesworthstone/frankensqlite/commit/72fc7e0f8),
    [b65e9c950](https://github.com/Dicklesworthstone/frankensqlite/commit/b65e9c950)).
  - Bounded changeset streams are applied in one SQL transaction
    ([8653108ff](https://github.com/Dicklesworthstone/frankensqlite/commit/8653108ff)).
  - Live native SQL transactions are captured into bounded changesets
    ([4bb1e8bac](https://github.com/Dicklesworthstone/frankensqlite/commit/4bb1e8bac)).
- **Replication.**
  - Source changes and ordered outbox records commit atomically
    ([0c65c66ce](https://github.com/Dicklesworthstone/frankensqlite/commit/0c65c66ce)).
  - Ordered, verified changesets are applied with atomic SQL cursors
    ([26634e799](https://github.com/Dicklesworthstone/frankensqlite/commit/26634e799)).
  - Fanout progress persists for every required replica
    ([6b4433a11](https://github.com/Dicklesworthstone/frankensqlite/commit/6b4433a11)).
  - Bounded native SQLite changeset rebasing
    ([49bff425e](https://github.com/Dicklesworthstone/frankensqlite/commit/49bff425e)).
- **Snapshot transfer.**
  - Authenticated snapshot transfers are bound to canonical manifests
    ([19ca2d1cc](https://github.com/Dicklesworthstone/frankensqlite/commit/19ca2d1cc)).
  - Frozen database images stream with bounded block retention
    ([552f79f7a](https://github.com/Dicklesworthstone/frankensqlite/commit/552f79f7a)).
  - A snapshot can bootstrap a writable SQL replica, including resuming a partial journaled bootstrap
    ([731ce3293](https://github.com/Dicklesworthstone/frankensqlite/commit/731ce3293),
    [50eba2a96](https://github.com/Dicklesworthstone/frankensqlite/commit/50eba2a96)).
- **TypeScript SDK / worker.**
  - Durable SQL job queues with fenced leases
    ([e8adb21c6](https://github.com/Dicklesworthstone/frankensqlite/commit/e8adb21c6)).
  - OPFS checkpoints with conflict-safe storage and recovery
    ([4033bf9fd](https://github.com/Dicklesworthstone/frankensqlite/commit/4033bf9fd),
    [12b17825d](https://github.com/Dicklesworthstone/frankensqlite/commit/12b17825d)).
  - Whole-transaction recovery with bounded conflict retries
    ([e6916f33b](https://github.com/Dicklesworthstone/frankensqlite/commit/e6916f33b)).
  - Verified schema migration plans applied atomically
    ([fc3359978](https://github.com/Dicklesworthstone/frankensqlite/commit/fc3359978)).
  - Bounded prepared-statement retention
    ([f9b1696a1](https://github.com/Dicklesworthstone/frankensqlite/commit/f9b1696a1)).
  - Snapshot session ownership leases
    ([1e7c98007](https://github.com/Dicklesworthstone/frankensqlite/commit/1e7c98007)).
  - Indexed, bounded table-scan paging
    ([e2bc210dc](https://github.com/Dicklesworthstone/frankensqlite/commit/e2bc210dc)).
  - CTE live queries ([28631e9b1](https://github.com/Dicklesworthstone/frankensqlite/commit/28631e9b1)).
  - Bounded HTTP changeset transport with replay-safe receipts
    ([84f011c65](https://github.com/Dicklesworthstone/frankensqlite/commit/84f011c65)).
- **Resumable HTTP bootstraps.** Atomic bootstraps resume through confirmed source acknowledgements over
  bounded, authorized HTTP staging, and installed evidence is verified before replay acknowledgements
  ([713a75edf](https://github.com/Dicklesworthstone/frankensqlite/commit/713a75edf), [9fe4b460a](https://github.com/Dicklesworthstone/frankensqlite/commit/9fe4b460a), [7619e4d36](https://github.com/Dicklesworthstone/frankensqlite/commit/7619e4d36), [4758cb871](https://github.com/Dicklesworthstone/frankensqlite/commit/4758cb871)).
- **Trigger-driven capture.** Trigger-driven transactions are captured through bounded before/after
  snapshots and retained atomically in the outbox ([64c9ca500](https://github.com/Dicklesworthstone/frankensqlite/commit/64c9ca500), [ab1a8890a](https://github.com/Dicklesworthstone/frankensqlite/commit/ab1a8890a)).
- **Receipts and UTF-16.** Ordered receipt history retires without replaying expired deliveries, and
  ordered replication supports UTF-16 with bounded fanout identities
  ([b137b4dd9](https://github.com/Dicklesworthstone/frankensqlite/commit/b137b4dd9), [a266a2632](https://github.com/Dicklesworthstone/frankensqlite/commit/a266a2632)).

### CLI

- **`.timer on|off`** reports statement resource usage via `getrusage`
  ([117bd4e3c](https://github.com/Dicklesworthstone/frankensqlite/commit/117bd4e3c)).
- **Page I/O on the shell's thread.** The shell runs page I/O inline on its dedicated thread
  ([9c2b10ade](https://github.com/Dicklesworthstone/frankensqlite/commit/9c2b10ade)).

### Release gate

Run on trj against an exact `git archive` export of the release tree (df5e599de; the tagged commit adds only
this CHANGELOG), never a working tree.

- **Build and lint:** `cargo fmt --all --check`, `cargo check --workspace --all-targets --locked`
  and `cargo clippy --workspace --all-targets --locked -- -D warnings`.
- **Tests:**
  - library tests for `fsqlite-pager`, `fsqlite-btree`, `fsqlite-wal` and `fsqlite-core`;
  - the `bd_orwh0` busy-timeout suite, `gh410_lock_byte_freelist` and `concurrent_freelist_churn`;
  - the savepoint, q37ep index-churn, WAL-recovery, crash-matrix and isolation e2e suites.
- **Keeper loop:** 320 runs of the bd-b5vmw keeper
  (`retried_create_index_persists_verbatim_sql_and_leaves_no_residue`), 8 workers under load, with output
  captured: 0 failures and 0 corruptions, with the index surviving in 145 runs.

**Caught by the gate and fixed before release (bd-11sz4).**
- **Symptom:** the first 0.4.6 candidate (95b42608b) failed 3 of 320 keeper runs with
  ``table `t` rowid N is missing from index `probe_verbatim` ``, which is lost index entries.
- **Cause:** a concurrent schema change counted as stale only if the global commit sequence had moved past
  its BEGIN. Commit sequence numbers come from each pager's view of WAL state and can jump across TRUNCATE
  checkpoints, so a peer's INSERT could commit at or below the DDL's begin sequence. The CREATE INDEX then
  built from a snapshot that missed those rows.
- **Fix:** a dense in-process write generation, bumped once each concurrent commit is visible. A schema
  change is stale if the generation moved since its BEGIN ([7c81a5cdd](https://github.com/Dicklesworthstone/frankensqlite/commit/7c81a5cdd)).
- **Verification:** a sensitized stress harness (quiescent integrity verdict, a paired control, about 36
  concurrent processes) found persistent corruption in 5 of 192 control runs and 0 of 192 with the fix.
- **Scope:** first caught on the 0.4.6 candidate. Cross-process peers still go through the sequence check.

### Known issues carried into this release

- **bd-dd24l (P1, GH#376):** Windows-only `VACUUM INTO` regression; no output file is produced.
- **bd-sz9j5 (P1):** there is no cross-process protocol version guard. An older engine (0.3.9, pre-GH#399/
  GH#411) can share a database with a 0.4.x engine undetected, and the layout/protocol version constants have
  not changed since v0.3.9. Do not mix engine versions on one database.
- **bd-ih8ak (P1, partially addressed above) and bd-jyeus (P1, macOS):** per-statement committed-state
  refresh cost, and a CI-gated throughput gap to C SQLite on Apple silicon.
- **bd-1hi.11 (P0):** the complete WAL-FEC recovery algorithm is not finished (see above).
- **Long-open multi-writer RELEASE-P0 beads** (filed 2026-08-04..07; v0.3.10 through v0.4.4 all shipped with
  them open) remain open and are not claimed fixed here, notably
  `bd-multipage-concurrent-update-missing-row-1fc2c` (a multi-page concurrent UPDATE workload lost a credit
  under 8 explicit-BEGIN writers) and `bd-gh302-continuous-overlap-freelist-reuse-i5tx4`.
- **Load-sensitive tests** (test-only; fixes in flight on main): `fsqlite-wal`
  `test_lane_stager_reuses_lanes_after_worker_churn` and `fsqlite-pager`
  `test_published_snapshot_retries_during_inflight_publication` can fail under a heavily loaded parallel run
  and pass in isolation. Pre-existing reds: `crates/fsqlite/tests/generate_series_constraints.rs`
  (bd-cpa8b, fixed on main after this release) and the `crates/fsqlite/tests/update_rowid_in_oracle.rs`
  plan-shape assertion (bd-4x6tm).
- **bd-4iaoi (P0, also present in v0.4.4 and earlier; fixed in 0.4.7):** a schema change inside an
  explicit `BEGIN IMMEDIATE` or `BEGIN EXCLUSIVE` transaction (for example `CREATE INDEX`, or `DROP` followed
  by `CREATE`) can leave rows missing from the index, persistently. The missing rows belong to another
  connection in the same process that autocommits writes in the default concurrent mode while the schema
  change commits. Cross-process peers have not yet been characterized. Deferred `BEGIN`, which is promoted to
  CONCURRENT by default, and autocommit DDL are not affected. Until 0.4.7, run explicit-transaction DDL only
  while no concurrent writers are active, or use autocommit DDL.

---

## [0.4.5] -- 2026-09-25 (crates.io only: `fsqlite-pager`)

This is a pager-only patch, published from tag
[`fsqlite-pager-v0.4.5`](https://github.com/Dicklesworthstone/frankensqlite/tree/fsqlite-pager-v0.4.5)
(commit `6843bf75e`). It is the v0.4.4 release tree plus one fix, and it follows the 0.4.3 precedent.

- **Other crates.** Every other crate stays at 0.4.4 and requires `fsqlite-pager` by caret, so consumers pick
  the fix up with `cargo update -p fsqlite-pager --precise 0.4.5`.
- **Not shipped here.** There is no GitHub Release and no binaries. `main`'s later pager/WAL/core/VDBE
  performance commits are not included.
- **Next release.** `main`'s pager manifest still reads 0.4.4, so the next uniform release must use 0.4.6 or
  later.

### Fixed

- **A live B-tree page could be freed and granted again across a WAL generation.** `integrity_check` then
  reported `page N is referenced multiple times`, either as freelist trunk plus index or as two index paths
  (bd-b5vmw; backport of [`a712b8ea4`](https://github.com/Dicklesworthstone/frankensqlite/commit/a712b8ea4d2f0e9ad203985e9817f598a24b91bd)).
  There were two paths, and both are now closed:
  - a refreshing connection classified its abandonment pool against a stale pre-generation `db_size`;
  - a connection dropped without refreshing handed its pool to the shared ledger from the wrong generation.
- **Downstream report.** The same signature corrupted a shared hfdt store in the field
  (hfdt-field-reports-20260925-otkbvu.1).

### Verification

- **Package tests.** `cargo test --locked -p fsqlite-pager --lib` on the tag tree (rch): 1004 passed,
  0 failed, 13 ignored.
- **Fixed keeper loop.** The keeper `retried_create_index_persists_verbatim_sql_and_leaves_no_residue`
  (from `a712b8ea4`) ran against v0.4.4 `fsqlite-core` plus this pager in a 120-run loop: 0 failures.
- **Unfixed control.** The identical loop on the unfixed v0.4.4 tree failed 3 times by run 108, with
  `page 1974 is referenced multiple times (freelist trunk[0]; index ix_v ...)` and
  `page 1973 ... (index ix_v ...; index ix_v ...)`. rch's 3600 s cap then killed that run.
- **Publish check.** `cargo publish --locked` verified the packaged crate against the published 0.4.4
  family. The published `.cargo_vcs_info.json` records `6843bf75e`, and `src/pager.rs` is byte-identical to
  the tag.

## [0.4.4] -- 2026-09-17 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.18...v0.4.4>

First uniform-version release: all 28 workspace crates are published at 0.4.4
together. **0.4.0 through 0.4.3 were crates.io publishes only** -- partial bumps
of whichever crates had changed, with no git tag, no GitHub Release and no
changelog section (0.4.3 went out for `fsqlite-pager` alone). That left the
workspace spread across five versions with no single number naming the tree.
This entry therefore covers the whole `v0.3.18..v0.4.4` range (269 commits,
2026-09-07 through 2026-09-17), including everything that shipped unannounced as
0.4.0-0.4.3.

### Shared-memory foundations for cross-process MVCC

This release lands the native transport that cross-process concurrent writers
need. It is infrastructure, not yet the user-facing capability: **public
`Connection` still binds concurrent transactions to process-local MVCC
authority**, so GH#329 / bd-zywqc.20 remains open and single-process behavior is
unchanged. Concurrent-writer mode remains on by default.

- The page lock table moved out of the process-private heap into a mapped SHM
  descriptor, so independent processes can acquire and release pages against the
  same backing without resetting live counter or owner state. `try_acquire`
  reports the failed-CAS owner after handoff, capacity exhaustion keeps stable
  keys, and heap regions, invalid capacities and descriptor corruption are
  refused without mutating live authority
  ([316235b31](https://github.com/Dicklesworthstone/frankensqlite/commit/316235b31),
  [b2b87c216](https://github.com/Dicklesworthstone/frankensqlite/commit/b2b87c216)).
- A Unix MVCC SHM companion is mapped beside the database with its header bound
  to the main database inode. Mapping refuses partial, foreign and copied
  headers, never reinitializes an existing payload, defers cleanup that cannot
  complete, and keeps its lifetime independent of the main database descriptor so
  a failed close cannot drop a still-aliased region
  ([a0603d1f6](https://github.com/Dicklesworthstone/frankensqlite/commit/a0603d1f6),
  [e5014ca3a](https://github.com/Dicklesworthstone/frankensqlite/commit/e5014ca3a)).
- `BEGIN CONCURRENT` can bind to a session-owned token. External token
  registration preserves the epoch and refuses reuse, and page acquisition stays
  on the admitting session so a failed first admission cannot select an
  allocation domain
  ([515c9d4d2](https://github.com/Dicklesworthstone/frankensqlite/commit/515c9d4d2)).
- Connection open/close and the VDBE engine are wired onto the mapped path so the
  new lock table is exercised rather than only unit-tested
  ([591b4222a](https://github.com/Dicklesworthstone/frankensqlite/commit/591b4222a)).
- `SimplePager`/`PagerBackend` forward dedicated MVCC mappings, releasing
  `PagerInner` before awaiting the admitted file handle, and reserved-database
  bootstrap is refused when a `.fsqlite-shm` authority companion already exists
  ([6de27a8e9](https://github.com/Dicklesworthstone/frankensqlite/commit/6de27a8e9)).
  Owner IDs are no longer reused and cancellation errors are preserved
  ([8e5979ffe](https://github.com/Dicklesworthstone/frankensqlite/commit/8e5979ffe)).
- Aligned SHM `u32`/`u16` words are copied under one backing mutex
  ([2eae650d0](https://github.com/Dicklesworthstone/frankensqlite/commit/2eae650d0)).

### Native WAL-index shared memory

- A Unix connection can publish and consume a stock shared-memory WAL index
  instead of a private reconstruction. `PendingWalAppendAttempt` owns exactly one
  native append even if its future is dropped; publication is split into
  scan/prepare/publish/finish so a crashed writer cannot leave a half-published
  header; and native recovery rebuilds the index from a fully validated WAL and
  refuses to proceed while recovery is still required
  ([304280b37](https://github.com/Dicklesworthstone/frankensqlite/commit/304280b37)).
- Readers gain `NativeReaderAttempt`/`WalIndexReaderLease`: a reader binds a
  header plus read boundary, keeps a sibling claim if acquisition is cancelled,
  and retries drop cleanup instead of leaking the slot. Checkpoint cleanup
  reconciles a WAL reset only after page readback matches, including the 12-byte
  page-1 header patch
  ([304280b37](https://github.com/Dicklesworthstone/frankensqlite/commit/304280b37)).
- WAL-index codec parity with bundled stock SQLite (GH#19): integer SHM header
  fields stay native-endian while salts copy WAL big-endian bytes unchanged,
  `iChange` is treated as a commit counter rather than a schema-cookie mirror,
  and `szPage` encodes 65536 as 1. `parse_shm_header` accepts headers actually
  emitted by stock across 512/4096/65536 page sizes. Group commit now restores
  the exact WAL-append lock instead of a generic level, so concurrent readers
  cannot observe the WRITE/main-file fence drop to the wrong state
  ([b9ad861dd](https://github.com/Dicklesworthstone/frankensqlite/commit/b9ad861dd),
  [86a9b4aed](https://github.com/Dicklesworthstone/frankensqlite/commit/86a9b4aed)).
- An appender-excluding WAL checkpoint fence was added and `TracingFile` was
  corrected to use it
  ([11190b7fa](https://github.com/Dicklesworthstone/frankensqlite/commit/11190b7fa),
  [bb7f6f600](https://github.com/Dicklesworthstone/frankensqlite/commit/bb7f6f600)).
- An empty WAL is retired before leaving WAL mode
  ([760e4a9ff](https://github.com/Dicklesworthstone/frankensqlite/commit/760e4a9ff)),
  native export and retired-WAL ownership are preserved
  ([fa6eebc31](https://github.com/Dicklesworthstone/frankensqlite/commit/fa6eebc31)),
  and WAL lifetime claims are retained across msdosfs placeholder-inode migration
  ([025336f92](https://github.com/Dicklesworthstone/frankensqlite/commit/025336f92)).

### Read-only openers, WAL joining and recovery

- A read-only opener can join an existing WAL without exclusive maintenance,
  keeps WAL mode while doing so, adopts a validated WAL during bootstrap, and
  binds under a live writer; empty database-only leases are allowed
  ([03869e055](https://github.com/Dicklesworthstone/frankensqlite/commit/03869e055),
  [ee1d5740d](https://github.com/Dicklesworthstone/frankensqlite/commit/ee1d5740d),
  [4f0730819](https://github.com/Dicklesworthstone/frankensqlite/commit/4f0730819),
  [683a241bc](https://github.com/Dicklesworthstone/frankensqlite/commit/683a241bc)).
  `BusyRecovery` at read-only WAL install boundaries is traced instead of
  swallowed
  ([f12edb1a0](https://github.com/Dicklesworthstone/frankensqlite/commit/f12edb1a0)).
- Explicit WAL-index recovery for schema-only opens is available on the core API
  and exposed on the worker API
  ([51a90934b](https://github.com/Dicklesworthstone/frankensqlite/commit/51a90934b),
  [c829c0b40](https://github.com/Dicklesworthstone/frankensqlite/commit/c829c0b40)).
  The foreign `WAL_WRITE_LOCK` is held while probing index recovery
  ([8e28e84ad](https://github.com/Dicklesworthstone/frankensqlite/commit/8e28e84ad)),
  and WAL reset with foreign readers is corrected in the async API recovery path
  ([ec0b37123](https://github.com/Dicklesworthstone/frankensqlite/commit/ec0b37123)).
- WAL open repairs reserved lock-byte freelist pages (GH#462)
  ([2d3b1fae8](https://github.com/Dicklesworthstone/frankensqlite/commit/2d3b1fae8)).
- In-flight WAL commit attempts settle without a second append, and an existing
  attempt is settled rather than starting another
  ([799067818](https://github.com/Dicklesworthstone/frankensqlite/commit/799067818),
  [4a2299f4b](https://github.com/Dicklesworthstone/frankensqlite/commit/4a2299f4b),
  [2801973ca](https://github.com/Dicklesworthstone/frankensqlite/commit/2801973ca)).

### WAL-FEC repair pipeline

RaptorQ erasure coding remains durability research: the compatibility runtime's
WAL commit and recovery paths still do not write or consult repair symbols.

- A scoped WAL-FEC repair pipeline with generation-matched reclaim can be spawned
  and awaited on live connections, and generation is pipelined from the CLI
  ([23cd965a8](https://github.com/Dicklesworthstone/frankensqlite/commit/23cd965a8),
  [484bcb818](https://github.com/Dicklesworthstone/frankensqlite/commit/484bcb818),
  [bde346896](https://github.com/Dicklesworthstone/frankensqlite/commit/bde346896)).
- Log bounds are enforced through the append and scan helpers, repair-pipeline
  diagnostics are bounded to exact totals, and repair budgets are preserved
  across workers
  ([3884cdca5](https://github.com/Dicklesworthstone/frankensqlite/commit/3884cdca5),
  [36446a8a2](https://github.com/Dicklesworthstone/frankensqlite/commit/36446a8a2),
  [135897529](https://github.com/Dicklesworthstone/frankensqlite/commit/135897529)).
- FEC sidecars are reclaimed with coordinated mutations, and valid FEC prefixes
  and header I/O are preserved
  ([2545fdbc5](https://github.com/Dicklesworthstone/frankensqlite/commit/2545fdbc5),
  [efff70465](https://github.com/Dicklesworthstone/frankensqlite/commit/efff70465)).

### TypeScript SDK and WASM worker

A substantial new client surface. Its validation is Node-based: the reference
runtime is Node 22.16.0 with Node SQLite 3.49.1 standing in for the SQL core, so
these are **not** browser or FrankenSQLite-WASM receipts. Memory remains the
default storage mode; OPFS and IndexedDB page-VFS modes are still unsupported.

- Worker protocol with statement/transaction recovery following the async
  recovery APIs, and serialized requests across async connection lifetimes
  ([518a84675](https://github.com/Dicklesworthstone/frankensqlite/commit/518a84675),
  [6dc947db4](https://github.com/Dicklesworthstone/frankensqlite/commit/6dc947db4)).
  Worker requests settle across close, crashes and disposal
  ([879929041](https://github.com/Dicklesworthstone/frankensqlite/commit/879929041)).
- Managed transaction scopes are enforced at the SQL execution boundary: explicit
  begin/commit/rollback requests with per-request ownership ids, nested savepoints
  in a bounded per-host stack, prepared handles bound to their scopes and
  finalized before the scope ends, and refusal of foreign and stale requests
  including writes already queued after an `OR ROLLBACK` failure. A failed
  rollback makes the host terminal rather than allowing an autocommit escape.
  Managed SQL is preflighted to reject manual transaction boundaries -- including
  `END`, quoted or comment-obscured tails, and multi-statement single calls --
  while still allowing `CREATE TRIGGER` bodies, comments, strings and `CASE END`.
  These scope ids and preflights are not an authorization sandbox
  ([af37d726c](https://github.com/Dicklesworthstone/frankensqlite/commit/af37d726c),
  [85cfaabbd](https://github.com/Dicklesworthstone/frankensqlite/commit/85cfaabbd),
  [1db259e3e](https://github.com/Dicklesworthstone/frankensqlite/commit/1db259e3e)).
  The public SDK surface -- `database`, `statement`, `transaction` and the worker
  client -- is bound to those worker-owned scopes
  ([9fee5e4a6](https://github.com/Dicklesworthstone/frankensqlite/commit/9fee5e4a6)).
- Named bindings and reusable typed prepared statements, with the worker binding
  named SQL parameters without rewriting the statement
  ([9a868d1db](https://github.com/Dicklesworthstone/frankensqlite/commit/9a868d1db),
  [b5433ded3](https://github.com/Dicklesworthstone/frankensqlite/commit/b5433ded3)).
- Query results are negotiated and transferred as bounded binary frames end to
  end, encoding exact SQL values rather than re-serialising through JSON
  ([24e038967](https://github.com/Dicklesworthstone/frankensqlite/commit/24e038967),
  [b88ee2905](https://github.com/Dicklesworthstone/frankensqlite/commit/b88ee2905),
  [4a07c2bf9](https://github.com/Dicklesworthstone/frankensqlite/commit/4a07c2bf9)).
- Whole transactions queue behind bounded FIFO admission; snapshot barriers are
  queued against a real worker lifecycle; snapshot pools refresh atomically; and
  bounded parallel queries execute over immutable snapshot replicas
  ([62d802014](https://github.com/Dicklesworthstone/frankensqlite/commit/62d802014),
  [76420a4ba](https://github.com/Dicklesworthstone/frankensqlite/commit/76420a4ba),
  [d81bca4ac](https://github.com/Dicklesworthstone/frankensqlite/commit/d81bca4ac),
  [bdaf3724f](https://github.com/Dicklesworthstone/frankensqlite/commit/bdaf3724f)).
- Committed table changes are tracked in a bounded transactional journal, exposed
  as bounded committed-change observers and pull streams
  ([82b2a4784](https://github.com/Dicklesworthstone/frankensqlite/commit/82b2a4784),
  [73c8e3827](https://github.com/Dicklesworthstone/frankensqlite/commit/73c8e3827)).
- Managed transaction cancellation is rollback-safe, and terminal connection
  failures are surfaced to private queue owners instead of being swallowed
  ([d5782e97b](%(B)s/commit/d5782e97b),
  [dab7ca0ac](%(B)s/commit/dab7ca0ac)).
- Commit-only queue subscriptions with change iteration, and demand-driven live
  `SELECT` queries over committed changes
  ([f28c13556](%(B)s/commit/f28c13556),
  [56a7f489d](%(B)s/commit/56a7f489d)).
- `db/tx.executeStream` performs transactional streaming imports over sync and
  async row sources with bounded sequential chunks, source-buffer copies, global
  failure indices, whole-import rollback, child savepoint isolation, immutable
  provisional progress, and cancellation through the final pre-COMMIT boundary
  ([a1af6c235](https://github.com/Dicklesworthstone/frankensqlite/commit/a1af6c235)).
- Atomic bulk writes execute as one request from owned database and statement
  handles, and cancel cooperatively with rollback-safe completion
  ([e428bbfe2](https://github.com/Dicklesworthstone/frankensqlite/commit/e428bbfe2),
  [2f1167a93](https://github.com/Dicklesworthstone/frankensqlite/commit/2f1167a93),
  [04201f65d](https://github.com/Dicklesworthstone/frankensqlite/commit/04201f65d)).
- Pre-IPC request backpressure: validated per-client `requestLimits` and frozen
  `requestQueue` snapshots, bounded protocol payloads captured before
  `postMessage`, count and bytes reserved until settlement and released exactly
  once on errors, crashes and disposal, full binary backing capacities counted
  with sparse oversized arrays rejected early, and one deduplicated close lane
  outside ordinary capacity
  ([b82f3e6c6](https://github.com/Dicklesworthstone/frankensqlite/commit/b82f3e6c6),
  [b0a5ba892](https://github.com/Dicklesworthstone/frankensqlite/commit/b0a5ba892)).
- Opt-in versioned IndexedDB snapshot storage: explicit whole-image checkpoints
  (not a page-level VFS) published through a strict-durability compare-and-swap
  transaction that acknowledges transaction completion rather than request
  success. Random revision tokens prevent stale-writer ABA after origin eviction
  or recreation; identity, envelope, SQLite header and page alignment, size and
  SHA-256 are validated; corrupt reads are never replaced with an empty database;
  images are bounded at 64 MiB. `db.checkpoint()` is published atomically through
  the host FIFO and refused inside managed or manually opened SQL transactions --
  SQL `COMMIT` and close never silently claim persistence
  ([5d3b2fc31](https://github.com/Dicklesworthstone/frankensqlite/commit/5d3b2fc31),
  [a640dcf7b](https://github.com/Dicklesworthstone/frankensqlite/commit/a640dcf7b)).

### Page accounting and freelist repair

- Pages abandoned by a rolled-back writer are reclaimed into the durable freelist
  again on the native WAL-index path. The native-reader guard correctly skipped
  the snapshot refresh (preparation owns a reader, not WRITE) but also drained the
  reclamation candidates into the abandonment pool with `Vec::append`, which
  empties its argument -- so the loop that feeds `pending_free_pages`, the only
  route into durable page-1 freelist metadata, ran over nothing. The pages cycled
  between the pool and the in-memory freelist and were lost at close, surfacing to
  stock SQLite's own `integrity_check` as `page N is never used`. Only the refresh
  is skipped now; the reclamation runs on both paths, reading the physical
  appended tail under the backend write lock (bd-u2kmg)
  ([e21d008b4](https://github.com/Dicklesworthstone/frankensqlite/commit/e21d008b4)).
- A pending freelist repair belongs to a real writer. Read-only, deferred and
  lazy-concurrent transactions no longer advertise pending writes and plan an
  unpublished commit when the repair flag is set, and repair now predicts every
  synthesized metadata page for conservative conflicts rather than page 1 alone
  (GH#462)
  ([4927ad6e1](https://github.com/Dicklesworthstone/frankensqlite/commit/4927ad6e1)).

### SQL, VDBE and storage correctness

- `INSERT ... OR IGNORE`/`OR FAIL` no longer burns a rowid when the row is
  discarded on a rowid table. The provisional landing state is cleared when a
  secondary-index conflict rolls the pending table row back, so the implicit
  allocator no longer believes the discarded rowid landed (bd-55kh5)
  ([725e31ee7](https://github.com/Dicklesworthstone/frankensqlite/commit/725e31ee7)).
- A `UNIQUE` violation names the columns that actually conflicted rather than a
  generic set (bd-towj6)
  ([d03340a26](https://github.com/Dicklesworthstone/frankensqlite/commit/d03340a26)).
- `NotFound`/`NotExists`/`IfNoHope` and `Found` position the `MemDatabase` cursor
  on a seek hit instead of leaving it unpositioned, so a following read observes
  the row the seek located (bd-29phg)
  ([72d325fd7](https://github.com/Dicklesworthstone/frankensqlite/commit/72d325fd7)).
- `VIRTUAL` generated-column expansion is bounded and its dependency cycles are
  rejected at program build; a `VIRTUAL` column is evaluated for `NOT NULL`
  enforcement; and builtin collations resolve through the expected fallback
  (bd-fjieg.5/.6/.7)
  ([8ec22dcea](https://github.com/Dicklesworthstone/frankensqlite/commit/8ec22dcea)).
- Integrity PRAGMAs check attached databases, not just `main` (bd-fjieg.1)
  ([ce74e6986](https://github.com/Dicklesworthstone/frankensqlite/commit/ce74e6986)).
- `DROP TABLE`/`DROP INDEX` free the b-tree in 2048-page batches after the
  catalog delete, and the teardown page budget is an actual bound: an explicit
  work queue replaces unbounded recursion so a single batch cannot exceed its
  budget on deep subtrees or long overflow chains (bd-fmhvo)
  ([0a5335a86](https://github.com/Dicklesworthstone/frankensqlite/commit/0a5335a86),
  [2a12368a8](https://github.com/Dicklesworthstone/frankensqlite/commit/2a12368a8),
  [a1edaba40](https://github.com/Dicklesworthstone/frankensqlite/commit/a1edaba40)).
- Virtual-table `UPDATE` callbacks are preserved and registered modules restored
  ([1e297d189](https://github.com/Dicklesworthstone/frankensqlite/commit/1e297d189)).
- Cancellation is observed before indexed and rowid read shortcuts, and pager
  cancellation is preserved while opening a cursor (bd-6hdwo.37)
  ([3ccf8ef32](https://github.com/Dicklesworthstone/frankensqlite/commit/3ccf8ef32),
  [d45708c1f](https://github.com/Dicklesworthstone/frankensqlite/commit/d45708c1f)).
- Bare `PRAGMA` table-valued functions are rewritten and CTE materialization is
  ordered ([30f56b466](https://github.com/Dicklesworthstone/frankensqlite/commit/30f56b466));
  `RefMut` takes are hoisted before `await` without dropping JSON aggregates
  ([6a55c2042](https://github.com/Dicklesworthstone/frankensqlite/commit/6a55c2042)).
- Tiered fetch preference allocation is bounded (bd-6hdwo.21.1) and foreign
  tiered fetch records are rejected before recovery (bd-ogbyg)
  ([911cfcb1d](https://github.com/Dicklesworthstone/frankensqlite/commit/911cfcb1d),
  [08d70ada8](https://github.com/Dicklesworthstone/frankensqlite/commit/08d70ada8)).

### Platform and build

- Asupersync 0.5 alignment across the storage library contexts -- the breaking
  change behind the 0.4.x series
  ([51f6348ac](https://github.com/Dicklesworthstone/frankensqlite/commit/51f6348ac)),
  with the asupersync 0.5.0 pin
  ([b027c58a7](https://github.com/Dicklesworthstone/frankensqlite/commit/b027c58a7))
  and `syn` 3.0.5 for `full`+`visit`
  ([5c52db333](https://github.com/Dicklesworthstone/frankensqlite/commit/5c52db333)).
- Windows database workers honor bounded inline I/O, and non-Unix generation
  probes use the identity accessor
  ([3a59109d8](https://github.com/Dicklesworthstone/frankensqlite/commit/3a59109d8),
  [043cc64c0](https://github.com/Dicklesworthstone/frankensqlite/commit/043cc64c0)).
- A main file that was zero-length at open adopts its materialized inode, fixing
  msdosfs placeholder `st_ino`
  ([8700145a3](https://github.com/Dicklesworthstone/frankensqlite/commit/8700145a3)).
- WASM preserves recoverable parser diagnostics under bindgen 128 (bd-7rg1a.3)
  ([b9d5ac148](https://github.com/Dicklesworthstone/frankensqlite/commit/b9d5ac148)).

### Performance

- Clearing a retained WAL-index tail copies hash slots once instead of repeatedly
  ([be5ee0fe9](https://github.com/Dicklesworthstone/frankensqlite/commit/be5ee0fe9)).
- Group-commit member traces are prepared only when tracing is enabled
  (bd-sde05.3)
  ([dcb6036b8](https://github.com/Dicklesworthstone/frankensqlite/commit/dcb6036b8)).
- The S3-FIFO hasher and work receipts are parameterized
  ([1c1a345a0](https://github.com/Dicklesworthstone/frankensqlite/commit/1c1a345a0));
  a rejected GH#402 hashing experiment was withdrawn and the S3-FIFO map restored
  after its failed regression gate
  ([406628c73](https://github.com/Dicklesworthstone/frankensqlite/commit/406628c73),
  [97264e608](https://github.com/Dicklesworthstone/frankensqlite/commit/97264e608)).

### Known test-suite defects

Two pre-existing unit tests are order- and load-sensitive rather than
deterministic. Neither reflects a product defect, and neither source file changed
in this release.

- Eight `fsqlite-btree` unit tests assert deltas on process-global observability
  counters, so a concurrently running test can move the counter between the two
  reads. They pass under `--test-threads=1` and fail intermittently under the
  default parallel run (bd-pyygd).
- `fsqlite-wal`'s `bd_ncivz_2_append_during_epoch_fence_stays_in_previous_epoch_lane`
  now fails deterministically. It passed at v0.3.18 and fails in this range, but
  `per_core_buffer.rs` itself has not changed since 2026-07-31: the test
  spin-waits with `std::thread::yield_now()` on a `current_thread` runtime's only
  thread while waiting for a task that same runtime must poll, so it only ever
  passed while the runtime happened to dispatch that task before the loop began.
  Bisected to
  [fdffd1c2e](https://github.com/Dicklesworthstone/frankensqlite/commit/fdffd1c2e),
  the Asupersync 0.4.11 upgrade, which no longer dispatches a spawned task
  before the spawning caller blocks the thread; the 200 ms deadline now always
  expires. The coordinator code under test is not implicated -- the test never
  reaches it (bd-lvy4c).

---

## [0.3.18] -- 2026-09-06 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.17...v0.3.18>

### SQL and transaction correctness

- The shell prints each result in a same-line SQL batch, including `-c`
  commands: `SELECT 1; SELECT 2;` now prints both values. It respects trigger
  bodies, quoted semicolons and comments, renders headers per statement, and
  preserves earlier output and committed changes when a later statement fails.
  Parsing errors retain the engine's SQLite-compatible diagnostics
  ([5eaecc593](https://github.com/Dicklesworthstone/frankensqlite/commit/5eaecc593),
  [a653ed1f2](https://github.com/Dicklesworthstone/frankensqlite/commit/a653ed1f2)).
- Aggregate queries with `AND`/`OR` predicates preserve lazy `IN`-subquery
  evaluation. A condition that already determines the result skips the other
  operand, including an erroring grouped subquery. Uncorrelated scalar `IN`
  operands in the WHERE clause's `AND`/`OR`/`NOT` branches reuse their results
  within its fallback loop; new executions use their current parameters and
  data. Scalar `IN` comparisons
  distinguish absent expression affinity from declared BLOB affinity, and
  fallback NUMERIC casts preserve SQLite's integer/real conversion behavior
  ([cec7dd37c](https://github.com/Dicklesworthstone/frankensqlite/commit/cec7dd37c)).
- Lazy membership memo state belongs to its executing future and is installed
  only while that future is being polled. Suspending or cancelling overlapping
  executions in a different order preserves each execution's parameter-bound
  results ([c2bc489d6](https://github.com/Dicklesworthstone/frankensqlite/commit/c2bc489d6)).
- Leaving WAL mode after a checkpoint preserves the pager's logical commit
  sequence while rebasing its main-file and WAL counters. This fixes stale
  snapshot errors on subsequent reads or writes in DELETE mode. The stock
  SQLite oracle also covers further writes after returning to WAL and reopening
  the final file with stock SQLite
  ([9bcba3129](https://github.com/Dicklesworthstone/frankensqlite/commit/9bcba3129)).
- Journal-mode guards now write in WAL, DELETE, TRUNCATE, PERSIST, MEMORY and
  OFF modes, then return to WAL. An independent stock SQLite reader checks
  acknowledged rows and database integrity after each mode's write, before
  the writer performs another operation
  ([c2bc489d6](https://github.com/Dicklesworthstone/frankensqlite/commit/c2bc489d6)).

### Query planning

- `WHERE <rowid> IN (?1, ?2, …)` on an INTEGER PRIMARY KEY table now plans as
  rowid seeks instead of a full table scan (GH#415). The IN-list seek only
  admitted integer literals, so the parameterized list every driver emits —
  and `rowid IN (?, …)`, `id = ?1 OR id = ?2`, and literal lists such as
  `IN (-5, '7', 2.0)` — walked the whole table with the membership test
  evaluated per row (cass GH#382 spent minutes scanning 12.9M rows for 5 ids).
  Bind-time-constant members are now materialized once into a sorted,
  de-duplicated ephemeral probe set and `SeekRowid`-looped, the same build-then-
  loop shape stock SQLite emits: NULL, non-integer, and duplicate members are
  dropped or collapsed at bind time, text/real members coerce under INTEGER
  affinity (`'3'`/`3.0` match rowid 3, `2.5`/`'3abc'` match nothing), and
  `ORDER BY <rowid>` (ASC/DESC) is served by the probe set without a sorter.
  The same path serves `COUNT(*)`, `UPDATE`, and `DELETE` by parameterized
  rowid list, including residual conjuncts (`… AND v = ?`) with correct
  anonymous-placeholder numbering. `EXPLAIN QUERY PLAN` reports
  `SEARCH t USING INTEGER PRIMARY KEY (rowid=?)` for these shapes, including
  the ORDER BY and aggregate forms that carry no planner directive. The
  secondary-index IN-list seek (`<indexed col> IN (?, ?)`) still requires
  integer literals and remains a follow-up
  ([0061b4ea7](https://github.com/Dicklesworthstone/frankensqlite/commit/0061b4ea7),
  [1e770535d](https://github.com/Dicklesworthstone/frankensqlite/commit/1e770535d)).

### Regression coverage

- The in-memory autocommit guard now runs 10,000 indexed writes and compares
  the complete contents with stock SQLite, then checks subsequent insert,
  update and delete visibility. Its counters distinguish parked writes from
  periodic commits. The GH409 indexed-read scaling guard now requires the
  exact inserted row in both fixtures; missing rows can no longer pass
  ([d65ada8ea](https://github.com/Dicklesworthstone/frankensqlite/commit/d65ada8ea)).
- File-backed autocommit guards now enforce the existing memory-only retention
  policy: every acknowledged write is visible before another writer operation.
  The crash guard requires the child to reach its intended transaction boundary
  and terminate abruptly; stock SQLite opens first and recovers acknowledged
  rows while discarding the open transaction's uncommitted row
  ([9bcba3129](https://github.com/Dicklesworthstone/frankensqlite/commit/9bcba3129)).
- DQS guards check default-on stock parity, explicit opt-out, valid quoted
  identifiers and restoring the setting on the same connection. Component
  repair guards tie successful repair witnesses to the expected content and
  distinguish failed decode from early bailout without fabricating a recovered
  payload. This does not establish automatic repair through the public database
  API ([f697ed38d](https://github.com/Dicklesworthstone/frankensqlite/commit/f697ed38d)).
- Release gate on the tagged commit (pinned nightly, Linux x86_64):
  `cargo fmt --check` and
  `cargo clippy --workspace --all-targets --locked -- -D warnings` clean, plus
  the GH#415 rowid IN-list oracle suite, the `fsqlite` compatibility suite
  (incl. the read-only WAL reader guards), the rowid residual oracles, and the
  `fsqlite-pager` unit suite green. The `fsqlite-vfs` unit suite is green
  single-threaded and in isolation; under default test parallelism two
  pre-existing tests
  (`namespace::tests::cleanup_retry_requires_a_fresh_complete_ownership_proof`,
  `uring::tests::io_uring_vfs_default_equals_new`) flake at the same rate on
  v0.3.17 as on this commit (4 of 6 runs each) and are tracked as a known
  test-harness race, not a release regression. Full workspace and
  all-features test execution and historical benchmark comparison were not
  part of this cut.

### Storage and WAL

- Read-only WAL readers keep their typed open flags independent of the shared
  canonical descriptor's write capability. A read-only handle shares a safe
  existing WAL mark and keeps the shared-lock recheck instead of publishing a
  new mark into the SHM sidecar, so opening a database read-only leaves the
  database, WAL and SHM bytes and timestamps unchanged; empty-WAL readers use
  the database-only slot0 fence, and a reader that finds no safe mark refuses
  rather than guessing. Writable callers keep mark publication and the
  concurrent-writer defaults
  ([f92a5b376](https://github.com/Dicklesworthstone/frankensqlite/commit/f92a5b376)).
- A read-only pager that cannot register its WAL reader slot now fails closed
  instead of warning and proceeding unregistered, where a peer could backfill
  or reset the WAL underneath its snapshot; registration errors propagate for
  both access modes. A directory in place of `*-shm` surfaces `IsADirectory`
  and leaves the main and WAL bytes untouched
  ([9d0cb346d](https://github.com/Dicklesworthstone/frankensqlite/commit/9d0cb346d),
  [fc4bae08e](https://github.com/Dicklesworthstone/frankensqlite/commit/fc4bae08e)).
- The Linux io_uring backend (`linux-asupersync-uring`) no longer releases
  in-flight buffers while the kernel may still be writing them. A driver
  failure used to disable the ring, drain the userspace queue and drop the
  in-flight allocations; completion counters were also bumped at SQE
  construction, so a failed `submit()` still looked submitted. Admission and
  cleanup now fail closed: `register_sync_cancel` is probed at init (kernels
  without it take the Unix fallback), enqueue is paired with availability
  under the queue lock, staged SQEs are submitted until the queue drains and
  only then counted, and `fail_driver` issues a synchronous cancel barrier
  before failing residual entries while still honoring writes whose CQE
  already completed (bd-6hdwo.35;
  [36eaa5397](https://github.com/Dicklesworthstone/frankensqlite/commit/36eaa5397)).
- The shared io_uring driver is woken by a nonblocking `eventfd` instead of
  waiting out its poll timeout: producers that find the driver waiting write
  the eventfd, the driver publishes `waiting` under the same lock and polls
  the ring fd and the eventfd together, coalesced wakeups are consumed by a
  single read, `POLLERR`/`POLLHUP`/`POLLNVAL` fail the driver, and a failed
  wake write only warns and falls back to the periodic wait. Init fails
  closed to the Unix backend when the eventfd cannot be created
  ([a91409e82](https://github.com/Dicklesworthstone/frankensqlite/commit/a91409e82)).
- `-ECANCELED` completions produced by the driver-failure quiescence barrier
  are reported as `Failed` with the driver error, no longer as `Cancelled`,
  which looked like the caller's own `Cx` cancellation and left a cancelled
  context behind for a later checkpoint. The `fsqlite-vfs` README documents
  the Linux 6.0+ synchronous-cancel probe and the Unix fallback
  ([88fdb5dde](https://github.com/Dicklesworthstone/frankensqlite/commit/88fdb5dde)).

### MVCC and SSI

- The SSI audit gate's e-process is trained only by commits that actually
  measured SSI edges. `should_skip_ssi_validation` was consulted before the
  BEGIN-time serializable policy was read, so FCW-only connections still hit
  the gate, and early `BusySnapshot` aborts (marked-for-abort, invalid handle,
  FCW conflict) fed a synthesized pivot/clean outcome into
  `observe_ssi_outcome`. The gate is now consulted only under a serializable
  BEGIN policy and observes only a completed SSI plan or a pivot abort
  ([c927c2a54](https://github.com/Dicklesworthstone/frankensqlite/commit/c927c2a54)).

### Replay diagnostics

- Agent-swarm replay captures actual connection-local fallback events around
  each statement, retaining engine identities and reporting bounded-ring
  losses. Scoring no longer treats SQL keywords, workload labels or seeded
  contract rows as fallback evidence. The operator runbook uses
  `PRAGMA fsqlite.fallback_events`; unavailable EXPLAIN CONCURRENCY and
  governor observations serialize as `null`. This connects the existing
  fallback producer and does not complete the broader coordination diagnostic
  surface ([300528044](https://github.com/Dicklesworthstone/frankensqlite/commit/300528044)).
- Bounded connection-local validation, publication and read-only finish
  events are recorded from the real concurrent commit path, tagged with
  transaction epochs and poll-scoped statement identities, and exposed through
  the public API and diagnostic PRAGMAs (capture controls plus snapshots).
  Swarm replay consumes only newly emitted events and never synthesizes
  commit evidence from trace labels or oracle rows; the SSI gate is trained
  only when BEGIN-time policy actually selected SSI validation. Retention,
  privacy, metric independence and event-versus-write counts are documented
  in `docs/user/observability.md`
  ([4b5309b03](https://github.com/Dicklesworthstone/frankensqlite/commit/4b5309b03)).

### Native storage

- Native namespace sidecars (`-fsqlite-ns-gate` / `-fsqlite-ns-use`) no longer
  fail `CannotOpen` on mounts that do not persist POSIX permission bits (a WSL2
  Windows drive without the `metadata` mount option, FAT/exFAT), where the
  0600 creation mode and every chmod are ignored and every file reports the
  mount mask. A sidecar this process just created with `O_CREAT|O_EXCL` is
  accepted whatever mode the mount reports back, and an existing sidecar is
  accepted when it grants no group/other bit that the database file itself
  does not already grant to the same principals — group bits count only when
  the sidecar and the database share a group, since POSIX resolves one
  permission class per process; the owner, single-hard-link and regular-file
  checks are unchanged, and a sidecar loosened beyond its database on a real
  POSIX filesystem still fails closed (beads_rust GH#491;
  [947d4aa85](https://github.com/Dicklesworthstone/frankensqlite/commit/947d4aa85),
  [64e75a742](https://github.com/Dicklesworthstone/frankensqlite/commit/64e75a742)).

---

## [0.3.17] -- 2026-09-06 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.16...v0.3.17>

### SQL correctness and prepared statements

- SQL `printf` and `format` now share the shell's SQLite 3.53 float decoder
  for `%!e`, `%!f` and `%!g`, including uppercase variants, fractional rounding
  carries and high precision. The bound-value differential guard compares
  10,680 results with bundled SQLite 3.53.2
  ([6455fbb29](https://github.com/Dicklesworthstone/frankensqlite/commit/6455fbb29)).
- Prepared reads that recompile after a schema change now release the failed
  attempt's automatic read transaction. Subsequent index creation and writes
  succeed, while explicit transactions retain their caller-owned changes and
  rollback behavior. The oracle guard covers buffered, single-row and streaming
  reads on memory and file databases
  ([19759440d](https://github.com/Dicklesworthstone/frankensqlite/commit/19759440d)).
- Correct grouped `IN`-subquery aggregate results under an `AND` condition
  (GH#407), preserve JSON subtypes in grouped JSON aggregates, and reject
  no-op writes on read-only connections during preparation.
- Resolve hidden `rowid`, `_rowid_` and `oid` update targets, re-key indexes,
  and report stock-compatible rowid collision errors. A declared column still
  shadows an alias. Updating hidden rowids on tables with UPDATE triggers
  remains explicitly unsupported
  ([7f2f076e1](https://github.com/Dicklesworthstone/frankensqlite/commit/7f2f076e1)).
- Align parser error text for empty input, recursion limits and window frames.
  Strengthen the deferred-foreign-key oracle: a rejected COMMIT leaves the
  transaction open so the caller can repair the violation or roll back.

### FTS5, page storage and transaction safety

- Lazy FTS5 query timing uses an optional clock supplied by the host reader.
  `FSQLITE_FTS5_TIMING` retains its diagnostic output; disabled timing avoids
  clock reads. The extension no longer acquires ambient clock authority
  ([d7c943c60](https://github.com/Dicklesworthstone/frankensqlite/commit/d7c943c60)).
- Preserve a live FTS5 index only when its persisted content metadata still
  matches. Queries observe foreign SQLite writes while unrelated commits avoid
  rebuilding an unchanged corpus (GH#408). Keep contentless lazy reads
  independent of MemDB hydration, and correct prefix BM25 scoring on the
  Shadow snapshot path
  ([af5ea90f9](https://github.com/Dicklesworthstone/frankensqlite/commit/af5ea90f9),
  [02a967145](https://github.com/Dicklesworthstone/frankensqlite/commit/02a967145)).
- Apply `PRAGMA cache_size` to real pager residency. Preserve independent TEMP
  and attached-database settings; rollback restores persistent header defaults
  without overwriting the connection's runtime cache suggestion. This is a
  resident-page budget, not a hard process-memory limit.
- Repair impossible freelist entries, keep the reserved lock-byte page out of
  the freelist, and accept unqualified `PRAGMA repair_freelist` (GH#410).
  Cancelled commit preparation preserves allocations still owned by savepoints.
- Account for private reservations and freed high pages during live integrity
  checks while retaining catalog visibility rules and corruption detection.
  Return savepoint-discarded allocations at transaction end, preserving their
  ownership across failed or cancelled commit preparation. File guards cover
  later growth through the same or another connection in WAL and rollback-journal
  modes, followed by stock SQLite reopen.
- Fold appended WAL frames into the existing tail index. Native trigger and FK
  cascade admission uses measured remaining stack where available; unknown
  stack size and wasm retain the existing depth caps (GH#414).

### Metrics and execution diagnostics

- Start the configured HTTP metrics listener through ordinary database open or
  the metrics PRAGMA. Wire actual SQL commit counts and latency, page-lock wait
  durations, successful WAL durability barriers, MVCC conflict outcomes,
  completed version-store GC passes, local schema invalidations, integrity-check
  outcomes, and active concurrent-writer counts to the existing exporters.
  Count held pager snapshots through their actual lifetime, including writer
  views and cached memory reads, and aggregate them across databases.
- Verify metric counts through real public SQL, live HTTP scrapes, structurally
  corrupted files, and an eight-writer workload with independently reopened
  stock SQLite row/payload checks. Disabled recording is covered separately.
  The metrics overhead target remains unmeasured; history gauges and accurate
  WAL checkpoint backlog accounting remain open.
- Expose actual SSI conflicts and bounded fallback execution events with
  connection, statement and transaction identities. Early SSI aborts report
  unmeasured edge counts explicitly. Bound pending and retained evidence counts,
  reject oversized diagnostic payloads before admission, and expose separate
  loss counters. The payload budget does not limit transaction validation or
  caller-side allocations
  ([464ed1ca5](https://github.com/Dicklesworthstone/frankensqlite/commit/464ed1ca5),
  [2ae21ab7e](https://github.com/Dicklesworthstone/frankensqlite/commit/2ae21ab7e)).

### Shell and native-storage components

- Shell list, column and line modes render bare text, empty NULL cells and
  stock-style separators/alignment. Quote-mode REAL formatting follows SQLite
  3.53's `%!.20g` expansion. CSV quotes empty text and blob values separately
  from NULL, and handles spaces, control bytes, quotes and non-ASCII bytes.
  Display writers preserve raw blob bytes up to the first NUL. Column mode
  enables headers unless explicitly overridden; empty result sets emit nothing.
  Stock-shell comparisons cover all 256 byte values in CSV.
- Await connection shutdown on shell exit, startup errors, command completion
  and database switching. Open transactions roll back, and committed WAL data
  receives the existing opportunistic close checkpoint. A failed `.open`
  preserves the current connection and transaction. Eight real-file scenarios
  verify committed rows and integrity through stock SQLite, including a copy
  of the main database without its WAL sidecar (bd-uo4uk).
- Replace placeholder replication repair bytes with actual RaptorQ encoding
  under caller `Cx` cancellation and admission checks. Harden root bootstrap
  authentication, supported layout checks, systematic-record reconstruction and
  bounded locator reads. Native-index admission preserves previously buffered
  updates when a later update is invalid. Packed and aligned symbol append
  reject malformed payload lengths and frame checksums before creating or
  modifying a segment
  ([4e0176378](https://github.com/Dicklesworthstone/frankensqlite/commit/4e0176378)).
- These native-storage changes are component work. Public durable ECS
  create/open, complete fault/restart proof, and compatibility-WAL repair-symbol
  integration remain unfinished. They are not advertised as shipped native
  storage or automatic erasure-coded recovery.
- Pin the Rust toolchain to `nightly-2026-08-31`. Release builds continue through
  DSR and crates.io; GitHub Actions remains disabled.

---

## [0.3.16] -- 2026-09-03 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.15...v0.3.16>

The headline is the FTS5 **lazy read path made usable at scale**: a downstream
consumer (cass) with a large contentless FTS5 index saw `MATCH` queries hang or
promote the whole corpus into memory on every short-lived process. This release
turns plain `MATCH`, `ORDER BY rank`/`bm25()`, and prefix ranking into
bounded, point-read operations that match stock SQLite's results without
hydrating the corpus. Alongside it: the bd-9inpb page-allocator double-grant is
closed, GH#405 makes FTS5 savepoints row-proportional, GH#382 removes a
per-page WAL rescan, GH#406 makes content-backed INSERT an incremental append,
and the dependency lockfile is refreshed.

### FTS5 lazy MATCH is fast at scale — the cass runaway (Fix A / Fix B / Fix C)

Measured against sqlite3 3.46.1 on a 400K-doc corpus, fresh process per query
(the cass reality: one query per short-lived CLI). Three defects compounded:

- **Fix A — plain MATCH scored BM25 it then discarded** (`a6712344a`). The lazy
  `MATCH` scan computed a full BM25 ranking (used for nothing, since ordering is
  a separate concern) and BM25 was O(hits²) — each doc rescanned the term's
  whole doclist. Added `Fts5LazyQuery::matched_rowids` /
  `Fts5Table::matched_rows_lazy` that resolve matching rowids without scoring.
  Content `MATCH 'cargo'` (10 000 hits): **17 s → 0.4 s**.
- **Fix B — contentless tables were not lazy on the ordinary open path**
  (`1e4db3c69`). `allow_lazy_contentless_fts5` was enabled only for schema-only
  opens, so a `content=''` table opened normally (cass's exact schema) was never
  bound lazily and fell into the in-memory scan **unhydrated**, hanging on any
  hit-bearing `MATCH`. Enabling it on `open_with_env_and_pager` routes contentless
  MATCH through the point-read path. Contentless `MATCH`: **hang (>300 s) → 0.10 s**.
- **Fix C — ranked MATCH promoted the whole corpus** (`07402981d`). `ORDER BY
  rank` / `bm25()` rebuilt the entire corpus into an in-memory index (2.5–3 GB
  RSS, 10–33 s). New `Fts5PrecomputedScores` extracts only the matched rows'
  BM25 inputs (corpus size, average doc length, per-term document frequency,
  per-row doc length, per-(term,row) column frequencies) via the on-disk reader
  at aux-context build time, walking each term's doclist once (O(hits), not
  O(hits²)). Ranked `MATCH 'cargo'`: **24.8 s / 2.5 GB → 0.30 s / 179 MB**
  (content), **33.3 s / 2.95 GB → 0.10 s / 53 MB** (contentless), BM25 values
  bit-identical to stock on both schemas. `snippet()`/`highlight()` need no
  score source (they read the projected row text), so the precomputed source
  serves every auxiliary consumer.

Prefix ranking (`MATCH 'x*' ORDER BY rank`) is now scored correctly too: a
prefix is one BM25 unit — document frequency is the number of distinct docs
matching the prefix and per-doc term frequency is the total position count
across all expanded terms — on both the lazy (`Fts5PrecomputedScores`,
`42695407b`) and in-memory (`bm25_score_with_score_terms`, `89564725c`) score
paths, via the new `Fts5ScoreTerm` classification (`ea11d3f16`). Guarded by the
`bd_fts5_lazy_ranked_parity` differential keeper (rowid order + BM25 value to
1e-6, lazy and in-memory, content and contentless, with multi-term prefixes).

Residuals tracked, not shipped as blockers: prefix scoring and a small
contentless BM25-value difference remain on the reverted *Shadow* score source,
reached only when a contentless table is promoted via the rare
delete-of-a-pre-origin-tracking-row edge (`bd-fts5-prefix-bm25-unranked`,
`bd-fts5-contentless-promote-bm25-value`). All user-facing paths (lazy reopen,
in-memory create+query) are correct.

**Delivered capability:** contentless FTS5 search — plain, ranked, and prefix —
answers in ~0.1 s on a large index without a per-connection corpus rebuild.
**Representative commits:**
[a6712344a](https://github.com/Dicklesworthstone/frankensqlite/commit/a6712344a),
[1e4db3c69](https://github.com/Dicklesworthstone/frankensqlite/commit/1e4db3c69),
[07402981d](https://github.com/Dicklesworthstone/frankensqlite/commit/07402981d),
[42695407b](https://github.com/Dicklesworthstone/frankensqlite/commit/42695407b),
[89564725c](https://github.com/Dicklesworthstone/frankensqlite/commit/89564725c).

### Page allocator: EOF-growth double-grant closed under the reserved append lock (bd-9inpb)

Two connections growing a database concurrently could each allocate the same
fresh EOF page and both commit it, aliasing one physical page into two b-trees
("2nd reference to page N", lost rows). The `bd-vnxjd` monotonicity gate only
compared the new size against the peer-advanced floor, missing the equal-size
case. `commit_flush` now, under the RESERVED append lock, re-derives the
pre-floor snapshot size and refuses any batch whose freshly allocated pages fall
in `(snapshot_db_size, durable_floor]` — first-committer-wins with a retryable
`BusySnapshot` (`6f61702f9`). Verified with a firing repro
(`am152_allocator_race_wal`, 8-way, stock-oracle-classified): 3 conclusive
double-grants / 76 runs before, 0 / 80 after; perf-neutral at 1/2/4/8 writers.
A separate 16-writer BEGIN-starvation regression the repro surfaced is filed at
`bd-gh382-16writer-begin-starvation`. **Representative commit:**
[6f61702f9](https://github.com/Dicklesworthstone/frankensqlite/commit/6f61702f9).

### FTS5: savepoints no longer clone the whole table (GH#405)

`Fts5Table::begin` and `Fts5Table::savepoint` used to deep-clone the entire
in-memory table state (inverted index, documents, shadow rows, locales), and
the connection wraps every statement in an internal savepoint, so each INSERT
into a large fts5 table cost O(table): on a 538,807-row table that was minutes
per statement at 100 % of one core and a doubled resident set. `Fts5Table` now
keeps an `Fts5UndoLog`: one inverse operation per mutated row (an insert
records only its rowid; an update or delete records the previous content,
locales and that row's postings), savepoints are positions in the log plus a
copy of the small table header, and only the bulk rewrites (shadow-row
materialization, contentless hydration, rebuild, bind) still fall back to a
full snapshot. `rollback`, `rollback_to` and `release` keep their observable
semantics (`f68fa54c1`, `afe829f4b`; downstream cass GH #413, bead
`coding_agent_session_search-cjugu`).

### WAL: appended-tail reads no longer rescan the whole WAL per page (cass GH #382)

`reclaim_disowned_in_range` (run by `checkpoint` and by the on-open reclamation
sweep) resolved every disowned-ledger page through
`WalBackend::read_page_at_appended_tail`, which walked every WAL frame header
backwards per page. On a 10 GB WAL-mode database with a 1,576,443-entry ledger
and a 48,607-frame (200 MB) WAL that is ~7.7e10 header reads: every writable
open and every checkpoint ran at 100 % of one core and never returned (strace +
gdb on the symbolized `fsqlite` CLI, 0.3.14 and 0.3.15). `WalBackendAdapter`
now keeps an `AppendedTailIndex` — one header pass over the tail, keyed on the
generation identity, the frame count and the last frame's checksum — so a
stable tail is indexed once and a changed tail costs one fresh pass; the answer
is unchanged (newest frame wins). Receipt on that archive's copy: `PRAGMA
wal_checkpoint(PASSIVE)` returned `0 | 48607 | 48607` in 53 s (commit
`8d012706a`; downstream bead `coding_agent_session_search-g3zyo`).

### FTS5: content-backed INSERT is an incremental segment append (GH#406)

A content-backed FTS5 INSERT now persists as an incremental on-disk segment
append rather than a full re-index, keeping bulk load proportional to the rows
inserted (`2f046606b`, with quarter-timing instrumentation in the scaling
keeper, `43cf32948`).

### Dependencies: lockfile refreshed to latest semver-compatible versions

`Cargo.lock` was refreshed (lockfile-only; no manifest constraints changed):
`asupersync` 0.4.8 → 0.4.10 (the async runtime), `franken-*` 0.4.9, `blake3`
1.8.7, `aes-gcm` 0.11.1, `smallvec` 1.16.0, `flate2` 1.1.10, `icu_*` 2.3.x, and
~30 more. `tinyvec` is held at 1.12.0 (1.13.0 fails to compile here). Validated
against the concurrency canon (`mvcc_concurrent_writers`, the concurrent-writer
e2e) plus `check`/`clippy --workspace --all-targets -D warnings` and the FTS5
suite before landing. See `UPGRADE_LOG.md` (`8935d7ecf`).

---

## [0.3.15] -- 2026-09-02 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.14...v0.3.15>

A patch release completing the FTS5 contentless migration story from v0.3.14
(GH#404), plus two data-safety hardening fixes and the restoration of the
macOS `clippy -D warnings` gate.

### FTS5 `'optimize'` now rewrites the index (bd-aks56)

`INSERT INTO t(t) VALUES('optimize')` used to promote the table and write
nothing back. It now does what stock's does (fts5_index.c
`sqlite3Fts5IndexOptimize`): every segment is merged into one freshly written
segment with `%_idx` seek rows, and an index that is already a single
tombstone-free, seekable segment is left untouched. A contentless table is
merged level by level from its persisted segments through the same lazy merge
path the automerge uses, so the corpus is never hydrated; a content-backed
table is re-indexed from `_content`. Because the rewrite uses the current
writer, **this is the in-place migration for FTS5 indexes written before
v0.3.14 (GH#404): for contentless tables, where `'rebuild'` is refused by
fsqlite and stock alike, it is the only in-engine one.** Verified on the GH#404
reporter's own corpus written by the crates.io 0.3.13 engine: before, stock
reported `fts5: corruption on page 47, segment 1` and `MATCH 'segment'` = 0;
after one `optimize`, stock `integrity_check` = ok and `MATCH 'segment'` = 2759
on both engines. Keepers:
`bd_aks56_optimize_merges_every_segment_into_one_stock_shaped_segment`,
`gh404_optimize_rewrites_a_seekless_legacy_index_into_stock_shape`.

### FTS5 legacy contentless: self-heal a structure poisoned into origin tracking (bd-kon3m residual)

A legacy contentless table (`content=''` without `contentless_delete=1`) whose
structure record was stamped into origin tracking by a pre-fix automerge kept
writing 3-column origin `_docsize` rows into its 2-column shadow and refused to
reopen. Origin tracking is now decided by the declared config, not the
structure byte: on a legacy table the incremental-insert path strips the
spurious origin fields (`Fts5StructureRecord::strip_origin_tracking`) before
appending, so the append writes legacy rows and rewrites a clean structure
record, self-healing the poison. Keeper: `bd_kon3m_legacy_origin_poison.rs`.

### FTS5 contentless index: fail closed instead of blanking on an empty re-encode (bd-dqcf5)

`persist_rootpage_zero_fts5_shadow_rows` (the fallback for a contentless DELETE
whose row predates origin tracking) now refuses to write an empty index over a
contentless table that still holds live rows, aborting the statement with a
typed error rather than silently losing the corpus. Guarded at the ext-fts5
layer by `test_fts5_contentless_hydrated_table_reencodes_nonempty_index`.

### macOS `clippy -D warnings` gate restored (bd-0v03x)

A batch of Linux-only process-liveness code left several items dead on macOS
(`PID_BIRTH_PROCFS_TAG` in fsqlite-mvcc, `use_path` and
`MAX_RESIDENT_GROWTH_BYTES` in fsqlite-vfs / fsqlite-e2e), and a `pub(crate)`
function in a private module tripped `redundant_pub_crate` on every platform,
so `cargo clippy --workspace --all-targets -- -D warnings` (the AGENTS.md
canonical gate) failed. Each item is now `#[cfg]`-gated to its actual use or
made `pub`; zero behavior change on any platform.

### Note

`Cargo.lock` was regenerated for the 0.3.15 workspace (a mid-flight bump had
left it pinned to 0.3.14, which would have failed `--locked`).

---

## [0.3.14] -- 2026-09-01 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.13...v0.3.14>

### FTS5 stock-compatibility: fsqlite-written indexes verify and search under stock SQLite (GH#404)

Two writer divergences made every multi-leaf-page FTS5 index written by
fsqlite fail stock SQLite's `PRAGMA integrity_check` ("malformed inverted
index") while stock `MATCH` silently returned 0 rows for any term beyond leaf
page 1 (`6fd828248`, `c6e5528f5`, `04655d158`):

- The segment-leaf header's first-rowid offset was set on every page (stock
  sets it only on doclist-continuation pages, which fsqlite never writes), so
  stock's segment iterator misparsed every page ≥ 2 as a doclist continuation.
  Written leaves now carry 0; stock-written continuation pages still decode.
- The `%_idx` seek shadow was never populated, so stock's term seek never left
  page 1. All four segment producers (initial flush, incremental contentless
  append, automerge output, full re-encode) now emit stock-shaped rows —
  page 1 keyed by the empty term, later leaves by their first term, `pgno`
  stored encoded `(page << 1) | dlidx_flag` — and the merge / `delete-all`
  paths swap or clear them with the segments they drop.

Keeper: `crates/fsqlite-core/tests/gh404_fts5_stock_compat.rs` (multi-page
corpus in the GH#404 shape; stock `integrity_check`, stock MATCH parity on
page>1 terms, `fts5vocab` enumeration, `%_idx` shape, `delete-all` clearing,
fsqlite reopen parity; demonstrated red against the pre-fix writer).

**Migration:** existing fsqlite-written FTS5 indexes stay fsqlite-readable but
need `INSERT INTO t(t) VALUES('rebuild')` (or `'optimize'`) after upgrading to
become stock-verifiable/searchable.

### FTS5: legacy contentless automerge no longer poisons the structure into origin tracking (bd-kon3m)

The automerge stamped origin tracking onto its merged segment even for a
legacy structure (`content=''` without `contentless_delete=1`); after the
first merge, incremental inserts appended 3-column `_docsize` rows into the
legacy 2-column shadow and the table refused to reopen. The merged segment now
inherits the structure's tracking mode (`6dad0e026`). A legacy table already
poisoned on disk by an earlier build keeps the condition (bd-kon3m tracks the
sanitizer decision); the resurrected
`fts5_contentless_incremental_persist` test is the keeper.

### WAL checkpoints resume from a generation-tagged backfill watermark (GH#402)

File-backed autocommit cost grew super-linearly with schema size because every
post-commit checkpoint re-read the whole WAL from frame 0 and the
autocheckpoint trigger keyed on raw WAL length. Checkpoints now resume from an
`nBackfill`-style watermark tagged with the WAL generation identity (any
reset — ours or a peer's — invalidates it), and the trigger keys on the
unbackfilled estimate (`714dd288d`, keepers in `82bdbecca`).

### `PRAGMA wal_checkpoint` reports cumulative nBackfill (bd-i5j5b)

The watermark change had regressed the pragma's third column to this-call
frames (0 after a resumed no-op pass); stock reports the cumulative
`nBackfill` for the current WAL (`wal.c:4378`), which is what the GH#399
keepers encode — they caught it. Restored in `5fb4e6d62`; durability was
never affected.

### Keyset stream lane hardening (GH#386)

Granular refusal reasons for the keyset stream lane, WITHOUT ROWID and N-vs-2N
scaling tests, and a scan-SQL fallback so a plain named inner table that is
neither memdb- nor pager-scannable during an active transaction scope is
served through normal dispatch instead of refusing the lane (`043fc79c7`,
`f4b819e4e`, `65bf55ef4`).

### Verification note

The full GH#399 corruption battery — the three beads-shaped concurrent-process
churn discriminators (no / PASSIVE / TRUNCATE close checkpoints), cross-process
cache-rebuild churn, the single-process overflow-UPDATE repro, and both
reader-horizon keepers — passed on macOS (Apple Silicon) at this release's
tree state, with stock SQLite `integrity_check` as the oracle after every
command. Previous keeper verification was Linux-only.

---

## [0.3.13] -- 2026-08-29 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.12...v0.3.13>

Single-fix follow-up to the v0.3.12 cross-process WAL wave (GH#399).
`register_wal_reader_slot` took the previous window's reader slot out of the
pager state and released it with `?`, so a failed `-shm` UNLOCK lost the slot
id while the SHARED claim (and its fcntl byte) persisted, leaking one of the
process's few reader slots. The slot id is now restored on a failed release so
the next window's registration retires it (b1c4609d9).

---

## [0.3.12] -- 2026-08-29 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.11...v0.3.12>

### Cross-process WAL reader registration and checkpoint horizon gating (GH#399)

The beads-shaped multi-process churn (short-lived single-connection
processes, one closing with `PRAGMA wal_checkpoint(TRUNCATE)`) corrupted a
shared database in its first round: the on-disk pager never published its
pinned WAL read snapshot to the shared `aReadMark` / `WAL_READ_LOCK(i)`
table, so a peer's checkpoint ran with no reader horizon, backfilled frames a
peer still served, and reset the WAL generation underneath a pinned snapshot
(zero-page reads, page aliases, stock "2nd reference to page N").

- C SQLite's reader protocol is now implemented on the Unix path
  (55c186682): `VfsFile` gains reader-slot acquire/release, the backfill gate
  (`WAL_READ_LOCK(0)`), the `mxSafeFrame` horizon walk and the exclusive
  reset gate (`WAL_READ_LOCK(1..)`), implemented by `UnixFile` on the mmap'd
  `-shm`; the pager registers one slot per active-transaction window and
  releases it at the last exit; `SimplePager::checkpoint` passes the sampled
  peer horizon as `oldest_reader_frame`; RESTART/TRUNCATE hold the reset
  gate and defer the reset while a peer pins the generation, which `PRAGMA
  wal_checkpoint` reports as `busy = 1`.
- The residual corruption was a generation-boundary fold, not the reset race
  (5946b3b7c): after a peer's legitimately ungated reset, a pager's "no
  committed WAL frame => provable hole" test for its abandoned-page pool ran
  against the fresh, empty generation and reclaimed live tree pages into the
  durable freelist. A refresh that observes a WAL generation change now drops
  its in-range pool the way the checkpointing pager already did, drop-time
  transaction exit releases the window's reader slot, and a peer-held
  checkpoint reports busy instead of failing.
- Keepers in `crates/fsqlite-core/tests/bulk_import_upsert_text_pk_phantom_pk.rs`
  (00b270651, 013b3b52d, b670a5570, 9b573e2f6): the three concurrent-process
  churn discriminators (peers closing without / with PASSIVE / with TRUNCATE
  checkpoints), cross-process cache-rebuild churn, the single-process
  overflow-UPDATE repro from bd-5wrwi, and two reader-horizon keepers (a
  stock-shaped slot reader clamps PASSIVE/TRUNCATE and blocks the reset until
  released; a tip-pinned slot exercises the reset gate alone), with stock
  `PRAGMA integrity_check` as the oracle after every command.

### Isolation policy is snapped at BEGIN CONCURRENT (GH#390)

`PRAGMA fsqlite.serializable` no longer retroactively changes how an
already-open concurrent transaction validates at commit: the policy is copied
onto the registry handle at BEGIN and fixed for that session. ON (default)
keeps full Page-SSI validation; OFF routes commit through first-committer-wins
only (base drift still aborts as `BusySnapshot`; the schema-staleness checks
stay). Recycled handles rewrite the flag so a pooled session cannot inherit a
previous transaction's policy (195f4b3d5, 8709600bc).

### Bounded prefix projection of overflow TEXT/BLOB (GH#400)

`substr(col, 1, N)` and byte-prefix projections hydrated the column's full
payload, including every overflow page, before slicing, and fell back to the
unbounded path for any non-ASCII TEXT. The record header already names the
byte length, so TEXT now reads at most `4*N + 8` bytes (whole UTF-8 code
points plus a trimmed partial trailing char) and BLOB only the first `N`
bytes, with results byte-identical to the generic path and to stock SQLite
(33b5035b4, d54b4f6b8; keeper `column_substr_prefix_bound.rs`).

### Session extension: indirect flags and typed format errors (GH#394)

`fsqlite-ext-session` discarded SQLite Session's per-change indirect byte on
decode -> encode. `ChangesetRow.indirect` is now decoded, written back
byte-for-byte and preserved through `invert()`; malformed bytes fail with a
typed `ChangesetFormatError`; `Changeset.kind` tags changeset vs patchset so
`invert()` rejects patchsets and `concat()` rejects mixed kinds, matching
`sqlite3changeset_invert()` / `sqlite3changeset_concat()`;
`Session::set_indirect()` mirrors `sqlite3session_indirect()` (86225c360;
byte fixtures captured from the sqlite3 3.46.1 shell).

### Other fixes

- FTS5 `INSERT INTO t(t) VALUES('integrity-check')` is admitted through
  read-only pagers and under `PRAGMA query_only`; every other FTS5
  maintenance command and ordinary write still fails `ReadOnly` (22af07533,
  027f62f22).
- Private-database cleanup (GH#388 / GH#393) emits structured
  `PrivateDatabaseCleanupOutcome` receipts with preflight verification, and
  VACUUM preserves them (d46336552, 591441915, d1991ca83).
- `PRAGMA schema_advisor` declares its exact 4-column metadata in
  `pragma_result_columns` (01a8526ad); the Unix VFS uses `access(2)` for the
  POSIX mode check and falls back on a promoted read-only open (b79628cb0).
- Beads-shaped bulk-import parity tests cover overflow and relation pages
  (7d85e59bd, 9fa068c79).

---

## [0.3.11] -- 2026-08-27 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.10...v0.3.11>

This patch release fixes a CHECK-constraint enforcement hole in freshly
created schemas. Parser expression spans intentionally omit grouping
parentheses, but the direct CREATE and ALTER paths previously reused those
spans as verbatim SQL. For a compound expression such as
`CHECK((a) AND (b))`, that produced the malformed fragment `a) AND (b`; VDBE
code generation then skipped the unparsable constraint, so it was not enforced
until the database was reopened and the schema was canonicalized.

CHECK source now keeps its verbatim spelling only when it reparses to the same
AST, otherwise it uses the AST's canonical rendering. VDBE code generation
also fails closed if an invalid stored CHECK ever reaches it. Regression tests
cover the exact downstream `beads_rust` status/closure invariant on a live and
reopened file database, compound CHECKs introduced by `ALTER TABLE ADD
COLUMN`, and malformed internal CHECK metadata.

---

## [0.3.10] -- 2026-08-26 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.9...v0.3.10>

This patch release is headed by two downstream-blocking schema fixes. CREATE
statements with leading comments now retain the original source coordinate
system while CHECK/default expressions are extracted, so every constraint is
enforced instead of silently slicing unrelated SQL. Explicit
`ALTER TABLE main.<name> RENAME TO ...` also works when a same-named TEMP table
shadows the persistent table, while preserving the TEMP binding and storing
canonical, unqualified DDL that stock SQLite accepts on reopen.

Other changes since v0.3.9 include:

- checkpoint and recovery hardening, including exclusive maintenance leases,
  commit-horizon preservation, multi-section journal recovery, and per-record
  parallel-WAL CRC32C validation;
- pager and B-tree safety work for reclamation, abandoned-page ledgers,
  allocator exhaustion, export/copy fencing, and quotient-filter fail-safe
  behavior;
- concurrent transaction lifecycle enforcement, commit-phase liveness fixes,
  and decomposed validation/I/O/publication observability;
- typed verified-backup and compaction APIs, plus process-wide metrics
  autostart and expanded transaction, conflict, lock, and WAL telemetry;
- planner and VDBE correctness for WITHOUT ROWID lookups, correlated EXISTS,
  composite ordering, triggers, and multi-clause UPSERT routing.

Release evidence for the schema fixes includes stock-SQLite differential
checks, canonical reopen plus `PRAGMA integrity_check`, the exact downstream
`beads_rust` reproducers, the full downstream library suite, and clean
workspace check/clippy gates. Two exhaustive workspace-test attempts were
terminated by remote infrastructure during test-binary linking (one full
volume and one external SIGTERM); neither emitted a source or test failure.

---

## [0.3.9] -- 2026-08-23 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.8...v0.3.9>

68 non-merge commits in the day after the [v0.3.8](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.8) tag. An **expedited** patch release with two ship-stopping reasons:

1. **The v0.3.8 prebuilt CLI binaries cannot open file-backed databases at all** (bd-slgya): every file-DB open fails with `not implemented: file-backed pager not available on this platform` on all five release platforms. crates.io libraries were unaffected. See "Release-artifact integrity" below.
2. The Windows read-only-open regression: fsqlite 0.3.7/0.3.8 error on **every read-only open+close on Windows**, which made downstream `beads_rust` (`br`) v0.4.0 fail on all Windows machines before its first command completed ([beads_rust#438](https://github.com/Dicklesworthstone/beads_rust/issues/438)).

### Release-artifact integrity (bd-slgya)

Delivered capability: the shipped shell and C API open file databases again, and the gap class is guarded at three layers.

The 0.3.8 dep-pin hardening ([`3444c9a34`](https://github.com/Dicklesworthstone/frankensqlite/commit/3444c9a34)) set `default-features = false` on the workspace `fsqlite`/`fsqlite-core` specs. Leaf consumers relying on facade defaults (fsqlite-cli, fsqlite-c-api, fsqlite-harness; fsqlite-e2e transitively) silently lost `fsqlite-core/native`, compiling out the file-backed pager. Workspace-unified builds masked it (fsqlite-wasm's host-target dep re-enables the feature via unification); only solo `-p` builds — exactly what the dsr release build runs — exposed it, and the installer canary checked only `--version`.

- Facade `native`/`linux-asupersync-uring` + extension features restored explicitly at all three leaf manifests. ([`63742e50f`](https://github.com/Dicklesworthstone/frankensqlite/commit/63742e50f))
- `async-api` now implies fsqlite's own `native` feature, with a `compile_error!` tripwire in `fsqlite/src/lib.rs` rejecting any future `async-api`-without-`native` combination (GH#379). ([`b7807f90e`](https://github.com/Dicklesworthstone/frankensqlite/commit/b7807f90e))
- Regression guards that run in solo `-p` builds: `test_file_backed_database_roundtrip` (fsqlite-cli) and `test_open_named_file_database_roundtrip` (fsqlite-c-api); the release gate now runs both crates' suites solo, and the release smoke includes a file-DB roundtrip.

### Windows read-only open regression (GH#438 downstream)

Delivered capability: read-only opens work on Windows again; advisory-lock sidecars are created only when a lock is actually taken.

The 0.3.7 change that made Windows read-only opens sidecar-less (`os_locks: None`) missed four `os_locks` consumers, so the close-path lock assertion `ordinary_locks_are_exactly_at` errored on every RO open+close cycle — reproduced on a real Windows host as `internal error: windows lock files are closed` from even `br init`. Fixes, verified on real Windows hardware by rebuilding `br` against the patched tree and re-running the failing commands:

- Sidecar-less (`None`) lock surfaces are treated as vacuously at-NONE by the exactly-at check and the restore-to-NONE path; `close` never materializes sidecars just to assert about them. ([`7ed48fee1`](https://github.com/Dicklesworthstone/frankensqlite/commit/7ed48fee1), [`2fda33ab0`](https://github.com/Dicklesworthstone/frankensqlite/commit/2fda33ab0))
- `lock()` and both external fences materialize the advisory-lock sidecars on demand; `check_reserved_lock` uses a transient read-only probe. ([`7ed48fee1`](https://github.com/Dicklesworthstone/frankensqlite/commit/7ed48fee1))
- Regression keeper: reserved-probe and open+close-cycle tests for sidecar-less RO bindings — the behavioral test the original change deferred. ([`ed5fbb343`](https://github.com/Dicklesworthstone/frankensqlite/commit/ed5fbb343))
- Windows-target clippy is green again (`&raw mut` for `GetProcessTimes` out-pointers). ([`a9fac172c`](https://github.com/Dicklesworthstone/frankensqlite/commit/a9fac172c))
- **Windows `VACUUM INTO` regression (GH#376, 0.3.4 → 0.3.7)**: the reserved-bootstrap lock-sidecar witness was captured after the VFS open that creates our own advisory sidecars, so `AllowExpected` mistook them for a foreign reservation and rejected the fresh target with `CannotOpen` before writing anything. The witness is now snapshotted before the open; Linux is unchanged (empty snapshot off-Windows). Cross-checked for `x86_64-pc-windows-gnu`; keeper `bd_dd24l_vacuum_into_reserved_target_gh376`; on-Windows behavioral confirmation still tracked on bd-dd24l. ([`dc34e7f1d`](https://github.com/Dicklesworthstone/frankensqlite/commit/dc34e7f1d))

### Downstream build repairs

- `fsqlite-harness` compiles again: it lost `fsqlite_vfs::host_fs` when those helpers moved behind the `native` feature; the harness now enables the feature on its own dependency edge (GH#378). ([`2fa816433`](https://github.com/Dicklesworthstone/frankensqlite/commit/2fa816433))
- `json()` JSON5 canonicalization no longer round-trips through serde: a lexical JSON5→JSON translator preserves numeric text verbatim. ([`bda321183`](https://github.com/Dicklesworthstone/frankensqlite/commit/bda321183))
- Broken-HEAD window repaired: a docs-only edit had swallowed a function signature in `fsqlite-core`. ([`ba35bb558`](https://github.com/Dicklesworthstone/frankensqlite/commit/ba35bb558), [`ff4c51cde`](https://github.com/Dicklesworthstone/frankensqlite/commit/ff4c51cde) for the earlier same-day WAL re-export miss)

### SQL correctness & stock-parity continuation

- UPSERT `DO UPDATE SET` evaluates correlated subqueries, `excluded.*` references, and CASE bare columns correctly. ([`bcaad2502`](https://github.com/Dicklesworthstone/frankensqlite/commit/bcaad2502))
- UPSERT with multiple `ON CONFLICT` clauses routes each clause to its own conflict target — on rowid tables and on WITHOUT ROWID tables. ([`9847fb9d1`](https://github.com/Dicklesworthstone/frankensqlite/commit/9847fb9d1), [`900cfc283`](https://github.com/Dicklesworthstone/frankensqlite/commit/900cfc283))
- A conflicting `ON CONFLICT DO UPDATE` fires BEFORE/AFTER **UPDATE** triggers (and suppresses AFTER INSERT), matching stock's resolved-action trigger semantics. ([`da07da1bd`](https://github.com/Dicklesworthstone/frankensqlite/commit/da07da1bd))
- Row-value against a scalar subquery — `(a,b) = (SELECT x,y ...)` — compares the subquery's row value instead of never matching. ([`cde3a8e82`](https://github.com/Dicklesworthstone/frankensqlite/commit/cde3a8e82))
- FTS5 `rank MATCH 'bm25(...)'` / `rank = '<ranking>'` custom-ranking assignment re-ranks instead of returning empty. ([`0c202552a`](https://github.com/Dicklesworthstone/frankensqlite/commit/0c202552a))
- INTEGER PRIMARY KEY / rowid collisions report stock's `UNIQUE constraint failed: t.k` instead of an internal VDBE-halt message; `?0`/out-of-range placeholders report stock's bare `variable number must be between ?1 and ?32766`. ([`a5bd1d395`](https://github.com/Dicklesworthstone/frankensqlite/commit/a5bd1d395), [`4e1592bae`](https://github.com/Dicklesworthstone/frankensqlite/commit/4e1592bae))
- Three-part `schema.table.column` references resolve end to end. ([`b00c66feb`](https://github.com/Dicklesworthstone/frankensqlite/commit/b00c66feb))
- Bounded integrity validation covers WITHOUT ROWID foreign keys (GH#341 part 3). ([`257da1f12`](https://github.com/Dicklesworthstone/frankensqlite/commit/257da1f12))
- JOIN projections emit `Rowid` for integer-primary-key columns; shadowed-`main` resolution splits correctly when a `temp` table shares the name; attached-target UPDATEs materialize cross-schema subqueries; lateral `json_each` oracle tests activated. ([`0bbe20448`](https://github.com/Dicklesworthstone/frankensqlite/commit/0bbe20448), [`06d4f90c2`](https://github.com/Dicklesworthstone/frankensqlite/commit/06d4f90c2), [`194989b25`](https://github.com/Dicklesworthstone/frankensqlite/commit/194989b25), [`1cf28a433`](https://github.com/Dicklesworthstone/frankensqlite/commit/1cf28a433))
- Error-message parity: parser expected/unexpected and row-value arity errors match stock `near "X"` / verbatim text; unexpected characters and bare parameter prefixes report `unrecognized token`; aggregate/window misuse validation runs on all prepared SELECT fast lanes; trigger-body DML restrictions match stock near-X/verbatim; `CREATE INDEX`/`VIEW`/`TRIGGER` with a reserved `sqlite_` name is rejected with stock text. ([`901a30eaa`](https://github.com/Dicklesworthstone/frankensqlite/commit/901a30eaa), [`f99897c55`](https://github.com/Dicklesworthstone/frankensqlite/commit/f99897c55), [`555bb131c`](https://github.com/Dicklesworthstone/frankensqlite/commit/555bb131c), [`3b6e1fb31`](https://github.com/Dicklesworthstone/frankensqlite/commit/3b6e1fb31), [`6bfba935b`](https://github.com/Dicklesworthstone/frankensqlite/commit/6bfba935b), [`10cb68269`](https://github.com/Dicklesworthstone/frankensqlite/commit/10cb68269))

### C API & async surface

- `sqlite3_bind_*` parameter surface added to the C API shim (int/int64/double/text/blob/null + parameter-count/name/index introspection), closing the prepare/step/column-only gap. ([`ba0c829fd`](https://github.com/Dicklesworthstone/frankensqlite/commit/ba0c829fd))
- Session extension exposes an async-api surface behind the feature flag. ([`b7807f90e`](https://github.com/Dicklesworthstone/frankensqlite/commit/b7807f90e))

### Performance & observability

- Trigger `WHEN` correlated-subquery guards no longer pay an unamortized linear `EXISTS` probe on large children (gated to small children; the planner index-seek remainder of GH#377 stays tracked), and multi-conjunct `IndexEquality` directives lower via labeled conjunct extraction. ([`778d96e24`](https://github.com/Dicklesworthstone/frankensqlite/commit/778d96e24), [`dd210b763`](https://github.com/Dicklesworthstone/frankensqlite/commit/dd210b763))
- The `/metrics` serve loop has a write timeout, so a stalled scraper cannot wedge the exporter. ([`2b6ead826`](https://github.com/Dicklesworthstone/frankensqlite/commit/2b6ead826))

---

## [0.3.8] -- 2026-08-22 (GitHub Release)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.7...v0.3.8>

419 non-merge commits after the [v0.3.7](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.7) tag (2026-08-20 → 2026-08-22), tag [`2cb6f69ba`](https://github.com/Dicklesworthstone/frankensqlite/commit/2cb6f69bae665bdc7f5fe66be5be2aa95e4fbc85). The window is dominated by a stock-SQLite conformance campaign driven by differential oracle probing (frank CLI/API vs bundled rusqlite and the sqlite3 CLI), plus a cluster of data-integrity fixes. 54 tracker workstreams closed in the window (see `.beads/issues.jsonl`); ~120 of the commits are `fix`/`feat`. Release gate: fsqlite-core `--lib` 3783/0, fsqlite `--lib --features async-api` 737/0, `clippy --workspace --all-targets -D warnings` clean, all in a pristine worktree at the tag SHA. This release also adds `SHA256SUMS.minisig` to the published assets (and backfills one for v0.3.7): the installer requires a minisign signature whenever the client has `minisign` installed, so unsigned releases hard-failed there.

### Data integrity & transaction semantics

Silent-corruption and atomicity bugs found by depth probes, all keeper-guarded.

Delivered capability: UPSERT, multi-row INSERT, and DML-subquery paths now fail loudly and atomically instead of losing rows.

- **UPSERT `DO UPDATE` no longer clobbers a colliding row** (P1): rewriting the conflict-target key onto a *different* existing row now aborts with `UNIQUE constraint failed` instead of silently REPLACE-overwriting that row.
- **INSERT `ABORT` statement-level atomicity**: a constraint failure on a later row of a multi-row INSERT now rolls back exactly that statement's rows (previously behaved like `OR FAIL` under a retained autocommit batch); `OR FAIL` still preserves.
- UPSERT can target an explicit secondary-UNIQUE index on WITHOUT ROWID tables; correlated `IN` subqueries work in UPDATE/DELETE WHERE via a two-phase rowid rewrite; RETURNING bind parameters resolve before INSERT..SELECT per-row replay; savepoints fan out to attached-schema child transactions.
- ATTACH of an empty aux to a UTF-16 main stays 0-byte/attachable until its first write, which stamps the header encoding *before* the first row-encode (the ordering that corrupted a prior attempt); namespace-binding sidecar fds quiesce on connection close/drop (lsof-clean e2e); VACUUM's dirty-page-eviction writeback child preserves the caller's cancellation mask.

Closed workstreams: bd-sque1 (P1), bd-01qa9, bd-yqjjx, bd-t7m7y, bd-qahvh, bd-lzbku, bd-9hp58, bd-twmyh, bd-sw2k5.

Representative commits: [`8fc5edf69`](https://github.com/Dicklesworthstone/frankensqlite/commit/8fc5edf69) (UPSERT clobber), [`f8740c3ae`](https://github.com/Dicklesworthstone/frankensqlite/commit/f8740c3ae) (ABORT atomicity guard un-ignored), [`ff995ca7a`](https://github.com/Dicklesworthstone/frankensqlite/commit/ff995ca7a) (WITHOUT ROWID upsert target), [`c4f95b28c`](https://github.com/Dicklesworthstone/frankensqlite/commit/c4f95b28c) (correlated IN in DML), [`a0706385d`](https://github.com/Dicklesworthstone/frankensqlite/commit/a0706385d) (ATTACH encoding), [`725e83651`](https://github.com/Dicklesworthstone/frankensqlite/commit/725e83651) (fd quiesce), [`6fde1c346`](https://github.com/Dicklesworthstone/frankensqlite/commit/6fde1c346) (mask-preserving eviction child).

### Stock-error-message parity campaign

Delivered capability: dozens of error paths emit byte-identical stock SQLite messages *and* the stock primary result code, and the checks fire at prepare time on both `query()` and the `execute()` VDBE fast-path (which previously bypassed them).

- Message families converted to verbatim stock text: no-such-table/column (schema-qualified where stock qualifies), ALTER/DROP/CREATE conflicts, ATTACH/DETACH, VACUUM, CHECK (named + unnamed), IPK duplicates, value-count mismatches (INSERT, INSERT..SELECT, ragged VALUES), aggregate/window misuse (WHERE/HAVING/RETURNING/bare-OVER/nested), transaction/savepoint control, reserved `sqlite_` names, unknown COLLATE, unterminated/malformed literals, "row value misused", `count(DISTINCT a,b)` arg-count.
- A new `FrankenError::DatatypeMismatch` carries stock's bare `datatype mismatch` under SQLITE_MISMATCH (code 20) for non-integral rowid/IPK inserts and non-integer LIMIT/OFFSET — message *and* code parity.
- The prepare-time validation bypass on the `execute()` fast lane was closed in three slices (aggregate/window misuse, UPSERT conflict-target, window OVER column resolution).
- The `" in <SQL> at offset N"` suffix experiment was reverted after oracle evidence showed it is not stock behavior.

Closed workstreams: bd-ttof2 (+ its offset-suffix regression bead), bd-errmsg-parity batches 1–4, bd-prepare-time-validation-bypass, bd-2phpu (DQS retry regression repair).

Representative commits: [`287f0a3ac`](https://github.com/Dicklesworthstone/frankensqlite/commit/287f0a3ac) (txn/savepoint), [`ad868aab9`](https://github.com/Dicklesworthstone/frankensqlite/commit/ad868aab9) (DDL conflicts), [`a0e51891e`](https://github.com/Dicklesworthstone/frankensqlite/commit/a0e51891e) (ragged VALUES), [`8bc1a70de`](https://github.com/Dicklesworthstone/frankensqlite/commit/8bc1a70de) (DatatypeMismatch variant), [`023cb4932`](https://github.com/Dicklesworthstone/frankensqlite/commit/023cb4932) / [`d010b1c69`](https://github.com/Dicklesworthstone/frankensqlite/commit/d010b1c69) / [`781cfb569`](https://github.com/Dicklesworthstone/frankensqlite/commit/781cfb569) (fast-path validation slices), [`8aea420be`](https://github.com/Dicklesworthstone/frankensqlite/commit/8aea420be) (lexer token errors), [`42e23be3b`](https://github.com/Dicklesworthstone/frankensqlite/commit/42e23be3b) (offset-suffix revert).

### JSON1 / JSON5

Delivered capability: JSON5 input everywhere stock 3.42+ accepts it, `json_group_*` aggregates, and a downstream-breaking viral cargo feature removed.

- `json()` and every scalar + table-valued JSON function accept JSON5 (unquoted keys, single quotes, trailing commas, comments, hex) and canonicalize to standard JSON.
- `json_group_array`/`json_group_object` registered as true aggregates with JSON-subtype preservation on both the VDBE and interpreted paths; `jsonb_extract` returns SQL scalars for scalar results and JSONB for containers; `json_remove`/`json_patch` preserve object key order; `json_tree` path-rooted scans report stock's root key.
- **Downstream fix (GH#375)**: `fsqlite-ext-json` no longer enables serde_json's `arbitrary_precision` — a viral feature that broke internally-tagged enums with floats in *every* downstream build since 0.3.5; GH#212 non-finite JSON numbers stay preserved without it.

Closed workstreams: bd-qear2 (3 increments), bd-76x57, bd-x473y, bd-elre4, bd-kzwze, bd-uxmhz (P1), bd-yalqc.

Representative commits: [`f23788e0d`](https://github.com/Dicklesworthstone/frankensqlite/commit/f23788e0d) / [`68a90d825`](https://github.com/Dicklesworthstone/frankensqlite/commit/68a90d825) (JSON5), [`69e4fa9ca`](https://github.com/Dicklesworthstone/frankensqlite/commit/69e4fa9ca) + [`e1823f046`](https://github.com/Dicklesworthstone/frankensqlite/commit/e1823f046) (json_group aggregates + subtype), [`2394a429c`](https://github.com/Dicklesworthstone/frankensqlite/commit/2394a429c) (arbitrary_precision), [`803267576`](https://github.com/Dicklesworthstone/frankensqlite/commit/803267576) (jsonb_extract).

### Scalar, datetime, and printf parity

Delivered capability: the scalar-function surface matches stock's C truncation/rendering semantics on edge inputs.

- `strftime`: `%U` implemented, `%J` renders as C `%.16g`, unknown specifiers return NULL; `date()`/`datetime()` render negative years as stock does and NULL past year 9999 / below JD 0; fractional month/year modifiers apply as calendar-integer plus flat days.
- `printf`: dynamic `*` width/precision use C's `va_arg(int)` cast semantics with the 1e9 limit, INT_MAX-overflow width parity, huge `%.<N>f` precision no longer panics, incomplete conversions return accumulated output, unsupported/positional specifiers NULL (with `%p`/`%r` added).
- `substr()`/`round()` i32-truncate their integer args like `sqlite3_value_int`; `quote()` renders infinity as re-parseable `9.0e+999`; GLOB reversed char-class ranges match their literal lower bound.

Closed workstreams: bd-zv4ra, bd-565ji, bd-13mqo, bd-0jlvk, bd-wzq8r, bd-mcgdb, bd-bcqpe, the printf edge-family beads, bd-e7i9d (FILTER error text).

Representative commits: [`0328347b4`](https://github.com/Dicklesworthstone/frankensqlite/commit/0328347b4) (%U), [`71a7d8682`](https://github.com/Dicklesworthstone/frankensqlite/commit/71a7d8682) (negative years), [`56253f6ab`](https://github.com/Dicklesworthstone/frankensqlite/commit/56253f6ab) (printf dynamic width), [`e98d82f43`](https://github.com/Dicklesworthstone/frankensqlite/commit/e98d82f43) (precision panic), [`e63e5ae8c`](https://github.com/Dicklesworthstone/frankensqlite/commit/e63e5ae8c) (substr i32).

### Extensions, CLI, and introspection

Delivered capability: R-Tree/FTS5 gaps closed, the CLI ships batteries-included, and PRAGMA introspection matches stock on views/indexes/collations.

- R-Tree: auxiliary columns (`rtree(id,x0,x1,+aux TYPE)`) and per-dimension `min<=max` enforcement with stock's constraint message; FTS5 `rank` usable in WHERE predicates.
- Parser/grammar: recursive CTE without the `RECURSIVE` keyword; bare-identifier `DEFAULT` is a string literal (stock's DQS-adjacent quirk); DEFAULT on generated columns and all-generated CREATE TABLE rejected at create time.
- CLI ships the json/fts5/rtree/icu/misc extensions by default; the installer fetches `SHA256SUMS` under its released name.
- PRAGMA parity: `table_info` on views (columns + decltypes resolved across joins/subqueries/nested views), verbatim parenthesized `dflt_value`, `index_list` `partial=1`, COLLATE/expression key terms in `index_info`/`index_xinfo`, table-valued `pragma_collation_list()`, TEMP-shadowed tables still listed in `main.sqlite_master`.
- New observability surface (all opt-in): Prometheus `/metrics` HTTP endpoint on a `std::net` daemon thread, StatsD UDP push transport, `PRAGMA write_amplification`, `PRAGMA gc_stats`, `PRAGMA schema_advisor`.

Closed workstreams: bd-2pibv, bd-hj6ly, bd-ic9nu, bd-b7g1o, bd-dv4xv, bd-0x4aj, bd-5m52m, bd-ivy5h, bd-y4yjq, bd-zywqc.11 (AC#2/AC#6), bd-t6sv2.9/.11/.15.

Representative commits: [`215047bab`](https://github.com/Dicklesworthstone/frankensqlite/commit/215047bab) (rtree aux), [`bafebbdba`](https://github.com/Dicklesworthstone/frankensqlite/commit/bafebbdba) (min<=max), [`04e61f7eb`](https://github.com/Dicklesworthstone/frankensqlite/commit/04e61f7eb) (FTS5 rank), [`d33ab919f`](https://github.com/Dicklesworthstone/frankensqlite/commit/d33ab919f) (CLI extensions), [`d52610a1e`](https://github.com/Dicklesworthstone/frankensqlite/commit/d52610a1e) (view decltypes), [`77bd7882c`](https://github.com/Dicklesworthstone/frankensqlite/commit/77bd7882c) (/metrics), [`54eb9f798`](https://github.com/Dicklesworthstone/frankensqlite/commit/54eb9f798) (StatsD), [`c399a9a10`](https://github.com/Dicklesworthstone/frankensqlite/commit/c399a9a10) (installer).

### Test & release infrastructure

- ~100 new differential oracle keepers vs the bundled rusqlite oracle — scalar/aggregate/window functions, datetime, printf, JSON/jsonb, collation, affinity/CAST, JOIN/set-ops/CTEs, UPSERT, RETURNING, FK actions, generated columns, STRICT, WITHOUT ROWID, triggers (timing, OLD/NEW, RAISE inside CASE), views, TEMP-schema resolution, transaction atomicity — wired into CI as an explicit workflow step ([`59a6efeca`](https://github.com/Dicklesworthstone/frankensqlite/commit/59a6efeca) + 7 follow-ups; takes effect when GitHub Actions is re-enabled).
- Internal workspace dependency pins tightened to exact `workspace = true` versions so downstream consumers cannot resolve a mismatched internal-crate mix ([`3444c9a34`](https://github.com/Dicklesworthstone/frankensqlite/commit/3444c9a34), bd-fsqlite-loose-internal-dep-floors — the downstream E0063 breakage class).
- The two parallel-load `--lib` tests de-flaked ([`22996cc79`](https://github.com/Dicklesworthstone/frankensqlite/commit/22996cc79)).

### Notes for agents

- Evidence for this entry: `git log v0.3.7..v0.3.8` (419 non-merge commits), the closed-in-window records in `.beads/issues.jsonl` (54 workstreams), and the release gate logs. Bead IDs above are grep-able in `.beads/issues.jsonl` for full root-cause narratives.
- Deferred with staged fixes at release time: bd-q2bju (explicit-txn multi-row conflict visibility, verified green but held for the freeze), bd-7lhxi (`/metrics` write timeout), bd-4gqeu (JSON5 lexical translator for duplicate-key/exponent fidelity).

---

## [0.3.7] -- 2026-08-20 (release candidate; tag `v0.3.7` is cut after the release gate)

Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.6...main>

182 non-merge commits after the [v0.3.6](https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.6) tag. The version bump — all 28 crate package versions plus the 27 internal workspace-dependency pins raised 0.3.6 → 0.3.7 with `Cargo.lock` refreshed — landed in [`8cc00ba7cd61141605ec7c27a0ecb92aa5264e22`](https://github.com/Dicklesworthstone/frankensqlite/commit/8cc00ba7cd61141605ec7c27a0ecb92aa5264e22); `cargo check --workspace` is green.

### GitHub issues closed

- **GH#345** — subquery-`WHEN` trigger ingestion no longer re-parses the schema on every write, and lazy per-read memdb hydration removes the O(rows²) reload ([`8f0bc0967c4e83e6cafc944555580084dca07c52`](https://github.com/Dicklesworthstone/frankensqlite/commit/8f0bc0967c4e83e6cafc944555580084dca07c52), [`40c40094090179aece36eeac9e3e75b439b0cdbe`](https://github.com/Dicklesworthstone/frankensqlite/commit/40c40094090179aece36eeac9e3e75b439b0cdbe)).
- **GH#366** — a coherent populated database reopened via `ReservedEmpty` opens instead of being falsely rejected ([`e3b5e68af2af6d692fef3f81bba6ddca9e5a1da5`](https://github.com/Dicklesworthstone/frankensqlite/commit/e3b5e68af2af6d692fef3f81bba6ddca9e5a1da5)).
- **GH#368 / GH#369** — memdb read-amplification pair: in-txn `sqlite_master` scans are counted for regression guarding, and memdb FK-parent hits are trusted only when the mirror is fully hydrated ([`d62a51b183fb6467323f1e89b400841672ac2d35`](https://github.com/Dicklesworthstone/frankensqlite/commit/d62a51b183fb6467323f1e89b400841672ac2d35), [`7bac82eb43cfb43e86455c10062dff12d5c84439`](https://github.com/Dicklesworthstone/frankensqlite/commit/7bac82eb43cfb43e86455c10062dff12d5c84439)).
- **GH#370** — reclaim orphaned `%_content` on legacy contentless FTS5 archives ([`a00e152cced9ab0fcf8005c26e2d6bcea0e94619`](https://github.com/Dicklesworthstone/frankensqlite/commit/a00e152cced9ab0fcf8005c26e2d6bcea0e94619)).
- **GH#371** — bound WITHOUT-ROWID / large-table `DROP` teardown memory, closing a P0 OOM ([`7a3625a7d23edcf9f433225f4545a90e6bbcae8f`](https://github.com/Dicklesworthstone/frankensqlite/commit/7a3625a7d23edcf9f433225f4545a90e6bbcae8f)).
- **GH#244** — attached-schema writes participate in explicit transactions via lazy child-txn enrollment with commit/rollback fan-out ([`f430ce46d0763cbf0581c01d388f468c0057ff8e`](https://github.com/Dicklesworthstone/frankensqlite/commit/f430ce46d0763cbf0581c01d388f468c0057ff8e)).

### Concurrency hardening — bd-xv5cm (7 facets)

- **L1** — a group-commit queue is removed only when the registry is its sole owner ([`0a1ab35b3d581f7cd1b964e79c756587019b1e19`](https://github.com/Dicklesworthstone/frankensqlite/commit/0a1ab35b3d581f7cd1b964e79c756587019b1e19)).
- **L2** — the post-cancel wait for the worker's authoritative outcome is bounded ([`14c7e59879e91300c8b2bf17a1e0f25bc2c3d2ab`](https://github.com/Dicklesworthstone/frankensqlite/commit/14c7e59879e91300c8b2bf17a1e0f25bc2c3d2ab)).
- **L3** — an autocommit `SELECT` that calls a user function is never auto-retried ([`b38b09d9f62728a8a336c8fbb643184c1f95f341`](https://github.com/Dicklesworthstone/frankensqlite/commit/b38b09d9f62728a8a336c8fbb643184c1f95f341)).
- **L4** — BusyRecovery identity is preserved on retry exhaustion (folded into [`f430ce46d0763cbf0581c01d388f468c0057ff8e`](https://github.com/Dicklesworthstone/frankensqlite/commit/f430ce46d0763cbf0581c01d388f468c0057ff8e)).
- **M4** — atomic single-write wal-fec append with a graceful scan over a corrupt tail ([`01c3965e4f1c4cb83b7f73aa1b626e9bb634c505`](https://github.com/Dicklesworthstone/frankensqlite/commit/01c3965e4f1c4cb83b7f73aa1b626e9bb634c505)).
- **M5** — the coordinator-map lock is released before the blocking ticker join ([`ad2e949b31efab42d1e6bb3a96c4775791867512`](https://github.com/Dicklesworthstone/frankensqlite/commit/ad2e949b31efab42d1e6bb3a96c4775791867512)).
- **L5** — chunk-id assertion. Batch closed at [`b0aa79f4b2aee44fd663e712e27816e30a30c48a`](https://github.com/Dicklesworthstone/frankensqlite/commit/b0aa79f4b2aee44fd663e712e27816e30a30c48a).

### Correctness & conformance

- Same-event triggers fire newest-first (LIFO), matching C SQLite ([`a60f6c60602602eab88c43a4c2a0db6c769472ba`](https://github.com/Dicklesworthstone/frankensqlite/commit/a60f6c60602602eab88c43a4c2a0db6c769472ba)).
- `json()` preserves number-literal source text and minifies its input instead of re-serializing through a lossy value round-trip ([`6b1ecba70e746f0884f168628fb81786bb5b119f`](https://github.com/Dicklesworthstone/frankensqlite/commit/6b1ecba70e746f0884f168628fb81786bb5b119f), [`6d5cbe28dbaf7297ead06c0a202965cbbe031b1d`](https://github.com/Dicklesworthstone/frankensqlite/commit/6d5cbe28dbaf7297ead06c0a202965cbbe031b1d)).
- `printf` `%!` alt-form-2 floats emit exact-double cap digits ([`d3cab54d7e2a1b1ccfe67ec4064bec6ae218b525`](https://github.com/Dicklesworthstone/frankensqlite/commit/d3cab54d7e2a1b1ccfe67ec4064bec6ae218b525)); unsigned datetime modifiers are accepted as positive ([`1da56ea84bf658a8353accb9f2f921105757cc1d`](https://github.com/Dicklesworthstone/frankensqlite/commit/1da56ea84bf658a8353accb9f2f921105757cc1d)); the `utc` modifier resolves DST-transition ambiguity via SQLite's iterative solver ([`49437629d18b70b0a10fce65571f56acdd7d1519`](https://github.com/Dicklesworthstone/frankensqlite/commit/49437629d18b70b0a10fce65571f56acdd7d1519)).
- Third-wave fresh-eyes review (REVIEW3): `HAVING` column-reference resolution ([`3875fb93a236aede5c32e78e6d4a86829e970107`](https://github.com/Dicklesworthstone/frankensqlite/commit/3875fb93a236aede5c32e78e6d4a86829e970107)); literal `X AND 0` folding in the VDBE binary-op emitter so dead `CASE WHEN` branches drop ([`9d0a9205b340463215cdb388afcbd84d4b8eb59a`](https://github.com/Dicklesworthstone/frankensqlite/commit/9d0a9205b340463215cdb388afcbd84d4b8eb59a)); external-content FTS5 scans and rebuild read the SOURCE ([`4b8a2504fa0d845d94d5c0b7ca44f05a0f7a3348`](https://github.com/Dicklesworthstone/frankensqlite/commit/4b8a2504fa0d845d94d5c0b7ca44f05a0f7a3348)); dual-recompute index keys under `integrity_check` so CAST-AS-BLOB and surrogate UTF-16 indexes stop false-flagging ([`96ae9d73c1eb5cc664bfd4e5613aa426c3a2bbdb`](https://github.com/Dicklesworthstone/frankensqlite/commit/96ae9d73c1eb5cc664bfd4e5613aa426c3a2bbdb)).
- FTS5 index-shadow reclaim: external-content delete-all keeps rows visible and clears only the index/doclist shadows ([`ee2e3a7ea975a1de5093760d246b8026cd578b8c`](https://github.com/Dicklesworthstone/frankensqlite/commit/ee2e3a7ea975a1de5093760d246b8026cd578b8c)), complementing the orphaned `%_content` reclaim above.
- `CAST(blob AS TEXT/numeric)` relabels bytes via the database text encoding ([`194e97de83c90f483d81e9ced2c1142e87952e0d`](https://github.com/Dicklesworthstone/frankensqlite/commit/194e97de83c90f483d81e9ced2c1142e87952e0d)); INDEXED-BY partial-cover LEFT→INNER strength reduction ([`56f90d8064c3a405f2c26eba4d8987726c0e075a`](https://github.com/Dicklesworthstone/frankensqlite/commit/56f90d8064c3a405f2c26eba4d8987726c0e075a)); ATTACH deferred-encoding reverted to an eager stamp to eliminate an aux-corruption hazard ([`f75649ef87f03bdb71074c15cabf01fbdfacfde0`](https://github.com/Dicklesworthstone/frankensqlite/commit/f75649ef87f03bdb71074c15cabf01fbdfacfde0)).

### Tracker

- Beads: [`.beads/issues.jsonl`](https://github.com/Dicklesworthstone/frankensqlite/blob/main/.beads/issues.jsonl).

---

## [0.3.6] -- 2026-08-19 (GitHub Release)

GitHub Release: <https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.6>
Tag `v0.3.6` at [`b793ce180a8c79089fd8b75e3c44105b71d3c52b`](https://github.com/Dicklesworthstone/frankensqlite/commit/b793ce180a8c79089fd8b75e3c44105b71d3c52b).
Compare: <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.5...v0.3.6>

First GitHub binary release since 0.3.4 (0.3.2-0.3.4 were fast-follow point
releases documented in their GitHub release notes; 0.3.5 was a crates.io-only
snapshot with no GitHub release). Full commit-cited notes are on the v0.3.6
GitHub release; highlights:

### Fixed

- **FTS5 reopen/hydration**: persisted contentless FTS5 segments bind by
  default on schema-only open (GH#358, 8d39b2fec + 6a7b52f00) — large indexed
  corpora no longer come up empty after reopen; doclist stitching across
  leaf-page continuations (GH#360, fd0fed9e8 + c6d0cccf4); ALTER TABLE RENAME
  on FTS5 tables (GH#209, b17cd7551); vtab column-DDL rejected like stock
  (GH#215); fresh-eyes FTS5 defect sweep (external-content delete-all,
  DoclistIndex reclamation, contentless UPDATE rejection, docsize validation).
- **integrity_check false positives**: bounded by header page_count
  (cfb8a3bc4); UTF-16 expression-index false corruption (d17976bd3).
- **Durability/concurrency**: autocommit reads retry transient BUSY
  (GH#367/#335, 0787e6888); torn WAL-header tolerance (GH#292, 653343e4b);
  WAL commit certificate bound to file identity (GH#364, bcae91436); CAS-safe
  rowid-allocator savepoint rewind (GH#147, 21793d0db); SSI edge discovery on
  the direct TransactionManager path (GH#189, c2f5ff8d4).
- **VACUUM**: sorted index keys in the rebuild/VACUUM copy path (GH#347,
  47e8ddf00, shipped in the 0.3.5 crates); VACUUM INTO preserves vtab
  sqlite_master entries (GH#357) and works from read-only sources (GH#342).
- Large SQL/PRAGMA conformance wave: GH#160/#173/#177/#178/#182/#204/#205/
  #206/#207/#212/#213/#216/#224/#227/#236/#241/#242/#245/#246/#250/#251/
  #263/#264/#275/#280 and more — see the GitHub release notes.

### Added

- `fts5vocab` virtual-table module (GH#271, 6cf73719e).
- Non-leading PRIMARY KEY WITHOUT ROWID reads and writes (92a4e4e73).

### Known issues

- GH#366 (fresh open after external DELETEs grow a reconstruct-produced DB's
  freelist) remains open — negative-repro probe landed (4e0ff4beb),
  investigation continues. GH#289 remains documented-open (structure-only).

## [0.3.5] -- 2026-08-17 (Tag / crates.io only — no GitHub Release)

> Git tag `v0.3.5` exists; **no GitHub Release was published.** 72 non-merge
> commits since v0.3.4. Compare:
> <https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.4...v0.3.5>

- Non-leading PRIMARY KEY WITHOUT ROWID reads and writes ([`92a4e4e735483be136b8a73cc6bf3a5d6263dcf8`](https://github.com/Dicklesworthstone/frankensqlite/commit/92a4e4e735483be136b8a73cc6bf3a5d6263dcf8)).
- Sorted index keys in the rebuild/VACUUM copy path (GH#347, [`47e8ddf0082ba2a09e4ba512e3d27691a0946be2`](https://github.com/Dicklesworthstone/frankensqlite/commit/47e8ddf0082ba2a09e4ba512e3d27691a0946be2)).

## [0.3.4] -- 2026-08-16 (GitHub Release)

GitHub Release: <https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.4>
14 non-merge commits since v0.3.3. Fast-follow for GH#353.

- WITHOUT ROWID auto-index dedups appended PK vs index key (GH#353, [`3d3cdda455f6acd7dc076135476812c82e29402f`](https://github.com/Dicklesworthstone/frankensqlite/commit/3d3cdda455f6acd7dc076135476812c82e29402f)).
- Preserve no-hydrator custom vtabs across file-backed schema reload ([`70f107e32ea6358333048cf46e3898e7c879d6ee`](https://github.com/Dicklesworthstone/frankensqlite/commit/70f107e32ea6358333048cf46e3898e7c879d6ee)).
- ORDER BY sort keys decoded in DB storage encoding; NOCASE/RTRIM over UTF-16 match stock ([`0b3e88dccfeabfff24463f3430c4eaca63865a69`](https://github.com/Dicklesworthstone/frankensqlite/commit/0b3e88dccfeabfff24463f3430c4eaca63865a69)).

## [0.3.3] -- 2026-08-16 (GitHub Release)

GitHub Release: <https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.3>
7 non-merge commits since v0.3.2.

- Workspace bump to 0.3.3; asupersync upgraded to 0.4.5 ([`08b58bdd0681492193fda5bfc163184c75b90ed6`](https://github.com/Dicklesworthstone/frankensqlite/commit/08b58bdd0681492193fda5bfc163184c75b90ed6)).
- IN-list `BoundOuterValue` donor affinity/collation honored in table-less IN ([`b700e396739854602d181c855483494cb4733271`](https://github.com/Dicklesworthstone/frankensqlite/commit/b700e396739854602d181c855483494cb4733271)).

## [0.3.2] -- 2026-08-16 (GitHub Release)

GitHub Release: <https://github.com/Dicklesworthstone/frankensqlite/releases/tag/v0.3.2>
539 non-merge commits since v0.3.1. Point-release tag after the post-0.3.1
correctness swarm; detailed notes live on the GitHub Release. Compare:
<https://github.com/Dicklesworthstone/frankensqlite/compare/v0.3.1...v0.3.2>

## [0.3.1] -- 2026-08-14

Post-0.3.0 correctness wave: every entry below cites the landed commit and its
tracking bead; all fixes carry keeper tests and were verified on the release
preflight target.

### Fixed

- **JSONB interop**: `encode_jsonb_value` now emits SQLite-spec JSONB — ASCII
  numeric payloads, direct size nibble, escaped-text elements — so stock
  SQLite reads our JSONB correctly instead of silently decoding garbage
  numbers (5f6602db6, bd-jsonb-numeric-payload-encoding-t75hg; byte-level
  oracle keepers + cross-engine round-trip).
- **PRAGMA data_version**: now bound to pager commit identity — it reflects
  other-connection data commits and no longer false-positives on the
  connection's own DDL (80a29780d, bd-pragma-data-version-approximation-b3dpn).
- **Committed-freelist safety**: append-gate guard refuses committed-freelist
  resurrection/erasure/double-consumption under continuous overlap
  (05144b4f4 + chain, bd-gh302, bd-0shxy).
- **Group-commit waiter livelock**: `wait_for_epoch_outcome_async` no longer
  livelocks when settlement resolves to a terminal error (62dfd9f98,
  bd-keoaf).

- **Schema text fidelity**: stored `CREATE TABLE` text now ends at the
  statement's final token (trailing `;`/comments stripped, `1c75f65fc`) and
  ALTER ADD COLUMN splicing skips comments and `[bracket]` identifiers so
  comment-bearing multi-line CREATEs cannot be corrupted on rewrite
  (`7e043b0f0`; bd-lgolw).
- **Allocator page aliasing**: post-savepoint allocations are quarantined to
  the owning transaction (own-txn reuse first, freelist only at txn end),
  closing the dominant intra-transaction page re-grant corruption path
  (`1f79a3482`; bd-0shxy).
- **Freelist resurrection**: the append gate refuses committed-freelist
  resurrection and erasure, keeping page counts bounded and stock-reader
  bytes stable under continuous overlap and racing writer churn
  (`05144b4f4`; bd-gh302-continuous-overlap-freelist-reuse-i5tx4).
- **Concurrent-writer EOF growth**: EOF high-water is shared and
  peer-claimed growth pages are rejected, eliminating cross-writer page
  aliasing under concurrent commit (`70102790f`; bd-o81ov).
- **Autocommit durability contract**: a successful file-backed autocommit is
  committed before `execute` returns — immediately visible to already-open
  peers and to peers opened only after acknowledgement (`38e903918`;
  bd-792q5).
- **Concurrent-open prepare**: `prepare`'s refresh prologue retries transient
  `Busy`/`BusyRecovery` within the busy-timeout window instead of failing
  fast during a peer's WAL-index recovery, fixing T16 showcase worker setup
  aborts (`55d7fe2c5`; bd-t16-busy-recovery-qzu9p).
- **Read-only open side effects**: read-only namespace admission of a
  database never opened by FrankenSQLite is now sidecar-less — no
  `-fsqlite-ns-gate`/`-ns-use` creation, byte-neutral for the whole file
  family (`a410c2735`; bd-daqmp, GH#140).
- **Darwin file locking**: OFD locks with flock fallback on macOS
  (`6d439dd9c`; bd-3u63s).

### Verified

- **`PRAGMA integrity_check` / `foreign_key_check`**: COLLATE-aware
  partial-index predicate re-evaluation and NULL row locators for
  WITHOUT ROWID children verified against the sqlite3 oracle and locked
  with keepers (`ba50ae51b`; bd-integrity-partial-collate-puctc, bd-y18u1).
- **IN-probe over `ORDER BY`+`LIMIT` subqueries**: the 0.3.0-reported
  `DELETE ... NOT IN (SELECT ... ORDER BY ... LIMIT ?)` halt is not
  reproducible at HEAD (compiled path proven under strict no-fallback);
  contract locked with oracle keepers (`391a445ea`; bd-brzp8).
- **Concurrent-reader consistency**: the reader-deadlock fix that shipped in
  0.3.0 (fail-closed start gate replacing the fallible post-open barrier)
  re-verified at HEAD, 58/58 with zero hangs (bd-concurrent-reader-ff2wv).

---

## [0.3.0] -- 2026-08-13 (Asupersync 0.4.3 compatibility + bug-fix and performance wave)

Lockstep `0.2.1 -> 0.3.0` bump of all 27 workspace members, carrying the
Asupersync `0.4.3` runtime migration **plus a large verified bug-fix wave and
a measured performance restoration** (see **Fixed** and **Performance**
below). Release *infrastructure* also changed: the release workflows and the
release-architecture note were edited to remove the GitHub Actions publish
paths, as described under
**Changed**.

**This is not a behavior-free release.** The async runtime underneath
FrankenSQLite changed. Read **Breaking changes** before upgrading.

> **BREAKING on two independent axes, neither of which is a hand-edited
> FrankenSQLite signature.**
>
> 1. **Type identity.** Every public signature keeps its exact shape, but some
>    of them name Asupersync types, and Asupersync `0.3.x` and `0.4.x` are
>    non-interchangeable. A caller that upgrades only FrankenSQLite gets type
>    errors.
> 2. **Runtime behavior.** Asupersync `0.4.0`-`0.4.3` changed scheduler,
>    lifecycle, cancellation, blocking-driver, stream-context, and
>    poll-panic-containment semantics. Upgrading FrankenSQLite swaps the
>    runtime that executes your database operations.
>
> So moving the Asupersync dependency in the same change is necessary but not
> sufficient: callers must also revalidate cancellation and runtime behavior
> against the new runtime. That is why this is a `0.3.0` family and not a
> `0.2.2` patch.

### Fixed

- **GH#333**: concurrent same-file open + write from multiple threads could
  fail with transient `BusyRecovery`/close-checkpoint errors; autocommit
  boundaries now retry transient recovery states (explicit-transaction
  first-committer-wins semantics unchanged).
- **GH#334**: reopen after file replacement now re-derives per-path
  pager/MVCC registry state from `FileIdentity(dev, inode)`; `VACUUM INTO`
  accepts whole-page trailing slack on VACUUM source receipts only.
- **8-writer rollback cascade**: `BusyRecovery` from ROLLBACK-after-failed-
  INSERT under 8 concurrent writers eliminated via a seven-fix stack
  (transient rollback absorption, Drop-time session reclamation,
  abandoned-epoch escape, busy-budget-scaled settle envelope, cleanup-root
  release ordering, exact-handle relay classification), verified green on two
  hosts with zero aborts in 80/80 formal-gate invocations.
- **Cross-connection query visibility**: a `SELECT` on one connection now
  observes tables committed by another connection after reader open
  (publication refresh reordered ahead of relation validation).
- **Acknowledged-write visibility**: file-backed autocommit writes are
  published before `execute` returns whenever a peer connection is open
  (retained batching now applies only to sole-connection usage).
- **PRAGMA under concurrent open**: transient `BusyRecovery` from
  `PRAGMA journal_mode=WAL` during concurrent connection open is retried
  within the busy-timeout budget (160-attempt keeper, zero escapes).
- **Read fast-lane routing**: five deterministic prepared-statement routing
  regressions fixed (correlated-scalar compile gate, preserialize profiling,
  publication reuse, `VALUES ... ORDER BY` parse parity).
- **SQL conformance**: subquery-family conformance improved from 139/148 to
  147/148 passing (deferred-evaluator comparison affinity, `LIMIT 0`
  short-circuit, `ORDER BY` placeholder ordinals, composite semijoin merge
  ordering, EQP `FullTableScan` verification, `f(*)` parse parity, VALUES
  donor constancy); the remaining case is tracked in `bd-di4he`.
- **Retained-flush checkpoint classification**: transient post-commit
  checkpoint failures no longer convert durable ordinary statements into
  errors.

### Performance

Measured at the release-gate boundary rerun (control `b612eb7b5` vs release
candidate, artifact
`tests/artifacts/perf/rc-boundary-d3448770-20260812T1330Z-superserver`,
2026-08-12, clean-clone canonical release builds, 20 paired ABBA
invocations/arm): FrankenSQLite is faster than bundled C SQLite 3.53.2 in
20 of 21 read cells (F/C 0.08-0.70) and 1.4x faster at 8 concurrent writers
(F/C 0.73), with zero aborted samples. Named residuals: point-read row
materialization (`bd-qcgn2`, partially improved in this release) and a
small-N `SUM+GROUP BY` cell (`bd-z22mq`). The post-async-migration read
regression investigated in `bd-dqdoe` was attributed largely to benchmark
instrument overhead (per-query runtime entry, now amortized in the bench
harness) plus the routing fixes above.

### Breaking changes

- **Asupersync moves from `0.3.10` to `>=0.4.3,<0.5`.** Asupersync 0.4.x
  re-anchors APIs already shipped in 0.3.10 and pins `franken-kernel`,
  `franken-decision`, and `franken-evidence` at `^0.4.3`, so the runtime
  family moves in lockstep. This is what lets FrankenSQLite and FrankenSearch
  share one Asupersync runtime universe in a single dependency graph
  (`bd-asupersync-043-compat-release-dk9ra`).

  *Why it is breaking:* `fsqlite-types` names Asupersync types in its public
  API under the `native` feature -- `Cx::set_native_cx`,
  `Cx::attached_native_cx`, and `Cx::native_spawn_budget` take or return
  `asupersync::Cx` and `asupersync::Budget`. Under Cargo's 0.x rules the
  minor version is the compatibility axis, so `asupersync` 0.3.x and 0.4.x
  are distinct, non-interchangeable types even where their definitions are
  identical. A consumer still on `asupersync = "0.3"` cannot hand its `Cx`
  across to a FrankenSQLite built against 0.4.x.

  *Migration:* move your own `asupersync` dependency to `>=0.4.3,<0.5` in the
  same change that moves `fsqlite` to `0.3`; the two cannot be upgraded
  separately. No FrankenSQLite call site needs a signature edit, but see the
  runtime-behavior entry below -- a clean compile is not sufficient evidence
  that this upgrade is safe for your application. A caller pinning
  `fsqlite = "0.2"` keeps the 0.2.1 API against Asupersync 0.3.10 and is not
  upgraded silently.

- **Asupersync `0.4.0`-`0.4.3` changes runtime behavior under FrankenSQLite.**
  FrankenSQLite does not create its own runtime -- the caller's `Cx` and
  executor drive every async operation -- so these are behavior changes to the
  engine as your application observes it, even though no FrankenSQLite source
  file changed. Per Asupersync's own changelog, the deltas across that range
  include: scheduler and lifecycle repairs (timed-task promotion on injected
  ready work, worker-spawn failure completing affected tasks, deferred regions
  draining before leak diagnostics, causal ordering of spawn effects);
  cancellation-safe synchronization fixes (waiter-ID wraparound, rejected
  repolls of completed MPSC reservations, restored interrupted semaphore
  acquisitions, preserved `RwLock` queue order, linearized `OnceCell` waiter
  state, mutex rank released before wakeup); I/O boundary changes (buffered
  seeks accounting for unread buffered data, bounded backpressure on framed
  writes); capability-context retention in ATP progress streams, which
  preserves sender wake registration and cancellation observation; an internal
  blocking driver that owns its wake and parking semantics and refuses runtime
  scheduler contexts before polling; and owned poll wrappers that contain
  polling and terminal-cleanup panics.

  *What this means for you:* revalidate cancellation, shutdown, and timeout
  behavior for your workload after upgrading -- particularly anything that
  cancels queries in flight, relies on `Drop` timing, or depends on task
  scheduling order. FrankenSQLite makes **no claim** that these runtime
  changes are behavior-preserving for your application, and **no performance
  claim** in either direction: Asupersync's own `0.4.2` notes record its owned
  blocking-driver ready path as slower in a recorded micro-measurement, and
  FrankenSQLite has not measured the effect of any of this.

### Changed

- **All 27 workspace members bumped `0.2.1 -> 0.3.0` in lockstep.** The bump
  covers 27 internal dependency declarations in the workspace root plus 38
  explicit per-manifest version sites, so no member can resolve a
  mixed-version sibling. `Cargo.lock` re-resolves the four Asupersync-family
  packages and the 27 internal packages; no external dependency other than
  Asupersync changed.

  **25 of the 27 members are publishable; 2 are not.** `fsqlite-e2e` and
  `fsqlite-harness` are marked `publish = false` and are workspace-only test
  infrastructure; they are versioned in lockstep so the workspace resolves
  coherently, but they are never uploaded to crates.io and no release artifact
  is expected for them. Publishable is a manifest property, not a receipt: no
  0.3.0 crate has been published to crates.io, and nothing in this entry should
  be read as evidence that one has.

- **GitHub Actions no longer has any release path, for either registry.**
  DSR is the sole publisher of both the crates.io crates and the npm package.
  Two workflows previously published:
  - `Release` would publish all 25 publishable crates to crates.io on any
    `v*` tag push. Its tag trigger is removed and its publish job is disabled
    fail-closed.
  - `fsqlite-wasm CI` ran `npm publish` with an `NPM_TOKEN` when a GitHub
    Release was published, and exposed a manual `publish` dispatch input. Its
    `release` trigger is removed, the input is inert, and its publish job is
    disabled fail-closed.

  Neither tagging a version nor publishing a GitHub Release now publishes
  anything anywhere. Both job bodies are retained, disabled, as reviewable
  references for DSR. The remaining `pull_request`/`push` CI in the WASM
  workflow is unchanged and still builds and tests the package.

### Known limitations carried from 0.2.1 and 0.2.0

The v0.2.0 limitations in `README.md` still apply, including UTF-8-only text
encoding, unwired page encryption (`PRAGMA key` silently ignored), the
documented FTS5/R-Tree/STRICT edge divergences, and first-contact sidecar
creation on never-before-opened stock databases (GH #140). This release
contains no hand-authored engine change, so it fixes none of them and adds
none of its own. That is a statement about FrankenSQLite's own code, not a
guarantee that observable behavior is identical to 0.2.1: the runtime beneath
the engine changed, as described under **Breaking changes**.

---

## [0.2.1] -- 2026-08-11 (correctness patch: mutation-free opens, FTS5 durability, REPLACE-victim semantics)

Bugfix-only patch release. No new features, no API changes, and **no
performance claims**: the release-gate performance matrix (T16 and the
same-source re-verification tracked by `bd-dqdoe`) remains open and is not
part of this release's scope.

### Fixed

- **Opening a database no longer mutates it (GH #294).** Steady-state
  read-only and schema-only opens of an already-initialized database leave
  every on-disk artifact byte-identical with unchanged mtime/ctime: the
  close-path passive checkpoint no longer re-backfills already-backfilled WAL
  frames or re-stamps the page-1 header change counter when nothing changed,
  and the `-wal-cert-head` sidecar is no longer rewritten byte-identically on
  every checkpoint. Durability fences are unchanged. Receipted by directory
  snapshot-equality tests over both close variants (Unix; first-contact
  sidecar creation on never-before-opened stock databases remains and is
  tracked by GH #140).
- **FTS5 oversized flushes no longer brick the index (GH #328 / cass#369).**
  The segment writer now partitions a flush across multiple `_data` leaves,
  each kept under the u16 term-offset ceiling, instead of failing the write
  with `segment leaf term offset exceeds u16` and leaving a large contentless
  index permanently unable to accept inserts or merges. A term whose single
  doclist alone exceeds the ceiling is skipped with a warning rather than
  failing the segment.
- **FTS5 deferred-validation open (cass#368).** Opening a database with a
  corrupt FTS5 shadow-structure record no longer fails at `Connection::open`;
  validation is deferred so repair tooling can reach the table.
- **FTS5 overlong terms are skipped, not fatal (cass#362).** Terms exceeding
  the term-length cap are skipped consistently across all tokenizer
  construction paths instead of failing the segment write.
- **`INSERT`/`UPDATE OR REPLACE` victim semantics.** Replaced rows now run
  their inbound foreign-key enforcement on every prepared execution lane, and
  fire their DELETE triggers when `PRAGMA recursive_triggers = ON`, matching
  stock SQLite ordering (victim triggers before the causing statement's own
  AFTER triggers). WITHOUT ROWID `UPDATE OR REPLACE` resolves conflicts in a
  strict three-phase plan (decide, delete victims + old row, insert new) so
  victim secondary-index entries can never be orphaned.
- **Trigger-depth contracts.** Depth is charged only after
  `recursive_triggers` suppression; the trigger and FK-action recursive
  program budgets are governed by one coherent pair of limits with the exact
  50/51 aggregate boundary enforced in both mixed directions; ATTACH
  delegation preserves one recursive-program budget across schemas; and
  ATTACH/DETACH inside trigger bodies fail closed at `CREATE TRIGGER` time.

### Known limitations carried from 0.2.0

The v0.2.0 limitations in `README.md` still apply, including UTF-8-only text
encoding, unwired page encryption (`PRAGMA key` silently ignored), and the
documented FTS5/R-Tree/STRICT edge divergences. Every open GitHub issue has an
explicit v0.2.0 disposition recorded on the tracker (`bd-fubxp`).

---

## [0.2.0] -- 2026-08-04 (async storage stack, adaptive skip-scan execution, parallel-WAL durability certificates)

Next full-workspace lockstep release (`0.1.19 -> 0.2.0`). `v0.1.19` is the
semantic and crates.io predecessor, but it is not an ancestor of current
`main`; `v0.1.18` is the latest released ancestor.

> **BREAKING.** This release makes the storage stack `async` end to end, which
> changes public signatures. Under Cargo's 0.x rules the minor version is the
> compatibility axis, so this ships as `0.2.0` rather than `0.1.20`: a caller
> pinning `fsqlite = "0.1"` keeps the synchronous 0.1.19 API and is not
> upgraded silently. See **Breaking changes** below for the migration.

### Breaking changes

- **The storage stack is `async` end to end.** `Connection::open` and its
  `open_existing` / `open_with_page_size` / `open_schema_only` / identity and
  reserved variants, along with `execute`, `execute_batch`, `query`,
  `query_row`, and `prepare`, are now `async fn`. Call sites add `.await`.
  The caller's executor polls these futures. Core `Connection` operations
  derive their `Cx` from the connection's `RuntimeContext`; callers that need
  explicit capability lineage can use `ConnectionEnv::new_with_root_cx`.
- **The sealed storage traits are no longer dyn-compatible.** `MvccPager`,
  `TransactionHandle`, and the neighbouring pager traits return
  `impl Future` (RPITIT) instead of being `async_trait`-boxed, which keeps the
  hot path allocation-free but means `&dyn MvccPager<Txn = _>` no longer
  compiles. Use a generic bound (`fn f<P: MvccPager>(p: &P)`) instead of a
  trait object. These traits are sealed, so this affects call sites rather
  than implementors.
- Tests and doc examples that drive the engine need an executor.
  `asupersync::test_utils::run_test(|| async { ... })` is the supported entry
  point and is available behind asupersync's `test-internals` feature.
- **Existing FTS5 `porter` indexes must be rebuilt after upgrading.** Porter
  tokenization now leaves tokens longer than 64 bytes unstemmed and enforces
  the 1,024-byte term limit before stemming. An index built by an earlier
  version may therefore contain terms that current queries no longer produce.
  For each ordinary or external-content porter-tokenized FTS5 table, run
  `INSERT INTO table_name(table_name) VALUES('rebuild')` after upgrading. For a
  contentless table, do not use the `delete-all` control command: it is
  unsupported in v0.2.0
  ([#253](https://github.com/Dicklesworthstone/frankensqlite/issues/253)).
  Recreate the table and re-ingest every document with its original rowid and
  indexed text from the application-managed source, preferably in one
  transaction.
  No rebuild is required solely for `unicode61`, `ascii`, or `trigram`
  indexes. The 1,024-byte term limit now applies to every tokenizer, but an
  existing index cannot contain an over-long term: writes carrying one failed
  outright in 0.1.x, so an index built by those releases cannot hold one.

### Release blockers

These are not accepted v0.2.0 limitations. Every item below must be fixed and
covered by terminal candidate-bound evidence before the release tag is cut:

- Ordinary implicit-autocommit stress with 10 or more concurrent writers can
  corrupt the database and return wrong row counts with zero writer errors in
  both WAL and rollback-journal modes (`bd-9inpb`).
- `ROLLBACK TO` after trigger activity involving a JOIN view can expose rows
  from the aborted savepoint in later queries
  ([#143](https://github.com/Dicklesworthstone/frankensqlite/issues/143),
  `bd-dpjhw`).
- `ALTER TABLE ... ADD COLUMN` on a populated table with a separate unique
  index can report success while producing a database stock SQLite diagnoses as
  malformed (`bd-wneoh`).
- `UPDATE OR REPLACE` on `WITHOUT ROWID` tables can corrupt replacement-victim
  and secondary-index state (`bd-yuj70`).
- A commit can become durable yet be reported as an error without publication
  to live commit/SSI state, and dropping a pending finalization future can
  strand or overwrite accepted work (`bd-zvxay`, `bd-6xjma`).
- `VACUUM INTO` can emit a candidate rejected by integrity validation, including
  a DESC composite-index case (`bd-tutln`,
  `bd-vacuum-desc-index-self-reject-y2aog`).
- `PRAGMA integrity_check` can miss a referenced empty non-root leaf that stock
  SQLite reports as malformed (`bd-y5urj`).

### Known limitations

- **R-tree write paths are incomplete.** `UPDATE` against an R-tree virtual
  table does not persist changed bounding boxes
  ([#208](https://github.com/Dicklesworthstone/frankensqlite/issues/208)),
  and `INSERT OR REPLACE` is not implemented
  ([#214](https://github.com/Dicklesworthstone/frankensqlite/issues/214)).
  Reads and ordinary inserts behave; avoid these two write forms in v0.2.0.
- **Column-qualified FTS5 `MATCH` is unsound.** A query intended to restrict
  matching to one indexed column can also match terms present only in another
  column and return false positives. Avoid column-qualified `MATCH`, or verify
  the selected column in application logic
  ([#249](https://github.com/Dicklesworthstone/frankensqlite/issues/249)).
- **Bounded external-snapshot database-image validation is not shipped in
  v0.2.0** and is unimplemented on macOS. Applications must not rely on that
  downstream capability as a portable `fsqlite::Connection` API
  ([#307](https://github.com/Dicklesworthstone/frankensqlite/issues/307)).
- **v0.2.0 makes no numeric performance claim.** The async storage migration
  invalidated the older benchmark matrices, and the current comprehensive and
  high-writer-count harnesses remain deliberately non-citable until their
  fail-closed provenance, workload-equivalence, and shipped-profile gates are
  complete. The historical results remain diagnostic evidence only.
- **The runtime-stub inventory is current but not exhaustive.** The v0.2.0
  release inventory records 99 known unsupported-runtime markers, and its
  canonical and root mirrors are byte-identical. Its scanner is line-based,
  however, and stops at the first textual `#[cfg(test)]` marker in each source
  file. That truncation is severe in one file only: it examines about 0.18% of
  `fsqlite-core/src/connection.rs`, but about 66% and 39% of the two other
  affected files. A green inventory check therefore proves that the recorded
  anchors are current; it does not prove that every unsupported runtime path
  has been inventoried
  ([#136](https://github.com/Dicklesworthstone/frankensqlite/issues/136)).
- **Page encryption is not wired; `PRAGMA key`, `PRAGMA rekey`, and
  `PRAGMA durability` are silently ignored.** The envelope-encryption
  implementation lives in `fsqlite-pager`, but `Connection` dispatches none of
  these PRAGMAs, and unrecognised PRAGMAs return success with no rows and no
  error. A successful `PRAGMA key` therefore does not encrypt anything: the
  database is written in plaintext. Do not use v0.2.0 where encryption at rest
  is required, and do not treat these PRAGMAs as having taken effect.
- **Read-only opens are not fully mutation-free in v0.2.0.** On Unix, a database
  that FrankenSQLite has opened before joins its existing namespace without
  rewriting the `-fsqlite-ns-gate` or `-fsqlite-ns-use` sidecar. First contact
  with a stock SQLite database instead creates both sidecars and therefore
  requires a writable parent directory. Windows opens also create the
  `-lock-shared`, `-lock-reserved`, and `-lock-pending` sidecars, including for
  read-only access. Namespace identity binds the database file's device and
  inode, so a database copied to different media cannot be rebound read-only.
  Do not use this release where the database directory is immutable or where a
  copied database must open directly from read-only media
  ([#140](https://github.com/Dicklesworthstone/frankensqlite/issues/140)). The
  explicit-read-only subcase reported in
  [#294](https://github.com/Dicklesworthstone/frankensqlite/issues/294) is fixed
  and verified on Unix for `OpenFlags::SQLITE_OPEN_READ_ONLY` opens of a database
  FrankenSQLite has opened before: regression coverage snapshots every
  directory entry and modification time across an open plus a WAL-backed query
  and requires full byte equality. Broader default-open behavior remains under
  #294; first-contact namespace mutation remains tracked in #140.
- **Windows WAL lock interoperability does not yet extend to shared-memory
  contents.** FrankenSQLite mirrors ordinary WAL lock slots onto stock
  SQLite's real `-shm` lock bytes, but its shared-memory region contents remain
  heap-backed. Do not concurrently mix FrankenSQLite and stock SQLite WAL
  connections to the same database on Windows
  ([#139](https://github.com/Dicklesworthstone/frankensqlite/issues/139)).
- **WAL-adapter commits do not yet provide cross-process `PerCommit` durable
  visibility.** The certificate/checkpoint publication path has a database-fsync
  recovery fence before truncation, but the WAL-adapter path lacks that
  per-commit cross-process fence
  ([#187](https://github.com/Dicklesworthstone/frankensqlite/issues/187)).
- **AUTOINCREMENT rowids may skip values after savepoint rollback in concurrent
  mode.** In the verified sequence, rolling back the second insert leaves a
  gap: FrankenSQLite commits rowids 1 and 3, whereas stock SQLite commits 1 and
  2. Values remain unique and increasing; applications must not depend on
  rowid contiguity
  ([#147](https://github.com/Dicklesworthstone/frankensqlite/issues/147)).
- **Database text encoding is UTF-8-only in v0.2.0.** The database-header
  codec recognizes all three valid SQLite encoding values, but the runtime
  admits only encoding 1 (UTF-8). Databases declaring encoding 2 (UTF-16le) or
  3 (UTF-16be) fail closed as unsupported before schema or text is decoded or
  exported. Admission reads the pager-visible page 1 (WAL-authoritative when a
  live WAL is installed). After normal pager open/recovery and journal/WAL
  authority setup, the gate performs no main-image rewrite or checkpoint before
  rejection; callers must not infer directory or sidecar immutability from this
  boundary. `PRAGMA encoding` attempts to select `UTF-16`, `UTF-16le`, or
  `UTF-16be` also fail as unsupported, before changing connection, schema, or
  exported database state. Convert UTF-16 databases to UTF-8 with stock SQLite
  before opening them in FrankenSQLite. BLOB bytes are unaffected.
- **Legacy SQLite double-quoted-string fallback is intentionally unsupported.**
  FrankenSQLite always interprets double-quoted tokens as identifiers. An
  unresolved `"token"` therefore returns an identifier-resolution error, whereas
  legacy-enabled SQLite may treat it as the string literal `token`. Use
  single-quoted SQL string literals. Ordinary double-quoted identifiers remain
  supported ([#148](https://github.com/Dicklesworthstone/frankensqlite/issues/148)).
- **The low-level `fsqlite-mvcc::TransactionManager` API is not by itself an
  SSI dependency collector.** Normal page reads and writes through that direct
  API do not populate the dangerous-structure flags and can therefore admit
  classic write-skew if callers treat it as a standalone serializable
  transaction layer. The connection pipeline's dependency tracking is the
  supported Page-SSI surface
  ([#189](https://github.com/Dicklesworthstone/frankensqlite/issues/189)).
- **Default concurrent transactions defer committed-freelist reuse while an
  older local snapshot is active.** When a concurrent transaction is the sole
  active local transaction and its snapshot is current, it reuses committed
  free pages at or below the current database size. Sustained overlap can still
  grow `page_count` while free pages remain pending epoch/versioned-freelist
  reclamation ([#302](https://github.com/Dicklesworthstone/frankensqlite/issues/302)).
  The current insertion-based `VACUUM` rebuild can itself retain freed or
  trailing pages, and the rebuilt image's page-1 header may report
  `freelist_trunk` and `freelist_count` as zero while the committed image still
  holds a nonzero freelist. The output remains a valid database that passes
  integrity validation, but v0.2.0 does not promise a zero-freelist,
  header-consistent, or fixed-point compact image. Do not use post-`VACUUM`
  `PRAGMA freelist_count` as an authoritative free-page count in this release
  ([#301](https://github.com/Dicklesworthstone/frankensqlite/issues/301)).
- **Header PRAGMA integer handling diverges from SQLite at signed 32-bit
  boundaries.** On file databases, negative `PRAGMA application_id` and
  `user_version` values such as -1 or -5 read back differently after close and
  reopen ([#263](https://github.com/Dicklesworthstone/frankensqlite/issues/263)).
  Separately, `PRAGMA user_version` retains an out-of-range assignment such as
  2147483648 where stock SQLite reads back 0
  ([#264](https://github.com/Dicklesworthstone/frankensqlite/issues/264)). Use
  non-negative values within signed 32-bit range for these header fields.
- **`PRAGMA auto_vacuum=FULL` and `INCREMENTAL` are not persisted.** The mode
  currently changes connection-local readback only and returns to `NONE` after
  reopen. FrankenSQLite does not yet write the pointer-map pages required to
  enable either mode safely, so applications must not rely on these settings
  for file compaction ([#265](https://github.com/Dicklesworthstone/frankensqlite/issues/265)).
- **FrankenSQLite-created FTS5 databases are not yet integrity-clean when
  reopened by stock SQLite.** Stock SQLite's `integrity_check` reports a
  malformed inverted index on the verified FrankenSQLite-created FTS5 fixture
  even when FrankenSQLite queries appear healthy. The exact on-disk cause is
  still unresolved ([#300](https://github.com/Dicklesworthstone/frankensqlite/issues/300)).
- **Renaming the content table of an external-content FTS5 table can make the
  database unavailable through FrankenSQLite after the renaming connection
  closes.** The established connection can also return stale `MATCH` rows.
  Rename the table back on that same connection before closing it; otherwise,
  stock SQLite can rename it back because the database file remains intact.
  Do not rename an external-content FTS5 content table in this release
  ([#211](https://github.com/Dicklesworthstone/frankensqlite/issues/211)).
- **TEMP schema catalog queries are not partitioned correctly.** On current
  main, `temp.sqlite_master` exposes main-schema objects while omitting TEMP
  objects, and `sqlite_temp_master` is not recognized. Do not use these catalog
  spellings for TEMP-schema introspection in this release
  ([#238](https://github.com/Dicklesworthstone/frankensqlite/issues/238)).
- **Cancelling an `AsyncConnection` call after dispatch stops the caller's
  wait, not the already-running worker operation.** The caller receives
  `Interrupt` while the worker runs the database operation to completion
  against its own connection. Dropping the connection signals shutdown and
  detaches rather than joining, so `Drop` itself does not block; the detached
  worker still performs its terminal cleanup after the operation finishes.
  There is no `interrupt()` or progress-handler equivalent, so applications
  that require a hard kill deadline must currently use process isolation
  ([#306](https://github.com/Dicklesworthstone/frankensqlite/issues/306)).
- **STRICT tables reject some losslessly convertible values.** Column type
  checking matches the input storage class exactly except for INTEGER-to-REAL
  conversion. An `INTEGER` column refuses the text `'42'` and the exact-integer
  real `1.0`, a `REAL` column refuses the text `'1.5'`, and a `TEXT` column
  refuses integer and real inputs where stock SQLite converts and stores the
  value. Supply values in the declared storage class, or use a non-STRICT table
  ([#162](https://github.com/Dicklesworthstone/frankensqlite/issues/162),
  [#163](https://github.com/Dicklesworthstone/frankensqlite/issues/163),
  [#164](https://github.com/Dicklesworthstone/frankensqlite/issues/164),
  [#272](https://github.com/Dicklesworthstone/frankensqlite/issues/272)).
- **`UPDATE` accepts assignments to generated columns.** Assigning to a
  `GENERATED ALWAYS AS` column in an `UPDATE` returns success instead of
  erroring as stock SQLite does; for a STORED column the persisted computed
  value is unchanged, and the VIRTUAL case also reports success. `INSERT`
  rejects the same assignment correctly, so only `UPDATE` is affected
  ([#165](https://github.com/Dicklesworthstone/frankensqlite/issues/165),
  [#166](https://github.com/Dicklesworthstone/frankensqlite/issues/166)).
- **Foreign-key constraint deferral is not implemented.** `DEFERRABLE
  INITIALLY DEFERRED` is accepted, but checks still run at statement time, and
  `PRAGMA defer_foreign_keys` does not enable deferral. A transaction that
  temporarily violates a constraint and repairs it before `COMMIT` therefore
  fails at the first violating statement. Writes are refused rather than
  mis-applied; order statements so no intermediate state violates a constraint
  ([#149](https://github.com/Dicklesworthstone/frankensqlite/issues/149),
  [#161](https://github.com/Dicklesworthstone/frankensqlite/issues/161)).
- **`INSERT OR REPLACE` does not run delete-side foreign-key actions for the
  replaced parent row.** Stock SQLite treats the replacement as a delete
  followed by an insert and fires `ON DELETE CASCADE` on dependent rows;
  FrankenSQLite leaves those rows in place. Because the replacement carries
  the same key, this retains logically stale dependent data rather than
  creating an orphan. Delete the parent explicitly before re-inserting it when
  dependent rows must be cascaded
  ([#142](https://github.com/Dicklesworthstone/frankensqlite/issues/142)).

### Added

- **MySQL-style skip scan.** Composite indexes are now usable when the leading
  column has no equality constraint, by iterating its distinct values. Covers
  `WHERE b = <const>` on a 2-column index, `IS NULL`, inclusive and exclusive
  bounds, and indexes wider than two columns. `SKIP_SCAN_WALK_PROBES` adaptively walks
  before seeking so the optimization degrades gracefully on low-cardinality
  leading columns.
- Skip scans stream a satisfied `ORDER BY` without materializing a sorter, and
  stream `LIMIT`/`OFFSET` on top of that ordering.
- **Batched async VFS I/O.** New batched read/write traits with an io_uring
  backend and fault-injection coverage, multiplexed through canonical
  descriptors with preserved uring-to-Unix fallback semantics.
- **Durable-certificate parallel WAL publication for the certificate/checkpoint
  path**, including a db-fsync recovery fence before WAL truncation over an
  async checkpoint target. This does not yet provide a cross-process
  `PerCommit` durable-visibility fence for the WAL-adapter path
  ([#187](https://github.com/Dicklesworthstone/frankensqlite/issues/187)).
- **Durable `.fsqlite-history` commit-snapshot sidecar** for MVCC.
- `Connection::open_existing_schema_only` and identity/environment variants
  provide an existing-only, writable database open that loads schema metadata
  without hydrating table rows into the compatibility `MemDatabase`. This is
  the bounded-memory entry point for repairing or incrementally updating very
  large SQLite-compatible databases.

### Performance

- `SELECT DISTINCT` is served by an adaptive loose/skip index scan, extended to
  composite and multi-column indexes.
- Composite indexes are seeked directly for a pure range on their leading term,
  with a guard for empty-prefix aggregate composite-range `IdxGT`.

### Fixed

- `compat::Transaction` preserves rollback-on-drop across the async migration.
  Because `Drop::drop` cannot await, an unfinalized wrapper records a mandatory
  cleanup obligation; the next public SQL entry point rolls the transaction
  back before executing the incoming statement. Explicit
  `commit().await` / `rollback().await` remains the immediate-finalization
  path, but abandoned writes cannot leak into a later statement
  ([`0e45f070`](https://github.com/Dicklesworthstone/frankensqlite/commit/0e45f070dc0b7dcfbb3f7ca8269cc6831bee2803)).
- **Connection-local TEMP tables and indexes can no longer allocate orphaned
  pages in the main database**
  ([#290](https://github.com/Dicklesworthstone/frankensqlite/issues/290)). TEMP
  roots now live exclusively in `MemDatabase`; finalized VDBE programs route
  those roots through the TEMP namespace, including fused inserts, explicit
  index creation/drop, UNIQUE enforcement, and snapshot reconstruction. A
  file-backed regression proves TEMP DDL leaves the main page count unchanged
  and stock SQLite reports both `quick_check` and `integrity_check` as `ok`.
- Schema-only registration of contentless FTS5 tables remains lazy instead of
  hydrating the historical corpus. Full scans enumerate persisted `_docsize`
  rowids, explicit-rowid appends reject duplicates and add bounded incremental
  segments, and each in-memory delta is discarded after durable persistence.
  This removes the multi-million-message registration OOM observed by CASS.
- FTS5 contentless-delete tables now persist the `_docsize.origin` column used
  by their deletion contract. Full cross-engine schema compatibility remains
  blocked by the stock-integrity limitation documented above.
- Schema reload now preserves the authoritative contentless FTS5 definition
  when a legacy database also contains a stale same-name implicit-content
  schema row whose required `_content` shadow is absent. Inserts after repair
  retain the correct shadow layout and remain searchable across a second open,
  fixing CASS legacy-schema repair without weakening strict rejection of a
  genuinely missing content shadow.
- **`DROP COLUMN` hardening.** Storage rewrites are now atomic, foreign-key
  ownership survives the rewrite, view and trigger dependencies are validated
  before the drop proceeds, and dependency validation no longer accepts
  unrelated objects.
- Delete cascades are ordered after parent removal rather than interleaved
  with it.
- `WITHOUT ROWID` index locators are preserved across schema reload.
- `REPLACE` now removes victim rows from *all* indexes, not just the one that
  detected the conflict.
- `VACUUM INTO` preserves `UNIQUE` constraints and validates receipt-bound
  source and candidate generations under cooperating concurrent writers
  ([#141](https://github.com/Dicklesworthstone/frankensqlite/issues/141)).
- `ALTER TABLE ... RENAME` now carries `sqlite_sequence` metadata with the
  table, and dropping a renamed AUTOINCREMENT table removes its sequence row.
  A rename no longer leaves both the old and new names in `sqlite_sequence`
  ([#150](https://github.com/Dicklesworthstone/frankensqlite/issues/150)).
- `ON DELETE SET DEFAULT` and `ON UPDATE SET DEFAULT` validate the substituted
  default against the parent table instead of assigning it unchecked, so an
  immediate referential action can no longer leave a missing-parent orphan.
  Both directions now fail the outer statement, matching stock SQLite
  ([#167](https://github.com/Dicklesworthstone/frankensqlite/issues/167),
  [#168](https://github.com/Dicklesworthstone/frankensqlite/issues/168)).
- A successful WAL sync advances the durable-frame watermark used by the
  two-phase publish invariant, so production accounting no longer leaves
  `last_fsynced_frame_count` behind `frame_count`. A failed sync preserves the
  prior watermark, and rebuilding WAL state clears a stale watermark
  ([#188](https://github.com/Dicklesworthstone/frankensqlite/issues/188)).
- An invalid WAL header is treated as an empty WAL, matching stock SQLite
  ([#292](https://github.com/Dicklesworthstone/frankensqlite/issues/292)).
- Explicit `INDEXED BY` is honored in the composite prefix-range seek
  ([#291](https://github.com/Dicklesworthstone/frankensqlite/issues/291)).
- Prepared indexed-equality caches are invalidated on write commit and on
  `MemDatabase` reload, so a prepared lookup cannot serve a stale row.
- Prepared TEMP inserts route to the TEMP namespace
  ([#290](https://github.com/Dicklesworthstone/frankensqlite/issues/290)).
- Explicitly `main.`-qualified `INSERT`, `UPDATE`, and `DELETE` statements now
  keep targeting the persistent table when a same-named TEMP table shadows it,
  including prepared statements and row-by-row replay locator materialization
  ([#144](https://github.com/Dicklesworthstone/frankensqlite/issues/144)).
- Cold MVCC history index lookups are bounded.
- Storage-engine durability, read-only safety, and MVCC shared-memory handling
  were hardened following an adversarial review of the durability wave.

### Dependencies

- `ftui` 0.4.1 -> 0.5.0, `chacha20poly1305` 0.10 -> 0.11 (RustCrypto `aead`
  0.6 / `hybrid-array`), `criterion` 0.7 -> 0.8, `jsonschema` 0.46.5 -> 0.48.5,
  and a full `Cargo.lock` refresh moving every other dependency to its latest
  semver-compatible release. `asupersync` is pinned to 0.3.10 with
  `default-features = false`, so no tokio-ecosystem crate enters the graph.

### Packaging

- `fsqlite-pager`'s dev-dependency on `fsqlite-mvcc` was pinned to an exact
  version by the previous lockstep bump. Because `fsqlite-mvcc` depends on
  `fsqlite-pager`, pager publishes first and that pin could not resolve against
  crates.io, which would have failed the release. Restored to a permissive
  range; it is the only forward dev-edge in the workspace.
- `Opcode::COUNT` is documented as the exclusive upper bound on discriminants
  (`1..COUNT`), and its unit test, which still asserted the pre-`cc17ee46`
  value, was corrected.

## [0.1.19] -- 2026-07-26 (atomic SQL semantics and migration-scale DDL reopening)

Full-workspace lockstep release (`0.1.18 -> 0.1.19`). Semver-compatible 0.1.x;
no breaking API changes.

### Correctness

- Multi-row `INSERT ... SELECT`, rowid and `WITHOUT ROWID` updates, nested
  triggers, `RETURNING`, conflict actions, and foreign-key validation now share
  statement-scoped rollback and change-counter semantics with C SQLite. The
  execution path preserves the correct effects for `FAIL`, rolls back the
  complete statement for `ABORT`/`ROLLBACK`, restores nested-trigger
  `last_insert_rowid()`, and retains exact transaction/savepoint history.
- `INSERT OR REPLACE` now records every logical victim, applies inbound
  foreign-key actions at the correct statement boundary, and handles
  `WITHOUT ROWID` primary and secondary uniqueness victims without leaking a
  partial delete or double-counting changes.
- Trigger `WHEN` expressions honor explicit collations after `OLD`/`NEW`
  binding, including bound literals under `COLLATE NOCASE`.

### DDL durability and fail-closed parsing

- Flat associative `AND`/`OR` predicates are serialized without recursive
  parenthesis growth, so repeatedly reopening a migration-heavy database no
  longer inflates catalog SQL until the parser exhausts a bounded stack.
- Parser nesting is tracked explicitly and returns a typed recursion-limit
  error rather than overflowing a 2 MiB worker stack.
- Catalog hydration now refuses malformed trigger definitions instead of
  silently dropping them. File-backed reopen tests pin exact catalog SQL and
  BLAKE3 hashes across four close/open cycles while executing triggers, partial
  indexes, views, rowid and `WITHOUT ROWID` foreign keys, and integrity checks.

### Verification

- Eleven atomic C-SQLite differential scenarios cover trigger interleavings,
  composite locators, foreign-key matrices, REPLACE cascades, savepoint/reopen
  behavior, `OR FAIL` history, and rowid-alias updates.
- The migration-scale DDL harness covers scalar and `EXISTS` trigger
  predicates, binary and `NOCASE` partial indexes, four durable reopen cycles,
  and nine deliberately corrupted catalog variants that must all fail closed.

### CI / Release

- All publishable `fsqlite` / `fsqlite-*` crates were published in lockstep at
  `0.1.19` by the successful
  [crates.io publishing workflow](https://github.com/Dicklesworthstone/frankensqlite/actions/runs/30200995163).
- The `v0.1.19` Git tag exists, but no GitHub Release object or native signed
  artifact set was published for this version.

## [0.1.18] -- 2026-07-18 (streaming composite-index count semijoins)

Full-workspace lockstep release (`0.1.17 -> 0.1.18`). Semver-compatible 0.1.x;
no breaking API changes.

### Performance

- `COUNT(*)` over an indexed outer column and a complete, ordered rowid
  subquery now streams both inputs as a merge semijoin and counts equal
  first-key index runs with `CountIndexEqRun`. The fast path supports safe
  composite indexes such as `UNIQUE(conversation_id, idx)`, avoids opening the
  covered table, and eliminates both automatic-index materialization and
  per-row Rust callbacks for this shape.

### Correctness

- Explicit `INDEXED BY` is honored before the generic planner scan directive
  for the proven streaming-count shape, while `NOT INDEXED`, descending first
  keys, partial or expression indexes, and collation mismatches continue to
  decline the optimization. Nullable leading keys are skipped explicitly, and
  list/materialized probes retain their single-key physical seek contract.
- Runtime and opcode regressions cover the production
  `messages(conversation_id, idx)` schema, orphan exclusion, duplicate complete
  keys, nullable first keys, matching non-binary collations, descending trailing
  terms, and every unsafe fallback class.

### CI / Release

- All publishable `fsqlite` / `fsqlite-*` crates were published to crates.io
  in lockstep at `0.1.18`.
- The annotated `v0.1.18` Git tag exists, but GitHub records no Release
  workflow run or Release object for that tag, and no native signed artifact
  set was published for this version.

## [0.1.17] -- 2026-07-18 (B-tree corruption and join-correctness fixes)

Full-workspace lockstep release (`0.1.16 -> 0.1.17`). Semver-compatible 0.1.x;
no breaking API changes.

### Added

- Strict multi-process operation now reports lock-admission exhaustion as a
  typed contract violation, opens existing regular files without following a
  final symlink, rejects hard-link aliases at the identity boundary, and can
  compute `octet_length(column)` from record metadata without materializing an
  overflow payload.

### Fixed

- **Direct updates of overflow-backed rows can no longer commit an out-of-order
  table B-tree** ([PR #287](https://github.com/Dicklesworthstone/frankensqlite/pull/287)).
  The report and reproducer from
  [@etafund](https://github.com/etafund) were independently reproduced and
  reimplemented in `796c4cb7`, with full contributor credit.
  A delete that drained a singleton leaf could leave its cursor on the logical
  successor in a different ancestor subtree. Reusing that position to reinsert
  the same rowid preserved leaf-local order while violating an ancestor
  separator, making the row scan-visible but unreachable by point seek and
  causing stock SQLite's `integrity_check` to report `Rowid out of order`.
  Prechecked inserts now validate both leaf neighbours and the complete
  root-to-leaf routing interval before reuse, falling back to a fresh root
  descent when necessary while retaining the zero-I/O same-leaf fast path.
  Deterministic depth-3 B-tree and file-backed SQL regressions prove every row
  remains point-seekable and that stock SQLite reports an intact image.
- Mutation paths now reconstruct a real root-to-leaf stack before balancing
  cached leaf-only cursor positions. Root identity is checked at both balance
  choke points, preventing rootless table inserts or deletes from treating a
  cached leaf as the tree root and extending the corruption coverage for
  composite-UNIQUE update churn (`c57499fb`,
  [#132](https://github.com/Dicklesworthstone/frankensqlite/issues/132)).
  The demonstrated delete/reinsert cursor-reuse mechanism is fixed, but #132
  intentionally remains open: its private historical artifacts have not yet
  been re-derived from a public generator or rerun against both fixes, so this
  release does not claim that every historical corruption mechanism has been
  eliminated.
- Cross-process first-committer-wins validation now distinguishes a benign
  stock-SQLite checkpoint/reset from a real external write. Across a WAL
  generation transition, every conflict candidate is admitted only when an
  exact page-number-associated BLAKE3 hash of the transaction's full snapshot
  page matches the latest committed full page (replacement WAL first, main
  database otherwise). Missing or ambiguous baselines, changed or truncated
  pages, invalid headers, page-size drift, and I/O failures all fail closed
  with `BusySnapshot`, while a byte-identical checkpoint no longer breaks
  retained autocommit.
- `ORDER BY ... COLLATE` resolution now preserves SQLite's positional and
  output-alias precedence, then matches the selected collation expression
  before falling back to its unwrapped structure. This keeps simple grouped
  queries such as `SELECT tag COLLATE BINARY ... ORDER BY tag COLLATE BINARY`
  working while retaining the compound-select fixes for collated aliases and
  positional terms.
- Checkpoint writes are failure-atomic with respect to both database size and
  shared pager publication. A failed page write no longer advances the
  in-memory page count, and checkpoint metadata is published only after the
  database durability barrier succeeds, so write or sync faults cannot stamp a
  page count beyond end-of-file or advertise unflushed checkpoint state
  ([#194](https://github.com/Dicklesworthstone/frankensqlite/issues/194),
  [#195](https://github.com/Dicklesworthstone/frankensqlite/issues/195)).
  Fault-injecting VFS regressions cover both failure orderings and successful
  recovery after the injected fault clears.
- Fresh and replacement WAL generations now seed both salts from operating
  system entropy, while checkpoint reset increments the first salt and
  re-randomizes the second. This replaces the deterministic `(0, 0)`, `(1, 1)`,
  `(2, 2)` sequence and ensures stale or copied frames fail generation
  validation instead of chaining against an unrelated database state
  ([#201](https://github.com/Dicklesworthstone/frankensqlite/issues/201)).
- `ALTER TABLE ... ADD COLUMN` rejects `CHECK` constraints containing
  subqueries before mutating or persisting the schema, matching SQLite and
  preventing creation of a database that stock SQLite reports as a malformed
  schema
  ([#252](https://github.com/Dicklesworthstone/frankensqlite/issues/252)).
- Row-value `IS`, `IS NOT`, `IS DISTINCT FROM`, and `IS NOT DISTINCT FROM`
  comparisons are NULL-safe and componentwise, while row-value `BETWEEN` and
  `NOT BETWEEN` use SQLite-compatible lexicographic bounds (#170, #171, #243).
- Generated columns are rejected from both column-level and table-level
  primary keys, and `PRAGMA foreign_key_check(table)` reports an unknown table
  instead of silently returning no rows (#181, #261).
- `DELETE` and `UPDATE` rowid fast paths now apply SQLite's exact-integer
  coercion before seeking. A predicate such as `rowid = 2.5` no longer
  truncates to rowid 2 and mutates the wrong row; integral numeric and text
  values still resolve normally, while non-integral, non-numeric, and `NULL`
  values select no row.
- Catalog scalar and `EXISTS` subqueries over `sqlite_master` and
  `sqlite_schema` now return through expression-aware executors after catalog
  materialization instead of failing in the JOIN-only executor
  ([#286](https://github.com/Dicklesworthstone/frankensqlite/issues/286)).
- Hash joins now retain and authoritatively evaluate residual `ON` predicates
  after probing extracted equality keys. Join, `WHERE`, and projection column
  references are bound once per statement instead of performing repeated
  case-insensitive name scans per candidate row, fixing the predicate-heavy
  quadratic path while preserving affinity, collation, duplicate, `NULL`, and
  outer-join semantics
  ([#285](https://github.com/Dicklesworthstone/frankensqlite/issues/285)).
- Parameter-dependent scalar subqueries are no longer constant-folded before
  bindings are available, preserving placeholder values in both FROM-less and
  table-backed `WHERE` expressions. The locked asupersync dependency is updated
  to 0.3.9 so native bulkhead admission uses the same explicit-time API in
  source builds, published crates, and downstream consumers.
- Scalar-function conformance now matches SQLite for unterminated `GLOB`
  character classes (#257), negative-zero and alternate-form-2
  `printf`/`format` output (#258, #176), two-argument `iif`/`if` (#183), and
  prepare-time `likelihood` probability validation (#182).
- JSON functions accept finite bare SQL integer and real values as JSON
  numbers. `json_valid` and `json_type` now report the same results as SQLite,
  while non-finite reals remain invalid (#259, #260). Interpreted
  `json_group_array` and `json_group_object` aggregates now honor their
  in-aggregate `ORDER BY` terms, and `json_group_array(DISTINCT ...)` removes
  duplicate values before ordering (#266, #267, #268).
- Recursive trigger execution now returns its typed depth-limit error before
  exhausting the Rust thread stack. The safety cap is tightened from 32 to 8
  frames, and the regression runs the complete ping-pong trigger chain on an
  explicit 1 MiB stack so future compiler frame growth fails in CI instead of
  aborting the process.
- The Unix installer now guards empty proxy-argument expansion under
  `set -u`, preserving zero arguments on macOS's system Bash 3.2 while still
  forwarding configured HTTP(S) proxies unchanged.

### Performance

- Partial-key `SeekGT`/`SeekLE` operations use a logarithmic biased B-tree
  descent rather than walking equal-prefix runs; the targeted MAX-prefix
  workload improved by more than 13x.
- Aggregate rowid equality with parameter, real, or text inputs uses a bounded
  seek with exact-integer coercion instead of a full table scan. Ordinary
  rowid reads use the same semantics.
- Integer parameter, arithmetic, and division results update compatible VDBE
  registers in place. Date/time formatting uses stack-backed buffers, `TRIM`
  borrows its output slice, parser prefix payloads move directly into the AST,
  exact identifier lookup takes a dedicated planner fast path, and R-tree
  duplicate-id detection is indexed in O(1).

### CI / Release

- All 25 publishable `fsqlite` / `fsqlite-*` crates remain in the validated
  topological crates.io release closure; `fsqlite-e2e` and
  `fsqlite-harness` remain private workspace packages.
- Native release artifacts cover fully static Linux x86-64 and arm64, macOS
  x86-64 and arm64, and Windows x86-64. Every archive is checksum-bound,
  minisign-authenticated, and accompanied by provenance plus an SPDX SBOM.

## [0.1.16] -- 2026-07-14 (corruption fixes and namespace-generation hardening)

Full-workspace lockstep release (`0.1.15 -> 0.1.16`). Semver-compatible 0.1.x;
two low-level helper APIs now expose failure explicitly instead of fabricating
lossy values.

### Added

- Production Unix and Windows installers now select the native release asset,
  require its SHA-256 entry, optionally authenticate the signed checksum
  manifest with the embedded minisign trust anchor, validate candidates before
  atomic replacement, and run bounded exact-version and SQL smoke tests by
  default. Exact-tag source and air-gapped fallbacks are explicit, mutually
  exclusive modes; the generic Linux assets are fully static musl binaries.

### Fixed

- **`INSERT ... ON CONFLICT DO UPDATE` no longer reopens a leaf page freed by
  the same transaction** ([#123](https://github.com/Dicklesworthstone/frankensqlite/issues/123)).
  A rootless cursor-path repair correctly descended from the root before
  DELETE, but that descent repopulated the table-seek cache with the landing
  leaf. If DELETE balancing then merged and freed that leaf, the successor
  re-seek could reuse the stale cache entry and report that the page had been
  freed twice. Insert balancing had the analogous stale-topology risk after a
  split. Both structural balance choke points now discard topology-dependent
  seek-cache anchors before their first write. A deterministic file-backed
  regression reproduces the reported leaf geometry, closes FrankenSQLite,
  reopens the database with stock SQLite, and requires all rows plus
  `PRAGMA integrity_check = 'ok'`.
- **In-place `VACUUM` is now failure-atomic for explicit reserved-prefix indexes and
  database-image replacement**
  ([#138](https://github.com/Dicklesworthstone/frankensqlite/issues/138)).
  `sqlite_autoindex_*` entries are considered implicit only when their stored
  SQL is `NULL` and their name canonically identifies an autoindex owned by
  that table, preserving explicit index definitions even under
  reserved-looking names. Rebuilt images are receipt-bound, reopened, and
  required to pass both `quick_check` and `integrity_check` before publication.
  Repeated, shrinking, and non-empty-WAL in-place `VACUUM` operations reopen
  successfully with both FrankenSQLite and stock SQLite.
- **Rollback-journal publication and recovery now fail closed under partial
  writes, silent corruption, cancellation, and cleanup failures.** Candidate
  images are published in place under cross-process maintenance fencing with a
  durable rollback journal whose magic remains zero until every preimage is
  durable. Recovery validates the complete journal before its first database
  write, then retains the hot journal until the restored database is re-read
  page-for-page and any caller-specific whole-image receipt is proven. A failed
  verification preserves the recovery record for a fresh-open retry; a
  successful recovery invalidates it only after durable restoration.
- **Native lock registries can no longer split one database generation into
  two in-process lock domains during a last-close/new-open race.** Unix inode
  opens publish their reference while holding the table shard and defer closing
  redundant descriptors until all process-wide `fcntl` claims have drained.
  Reserved-lock probes consult that coalesced process state before `F_GETLK`,
  so sibling connections observe local writers as well as other processes.
  Unix and Windows register SHM owners atomically with table lookup and remove
  an orphan only when the table still names that exact state object. MemoryVfs
  uses monotonic process-local file-generation identities and applies the same
  exact-generation rule during Drop cleanup, so a detached old handle cannot
  erase a replacement's SHM state or group-commit coordination.
- Windows rollback-mode connections and full-image maintenance now contend on
  stock SQLite's real main-file lock ranges; WAL maintenance additionally
  fences the real `-shm` write/checkpoint bytes and unwinds every acquired
  range after partial failure. Ordinary Windows WAL `shm_lock` also mirrors
  every lock slot onto stock SQLite's real `-shm` bytes with process-aggregated
  ownership, reference counts, and reverse-order partial-failure unwind
  ([#139](https://github.com/Dicklesworthstone/frankensqlite/issues/139)).
  Windows shared-memory region contents remain heap-backed, however, so this
  lock interoperability does not claim that concurrently mixing FrankenSQLite
  and stock SQLite WAL connections is safe.
- **INSERT OR REPLACE churn rebuilds rootless cursor stacks before structural
  mutation** (bd-kwei8), preventing empty child leaves and stale parent links
  from producing a database image rejected by stock SQLite.
- Expression serialization preserves explicit grouping parentheses and folds
  `IS NULL`/`IS NOT NULL` without changing precedence.
- The C API's active-statement accounting remains warning-free on current
  nightly while retaining the workspace's Rust 1.85 MSRV.
- Windows WAL shared-memory mappings now retain their explicitly shared
  backing across handles and region growth. This restores cross-handle write
  visibility after `ShmRegion::clone()` became a deliberate deep-copy API and
  prevents resized mappings from diverging into detached heap buffers.
- **Pager transaction handles retain a coherent begin-time snapshot when the
  database grows concurrently** ([#124](https://github.com/Dicklesworthstone/frankensqlite/issues/124)).
  Accessing a page beyond the captured database size now returns
  `BusySnapshot`; it never advances the handle's database-size and commit-sequence
  bounds in place or mixes pre- and post-BEGIN page images.
- `FreelistTrunk::write` now returns a typed error for undersized destination
  pages or leaf vectors that exceed the trunk's capacity, and leaves the
  destination untouched on failure instead of silently truncating free-page
  accounting ([#125](https://github.com/Dicklesworthstone/frankensqlite/issues/125)).
- `RangeReservation::end_rowid_inclusive` now returns `None` for an empty range
  or an externally constructed overflowing bound. Empty half-open intervals no
  longer manufacture a rowid below their start
  ([#126](https://github.com/Dicklesworthstone/frankensqlite/issues/126)).
- Interior-index separator replacement now retains the old overflow chain until
  structural balancing succeeds. A deterministic packed-page fault-injection
  regression forces the first balance write to fail and proves the rollback
  source still owns its overflow pages
  ([#127](https://github.com/Dicklesworthstone/frankensqlite/issues/127)).
- Concurrent first-committer-wins planning now treats its lock-free conflict
  estimate as a correctness-preserving superset: free-only transactions carry
  every explicitly freed page plus a shared page-1 freelist metadata token.
  Core-layer regressions prove those pages are locked and a newer freed-page
  publication aborts with `BusySnapshot`
  ([#128](https://github.com/Dicklesworthstone/frankensqlite/issues/128)).
- Transaction-local page images are authoritative for the lifetime of a pager
  handle. Re-reading a page after another transaction commits can no longer
  consult a shared latest-image cache first and silently combine two snapshots
  ([#129](https://github.com/Dicklesworthstone/frankensqlite/issues/129)).
- Successful local commits now advance both the aggregate commit clock and its
  cached rollback-journal/WAL identity components. The next transaction no
  longer mistakes the pager's own commit for an external composition change or
  discards valid publication, cache, and volatile-freelist state.
- Composite-index `ORDER BY` planning now consumes repeated equality-constrained
  prefix terms before comparing the remaining index order. Queries such as
  `WHERE a = ? ORDER BY a, b` use the `(a, b)` index without a sorter and retain
  SQLite-compatible parameterized `LIMIT` behavior
  ([#130](https://github.com/Dicklesworthstone/frankensqlite/issues/130)).
- A saturated clean page-buffer pool no longer turns an otherwise valid write
  into a false `OutOfMemory` failure. Bounded clean-page reclamation is
  serialized per shard, never evicts dirty state, preserves transaction state
  when staging fails, and is covered by concurrent saturation plus file-backed
  commit/rollback regressions
  ([#131](https://github.com/Dicklesworthstone/frankensqlite/issues/131)).
- Full `PRAGMA integrity_check` now recognizes SQLite's intentionally unused
  lock-byte page at the 1 GiB boundary. The page remains forbidden as a B-tree,
  overflow, or freelist reference, while an unreferenced lock-byte page is no
  longer misreported as corruption; every neighboring page still requires an
  owner ([#133](https://github.com/Dicklesworthstone/frankensqlite/issues/133)).
- Composite index aggregate seeks no longer drop residual predicates after an
  equality prefix or prefix-plus-range constraint. Integer, text, and bound
  parameter residuals are evaluated for every candidate row, preserving
  `COUNT`/`SUM` correctness while retaining the bounded seek plan.
- Explicit `NOT INDEXED` table hints now disable the pre-directive covering
  range fast path as well as ordinary planner-selected indexes. The optimized
  path can no longer emit `IdxRowid`/`SeekGE` behind a caller's scan directive.
- `MIN`/`MAX` over the trailing term of a composite index now seeks with a true
  partial equality-prefix key. It no longer fabricates an integer sentinel
  that skips the NULL region on ascending indexes or points at the wrong
  physical edge on descending indexes; ASC/DESC, NULL-only, absent-prefix,
  boundary, wrapper, empty-table, and `COUNT` controls are oracle-gated.
- WASM uses browser-backed monotonic and wall-clock time instead of calling
  unsupported `std::time` clocks or advancing a synthetic per-call tick. This
  fixes time-travel snapshot capture and `CURRENT_TIMESTAMP` in real browser
  runtimes.
- Write-existing VFS opens are strictly non-creating, and open handles expose a
  stable file identity so pager/runtime layers can distinguish backing files
  without weakening create-vs-open semantics.
- Native Unix and Windows file-backed connections now bind one stable absolute
  database path to one descriptor-derived file identity for the full pager
  lifetime. Persistent gate/use lock sidecars serialize namespace generations;
  new and caller-reserved empty databases retain exclusive admission through
  pager, schema, journal, and WAL bootstrap before becoming joinable. Existing
  peers must open the recorded identity without `CREATE`, while pathname
  replacement, identity drift, or unexpected reserved-bootstrap companions fail
  before recovery or mutation. The protocol deliberately assumes a trusted
  parent directory and cooperating FrankenSQLite processes; raw external
  unlink/rename and hard-link aliases remain outside its advisory-lock boundary.

### Security

- Updated `crossbeam-epoch` to 0.9.20, fixing RUSTSEC-2026-0204 (invalid
  pointer dereference while formatting an invalid/null epoch pointer).
- Updated transitive `anyhow` to 1.0.103, fixing RUSTSEC-2026-0190
  (`Error::downcast_mut` borrow-rule violation after adding context).

### Performance

- Aggregate and ordinary SELECT codegen now emits bounded index/rowid seeks
  for equality, ranges, IN lists, and normalized OR-of-equalities, including
  covering-index plans that avoid table lookups.
- Numeric, text, placeholder-bound, and composite prefix-plus-range scans can
  stream directly from indexes. Compatible `ORDER BY` shapes use the same
  forward or reverse traversal (including deterministic rowid tie order)
  without a sorter; unsupported collation/direction shapes still fall back.
- Pager sparse-cache clearing touches only populated slots.

### CI / Release

- The unstable prefetch-intrinsic feature gate is now enabled only on x86-64,
  where the optimized pager and B-tree paths use it. Arm64 release builds no
  longer fail strict warning gates on an otherwise unused crate feature.
- The crates.io release plan now contains all 25 publishable workspace crates,
  including `fsqlite-cli`, `fsqlite-c-api`, and `fsqlite-wasm`. It derives the
  public package set from Cargo metadata and fails before publishing if the
  configured topological sequence is incomplete. The intentionally private
  `fsqlite-e2e` and `fsqlite-harness` packages remain `publish = false`.
- The MVCC concurrent-writer Criterion benchmark now releases its validation
  guard before abort cleanup re-locks the same session. `cargo test
  --workspace --all-targets` no longer self-deadlocks in the benchmark harness.
- The file-backed clustered-seek pipeline benchmark now exercises equivalent
  prepared FrankenSQLite and C SQLite lifecycles, with an exact preflight row
  oracle before either engine is measured.
- Planner replay artifacts are emitted only when the dedicated artifact path is
  explicitly requested. Ordinary parallel unit tests no longer rewrite tracked
  evidence with wall-clock timing or process-global metric noise.
- Pager fault-injection scenarios now own a serialized, panic-safe session.
  Every one-shot hook is tagged with its owner generation, only explicitly
  enrolled worker threads can consume it, and teardown revokes the generation
  atomically with clearing hook state. Parallel tests and late participants can
  neither steal another scenario's fault nor leak it as a global hook.
- MVCC reclamation registries now own independent epoch collectors, and
  process-global tracing, telemetry, SSI evidence, logical-clock, pager-profile,
  and runtime-obligation tests isolate their mutable state. The default-parallel
  MVCC suite and release gates are deterministic instead of cross-blocking or
  consuming another test's observations.
- Release verification no longer assumes every runner has `rch`: the feature
  coverage dashboard selects remote compilation when available and otherwise
  runs Cargo locally. Parallel benchmark reporting also treats utilization,
  throughput, and worker-count samples as gauges rather than subtracting them
  as unsigned counters.
- The WASM workflow executes the binding suite in headless Chrome in addition
  to host tests and wasm32 compilation, so browser-only runtime panics are
  release-blocking.

## [0.1.15] -- 2026-07-06 (FTS5 UPDATE on WITHOUT ROWID shadow tables)

Full-workspace lockstep release (`0.1.14 -> 0.1.15`). Semver-compatible 0.1.x;
no breaking API changes.

### Fixed

- **`UPDATE` on an FTS5-indexed table in a canonically-valid, stock-SQLite-created
  store no longer aborts as false corruption** (#121). Such an `UPDATE` routes its
  FTS5 shadow-table maintenance through the rootpage-0 FTS5 write path, whose
  `replace_storage_table_rows` helper unconditionally opened a *table* cursor.
  Stock SQLite persists the `%_idx` / `%_config` FTS5 shadows as `WITHOUT ROWID`
  (index-structured) b-trees, so a table cursor anchored on an index-structured
  root tripped the `table_seek_for_insert` `is_table` guard and aborted with
  `database disk image is malformed: table_seek called on index page`. The
  shadow-write path now reads each shadow's `without_rowid` flag and, for
  `WITHOUT ROWID` shadows, opens an *index* cursor and re-inserts each row as a
  full-record index key; the rowid-shadow path is unchanged. FrankenSQLite-created
  stores were unaffected (their shadows are rowid tables), so this surfaced only
  against stores built by stock SQLite (e.g. a pre-existing cass index).

## [0.1.14] -- 2026-07-05 (WITHOUT ROWID completion, omitted ON CONFLICT target, C SQLite parity sweep)

Full-workspace lockstep release (`0.1.13 -> 0.1.14`). Semver-compatible 0.1.x;
no breaking API changes.

### Added

- **WITHOUT ROWID DML completion (bd-eja6l).** `RETURNING` on INSERT/UPDATE/
  DELETE, `INSERT ... SELECT`, `ON CONFLICT DO UPDATE` (PK target), and
  `UPDATE ... FROM` now work on WITHOUT ROWID tables, all oracle-gated against
  the bundled C SQLite. WITHOUT ROWID PRIMARY KEY columns now enforce their
  implicit NOT NULL (bd-0re6l).
- **Omitted `ON CONFLICT` target (SQLite 3.35+, bd-6geae).**
  `INSERT ... ON CONFLICT DO UPDATE` without a conflict target fires on
  whichever uniqueness constraint the new row violates first — the rowid/
  INTEGER PRIMARY KEY or any UNIQUE column/index, probed in schema order. A
  targetless clause must be the last `ON CONFLICT` clause (parse rule pinned).
- **Per-constraint conflict actions + `ON CONFLICT DO NOTHING`/`IGNORE`
  codegen** for declared `ON CONFLICT <algo>` column/index constraints.
- **WITHOUT ROWID secondary-index reads (bd-rjaff).** Index-driven SELECTs
  (`INDEXED BY`, planner-chosen equality/range scans, ORDER-BY-via-index) now
  seek the table b-tree by the PK suffix stored in the index entry instead of
  failing with "index key record missing trailing integer rowid"; join lookup
  lanes fall back to the generic path.
- New oracle parity gates: scalar/dtoa/aggregate result parity (180 cases),
  collation propagation, broad divergence hunt, WITHOUT ROWID upsert/
  UPDATE-FROM/index-read gates, omitted-target upsert gate, printf/datetime
  edge gate.

### Fixed

- **`GROUP BY` honors column-declared `COLLATE NOCASE`/`RTRIM`** (bd-cdl4w):
  collated group keys route off the BINARY-only VDBE storage substrate.
- **UNION dedup survivor matches C SQLite on NOCASE/RTRIM collations**
  (bd-a6mlo): first occurrence within the last compound arm wins.
- **Underscore digit separators** in numeric literals (SQLite 3.46+,
  bd-n8m5v): `1_000`, `0xFF_FF`, `1.0_5`, `1e1_0`.
- **Overflowing float literals are ±Infinity** (`SELECT 9e999` returns `Inf`,
  not `1.7976931348623157e308`), matching C SQLite text-to-real conversion.
- **printf/format parity:** non-finite floats render as `Inf`/`-Inf`/`NaN`
  with sign flags honored; `%e`/`%g` honor `+`/space/`0` flags; `.*` takes
  precision from the argument list; `%s` precision counts bytes.
- **date/time parity:** numeric arguments outside the valid Julian-day range
  return NULL unless a reinterpreting modifier (`unixepoch`, `julianday`,
  `auto`) comes first.
- **CLI:** multi-line `CREATE TRIGGER ... BEGIN ... END;` works in the REPL,
  `.read`, and batch mode (sqlite3_complete()-style END tracking); `.dump`
  emits `PRAGMA foreign_keys=OFF;` and dumps non-finite REALs as
  `9.0e+999`/`-9.0e+999`/NULL so output reloads cleanly.
- **Pager (journal mode):** cross-connection commits now detect committed-
  freelist page aliasing — a peer consuming (or resurrecting) a committed free
  page without growing the file aborts the second committer with
  `BusySnapshot` instead of corrupting the b-tree (extends the bd-9inpb/am#152
  db-size growth check).
- **WAL:** `WalFile::create` fsyncs the fresh header + truncation before any
  frame append, closing a stale-generation frame-replay window after a crash.
- **VDBE:** INSERT target alias resolves through the WITHOUT ROWID
  upsert/insert paths (`INSERT INTO t AS x ... DO UPDATE SET v = x.v`).
- Build: the `#[cfg(not(feature = "ext-fts5"))]` reload stub matches its call
  site (fixes `--no-default-features`).

### CI / Release

- `release.yml` publishes the full topological closure of `fsqlite`'s
  versioned dependencies (22 crates) instead of a 6-crate subset that relied
  on prior manual publishes.

---

## [0.1.13] -- 2026-06-29 (post-0.1.12 UPSERT correctness + adaptive pager eviction)

Full-workspace lockstep release (`0.1.12 -> 0.1.13`). Semver-compatible 0.1.x;
no breaking API changes.

### Fixed

- **UPSERT `DO UPDATE` now aborts on a non-target UNIQUE/PK conflict.**
  `INSERT ... ON CONFLICT(<target>) DO UPDATE` resolved a conflict only on the
  named target index; when the inserted row instead violated a *different*
  unique/PK index, the statement silently swallowed it (reported 0 rows
  affected, no error) instead of raising `SQLITE_CONSTRAINT` like stock SQLite.
  VDBE codegen now scopes the forced `IGNORE` to `DO NOTHING`; `DO UPDATE`
  carries the genuine statement conflict action (default ABORT) on the
  no-conflict insert path. The target-conflict UPDATE branch is unchanged.
  Adds oracle-backed regression coverage that was previously absent (bd-1z8wg).
- MVCC: atomic commit-sequence allocation + registration closes the INV-6
  visibility window (bd-707lc); unpinned MVCC reads proven ABA-safe via
  generational `VersionIdx` without epoch pins (bd-3wop3.5).
- Pager: journal-mode cross-connection page-alias detection (bd-9inpb).
- Core: `PRAGMA fsqlite_concurrency` reports the raw `journal_mode` for exact
  parity with `PRAGMA journal_mode` and de-silences a `journal_mode='wal'`
  -> MVCC divergence (bd-nao48).
- Pager/types: deterministic eviction probe greens the ambient-authority gate
  (bd-w4yc9).

### Added

- Pager: ARC adaptive-replacement eviction model + page-cache policy wiring,
  with Thompson-sampling auto-tuning of the S3-FIFO split (bd-5ftij, bd-q7zls).
  Real-policy eviction hit-rate benchmarks select S3-FIFO as the default.
- Core: `PRAGMA fsqlite_concurrency` diagnostic (bd-nao48).

### Changed

- VDBE: root-page init writes via owned passthrough rather than a borrowed copy
  (bd-1dp9.6.2); `RETURNING` DML proven to route through VDBE codegen rather
  than the interpreter (bd-asvja).
- BTree: conflict-topology split-policy mode consistency and K2 deferred-delete
  rebalancing instrumentation (bd-1dp9.6.7, bd-yywuv).

---

## [0.1.12] -- 2026-06-19 (post-0.1.11 correctness and release hardening)

Full-workspace lockstep release candidate (`0.1.11 -> 0.1.12`).

### Fixed

- Corrected in-transaction `PRAGMA integrity_check` false positives around
  transaction-local page growth and reserved-but-unissued pager leases
  ([#113](https://github.com/Dicklesworthstone/frankensqlite/issues/113)).
- Fixed pager transaction live-size accounting so reserved-but-unissued page
  leases do not leak into `freelist_count`/integrity-style observations.
- Preserved indexed `COUNT(*)` correlated-`EXISTS` probes while routing the
  unsupported correlated `EXISTS` WHERE shapes through the connection fallback.
- Matched SQLite compound DISTINCT result ordering for
  `UNION`/`INTERSECT`/`EXCEPT` while preserving `UNION ALL` append order.
- Corrected `concat_ws` so empty-string arguments are retained and only `NULL`
  separator/payload semantics are special.
- Hardened the concurrent commit validate/write/publish regression test for
  [#115](https://github.com/Dicklesworthstone/frankensqlite/issues/115) so the
  process-global hook cannot be stolen by unrelated parallel tests.
- Updated RaptorQ repair symbol emission for `asupersync 0.3.5`.
- Isolated record/hot-path profiling tests with thread-local guards and local
  serializers so full-workspace parallel test runs do not cross-contaminate
  global counters.
- Added a CLI `-V` / `--version` path for release/package-manager verification.

### Changed

- Refreshed direct Rust and TypeScript dependencies to current stable versions,
  including the latest local `/dp/asupersync` checkout.
- Updated browser SDK/worker tooling to TypeScript 6, Vitest 4, and Playwright
  1.61.
- Added a dev-profile override for the local `asupersync` path dependency to
  reduce debug compile cost during workspace release gates.

### Release Notes

- `asupersync 0.3.5` must be available on crates.io before this workspace can be
  published to crates.io without path dependencies.
- The publishable crates remain the `fsqlite` / `fsqlite-*` crates; harness and
  e2e crates stay `publish = false`.

---

## [0.1.8] -- 2026-06-05 (asupersync 0.3.2 production-context fix; full-workspace release)

Full-workspace lockstep release (`0.1.7 → 0.1.8`). Ships the fix for a build
regression in 0.1.7 that broke **fresh downstream installs**.

### Fixed

- **`fsqlite-types` no longer mints asupersync request contexts in production
  code** — [#108](https://github.com/Dicklesworthstone/frankensqlite/issues/108),
  fixed on `main` in
  [`57e89a94`](https://github.com/Dicklesworthstone/frankensqlite/commit/57e89a94)
  ("fix(native): stop minting asupersync request contexts").

  0.1.7 shipped a `Cx::effective_native_cx` helper (added post-0.1.5) that called
  `NativeCx::for_request_with_budget` from **non-test** code. Under asupersync
  0.3.2 that constructor is `#[cfg(any(test, feature = "test-internals"))]`-gated
  — production consumers are expected to mint request contexts through the
  runtime boundary instead. The frankensqlite workspace's own builds compiled
  because their test profile enables `test-internals`, **masking** the breakage;
  any plain downstream (`default-features` minus `test-internals`) that resolved
  asupersync 0.3.2 failed with:

  ```
  error[E0599]: no function or associated item named `for_request_with_budget`
                found for struct `NativeCx`  (cx.rs:662)
  ```

  Because the published requirement was `asupersync ^0.3.1`, a fresh
  `cargo add fsqlite` resolved asupersync 0.3.2 (not yanked) and **did not
  compile**. Existing consumers survived only because their lockfiles pinned an
  older asupersync. The fix test-gates the helper, requires RaptorQ to use an
  attached/ambient native `Cx`, switches commit-repair to synchronous
  capacity/signaling instead of blocking on asupersync futures, and stops the
  async API from creating synthetic worker/request native contexts. Validated
  with `cargo check -p fsqlite --no-default-features --features 'native fts5'`
  (production, no `test-internals`) — clean.

### Notes for downstream consumers

- This is the version to depend on for any fresh integration of `fsqlite` —
  0.1.7 will fail to compile against asupersync 0.3.2. beads_rust, cass, and
  meta_skill are bumped to 0.1.8.

---

## [0.1.7] -- 2026-06-05 (MVCC transaction-local self-conflict + FTS5 reload regressions; full-workspace release)

Full-workspace lockstep release: every publishable crate is bumped to `0.1.7`
(`0.1.5 → 0.1.7`, with `fsqlite-btree`/`fsqlite-vfs` `0.1.6 → 0.1.7`) and
republished together, so the dependency graph carries a single coherent version
with no inter-crate skew. The two fix families below were committed on `main`
but unreleased; cutting an intermediate `0.1.5`/`0.1.6` release would have
propagated the MVCC regression below to every consumer, so the release waited
until both were resolved. Downstream consumers (beads_rust, cass,
mcp_agent_mail) were blocked on this release.

### Fixed

- **MVCC: spurious `BusySnapshot` self-conflict when a sole `BEGIN EXCLUSIVE`
  writer grows the database** —
  [#106](https://github.com/Dicklesworthstone/frankensqlite/issues/106),
  fixed in
  [`ab4fa4d0`](https://github.com/Dicklesworthstone/frankensqlite/commit/ab4fa4d0).
  A single connection (no concurrency) running a schema-migration / table-rebuild
  transaction under `BEGIN EXCLUSIVE` failed deterministically with
  `BusySnapshot { conflicting_pages: "page N > snapshot db_size M (latest: M)" }`
  — the sole writer conflicting with *its own* uncommitted page growth.
  Introduced by the 0.1.5/0.1.6 btree-reload and allocation changes. Root cause:
  the committed-pager refresh and fast-path scan gates keyed only on
  `in_transaction`, which missed savepoints, active transaction borrows, and
  internal statement savepoints; prepared-metadata refreshes and committed
  read-only scans could therefore observe the published pager image while the
  same connection still held uncommitted DDL plus EOF/freelist page allocations,
  rejecting a later intra-transaction write past the snapshot `db_size`. The fix
  adds a single `local_transaction_scope_is_active` predicate, routes the
  committed-pager refresh gates through it, keeps file-backed fast-path scans out
  of explicit transaction scopes, and forces schema/autocommit boundaries to
  publish immediately even when time-travel capture is disabled. Adds
  `crates/fsqlite/tests/issue_106_rebuild_db_size.rs` (rebuild-then-read, a
  beads-like 38-column rebuild + index burst, and a single-grow variant) — all
  three fail against 0.1.5 and pass here.

- **FTS5: command-column deletes, `WITHOUT ROWID` reload, and order-by fast-path
  regressions** —
  [#99/#102/#103](https://github.com/Dicklesworthstone/frankensqlite/issues/102),
  fixed in
  [`af4abb27`](https://github.com/Dicklesworthstone/frankensqlite/commit/af4abb27).
  Live FTS5 command-column INSERTs (the magic-row delete form) are now detected
  before ordinary virtual-table INSERT dispatch, validated, routed through the
  live `xUpdate` delete path, and counted correctly, while regular user columns
  named like the command column stay on the normal insert path. Populated
  `WITHOUT ROWID` tables (including FTS5 `_config`/`_idx` shadow tables) load
  through index-btree payload records with synthesized stable rowids so they
  survive reopen, and the MemDB `SimpleOrderByLimit` fast path no longer
  mis-claims ordering it cannot satisfy.

- **FTS5: lazy-bind persisted shadow segments instead of full re-tokenization on
  reopen** — fixed in
  [`39eaa54f`](https://github.com/Dicklesworthstone/frankensqlite/commit/39eaa54f).
  A reopened on-disk index now binds its existing posting-list segments
  (rowid-walkable `_data` + `_docsize` shadow tables, resolving content via the
  stored/external/contentless rules SQLite FTS5 applies) instead of
  re-tokenizing the entire `_content` table on every connection — the root cause
  of the multi-hour `cass` open on large corpora. Replaces the previously
  reverted shadow-bind fast path (which was unsafe on the first write and
  `O(N²)` on large corpora) with a correct rebuild-from-postings path.

### Notes for downstream consumers

- **beads_rust** ([#316](https://github.com/Dicklesworthstone/beads_rust/issues/316)):
  the `fsqlite 0.1.3 → 0.1.x` bump previously failed its schema-migration tests
  (`test_migration_adds_missing_*`) with the #106 `BusySnapshot`. Pin
  `fsqlite = "0.1.7"` (and the explicit transitive `fsqlite-*` entries) to clear it.
- **cass** (#266/#268/#269/#271): large populated-DB growth and FTS5 rebuild-pipeline
  wedges were the #106 MVCC self-conflict and the FTS5 reload regressions
  manifesting in cass's `messages` ingest and `watch_startup` rebuild paths.
  Bump to `fsqlite 0.1.7` (feature `fts5`).
- **mcp_agent_mail**: no action required beyond a routine `cargo update`.

---

## [0.1.6] -- 2026-05-28 (fsqlite-btree patch)

Single-crate patch release of `fsqlite-btree` (0.1.5 → 0.1.6). No other crate
versions changed; downstream consumers pick up the fix automatically on next
`cargo update` via caret-semver resolution (`fsqlite-btree = "0.1.5"` → 0.1.6).

### Fixed

- **`BtCursor::prev()` infinite re-read on multi-level index B-trees with
  an empty leftmost-path subtree** — a *secondary* defect from the
  [#95](https://github.com/Dicklesworthstone/frankensqlite/issues/95) fix
  family, found by a second fresh-eyes review pass over the 0.1.5
  `advance_prev` work and committed as
  [`2af6756a`](https://github.com/Dicklesworthstone/frankensqlite/commit/2af6756a)
  (adversarial test) +
  [`3dd0771a`](https://github.com/Dicklesworthstone/frankensqlite/commit/3dd0771a)
  (fix). The 0.1.5 `advance_prev` iterative-loop rewrite eliminated the
  leaf-recovery recursion but left a `return self.advance_prev(cx);`
  recursive self-call on the interior-branch pop path. On an interior page
  X (at `cell_idx=0`) whose left subtree is empty/exhausted, popping X and
  re-entering `advance_prev` from the parent G re-descended into X again
  (because G's `cell_idx` still pointed at the slot from which X had just
  been popped), and `move_to_rightmost_leaf(X)` happily replayed X's
  rightmost leaf — a row already returned earlier in the reverse scan.
  The result was a deterministic infinite replay loop of the form
  `["d", "c", "bb", "b", "bb", "b", ...]` on tree shapes that occur
  naturally after bulk deletes leave interior pages with empty left
  subtrees.

  The fix replaces the recursive self-call with an iterative ascent loop
  bounded by the same `BTREE_MAX_DEPTH * 8` ceiling as the leaf-recovery
  loop. Each iteration either returns the previous separator
  (`parent_cell_idx > 0` → decrement and return `Ok(true)`), pops another
  frame (`parent_cell_idx == 0` → continue), or terminates with `Ok(false)`
  on empty stack. Mirrors the leaf-recovery loop; a `debug_assert!` guards
  against future regressions.

  Adds a 3-level adversarial regression test
  (`test_advance_prev_interior_pop_does_not_recurse_on_empty_leftmost_subtree`)
  that builds the failing tree shape and verifies the reverse scan
  terminates within a bounded iteration count and produces every key
  exactly once. The test fails against 0.1.5 (hits the 32-iter cap) and
  passes against 0.1.6.

### Notes for downstream consumers

- **cass v0.6.4** (released 2026-05-28) was built against `fsqlite 0.1.5`
  and contains the buggy `BtCursor::prev()` interior-pop path. cass usage
  is dominated by forward TABLE-B-tree scans + FTS5 reverse scans;
  reverse INDEX-B-tree scans over the specific failing shape are unusual
  in normal session-search workloads, so most users won't hit the bug
  in v0.6.4. The next routine cass release will pick up `fsqlite-btree
  0.1.6` automatically.

---

## [0.1.5] -- 2026-05-27

Critical forward-progress fix for `BtCursor::next()` on multi-level table
B-trees ([#95](https://github.com/Dicklesworthstone/frankensqlite/issues/95)).
Downstream consumers with non-trivial corpora hung during full forward scans
(e.g. `cass index --full` for any user with more than ~6,000 messages, because
once the `messages` table's B-tree reaches multi-level the cursor could spin
re-reading the same pages indefinitely while `/proc/<pid>/io` showed `rchar`
climb and then plateau with `read_bytes = 0`).

Version bump across all workspace crates for crates.io publish (`fsqlite-vfs`
to 0.1.6).

### Fixed

- **`BtCursor::next()` infinite re-read on multi-level table B-trees**
  ([#95](https://github.com/Dicklesworthstone/frankensqlite/issues/95)).
  `BtCursor::advance_next_impl` used mutual recursion with
  `move_to_leftmost_leaf` plus a recursive recovery branch that, on failure of
  an inner descent (`move_to_leftmost_leaf` returning `false` for an empty
  subtree), restored a `resume_stack`, cleared `at_eof`, and re-entered
  `advance_next_impl`. That recovery path lacked a hard forward-progress
  invariant — on multi-level table B-trees the cursor's stack could be left in
  a state where the empty-stack re-seek from `root_page` (cursor.rs:4302-4305)
  fired during what should have been a forward scan, causing the cursor to
  re-descend from root and re-visit rows it had already returned.

  The fix replaces the recursive recovery with an explicit iterative loop in
  both the table-leaf-exhausted branch and the index-interior branch. The loop
  maintains the invariant that every iteration either returns a row, pops a
  stack frame, or strictly advances `cell_idx` on the current stack top. The
  empty-stack re-seek from `root_page` is preserved at the top of
  `advance_next_impl` for legitimate SQLite-style "before-first" recovery
  (after `prev()` falls off the start, `next()` re-positions at row 1) but is
  never reached from within the loop body. A `debug_assert!`-gated iteration
  ceiling (`BTREE_MAX_DEPTH * 8`) surfaces any latent forward-progress
  regression loudly in tests; in release builds the cursor degrades safely by
  setting `at_eof = true` and returning `false` rather than spinning.

  Adds two regression tests in `crates/fsqlite-btree/src/cursor.rs`:
  `test_advance_next_terminates_on_multi_level_table_btree_frankensqlite_95`
  (6,000-row depth-3 table with INTEGER PK + payload, matches the cass
  `messages`-table shape) and
  `test_advance_next_terminates_on_multi_level_with_empty_subtree_frankensqlite_95`
  (hand-crafted depth-3 tree with an empty middle subtree to exercise the
  recovery path).

- **`BtCursor::prev()` symmetric forward-progress hardening** (defensive,
  same fix family as #95). `BtCursor::advance_prev` used a recursive
  recovery (`return self.advance_prev(cx);`) without snapshotting a
  `resume_stack` — even more fragile than the forward path. Although the
  exhaustion path appeared to terminate (each inner descent strictly
  decrements `parent.cell_idx`), the recursive pattern was the exact one
  the forward fix removed, and any future change leaving the cursor in
  `(stack_empty, at_eof=false)` after a failed
  `move_to_rightmost_leaf` would have triggered the rightmost re-seek
  from `root_page` and replayed rows. Replaced both the leaf-recovery
  loop and the index-interior recovery with an iterative pattern that
  matches `advance_next_impl`: `resume_stack` snapshot/restore around
  every `move_to_rightmost_leaf` call, `BTREE_MAX_DEPTH * 8` iteration
  ceiling, `debug_assert!`-gated regression detection, and safe
  degradation in release builds. Adds two regression tests:
  `test_advance_prev_terminates_on_multi_level_table_btree_frankensqlite_95`
  (6,000-row reverse scan) and
  `test_advance_prev_terminates_on_multi_level_with_empty_subtree_frankensqlite_95`
  (hand-crafted depth-3 tree with empty middle subtree).

## [0.1.4] -- 2026-05-26

FTS5 join and delete-all correctness fixes for downstream consumers
(`coding_agent_session_search`/cass, `destructive_command_guard`/dcg). Version
bump across all workspace crates for crates.io publish (`fsqlite-vfs` to 0.1.5).

### Fixed

- **FTS5 / virtual-table join projection width**
  ([#93](https://github.com/Dicklesworthstone/frankensqlite/issues/93)).
  `join_table_supports_hidden_rowid` no longer counts a phantom hidden-rowid
  column for virtual tables, so `scan_width()` matches the materialized row.
  This eliminates the `range end index N out of range for slice of length M`
  panic in `execute_join_select` (and the accompanying wrong/empty column
  ordering in FTS5 bm25 join results) for queries whose left-hand side is an
  FTS5 virtual table.
  [`e3714db5`](https://github.com/Dicklesworthstone/frankensqlite/commit/e3714db5a16a224e93bf874b46d99062145b4145),
  with a self-diagnosing `debug_assert` guard at the join slice
  [`c065aa07`](https://github.com/Dicklesworthstone/frankensqlite/commit/c065aa073).
- **FTS5 delete-all + re-insert**
  ([#94](https://github.com/Dicklesworthstone/frankensqlite/issues/94)).
  `DELETE FROM <fts5>` with no `WHERE` clause now routes through the module's
  per-row `xUpdate` delete via `Connection::execute_live_vtab_delete`,
  enumerating live rowids and clearing the in-memory `Fts5Table` state in
  lockstep with the backing storage. The `DELETE FROM <fts>; <re-INSERT each
  rowid>` rebuild pattern no longer trips `PrimaryKeyViolation` in stored,
  contentless, or external-content modes. FTS5 rusqlite conformance 59/59.
  [`a0425adb`](https://github.com/Dicklesworthstone/frankensqlite/commit/a0425adb)

## [0.1.3] -- 2026-05-02

Version bump across all workspace crates for crates.io republish
(`fsqlite-vfs` 0.1.3 → 0.1.4, adds `native` feature).

[`75f380eb`](https://github.com/Dicklesworthstone/frankensqlite/commit/75f380eb),
[`992d54ea`](https://github.com/Dicklesworthstone/frankensqlite/commit/992d54ea)

## [0.1.2] -- 2026-03-21

Version bump across all 26 workspace crates for crates.io republish.

[`93f1f55f`](https://github.com/Dicklesworthstone/frankensqlite/commit/93f1f55f34a377eb8615172d7985bb5140780b2e)

## [0.1.1] -- 2026-02-21

Initial version bump from 0.1.0 across all crates. Added crates.io metadata and
version specifiers for publishing.

[`8ae63da9`](https://github.com/Dicklesworthstone/frankensqlite/commit/8ae63da9e812cc0fb4dc70a24dd624f3ae126cd4),
[`508d2cd8`](https://github.com/Dicklesworthstone/frankensqlite/commit/508d2cd8d39d3eaadea5a32229f1ea020c0b6cf0)

## [0.1.0] -- 2026-02-06

Project inception. Workspace infrastructure, foundation crates (`fsqlite-types`
with 64 tests, `fsqlite-error` with 13 tests), and stub crates for all 23
subsystems.

[`a137671e`](https://github.com/Dicklesworthstone/frankensqlite/commit/a137671e2e7c4b25547d24e540d72f69a5c9efe1)
through
[`b559f58e`](https://github.com/Dicklesworthstone/frankensqlite/commit/b559f58e426d995f4ba101ecd80096977b9834f4)

---

## Development Log (pre-release, by capability)

### Specification and Architecture Design

A comprehensive specification was developed and evolved through 10+ deep audit
rounds before any engine code was written (2026-02-06 through 2026-02-25).

- **Formal MVCC specification** with proofs and implementation order.
  [`8841a3ec`](https://github.com/Dicklesworthstone/frankensqlite/commit/8841a3ec70cac0eec5ea626186d435ffd4287795)
- **Comprehensive specification documents** (8,628 + 1,206 lines).
  [`c08f1602`](https://github.com/Dicklesworthstone/frankensqlite/commit/c08f1602d03b1833a4f91c8f77347f8f196bac9d)
- **RFC 6330 (RaptorQ)** reference document.
  [`c293739f`](https://github.com/Dicklesworthstone/frankensqlite/commit/c293739fccb9d88a948f1d151b8fcf877424760d)
- **Spec V1.3**: scope doctrine, ECS substrate, multi-process MVCC, encryption.
  [`9800b17d`](https://github.com/Dicklesworthstone/frankensqlite/commit/9800b17df4a56c2dc065cf566c2810d4ed2e576c)
- **Spec V1.4**: Codex synthesis -- RaptorQ everywhere, WAL sidecar overhaul,
  ECS layout, replication.
  [`5ad34871`](https://github.com/Dicklesworthstone/frankensqlite/commit/5ad34871f7242de61378843c6c1e8311e35d9fa3)
- **Spec V1.5**: alien-artifact discipline -- decision-theoretic SSI, BOCPD,
  monitoring stack, native mode.
  [`7b2c677c`](https://github.com/Dicklesworthstone/frankensqlite/commit/7b2c677cf61adda977e71524b59d7ec234137962)
- **Spec V1.6a-h**: SSI detection algorithm with proof-carrying commit, arena
  allocators, CAR cache, RaptorQ-native SSI witness plane, native mode commit
  protocol.
  [`bf042641`](https://github.com/Dicklesworthstone/frankensqlite/commit/bf0426417685504bb2b2f5acfc4de2c2f087ef8b)
  through
  [`0404e42c`](https://github.com/Dicklesworthstone/frankensqlite/commit/0404e42c9a46cc2e82e1e77e16af18e1a4c2fb80)
- **Spec V1.7a-j**: 10 deep audit rounds covering MVCC formal model (Sec 5),
  buffer pool ARC cache (Sec 6), checksums/integrity (Sec 7), BtreeCursorOps
  (Sec 9), lexer/cost model (Sec 10), SQL coverage (Sec 12), strftime/aggregate
  ORDER BY (Sec 13), FTS5 (Sec 14), Asupersync e-process math (Sec 4), and
  RaptorQ MTU/sub-blocking (Sec 3).
  [`d7b38efe`](https://github.com/Dicklesworthstone/frankensqlite/commit/d7b38efea49b120b3f7f24e80e6e35eae1f6b7e2)
  through
  [`a3e7ae52`](https://github.com/Dicklesworthstone/frankensqlite/commit/a3e7ae52dc8cbe12b2da444e0ac4e90bf7a66ba4)
- **Spec evolution visualization** -- interactive viewer deployed to Cloudflare
  Pages with dataset tooling, clustering, heat stripes, and story mode.
  [`311b7db9`](https://github.com/Dicklesworthstone/frankensqlite/commit/311b7db917a6e97e99f09e78e5e6a45cff9a61f1)
- **Beads issue tracker** initialized with 92 work items, grew to 458+ tracked
  tasks across all phases.
  [`be5dc72e`](https://github.com/Dicklesworthstone/frankensqlite/commit/be5dc72edf86b3f831eea68d729cb5aed0a43034)

---

### MVCC Concurrent Writers and SSI

The core differentiating feature: page-level Multi-Version Concurrency Control
with Serializable Snapshot Isolation replacing SQLite's single-writer lock.

#### Transaction Lifecycle and Core MVCC

- **MVCC core types, AST, and capability context** (Cx).
  [`a2ce704a`](https://github.com/Dicklesworthstone/frankensqlite/commit/a2ce704a27b1afb47f4f1de348ee16e0b2fcabed)
- **MVCC transaction lifecycle** with formal invariants.
  [`362ea4bb`](https://github.com/Dicklesworthstone/frankensqlite/commit/362ea4bbb3eb455b8a2a0e25a38e1c30a2d2f31c),
  [`c62cde5a`](https://github.com/Dicklesworthstone/frankensqlite/commit/c62cde5aa25a9e7b7dac08fd1f1f5e06c1c9e8f7)
- **BEGIN CONCURRENT** wired through ConcurrentRegistry.
  [`b8e34e01`](https://github.com/Dicklesworthstone/frankensqlite/commit/b8e34e01ce8e7a02b33c15cd9a94aa4b3a24e74f),
  [`e803849a`](https://github.com/Dicklesworthstone/frankensqlite/commit/e803849a232f8452bd0daf27305bb2a1de356895)
- **MVCC page-level locking** for concurrent transactions.
  [`43bde5a1`](https://github.com/Dicklesworthstone/frankensqlite/commit/43bde5a1c8a5e8be2be94a5e1b1e3cdc04b7e58d)
- **Page-level MVCC conflict detection** and SSI cycle validation.
  [`5439982a`](https://github.com/Dicklesworthstone/frankensqlite/commit/5439982ae0e4cf65b56d44bf35bb9206472dbc1f)
- **`PRAGMA concurrent_mode`** toggle and `BEGIN CONCURRENT`.
  [`8883ce4b`](https://github.com/Dicklesworthstone/frankensqlite/commit/8883ce4b27a1ebf29e9eaa8e9f397b2ab5a37bc1)
- **Version-chain length controls** with eager GC and backpressure.
  [`ef3a472e`](https://github.com/Dicklesworthstone/frankensqlite/commit/ef3a472e683b6b4ef12483dec7271674e4a7b207)
- **Per-handle `Arc<Mutex<ConcurrentHandle>>`** replacing registry-wide Mutex.
  [`1f915617`](https://github.com/Dicklesworthstone/frankensqlite/commit/1f91561769303b2bb94b06e3e94bc0b08e3dfb08)

#### SSI Validation and Conflict Detection

- **Commit-time SSI validation** with proof-carrying artifacts.
  [`d1b1f696`](https://github.com/Dicklesworthstone/frankensqlite/commit/d1b1f6966c62e2ecc58cb2d0f9b6af54e7eb8ec2)
- **SSI witness objects** with hot/cold plane discovery.
  [`235fc953`](https://github.com/Dicklesworthstone/frankensqlite/commit/235fc953ebcccbc08a7e1a20cfbe62ebc3e9e9b4)
- **FCW conflict detection** with GF(256) rebase and SharedPageLockTable.
  [`634ac590`](https://github.com/Dicklesworthstone/frankensqlite/commit/634ac590cdc1f26e6bc8a07a4df3da63e1a4c75e)
- **SharedPageLockTable** with rolling rebuild protocol.
  [`ab4c8ba8`](https://github.com/Dicklesworthstone/frankensqlite/commit/ab4c8ba8ce4eeb7ea23b3a60b4a458f3e7da6839)
- **Distributionally Robust Optimization (DRO)** layer for SSI T3 abort
  decisions with sliding-window radius estimation.
  [`10b5e45c`](https://github.com/Dicklesworthstone/frankensqlite/commit/10b5e45cf45a1691d420784af7249617070f54b4),
  [`d598a108`](https://github.com/Dicklesworthstone/frankensqlite/commit/d598a108e4e977197e2769388a3be3a941064303)
- **SSI CommittedPivot detection**, ghost epoch tracking, cache invalidation.
  [`d53bd9c6`](https://github.com/Dicklesworthstone/frankensqlite/commit/d53bd9c6e8c5f4f7dfecf3564b4665fe33601a75)
- **CommitIndex migrated to left-right publication** for lock-free reads.
  [`1efe6740`](https://github.com/Dicklesworthstone/frankensqlite/commit/1efe6740b0ed321d74ee58351a186a898e0c7316)
- **Shared conflict observer** and always-on column defaults.
  [`bacfdcc4`](https://github.com/Dicklesworthstone/frankensqlite/commit/bacfdcc4285819b6dc4313777e86ca444d3b16b1)

#### Cell-Level MVCC Visibility (Track D, late March)

Finer-grained visibility tracking at the cell level rather than the page level.

- **Cell-level MVCC visibility system** -- delta WAL, structural/logical
  boundary design.
  [`0094bdab`](https://github.com/Dicklesworthstone/frankensqlite/commit/0094bdab036de0cebfc0d25ec50f8637f7c912c7)
- **Cell-level visibility log** and structural page tracking (C4).
  [`25c651e5`](https://github.com/Dicklesworthstone/frankensqlite/commit/25c651e5d57b6e5a38d4c2cd51f9e98ccf2d88c0)
- **Cell-level delta commit module** in WAL.
  [`386d641d`](https://github.com/Dicklesworthstone/frankensqlite/commit/386d641d184c2b439d178d554db80d1439d87ce1)
- **Deferred EBR slot recycling** plumbing.
  [`d2cb6619`](https://github.com/Dicklesworthstone/frankensqlite/commit/d2cb6619aca52e3b32a0e0e4dd97e7e7baebe03f)

#### Epoch-Based Reclamation (EBR) and GC

- **Epoch-based reclamation module** for safe concurrent version store cleanup.
  [`f050a132`](https://github.com/Dicklesworthstone/frankensqlite/commit/f050a132310387e31014233c109801ee4ddaae90)
- **MVCC GC wiring** complete.
  [`0b755289`](https://github.com/Dicklesworthstone/frankensqlite/commit/0b7552899f62515ddb8a6f9d2ea93c6e2e9cfe51)
- **Lock-free CAS-based chain head table**.
  [`f5525fbc`](https://github.com/Dicklesworthstone/frankensqlite/commit/f5525fbc3cd3e9a065db98e119a40f19e1f2e56a)
- **GC-horizon index**, MVCC write profiling, version chain retention fix.
  [`cc725ccc`](https://github.com/Dicklesworthstone/frankensqlite/commit/cc725cccd3e33e2e469c1bbaa9f3e43f462b7f36)
- **Active snapshot refcounts** for correct GC horizon caching.
  [`29abdee5`](https://github.com/Dicklesworthstone/frankensqlite/commit/29abdee5dd17ba118ed7b5e5b1f7e18b3f5f7d25)
- **History compression** and merge certificates.
  [`40865985`](https://github.com/Dicklesworthstone/frankensqlite/commit/40865985f43f0e1eee39e1cf22f2a36e9bfae99e)

#### Transaction Lifecycle Observability

- **`PRAGMA fsqlite_txn_stats`**, `PRAGMA fsqlite_transactions`, `PRAGMA
  fsqlite_txn_advisor`, `PRAGMA fsqlite_txn_timeline_json` -- full transaction
  lifecycle introspection.
  [`855eaabf`](https://github.com/Dicklesworthstone/frankensqlite/commit/855eaabfef09b5f2e2a3e0bcff06e1e73b1f4afe)
- **Adaptive checkpoint scheduling** with advisor PRAGMAs.
  [`191ecaeb`](https://github.com/Dicklesworthstone/frankensqlite/commit/191ecaeba903077a95e0c27f4f33ba8d21c18690)

---

### Parallel WAL and Group Commit

The single biggest architectural push (Track D, 2026-03-17 through 2026-03-21).
Introduced infrastructure for multiple concurrent WAL writers and a group commit
protocol that pipelines epoch flushes.

- **Lock-free per-thread WAL buffers (D1)** -- each writer thread gets a
  dedicated WAL buffer, eliminating contention on the shared WAL append path.
  [`bf1466ce`](https://github.com/Dicklesworthstone/frankensqlite/commit/bf1466ceee2d201bbba63dba7464f7f7bcdbc7de)
- **Background epoch ticker (D1.5)** -- dedicated thread advances the global
  epoch.
  [`0cdc48ce`](https://github.com/Dicklesworthstone/frankensqlite/commit/0cdc48ceb1387c41b2a4d673ae7d36b4c89f8d3d)
- **Segment file I/O and recovery (D1.6, D1.7)** -- parallel WAL backed by
  segment files with a recovery path.
  [`fa2745f4`](https://github.com/Dicklesworthstone/frankensqlite/commit/fa2745f4f3266129d46ceedc656d66eeb6cee6e3),
  [`712dc88a`](https://github.com/Dicklesworthstone/frankensqlite/commit/712dc88a57be3031fb56bc3d7b799ab81a1a07ac)
- **D2 ShardedPageCache** -- 128-partition page cache for thread scalability.
  [`ca3caf26`](https://github.com/Dicklesworthstone/frankensqlite/commit/ca3caf26608754fe1af7b3f3dd543ef0bbf59ea5)
- **D3 CommitSequenceCombiner** -- batched commit sequence allocation via
  flat-combining.
  [`97e98c83`](https://github.com/Dicklesworthstone/frankensqlite/commit/97e98c83585382ec1790413678fb699c5c830072)
- **Split-lock commit protocol (D1-CRITICAL)** -- separates the commit surface
  from the conflict surface so WAL growth does not block conflict detection.
  [`1e4d6379`](https://github.com/Dicklesworthstone/frankensqlite/commit/1e4d637942d31d58dc9d0898aa5829494e36267e)
- **Epoch pipelining** -- eliminates `flushing_wait` bottleneck in group commit.
  [`a17ba22a`](https://github.com/Dicklesworthstone/frankensqlite/commit/a17ba22ae7a618c6ea0ddf035354d8561d524fb4)
- **Page 1 conflict elimination** -- header page no longer a mandatory conflict
  surface.
  [`b97a3b77`](https://github.com/Dicklesworthstone/frankensqlite/commit/b97a3b777797b36cc0cbb1e32f52abdbd2c8a504)
- **RwLock WAL backend** -- Mutex to RwLock migration for concurrent page reads.
  [`cfd60a53`](https://github.com/Dicklesworthstone/frankensqlite/commit/cfd60a538af0657ade86edbff01102bd00ffdee5)
- **Major connection expansion**, pager improvements, group commit scaling.
  [`42208411`](https://github.com/Dicklesworthstone/frankensqlite/commit/42208411e94e7696e9557ffd3dd6b762d4cac156)
- **Commit path phase timing** (A/B/C1/C2) benchmarking.
  [`0778a8f6`](https://github.com/Dicklesworthstone/frankensqlite/commit/0778a8f6fd8b4104c2a4a825e46547e35ca91262)

---

### Write-Ahead Log (WAL)

#### WAL Core Implementation

- **WAL header parsing, frame I/O, and index header types**.
  [`2871974a`](https://github.com/Dicklesworthstone/frankensqlite/commit/2871974a5c2c994ffb4f28fb2e3cfe2e57e5be37)
- **WAL checksum, index, and test infrastructure**.
  [`c88da7c2`](https://github.com/Dicklesworthstone/frankensqlite/commit/c88da7c2b55e23beb4f95e0e117b20b3b11e9e3e)
- **Checkpoint executor and implementation modules**.
  [`3915f847`](https://github.com/Dicklesworthstone/frankensqlite/commit/3915f847c3a2d8cb42fc29c2b21aa5b4ceb2e299)
- **WAL checkpoint integration** into pager.
  [`553c3b9f`](https://github.com/Dicklesworthstone/frankensqlite/commit/553c3b9fc96f19e4e21fbb3e8e6e7f1e6d19e3ef)
- **Pin WAL read snapshot at transaction begin** to prevent visibility drift.
  [`42011dd8`](https://github.com/Dicklesworthstone/frankensqlite/commit/42011dd89e1a0ff6e099c6cd1ade65be9e02bb74)
- **Two-pass checkpoint deduplication** for sequential page writes.
  [`b3953199`](https://github.com/Dicklesworthstone/frankensqlite/commit/b39531996ac2e2b48f2266ae1e10e18e5eef24a2)

#### WAL Recovery and Hardening

- **WAL-recovery for stale main-file headers** and read-only WAL backend install.
  [`6da92596`](https://github.com/Dicklesworthstone/frankensqlite/commit/6da9259684813dfe2ed0e5ff62c0edcd971b9ffa)
- **Centralized WAL backend installation** with page-size validation.
  [`ea2ff736`](https://github.com/Dicklesworthstone/frankensqlite/commit/ea2ff736c5186aa3132c475dc79fb6bf9a42ea40)
- **Crash-loop replay determinism test** for WAL recovery.
  [`3675601a`](https://github.com/Dicklesworthstone/frankensqlite/commit/3675601a82d48a6979dd99c0dd54e9e49023d96c)
- **Absorb frames only up to last commit boundary** and detect ABA resets.
  [`06155f84`](https://github.com/Dicklesworthstone/frankensqlite/commit/06155f84e97b55b5bf2e7330e4cf68f4816e2c5d)
- **WAL page index ABA hazard prevention** via generation identity tracking.
  [`2df16c8e`](https://github.com/Dicklesworthstone/frankensqlite/commit/2df16c8e3eebe276fb4da05393681f0dcfa80a0b)
- **WAL checksum accumulator order** correction.
  [`c7ccc0be`](https://github.com/Dicklesworthstone/frankensqlite/commit/c7ccc0be2ed8d3643c5c359f67f962b1bcbae6df)
- **Split prepared-frame append** into pre-lock finalize and durable write
  phases.
  [`ea3e9e00`](https://github.com/Dicklesworthstone/frankensqlite/commit/ea3e9e0005c4ed8325f262b4632f5d8057518a73)

#### WAL-FEC (Forward Error Correction)

- **Fountain-coded WAL recovery** with decode proofs (RaptorQ).
  [`2ee3f10f`](https://github.com/Dicklesworthstone/frankensqlite/commit/2ee3f10f3c4f8148a0feab63e9f57c375acb08e4)
- **WAL-FEC sidecar format**.
  [`58db07c7`](https://github.com/Dicklesworthstone/frankensqlite/commit/58db07c7c00bd9a2f3b3e3bb5f15b9e2a2dbc75e)
- **Pipelined WAL-FEC repair generation**.
  [`d57e1693`](https://github.com/Dicklesworthstone/frankensqlite/commit/d57e16931fc2af5e4ebe2c2fd34bc88f5fc8f64b)
- **WAL-FEC RaptorQ repair symbols**.
  [`2ac8b760`](https://github.com/Dicklesworthstone/frankensqlite/commit/2ac8b760d7d8b94bcf0ddf9e37c03a2a75e7f505)

---

### Pager and Page Cache

- **ARC-based page cache** with adaptive scan resistance.
  [`8e0e7031`](https://github.com/Dicklesworthstone/frankensqlite/commit/8e0e7031f1cf3dc86c2c2b3e72b2cf9c6b2c0db8)
- **S3-FIFO page cache** with LRU/ARC benchmark harness.
  [`e7389ffb`](https://github.com/Dicklesworthstone/frankensqlite/commit/e7389ffb1e2e4c3c2fddecfcb01c3c3c3dfc4f73)
- **Rollback journal format** with lock-byte page support.
  [`0cba28bb`](https://github.com/Dicklesworthstone/frankensqlite/commit/0cba28bbb82b0ff4f96be73ffa5984fbe3f52e6b)
- **Freelist serialization** into write set instead of direct I/O.
  [`70818421`](https://github.com/Dicklesworthstone/frankensqlite/commit/70818421fd3c06b32a8fbfb7f61ff0fa8d69fb2f)
- **Persist freelist to SQLite freelist pages**.
  [`a7c95f42`](https://github.com/Dicklesworthstone/frankensqlite/commit/a7c95f4203051ec2d9be253ffdd3c24c24501f89)
- **Page1 header patching** on commit to prevent malformed DB.
  [`1ab7cee6`](https://github.com/Dicklesworthstone/frankensqlite/commit/1ab7cee6670ba87a69a3eaf1fc8ca307f15b7a03)
- **MVCC snapshot db_size boundary guard** to prevent corruption.
  [`4202ea30`](https://github.com/Dicklesworthstone/frankensqlite/commit/4202ea3093c1c04f38b8d0b35f91cabe3e8d0bab)
- **Batch-allocate EOF pages** to reduce mutex contention.
  [`878e8215`](https://github.com/Dicklesworthstone/frankensqlite/commit/878e8215f63de2758af6f9d760dadf7edc877539)
- **Per-transaction page read cache** to eliminate `inner.lock` contention.
  [`cc8a47aa`](https://github.com/Dicklesworthstone/frankensqlite/commit/cc8a47aadbf9ee5261854065fcb8ca2e6251121e)
- **Cache-line-striped atomics** for publication counters.
  [`e7612b1b`](https://github.com/Dicklesworthstone/frankensqlite/commit/e7612b1b6e5b4f72d48c5fead4eff42ccf7eb58d)
- **Separate conflict surface from commit surface** for concurrent WAL growth.
  [`f74c5d55`](https://github.com/Dicklesworthstone/frankensqlite/commit/f74c5d55a3ee7c1bff67f2d1a20b1f7c31b6e5cc)

---

### B-Tree Engine

- **B-tree scaffold** -- cursor, cell, balance, overflow, freelist, and payload.
  [`239e16a6`](https://github.com/Dicklesworthstone/frankensqlite/commit/239e16a6a4b6fa5a78cbfc1a24f7f73a25d33637)
- **N-ary split** for root node overflow.
  [`33551179`](https://github.com/Dicklesworthstone/frankensqlite/commit/33551179dbc1b3cf2bb8a476aa34e962523a2fc4)
- **Balance-shallower root collapse**.
  [`b29a4ae0`](https://github.com/Dicklesworthstone/frankensqlite/commit/b29a4ae0a68aee5e10bd27b85b209faaeddd5c78)
- **UNIQUE index enforcement** and record comparison semantics.
  [`fff4cbb5`](https://github.com/Dicklesworthstone/frankensqlite/commit/fff4cbb52fb948ef624f82fc23f372f19699d909)
- **Interior-node deletion** with rebalance.
  [`455043bd`](https://github.com/Dicklesworthstone/frankensqlite/commit/455043bdd0d5dc3045fdd44a69d463d77f7cc6cf)
- **SwissIndex SIMD hash map** integrated into VdbeEngine and MemDatabase.
  [`61ced98e`](https://github.com/Dicklesworthstone/frankensqlite/commit/61ced98ed66bd73c2f98b8e5d66f7d69e6ae3b58)
- **60/40 biased leaf split**, cursor `last_insert_rowid`, overflow fixes.
  [`2ff3a888`](https://github.com/Dicklesworthstone/frankensqlite/commit/2ff3a888f1c2c6c7e1e5f00ac3ae3ccbcfc5e098)
- **Safe prefetch hints** in cursor descent.
  [`2a2434cf`](https://github.com/Dicklesworthstone/frankensqlite/commit/2a2434cf3a17cf4d0d01413ad1f47ee0bb60f76a)
- **O(n) slope-constraint PLA** replacing O(n*k) brute-force segment training
  in learned index.
  [`22fb1dae`](https://github.com/Dicklesworthstone/frankensqlite/commit/22fb1daedc5140ba79d75233ed2ebbfb9e531322)
- **Handle oversized interior cell replacement** via structural rebalance.
  [`f417dcad`](https://github.com/Dicklesworthstone/frankensqlite/commit/f417dcad546aff15269ec19ce0c271d0a118057e)
- **Replace MockBtreeCursor with real BtCursor** for storage cursors.
  [`d71450f1`](https://github.com/Dicklesworthstone/frankensqlite/commit/d71450f1d6bc6b6f8a2be37a86a765f02b4c2dfb)

---

### Virtual File System (VFS)

- **Unix VFS** with full file locking, SHM, and memory VFS.
  [`37ff9bdf`](https://github.com/Dicklesworthstone/frankensqlite/commit/37ff9bdf6ce4d6a5a7ae0e55f42f0b22e2c53de0)
- **Windows VFS** backend and cross-platform libc type compatibility.
  [`38e81bac`](https://github.com/Dicklesworthstone/frankensqlite/commit/38e81bac4f2b2b37ed2b8277f7e65f2a9b6f2f2e),
  [`dd92a350`](https://github.com/Dicklesworthstone/frankensqlite/commit/dd92a350a55d9dbd5dfe1ee3ff3be8d7e22cf1c7)
- **Mmap-based SHM layer** for multi-process WAL correctness.
  [`98cc42c3`](https://github.com/Dicklesworthstone/frankensqlite/commit/98cc42c36bd5c2d4f8ca3e7be97be81ff3ddecbb)
- **Cross-process file locking** for WAL write safety.
  [`20b1d153`](https://github.com/Dicklesworthstone/frankensqlite/commit/20b1d153830218d1c193f9c23e4e27e3e66a3efb)
- **io_uring backend** with asupersync integration, runtime disable-on-failure,
  and SHM lock fixes.
  [`a26db876`](https://github.com/Dicklesworthstone/frankensqlite/commit/a26db87636af01792f7a948c9808d6a3d073bdc9)
- **io_uring wired as default Linux pager backend**.
  [`00f4a6ac`](https://github.com/Dicklesworthstone/frankensqlite/commit/00f4a6ac64cfb3cfdc597528435223ca02c2d238)
- **TracingFile wrapper** and VfsMetrics for observability.
  [`b8658d7f`](https://github.com/Dicklesworthstone/frankensqlite/commit/b8658d7f4e2e4b9e1eadabe98ddabe4f0a0d2619)

---

### SQL Parser

- **Complete SQL lexer and parser** -- hand-written recursive descent with Pratt
  expression parsing.
  [`c70c530b`](https://github.com/Dicklesworthstone/frankensqlite/commit/c70c530bf717ca052908be1770c770592944fb46)
- **Semantic analysis** with parse metrics and SQLite-compat lexer fixes.
  [`bd302d93`](https://github.com/Dicklesworthstone/frankensqlite/commit/bd302d93e47f8c00b3a52d1f2cfd26ab25a63a2a)
- **IS [NOT] DISTINCT FROM**, NOT NULL postfix, NULL constraint, block comments.
  [`bbee1add`](https://github.com/Dicklesworthstone/frankensqlite/commit/bbee1addded154cfb1f0cde8a1e2c44f97898b3f)
- **SQL:2011 temporal query parsing** (`FOR SYSTEM_TIME AS OF`).
  [`29f6dbea`](https://github.com/Dicklesworthstone/frankensqlite/commit/29f6dbeabd2f52fe1d734972c678b59d1c3281f1)
- **Parser recursion guard** RAII helper.
  [`04bb7818`](https://github.com/Dicklesworthstone/frankensqlite/commit/04bb7818ac7bebb2c8a339e1e7f0a13b8f1f7ca5)
- **Proptest property suites** for parser round-trip.
  [`044c683e`](https://github.com/Dicklesworthstone/frankensqlite/commit/044c683e17adf3e94c5aba03cac32e3d44b02cff)

---

### Query Planner

- **Cost-based query planner** with join reordering and selectivity estimation.
  [`7a5f6f47`](https://github.com/Dicklesworthstone/frankensqlite/commit/7a5f6f4799651ce53bddd42b32ddefaf123bd0bf)
- **Beam search join ordering** (NGQP-inspired).
  [`ef9ba57e`](https://github.com/Dicklesworthstone/frankensqlite/commit/ef9ba57e6f0f88d4e92e5b7fdff4d5cc6a4c8a36)
- **Partial index and expression index** support.
  [`b6715fea`](https://github.com/Dicklesworthstone/frankensqlite/commit/b6715fea741ba96da23a2cfe5f5e76edec6bdb40),
  [`4a857675`](https://github.com/Dicklesworthstone/frankensqlite/commit/4a857675a962e35443d9e5918a868cd1d0a13dd1)
- **Skip scan index access path**.
  [`d15f40ec`](https://github.com/Dicklesworthstone/frankensqlite/commit/d15f40ec5b83e8f70fba6eea3b56b7ffa58a5a6d)
- **WHERE predicate pushdown** to primary table scan in multi-table joins.
  [`7e70d9c7`](https://github.com/Dicklesworthstone/frankensqlite/commit/7e70d9c7e87b5fcfbcfb29d5d05e05c3b8f99d18)
- **INTEGER PRIMARY KEY point lookups** upgraded to SeekRowid.
  [`f61099a6`](https://github.com/Dicklesworthstone/frankensqlite/commit/f61099a64e7949a4aa86ed44f84e60b50ed2a0d3)
- **LIKE prefix range optimization** with upper bound computation.
  [`a7fe176c`](https://github.com/Dicklesworthstone/frankensqlite/commit/a7fe176cdf1cc2b0d6d1de3a38e75b26fb70a8a6)
- **ANALYZE/REINDEX** with `sqlite_stat1` support.
  [`825cb634`](https://github.com/Dicklesworthstone/frankensqlite/commit/825cb6349980c1e3fdb2f5200883fb5ad6e8f005)

---

### VDBE (Virtual Database Engine)

#### Bytecode Engine

- **VDBE program builder**, label system, and coroutines.
  [`9df8fa4c`](https://github.com/Dicklesworthstone/frankensqlite/commit/9df8fa4cf85deb4ce05dd2f3fda1c8a3aee1b3e0)
- **190+ opcodes** implemented across multiple landing commits.
  [`6c4d9664`](https://github.com/Dicklesworthstone/frankensqlite/commit/6c4d966471b57389f2b79da38ba2b8cc2a0fa740)
- **Cursor-level decode cache** with hit/miss instrumentation.
  [`88c650d0`](https://github.com/Dicklesworthstone/frankensqlite/commit/88c650d06702edfe44adf9c69a9562f4fccd3fa6)
- **Cached VDBE engine reuse**, budgeted SSI evidence, B-tree overflow
  refinements.
  [`81aeb3cf`](https://github.com/Dicklesworthstone/frankensqlite/commit/81aeb3cf51eeb27f3e7a8df0f87e7b1c4b093cfa)
- **ReadCookie/SetCookie opcodes** and NewRowid write-through.
  [`6996c1b8`](https://github.com/Dicklesworthstone/frankensqlite/commit/6996c1b8c4e79aa3edeacbfb57d29f36b40eff11)

#### Vectorized Execution

- **Vectorized hash-join operator** with inner/left/semi/anti variants.
  [`408b419f`](https://github.com/Dicklesworthstone/frankensqlite/commit/408b419f7c1a8de05aa3c8f4aecb8a3fc10e7d0e)
- **Vectorized aggregation operator** with hash and ordered paths.
  [`168b9dc7`](https://github.com/Dicklesworthstone/frankensqlite/commit/168b9dc79060b0f90d2c58d8ce2cf2bfca5ad04e)
- **Vectorized filter** with selection composition and SIMD tracking.
  [`60e1ce6a`](https://github.com/Dicklesworthstone/frankensqlite/commit/60e1ce6a8cd1d1d4b2d6bb4c08d4bf06b2ab0e56)
- **External merge sort** with spill-to-disk.
  [`a477373f`](https://github.com/Dicklesworthstone/frankensqlite/commit/a477373f6e3c3d6f5fcb1df6ea6c2af3e88c7e00)
- **Morsel-driven parallel execution** with exchange operators.
  [`d4d4615e`](https://github.com/Dicklesworthstone/frankensqlite/commit/d4d4615e8c16c0a3c7b6c2a5d5ab9c9e2dc9e0f8)
- **L2-aware morsel auto-tuning** and dispatch observability.
  [`46248fb5`](https://github.com/Dicklesworthstone/frankensqlite/commit/46248fb56e1a5cc3d6e1b1f77c1b44e1c5d7a3f0)
- **VDBE JOIN codegen** for simple INNER JOIN queries.
  [`2d0ea19c`](https://github.com/Dicklesworthstone/frankensqlite/commit/2d0ea19ca0f1c1c8b8d1c6f14e4eba89a7e7d54c)

---

### SQL Feature Coverage

#### DML (INSERT/UPDATE/DELETE)

- **INSERT codegen** with INTEGER PRIMARY KEY rowid routing.
  [`6d20abaf`](https://github.com/Dicklesworthstone/frankensqlite/commit/6d20abaf3f5bff88f965d0c12a7e74de9ed15b84)
- **INSERT...SELECT** in VDBE and planner codegen.
  [`b8d13882`](https://github.com/Dicklesworthstone/frankensqlite/commit/b8d138824a1bc1e5df7a52ef39d4e50c0fb6f6de)
- **INSERT OR REPLACE / INSERT OR IGNORE** conflict handling.
  [`778cf161`](https://github.com/Dicklesworthstone/frankensqlite/commit/778cf1618e3c2ad50f6f1c3dc3816bb0c1a2b97d)
- **UPSERT/ON CONFLICT** with DO UPDATE and DO NOTHING.
  [`e753b7bb`](https://github.com/Dicklesworthstone/frankensqlite/commit/e753b7bb0445c690be8c7d85ee698bac8bd0f922)
- **INSERT/UPDATE/DELETE RETURNING** clause support.
  [`b16c2ab6`](https://github.com/Dicklesworthstone/frankensqlite/commit/b16c2ab6b02e40a2e82d1b0a61cc3de2ee3c4c3b),
  [`9c70f50d`](https://github.com/Dicklesworthstone/frankensqlite/commit/9c70f50d5d4cdcab0e9cf7b7a8cd3eb5ce68f0b0)
- **Native `Connection::execute_batch`** with no-op detection.
  [`1021ead3`](https://github.com/Dicklesworthstone/frankensqlite/commit/1021ead32742266822b529c688d8009e0724de2e)
- **Two-pass DELETE** to prevent self-referencing subquery corruption.
  [`42a6f5da`](https://github.com/Dicklesworthstone/frankensqlite/commit/42a6f5daed2bf1e7e4b0b1ab67ebf64b8e1b6e93)

#### DDL

- **CREATE TABLE AS SELECT**.
  [`f3fa1ad7`](https://github.com/Dicklesworthstone/frankensqlite/commit/f3fa1ad7f8ddc4ff4bd0f8e2db0bdf2ed2f5f3e7)
- **CREATE INDEX** with backfill of existing rows.
  [`b1d368c5`](https://github.com/Dicklesworthstone/frankensqlite/commit/b1d368c56b84e23f93aced5a00c1bd6f97fd37a5)
- **CREATE VIEW, DROP INDEX/VIEW**.
  [`e1a788b4`](https://github.com/Dicklesworthstone/frankensqlite/commit/e1a788b43a08e0e8ef5e0c91bb49384e27df6e37)
- **WITHOUT ROWID table creation** with DML rejection guards.
  [`cdfa9052`](https://github.com/Dicklesworthstone/frankensqlite/commit/cdfa9052a2efb6b6b7a9e9eef7d8e8c79a8e3f5f)
- **ALTER TABLE ADD/DROP COLUMN** with schema fidelity.
  [`3b9848a6`](https://github.com/Dicklesworthstone/frankensqlite/commit/3b9848a649420d91859d15c66b9ac45f44d74c02)
- **VACUUM INTO**.
  [`f96986a4`](https://github.com/Dicklesworthstone/frankensqlite/commit/f96986a4dc87dc696d05e4eb5943278e355eb9c0)
- **STRICT type enforcement**.
  [`f8e4006d`](https://github.com/Dicklesworthstone/frankensqlite/commit/f8e4006d4368cfa25a7b7989013c5399220e47b7)

#### Queries (SELECT)

- **JOINs** (INNER, LEFT, CROSS, RIGHT, FULL OUTER, NATURAL, USING).
  [`cda92efd`](https://github.com/Dicklesworthstone/frankensqlite/commit/cda92efdef9d9e7ad39e7e2e7e9adb6bb5d6db38),
  [`5544f57f`](https://github.com/Dicklesworthstone/frankensqlite/commit/5544f57fbbae9c2b6d5e83dcf7c5bfe1e29daae5),
  [`e6546940`](https://github.com/Dicklesworthstone/frankensqlite/commit/e6546940a8e57f1e0cc7ddd9e6a75c3ca8cc1e76)
- **GROUP BY** with JOIN, expressions, aliases, numeric index.
  [`ba48fbd3`](https://github.com/Dicklesworthstone/frankensqlite/commit/ba48fbd343de4e5cb0b5bd9e4cf9faa1de5f1e2f),
  [`057cf615`](https://github.com/Dicklesworthstone/frankensqlite/commit/057cf615c5e55fb5b2b59e75c2c9b2a5fb2f23b5)
- **HAVING** clause with complex aggregate arguments.
  [`1cef2110`](https://github.com/Dicklesworthstone/frankensqlite/commit/1cef21103fbadf2b11ddeec3c7e2a0c8c6cc6f17),
  [`73484b42`](https://github.com/Dicklesworthstone/frankensqlite/commit/73484b42e9c08d6c3bed2e60f82f8a3e8e0ce9d8)
- **DISTINCT** via row dedup.
  [`a5c60f7b`](https://github.com/Dicklesworthstone/frankensqlite/commit/a5c60f7b6e3c6ddc3f3c7b1e8f7de2ea4a6e5c5a)
- **Compound SELECT** (UNION/UNION ALL/INTERSECT/EXCEPT).
  [`5f3b008f`](https://github.com/Dicklesworthstone/frankensqlite/commit/5f3b008f9e84b60f02dbcee8e36ab4ffdd93c99a)
- **Common Table Expressions** (WITH clause) via table materialization.
  [`5efb72ef`](https://github.com/Dicklesworthstone/frankensqlite/commit/5efb72ef32c8b86d3dcc2fea6e74acda0a5fea72)
- **Recursive CTEs** with proper self-reference detection.
  [`2d143861`](https://github.com/Dicklesworthstone/frankensqlite/commit/2d143861e3f2f6e10bb8f1c6fa2b2f7a8b3ad4ff)
- **Subqueries in FROM clause** (derived tables).
  [`b222522b`](https://github.com/Dicklesworthstone/frankensqlite/commit/b222522b02bbb1d34c8c90b5f2b1b47fa8d39de3)
- **IN (SELECT ...)** subquery support.
  [`c985721c`](https://github.com/Dicklesworthstone/frankensqlite/commit/c985721c39dda34d09f7f7e09e8e2d69ad6fb41b)
- **Correlated scalar subqueries** with JOINs in FROM clause.
  [`6092fb23`](https://github.com/Dicklesworthstone/frankensqlite/commit/6092fb23b61d39b8a23e2e0b6fdab05d2e6d68c6)
- **ORDER BY NULLS FIRST/LAST**.
  [`725bd298`](https://github.com/Dicklesworthstone/frankensqlite/commit/725bd298642d33386d984c0f047def98bb5c0a6a)
- **Time-travel queries** (SQL:2011 `FOR SYSTEM_TIME AS OF`).
  [`69b57ecf`](https://github.com/Dicklesworthstone/frankensqlite/commit/69b57ecf8572f06dca6f9a58cb12d6fd4e1b5f0e)

#### Window Functions

- **Full window function support** -- ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD,
  NTH_VALUE, CUME_DIST, PERCENT_RANK, NTILE, FIRST_VALUE, LAST_VALUE.
  [`25cfd93e`](https://github.com/Dicklesworthstone/frankensqlite/commit/25cfd93e7652291b801eeac981e964e6d5699a92)
- **Per-function partition/sort** for multiple window specs.
  [`12638ce9`](https://github.com/Dicklesworthstone/frankensqlite/commit/12638ce9cf5cb327e626ae52d8987761ef9cffcb)
- **Aggregate-as-window functions** (SUM, AVG, COUNT, MIN, MAX, TOTAL).
  [`07a6003a`](https://github.com/Dicklesworthstone/frankensqlite/commit/07a6003abdb7bcfc9ac1e84f3e3c8a3c15c41f84)
- **Two-pass evaluation** for all partition-dependent window functions.
  [`0b39083c`](https://github.com/Dicklesworthstone/frankensqlite/commit/0b39083c089d5f71181cb4f553df21d2db3dda96)
- **RANGE and GROUPS** frame semantics.
  [`25cfd93e`](https://github.com/Dicklesworthstone/frankensqlite/commit/25cfd93e7652291b801eeac981e964e6d5699a92)

#### Constraints and Integrity

- **CHECK constraint enforcement** on INSERT and UPDATE.
  [`3db8d9ae`](https://github.com/Dicklesworthstone/frankensqlite/commit/3db8d9aee955018d79b79da9ff591ced270ad2df)
- **NOT NULL constraint enforcement** at codegen level.
  [`573a1006`](https://github.com/Dicklesworthstone/frankensqlite/commit/573a1006f4815e13167df204a4bb9c55db18aa8f)
- **UNIQUE constraint enforcement** in MemDatabase and pager-backed paths.
  [`b93d5cdf`](https://github.com/Dicklesworthstone/frankensqlite/commit/b93d5cdf3e0c2e58cc2e0ad1a4d32d3ae8a04a6f)
- **AUTOINCREMENT/sqlite_sequence** support.
  [`bcb357a7`](https://github.com/Dicklesworthstone/frankensqlite/commit/bcb357a71d97752a8165b437923494d8eada1347)
- **Foreign key enforcement** on UPDATE/DELETE with CASCADE propagation.
  [`d314d32b`](https://github.com/Dicklesworthstone/frankensqlite/commit/d314d32bcd46b4eabc7a2b80c06b21ec2a03fc94),
  [`abef2e0f`](https://github.com/Dicklesworthstone/frankensqlite/commit/abef2e0f8c0be8f44eac6f0eea5f3dbc1c1c7c5f)
- **Generated columns** (stored and virtual).
  [`2b4503ec`](https://github.com/Dicklesworthstone/frankensqlite/commit/2b4503ece38e87b7ee26457e059505acdb0be3fb)

#### Triggers

- **Per-row OLD/NEW pseudo-table values** for DML triggers.
  [`ce5d92bd`](https://github.com/Dicklesworthstone/frankensqlite/commit/ce5d92bd3e2da6f6c9f4e7d96b88f6b4b1f7b1cb)
- **UPDATE OF column-change filtering**.
  [`32e50e3a`](https://github.com/Dicklesworthstone/frankensqlite/commit/32e50e3a0f08cf50b9c7def19c31b09b1618ec7d)
- **RAISE() action handling** in trigger body statements.
  [`c04c03dc`](https://github.com/Dicklesworthstone/frankensqlite/commit/c04c03dcf7c26f4aeb8c6c7d7ad8ffc4e3a3c2e1)

---

### Built-in Functions

- **Function registry** with builtins, authorizer, and collation.
  [`a37017ed`](https://github.com/Dicklesworthstone/frankensqlite/commit/a37017edf1fdd8e3c3c1e10dc7bdf0e16c0d6f9c)
- **20+ common SQLite scalar functions** in `eval_scalar_fn`.
  [`76361291`](https://github.com/Dicklesworthstone/frankensqlite/commit/76361291acbfe49dfda9e1e1e7c4d87e76b5b7df)
- **Kahan-Babuska-Neumaier compensated summation** for SUM/AVG/total.
  [`d5ac7704`](https://github.com/Dicklesworthstone/frankensqlite/commit/d5ac770473ee9242282da0e1a6e6ebdf8903e2b0)
- **Datetime functions** with localtime/utc modifiers, month/year overflow.
  [`0a269b5b`](https://github.com/Dicklesworthstone/frankensqlite/commit/0a269b5b9a68a5740c4a9c0209782e29e7c01117)
- **CollationRegistry** threaded through ORDER BY, sorting, and GROUP BY paths.
  [`87e8d9db`](https://github.com/Dicklesworthstone/frankensqlite/commit/87e8d9db84c6a2c3ec6cb7afdd8a36a59c10e8c6),
  [`d8834079`](https://github.com/Dicklesworthstone/frankensqlite/commit/d8834079a03d1d1bdbed2f6c7a9fbd2eec3a9ff3)
- **UDF registration API** for user-defined functions.
  [`204a241e`](https://github.com/Dicklesworthstone/frankensqlite/commit/204a241e8780a5e6c08e7e9ff1dbfb0e1fc76d31)
- **`%!.15g` float-to-text formatting** matching C SQLite.
  [`6ae07957`](https://github.com/Dicklesworthstone/frankensqlite/commit/6ae079572979023507c7fc2cfc73f696a4fd5739)
- **Round-half-away-from-zero** matching SQLite custom printf.
  [`57d26354`](https://github.com/Dicklesworthstone/frankensqlite/commit/57d26354b1f7b00f9c4f29b5fde04ec02c45d22e)

---

### Extensions

#### FTS5 Full-Text Search

- **FTS5 virtual table creation**, MATCH operator routing.
  [`a1603989`](https://github.com/Dicklesworthstone/frankensqlite/commit/a1603989e8d0e7eadb0eb2b94e5c5d1e3a9a0b7c)
- **Real column filter evaluation** in FTS5 queries.
  [`0147eb5b`](https://github.com/Dicklesworthstone/frankensqlite/commit/0147eb5b4b8c5b1876a7fde6afe2ea5ee663498d)
- **highlight() and snippet()** scalar functions.
  [`3f6d1189`](https://github.com/Dicklesworthstone/frankensqlite/commit/3f6d1189985f6bb7751ccf00027448d878bc2c2b)
- **Multiple MATCH constraints** combined with AND.
  [`46c4bd99`](https://github.com/Dicklesworthstone/frankensqlite/commit/46c4bd99f8db7ded4e38c0bb19d8bfcb09fa05e1)

#### FTS3/FTS4 Full-Text Search

- **FTS3/FTS4 legacy full-text search extension**.
  [`50e512eb`](https://github.com/Dicklesworthstone/frankensqlite/commit/50e512ebc85ec7b4b0b35f18b5c1d9c9cb6b2f36)

#### R-Tree Spatial Index

- **R*-tree spatial index** with geopoly format functions.
  [`4b04e603`](https://github.com/Dicklesworthstone/frankensqlite/commit/4b04e603b4dea5e7c5b57b8c5dd1b1e1b0fbe5bc)
- **R-tree extension parity** with fsqlite-func integration and harness
  coverage.
  [`d48445e7`](https://github.com/Dicklesworthstone/frankensqlite/commit/d48445e7667f88209c31ef1529e73bed6ea32aa6)

#### JSON Extension

- **JSON1 scalar core** and path extraction foundation.
  [`3e79382a`](https://github.com/Dicklesworthstone/frankensqlite/commit/3e79382aac3a78e9edf5d7e1f8e8f18eab2db7d0)
- **json_each/json_tree** virtual-table cursors.
  [`8334743c`](https://github.com/Dicklesworthstone/frankensqlite/commit/8334743c0d8a1eda7fb2d0ef4f8a5df5e8c4a0cf)
- **JSONB scalar function** parity and blob input support.
  [`fbfe5675`](https://github.com/Dicklesworthstone/frankensqlite/commit/fbfe5675ece6c2d3cf2f753f66b30f54f3821ce5)
- **Reject BLOB values** in json_quote per SQLite specification.
  [`05c257fb`](https://github.com/Dicklesworthstone/frankensqlite/commit/05c257fbc75a2c16e4ee06e9dd7ac4b9db3e7d0e)

#### Session Extension

- **Changeset/patchset tracking** and application.
  [`bc54260d`](https://github.com/Dicklesworthstone/frankensqlite/commit/bc54260db5ced36e7d0e2e7b7f00c8c65f8cd1f3)
- **Session format corrections** -- binary format, change coalescing, conflict
  apply semantics.
  [`366e1eda`](https://github.com/Dicklesworthstone/frankensqlite/commit/366e1edaaf5a88a7f8c2b0d1ac9e3bc1a82b2e7e)

#### ICU Extension

- **Unicode collation**, case mapping, and tokenization.
  [`08a0c9d0`](https://github.com/Dicklesworthstone/frankensqlite/commit/08a0c9d03f32bf3e05ace96fa1cdc74e0a1dd2b0)

#### Miscellaneous Extensions

- **generate_series, decimal arithmetic, and UUID**.
  [`f0906c21`](https://github.com/Dicklesworthstone/frankensqlite/commit/f0906c210a2b4cbf4c49a3c45b83a7f2c8a1c18a)
- **Prevent infinite loop** in generate_series on integer overflow.
  [`44c114fe`](https://github.com/Dicklesworthstone/frankensqlite/commit/44c114fe1f3f8c35e2a54e09e5a8e1e3f7b6e4f1)

---

### RaptorQ Durability (Forward Error Correction)

- **Unified RaptorQ repair engine** with BLAKE3 proofs.
  [`c30cf910`](https://github.com/Dicklesworthstone/frankensqlite/commit/c30cf910ec8e6dba6bbbea3e4ab5e1ab56c8ac7d)
- **RaptorQ source block partitioning**.
  [`427389b0`](https://github.com/Dicklesworthstone/frankensqlite/commit/427389b0ae5ed1c3f1f7b2a4a6e25c3f0e4ffeaa)
- **Fountain-coded snapshot shipping** (replication).
  [`897fa04c`](https://github.com/Dicklesworthstone/frankensqlite/commit/897fa04c0ebff8ab11e3a73fd7cf5fb7e5aef4a9)
- **Erasure-coded page storage** (`.db-fec` sidecar).
  [`ba45ad1d`](https://github.com/Dicklesworthstone/frankensqlite/commit/ba45ad1d3c7b7f2b5f8bbfc2df6e7b0e7e3e3fe7)
- **XOR parity replaced with RFC 6330 RaptorQ** InactivationDecoder.
  [`8b162c4b`](https://github.com/Dicklesworthstone/frankensqlite/commit/8b162c4b6a4ef4e8e4a4e1b0a0c1b5e0c0edf2a3)
- **Proofs of Retrievability** (PoR) module.
  [`9e2fcb2c`](https://github.com/Dicklesworthstone/frankensqlite/commit/9e2fcb2cc4cdbbe9fd48c4d0d73c99c8bd7a5e99)

---

### C API Compatibility Shim

- **SQLite C API compatibility shim crate** (`fsqlite-c-api`).
  [`b14643bb`](https://github.com/Dicklesworthstone/frankensqlite/commit/b14643bbb77c6a6faec02fc270dccc33efca0b88)
- **`sqlite3_prepare_v2`-style statement parsing** with tail offset support.
  [`cf10270d`](https://github.com/Dicklesworthstone/frankensqlite/commit/cf10270d125313398ce5761af1eaf8df87a4cfcd)
- **Multi-statement batch execution** and real column names in `sqlite3_exec`.
  [`d40e4cb2`](https://github.com/Dicklesworthstone/frankensqlite/commit/d40e4cb27d29a4050884fb1cb8455bb2e546afbb)
- **Panic-safe close** -- `catch_unwind` around `sqlite3_open`,
  `sqlite3_prepare_v2`, and `sqlite3_step`.
  [`826ab12b`](https://github.com/Dicklesworthstone/frankensqlite/commit/826ab12beed2f0b48b54b3bca7ee94b83e4e69f4),
  [`a4f9b47d`](https://github.com/Dicklesworthstone/frankensqlite/commit/a4f9b47d5c9c7f6fde7f0cc16c0e9af0f3c2b8f4)
- **Temporary database lifecycle**, finalize error propagation, VDBE result code
  parsing.
  [`20b587f3`](https://github.com/Dicklesworthstone/frankensqlite/commit/20b587f385a38902d16aed86ec12ce0d75bceafd)

---

### WASM (WebAssembly)

- **Enable FrankenSQLite compilation** for `wasm32-unknown-unknown`.
  [`202b9f26`](https://github.com/Dicklesworthstone/frankensqlite/commit/202b9f2682718edab6b3cef56c1db6a7d9e65842)
- **Full WASM database engine** with R-tree virtual table adapter.
  [`f76c7de2`](https://github.com/Dicklesworthstone/frankensqlite/commit/f76c7de2a55d7a4aa9b869ba526388664dcb2fb4)
- **Gate OS-specific deps** for wasm32 compatibility across pager, WAL, MVCC,
  VDBE, btree, ext-misc, observability, and core crates.
  [`dbfa3317`](https://github.com/Dicklesworthstone/frankensqlite/commit/dbfa33171e7e0edbb98d6e93bb0afc3cdd1e0c8c)
  through
  [`56cdcc51`](https://github.com/Dicklesworthstone/frankensqlite/commit/56cdcc51cc57f5a6cf89e5d00bf0d9e3cb6baf23)
- **JS-facing coverage** and host connection tests.
  [`9df761c3`](https://github.com/Dicklesworthstone/frankensqlite/commit/9df761c35fa6f5bd8d580f7990ea04d78c314cbd)

---

### Type System

- **Core type system** with 64 tests (`fsqlite-types` Phase 1).
  [`bfd62701`](https://github.com/Dicklesworthstone/frankensqlite/commit/bfd62701858561f59913a2d61a966d7dcc239152)
- **Record format serialization** for SQLite binary format.
  [`3756438e`](https://github.com/Dicklesworthstone/frankensqlite/commit/3756438eec6c51bb8a84e1aadba1e3e8b21b5fdf)
- **`SqliteValue::Text/Blob` migration to `Arc<str>`/`Arc<[u8]>`** for O(1)
  clone across the entire workspace (all 6 extension crates, func, core, VDBE,
  harness, compat).
  [`fa399373`](https://github.com/Dicklesworthstone/frankensqlite/commit/fa3993737120862696e26bcdc0dcfa40c4693528)
  through
  [`580575b2`](https://github.com/Dicklesworthstone/frankensqlite/commit/580575b239ed10e15e691e1c9e968b709e2d6652)
- **SQL three-valued NULL logic** for comparisons.
  [`44b6f1dc`](https://github.com/Dicklesworthstone/frankensqlite/commit/44b6f1dc2cdcdeb568e2be20c42963378c251327)
- **Non-numeric text arithmetic** yields integer, not float.
  [`49167f2b`](https://github.com/Dicklesworthstone/frankensqlite/commit/49167f2b94b4f4e6bef99bc6bfbd14e73e87b7d9)
- **Type affinity coercion** in comparisons.
  [`84e01813`](https://github.com/Dicklesworthstone/frankensqlite/commit/84e018131715652ad301793754e4d11c7cb08319)
- **Lazy record decode**, conditional profiling, and value helpers.
  [`53d4ec87`](https://github.com/Dicklesworthstone/frankensqlite/commit/53d4ec87b0e0bb0f0a65f6b8f3e8b7e5f0be8c59)

---

### Connection and Public API

- **`fsqlite::Connection`** -- `open()`, `execute()`, `query()`, `prepare()`.
  [`256b7c0b`](https://github.com/Dicklesworthstone/frankensqlite/commit/256b7c0b97dbee0e4cb0a5ca2d8e84f3c9dc0e0d)
- **Real SQLite binary format persistence**.
  [`b30fc295`](https://github.com/Dicklesworthstone/frankensqlite/commit/b30fc295d5d1f6a3e0f2f1d2c0dcddf3c6a0d25f)
- **Phase 5A complete** -- schema loading, cookies, storage cursors.
  [`d6bc2aa5`](https://github.com/Dicklesworthstone/frankensqlite/commit/d6bc2aa5e4132fcf0b24049f217ee033d313e462)
- **PreparedStatement::execute_with_params** now works for DML.
  [`1fc5bb82`](https://github.com/Dicklesworthstone/frankensqlite/commit/1fc5bb82b0d09649734c88d1b655d2ba9c034324)
- **Pre-compiled INSERT reuse** and schema-scoped compiled cache.
  [`53ee09c9`](https://github.com/Dicklesworthstone/frankensqlite/commit/53ee09c93c4ef8c9dc6ed04b1c0f71b1deff40a6)
- **Rusqlite compat layer**.
  [`f9c447e5`](https://github.com/Dicklesworthstone/frankensqlite/commit/f9c447e560d5eb4e0f5ad3c87e7c0d1d1e27d71e)

---

### Performance Optimization

- **Hekaton-style lock-free page locks** and cached read snapshots.
  [`bb6f3606`](https://github.com/Dicklesworthstone/frankensqlite/commit/bb6f36066209de6bb71985d39c9eef399a305773)
- **Batch commit-index fence**, SmallVec active-commits, proactive chain
  compaction.
  [`55ddcc6c`](https://github.com/Dicklesworthstone/frankensqlite/commit/55ddcc6c65d7769042fb5ac076fe231f8e1e84c1)
- **Zero-cost observability**, in-memory pager fast path, SQL normalization.
  [`f44dddfb`](https://github.com/Dicklesworthstone/frankensqlite/commit/f44dddfb20803d088850c656869d46b3856445fb)
- **Autocommit hot path** optimization -- skip external schema refresh for
  in-memory, skip WAL post-commit backfill for `:memory:`.
  [`63fdd78c`](https://github.com/Dicklesworthstone/frankensqlite/commit/63fdd78c9f4907d53b7d3ce4dfce90ecddb49c6b),
  [`bdace094`](https://github.com/Dicklesworthstone/frankensqlite/commit/bdace094d2ac2a89e55f27e1db6e9b2eda52a7e7)
- **Autoincrement sequence fast-path**, post-write action pipeline.
  [`14e1ac90`](https://github.com/Dicklesworthstone/frankensqlite/commit/14e1ac908bf8a4cd52e68e3af5fb5ba96cc1e8a8)
- **Sort-based GROUP BY**, `Arc<Statement>` in prepared stmts, NOCASE
  optimization.
  [`69e8af20`](https://github.com/Dicklesworthstone/frankensqlite/commit/69e8af20d5141f3b32ad4e81570667deec8c8f41)
- **Reduce per-statement overhead** -- uncontended finalize fast path, inline
  hot register lookups.
  [`9cf25d4a`](https://github.com/Dicklesworthstone/frankensqlite/commit/9cf25d4ad5b1fdb6d9a9ec7dbe2cc5dcd25b7e4f)
- **Prechecked insert**, handle recycling, memory autocommit fast path.
  [`af73463d`](https://github.com/Dicklesworthstone/frankensqlite/commit/af73463d0fca0b7c3b4b4a6deb6e7f9e8c68c7e0)
- **O(1) atomic occupancy counter** for lock table.
  [`8a08921c`](https://github.com/Dicklesworthstone/frankensqlite/commit/8a08921c5aef12fbd8cf7ed18c2badc7e36c0c8a)
- **SmallVec for VDBE program ops** and optimized record parsing.
  [`a2b112fc`](https://github.com/Dicklesworthstone/frankensqlite/commit/a2b112fc3e71e34e97af4de7e6ca5fe5c5de3f10)
- **Owned-page write fast path**, StorageCursor rewrite.
  [`a46d6f30`](https://github.com/Dicklesworthstone/frankensqlite/commit/a46d6f30b5b2f3fd2e44c6b7c6f0f7e3bf2b2e4c)

---

### Conformance and Differential Testing

A massive conformance effort produced 500+ oracle tests comparing FrankenSQLite
results against C SQLite on identical SQL.

- **Conformance oracle framework** with C SQLite comparison.
  [`57ffa844`](https://github.com/Dicklesworthstone/frankensqlite/commit/57ffa8441c64e0a06bb0f6ad7dbcae7c57c9c8de)
- **500+ oracle conformance tests** covering JOINs, aggregates, window
  functions, subqueries, CTEs, triggers, foreign keys, UPSERT, DISTINCT,
  COLLATE, type coercion, NULL semantics, and dozens of edge cases.
  Representative batch commits:
  [`11665f60`](https://github.com/Dicklesworthstone/frankensqlite/commit/11665f60da9ba1d61ac42577d7df954032f74f01) (200 total),
  [`8cf6f075`](https://github.com/Dicklesworthstone/frankensqlite/commit/8cf6f0752fd93f977da844d2e8396516b73d5cfe) (353 total),
  [`529f1164`](https://github.com/Dicklesworthstone/frankensqlite/commit/529f11643a1d4c0e0e56f1f45f1b9c1b3c3e8bb8) (457 total)
- **Parity-certification mode** with MVCC visibility telemetry and WAL replay
  tracing.
  [`84d7b1a6`](https://github.com/Dicklesworthstone/frankensqlite/commit/84d7b1a6a7851d6583d8d961ad07bf3a1d12c741)
- **Exhaustive function parity matrix** differential test against C SQLite.
  [`4c9cf08e`](https://github.com/Dicklesworthstone/frankensqlite/commit/4c9cf08e9a9c078eb0a75453c9da2ab3318d2c7a)
- **Oracle preflight doctor** in CI workflow.
  [`6f491bf8`](https://github.com/Dicklesworthstone/frankensqlite/commit/6f491bf814b229ea351c814c0b3b390e9bc03baf)
- **Property-based testing** -- proptest suites for cell visibility invariants,
  parser round-trip, MVCC snapshot isolation, vectorized operator equivalence.
  [`6f5582f6`](https://github.com/Dicklesworthstone/frankensqlite/commit/6f5582f69182d29c0dcc77ab9b144bccda3ee4a5),
  [`044c683e`](https://github.com/Dicklesworthstone/frankensqlite/commit/044c683e17adf3e94c5aba03cac32e3d44b02cff),
  [`f1b31fb9`](https://github.com/Dicklesworthstone/frankensqlite/commit/f1b31fb9c00e7a2d7d47e2c4e4d7b1e0e8e3d5f1)

---

### E2E Testing and Benchmarks

- **Comprehensive benchmark suite** (FrankenSQLite vs C SQLite).
  [`0b5512cc`](https://github.com/Dicklesworthstone/frankensqlite/commit/0b5512cc36daee77a84b9c3f5eecc5f37e7a5fe4)
- **Persistent concurrency benchmark** and perf gate tooling.
  [`3a8154e2`](https://github.com/Dicklesworthstone/frankensqlite/commit/3a8154e233a7fc45b3b1f2e2f7f3bb5e9fc54c5c)
- **Corruption injection framework** with scenario catalog and recovery runners.
  [`da9dc5e0`](https://github.com/Dicklesworthstone/frankensqlite/commit/da9dc5e06bcb3c8fc1e8f43cfb5d97d2f77e52a0)
- **Interactive TUI viewer** for run records and benchmarks.
  [`f5c0b01e`](https://github.com/Dicklesworthstone/frankensqlite/commit/f5c0b01eb3c5ee2f2e2dbee0d7e9c77c8e2b2e7c)
- **Hot-path profiling API** for pre-built oplogs, concurrent writer profiling.
  [`2fae093d`](https://github.com/Dicklesworthstone/frankensqlite/commit/2fae093d3c3e3c3f2e0e6f8e3aeef78c9a5b7e1d)
- **MVCC concurrent writers scaling test suite**.
  [`791ab0a1`](https://github.com/Dicklesworthstone/frankensqlite/commit/791ab0a1bb27f7e31bfad17b5c4f8f6c2f2d2cc2)
- **SHA-256 artifact integrity** and cross-process lock testing.
  [`53db4db1`](https://github.com/Dicklesworthstone/frankensqlite/commit/53db4db1df66e33b58ebb4c6d11b9d57c3ef3f9c)

---

### Observability

- **MVCC conflict analytics** and observability suite.
  [`492428dd`](https://github.com/Dicklesworthstone/frankensqlite/commit/492428dd37e5f1e67aee0c6a72f8c5bdb8be7ffa)
- **TxnSlot lifecycle telemetry** and instrumentation.
  [`4ddfb008`](https://github.com/Dicklesworthstone/frankensqlite/commit/4ddfb0087f42f2c7e0d1a5db64f3f2c2e8f3a5e1)
- **RaptorQ metrics** and tracing spans.
  [`dee49104`](https://github.com/Dicklesworthstone/frankensqlite/commit/dee49104a8e28f7f3cdd5b5a6fc1c8da4f4e0a7f)
- **WAL metrics counters** and tracing span.
  [`b8931ec7`](https://github.com/Dicklesworthstone/frankensqlite/commit/b8931ec7a0e3c9e67e7bfa0de2bf3ebf8c3e8c55)
- **SSI metrics counters** and tracing span for `ssi_validate`.
  [`a31aa3b3`](https://github.com/Dicklesworthstone/frankensqlite/commit/a31aa3b3c9e9db3e0c6a0a5c0b0f0c3b5e5eab85)
- **TracingFile wrapper** and VfsMetrics.
  [`b8658d7f`](https://github.com/Dicklesworthstone/frankensqlite/commit/b8658d7f4e2e4b9e1eadabe98ddabe4f0a0d2619)

---

### CLI

- **REPL shell** with `-c/--command` and `.read`.
  [`30108bef`](https://github.com/Dicklesworthstone/frankensqlite/commit/30108bef4d1db29c7bbfb1e7e2c4a8c3b7e5e9f7)
- **Propagate SQL and dot-command errors** to shell exit code.
  [`872948e7`](https://github.com/Dicklesworthstone/frankensqlite/commit/872948e7fbb3c3f8d0e6c38f7d89b9b0c4f4c1c8)

---

### Licensing

- **MIT + OpenAI/Anthropic Rider** adopted across workspace (2026-02-18).
  [`5d684f5f`](https://github.com/Dicklesworthstone/frankensqlite/commit/5d684f5f4da037afd971ba1ea28846597939d653)

---

## Workspace Crates

| Crate | Role |
|-------|------|
| `fsqlite` | Top-level public API facade |
| `fsqlite-core` | Connection, query dispatch, schema management |
| `fsqlite-types` | Core type system (`SqliteValue`, `PageNumber`, `TxnId`, etc.) |
| `fsqlite-error` | Structured error types |
| `fsqlite-vfs` | Virtual File System (POSIX, io_uring, WASM) |
| `fsqlite-pager` | Page cache, group commit, WAL integration |
| `fsqlite-wal` | Write-Ahead Log (compat + parallel) |
| `fsqlite-mvcc` | Page-level MVCC, SSI, EBR, version store |
| `fsqlite-btree` | B-tree engine with learned index |
| `fsqlite-ast` | SQL abstract syntax tree |
| `fsqlite-parser` | SQL parser |
| `fsqlite-planner` | Query planner and optimizer |
| `fsqlite-vdbe` | Virtual Database Engine (bytecode interpreter) |
| `fsqlite-func` | Built-in scalar and aggregate functions |
| `fsqlite-ext-fts3` | FTS3 extension |
| `fsqlite-ext-fts5` | FTS5 extension |
| `fsqlite-ext-rtree` | R-Tree extension |
| `fsqlite-ext-json` | JSON/JSONB extension |
| `fsqlite-ext-session` | Session extension |
| `fsqlite-ext-icu` | ICU extension |
| `fsqlite-ext-misc` | Miscellaneous extensions (`generate_series`, etc.) |
| `fsqlite-c-api` | Optional C ABI shim (only `unsafe` code in workspace) |
| `fsqlite-cli` | Command-line shell |
| `fsqlite-e2e` | End-to-end tests and benchmarks |
| `fsqlite-harness` | Conformance test harness and oracle infrastructure |
| `fsqlite-wasm` | WebAssembly database engine |
| `fsqlite-observability` | Telemetry and instrumentation |
