# Changelog

All notable changes to FrankenSQLite are documented in this file.

FrankenSQLite is an independent ground-up Rust reimplementation of SQLite with
page-level MVCC concurrent writers and Serializable Snapshot Isolation (SSI).
RaptorQ erasure coding is durability research: the codecs and WAL repair
routines exist in the workspace, but the compatibility runtime's WAL commit and
recovery paths do not write or consult repair symbols. The project is organized
as a 27-member Cargo workspace under `crates/`.

> The project is pre-release. Crates are published to crates.io as `fsqlite`
> and the `fsqlite-*` workspace members. The historical entries below
> (0.1.0–0.1.2) are organized by capability area rather than raw diff order,
> covering all 2,520 commits from project inception (2026-02-06) through
> 2026-03-21; the 0.1.3 and 0.1.4 entries are point releases.

Repository: <https://github.com/Dicklesworthstone/frankensqlite>

---

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
