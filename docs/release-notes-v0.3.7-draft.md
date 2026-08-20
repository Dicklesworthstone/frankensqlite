## FrankenSQLite v0.3.7

Post-0.3.6 correctness and concurrency-hardening wave (182 non-merge commits
since v0.3.6). This release closes seven GitHub issues — attached-schema
multi-database transactions, an FTS5 contentless `%_content` reclaim, a
`ReservedEmpty` reopen coherence bug, a memdb read-amplification pair, and a
WITHOUT-ROWID teardown OOM — lands the seven-facet **bd-xv5cm** concurrency
hardening batch, and completes a third-wave "fresh-eyes" review sweep of SQL
semantics and conformance (trigger firing order, `json()` number preservation,
`printf`/datetime, index-integrity recomputation). **No format change — 0.3.6
databases open unchanged.**

### GitHub issues closed

- **GH#244** — attached-schema writes participate in explicit transactions via
  lazy child-txn enrollment with commit/rollback fan-out (`f430ce46d`).
- **GH#345** — subquery-`WHEN` trigger ingestion no longer re-parses the schema
  on every write; lazy per-read memdb hydration removes an O(rows²) reload
  (`8f0bc0967`, `40c400940`).
- **GH#366** — a coherent populated database reopened via `ReservedEmpty` opens
  instead of being falsely rejected (`e3b5e68af`, follow-up `1bb97fabd`).
- **GH#368 / GH#369** — memdb read-amplification pair: in-txn `sqlite_master`
  scans counted for regression guarding; memdb foreign-key parent hits trusted
  only when the mirror is fully hydrated (`d62a51b18`, `7bac82eb4`).
- **GH#370** — reclaim orphaned `%_content` on legacy contentless FTS5 archives
  (`a00e152cc`).
- **GH#371** — bound WITHOUT-ROWID / large-table `DROP` teardown memory, closing
  a P0 out-of-memory (`7a3625a7d`).

### Concurrency hardening — bd-xv5cm (7 facets)

- Group-commit queue removed only when the registry is its sole owner (`0a1ab35b3`).
- Bounded post-cancel wait for the worker's authoritative outcome (`14c7e5987`).
- Autocommit `SELECT` that calls a user function is never auto-retried (`b38b09d9f`).
- BusyRecovery identity preserved on retry exhaustion (`f430ce46d`).
- Atomic single-write wal-fec append with a graceful scan over a corrupt tail (`01c3965e4`).
- Coordinator-map lock released before the blocking ticker join (`ad2e949b3`).
- Chunk-id assertion; batch closed at `b0aa79f4b`.

### Correctness & conformance

- Same-event triggers fire newest-first (LIFO), matching C SQLite (`a60f6c606`).
- `json()` preserves number-literal source text and minifies its input instead
  of a lossy value round-trip (`6b1ecba70`, `6d5cbe28d`).
- `printf` `%!` alt-form-2 floats emit exact-double cap digits (`d3cab54d7`);
  unsigned datetime modifiers accepted as positive (`1da56ea84`); the `utc`
  modifier resolves DST-transition ambiguity via SQLite's iterative solver
  (`49437629d`).
- Third-wave "fresh-eyes" review (REVIEW3): `HAVING` column-reference resolution
  (`3875fb93a`), literal `X AND 0` folding in the VDBE binary-op emitter
  (`9d0a9205b`), external-content FTS5 scans and rebuild read the SOURCE
  (`4b8a2504f`), and dual-recompute index keys under `integrity_check` so
  CAST-AS-BLOB and surrogate UTF-16 indexes stop false-flagging (`96ae9d73c`).
- DQS-ON compatibility: an unresolvable double-quoted identifier falls back to a
  string literal, matching SQLite's legacy double-quoted-string behavior
  (`f08b20fbf`).
- `CAST(blob AS TEXT/numeric)` relabels bytes via the database text encoding
  (`194e97de8`); INDEXED-BY partial-cover LEFT→INNER strength reduction
  (`56f90d806`); FTS5 external-content delete-all keeps rows visible and clears
  only the index/doclist shadows (`ee2e3a7ea`).

---

**Full changelog:** <https://github.com/Dicklesworthstone/frankensqlite/blob/main/CHANGELOG.md>

Static Linux artifacts (musl; run on glibc and musl distros alike), native
macOS (Intel + Apple Silicon), and Windows x64. Verify downloads against
`SHA256SUMS`.
