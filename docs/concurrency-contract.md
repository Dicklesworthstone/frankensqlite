# FrankenSQLite Concurrency Contract

**Bead / Issue:** [frankensqlite#70](https://github.com/Dicklesworthstone/frankensqlite/issues/70)
(closed 2026-05-24; this document is the durable contract that outlived the
tracking issue). Remaining multi-process durability work is carried by the open
P0 Beads epic `bd-zywqc`.

**Purpose:** state, unambiguously, what concurrency guarantees FrankenSQLite
claims today — and what it does *not* — so caller projects stop re-filing
the same surface-level symptom issues every time the concurrent-write
layer drops a page or returns a stale read.

If a caller asks "does fsqlite work under N-process swarm load," the answer
should be this document plus the harness status, not a chain of point-fix
issues whose common cause was never named.

---

## TL;DR

- **Single-process, multi-Connection via MVCC WAL**: *release-blocked on the
  current candidate*. This is the intended default, but `bd-9inpb` reproduces
  corruption and wrong row counts with 10 or more ordinary implicit-autocommit
  writers in both WAL and rollback-journal modes. The v0.2.0 tag must not be cut
  until the exact gate is green.
- **Single-process, single-Connection across threads**: *not supported by
  API*. `Connection` is `!Send + !Sync` by construction. Spawn one
  Connection per OS thread against the same file-backed database and
  coordinate below the Connection API.
- **Multi-process, multi-writer WAL**: *partial, bounded by measured harness
  scale*.
  The `swarm_multiprocess` harness exists (`crates/fsqlite-e2e/src/bin/swarm_multiprocess.rs`)
  because this path was the source of the closed
  [#70 roll-up](https://github.com/Dicklesworthstone/frankensqlite/issues/70)
  family of bugs. Known limitations are enumerated below; the harness remains
  authoritative for the largest process count proven green on each platform.
- **Failing closed vs silently corrupting**: *goal*. Where a scenario is
  known to be unsafe, the runtime should emit an observable error before
  it scribbles on durable state, not after. Concrete escalation paths are
  listed under [§ Caller guidance](#caller-guidance) below.

---

## Authority

This document is advisory/normative. The machine-readable sources of
truth are:

1. **`crates/fsqlite-e2e/src/bin/swarm_multiprocess.rs`** — the
   multi-process swarm harness. Reproducible: what it asserts is what
   fsqlite currently guarantees under swarm-write load. What it fails
   on (check `crates/fsqlite-e2e/artifacts/swarm-multiprocess/` for
   captured forensic bundles) is what fsqlite does *not* yet guarantee.
2. **`crates/fsqlite-e2e/tests/correctness_concurrent_writes.rs`** and
   **`crates/fsqlite-e2e/tests/mvcc_concurrent_writers.rs`** — in-process
   multi-Connection concurrency tests.
3. **`docs/canonical_parity_contract.md`** — the SQLite-compatibility
   surface contract; multi-process WAL behavior must match stock SQLite
   for anything this doc classifies as supported.
4. **[`docs/bench-methodology-concurrent-writers.md`](./bench-methodology-concurrent-writers.md)** —
   bench-vs-correctness boundary. Read before quoting any
   throughput number from `bench_concurrent_writers`.

If this document diverges from the harness, the harness wins and this
document is out of date. Fix this doc, not the harness.

---

## The seven questions, answered

This section answers, in plain English, the seven questions enumerated in
[#80](https://github.com/Dicklesworthstone/frankensqlite/issues/80). For
mechanism (MVCC, DPOR, page-conflict math), see the README's
"Concurrency Model" / "Multi-Process MVCC" sections; this section is the
*caller-facing contract*.

### 1. Process count — how many caller processes can safely share one DB file?

- **Single-process**: multiple `Connection` instances are the intended default,
  but the current candidate is release-blocked by `bd-9inpb`; do not claim an
  unconditional count until its four-scenario writer gate is green.
- **Multi-process**: target is N ≤ 32 short-lived writers per file
  (matching the swarm harness scale). Today the multi-process surface is
  *partial* — see the closed
  [#70](https://github.com/Dicklesworthstone/frankensqlite/issues/70)
  roll-up for root-cause history. Treat the largest N at which
  `cargo run -p fsqlite-e2e --bin swarm-multiprocess --workers N --seconds 3600`
  is green on your platform as the conservative upper bound. Mixing
  fsqlite and stock-SQLite *concurrent* opens against the same file is
  not supported (each side enforces invariants over its own pager only;
  the on-disk format is interoperable when checkpointed and idle).

### 2. Connection lifetime — short-lived OK, or long-lived only?

Both are supported. The contract is written for short-lived
(`open / work / commit / close` churn), which is the realistic AI-agent
workload. Long-lived connections are also supported and are the cheaper
path: open-time cost amortizes across more transactions. There is no
PRAGMA or runtime mode you have to flip to opt into either; the same
`Connection::open` handles both.

The fsqlite-pager freelist allocator persists committed allocations
through close (the [#56](https://github.com/Dicklesworthstone/frankensqlite/issues/56)
"freelist trunk page exceeds db_size on third+ open" class is regression-netted
by the swarm harness's `sequential_open` criterion); short-lived churn
must not produce process-global state leakage.

### 3. Visibility guarantee — when does B see A's COMMIT?

- **Read-your-own-writes**: immediately, on the same `Connection`.
  `COMMIT` returning `Ok` means the next `query`/`query_with_params` on
  the same `Connection` sees the new row. This is unconditional.
- **Cross-Connection (same process)**: visible at B's *next transaction
  boundary*. fsqlite uses `schema_cookie` + per-connection prepared-plan
  invalidation (see `crates/fsqlite-core/src/connection.rs` —
  `schema_cookie` / `schema_generation` checks at prepared-statement
  reuse). Within an active read transaction on B, B sees its own
  consistent snapshot; A's commits become visible when B begins a
  new transaction.
- **Cross-process**: same as cross-Connection, mediated by the
  shared-memory file (`*.fsqlite-shm`, see `crates/fsqlite-mvcc/src/shm.rs`)
  for MVCC commit_seq propagation, plus standard SQLite WAL-index for
  page-level visibility. The window between "process A's `COMMIT`
  returns" and "process B's next `BEGIN` sees the new state" is bounded
  by shared-memory atomic visibility (typically nanoseconds on the same
  host) plus B's own read-snapshot acquisition. There is no indeterminate
  staleness window once B has started a fresh transaction.

### 4. Plan-cache visibility — does the prepared-statement cache participate?

**Yes**, and this is exactly the bug surface that produced the
[beads_rust#252 / #254 / #255](https://github.com/Dicklesworthstone/beads_rust/issues/252)
family. The contract:

- The prepared-plan cache *is* invalidated on schema change
  (`schema_cookie` bump). That part has worked since this code shipped.
- Historically, plan-cache entries did **not** always invalidate on
  *data* changes that shifted index layout (e.g. a B-tree split from a
  concurrent INSERT could leave a cached cursor pointing at a stale
  page). That class is what beads_rust#252/#254/#255 saw as
  `SELECT … WHERE pk = ?` returning zero rows for a freshly-committed
  row, or the wrong row.
- After the fixes referenced in the
  `flat-combining-page-locks` /
  `feat/conformal-retry-budget` series, the prepared-plan cache is
  required to invalidate on cross-Connection commit boundaries that
  touch the cache's covered pages. The
  `cross_process_visibility` and `wrong_row_returns` criteria in the
  swarm harness exist to nail this down.
- Caller obligation: do **not** assume that re-using a prepared
  statement across transaction boundaries on the same connection
  preserves a snapshot. Re-prepare or rely on the connection-level
  invalidation; do not cache plan handles in an outer pool that
  outlives a transaction without consulting `schema_cookie`.

### 5. Lock-timeout semantics — does `PRAGMA busy_timeout` apply across processes?

- Same-process, multi-Connection: yes, unambiguously. `busy_timeout`
  applies to MVCC abort/retry and to WAL-index locks alike.
- Cross-process: yes for advisory `fcntl(F_SETLK)` byte-range locks on
  the DB file (see `crates/fsqlite-vfs/src/unix.rs` lines 275–410). The
  VFS retries `F_SETLK` with exponential backoff up to `busy_timeout`,
  matching stock SQLite. Historical gap
  ([#45](https://github.com/Dicklesworthstone/frankensqlite/issues/45))
  was a non-blocking `F_SETLK` that returned immediately. That issue is closed;
  the swarm harness's `busy_timeout` criterion is the
  regression net.
- On exhaustion, the failure mode is `FrankenError::Busy` (or
  `BusyRecovery` / `BusySnapshot { .. }` for the snapshot-isolation
  variants). It is **never** an indefinite hang and **never** a
  silent zero-rows-committed exit. Callers retry on `Busy*`.
- Granularity: timeout applies per *acquisition attempt*. A long
  transaction holding many page locks does not consume the timeout once
  acquired — only the wait phase does.

### 6. Where fsqlite is **weaker** than stock SQLite

These are intentional or known gaps documented so callers can plan:

- **Multi-process WAL checkpoint coordination** is currently weaker.
  Stock SQLite's checkpoint protocol has been hardened over decades
  against multi-process opener contention; fsqlite's Silo-style epoch
  group commit is newer. Substantial #70 hardening work has landed, while the
  remaining program is tracked by `bd-zywqc`; multi-process *checkpoint* (not
  normal commit) remains the weakest surface. Treat this as a measured
  limitation rather than evidence that the remaining program is complete. The
  symptoms ("WAL file too small for header during rebuild" on warm
  start; transient `freelist trunk page exceeds db_size`) are cataloged
  in the [#70 history](https://github.com/Dicklesworthstone/frankensqlite/issues/70).
  Mitigation: callers should not race `wal_checkpoint(TRUNCATE)` from
  multiple processes; let the auto-checkpointer coordinate, or
  serialize through a single owner.
- **Stale prepared-plan cache vs. concurrent index growth**: stock
  SQLite re-validates prepared statements aggressively on schema or
  catalog change; fsqlite's plan cache participates in cross-process
  visibility but historically has had narrower invalidation windows
  than stock. This is the beads_rust#252/#254/#255 class. Best caller
  defense today: do not pool prepared statements across transaction
  boundaries in code that runs against fsqlite without explicit
  `schema_cookie` checks.
- **Cross-process advisory lock interop with non-fsqlite openers**:
  stock SQLite is the de-facto standard. If you open the same file
  with both fsqlite and stock SQLite *at the same time*, only stock's
  invariants are guaranteed at the wire format. Sequential
  hand-off (close one, open the other) is fine and is what
  conformance tests exercise.
- **No shm-less WAL fallback parity** when shared memory is
  unavailable: stock SQLite degrades gracefully via heap-shm; fsqlite
  falls back to file-lock-based coordination that degrades to
  single-writer (per the README "File-Lock Fallback" section). This is
  a parity gap, not a correctness gap — it is loud, not silent.

### 7. Where fsqlite intends stock-SQLite parity

- Wire format on a checkpointed file is intended to be byte-for-byte
  stock-compatible on the documented supported surface. After successful
  checkpoint, stock SQLite must open the file and
  pass `PRAGMA integrity_check`. This is enforced by
  `crates/fsqlite-e2e/tests/compat_file_format.rs`,
  `crates/fsqlite-e2e/tests/golden_integrity.rs`,
  and the `swarm-multiprocess` harness's `sqlite_integrity_check`
  criterion.
- SQL surface for the conformance corpus: see
  [`docs/canonical_parity_contract.md`](./canonical_parity_contract.md).
- Single-writer + multi-reader WAL semantics: full parity, including
  `busy_timeout` honor, `SQLITE_BUSY` error codes, and `PRAGMA
  wal_autocheckpoint` thresholds.
- `PRAGMA integrity_check` semantics: parity is the target. The current checker
  can miss a referenced empty non-root leaf that stock SQLite reports as
  malformed (`bd-y5urj`), so a green FrankenSQLite result is not by itself a
  complete corruption proof for v0.2.0.
- Connection lifecycle: `open` / `close` semantics match stock —
  including the hand-off via WAL checkpoint on the last connection
  closing.

Where parity is a *goal* but not yet *enforced*: anywhere this
document marks "partial" above. The swarm harness is the binary
boundary between parity-claimed and parity-aspired.

---

## The concurrency contract

### Release-blocked candidate: single-process, multi-Connection via MVCC WAL

- *N* Connections opened against the same file-backed database within
  a single process, coordinated through the MVCC WAL layer
  (`fsqlite-mvcc`, `fsqlite-wal`), with the Connection API per thread.
- Read-your-own-writes: a caller that receives success from `COMMIT` can
  read the committed row back on the same Connection.
- Cross-Connection visibility: after a reader's next transaction
  boundary, committed rows from other Connections are visible.
- `PRAGMA integrity_check = ok` after the workload terminates.

The bullets above are the contract target, not a green statement about the
current candidate. `bd-9inpb` currently fails ordinary implicit-autocommit
writer stress in both WAL and rollback-journal modes while the two explicit
`BEGIN CONCURRENT` barrier controls pass. All four scenarios must pass on the
candidate before this section can return to **Supported**.

**`journal_mode = 'wal'` means MVCC here, not SQLite's single-writer WAL.**
For file-format compatibility, `PRAGMA journal_mode` reports `wal` (and
file-backed databases default to it), but FrankenSQLite intentionally
replaces SQLite's single-writer `WAL_WRITE_LOCK` with page-level MVCC:
while `concurrent_mode_default` is on (the default and a core invariant),
every `BEGIN` auto-promotes to `BEGIN CONCURRENT`, so multiple writers
commit in parallel as long as they touch disjoint pages. Do **not** assume
the classic single-writer WAL contract from the `journal_mode` value alone.
Query the live model with the read-only introspection PRAGMA:

```sql
PRAGMA fsqlite_concurrency;   -- aliases: fsqlite.concurrency, concurrency
```

It returns key/value rows: `journal_mode`, `write_concurrency`
(`concurrent` | `single_writer`), `begin_promotes_to`
(`BEGIN CONCURRENT` | `BEGIN`), `wal_contract` (`mvcc_page_level` |
`sqlite_single_writer` | `n/a`), `write_merge`, and a human-readable
`note`. It is purely observational and changes no behavior. The classic
single-writer WAL contract is available as a comparison/fallback path —
see "Supported: multi-reader, single-writer WAL" below.

**Obligation on callers**: open one Connection per logical worker;
do not try to share a single `Connection` across OS threads — it is
`!Send + !Sync` and will not compile.

### Supported: multi-reader, single-writer WAL

- The classic SQLite WAL contract: readers never block, one writer at
  a time, `PRAGMA busy_timeout` governs contention.
- `busy_timeout` is honored at the Connection API. See
  [frankensqlite#45](https://github.com/Dicklesworthstone/frankensqlite/issues/45)
  for the historical `F_SETLK` gap — check its status before relying on
  cross-process busy-timeout honor.

### Partial: multi-process, multi-writer WAL

This is the path the closed
[#70](https://github.com/Dicklesworthstone/frankensqlite/issues/70) roll-up
described. Historical symptom families were individually patched, and the
meta-issue closed once substantial work landed, handing its remaining scope to
the Beads tree that now carries multi-process durability:

| Family | Example issues | Observable symptom |
|---|---|---|
| WAL header / checkpoint rebuild | [#19](https://github.com/Dicklesworthstone/frankensqlite/issues/19), [#56](https://github.com/Dicklesworthstone/frankensqlite/issues/56) | "WAL file too small for header during rebuild" on warm start |
| Cross-process locking | [#45](https://github.com/Dicklesworthstone/frankensqlite/issues/45) | `F_SETLK` non-blocking ignores `PRAGMA busy_timeout` |
| Process-global state leak | [#56](https://github.com/Dicklesworthstone/frankensqlite/issues/56) | "freelist trunk page exceeds db_size" on third+ open |
| Prepared-plan cache invalidation | beads_rust [#252](https://github.com/Dicklesworthstone/beads_rust/issues/252), [#254](https://github.com/Dicklesworthstone/beads_rust/issues/254), [#255](https://github.com/Dicklesworthstone/beads_rust/issues/255) | `SELECT ... WHERE pk = ?` returns zero rows (or wrong row) for freshly-committed rows; CTE wrapper masks it |
| Schema-changed-mid-INSERT | [#65](https://github.com/Dicklesworthstone/frankensqlite/issues/65) | "database schema has changed" during long rebuilds |
| FK validation on reused prepared stmt | [#59](https://github.com/Dicklesworthstone/frankensqlite/issues/59) | Wrong FK verdict after prepared statement reuse in write txn |
| Import that survives on stock SQLite | [#69](https://github.com/Dicklesworthstone/frankensqlite/issues/69) | fsqlite corrupts, stock SQLite stays clean on same import |

The common shape — "works on stock SQLite, corrupts or mis-reads on
fsqlite under concurrent load" — was what #70 asked to address at the
root rather than through another point fix.

**Current status**: substantial #70 fixes have landed, but the open P0
`bd-zywqc` epic still carries the remaining program. Multi-process multi-writer
remains **partial** until the current candidate's swarm harness is green at
N ≥ 8 for ≥ 1 hour on the target platform. The bound is the measurement, not a
claim that the historical issue closure completed every root-cause gate.

### Not supported today

- Cross-process advisory locks honored by non-fsqlite opener. If you
  open the same file with stock SQLite at the same time as fsqlite,
  each side's invariants are about its own pager, not the other's.
- Lock-free cross-process WAL traversal during checkpoint. Readers on
  other processes may observe transient errors during a checkpoint
  if the checkpoint coordinator has not advanced the visible watermark.

---

## Invariants the harness enforces

The `swarm_multiprocess` harness
(`crates/fsqlite-e2e/src/bin/swarm_multiprocess.rs`) is the canonical
test of every supported-scenario invariant. What it asserts:

1. **No disk-image corruption**: `PRAGMA integrity_check` post-run on
   both fsqlite and stock SQLite opening the same file.
2. **No WAL corruption**: no "WAL page index integrity failure,"
   "short header read," or frame-order anomaly after checkpoint.
3. **No silent lost writes**: a COMMIT success means the value is
   readable back by the same connection and by others after their
   next txn boundary.
4. **No stale-plan reads**: `SELECT ... WHERE pk = ?` for a freshly
   committed row returns that row (not zero rows, not a prior
   committed version).
5. **No silent wrong-row returns**: predicate matches actual row.
6. **No process-global leakage**: Nth `Connection::open` against a
   fresh file succeeds with consistent semantics.
7. **Graceful contention**: `PRAGMA busy_timeout` honored across
   processes; callers see `SQLITE_BUSY`, not hang and not zero-commit.

The canonical validation surface is:

```bash
cargo test -p fsqlite-e2e --test correctness_concurrent_writes
cargo test -p fsqlite-e2e --test mvcc_concurrent_writers
cargo run -p fsqlite-e2e --bin swarm_multiprocess -- \
    --workers 8 --seconds 60
```

When the swarm harness fails, it writes a forensic bundle
under `crates/fsqlite-e2e/artifacts/swarm-multiprocess/run-<ts>-pid<pid>/`.
Those bundles are the source-of-truth reproducers for any issue filed
against this document.

---

## Caller guidance

For caller projects (`beads_rust`, `mcp_agent_mail_rust`, anything
that opens `fsqlite` as a dependency):

### Default assumption

Design around **single-process, multi-Connection via MVCC WAL**; that is the
intended default. Do not deploy the current v0.2.0 candidate under concurrent
write load until `bd-9inpb` is fixed and its four-scenario writer gate is green.
If you think you need multi-process access, re-check — most reports turn out to
be multiple callers that could run in the same process.

### If you really must open from multiple processes

- Treat the swarm as a stress test, not a production load-bearing
  contract. Substantial #70 fixes have landed, but `bd-zywqc` remains open; the
  practical bound is the largest harness scale and duration you have measured
  green on your platform.
- Cap N at whatever your `swarm_multiprocess --workers N --seconds
  3600` run is green on. Publish that number in your caller's own
  README so downstream is not guessing.
- On startup, clean up 0–32-byte WAL sidecars before opening (see
  `mcp_agent_mail_rust/crates/mcp-agent-mail-db/src/pool.rs::cleanup_empty_wal_sidecar`
  for a tested implementation of that pattern).
- On a checkpoint failure with `WAL file too small for header during
  rebuild` or `freelist trunk page exceeds db_size`, do not fail
  closed immediately — it is an fsqlite-known recoverable class. Log,
  clean up the sidecar, and retry once before escalating.
- A green FrankenSQLite `PRAGMA integrity_check` is not a complete corruption
  proof while `bd-y5urj` remains open. Preserve the file, close FrankenSQLite,
  run stock SQLite's checker, and retain the exact reproducer before deciding
  whether the database or an in-process verdict is wrong.

### When you hit a new symptom

Run the harness against your repro:

```bash
cd $(fsqlite-repo)
cargo run -p fsqlite-e2e --bin swarm_multiprocess -- \
    --workers 8 --seconds 300 --seed "$(date +%s)"
```

If the harness is green and your application still corrupts, file an
issue with:

1. The fsqlite commit you are linking against.
2. The forensic-bundle path you captured (see above).
3. Whether stock SQLite opening the same file passes
   `PRAGMA integrity_check` (this is the key diagnostic — if stock
   SQLite also fails, the issue is probably your write path, not
   fsqlite's).
4. Your `PRAGMA`s at open time (especially `journal_mode`,
   `synchronous`, `busy_timeout`, `wal_autocheckpoint`).

A report without (3) is usually a support ticket, not a bug; (3) is
what turns a symptom into a repro against the concurrency contract.

---

## Optional opt-in: refuse multi-process access

For deployments where multi-process swarm access is known to be
unsafe (e.g., the swarm harness red on your configuration, and you
would rather fail closed than degrade), a loud-refusal opt-in would
let callers detect a second-process open and refuse with a
specific `FrankenError::MultiProcessAccessRefused` variant rather than
silently accepting it.

This is **not implemented yet**. It was sketched out in the closed
[#70 triage](https://github.com/Dicklesworthstone/frankensqlite/issues/70)
comment thread as a candidate follow-up and has no live GitHub tracker.
The broader multi-process durability program remains tracked by `bd-zywqc`.
Design notes for whoever
picks it up:

- Use an advisory `fcntl(F_SETLK)` exclusive lock on a sidecar file
  (`.fsqlite-single-process-lock` next to the DB), not on the DB
  file itself — fsqlite's own cross-process locking already owns the
  DB file bytes.
- Gate behind a new `OpenFlags::REFUSE_MULTI_PROCESS` bit so the
  opt-in is explicit; default remains current behavior.
- Emit a distinctive error with the PID of the lock holder so
  operators can identify the other process, rather than a generic
  "locked" message.

When this lands, this document should grow a *"Supported: fail-closed
multi-process refusal"* section above, and the swarm harness should
grow a scenario that proves the refusal actually refuses.

---

## Change log

- **2026-04-20**: Document created in response to #70. Captures the
  contract as it stands at commit `bd770f2f` (Silo-style epoch group
  commit primitive just landed; multi-process swarm path still under
  hardening via the branches enumerated above).
- **2026-04-29**: Added the "seven questions, answered" section in
  response to [#80](https://github.com/Dicklesworthstone/frankensqlite/issues/80).
  Plain-English caller-facing answers to: process count, connection
  lifetime, visibility guarantee, plan-cache visibility, lock-timeout
  semantics, where fsqlite is weaker than stock, where parity is
  intended. Companion regression net is the
  [#79](https://github.com/Dicklesworthstone/frankensqlite/issues/79)
  swarm-writer harness at
  `crates/fsqlite-e2e/tests/swarm_writer_harness.rs`
   (`#[ignore]`-gated; runs via
   `cargo test --profile release-perf -p fsqlite-e2e --test swarm_writer_harness -- --ignored --nocapture --test-threads=1`).
- **2026-08-05**: #70, #79, and #80 are closed. Remaining multi-process work
  moved to the Beads tree; the contract stays *partial* by measurement, and
  multi-process checkpoint remains the weakest surface.
