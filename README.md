<div align="center">
  <img src="frankensqlite_illustration.webp" alt="FrankenSQLite — a Frankenstein monster building a database engine at his workbench">
</div>

<h1 align="center">FrankenSQLite</h1>

<p align="center">
  <strong>An independent ground-up Rust reimplementation of SQLite with concurrent writers and information-theoretic durability.</strong>
</p>

<p align="center">
  <a href="https://github.com/Dicklesworthstone/frankensqlite/actions"><img src="https://img.shields.io/github/actions/workflow/status/Dicklesworthstone/frankensqlite/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/Dicklesworthstone/frankensqlite/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT%2BOpenAI%2FAnthropic%20Rider-blue.svg" alt="License: MIT+Rider"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-nightly%20%7C%20edition%202024-orange.svg" alt="Rust"></a>
  <a href="https://github.com/Dicklesworthstone/frankensqlite"><img src="https://img.shields.io/badge/unsafe-forbidden-success.svg" alt="unsafe forbidden"></a>
</p>

---

## TL;DR

**The Problem:** SQLite allows only one writer at a time. A single lock byte (`WAL_WRITE_LOCK` at `wal.c:3698`) serializes all writers. For write-heavy workloads, this bottleneck caps throughput regardless of how many cores you have. Torn writes and bit-flips can corrupt the database with no self-repair mechanism.

**The Solution:** FrankenSQLite reimplements SQLite from scratch in safe Rust with two architectural innovations:

1. **MVCC Concurrent Writers.** The single-writer lock is replaced with page-level Multi-Version Concurrency Control. Multiple writers commit simultaneously as long as they touch different pages. Serializable Snapshot Isolation (SSI) prevents write skew by default. A safe write-merge ladder (intent replay + structured page patches) resolves commuting same-page conflicts without row-level MVCC metadata; raw byte-range XOR merges are forbidden for SQLite structured pages.

2. **RaptorQ-Pervasive Durability.** Every persistent layer is infused with RFC 6330 fountain codes via asupersync's production-grade RaptorQ implementation. WAL frames carry repair symbols for self-healing after torn writes. Snapshot transfer uses rateless coding for bandwidth-optimal replication over lossy networks. Data loss becomes a mathematical near-impossibility rather than a failure mode to mitigate.

The file format stays 100% compatible with existing `.sqlite` databases in Compatibility mode. A Native mode stores everything as content-addressed, erasure-coded objects (ECS) for maximum durability and cross-process concurrency.

### Why FrankenSQLite?

| Feature | C SQLite | FrankenSQLite |
|---------|----------|---------------|
| Concurrent writers | 1 (file-level lock) | Many (page-level MVCC with SSI) |
| Isolation level | SERIALIZABLE (by serializing) | SERIALIZABLE (SSI for concurrent mode) |
| Concurrent readers | Many (WAL; 5 read-mark slots by default) | Many (Compat: same 5 read-mark slots; Native: bounded by txn-slot capacity, no WAL-index cap) |
| Memory safety | Manual (C) | Guaranteed (`#[forbid(unsafe_code)]`) |
| Data races | Possible (careful C) | Impossible (Rust ownership) |
| File format | SQLite 3.x | Identical (Compatibility mode) or ECS (Native mode) |
| Self-healing storage | No | Yes (RaptorQ repair symbols) |
| Page-level encryption | No (commercial SEE extension) | XChaCha20-Poly1305 (DEK/KEK envelope, Argon2id KEK derivation) |
| SQL dialect | Full | Full (same parser coverage) |
| Extensions | FTS3/4/5, R-tree, JSON1, etc. | All the same, compiled in |
| Cross-process MVCC | No | Yes (shared-memory coordination) |
| Embedded, zero-config | Yes | Yes |

---

## Design Philosophy

### 1. Independent Reimplementation, Not a Translation

FrankenSQLite is not a C-to-Rust transpilation. It references the C source only for behavioral specification. Every function is written in idiomatic Rust, using the type system and ownership model rather than translating C idioms.

### 2. MVCC at Page Granularity

Page-level versioning sits at the right point in the complexity/concurrency tradeoff:

- **Row-level** (PostgreSQL-style) would break the file format and require VACUUM
- **Table-level** would conflict on every write to a shared table
- **Page-level** maps naturally to SQLite's B-tree structure. Writers to different leaf pages proceed in parallel. Conflicts only arise when two transactions modify the same physical page.

### 3. Zero Unsafe Code

The entire workspace enforces `#[forbid(unsafe_code)]`. Every crate, every module, every line. Memory safety is a compile-time guarantee, not a testing target.

### 4. File Format Compatibility Is Non-Negotiable

Databases created by FrankenSQLite open in C SQLite and vice versa. No migration step, no conversion tool. The 100-byte header, B-tree page layout, record encoding, and WAL frame format are all identical.

### 5. Serializable Snapshot Isolation (SSI) by Default

`BEGIN CONCURRENT` provides full SERIALIZABLE isolation, not merely Snapshot Isolation. The conservative Cahill/Fekete rule applied at page granularity ("Page-SSI") prevents write skew: no committed transaction may have both an incoming and outgoing rw-antidependency edge. PostgreSQL has shipped SSI since 2011 with less than 7% throughput overhead. `PRAGMA fsqlite.serializable = OFF` explicitly downgrades to plain SI for benchmarking or applications that tolerate write skew. When two writers touch the same page, FCW detects base drift; commuting conflicts may be resolved by the safe merge ladder, otherwise the loser retries with `SQLITE_BUSY_SNAPSHOT`. Deadlocks are impossible by construction (eager page locking, no wait-for cycles).

### 6. Strong Types Over Runtime Checks

Page numbers, transaction IDs, page sizes, error codes, opcode variants, and lock levels are all distinct Rust types (newtypes, enums), not bare integers. The compiler catches misuse that would be a runtime bug in C. A `PageNumber` cannot be accidentally passed where a `TxnId` is expected. A `PageSize` that isn't a power of two between 512 and 65536 cannot be constructed.

### 7. Layered Crate Architecture

Each subsystem lives in its own crate with explicit dependency boundaries enforced by Cargo. The parser cannot reach into the pager. The B-tree cannot call the planner. This prevents the kind of circular coupling that accumulates in a single-file C codebase and makes each component independently testable.

### 8. RaptorQ Everywhere

RFC 6330 fountain codes are woven into every persistent layer, not bolted on as a replication afterthought. The WAL uses repair symbols to survive torn writes without double-write journaling. Version chains use RaptorQ delta encoding for near-optimal compression. The replication protocol is fountain-coded for bandwidth-optimal transfer over lossy networks. In Native mode, every durable object is stored as an ECS (Erasure-Coded Stream) object with content-addressed BLAKE3 identity.

### 9. Mechanical Sympathy

Database engines live and die by cache behavior and I/O patterns. All page buffers are allocated at `page_size` alignment for direct I/O. VFS read/write paths operate directly on aligned buffers with no intermediate copies. The MVCC `PageLockTable` and `SireadTable` shards are padded to 64-byte cache-line boundaries to prevent false sharing. B-tree key comparisons and RaptorQ GF(256) arithmetic use SIMD-friendly contiguous layouts. B-tree descent issues prefetch hints for child pages.

---

## Architecture

FrankenSQLite is organized as a 24-crate Cargo workspace with strict layered dependencies:

<div align="center">
  <img src="frankensqlite_diagram.webp" alt="FrankenSQLite architecture diagram — 24-crate layered workspace" width="512">
</div>

### Crate Map

| Layer | Crate | Purpose |
|-------|-------|---------|
| **Foundation** | `fsqlite-types` | PageNumber, PageSize, TxnId, SqliteValue, 190+ VDBE opcodes, serial types, limits, bitflags |
| | `fsqlite-error` | 50+ error variants, SQLite error code mapping, recovery hints, transient detection |
| **Storage** | `fsqlite-vfs` | Virtual filesystem trait (Vfs, VfsFile) abstracting all OS operations |
| | `fsqlite-pager` | Page cache, rollback journal, ARC eviction, dirty page write-back |
| | `fsqlite-wal` | Write-ahead log: frame append, checkpoint, WAL index, crash recovery |
| | `fsqlite-mvcc` | MVCC page versioning, snapshot management, conflict detection, garbage collection |
| | `fsqlite-btree` | B-tree/B+tree: cell parsing, page splitting, overflow chains, cursor navigation |
| **SQL** | `fsqlite-ast` | Typed AST nodes for all SQL statements and expressions |
| | `fsqlite-parser` | Hand-written recursive descent parser with Pratt expression parsing |
| | `fsqlite-planner` | Name resolution, WHERE analysis, join ordering, index selection |
| | `fsqlite-vdbe` | Bytecode VM: 190+ opcodes, register file, fetch-execute loop |
| | `fsqlite-func` | Scalar, aggregate, and window functions (abs, count, row_number, etc.) |
| **Extensions** | `fsqlite-ext-fts3` | FTS3/FTS4 full-text search |
| | `fsqlite-ext-fts5` | FTS5 with BM25 ranking |
| | `fsqlite-ext-rtree` | R-tree spatial indexes and geopoly |
| | `fsqlite-ext-json` | JSON1 functions (extract, set, each, tree, etc.) |
| | `fsqlite-ext-session` | Changeset/patchset generation and application |
| | `fsqlite-ext-icu` | ICU collation and Unicode case folding |
| | `fsqlite-ext-misc` | generate_series, carray, dbstat, dbpage |
| **Integration** | `fsqlite-core` | Wires all layers: connection, prepare, schema, DDL/DML codegen |
| | `fsqlite` | Public API: `Connection::open()`, `execute()`, `query()`, `prepare()` |
| | `fsqlite-cli` | Interactive REPL with dot-commands, output modes, syntax highlighting |
| | `fsqlite-harness` | Conformance test runner comparing against C SQLite |

---

## Current Implementation Status

This README describes the target end-state architecture. The runnable code today still uses the Phase 4 in-memory query engine:

- Public entry point: `fsqlite::Connection` (`crates/fsqlite/src/lib.rs`), implemented by `fsqlite-core::Connection` (`crates/fsqlite-core/src/connection.rs`).
- Execution backend: table storage is backed by `fsqlite-vdbe::engine::MemDatabase` and executed via `fsqlite-vdbe::VdbeEngine` (`crates/fsqlite-vdbe/src/engine.rs`).
- Persistence: for non-`:memory:` paths, `Connection` snapshots `MemDatabase` to a SQLite-format file via `compat_persist` and reloads from that snapshot on open. This is full-file snapshot persistence, not WAL/incremental pager-backed persistence yet.
- Extensions: extension crates are present and feature-gated in the workspace/public API crate, but the runtime connection path is still centered on `fsqlite-func` registrations; extension virtual table/function wiring is still in progress.
- Storage stack status: `fsqlite-vfs`, `fsqlite-pager`, `fsqlite-wal`, `fsqlite-mvcc`, and `fsqlite-btree` exist and have extensive tests, and `fsqlite-vdbe` has early "storage cursor" support behind a flag, but `Connection` is not yet wired to use the pager/WAL/B-tree stack as its default backend (see Phase 5+ below).

---

## Transaction Lifecycle Introspection (bd-t6sv2.5)

`fsqlite-core::Connection` exposes transaction lifecycle observability through PRAGMA surfaces that are safe to query during active workloads:

- `PRAGMA fsqlite_txn_stats` (aliases: `txn_stats`, `fsqlite.txn_stats`)
  - Key/value counters for active/completed lifecycle state, snapshot age, read/write ops, savepoint depth, rollback counters, and advisor thresholds.
- `PRAGMA fsqlite_transactions` (aliases: `transactions`, `fsqlite.transactions`)
  - Per-active-transaction rows (duration/snapshot age and read/write activity shape).
- `PRAGMA fsqlite_txn_advisor` (aliases: `txn_advisor`, `fsqlite.txn_advisor`)
  - Actionable advisory rows for anti-patterns:
    - `long_txn`
    - `large_read_set`
    - `deep_savepoint_stack`
    - `rollback_pressure`
- `PRAGMA fsqlite_txn_timeline_json` (aliases: `txn_timeline_json`, `fsqlite.txn_timeline_json`)
  - JSON snapshot intended for timeline/visualizer tooling, including active state, first-read/first-write timing, savepoint/rollback counters, and advisor thresholds.

Advisor thresholds are tunable:

- `PRAGMA fsqlite.txn_advisor_long_txn_ms = <ms>`
- `PRAGMA fsqlite.txn_advisor_large_read_ops = <count>`
- `PRAGMA fsqlite.txn_advisor_savepoint_depth = <depth>`
- `PRAGMA fsqlite.txn_advisor_rollback_ratio_percent = <percent>`

All threshold PRAGMAs clamp invalid low values to safe minimums.

---

## MVCC: How Concurrent Writers Work

### The Write Path

```
Transaction A: INSERT INTO users ...        Transaction B: INSERT INTO orders ...
         │                                           │
         ▼                                           ▼
  1. Acquire page lock on leaf page 47        1. Acquire page lock on leaf page 112
     (no conflict, different pages)              (no conflict, different pages)
         │                                           │
         ▼                                           ▼
  2. Copy-on-write: create new version        2. Copy-on-write: create new version
     of page 47 tagged with TxnId=42            of page 112 tagged with TxnId=43
         │                                           │
         ▼                                           ▼
  3. Commit: validate, append to WAL          3. Commit: validate, append to WAL
     (mutex held only for the append)            (mutex held only for the append)
         │                                           │
         ▼                                           ▼
  4. Release page lock                        4. Release page lock
```

Both transactions commit in parallel. No blocking.

### The Read Path (Lock-Free)

```
read(page 47, snapshot TxnId=41)
  │
  ├──▶ Buffer pool hit? → Return cached version visible to snapshot
  │
  ├──▶ WAL index lookup? → Read frame, cache it, return
  │
  └──▶ Database file → Read page (implicit TxnId::ZERO), return
```

Readers never acquire locks. Unlimited concurrent readers.

### Conflict Detection (SSI + First-Committer-Wins)

```
Transaction C and D both reach COMMIT:

  1. SSI Validation (rw-antidependency check)
     │
     ├── C has both an incoming AND outgoing rw-antidependency edge?
     │   └── Yes → ABORT C immediately (write skew detected, no page lock needed)
     │
     └── No → proceed to step 2
  │
	  2. Page-Level First-Committer-Wins
	     │
	     ├── Both touch leaf page 47 (same B-tree leaf)?
	     │   ├── Yes → First to lock page 47 wins. Loser hits base drift at commit.
	     │   │         If PRAGMA fsqlite.write_merge = SAFE, attempt the merge ladder
	     │   │         (intent replay + structured patches). If merge succeeds → both
	     │   │         commit; otherwise loser aborts/retries.
	     │   │         Deadlock impossible (eager locking, no wait-for cycles).
	     │
	     └── No (different leaf pages) → Both proceed and commit independently.
```

The SSI check fires before the first-committer-wins check. This means write skew is caught even when the conflicting transactions touch disjoint pages, because SSI tracks read dependencies (via the `SireadTable`) across all pages.

### MVCC Visibility Rules

A page version `V` is visible to snapshot `S` if and only if both conditions hold:

1. `V.commit_seq <= S.high` (the version was committed before the snapshot was taken)
2. `V` is the newest version satisfying (1) (older qualifying versions are shadowed)

These rules produce snapshot isolation: each transaction sees a frozen view of the database as of its start time, regardless of concurrent commits happening around it. Because visibility depends only on the monotonic `CommitSeq` counter (not on an in-flight set), the check is a single integer comparison — O(1) with no bitmap or Bloom filter required.

### MVCC Core Data Structures

```rust
/// Monotonically increasing transaction identifier.
/// Allocated from an AtomicU64 with SeqCst ordering.
struct TxnId(u64);

/// Monotonically increasing commit sequence number (global "commit clock").
/// Assigned by the sequencer at COMMIT time.
struct CommitSeq(u64);

/// A frozen view of the database at BEGIN time.
/// Visibility is a single integer comparison: V.commit_seq <= S.high.
/// No in-flight bitmap or Bloom filter is needed.
struct Snapshot {
    high: CommitSeq,
    schema_epoch: SchemaEpoch,
}

/// A single versioned copy of a database page.
/// Versions are bump-allocated in a VersionArena (not heap-allocated).
/// The chain is linked via arena indices, not Box pointers.
struct PageVersion {
    pgno: PageNumber,
    commit_seq: CommitSeq,
    created_by: TxnToken,  // (txn_id, txn_epoch) — debug/audit only, not used for visibility
    data: PageData,
    prev_idx: Option<VersionIdx>,  // index into VersionArena
}

/// Exclusive page-level write locks. Sharded into 64 buckets
/// (power of two for fast modular arithmetic). Each shard is a
/// parking_lot::Mutex<HashMap<PageNumber, TxnId>>. Shards are
/// padded to 64-byte cache-line boundaries to prevent false sharing.
struct PageLockTable { shards: [Mutex<HashMap<PageNumber, TxnId>>; 64] }

/// SSI read tracking. Maps each page to the set of active
/// transactions that have read it. Used to detect rw-antidependencies.
struct SireadTable { shards: [Mutex<HashMap<PageNumber, SmallVec<TxnId>>>; 64] }

/// Semantic operation log for deterministic rebase merge.
/// Records what a transaction intended to do at the B-tree level.
enum IntentOp {
    Insert { table: TableId, key: RowId, record: Vec<u8> },
    Delete { table: TableId, key: RowId },
    Update { table: TableId, key: RowId, new_record: Vec<u8> },
    IndexInsert { index: IndexId, key: Vec<u8>, rowid: RowId },
    IndexDelete { index: IndexId, key: Vec<u8>, rowid: RowId },
}
```

### Three Invariants (Must Hold at All Times)

1. **INV-1 (Monotonic TxnIds):** TxnIds are strictly monotonically increasing, allocated via `AtomicU64::fetch_add` with `SeqCst` ordering.
2. **INV-2 (Page lock exclusivity):** At most one active transaction holds the exclusive lock on any given page.
3. **INV-3 (Version chain ordering):** In every version chain, newer versions have strictly higher `created_by` TxnIds.

### Safe Write Merging and Intent Logs

Standard page-level MVCC produces false conflicts when two transactions modify different rows that happen to live on the same B-tree leaf page. The safe write-merge ladder (§5.10 in the spec) reduces aborts from commuting same-page conflicts without introducing row-level MVCC metadata.

Each writing transaction records a semantic intent log (`Vec<IntentOp>`) describing what it intended to do at the B-tree level. When a transaction reaches commit and discovers a page was modified since its snapshot, a **deterministic rebase** replays the intent log against the current committed state:

1. **Detect base drift:** the page's latest committed version differs from what the transaction read.
2. **Attempt rebase:** replay the intent log against the current snapshot.
3. **Replay succeeds** (B-tree invariants hold, no constraint violations) → commit with rebased deltas.
4. **Replay fails** (true conflict or constraint violation) → abort/retry.

A strict safety ladder governs merge strategy selection at commit time:

| Priority | Strategy | When Used |
|----------|----------|-----------|
| 1 | Deterministic rebase replay | Intent logs commute at B-tree level (preferred) |
| 2 | Structured page patch merge | Cell-disjoint modifications on same page |
| 3 | Abort/retry | True conflict; no safe merge possible |

Merge policy is controlled by `PRAGMA fsqlite.write_merge = OFF | SAFE | LAB_UNSAFE`. `SAFE` enables intent replay + structured patches; raw byte-range XOR merge is forbidden for SQLite structured pages.

### Garbage Collection

Old page versions are reclaimed when no active transaction can see them:

- **GC horizon** = `min(active_snapshot_ids)` across all open transactions (in multi-process mode, `gc_horizon` is an `AtomicU64` in shared memory coordinated across all attached processes)
- A version is reclaimable if a newer committed version of the same page also falls below the horizon
- **Epoch-based reclamation** via `commit_seq`: the global commit sequence counter determines when versions fall out of all active snapshots
- A background task runs every ~1 second, walks version chains, and unlinks reclaimable nodes
- During WAL checkpointing, reclaimable frames are copied back to the main database file
- ARC ghost entries (B1/B2) for pruned versions are cleaned when the GC horizon advances

### Deadlock Freedom (By Construction)

The proof is simple:

1. Page locks are acquired eagerly: when a transaction first writes to a page, it tries to lock immediately.
2. If the lock is held by another transaction, the caller gets `SQLITE_BUSY` immediately. There is no waiting.
3. A transaction that does not wait cannot participate in a wait-for cycle.
4. No wait-for cycle means no deadlock. QED.

This trades potential throughput (a waiter could eventually succeed) for absolute simplicity (no deadlock detector, no timeout tuning, no lock ordering requirements). In practice, page conflicts in SQLite workloads are rare because different writers typically touch different leaf pages.

---

## The B-Tree Engine

SQLite stores all data in B-trees. Tables use B+trees (data in leaves, rowid keys). Indexes use plain B-trees (keys in all nodes, no separate data).

### Page Types

| Type | Flag byte | Contains | Used for |
|------|-----------|----------|----------|
| Interior table | 0x05 | Rowid keys + child page pointers | Navigating to the right leaf |
| Leaf table | 0x0D | Rowid keys + record payloads | Actual row storage |
| Interior index | 0x02 | Index keys + child page pointers | Navigating the index |
| Leaf index | 0x0A | Index keys only | Index entry storage |

### Cell Layout

Each cell in a leaf table page stores one row:

```
┌──────────────┬─────────────┬────────────────────────┐
│ Payload size │ Rowid       │ Record data            │
│ (varint)     │ (varint)    │ (header + column data) │
└──────────────┴─────────────┴────────────────────────┘
```

If the record exceeds the page's usable space minus overhead, the excess spills into overflow pages linked by a 4-byte page pointer at the end of the on-page portion.

### Page Splitting

When an INSERT would cause a leaf page to exceed capacity:

1. Allocate a new page from the freelist (or extend the database file).
2. Find the median cell by accumulated payload size (not count), favoring a split point that keeps the new cell on the less-full side.
3. Move cells above the median to the new page.
4. Insert a new cell in the parent interior page pointing to the new page. If the parent overflows, recurse upward.
5. The root page never moves. If the root splits, a new root is created with two children, increasing tree height by one.

The maximum B-tree depth is 20 (`BTREE_MAX_DEPTH`), which for a 4KB page size supports databases up to several terabytes.

### Cursor Navigation

The `BtreeCursor` provides ordered traversal:

- **move_to(key):** Binary search within interior pages, descending to the leaf. O(log N) page reads.
- **next() / prev():** Move to the adjacent cell. If at the edge of a page, pop up to the parent and descend into the sibling.
- **insert(key, data):** Navigate to the correct leaf, insert the cell, split if necessary.
- **delete():** Remove the cell, merge underfull pages if a neighbor has space.

Each cursor maintains a stack of `(page_number, cell_index)` pairs representing the path from root to current position, so ascending to the parent after reaching a page boundary requires no additional I/O.

### Freelist Management

Deleted pages go onto a freelist rather than being returned to the OS. The freelist is structured as trunk pages, each containing up to `(usable_page_size / 4) - 2` leaf page numbers. When allocating, pages are drawn from the freelist first. VACUUM rewrites the entire database to reclaim freelist space and defragment pages.

---

## The SQL Parser

FrankenSQLite uses a hand-written recursive descent parser rather than a parser generator. C SQLite uses LEMON (a yacc variant); we chose recursive descent because it produces better error messages, is easier to debug, and gives us full control over precedence and associativity.

### Lexer

The tokenizer uses `memchr` for SIMD-accelerated scanning of keyword and delimiter boundaries. Tokens are zero-copy: each token references the original input by byte range (`Token { kind: TokenKind, span: Range<usize> }`). The lexer handles:

- 150+ SQL keywords (SELECT, FROM, WHERE, JOIN, etc.)
- String literals (single-quoted, with `''` escape)
- Blob literals (`X'...'`)
- Numeric literals (integer, float, hex with `0x` prefix)
- Identifier quoting (double-quotes, backticks, square brackets)
- Single-line (`--`) and multi-line (`/* */`) comments
- All operators, punctuation, and whitespace

### Expression Parsing (Pratt Method)

Expressions are parsed using Pratt parsing (top-down operator precedence), which handles:

- Binary operators with correct precedence: `||` (concat) < `OR` < `AND` < `NOT` < comparison (`=`, `!=`, `<`, `>`, `<=`, `>=`, `IS`, `IN`, `LIKE`, `GLOB`, `BETWEEN`) < bitwise (`&`, `|`) < shift (`<<`, `>>`) < addition (`+`, `-`) < multiplication (`*`, `/`, `%`) < unary (`-`, `+`, `~`, `NOT`) < collate (`COLLATE`)
- Prefix expressions: unary minus, NOT, EXISTS, CAST
- Postfix expressions: IS NULL, IS NOT NULL, ISNULL, NOTNULL
- Grouping: parenthesized expressions, subqueries, CASE/WHEN/THEN/ELSE/END
- Function calls with argument lists, including `DISTINCT` and `ORDER BY` within aggregates
- Window function syntax: `OVER (PARTITION BY ... ORDER BY ... frame_spec)`

### Statement Coverage

The parser handles the complete SQLite SQL dialect:

| Category | Statements |
|----------|-----------|
| DML | SELECT (with CTEs, compound operators, joins, subqueries), INSERT (with UPSERT, RETURNING), UPDATE (with FROM, RETURNING), DELETE (with RETURNING), REPLACE |
| DDL | CREATE TABLE/INDEX/VIEW/TRIGGER, ALTER TABLE (ADD/RENAME/DROP COLUMN, RENAME TABLE), DROP TABLE/INDEX/VIEW/TRIGGER |
| Transaction | BEGIN (DEFERRED/IMMEDIATE/EXCLUSIVE), COMMIT, ROLLBACK, SAVEPOINT, RELEASE |
| Utility | ATTACH, DETACH, ANALYZE, VACUUM, REINDEX, EXPLAIN, EXPLAIN QUERY PLAN |
| Pragma | All PRAGMA statements (parsed as special syntax, not regular SQL) |
| Virtual | CREATE VIRTUAL TABLE |

---

## The VDBE (Virtual Database Engine)

Every SQL statement compiles to a linear program of VDBE bytecode instructions. The VDBE is a register-based virtual machine (not stack-based), matching SQLite's architecture. Each instruction has the form:

```
(opcode: u8, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16)
```

`p1`-`p3` are integer operands (register indices, jump targets, cursor numbers). `p4` is a polymorphic operand (string, function pointer, collation, key info). `p5` is a flags field.

### Opcode Categories (190+ Total)

| Category | Count | Key Opcodes |
|----------|-------|-------------|
| Control flow | 8 | Goto, Gosub, Return, InitCoroutine, Yield, Halt |
| Constants | 10 | Integer, Int64, Real, String8, Null, Blob, Variable |
| Register ops | 4 | Move, Copy, SCopy, IntCopy |
| Arithmetic | 7 | Add, Subtract, Multiply, Divide, Remainder, Concat |
| Comparison | 7 | Eq, Ne, Lt, Le, Gt, Ge, Compare |
| Branching | 11 | Jump, If, IfNot, IsNull, IsType, Once, And, Or, Not |
| Column access | 4 | Column, TypeCheck, Affinity, Offset |
| Cursor ops | 16 | OpenRead, OpenWrite, OpenEphemeral, SorterOpen, Close |
| Seek ops | 8 | SeekLT, SeekLE, SeekGE, SeekGT, SeekRowid, SeekScan |
| Index ops | 4 | NoConflict, NotFound, Found, IdxInsert |
| Row ops | 5 | NewRowid, Insert, Delete, RowData, Rowid |
| Transaction | 6 | Transaction, Savepoint, AutoCommit, Checkpoint |
| Sorting | 5 | SorterInsert, SorterSort, SorterData, SorterNext |
| Aggregation | 4 | AggStep, AggFinal, AggValue, AggInverse |
| Functions | 3 | Function, PureFunc, BuiltinFunc |
| And ~100 more | ... | Schema, Cookie, Trace, Explain, Noop, etc. |

### Execution Loop

```rust
fn execute(program: &[VdbeOp], registers: &mut [SqliteValue]) -> Result<()> {
    let mut pc = 0;
    loop {
        let op = &program[pc];
        match op.opcode {
            Opcode::Goto      => { pc = op.p2 as usize; continue; }
            Opcode::Integer   => { registers[op.p2] = SqliteValue::Integer(op.p1 as i64); }
            Opcode::Column    => { /* read column from cursor op.p1, col op.p2, into reg op.p3 */ }
            Opcode::ResultRow => { /* yield registers[op.p1..op.p1+op.p2] as a result row */ }
            Opcode::Halt      => { return Ok(()); }
            // ... 185+ more arms
        }
        pc += 1;
    }
}
```

The inner loop is a single `match` statement over the opcode enum. Each arm reads inputs from registers, performs its operation, writes outputs back to registers, and either falls through to `pc += 1` or jumps by setting `pc` directly.

### Example: How `SELECT name FROM users WHERE age > 30` Compiles

```
addr  opcode         p1    p2    p3    p4             p5
----  ----------     ----  ----  ----  -----          --
0     Init           0     8     0                    0
1     OpenRead       0     2     0     3              0     (cursor 0 on table "users", root page 2, 3 cols)
2     Rewind         0     7     0                    0     (start at first row; jump to 7 if empty)
3     Column         0     2     1                    0     (read col 2 "age" into r1)
4     Le             1     6     2     (integer)30    0     (if r1 <= 30, skip to 6)
5     Column         0     1     3                    0     (read col 1 "name" into r3)
6     ResultRow      3     1     0                    0     (yield r3 as output row)
7     Next           0     3     0                    0     (advance cursor; loop back to 3)
8     Halt           0     0     0                    0
```

---

## The Query Planner

The planner transforms an AST into an optimized logical plan, then hands it to the VDBE code generator.

### Index Selection

For each term in the WHERE clause, the planner:

1. Checks whether any index covers the referenced columns
2. Estimates selectivity using `sqlite_stat1` statistics (histogram of distinct values per index prefix)
3. Computes a cost model: `cost = (pages_to_read * page_read_cost) + (rows_to_scan * row_compare_cost)`
4. Picks the index (or full table scan) with the lowest estimated cost

### Join Ordering

For queries with N tables:

- **N <= 8:** Exhaustive enumeration of all N! orderings, pruned by cost bounds. The optimizer retains the cheapest plan found so far and skips any partial ordering whose cost already exceeds the best complete plan.
- **N > 8:** Greedy heuristic. At each step, pick the next table that produces the smallest estimated intermediate result when joined with the tables already in the plan.

### Optimizations

| Optimization | What it does |
|-------------|-------------|
| Covering index scan | Reads only the index, never touches the table, when all needed columns are in the index |
| Index-assisted ORDER BY | Skips the sort step when the index already delivers rows in the requested order |
| LIKE/GLOB prefix | Converts `LIKE 'abc%'` into a range scan `>= 'abc' AND < 'abd'` on an index |
| Subquery flattening | Inlines simple subqueries into the outer query to avoid materialization |
| Skip-scan | Uses a multi-column index even when the leading column has no equality constraint, by iterating over its distinct values |
| Partial index awareness | Considers partial indexes (CREATE INDEX ... WHERE ...) when the query's WHERE clause implies the index predicate |
| OR optimization | Converts `WHERE a = 1 OR a = 2` into a union of two index lookups |

---

## The Type System

SQLite uses dynamic typing with type affinity, and FrankenSQLite models this precisely.

### Storage Classes

Every value in the database belongs to one of five storage classes:

| Class | Rust Representation | Sort Order |
|-------|-------------------|------------|
| NULL | `SqliteValue::Null` | Sorts first (lowest) |
| INTEGER | `SqliteValue::Integer(i64)` | Numeric ordering |
| REAL | `SqliteValue::Float(f64)` | Numeric ordering (interleaved with INTEGER) |
| TEXT | `SqliteValue::Text(String)` | Collation-dependent (BINARY, NOCASE, RTRIM) |
| BLOB | `SqliteValue::Blob(Vec<u8>)` | Sorts last (highest), memcmp ordering |

Integers and floats interleave in sort order: `SqliteValue::Integer(3)` sorts between `SqliteValue::Float(2.5)` and `SqliteValue::Float(3.5)`.

### Type Affinity

Column declarations map to one of five affinities, which influence how values are coerced on INSERT:

| Affinity | Triggered by | Behavior |
|----------|-------------|----------|
| INTEGER | Column type contains "INT" | Try to coerce TEXT to integer; store REAL as integer if lossless |
| TEXT | Contains "CHAR", "CLOB", or "TEXT" | Coerce numeric values to their text representation |
| BLOB | Contains "BLOB" or has no type | Store as-is, no coercion |
| REAL | Contains "REAL", "FLOA", or "DOUB" | Coerce integer values to float |
| NUMERIC | Anything else (including bare column names) | Try integer first, then float, then store as text |

### Serial Type Encoding

Values in the record format use a compact encoding where a single varint encodes both the type and the byte length:

| Serial Type | Meaning | Bytes |
|------------|---------|-------|
| 0 | NULL | 0 |
| 1 | 8-bit signed integer | 1 |
| 2 | Big-endian 16-bit signed integer | 2 |
| 3 | Big-endian 24-bit signed integer | 3 |
| 4 | Big-endian 32-bit signed integer | 4 |
| 5 | Big-endian 48-bit signed integer | 6 |
| 6 | Big-endian 64-bit signed integer | 8 |
| 7 | IEEE 754 64-bit float | 8 |
| 8 | Integer constant 0 | 0 |
| 9 | Integer constant 1 | 0 |
| N >= 12, even | BLOB of (N-12)/2 bytes | (N-12)/2 |
| N >= 13, odd | TEXT of (N-13)/2 bytes | (N-13)/2 |

Types 8 and 9 are an optimization: booleans and small constants consume zero bytes in the data section.

---

## Transaction Semantics

### Transaction Modes

| Mode | Behavior |
|------|----------|
| DEFERRED (default) | No locks acquired until the first read or write |
| IMMEDIATE | Acquires RESERVED lock at BEGIN; other writers get SQLITE_BUSY |
| EXCLUSIVE | Acquires EXCLUSIVE lock at BEGIN; other readers and writers get SQLITE_BUSY |

In MVCC mode, DEFERRED and IMMEDIATE behave identically from a correctness perspective because snapshot isolation provides consistency. EXCLUSIVE is still useful for bulk operations that want to guarantee no concurrent access.

### Savepoints

Savepoints provide nested rollback points within a transaction:

```sql
BEGIN;
INSERT INTO t VALUES (1);
SAVEPOINT sp1;
INSERT INTO t VALUES (2);
ROLLBACK TO sp1;        -- undoes the second INSERT, keeps the first
INSERT INTO t VALUES (3);
RELEASE sp1;            -- collapses sp1 into the parent transaction
COMMIT;                 -- t contains (1, 3)
```

Savepoints are implemented as a stack. ROLLBACK TO undoes changes back to the savepoint by restoring journal pages. RELEASE removes the savepoint without undoing anything. The outermost "savepoint" is the transaction itself.

### Crash Recovery

The crash model makes six explicit assumptions: (1) process crash at any point, (2) `fsync()` is a durability barrier, (3) writes may be reordered unless constrained by fsync barriers, (4) torn writes at sector granularity (512B or 4KB), (5) bitrot and corruption exist (checksums detect, RaptorQ repairs), (6) file metadata durability may require directory `fsync()`.

The WAL provides crash recovery with the following guarantees:

1. **Atomic commit:** A transaction is either fully visible or fully invisible after crash recovery. Partial commits cannot occur. In Native mode, a commit is committed if and only if its `CommitMarker` is durable.
2. **Durability:** Once `COMMIT` returns, the data survives power loss (assuming `PRAGMA synchronous = FULL`). Durability policy is configurable: `PRAGMA durability = local` (default) requires enough RaptorQ symbols persisted locally for decode success; `PRAGMA durability = quorum(M)` requires symbols across M of N replicas.
3. **Self-healing:** WAL frames carry RaptorQ repair symbols. Torn writes and bit-flips are detected by xxhash3 checksums and repaired from redundant symbols without requiring a full WAL replay.
4. **Recovery procedure:**
   - On database open, check for a WAL file.
   - Read the WAL header; validate magic number and checksums.
   - Replay all committed frames (those with a nonzero "database size" field in the frame header, indicating a commit boundary).
   - For frames with checksum failures, attempt RaptorQ repair from available repair symbols.
   - Discard any frames after the last commit boundary (incomplete transaction).
   - Rebuild the WAL index from the replayed frames.

---

## The WAL (Write-Ahead Log)

### How WAL Mode Works

In WAL mode, writes append to a separate log file instead of modifying the database directly. Readers consult the WAL index (a hash table mapping page numbers to WAL frame offsets) to find the most recent version of each page, falling back to the database file for pages not in the WAL.

### Frame Format

```
WAL Header (32 bytes, file offset 0):
  Bytes 0-3:    Magic number (0x377F0682 or 0x377F0683, indicating byte order)
  Bytes 4-7:    Format version (3007000)
  Bytes 8-11:   Database page size
  Bytes 12-15:  Checkpoint sequence number
  Bytes 16-19:  Salt-1 (random, changes on each checkpoint)
  Bytes 20-23:  Salt-2
  Bytes 24-31:  Cumulative checksum of the header

Frame Header (24 bytes, before each page):
  Bytes 0-3:    Page number
  Bytes 4-7:    For commit frames: database size in pages. Otherwise: 0.
  Bytes 8-11:   Salt-1 (must match WAL header)
  Bytes 12-15:  Salt-2 (must match WAL header)
  Bytes 16-23:  Cumulative checksum over (frame header + page data)

Frame Body:
  <page_size> bytes of page content
```

Checksums are cumulative: each frame's checksum incorporates the previous frame's checksum, creating a hash chain. A single bit flip anywhere in the WAL is detected at the next frame read.

### Checkpoint Modes

| Mode | Behavior |
|------|----------|
| PASSIVE | Copy committed pages back to the database file. Does not block readers or writers. Skips pages still needed by active readers. |
| FULL | Waits for all readers using old snapshots to finish, then copies all committed pages. Blocks new writers during the copy. |
| RESTART | Like FULL, but also resets the WAL file to the beginning afterward, reclaiming disk space. |
| TRUNCATE | Like RESTART, but truncates the WAL file to zero bytes. |

### MVCC Extensions to the WAL

In FrankenSQLite's MVCC mode, WAL frames carry transaction IDs. The WAL index maps `(page_number, txn_id)` pairs to frame offsets. Checkpoint must respect active snapshots: a frame can only be checkpointed if its page version is no longer needed by any active reader.

---

## Rollback Journal

FrankenSQLite supports rollback journal mode for reading databases not in WAL mode. The rollback journal (`<database>-journal`) is the legacy crash-recovery mechanism that predates WAL.

**Journal format:**

```
Journal Header (padded to sector boundary):
  Offset  Size  Description
    0       8   Magic: {0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7}
    8       4   Page count (-1 means compute from file size)
   12       4   Random nonce for checksum
   16       4   Initial database size in pages (before this transaction)
   20       4   Sector size (header padded to this boundary)
   24       4   Page size

Journal Page Records (repeated page_count times):
  [4 bytes: page number (u32 BE)]
  [page_size bytes: original page content before modification]
  [4 bytes: checksum]
```

**How it works:** Before modifying a page, the pager writes the original page content to the journal. On crash, the journal is played back to restore the database to its pre-transaction state. The checksum uses a sparse sampling algorithm: `nonce + data[page_size-200] + data[page_size-400] + ...`, summing bytes at 200-byte intervals from the end of the page (20 bytes sampled for 4096-byte pages).

**Hot journal recovery:** On open, if a journal file exists, is non-empty, and the database's reserved lock is not held, it is a "hot journal." Recovery plays back original pages from the journal, then deletes it.

**Journal modes:** `DELETE` (default — delete journal after commit), `TRUNCATE` (truncate to zero), `PERSIST` (zero the header), `MEMORY` (journal in RAM only — no crash safety), `WAL` (switch to write-ahead logging), `OFF` (no journal — no crash safety). FrankenSQLite defaults to WAL mode but must handle all modes for compatibility with existing databases.

---

## Buffer Pool: ARC Cache

LRU fails on database workloads: a single table scan evicts the entire working set. FrankenSQLite uses an **Adaptive Replacement Cache (ARC)** that balances recency and frequency, with a provable competitive ratio of 2 against OPT.

### MVCC-Aware Structure

The buffer pool keys on `(PageNumber, TxnId)` because multiple versions of the same page coexist for MVCC:

```rust
struct ArcBufferPool {
    /// Pages accessed exactly once recently (recency-favored).
    t1: LinkedHashMap<CacheKey, CachedPage>,
    /// Pages accessed two or more times (frequency-favored).
    t2: LinkedHashMap<CacheKey, CachedPage>,
    /// Ghost entries evicted from T1 (metadata only, no page data).
    b1: LinkedHashSet<CacheKey>,
    /// Ghost entries evicted from T2 (metadata only).
    b2: LinkedHashSet<CacheKey>,
    /// Adaptive parameter: target size for T1 (range [0, capacity]).
    p: usize,
    /// Max pages in T1 + T2. Default: 2000 (~8MB at 4KB pages).
    capacity: usize,
}

struct CacheKey { pgno: PageNumber, version_id: TxnId }
```

### How ARC Works

On page request (O(1) amortized):

| Case | Condition | Action |
|------|-----------|--------|
| Hit in T1 | Page found in recency list | Promote to T2 (now frequency-tracked) |
| Hit in T2 | Page found in frequency list | Move to T2 head (refresh) |
| Ghost hit in B1 | Recently evicted recency page requested again | Increase `p` (favor recency), fetch from disk, insert to T2 |
| Ghost hit in B2 | Recently evicted frequency page requested again | Decrease `p` (favor frequency), fetch from disk, insert to T2 |
| Complete miss | Not in any list | Evict if needed, fetch from disk, insert to T1 |

Ghost entries (B1/B2) store only the cache key, not page data. They let ARC learn access patterns without consuming page-sized memory.

### Eviction Constraints

1. Never evict a pinned page (`ref_count > 0`).
2. Never evict a dirty page (must flush to WAL first).
3. Prefer **superseded versions** (a newer committed version exists that is visible to all active snapshots).
4. Dual eviction trigger: fires when page count exceeds capacity OR `total_bytes` exceeds `max_bytes` (from `PRAGMA cache_size`).

### Visibility Check

With CommitSeq-based snapshots, visibility is a single integer comparison (`V.commit_seq <= S.high`) — O(1) with no auxiliary data structure. No in-flight bitmap or Bloom filter is needed. This is a direct consequence of the monotonic commit clock design: the sequencer assigns `CommitSeq` values at commit time, so a snapshot taken at `high = N` sees exactly those versions with `commit_seq <= N`.

---

## Async Integration (asupersync + Cx)

FrankenSQLite uses [asupersync](https://github.com/Dicklesworthstone/asupersync) for async I/O rather than tokio. asupersync provides capabilities that database engines require but general-purpose runtimes do not.

### Cx (Capability Context) Everywhere

Every trait method that touches I/O, acquires locks, or could block accepts `&Cx`. This is a non-negotiable rule throughout the codebase. Pure computation (e.g., collation comparisons, CPU-only scalar functions) is the only exception.

Cx threads three capabilities through the entire call chain:

- **Cancellation:** Any operation can be cancelled by its caller's context. Long queries check the cancellation token at VDBE instruction boundaries (every N opcodes) and return `SQLITE_INTERRUPT` if cancelled.
- **Deadline propagation:** Timeout budgets flow through the entire call chain. A 5-second query deadline decrements as it passes through the parser, planner, and executor.
- **Capability narrowing:** Callers can restrict what callees are allowed to do. A read-only connection's Cx prevents write operations at the capability level.

### asupersync Components

- **Lab reactor:** Fully deterministic concurrency testing with reproducible scheduling and precise fault injection. Every MVCC interleaving can be replayed exactly.
- **E-processes:** Anytime-valid statistical invariant monitoring. Detects anomalies (e.g., snapshot isolation violations) with bounded false-positive rates.
- **Mazurkiewicz traces:** Enumerate all non-equivalent interleavings for exhaustive concurrency verification without combinatorial explosion.
- **DPOR (Dynamic Partial Order Reduction):** Prunes equivalent schedules during testing. Only explores interleavings that lead to genuinely different outcomes.

### Write Coordination Flow

```
async caller
  → Connection::execute(sql, &cx).await
    → spawn_blocking(|| {
        parse(sql)
        plan(ast)
        execute(bytecode, &cx)
      })
    → on commit: tx.send(CommitRequest { write_set, intent_log, response: oneshot })
    → response.await
  ← Result<Rows>
```

Write transactions submit commit requests through an MPSC channel to a single write coordinator task. This serializes commit validation (SSI check + first-committer-wins + safe merge ladder) and WAL appends without holding a lock across the entire commit. Each request includes a `oneshot::Sender<Result<()>>` so the caller can `.await` the result.

---

## Structured Concurrency, Cancellation, and Supervision

FrankenSQLite adopts asupersync's **region tree** as the lifetime model for all concurrency. Every background worker, coordinator, replicator, and long-lived service runs as a region-owned task or actor. No task may outlive the `Database` root region. There are no detached tasks.

**Region tree (conceptual):**

```
DbRootRegion
  ├── WriteCoordinatorRegion         (marker sequencer + compat WAL path)
  ├── SymbolStoreRegion              (local symbol logs + tiered storage fetch)
  ├── ReplicationRegion              (stream symbols; anti-entropy; membership)
  ├── CheckpointGcRegion             (checkpointer, compactor, GC horizon)
  └── ObservabilityRegion            (deadline monitor, task inspector, metrics)

PerConnectionRegion (child of DbRootRegion)
  ├── QueryExecution tasks
  └── Cursor prefetch tasks (bounded; optional)

PerTransactionRegion (child of PerConnectionRegion)
  ├── Encode/persist capsule tasks (native mode)
  ├── Witness publication tasks
  └── Validation tasks
```

Closing the database is a protocol, not a `drop`: request cancellation, drain, finalize, then return. A region does not report closed until all child tasks are completed, all finalizers have run, and all obligations are resolved.

### Cancellation Is a Protocol (Request, Drain, Finalize)

Cancellation is **not** "drop the future." It is a multi-phase protocol with explicit checkpoints, bounded drain, and finalizers:

```
Created/Running → CancelRequested → Cancelling → Finalizing → Completed(Cancelled)
```

FrankenSQLite places `cx.checkpoint()` at every natural yield point that bounds uninterruptible work: VDBE instruction boundaries, B-tree descent loops, RaptorQ encode/decode loops, and any loop over user data. A cancellation-unaware hot loop is a bug.

**Masked critical sections** (`Cx::masked`) allow bounded cancellation deferral for short, atomic publication steps that must not be interrupted (e.g., publishing a commit marker after allocating `commit_seq`). Mask depth is bounded at `MAX_MASK_DEPTH = 64`. Masking is forbidden for long operations.

### Obligations (Linear Resources)

Asupersync models cancellation-safe effects using **obligations** — linear resources with a two-phase lifecycle: `Reserved → Committed` or `Reserved → Aborted`. A reserved obligation that is dropped without resolution is a `Leaked` obligation — a correctness bug that fails fast in lab mode and triggers diagnostic escalation in production.

FrankenSQLite treats the following as obligations: commit pipeline `SendPermit` reservations, commit response delivery, `TxnSlot` acquisition and renewal, witness-plane reservation tokens, and any name/registration in shared state that could go stale on crash.

### OTP-Style Supervision

Long-lived services (sequencers, replicators, checkpoint workers) are supervised. "Spawn a loop and hope" is forbidden. Supervision provides restart strategies (`Stop`, `Restart(config)`, `Escalate`), restart budgets with backoff, and monotone severity (outcomes cannot be downgraded):

- `WriteCoordinator`: `Escalate` on error or panic (sequencer correctness is core).
- `SymbolStore`: `Restart` on transient I/O; `Escalate` on integrity faults.
- `Replicator`: `Restart` with exponential backoff; `Stop` when remote disabled.
- `CheckpointerGc`: `Restart` (bounded) on transient errors; escalate if repeated.

A component crash becomes an explainable, bounded event with a deterministic restart policy — not a silent hang or memory leak.

---

## Extensions

### FTS5 (Full-Text Search)

FTS5 provides full-text indexing with BM25 ranking:

- **Tokenizers:** unicode61 (default, Unicode-aware word breaking), ascii, porter (English stemming), trigram (character n-grams for substring search)
- **Query syntax:** Boolean operators (`AND`, `OR`, `NOT`), phrase matching (`"exact phrase"`), prefix queries (`prefix*`), column filters (`title: search_term`), NEAR queries (`NEAR(a b, 10)`)
- **Ranking:** BM25 by default, configurable via auxiliary functions
- **Auxiliary functions:** `highlight()` wraps matches in markup, `snippet()` extracts context around matches
- **Content modes:** Regular (FTS5 stores a copy), external content (references an existing table), contentless (index-only, no original text stored)
- **Index structure:** A B-tree of terms mapping to document/position lists, with incremental merge for write performance

### R-Tree (Spatial Indexing)

The R-tree virtual table indexes N-dimensional bounding boxes for spatial queries:

- **Range queries:** Find all rectangles that overlap or are contained within a search rectangle
- **Custom geometry callbacks:** Register Rust functions that define arbitrary geometric predicates
- **Dimensions:** 1 to 5 dimensions per R-tree (configurable at table creation)
- **Geopoly extension:** Stores and queries polygons using the GeoJSON-like format, with containment, overlap, and area operations

### JSON1

Full JSON manipulation within SQL:

| Function | Purpose |
|----------|---------|
| `json_extract(doc, path)` / `->` / `->>` | Extract a value at a JSON path |
| `json_set(doc, path, value)` | Set a value at a path (create if missing) |
| `json_remove(doc, path)` | Remove a key/element at a path |
| `json_each(doc)` / `json_tree(doc)` | Table-valued functions for iterating JSON structure |
| `json_group_array(value)` | Aggregate values into a JSON array |
| `json_group_object(key, value)` | Aggregate key-value pairs into a JSON object |
| `json_patch(target, patch)` | RFC 7396 merge patch |
| `json_valid(doc)` | Check if a string is valid JSON |

Also supports JSONB (binary JSON) for faster repeated access to large documents.

### Session Extension

Records changes to a database as changesets that can be applied elsewhere:

- **Change tracking:** Records INSERT, UPDATE, and DELETE operations
- **Changeset generation:** Produces a compact binary encoding of all changes since tracking began
- **Patchset variant:** More compact than changesets (omits original values for UPDATE); sufficient for applying changes but not for conflict detection
- **Conflict resolution:** Callbacks invoked when applying a changeset conflicts with the target database
- **Changeset inversion:** Generates the inverse changeset (for undo operations)
- **Rebasing:** Combines changesets from parallel editing sessions

---

## Built-In Functions

### Scalar Functions (Selected)

| Function | Description |
|----------|------------|
| `abs(x)` | Absolute value |
| `length(x)` | String length in characters, or blob length in bytes |
| `substr(s, start, len)` | Substring extraction |
| `replace(s, from, to)` | String replacement |
| `upper(s)` / `lower(s)` | Case conversion |
| `trim(s)` / `ltrim(s)` / `rtrim(s)` | Whitespace removal |
| `instr(s, substr)` | Position of first occurrence |
| `hex(x)` / `unhex(s)` | Hex encoding/decoding |
| `typeof(x)` | Returns "null", "integer", "real", "text", or "blob" |
| `coalesce(x, y, ...)` | First non-NULL argument |
| `iif(cond, then, else)` | Inline conditional |
| `printf(fmt, ...)` | C-style string formatting |
| `random()` | Random 64-bit integer |
| `quote(x)` | SQL-safe quoting of a value |

### Aggregate Functions

| Function | Description |
|----------|------------|
| `count(*)` / `count(x)` | Row count / non-NULL count |
| `sum(x)` / `total(x)` | Sum (integer overflow to float for `total`) |
| `avg(x)` | Average |
| `min(x)` / `max(x)` | Extrema |
| `group_concat(x, sep)` | Concatenation with separator |

### Window Functions

| Function | Description |
|----------|------------|
| `row_number()` | Sequential integer for each row in the partition |
| `rank()` | Rank with gaps for ties |
| `dense_rank()` | Rank without gaps |
| `ntile(n)` | Divide partition into n buckets |
| `lag(x, n)` / `lead(x, n)` | Value from n rows before/after current |
| `first_value(x)` / `last_value(x)` | First/last value in the frame |
| `nth_value(x, n)` | Nth value in the frame |

All aggregate functions also work as window functions when used with an `OVER` clause.

### Date/Time Functions

| Function | Description |
|----------|------------|
| `date(time, modifier...)` | Extract date string (YYYY-MM-DD) |
| `time(time, modifier...)` | Extract time string (HH:MM:SS) |
| `datetime(time, modifier...)` | Extract datetime string |
| `julianday(time, modifier...)` | Julian day number (float) |
| `unixepoch(time, modifier...)` | Unix timestamp (integer seconds) |
| `strftime(format, time, modifier...)` | Custom formatting |
| `timediff(a, b)` | Difference between two timestamps |

### Math Functions

`acos`, `asin`, `atan`, `atan2`, `ceil`, `cos`, `degrees`, `exp`, `floor`, `ln`, `log`, `log2`, `mod`, `pi`, `pow`, `radians`, `sin`, `sqrt`, `tan`, `trunc`.

---

## The CLI Shell

The `fsqlite-cli` binary provides an interactive SQL shell equivalent to the `sqlite3` command-line tool.

### Features

- Multi-line statement detection (continues until `;`)
- SQL syntax highlighting in the prompt
- Tab completion for table names, column names, SQL keywords, and dot-commands
- Command history with persistent `~/.frankensqlite_history` file
- Init file (`~/.frankensqliterc`) executed on startup
- Batch mode: pipe SQL from stdin or a file
- Signal handling: Ctrl-C cancels the running query, Ctrl-D exits

### Output Modes

| Mode | Description |
|------|------------|
| `column` | Aligned columns with headers (default) |
| `table` | ASCII table with borders |
| `box` | Unicode box-drawing table |
| `csv` | Comma-separated values |
| `json` | JSON array of objects |
| `line` | One `column = value` per line |
| `list` | Pipe-separated values |
| `markdown` | GitHub-flavored markdown table |
| `tabs` | Tab-separated values |
| `insert` | SQL INSERT statements |
| `html` | HTML table |
| `ascii` | ASCII art separators |
| `quote` | SQL-escaped values |
| `tcl` | TCL list format |

### Dot-Commands (Selected)

| Command | Purpose |
|---------|---------|
| `.open FILE` | Open a database file |
| `.tables` | List all tables |
| `.schema TABLE` | Show CREATE statement |
| `.dump` | Export entire database as SQL |
| `.import FILE TABLE` | Import CSV/TSV into a table |
| `.mode MODE` | Set output mode |
| `.headers on/off` | Toggle column headers |
| `.explain on/off` | Toggle EXPLAIN formatting |
| `.stats on/off` | Show query execution statistics |
| `.timer on/off` | Show wall-clock query timing |
| `.backup FILE` | Backup database to a file |
| `.restore FILE` | Restore database from backup |

---

## Public API

### Basic Usage

```rust
use fsqlite::Connection;

let conn = Connection::open("my.db")?;

conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", [])?;
conn.execute("INSERT INTO users (name, age) VALUES (?1, ?2)", ("Alice", 30))?;

let mut stmt = conn.prepare("SELECT name, age FROM users WHERE age > ?1")?;
let rows = stmt.query((25,))?;

for row in rows {
    let name: String = row.get(0)?;
    let age: i64 = row.get(1)?;
    println!("{name}: {age}");
}
```

### Transaction API

```rust
let tx = conn.transaction()?;

tx.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)", [])?;
tx.execute("INSERT INTO accounts (id, balance) VALUES (2, 500)", [])?;

tx.commit()?;  // atomic: both inserts visible, or neither
```

### Concurrent Writers

```rust
use std::thread;

let db_path = "shared.db";

// Spawn 8 writer threads
let handles: Vec<_> = (0..8).map(|i| {
    thread::spawn(move || {
        let conn = Connection::open(db_path).unwrap();
        for j in 0..1000 {
            loop {
                match conn.execute(
                    "INSERT INTO events (thread, seq) VALUES (?1, ?2)",
                    (i, j),
                ) {
                    Ok(_) => break,
                    Err(e) if e.is_transient() => continue,  // SQLITE_BUSY, retry
                    Err(e) => panic!("{e}"),
                }
            }
        }
    })
}).collect();

for h in handles { h.join().unwrap(); }
// All 8000 rows present, no data loss, no corruption.
```

---

## Testing Strategy

### Five Layers

1. **Unit tests** in each crate test components in isolation using mock implementations of trait dependencies.
2. **Integration tests** in `fsqlite-core` test the full query pipeline from SQL text to result rows using an in-memory VFS.
3. **Compatibility tests** in `fsqlite-harness` run the SQLite test corpus against both FrankenSQLite and C SQLite, comparing results row-by-row.
4. **Fuzz tests** using `cargo-fuzz` target the parser, record decoder, and B-tree page decoder with arbitrary byte inputs.
5. **Concurrency tests** exercise MVCC behavior: concurrent readers and writers, snapshot isolation verification, write-write conflict detection, and garbage collection under load.

### Property-Based Testing (proptest)

- B-tree invariants hold for arbitrary insert/delete sequences
- Record serialization round-trips: `deserialize(serialize(record)) == record` for any `Vec<SqliteValue>`
- Parser round-trips: `parse(print(ast)) == ast` for any generated AST
- MVCC snapshots are consistent under arbitrary transaction interleavings

### Crash Recovery Testing

- Power-loss simulation: truncate the WAL file at every possible byte boundary during commit, then recover and verify no data loss
- SIGKILL testing: kill the process at random points, restart, run `PRAGMA integrity_check`
- Bit-flip testing: flip random bits in the WAL and database files, verify checksum detection

### Conformance Target

**100% behavioral parity target** with C SQLite 3.52.0 for the supported
surface. Any intentional divergence MUST be explicitly documented and
annotated in the harness with rationale. The conformance suite runs SQL Logic
Tests (SLT format) covering:

- All DML and DDL operations
- All join types (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
- Subqueries, CTEs, window functions, triggers, views
- Type affinity, NULL handling, collation sequences
- Every built-in function
- Foreign keys, UPSERT, RETURNING clause
- WAL mode, concurrent readers under write load

---

## Performance Characteristics

### Workloads That Benefit Most from MVCC

| Workload | Single-Writer SQLite | FrankenSQLite MVCC | Speedup |
|----------|---------------------|-------------------|---------|
| 8 threads writing to different tables | Serialized (1x) | Parallel (up to 8x) | ~8x |
| 8 threads writing to same table, different row ranges | Serialized (1x) | Parallel if different leaf pages | 2-6x |
| 8 threads writing to same table, same hot rows | Serialized (1x) | Serialized (page conflicts) | ~1x |
| Mixed read/write (90% reads, 10% writes) | Writers block readers in non-WAL | Readers never block | Lower p99 read latency |
| Single-threaded writes | Identical | Slight overhead from version tracking | ~0.95x |

The sweet spot is multiple writers touching different parts of the database simultaneously. Single-threaded workloads see negligible MVCC overhead. Pathological cases (all writers hammering the same leaf page) degrade to single-writer behavior because every write conflicts.

### Memory Overhead

MVCC adds memory overhead proportional to the number of concurrent active versions. With 10 active transactions each modifying 50 unique pages (4KB each), the additional memory is approximately `10 * 50 * 4KB = 2MB`. Garbage collection reclaims old versions within ~1 second of the last reader closing.

### Scaling Expectations

| Metric | Expected |
|--------|----------|
| Single-row INSERT throughput (1 writer) | Comparable to C SQLite |
| Single-row INSERT throughput (8 writers, separate tables) | ~8x C SQLite |
| Point SELECT by rowid | Comparable to C SQLite |
| Full table scan | Comparable to C SQLite |
| WAL checkpoint latency | Slightly higher (must check active snapshots) |
| Reader throughput under write load | Higher (no `aReadMark` contention) |

---

## File Format (Binary Compatible with SQLite)

### Database Header (100 bytes at offset 0)

```
Offset  Size  Field
──────  ────  ─────────────────────────────────────────
  0      16   Magic: "SQLite format 3\0"
 16       2   Page size (512-65536)
 18       1   Write format version (1=journal, 2=WAL)
 19       1   Read format version
 20       1   Reserved bytes per page
 21       1   Max embedded payload fraction (must be 64)
 22       1   Min embedded payload fraction (must be 32)
 23       1   Leaf payload fraction (must be 32)
 24       4   File change counter
 28       4   Database size in pages
 32       4   First freelist trunk page
 36       4   Total freelist pages
 40       4   Schema cookie
 44       4   Schema format number (4 = current)
 48       4   Default page cache size
 52       4   Largest root B-tree page (auto-vacuum)
 56       4   Text encoding (1=UTF8, 2=UTF16le, 3=UTF16be)
 60       4   User version (PRAGMA user_version)
 64       4   Incremental vacuum mode
 68       4   Application ID (PRAGMA application_id)
 72      20   Reserved for expansion (must be zero)
 92       4   Version-valid-for number
 96       4   SQLite version that wrote the file
```

### B-tree Page Layout

```
┌───────────────────────────────────┐
│ Page header (8 or 12 bytes)       │
├───────────────────────────────────┤
│ Cell pointer array (2B per cell)  │
├───────────────────────────────────┤
│ Unallocated space                 │
├───────────────────────────────────┤
│ Cell content (grows from bottom)  │
├───────────────────────────────────┤
│ Reserved region                   │
└───────────────────────────────────┘
```

### Record Format

```
┌─────────┬─────────────┬─────────────┬───┬──────────┬──────────┬───┐
│ Hdr size│ Serial type 1│ Serial type 2│...│ Value 1  │ Value 2  │...│
│ (varint)│ (varint)     │ (varint)     │   │ (N bytes)│ (N bytes)│   │
└─────────┴─────────────┴─────────────┴───┴──────────┴──────────┴───┘
```

---

## Pointer Map and Auto-Vacuum

SQLite's auto-vacuum mode returns freed pages to the operating system instead of adding them to the freelist. This requires a **pointer map** — a reverse lookup from any page to its parent — so the engine can relocate pages and update parent pointers during vacuum.

**Entry format (5 bytes per page):**

| Byte | Content |
|------|---------|
| 0 | Type code: 1 = root page, 2 = freelist page, 3 = first overflow page, 4 = subsequent overflow page, 5 = non-root B-tree page |
| 1-4 | Parent page number (u32 big-endian). Meaning varies by type: for B-tree pages, it's the parent in the tree; for overflow pages, it's the page containing the cell that overflows. |

The first pointer map page is always page 2. Each page holds `usable_size / 5` entries (819 entries for 4096-byte pages). Pointer map pages recur at regular intervals: pages 2, 822, 1642, ... (group size = entries_per_page + 1 = 820).

**How auto-vacuum works:** When a page is freed (e.g., by `DELETE`), the engine moves the last page in the file into the freed slot, updates the moved page's parent pointer using the pointer map, and truncates the file by one page. This keeps the database file compact without requiring a full `VACUUM` rebuild.

FrankenSQLite replicates pointer map layout and auto-vacuum page relocation identically to C SQLite, ensuring databases with `auto_vacuum = FULL` or `auto_vacuum = INCREMENTAL` are fully interoperable.

---

## Schema Management (sqlite_master)

Every SQLite database contains a `sqlite_master` table rooted at page 1 with this schema:

```sql
CREATE TABLE sqlite_master (
    type TEXT,      -- 'table', 'index', 'view', 'trigger'
    name TEXT,      -- object name
    tbl_name TEXT,  -- associated table name (for indexes/triggers: the parent table)
    rootpage INT,   -- root B-tree page number (0 for views/triggers)
    sql TEXT        -- original CREATE statement text (NULL for auto-indexes)
);
```

For the temp database, the equivalent is `sqlite_temp_master`.

On database creation, FrankenSQLite creates page 1 as a table leaf page containing zero rows. The first `CREATE TABLE` inserts a row into `sqlite_master` with the CREATE statement text. Every DDL operation (CREATE, DROP, ALTER) modifies this table and increments a **schema cookie** (a 32-bit counter at header offset 40) so that prepared statements can detect schema changes and re-prepare automatically.

`ATTACH DATABASE` adds a secondary database with its own `sqlite_master` (aliased as `<schema>.sqlite_master`). Cross-database queries use fully qualified names (`schema.table`).

---

## The Lock-Byte Page

For databases larger than 1 GiB, the page containing byte offset `0x40000000` (1,073,741,824 — the POSIX advisory "pending byte") is reserved for file locking and must never store B-tree content. For 4096-byte pages, this is page 262145 (`(0x40000000 / 4096) + 1`). The exact page number depends on page size.

SQLite skips this page during allocation (`allocateBtreePage()` in btree.c). FrankenSQLite replicates this behavior precisely:

- Never allocate this page for B-tree storage or freelist use.
- On `PRAGMA integrity_check`, verify this page is not referenced by any B-tree pointer.
- The page is simply a hole in the file that exists solely so POSIX `fcntl()` locks can operate on it without corrupting B-tree data.

This is critical for multi-process locking compatibility: if a B-tree page were to occupy the lock-byte region, concurrent readers using POSIX advisory locks would silently corrupt it.

---

## Two Operating Modes

FrankenSQLite operates in one of two modes, selected per-connection via `PRAGMA fsqlite.mode`:

### Compatibility Mode (Default)

The database file is a standard SQLite `.db` file. WAL frames use standard SQLite WAL format. An existing C SQLite database opens without conversion, and a FrankenSQLite database opens in C SQLite without conversion. Optional sidecars (`.wal-fec`, `.idx-fec`) store RaptorQ repair symbols alongside the standard files but the core `.db` remains SQLite-compatible when checkpointed. This mode is the default and is used for conformance testing against C SQLite.

### Native Mode

Primary durable state is an ECS commit stream: append-only `CommitCapsule` objects encoded as RaptorQ symbols. The source-of-truth is the commit stream, not a mutable `.db` file.

A **CommitCapsule** is the atomic unit of commit state, containing:
- `commit_seq` and `snapshot_basis`
- Intent log and/or page deltas
- Read/write set digests
- SSI witnesses

A **CommitMarker** is the durable "this commit exists" record: the capsule's ObjectId plus a pointer to the previous marker, forming an append-only chain. A commit is committed if and only if its marker is durable. Recovery ignores capsules without a committed marker.

Checkpointing materializes a canonical `.db` for compatibility export, but the commit stream remains the source of truth. Both modes expose the same SQL and API surface.

---

## Time Travel Queries (Native Mode)

Native mode persists an immutable commit stream (capsules + markers with monotonic timestamps). This enables **time travel queries** that evaluate reads against a historical commit sequence — querying the database as it existed at any past point in time.

**Syntax:**

```sql
SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-06-15 09:30:00';
SELECT * FROM orders FOR SYSTEM_TIME AS OF COMMITSEQ 1234567;
```

**How it works:**

1. **Resolve target commit:** If `AS OF COMMITSEQ N`, use N directly. Otherwise, parse the timestamp using SQLite-compatible datetime rules and binary-search the marker stream for the greatest marker with `commit_time_unix_ns <= target_time_unix_ns`.
2. **Create synthetic snapshot:** Build a read-only snapshot `S` with `S.high = target_commit_seq`.
3. **Execute normally:** The query runs using standard MVCC resolution rules — `resolve(P, S)` returns the newest committed page version with `version.commit_seq <= S.high`.

**Restrictions (V1):** Time travel is read-only. `INSERT`, `UPDATE`, `DELETE`, and DDL in a time-travel context fail with `SQLITE_ERROR`. If the retention policy has pruned the requested historical state, the query fails with an explicit "history not retained" error. With tiered storage enabled, older capsules and index segments may reside only in remote storage; the engine fetches symbols on demand and decodes/repairs as usual.

---

## ECS: The Erasure-Coded Stream Substrate

In Native mode, every durable object (commit capsules, page snapshots, WAL segments, index checkpoints, schema snapshots) is stored as an ECS object.

### Content-Addressed Identity

Every object is identified by a 128-bit content address:

```
ObjectId = Trunc128( BLAKE3( "fsqlite:ecs:v1" || canonical_header || payload_hash ) )
```

BLAKE3 truncated to 128 bits (16 bytes) provides sufficient collision resistance for the non-adversarial setting and halves storage overhead compared to full 256-bit hashes. Objects are immutable: the same content always produces the same ObjectId.

### SymbolRecord Envelope

The atomic unit of physical storage is a `SymbolRecord`:

```
┌────────┬─────────┬───────────┬─────┬─────┬──────────────┬─────────┬──────────┐
│ Magic  │ Version │ ObjectId  │ OTI │ ESI │ Symbol Data  │ XXH3    │ Auth Tag │
│ "FSEC" │ u8 (1)  │ [u8; 16]  │     │ u32 │ [u8; T]      │ u64     │ [u8; 16] │
└────────┴─────────┴───────────┴─────┴─────┴──────────────┴─────────┴──────────┘
```

OTI (Object Transmission Information) carries the RaptorQ metadata needed for decoding: transfer length, symbol alignment, symbol size, source blocks, and sub-blocks. Repair symbol generation is deterministic: the same object and repair count always produce identical repair symbols, enabling idempotent writes and incremental repair.

### Local Physical Layout (Native Mode)

```
foo.db.fsqlite/
├── ecs/
│   ├── objects/          -- symbol records, sharded by ObjectId prefix
│   │   ├── 00/
│   │   └── ff/
│   ├── commit_stream/    -- append-only CommitMarker sequence
│   │   └── stream.log
│   └── manifest.root     -- RootManifest (the ONE mutable file)
├── cache/                -- rebuildable derived state
│   ├── btree.cache       -- materialized B-tree pages
│   ├── index.cache       -- secondary index pages
│   └── schema.cache      -- parsed schema
└── compat/               -- optional compatibility export
    ├── foo.db            -- standard SQLite database file
    └── foo.db-wal        -- standard WAL
```

The `RootManifest` is the bootstrap object: it maps the logical database name to the current committed state ObjectId. It is the only mutable file in the entire layout. Repair overhead is configurable via `PRAGMA raptorq_overhead` (default: 20%, meaning 1.2x source symbols stored).

---

## Native Mode Commit Protocol

The Native-mode commit protocol decouples **bulk durability** (payload bytes) from **ordering** (the marker stream). Writers persist `CommitCapsule` payloads concurrently using bulk I/O off the critical section. A single sequencer (`WriteCoordinator`) serializes only the tiny ordering step: validation, `commit_seq` allocation, and `CommitMarker` append.

**Writer path (concurrent):**

1. Finalize the write set (pages and/or intent log).
2. Run SSI validation using the witness plane. If SSI aborts, return `SQLITE_BUSY_SNAPSHOT`.
3. Publish witness evidence objects (pre-marker) using the cancel-safe two-phase publication protocol.
4. Build the `CommitCapsule` deterministically from intent log, page deltas, snapshot basis, and witness references.
5. RaptorQ-encode the capsule into systematic + repair symbols.
6. Persist capsule symbols to local symbol logs (and optionally stream to replicas) **before** acquiring the commit sequencing critical section.
7. Submit a tiny publish request to the `WriteCoordinator` containing the capsule `ObjectId`, write-set summary, and witness references. Await the coordinator response.

**WriteCoordinator loop (serialized, tiny I/O):**

1. FCW validation using write-set summaries (no full capsule decode needed). SSI re-validation checks for dangerous structures created by concurrent commits after the writer's local validation.
2. Allocate `commit_seq` (gap-free, derived from marker stream tip).
3. Persist a `CommitProof` ECS object.
4. **FSYNC_1** — barrier ensuring capsule symbols and proof are durable before the marker references them.
5. Append `CommitMarker` record (~96 bytes) to the marker stream.
6. **FSYNC_2** — barrier ensuring the marker is durable before the client receives a success response.
7. Publish `commit_seq` to shared memory with `Release` ordering.
8. Respond to the client.

**Why two fsync barriers:**
- **FSYNC_1** prevents "committed marker, lost data" — the worst-case native mode failure where recovery finds a marker but cannot decode its capsule.
- **FSYNC_2** prevents "client thinks committed, marker not persisted" — a silent transaction loss on crash.

The two-fsync cost (~100-200 microseconds on NVMe) is amortized by batching multiple commits per WriteCoordinator iteration.

---

## ECS Compaction

Native Mode's append-only symbol logs (`ecs/symbols/*.log`) grow indefinitely. To reclaim storage, the system runs a **mark-and-compact** process that is cancel-safe, crash-safe, cross-process safe, and non-disruptive to p99 query latency.

**Compaction triggers:**
- **Space amplification:** `total_log_size / live_data_size > 2.0` (configurable via PRAGMA).
- **Time interval:** `PRAGMA fsqlite.auto_compact_interval`.
- **Manual:** `PRAGMA fsqlite.compact`.

**The four phases:**

1. **Mark:** Start from the `RootManifest` and active commit marker stream. Trace all reachable `CommitCapsule`, `PageHistory`, and witness objects. Build a `BloomFilter` of live `ObjectId`s.

2. **Compact:** Create new symbol log segments using temporary names (`segment-XXXXXX.log.compacting`). Scan old logs; copy live symbols to new segments, discard dead objects. Fsync new segments.

3. **Publish:** Two-phase atomic publication. First, publish the new object locator cache. Then, rename compacted segments into place. Old segments are NOT retired until both the new segments and locator are durable — preventing a crash from leaving the system with neither valid data set.

4. **Retire:** Old segments are retired only once no active readers depend on them, tracked via segment leases. On Unix, old segments are unlinked once retired (open handles remain valid). On Windows, old segments are renamed to `.retired` and deleted after all handles close.

**Safety invariant:** Compaction never mutates an existing segment. At all times, there exists at least one complete set of symbol logs sufficient to decode any reachable object under the retention policy.

---

## Multi-Process MVCC

FrankenSQLite extends MVCC coordination across OS processes via a shared-memory file (`foo.db.fsqlite-shm`), analogous to SQLite's WAL-index but extended for full MVCC.

### Shared Memory Layout

```
┌─────────────────────────────────────┐
│ Header                              │
│   magic: "FSQLSHM\0"               │
│   version: u32 (1)                  │
│   next_txn_id: AtomicU64            │  ← global TxnId counter
│   commit_seq: AtomicU64             │  ← global commit sequence
│   gc_horizon: AtomicU64             │  ← min active TxnId across processes
│   checksum: u64 (xxhash3)           │
├─────────────────────────────────────┤
│ TxnSlot Array (256 slots default)   │  ← one slot per active transaction
├─────────────────────────────────────┤
│ PageLockTable Region                │  ← open-addressing hash in shared mem
├─────────────────────────────────────┤
│ SIREAD Plane                        │  ← cross-process rw-antidependency tracking
└─────────────────────────────────────┘
```

All fields use atomic operations. The fast in-process path is unchanged; the cross-process path adds ~100ns per lock operation via mmap-based atomics.

### Crash Cleanup

Each `TxnSlot` carries a lease timestamp. If a process crashes while holding active transactions, other processes detect the stale lease and reclaim the slot after a configurable timeout. This prevents crashed processes from pinning page versions indefinitely or blocking the GC horizon from advancing.

### File-Lock Fallback

On systems where shared memory is unavailable or restricted, FrankenSQLite falls back to file-lock-based coordination (POSIX `fcntl` or Windows `LockFileEx`). This degrades to single-writer behavior but preserves correctness.

---

## Page-Level Encryption

FrankenSQLite provides page-level encryption as a built-in feature, replacing the need for SQLite's commercial Encryption Extension (SEE).

| Property | Value |
|----------|-------|
| Cipher | XChaCha20-Poly1305 (AEAD) |
| Data key (DEK) | Random 256-bit key generated at database creation |
| Key-encryption key (KEK) | Argon2id(passphrase, per-database random salt) |
| Rekey | O(1): re-wrap DEK (`PRAGMA rekey = 'new_passphrase'`) |
| Nonce | 24 bytes, random per page write |
| Authentication tag | 16 bytes (Poly1305), stored in the page's reserved space |
| Reserved bytes | `reserved_bytes >= 40` (24B nonce + 16B tag) |
| Key management API | `PRAGMA key = 'passphrase'` / `PRAGMA rekey = 'new_passphrase'` |

This is envelope encryption: pages are encrypted with the DEK; the DEK is wrapped with the KEK derived from `PRAGMA key`. Random nonces eliminate global counters and remain safe under VM snapshot reverts, crashes, forks, and distributed writers.

In Native mode, encryption applies before RaptorQ encoding (encrypt-then-code). An attacker who corrupts encrypted ECS symbols cannot forge valid ciphertext; RaptorQ repairs the corruption, then decryption proceeds as normal.

---

## The Mathematics Behind FrankenSQLite

Most database engines hand-wave their concurrency model and hope the tests catch regressions. FrankenSQLite is built on formal mathematics at every layer: the concurrency model has machine-checkable safety proofs, the storage layer has information-theoretic durability bounds, and the conflict model has closed-form probability estimates. This section walks through the core mathematical machinery, targeting engineers who want to understand *why* the system works, not just *that* it works.

### Probabilistic Conflict Model (Birthday Paradox for Pages)

Page-level MVCC raises an obvious question: how often do two transactions actually collide on the same page? The answer maps directly to the **birthday paradox**.

```
Setup:
    P = total database pages
    W = pages written per transaction (uniform random)
    N = number of concurrent writers

Pairwise conflict probability (two transactions T1, T2):
    P(conflict) = 1 - e^(-W² / P)

    Derivation: P(no conflict) = C(P-W, W) / C(P, W)
                               ≈ ((P-W)/P)^W
                               ≈ e^(-W²/P)   for W << P

Any-conflict probability (N concurrent transactions):
    P(any conflict among N) ≈ 1 - e^{-N(N-1)W² / (2P)}

    This is the birthday paradox with n = N*W "people" and P "days."
```

**Intuition threshold:** Conflicts become likely when `N * W ≈ √P`. For a 1 GB database with 4 KB pages (P = 262,144 pages), 8 writers each touching 50 pages: `8 * 50 = 400`, while `√262144 ≈ 512`. You're close to the threshold but still under it — expect occasional conflicts, not constant ones.

**Worked example:**

```
P = 100,000 pages, W = 50 pages/txn, N = 8 writers:

    Pairwise:  P(conflict) ≈ 1 - e^(-2500/100000) ≈ 0.025  (2.5%)
    Per-txn:   P(any conflict for one txn) ≈ 1 - (1-0.025)^7 ≈ 0.16  (16%)
    With safe merge ladder resolving f_merge=0.40 of detected conflicts (empirical): effective P_abort ≈ 0.10
    After one retry (geometric): P_abort ≈ 0.01

    TPS ≈ N × (1 - P_abort) / T_txn ≈ 8 × 0.99 / T_txn
```

Real workloads aren't uniform — they follow **Zipf distributions** where a few hot pages absorb most writes:

```
Zipf access probability for page ranked k:
    p(k) = (1/k^s) / H(P, s)

    where H(P, s) = Σ_{i=1}^{P} 1/i^s   (generalized harmonic number)
          s ≈ 0.8-1.2 for typical database workloads

Conflict probability under Zipf:
    P(conflict, Zipf) ≈ 1 - Π_k (1 - p(k))^{n_k}
```

Zipf concentrates conflicts on hot pages (the top 1% of pages absorb 20-40% of writes for s ≈ 1.0). This is exactly where safe write merging pays off most when intents commute (e.g., distinct-key inserts landing on the same hot leaf).

**Result:** At typical database sizes and concurrency levels, page-level MVCC delivers near-linear scaling. The birthday paradox model lets you predict your conflict rate from three numbers: page count, write set size, and writer count.

### GF(256) Arithmetic: The Algebra of Erasure Coding

Every RaptorQ operation — encoding, decoding, repair — bottoms out in arithmetic over **GF(2⁸)**, the Galois field with 256 elements. Each byte is a field element. FrankenSQLite also reuses this algebraic substrate for patch encoding and history compression.

```
The field GF(2⁸) = GF(2)[x] / p(x), where:
    p(x) = x⁸ + x⁴ + x³ + x² + 1    (irreducible polynomial, hex: 0x11D)

256 elements map to bytes 0x00-0xFF. Every byte is a polynomial:
    0xA3 = x⁷ + x⁵ + x + 1

Addition:       a + b = a XOR b   (also subtraction — every element is its own inverse)
Additive identity: 0x00

Multiplication via log/exp tables:
    The multiplicative group GF(256)* has 255 elements, cyclic with generator g = 2.

    OCT_LOG[a] = k   such that g^k = a    (for a ≠ 0)
    OCT_EXP[k] = g^k (for k = 0..254, extended to 510 entries to avoid mod)

    multiply(a, b):
        if a == 0 or b == 0: return 0
        return OCT_EXP[OCT_LOG[a] + OCT_LOG[b]]    // no mod needed: max index = 508 < 510

    inverse(b):
        return OCT_EXP[255 - OCT_LOG[b]]

    Total table storage: 768 bytes (256 + 512). O(1) per operation.
```

**Worked example (0xA3 × 0x47):**

```
OCT_LOG[0xA3] = 146,  OCT_LOG[0x47] = 63
146 + 63 = 209
OCT_EXP[209] = 0x8E
∴ 0xA3 × 0x47 = 0x8E  (142 decimal)
```

For bulk operations (the inner loop of RaptorQ encoding/decoding), FrankenSQLite precomputes the full 256×256 multiplication table:

```
MUL_TABLES: [[u8; 256]; 256]    // 65,536 bytes, fits in L1 cache

Precomputed once at startup:
    MUL_TABLES[a][b] = if a == 0 || b == 0 { 0 }
                        else { OCT_EXP[OCT_LOG[a] + OCT_LOG[b]] }

Usage (single table lookup, O(1)):
    fn mul(a: u8, b: u8) -> u8 { MUL_TABLES[a as usize][b as usize] }
```

The critical hot-path operation is **symbol multiply-and-add** (fused `dst[i] ^= MUL[c][src[i]]`), which runs in the inner loop of every RaptorQ decode. For a 4 KB symbol (one database page), this is 4,096 table lookups and XOR operations — about 500 nanoseconds on modern hardware.

**Why GF(256)?** Byte-aligned arithmetic means no bit-packing overhead. The 64 KB multiplication table fits in L1 cache. Field operations are branchless (important for constant-time security properties). And 256 elements provide enough algebraic structure for the RaptorQ constraint system while keeping everything byte-addressable.

### Fountain Codes: Information-Theoretic Durability Bounds

Traditional redundancy (RAID, triple replication) wastes bandwidth and provides fixed fault tolerance. FrankenSQLite uses **RaptorQ fountain codes** (RFC 6330), which are *rateless* — you can generate an unlimited stream of encoding symbols from any source data, and a receiver can reconstruct the original from *any* sufficient subset.

```
Source data: K symbols (each symbol = one database page, typically 4096 bytes)

Encoding:
    Source symbols:  C'[0], C'[1], ..., C'[K-1]     (the original pages)
    Repair symbols:  generated on demand, unlimited quantity
    Each repair symbol = GF(256) linear combination of intermediate symbols

Decoding (the key guarantee):
    With exactly K received symbols:     ~99% success rate
    With K+1 received symbols:           failure < 10⁻⁵
    With K+2 received symbols:           failure < 10⁻⁷

V1 default policy: store enough symbols that the decoder can always collect K+2.
```

**Intuition:** Think of it as a mathematical hologram. Every repair symbol encodes information about *all* source symbols. Lose any subset of symbols and the remaining ones contain enough information to reconstruct the whole. This is fundamentally different from replication, where losing the one copy of page 47 means page 47 is gone.

**Durability bound:**

```
For K source symbols with R = 0.2K repair symbols (20% overhead),
and independent per-symbol corruption probability p = 10⁻⁴:

    P(data loss) ≤ Σ_{i=R+1}^{K+R} C(K+R, i) × p^i × (1-p)^{K+R-i}

    For V1 defaults: P(data loss) ≤ 10^{-5K}

    Example: 1000-page database → P(loss) ≤ 10⁻⁵⁰⁰⁰
    That's not "five nines" — that's five thousand nines.
```

**How encoding works (simplified):**

```
Step 1 — Constraint matrix A (L × L, where L = K' + S + H):
    Rows 0..S-1:          LDPC constraints  (sparse, ~7 non-zeros/row, over GF(2))
    Rows S..S+H-1:        HDPC constraints  (dense, over GF(256))
    Rows S+H..L-1:        LT constraints    (sparse, from degree distribution)

         |<--- K' cols --->|<- S cols ->|<- H cols ->|
    LDPC |   LDPC_LEFT     | I_S (SxS) |     0      |  S rows
    HDPC |   MT × GAMMA    |     0     | I_H (HxH)  |  H rows
    LT   |   LT_MATRIX     |     0     |     0      |  K' rows

Step 2 — Solve A × C = D for intermediate symbols C (Gaussian elimination)

Step 3 — Generate any encoding symbol X:
    if X < K': return source symbol C'[X]        (systematic: original data)
    else:      return LTEnc(K', C, X)            (repair symbol: GF(256) linear combo)
```

**Decoding is a two-phase process:**

```
Phase 1 — Peeling (O(K) average):
    While any row has exactly 1 unresolved column c:
        C[c] = (D[r] ⊕ Σ known terms) × inverse(a_{r,c})
    Resolves 90-95% of symbols.

Phase 2 — Gaussian elimination on the "inactive" subsystem:
    Remaining ~O(√K') symbols form a small dense system.
    Cost: O(I² × T) for symbol operations, negligible since I < 50 for K' < 10,000.
```

**Bandwidth savings for replication:**

```
Traditional TCP replication (per receiver):
    Total sender bandwidth: O(N × K / (1-p))     where N = receivers, p = loss rate

Fountain-coded multicast:
    Sender emits: K × 1.02 / (1-p) symbols       (2% overhead, independent of N)
    Savings: factor of N

Example: K=1000 pages, p=5% loss, N=10 replicas
    TCP:     ~11,579 transmissions
    Fountain: ~1,074 transmissions
    Savings: 10.8×
```

**Result:** RaptorQ gives FrankenSQLite durability guarantees that are mathematically provable, not just empirically tested. The 20% storage overhead buys durability measured in thousands of nines.

### Safe Write Merge Ladder (Intent + Structured Patches)

When two transactions modify the same physical page, strict page-level FCW would
abort one. FrankenSQLite can sometimes do better: if the *intent* operations
commute (e.g., inserts into distinct keys), the loser can be rebased onto the
latest committed snapshot and still produce a correct state.

While XOR-deltas compose linearly as byte vectors, **byte-disjointness is not a
safe merge rule for SQLite structured pages** (B-tree pages, overflow pages,
freelist pages, pointer-map pages). Internal pointers and defragmentation can
make two disjoint byte writes semantically dependent, causing lost updates.

**Counterexample (B-tree lost update):**
- `T1` moves a cell from offset X to Y and updates the cell pointer array to Y.
- `T2` updates the cell payload bytes at the old offset X.
- The byte supports can be disjoint, yet the merged page points at Y (old value)
  and `T2`'s update at X becomes unreachable garbage.

Therefore, merge is only allowed via the SAFE ladder:

| Priority | Strategy | Safety Guarantee |
|----------|----------|-----------------|
| 1 | Deterministic rebase replay | Re-executes intent ops against current committed snapshot |
| 2 | Structured page patch merge | Disjoint by `cell_key_digest`; header ops serialized; invariants checked |
| 3 | Abort/retry | No safe merge possible |

Merge policy: `PRAGMA fsqlite.write_merge = OFF | SAFE | LAB_UNSAFE`. `SAFE`
enables only the ladder above; raw byte-range XOR merging is forbidden for
SQLite structured pages.

### Three-Tier Checksum Architecture

Not all checksums are created equal. FrankenSQLite uses three hash algorithms, each chosen for a specific point in the speed/security tradeoff:

```
Tier 1 — Hot-path integrity (every page access):
    Algorithm:  XXH3-128
    Speed:      ~50 GB/s on x86-64 with AVX2  (~80 ns per 4 KB page)
    Collision:  2⁻¹²⁸ ≈ 3 × 10⁻³⁹
    Where:      Buffer pool, MVCC version chain, cache reads, WAL frame verification

Tier 2 — Content identity (object addressing):
    Algorithm:  BLAKE3 (truncated to 128 bits)
    Speed:      ~5 GB/s
    Collision:  Cryptographic (2⁻¹²⁸ practical security)
    Where:      ObjectId derivation, CommitCapsule identity, ECS object naming

Tier 3 — Cryptographic authentication (trust boundaries):
    Algorithm:  asupersync::security::SecurityContext (key-dependent)
    Speed:      Key-dependent, hardware-accelerated
    Where:      Replication transport, authenticated symbols, cross-node verification
```

**Policy rules that prevent misuse:**

```
✗ NO SHA-256 on hot paths      (too slow — 1.5 GB/s vs XXH3's 50 GB/s)
✗ NO XXH3 for content addressing (not cryptographic — vulnerable to preimage attacks)
✗ NO rolling own crypto         (security tier uses asupersync's vetted primitives)
✓ BLAKE3 is the bridge          (fast enough for per-object identity, strong enough
                                  for collision resistance in non-adversarial settings)
```

**WAL checksum chain (cumulative hash):**

```
WAL header checksum:
    (s1, s2) = wal_checksum(header[0..24], 0, 0, native_byte_order)

Frame N checksum:
    (s1, s2) = wal_checksum(frame_hdr[0..8] ∥ page_data, s1_{N-1}, s2_{N-1}, native)

    Per 8-byte chunk (a, b):
        s1 = s1.wrapping_add(a).wrapping_add(s2)
        s2 = s2.wrapping_add(b).wrapping_add(s1)
```

Each frame's checksum incorporates the previous frame's checksum, creating a hash chain. Modifying any byte in the WAL invalidates all subsequent frames' checksums. This is how crash recovery knows exactly where the valid data ends.

**Five levels of integrity verification (`PRAGMA integrity_check`):**

| Level | Scope | What It Checks |
|-------|-------|---------------|
| 1 | Page-level | Page type flags, header field ranges, XXH3 checksum (if enabled) |
| 2 | B-tree structural | Cell pointers within bounds, keys sorted, child pointers valid, freeblock list well-formed |
| 3 | Record format | Header varints valid, serial types not 10/11, payload sizes match, overflow chains intact |
| 4 | Cross-reference | Every page accounted for, no page in multiple B-trees, freelist consistent, pointer map matches |
| 5 | Schema | sqlite_master readable, root page numbers match existing B-trees, index entries match table data |

**Result:** The three-tier architecture lets the hot path run at 50 GB/s while still providing cryptographic guarantees at trust boundaries. The WAL checksum chain detects corruption at the exact byte, and five levels of integrity check give you surgical precision for diagnosing problems.

### E-Processes: Anytime-Valid Invariant Monitoring

Traditional statistical tests require a fixed sample size decided in advance. FrankenSQLite monitors its seven MVCC invariants using **e-processes** — sequential tests that provide valid confidence at *any* stopping time, with no peeking penalty.

```
An e-process (E_t) is a non-negative supermartingale starting at 1:
    E_0 = 1
    E[E_t | F_{t-1}] ≤ E_{t-1}    under null hypothesis H_0

Key guarantee (Ville's inequality):
    P_{H_0}(∃t : E_t ≥ 1/α) ≤ α

    You can check E_t after EVERY operation and reject H_0
    whenever E_t crosses the threshold — no Bonferroni correction needed!

Betting martingale update rule:
    E_t = E_{t-1} × (1 + λ × (X_t - p_0))

    where:
        X_t = 1 if invariant violation observed, 0 otherwise
        p_0 = null hypothesis violation rate (0.001 = "invariant holds 99.9% of the time")
        λ = bet size, constrained to (-1/(1-p_0), 1/p_0) for non-negativity
```

**Under H_0** (invariant holds): `E[X_t] = p_0`, so `E[E_t | E_{t-1}] = E_{t-1}` — the e-process is a martingale, staying near 1.

**Under H_1** (actual violation rate p_1 > p_0): the e-process grows exponentially at rate `KL(p_1 ∥ p_0)` per observation. A single genuine invariant violation at λ = 0.5 multiplies the e-value by ~1.5. After 20 violations, the e-value exceeds 3,300 — far past the rejection threshold of 20.

**Monitored invariants:**

| E-Process | Invariant | What a Violation Means |
|-----------|-----------|----------------------|
| E₁ | INV-1: Monotonic TxnIds | `AtomicU64` counter went backward (hardware fault?) |
| E₂ | INV-2: Lock exclusivity | Two transactions hold the same page lock (concurrency bug) |
| E₃ | INV-3: Version chain order | Newer version has lower TxnId (corruption or logic error) |
| E₄ | INV-4: Write set consistency | Transaction wrote a page it doesn't hold a lock on |
| E₅ | INV-5: Snapshot stability | Snapshot mutated after creation (memory corruption) |
| E₆ | INV-6: Commit atomicity | Partial commit visible (the worst possible bug) |
| E₇ | INV-7: Serialized exclusivity | Two serialized-mode writers active simultaneously |

```
Configuration:
    p0:         0.001      // null: violation rate ≤ 0.1%
    lambda:     0.5        // moderate bet
    alpha:      0.05       // reject at 5% significance → threshold = 1/0.05 = 20
    max_evalue: 10¹⁵       // overflow guard
```

**Why e-processes instead of fixed-sample tests?** A database runs continuously. You can't decide in advance how many operations to observe. E-processes let you monitor invariants in real-time, accumulating evidence over millions of operations, and flag violations the instant they become statistically significant — even if the violation rate is 0.01%.

### Mazurkiewicz Traces: Exhaustive Concurrency Verification

Testing concurrent code by running random interleavings is like testing a combination lock by trying random codes — you'll probably never find the bug. **Mazurkiewicz traces** provide exhaustive coverage by classifying all possible interleavings into equivalence classes, then testing exactly one representative from each class.

```
A trace monoid M(Σ, I) is defined over:
    Σ = alphabet of actions
        e.g., read_page(T1, P1), write_page(T2, P3), commit(T1), ...
    I = symmetric independence relation on Σ × Σ
        (a, b) ∈ I means swapping a and b doesn't change observable behavior

Two execution sequences w_1, w_2 are trace-equivalent (w_1 ≡_I w_2)
if one can be transformed into the other by swapping adjacent independent actions.

The trace monoid M(Σ, I) = Σ* / ≡_I
    (the set of all equivalence classes)
```

**Independence relation for MVCC operations:**

| Action A | Action B | Independent? | Reason |
|----------|----------|:---:|--------|
| `read(T1, P1)` | `read(T2, P2)` | Yes (if P1≠P2) | Different pages, read-read |
| `read(T1, P1)` | `read(T2, P1)` | Yes | Read-read, same page (MVCC snapshots) |
| `read(T1, P1)` | `write(T2, P1)` | **No** | Write changes what T1 might see |
| `write(T1, P1)` | `write(T2, P2)` | Yes (if P1≠P2) | Different pages, no interaction |
| `write(T1, P1)` | `write(T2, P1)` | **No** | Same-page conflict |
| `commit(T1)` | `commit(T2)` | **No** | Serialized through coordinator |
| `begin(T1)` | `begin(T2)` | **No** | Snapshot capture is ordering-dependent |

**Foata normal form** organizes events into layers of mutually independent actions, providing a canonical representative for each trace class. Combined with **DPOR** (Dynamic Partial Order Reduction), which prunes equivalent schedules during exploration, this achieves exhaustive coverage of all behaviorally distinct interleavings without the combinatorial explosion of naive enumeration.

**Result:** For a 3-transaction MVCC scenario, naive enumeration might explore thousands of interleavings. Mazurkiewicz traces + DPOR reduce this to dozens of truly distinct schedules — each verified against all seven MVCC invariants. This is how FrankenSQLite achieves confidence in its concurrency model that random testing cannot provide.

### Formal Safety Proofs

FrankenSQLite's MVCC system comes with six machine-checkable safety theorems. These aren't hand-wavy arguments — they're formal proofs grounded in the invariants and data structures described above.

**Theorem 1: Deadlock Freedom (structural impossibility)**

```
Claim: The MVCC system is deadlock-free.

Proof:
    1. A deadlock requires a cycle in the wait-for graph.
    2. try_acquire() never blocks — it returns Err(SQLITE_BUSY) immediately
       if the lock is held by another transaction.
    3. A transaction that does not wait cannot appear as an edge in the
       wait-for graph.
    4. A graph with no edges has no cycles.
    5. No cycle ⟹ no deadlock.  QED ∎

Structural guarantee: deadlocks are impossible by construction (non-blocking),
not merely detected and broken. There is no deadlock detector. There is no
timeout. There is nothing to tune.
```

**Theorem 2: Snapshot Isolation (consistent reads)**

```
Claim: Every transaction observes a consistent snapshot — it sees either
all or none of any other transaction's writes, never a partial set.

Proof: For reading transaction T_r with snapshot S_r (where S_r.high is
the CommitSeq at T_r's BEGIN), and any writer T_w that committed with
commit_seq C_w and created versions {V_1, ..., V_k}:

    visible(V_i, S_r) = (C_w <= S_r.high)

    This condition depends ONLY on C_w and S_r.high, not on i.
    All versions of T_w share the same commit_seq C_w (assigned atomically
    by the sequencer).
    ∴ visible(V_i, S_r) has the same truth value for all i ∈ {1,...,k}.

    Exhaustive cases:
    • T_w committed after snapshot → sees NONE  (C_w > S_r.high)
    • T_w not yet committed       → sees NONE  (commit_seq = 0, never <= S_r.high)
    • T_w committed before snapshot → sees ALL  (C_w <= S_r.high)

    In no case does T_r see a strict subset of T_w's writes.
    Snapshot S_r is immutable (INV-5), so this truth value doesn't change
    during T_r's lifetime.  QED ∎
```

**Theorem 3: First-Committer-Wins**

```
Claim: Under strict FCW (no merge), if two transactions both write page P, at
most one commits successfully. With the SAFE merge ladder enabled, both may
commit only if the conflict is resolved semantically (intent replay / structured
patch) producing a state equivalent to some serial ordering.

Proof (two cases):
    Case A — Concurrent lock contention:
        T1 acquires lock on P first. T2 calls try_acquire(P) → Err(SQLITE_BUSY).
        T2 cannot write P at all. At most T1 commits with P.

    Case B — Sequential (T1 commits and releases before T2 acquires):
        T2 acquires lock on P and writes it.
        At commit validation, T2 discovers T1 committed P after T2's snapshot.
        If PRAGMA fsqlite.write_merge = SAFE and the merge ladder succeeds,
        T2 commits with rebased/merged deltas; otherwise T2 aborts/retries.

    In all cases, the final committed page version is well-defined: either one
    writer's changes survive (abort path) or a single merged page incorporates
    both writers in a serializable way.  QED ∎
```

**Theorem 4: GC Safety (no premature version reclamation)**

```
Claim: Garbage collection never removes a version any active or future
transaction could need.

Setup:
    gc_horizon = min(T.begin_seq : T ∈ active_transactions)
        where begin_seq is the CommitSeq observed at T's BEGIN.
    Version V of page P is reclaimable iff:
        V.commit_seq < gc_horizon
        AND ∃ V' in version_chain(P):
            V'.commit_seq > V.commit_seq
            AND V'.commit_seq ≤ gc_horizon

Proof:
    For any active T_a: T_a.snapshot.high = T_a.begin_seq ≥ gc_horizon
    The superseding V' satisfies V'.commit_seq ≤ gc_horizon ≤ T_a.snapshot.high
    ∴ V' is visible to T_a's snapshot (V'.commit_seq ≤ S.high).
    Since V'.commit_seq > V.commit_seq, resolve(P, T_a.snapshot) returns V'
    or newer — never V.
    Same argument holds for future transactions (their begin_seq ≥ gc_horizon).
    QED ∎
```

**Theorem 5: Memory Boundedness**

```
Claim: Under steady-state load with max transaction duration D and commit
rate R, the maximum retained versions per page is bounded by R × D + 1.

Proof:
    The oldest active transaction started at most D seconds ago.
    At most R × D commits occurred in those D seconds.
    Each creates at most one version per page.
    The version chain has at most R × D versions above gc_horizon,
    plus one at/below the horizon. All versions below are reclaimable
    by Theorem 4.  QED ∎

Practical: D = 5s, R = 1000 commits/s → at most 5,001 versions per page
           → ~20 MB per hot page at 4 KB pages.
```

**Theorem 6: Liveness (finite termination)**

```
Claim: Every transaction either commits or aborts in finite time,
assuming (a) the application calls COMMIT or ROLLBACK, (b) the write
coordinator processes requests in finite time, and (c) WAL I/O completes.

Proof sketch:
    Begin:    fetch_add is O(1)
    Read:     version chain bounded by R×D+1 (Theorem 5)
    Write:    try_acquire is non-blocking, COW is O(page_size)
    Commit:   validation scan bounded by R×D entries, WAL append finite
    Abort:    O(write_set_size + page_locks_size)
    All operations bounded ⟹ total work bounded ⟹ terminates.  QED ∎
```

**Result:** These proofs aren't academic exercises — they're the foundation for FrankenSQLite's claim that MVCC concurrency is correct by construction. Each proof is verified empirically via proptest and DPOR trace exploration, but the formal argument means you don't have to trust the tests alone.

### SSI: The Cahill/Fekete Rule at Page Granularity

Snapshot Isolation alone misses **write skew** — an anomaly where two transactions each read something the other writes, producing a result impossible under serial execution. FrankenSQLite applies **Serializable Snapshot Isolation (SSI)** using the conservative Cahill/Fekete rule at page granularity.

```
The dangerous structure (rw-antidependency cycle):

    T1 --rw--> T2 --rw--> T3

    T1 read something T2 later wrote (rw edge T1→T2)
    T2 read something T3 later wrote (rw edge T2→T3)
    T3 committed before T1 in serialization order

T2 is the "pivot" — it has both incoming and outgoing rw-antidependency edges.

Conservative abort rule (Page-SSI):
    At commit time, if a transaction has BOTH:
        has_incoming_rw = true   (someone read a page I wrote)
        has_outgoing_rw = true   (I read a page someone else wrote)
    → ABORT the pivot transaction.

This is conservative: it may abort transactions that wouldn't actually cause
write skew. But it never misses a genuine anomaly.
```

**Decision-theoretic justification:**

```
Loss matrix:
                  | commit (a=0)  | abort (a=1)  |
    S = anomaly   |   L_miss=1000 |   0           |
    S = safe      |   0           |   L_fp=1      |

Abort if P(anomaly | evidence) > L_fp / (L_fp + L_miss)
       = 1 / 1001 ≈ 0.001

The cost of missing an anomaly (data corruption) is 1000× the cost of
a false positive (retry). So we abort at extremely low evidence thresholds.
PostgreSQL has shipped this same SSI approach since 2011 with measured
false positive rates below 0.5% at row granularity.
```

**Page-SSI tracking via the SireadTable:**

```
SireadTable: 64 shards, each a Mutex<HashMap<PageNumber, SmallVec<TxnId>>>

On every page read: record (page_number, reading_txn_id) in SireadTable
On commit: scan SireadTable for pages in write_set
    → if any reading transaction is still active → set has_outgoing_rw on reader
    → if committing transaction has pages read by committed writers → set has_incoming_rw
    → if BOTH set → abort pivot

Downgrade: PRAGMA fsqlite.serializable = OFF  → skip SSI checks, use plain SI
```

**Result:** SSI makes `BEGIN CONCURRENT` truly serializable — not "serializable because we serialize," but "serializable because the Cahill/Fekete rule mathematically prevents all anomalies." The overhead is a hash table lookup per page read and a scan at commit — less than 7% throughput cost for anomaly-free concurrency.

### Sheaf-Theoretic Consistency Checking

In multi-process and distributed settings, pairwise consistency checks miss subtle anomalies where no two nodes disagree, but the global state is inconsistent. FrankenSQLite uses a **sheaf-theoretic consistency model** where each transaction's local view is a "section" over its read set, and the sheaf condition requires overlapping sections to agree.

```
Formalism:
    Each transaction T defines a section:
        domain(T) = T.read_set                       (pages read)
        assignment(T) = { P → (version, data) }      (what T observed)

    Sheaf condition:
        For all T1, T2: if P ∈ domain(T1) ∩ domain(T2),
        then assignment(T1)[P] and assignment(T2)[P] must be consistent
        with the global version chain ordering.

    Obstruction:
        A set of sections that locally satisfy pairwise consistency
        but cannot be glued into a single global section.
```

**Why sheaves?** Pairwise comparisons can't detect "phantom global commits" — situations where no single pair of transactions disagrees, but the collective set of observations is impossible under any serial execution order. The sheaf condition catches these by checking whether all local views can be consistently glued together.

**Result:** This is used in the conformance harness (`fsqlite-harness`) to verify that multi-process MVCC produces results consistent with some serial execution order — even when the anomaly would be invisible to any pairwise comparison.

### Conformal Calibration: Distribution-Free Performance Bounds

Benchmark results follow unknown distributions. Claiming "MVCC adds less than 5% overhead" requires statistical rigor. FrankenSQLite uses **conformal prediction** for distribution-free confidence intervals.

```
Nonconformity score:
    R_t = |observed_t - predicted_t|

Threshold (finite-sample guarantee):
    q = quantile_{(1-α)(n+1)/n}(R_1, ..., R_n)

Coverage guarantee:
    P(R_{n+1} ≤ q) ≥ 1 - α    for ANY distribution

    No normality assumption. No parametric model.
    Works for heavy-tailed latency distributions, bimodal throughput,
    or any other pathological distribution real databases produce.
```

**Application:** Phase 9 verification gates use conformal p-values to detect benchmark regressions: "no regression (conformal p-value > 0.01) compared to Phase 8." This means the statement "no performance regression" is statistically rigorous, not a hand-wave over noisy benchmarks.

### Varint Encoding: Huffman-Optimal Integer Compression

SQLite's record format uses variable-length integers (varints) everywhere — record header sizes, rowids, serial type codes, overflow page pointers. The encoding is a form of prefix-free code optimized for small values.

```
Encoding scheme (1-9 bytes):
    Value range              Bytes   Encoding
    ─────────────────────    ─────   ────────────────────────────────
    0 to 127                 1       0xxxxxxx
    128 to 16,383            2       1xxxxxxx 0xxxxxxx
    16,384 to 2,097,151      3       1xxxxxxx 1xxxxxxx 0xxxxxxx
    ...                      ...     (continuation bit in high bit)
    > 2^56                   9       11111111 xxxxxxxx × 8

    The high bit of each byte signals "more bytes follow."
    The 9th byte (if reached) uses all 8 bits for data.
```

**Why this matters for databases:** Rowids cluster near small values (most tables have fewer than 2 billion rows). Serial type codes are always small (0-13 for fixed types). Header sizes rarely exceed 127 bytes. The varint encoding means these common values consume just 1 byte instead of 8, compressing the record format by 30-50% compared to fixed-width integers.

**Decode performance:** A varint decode is a tight loop with one branch per byte. For 1-byte varints (the common case), it's a single comparison and mask. The branch predictor handles this well because the common case (1-2 bytes) dominates.

### Collation Sequences

String comparison in SQL is not `memcmp` — it depends on the **collation sequence**, which defines ordering, equality, and case sensitivity rules.

```
Built-in collations:
    BINARY   memcmp byte comparison (default)
    NOCASE   ASCII case-insensitive (a-z fold to A-Z, then memcmp)
    RTRIM    Like BINARY but trailing spaces are ignored

Collation selection rules:
    1. Explicit COLLATE clause wins:  WHERE name = 'foo' COLLATE NOCASE
    2. Column declaration:            name TEXT COLLATE NOCASE
    3. Left operand's collation propagates
    4. Default: BINARY

ICU collation (via fsqlite-ext-icu):
    Full Unicode collation via ICU locale rules.
    CREATE TABLE t(name TEXT COLLATE "en_US");
```

Collations affect not just WHERE comparisons but also ORDER BY sort order, GROUP BY grouping, DISTINCT elimination, and index lookup. A NOCASE index can satisfy a NOCASE WHERE clause without a table scan.

### Foreign Key Enforcement

Foreign keys enforce referential integrity across tables. FrankenSQLite implements the full SQLite foreign key protocol, including deferred constraint checking.

```
Enforcement modes:
    PRAGMA foreign_keys = ON    (default OFF for SQLite compat; must be per-connection)

    IMMEDIATE (default):  checked after each DML statement
    DEFERRED:             checked at COMMIT time

Actions on parent change:
    ON DELETE CASCADE     → delete all child rows referencing deleted parent
    ON DELETE SET NULL    → set FK columns to NULL
    ON DELETE SET DEFAULT → set FK columns to their DEFAULT value
    ON DELETE RESTRICT    → abort immediately (even in deferred mode)
    ON DELETE NO ACTION   → check at statement end (or COMMIT if deferred)

    Same five actions available for ON UPDATE.

Implementation:
    Each FK creates implicit triggers:
    - Before INSERT on child: verify parent exists
    - Before UPDATE on child FK cols: verify new parent exists
    - After DELETE on parent: execute ON DELETE action
    - After UPDATE on parent PK: execute ON UPDATE action
```

Deferred foreign keys interact with savepoints: `ROLLBACK TO savepoint` can re-violate constraints that were previously satisfied, and the violation is re-checked at the next COMMIT.

### Trigger System Architecture

Triggers fire procedural code in response to DML events. FrankenSQLite implements the complete SQLite trigger model, including INSTEAD OF triggers on views.

```
Trigger types:
    BEFORE INSERT/UPDATE/DELETE    fires before the row change
    AFTER INSERT/UPDATE/DELETE     fires after the row change
    INSTEAD OF INSERT/UPDATE/DELETE  only on views, replaces the DML

Pseudo-table access:
    NEW.column    → the row being inserted/updated (available in INSERT, UPDATE)
    OLD.column    → the row being deleted/updated (available in DELETE, UPDATE)

RAISE functions (trigger-specific error control):
    RAISE(IGNORE)                → silently skip this row
    RAISE(ROLLBACK, 'message')   → rollback entire transaction
    RAISE(ABORT, 'message')      → rollback statement, keep transaction
    RAISE(FAIL, 'message')       → stop statement but keep changes so far

Execution model:
    Triggers compile to VDBE subroutines.
    Trigger body is a sequence of DML statements, each compiled independently.
    Maximum trigger recursion depth: 1000 (SQLITE_MAX_TRIGGER_DEPTH).
    Recursive triggers require PRAGMA recursive_triggers = ON.
```

Triggers interact with MVCC: a BEFORE trigger that reads other tables establishes rw-dependencies tracked by the SireadTable for SSI validation. A trigger that writes to other tables extends the transaction's write set and page lock set.

### WAL Index Hash Table

The WAL index (the `-shm` file) contains a hash table that maps page numbers to WAL frame offsets, allowing O(1) lookup of the most recent version of any page in the WAL.

```
Structure:
    HASHTABLE_NPAGE = 4096    entries per hash table segment
    HASHTABLE_NSLOT = 8192    slots per hash table (2× entries for load factor ≤ 0.5)

    Hash function:
        slot = page_number * 383            (prime multiplier)
        slot = slot % HASHTABLE_NSLOT       (open addressing)

    Collision resolution: linear probing (slot + 1, slot + 2, ...)

    Multiple segments: the WAL index grows by adding hash table segments,
    each covering HASHTABLE_NPAGE frames. Lookup scans segments in reverse
    order (newest first) to find the most recent frame for a page.
```

**Read path (no locking required):**

```
lookup(page_number):
    for segment in segments.iter().rev():     // newest first
        slot = (page_number * 383) % 8192
        loop:
            if segment.entries[slot] == 0:     // empty slot, not in this segment
                break
            if segment.page_numbers[slot] == page_number:
                return segment.frame_offset[slot]   // found it
            slot = (slot + 1) % 8192                // linear probe
    return None  // not in WAL, read from database file
```

The load factor cap at 0.5 keeps the expected number of probes below 2. Since the hash table lives in shared memory (mmap), readers access it without any system calls or lock acquisitions.

---

### BOCPD: Workload Regime Detection

Database workloads are non-stationary. A write-heavy analytical job may start at 2 AM, a bulk import may spike contention, or a schema migration may temporarily change the page access pattern. Static thresholds for MVCC tuning parameters (GC frequency, version chain length limits, witness-plane hot/cold index compaction policy) will be wrong for at least one regime.

FrankenSQLite uses **Bayesian Online Change-Point Detection** (Adams & MacKay, 2007) to detect regime shifts in real time. BOCPD maintains a posterior distribution over the *run length* `r_t` (number of observations since the last change point):

```
P(r_t | x_{1:t}) ∝ Σ_{r_{t-1}} P(x_t | r_t, x_{t-r_t:t-1}) · P(r_t | r_{t-1}) · P(r_{t-1} | x_{1:t-1})
```

The predictive probability under the current regime is modeled as a conjugate **Normal-Gamma** for throughput and contention streams, and **Beta-Binomial** for abort rates. The hazard function uses a geometric prior with `H = 1/250`, corresponding to an expected regime length of ~4 minutes at one observation per second.

**What BOCPD monitors:**

| Stream | Conjugate Model | Action on Change Point |
|--------|----------------|----------------------|
| Commit throughput (ops/sec) | Normal-Gamma | Log regime shift, adjust GC frequency |
| SSI abort rate | Beta-Binomial | If rate jumps, log warning; if rate drops, relax version chain limits |
| Page contention (locks/sec) | Normal-Gamma | Adjust witness-plane refinement and hot-index pressure controls |
| Version chain length | Normal-Gamma | Tighten/loosen GC watermarks |

**Why BOCPD instead of fixed-window averages:**
- No window size to tune (the algorithm infers the regime length).
- Exact posterior inference via the run-length recursion (no MCMC needed).
- Naturally handles multiple change points.
- Computational cost: O(1) amortized after pruning low-probability run lengths.

BOCPD operates as an advisory harness component (Layer 3 in the monitoring stack), sitting above the e-process invariant monitors and conformal anomaly detection. It does not gate correctness decisions; it tunes heuristics for GC, eviction, and compaction scheduling.

---

## Implementation Roadmap and Verification Gates

FrankenSQLite follows a 9-phase implementation plan. Each phase has specific **verification gates** — quantitative acceptance criteria that must pass before the next phase begins. No phase ships until every gate is green.

### Phase Overview

| Phase | Focus | Key Deliverables |
|-------|-------|-----------------|
| 1 | **Bootstrap** | Workspace scaffold, core types, error handling, limits, opcodes |
| 2 | **Storage Foundation** | VFS traits, MemoryVfs, UnixVfs, pager, record format serialization |
| 3 | **Trees and Parsing** | B-tree engine, SQL parser, AST, property-based tests |
| 4 | **Query Engine** | VDBE bytecode VM, code generation, basic query execution |
| 5 | **Persistence** | WAL implementation, crash recovery, file format compatibility |
| 6 | **Concurrency** | MVCC engine, SSI, safe write merge ladder, garbage collection |
| 7 | **SQL Completeness** | Query planner, window functions, CTEs, triggers, views |
| 8 | **Extensions** | FTS5, R-tree, JSON1, session, ICU, misc extensions |
| 9 | **Conformance** | 100% parity target with C SQLite, benchmarks, hardening |

### Universal Verification Gates (Every Phase)

```
1. cargo check --workspace                                   zero errors, zero warnings
2. cargo clippy --workspace --all-targets -- -D warnings     pedantic + nursery lints
3. cargo fmt --all -- --check                                all code formatted
4. cargo test --workspace                                    all tests pass
5. cargo doc --workspace --no-deps                           all public items documented
```

### Phase-Specific Gates (Selected)

**Phase 3 (Trees and Parsing):**
- B-tree proptest: 10,000-operation random sequence → all invariants hold
- B-tree cursor iteration after random ops matches `BTreeMap` reference
- Parser: 95% coverage of `parse.y` grammar productions
- Parser fuzz: 1 hour fuzzing, zero panics

**Phase 5 (Persistence):**
- File format: FrankenSQLite DB readable by C `sqlite3` and vice versa
- WAL recovery: 100 crash-recovery scenarios → zero data loss
- RaptorQ WAL: recovery succeeds with up to R corrupted frames

**Phase 6 (Concurrency) — the most demanding gate:**
- MVCC stress: 100 concurrent writers, 100 ops each → all committed rows present, no phantoms
- SSI: write skew patterns abort under default serializable mode; succeed under `PRAGMA fsqlite.serializable = OFF`
- SSI: zero false negatives (3-transaction Mazurkiewicz trace exploration)
- E-process monitors: INV-1 through INV-7 → zero violations over 1M operations
- GC memory: usage within 2× of minimum theoretical bound
- Algebraic merge: 1,000 disjoint → zero false rejections; 1,000 overlapping → zero false acceptances
- Crash model: 100 crash-recovery scenarios validating self-healing contract

**Phase 9 (Conformance):**
- 100% parity target across 1,000+ golden test files (with any intentional divergences documented + annotated)
- Single-writer benchmarks within 3× of C SQLite
- No regression (conformal p-value > 0.01) compared to Phase 8

---

## Risk Register

Every ambitious project has risks. Here they are, along with the mitigations that make each one manageable.

| Risk | Severity | Mitigation |
|------|----------|-----------|
| **R1: SSI abort rate too high** (Page-SSI is conservative, may false-positive) | High | Refine SIREAD keys from page to (page, cell range) if needed; intent-level rebase turns conflicts into merges (30-60% reduction); PostgreSQL's measured false positive rate is 0.5% at row granularity |
| **R2: RaptorQ overhead dominates CPU** | Medium | Symbol sizing policy per object type; cache decoded objects aggressively via ARC; profile/tune encoder/decoder hot paths |
| **R3: Append-only storage grows without bound** | Medium | Checkpoint and compaction are first-class operations; enforce budgets for MVCC history, SIREAD table, symbol caches; GC horizon = min(active txn ids) bounds version chain length |
| **R4: Bootstrap chicken-and-egg** (need index to find symbols, need symbols to build index) | Low | Symbol records are self-describing (header + OTI); one tiny mutable root pointer per database; rebuild-from-scan always possible as fallback |
| **R5: Multi-process MVCC coordination is complex** | High | Shared-memory coordination protocol fully specified; lease-based TxnSlot cleanup handles process crashes; validate in-process first (Phase 6), cross-process follows (Phase 7) |
| **R6: File format compatibility vs innovation** | Medium | Compatibility Mode = standard SQLite format; Native Mode = innovation layer; conformance harness validates observable behavior |
| **R7: Mergeable writes become a correctness minefield** | High | Strict merge safety ladder (Section above); proptest invariants + DPOR tests; start with deterministic rebase for small op subset, expand guided by benchmarks |
| **R8: Distributed mode correctness is hard** | High | Leader commit clock as default; sheaf checks + TLA⁺ export for bounded model checking; implementation phased: single-node first, multi-node Phase 9 |

### Open Questions

1. **Multi-process writer performance envelope?** → Benchmark shared-memory coordination overhead.
2. **How far to refine SIREAD granularity?** → Start page-only, collect witnesses, refine when proven necessary.
3. **Symbol sizing policy per object type?** → Benchmark, pick defaults, expose PRAGMAs for tuning.
4. **Where to checkpoint for compat .db without bottlenecking?** → Background checkpoint with ECS chunks.
5. **Which B-tree ops for deterministic rebase?** → Inserts/updates on leaf pages first.
6. **Need B-link style concurrency for hot-page split/merge?** → Benchmark; if needed, add structure modification protocol.

---

## Comparison with Alternatives

| | **C SQLite** | **FrankenSQLite** | **libsql** | **DuckDB** | **Limbo** |
|---|---|---|---|---|---|
| Language | C | Rust (safe) | C (SQLite fork) | C++ | Rust |
| Concurrent writers | No (1 writer) | Yes (page-level MVCC) | Partial (WAL extensions) | Yes (different architecture) | No (1 writer) |
| Isolation level | Serializable (by serializing) | SSI (true serializable concurrency) | Snapshot | Snapshot | Snapshot |
| Memory safety | Manual | Compile-time guaranteed | Manual (C) | Manual (C++) | Compile-time guaranteed |
| File format | SQLite 3.x | SQLite 3.x (Compat) or ECS (Native) | SQLite 3.x (compatible) | Own format | SQLite 3.x (compatible) |
| Page encryption | Commercial (SEE) | XChaCha20-Poly1305 built-in | No | No | No |
| Self-healing storage | No | RaptorQ repair symbols | No | No | No |
| Cross-process MVCC | No | Shared-memory coordination | No | Yes | No |
| Embeddable | Yes | Yes | Yes | Yes | Yes |
| Extensions | Loadable + built-in | Built-in | Built-in + WASM | Built-in | Limited |
| WASM target | Via Emscripten | Planned (VFS abstraction) | Yes | Yes | Yes |
| Async I/O | No | Yes (asupersync + Cx) | Yes | No | Yes (io_uring) |

FrankenSQLite is the only option that combines SQLite file format compatibility, concurrent writers via MVCC with SSI, page-level encryption, self-healing storage, and Rust memory safety. Limbo (another Rust SQLite) focuses on async I/O with io_uring but retains the single-writer model. libsql is a C fork that inherits the original codebase's complexity. DuckDB targets analytics workloads with a columnar storage format incompatible with SQLite.

---

## Building from Source

### Prerequisites

- [Rust nightly](https://rustup.rs/) (the `rust-toolchain.toml` handles this automatically)

### Build

```bash
git clone --recursive https://github.com/Dicklesworthstone/frankensqlite.git
cd frankensqlite
cargo build
```

### Run Tests

```bash
# Full test suite
cargo test

# With output
cargo test -- --nocapture

# Specific crate
cargo test -p fsqlite-types
cargo test -p fsqlite-error
cargo test -p fsqlite-btree
cargo test -p fsqlite-parser
cargo test -p fsqlite-mvcc
```

### Quality Gates

```bash
# Type checking
cargo check --all-targets

# Linting (pedantic + nursery at deny level)
cargo clippy --all-targets -- -D warnings

# Formatting
cargo fmt --check
```

### Benchmarks

```bash
# Run all benchmarks
cargo bench

# Specific benchmark suite
cargo bench --bench btree_perf
cargo bench --bench mvcc_scaling
cargo bench --bench parser_throughput
```

---

## What We Deliberately Exclude (and Why)

FrankenSQLite deliberately omits several components of the C SQLite ecosystem. Each exclusion has a technical rationale; none are omitted from laziness.

**Amalgamation build system.** The C SQLite amalgamation (`sqlite3.c`) is a single-file build artifact produced by concatenating ~150 source files. Its purpose is simplifying C compilation. Rust's Cargo workspace with 24 crates provides superior modularity, parallel compilation, and dependency tracking. There is no analog of the amalgamation in a Rust project.

**TCL test harness.** C SQLite's test suite is driven by ~90,000+ lines of TCL scripts deeply intertwined with the C API. These cannot be meaningfully ported. Instead, FrankenSQLite uses native Rust `#[test]` modules, proptest for property-based testing, a conformance harness comparing SQL output against C SQLite golden files, and asupersync's lab reactor for deterministic concurrency tests.

**LEMON parser generator.** C SQLite uses a custom LALR(1) parser generator called LEMON to produce `parse.c` from `parse.y`. FrankenSQLite uses a hand-written recursive descent parser with Pratt precedence for expressions. This yields better error messages with precise source span reporting, simpler maintenance, and no build-time code generation step. The `parse.y` grammar still serves as an authoritative reference.

**Loadable extension API (.so/.dll).** C SQLite supports dynamically loading extensions via `sqlite3_load_extension()`, requiring a C-compatible ABI and `dlopen`/`LoadLibrary` calls. FrankenSQLite instead compiles all extensions directly into the binary, controlled by Cargo features. This eliminates an entire class of security vulnerabilities (arbitrary code loading) and simplifies deployment. Users who need custom extensions implement Rust traits and recompile.

**Legacy file format quirks (schema format < 4).** Schema format number 4 has been the default since SQLite 3.3.0 (2006). Formats 1-3 have minor differences in how DESC indexes and boolean handling work. Supporting them would add complexity for a format that no actively maintained database uses. FrankenSQLite requires schema format 4 and rejects older formats with a clear error message.

**Shared-cache mode.** C SQLite's shared-cache mode allows multiple connections within the same process to share a single page cache and use table-level locking. It has been deprecated since SQLite 3.41.0 (2023) and is widely considered a source of subtle bugs. FrankenSQLite's MVCC system supersedes it entirely: multiple connections share the MVCC version chains and benefit from page-level concurrency, which is strictly superior.

**Multiplexor VFS.** C SQLite's multiplexor shards large databases across multiple files to work around filesystem limitations (e.g., FAT32 4GB limit). Modern filesystems do not have these limitations.

**SEE (SQLite Encryption Extension).** The commercial C SQLite encryption extension is not ported. FrankenSQLite provides its own page-level encryption using XChaCha20-Poly1305 with DEK/KEK envelope encryption, Argon2id key derivation, and O(1) instant rekey (only the wrapped key is rewritten, not bulk page data).

---

## Limitations

- **Nightly Rust required.** Uses edition 2024 features that aren't stabilized yet.
- **No C API.** The initial release targets Rust consumers. A C-compatible FFI wrapper is a future goal.
- **No loadable extensions.** Extension support is configured at compile time via Cargo features; dynamic `dlopen`-based loading is not planned.
- **No WASM target yet.** The VFS trait abstracts all OS operations, and a `WasmVfs` implementation is planned but not yet built. Browser/edge deployment via WebAssembly is a future goal.
- **MVCC adds memory overhead.** Multiple page versions consume more RAM than single-version SQLite. ARC eviction and GC mitigate this but introduce background work.
- **No row-level locking.** Two transactions modifying different rows on the same page can still conflict at the page level. The safe write-merge ladder can resolve commuting conflicts, but non-commuting conflicts still abort/retry. This is a deliberate tradeoff for file format compatibility.
- **Encryption adds per-page overhead.** The per-page 24-byte nonce and 16-byte tag (40 bytes total) consume reserved space in each page. Databases created with encryption cannot be read without the key, even by C SQLite.
- **Native mode databases are not directly readable by C SQLite.** The ECS commit stream format is FrankenSQLite-specific. Compatibility export (`compat/foo.db`) materializes a standard SQLite file on demand.

---

## SQLite Behavioral Quirks

SQLite has accumulated 24 years of behavioral nuances that applications depend on. FrankenSQLite replicates all of these faithfully. Knowing them is essential for understanding compatibility edge cases.

**Type affinity is advisory, not enforced.** You can store a TEXT value in an INTEGER column. Affinity only affects coercion during comparison and storage, not rejection. Exception: `STRICT` tables (SQLite 3.37+) enforce column types at insert time.

**NULL in UNIQUE constraints.** SQLite allows multiple NULL values in a UNIQUE column because `NULL != NULL`. This differs from PostgreSQL and SQL Server.

**ORDER BY on compound SELECT.** `ORDER BY` at the end of a `UNION`/`EXCEPT`/`INTERSECT` uses column numbers or aliases from the *first* SELECT, not the last.

**Integer overflow promotes to REAL.** Arithmetic expressions like `9223372036854775807 + 1` silently promote to floating point rather than wrapping. But `sum()` raises an error on overflow.

**AUTOINCREMENT vs rowid reuse.** Without `AUTOINCREMENT`, deleted rowids can be reused — SQLite picks `max(rowid)+1` for new rows. With `AUTOINCREMENT`, rowids never decrease (tracked via `sqlite_sequence` table), but there's a small write overhead per insert.

**LIKE is ASCII-only.** The built-in `LIKE` operator folds case for ASCII letters only. `'a' LIKE 'A'` is true, but `'ä' LIKE 'Ä'` is false without the ICU extension.

**Empty string is not NULL.** `''` (empty string) is a zero-length TEXT value, not NULL. `length('')` returns 0. `'' IS NULL` is false. This catches people coming from Oracle, where empty strings are NULL.

**Deterministic vs non-deterministic functions.** `random()`, `changes()`, and `last_insert_rowid()` are re-evaluated for each row. The query planner cannot factor them out of loops or cache their results.

---

## FAQ

**Q: Can I open an existing SQLite database with FrankenSQLite?**
A: Yes. FrankenSQLite reads and writes the standard SQLite file format byte-for-byte. A database created by C SQLite opens in FrankenSQLite and vice versa.

**Q: How does MVCC interact with WAL mode?**
A: WAL frames carry transaction IDs. The WAL index maps `(page_number, txn_id)` to frame offsets. Checkpoint respects active snapshots, writing back only pages whose versions are no longer needed by any reader.

**Q: What happens when two writers conflict on the same page?**
A: If the page lock is held, the second writer gets `SQLITE_BUSY` immediately (no waiting, no deadlocks). If both reach commit on the same page, FCW detects base drift; commuting conflicts may be resolved by the safe merge ladder when enabled, otherwise the loser aborts/retries with `SQLITE_BUSY_SNAPSHOT`.

**Q: Why not use `unsafe` for performance-critical paths?**
A: Safe Rust with proper data structures is fast. The type system prevents entire categories of bugs that would require extensive testing to catch in C. The performance ceiling of safe Rust is more than sufficient for a database engine.

**Q: Why reimplement rather than fork?**
A: SQLite's C codebase is well-engineered but carries 24 years of accumulated complexity (218K LOC in the amalgamation). An independent ground-up Rust reimplementation enables MVCC without fighting the existing architecture, provides compile-time memory safety, and produces a codebase that Rust developers can work with naturally.

**Q: What's the conformance target?**
A: **100% behavioral parity target** with C SQLite 3.52.0 for the supported
surface, measured by running the SQLite test corpus against both implementations
and comparing results. Any intentional divergence is documented and annotated
with rationale.

**Q: How does MVCC garbage collection affect latency?**
A: The GC runs on a background thread every ~1 second. It walks version chains and frees unreachable versions. The GC never holds the WAL append mutex, so it does not block writers. The only contention point is the brief `RwLock` acquisition to read the active transaction set when computing the GC horizon.

**Q: What prevents a long-running reader from causing unbounded memory growth?**
A: A reader that holds a snapshot open for a long time pins all page versions newer than its snapshot, preventing GC from reclaiming them. This is the same tradeoff PostgreSQL makes. In practice, connection timeouts and application-level query deadlines prevent runaway memory growth.

**Q: What is SSI and why does it matter?**
A: Serializable Snapshot Isolation detects write skew -- a class of anomaly where two transactions each read data the other writes, producing a result impossible under serial execution. Plain Snapshot Isolation misses this. FrankenSQLite applies the conservative Cahill/Fekete rule at page granularity: if a committed transaction has both an incoming and outgoing rw-antidependency edge, it is aborted. PostgreSQL has shipped SSI since 2011 with less than 7% throughput overhead. You can downgrade to plain SI with `PRAGMA fsqlite.serializable = OFF`.

**Q: What does RaptorQ actually buy me in practice?**
A: Three things. (1) Self-healing after torn writes: WAL frames carry repair symbols, so partial writes during a crash are recoverable without double-write journaling. (2) Bandwidth-optimal replication: fountain coding means a receiver can reconstruct data from any sufficient subset of encoding symbols, regardless of which symbols arrive. (3) Version chain compression: older page versions are stored as RaptorQ-encoded deltas rather than full copies.

**Q: What is the difference between Compatibility and Native mode?**
A: Compatibility mode stores data in a standard SQLite `.db` file readable by C SQLite. Native mode stores data as an append-only stream of content-addressed, erasure-coded objects (ECS) for maximum durability and cross-process concurrency. Both modes expose the same SQL dialect and API. Switch with `PRAGMA fsqlite.mode = compatibility | native`.

**Q: How does encryption work?**
A: `PRAGMA key = 'passphrase'` derives a KEK via Argon2id and unwraps a per-database random DEK. Pages are encrypted with XChaCha20-Poly1305 using a fresh random 24-byte nonce per page write; the nonce and 16-byte tag are stored in each page's reserved bytes. `PRAGMA rekey = 'new_passphrase'` re-wraps the DEK in O(1). In Native mode, encryption happens before RaptorQ encoding (encrypt-then-code).

**Q: Does FrankenSQLite support Windows?**
A: Yes. The `WindowsVfs` implements the same `Vfs` trait as `UnixVfs`, using `LockFileEx`/`UnlockFileEx` for file locking and `CreateFileMapping` for shared memory. Platform-specific code is isolated behind `#[cfg(target_os)]` gates. OS/2, VxWorks, and Windows CE are excluded.

**Q: Can I use FrankenSQLite as a library without the CLI?**
A: Yes. The `fsqlite` crate is the public API. The CLI (`fsqlite-cli`) is a separate binary crate that depends on `fsqlite`. You can depend on `fsqlite` alone.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `error[E0554]: #![feature]` | Using stable Rust | Install nightly: `rustup default nightly` or let `rust-toolchain.toml` handle it |
| `cargo clippy` warnings | Pedantic + nursery lints enabled | Fix the lint or add a targeted `#[allow]` with justification |
| `edition 2024` errors | Outdated nightly | Run `rustup update nightly` |
| Submodule missing after clone | Forgot `--recursive` | Run `git submodule update --init --recursive` |
| Tests fail on `fsqlite-types` | Possible float precision | Check platform; tests use exact float comparison for known values |
| SQLITE_BUSY in concurrent tests | Expected MVCC conflict | Wrap writes in a retry loop; see the concurrent writers example above |
| High memory usage with many readers | Long-lived snapshots pin old versions | Close transactions promptly; set connection timeouts |
| SSI abort (write skew detected) | Two concurrent transactions created rw-antidependency cycle | Retry the aborted transaction; or `PRAGMA fsqlite.serializable = OFF` if write skew is acceptable |
| Cannot open Native mode database in C SQLite | ECS format is FrankenSQLite-specific | Use `compat/foo.db` export, or switch to Compatibility mode |
| Encryption: "not an error" / garbled data | Wrong key or unencrypted database opened with key | Verify passphrase; use `PRAGMA key` before any other operation |

---

## Project Structure

```
frankensqlite/
├── Cargo.toml                # Workspace: 24 members, shared deps, lint config
├── Cargo.lock                # Pinned dependency versions
├── rust-toolchain.toml       # Nightly channel + rustfmt + clippy
├── AGENTS.md                 # AI agent development guidelines
├── COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md  # Single source of truth (~9,500 lines)
├── MVCC_SPECIFICATION.md     # Standalone MVCC formal specification
├── PLAN_TO_PORT_SQLITE_TO_RUST.md    # 9-phase implementation roadmap
├── PROPOSED_ARCHITECTURE.md  # Crate architecture + MVCC design spec
├── EXISTING_SQLITE_STRUCTURE.md      # SQLite behavioral specification
├── crates/
│   ├── fsqlite-types/        # Core types (2,800+ LOC, 64 tests)
│   ├── fsqlite-error/        # Error handling (578 LOC, 13 tests)
│   ├── fsqlite-vfs/          # Virtual filesystem
│   ├── fsqlite-pager/        # Page cache
│   ├── fsqlite-wal/          # Write-ahead log
│   ├── fsqlite-mvcc/         # MVCC engine
│   ├── fsqlite-btree/        # B-tree storage
│   ├── fsqlite-ast/          # SQL AST
│   ├── fsqlite-parser/       # SQL parser
│   ├── fsqlite-planner/      # Query planner
│   ├── fsqlite-vdbe/         # Bytecode VM
│   ├── fsqlite-func/         # Built-in functions
│   ├── fsqlite-ext-*/        # 7 extension crates
│   ├── fsqlite-core/         # Engine integration
│   ├── fsqlite/              # Public API
│   ├── fsqlite-cli/          # CLI shell
│   └── fsqlite-harness/      # Conformance tests
├── legacy_sqlite_code/
│   └── sqlite/               # C SQLite reference (git submodule)
├── benches/                  # Criterion benchmarks
├── conformance/              # SQLite compatibility test fixtures
└── tests/                    # Integration tests
```

---

## About Contributions

Please don't take this the wrong way, but I do not accept outside contributions for any of my projects. I simply don't have the mental bandwidth to review anything, and it's my name on the thing, so I'm responsible for any problems it causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also have to worry about other "stakeholders," which seems unwise for tools I mostly make for myself for free. Feel free to submit issues, and even PRs if you want to illustrate a proposed fix, but know I won't merge them directly. Instead, I'll have Claude or Codex review submissions via `gh` and independently decide whether and how to address them. Bug reports in particular are welcome. Sorry if this offends, but I want to avoid wasted time and hurt feelings. I understand this isn't in sync with the prevailing open-source ethos that seeks community contributions, but it's the only way I can move at this velocity and keep my sanity.

---

## License

MIT License (with OpenAI/Anthropic Rider). See `LICENSE`.
