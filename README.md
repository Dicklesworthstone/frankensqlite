<p align="center">
  <br>
  <img width="460" src="https://upload.wikimedia.org/wikipedia/commons/3/38/SQLite370.svg" alt="FrankenSQLite">
  <br><br>
</p>

<h1 align="center">FrankenSQLite</h1>

<p align="center">
  <strong>A clean-room Rust reimplementation of SQLite with concurrent writers and information-theoretic durability.</strong>
</p>

<p align="center">
  <a href="https://github.com/Dicklesworthstone/frankensqlite/actions"><img src="https://img.shields.io/github/actions/workflow/status/Dicklesworthstone/frankensqlite/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/Dicklesworthstone/frankensqlite/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-nightly%20%7C%20edition%202024-orange.svg" alt="Rust"></a>
  <a href="https://github.com/Dicklesworthstone/frankensqlite"><img src="https://img.shields.io/badge/unsafe-forbidden-success.svg" alt="unsafe forbidden"></a>
</p>

---

## TL;DR

**The Problem:** SQLite allows only one writer at a time. A single lock byte (`WAL_WRITE_LOCK` at `wal.c:3698`) serializes all writers. For write-heavy workloads, this bottleneck caps throughput regardless of how many cores you have. Torn writes and bit-flips can corrupt the database with no self-repair mechanism.

**The Solution:** FrankenSQLite reimplements SQLite from scratch in safe Rust with two architectural innovations:

1. **MVCC Concurrent Writers.** The single-writer lock is replaced with page-level Multi-Version Concurrency Control. Multiple writers commit simultaneously as long as they touch different pages. Serializable Snapshot Isolation (SSI) prevents write skew by default. Algebraic write merging resolves 30-50% of same-page conflicts at byte granularity, further reducing contention.

2. **RaptorQ-Pervasive Durability.** Every persistent layer is infused with RFC 6330 fountain codes via asupersync's production-grade RaptorQ implementation. WAL frames carry repair symbols for self-healing after torn writes. Snapshot transfer uses rateless coding for bandwidth-optimal replication over lossy networks. Data loss becomes a mathematical near-impossibility rather than a failure mode to mitigate.

The file format stays 100% compatible with existing `.sqlite` databases in Compatibility mode. A Native mode stores everything as content-addressed, erasure-coded objects (ECS) for maximum durability and cross-process concurrency.

### Why FrankenSQLite?

| Feature | C SQLite | FrankenSQLite |
|---------|----------|---------------|
| Concurrent writers | 1 (file-level lock) | Many (page-level MVCC with SSI) |
| Isolation level | SERIALIZABLE (by serializing) | SERIALIZABLE (SSI for concurrent mode) |
| Concurrent readers | Unlimited (WAL mode) | Unlimited (no `aReadMark[5]` limit) |
| Memory safety | Manual (C) | Guaranteed (`#[forbid(unsafe_code)]`) |
| Data races | Possible (careful C) | Impossible (Rust ownership) |
| File format | SQLite 3.x | Identical (Compatibility mode) or ECS (Native mode) |
| Self-healing storage | No | Yes (RaptorQ repair symbols) |
| Page-level encryption | No (commercial SEE extension) | AES-256-GCM with Argon2id key derivation |
| SQL dialect | Full | Full (same parser coverage) |
| Extensions | FTS3/4/5, R-tree, JSON1, etc. | All the same, compiled in |
| Cross-process MVCC | No | Yes (shared-memory coordination) |
| Embedded, zero-config | Yes | Yes |

---

## Design Philosophy

### 1. Clean-Room, Not a Translation

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

`BEGIN CONCURRENT` provides full SERIALIZABLE isolation, not merely Snapshot Isolation. The conservative Cahill/Fekete rule applied at page granularity ("Page-SSI") prevents write skew: no committed transaction may have both an incoming and outgoing rw-antidependency edge. PostgreSQL has shipped SSI since 2011 with less than 7% throughput overhead. `PRAGMA fsqlite.serializable = OFF` explicitly downgrades to plain SI for benchmarking or applications that tolerate write skew. When two writers touch the same page, the first to commit wins. The second gets `SQLITE_BUSY` and retries. Deadlocks are impossible by construction (eager page locking, no wait-for cycles).

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

FrankenSQLite is organized as a 23-crate Cargo workspace with strict layered dependencies:

```
                          ┌──────────────┐
                          │  fsqlite-cli │  Interactive SQL shell
                          └──────┬───────┘
                                 │
                          ┌──────┴───────┐
                          │   fsqlite    │  Public API facade
                          └──────┬───────┘
                                 │
                          ┌──────┴───────┐
                          │ fsqlite-core │  Engine orchestration
                          └──────┬───────┘
                                 │
            ┌────────────────────┼────────────────────┐
            │                    │                     │
     ┌──────┴──────┐    ┌───────┴───────┐    ┌────────┴───────┐
     │  SQL Layer  │    │ Storage Layer │    │   Extensions   │
     ├─────────────┤    ├───────────────┤    ├────────────────┤
     │ parser      │    │ btree         │    │ ext-fts3       │
     │ ast         │    │ pager         │    │ ext-fts5       │
     │ planner     │    │ wal           │    │ ext-rtree      │
     │ vdbe        │    │ mvcc          │    │ ext-json       │
     │ func        │    │ vfs           │    │ ext-session    │
     └──────┬──────┘    └───────┬───────┘    │ ext-icu        │
            │                   │            │ ext-misc       │
            └─────────┬─────────┘            └────────┬───────┘
                      │                               │
            ┌─────────┴───────────────────────────────┘
            │
     ┌──────┴──────┐    ┌──────────────┐
     │fsqlite-types│    │fsqlite-error │  Foundation (no internal deps)
     └─────────────┘    └──────────────┘
```

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
     │   ├── Yes → First to lock page 47 wins. Second gets SQLITE_BUSY.
     │   │         Deadlock impossible (eager locking, no wait-for cycles).
     │   │
     │   └── If algebraic write merging is enabled (PRAGMA raptorq_write_merge = ON):
     │       └── Attempt deterministic rebase of loser's intent log against
     │           current committed state. If replay succeeds → both commit.
     │
     └── No (different leaf pages) → Both proceed and commit independently.
```

The SSI check fires before the first-committer-wins check. This means write skew is caught even when the conflicting transactions touch disjoint pages, because SSI tracks read dependencies (via the `SireadTable`) across all pages.

### MVCC Visibility Rules

A page version `V` is visible to snapshot `S` if and only if all three conditions hold:

1. `V.created_by <= S.high_water_mark` (committed before the snapshot was taken)
2. `V.created_by` is not in `S.in_flight` (the creating transaction had finished when the snapshot was taken)
3. `V` is the newest version satisfying (1) and (2) (older qualifying versions are shadowed)

These rules produce snapshot isolation: each transaction sees a frozen view of the database as of its start time, regardless of concurrent commits happening around it.

### MVCC Core Data Structures

```rust
/// Monotonically increasing transaction identifier.
/// Allocated from an AtomicU64 with SeqCst ordering.
struct TxnId(u64);

/// A frozen view of which transactions are committed.
/// Captured at BEGIN. Uses RoaringBitmap for O(1) membership
/// tests and compressed storage (replacing SortedVec + BloomFilter).
struct Snapshot {
    high_water_mark: TxnId,
    in_flight: RoaringBitmap,
}

/// A single versioned copy of a database page.
/// Versions form a singly-linked list, newest to oldest.
struct PageVersion {
    pgno: PageNumber,
    created_by: TxnId,
    data: PageData,
    prev: Option<Box<PageVersion>>,
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

### Algebraic Write Merging and Intent Logs

Standard page-level MVCC produces false conflicts when two transactions modify different rows that happen to live on the same B-tree leaf page. Algebraic write merging reduces these false conflicts by 30-50% without introducing row-level MVCC metadata.

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
| 3 | Sparse XOR merge | Byte-range disjointness proven via GF(256) patches |
| 4 | Abort/retry | True conflict; no safe merge possible |

Algebraic write merging is gated behind `PRAGMA raptorq_write_merge = ON` (off by default). When enabled, intent logs are recorded automatically during writes and evaluated at commit time.

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

### Visibility Bloom Filter

Each `Snapshot` includes a Bloom filter over its `in_flight` set for O(1) amortized visibility checks. For small in-flight sets (< 8 transactions), binary search on the `RoaringBitmap` is used instead. Parameters scale with transaction concurrency: n=10 transactions need only 12 bytes; n=1000 need ~1.2 KB.

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

Write transactions submit commit requests through an MPSC channel to a single write coordinator task. This serializes commit validation (SSI check + first-committer-wins + optional algebraic merge) and WAL appends without holding a lock across the entire commit. Each request includes a `oneshot::Sender<Result<()>>` so the caller can `.await` the result.

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

95%+ behavioral compatibility with C SQLite 3.52.0. Every known incompatibility is documented with rationale. The conformance suite runs SQL Logic Tests (SLT format) covering:

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

FrankenSQLite provides AES-256-GCM page-level encryption as a built-in feature, replacing the need for SQLite's commercial Encryption Extension (SEE).

| Property | Value |
|----------|-------|
| Cipher | AES-256-GCM |
| Key derivation | Argon2id from passphrase |
| Nonce | 12 bytes, derived from `(page_number, write_counter)` |
| Authentication tag | 16 bytes, stored in the page's reserved space |
| Key management API | `PRAGMA key = 'passphrase'` / `PRAGMA rekey = 'new_passphrase'` |

The nonce is derived deterministically from the page number and a per-page write counter, ensuring uniqueness without additional storage overhead. The 16-byte GCM authentication tag fits in the reserved-bytes-per-page field from the database header.

In Native mode, encryption applies before RaptorQ encoding (encrypt-then-code). An attacker who corrupts encrypted ECS symbols cannot forge valid ciphertext; RaptorQ repairs the corruption, then decryption proceeds as normal.

---

## Comparison with Alternatives

| | **C SQLite** | **FrankenSQLite** | **libsql** | **DuckDB** | **Limbo** |
|---|---|---|---|---|---|
| Language | C | Rust (safe) | C (SQLite fork) | C++ | Rust |
| Concurrent writers | No (1 writer) | Yes (page-level MVCC) | Partial (WAL extensions) | Yes (different architecture) | No (1 writer) |
| Isolation level | Serializable (by serializing) | SSI (true serializable concurrency) | Snapshot | Snapshot | Snapshot |
| Memory safety | Manual | Compile-time guaranteed | Manual (C) | Manual (C++) | Compile-time guaranteed |
| File format | SQLite 3.x | SQLite 3.x (Compat) or ECS (Native) | SQLite 3.x (compatible) | Own format | SQLite 3.x (compatible) |
| Page encryption | Commercial (SEE) | AES-256-GCM built-in | No | No | No |
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

## Limitations

- **Nightly Rust required.** Uses edition 2024 features that aren't stabilized yet.
- **No C API.** The initial release targets Rust consumers. A C-compatible FFI wrapper is a future goal.
- **No loadable extensions.** All extensions are compiled in. Dynamic `dlopen`-based loading is not planned.
- **No WASM target yet.** The VFS trait abstracts all OS operations, and a `WasmVfs` implementation is planned but not yet built. Browser/edge deployment via WebAssembly is a future goal.
- **MVCC adds memory overhead.** Multiple page versions consume more RAM than single-version SQLite. ARC eviction and GC mitigate this but introduce background work.
- **No row-level locking.** Two transactions modifying different rows on the same page still conflict at the page level. Algebraic write merging reduces false conflicts by 30-50% when enabled, but does not eliminate them entirely. This is a deliberate tradeoff for file format compatibility.
- **Encryption adds per-page overhead.** The 16-byte GCM tag consumes reserved space in each page. Databases created with encryption cannot be read without the key, even by C SQLite.
- **Native mode databases are not directly readable by C SQLite.** The ECS commit stream format is FrankenSQLite-specific. Compatibility export (`compat/foo.db`) materializes a standard SQLite file on demand.

---

## FAQ

**Q: Can I open an existing SQLite database with FrankenSQLite?**
A: Yes. FrankenSQLite reads and writes the standard SQLite file format byte-for-byte. A database created by C SQLite opens in FrankenSQLite and vice versa.

**Q: How does MVCC interact with WAL mode?**
A: WAL frames carry transaction IDs. The WAL index maps `(page_number, txn_id)` to frame offsets. Checkpoint respects active snapshots, writing back only pages whose versions are no longer needed by any reader.

**Q: What happens when two writers conflict on the same page?**
A: The first to acquire the page lock wins. The second gets `SQLITE_BUSY` immediately (no waiting, no deadlocks). The application retries, exactly as with existing SQLite busy handling.

**Q: Why not use `unsafe` for performance-critical paths?**
A: Safe Rust with proper data structures is fast. The type system prevents entire categories of bugs that would require extensive testing to catch in C. The performance ceiling of safe Rust is more than sufficient for a database engine.

**Q: Why reimplement rather than fork?**
A: SQLite's C codebase is well-engineered but carries 24 years of accumulated complexity (218K LOC in the amalgamation). A clean-room Rust implementation enables MVCC without fighting the existing architecture, provides compile-time memory safety, and produces a codebase that Rust developers can work with naturally.

**Q: What's the conformance target?**
A: 95%+ behavioral compatibility with C SQLite 3.52.0, measured by running the SQLite test corpus against both implementations and comparing results. Known incompatibilities are documented with rationale.

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
A: `PRAGMA key = 'passphrase'` derives a 256-bit key via Argon2id and encrypts every page with AES-256-GCM. The 12-byte nonce comes from the page number and a write counter; the 16-byte authentication tag is stored in the page's reserved space. In Native mode, encryption happens before RaptorQ encoding (encrypt-then-code).

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
├── Cargo.toml                # Workspace: 23 members, shared deps, lint config
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

MIT
