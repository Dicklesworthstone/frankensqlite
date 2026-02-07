# Proposed Architecture for FrankenSQLite

## Overview

Clean-room Rust reimplementation of SQLite with MVCC page-level versioning.

FrankenSQLite aims to provide a fully compatible, embeddable SQL database engine
written in safe Rust that can read and write standard SQLite database files. The
key architectural differentiator is replacing SQLite's single-writer/multi-reader
concurrency model with true multi-version concurrency control (MVCC) at the page
level, enabling concurrent readers and writers without blocking.

---

## Crate Dependency Graph

The project is organized into 23 crates with a strict layered dependency
structure. Leaf crates have no internal dependencies. Higher-level crates
compose lower-level ones.

```
fsqlite-cli
  └── fsqlite
        └── fsqlite-core
              ├── fsqlite-vdbe
              │     ├── fsqlite-btree
              │     │     ├── fsqlite-pager
              │     │     │     ├── fsqlite-vfs
              │     │     │     │     ├── fsqlite-types
              │     │     │     │     └── fsqlite-error
              │     │     │     ├── fsqlite-types
              │     │     │     └── fsqlite-error
              │     │     ├── fsqlite-types
              │     │     └── fsqlite-error
              │     ├── fsqlite-pager
              │     ├── fsqlite-types
              │     └── fsqlite-error
              ├── fsqlite-planner
              │     ├── fsqlite-ast
              │     │     └── fsqlite-types
              │     ├── fsqlite-types
              │     └── fsqlite-error
              ├── fsqlite-parser
              │     ├── fsqlite-ast
              │     ├── fsqlite-types
              │     ├── fsqlite-error
              │     └── memchr (external)
              ├── fsqlite-mvcc
              │     ├── fsqlite-wal
              │     │     ├── fsqlite-pager
              │     │     ├── fsqlite-vfs
              │     │     ├── fsqlite-types
              │     │     └── fsqlite-error
              │     ├── fsqlite-pager
              │     ├── fsqlite-types
              │     ├── fsqlite-error
              │     └── parking_lot (external)
              ├── fsqlite-func
              │     ├── fsqlite-types
              │     └── fsqlite-error
              ├── fsqlite-ext-json
              │     ├── fsqlite-types
              │     └── fsqlite-error
              ├── fsqlite-ext-fts5
              │     ├── fsqlite-types
              │     └── fsqlite-error
              ├── fsqlite-ext-rtree
              │     ├── fsqlite-types
              │     └── fsqlite-error
              ├── fsqlite-wal
              ├── fsqlite-vfs
              ├── fsqlite-btree
              ├── fsqlite-pager
              ├── fsqlite-ast
              ├── fsqlite-types
              └── fsqlite-error

fsqlite-harness
  ├── fsqlite
  └── fsqlite-error
```

### Crate Summary Table

| Crate               | Role                                      | Internal Dependencies                                      | External Dependencies |
|----------------------|-------------------------------------------|------------------------------------------------------------|-----------------------|
| `fsqlite-types`      | Shared type definitions                   | (none -- leaf)                                             | (none)                |
| `fsqlite-error`      | Error types and result aliases            | (none -- leaf)                                             | `thiserror`           |
| `fsqlite-ast`        | Abstract syntax tree node types           | `fsqlite-types`                                            | (none)                |
| `fsqlite-vfs`        | Virtual filesystem abstraction            | `fsqlite-types`, `fsqlite-error`                           | (none)                |
| `fsqlite-pager`      | Page cache and I/O layer                  | `fsqlite-types`, `fsqlite-error`, `fsqlite-vfs`           | (none)                |
| `fsqlite-wal`        | Write-ahead log implementation            | `fsqlite-types`, `fsqlite-error`, `fsqlite-vfs`, `fsqlite-pager` | (none)          |
| `fsqlite-mvcc`       | Multi-version concurrency control         | `fsqlite-types`, `fsqlite-error`, `fsqlite-pager`, `fsqlite-wal` | `parking_lot`   |
| `fsqlite-btree`      | B-tree storage engine                     | `fsqlite-types`, `fsqlite-error`, `fsqlite-pager`         | (none)                |
| `fsqlite-parser`     | SQL lexer and parser                      | `fsqlite-types`, `fsqlite-error`, `fsqlite-ast`           | `memchr`              |
| `fsqlite-planner`    | Query planning and optimization           | `fsqlite-types`, `fsqlite-error`, `fsqlite-ast`           | (none)                |
| `fsqlite-vdbe`       | Virtual database engine (bytecode VM)     | `fsqlite-types`, `fsqlite-error`, `fsqlite-pager`, `fsqlite-btree` | (none)         |
| `fsqlite-func`       | Built-in scalar and aggregate functions   | `fsqlite-types`, `fsqlite-error`                           | (none)                |
| `fsqlite-ext-json`   | JSON1 extension                           | `fsqlite-types`, `fsqlite-error`                           | (none)                |
| `fsqlite-ext-fts5`   | Full-text search extension                | `fsqlite-types`, `fsqlite-error`                           | (none)                |
| `fsqlite-ext-rtree`  | R-tree spatial index extension            | `fsqlite-types`, `fsqlite-error`                           | (none)                |
| `fsqlite-core`       | Database engine orchestration             | `fsqlite-types`, `fsqlite-error`, `fsqlite-vfs`, `fsqlite-pager`, `fsqlite-wal`, `fsqlite-mvcc`, `fsqlite-btree`, `fsqlite-ast`, `fsqlite-parser`, `fsqlite-planner`, `fsqlite-vdbe`, `fsqlite-func` | (none) |
| `fsqlite`            | Public API facade                         | `fsqlite-types`, `fsqlite-error`, `fsqlite-core`          | (none)                |
| `fsqlite-cli`        | Command-line REPL shell                   | `fsqlite`, `fsqlite-error`                                 | (none)                |
| `fsqlite-harness`    | Test harness and compatibility suite      | `fsqlite`, `fsqlite-error`                                 | (none)                |

---

## Trait Hierarchy

### Storage Traits

```rust
/// Virtual filesystem -- abstracts OS file operations.
pub trait Vfs: Send + Sync {
    type File: VfsFile;

    fn open(&self, path: &str, flags: OpenFlags) -> Result<Self::File>;
    fn delete(&self, path: &str, sync_dir: bool) -> Result<()>;
    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool>;
    fn full_pathname(&self, path: &str) -> Result<String>;
}

/// An open file handle within a VFS.
pub trait VfsFile: Send + Sync {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8], offset: u64) -> Result<()>;
    fn truncate(&self, size: u64) -> Result<()>;
    fn sync(&self, flags: SyncFlags) -> Result<()>;
    fn file_size(&self) -> Result<u64>;
    fn lock(&self, level: LockLevel) -> Result<()>;
    fn unlock(&self, level: LockLevel) -> Result<()>;
}

/// Cache policy is pluggable: ARC/LRU/TinyLFU are implementations, not
/// assumptions. Pager logic MUST NOT hardcode eviction behavior.
pub trait CachePolicy: Send + Sync {
    fn on_hit(&self, key: PageCacheKey);
    fn on_insert(&self, key: PageCacheKey);
    fn choose_victim(&self) -> Option<PageCacheKey>;
}

/// Pager -- snapshot/txn-aware page access API (MVCC-first).
///
/// Key point: the snapshot/txn context is explicit in signatures so MVCC and
/// correctness constraints shape the design from day 1.
pub trait Pager: Send + Sync {
    type Transaction;
    type Snapshot;

    fn begin(&self, cx: &Cx, mode: TxnMode) -> Result<Self::Transaction>;
    fn snapshot(&self, txn: &Self::Transaction) -> Self::Snapshot;

    fn get_page(&self, cx: &Cx, snap: &Self::Snapshot, pgno: PageNumber) -> Result<PageRef>;
    fn write_page(&self, cx: &Cx, txn: &mut Self::Transaction, pgno: PageNumber) -> Result<PageRef>;

    fn commit(&self, cx: &Cx, txn: Self::Transaction) -> Result<()>;
    fn rollback(&self, cx: &Cx, txn: Self::Transaction) -> Result<()>;
}

/// Cursor operations over a B-tree.
/// Provides ordered traversal and mutation.
pub trait BtreeCursorOps {
    fn move_to(&mut self, key: &[u8]) -> Result<CursorPosition>;
    fn next(&mut self) -> Result<bool>;
    fn prev(&mut self) -> Result<bool>;
    fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()>;
    fn delete(&mut self) -> Result<()>;
}
```

### Function Traits

```rust
/// A deterministic or non-deterministic scalar function.
pub trait ScalarFunction: Send + Sync {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue>;
}

/// An aggregate function with step/finalize semantics.
pub trait AggregateFunction: Send + Sync {
    type State: Default;

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;
}

/// A window function extends aggregate with inverse and value.
pub trait WindowFunction: Send + Sync {
    type State: Default;

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;
    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;
    fn value(&self, state: &Self::State) -> Result<SqliteValue>;
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;
}
```

### Extension Traits

```rust
/// A virtual table implementation.
pub trait VirtualTable: Send + Sync {
    type Cursor: VirtualTableCursor;

    fn connect(db: &Database, args: &[&str]) -> Result<Self>
    where
        Self: Sized;
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;
    fn open(&self) -> Result<Self::Cursor>;
    fn disconnect(&mut self) -> Result<()>;
}

/// A cursor over a virtual table's rows.
pub trait VirtualTableCursor: Send {
    fn filter(&mut self, idx_num: i32, idx_str: Option<&str>, args: &[SqliteValue]) -> Result<()>;
    fn next(&mut self) -> Result<()>;
    fn eof(&self) -> bool;
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()>;
    fn rowid(&self) -> Result<i64>;
}
```

---

## MVCC Design (Detailed)

### Core Types

```rust
/// Monotonically increasing transaction identifier.
/// Allocated by an AtomicU64 in the MvccManager.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TxnId(u64);

impl TxnId {
    /// The implicit version for data read from the original database file
    /// before any WAL writes.
    const ZERO: TxnId = TxnId(0);
}

/// A consistent point-in-time view of the database.
/// Captured at transaction BEGIN.
struct Snapshot {
    /// All transactions with id <= high_water_mark that are NOT in
    /// in_flight are considered committed and visible.
    high_water_mark: TxnId,

    /// Transaction ids that were active (uncommitted) at the time
    /// this snapshot was taken. Their writes are invisible to us.
    in_flight: Vec<TxnId>,
}

/// A single versioned copy of a database page.
/// Versions form a singly-linked list from newest to oldest.
struct PageVersion {
    /// The page number this version belongs to.
    pgno: PageNumber,

    /// The transaction that created this version.
    created_by: TxnId,

    /// The 4096-byte (or configured page size) page content.
    data: PageData,

    /// Link to the previous (older) version, if any.
    prev: Option<Box<PageVersion>>,
}

/// Tracks which transaction holds exclusive write access to each page.
/// Used to detect write-write conflicts eagerly (no deadlocks).
struct PageLockTable(BTreeMap<PageNumber, TxnId>);
```

### Visibility Rules

A page version `V` is visible to snapshot `S` if and only if all three
conditions hold:

1. **Committed before snapshot horizon:** `V.created_by <= S.high_water_mark`
2. **Not from an in-flight transaction:** `V.created_by NOT IN S.in_flight`
3. **Most recent qualifying version:** `V` is the LATEST version satisfying
   conditions (1) and (2). Older qualifying versions are shadowed.

These rules guarantee snapshot isolation: each transaction sees a consistent
view of the database as of its start time, regardless of concurrent commits.

### Write Path

1. **Acquire page lock.** The writing transaction calls
   `PageLockTable::acquire(pgno, my_txn_id)`. This is a `BTreeMap` lookup
   protected by a `parking_lot::Mutex`.

2. **Conflict detection.** If the lock is already held by a different
   transaction, return `SQLITE_BUSY` immediately. There is no wait queue and
   therefore no possibility of deadlock. The application retries or aborts.

3. **Copy-on-write.** Create a new `PageVersion` with `created_by = my_txn_id`.
   The new version's `prev` pointer links to the existing version chain. The
   page content is copied from the version visible to the writing transaction's
   snapshot, then mutated in place.

4. **Commit validation.** At commit time, iterate the transaction's write set.
   For each page, verify that no other transaction committed a version of that
   page after this transaction's snapshot was taken. If validation fails, the
   commit is rejected (serialization failure) and the transaction must retry.

5. **WAL append.** Commit serialization is enforced by a `Mutex` around the WAL
   writer. Sequential WAL writes are fast because they are append-only. Each
   committed frame records the `TxnId`, page number, and page data with a
   checksum.

6. **Release page locks.** After the WAL append completes and the commit record
   is durable, all page locks held by this transaction are released atomically.

### Read Path (MvccPager::get_page)

The read path performs a three-tier lookup with no locks on the hot path:

1. **Buffer pool.** Check the in-memory `BTreeMap<(PageNumber, TxnId), CachedPage>`
   for a version visible to the current snapshot. This is the fast path and
   handles the vast majority of reads in a warm database.

2. **WAL index.** If not in the buffer pool, consult the WAL hash index for the
   most recent committed frame for this page that is visible to the snapshot.
   Read the frame from the WAL file into the buffer pool.

3. **Database file.** If no WAL frame exists, read the page directly from the
   main database file. This data has an implicit `TxnId::ZERO` version and is
   visible to all snapshots.

### Garbage Collection

Old page versions that can never be needed by any active transaction are
reclaimable:

- **GC horizon** is defined as `min(active_txn_ids)` across all currently open
  transactions. If no transactions are active, the horizon is the latest
  committed `TxnId`.

- A version `V` is reclaimable if:
  - `V.created_by < gc_horizon`, AND
  - There exists a newer committed version `V'` of the same page where
    `V'.created_by <= gc_horizon`.

  In other words, `V` has been superseded by a version that is also below the
  horizon, so no active or future transaction can ever need `V`.

- Garbage collection runs as a background task with a configurable interval
  (default: 1 second). It walks the version chains and unlinks reclaimable
  nodes, freeing their memory.

- During WAL checkpointing, reclaimable WAL frames are also reclaimed by
  copying their latest committed data back into the main database file.

### Buffer Pool

```rust
struct BufferPool {
    /// Versioned page cache. Keyed by (pgno, version) so that multiple
    /// versions of the same page can coexist.
    pages: BTreeMap<PageCacheKey, CachedPage>,

    /// Pluggable eviction policy (e.g., ARC by default).
    policy: Arc<dyn CachePolicy>,

    /// Maximum number of pages to keep in the pool.
    /// Default: 2000 pages (~8MB at 4KB page size).
    capacity: usize,
}

struct CachedPage {
    data: PageData,
    dirty: bool,
    ref_count: AtomicU32,
}
```

Eviction policy:
- Only pages with `ref_count == 0` are eviction candidates.
- Among candidates, the configured `CachePolicy` chooses victims using signals
  like recency/frequency and pager-provided hints (clean vs dirty, superseded
  vs latest).
- Dirty pages are never evicted; they must be flushed to the WAL first.

---

## Async Integration

FrankenSQLite uses **asupersync** as its async runtime and I/O substrate.
There is no Tokio layer. The engine is written to be cancellation-aware and
deadline-aware by threading `&Cx` through long-running operations.

### Components

- **Cx (Capability Context).** Every database operation accepts `&Cx` for
  cooperative cancellation and deadlines. Long-running queries observe
  cancellation at explicit yield points (e.g., VDBE instruction boundaries)
  and return `SQLITE_INTERRUPT` when cancelled.

- **Two-phase channels.** Commit publication and other critical pipelines use
  asupersync's two-phase MPSC channels (reserve/commit) for bounded backpressure
  and cancellation-safe publication.

- **WriteCoordinator.** A single sequencer serializes only the tiny ordering
  step of commit publication (validation + marker append). Bulk durability I/O
  is performed by writers off the critical section.

### Flow

```
caller (sync or async)
  → Connection::execute(&cx, sql)
    → parse(sql)
    → plan(ast)
    → execute(bytecode, &cx)
      ← periodically checks cx.is_cancelled()
    → on commit: publish request (two-phase MPSC) → await coordinator response
  ← Result<Rows>
```

---

## Query Pipeline

The full lifecycle of a SQL statement from text to results:

```
SQL text
  |
  v
Lexer (tokenizer)
  - Produces a stream of Token { kind: TokenKind, span: Range<usize> }
  - Uses memchr for fast keyword/identifier boundary detection
  - Handles string literals, blob literals, numeric literals, operators
  - Zero-copy: tokens reference the original input by byte range
  |
  v
Parser (recursive descent, Pratt precedence for expressions)
  - Top-level: parse_statement() dispatches on first token
  - SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, etc.
  - Expressions use Pratt parsing for correct precedence and associativity
  - Produces a typed AST with source spans for error reporting
  |
  v
AST (Abstract Syntax Tree)
  - Strongly typed enum hierarchy: Statement, Expr, SelectCore, JoinClause, etc.
  - All identifiers stored as interned strings
  - Preserves source location for diagnostics
  |
  v
Name Resolution
  - Resolves table names to schema entries
  - Resolves column names to (table_index, column_index) pairs
  - Validates that all referenced tables and columns exist
  - Expands SELECT * into explicit column lists
  - Detects ambiguous column references in joins
  |
  v
Query Planning (index selection, join ordering)
  - Enumerates candidate indexes for each WHERE clause term
  - Estimates selectivity using table statistics (sqlite_stat1)
  - Chooses join order using a greedy heuristic (smallest estimated
    intermediate result first) for queries with <= 8 tables,
    exhaustive search for fewer tables
  - Generates LogicalPlan tree: Scan, IndexLookup, NestedLoopJoin,
    Sort, Aggregate, Limit, etc.
  |
  v
VDBE Bytecode Generation
  - Translates LogicalPlan into a linear sequence of VdbeOp instructions
  - Register-based VM (not stack-based) matching SQLite's design
  - Key opcodes: Init, Transaction, OpenRead, OpenWrite, Rewind, Next,
    Column, ResultRow, MakeRecord, Insert, Delete, IdxGE, IdxLT,
    Goto, If, IfNot, Halt, Function, AggStep, AggFinal, SorterOpen,
    SorterInsert, SorterSort, SorterData, SorterNext
  - Instruction encoding: (opcode: u8, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16)
  |
  v
Execution (fetch-execute loop, match-based opcode dispatch)
  - Main loop: while pc < program.len() { match program[pc].opcode { ... } }
  - Each opcode handler reads inputs from registers, performs its operation,
    writes outputs to registers, and advances pc (or jumps)
  - B-tree cursors opened by OpenRead/OpenWrite, traversed by Rewind/Next/Prev
  - ResultRow yields a row to the caller; execution suspends (coroutine-style)
  - Halt terminates execution with a result code
  |
  v
Results
  - Rows yielded one at a time via an iterator interface
  - Each row is a slice of SqliteValue (Null, Integer, Real, Text, Blob)
  - Column metadata (name, declared type, database, table, origin) available
    via the statement handle
```

---

## File Format Compatibility

FrankenSQLite reads and writes standard SQLite database files with full
format compatibility. This means databases created by FrankenSQLite can be
opened by the C SQLite library and vice versa.

### Database File Header (100 bytes at offset 0)

```
Offset  Size  Description
------  ----  -----------
  0      16   Header string: "SQLite format 3\000"
 16       2   Page size in bytes (512..65536, or 1 for 65536)
 18       1   File format write version (1=legacy, 2=WAL)
 19       1   File format read version (1=legacy, 2=WAL)
 20       1   Reserved space at end of each page
 21       1   Max embedded payload fraction (must be 64)
 22       1   Min embedded payload fraction (must be 32)
 23       1   Leaf payload fraction (must be 32)
 24       4   File change counter
 28       4   Size of database in pages
 32       4   Page number of first freelist trunk page
 36       4   Total number of freelist pages
 40       4   Schema cookie
 44       4   Schema format number (4 for current)
 48       4   Default page cache size
 52       4   Largest root b-tree page number (auto-vacuum/incremental-vacuum)
 56       4   Database text encoding (1=UTF8, 2=UTF16le, 3=UTF16be)
 60       4   User version (set by PRAGMA user_version)
 64       4   Incremental vacuum mode flag
 68       4   Application ID (set by PRAGMA application_id)
 72      20   Reserved for expansion (must be zero)
 92       4   Version-valid-for number
 96       4   SQLite version number that wrote the file
```

### B-tree Page Structure

Each page in the database file is a B-tree node:

- **Interior table B-tree page**: Contains keys (rowids) and child page
  pointers. Used for navigating to the correct leaf.
- **Leaf table B-tree page**: Contains keys (rowids) and row data (records).
- **Interior index B-tree page**: Contains index keys and child page pointers.
- **Leaf index B-tree page**: Contains index keys only.

Page layout:
```
+---------------------------+
| Page header (8 or 12 bytes)|
+---------------------------+
| Cell pointer array         |
| (2 bytes per cell)         |
+---------------------------+
| Unallocated space          |
+---------------------------+
| Cell content area          |
| (grows from end of page)  |
+---------------------------+
| Reserved region            |
+---------------------------+
```

### Record Format

Each row is stored as a record with a header followed by data:

```
+------------------+------------------+------------------+
| Header size      | Serial type 1    | Serial type 2    | ...
| (varint)         | (varint)         | (varint)         |
+------------------+------------------+------------------+
| Value 1 bytes    | Value 2 bytes    | ...
+------------------+------------------+
```

Serial type encoding:
| Serial Type | Content Size | Meaning                           |
|-------------|-------------|-----------------------------------|
| 0           | 0           | NULL                              |
| 1           | 1           | 8-bit twos-complement integer     |
| 2           | 2           | 16-bit big-endian integer         |
| 3           | 3           | 24-bit big-endian integer         |
| 4           | 4           | 32-bit big-endian integer         |
| 5           | 6           | 48-bit big-endian integer         |
| 6           | 8           | 64-bit big-endian integer         |
| 7           | 8           | IEEE 754 64-bit float             |
| 8           | 0           | Integer value 0                   |
| 9           | 0           | Integer value 1                   |
| 10, 11      | 0           | Reserved for expansion             |
| N >= 12 even| (N-12)/2    | BLOB of (N-12)/2 bytes            |
| N >= 13 odd | (N-13)/2    | TEXT of (N-13)/2 bytes             |

### WAL Frame Format

When in WAL mode, writes go to the write-ahead log rather than the main
database file. Each WAL frame has the following structure:

```
WAL Header (32 bytes, at start of WAL file):
  Offset  Size  Description
  ------  ----  -----------
    0       4   Magic number (0x377F0682 or 0x377F0683)
    4       4   File format version (3007000)
    8       4   Database page size
   12       4   Checkpoint sequence number
   16       4   Salt-1 (random value, changes on each checkpoint)
   20       4   Salt-2 (random value, changes on each checkpoint)
   24       4   Checksum-1 (cumulative checksum of WAL header)
   28       4   Checksum-2

WAL Frame Header (24 bytes, before each page):
  Offset  Size  Description
  ------  ----  -----------
    0       4   Page number
    4       4   For commit records: database size in pages after commit.
                For non-commit frames: 0.
    8       4   Salt-1 (must match WAL header)
   12       4   Salt-2 (must match WAL header)
   16       4   Checksum-1 (cumulative over frame header + page data)
   20       4   Checksum-2

Frame Body:
  <page_size> bytes of page data
```

The WAL index (wal-index, stored in shared memory or a `-shm` file) provides
a hash table mapping page numbers to their most recent frame in the WAL,
enabling O(1) lookup during reads.

---

## Testing Strategy

### Layers

1. **Unit tests** in each crate test individual components in isolation using
   mock implementations of trait dependencies.

2. **Integration tests** in `fsqlite-core` test the full query pipeline from
   SQL text to result rows, using an in-memory VFS.

3. **Compatibility tests** in `fsqlite-harness` run the standard SQLite test
   suite (adapted from the TCL-based test harness) against FrankenSQLite,
   verifying behavioral equivalence with the C implementation.

4. **Fuzz tests** using `cargo-fuzz` target the parser, record decoder, and
   B-tree page decoder with arbitrary byte inputs.

5. **Concurrency tests** specifically exercise MVCC behavior: concurrent
   readers and writers, snapshot isolation verification, write-write conflict
   detection, and garbage collection under load.

### File Format Round-Trip

A key test category creates databases with the C SQLite library, reads them
with FrankenSQLite (and vice versa), and verifies identical query results.
This ensures file format compatibility is maintained.
