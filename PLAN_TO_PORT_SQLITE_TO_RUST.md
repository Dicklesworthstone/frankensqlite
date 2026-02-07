# Plan to Port SQLite to Rust (FrankenSQLite)

## Overview

FrankenSQLite is a clean-room Rust reimplementation of SQLite 3.52.0, which comprises approximately 218,000 lines of C code in its amalgamation form. The goal is not a line-by-line translation but a principled reimplementation that leverages Rust's type system, memory safety guarantees, and concurrency primitives to produce a database engine that is fully compatible with SQLite's file format and SQL dialect while introducing a key architectural innovation: MVCC page-level versioning for truly concurrent writers.

SQLite's existing concurrency model permits unlimited concurrent readers but only a single writer at a time, enforced by file-level locking. FrankenSQLite replaces this with a multi-version concurrency control scheme operating at page granularity, allowing multiple writers to proceed in parallel as long as they do not modify the same pages. This unlocks significant throughput improvements for write-heavy workloads without sacrificing the simplicity and reliability that make SQLite the most widely deployed database engine in the world.

The target is full SQLite compatibility including all major extensions (FTS3/4/5, R-tree, JSON1, Session, ICU), the interactive CLI shell, and binary-level file format compatibility so that existing SQLite databases can be opened, read, and written by FrankenSQLite without any migration step.

## Scope

### In Scope

- **Complete SQL parser**: A hand-written recursive descent parser replacing the LEMON-generated `parse.y` used by C SQLite. The parser will produce a strongly-typed AST covering the full SQLite SQL dialect including CTEs, window functions, UPSERT, RETURNING, and all pragma statements.
- **Full VDBE bytecode interpreter**: Reimplementation of the Virtual Database Engine with all 190+ opcodes. The VDBE is the heart of SQLite's execution model; every SQL statement is compiled to a program of VDBE opcodes that are then interpreted. FrankenSQLite will maintain opcode-level compatibility to ease testing and debugging.
- **B-tree storage engine**: Complete implementation of SQLite's B-tree and B+tree structures with page-level operations. This includes interior pages, leaf pages, overflow pages, free-list management, and auto-vacuum support.
- **WAL with MVCC-aware concurrent writers**: The Write-Ahead Log implementation will be extended to support multiple concurrent writers through page-level versioning. The WAL format will remain compatible with SQLite's WAL format for reader compatibility.
- **All extensions**:
  - FTS3/FTS4/FTS5 (full-text search with tokenizers, ranking, and auxiliary functions)
  - R-tree (spatial indexing)
  - JSON1 (JSON path queries, modification, and aggregation)
  - Session (changeset/patchset generation and application)
  - ICU (Unicode-aware collation and case folding)
  - Miscellaneous functions (generate_series, csv, fileio, completion, etc.)
- **CLI shell**: An interactive database shell built using frankentui, providing equivalent functionality to the `sqlite3` command-line tool including dot-commands, import/export, and query formatting.
- **Conformance test suite**: A comprehensive test suite that runs against both FrankenSQLite and C SQLite to verify behavioral compatibility, targeting 95%+ compatibility on the SQLite test corpus.

### Out of Scope (Initial Release)

- **TCL test harness compatibility**: SQLite's primary test suite is written in TCL. Porting the TCL harness itself is out of scope; instead, we will build a Rust-native conformance suite that tests the same behaviors.
- **Loadable extension API (dlopen)**: The initial release will not support dynamically loading shared library extensions at runtime. All extensions will be compiled in.
- **Windows VFS**: The initial Virtual File System implementation will target Unix (Linux and macOS) only. Windows support will follow in a subsequent release.
- **WebAssembly target**: Compiling FrankenSQLite to WASM for browser or edge deployment is a future goal but not part of the initial release.

## Architecture

The project is organized as a Cargo workspace containing 23 crates under the `crates/` directory. This modular structure enforces clean dependency boundaries, enables parallel compilation, and allows individual components to be tested and documented in isolation.

```
frankensqlite/
├── Cargo.toml                  # workspace root
├── crates/
│   ├── frankensqlite/          # public API facade (re-exports)
│   ├── core/                   # fundamental types, error codes, constants
│   ├── vfs/                    # Virtual File System abstraction
│   ├── vfs-unix/               # Unix VFS implementation (open, read, write, lock, sync)
│   ├── pager/                  # page cache and page management
│   ├── wal/                    # Write-Ahead Log
│   ├── mvcc/                   # MVCC page versioning, snapshot management, conflict detection
│   ├── btree/                  # B-tree and B+tree implementation
│   ├── schema/                 # schema representation, sqlite_master parsing
│   ├── lexer/                  # SQL tokenizer/lexer
│   ├── parser/                 # recursive descent SQL parser
│   ├── ast/                    # typed AST nodes for all SQL statements
│   ├── analyzer/               # semantic analysis, name resolution, type checking
│   ├── planner/                # query planner and optimizer
│   ├── vdbe/                   # bytecode definitions, compiler, interpreter
│   ├── func/                   # built-in SQL functions (core, date/time, math, aggregate)
│   ├── fts5/                   # FTS5 full-text search extension
│   ├── fts3/                   # FTS3/FTS4 full-text search extensions
│   ├── rtree/                  # R-tree spatial index extension
│   ├── json/                   # JSON1 extension
│   ├── session/                # Session extension (changesets)
│   ├── icu/                    # ICU Unicode extension
│   └── shell/                  # CLI shell application
├── tests/                      # integration and conformance tests
└── benches/                    # benchmarks
```

### Key Architectural Principles

- **All async I/O via asupersync**: The asupersync crate provides the async runtime, channels, and synchronization primitives used throughout FrankenSQLite. All file I/O, inter-thread communication, and timer-based operations go through asupersync, ensuring a consistent and testable async model.
- **TUI via frankentui**: The interactive CLI shell is built on frankentui, which provides terminal rendering, input handling, line editing, and syntax highlighting.
- **Unsafe code forbidden at workspace level**: The workspace-level `Cargo.toml` sets `#![forbid(unsafe_code)]` as a default. Any crate that genuinely requires unsafe (such as `vfs-unix` for raw syscalls) must explicitly opt in with a documented justification. The vast majority of the codebase will be safe Rust.
- **Zero-copy where possible**: Page buffers, string slices, and blob data use borrowing and arena allocation to minimize copying. The pager hands out `&[u8]` references to pinned page buffers rather than copying page contents.
- **Strong typing over runtime checks**: SQL types, opcode arguments, page numbers, and error codes are represented as distinct Rust types (newtypes, enums) rather than bare integers, catching misuse at compile time.

## Phases

### Phase 1: Bootstrap & Spec Extraction

**Duration**: 2 weeks
**Goal**: Set up the workspace, extract specifications from SQLite source, and establish foundational types.

Deliverables:
- Cargo workspace with all 23 crate stubs created and compiling
- CI pipeline running `cargo check`, `cargo clippy`, `cargo fmt --check`, and `cargo test`
- `core` crate with:
  - All SQLite error codes as a Rust enum (`SQLITE_OK`, `SQLITE_ERROR`, `SQLITE_BUSY`, etc.)
  - Page size constants and limits
  - File format magic bytes and header structure
  - Type affinity definitions
  - Collation sequence identifiers
- Specification documents extracted from SQLite source comments and documentation:
  - File format specification (header, pages, cells, overflow, freelist)
  - WAL format specification (header, frames, checkpoints)
  - VDBE opcode catalog with semantics
  - B-tree invariants and algorithms
- `Result<T>` type alias and error conversion traits

### Phase 2: Core Types & Storage Foundation

**Duration**: 3 weeks
**Goal**: Implement the VFS abstraction, pager, and page cache.

Deliverables:
- `vfs` crate:
  - `Vfs` trait with methods for open, delete, access, full_pathname, randomness, sleep, current_time
  - `VfsFile` trait with methods for read, write, truncate, sync, file_size, lock, unlock, check_reserved_lock
  - Lock level enum: None, Shared, Reserved, Pending, Exclusive
  - In-memory VFS implementation for testing
- `vfs-unix` crate:
  - Full Unix VFS implementation using POSIX file I/O
  - Advisory locking via `fcntl(F_SETLK)`
  - `fsync` / `fdatasync` for durability
  - Shared memory for WAL index (`mmap` of `-shm` file)
- `pager` crate:
  - Page cache with configurable size (default 2000 pages)
  - LRU eviction policy with dirty-page write-back
  - Page acquisition: `get_page(pgno) -> &Page`
  - Page modification: `write_page(pgno) -> &mut Page`
  - Journal/WAL integration points (stubbed)
  - Page reference counting
- Database header parsing and validation (first 100 bytes)
- Unit tests for VFS operations, page cache eviction, and header parsing

### Phase 3: B-Tree & SQL Parser

**Duration**: 5 weeks
**Goal**: Implement the B-tree storage engine and the complete SQL parser.

Deliverables:
- `btree` crate:
  - B+tree for table storage (integer keys, data in leaves)
  - B-tree for index storage (arbitrary keys, no data)
  - Cell parsing and serialization (record format with type codes and serial types)
  - Page splitting and merging (maintaining balance)
  - Overflow page chains for large records
  - Cursor abstraction: `BtCursor` with `move_to`, `next`, `previous`, `insert`, `delete`
  - Free-list management (trunk pages and leaf pages)
  - Interior page binary search
  - Support for WITHOUT ROWID tables (index-organized tables)
- `lexer` crate:
  - Hand-written tokenizer producing token stream
  - All SQLite token types: keywords (150+), operators, literals, identifiers
  - String literal handling (single-quoted, with escape sequences)
  - Blob literal handling (X'...')
  - Numeric literal handling (integer, float, hex)
  - Identifier quoting (double-quotes, backticks, square brackets)
  - Comment handling (single-line `--` and multi-line `/* */`)
  - Position tracking for error reporting
- `parser` crate:
  - Recursive descent parser for all SQLite SQL statements
  - SELECT (with joins, subqueries, compound operators UNION/INTERSECT/EXCEPT)
  - INSERT (with DEFAULT VALUES, ON CONFLICT/UPSERT, RETURNING)
  - UPDATE (with FROM clause, RETURNING)
  - DELETE (with RETURNING)
  - CREATE TABLE/INDEX/VIEW/TRIGGER
  - ALTER TABLE (ADD COLUMN, RENAME COLUMN, RENAME TABLE, DROP COLUMN)
  - DROP TABLE/INDEX/VIEW/TRIGGER
  - All expression types: arithmetic, comparison, logical, CASE, CAST, BETWEEN, IN, EXISTS, subquery, function call, window function, collate, aggregate
  - PRAGMA statements
  - ATTACH/DETACH
  - BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE
  - EXPLAIN and EXPLAIN QUERY PLAN
  - CREATE VIRTUAL TABLE
- `ast` crate:
  - Strongly-typed AST nodes for every statement and expression type
  - Visitor pattern for AST traversal and transformation
  - Pretty-printer for AST-to-SQL round-tripping
- `schema` crate:
  - Parsing of `sqlite_master` table entries
  - Schema object representation (Table, Index, View, Trigger)
  - Column definition with type affinity, constraints, default values
  - Index column list with collation and sort order
- Unit tests: parser round-trip tests, B-tree insert/delete/search tests

### Phase 4: VDBE & Query Pipeline

**Duration**: 5 weeks
**Goal**: Implement the VDBE bytecode interpreter, query planner, and public API.

Deliverables:
- `vdbe` crate:
  - Opcode enum with all 190+ opcodes
  - `VdbeOp` struct: opcode, p1, p2, p3, p4, p5
  - Register file: `Vec<Value>` with dynamic typing
  - Value type: Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>)
  - Bytecode compiler from AST to VDBE program
  - Bytecode interpreter main loop (`step()` function)
  - Core opcodes implemented:
    - Control flow: Init, Goto, If, IfNot, Halt, Return, Gosub
    - Comparison: Eq, Ne, Lt, Le, Gt, Ge, Compare, Jump
    - Arithmetic: Add, Subtract, Multiply, Divide, Remainder
    - String: Concat, Length, Substr, Upper, Lower, Trim
    - Column access: Column, Rowid, MakeRecord, ResultRow
    - Cursor operations: OpenRead, OpenWrite, Rewind, Next, Prev, Seek, SeekGE, SeekGT, SeekLE, SeekLT, NotFound, Found, Insert, Delete, IdxInsert, IdxDelete
    - Aggregation: AggStep, AggFinal, AggValue
    - Transaction: Transaction, Commit, Rollback, Savepoint, AutoCommit
    - Sort: SorterOpen, SorterInsert, SorterSort, SorterNext, SorterData
    - Miscellaneous: Null, Integer, String8, Blob, Variable, Move, Copy, SCopy, NotNull, IsNull, Once, Cast, Affinity, Function
  - `EXPLAIN` output formatter
- `planner` crate:
  - Basic query planner (single-table scans, index selection)
  - Join ordering (nested loop joins)
  - WHERE clause analysis and index usability detection
  - ORDER BY optimization (index-satisfying sorts vs. sorter)
  - LIMIT/OFFSET pushdown
  - Subquery flattening (basic cases)
  - Correlated subquery detection
- `func` crate:
  - All core functions: abs, char, coalesce, glob, hex, ifnull, iif, instr, last_insert_rowid, length, like, likelihood, likely, lower, ltrim, max, min, nullif, printf/format, quote, random, randomblob, replace, round, rtrim, sign, soundex, substr/substring, total_changes, trim, typeof, unicode, unlikely, upper, zeroblob
  - All aggregate functions: avg, count, group_concat, max, min, sum, total
  - Date/time functions: date, time, datetime, julianday, strftime, unixepoch, timediff
  - Math functions: acos, asin, atan, atan2, ceil, cos, degrees, exp, floor, ln, log, log2, mod, pi, pow, radians, sin, sqrt, tan, trunc
- `frankensqlite` crate (public API facade):
  - `Connection::open(path)`, `Connection::open_in_memory()`
  - `connection.execute(sql, params)` for statements without results
  - `connection.query(sql, params)` returning row iterator
  - `connection.prepare(sql)` returning prepared statement
  - `Statement::bind()`, `Statement::step()`, `Statement::column_*()`, `Statement::reset()`
  - `Row` type with column access by index and name
  - Parameter binding: positional (`?`), numbered (`?NNN`), named (`:name`, `@name`, `$name`)
  - Transaction API: `connection.transaction()` returning RAII guard
  - Pragma support
  - Backup API
  - Busy handler and busy timeout
- Integration tests: execute queries end-to-end, verify results against expected output

### Phase 5: Persistence, WAL & Transactions

**Duration**: 4 weeks
**Goal**: Implement durable persistence, WAL, and full transaction support.

Deliverables:
- `wal` crate:
  - WAL file format: 32-byte header, 24-byte frame headers, page data
  - WAL writer: append frames atomically
  - WAL reader: read pages from WAL with frame lookup via WAL index
  - WAL index (shared memory hash table): mapping from page number to WAL frame
  - Checkpoint modes: PASSIVE, FULL, RESTART, TRUNCATE
  - WAL checksum verification (big-endian, cumulative)
  - Recovery: rebuild WAL index from WAL file on open
- Rollback journal support (legacy mode):
  - Journal file creation and management
  - Hot journal detection and rollback on open
  - Journal modes: DELETE, TRUNCATE, PERSIST, MEMORY, OFF
- Transaction implementation:
  - BEGIN DEFERRED / IMMEDIATE / EXCLUSIVE
  - COMMIT and ROLLBACK
  - Savepoints: SAVEPOINT name / RELEASE name / ROLLBACK TO name
  - Nested savepoint stack
  - Auto-commit mode
  - Statement journaling for statement-level rollback
- Crash recovery:
  - Hot journal rollback
  - WAL recovery (replay committed frames, discard uncommitted)
  - Integrity checking (PRAGMA integrity_check)
  - Database file lock recovery
- Sync and durability:
  - PRAGMA synchronous = OFF / NORMAL / FULL / EXTRA
  - PRAGMA journal_mode switching
  - Atomic commit protocol (lock escalation: SHARED -> RESERVED -> PENDING -> EXCLUSIVE)
- Stress tests: concurrent readers during checkpoint, crash simulation, power-loss simulation

### Phase 6: MVCC Concurrent Writers

**Duration**: 6 weeks
**Goal**: Implement the key innovation -- MVCC page-level versioning for concurrent writers. This is the most architecturally novel phase and the one that differentiates FrankenSQLite from C SQLite.

Deliverables:
- `mvcc` crate:
  - **Transaction ID allocation**: monotonically increasing 64-bit transaction IDs, atomic counter
  - **Page version chains**: each page maintains a linked list of versions, each tagged with the transaction ID that created it. Readers see the version visible to their snapshot; writers create new versions.
  - **Snapshot management**: when a transaction begins, it records the current transaction ID as its snapshot point. All reads within that transaction see only page versions committed before the snapshot point.
  - **Conflict detection**: at commit time, check whether any page written by this transaction was also written by a transaction that committed after this transaction's snapshot point. If so, abort with SQLITE_BUSY (first-committer-wins).
  - **Eager page locking**: when a transaction first writes to a page, it acquires a lightweight lock on that page number. If the lock is already held by another active writer, the transaction receives SQLITE_BUSY immediately. This prevents deadlocks (no wait-for cycles possible because locks are acquired eagerly, one at a time).
  - **WAL append serialization**: WAL frame appends are serialized via a mutex. This is acceptable because WAL appends are sequential writes and thus cheap. The mutex is held only for the duration of the memcpy + write syscall.
  - **Garbage collection**: a background task periodically scans version chains and removes versions that are no longer visible to any active snapshot. The oldest active snapshot ID is tracked; versions older than this can be collected.
  - **Read scalability**: readers never block and never acquire locks. Unlimited concurrent readers (no `aReadMark[5]` limit from C SQLite's WAL implementation).
  - **Checkpoint integration**: checkpointing now must account for multiple version chains. The checkpoint writes the latest committed version of each dirty page back to the main database file, but only pages whose versions are no longer needed by any active reader.
- Integration with pager:
  - `pager::get_page()` now returns the version visible to the calling transaction's snapshot
  - `pager::write_page()` now creates a new version tagged with the calling transaction's ID
  - Page cache now indexes by (page_number, version) instead of just page_number
- Integration with WAL:
  - WAL frames now carry transaction IDs
  - WAL index maps (page_number, txn_id) -> frame_offset
  - Checkpoint must respect active snapshots
- Deadlock freedom proof:
  - Document the invariant: locks are acquired in page-number order within a transaction, and eager acquisition means no transaction ever waits while holding a lock
  - Property-based tests with arbitrary transaction interleavings
- Performance targets:
  - 100 concurrent writers, each inserting 100 rows into separate tables: no SQLITE_BUSY errors
  - 100 concurrent writers, each inserting into the SAME table: graceful degradation, SQLITE_BUSY on page conflicts, no crashes, no corruption
  - Read throughput unaffected by concurrent writers

### Phase 7: Advanced Query Planner & Full VDBE

**Duration**: 5 weeks
**Goal**: Implement advanced query optimization features and the remaining VDBE opcodes for full SQL coverage.

Deliverables:
- Advanced planner features:
  - Cost-based optimization using table statistics (sqlite_stat1, sqlite_stat4)
  - Multi-index OR optimization (OR-by-union)
  - Covering index detection and index-only scans
  - Automatic index creation for uncorrelated subqueries
  - Skip-scan optimization for leading-column mismatches
  - WHERE clause term decomposition and transitive closure
  - LIKE/GLOB optimization using index prefixes
  - Partial index awareness
  - Expression index support
- Window functions:
  - OVER clause: PARTITION BY, ORDER BY, frame specification (ROWS, RANGE, GROUPS)
  - Built-in window functions: row_number, rank, dense_rank, ntile, lag, lead, first_value, last_value, nth_value
  - Aggregate functions as window functions (sum, avg, count, min, max over windows)
  - Frame types: UNBOUNDED PRECEDING, N PRECEDING, CURRENT ROW, N FOLLOWING, UNBOUNDED FOLLOWING
  - EXCLUDE clause: CURRENT ROW, GROUP, TIES, NO OTHERS
- Common Table Expressions (CTEs):
  - Non-recursive CTEs (WITH clause)
  - Recursive CTEs (WITH RECURSIVE)
  - CTE materialization vs. inlining optimization
  - Multiple CTEs in a single query
- Triggers:
  - BEFORE, AFTER, INSTEAD OF triggers
  - INSERT, UPDATE, DELETE triggers
  - Row-level triggers with OLD and NEW references
  - WHEN clause filtering
  - Trigger recursion (PRAGMA recursive_triggers)
  - Cascading trigger execution
- Views:
  - View definition storage and retrieval
  - View expansion during query planning
  - Updatable views (via INSTEAD OF triggers)
- Remaining VDBE opcodes:
  - Bloom filter opcodes: FilterAdd, Filter
  - Sequence opcodes: Sequence, NewRowid
  - Virtual table opcodes: VOpen, VFilter, VNext, VColumn, VRename, VUpdate
  - Authorization opcodes: before each operation
  - Trace/profile callbacks

### Phase 8: Extensions

**Duration**: 6 weeks
**Goal**: Implement all major SQLite extensions.

Deliverables:
- `fts5` crate (Full-Text Search 5):
  - FTS5 virtual table implementation
  - Tokenizers: unicode61 (default), ascii, porter (stemming), trigram
  - Full-text index structure (b-tree of terms -> document lists)
  - MATCH queries with boolean operators (AND, OR, NOT, NEAR)
  - Column filters in MATCH expressions
  - Prefix queries
  - Ranking functions: bm25 (default), custom ranking via auxiliary functions
  - highlight() and snippet() auxiliary functions
  - FTS5 content tables (external content, contentless)
  - Incremental merge and optimization
  - FTS5 vocab virtual table
- `fts3` crate (Full-Text Search 3/4):
  - FTS3 and FTS4 virtual table implementations
  - Tokenizers: simple, porter, unicode61, icu
  - matchinfo(), offsets(), snippet() functions
  - Enhanced query syntax (FTS4)
  - Compress/uncompress hooks (FTS4)
  - Languageid support (FTS4)
- `rtree` crate (R-Tree Spatial Index):
  - R-tree virtual table for N-dimensional rectangles
  - Range queries (overlap and containment)
  - Custom R-tree queries via callback geometry
  - GeoJSON integration
  - R*tree variant with improved splitting
- `json` crate (JSON1):
  - json(value) - validate and minify
  - json_array(), json_object() - construction
  - json_extract() / -> / ->> operators - extraction
  - json_insert(), json_replace(), json_set(), json_remove() - modification
  - json_type(), json_valid() - introspection
  - json_each(), json_tree() - table-valued functions
  - json_group_array(), json_group_object() - aggregation
  - json_patch() - RFC 7396 merge patch
  - JSONB binary format support
- `session` crate (Session Extension):
  - Change tracking (INSERT, UPDATE, DELETE)
  - Changeset generation and application
  - Patchset generation (more compact, no original values for UPDATE)
  - Conflict resolution callbacks
  - Changeset inversion
  - Changeset concatenation
  - Rebasing (for collaborative editing)
  - Changeset iteration and filtering
- `icu` crate (ICU Extension):
  - Unicode-aware LIKE operator
  - ICU collation sequences
  - Unicode case folding (upper/lower)
  - ICU regex support
  - Integration with system ICU library or bundled ICU data
- Miscellaneous extensions:
  - generate_series(start, stop, step) table-valued function
  - csv virtual table (read CSV files as tables)
  - fileio functions (readfile, writefile, lsdir)
  - completion(prefix, wholeline) for shell tab-completion
  - dbstat virtual table (page-level storage analysis)
  - stmt virtual table (prepared statement introspection)
  - sha1/sha256/sha3 functions
  - decimal extension (arbitrary-precision decimal arithmetic)
  - ieee754 extension (floating-point introspection)
  - uuid extension (UUID generation and formatting)

### Phase 9: CLI Shell & Conformance

**Duration**: 4 weeks
**Goal**: Build the interactive CLI shell and achieve full conformance with C SQLite.

Deliverables:
- `shell` crate:
  - Interactive SQL prompt with readline-style editing via frankentui
  - Multi-line statement detection (continue until `;`)
  - Dot-commands:
    - `.backup`, `.bail`, `.cd`, `.changes`, `.check`, `.clone`
    - `.databases`, `.dbconfig`, `.dbinfo`, `.dump`, `.echo`
    - `.eqp`, `.excel`, `.exit`/`.quit`, `.expert`, `.explain`
    - `.filectrl`, `.fullschema`, `.headers`, `.help`
    - `.import`, `.indexes`, `.limit`, `.lint`, `.load`, `.log`
    - `.mode` (ascii, box, column, csv, html, insert, json, line, list, markdown, quote, table, tabs, tcl)
    - `.nullvalue`, `.once`, `.open`, `.output`, `.parameter`
    - `.print`, `.progress`, `.prompt`, `.read`, `.recover`
    - `.restore`, `.save`, `.scanstats`, `.schema`, `.selftest`
    - `.separator`, `.sha3sum`, `.shell`, `.show`, `.stats`
    - `.tables`, `.testcase`, `.timeout`, `.timer`, `.trace`
    - `.vfsinfo`, `.vfslist`, `.vfsname`, `.width`
  - Output formatting for all 14 output modes
  - SQL syntax highlighting in the prompt
  - History file (~/.frankensqlite_history)
  - Tab completion for table names, column names, SQL keywords, and dot-commands
  - Init file (~/.frankensqliterc)
  - Batch mode (read SQL from stdin or file)
  - Signal handling (Ctrl-C to cancel query, Ctrl-D to exit)
- Conformance test suite:
  - SQL Logic Tests (SLT) runner compatible with the sqllogictest format
  - Tests ported from SQLite's test suite covering:
    - Basic CRUD operations
    - All join types (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
    - Subqueries (scalar, table, EXISTS, IN)
    - Aggregate queries with GROUP BY and HAVING
    - Window functions
    - CTEs (recursive and non-recursive)
    - UPSERT (INSERT ON CONFLICT)
    - RETURNING clause
    - All data types and type affinity
    - NULL handling
    - Collation sequences (BINARY, NOCASE, RTRIM)
    - CAST expressions
    - Date/time functions
    - JSON functions
    - Full-text search
    - R-tree queries
    - Foreign keys
    - Triggers (BEFORE, AFTER, INSTEAD OF)
    - Views
    - ALTER TABLE
    - VACUUM
    - REINDEX
    - ANALYZE
    - ATTACH/DETACH
    - Savepoints
    - WAL mode operations
    - Concurrent reader/writer scenarios
  - Target: 95%+ compatibility with C SQLite behavior
  - Known incompatibilities documented with rationale
- Benchmarks:
  - Micro-benchmarks:
    - Single-row INSERT throughput (rows/sec)
    - Point SELECT by rowid (queries/sec)
    - Range SELECT with index (rows/sec)
    - Aggregate queries (sum, count, avg over 1M rows)
    - FTS5 search throughput (queries/sec)
  - Macro-benchmarks:
    - TPC-C-like workload adapted for SQLite
    - Mixed read/write workload with configurable ratios
    - Large import (1M+ rows via INSERT or .import)
    - Large export (.dump, CSV export)
  - MVCC-specific benchmarks:
    - Concurrent writers scaling (1, 2, 4, 8, 16, 32, 64, 128 writers)
    - Reader throughput under write load
    - MVCC overhead vs. single-writer mode
    - Garbage collection impact on latency
  - Comparison benchmarks against C SQLite 3.52.0

## Key Design Decisions

### MVCC at Page Granularity (Not Row or Table)

Page-level MVCC was chosen as the sweet spot between complexity and concurrency:

- **Row-level MVCC** (as in PostgreSQL) would require fundamental changes to the record format, a visibility map, and per-row transaction metadata. This would break file-format compatibility and add significant space overhead. It would also require vacuum/MVCC cleanup analogous to PostgreSQL's VACUUM, which is a source of operational complexity.
- **Table-level MVCC** would be too coarse. Two writers inserting into different parts of the same table would conflict unnecessarily. Since SQLite is often used with just a few tables, table-level locking would provide little benefit over the existing single-writer model.
- **Page-level MVCC** maps naturally to SQLite's B-tree structure. Each page is the unit of I/O and caching, so versioning at this level adds no new abstraction. Writers to different parts of the same B-tree (different leaf pages) can proceed in parallel. Only when two writers modify the same leaf page (or the same interior page due to a split) do they conflict.

### Snapshot Isolation with First-Committer-Wins

When two transactions conflict (they both wrote to the same page), the first to commit succeeds and the second is aborted with SQLITE_BUSY. This is simpler than first-updater-wins (which requires tracking read sets) and avoids the complexity of optimistic validation. Applications already handle SQLITE_BUSY by retrying, so this fits the existing SQLite programming model.

### Eager Page Locking (No Deadlocks Possible)

When a transaction first writes to a page, it immediately attempts to acquire an exclusive lock on that page number. If the lock is held by another active writer, SQLITE_BUSY is returned immediately (no waiting). This design makes deadlocks impossible by construction:

- A transaction never waits while holding a page lock
- Therefore, there can be no wait-for cycle
- Therefore, there is no deadlock

This is a deliberately simple design. Waiting with timeouts or deadlock detection would add complexity for marginal throughput gains. In practice, page conflicts in SQLite workloads are rare because different writers typically touch different leaf pages.

### WAL Append Serialized via Mutex (Cheap Sequential Writes)

The WAL file is append-only during normal operation. Appending frames to the WAL is serialized via a single mutex. This might sound like a bottleneck, but it is not in practice because:

- WAL appends are sequential writes, which are very fast on any storage medium
- The critical section is tiny: write frame header (24 bytes) + page data (typically 4096 bytes) + update WAL index
- The mutex is uncontended most of the time because the actual transaction work (B-tree traversal, record serialization, conflict detection) happens outside the critical section

This is vastly simpler than a lock-free WAL append protocol and provides sufficient throughput for SQLite's target workloads.

### Unlimited Concurrent Readers (No aReadMark Limit)

C SQLite's WAL implementation uses a fixed-size array of 5 read marks (`aReadMark[WAL_NREADER]`) in shared memory. This limits concurrent readers to 5 (or more precisely, to 5 distinct snapshot points). FrankenSQLite replaces this with a dynamically-sized reader registry, allowing unlimited concurrent readers. Each reader registers its snapshot transaction ID; the oldest active snapshot ID is used to determine which WAL frames and page versions can be safely cleaned up.

## Dependencies

### First-Party Dependencies

- **asupersync**: Async runtime, channels, synchronization primitives, timers. Provides `spawn`, `sleep`, `Mutex`, `RwLock`, `Condvar`, `channel`, `select`. Used throughout for all async operations and inter-thread coordination.
- **frankentui**: Terminal UI library for the CLI shell. Provides terminal raw mode, input event handling, line editor with history, syntax highlighting, and styled text rendering.

### Third-Party Dependencies

- **thiserror** (1.x): Derive macro for `std::error::Error` implementations. Used in every crate for ergonomic error type definitions.
- **serde** (1.x) + **serde_json**: Serialization framework. Used for JSON extension, configuration files, and test fixture serialization.
- **bitflags** (2.x): Macro for defining bitflag types. Used for page flags, column flags, index flags, and various option sets throughout the codebase.
- **parking_lot** (0.12.x): Fast, compact mutex and rwlock implementations. Used for internal synchronization where async is not needed (page cache locks, schema cache, etc.).
- **sha2** (0.10.x): SHA-256 implementation for `.sha3sum` command and integrity verification.
- **memchr** (2.x): Optimized byte search. Used in the lexer for fast scanning, in FTS tokenizers, and in various string processing operations.
- **smallvec** (1.x): Stack-allocated small vectors. Used for VDBE register windows, short column lists, and other small collections that rarely exceed a fixed size, avoiding heap allocation in the common case.

### Development Dependencies

- **criterion**: Benchmarking framework for micro and macro benchmarks.
- **tempfile**: Temporary file and directory creation for tests.
- **proptest**: Property-based testing for B-tree invariants, MVCC conflict detection, parser round-trips, and transaction interleaving.
- **tracing** + **tracing-subscriber**: Structured logging for debugging and performance analysis during development.

## Verification

### Continuous Integration

Every pull request and every commit to the main branch triggers the full CI pipeline:

- `cargo fmt --all --check` -- enforce consistent formatting
- `cargo clippy --workspace --all-targets -- -D warnings` -- zero warnings policy
- `cargo check --workspace` -- type checking across all crates
- `cargo test --workspace` -- unit and integration tests
- `cargo test --workspace --release` -- release-mode tests (catches UB from optimizations)
- `cargo doc --workspace --no-deps` -- documentation builds without errors

### Conformance Suite

The conformance test suite is the primary quality gate. It runs every SQL statement from the test corpus against both FrankenSQLite and C SQLite 3.52.0, comparing results:

- Exact result set matching (order-sensitive for ORDER BY queries, order-insensitive otherwise)
- Error code matching (same error for same malformed input)
- Type affinity matching (same types returned for same expressions)
- NULL handling matching
- Floating-point comparison with epsilon tolerance where appropriate
- Target: 95%+ compatibility, with every known incompatibility documented and justified

### Stress Tests

- **100 threads x 100 writes**: 100 threads each perform 100 INSERT operations concurrently. Verify no data corruption, no lost writes, correct row counts, and that MVCC conflict resolution works correctly under load.
- **Reader-writer mix**: 50 reader threads performing continuous SELECTs while 50 writer threads perform continuous INSERTs/UPDATEs. Verify that readers always see a consistent snapshot and that no reader ever sees partial transaction state.
- **Checkpoint under load**: Continuous writes while periodic checkpoints occur. Verify that checkpointing does not corrupt the database and that readers are not blocked.
- **Long-running transactions**: One reader holds a snapshot open for an extended period while many writers commit. Verify that the old snapshot remains readable and that garbage collection correctly preserves needed versions.

### Crash Recovery Verification

- **Power-loss simulation**: Use `fallocate` and `ftruncate` to simulate incomplete writes at every possible byte boundary during WAL append and checkpoint operations. Verify that the database recovers correctly and no committed data is lost.
- **Kill-9 testing**: Run a write workload, send SIGKILL at random points, restart and verify database integrity with `PRAGMA integrity_check`.
- **Bit-flip testing**: Introduce random bit flips in the WAL and database files, verify that FrankenSQLite detects the corruption (via checksums) and reports it rather than silently returning wrong results.

### Performance Regression Testing

- Benchmark suite runs nightly on dedicated hardware
- Results compared against previous runs with 5% regression threshold
- Key metrics tracked: INSERT throughput, SELECT throughput, concurrent writer scaling, WAL checkpoint latency, FTS5 query latency
- Comparison against C SQLite 3.52.0 on the same hardware and workloads
