# Plan to Port SQLite to Rust

## 1. Scope

### What We Are Building

FrankenSQLite is a **clean-room Rust reimplementation** of SQLite version 3.52.0.
The reference C codebase is approximately 218,000 lines of C (core library, not
counting test code or generated amalgamation). FrankenSQLite targets full SQL
compatibility, file format round-trip interoperability with C SQLite, and a
strictly safe Rust implementation (`unsafe_code = "forbid"` at workspace level).

The project is organized as a Cargo workspace of **23 crates** under the
`crates/` directory, following the same layout conventions used in the
`frankentui` project.

### The Key Innovation: MVCC Concurrent Writers

SQLite's single biggest architectural limitation is its write concurrency model.
In WAL mode, a single exclusive lock byte (`WAL_WRITE_LOCK` at `wal.c:3698`)
serializes all writers. Any connection attempting to write while another holds
this lock receives `SQLITE_BUSY` and must retry.

FrankenSQLite replaces this with **MVCC page-level versioning**:

- **Multiple concurrent writers.** Two transactions modifying different B-tree
  pages proceed in full parallel with zero contention.
- **Snapshot-based reads + SSI by default.** Each transaction captures a
  consistent point-in-time view at BEGIN. `BEGIN CONCURRENT` targets
  SERIALIZABLE behavior via Serializable Snapshot Isolation (SSI), not merely
  plain Snapshot Isolation (SI). A PRAGMA may explicitly downgrade to SI for
  benchmarking or applications that tolerate write skew.
- **Page-level conflict detection (FCW + safe merge ladder).** When two
  transactions modify the same page, first-committer-wins (FCW) detects base
  drift at commit. If configured, commuting same-page conflicts may be resolved
  by the safe write-merge ladder (deterministic rebase + structured page
  patches); otherwise the loser retries with `SQLITE_BUSY_SNAPSHOT`. Deadlocks
  are impossible by construction (eager page locking, no wait-for cycles).
- **Unlimited concurrent readers.** No `aReadMark[5]` limit. Any number of
  readers can hold independent snapshots simultaneously.
- **Serialized commit ordering, not serialized I/O.** Writers perform bulk
  persistence work in parallel; only a tiny ordering step (FCW + commit_seq +
  commit marker append) is serialized.

This design is similar in spirit to PostgreSQL's MVCC, but operates at page
granularity rather than tuple granularity, matching the B-tree page-oriented
storage architecture.

### Why Page-Level Granularity

Page-level MVCC was chosen as the sweet spot between complexity and concurrency:

- **Row-level MVCC** (as in PostgreSQL) would require fundamental changes to the
  record format, a visibility map, and per-row transaction metadata. This would
  break file-format compatibility and add significant space overhead.
- **Table-level MVCC** would be too coarse. Two writers inserting into different
  parts of the same table would conflict unnecessarily. Since SQLite is often
  used with just a few tables, table-level locking would provide little benefit
  over the existing single-writer model.
- **Page-level MVCC** maps naturally to SQLite's B-tree structure. Each page is
  the unit of I/O and caching, so versioning at this level adds no new
  abstraction. Writers to different parts of the same B-tree (different leaf
  pages) can proceed in parallel.

### Concurrency Scope (Explicit)

- **In-process (threads):** In scope. This is the minimum bar.
- **Multi-process (same DB file):** In scope. Cross-process coordination uses
  shared-memory state + OS file locks. The project MUST not assume
  single-process ownership of a database file.

### Target Behavior

- Read and write standard SQLite database files (format compatible with C SQLite)
- Support the full SQL dialect recognized by SQLite 3.52.0
- Implement all major extensions (FTS3/4/5, R-tree, JSON1, Session, ICU, misc)
- Provide a drop-in interactive CLI shell replacement
- **100% behavioral parity target** against a golden-file test suite derived
  from C SQLite (for the supported surface). Any intentional divergence MUST be
  explicitly documented and annotated in the harness with rationale.

---

## 2. Exclusions

The following components of the C SQLite codebase are explicitly **not ported**:

### Amalgamation Build System

SQLite's distribution model relies on a generated amalgamation (`sqlite3.c` +
`sqlite3.h`) produced by a custom TCL build script. FrankenSQLite uses a
standard Cargo workspace with 23 crates and has no need for source
concatenation.

### TCL Test Harness

The C SQLite test suite is written in TCL (~90,000+ lines of test scripts).
FrankenSQLite replaces this with:
- Native Rust `#[test]` functions in each crate
- Property-based tests via `proptest`
- Snapshot tests via `insta`
- Fuzz tests via `cargo-fuzz`
- A dedicated conformance harness (`fsqlite-harness`) that compares output
  against C SQLite golden files

### LEMON Parser Generator

SQLite uses a custom LALR parser generator called LEMON to produce a push-down
automaton from the `parse.y` grammar (1,963 lines). FrankenSQLite instead uses
a **hand-written recursive descent parser** with Pratt precedence for
expressions. This provides better error messages, simpler maintenance, and
avoids the need for a code generation step.

### Loadable Extension API (.so/.dll Loading)

SQLite supports runtime-loaded shared library extensions via
`sqlite3_load_extension()`. FrankenSQLite compiles all extensions directly into
their respective crates and links them at build time. There is no dynamic
loading mechanism. Extensions are enabled or disabled via Cargo feature flags.

### Legacy File Format Quirks

The SQLite file format has accumulated backward-compatibility quirks from the
pre-3.0 era (2004). FrankenSQLite targets schema format number 4 (the current
format) and does not implement support for:
- Schema format numbers 1, 2, or 3
- Legacy typeAffinity mappings from SQLite 2.x
- DESC index encoding variations from early format versions

### Obsolete VFS Implementations

The C SQLite source includes VFS implementations for operating systems that
are no longer relevant:
- **OS/2** (`os_os2.c`) -- IBM OS/2, discontinued
- **VxWorks** -- real-time OS specific paths in `os_unix.c`
- **Windows CE** (`os_win.c` conditional sections) -- discontinued platform

FrankenSQLite implements:
- `UnixVfs` -- POSIX file I/O via `asupersync::BlockingPool`
- `MemoryVfs` -- in-memory storage for testing and ephemeral databases

Windows VFS support may be added in the future as a separate crate but is not
in scope for initial delivery.

### Shared-Cache Mode

SQLite's shared-cache mode (`SQLITE_OPEN_SHAREDCACHE`) allows multiple
connections within the same process to share a single page cache and B-tree
instance, using table-level locking for concurrency. This mode is deprecated
in modern SQLite and is architecturally superseded by FrankenSQLite's MVCC
design, which provides superior concurrency without the complexity and
limitations of shared-cache table locks.

---

## 3. Phases

### Phase 1: Bootstrap and Spec Extraction

**Goal:** Create the workspace scaffold, write specification documents, and
establish the project infrastructure.

**Work items:**
- Create workspace `Cargo.toml` with `resolver = "2"`, `edition = "2024"`,
  `unsafe_code = "forbid"`, and `clippy::pedantic = "deny"` +
  `clippy::nursery = "deny"`
- Create `rust-toolchain.toml` (nightly channel)
- Scaffold all 23 crate directories with stub `Cargo.toml` and `src/lib.rs`
- Write `EXISTING_SQLITE_STRUCTURE.md` (complete behavior extraction from C
  source: all data structures from `sqliteInt.h`, SQL grammar from `parse.y`,
  all 190+ VDBE opcodes from `vdbe.c`, B-tree page format, WAL frame format,
  database file header, all PRAGMA behaviors, all built-in functions, extension
  APIs)
- Write `PROPOSED_ARCHITECTURE.md` (Rust design: MVCC detail, trait hierarchy,
  crate dependency graph, async integration patterns, query pipeline, file
  format compatibility, testing strategy)
- Write `PLAN_TO_PORT_SQLITE_TO_RUST.md` (this document)
- Update `AGENTS.md` for the FrankenSQLite project
- Scaffold `crates/fsqlite-harness/` with:
  - an Oracle runner (C sqlite3) that can execute SQL and emit JSON fixtures
  - a minimal golden comparison loop (rows, error codes, and side effects)
- Establish perf regression discipline from day 1:
  - define baseline artifact layout (criterion/hyperfine/profiles)
  - capture at least one tiny baseline run (so "perf later" never happens)

**Acceptance criteria:**
- `cargo check --workspace` completes with zero errors
- `cargo clippy --workspace --all-targets -- -D warnings` produces zero warnings
- `cargo fmt --all -- --check` passes
- All three specification documents are complete and reviewed
- Oracle runner can execute at least 10 fixtures against C sqlite3 and emit
  self-describing JSON fixtures (golden inputs/outputs)
- First perf baseline artifact captured (even if trivial)

---

### Phase 2: MVCC-Shaped Storage Foundation

**Goal:** Implement the foundational type system and the lowest layers of the
storage stack (VFS + pager), but with MVCC/SSI requirements baked into the API
from day 1 (snapshot/txn-aware page access; policy-pluggable cache). This phase
is explicitly designed to avoid a "build then refactor" trap.

**Work items:**
- `fsqlite-types`: `PageNumber(u32)`, `PageSize`, `PageData`, `SqliteValue`
  enum (Null, Integer(i64), Float(f64), Text(String), Blob(Vec<u8>)), all 190+
  VDBE opcodes as an enum, `SQLITE_MAX_*` limit constants, serial type
  encoding, bitflag types (OpenFlags, SyncFlags, LockLevel)
- `fsqlite-error`: `FrankenError` enum using `thiserror`, error codes matching
  SQLite result codes, `StructuredError` with source location
- `fsqlite-vfs`: `Vfs` and `VfsFile` traits, `MemoryVfs` implementation,
  `UnixVfs` implementation with `asupersync` blocking I/O
- `fsqlite-pager`: Pager state machine with **snapshot/txn-aware** APIs (e.g.
  `get_page(pgno, snapshot)` and `write_page(pgno, txn)`), dirty page tracking,
  and a cache policy plug-in boundary (LRU/ARC/TinyLFU are implementations, not
  assumptions)
- `fsqlite-mvcc` (API-first): define the core types/predicates that shape pager
  and WAL design (`TxnId`, `Snapshot`, visibility predicate, page-lock table
  interfaces, and commit pipeline channel types). The goal here is correctness
  of *interfaces* and invariants, not full concurrency throughput yet.

**Acceptance criteria:**
- `cargo check --workspace` clean
- `cargo clippy -- -D warnings` clean
- Memory VFS stores and retrieves pages correctly
- Pager API makes snapshot/txn context explicit; cache policy is pluggable
- Conformance harness can execute basic fixtures against FrankenSQLite (even if
  the SQL surface is still small) and compare outputs against Oracle fixtures
- 200+ unit tests passing

---

### Phase 3: B-Tree and SQL Parser

**Goal:** Implement the B-tree storage engine and the SQL parsing pipeline.

**B-Tree work (`fsqlite-btree`):**
Port from `btree.c` (11,568 lines) and `btreeInt.h`:
- `cursor.rs` -- BtCursor with page stack traversal (max depth 20)
- `cell.rs` -- Cell format parsing (INTKEY tables, BLOBKEY indexes)
- `balance.rs` -- Page splitting (balance_nonroot, balance_deeper)
- `overflow.rs` -- Overflow page chains
- `free_list.rs` -- Free page management (trunk + leaf pages)
- `payload.rs` -- BtreePayload read/write with overflow handling

**SQL Parser work (`fsqlite-parser` + `fsqlite-ast`):**
Hand-written recursive descent replacing LEMON-generated parser:
- `lexer.rs` -- Tokenizer (from `tokenize.c`, 899 lines) with
  `memchr`-accelerated scanning, zero-copy token spans
- `parser.rs` -- Main parser with Pratt precedence for expressions
- `keyword.rs` -- Perfect hash keyword lookup (150+ SQL keywords)
- AST types for all SQL statements (SELECT, INSERT, UPDATE, DELETE, CREATE,
  DROP, ALTER, PRAGMA, ATTACH/DETACH, BEGIN/COMMIT/ROLLBACK, EXPLAIN, etc.)

**Acceptance criteria:**
- B-tree integrity check passes after 10,000 random inserts
- Parser accepts the full SQLite SQL grammar required by the conformance corpus
  (any remaining gaps are explicitly tracked and driven to zero)
- 500+ tests passing

---

### Phase 4: VDBE and Query Pipeline

**Goal:** Implement the bytecode virtual machine, query planner, code generator,
and public API to enable end-to-end SQL execution.

**VDBE work (`fsqlite-vdbe`):**
Port from `vdbe.c` (9,316 lines):
- Core fetch-execute loop with match-based opcode dispatch
- 50+ critical opcodes first: Transaction, OpenRead, OpenWrite, Seek*, Column,
  Insert, Delete, Goto, If, ResultRow, MakeRecord, Halt, Function
- `mem.rs` -- Mem/sqlite3_value with type affinity conversions
- `sort.rs` -- External merge sort for ORDER BY

**Query Planner (`fsqlite-planner`):**
Port from `resolve.c`, `where.c`, `wherecode.c`, `whereexpr.c`:
- Name resolution (table and column binding)
- Index selection and cost estimation
- Basic join ordering (greedy heuristic)

**Core Assembly (`fsqlite-core`):**
- `connection.rs` -- sqlite3 struct equivalent
- `prepare.rs` -- SQL text -> parsed AST -> VDBE bytecode
- `schema.rs` -- Schema cache management
- SELECT/INSERT/UPDATE/DELETE code generation

**Public API (`fsqlite`):**
- `Connection::open`, `Connection::open_in_memory`
- `Connection::execute`, `Connection::prepare`
- `Statement::query_map`, `Statement::query_row`
- `Row::get::<T>(index)` with `FromSql` trait
- `Transaction` with commit/rollback

**Acceptance criteria:**
- End-to-end execution: CREATE TABLE, INSERT, SELECT with WHERE/ORDER BY/LIMIT
- 1,000+ tests passing

---

### Phase 5: WAL + Transactions + MVCC Concurrent Writers (SSI)

**Goal:** Implement durable storage with file-backed persistence, WAL mode, and
full transaction semantics, while enabling concurrent writers via page-level
MVCC. MVCC is not a "bolt-on": Phase 2 already shaped the pager/WAL interfaces;
this phase fills in the full implementation and hard correctness gates.

**Persistence (Compatibility mode first):**
- Unix VFS with POSIX file locking (fcntl advisory locks)
- Rollback journal modes (DELETE, TRUNCATE, PERSIST, MEMORY, OFF) as needed for
  SQLite parity
- Database file format round-trip (read/write standard `.sqlite` files)

**WAL (`fsqlite-wal`):**
- WAL frame append + read-back (canonical header/frame format)
- WAL index (page-to-frame-offset hash mapping via shared memory / `-shm` file)
- Checkpoint modes: PASSIVE, FULL, RESTART, TRUNCATE
- Crash recovery (scan WAL, replay committed frames, discard uncommitted)

**Transactions:**
- BEGIN DEFERRED / IMMEDIATE / EXCLUSIVE
- COMMIT / ROLLBACK and auto-commit semantics
- Savepoints: SAVEPOINT / RELEASE / ROLLBACK TO
- PRAGMAs: journal_mode, synchronous, cache_size

**MVCC + SSI (`fsqlite-mvcc`):**
- Page-level version chains (copy-on-write per page) with bounded GC horizon
- Page-level lock table (eager, non-blocking acquisition; no wait-for cycles)
- First-Committer-Wins validation and safe write-merge ladder for commuting
  conflicts (deterministic rebase + structured page patches; raw XOR forbidden
  for SQLite structured pages)
- Serializable Snapshot Isolation (SSI) by default for `BEGIN CONCURRENT`
  (explicit PRAGMA to downgrade to SI)
- WriteCoordinator commit pipeline (two-phase MPSC reserve/commit) with
  serialized **ordering only** (bulk I/O happens off the critical section)

**Asupersync integration (non-negotiable):**
- All file I/O via asupersync (no Tokio)
- `Cx` for cooperative cancellation and deadlines
- Two-phase channels for commit publication and bounded backpressure

**Acceptance criteria:**
- Create database, close, reopen, read back data correctly
- File format round-trip with C SQLite (create with one, read with the other)
- Crash recovery: kill mid-transaction, verify database consistency on reopen
- Concurrent readers + writers: long-running reader sees consistent snapshot
- Two concurrent writers touching disjoint pages both commit (no blocking)
- Same-page conflict: loser retries with correct error (or succeeds only via
  safe merge ladder when provably commuting)
- SSI: write-skew patterns abort under default serializable mode
- GC keeps memory bounded under sustained write load
- Conformance harness runs continuously through this phase (no end-game deferral)
- Perf baselines captured for MVCC hot paths (resolve, lock table, commit path)
- 2,000+ tests passing

---

### Phase 6: Advanced Query Planner and Full VDBE

**Goal:** Complete the query optimizer and implement all remaining VDBE opcodes
and SQL features.

**Work items:**
- Complete WHERE clause optimization (index selection, range scans, OR
  optimization, skip-scan, LIKE/GLOB prefix optimization)
- Join ordering (nested loop join, hash join for large tables)
- Subquery flattening and correlated subqueries
- All 190+ VDBE opcodes (including Bloom filter, virtual table, authorization)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE,
  FIRST_VALUE, LAST_VALUE, NTH_VALUE; frame types ROWS/RANGE/GROUPS)
- Common table expressions (WITH, WITH RECURSIVE)
- ANALYZE statistics (sqlite_stat1, sqlite_stat4)
- Foreign key enforcement
- Triggers (BEFORE, AFTER, INSTEAD OF; INSERT, UPDATE, DELETE; recursive)
- Views (definition, expansion, updatable via INSTEAD OF triggers)
- ALTER TABLE (RENAME TABLE, RENAME COLUMN, ADD COLUMN, DROP COLUMN)
- VACUUM and REINDEX

**Acceptance criteria:**
- TPC-B benchmark within 3x of C SQLite performance
- EXPLAIN output matches C SQLite for common query patterns
- 3,000+ tests passing

---

### Phase 7: Extensions

**Goal:** Implement all major SQLite extensions, each in its own feature-gated
crate.

| Extension | Crate | C Source Reference | Key Components |
|-----------|-------|--------------------|----------------|
| JSON1 | `fsqlite-ext-json` | `ext/misc/json.c` (171KB) | `json()`, `json_extract()`, `json_set()`, `json_remove()`, `json_each` vtable, `json_tree` vtable, `json_group_array()`, `json_group_object()`, `json_patch()`, JSONB binary format |
| FTS5 | `fsqlite-ext-fts5` | `ext/fts5/` (29K LOC) | Tokenizers (unicode61, ascii, porter, trigram), inverted index, BM25 ranking, `highlight()`, `snippet()`, content/contentless modes, vocab vtable |
| FTS3/4 | `fsqlite-ext-fts3` | `ext/fts3/` (21K LOC) | Compatibility layer, `matchinfo()`, `offsets()`, `snippet()`, enhanced query syntax (FTS4), compress/uncompress, languageid |
| R-Tree | `fsqlite-ext-rtree` | `ext/rtree/` (7K LOC) | Spatial index, range queries, geopoly functions, custom geometry callbacks |
| Session | `fsqlite-ext-session` | `ext/session/` (13K LOC) | Change tracking, changeset/patchset generation and application, conflict resolution, inversion, concatenation, rebasing |
| ICU | `fsqlite-ext-icu` | `ext/icu/` | Unicode-aware collation, case folding, FTS tokenizer |
| Misc | `fsqlite-ext-misc` | `ext/misc/` | `generate_series`, `carray`, `dbstat`, `dbpage`, `completion`, `csv`, decimal, ieee754, uuid |

**Acceptance criteria:**
- FTS5 queries return correct ranked results
- JSON functions match C SQLite output for all documented behaviors
- Each extension independently enableable via Cargo feature flags
- 3,500+ tests passing

---

### Phase 8: CLI Shell, Conformance, Benchmarks

**Goal:** Deliver a production-quality interactive CLI and achieve verified
conformance against C SQLite.

**CLI (`fsqlite-cli`):**
Interactive shell built with `frankentui`:
- Dot-commands: `.tables`, `.schema`, `.mode`, `.import`, `.dump`, `.headers`,
  `.explain`, `.backup`, `.databases`, `.dbinfo`, `.indexes`, `.fullschema`,
  `.stats`, `.timer`, `.width`, `.separator`, `.nullvalue`, `.output`, `.once`,
  `.read`, `.open`, `.save`, `.restore`, `.quit`
- Output modes: ascii, box, column, csv, html, insert, json, line, list,
  markdown, quote, table, tabs
- Tab completion for table names, column names, SQL keywords, and dot-commands
- SQL syntax highlighting
- Command history with persistence (`~/.frankensqlite_history`)
- Init file support (`~/.frankensqliterc`)
- Batch mode (read SQL from stdin or file)
- Signal handling (Ctrl-C to cancel query, Ctrl-D to exit)

**Conformance suite (`fsqlite-harness`):**
- Execute identical SQL against both FrankenSQLite and C sqlite3
- Compare output row-by-row (order-sensitive for ORDER BY, order-insensitive
  otherwise)
- Error code matching (same error for same malformed input)
- Type affinity and NULL handling verification
- Golden file storage in `conformance/golden/`
- Target: **100% behavioral parity target** across 1,000+ test SQL files (for
  the supported surface)
- Any intentional divergence explicitly documented and annotated with rationale

**Benchmarks:**
- `benches/btree_insert.rs` -- insertion throughput
- `benches/btree_lookup.rs` -- point lookup latency
- `benches/parser_throughput.rs` -- statements parsed per second
- `benches/mvcc_contention.rs` -- concurrent writer throughput (1-128 writers)
- `benches/tpc_b.rs` -- TPC-B-like mixed workload
- `benches/concurrent_rw.rs` -- mixed read/write with varying ratios
- `benches/wal_checkpoint.rs` -- checkpoint speed under load

**Acceptance criteria:**
- CLI usable as a drop-in sqlite3 replacement for interactive use
- 100% parity target against golden file suite (with any intentional divergences documented + annotated)
- Documented benchmark results with comparison to C SQLite
- 4,000+ tests passing across the entire workspace

---

### Verification Gate (All Phases)

Every phase must pass this gate before proceeding:

1. `cargo check --workspace` -- zero errors
2. `cargo clippy --workspace --all-targets -- -D warnings` -- zero warnings
3. `cargo fmt --all -- --check` -- correctly formatted
4. `cargo test --workspace` -- all tests pass
5. `cargo bench --workspace` -- no performance regressions (Phase 4+)

Additional verification for Phase 5+ (MVCC-enabled):

6. Stress test: 100 threads x 100 writes -- all rows present, no corruption
7. Long-running reader + concurrent writer: snapshot consistency verified
8. Kill-and-recover: data integrity after forced process termination
9. Memory growth under sustained MVCC load: GC keeps version count bounded

---

## 4. Crate Map

All 23 crates, their roles, and their dependency relationships:

| # | Crate | Role | Internal Dependencies | External Dependencies |
|---|-------|------|-----------------------|-----------------------|
| 1 | `fsqlite-types` | Shared type definitions: PageNumber, SqliteValue, TxnId, Opcode enum, SQLITE_MAX_* limits, serial types, bitflags | (none -- leaf crate) | (none) |
| 2 | `fsqlite-error` | Error types: FrankenError enum, ErrorCode, StructuredError with source spans, Result alias | (none -- leaf crate) | `thiserror` |
| 3 | `fsqlite-ast` | Abstract syntax tree node types: Statement, Expr, SelectCore, JoinClause, DDL, DML, visitor pattern, pretty-printer | `fsqlite-types` | (none) |
| 4 | `fsqlite-vfs` | Virtual filesystem abstraction: Vfs + VfsFile traits, UnixVfs (asupersync I/O), MemoryVfs | `fsqlite-types`, `fsqlite-error` | (none) |
| 5 | `fsqlite-pager` | Page cache and I/O layer: pager state machine, dirty page tracking, LRU eviction, journal integration | `fsqlite-types`, `fsqlite-error`, `fsqlite-vfs` | (none) |
| 6 | `fsqlite-wal` | Write-ahead log: WAL frame I/O, WAL index hash table, checkpoint (PASSIVE/FULL/RESTART/TRUNCATE), crash recovery | `fsqlite-types`, `fsqlite-error`, `fsqlite-vfs`, `fsqlite-pager` | (none) |
| 7 | `fsqlite-mvcc` | MVCC concurrency control: page version chains, TxnId allocation, snapshot isolation, page lock table, conflict detection, garbage collection, MVCC-aware WAL, buffer pool | `fsqlite-types`, `fsqlite-error`, `fsqlite-pager`, `fsqlite-wal` | `parking_lot` |
| 8 | `fsqlite-btree` | B-tree storage engine: cursor traversal, cell parsing, page balancing/splitting, overflow chains, freelist management, integrity checking | `fsqlite-types`, `fsqlite-error`, `fsqlite-pager` | (none) |
| 9 | `fsqlite-parser` | SQL lexer and recursive descent parser: tokenizer, Pratt expression parsing, keyword lookup | `fsqlite-types`, `fsqlite-error`, `fsqlite-ast` | `memchr` |
| 10 | `fsqlite-planner` | Query planning and optimization: name resolution, WHERE analysis, index selection, cost estimation, join ordering | `fsqlite-types`, `fsqlite-error`, `fsqlite-ast` | (none) |
| 11 | `fsqlite-vdbe` | Virtual database engine (bytecode VM): fetch-execute loop, 190+ opcode handlers, Mem values with type affinity, external merge sort, register file | `fsqlite-types`, `fsqlite-error`, `fsqlite-pager`, `fsqlite-btree` | (none) |
| 12 | `fsqlite-func` | Built-in functions: scalar (abs, length, substr, ...), aggregate (count, sum, avg, ...), window (row_number, rank, ...), date/time, math, function registry | `fsqlite-types`, `fsqlite-error` | (none) |
| 13 | `fsqlite-ext-fts3` | FTS3/FTS4 full-text search extension | `fsqlite-types`, `fsqlite-error` | (none) |
| 14 | `fsqlite-ext-fts5` | FTS5 full-text search extension: tokenizers, inverted index, BM25, highlight, snippet | `fsqlite-types`, `fsqlite-error` | (none) |
| 15 | `fsqlite-ext-rtree` | R-tree spatial index + geopoly extension | `fsqlite-types`, `fsqlite-error` | (none) |
| 16 | `fsqlite-ext-json` | JSON1 extension: json(), json_extract(), json_set(), json_each/json_tree virtual tables, JSONB | `fsqlite-types`, `fsqlite-error` | (none) |
| 17 | `fsqlite-ext-session` | Session extension: changeset/patchset generation and application, conflict resolution | `fsqlite-types`, `fsqlite-error` | (none) |
| 18 | `fsqlite-ext-icu` | ICU collation extension: Unicode-aware collation sequences, FTS tokenizer | `fsqlite-types`, `fsqlite-error` | (none) |
| 19 | `fsqlite-ext-misc` | Miscellaneous extensions: generate_series, carray, dbstat, dbpage, completion, csv, decimal, uuid | `fsqlite-types`, `fsqlite-error` | (none) |
| 20 | `fsqlite-core` | Engine orchestration: connection management, prepare/compile pipeline, schema cache, DDL/DML code generation, PRAGMA handling | `fsqlite-types`, `fsqlite-error`, `fsqlite-vfs`, `fsqlite-pager`, `fsqlite-wal`, `fsqlite-mvcc`, `fsqlite-btree`, `fsqlite-ast`, `fsqlite-parser`, `fsqlite-planner`, `fsqlite-vdbe`, `fsqlite-func` | (none) |
| 21 | `fsqlite` | Public API facade: Connection, Statement, Row, Transaction, FromSql/ToSql traits, high-level ergonomic interface | `fsqlite-types`, `fsqlite-error`, `fsqlite-core` | (none) |
| 22 | `fsqlite-cli` | Interactive REPL shell: dot-commands, output formatting, tab completion, syntax highlighting, command history | `fsqlite`, `fsqlite-error` | `frankentui` |
| 23 | `fsqlite-harness` | Conformance test runner: execute SQL against FrankenSQLite and C SQLite, compare output, golden file management | `fsqlite`, `fsqlite-error` | (none) |

### Dependency Graph (Layered)

```
Layer 0 (leaves):     fsqlite-types    fsqlite-error
Layer 1 (storage):    fsqlite-vfs      fsqlite-ast
Layer 2 (cache):      fsqlite-pager    fsqlite-parser     fsqlite-func
Layer 3 (log):        fsqlite-wal      fsqlite-planner
Layer 4 (btree):      fsqlite-btree
Layer 5 (vm):         fsqlite-vdbe
Layer 6 (mvcc):       fsqlite-mvcc
Layer 7 (ext):        fsqlite-ext-{fts3,fts5,rtree,json,session,icu,misc}
Layer 8 (core):       fsqlite-core
Layer 9 (api):        fsqlite
Layer 10 (apps):      fsqlite-cli      fsqlite-harness
```

### Full Dependency Tree

```
fsqlite-cli
  +-- fsqlite
  |     +-- fsqlite-core
  |           +-- fsqlite-vdbe
  |           |     +-- fsqlite-btree
  |           |     |     +-- fsqlite-pager
  |           |     |     |     +-- fsqlite-vfs
  |           |     |     |     |     +-- fsqlite-types
  |           |     |     |     |     +-- fsqlite-error
  |           |     |     |     +-- fsqlite-types
  |           |     |     |     +-- fsqlite-error
  |           |     |     +-- fsqlite-types
  |           |     |     +-- fsqlite-error
  |           |     +-- fsqlite-pager
  |           |     +-- fsqlite-types
  |           |     +-- fsqlite-error
  |           +-- fsqlite-planner
  |           |     +-- fsqlite-ast
  |           |     |     +-- fsqlite-types
  |           |     +-- fsqlite-types
  |           |     +-- fsqlite-error
  |           +-- fsqlite-parser
  |           |     +-- fsqlite-ast
  |           |     +-- fsqlite-types
  |           |     +-- fsqlite-error
  |           |     +-- memchr (external)
  |           +-- fsqlite-mvcc
  |           |     +-- fsqlite-wal
  |           |     |     +-- fsqlite-pager
  |           |     |     +-- fsqlite-vfs
  |           |     |     +-- fsqlite-types
  |           |     |     +-- fsqlite-error
  |           |     +-- fsqlite-pager
  |           |     +-- fsqlite-types
  |           |     +-- fsqlite-error
  |           |     +-- parking_lot (external)
  |           +-- fsqlite-func
  |           |     +-- fsqlite-types
  |           |     +-- fsqlite-error
  |           +-- fsqlite-ext-json
  |           +-- fsqlite-ext-fts5
  |           +-- fsqlite-ext-rtree
  |           +-- (other extensions...)
  |           +-- fsqlite-wal
  |           +-- fsqlite-vfs
  |           +-- fsqlite-btree
  |           +-- fsqlite-pager
  |           +-- fsqlite-ast
  |           +-- fsqlite-types
  |           +-- fsqlite-error
  +-- fsqlite-error

fsqlite-harness
  +-- fsqlite
  +-- fsqlite-error
```

---

## 5. Dependencies

### Runtime Dependencies

| Crate | Version | Used By | Rationale |
|-------|---------|---------|-----------|
| `asupersync` | path dep (`/dp/asupersync`) | `fsqlite-vfs`, `fsqlite-mvcc`, `fsqlite-core` | Async runtime, blocking thread pool for file I/O, sync primitives (Mutex, RwLock, Semaphore), MPSC/oneshot channels. Project requirement -- no tokio. |
| `frankentui` | path dep (`/dp/frankentui`) | `fsqlite-cli` | Terminal UI framework for the interactive shell (syntax highlighting, tab completion, key handling, styled text rendering). |
| `thiserror` | 2.x | `fsqlite-error` | Derive macro for ergonomic error type definitions with automatic `Display` and `From` impls. |
| `serde` | 1.x | `fsqlite-types`, `fsqlite-ext-json` | Serialization/deserialization for JSON extension output and potential config serialization. |
| `serde_json` | 1.x | `fsqlite-ext-json` | JSON parsing and generation for the JSON1 extension functions. |
| `tracing` | 0.1.x | multiple | Structured logging and diagnostic instrumentation throughout the engine. |
| `bitflags` | 2.x | `fsqlite-types` | Type-safe bitflag definitions for OpenFlags, SyncFlags, and other flag sets. |
| `smallvec` | 1.x | `fsqlite-vdbe`, `fsqlite-btree` | Stack-allocated small vectors for register files, cell arrays, and other bounded collections to reduce heap allocations in hot paths. |
| `memchr` | 2.x | `fsqlite-parser` | SIMD-accelerated byte scanning for fast tokenizer keyword and delimiter detection. |
| `parking_lot` | 0.12.x | `fsqlite-mvcc` | High-performance mutex and RwLock for the page lock table and MVCC version store. Lower overhead than std locks. |
| `sha2` | 0.10.x | `fsqlite-wal` | SHA-256 checksumming for WAL frame integrity verification (supplementing SQLite's CRC-based checksums). |

### Development Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `criterion` | 0.8.x | Statistical benchmarking framework for performance regression testing. |
| `proptest` | 1.x | Property-based testing for B-tree operations, parser edge cases, MVCC invariants, and transaction interleaving. |
| `insta` | 1.x | Snapshot testing for parser AST output, EXPLAIN bytecode output, and query plan representations. |
| `tempfile` | 3.x | Temporary file and directory creation for integration tests that exercise persistence and WAL. |

### Workspace Configuration

```toml
[workspace.package]
edition = "2024"
license = "MIT"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
pedantic = "deny"
nursery = "deny"

[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
panic = "abort"
strip = true
```

---

## 6. Testing Strategy

### Unit Tests (Per-Crate)

Every crate contains `#[test]` modules exercising individual components in
isolation. Trait dependencies are satisfied with mock implementations.

**Coverage targets by crate:**
- `fsqlite-types`: Serialization round-trips, varint encoding/decoding, value
  type conversions, serial type encoding/decoding
- `fsqlite-error`: Error construction, display formatting, error code mapping
- `fsqlite-vfs`: MemoryVfs read/write/truncate/lock, UnixVfs file operations
- `fsqlite-pager`: Page cache eviction, dirty page tracking, pager state
  transitions, journal integration
- `fsqlite-wal`: Frame encoding/decoding, WAL index operations, checkpoint
  correctness, recovery from partial writes
- `fsqlite-mvcc`: Visibility rule correctness, conflict detection, GC
  reclaimability, snapshot consistency, page lock table semantics
- `fsqlite-btree`: Cell parsing, page splitting/merging, cursor traversal,
  overflow chains, freelist, integrity checking
- `fsqlite-parser`: Token stream verification, AST structure for all statement
  types, error recovery, edge cases
- `fsqlite-planner`: Name resolution, index selection, cost model calculations
- `fsqlite-vdbe`: Individual opcode semantics, type affinity conversions, sort
  correctness
- `fsqlite-func`: All built-in function return values and edge cases
- Extensions: Each function and virtual table independently tested

### Integration Tests

Located in `tests/` at workspace root and within `fsqlite-core`. These test the
full pipeline from SQL text to result rows:

- **DDL tests**: CREATE TABLE/INDEX/VIEW/TRIGGER, ALTER TABLE, DROP
- **DML tests**: INSERT, UPDATE, DELETE with various WHERE clauses
- **Query tests**: SELECT with joins, subqueries, aggregation, window functions,
  CTEs
- **Transaction tests**: BEGIN/COMMIT/ROLLBACK, savepoints, auto-commit
- **Persistence tests**: Write, close, reopen, verify (using `tempfile`)
- **WAL tests**: Concurrent reader/writer scenarios
- **MVCC tests**: Concurrent writer scenarios, conflict detection, snapshot
  isolation

### Property-Based Tests (proptest)

- **B-tree invariant testing**: Random sequences of insert/delete/lookup
  operations must maintain B-tree ordering invariants and page structure
  validity.
- **Parser round-trip**: Parse SQL, pretty-print AST, re-parse -- the two ASTs
  must be structurally equivalent.
- **MVCC linearizability**: Random interleaving of concurrent transactions must
  produce results consistent with some serial ordering.
- **Record format**: Random SqliteValue sequences must survive encode/decode
  round-trip.

### Fuzz Tests (cargo-fuzz)

Targeted at components that process untrusted input:
- SQL parser (arbitrary byte strings as SQL input)
- Record decoder (arbitrary byte strings as record data)
- B-tree page decoder (arbitrary byte strings as page content)
- WAL frame decoder (arbitrary byte strings as WAL content)

### Conformance Harness (`fsqlite-harness`)

The golden-file conformance system:

1. **Generation**: Run SQL scripts against the C `sqlite3` binary, capture
   stdout/stderr as golden files stored in `conformance/golden/`.
2. **Verification**: Run the same SQL scripts against `fsqlite`, compare output
   row-by-row against golden files.
3. **Reporting**: Produce a conformance matrix showing pass/fail/skip per test
   file, with diff output for failures.
4. **Target**: **100% behavioral parity target** across 1,000+ test SQL files
   (for the supported surface), with any intentional divergences explicitly
   documented and annotated.

### File Format Round-Trip

A key test category creates databases with the C SQLite library, reads them
with FrankenSQLite (and vice versa), and verifies identical query results.
This ensures file format compatibility is maintained. Includes byte-level
comparison of page contents for known datasets.

### Concurrency Stress Tests (Phase 5+)

- 100 threads x 100 writes: verify all 10,000 rows present, no corruption
- Long-running reader + concurrent writer: snapshot consistency
- Kill-and-recover: data integrity after forced process termination
- Memory growth under sustained MVCC load: GC keeps version count bounded
- Checkpoint under concurrent write load: no corruption, no reader blocking

### Crash Recovery Verification

- **Power-loss simulation**: Simulate incomplete writes at every possible byte
  boundary during WAL append and checkpoint. Verify recovery correctness.
- **Kill-9 testing**: Run write workload, send SIGKILL at random points,
  restart and verify with `PRAGMA integrity_check`.
- **Bit-flip testing**: Random bit flips in WAL and database files; verify
  checksum detection rather than silent wrong results.

### Cumulative Test Count Targets

| Phase | Cumulative Tests |
|-------|-----------------|
| Phase 1 | Workspace compiles, specification complete |
| Phase 2 | 200+ |
| Phase 3 | 500+ |
| Phase 4 | 1,000+ |
| Phase 5 | 2,000+ |
| Phase 6 | 3,000+ |
| Phase 7 | 3,500+ |
| Phase 8 | 4,000+ |

---

## 7. Risk Assessment

### High Risk

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **MVCC overhead exceeds budget.** Page-level versioning adds memory and CPU cost per page access. If overhead exceeds 2x for non-contended workloads, the concurrency benefit may not justify the cost. | High | Medium | Benchmark continuously from Phase 5. Buffer pool eviction of superseded versions keeps memory bounded. Page lock table uses `BTreeMap` (not `HashMap`) for cache-friendly access patterns. Profile and optimize hot paths with `criterion`. |
| **B-tree balancing correctness.** The page splitting/merging algorithms in `btree.c` are among the most complex and subtle code in SQLite (~4,000 lines for balance_nonroot alone). Correctness bugs here corrupt data silently. | High | Medium | Extensive property-based testing with `proptest` (random insert/delete sequences). Integrity check function runs after every test. Compare B-tree structure against C SQLite for identical insertion sequences. |
| **File format incompatibility.** Subtle encoding differences (varint edge cases, cell pointer alignment, overflow threshold calculations) could produce files that C SQLite rejects or misreads. | High | Medium | Golden-file round-trip tests: create with FrankenSQLite, read with C SQLite (and vice versa). Byte-level comparison of page contents for known datasets. Test with databases up to 1GB. |

### Medium Risk

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **Parser coverage gaps.** SQLite's grammar has accumulated many extensions and edge cases over 20+ years. A hand-written parser may miss obscure syntax accepted by the LEMON-generated parser. | Medium | High | Use `parse.y` as the authoritative grammar reference. Fuzz the parser with SQL extracted from real-world applications. Conformance harness catches syntax acceptance differences. |
| **WAL crash recovery correctness.** Incorrect WAL replay after a crash can lose committed transactions or apply uncommitted ones. | High | Low | Simulate crashes at every possible point in the commit sequence (before WAL write, after WAL write but before checkpoint, mid-checkpoint). Verify database integrity and transaction atomicity after recovery. |
| **Query planner regression.** Different plan choices than C SQLite could cause dramatic performance differences for specific query patterns, even if the average is acceptable. | Medium | Medium | Start with a conservative planner that mimics C SQLite's heuristics. Use EXPLAIN comparison tests. Add ANALYZE support early. |
| **Extension behavioral differences.** Extensions like FTS5 and JSON1 have large surface areas with many edge cases in ranking, tokenization, and JSON path evaluation. | Medium | Medium | Conformance harness with extension-specific test SQL files. Port existing extension test cases from the C source tree. |
| **MVCC GC stalls.** If garbage collection does not keep pace with version creation under high write throughput, memory usage grows unboundedly. | Medium | Medium | Configurable GC interval via PRAGMA. GC runs in background without blocking writers. Monitor version chain length and buffer pool size. Alert when approaching capacity. |

### Low Risk

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **Nightly Rust breakage.** Using nightly channel means compiler updates can break builds. | Low | Medium | Pin toolchain in `rust-toolchain.toml`. Update periodically with full test suite. Avoid depending on unstable features likely to change. |
| **asupersync API changes.** asupersync is an internal library that may evolve. | Low | Low | Isolate asupersync usage behind internal abstraction layers. VFS trait already provides this isolation for file I/O. |
| **Deadlock in MVCC.** Despite the eager-locking design intended to prevent deadlocks, subtle bugs could introduce waiting cycles. | High | Low | Deadlock freedom is structural (no transaction ever waits while holding a page lock). Property-based tests with arbitrary transaction interleavings verify this invariant. Timeouts as a safety net. |

---

## 8. Key Reference Files

The C SQLite source files most relevant to this port, with approximate sizes:

### Core Engine

| File | Purpose | Size |
|------|---------|------|
| `legacy_sqlite_code/sqlite/src/sqliteInt.h` | Main internal header: ALL core data structures (sqlite3, Vdbe, BtCursor, Parse, Select, Expr, Table, Column, Index, etc.) | ~250 KB |
| `legacy_sqlite_code/sqlite/src/btree.c` | B-tree storage engine: cursor operations, cell management, page balancing, overflow, freelist | 11,568 lines |
| `legacy_sqlite_code/sqlite/src/btreeInt.h` | B-tree internal structures: MemPage, BtCursor, BtShared, CellInfo | ~600 lines |
| `legacy_sqlite_code/sqlite/src/pager.c` | Page cache, transaction management, journal I/O, pager state machine | 7,834 lines |
| `legacy_sqlite_code/sqlite/src/wal.c` | Write-ahead logging: WAL frame format, WAL index, checkpoint, recovery. **WAL_WRITE_LOCK at line 3698** -- the lock we are replacing with MVCC. | 4,621 lines |
| `legacy_sqlite_code/sqlite/src/vdbe.c` | VDBE bytecode interpreter: the main execution loop with 190+ opcode case handlers | 9,316 lines |
| `legacy_sqlite_code/sqlite/src/vdbeInt.h` | VDBE internal structures: Vdbe, VdbeOp, Mem, VdbeCursor | ~700 lines |
| `legacy_sqlite_code/sqlite/src/vdbeaux.c` | VDBE auxiliary: program construction, Mem operations, column cache | ~5,000 lines |
| `legacy_sqlite_code/sqlite/src/vdbeapi.c` | VDBE public API: step(), column_*(), finalize() | ~2,000 lines |

### SQL Parsing and Planning

| File | Purpose | Size |
|------|---------|------|
| `legacy_sqlite_code/sqlite/src/parse.y` | LEMON parser grammar: the authoritative SQL grammar definition | 1,963 lines |
| `legacy_sqlite_code/sqlite/src/tokenize.c` | SQL tokenizer: character classification, string/number/identifier scanning | 899 lines |
| `legacy_sqlite_code/sqlite/src/select.c` | SELECT statement compilation: join processing, subquery flattening, compound SELECT | 8,972 lines |
| `legacy_sqlite_code/sqlite/src/where.c` | WHERE clause optimization: index selection, cost estimation, skip-scan | 7,858 lines |
| `legacy_sqlite_code/sqlite/src/wherecode.c` | WHERE code generation: translates WHERE plan into VDBE bytecode | ~3,500 lines |
| `legacy_sqlite_code/sqlite/src/whereexpr.c` | WHERE expression analysis: term extraction, OR optimization | ~2,000 lines |
| `legacy_sqlite_code/sqlite/src/resolve.c` | Name resolution: column and table binding | ~2,000 lines |
| `legacy_sqlite_code/sqlite/src/expr.c` | Expression handling: evaluation, comparison, code generation | ~6,500 lines |

### DML and DDL

| File | Purpose | Size |
|------|---------|------|
| `legacy_sqlite_code/sqlite/src/insert.c` | INSERT code generation | ~2,500 lines |
| `legacy_sqlite_code/sqlite/src/update.c` | UPDATE code generation | ~1,500 lines |
| `legacy_sqlite_code/sqlite/src/delete.c` | DELETE code generation | ~1,200 lines |
| `legacy_sqlite_code/sqlite/src/build.c` | DDL: CREATE/DROP TABLE/INDEX/VIEW/TRIGGER, ALTER TABLE | ~5,000 lines |
| `legacy_sqlite_code/sqlite/src/trigger.c` | Trigger compilation and execution | ~1,200 lines |
| `legacy_sqlite_code/sqlite/src/pragma.c` | PRAGMA command handling | ~3,000 lines |

### VFS and OS Interface

| File | Purpose | Size |
|------|---------|------|
| `legacy_sqlite_code/sqlite/src/os_unix.c` | Unix VFS: file I/O, advisory locking, mmap, shared memory | ~9,000 lines |
| `legacy_sqlite_code/sqlite/src/os.c` | OS abstraction layer: VFS registration, default VFS selection | ~400 lines |

### Extensions

| File/Directory | Purpose | Size |
|----------------|---------|------|
| `legacy_sqlite_code/sqlite/ext/fts5/` | FTS5 full-text search | ~29,000 lines total |
| `legacy_sqlite_code/sqlite/ext/fts3/` | FTS3/FTS4 full-text search | ~21,000 lines total |
| `legacy_sqlite_code/sqlite/ext/rtree/` | R-tree spatial index + geopoly | ~7,000 lines total |
| `legacy_sqlite_code/sqlite/ext/misc/json.c` | JSON1 extension | ~171 KB |
| `legacy_sqlite_code/sqlite/ext/session/` | Session/changeset extension | ~13,000 lines total |
| `legacy_sqlite_code/sqlite/ext/icu/` | ICU Unicode collation | ~500 lines |

### External Reference (asupersync)

| File | Purpose | Size |
|------|---------|------|
| `/dp/asupersync/src/database/sqlite.rs` | Asupersync's existing SQLite wrapper (API reference) | ~800 lines |
| `/dp/asupersync/src/sync/` | Mutex, RwLock, Semaphore, Pool primitives | ~1,500 lines |
| `/dp/asupersync/src/channel/mpsc.rs` | Two-phase reserve/commit MPSC channel | ~500 lines |
| `/dp/beads_rust/src/error/mod.rs` | Reference error pattern (thiserror usage) | ~300 lines |
| `/dp/frankentui/Cargo.toml` | Reference workspace layout and conventions | -- |
