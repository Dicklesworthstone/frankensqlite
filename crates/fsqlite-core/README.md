# fsqlite-core

Core database engine for FrankenSQLite. Provides the `Connection` API that ties
together parsing, planning, codegen, VDBE execution, schema management, and the
storage stack (pager, WAL, B-tree, MVCC, VFS).

## Overview

`fsqlite-core` is the integration hub of the FrankenSQLite workspace. It owns
the `Connection` struct -- the primary entry point for opening databases,
preparing statements, executing queries, and managing transactions. Internally,
a query flows through:

1. **Parsing** (`fsqlite-parser`) -- SQL text to AST.
2. **Planning** (`fsqlite-planner`) -- name resolution, projection expansion,
   index selection, codegen to VDBE bytecode.
3. **Execution** (`fsqlite-vdbe`) -- bytecode interpretation with register file,
   cursors, and result row collection.
4. **Storage** -- pager, WAL, B-tree cursors, and MVCC concurrency control.

Beyond query execution, `fsqlite-core` also provides:

- **Schema management** -- CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX,
  CREATE VIEW, and PRAGMA handling.
- **Transactions** -- BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE.
- **MVCC** -- concurrent writers with SSI (Serializable Snapshot Isolation),
  conflict detection, and garbage collection.
- **Bounded parallelism** -- bulkhead framework for internal background work
  with configurable concurrency limits and `SQLITE_BUSY` overflow rejection.
- **Replication** -- ECS replication sender/receiver, snapshot shipping, and
  RaptorQ-based forward error correction for WAL frames.
- **EXPLAIN** -- query plan explanation output.
- **Observability** -- metrics, tracing spans, and decision audit trails.

**Position in the dependency graph:**

```
fsqlite-core (this crate) -- the integration layer
  --> fsqlite-parser (SQL parsing)
  --> fsqlite-planner (query planning + codegen)
  --> fsqlite-vdbe (bytecode execution)
  --> fsqlite-btree (B-tree cursors)
  --> fsqlite-pager (page cache)
  --> fsqlite-wal (write-ahead log)
  --> fsqlite-mvcc (concurrency control)
  --> fsqlite-vfs (file system abstraction)
  --> fsqlite-func (built-in SQL functions)
  --> fsqlite-ext-json, fsqlite-ext-fts5 (extensions)
  --> fsqlite-observability (metrics + tracing)
```

`fsqlite-core` is consumed by the public facade crate `fsqlite`, which
re-exports `Connection`, `Row`, and `PreparedStatement`.

## Key Types

- `Connection` -- Database connection. Open with `Connection::open(path)`.
  Supports `:memory:` and file-backed databases. Holds the in-memory table
  store, pager backend, schema catalog, function registry, and transaction
  state.
- `PreparedStatement` -- A compiled SQL statement bound to a connection. Call
  `query()`, `query_with_params()`, `execute()`, or `query_row()`.
- `Row` -- A single result row. Access column values via `values()`.
- `BulkheadConfig` -- Bounded parallelism configuration (max concurrency,
  queue depth, overflow policy).
- `OverflowPolicy` -- What to do when the bulkhead is full (`DropBusy`).
- `ParallelismProfile` -- Runtime profile for parallelism defaults (`Balanced`).

## Usage

```rust
use fsqlite_core::connection::{Connection, Row};

// Open an in-memory database
let conn = Connection::open(":memory:").expect("open failed");

// DDL
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;

// DML
conn.execute("INSERT INTO users VALUES (1, 'Alice');")?;
conn.execute("INSERT INTO users VALUES (2, 'Bob');")?;

// Query
let rows: Vec<Row> = conn.query("SELECT id, name FROM users ORDER BY id;")?;
for row in &rows {
    println!("{:?}", row.values());
}

// Prepared statement with parameters
let stmt = conn.prepare("SELECT name FROM users WHERE id = ?1;")?;
let row = stmt.query_row_with_params(&[SqliteValue::Integer(1)])?;
```

## License

MIT (with OpenAI/Anthropic Rider) -- see workspace root LICENSE file.
