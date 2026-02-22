# fsqlite

Public API facade for FrankenSQLite -- a from-scratch SQLite-compatible database engine written in Rust.

## Overview

`fsqlite` is the top-level crate that application code depends on. It re-exports a stable, ergonomic API surface from the internal workspace crates (`fsqlite-core`, `fsqlite-vfs`, `fsqlite-types`, `fsqlite-error`) and gates optional extension modules behind Cargo features. This is the primary entry point for opening connections, executing queries, and working with prepared statements.

```
fsqlite-error --> fsqlite-types --> fsqlite-ast --> fsqlite-parser
                      |                                |
                      +---> fsqlite-func               |
                      +---> fsqlite-observability       |
                      |                                v
                      +--------------> fsqlite-core <---+
                                           |
                  fsqlite-vfs -------------+
                      |                    |
                      +-----> fsqlite (facade) <-- you are here
                                |
                      optional extensions:
                        fsqlite-ext-json
                        fsqlite-ext-fts5
                        fsqlite-ext-fts3
                        fsqlite-ext-rtree
                        fsqlite-ext-session
                        fsqlite-ext-icu
                        fsqlite-ext-misc
```

## Cargo Features

| Feature   | Default | Description                          |
|-----------|---------|--------------------------------------|
| `json`    | yes     | JSON1 extension (`json()`, `json_extract()`, etc.) |
| `fts5`    | yes     | Full-text search v5                  |
| `rtree`   | yes     | R-Tree spatial index                 |
| `fts3`    | no      | Full-text search v3/v4 (legacy)      |
| `session` | no      | Session extension (changeset/patchset) |
| `icu`     | no      | ICU Unicode collation/tokenization   |
| `misc`    | no      | Miscellaneous extensions             |
| `raptorq` | no      | RaptorQ erasure coding support       |
| `mvcc`    | no      | Multi-version concurrency control    |

## Key Types (re-exported)

- `Connection` - A database connection. Open with `Connection::open(path)` or `Connection::open(":memory:")`.
- `PreparedStatement` - A compiled SQL statement for repeated execution with different parameters.
- `Row` - A single result row. Access columns by index with `row.get(i)` or get all values with `row.values()`.
- `TraceEvent` / `TraceMask` - Tracing callback types for monitoring SQL execution.
- `fsqlite_vfs` - The virtual filesystem layer (re-exported module).

## Usage

```rust
use fsqlite::Connection;

// Open an in-memory database
let conn = Connection::open(":memory:").expect("failed to open database");

// Execute DDL
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
    .expect("create table failed");

// Insert data
conn.execute("INSERT INTO users VALUES (1, 'Alice');")
    .expect("insert failed");

// Query with results
let rows = conn.query("SELECT id, name FROM users;")
    .expect("query failed");
for row in &rows {
    println!("id={:?}, name={:?}", row.get(0), row.get(1));
}

// Prepared statements with parameters
use fsqlite_types::SqliteValue;
let stmt = conn.prepare("SELECT * FROM users WHERE id = ?1;").unwrap();
let rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).unwrap();
assert_eq!(rows.len(), 1);

// Single-row convenience
let row = conn.query_row("SELECT count(*) FROM users;").unwrap();
```

## License

MIT
