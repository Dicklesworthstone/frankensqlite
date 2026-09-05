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
| `icu`     | yes     | ICU Unicode collation/tokenization   |
| `misc`    | yes     | Miscellaneous extensions             |
| `raptorq` | no      | Currently an empty feature flag      |
| `mvcc`    | no      | Currently an empty feature flag; MVCC concurrent writers are enabled by default |

## Key Types (re-exported)

- `Connection` - A database connection. Open with `Connection::open(path).await?` or `Connection::open(":memory:").await?`.
- `PreparedStatement` - A compiled SQL statement for repeated execution with different parameters.
- `Row` - A single result row. Access columns by index with `row.get(i)` or get all values with `row.values()`.
- `TraceEvent` / `TraceMask` - Tracing callback types for monitoring SQL execution.
- `fsqlite_vfs` - The virtual filesystem layer (re-exported module).

## Usage

Add the database and the caller's runtime to your application's `Cargo.toml`:

```toml
[dependencies]
fsqlite = "0.3.16"
asupersync = { version = "0.4.10", default-features = false }
```

Connection and prepared-statement operations are asynchronous. This complete
program creates the application's runtime, awaits each operation, and closes
the connection before the runtime exits. `Connection` is `!Send` and `!Sync`;
keep it on its owning thread.

```rust
#![recursion_limit = "512"]

use asupersync::runtime::RuntimeBuilder;
use fsqlite::{Connection, SqliteValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RuntimeBuilder::current_thread().build()?;
    runtime.block_on(async {
        let conn = Connection::open(":memory:").await?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .await?;
        conn.execute("INSERT INTO users VALUES (1, 'Alice');").await?;

        let rows = conn.query("SELECT id, name FROM users;").await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(rows[0].get(1), Some(&SqliteValue::from("Alice")));
        for row in &rows {
            println!("id={:?}, name={:?}", row.get(0), row.get(1));
        }

        // Prepared statements borrow the connection; finish them before close.
        {
            let stmt = conn.prepare("SELECT name FROM users WHERE id = ?1;").await?;
            let rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::from("Alice")));
        }

        let row = conn.query_row("SELECT count(*) FROM users;").await?;
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(1)));
        conn.close().await
    })?;
    Ok(())
}
```

## License

MIT
