# fsqlite-c-api

SQLite C API compatibility shim for FrankenSQLite. Provides a drop-in
replacement for the most commonly used `sqlite3_*` functions via C FFI.

## Overview

`fsqlite-c-api` exposes FrankenSQLite through the standard SQLite C API, making
it possible to link existing C/C++ applications against FrankenSQLite without
code changes. The crate builds as a `cdylib` (shared library), `staticlib`
(static library), and `rlib` (Rust library).

The shim wraps the Rust `Connection` type behind opaque `Sqlite3` and
`Sqlite3Stmt` handles, translating between C types and Rust types at the
boundary. All SQLite result codes (`SQLITE_OK`, `SQLITE_ERROR`, `SQLITE_ROW`,
`SQLITE_DONE`, etc.) and column type constants (`SQLITE_INTEGER`,
`SQLITE_FLOAT`, `SQLITE_TEXT`, `SQLITE_BLOB`, `SQLITE_NULL`) are exported.

Built-in observability: every API call increments an atomic counter, accessible
via `compat_metrics_snapshot()`. Tracing spans are emitted at INFO level with
the `compat_api` span name.

**Position in the dependency graph:**

```
C/C++ application
  --> fsqlite-c-api (this crate) -- C FFI boundary
    --> fsqlite (public API facade)
      --> fsqlite-core (engine)
        --> fsqlite-parser, fsqlite-planner, fsqlite-vdbe, ...
```

Dependencies: `fsqlite`, `fsqlite-error`, `fsqlite-types`.

Note: This crate allows `unsafe_code` (required for FFI) while the rest of the
workspace forbids it.

## Exported FFI Functions

- `sqlite3_open` -- Open a database connection.
- `sqlite3_close` -- Close a database connection.
- `sqlite3_exec` -- Execute SQL with a callback for each result row.
- `sqlite3_free` -- Free memory allocated by `sqlite3_exec` error messages.
- `sqlite3_prepare_v2` -- Compile SQL into a prepared statement.
- `sqlite3_step` -- Advance a prepared statement to the next result row.
- `sqlite3_finalize` -- Destroy a prepared statement.
- `sqlite3_reset` -- Reset a prepared statement for re-execution.
- `sqlite3_column_count` -- Number of columns in the result set.
- `sqlite3_column_type` -- Datatype of a result column.
- `sqlite3_column_int` / `sqlite3_column_int64` -- Integer column accessors.
- `sqlite3_column_double` -- Float column accessor.
- `sqlite3_column_text` -- Text column accessor.
- `sqlite3_column_blob` -- Blob column accessor.
- `sqlite3_column_bytes` -- Byte length of a column value.
- `sqlite3_errmsg` -- Last error message.
- `sqlite3_errcode` -- Last error code.
- `sqlite3_changes` -- Number of rows changed by the last DML statement.

## Key Types

- `Sqlite3` -- Opaque database connection handle (wraps `Connection` +
  last-error state).
- `Sqlite3Stmt` -- Opaque prepared statement handle (wraps SQL string, row
  cache, cursor position, and column metadata).
- `CompatMetricsSnapshot` -- Point-in-time counters for each API function
  (open, close, exec, prepare, step, finalize, column, errmsg).

## Usage (from C)

```c
#include <stdio.h>

// Link against libfsqlite_c_api.so / libfsqlite_c_api.a
typedef struct Sqlite3 sqlite3;
typedef struct Sqlite3Stmt sqlite3_stmt;

extern int sqlite3_open(const char *filename, sqlite3 **ppDb);
extern int sqlite3_prepare_v2(sqlite3 *db, const char *sql, int nByte,
                              sqlite3_stmt **ppStmt, const char **pzTail);
extern int sqlite3_step(sqlite3_stmt *stmt);
extern const char *sqlite3_column_text(sqlite3_stmt *stmt, int iCol);
extern int sqlite3_finalize(sqlite3_stmt *stmt);
extern int sqlite3_close(sqlite3 *db);

int main(void) {
    sqlite3 *db;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT 'hello, fsqlite!';", -1, &stmt, NULL);

    if (sqlite3_step(stmt) == 101 /* SQLITE_ROW? -- use 100 for ROW */) {
        printf("%s\n", sqlite3_column_text(stmt, 0));
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}
```

## Usage (from Rust, for metrics)

```rust
use fsqlite_c_api::{compat_metrics_snapshot, reset_compat_metrics};

let snapshot = compat_metrics_snapshot();
println!("Total API calls: {}", snapshot.total());
```

## License

MIT (with OpenAI/Anthropic Rider) -- see workspace root LICENSE file.
