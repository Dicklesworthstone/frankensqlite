# fsqlite-error

Structured error types for FrankenSQLite, modeled after SQLite's error codes with Rust-idiomatic structure.

## Overview

`fsqlite-error` is the foundational error crate for the FrankenSQLite workspace. It defines a single comprehensive error enum (`FrankenError`) covering every category of failure that can occur in a SQLite-compatible database engine: database errors, I/O errors, SQL errors, constraint violations, transaction errors, MVCC conflicts, type mismatches, limit violations, WAL errors, and VFS errors.

This crate sits at the bottom of the dependency graph -- it has no dependencies on other fsqlite crates and is depended upon by nearly every other crate in the workspace.

```
fsqlite-error  (no fsqlite dependencies)
  ^
  |-- fsqlite-types
  |-- fsqlite-func
  |-- fsqlite (facade)
  |-- ... (most other workspace crates)
```

## Key Types

- `FrankenError` - Primary error enum with ~40 variants organized by category (database, I/O, SQL, constraints, transactions, MVCC, types, limits, WAL, VFS, internal). Each variant carries structured context fields (paths, page numbers, column names, etc.) rather than opaque strings.
- `ErrorCode` - Numeric error codes matching SQLite's C `sqlite3.h` constants (`SQLITE_OK = 0`, `SQLITE_ERROR = 1`, `SQLITE_BUSY = 5`, etc.) for wire protocol compatibility.
- `Result<T>` - Type alias for `std::result::Result<T, FrankenError>`.

## Notable Methods on FrankenError

- `error_code()` - Maps the error to a SQLite-compatible `ErrorCode`.
- `extended_error_code()` - Returns SQLite extended error codes (e.g., `SQLITE_BUSY_SNAPSHOT = 517`).
- `is_transient()` - Whether the error is transient and may succeed on retry (e.g., `Busy`, `WriteConflict`).
- `is_user_recoverable()` - Whether the user can likely fix this without code changes.
- `suggestion()` - Human-friendly hint for resolving the error.
- `exit_code()` - Process exit code for CLI use.
- Convenience constructors: `syntax()`, `parse()`, `internal()`, `not_implemented()`, `function_error()`.

## Usage

```rust
use fsqlite_error::{FrankenError, Result};

fn open_database(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(FrankenError::CannotOpen {
            path: path.into(),
        });
    }
    Ok(())
}

fn handle_error(err: &FrankenError) {
    if err.is_transient() {
        eprintln!("Retryable error: {err}");
    }
    if let Some(hint) = err.suggestion() {
        eprintln!("Suggestion: {hint}");
    }
    // Map to SQLite numeric code for C API compatibility
    let code = err.error_code() as i32;
    eprintln!("SQLite error code: {code}");
}
```

## License

MIT
