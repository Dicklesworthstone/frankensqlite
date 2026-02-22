# fsqlite-func

Built-in scalar, aggregate, and window SQL functions and extensibility traits for FrankenSQLite.

## Overview

`fsqlite-func` provides the function infrastructure for FrankenSQLite. It defines open, user-implementable traits for scalar functions, aggregate functions, window functions, virtual tables, collation sequences, and authorizer callbacks. It also ships a `FunctionRegistry` that resolves functions by `(name, num_args)` key with variadic fallback, plus built-in implementations for standard SQLite scalar, aggregate, window, math, and datetime functions.

This crate depends on `fsqlite-types`, `fsqlite-error`, and `tracing`.

```
fsqlite-error --+
                +--> fsqlite-func
fsqlite-types --+        ^
                         |-- fsqlite-core (VDBE engine)
                         |-- fsqlite (facade, via core)
```

## Modules

- `scalar` - `ScalarFunction` trait.
- `aggregate` - `AggregateFunction` trait and type-erased `AggregateAdapter`.
- `window` - `WindowFunction` trait and type-erased `WindowAdapter`.
- `builtins` - Standard SQLite scalar built-ins (e.g., `abs`, `length`, `typeof`, `coalesce`, `ifnull`, `nullif`, `hex`, `quote`, `unicode`, `zeroblob`, `last_insert_rowid`, `changes`, etc.).
- `agg_builtins` - Standard aggregate built-ins (`count`, `sum`, `avg`, `min`, `max`, `group_concat`, `total`).
- `window_builtins` - Standard window built-ins (`row_number`, `rank`, `dense_rank`, `ntile`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`).
- `math` - SQLite math functions (`ceil`, `floor`, `ln`, `log`, `log2`, `log10`, `pow`, `sqrt`, `sign`, `trunc`, etc.).
- `datetime` - Date/time functions (`date`, `time`, `datetime`, `julianday`, `strftime`, `unixepoch`).
- `collation` - `CollationFunction` trait, `CollationRegistry`, and built-in collations (`BINARY`, `NOCASE`, `RTRIM`).
- `authorizer` - `Authorizer` trait and `AuthAction`/`AuthResult` types for access control.
- `vtab` - `VirtualTable` and `VirtualTableCursor` traits for virtual table modules.

## Key Types

- `ScalarFunction` - Trait for user-defined scalar functions. Implement `invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue>`.
- `AggregateFunction` - Trait for user-defined aggregates with associated `State` type. Methods: `init`, `step`, `finalize`.
- `WindowFunction` - Trait for user-defined window functions. Extends aggregate with `value` and `inverse` methods.
- `FunctionRegistry` - In-memory registry keyed by `(name, num_args)`. Lookup tries exact match first, then variadic fallback (`num_args = -1`).
- `FunctionKey` - Composite lookup key: uppercase name + argument count.
- `FuncMetricsSnapshot` - Snapshot of function evaluation metrics (call count, cumulative duration).
- `CollationFunction` / `CollationRegistry` - Trait and registry for custom collation sequences.
- `Authorizer` - Trait for SQL statement authorization callbacks.
- `VirtualTable` / `VirtualTableCursor` - Traits for virtual table module implementations.

## Usage

```rust
use fsqlite_func::{ScalarFunction, FunctionRegistry};
use fsqlite_types::SqliteValue;
use fsqlite_error::Result;

// Define a custom scalar function
struct Square;

impl ScalarFunction for Square {
    fn name(&self) -> &str { "square" }
    fn num_args(&self) -> i32 { 1 }

    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let n = args[0].to_integer();
        Ok(SqliteValue::Integer(n * n))
    }
}

// Register it
let mut registry = FunctionRegistry::new();
registry.register_scalar(Square);

// Look it up
let func = registry.find_scalar("square", 1).expect("should find square");
```

## License

MIT
