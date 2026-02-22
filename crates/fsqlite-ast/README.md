# fsqlite-ast

SQL abstract syntax tree node types for FrankenSQLite.

## Overview

`fsqlite-ast` defines the complete AST type hierarchy for the SQLite SQL dialect. Every SQL statement parsed by the FrankenSQLite parser produces a tree of these nodes. All expression nodes carry a `Span` for source-location tracking, enabling precise error messages and EXPLAIN output.

This crate depends only on `fsqlite-types` and has no other fsqlite dependencies. It is consumed by the parser (`fsqlite-parser`) and the query execution engine (`fsqlite-core`).

```
fsqlite-error --> fsqlite-types --> fsqlite-ast
                                      ^
                                      |-- fsqlite-parser
                                      |-- fsqlite-core
```

## Modules

- Root module - All AST node types, expressions, statements, and operators.
- `display` - `Display` implementations for AST nodes (SQL pretty-printing).
- `rebase` - Rebase expression types for conflict resolution.

## Key Types

### Statements
- `Statement` - Top-level enum covering all SQL statements: `Select`, `Insert`, `Update`, `Delete`, `CreateTable`, `CreateIndex`, `CreateView`, `CreateTrigger`, `CreateVirtualTable`, `Drop`, `AlterTable`, `Begin`, `Commit`, `Rollback`, `Savepoint`, `Release`, `Attach`, `Detach`, `Pragma`, `Vacuum`, `Reindex`, `Analyze`, `Explain`.

### Expressions
- `Expr` - Expression node enum: `Literal`, `Column`, `BinaryOp`, `UnaryOp`, `Between`, `In`, `Like`, `FunctionCall`, `Cast`, `Case`, `Subquery`, `Exists`, and more. Every variant carries a `Span`.
- `BinaryOp` - Arithmetic, string, comparison, logical, and bitwise binary operators.
- `UnaryOp` - Negate, Plus, BitNot, Not.
- `Literal` - Integer, Float, String, Blob, Null, True, False, CurrentTime, CurrentDate, CurrentTimestamp.

### Names and References
- `QualifiedName` - A possibly schema-qualified name (e.g., `main.users`).
- `ColumnRef` - A possibly table-qualified column reference.
- `TypeName` - Column type as written in DDL, with optional size parameters.

### Source Tracking
- `Span` - Byte-offset range (`start..end`) into the original SQL source text.

## Usage

```rust
use fsqlite_ast::{Statement, Expr, Literal, Span, QualifiedName, BinaryOp};

// Construct an AST node for the expression `1 + 2`
let expr = Expr::BinaryOp {
    left: Box::new(Expr::Literal(Literal::Integer(1), Span::new(0, 1))),
    op: BinaryOp::Add,
    right: Box::new(Expr::Literal(Literal::Integer(2), Span::new(4, 5))),
    span: Span::new(0, 5),
};

// Qualified names for schema-qualified references
let table = QualifiedName::qualified("main", "users");
assert_eq!(table.to_string(), "main.users");

let bare = QualifiedName::bare("users");
assert_eq!(bare.to_string(), "users");
```

## License

MIT
