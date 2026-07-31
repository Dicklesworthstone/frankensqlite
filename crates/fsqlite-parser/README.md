# fsqlite-parser

Hand-written SQL parser with direct statement/DDL routines and explicit
heap-backed state machines: Pratt tasks for expressions and frames for SELECT
trees. Converts SQL text into a typed AST defined in `fsqlite-ast`.

## Overview

`fsqlite-parser` is the front-end of the FrankenSQLite query pipeline. It
tokenizes raw SQL strings via a `memchr`-accelerated lexer, then parses them
into a complete AST covering SELECT, INSERT, UPDATE, DELETE, CREATE TABLE,
CREATE INDEX, CREATE VIEW, CREATE TRIGGER, ALTER TABLE, ATTACH, PRAGMA,
transactions, and more.

A separate semantic analysis pass (`Resolver`) handles name resolution, scope
tracking, and function arity validation.

**Position in the dependency graph:**

```
SQL text
  --> fsqlite-parser (this crate)
    --> fsqlite-ast (AST nodes)
    --> fsqlite-planner (query planning)
    --> fsqlite-vdbe (bytecode codegen)
```

Dependencies: `fsqlite-types`, `fsqlite-error`, `fsqlite-ast`, `hashbrown`,
`memchr`, and `tracing`.

## Key Types

- `Lexer` -- Tokenizer that converts SQL text into a stream of `Token` values.
  Uses `memchr` for fast string literal scanning and tracks line/column for
  error reporting.
- `Token` / `TokenKind` -- A single lexical token with span information and its
  variant (keyword, identifier, literal, operator, etc.).
- `Parser` -- SQL parser. Use `Parser::from_sql()` followed by
  `Parser::parse_all()` to produce `Statement` values and structured parse
  errors.
- `ParseError` -- Error type returned when parsing fails, with span and message.
- `Resolver` / `Schema` / `Scope` -- Semantic analysis layer for name
  resolution, column validation, and function arity checking after parsing.
- `SemanticError` -- Error type for semantic analysis failures (unknown column,
  ambiguous reference, wrong arity, etc.).
- `ParseMetricsSnapshot` / `TokenizeMetricsSnapshot` -- Point-in-time counters
  for parsed statements and tokenized tokens, useful for observability.

## Usage

```rust
use fsqlite_parser::{Lexer, Parser};
use fsqlite_ast::Statement;

// Tokenize
let tokens = Lexer::tokenize("SELECT 1 + 2;");
assert!(!tokens.is_empty());

// Parse
let mut parser = Parser::from_sql(
    "SELECT name, age FROM users WHERE id = ?1;",
);
let (statements, errors) = parser.parse_all();
assert!(errors.is_empty(), "valid SQL: {errors:?}");
assert!(matches!(statements.as_slice(), [Statement::Select(_)]));

// Semantic analysis (optional, requires schema info)
use fsqlite_parser::{Resolver, Schema};
let schema = Schema::default();
let resolver = Resolver::new(&schema);
// resolver.resolve(&statements[0]) ...
```

## License

MIT (with OpenAI/Anthropic Rider) -- see workspace root LICENSE file.
