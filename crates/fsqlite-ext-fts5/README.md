# fsqlite-ext-fts5

FTS5 full-text search extension for FrankenSQLite.

## Overview

This crate implements the FTS5 full-text search extension, the successor to FTS3/FTS4 with a cleaner API and improved performance. It provides a complete tokenizer framework (unicode61, ascii, porter stemming, trigram), boolean query parsing (implicit AND, OR, NOT, phrase, prefix, NEAR, column filter, caret), an inverted index for document storage and retrieval, BM25 ranking, highlight/snippet auxiliary functions, and a virtual table implementation with content modes (stored and contentless) and secure-delete/contentless-delete configuration.

This crate sits at the extension layer of the fsqlite workspace. It depends on `fsqlite-types`, `fsqlite-error`, `fsqlite-func` (for `ScalarFunction` and `VirtualTable` traits), and `tracing`.

## Key Types

- `Fts5Config` - Configuration for an FTS5 virtual table: content mode, secure-delete, and contentless-delete settings
- `ContentMode` - Enum: `Stored` or `Contentless`
- `DeleteAction` - Enum: `Reject`, `Tombstone`, or `PhysicalPurge`
- `Fts5Token` - A single token produced by a tokenizer, with text and byte offset
- `Fts5Tokenizer` (trait) - Tokenizer interface; implementations: `Unicode61Tokenizer`, `AsciiTokenizer`, `PorterTokenizer`, `TrigramTokenizer`
- `Fts5QueryToken` - A parsed token from an FTS5 query expression
- `Fts5QueryTokenKind` - Token classification for FTS5 queries (term, phrase, AND, OR, NOT, NEAR, column filter, prefix, caret, etc.)
- `Fts5QueryError` - Error type for query parsing failures
- `Fts5Expr` - Parsed FTS5 expression tree (Term, Phrase, And, Or, Not, Near, ColumnFilter)
- `InvertedIndex` - In-memory inverted index mapping terms to postings lists with term frequency and position data
- `Posting` - A single posting: document ID, term frequency, and positions
- `Fts5Table` - FTS5 virtual table implementation (implements `VirtualTable`)
- `Fts5Cursor` - Cursor for iterating FTS5 query results (implements `VirtualTableCursor`)
- `Fts5SourceIdFunc` - Scalar function returning the FTS5 source identifier

## Key Functions

- `extension_name()` - Returns `"fts5"`
- `create_tokenizer(name)` - Factory function returning a boxed tokenizer by name
- `parse_fts5_query(query)` - Parse an FTS5 query string into a token sequence
- `build_expr(tokens)` - Build an expression tree from parsed query tokens
- `evaluate_expr(index, expr)` - Evaluate an FTS5 expression against an inverted index, returning matching document IDs
- `bm25_score(...)` - Compute BM25 relevance score for a document
- `highlight(text, terms, open_tag, close_tag)` - Highlight matching terms in text
- `snippet(text, terms, open_tag, close_tag, ellipsis, max_tokens)` - Generate a snippet with highlighted terms
- `register_fts5_scalars(registry)` - Register all FTS5 scalar functions into a function registry

## Dependencies

- `fsqlite-types`
- `fsqlite-error`
- `fsqlite-func`
- `tracing`

## License

MIT
