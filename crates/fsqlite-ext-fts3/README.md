# fsqlite-ext-fts3

FTS3/FTS4 full-text search extension for FrankenSQLite.

## Overview

This crate implements the FTS3 and FTS4 full-text search extensions. It provides query tokenization and validation (with explicit boolean operators AND, OR, NOT, NEAR, and phrase matching), matchinfo format validation and width computation, and offsets parsing/formatting. FTS3 is the legacy full-text search module; FTS4 is its backward-compatible successor with additional features.

This is a leaf crate in the fsqlite workspace dependency graph. It depends on `fsqlite-types`, `fsqlite-error`, and `fsqlite-func`.

## Key Types

- `FtsDialect` - Enum distinguishing `Fts3` and `Fts4` dialects
- `QueryToken` - A parsed token from a full-text query, carrying a `QueryTokenKind` and the original lexeme
- `QueryTokenKind` - Token classification: `Term`, `Phrase`, `And`, `Or`, `Not`, `Near`, `LParen`, `RParen`
- `QueryValidationError` - Error enum: `EmptyQuery`, `UnclosedPhrase`, `UnbalancedParentheses`, `ImplicitAnd`
- `MatchinfoFormatError` - Error enum for matchinfo format strings: `EmptyFormat`, `InvalidChar`, `ArithmeticOverflow`
- `OffsetEntry` - A single offset record with column, term, byte_offset, and byte_length fields
- `OffsetsParseError` - Error enum for offsets parsing: `InvalidFieldCount`, `InvalidInteger`

## Key Functions

- `extension_name()` - Returns `"fts3"`
- `supports_dialect(dialect)` - Returns whether the given FTS dialect is supported
- `parse_query(query)` - Tokenize and validate an FTS3/4 query string, rejecting implicit AND and unbalanced parentheses
- `validate_matchinfo_format(format)` - Validate a matchinfo format string against the allowed character set (`p`, `c`, `n`, `a`, `l`, `s`, `x`)
- `matchinfo_u32_width(format, phrase_count, column_count)` - Compute the u32 output width for a matchinfo format string
- `parse_offsets(payload)` / `format_offsets(entries)` - Parse and format FTS3/4 offsets function output
- `is_matchinfo_format_char(ch)` - Check whether a character is valid in a matchinfo format string

## Dependencies

- `fsqlite-types`
- `fsqlite-error`
- `fsqlite-func`

## License

MIT
