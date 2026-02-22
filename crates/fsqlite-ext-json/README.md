# fsqlite-ext-json

JSON1 functions and virtual tables for FrankenSQLite.

## Overview

This crate implements the SQLite JSON1 extension, providing JSON validation, parsing, extraction, mutation, formatting, and virtual table access. It supports the full JSON1 function surface including JSONB binary encoding/decoding, JSON5 validation, path-based extraction with SQLite semantics (single-path returns native SQL types, multi-path returns a JSON array), mutators (set, insert, replace, remove, patch), aggregate functions (group_array, group_object), and the `json_each`/`json_tree` virtual tables for flattening JSON structures.

Path syntax follows SQLite conventions: `$` (root), `$.key` (object member), `$."key.with.dots"` (quoted member), `$[N]` (array index), `$[#]` (append), `$[#-N]` (reverse index).

This crate depends on `fsqlite-types`, `fsqlite-error`, `fsqlite-func`, `serde_json`, and `json5`.

## Key Types

- `JsonTableRow` - A row from `json_each` or `json_tree`, containing key, value, type, atom, id, parent, fullkey, and path
- `JsonEachVtab` / `JsonEachCursor` - Virtual table and cursor for `json_each()` (flat iteration over JSON values)
- `JsonTreeVtab` / `JsonTreeCursor` - Virtual table and cursor for `json_tree()` (recursive iteration)
- `JsonFunc`, `JsonValidFunc`, `JsonTypeFunc`, `JsonExtractFunc` - Scalar function implementations for core JSON operations
- `JsonArrayFunc`, `JsonObjectFunc`, `JsonQuoteFunc` - Scalar constructors
- `JsonSetFunc`, `JsonInsertFunc`, `JsonReplaceFunc`, `JsonRemoveFunc`, `JsonPatchFunc` - Scalar mutators
- `JsonArrayLengthFunc`, `JsonErrorPositionFunc`, `JsonPrettyFunc` - Scalar utility functions

## Key Functions

- `json(input)` - Parse and minify JSON text
- `json_valid(input, flags)` - Validate JSON text under configurable flags (RFC-8259, JSON5, JSONB superficial, JSONB strict)
- `jsonb(input)` / `json_from_jsonb(input)` - Convert between JSON text and JSONB binary format
- `json_type(input, path)` - Return the JSON type name at root or a given path
- `json_extract(input, paths)` - Extract values by path with SQLite single/multi-path semantics
- `json_arrow(input, path)` / `json_double_arrow(input, path)` - `->` and `->>` operator equivalents
- `json_set`, `json_insert`, `json_replace`, `json_remove`, `json_patch` - In-place JSON mutation (with `jsonb_*` variants)
- `json_array`, `json_object`, `json_quote` - JSON value constructors
- `json_group_array`, `json_group_object` - Aggregate functions (with `jsonb_*` variants)
- `json_each(input, path)` / `json_tree(input, path)` - Flatten JSON into virtual table rows
- `json_pretty(input, indent)` - Pretty-print JSON with configurable indentation
- `json_error_position(input)` - Return the byte offset of the first JSON parse error
- `json_array_length(input, path)` - Return the length of a JSON array
- `register_json_scalars(registry)` - Register all JSON scalar functions into a function registry

## Dependencies

- `fsqlite-types`
- `fsqlite-error`
- `fsqlite-func`
- `serde_json`
- `json5`

## License

MIT
