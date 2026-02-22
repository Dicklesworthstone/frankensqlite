# fsqlite-ext-misc

Miscellaneous extensions for FrankenSQLite: generate_series, decimal arithmetic, and UUID generation.

## Overview

This crate bundles three independent extension families that do not warrant their own crates:

1. **generate_series(START, STOP [, STEP])**: A table-valued function (virtual table) that generates a sequence of integers, commonly used in joins and CTEs for generating test data or numeric ranges.

2. **Decimal arithmetic**: Exact string-based decimal operations that avoid floating-point precision loss. Functions include `decimal` (normalize), `decimal_add`, `decimal_sub`, `decimal_mul`, and `decimal_cmp`.

3. **UUID generation**: `uuid()` generates random UUID v4 strings, `uuid_str` converts a 16-byte blob to a UUID string, and `uuid_blob` converts a UUID string to a 16-byte blob.

This crate depends on `fsqlite-types`, `fsqlite-error`, `fsqlite-func` (for scalar function and virtual table traits), and `tracing`.

## Key Types

- `GenerateSeriesTable` - Virtual table implementation for `generate_series()` (implements `VirtualTable`)
- `GenerateSeriesCursor` - Cursor for iterating over a generated integer series (implements `VirtualTableCursor`)
- `DecimalFunc` - Scalar function to normalize a decimal string to canonical form
- `DecimalAddFunc` - Scalar function for exact decimal addition
- `DecimalSubFunc` - Scalar function for exact decimal subtraction
- `DecimalMulFunc` - Scalar function for exact decimal multiplication
- `DecimalCmpFunc` - Scalar function for exact decimal comparison
- `UuidFunc` - Scalar function generating random UUID v4 strings
- `UuidStrFunc` - Scalar function converting a 16-byte blob to a UUID string
- `UuidBlobFunc` - Scalar function converting a UUID string to a 16-byte blob

## Key Functions

- `extension_name()` - Returns `"misc"`
- `register_misc_scalars(registry)` - Register all miscellaneous scalar functions (decimal and UUID) into a function registry

## Dependencies

- `fsqlite-types`
- `fsqlite-error`
- `fsqlite-func`
- `tracing`

## License

MIT
