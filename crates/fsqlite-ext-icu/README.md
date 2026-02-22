# fsqlite-ext-icu

ICU Unicode collation and case mapping extension for FrankenSQLite.

## Overview

This crate implements the ICU (International Components for Unicode) extension, providing locale-aware collation, case mapping, and word-break tokenization that go beyond SQLite's ASCII-only built-in functions. It handles Unicode edge cases such as Turkish dotted/dotless I, German sharp-s (eszett) uppercasing, and Greek final sigma lowercasing. The word-break tokenizer supports CJK languages where words are not space-delimited.

This crate depends on `fsqlite-types`, `fsqlite-error`, `fsqlite-func` (for scalar function and collation registration), and `tracing`.

## Key Types

- `IcuLocale` - Parsed ICU locale identifier (e.g. `de_DE`, `zh_CN`, `tr_TR`) with language and optional country code; supports both `_` and `-` separators
- `IcuUpperFunc` - Scalar function implementing locale-aware `icu_upper()` (uppercase conversion)
- `IcuLowerFunc` - Scalar function implementing locale-aware `icu_lower()` (lowercase conversion)
- `IcuCollation` - A named collation using Unicode Collation Algorithm (UCA) rules for a given locale
- `IcuLoadCollationFunc` - Scalar function for `icu_load_collation(locale, name)` that registers a locale-aware collation at runtime
- `IcuWordBreaker` - ICU word-boundary tokenizer for FTS3/4/5, using UAX #29 rules for locale-aware word segmentation

## Key Functions

- `extension_name()` - Returns `"icu"`
- `register_icu_scalars(registry)` - Register `icu_upper` and `icu_lower` scalar functions
- `register_icu_load_collation(registry, collation_registry)` - Register the `icu_load_collation` function for creating locale-aware collations at runtime

## Dependencies

- `fsqlite-types`
- `fsqlite-error`
- `fsqlite-func`
- `tracing`

## License

MIT
