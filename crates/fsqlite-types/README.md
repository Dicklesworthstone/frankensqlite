# fsqlite-types

Core type definitions for the FrankenSQLite database engine.

## Overview

`fsqlite-types` provides the shared vocabulary types used throughout the FrankenSQLite workspace. It defines page-level primitives, SQL values, transaction identifiers, b-tree structures, record serialization, MVCC glossary types, erasure coding (ECS) support, GF(256) arithmetic, and database limits.

This crate depends only on `fsqlite-error` and a small set of external crates (`bitflags`, `blake3`, `smallvec`, `serde`, `xxhash-rust`). It is a direct dependency of most other fsqlite crates.

```
fsqlite-error
  ^
  |
fsqlite-types
  ^
  |-- fsqlite-ast
  |-- fsqlite-func
  |-- fsqlite-observability
  |-- fsqlite-core, fsqlite-vfs, ...
  |-- fsqlite (facade)
```

## Modules

- `value` - `SqliteValue` enum (Integer, Float, Text, Blob, Null) for runtime SQL values.
- `glossary` - MVCC and transaction vocabulary: `TxnId`, `TxnToken`, `CommitSeq`, `Snapshot`, `PageVersion`, `IntentLog`, `RowId`, `RowIdAllocator`, `Saga`, and many more.
- `cx` - Capability context (`Cx`) for threading cancellation and trace context.
- `ecs` - Erasure coding symbol types: `SymbolRecord`, `ObjectId`, `PayloadHash`, systematic run layout/reconstruction/validation helpers.
- `record` - SQLite record format serialization/deserialization.
- `serial_type` - SQLite serial type encoding.
- `opcode` - VDBE opcode definitions.
- `flags` - Bitflag types for database open modes and configuration.
- `encoding` - Text encoding types.
- `limits` - Compile-time and runtime limit constants (max page size, max columns, etc.).
- `obligation` - Obligation tracking types.

## Key Types

- `PageNumber` - A 1-based page number backed by `NonZeroU32`. Page 0 does not exist in SQLite.
- `PageSize` - Database page size (power of two, 512..=65536, default 4096).
- `PageData` - Owned page byte buffer with `Arc`-backed copy-on-write.
- `SqliteValue` - Runtime SQL value enum (Integer, Float, Text, Blob, Null).
- `MergePageKind` - Page classification for merge-safety policy (leaf/interior table/index, overflow, freelist, etc.).
- `PageNumberHasher` / `PageNumberBuildHasher` - Zero-cost identity hasher for `PageNumber` keys in hash maps.
- `BTreePageType` - B-tree page type discriminant (LeafTable, InteriorTable, LeafIndex, InteriorIndex).
- `gf256_mul_byte`, `gf256_add_byte`, `gf256_inverse_byte` - GF(256) arithmetic primitives for RaptorQ encoding and XOR-delta compression.

## Usage

```rust
use fsqlite_types::{PageNumber, PageSize, PageData, SqliteValue};

// Page numbers are 1-based
let page = PageNumber::new(1).expect("page 1 is valid");
assert_eq!(page.get(), 1);

// Page sizes must be powers of two in [512, 65536]
let size = PageSize::new(4096).unwrap();
let data = PageData::zeroed(size);
assert_eq!(data.as_bytes().len(), 4096);

// SQL values
let val = SqliteValue::Integer(42);
let text = SqliteValue::Text("hello".to_owned());
```

## License

MIT
