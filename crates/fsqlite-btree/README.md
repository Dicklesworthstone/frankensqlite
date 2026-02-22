# fsqlite-btree

B-tree storage engine for fsqlite. This crate implements the on-disk B-tree data structures that organize database tables and indexes, providing cursor-based navigation and mutation through a sealed trait interface.

## Overview

`fsqlite-btree` is the storage engine layer that the VDBE uses to read and write rows. It implements both SQLite B-tree variants:

- **Table B-trees (intkey):** Keyed by `i64` rowid, leaf pages store serialized record payloads.
- **Index B-trees (blobkey):** Keyed by arbitrary byte sequences, leaf pages are key-only.

The crate provides cursor-based traversal (`first`, `last`, `next`, `prev`, `seek`), point mutations (`insert`, `delete`), and tree rebalancing. It also includes several advanced indexing structures: a write-optimized Be-tree (buffered epsilon tree), adaptive learned indexes, database cracking, and pointer swizzling for hot-path optimization.

All page access goes through `fsqlite-pager` traits, so the B-tree engine is agnostic to whether pages come from the page cache, WAL, or MVCC version chains.

**Position in the dependency graph:**

```
   fsqlite-vfs
        |
   fsqlite-pager
      /    \
fsqlite-wal  fsqlite-btree    <-- you are here
      \    /
   fsqlite-mvcc
```

## Key Types

### Cursor Trait (sealed)

- `BtreeCursorOps` -- The primary interface consumed by the VDBE. Supports seek (`index_move_to`, `table_move_to`), navigation (`first`, `last`, `next`, `prev`), mutation (`index_insert`, `table_insert`, `delete`), and access (`payload`, `rowid`, `eof`). Sealed to enforce MVCC safety invariants.
- `SeekResult` -- Result of a seek: `Found` (exact match) or `NotFound` (cursor at successor position).
- `MockBtreeCursor` -- Test mock with pre-seeded entries, exported for cross-crate testing.

### Cursor Implementation

- `BtCursor` -- The real B-tree cursor implementation. Bound to a single transaction and root page.
- `TransactionPageIo` / `PageReader` / `PageWriter` -- Page I/O adapters that connect the cursor to the pager.
- `MemPageStore` -- In-memory page store for unit testing.

### Cell and Page Layout

- `BtreePageHeader` / `BtreePageType` -- On-disk page header parsing (leaf table, interior table, leaf index, interior index).
- `CellRef` -- Reference to a single cell within a B-tree page.
- `read_cell_pointers` / `write_cell_pointers` -- Cell pointer array serialization.
- `local_payload_size` / `max_local_payload` / `min_local_payload` -- Payload spill threshold computation.
- `has_overflow` -- Check whether a cell spills to overflow pages.

### Overflow and Payload

- `overflow` (module) -- Overflow page chain reading and writing for large payloads.
- `payload` (module) -- Payload serialization and deserialization.

### Tree Balancing

- `balance` (module) -- B-tree rebalancing (split and merge) after insertions and deletions.
- `freelist` (module) -- Free page list management.

### Advanced Indexing

- `BeTree` / `BeTreeConfig` -- Write-optimized buffered epsilon tree. Batches updates in internal node buffers and flushes them down lazily.
- `LearnedIndex` / `LearnedIndexConfig` -- Learned index structures that use key distribution models to predict page positions, reducing binary search steps.
- `CrackedColumn` -- Database cracking: incrementally partitions data in response to query predicates for adaptive physical layout.

### Pointer Swizzling

- `SwizzlePtr` / `SwizzleState` / `PageTemperature` -- Replaces on-disk page numbers with direct memory pointers for frequently accessed ("hot") pages, eliminating hash-table lookups on the critical path.

### Cooling

- `CoolingStateMachine` / `CoolingConfig` -- Manages the transition of hot swizzled pointers back to cold page numbers when memory pressure increases.

### Instrumentation

- `BtreeMetricsSnapshot` / `BtreeOpType` -- Per-operation counters (seek, next, insert, delete) with snapshot export.

### Key Comparison

- `compare_key_bytes_contiguous` -- Hot-path byte-slice comparison with sequential memory access and no pointer chasing.

## Usage

```rust
use fsqlite_btree::{BtreeCursorOps, MockBtreeCursor, SeekResult};
use fsqlite_types::cx::Cx;

let cx = Cx::new();

// Create a cursor with some pre-seeded table entries.
let entries = vec![
    (1, b"alice".to_vec()),
    (2, b"bob".to_vec()),
    (3, b"charlie".to_vec()),
];
let mut cursor = MockBtreeCursor::new(entries);

// Navigate to the first entry.
assert!(cursor.first(&cx).unwrap());
assert_eq!(cursor.rowid(&cx).unwrap(), 1);
assert_eq!(cursor.payload(&cx).unwrap(), b"alice");

// Seek by rowid.
let result = cursor.table_move_to(&cx, 2).unwrap();
assert_eq!(result, SeekResult::Found);
assert_eq!(cursor.payload(&cx).unwrap(), b"bob");

// Insert a new row.
cursor.table_insert(&cx, 4, b"dave").unwrap();
```

## Dependencies

- `fsqlite-types` -- Shared type definitions.
- `fsqlite-error` -- Unified error/result types.
- `fsqlite-pager` -- Page-level storage interface (traits and page buffers).
- `tracing` -- Structured logging.
- `hashbrown` -- Fast hash maps for internal bookkeeping.

## License

MIT
