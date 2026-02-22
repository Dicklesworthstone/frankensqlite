# fsqlite-pager

Page cache, journal management, and the MVCC-aware pager interface for fsqlite. This crate sits between the VFS layer below and the B-tree and WAL layers above, providing transactional page-level storage with encryption support.

## Overview

`fsqlite-pager` defines the core abstractions that the B-tree engine and VDBE consume for reading and writing database pages. It manages an in-process page cache, rollback journal I/O, page-level encryption, and exposes sealed traits (`MvccPager`, `TransactionHandle`, `CheckpointPageWriter`) that encode MVCC safety invariants. Higher layers never touch files directly -- they go through the pager.

The crate also provides the `WalBackend` trait, an open interface that breaks the circular dependency between the pager and WAL: it is defined here but implemented by an adapter in `fsqlite-core` that wraps `fsqlite-wal::WalFile`.

**Position in the dependency graph:**

```
   fsqlite-vfs
        |
   fsqlite-pager        <-- you are here
      /    \
fsqlite-wal  fsqlite-btree
      \    /
   fsqlite-mvcc
```

## Key Types

### Traits (sealed -- only this crate can implement them)

- `MvccPager` -- Primary page-level storage interface. Supports `begin` (create a transaction), `journal_mode` / `set_journal_mode`, and `set_wal_backend`. Consumed by the B-tree layer and VDBE.
- `TransactionHandle` -- Handle to an active MVCC transaction. Provides `get_page`, `write_page`, `allocate_page`, `free_page`, `commit`, `rollback`, and savepoint operations. Page resolution follows the chain: write-set -> version chain -> disk.
- `CheckpointPageWriter` -- Write-back interface used during WAL checkpointing to transfer frames to the database file.

### Traits (open -- implementable by downstream crates)

- `WalBackend` -- Backend for WAL operations (`begin_transaction`, `append_frame`, `read_page`, `sync`, `checkpoint`). Defined here to avoid a pager<->WAL circular dependency.

### Enums

- `TransactionMode` -- `Deferred`, `Immediate`, `Exclusive`, `Concurrent`, `ReadOnly`.
- `JournalMode` -- `Delete` (rollback journal) or `Wal` (write-ahead log).
- `CheckpointMode` -- `Passive`, `Full`, `Restart`, `Truncate`.

### Page Cache

- `PageCache` / `PageCacheMetricsSnapshot` -- The main page cache with hit/miss tracking.
- `ArcCache` / `CachedPage` / `CacheLookup` -- ARC (Adaptive Replacement Cache) eviction policy.
- `S3Fifo` / `S3FifoConfig` -- S3-FIFO eviction policy with small/main/ghost queues and rollout gating.
- `PageBuf` / `PageBufPool` -- Pooled, aligned page buffers.

### Pager Implementations

- `SimplePager` / `SimpleTransaction` -- Single-writer pager with rollback journal and optional WAL support.
- `SimplePagerCheckpointWriter` -- Checkpoint writer for `SimplePager`.

### Encryption

- `PageEncryptor` -- ChaCha20-Poly1305 page encryption with per-page nonces.
- `KeyManager` -- Key derivation via Argon2 with database-ID binding.

### Journal

- `JournalHeader` / `JournalPageRecord` -- Rollback journal format structures.
- `journal_checksum` -- Checksum computation for journal integrity.

### Mocks (exported for cross-crate testing)

- `MockMvccPager`, `MockTransaction`, `MockCheckpointPageWriter`.

## Usage

```rust
use fsqlite_pager::{MvccPager, TransactionHandle, TransactionMode, MockMvccPager};
use fsqlite_types::cx::Cx;
use fsqlite_types::PageNumber;

let cx = Cx::new();
let pager = MockMvccPager;

// Begin a transaction.
let mut txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();

// Read a page.
let page_no = PageNumber::new(1).unwrap();
let data = txn.get_page(&cx, page_no).unwrap();

// Write a page.
txn.write_page(&cx, page_no, &[0u8; 4096]).unwrap();

// Allocate a new page.
let new_page = txn.allocate_page(&cx).unwrap();

// Commit.
txn.commit(&cx).unwrap();
```

## Dependencies

- `fsqlite-types` -- Shared type definitions (`PageNumber`, `PageData`, `Cx`).
- `fsqlite-error` -- Unified error/result types.
- `fsqlite-vfs` -- File I/O abstraction.
- `parking_lot` -- Fast mutexes and reader-writer locks.
- `xxhash-rust` -- Fast hashing for cache keys and checksums.
- `chacha20poly1305` -- AEAD encryption for at-rest page encryption.
- `argon2` -- Password-based key derivation.

## License

MIT
