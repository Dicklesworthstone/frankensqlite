# fsqlite-wal

Write-ahead logging implementation for fsqlite. This crate handles WAL file I/O, frame checksumming, checkpointing, group commit consolidation, WAL index (shared-memory) management, crash recovery, and forward error correction (FEC) for WAL frames.

## Overview

`fsqlite-wal` implements the WAL protocol that enables concurrent readers alongside a writer without blocking. Committed pages are appended as frames to a WAL file; checkpoint operations transfer those frames back to the main database. The crate provides extensive integrity checking (five levels), torn-write detection, and an optional RaptorQ-based FEC sidecar that can reconstruct damaged WAL frames from repair symbols.

The WAL crate depends on `fsqlite-vfs` for file I/O but does not depend on `fsqlite-pager` directly. Instead, the pager defines a `WalBackend` trait that an adapter in `fsqlite-core` implements by wrapping `WalFile` from this crate. This breaks the circular dependency.

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

### WAL File

- `WalFile` -- Core WAL file handle. Manages frame append, read-back, header parsing, and sync.

### Checksum and Integrity

- `WalHeader` / `WalFrameHeader` -- On-disk WAL and frame header structures.
- `SqliteWalChecksum` / `Xxh3Checksum128` -- Checksum algorithms (SQLite-compatible and XXH3-128).
- `integrity_check_*` functions -- Five levels of integrity checking: L1 page checksums, L2 B-tree structure, L3 overflow chains, L4 cross-reference, L5 schema validation.
- `detect_torn_write_in_wal` -- Torn-write detection using sector-size analysis.
- `WalChainValidation` / `validate_wal_chain` -- End-to-end WAL chain validation.

### Checkpointing

- `CheckpointMode` -- `Passive`, `Full`, `Restart`, `Truncate`.
- `CheckpointPlan` / `CheckpointState` / `CheckpointProgress` -- Checkpoint planning and execution state.
- `plan_checkpoint` / `execute_checkpoint` -- Plan and execute a checkpoint operation.

### Group Commit

- `GroupCommitConsolidator` / `GroupCommitConfig` -- Batches multiple transaction frame submissions into consolidated WAL writes for throughput.
- `FrameSubmission` / `TransactionFrameBatch` -- Individual and batched frame submissions.
- `write_consolidated_frames` -- Writes a batch of consolidated frames to the WAL.

### WAL Index (Shared Memory)

- `WalIndexHdr` / `WalCkptInfo` -- Shared-memory header and checkpoint info structures.
- `WalIndexHashSegment` -- Hash table segments for fast page-to-frame lookup.
- `parse_shm_header` / `write_shm_header` -- Read/write the WAL-index header in shared memory.
- `wal_index_hash_slot` -- Hash function for WAL index page lookups.

### Forward Error Correction (FEC)

- `WalFecRepairPipeline` / `WalFecRepairPipelineConfig` -- Pipeline for detecting and repairing damaged WAL frames using RaptorQ erasure coding.
- `WalFecGroupMeta` / `WalFecGroupRecord` -- FEC group metadata and recovery records.
- `generate_wal_fec_repair_symbols` -- Generate RaptorQ repair symbols for a commit group.
- `recover_wal_fec_group_with_config` -- Attempt FEC-based recovery of a damaged commit group.

### Recovery

- `WalRecoveryDecision` / `RecoveryAction` -- Recovery logic for checksum mismatches and corrupted frames.
- `recovery_compaction` (module) -- WAL compaction during recovery.

### Metrics

- `WalMetrics` / `GroupCommitMetrics` / `WalFecRepairCounters` / `WalRecoveryCounters` -- Global atomic counters with snapshot export.

## Usage

```rust
use fsqlite_wal::{
    WalHeader, WalFrameHeader, SqliteWalChecksum,
    compute_wal_frame_checksum, WAL_HEADER_SIZE, WAL_FRAME_HEADER_SIZE,
};

// Parse a WAL header from raw bytes.
let header_bytes = [0u8; WAL_HEADER_SIZE];
// ... read from file ...

// Compute a frame checksum (SQLite-compatible big-endian).
let frame_header = [0u8; WAL_FRAME_HEADER_SIZE];
let page_data = vec![0u8; 4096];
let (s0, s1) = compute_wal_frame_checksum(
    &frame_header,
    &page_data,
    (0, 0), // running checksum from previous frame
    true,   // big-endian (WAL_MAGIC_BE)
);
```

## Dependencies

- `fsqlite-types` -- Shared type definitions.
- `fsqlite-error` -- Unified error/result types.
- `fsqlite-vfs` -- File I/O abstraction.
- `xxhash-rust` -- XXH3 hashing.
- `crc32c` -- CRC32C checksums.
- `blake3` -- BLAKE3 content-address hashing.
- `tracing` -- Structured logging.
- `asupersync` -- Async-compatible synchronization primitives.

## License

MIT
