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

## Recover into a new database

The native `fsqlite-recover` binary connects WAL-FEC decoding to a standalone
recovered database, without opening an SQL connection on the damaged source:

```bash
cargo run --locked -p fsqlite-wal --bin fsqlite-recover -- damaged.db recovered.db
```

This is an explicit administrative recovery command, **not automatic recovery
on `Connection::open`**. Preserve the source main/WAL/FEC set before using any
SQL tool that might checkpoint it. The source header must still declare WAL
mode; rollback-mode sources are refused because a leftover WAL could be stale.
An existing `damaged.db-wal` is required;
repair symbols, when needed, come from `damaged.db-wal-fec`. Native capture uses
the existing main/WAL recovery fences and a shared sidecar mutation guard.
Active reader/writer contention fails without waiting. Source data is not
rewritten, although VFS admission can create lock/SHM companions and therefore
requires a writable source namespace. Use cooperative trusted directories;
external pathname replacement or raw writes that bypass VFS locks are outside
this contract. Source locks are released before decoding and exporting.

The output must not exist or have old recovery companions. The command reserves
it with exclusive creation, writes and synchronizes the body behind an invalid
header, then writes and synchronizes the final header, verifies the output by
streaming byte-for-byte readback, and synchronizes the parent where the VFS
supports it. No overwrite or automatic deletion is performed.
Do not open or manipulate the destination until the command reports success.
On an error after destination creation, the candidate is retained and its
completion is not certified. A success reports page/frame counts and the output
BLAKE3 digest from readback; it does not certify B-tree integrity of untouched source pages.
Run `PRAGMA integrity_check` against the **output** before using it.

Recovery can restore terminal page numbers, commit sizes and salts when the
original rolling checksum survives. When that checksum is damaged too, a later
original WAL frame or a later repaired group's original terminal checksum can
anchor the preceding reconstructed chain. Such repairs remain tentative until
that independent checksum matches; a successful payload decode alone is not
enough. A chain with no surviving anchor is never exported as a complete database.

When no original checksum survives, the command also captures an optional
`damaged.db-wal-cert` under the same native recovery fence. A current-format
durable certificate can validate the final recovered transaction only when its
nonzero database identity matches the **captured main header**, its WAL generation
matches, and its ordered BLAKE3 payload digest covers the **entire** tentative
repair interval from a transaction boundary through the final commit. A later
certificate cannot validate earlier frames outside its own interval. Exact
duplicate records are harmless; contradictory eligible records are refused.
Certificates cannot supply absent WAL frames or missing FEC data. Legacy/zero
identities, malformed or torn certificate streams, and oversized proof streams
provide no certificate authority; original-checksum recovery remains available.
Certificate filesystem access/identity failures are errors, not permission to
continue with a potentially incoherent capture. The success report includes the
number of certificate-validated intervals. No certificate or source data is rewritten.

Recovery refuses unresolved corruption, unanchored commit chains, partial
WAL tails, ambiguous FEC groups, mismatched headers, and unexplained missing
pages. It does not silently substitute a shortened WAL prefix: the main file
may already contain newer checkpointed pages. The materializer applies latest
committed page versions, honors shrink/regrowth boundaries, preserves page-one
metadata and WAL mode, and does not copy the old WAL or SHM into the destination.
This is not recovery of unrelated main-file B-tree corruption.

Defaults bound main/output to 256 MiB, WAL to 64 MiB, FEC and certificates each
to 32 MiB, the certificate stream to 4,096 records, and each
decode to 256 source pages. `--max-bytes N` changes the per-file/output bound;
`--max-source-pages N` changes the decode source bound. These are admission
limits, not a process-RSS guarantee. Use `--help` for the full contract.

Library callers with already-coherent immutable snapshots can use
`wal_fec::replay::recover_wal_fec_image` followed by
`WalFecReplayResult::database_image`. These functions perform no filesystem I/O.
Callers with a coherently captured certificate stream can instead use
`recover_wal_fec_image_with_certificates`; supply the identity from the main
header, never from the certificate or the reconstructed WAL. Accepted authority
intervals are available through `WalFecReplayResult::certificate_anchors`.

The inline command tests cover real encoded FEC input, native lock contention,
source preservation, output refusal, cancellation, and publication failures.
Execute them explicitly through the approved build route:

```bash
cargo test --locked -p fsqlite-wal --bin fsqlite-recover
cargo test --locked -p fsqlite-wal --lib wal_fec::replay
```

These Rust tests were added but were **not executed in the authoring environment**.
The separately executed SQLite image-oracle model is not a Rust, native-lock,
RaptorQ-decoder, or cross-platform runtime acceptance result.

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
