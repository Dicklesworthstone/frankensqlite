# fsqlite-mvcc

Multi-version concurrency control (MVCC) for fsqlite, enabling multiple concurrent writers with page-level versioning, serializable snapshot isolation (SSI), and cross-process coordination.

## Overview

`fsqlite-mvcc` is the concurrency control layer that sits atop the pager, WAL, and B-tree crates. It implements page-level MVCC so that multiple transactions can read and write concurrently without global locking. Conflicts are detected at commit time using SSI validation and a First-Committer-Wins protocol. When possible, conflicting writes are resolved automatically through a deterministic merge ladder (intent replay, structured page patches, XOR deltas) instead of aborting.

The crate also provides the infrastructure for `BEGIN CONCURRENT` semantics, epoch-based reclamation (EBR) for safe garbage collection of old page versions, shared-memory coordination for cross-process operation, two-phase commit for multi-database transactions, time-travel snapshots, and differential privacy primitives.

**Position in the dependency graph:**

```
   fsqlite-vfs
        |
   fsqlite-pager
      /    \
fsqlite-wal  fsqlite-btree
      \    /
   fsqlite-mvcc          <-- you are here
        |
   fsqlite-core
```

## Key Types

### Transaction Lifecycle

- `TransactionManager` -- Orchestrates begin/read/write/commit/abort for both serialized and concurrent modes.
- `BeginKind` -- Transaction modes: `Deferred`, `Immediate`, `Exclusive`, `Concurrent`.
- `CommitResponse` -- Result of the commit sequencer.
- `Savepoint` -- B-tree-level page state snapshot within a transaction.
- `MvccError` -- MVCC-specific error variants (busy, conflict, stale snapshot).

### Core Runtime (Version Store)

- `VersionArena` / `VersionIdx` -- Chunked arena allocator for page versions with generation-based ABA protection.
- `VersionStore` / `ChainHeadTable` -- Lock-free version chain storage. Each page has a chain of versions; readers walk the chain to find the version visible to their snapshot.
- `InProcessPageLockTable` -- Sharded page-level lock table for write conflict detection.
- `CommitLog` / `CommitIndex` / `CommitRecord` -- Append-only commit log mapping commit sequence numbers to transaction metadata.
- `Transaction` / `TransactionState` / `TransactionMode` -- Per-transaction runtime state.
- `TxnManager` / `SerializedWriteMutex` -- Transaction slot management and serialized-writer coordination.

### BEGIN CONCURRENT

- `ConcurrentRegistry` / `ConcurrentHandle` -- Registration and management of concurrent writer sessions.
- `concurrent_commit_with_ssi` -- Commit path for concurrent transactions with full SSI validation.
- `validate_first_committer_wins` -- First-Committer-Wins conflict check.
- `ConcurrentSavepoint` -- Savepoint support within concurrent transactions.

### SSI Validation

- `SsiState` -- Per-transaction SSI bookkeeping (read set, write set, dependency edges).
- `ssi_validate_and_publish` -- Core SSI validation: discovers rw-dependency edges and checks for dangerous structures.
- `discover_incoming_edges` / `discover_outgoing_edges` -- Dependency graph edge discovery.
- `SsiAbortReason` -- Why a transaction was aborted (rw-conflict, write-skew, etc.).

### SSI Abort Policy

- `SsiEvidenceLedger` / `SsiDecisionCard` -- Evidence-based abort policy using historical conflict data.
- `select_victim` -- Victim selection for multi-transaction conflicts using cost matrices.
- `ConformalCalibrator` -- Online calibration of abort thresholds using conformal prediction.

### Conflict Resolution (Merge Ladder)

- `deterministic_rebase` -- Replays intent-level operations against a newer base version.
- `physical_merge` / `StructuredPagePatch` -- Structured B-tree page diffing and merging.
- `xor_delta` / `SparseXorDeltaObject` -- Compact delta encoding for page versions using sparse XOR runs.

### Witness System

- `WitnessSet` / `WitnessKey` -- Tracks which pages a transaction read (read witnesses) and wrote (write witnesses) for SSI validation.
- `HotWitnessIndex` -- Cache-friendly index for high-frequency witness lookups.
- `WitnessPublisher` / `ProofCarryingCommit` -- Proof-carrying commit protocol where each commit publishes cryptographic evidence of its read/write set.
- `witness_refinement` -- Value-of-information-based refinement of coarse witness keys into precise ones.

### Epoch-Based Reclamation (EBR) and GC

- `VersionGuardRegistry` / `VersionGuard` -- Epoch-based reader registration for safe old-version reclamation.
- `QsbrRegistry` / `QsbrHandle` -- Quiescent-state-based reclamation (QSBR) for lock-free data structures.
- `GcScheduler` / `gc_tick` / `prune_page_chain` -- Background garbage collection of unreachable page versions.
- `RcuCell` / `RcuPair` / `RcuTriple` -- Read-copy-update primitives.

### Concurrency Primitives

- `SeqLock` -- Sequence lock for fast read-side access to small shared state.
- `LeftRight` -- Left-right concurrency primitive for wait-free reads.
- `FlatCombiner` -- Flat combining for serializing concurrent operations through a combiner thread.
- `CacheAligned` / `TxnSlotArray` / `RecentlyCommittedReadersIndex` -- Cache-line-aligned transaction slot management.

### Shared Memory and IPC

- `SharedMemoryLayout` / `ShmSnapshot` -- Cross-process shared memory layout for MVCC coordination.
- `coordinator_ipc` (module) -- Unix domain socket IPC for the write coordinator.
- `WriteCoordinator` / `CoordinatorMode` -- Centralized write coordination with spill-to-disk support for large write sets.

### History Compression

- `CompressedPageHistory` / `compress_page_history` -- Compresses version chains by merging independent operations.
- `MergeCertificate` / `generate_merge_certificate` -- Cryptographic certificates proving merge correctness.

### Conflict Modeling

- `AmsSketch` / `NitroSketch` -- Approximate sketches for estimating write-set overlap and conflict probability.
- `birthday_conflict_probability_uniform` / `pairwise_conflict_probability` -- Analytical conflict probability models.
- `SpaceSavingSummary` -- Heavy-hitter detection for hot pages.

### Two-Phase Commit

- `TwoPhaseCoordinator` / `TwoPhaseState` -- Coordinator for multi-database atomic commits.
- `GlobalCommitMarker` -- Durable commit markers for crash recovery of distributed transactions.

### Time Travel

- `create_time_travel_snapshot` / `resolve_page_at_commit` -- Read historical page versions at a given commit or timestamp.

### Observability

- `CasMetricsSnapshot` / `SsiMetricsSnapshot` / `SnapshotReadMetricsSnapshot` -- Atomic metric counters for CAS retries, SSI aborts, and snapshot read performance.
- `BocpdMonitor` -- Bayesian online changepoint detection for regime shifts in conflict rates.

### Retry Policy

- `RetryController` / `RetryAction` -- Adaptive retry policy using Gittins-index approximations and hazard models to decide between retry and fail-fast.

### Differential Privacy

- `DpEngine` / `PrivacyBudget` -- Differential privacy query engine with configurable noise mechanisms and privacy budget tracking.

### Row ID Allocation

- `ConcurrentRowIdAllocator` / `LocalRowIdCache` -- Lock-free row ID allocation with per-thread range reservations.

## Usage

```rust
use fsqlite_mvcc::{
    TransactionManager, BeginKind, CommitResponse,
    Transaction, TransactionMode, TransactionState,
    VersionStore, CommitLog, InProcessPageLockTable,
};

// The TransactionManager orchestrates the full lifecycle.
// In practice, it is constructed by fsqlite-core and wired to the
// pager, WAL, and B-tree layers. A simplified sketch:

// Begin a concurrent transaction.
// let txn = manager.begin(BeginKind::Concurrent)?;

// Read pages (recorded in the SSI read-witness set).
// let page = txn.read_page(page_no)?;

// Write pages (page locks acquired, write-witness recorded).
// txn.write_page(page_no, &new_data)?;

// Commit -- runs SSI validation, First-Committer-Wins, merge ladder.
// let response = manager.commit(txn)?;
```

## Dependencies

- `fsqlite-types` -- Shared type definitions (`TxnId`, `CommitSeq`, `Snapshot`, `PageVersion`, `WitnessKey`).
- `fsqlite-error` -- Unified error/result types.
- `fsqlite-observability` -- Global metric sinks.
- `fsqlite-pager` -- Page cache and pager traits.
- `fsqlite-wal` -- WAL frame operations and FEC repair symbols.
- `crossbeam-epoch` -- Epoch-based memory reclamation.
- `parking_lot` -- Fast mutexes and reader-writer locks.
- `smallvec` -- Stack-allocated small vectors.
- `serde` -- Serialization for snapshot export and IPC.
- `xxhash-rust` -- Fast hashing.
- `blake3` -- Cryptographic hashing for merge certificates and content addressing.
- `nix` -- Unix domain socket and process credential support.
- `tracing` -- Structured logging.

## License

MIT
