# Critical Invariant Catalog

This document catalogs the critical invariants that FrankenSQLite must maintain for correctness. Each invariant has:
- **Definition**: What the invariant states
- **Owner**: Crate responsible for enforcement
- **Pass Signal**: How test coverage verifies the invariant
- **Evidence Hooks**: Test files/functions that exercise the invariant

---

## MVCC Invariants (fsqlite-mvcc)

### INV-1: TxnId/CommitSeq Monotonicity

**Definition**: `TxnId` and `CommitSeq` values are strictly monotonically increasing. No two transactions share the same `TxnId`. No two commits share the same `CommitSeq`.

**Owner**: `fsqlite-mvcc::invariants::TxnManager`

**Pass Signal**:
- `TxnManager::alloc_txn_id()` returns strictly increasing values
- CAS loop with `SeqCst` ordering guarantees thread-safety
- Exhaustion returns `None` rather than wrapping

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv1_txnid_monotonic_cas_loop`
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv1_txnid_exhaustion`
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv1_commit_seq_monotonic`
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv1_txnid_multithreaded_monotonicity`

---

### INV-2: Page Lock Exclusivity

**Definition**: At most one active transaction holds the exclusive lock on any given page at any time. Lock acquisition is non-blocking (immediate `SQLITE_BUSY` on conflict).

**Owner**: `fsqlite-mvcc::core_types::InProcessPageLockTable`

**Pass Signal**:
- `try_acquire()` returns `Err(holder_txn_id)` if already locked by another transaction
- Same transaction can re-acquire (idempotent)
- Release allows other transactions to acquire

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv2_page_lock_exclusivity`
- `crates/fsqlite-harness/tests/bd_2npr_mvcc_concurrent_writer_stress.rs`

---

### INV-3: Version Chain Order

**Definition**: In every version chain, newer versions have strictly higher `commit_seq` values. The chain is traversed from head (most recent) to tail (oldest).

**Owner**: `fsqlite-mvcc::invariants::VersionStore`

**Pass Signal**:
- `walk_chain()` returns versions in descending `commit_seq` order
- `publish()` links new versions at chain head

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv3_version_chain_descending`

---

### INV-4: Write Set Consistency

**Definition**: Every page in a transaction's write set must have its lock held before the page is modified.

**Owner**: `fsqlite-mvcc::core_types::Transaction`

**Pass Signal**:
- For all `p in write_set`: `p in page_locks`
- Violations trigger assertion failure

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv4_write_set_requires_lock`

---

### INV-5: Snapshot Stability

**Definition**: Once a transaction's snapshot is established, it cannot change. For DEFERRED transactions, the snapshot is provisional until the first read operation.

**Owner**: `fsqlite-mvcc::core_types::Transaction`

**Pass Signal**:
- `snapshot_established` flag transitions `false → true` exactly once
- After establishment, `snapshot.high` is immutable

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv5_deferred_snapshot_provisional`

---

### INV-6: Commit Atomicity

**Definition**: All pages in a transaction's write set become visible at the same `CommitSeq`, or none do. There is no partial visibility.

**Owner**: `fsqlite-mvcc::invariants::VersionStore`

**Pass Signal**:
- Before commit: no pages visible at snapshot `< commit_seq`
- After commit: all pages visible at snapshot `>= commit_seq`

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv6_commit_atomicity_all_visible_or_none`

---

### INV-7: Serialized Mode Exclusivity

**Definition**: In Serialized mode, the global write mutex ensures at most one writing transaction at a time (legacy SQLite behavior).

**Owner**: `fsqlite-mvcc::invariants::SerializedWriteMutex`

**Pass Signal**:
- `try_acquire()` fails when mutex is held by another transaction
- Release allows next waiter to proceed

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv7_serialized_write_mutex_exclusivity`

---

## Durability Invariants (fsqlite-wal)

### INV-D1: WAL Frame Integrity

**Definition**: Every WAL frame carries a cumulative checksum that chains from the previous frame. A single bit flip anywhere is detectable.

**Owner**: `fsqlite-wal::wal`

**Pass Signal**:
- Checksum validation on frame read
- Corruption detected before data returned to caller

**Evidence Hooks**:
- `crates/fsqlite-harness/tests/bd_2fas_wal_checksum_chain_recovery_compliance.rs`
- `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_bitflip_detected`

---

### INV-D2: Crash Recovery Completeness

**Definition**: After crash recovery, the database reflects exactly the set of committed transactions (those with commit markers in WAL). No partial commits, no phantom commits.

**Owner**: `fsqlite-wal::recovery_compaction`

**Pass Signal**:
- Recovery replays only frames up to last commit boundary
- Database state matches pre-crash committed state

**Evidence Hooks**:
- `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_recovery_*`
- `crates/fsqlite-core/src/commit_repair.rs` tests

---

### INV-D3: RaptorQ Self-Healing

**Definition**: WAL frames carry RaptorQ repair symbols. Torn writes and bit-flips within the repair budget are automatically corrected.

**Owner**: `fsqlite-wal::wal_fec`

**Pass Signal**:
- Corruption within R symbols repaired silently
- Corruption beyond R symbols reported as error

**Evidence Hooks**:
- `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_bitflip_repair`
- `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_symbol_loss_within_R`
- `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_symbol_loss_beyond_R`

---

## Concurrent Writer Invariants (fsqlite-core)

### INV-C1: Concurrent Mode Default

**Definition**: `BEGIN` promotes to `BEGIN CONCURRENT` by default. The `concurrent_mode_default` field in `Connection` MUST be `true`. This is the project's core innovation - disabling it defeats the purpose.

**Owner**: `fsqlite-core::connection::Connection`

**Pass Signal**:
- `concurrent_mode_default: RefCell::new(true)` in `Connection::new()`
- `HarnessSettings::default().concurrent_mode == true`
- `FsqliteExecConfig::default().concurrent_mode == true`

**Evidence Hooks**:
- `crates/fsqlite-core/src/connection.rs` (default value)
- `crates/fsqlite-e2e/src/lib.rs::HarnessSettings::default()`
- `crates/fsqlite-e2e/src/fsqlite_executor.rs::FsqliteExecConfig::default()`

---

### INV-C2: No Wait-For Cycles

**Definition**: Deadlocks are impossible by construction. Lock acquisition is non-blocking - immediate failure on conflict, no waiting.

**Owner**: `fsqlite-mvcc::core_types::InProcessPageLockTable`

**Pass Signal**:
- No `wait()` or `block()` calls in lock acquisition path
- `try_acquire()` returns immediately with success or `SQLITE_BUSY`

**Evidence Hooks**:
- `crates/fsqlite-mvcc/src/invariants.rs::test_inv2_page_lock_exclusivity`

---

### INV-C3: Per-Core WAL Buffer Lane Safety (`bd-ncivz.1`)

**Definition**: Per-core WAL buffering enforces deterministic double-buffer lane transitions (`Writable -> Sealed -> Flushing -> Writable`) and deterministic fallback when overflow exceeds budget.

**Owner**: `fsqlite-wal::per_core_buffer` (prototype contract for `bd-ncivz.*`)

**Pass Signal**:
- invalid lane transitions fail closed (no partial mutation)
- overflow policy is explicit (`BlockWriter` or `AllocateOverflow`)
- fallback latch deterministically triggers serialized drain on overflow budget breach
- one-writer-per-core prototype run shows zero lock contention

**Evidence Hooks**:
- `crates/fsqlite-wal/src/per_core_buffer.rs::bd_ncivz_1_state_machine_double_buffering`
- `crates/fsqlite-wal/src/per_core_buffer.rs::bd_ncivz_1_overflow_block_writer_policy`
- `crates/fsqlite-wal/src/per_core_buffer.rs::bd_ncivz_1_overflow_allocate_triggers_deterministic_fallback`
- `crates/fsqlite-wal/src/per_core_buffer.rs::bd_ncivz_1_per_core_pool_concurrent_writers_no_contention`
- `docs/design/per-core-wal-buffer-architecture.md`

---

## Schema Invariants (fsqlite-core)

### INV-S1: Schema Epoch Consistency

**Definition**: Schema changes increment `SchemaEpoch`. Transactions with stale schema epochs are invalidated on commit.

**Owner**: `fsqlite-core::connection`

**Pass Signal**:
- DDL operations increment schema epoch
- Transactions detect schema changes via epoch mismatch

**Evidence Hooks**:
- `crates/fsqlite-core/src/connection.rs` schema tests

---

## Type System Invariants (fsqlite-types)

### INV-T1: PageNumber Validity

**Definition**: A `PageNumber` cannot be zero (page 0 does not exist in SQLite format). Construction validates this.

**Owner**: `fsqlite-types::PageNumber`

**Pass Signal**:
- `PageNumber::new(0)` returns `None`
- All `PageNumber` values are >= 1

**Evidence Hooks**:
- `crates/fsqlite-types/src/lib.rs` PageNumber tests

---

### INV-T2: PageSize Power of Two

**Definition**: `PageSize` must be a power of two between 512 and 65536 bytes.

**Owner**: `fsqlite-types::PageSize`

**Pass Signal**:
- Construction rejects non-power-of-two values
- Construction rejects values outside range

**Evidence Hooks**:
- `crates/fsqlite-types/src/lib.rs` PageSize tests

---

## Updating This Catalog

When adding new invariants:

1. Define the invariant with clear pass/fail criteria
2. Identify the owning crate/module
3. Write tests that exercise both success and failure cases
4. Add evidence hooks (test file:function paths)
5. Update this document

## Related Documents

- [ADR-0001: Coverage Toolchain Selection](adr/0001-coverage-toolchain-selection.md)
- [Test Realism Inventory](test-realism/README.md)
