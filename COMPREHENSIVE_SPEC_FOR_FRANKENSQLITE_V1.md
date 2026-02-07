# COMPREHENSIVE SPECIFICATION FOR FRANKENSQLITE

> A clean-room Rust reimplementation of SQLite 3.52.0 with MVCC concurrent
> writers and RaptorQ-pervasive information-theoretic architecture.

---

## 0. How to Read This Document

This is the single authoritative specification for FrankenSQLite. It is
self-contained: every design decision, formal model, and implementation detail
needed to build the system is here. It supersedes and consolidates:

- `PROPOSED_ARCHITECTURE.md` (Rust design)
- `MVCC_SPECIFICATION.md` (concurrency model)
- `PLAN_TO_PORT_SQLITE_TO_RUST.md` (implementation phases)
- `EXISTING_SQLITE_STRUCTURE.md` (C SQLite behavior extraction)

Those documents remain in the repository for reference but this document is
the source of truth. Where they conflict, this document wins.

**Audience:** AI coding agents, human reviewers, and any collaborator who needs
the full picture of what FrankenSQLite is, why it exists, and exactly how to
build it.

### 0.1 Non-Negotiable Scope Doctrine

This specification describes the **complete target system**. There is no
"V1 scope" and no "we'll do it later" escape hatch. Every feature, protocol,
and subsystem described in this document is in scope for implementation. If
something is genuinely excluded, it appears in Section 15 (Exclusions) with a
technical rationale. Everything else MUST be built.

Implementation is phased (Section 16) for practical sequencing, not for
scope reduction. A feature being in Phase 9 does not make it optional -- it
means it depends on Phase 8 being complete.

### 0.2 Normative Language

This specification uses RFC 2119 / RFC 8174 keywords:

- **MUST** / **MUST NOT**: Absolute requirement or prohibition. Violation is a
  spec-conformance bug.
- **SHOULD** / **SHOULD NOT**: Strong recommendation. Deviation requires
  documented justification in code comments.
- **MAY**: Truly optional. Implementation can omit without justification.

Pseudocode and type definitions are normative unless explicitly labeled
"illustrative" or "example."

### 0.3 Glossary

| Term | Definition |
|------|-----------|
| **MVCC** | Multi-Version Concurrency Control. Transactions see a consistent snapshot while writers create new versions. |
| **SSI** | Serializable Snapshot Isolation. Extends SI to detect write skew via rw-antidependency tracking. |
| **ECS** | Erasure-Coded Stream. The universal persistence substrate: objects encoded as RaptorQ symbols. |
| **ObjectId** | Content-addressed identifier: `Trunc128(BLAKE3("fsqlite:ecs:v1" || canonical_object_header || payload_hash))`, 16 bytes. Canonical full digest is BLAKE3-256; the 128-bit truncation is for storage efficiency. Birthday-bound collision resistance is ~2^64 operations — sufficient for FrankenSQLite's expected object population (well under 2^40) but NOT equivalent to 128-bit security. |
| **CommitCapsule** | Atomic unit of commit state in Native mode: intent log, page deltas, SSI witnesses. |
| **CommitMarker** | The durable "this commit exists" record in Native mode: `(commit_seq, commit_time_unix_ns, capsule_object_id, proof_object_id, prev_marker, integrity_hash)`. |
| **CommitSeq** | Monotonically increasing `u64` commit sequence number (global "commit clock" for ordering). |
| **RaptorQ** | RFC 6330 fountain code: K source symbols → unlimited encoding symbols, recoverable from any K' ≈ K. |
| **OTI** | Object Transmission Information. RaptorQ metadata needed for decoding: (F, Al, T, Z, N). |
| **DecodeProof** | Auditable witness artifact produced by the RaptorQ decoder when repairing or failing to repair (lab/debug). |
| **Cx** | Capability context (asupersync). Threads cancellation via `is_cancel_requested()` / `checkpoint()`, progress via `checkpoint_with()`, budgets/deadlines via `Budget` and scoped budgets, and type-level restriction via `Cx::restrict::<NewCaps>()`. |
| **Budget** | Asupersync resource budget carried by `Cx`: `{ deadline: Option<Time>, poll_quota: u32, cost_quota: Option<u64>, priority: u8 }`. Combined via component-wise `combine`: deadline/poll/cost use `min` (tighter resource limit wins); priority uses `max` (higher priority propagates inward). Note: the priority `max` rule makes this a **product lattice with mixed meet/join**, not a pure meet-semilattice — deadline/poll/cost follow meet order (tighter = lower) while priority follows join order (higher = more urgent). Budget exhaustion requests cancellation. |
| **Outcome** | Asupersync 4-valued result lattice for concurrent tasks: `Ok < Err < Cancelled < Panicked`. Used for supervision and combinators; FrankenSQLite maps outcomes into SQLite error codes at API boundaries. |
| **EpochId** | Monotonically increasing `u64` epoch for distributed coordination and validity windows (asupersync `epoch`). Used for key rotation, tiered-storage policy transitions, and cross-process barriers. |
| **SymbolValidityWindow** | Epoch interval `[from_epoch, to_epoch]` used to accept/reject symbols/segments under key rotation and retention policies. |
| **RemoteCap** | Asupersync capability token required for remote tier (L3) fetch/upload or remote compute. |
| **SymbolAuthMasterKeyCap** | Capability token (via `Cx`) that provides the per-database secret material used to derive epoch-scoped symbol authentication keys when page encryption is disabled. Required to enable `PRAGMA fsqlite.symbol_auth = on` in non-encrypted deployments. |
| **IdempotencyKey** | Stable identifier used to deduplicate remote requests under retries (remote fetch/upload/compaction publish). |
| **Saga** | Structured multi-step operation with deterministic compensations; used for compaction and tier eviction so cancellation never leaves partial state. |
| **Region** | Asupersync structured concurrency scope: a tree of owned tasks. Region close implies quiescence (no live children, all finalizers run, all obligations resolved). |
| **PageNumber** | 1-based `NonZeroU32` identifying a database page. Page 1 is always the database header. |
| **TxnId** | Monotonically increasing `u64` transaction begin identifier (allocated at `BEGIN`). `TxnSlot.txn_id = 0` is reserved to mean "free slot", so real TxnIds are non-zero. |
| **TxnEpoch** | Monotonically increasing `u32` generation counter for a reused TxnSlot (prevents stale slot-id interpretation). |
| **TxnToken** | Canonical transaction identity for SSI witness plane: `(TxnId, TxnEpoch)`. |
| **SchemaEpoch** | Monotonically increasing `u64` epoch for schema/physical-layout changes (DDL/VACUUM). Captured at `BEGIN` and carried through intent logs to forbid replay/merge across schema boundaries. |
| **SIREAD witness (legacy term)** | PostgreSQL terminology for SSI read evidence ("SIREAD locks"). In FrankenSQLite this is represented by `ReadWitness` objects plus hot-plane reader bits; it does not block and is not a lock. |
| **Intent log** | Semantic operation log: `Vec<IntentOp>`. Records what a transaction intended to do (insert, delete, update). |
| **Deterministic rebase** | Replaying intent logs against the current committed snapshot to merge without byte-level patches. |
| **PageHistory** | Compressed version chain: newest = full image, older = patches (intent logs and/or structured patches). |
| **ARC** | Adaptive Replacement Cache. Balances recency and frequency for buffer pool eviction. |
| **RootManifest** | Bootstrap object in ECS: maps logical database name → current committed state ObjectId. |
| **TxnSlot** | Fixed-size shared-memory record for cross-process MVCC coordination. |
| **WitnessKey** | The canonical key-space for SSI read/write evidence: `Page(pgno)` or finer tags like `Cell(btree_root_pgno, tag)` and `ByteRange(page, start, len)`. |
| **RangeKey** | Hierarchical bucket key for witness indexing: `(level, hash_prefix)` in a prefix tree over `WitnessKey` hashes. |
| **ReadWitness** | ECS object: durable evidence of a transaction's reads over a `RangeKey` bucket (sound, no false negatives for its coverage claim). |
| **WriteWitness** | ECS object: durable evidence of a transaction's writes over a `RangeKey` bucket (sound, no false negatives for its coverage claim). |
| **WitnessIndexSegment** | ECS object: compacted readers/writers bitmap for a `RangeKey` bucket over a commit sequence range; rebuildable from deltas. |
| **DependencyEdge** | ECS object: rw-antidependency evidence edge `(from, to, key_basis, observed_by)`. Mandatory for explainable SSI. |
| **CommitProof** | ECS object: replayable proof-carrying artifact for a commit's SSI validation (witness refs + segments used + edges emitted). |
| **VersionPointer** | Stable, content-addressed pointer from page index to patch object: `(commit_seq, patch_object: ObjectId, patch_kind, base_hint)`. |

### 0.4 What "RaptorQ Everywhere" Means (No Weasel Words)

RaptorQ is not an "optional replication feature." It is the default substrate
for:

- **Durability objects:** commit capsules, markers, checkpoints.
- **Indexing objects:** index segments, locator segments, manifest segments.
- **Replication traffic:** symbols, not files.
- **Repair:** recover from partial loss/corruption by decoding, not by
  panicking.
- **History compression:** patch chains stored as coded objects, not infinite
  full-page copies.

If a subsystem persists or synchronizes bytes, it MUST specify how those bytes
are represented as ECS objects and how they are repaired/replicated (see the
RaptorQ Permeation Map in §3.5.7).

## Table of Contents

- 0. How to Read This Document
- 1. Project Identity
- 2. Why Page-Level MVCC
- 3. RaptorQ: The Information-Theoretic Foundation
- 4. Asupersync Deep Integration
- 5. MVCC Formal Model (Revised)
- 6. Buffer Pool: ARC Cache
- 7. Checksums and Integrity
- 8. Architecture: Crate Map and Dependencies
- 9. Trait Hierarchy
- 10. Query Pipeline
- 11. File Format Compatibility
- 12. SQL Coverage
- 13. Built-in Functions
- 14. Extensions
- 15. Exclusions (What We Are NOT Building)
- 16. Implementation Phases
- 17. Testing Strategy
- 18. Probabilistic Conflict Model
- 19. C SQLite Behavioral Reference
- 20. Key Reference Files
- 21. Risk Register, Open Questions, and Future Work
- 22. Verification Gates
- 23. Summary: What Makes FrankenSQLite Alien

---

## 1. Project Identity

### 1.1 What It Is

FrankenSQLite is a **clean-room Rust reimplementation** of SQLite version 3.52.0
(~238K lines of C in the amalgamation). **Note:** SQLite 3.52.0 is a forward
target (scheduled for release ~March 2026). If 3.52.0's final API surface
differs from this spec, the spec will be updated to match the release. All
references to "3.52.0" throughout this document denote this forward target.
It targets:

- Full SQL dialect compatibility with C SQLite
- File format round-trip interoperability (read/write standard `.sqlite` files)
- Safe Rust (`unsafe_code = "forbid"` at workspace level)
- **100% behavioral parity target** against a golden-file test suite (Oracle =
  C sqlite3). Any intentional divergence MUST be explicitly documented and
  annotated in the harness with rationale.

### 1.2 The Two Innovations

**Innovation 1: MVCC Concurrent Writers.** SQLite's single biggest limitation
is the `WAL_WRITE_LOCK` in `wal.c` (function `sqlite3WalBeginWriteTransaction`;
line numbers shift by SQLite version) -- a single exclusive lock byte that
serializes ALL writers. FrankenSQLite replaces this with page-level MVCC
versioning, allowing transactions that touch different pages to commit in full
parallel. This is the PostgreSQL concurrency model applied at page granularity.

**Innovation 2: RaptorQ-Pervasive Architecture.** Every layer of FrankenSQLite
is infused with RaptorQ fountain codes (RFC 6330), leveraging asupersync's
production-grade implementation. This isn't bolted-on replication -- it's woven
into the storage format, WAL durability, snapshot transfer, version chain
compression, and conflict resolution. The result is a database that treats data
loss as a quantitatively bounded, repairable event under an explicit failure
model rather than a silent corruption or a "panic and pray" failure mode.

### 1.3 Key External Dependencies

| Dependency | Location | Role |
|-----------|----------|------|
| `asupersync` | `/dp/asupersync` | Async runtime, RaptorQ codec, `Cx` capability contexts, structured concurrency (`Scope` + macros), lab runtime (deterministic scheduling, cancellation injection, chaos), oracles/e-process monitors, deadline monitoring, and trace/TLA export |
| `frankentui` | `/dp/frankentui` | TUI framework (CLI shell only) |

**No tokio.** All async I/O uses asupersync exclusively.

### 1.4 Constraints

- **Edition 2024**; nightly toolchain required (see `rust-toolchain.toml`) for
  asupersync and other nightly-only APIs
- **`unsafe_code = "forbid"`** -- no escape hatches
- **Clippy pedantic + nursery at deny level** -- with specific documented allows
- **23 crates** in workspace under `crates/`
- **Release profile** (as configured in the workspace `Cargo.toml`): `opt-level = "z"`,
  `lto = true`, `codegen-units = 1`, `panic = "abort"`, `strip = true`.
  For throughput benchmarking and perf work, use a separate `release-perf`
  profile that inherits from `release` but sets `opt-level = 3`.

**Engineering & Process Constraints (from `AGENTS.md`):**
- **User is in charge.** If the user overrides anything, follow the user.
- **No file deletion** without explicit written permission.
- **No destructive commands** (e.g. `rm -rf`, `git reset --hard`) without explicit confirmation.
- **Branch:** `main` only.
- **No script-based code transformations.** Manual edits only. Brittle regex scripts are forbidden.
- **No file proliferation.** Revise existing files in place; do not create `_v2` or `_improved` variants.
- **After substantive changes:** Run `cargo check/clippy/fmt` and tests. Use `br` for task tracking.

### 1.5 Mechanical Sympathy

Database engines live and die by cache behavior, memory layout, and I/O
patterns. The following constraints are non-negotiable for hot-path code:

- **Page alignment.** All page buffers MUST be allocated at `page_size`
  alignment (4096 by default). This enables direct I/O (`O_DIRECT`) where
  physically compatible and avoids partial-page kernel copies. Use
  `std::alloc::Layout` with alignment.
  **Compatibility note:** SQLite `.wal` frames are `24 + page_size` bytes and
  therefore do not preserve sector alignment at the frame boundaries. In
  Compatibility mode, implementations MUST NOT require `O_DIRECT` for `.wal`
  I/O; buffered I/O is required there. Direct I/O MAY still be used for
  page-aligned `.db` I/O and FrankenSQLite-native sidecars/logs whose record
  format preserves alignment.

- **Zero-copy I/O.** The VFS read/write paths MUST NOT allocate intermediate
  buffers. `read_exact_at` / `write_all_at` operate directly on page-aligned
  buffers. The pager hands out `&[u8]` references to cached pages, not copies.

- **SIMD-friendly layouts.** Hot comparison paths (B-tree key comparison,
  checksum computation, RaptorQ GF(256) arithmetic) SHOULD use types whose
  in-memory representation is SIMD-friendly: contiguous byte arrays, no
  pointer chasing, no padding between elements. `xxhash3` already exploits
  this; B-tree cell comparison and RaptorQ matrix ops SHOULD follow suit.

- **Canonical byte representation.** All on-disk structures (page headers,
  cell formats, WAL frames, ECS symbol records) MUST have a single canonical
  byte encoding. Big-endian for SQLite-compatible structures (matching C
  SQLite), little-endian for FrankenSQLite-native ECS structures (matching
  x86/ARM native order for zero-cost decode).

- **Cache-line awareness.** The MVCC `PageLockTable` shards (Section 5.8)
  and hot-plane witness index buckets (Section 5.6.4.5) MUST be padded to
  64-byte cache-line boundaries to prevent false sharing between concurrent
  writers.

- **Systematic fast-path reads.** When persisting ECS objects, writers MUST
  pre-position systematic symbols (ESI 0..K-1) as contiguous runs in the local
  symbol store when possible (Section 3.5.2). This enables a "happy path" read
  that concatenates systematic symbol payloads without invoking the GF(256)
  decoder (matrix multiply is only needed for repair).

- **Prefetch hints.** B-tree descent SHOULD issue prefetch hints for child pages
  when the next page number is known. Because workspace members forbid `unsafe`,
  prefetch MUST be implemented only via safe APIs (e.g., asupersync-provided safe
  hints) and MUST degrade to a no-op if no safe prefetch primitive exists on the
  platform.

- **VFS platform operations.** Workspace members forbid `unsafe` (§1.4). The
  VFS MUST therefore rely on safe platform abstractions (e.g., asupersync's safe
  file/shm/lock primitives) rather than direct FFI. If a platform feature (e.g.,
  `mmap`-backed shared memory) cannot be expressed safely, that feature MUST be
  disabled or moved behind an external dependency boundary (not implemented as
  `unsafe` inside this repository).

- **Avoid allocation in the read path.** Cache lookups, version checks, and
  index resolution MUST be allocation-free in the common case. Hot-path
  structures (e.g., active transaction sets) should use stack-allocated
  small vectors (`SmallVec`) where possible.

- **Exploit auto-vectorization.** GF(256) symbol ops and XOR patches should
  operate on `u64`/`u128` chunks in safe Rust loops that LLVM can easily
  vectorize. Use optimized dependencies (`xxhash-rust`, `asupersync`) for
  heavy lifting rather than writing `unsafe` SIMD intrinsics manually.

---

## 2. Why Page-Level MVCC

### 2.1 The Problem

In WAL mode, C SQLite allows multiple concurrent readers but caps the number
of simultaneously active reader locks via `WAL_NREADER` in the wal-index shared
memory (default: 5). It still allows only ONE writer at a time. The
`WAL_WRITE_LOCK` (byte 120 of the WAL index shared memory) is an exclusive
advisory lock. Any connection attempting to write while another holds this lock
receives `SQLITE_BUSY` (or `SQLITE_BUSY_SNAPSHOT` when a reader-turned-writer
detects a WAL snapshot conflict: the wal-index header has changed since the
read transaction started, so upgrading to a writer would create a fork).

For applications with mixed read/write workloads across different tables or
different regions of the same table, this is a needless bottleneck. Two users
inserting into unrelated tables should never wait for each other.

### 2.2 Why Page Granularity

| Granularity | Pros | Cons |
|-------------|------|------|
| **Row-level** (PostgreSQL) | Minimal false conflicts | Requires visibility map, per-row xmin/xmax, breaks file format |
| **Page-level** (our choice) | Maps to B-tree I/O unit, preserves file format, simple version chains | False conflicts when rows share a page |
| **Table-level** | Trivial implementation | Nearly useless (most apps have few tables) |

Page-level is the sweet spot: it maps directly to SQLite's B-tree page
architecture (pages are already the unit of I/O, caching, and WAL frames),
preserves the on-disk file format, and provides meaningful concurrency for
real-world workloads where writers typically touch different leaf pages.

### 2.3 The Isolation Level Problem (CRITICAL)

**C SQLite provides SERIALIZABLE isolation** -- trivially, because writers are
serialized by the WAL_WRITE_LOCK. Every execution is equivalent to some serial
ordering of transactions.

**Page-level MVCC provides Snapshot Isolation (SI)**, which is weaker. SI allows
the **write skew anomaly**: two transactions T1 and T2 each read overlapping
data, each writes to a different item based on what they read, and both commit
successfully -- but the combined result is inconsistent.

**Example:** Table has two rows (A=50, B=50), sum=100. Constraint: sum >= 0.
T1 reads both (50, 50), decides safe to withdraw 90, writes A = 50-90 = -40.
T2 reads both (50, 50), decides safe to withdraw 90, writes B = 50-90 = -40.
Both commit. Sum is now -40 + -40 = -80. Constraint violated. Under
SERIALIZABLE, one would have seen the other's write and aborted.

**This is a data corruption risk.** SQLite users depend on SERIALIZABLE. We
cannot silently downgrade.

### 2.4 The Solution: Layered Isolation

**Layer 1 (Default): SQLite behavioral compatibility mode (single-writer, WAL semantics).**
- `BEGIN` / `BEGIN DEFERRED`: DEFERRED. No writer-exclusion lock is acquired at
  `BEGIN`. Readers do not block readers. On the first write attempt, the
  transaction MUST upgrade to a Serialized writer by acquiring the global write
  mutex (§5.4) and then proceed as the single writer.
- `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`: Acquire the global write mutex at
  `BEGIN` (writer-intent). This provides the usual "single writer" behavior
  while allowing concurrent readers (WAL semantics).
- This is the default mode. **Within FrankenSQLite**, existing SQLite
  applications observe SERIALIZABLE behavior for writer interactions (writers
  are serialized) without sacrificing concurrent readers.
- **Interop boundary:** When running Hybrid SHM (`foo.db.fsqlite-shm`), legacy
  SQLite processes are supported as readers only; legacy writers are excluded
  and will observe `SQLITE_BUSY` while the coordinator is alive (§5.6.6.1,
  §5.6.7).

**Layer 2: MVCC concurrent mode with SSI (Serializable by Default).**
- `BEGIN CONCURRENT`: New non-standard syntax (matching SQLite's own
  experimental `BEGIN CONCURRENT` branch). Uses page-level MVCC with
  **Serializable Snapshot Isolation (SSI)** -- not merely Snapshot Isolation.
- Multiple concurrent writers, first-committer-wins on page conflicts, plus
  SSI validation to prevent write skew anomalies.
- SSI implements the conservative Cahill/Fekete rule at page granularity
  ("Page-SSI"): no committed transaction may have both an incoming AND
  outgoing rw-antidependency edge. This prevents serialization cycles.
- Applications that opt in get **SERIALIZABLE** concurrent writes. The 3–7%
  throughput overhead measured on OLTP benchmarks with PostgreSQL 9.1+ (Ports &
  Grittner, VLDB 2012; up to 10–20% on synthetic microbenchmarks without
  read-only optimizations) is acceptable for correctness.
- `PRAGMA fsqlite.serializable = OFF` provides an explicit opt-out to plain
  Snapshot Isolation for benchmarking or applications that tolerate write skew.
  This is NOT the default.
- This is where the concurrency innovation lives.

**Why SSI ships by default (not deferred):**
- SI silently downgrades correctness. SQLite users depend on SERIALIZABLE.
  Shipping SI-only concurrent mode creates a correctness trap where applications
  that switch from `BEGIN` to `BEGIN CONCURRENT` get weaker guarantees without
  warning.
- The conservative Page-SSI rule (`has_in_rw && has_out_rw => abort`) is
  simple to implement: two boolean flags per transaction plus a witness plane
  that makes read/write evidence discoverable across processes (§5.6.4, §5.7).
  Hot-plane overhead is bounded by `TxnSlot` count and hot bucket capacity
  (bitsets over slots); cold-plane evidence is append-only but GC-able by
  `safe_gc_seq` horizons.
- PostgreSQL has proven SSI viable in production since 2011 with 3–7% OLTP
  overhead (up to 10–20% on microbenchmarks) and ~0.5% false positive abort
  rate. At page granularity, our false positive
  rate will be somewhat higher, but the safe write-merge ladder (Section 5.10)
  compensates by turning many apparent conflicts into successful merges.
- Starting with SSI from day one means we never ship a correctness regression.
  We can always *reduce* abort rates later (finer witness keys + refinement,
  better victim selection), but we cannot retroactively fix applications that
  relied on SI and experienced silent write skew.

**Layer 3 (Future refinement): Reduced-abort SSI.**
- Refine witness keys from `Page(pgno)` to `Cell(btree_root_pgno, cell_tag)` and/or
  `ByteRange(page, start, len)` to reduce false positive aborts on hot pages.
- Smarter victim selection (instead of always aborting the committing pivot).
- These are optimizations of SSI, not correctness changes.
- **Value of Information (VOI) for granularity investment:** The decision to
  invest engineering effort in cell/byte-range witness refinement should be
  data-driven. Compute `VOI = E[ΔL_fp] * N_txn/day - C_impl`, where `E[ΔL_fp]`
  is the expected reduction in false positive abort cost (measured by the SSI
  e-process monitor INV-SSI-FP in §5.7), `N_txn/day` is daily transaction volume,
  and `C_impl` is the amortized implementation cost. Only invest when VOI > 0.
  This prevents premature optimization of witness granularity.

---

## 3. RaptorQ: The Information-Theoretic Foundation

### 3.1 What RaptorQ Is

RaptorQ (RFC 6330) is a fountain code -- a class of erasure codes where the
encoder can produce a practically unlimited stream of encoding symbols from K
source symbols, and the decoder can recover the original K source symbols from
ANY set of K' encoding symbols where K' is only slightly larger than K (in most
cases, K' = K suffices).

**Key properties:**
- **Near-optimal (engineering sense)**: Approaches erasure-channel capacity with
  small overhead. RaptorQ trades the last fraction of optimality for practical,
  polynomial-time encoding/decoding under real-world constraints.
- **Systematic**: The first K encoding symbols ARE the source symbols (zero
  encoding overhead for the common no-loss case)
- **Rateless**: Generate as many repair symbols as needed on-the-fly
- **Universal**: Works for any symbol size (we use page-sized symbols)

RaptorQ improves upon the original Raptor code (RFC 5053) in several ways:
it uses GF(256) arithmetic for the HDPC constraints instead of GF(2), which
dramatically improves the failure probability at low overhead. Where Raptor
codes over GF(2) have a ~5-10% failure rate when decoding with exactly K
symbols (Shokrollahi, "Raptor Codes", IEEE Trans. Info. Theory, 2006;
exact rate varies with K), RaptorQ achieves ~1% failure rate (RFC 6330 Annex B: for most K
values, P_fail(K) < 0.01). With just one additional symbol (K+1 received),
the failure rate drops to approximately 10^-4. With two additional symbols
(K+2), it drops to approximately 10^-7. This near-perfect recovery rate is
what makes RaptorQ suitable as a foundational building block for database
durability rather than merely a network transport optimization.

**Caution on failure probability claims:** The exact failure probability
depends on K, the symbol size, and implementation quality. The figures above
are from RFC 6330 Annex B simulation data. Do not cite "0.01%" (10^-4) for
exactly-K decoding; that overstates the guarantee by ~100x. Our V1 policy
(K+2 symbols) is specifically chosen to push well past this ambiguity.

The RFC 6330 specification defines behavior for source blocks containing up
to 56,403 source symbols (K_max = 56403). Each symbol is a contiguous block
of T octets. For FrankenSQLite, T = page_size (typically 4096 bytes), so a
single source block can cover up to 56,403 pages, or ~220 MiB (231 MB) of
database content. Larger databases are partitioned into multiple source blocks
   (see Section 3.4.3).

### 3.1.1 Operational Guidance: Overhead and Failure Probability

RaptorQ is "any K symbols suffice" in the *engineering* sense, but the decode
success probability at exactly `K` is not literally 1. The point of repair
symbols is to drive decode failure probability into the floor.

**Rules of thumb (RFC 6330 Annex B simulation data):**
- Decoding with **exactly K** received symbols: ~99% success (P_fail < 0.01).
- Decoding with **K+1** symbols: P_fail < 10^-4.
- Decoding with **K+2** symbols: P_fail < 10^-7.

**V1 Default Policy:** Aim to persist/replicate enough symbols that a decoder
can almost always collect **K+2** symbols without coordination. This eliminates
the need for "just one more symbol" negotiation loops in the common case.

### 3.2 How RaptorQ Works (Essential Understanding)

This section provides the depth necessary for an implementor to understand
every step of the RaptorQ encoding and decoding pipeline. While FrankenSQLite
uses asupersync's production-grade implementation rather than re-implementing
RFC 6330, understanding the internals is essential for correct integration,
debugging, and performance tuning.

#### 3.2.1 GF(256) Arithmetic -- The Algebraic Foundation

All RaptorQ operations beyond simple XOR are performed over the Galois Field
GF(2^8), commonly written GF(256). This is the field with exactly 256
elements, which maps perfectly to byte values 0x00 through 0xFF. Understanding
this arithmetic is critical because it appears in HDPC constraint generation,
the LT encoding function, and all symbol operations.

**The Field GF(2^8) with Irreducible Polynomial**

GF(2^8) is constructed as the quotient ring GF(2)[x] / p(x), where p(x) is
an irreducible polynomial of degree 8 over GF(2). RFC 6330 specifies:

```
p(x) = x^8 + x^4 + x^3 + x^2 + 1
```

In hexadecimal, this is 0x11D (binary: 1_0001_1101). The field elements are
the 256 polynomials of degree < 8 with coefficients in GF(2) = {0, 1}. Each
such polynomial maps to a byte:

```
Element     Polynomial            Byte
-------     ----------            ----
0           0                     0x00
1           1                     0x01
2           x                     0x02
3           x + 1                 0x03
...
0xA3        x^7 + x^5 + x + 1    0xA3
0x47        x^6 + x^2 + x + 1    0x47
...
255         x^7 + ... + x + 1     0xFF
```

**Addition: XOR**

Addition in GF(2^8) is polynomial addition with coefficients reduced modulo 2.
Since coefficients are in {0, 1}, addition modulo 2 is just XOR:

```
a + b = a XOR b
```

The additive identity is 0x00. Every element is its own additive inverse
(a + a = a XOR a = 0), which means subtraction is also XOR:

```
a - b = a XOR b = a + b
```

This is enormously convenient for implementation: addition is a single XOR
instruction, and it works on any register width. On a 64-bit machine, we can
add 8 GF(256) elements simultaneously with a single u64 XOR.

**Multiplication via Log/Exp Tables**

Direct polynomial multiplication modulo p(x) requires a sequence of shifts
and conditional XORs. While possible, this is slow. RaptorQ instead uses
logarithm and exponential tables based on a primitive element (generator) of
the multiplicative group GF(256)*.

The multiplicative group GF(256)* consists of the 255 non-zero elements and
is cyclic. RFC 6330 §5.7 specifies the generator g = 2 (the polynomial
x). Every non-zero element a can be written as a = g^k for some unique
k in {0, 1, ..., 254}. We define:

```
OCT_LOG[a] = k    such that g^k = a    (for a != 0)
OCT_EXP[k] = g^k  (for k = 0, 1, ..., 254)
```

The OCT_LOG table has 256 entries (OCT_LOG[0] is undefined / sentinel).
The OCT_EXP table has 256 entries but is typically extended to 510 entries
(OCT_EXP[k] for k = 0..509, where OCT_EXP[k+255] = OCT_EXP[k]) to avoid
a modular reduction after addition of logarithms.

Together, these tables consume 256 + 510 = 766 bytes. In practice, the
OCT_EXP table is stored with 512 entries for alignment, so total storage
is 256 + 512 = 768 bytes for the base lookup tables.

**Multiplication algorithm:**

```
multiply(a, b):
    if a == 0 or b == 0: return 0
    return OCT_EXP[(OCT_LOG[a] + OCT_LOG[b]) % 255]
```

With the extended OCT_EXP table (510 entries), the modular reduction
is unnecessary since OCT_LOG[a] + OCT_LOG[b] <= 254 + 254 = 508 < 510:

```
multiply(a, b):
    if a == 0 or b == 0: return 0
    return OCT_EXP[OCT_LOG[a] + OCT_LOG[b]]    // no modular reduction needed
```

This is O(1): two table lookups, one addition, one more table lookup.

**Division:**

```
divide(a, b):
    assert(b != 0)
    if a == 0: return 0
    return OCT_EXP[(OCT_LOG[a] - OCT_LOG[b] + 255) % 255]
```

Or equivalently, using the multiplicative inverse:

```
inverse(b):
    assert(b != 0)
    return OCT_EXP[255 - OCT_LOG[b]]

divide(a, b):
    return multiply(a, inverse(b))
```

**Worked Example: 0xA3 * 0x47**

Let us multiply 0xA3 (163 decimal) by 0x47 (71 decimal) step by step.

```
Step 1: Look up logarithms
    0xA3 = x^7 + x^5 + x + 1
    Using the OCT_LOG table (computed from g = 2, p(x) = 0x11D):
    OCT_LOG[0xA3] = 91     (i.e., 2^91 mod p(x) = 0xA3)
    OCT_LOG[0x47] = 253    (i.e., 2^253 mod p(x) = 0x47)

Step 2: Add logarithms (modulo 255, since the multiplicative group has order 255)
    91 + 253 = 344
    344 mod 255 = 89

Step 3: Look up exponential
    OCT_EXP[89] = 0xE1   (this is g^89 mod p(x))

Step 4: Result
    0xA3 * 0x47 = 0xE1   (225 decimal)
```

Verification: 0xE1 = x^7 + x^6 + x^5 + 1. We can confirm by directly
multiplying the polynomials (x^7 + x^5 + x + 1)(x^6 + x^2 + x + 1) modulo
p(x) = x^8 + x^4 + x^3 + x^2 + 1, and reducing modulo 2 in each coefficient.
The unreduced product x^13 + x^11 + x^9 + x^8 + x^7 + x^5 + x^3 + 1 reduces
to x^7 + x^6 + x^5 + 1 after substituting x^8 ≡ x^4 + x^3 + x^2 + 1 and
collapsing terms mod 2.

**Bulk Multiplication Tables (MUL_TABLES)**

For high-throughput encoding and decoding, asupersync precomputes a 64KB
table MUL_TABLES[256][256] where MUL_TABLES[a][b] = a * b in GF(256). This
trades memory for speed: a single array index replaces the log-add-exp
sequence, reducing multiplication to a single memory load.

```
MUL_TABLES: [[u8; 256]; 256]    // 65,536 bytes total

// Precomputation (done once at startup):
for a in 0..256 {
    for b in 0..256 {
        MUL_TABLES[a][b] = if a == 0 || b == 0 {
            0
        } else {
            OCT_EXP[(OCT_LOG[a] as u16 + OCT_LOG[b] as u16) as usize]
        };
    }
}

// Usage (O(1) single lookup):
fn mul(a: u8, b: u8) -> u8 {
    MUL_TABLES[a as usize][b as usize]
}
```

**Why GF(256) and Not GF(2)?**

The original Raptor codes (RFC 5053) use GF(2) (binary) for all operations,
meaning addition is XOR and the only multiplication is by 0 or 1. This is
extremely fast but limits the algebraic structure. RaptorQ uses GF(256) for
the HDPC (Half-Distance Parity-Check) constraints specifically because:

1. **Byte alignment**: GF(256) elements are exactly one byte. All operations
   are naturally aligned to the machine's byte-addressable memory model.
2. **SIMD friendliness**: XOR (addition) works on entire 64-bit words,
   processing 8 GF(256) additions in a single instruction. For multiplication,
   modern CPUs with PCLMULQDQ or VPGATHERDD can process multiple GF(256)
   multiplications in parallel.
3. **Algebraic strength**: The HDPC constraints over GF(256) provide much
   stronger error-correction capability than GF(2), which is the primary
   reason RaptorQ achieves better failure probability than Raptor codes.
4. **Information density**: Each GF(256) coefficient carries 8 bits of
   information (vs 1 bit for GF(2)), meaning the dense HDPC matrix rows
   carry 8x more constraint information per element.

The cost is that GF(256) multiplication is more expensive than GF(2)
multiplication (a table lookup vs a single AND), but this is paid only in
the HDPC rows (H rows out of L total), not in the LDPC or LT rows which
remain sparse and binary.

#### 3.2.2 Symbol Operations

A **symbol** in RaptorQ is a vector of T octets, where T is the symbol size.
For FrankenSQLite, T = page_size = 4096 bytes (the default SQLite page size).
All encoding and decoding operations are performed symbol-by-symbol, where
each "scalar" operation on a GF(256) element is lifted to a vector operation
on T octets.

**Symbol Addition (XOR)**

```
symbol_add(A: &[u8; T], B: &[u8; T]) -> [u8; T]:
    result = [0u8; T]
    for i in 0..T:
        result[i] = A[i] ^ B[i]
    return result
```

In practice, this is SIMD-accelerated by operating on u64 (8 bytes at a time)
or u128 / SIMD registers (16-32 bytes at a time):

```
symbol_add_fast(A: &[u8; T], B: &[u8; T], out: &mut [u8; T]):
    let a_words = A.as_ptr() as *const u64
    let b_words = B.as_ptr() as *const u64
    let o_words = out.as_mut_ptr() as *mut u64
    for i in 0..(T / 8):
        *o_words.add(i) = *a_words.add(i) ^ *b_words.add(i)
```

For T = 4096, this is 512 u64 XOR operations = 512 instructions, which
modern CPUs can execute in ~64 cycles (8-wide superscalar pipeline). This
is the dominant operation in both encoding and decoding.

**Symbol Scalar Multiplication**

Multiplying a symbol by a GF(256) scalar c means multiplying each byte
independently:

```
symbol_mul(c: u8, A: &[u8; T]) -> [u8; T]:
    if c == 0: return [0u8; T]
    if c == 1: return A.clone()
    result = [0u8; T]
    for i in 0..T:
        result[i] = MUL_TABLES[c as usize][A[i] as usize]
    return result
```

This requires T table lookups. For T = 4096, that is 4096 lookups into the
same 256-byte row of MUL_TABLES (MUL_TABLES[c]), which fits in L1 cache
and achieves excellent throughput.

**Symbol Multiply-and-Add (Fused Operation)**

The most common operation in Gaussian elimination is "add c * row_j to row_i":

```
symbol_addmul(dst: &mut [u8; T], c: u8, src: &[u8; T]):
    if c == 0: return    // no-op
    if c == 1:
        symbol_xor(dst, src)    // just XOR
        return
    let mul_row = &MUL_TABLES[c as usize]
    for i in 0..T:
        dst[i] ^= mul_row[src[i] as usize]
```

This fused operation avoids allocating a temporary symbol and is the
innermost loop of the decoder. Performance here directly determines
overall decode throughput.

**Symbol Operations Are the Building Blocks**

Every RaptorQ operation -- LDPC constraint evaluation, HDPC constraint
evaluation, LT encoding, Gaussian elimination during decoding -- reduces
to sequences of symbol_add (XOR) and symbol_addmul. The entire algebraic
machinery of GF(256) ultimately manifests as these two operations applied
to 4096-byte vectors.

#### 3.2.3 Encoding Step by Step

The RaptorQ encoding process transforms K source symbols into a potentially
unlimited stream of encoding symbols. Here is the complete procedure:

**Step 1: Determine Coding Parameters**

Given K source symbols C'[0], C'[1], ..., C'[K-1]:

1. Look up K' in the systematic index table (RFC 6330 Table 2). K' is the
   smallest value in the table that is >= K. (Table 2 enumerates the supported
   K' values up to 56,403.) For example:
   - K = 5 -> K' = 6
   - K = 10 -> K' = 10
   - K = 100 -> K' = 101

2. Pad the source block with (K' - K) zero symbols to get exactly K' source
   symbols: C'[0], ..., C'[K-1], 0, 0, ..., 0.

3. For K', the systematic index table also defines:
   - J(K'): the systematic index (used in the Tuple generator)
   - S(K'): the number of LDPC symbols
   - H(K'): the number of HDPC symbols
   - W(K'): the LT generator modulus parameter

   FrankenSQLite relies on asupersync's RFC 6330 implementation for these
   derivations; do not substitute ad-hoc formulas here.

4. L = K' + S + H: the total number of intermediate symbols.

**Step 2: Construct the Constraint Matrix A**

The constraint matrix A is an L x L matrix that encodes the relationship
between intermediate symbols C[0], ..., C[L-1] and the source/constraint
data. A is divided into three regions:

```
A (L x L matrix):
    Rows 0 to S-1:          LDPC constraints (sparse, over GF(2))
    Rows S to S+H-1:        HDPC constraints (dense, over GF(256))
    Rows S+H to L-1:        LT constraints for source symbols (sparse, over GF(2))

         |<--- K' cols --->|<- S cols ->|<- H cols ->|
    LDPC |   LDPC_LEFT     | I_S(SxS)  |   0        |  S rows
    HDPC |   MT * GAMMA    |   0        | I_H(HxH)  |  H rows
    LT   |   LT_MATRIX     |   0        |   0        |  K' rows
```

**LDPC rows (0..S-1):** Each LDPC row has exactly ceil(K'/S) + 2 non-zero
entries in the leftmost K' columns, plus a 1 on the diagonal of the S x S
identity block. These constraints are sparse (typically ~7 non-zero entries
per row for typical K' values) and binary (over GF(2)).

The LDPC constraint for row i (0 <= i < S) sets the following positions to 1:
- For j = 0, 1, ..., W-2: if (i == (j % S)): set column j to 1
- Two additional columns determined by Rand(j, i, S) for specific j values
- Column K' + i is set to 1 (the identity block)

**HDPC rows (S..S+H-1):** These rows use GF(256) coefficients and are dense
over the first K' + S columns. The HDPC constraints are generated using:
1. The MT matrix (H x (K'+S)), computed from a random walk using the
   Rand function
2. The GAMMA matrix ((K'+S) x (K'+S)), a specific structured matrix over
   GF(256) defined by alpha (a primitive element of GF(256))

The HDPC rows provide the "algebraic strength" that makes RaptorQ achieve
near-optimal failure probability. They are the reason GF(256) is used.

**LT rows (S+H..L-1):** Row S+H+i corresponds to source symbol C'[i]. Each
LT row is generated by the Tuple function and the LT encoding relation. For
source symbol i:

```
(d, a, b, d1, a1, b1) = Tuple(K', i)
// d = LT degree, a/b = LT parameters
// d1, a1, b1 = permanent inactivation parameters

Row S+H+i has 1s at positions:
    b                          (always)
    (b + a) mod W              (if d >= 2)
    (b + 2*a) mod W            (if d >= 3)
    ...
    (b + (d-1)*a) mod W        (if degree is d)
Plus "permanent inactivation" entries from d1, a1, b1 in columns W..K'-1
```

**Step 3: Build the Source Vector D**

The source vector D has L entries:

```
D[0..S-1]      = zero symbols (LDPC constraints have zero right-hand side)
D[S..S+H-1]    = zero symbols (HDPC constraints have zero right-hand side)
D[S+H..L-1]    = C'[0], C'[1], ..., C'[K'-1]  (the padded source symbols)
```

**Step 4: Solve A * C = D for Intermediate Symbols**

This is the key step. We need to find intermediate symbols C[0], ..., C[L-1]
such that A * C = D. Since A is L x L and invertible (by construction for
valid K'), this is a standard linear system solve over GF(256).

The solve uses Gaussian elimination with partial pivoting. The matrix A has
been carefully designed so that its structure (sparse LDPC + dense HDPC +
sparse LT) is amenable to efficient elimination. In particular, the
inactivation decoding algorithm (Section 3.2.4) exploits this structure.

After solving, we have intermediate symbols C[0], C[1], ..., C[L-1].

**Step 5: Generate Encoding Symbols**

Given the intermediate symbols, any encoding symbol with Internal Symbol ID
(ISI) X can be generated:

```
generate_symbol(X, K', C[0..L-1]):
    if X < K':
        return C'[X]    // systematic: return the source symbol itself
    else:
        return LTEnc(K', C[0..L-1], X)
```

The LTEnc function for ISI X >= K':

```
LTEnc(K', C[0..L-1], X):
    (d, a, b, d1, a1, b1) = Tuple(K', X)
    result = C[b]
    for j in 1..d:
        b = (b + a) mod W
        result = result XOR C[b]
    // Permanent inactivation component
    while b1 >= L:
        b1 = (b1 + a1) mod P1
    result = result XOR C[b1]
    for j in 1..d1:
        b1 = (b1 + a1) mod P1
        while b1 >= L:
            b1 = (b1 + a1) mod P1
        result = result XOR C[b1]
    return result
```

**Systematic Property:** For ISI X < K', the encoding symbol is exactly the
source symbol C'[X]. This means that in the no-loss case, the receiver
already has all K source symbols and no decoding is needed. The repair
symbols (ISI >= K') are generated only as redundancy.

#### 3.2.4 Decoding Step by Step

Decoding is the inverse problem: given N received encoding symbols (where
N >= K' and ideally N is close to K'), recover the K' source symbols.

**Step 1: Collect Received Symbols**

The receiver collects N encoding symbols with their ISIs. Some may be source
symbols (ISI < K'), others may be repair symbols (ISI >= K'). The receiver
does not need to know which symbols were lost -- it only needs N symbols,
any N symbols.

**Step 2: Build the Decoding Matrix A'**

Construct an N x L matrix A' where row i corresponds to received symbol with
ISI X_i:

```
For each received symbol with ISI X_i:
    If X_i < K' (source symbol):
        Row i = row S+H+X_i of the original constraint matrix A
    Else (repair symbol):
        Row i = LT encoding vector for ISI X_i
        (computed from Tuple(K', X_i), same as during encoding)
```

Prepend the S LDPC constraint rows and H HDPC constraint rows to get the
full system. The extended matrix has (S + H + N) rows and L columns:

```
A_extended (S+H+N rows x L columns):
    Rows 0..S-1:       LDPC constraints
    Rows S..S+H-1:     HDPC constraints
    Rows S+H..S+H+N-1: received symbol constraints

D_extended:
    D[0..S-1]       = zero symbols
    D[S..S+H-1]     = zero symbols
    D[S+H..S+H+N-1] = received symbol data
```

The system is overdetermined (S+H+N >= L when N >= K'), so we need to find
C[0..L-1] satisfying at least L of the S+H+N equations.

**Step 3: Inactivation Decoding (Two Phases)**

This is the heart of RaptorQ decoding and what makes it efficient. Direct
Gaussian elimination on an L x L matrix over GF(256) would cost O(L^3)
operations. Inactivation decoding exploits the sparse structure to achieve
near-linear average-case performance.

**Phase 1: Peeling (O(K) average case)**

The peeling phase iteratively processes rows that have exactly one unknown
symbol (i.e., rows with exactly one non-zero entry in the remaining
unresolved columns):

```
peeling():
    resolved = {}   // set of resolved symbol indices
    while exists row r with exactly 1 unresolved column c:
        // Row r: a_{r,c} * C[c] = D[r] - sum(a_{r,j} * C[j] for j in resolved)
        // Since a_{r,c} is the only unresolved coefficient:
        C[c] = (D[r] XOR sum(a_{r,j} * C[j] for resolved j)) * inverse(a_{r,c})
        resolved.add(c)
        // Remove column c from all other rows (update their right-hand sides)
```

Because the LDPC and LT rows are sparse, the peeling phase resolves the
majority of intermediate symbols. For a well-received block (N slightly
above K'), peeling typically resolves 90-95% of symbols in O(K) total
operations (each row touches only ~d columns where d is the LT degree,
and the average degree is O(log K)).

The peeling phase also identifies **inactive** symbols: those that cannot
be resolved by peeling because they appear in multiple unresolved rows.
The number of inactive symbols is typically small (on the order of
sqrt(K') to log(K')), thanks to the careful code design.

**Phase 2: Gaussian Elimination on the Inactive Subsystem**

After peeling, a small dense subsystem of I inactive symbols remains.
This subsystem has I unknowns and is solved by standard Gaussian
elimination over GF(256):

```
gaussian_solve(inactive_matrix, inactive_rhs):
    // inactive_matrix is approximately I x I where I ~ O(sqrt(K'))
    // Standard GF(256) Gaussian elimination with partial pivoting:
    for col in 0..I:
        // Find pivot row
        pivot_row = find_row_with_nonzero_entry_in_column(col)
        if pivot_row is None:
            return DECODING_FAILURE
        swap_rows(col, pivot_row)
        // Eliminate column from all other rows
        pivot_val = inactive_matrix[col][col]
        for row in 0..I:
            if row != col and inactive_matrix[row][col] != 0:
                factor = mul(inactive_matrix[row][col], inverse(pivot_val))
                // Row operation: row[row] -= factor * row[col]
                for j in col..I:
                    inactive_matrix[row][j] ^= mul(factor, inactive_matrix[col][j])
                inactive_rhs[row] = symbol_addmul(inactive_rhs[row], factor, inactive_rhs[col])
    // Back-substitute to get inactive symbol values
    for col in (0..I).rev():
        C[inactive[col]] = symbol_mul(inverse(inactive_matrix[col][col]), inactive_rhs[col])
```

The cost of Phase 2 is O(I^2 * T) for the symbol operations plus O(I^3)
for the matrix operations. Since I is small (typically < 50 for K' < 10000),
this is negligible compared to Phase 1.

**Step 4: Recover All Intermediate Symbols**

After Phase 2, all inactive symbols are known. We then "reverse peel" through
the Phase 1 resolutions in reverse order to recover all intermediate symbols.

**Step 5: Reconstruct Source Symbols**

With all intermediate symbols C[0..L-1] known, any source symbol can be
reconstructed:

```
for i in 0..K':
    C'[i] = LTEnc(K', C[0..L-1], i)
    // But since the code is systematic, this just picks the right
    // linear combination of intermediate symbols
```

For source symbols that were received directly, the reconstructed value
should match exactly (this serves as a verification check).

**Step 6: Strip Padding**

Discard the (K' - K) padding symbols to recover the original K source
symbols C'[0], ..., C'[K-1].

**Decoding Failure Behavior (Normative):**

RFC 6330 states that the decoder can recover the source block from *almost any*
set of encoding symbols of sufficient cardinality: *in most cases* `K` symbols
suffice; *in rare cases* slightly more than `K` are required. We therefore
treat decoding failure as a normal, recoverable event:

- Correctness MUST NOT depend on decoding succeeding with exactly `K` symbols.
- Durability/replication code MUST be able to obtain more symbols (local repair
  store and/or peers) and retry decode.
- For durability-critical objects, the writer MUST persist an explicit overhead
  policy (e.g., "store `K + r` repair symbols") in the object metadata so
  readers know what to request.

**Verification (Alien-Artifact Discipline):** we do not hard-code or assume
numerical failure probabilities. Instead, we continuously validate the *observed*
failure rate envelope as a function of `(K, r, symbol_size)` using lab tests and
anytime-valid monitoring (e-process/e-values) so regressions are caught even
under optional stopping.

#### 3.2.5 The Tuple Generator and Systematic Index Table

The Tuple function maps an ISI (Internal Symbol ID) to a 6-tuple
(d, a, b, d1, a1, b1) that determines which intermediate symbols participate
in generating that encoding symbol. This function is deterministic and
depends only on K' and the ISI.

The systematic index table (RFC 6330 Table 2) is a precomputed table of
supported K' values. For each K', it stores a value J(K') such that the
first K' encoding symbols (ISIs 0 through K'-1) correspond exactly to the
K' source symbols. This is the "systematic" property -- it's engineered so
that the encoding matrix has an embedded identity for the source symbols.

The Tuple function uses the Rand function (a hash combining K', ISI, and
an iteration counter) to pseudorandomly but deterministically select the
LT degree and the positions of the non-zero entries. The degree distribution
is the "RaptorQ degree distribution" (RFC 6330 §5.3.5.4), which is a
carefully tuned soliton-like distribution optimized for inactivation decoding.

### 3.3 Asupersync's RaptorQ Implementation

Asupersync contains a complete, production-grade RFC 6330 implementation:

- **GF(256) engine**: 64KB MUL_TABLES for O(1) multiply, u64-wide bulk XOR
  operations for SIMD-like throughput on symbol data
- **Systematic encoder**: Full LDPC+HDPC+LT constraint construction, Gaussian
  elimination for intermediate symbol generation
- **Inactivation decoder**: Two-phase (peeling then Gaussian on inactive subset),
  efficient for the typical case where most symbols are "easy"
- **Decode proof system**: When decoding fails, produces explainable artifacts
  with replay verification
- **Cancel-safe pipelines**: Uses Cx checkpoint at symbol boundaries for
  cooperative cancellation
- **Distributed module**: Consistent hashing, quorum-based symbol distribution,
  recovery protocols

The implementation is structured as a layered set of modules (asupersync paths
shown for navigation):

```
src/raptorq/gf256.rs        -- GF(256) arithmetic
src/raptorq/linalg.rs       -- sparse/dense linear algebra over GF(256)
src/raptorq/systematic.rs   -- systematic index table + tuple generator machinery
src/raptorq/decoder.rs      -- inactivation decoder (peeling + Gaussian)
src/raptorq/proof.rs        -- explainable decode proofs / failure reasons
src/raptorq/pipeline.rs     -- end-to-end sender/receiver pipelines
src/distributed/            -- quorum routing + recovery (for replication use-cases)
```

FrankenSQLite integrates primarily via the pipeline builders (`RaptorQSender*`
and `RaptorQReceiver*`) plus the lower-level decode proof artifacts:

```rust
use asupersync::config::RaptorQConfig;
use asupersync::raptorq::{RaptorQReceiverBuilder, RaptorQSenderBuilder};

// Encoding + send (transport is a SymbolSink; omitted here)
let config = RaptorQConfig::default();
let mut sender = RaptorQSenderBuilder::new()
    .config(config.clone())
    .transport(sink)
    .build()?;
sender.send_object(cx, object_id, &bytes)?;

// Receive + decode (source is a SymbolStream; omitted here)
let mut receiver = RaptorQReceiverBuilder::new()
    .config(config)
    .source(stream)
    .build()?;
let out = receiver.receive_object(cx, &params)?;
let bytes = out.data;
```

### 3.4 RaptorQ Integration Points in FrankenSQLite

RaptorQ permeates every layer of FrankenSQLite:

#### 3.4.1 Self-Healing WAL (Erasure-Coded Durability)

**Problem:** Traditional WAL relies on "hope nothing goes wrong during the
write." A torn write (power loss mid-write) corrupts the frame. SQLite detects
this via checksums and discards the frame, losing the transaction.

**Solution:** Each WAL commit group is RaptorQ-encoded.

```
WAL Commit (N pages):
  Source symbols:   [Page1_data | Page2_data | ... | PageN_data]
  Repair symbols:   R additional symbols (configurable redundancy)
  Written to disk:
    - `.wal`: N standard SQLite WAL frames (source symbols)
    - `.wal-fec`: R repair symbols + group metadata (sidecar)

Recovery:
  If any frames are torn/corrupted (detected by checksum):
    Collect surviving source frames from `.wal` that can be validated:
      - frames before the first checksum mismatch validate via the cumulative chain (§7.5)
      - frames at/after the mismatch validate via `.wal-fec` per-source `source_page_xxh3` hashes
    Collect repair symbols from `.wal-fec` for that commit group
    If |surviving_sources| + |repairs| >= N: RaptorQ-decode to recover missing source pages
    Else: transaction is truly lost (requires catastrophic multi-frame loss)
```

**Concrete WAL Commit Frame Layout (Compatibility Mode)**

Standard SQLite WAL frames are exactly 24 bytes (header) + page_size (data). They have **no spare padding**. Therefore, we cannot embed RaptorQ metadata in the WAL file itself without breaking compatibility.

Instead, we use a **sidecar file** (`.wal-fec`) to store repair symbols.

**The `.wal` file:** Contains ONLY standard, valid SQLite WAL frames (source symbols).
**The `.wal-fec` file:** Contains repair symbols and metadata for each commit group.

**Sidecar (`.wal-fec`) Object Model and Format**

We treat each committed SQLite WAL transaction (the set of frames up to the
commit frame with `db_size != 0`) as a compat ECS object:

- **Object type:** `CompatWalCommitGroup`
- **Source symbols (K):** the ordered list of page images written by the group
  (taken from `.wal` frames, not duplicated into `.wal-fec`)
- **Repair symbols (R):** `PRAGMA raptorq_repair_symbols` repair symbols, stored
  in `.wal-fec`

Each group has a stable identifier:

```
group_id := (wal_salt1, wal_salt2, end_frame_no)
```

The `.wal-fec` file is an append-only sequence of:

1. A `WalFecGroupMeta` record (variable length; length-prefixed)
2. `R` ECS `SymbolRecord`s (Section 3.5.2) for ESIs `K..K+R-1`

```
WalFecGroupMeta := {
    magic          : [u8; 8],    // "FSQLWFEC"
    version        : u32,        // 1
    wal_salt1      : u32,
    wal_salt2      : u32,
    start_frame_no : u32,        // inclusive, 1-based frame numbering within the WAL
    end_frame_no   : u32,        // inclusive; commit frame
    db_size_pages  : u32,        // commit frame db_size (pages) after this commit
    page_size      : u32,
    k_source       : u32,        // K
    r_repair       : u32,        // R
    oti            : OTI,        // decoding params (symbol size, block partitioning)
    object_id      : [u8; 16],   // ObjectId of CompatWalCommitGroup (content-addressed)
    page_numbers   : Vec<u32>,   // length = K; maps ISI 0..K-1 -> Pgno (frame order; duplicates permitted)

    // Independent per-source validation to break the cumulative-checksum catch-22 (§7.5):
    // - SQLite's WAL checksums are cumulative, so once the chain breaks at frame i,
    //   frames i+1.. cannot be validated via the WAL format alone.
    // - These hashes allow random-access validation of "surviving" source frames
    //   (page payload bytes) so they can be safely fed into a RaptorQ decoder.
    source_page_xxh3: Vec<u64>,  // length = K; xxh3_64(page_data) for ISI i (frame start_frame_no + i)
    checksum       : u64,        // xxh3_64 of all preceding fields
}
```

**WalFecGroupMeta invariants (normative):**
- `k_source == end_frame_no - start_frame_no + 1`
- `page_numbers.len() == k_source`
- `source_page_xxh3.len() == k_source`
- `end_frame_no` is the group's commit frame (the corresponding WAL frame has
  `db_size != 0` when fully intact), and `db_size_pages` MUST equal that commit
  frame's `db_size` field.

**Write ordering and semantics (normative):**

- **Durable (SQLite semantics):** a commit is durable once the `.wal` frames for
  the group (including the commit frame) are written and `fsync`'d and the
  wal-index (`foo.db-shm`) is updated (§5.6.7 step 2).
- **Repairable (FrankenSQLite enhancement):** a commit group becomes repairable
  only after its `.wal-fec` `WalFecGroupMeta` + `R` repair `SymbolRecord`s are
  appended and `fsync`'d.

**Pipelined repair symbols (required):** GF(256) encoding work (RaptorQ repair
symbols) MUST NOT occur inside the WAL write critical section. Instead, the
coordinator MUST acknowledge commit durability after Phase 1 (`.wal` fsync) and
enqueue a background job that generates and appends `.wal-fec` repair symbols
for the just-committed group. If the process crashes before the `.wal-fec` job
completes, the commit remains valid (durable) but may be temporarily
non-repairable until catch-up regenerates repair symbols deterministically from
the `.wal` frames.

**Worked Example: Commit of 5 Pages with 2 Repair Symbols**

Transaction writes pages 7, 12, 45, 100, 203. `PRAGMA raptorq_repair_symbols = 2`.

1.  **Write to `.wal`:**
    - Write 5 standard SQLite WAL frames (pages 7, 12, 45, 100, 203).
    - Total `.wal` growth: 5 * (24 + 4096) = 20,600 bytes.
    - These are the K=5 source symbols.

2.  **Write to `.wal-fec`:**
    - Enqueue a background FEC job for the group with `r_repair=2`.
    - The encoder thread reads the 5 source frames from `.wal`, generates 2
      deterministic repair symbols, and appends:
        - one `WalFecGroupMeta` record describing the group, then
        - two repair `SymbolRecord`s (Section 3.5.2) for repair ESIs 5 and 6.
    - The encoder then `fsync`s `.wal-fec` to make the group repairable.

3.  **Commit:** `fsync` `.wal` (durable). `.wal-fec` may lag briefly; once the
    background job completes and `fsync`s `.wal-fec`, the group is repairable.

**Recovery Algorithm (Compatibility Mode)**

On recovery, we scan the `.wal` file. If we encounter a torn write (invalid checksum):

1.  Identify the damaged commit group in the `.wal`.
2.  Locate the corresponding `WalFecGroupMeta` in `.wal-fec` (matching `group_id`).
3.  Collect **validated** source frames from `.wal`:
    - For each source ISI `i ∈ [0, K)` (frame `f = start_frame_no + i`), read the
      frame's `page_data` bytes and compute `xxh3_64(page_data)`.
    - If the hash matches `WalFecGroupMeta.source_page_xxh3[i]`, the source symbol
      is valid and MAY be used for decoding.
    - Otherwise, treat the source as missing/corrupt (do not feed it to the decoder).
    This step is required because the WAL checksum chain is cumulative (§7.5); once
    the chain breaks, frames cannot be validated via the WAL format alone.
4.  Collect repair `SymbolRecord`s from `.wal-fec` for this group, verifying each
    record's `frame_xxh3` (and `auth_tag` if enabled).
5.  If `valid_sources + valid_repairs >= K`:
    - Decode to recover missing/corrupted source pages.
    - Treat recovered pages as if they were successfully read from the WAL.
    - The commit frame's `db_size` MUST be taken from `WalFecGroupMeta.db_size_pages`
      (it is needed to apply truncation/extension semantics during WAL replay).
6.  If `valid_sources + valid_repairs < K`:
    - The commit is lost (catastrophic failure). Truncate WAL before this group.

**PRAGMA raptorq_repair_symbols Semantics**

```
PRAGMA raptorq_repair_symbols;          -- Query current value (default: 2)
PRAGMA raptorq_repair_symbols = N;      -- Set to N (0 disables, max 255)
```

- N = 0: Exact C SQLite behavior. No `.wal-fec` repair symbols written. No recovery
  from torn writes beyond what the checksum chain provides.
- N = 1: Tolerates 1 corrupted frame per commit group. Recommended minimum
  for production use. Overhead: 1/K additional WAL space per commit.
- N = 2: Tolerates 2 corrupted frames. Default. Overhead: 2/K additional
  WAL space.
- N > K: Valid but wasteful (more repair symbols than source symbols). The
  encoder will generate them, but the marginal benefit beyond N = 3 or 4
  is negligible for typical corruption patterns.

The PRAGMA is persistent.

- **Compatibility mode:** Persist the setting in the `.wal-fec` sidecar (a small
  header record with checksum), not in the main database file header. The
  SQLite database header remains standard and user-controlled (`user_version`,
  `application_id`), and bytes 72-91 ("reserved for expansion") remain zero as
  required by the file format.
- **Native mode:** Persist the setting in the ECS `RootManifest` metadata.

**Impact:** A commit of 10 pages with 2 repair symbols survives ANY 2 torn
frames. The probability of losing a committed transaction drops from
`P(any_torn_write)` to `P(more_than_R_torn_writes)`, which for R >= 2 is
astronomically small. This eliminates the need for double-write journaling.

**Configuration:** `PRAGMA raptorq_repair_symbols = N` (default: 2).
Set to 0 for exact C SQLite behavior (no repair symbols).

#### 3.4.2 Fountain-Coded Replication

**Problem:** Database replication traditionally uses TCP streams or
change-based approaches. These are fragile (connection drops require
restart), bandwidth-inefficient (retransmission of lost packets), and
order-dependent.

**Solution:** FrankenSQLite's replication protocol is fountain-coded:

```
Replication of changeset C (dirty pages + metadata):
  Sender: Serialize C -> `changeset_bytes` (length F) and fountain-code it
  Receiver: Collect encoding symbols until decode succeeds (K' ≳ K_source)
  Decode: Recover `changeset_bytes`, then parse into `(page_number, page_data)` pairs
  Apply: Write pages to local database

Properties:
  - UDP-based: no connection state, no retransmission
  - Multicast-capable: one sender, many receivers
  - Bandwidth-optimal: no wasted retransmission
  - Order-independent: symbols arrive in any order
  - Resumable: receiver can start collecting from any point
```

**Protocol State Machine -- Sender Side**

```
States: IDLE -> ENCODING -> STREAMING -> COMPLETE

IDLE:
    Entry: No active replication session.
    Trigger: New committed transaction (or explicit REPLICATE command).
    Action: Collect the transaction's write set (`K_pages` dirty pages).
    Transition -> ENCODING

ENCODING:
    Entry: Have `K_pages` pages (page data) and a deterministic changeset encoding.
    Action:
        - Deterministically serialize the changeset (the pages + metadata) into
          a byte stream of length F bytes (`changeset_bytes`).
        - Choose a transport symbol size `T_replication` (bytes per encoding
          symbol on the wire). `T_replication` is independent of the SQLite
          page size; it is chosen to respect the transport's constraints (MTU,
          fragmentation tolerance, etc.).
        - Create a RaptorQ encoder for `changeset_bytes` using symbol size
          `T_replication`, yielding `K_source = ceil(F / T_replication)` source
          symbols for the block.
        - Compute intermediate symbols (one-time cost: O(F) bytes of work)
        - Prepare the ISI counter starting at 0
    Transition -> STREAMING

STREAMING:
    Entry: Encoder ready, ISI counter initialized.
    Action (loop):
        - Generate encoding symbol for current ISI
        - Package into UDP packet (format below)
        - Send packet to destination(s) (unicast or multicast)
        - Increment ISI
        - If ISI < K_source: sending source symbols (systematic)
        - If ISI >= K_source: sending repair symbols (fountain)
        - Continue until:
            a) Receiver ACKs completion (optional, for unicast), OR
            b) ISI reaches sender-configured maximum (e.g., 2*K_source), OR
            c) Explicit stop command
    Transition -> COMPLETE (on any stop condition)

COMPLETE:
    Entry: Streaming finished.
    Action: Release encoder resources. Log replication metrics.
    Transition -> IDLE
```

**Changeset encoding (normative):** `changeset_bytes` MUST be self-delimiting and
unambiguously parseable even when the RaptorQ symbol stream includes zero-padding
in the final symbol. A recommended canonical encoding is:

```
ChangesetHeader := {
  magic      : [u8; 4],   -- "FSRP"
  version    : u16,       -- 1
  page_size  : u32,
  n_pages    : u32,
  total_len  : u64,       -- total changeset byte length (including header), before padding
}

PageEntry := {
  page_number: u32,
  page_xxh3  : u64,       -- xxh3_64(page_bytes) for corruption detection
  page_bytes : [u8; page_size],
}
```

All integer fields are encoded little-endian.

`PageEntry`s MUST be sorted by `page_number` ascending. Receivers MUST validate
`page_xxh3` for every page before applying it; on mismatch, the changeset MUST
be rejected (or repaired via additional symbols if possible).

**UDP Packet Format**

```
Replication Packet (variable size):
    Offset  Size    Field
    ------  ----    -----
    0       1       Source block number (u8)
                    - Identifies which source block this symbol belongs to
                    - For changesets with K_source <= 56,403, always 0
                    - For larger changesets, identifies the partition
    1       3       Encoding Symbol ID (u24 big-endian)
                    - The ISI of this symbol
                    - 0 to K_source-1 for source symbols, >= K_source for repair symbols
    4       4       Source block size K_source (u32 big-endian)
                    - Number of source symbols in this block
    8       T       Symbol data (T bytes, where T = T_replication)
                    - The actual encoding symbol content

Total packet size: 8 + T bytes (e.g., 8 + 1368 = 1376 bytes for an MTU-safe
configuration on Ethernet MTU 1500 with IPv4).
```

**Hard wire limit (physical):** For IPv4 UDP, the application payload MUST be
`<= 65,507` bytes. Therefore, `8 + T <= 65,507`. Implementations MUST reject
any configuration that violates this bound.

**Reliability note (normative guidance):** IP fragmentation amplifies loss:
when a single symbol is split across many Ethernet frames, losing any one frame
drops the entire symbol. Therefore, for MTU-constrained networks, implementations
SHOULD choose a symbol size that avoids fragmentation entirely (e.g., `T <= 1464`
for Ethernet MTU 1500 minus 20-byte IPv4 header, 8-byte UDP header, and the
8-byte replication header above). Encoding packets MUST carry whole encoding
symbols (RFC 6330 §4.4.2); replication MUST NOT assume that a SQLite page is a
single on-wire symbol. If `page_size > T`, the changeset serialization simply
spans multiple symbols.

**Receiver State Machine**

```
States: LISTENING -> COLLECTING -> DECODING -> APPLYING -> COMPLETE

LISTENING:
    Entry: Receiver is ready to accept replication data.
    Action: Listen on configured UDP port (unicast or multicast group).
    Trigger: First packet received.
    Transition -> COLLECTING

COLLECTING:
    Entry: At least one packet received.
    State:
        - decoder: RaptorQDecoder (created from K_source in first packet)
        - received_count: number of symbols added to decoder
        - source_block_decoders: HashMap<u8, RaptorQDecoder> (for multi-block)
    Action (on each packet):
        - Parse packet header (source_block, ISI, K_source)
        - Get or create decoder for this source block
        - Add symbol to decoder: decoder.add_symbol(ISI, symbol_data)
        - Increment received_count
        - If received_count >= K_source: attempt decode
    Transition -> DECODING (when enough symbols collected)

DECODING:
    Entry: >= K_source symbols collected for at least one source block.
    Action:
        - Call decoder.decode(cx)
        - If success: recovered K_source source symbols (and thus `changeset_bytes`)
        - If failure (rare, ~1% at exactly K_source): stay in COLLECTING, wait for more
    Transition -> APPLYING (on successful decode)
    Transition -> COLLECTING (on decode failure, need more symbols)

APPLYING:
    Entry: All K_source source symbols recovered.
    Action:
        - Parse the decoded `changeset_bytes` into the ordered set of
          `(page_number, page_data)` pairs.
        - For each page:
            - Write the page to the local database at the correct page number
        - Flush WAL / checkpoint as needed
    Transition -> COMPLETE

COMPLETE:
    Entry: All pages applied.
    Action:
        - Optionally send ACK to sender (for unicast protocols)
        - Log replication metrics (symbols received, decode time, etc.)
    Transition -> LISTENING (ready for next changeset)
```

**Multicast Operation**

Fountain coding is uniquely suited to multicast replication. The sender emits
the same stream of encoding symbols to a multicast group address:

```
Sender:     [sym_0] [sym_1] [sym_2] ... [sym_K-1] [sym_K] [sym_K+1] ...
                |       |       |           |          |         |
Multicast:  ====|=======|=======|===========|==========|=========|======
                |       |       |           |          |         |
Receiver A: [sym_0] [  X  ] [sym_2] ... [sym_K-1] [sym_K] [  X    ] ...
Receiver B: [  X  ] [sym_1] [  X  ] ... [  X    ] [sym_K] [sym_K+1] ...
Receiver C: [sym_0] [sym_1] [sym_2] ... [sym_K-1] [  X  ] [  X    ] ...
```

Each receiver experiences different packet losses (marked X). But since
RaptorQ decoding works with ANY K' >= K symbols, each receiver independently
collects until it has enough and then decodes. No retransmission is needed.
No feedback channel from receiver to sender is needed.

For N receivers with independent packet loss rate p, the sender needs to
emit approximately K / (1 - p) symbols total. All N receivers decode
simultaneously from this single stream. Compare with TCP unicast, which
requires N separate streams, each requiring K / (1 - p) symbols plus
retransmission overhead from ACK/NACK handshakes.

**Bandwidth Analysis**

Let K = number of source symbols (pages), p = packet loss rate, N = number
of receivers.

```
Traditional TCP (per receiver):
    Expected transmissions: K / (1 - p) + retransmission_overhead
    For N receivers: N * K / (1 - p) * (1 + overhead)
    Total sender bandwidth: O(N * K / (1 - p))

Fountain-coded multicast:
    Sender emits: K * (1 + epsilon) / (1 - p) symbols, where epsilon ~ 0.02
    All N receivers decode from this single stream
    Total sender bandwidth: O(K / (1 - p))
    Bandwidth savings: factor of N

Example:
    K = 1000 pages, p = 5% loss, N = 10 receivers
    TCP: ~10 * 1000 / 0.95 * 1.1 ~ 11,579 transmissions from sender
    Fountain: ~1000 * 1.02 / 0.95 ~ 1,074 transmissions from sender
    Savings: 10.8x
```

**This is the killer feature for edge/IoT deployments** where network
reliability is poor. A sensor network can replicate its database to a
central server over lossy radio links with optimal bandwidth usage.

#### 3.4.3 Fountain-Coded Snapshot Shipping

**Problem:** Initializing a new replica requires transferring the entire
database. A 1GB database over a lossy link is painful with TCP.

**Solution:** The database snapshot is treated as a large source block and
fountain-coded:

```
Snapshot Transfer (P total pages):
  Partition into source blocks of up to 56,403 symbols each (RFC 6330 max)
  For each source block:
    Emit encoding symbols continuously
  Receiver:
    For each source block: collect until K' >= K, decode
  Result: Complete database reconstructed

Advantages:
  - No handshake or acknowledgment needed
  - Receiver can start receiving from any point in the stream
  - Multiple partial receives can be combined
  - Natural multicast: initialize many replicas simultaneously
```

**Source Block Partitioning Algorithm**

RFC 6330 limits each source block to K_max = 56,403 source symbols. For a
database with P pages (where P may exceed K_max), we must partition into
multiple source blocks:

```
partition_source_blocks(P: u32, page_size: u32) -> Vec<SourceBlock>:
// RFC 6330 §4.4.1 source block partitioning
    K_max = 56403
    T = page_size    // symbol size = page size

    if P <= K_max:
        // Single source block covers the entire database
        return [SourceBlock { index: 0, start_page: 1, num_pages: P }]

    // Multiple source blocks needed
    // Partition P pages into Z blocks as evenly as possible
    Z = ceil(P / K_max)
    // RFC 6330 partitioning: Z_L blocks of K_L symbols, Z_S blocks of K_S symbols
    K_L = ceil(P / Z)    // larger block size
    K_S = floor(P / Z)   // smaller block size
    Z_L = P - K_S * Z    // number of larger blocks
    Z_S = Z - Z_L        // number of smaller blocks

    blocks = []
    offset = 1    // page numbers are 1-based
    for i in 0..Z_L:
        blocks.append(SourceBlock { index: i, start_page: offset, num_pages: K_L })
        offset += K_L
    for i in Z_L..(Z_L + Z_S):
        blocks.append(SourceBlock { index: i, start_page: offset, num_pages: K_S })
        offset += K_S

    assert(offset == P + 1)
    return blocks
```

Example: A 1GB database with 4096-byte pages has P = 262,144 pages.

```
Z = ceil(262144 / 56403) = 5 source blocks
K_L = ceil(262144 / 5) = 52429
K_S = floor(262144 / 5) = 52428
Z_L = 262144 - 52428 * 5 = 4 blocks of 52429 pages
Z_S = 5 - 4 = 1 block of 52428 pages

Source blocks:
    Block 0: pages 1-52429      (52,429 pages, ~205 MB)
    Block 1: pages 52430-104858 (52,429 pages, ~205 MB)
    Block 2: pages 104859-157287 (52,429 pages, ~205 MB)
    Block 3: pages 157288-209716 (52,429 pages, ~205 MB)
    Block 4: pages 209717-262144 (52,428 pages, ~205 MB)
```

**Progressive Transfer: Receiver Can Start Using Partial Data**

Because source blocks are independent, the receiver can begin using data
from decoded blocks before the entire database is transferred:

```
progressive_receive():
    for each source block (in any order):
        collect symbols until K' >= K
        decode source block -> recovered pages
        write recovered pages to local database file
        // At this point, queries touching only these pages can execute
        // (read-only, since the database is still being populated)

    after all source blocks decoded:
        verify database integrity (PRAGMA integrity_check)
        mark replica as fully initialized
        enable read-write access
```

This is particularly valuable for large databases: a 1GB database partitioned
into 5 source blocks means the receiver has usable data after receiving just
20% of the total. For read-heavy workloads where the query working set may
be concentrated in a small region of the database, the receiver can answer
queries before the full transfer completes.

**Resume Protocol After Connection Loss**

Because fountain codes are rateless and stateless, resuming after a
connection loss requires no protocol negotiation:

```
resume_protocol():
    // Receiver state is just: for each source block, the set of received symbols
    // This state is persisted locally in a small metadata file:
    // resume_state.bin: [block_id(1B) | num_received(4B) | ISI_bitmap(variable)] per block

    on_connection_loss():
        persist resume_state to disk

    on_reconnect():
        load resume_state from disk
        for each incomplete source block:
            // Tell sender to continue from any ISI (sender doesn't care which)
            // Actually, sender doesn't need to know anything --
            // it just keeps emitting symbols, receiver ignores duplicates
            continue collecting symbols
            // Duplicates (same ISI received twice) are detected and discarded
            // by the decoder in O(1) via a hash set of received ISIs

    // The sender doesn't even need to know the receiver reconnected.
    // If the sender is continuously streaming (e.g., multicast), the receiver
    // simply starts collecting again from wherever the stream currently is.
```

This is fundamentally different from TCP-based transfer protocols, which
must negotiate sequence numbers, retransmit lost segments, and maintain
connection state. Fountain-coded transfer is inherently resumable with
zero overhead.

#### 3.4.4 MVCC Version Chain Compression

**Problem:** Version chains store full copies of each page version. For pages
where only a few bytes change per transaction, this wastes memory.

**Solution (normative):** Store diffs as **XOR deltas** (optionally sparse-encoded)
between adjacent versions in the chain. Deltas are *compression*, not erasure
coding. RaptorQ applies at the ECS object level for durability of delta objects
just like any other object.

```
Version chain for page P:
  V3 (newest): full page data (4096 bytes)
  V2 delta: XOR(V2, V3)  (sparse encoding)
  V1 delta: XOR(V1, V2)  (sparse encoding)

Reconstruction of V1:
  Start from V3 (full data)
  V2 = V3 XOR delta(V2,V3)
  V1 = V2 XOR delta(V1,V2)

Space savings:
  If delta between versions is D bytes out of 4096:
  Full copy: 4096 bytes per version
  Sparse XOR delta: ~D bytes per version (plus small header/indices)
```

**Worked Example with Actual Byte Values**

Consider a B-tree leaf page (page 42) that undergoes three successive
modifications. The page starts as version V1, then V2 and V3 are created
by subsequent transactions:

```
V1 (TxnId=100): Original page (4096 bytes)
    Bytes 0-7:     [0D 00 00 00 03 0F E0 00]  (page header: leaf, 3 cells, cell offset)
    Bytes 8-99:    [cell pointer array + free block list]
    Bytes 100-999: [Cell 1: rowid=5, data="Alice"]
    Bytes 1000-1999: [Cell 2: rowid=10, data="Bob"]
    Bytes 2000-2999: [Cell 3: rowid=15, data="Charlie"]
    Bytes 3000-4095: [free space, all zeros]

V2 (TxnId=105): INSERT rowid=12
    Changes from V1:
    - Bytes 0-7 updated: cell count 3->4, cell content offset changes
    - Bytes 8-99: cell pointer array gains one entry
    - Bytes 2000-2099: Cell 3 shifted right by ~100 bytes
    - Bytes 1900-1999: New Cell 4 inserted (rowid=12, data="Dana")
    Delta size: ~300 bytes modified out of 4096

V3 (TxnId=110): UPDATE SET data="Robert" WHERE rowid=10
    Changes from V2:
    - Bytes 1000-1049: Cell 2 data changed from "Bob" to "Robert"
    - Bytes 0-7: cell content offset may change
    Delta size: ~60 bytes modified out of 4096
```

**Storage under full-copy version chain:**

```
V3: 4096 bytes (full page, always stored in full)
V2: 4096 bytes (full copy)
V1: 4096 bytes (full copy)
Total: 12,288 bytes for 3 versions
```

**Storage under XOR delta compression (stored as ECS objects, optionally
erasure-coded for durability):**

**Clarification:** The delta is a plain XOR (or sparse-encoded XOR), NOT a
RaptorQ encoding of the delta. RaptorQ operates at the ECS object level to
provide erasure-coded durability for *any* object, including delta objects.
The two concerns are separate:
- **Delta compression:** XOR(V_old, V_new) → sparse representation.
- **Durability:** The resulting delta object is stored as an ECS object and
  MAY have RaptorQ repair symbols generated for it (like any ECS object).

```
V3: 4096 bytes (full page, stored as ECS object)
V2 delta: XOR(V2, V3) → sparse encoding
    V2 XOR V3 has ~60 non-zero bytes out of 4096
    Sparse representation: [delta_header(8B) | (offset,len,data)* (~80B)]
    Total: ~88 bytes (stored as ECS object)

V1 delta: XOR(V1, V2) → sparse encoding
    V1 XOR V2 has ~300 non-zero bytes out of 4096
    Sparse representation: [delta_header(8B) | (offset,len,data)* (~340B)]
    Total: ~348 bytes (stored as ECS object)

Total: 4096 + 88 + 348 = 4,532 bytes for 3 versions
Savings: 63% reduction (4,532 vs 12,288)
```

**When to Use Delta vs Full Copy (Threshold Analysis)**

Delta compression is not always beneficial. When the delta is large (many
bytes changed), the overhead of the delta header plus the compressed delta
may approach or exceed the full page size. The decision threshold:

```
use_delta(old_page, new_page) -> bool:
    delta = old_page XOR new_page
    nonzero_bytes = count_nonzero(delta)

    // Fixed overhead: delta header (8 bytes) + sparse encoding overhead
    OVERHEAD = 16

    // Sparse delta size is approximately nonzero_bytes * 1.05
    // (the 5% accounts for run headers/varints/padding for small deltas)
    estimated_delta_size = OVERHEAD + (nonzero_bytes as f64 * 1.05) as usize

    // COST MODEL (Extreme Optimization Discipline):
    // The threshold balances memory savings vs CPU cost of delta application.
    //   t_copy = page_size / mem_bandwidth     (full-page copy cost)
    //   t_delta = delta_size / mem_bandwidth + delta_ops * t_per_op  (apply cost)
    //   cache_benefit = (page_size - delta_size) * cache_value_per_byte
    //
    // Use delta when: cache_benefit > (t_delta - t_copy)
    // For page_size=4096, mem_bandwidth=40GB/s, t_per_op~1ns:
    //   t_copy = 100ns, t_delta(25% savings) = 75ns + 20ns = 95ns
    //   cache_benefit(25% savings) = 1024 bytes * cache pressure factor
    //
    // The 25% threshold (3072 bytes) is the crossover point where the cache
    // capacity benefit of smaller version entries outweighs the marginal CPU
    // cost of delta reconstruction. This is hardware-dependent; on systems
    // with very constrained cache (embedded ARM), the threshold could drop
    // to 10%. On large-cache server CPUs, even 5% savings justifies delta.
    // Configurable via PRAGMA fsqlite.delta_threshold_pct (default: 25).
    return estimated_delta_size < page_size * 3 / 4
```

Typical thresholds for T = 4096:

```
| Workload                    | Avg bytes changed | Delta size | Use delta? |
|-----------------------------|-------------------|------------|------------|
| Single-row UPDATE (leaf)    | 20-100            | ~120       | Yes (97% savings) |
| INSERT into leaf page       | 100-500           | ~540       | Yes (87% savings) |
| B-tree split (interior)     | 2048 (half page)  | ~2160      | Yes (47% savings) |
| VACUUM (page rewrite)       | 4096 (full page)  | ~4320      | No (delta > page) |
| Bulk INSERT (new page)      | 4096 (full page)  | ~4320      | No |
```

**Compression Ratio Estimates for Different Workloads**

```
| Workload                          | Avg versions/page | Avg delta | Compression ratio |
|-----------------------------------|-------------------|-----------|-------------------|
| OLTP (single-row updates)         | 5-10              | 50 bytes  | 10-15x            |
| Mixed read-write web app          | 3-5               | 200 bytes | 4-6x              |
| Batch import (sequential inserts) | 2-3               | 1500 bytes | 1.5-2x           |
| Analytics (read-heavy, few writes)| 1-2               | N/A       | 1x (no versions)  |
```

This is particularly effective for B-tree interior pages where only child
pointers change during splits, and for leaf pages where insertions affect
only a portion of the page.

#### 3.4.5 GF(256) Patch Algebra: Encoding, Not Write-Merge Correctness

This section is about the **byte algebra** that underlies patch encodings.
It is *not* a license to merge arbitrary structured SQLite pages by checking
"byte-disjointness".

**Goal:** Reduce aborts from page-granularity first-committer-wins (FCW) when
two transactions perform *logically commuting* operations that nevertheless
touch the same page (see §5.10).

**Critical distinction (normative):**

- **Byte algebra:** Pages are byte vectors; XOR-deltas compose linearly.
- **SQLite page semantics:** Many page types are **self-referential** (internal
  pointers, variable layout, derived metadata). A change to one byte range can
  change the *meaning* of bytes in a different range without touching them.
  Therefore, byte-disjointness is not a sufficient merge condition.

##### Lemma (Disjoint-Delta Byte Composition)

Let a page be a vector `P ∈ GF(2)^n` (bit vector). Let `P0` be the page at a
transaction's snapshot point. Two transactions produce:

- `P1 = P0 ⊕ D1` where `D1 = P1 ⊕ P0`
- `P2 = P0 ⊕ D2` where `D2 = P2 ⊕ P0`

Define support:
```
supp(D) = { i : D[i] != 0 }   // bit positions where D is non-zero
```

If `supp(D1) ∩ supp(D2) = ∅`, then:
```
Pmerge = P0 ⊕ D1 ⊕ D2
```
is the unique byte vector that equals `P1` on `supp(D1)`, equals `P2` on
`supp(D2)`, and equals `P0` elsewhere.

This lemma is **purely about vectors**. It does not imply semantic correctness
for structured pages.

##### Counterexample (Lost Update on B-tree Pages)

SQLite B-tree pages contain internal pointers (cell pointer array offsets,
freeblock list links) and are routinely **defragmented**, which moves cells and
updates pointers.

Consider two transactions that start from the same snapshot `P0`:

1. `T1` moves a cell from offset `X` to offset `Y` (defragmentation or balance):
   it updates the pointer entry to `Y`, and writes the cell bytes at `Y`.
2. `T2` updates the same logical cell's payload bytes at the *old* offset `X`.

It is possible for `supp(D1)` and `supp(D2)` to be disjoint if `T1` does not
overwrite `X` (leaves stale bytes or a freeblock). A naive XOR merge produces:

- pointer now references `Y` (from `T1`)
- cell at `Y` contains the **old** payload copied by `T1` from `P0`
- updated payload written by `T2` remains at `X`, now unreachable garbage

The merged page can satisfy all structural invariants (ordering, free space,
checksums) while still being **logically wrong** (a real lost update).

##### Normative Rule (Merge Safety)

1. **Raw byte-disjoint XOR merge MUST NOT be used to accept a commit for any
   SQLite file-format page kind whose semantics include internal pointers or
   variable layout.** This includes (at minimum) all B-tree pages, overflow
   pages, freelist pages, and pointer-map pages.
2. For such pages, a merge is only permitted when the engine can justify
   semantic correctness by construction:
   - deterministic rebase via intent replay (§5.10.2), and/or
   - structured page patch merge keyed by stable identifiers (§5.10.3),
     with post-merge invariant checks and proof artifacts (§5.10.5).
3. XOR/`GF(256)` deltas remain useful as an **encoding** of patches and for
   history compression. They are not a correctness criterion.

##### Configuration: Write-Merge Policy (PRAGMA)

Write-merge behavior is controlled by:

```
PRAGMA fsqlite.write_merge = OFF | SAFE | LAB_UNSAFE;
```

- `OFF`: FCW conflicts abort/retry (no merge attempts).
- `SAFE` (default for `BEGIN CONCURRENT`): enable §5.10 merges that are justified
  semantically (rebase + structured patches). Raw XOR merge is forbidden for
  structured SQLite pages.
- `LAB_UNSAFE`: permits additional *debug-only* merge experiments (e.g., raw XOR
  merges on explicitly-declared opaque pages). This mode MUST be rejected in
  release builds and MUST never enable raw XOR merging for B-tree/overflow/
  freelist/pointer-map pages.

#### 3.4.6 Erasure-Coded Page Storage

For maximum durability, database pages themselves can be stored with redundancy:

```
Page group (G pages):
  RaptorQ-encode G source pages into G + R symbols
  Store all G + R symbols across storage

On read:
  Read G symbols (prefer source symbols for zero-decode overhead)
  If any corrupted: decode from remaining symbols

Effect: Tolerates up to R corrupted pages per group
```

**Page Group Partitioning**

The database is divided into page groups. The partitioning strategy must
balance several concerns:
- Group size G determines the granularity of redundancy (larger G = more
  efficient encoding but larger blast radius for correlated corruption)
- Groups should align with B-tree structure for locality
- The first page (database header) requires special handling

**Derivation of G and R (Alien-Artifact Discipline):**

G and R are chosen by minimizing expected cost over the corruption model:

```
min_{G,R} [ P_loss(G,R,p) * L_loss + (R/G) * L_overhead ]
```

where `P_loss(G,R,p) = sum_{i=R+1}^{G+R} C(G+R,i) * p^i * (1-p)^(G+R-i)`
(Durability Bound theorem, Section 23). For design-time calculation we use a
representative sector corruption design point `p_design = 10^-4`; at runtime,
the durability autopilot maintains living estimates and conservative bounds for
`p` (§3.5.12) and MAY harden by increasing redundancy.
`L_loss = 10^9` (data loss cost in arbitrary units), `L_overhead = 1` per
1% space overhead.

| G   | R  | Overhead (R/G) | P_loss (p=10^-4) | Expected cost |
|-----|----|----------------|------------------|---------------|
| 32  | 2  | 6.25%          | ~6 x 10^-9       | 6.25 + ~0     |
| 64  | 4  | 6.25%          | ~1 x 10^-13      | 6.25 + ~0     |
| 64  | 2  | 3.12%          | ~5 x 10^-8       | 3.12 + ~0     |
| 128 | 4  | 3.12%          | ~3 x 10^-13      | 3.12 + ~0     |
| 128 | 8  | 6.25%          | ~2 x 10^-23      | 6.25 + ~0     |

At p=10^-4, P_loss is negligible for all reasonable (G,R) pairs. The
binding constraint is **correlated failure**: if a firmware bug, power
failure, or media degradation affects multiple contiguous pages, the
independence assumption breaks. The blast radius of correlated corruption
is bounded by the group size G. Choosing G=64 (256KB) limits the blast
radius to 256KB while keeping encoding/decoding tractable (RaptorQ on 64
symbols is ~2us). R=4 gives tolerance for up to 4 corrupted pages per
group, which covers all observed single-event corruption patterns in the
SQLite crash-test corpus.

The header page gets R=4 for G=1 (400% redundancy) because the header is
a single point of failure for the entire database. The expected cost
framework gives `L_loss_header >> L_loss_data` (losing the header means
losing the database, not just one page), justifying the asymmetric policy.

```
partition_page_groups(db_size_pages: u32) -> Vec<PageGroup>:
    G = 64    // Derived: 256KB blast radius, ~2us encode/decode
    R = 4     // Derived: tolerates 4 corrupted pages per group

    groups = []
    pgno = 1    // pages are 1-based

    // Special group for the database header page
    // (page 1 is critical; give it extra redundancy)
    groups.append(PageGroup {
        start: 1,
        size: 1,
        repair: 4,    // 4 repair symbols for just 1 page = 400% redundancy
    })
    pgno = 2

    // Group remaining pages in chunks of G
    while pgno <= db_size_pages:
        remaining = db_size_pages - pgno + 1
        group_size = min(G, remaining)
        groups.append(PageGroup {
            start: pgno,
            size: group_size,
            repair: R,
        })
        pgno += group_size

    return groups
```

**SQLite file-format compatibility rule (normative):** In Compatibility mode,
the SQLite `.db` file MUST remain a pure page array of size `P * page_size`. It
MUST NOT embed any FrankenSQLite-specific "repair region" past the last page.
(Many SQLite tools rewrite/truncate the database to exactly
`db_size_pages * page_size` during VACUUM/backup/restore; appending extra bytes
invites silent loss.)

Therefore, the repair symbols for each page group are stored in a **sidecar**
file, analogous to `.wal-fec`:

- **Compatibility mode:** `foo.db-fec` adjacent to `foo.db` (or under the
  database's `.fsqlite/` directory).
- **Native mode:** the same idea is represented as ECS objects in the symbol
  store; no SQLite `.db` file is ever treated as authoritative state.

**Sidecar layout (Compatibility mode):**

```
foo.db       -- standard SQLite database file (no trailing repair region)
foo.db-fec   -- page-group repair symbols + metadata
```

`foo.db-fec` MUST begin with a small header that is sufficient to locate/repair
page 1 even if the SQLite header page is corrupted. At minimum it MUST
redundantly store:

- `page_size`
- the page-group policy parameters (G/R and the page-1 special case)
- a digest binding it to the target `foo.db` generation (so stale sidecars are
  detected and ignored)

**Read Path with On-the-Fly Repair**

The read path is modified to detect and repair corrupted pages transparently:

```
read_page_with_repair(pgno: PageNumber) -> Result<PageData>:
    // Step 1: Read the page directly (fast path, no overhead)
    page = read_raw_page(pgno)
    checksum = compute_xxhash3(page)

    if checksum == stored_checksum(pgno):
        return Ok(page)    // Page is intact, zero overhead

    // Step 2: Page is corrupted. Attempt on-the-fly repair.
    // The group lookup uses `.db-fec` geometry and MUST NOT depend on page 1.
    group = find_page_group_from_db_fec(pgno)

    // Read all pages in the group + repair symbols
    available_symbols = []
    for pg in group.start..(group.start + group.size):
        if pg == pgno:
            continue    // Skip the corrupted page
        page_data = read_raw_page(pg)
        if verify_checksum(pg, page_data):
            available_symbols.append((pg - group.start, page_data))    // ISI = offset within group

    // Read repair symbols for this group
    for r in 0..group.repair:
        repair_data = read_repair_symbol_from_db_fec(group, r)
        if verify_checksum_repair(group, r, repair_data):
            available_symbols.append((group.size + r, repair_data))    // ISI = G + r

    if available_symbols.len() >= group.size:
        // Enough symbols to decode
        decoder = RaptorQDecoder::new(group.size, page_size)
        for (isi, data) in available_symbols:
            decoder.add_symbol(isi, data)
        recovered = decoder.decode()?
        // Extract the corrupted page from recovered data
        repaired_page = recovered[pgno - group.start]

        // Write back the repaired page (self-healing) using the normal durability path.
        write_raw_page(pgno, repaired_page)
        update_checksum(pgno, compute_xxhash3(repaired_page))

        return Ok(repaired_page)
    else:
        return Err(SQLITE_CORRUPT)    // Unrecoverable: too many corrupted pages in group
```

**Interaction with B-tree Page Types**

Different B-tree page types have different corruption characteristics and
repair priorities:

```
| Page Type              | Corruption Impact | Repair Priority | Notes |
|------------------------|-------------------|-----------------|-------|
| Interior table (0x05)  | Lose subtree access | HIGH         | Can be rebuilt from leaves in theory |
| Leaf table (0x0D)      | Lose row data      | CRITICAL      | Contains actual user data |
| Interior index (0x02)  | Lose index subtree | MEDIUM        | Rebuildable via REINDEX |
| Leaf index (0x0A)      | Lose index entries | MEDIUM        | Rebuildable via REINDEX |
| Overflow page          | Lose large values  | HIGH          | Part of a chain; one loss breaks chain |
| Freelist trunk/leaf    | Lose free pages    | LOW           | VACUUM can rebuild |
| Pointer map (auto-vac) | Lose page mapping  | HIGH          | Needed for auto-vacuum |
```

Page grouping should ideally keep related pages together (e.g., a parent
interior page and its child leaves in the same group) so that correlated
corruption (e.g., a bad disk sector affecting contiguous pages) is more
likely to be repairable. However, this creates a tension: correlated
corruption within a group is the worst case for repair (all corrupted
pages might be in the same group). The default grouping by page number
(sequential groups of 64) is a reasonable compromise that works well
for the common case of random single-page corruption.

For maximum resilience, a future enhancement could interleave group
membership (page i belongs to group i mod Z), ensuring that contiguous
disk corruption distributes across multiple groups. This is analogous to
RAID striping and would be configurable via PRAGMA.

This transforms the database file from "one bit flip = SQLITE_CORRUPT" to
"R bit flips per group = automatically recovered." Combined with the
self-healing WAL, this creates defense in depth where data corruption
becomes a mathematical near-impossibility.

#### 3.4.7 Replication Architecture (ECS-Native, Symbol-Native)

The low-level transport mechanics are specified in §3.4.2 (fountain-coded
replication) and §3.4.3 (snapshot shipping). This section specifies the
high-level replication architecture: roles, modes, routing, convergence,
durability guarantees, and security.

**Replication Roles and Modes:**

We define two modes:

1. **Leader commit clock (V1 default):** One node publishes the authoritative
   marker stream. Other nodes replicate objects + markers and serve reads.
   Writers can still be concurrent within the leader (MVCC). This keeps
   semantics sharp and testable.
2. **Multi-writer (experimental):** Multiple nodes publish capsules. Marker
   stream ordering becomes a distributed problem (not V1 default). Requires
   distributed consensus integration (see §21.4).

**What We Replicate (Object Classes):**

We replicate ECS objects, not files:
- `CommitCapsule` objects (and patch objects they reference).
- `CommitMarker` records (the commit clock).
- `IndexSegment` objects (page version, object locator, manifest).
- SSI witness-plane objects (§5.6.4, §5.7): `ReadWitness` / `WriteWitness` / `WitnessDelta` /
  `WitnessIndexSegment` / `DependencyEdge` / `CommitProof` / `AbortWitness` / `MergeWitness`.
- `CheckpointChunk` and `SnapshotManifest` objects.
- Optionally: `DecodeProof` / audit traces for debugging.

**Transport Substrate (asupersync):**

We build replication on:
- `asupersync::transport::{SymbolSink, SymbolStream, SymbolRouter,
  MultipathAggregator, SymbolDeduplicator, SymbolReorderer}`
- `asupersync::transport::mock::SimNetwork` for tests.
- `asupersync::security::{SecurityContext, AuthenticatedSymbol}` for
  security.

**Symbol Routing: Consistent Hashing + Policies:**

We assign **symbols** to nodes, not objects:
- Encode object into `K_total` source symbols + `R` repair symbols.
- Assign each symbol to one or more nodes via
  `asupersync::distributed::consistent_hash`.
- Replication factor and `R` determine node-loss tolerance, loss tolerance,
  and catch-up rate.

**Anti-Entropy Loop (Convergence Protocol):**

Replication MUST converge even if nodes are offline. The anti-entropy loop:

1. **Exchange tips:** Latest `RootManifest` ObjectId, latest marker stream
   position, optional index segment tips.
2. **Compute missing:** ObjectId set difference via manifests/index summaries.
3. **Request symbols:** For missing objects.
4. **Stream until decode:** Send symbols until the receiver reports completion
   (typically around `K_total + ε` symbols). Stop early.
5. **Persist and update:** Decoded objects persisted locally; caches refreshed.

Because objects are fountain-coded, a requester can ask for "any symbols for
object X" without tracking which ESIs it already has. The responder sends
whatever is convenient (source first, then repairs).

**Quorum Durability (Commit-Time Policy):**

Commit can be declared durable only after a quorum of symbol stores have
accepted enough symbols. We reuse asupersync quorum semantics
(`asupersync::combinator::quorum`):

- Local-only: `quorum(1, [local_store])`
- 2-of-3: `quorum(2, [storeA, storeB, storeC])`

Integrated into the commit protocol: the marker is not published until the
durability policy's quorum reports satisfaction.

**Consistency Checking (Sheaf + TLA+ Export):**

We treat distributed correctness as first-class:
- **Sheaf check:** `asupersync::trace::distributed::sheaf` detects anomalies
  that pairwise comparisons miss (phantom global commits that no single node
  witnessed end-to-end).
- **TLA+ export:** `asupersync::trace::tla_export` exports traces into TLA+
  behaviors for model checking of bounded scenarios (commit, replication,
  recovery).

**Security (Authenticated Symbols):**

Replication MAY be secured by enabling an
`asupersync::security::SecurityContext`:
- Writers attach `auth_tag` to `SymbolRecord`s (see §3.5.2) using epoch-scoped keys
  (§4.18.2).
- Receivers verify tags before accepting symbols for decoding.
- Unauthenticated/corrupted symbols are ignored (repair handles loss).
- Security is orthogonal: it does not change ECS semantics; it only rejects
  unauthenticated bytes before they can influence decoding.

### 3.5 ECS: The Erasure-Coded Stream Substrate

ECS is the universal persistence layer for Native mode. Every durable object
in FrankenSQLite -- commit capsules, page snapshots, WAL segments, index
checkpoints, schema snapshots -- is stored as an ECS object. ECS provides
content addressing, self-describing encoding, deterministic repair symbol
generation, and rebuildable indexes.

#### 3.5.1 ObjectId: Content-Addressed Identity

Every ECS object is identified by its ObjectId. To ensure deterministic addressing across all replicas:

**Canonical Encoding Rules (Deterministic Bytes, Not "Serde Vibes"):**
- **Explicit versioned wire format:** The byte stream must be fully defined, not dependent on compiler layout or serialization library defaults.
- **Little-endian integers:** All fixed-width integers use little-endian byte order (matches native x86/ARM/WASM).
- **Sorted map keys:** If map-like structures are encoded, keys must be sorted lexicographically by byte representation.
- **No floating-point in headers:** Canonical headers must use fixed-point or integers to avoid NaN/rounding non-determinism.

**ObjectId Construction:**

```
ObjectId = Trunc128( BLAKE3( "fsqlite:ecs:v1" || canonical_object_header || payload_hash ) )
```

We use BLAKE3 for speed and security, truncated to 128 bits (16 bytes) for storage efficiency. The prefix "fsqlite:ecs:v1" prevents cross-protocol collisions.

**ObjectId properties:**
- Immutable: once created, an ObjectId never changes. Objects are write-once-read-many.
- Content-addressed: identical objects have identical ObjectIds. Deduplication is automatic.
- Collision-resistant: 128-bit BLAKE3 is sufficient for all non-adversarial collisions and most adversarial ones in this context.

#### 3.5.2 Symbol Record Envelope

Every ECS object is stored as one or more **symbol records**. A symbol record is the atomic unit of physical storage -- the smallest thing that can be read, written, verified, and transmitted.

```
SymbolRecord := {
    magic       : [u8; 4],      -- 0x46 0x53 0x45 0x43 ("FSEC")
    version     : u8,           -- envelope version (1)
    object_id   : [u8; 16],     -- ObjectId (128-bit)
    oti         : OTI,          -- RaptorQ Object Transmission Information
    esi         : u32,          -- Encoding Symbol Identifier (which symbol this is)
    symbol_size : u32,          -- T: symbol size in bytes
    symbol_data : [u8; T],      -- the actual RaptorQ encoding symbol
    flags       : u8,           -- bitflags (see below)
    frame_xxh3  : u64,          -- xxhash3 of all preceding fields (fast integrity)
    auth_tag    : [u8; 16],     -- Optional: HMAC/Poly1305 for authenticated transport
}

OTI := {
    F  : u64,       -- transfer length (original object size in bytes)
    Al : u16,       -- symbol alignment (always 4 for FrankenSQLite)
    T  : u16,       -- symbol size (derived from F, Al, and target symbol count)
    Z  : u32,       -- number of source blocks
    N  : u32,       -- number of sub-blocks per source block
}
```

**Self-describing property:** A symbol record contains everything needed to decode it: the ObjectId identifies which object this symbol belongs to, the OTI provides the RaptorQ parameters, and the ESI identifies which encoding symbol this is. A decoder collecting K' symbols with the same ObjectId can reconstruct the original object without any external metadata.

**Flags (normative):**

- `0x01 = SYSTEMATIC_RUN_START`: This record is the first source symbol (`esi = 0`)
  and the writer attempted to place the entire systematic run (`esi = 0..K_source-1`)
  contiguously in the local symbol log.

The local symbol store MAY define additional flags, but they MUST be treated as
advisory optimization hints. Correctness never depends on them.

**Authenticated symbols (normative when enabled):**

`auth_tag` is the optional authenticity check for symbols received from
untrusted transport (replication, remote tier). When enabled, receivers MUST
verify `auth_tag` before accepting a symbol for decoding.

- Enable via `PRAGMA fsqlite.symbol_auth = on` (default: `off` for local-only
  durability).
- If `PRAGMA durability = quorum(M)` and the transport is not already
  authenticated, `symbol_auth` MUST be enabled.

**Tag construction (normative):**

Let `epoch_id` be the `SymbolSegmentHeader.epoch_id` of the segment containing
this `SymbolRecord` (§3.5.4.2). Derive the epoch key `K_epoch` as in §4.18.2.
Then compute:

```
auth_tag = Trunc128( BLAKE3_KEYED( K_epoch,
                  "fsqlite:symbol-auth:v1" || bytes(magic..frame_xxh3) ) )
```

**Failure behavior (normative):**
- If `symbol_auth = on` and `auth_tag` verification fails, the symbol MUST be
  rejected (it MAY still be counted as a corruption observation for §3.5.12).
- If `symbol_auth = off`, `auth_tag` MUST be all-zero and MUST be ignored.

**Systematic read fast path (hybrid decode):**

RaptorQ is systematic: the first `K_source` symbols are (a padded form of) the
original bytes. Therefore, for local reads, the engine SHOULD attempt:

1. Locate `SYSTEMATIC_RUN_START` for the object (via object locator / index).
2. Read `K_source = ceil(F / T)` symbol records sequentially.
3. Verify per-record `frame_xxh3` (and `auth_tag` if enabled).
4. Concatenate `symbol_data` payloads and truncate to `F` bytes.

If all checks pass, decoding is complete **without** invoking GF(256) matrix
solve. If any record is missing/corrupt, fall back to the general fountain-code
decoder (collect any `K'` symbols including repairs; decode; optionally produce
`DecodeProof`).

This design ensures:
- Happy-path reads are "read + checksum" (low latency).
- Repair-path reads are "decode + proof" (self-healing, auditable).

**Symbol record sizing:** The symbol size `T` is object-type-aware and is encoded
in the object's OTI (self-describing). The default policy is specified in
§3.5.10 (and is versioned in `RootManifest` so replicas decode correctly).
Examples:
- `PageHistory` and full page images typically use `T = page_size` (one source
  symbol per page).
- `CommitCapsule` defaults to `T = min(page_size, 4096)` to keep happy-path
  reads "range read + checksum" while avoiding excessive symbol counts (`K_source`)
  for medium/large capsules.

#### 3.5.3 Deterministic Repair Symbol Generation

Given an ECS object and a repair symbol count `R`, the set of repair symbols
is deterministic: the same object and same `R` always produce the same repair
symbols. This enables:

1. **Verification without the original object:** Given the ObjectId and repair
   symbols, any node can verify that the repair symbols are valid by
   re-encoding from the source symbols.
2. **Incremental repair:** If a storage node discovers corruption, it can
   request specific ESIs from peers and verify them independently.
3. **Idempotent writes:** Writing the same repair symbols twice has no effect.

The repair symbol budget is controlled per-object-type:
```
PRAGMA raptorq_overhead = <percent>    -- default: 20%
```

This means: for `K_source` source symbols, budget deterministic repair symbols:

```
slack_decode = 2  // V1 default: target K_source+2 decode slack (RFC 6330 Annex B)
R = max(slack_decode, ceil(K_source * overhead_percent / 100))
```

The additive `slack_decode` is not "extra safety for erasures"; it is there to
drive RaptorQ's *exactly-K* decode failure probability into the floor. The
multiplicative term is the erasure/corruption budget.

**Important:** There are two distinct "overheads":
- **Decode slack (additive):** the number of *extra* symbols beyond `K_source`
  needed to make decode failure probability negligible (V1 targets `K_source+2`
  per RFC 6330 Annex B; see §3.1.1).
- **Loss budget (multiplicative):** how many symbols we can afford to lose to
  erasures/corruption and still collect `K_source + slack_decode` survivors.

Therefore the tolerated erasure fraction without coordination is approximately:

```
loss_fraction_max ≈ max(0, (R - slack_decode) / (K_source + R))
```

and for large `K_source` it approaches `R/(K_source+R) ≈ overhead/(100+overhead)`.
Small objects (small `K_source`) are dominated by the additive slack; the
implementation MUST clamp policies to avoid under-provisioning.

**Adaptive overhead (alien-artifact, optional but recommended):**

The engine MAY auto-tune `PRAGMA raptorq_overhead` using anytime-valid evidence:

- Maintain an e-process monitor on symbol survival/corruption (Section 4.3).
- If evidence suggests the symbol erasure rate exceeds the assumed budget,
  increase `overhead_percent` (and thus `R`) until the derived `loss_fraction_max`
  clears the new budget with margin.
- If evidence suggests the erasure rate is far below budget for a sustained
  period, the engine MAY decrease `overhead_percent` to reduce space/write
  amplification, but only under a conservative loss matrix where the cost of a
  false decrease (future data loss risk) dwarfs the benefit of saved bytes.

Every automatic retune MUST emit an evidence ledger: the prior/assumed budget,
the observed e-value trajectory, the chosen new overhead, and the implied
`loss_fraction_max` bound.

#### 3.5.4 Local Physical Layout (Native Mode)

In Native mode, the database directory has the following layout, optimized for
sequential write throughput (log-structured):

```
foo.db.fsqlite/
├── ecs/
│   ├── root              -- tiny mutable pointer file (atomic update)
│   │                     -- contains: [latest_manifest_object_id (16B) | checksum (8B)]
│   ├── symbols/          -- append-only symbol record logs
│   │   ├── segment-000000.log
│   │   ├── segment-000001.log
│   │   └── ...
│   └── markers/          -- append-only commit marker stream
│       └── segment-000000.log
├── cache/                -- rebuildable derived state (NOT source of truth)
│   ├── object_locator.cache -- map ObjectId -> (SegmentId, Offset)
│   ├── btree.cache       -- materialized B-tree pages (hot set)
│   ├── index.cache       -- secondary index pages
│   └── schema.cache      -- parsed schema
└── compat/               -- optional compatibility export
    ├── foo.db            -- standard SQLite database file
    └── foo.db-wal        -- standard WAL (if compat checkpoint active)
```

**Key invariants:**
- `ecs/` is the source of truth. Everything in `cache/` is rebuildable from
  `ecs/`. Deleting `cache/` is always safe (costs a rebuild).
- `ecs/symbols/*.log` are immutable once rotated.
- `ecs/root` is the **ONLY** mutable file in the ECS directory. It is updated
  atomically via `write-to-temp` + `rename`.
- `compat/` is an export target for compatibility mode. It is NOT the source
  of truth in Native mode.

#### 3.5.4.1 Commit Marker Stream Format (Random-Access, Auditable)

The CommitMarker stream under `ecs/markers/` is the **total order** of commits.
It MUST be:

- append-only,
- record-aligned (fixed-size records),
- seekable by `commit_seq` in O(1),
- auditable (tamper-evident hash chain).

**On-disk encoding (normative):**
- All fixed-width integers are encoded little-endian (§3.5.1).
- All sizes below are **byte-exact**. Implementations MUST NOT use language
  `sizeof(struct)` / `mem::size_of::<T>()` for on-disk offset math (padding would
  silently corrupt indexing).

**V1 constants (normative):**

```
MARKER_SEGMENT_HEADER_BYTES := 36
COMMIT_MARKER_RECORD_BYTES  := 88
```

**Marker segment file:**

`ecs/markers/segment-XXXXXX.log` stores a contiguous range of markers. Each file
starts with a header, followed by a dense array of fixed-size records.

```
MarkerSegmentHeader := {
  magic           : [u8; 4],    -- "FSMK"
  version         : u32,        -- 1
  segment_id      : u64,        -- monotonic identifier (matches filename)
  start_commit_seq: u64,        -- first commit_seq stored in this segment
  record_size     : u32,        -- bytes per CommitMarkerRecord (MUST be 88 in V1)
  header_xxh3     : u64,        -- xxhash3 of all preceding header fields
}

CommitMarkerRecord := {
  commit_seq         : u64,
  commit_time_unix_ns: u64,
  capsule_object_id  : [u8; 16],
  proof_object_id    : [u8; 16],
  prev_marker_id     : [u8; 16],  -- 0 for genesis
  marker_id          : [u8; 16],  -- domain-separated BLAKE3-128 of record prefix (see MarkerId definition)
  record_xxh3        : u64,       -- xxhash3 of all preceding fields in this record
}
```

**MarkerId definition (normative):**

`marker_id` MUST be computed with domain separation:

```
marker_id = Trunc128( BLAKE3( "fsqlite:marker:v1" || record_prefix_bytes ) )
```

where `record_prefix_bytes` is the canonical byte encoding of:
`(commit_seq, commit_time_unix_ns, capsule_object_id, proof_object_id, prev_marker_id)`.

`marker_id` is both:
- the marker's integrity hash (tamper-evident), and
- an `ObjectId`-compatible identifier (128-bit BLAKE3) suitable for use in
  `RootManifest.current_commit` and `CommitMarker.prev_marker`.

**Density invariant (normative, required for O(1) seeks):**

- Within any marker segment, the record at slot index `i` (0-based) MUST be the
  marker for `commit_seq = start_commit_seq + i`.
- The marker stream MUST NOT have gaps in `commit_seq` for committed markers.
  If record `commit_seq = N` exists, then every `commit_seq < N` MUST also have
  a record (except before genesis).

This is not an aesthetic choice: the O(1) seek formula below is only correct if
the on-disk marker stream is a dense array in `commit_seq` order.

**CommitSeq allocation (native mode, gap-free, crash-safe):**

`commit_seq` MUST be derived from the **physical marker stream tip** inside the
same cross-process serialized section used to append the marker record.
Implementations MUST NOT allocate `commit_seq` from an in-memory counter that
can advance without a durably appended marker (e.g., `AtomicU64::fetch_add` in
shared memory) because a crash after allocation but before marker persistence
would create a permanent gap and break O(1) indexing.

The allocator for the next commit sequence number is:

```
// Inside the marker-append lock / commit section.
let n_records = floor((segment_file_len_bytes - MARKER_SEGMENT_HEADER_BYTES) / record_size);
next_commit_seq = start_commit_seq + n_records;
```

**Torn tail handling (normative):**

- If a marker segment ends with a partial record (i.e.,
  `(segment_file_len_bytes - MARKER_SEGMENT_HEADER_BYTES) % record_size != 0`),
  those trailing bytes MUST be treated as a torn-write tail and MUST be ignored
  for `commit_seq` allocation. Recovery MAY truncate them.
- If the last complete record fails `record_xxh3` verification, recovery MUST
  treat it (and any subsequent records in the segment) as corrupt/torn and MUST
  ignore it for `commit_seq` allocation. (The simplest safe policy: scan forward
  from the segment start until the first invalid record; the valid prefix is
  authoritative.)

**O(1) seek by commit_seq (normative):**

Given a target `commit_seq`, the reader locates the segment containing it
(by either scanning segment headers, or using a fixed rotation policy).

**Fixed rotation policy (recommended):**

- `markers_per_segment` is a constant (default: 1,000,000).
- `segment_id := commit_seq / markers_per_segment`.
- `start_commit_seq := segment_id * markers_per_segment`.

Then compute:

```
offset = MARKER_SEGMENT_HEADER_BYTES
       + (commit_seq - start_commit_seq) * record_size
```

It then reads exactly one `CommitMarkerRecord`, verifies `record_xxh3`, and
verifies that `record.commit_seq == commit_seq`.

**Binary search by time (enables time travel):**

Because `commit_time_unix_ns` is monotonic non-decreasing in `commit_seq`,
mapping a timestamp to a commit uses binary search over `[0, latest_commit_seq]`
with random-access record reads. Complexity: `O(log N)` record reads.

**Fork/divergence detection (replication correctness):**

To check whether two replicas share the same commit history prefix:
- Compare `(latest_commit_seq, latest_marker_id)`.
- If `latest_commit_seq` differs, use the smaller one as the comparison bound.
- If marker ids mismatch, binary search in `commit_seq` space for the greatest
  `k` such that `marker_id(seq=k)` matches on both replicas. This yields the
  greatest-common-prefix commit in `O(log N)` marker reads without scanning.

**Optional (recommended for distributed replication): Merkle Mountain Range (MMR)**

The hash chain (`prev_marker_id`) is tamper-evident, but it only supports
efficient verification in one direction:
- verifying a *specific* marker requires either trusting the local marker stream,
  or scanning backward to a trusted anchor (O(length)).

For replication and audit, we want **small proofs**:
- "marker at commit_seq = k is in the history" (inclusion proof)
- "these replicas share the same prefix up to k" (prefix proof)

Use a **Merkle Mountain Range** (MMR) over the marker stream. This is an
append-only Merkle accumulator that supports O(log N) proofs without rewriting
history.

**Leaf hash (normative if MMR enabled):**

```
leaf_hash(seq=k) = BLAKE3_256( "fsqlite:mmr:leaf:v1"
                               || le_u64(commit_seq)
                               || marker_id )
```

**Node hash (normative if MMR enabled):**

```
node_hash = BLAKE3_256( "fsqlite:mmr:node:v1" || left || right )
```

**MMR state:**
- Maintain MMR peaks for the current marker stream tip.
- Persist periodic `MMRCheckpoint` objects (e.g., every 1,048,576 markers) that
  store:
  - `n_leaves` (= latest_commit_seq + 1 if commit_seq is 0-based dense),
  - `peaks[]`,
  - `bagged_root = BLAKE3_256("fsqlite:mmr:bag:v1" || peaks concatenated)`

**Replication use:**
- Replicas exchange `(latest_commit_seq, latest_marker_id, bagged_root)`.
- If `bagged_root` matches at the same `n_leaves`, the histories are identical
  without any further reads.
- Otherwise, replicas can request inclusion/prefix proofs to identify the
  greatest common prefix without scanning the marker stream.

MMR is optional in V1; when disabled, the hash chain + binary search remains
the default divergence check.

#### 3.5.4.2 Symbol Record Logs (Append-Only, Locator-Friendly, Epoch-Typed)

Symbol record logs under `ecs/symbols/` are the persistence substrate for ECS
objects. Unlike the marker stream (fixed-size records), symbol logs store
variable-sized `SymbolRecord`s (because `T = symbol_size` is object-type-aware).

The format is optimized for:
- sequential append writes,
- sequential scans (for rebuild),
- random access via locator offsets (for decode).

**V1 constants (normative):**

```
SYMBOL_SEGMENT_HEADER_BYTES := 40
```

**Symbol segment file:**

`ecs/symbols/segment-XXXXXX.log` stores a sequence of symbol records. Each file
starts with a header, followed by a concatenation of `SymbolRecord` entries.

```
SymbolSegmentHeader := {
  magic      : [u8; 4],   -- "FSSY"
  version    : u32,       -- 1
  segment_id : u64,       -- monotonic identifier (matches filename)
  epoch_id   : u64,       -- ECS coordination epoch (RootManifest.ecs_epoch at segment creation)
  created_at : u64,       -- unix_ns (monotonic in lab via virtual time)
  header_xxh3: u64,       -- xxhash3 of all preceding header fields
}
```

**Epoch meaning (normative):**
`epoch_id` is not needed for RaptorQ decoding (OTI+ESI are sufficient). It
exists to make distributed operation and security policy explicit:
- Symbol auth key derivation is epoch-scoped (§4.18.2).
- Remote durability/quorum configuration is epoch-scoped (§4.18.3).
- Epoch transitions are explicit and quiescent (§4.18.4).

**Torn tail handling (normative):**
If a symbol segment ends with a partial `SymbolRecord` (incomplete bytes),
rebuild/recovery MUST ignore the torn tail.

**Locator offsets (normative):**

The locator indexes symbols by their location in a segment:

```
SymbolLogOffset := {
  segment_id   : u64,
  offset_bytes : u64,  -- byte offset AFTER SymbolSegmentHeader
}
```

`cache/object_locator.cache` stores (at minimum) `ObjectId -> Vec<SymbolLogOffset>`.
It is an accelerator only and MUST be rebuildable by scanning `ecs/symbols/`
and parsing symbol records.

#### 3.5.5 RootManifest: Bootstrap

The RootManifest is the bootstrap entry point, stored as a standard ECS object.
The `ecs/root` file points to it.

```
RootManifest := {
    magic           : [u8; 8],     -- "FSQLROOT"
    version         : u32,         -- manifest version
    database_name   : String,      -- human-readable name
    current_commit  : ObjectId,    -- ObjectId of the latest CommitMarker
    commit_seq      : u64,         -- latest commit sequence number
    schema_snapshot : ObjectId,    -- ObjectId of current schema ECS object
    schema_epoch    : u64,         -- monotonic schema epoch (bumps on DDL/VACUUM)
    ecs_epoch       : u64,         -- monotonic ECS coordination epoch (remote config + symbol auth key derivation)
    checkpoint_base : ObjectId,    -- ObjectId of last full checkpoint
    gc_horizon      : u64,         -- safe GC horizon commit sequence (min active begin_seq)
    created_at      : u64,         -- Unix timestamp
    updated_at      : u64,         -- Unix timestamp
    checksum        : u64,         -- xxhash3 of all preceding fields
}
```

**Bootstrap sequence:**
1. Read `ecs/root`. Verify checksum. Get `manifest_object_id`.
2. Fetch `RootManifest` object from symbol logs (using `object_locator.cache` or scan).
3. Decode `RootManifest`.
4. Fetch and verify the latest `CommitMarkerRecord`:
   - Locate record by `RootManifest.commit_seq` via §3.5.4.1.
   - Verify `marker_id == RootManifest.current_commit`.
   - (Optional, bounded): verify the marker hash chain back to the latest
     checkpoint tip (detects marker-stream corruption early without O(N) open).
5. Fetch `schema_snapshot` → reconstruct schema cache.
6. Fetch `checkpoint_base` → populate B-tree page cache for hot pages.
7. Database is open and ready for queries.

If `ecs/root` is corrupted (missing or invalid checksum), the database can be
recovered by scanning `ecs/markers/*.log` to find the latest valid CommitMarker,
or `ecs/symbols/*.log` to find the latest RootManifest symbol.

#### 3.5.6 Inter-Object Coding (Replication Optimization)

For replication, ECS objects can be coded across objects using inter-object
RaptorQ encoding. This allows a replica to reconstruct missing objects from
a subset of symbols spanning multiple objects:

```
Inter-object coding group:
    Objects O1, O2, ..., Ok share a coding group
    RaptorQ-encode the concatenation of their canonical encodings
    Transmit encoding symbols with group metadata

Receiver:
    Collect symbols from any subset of the group
    Decode to recover all objects in the group
```

This is particularly effective for replication catch-up: a lagging replica
can request "all commits since sequence N" as a single coded group, and
recover even if some symbols are lost in transit (UDP multicast).

#### 3.5.7 RaptorQ Permeation Map (Every Pore, Every Layer)

This is the "no excuses" mapping from subsystem to ECS/RaptorQ role. If a
subsystem persists or ships bytes, it MUST declare its ECS object type, symbol
policy (K/R), and repair story.

**Durability plane (disk):**

| Subsystem | ECS Object Type | Symbol Policy | Repair Story |
|-----------|----------------|---------------|--------------|
| Commits | `CommitCapsule` + `CommitProof` (coded) + `CommitMarkerRecord` (marker stream) | Capsule/Proof: T = `min(page_size, 4096)`, R = 20% default; Marker: 88B fixed records | Capsule/Proof: decode from surviving symbols; Marker: torn-tail ignore + `record_xxh3` + hash-chain audit |
| Checkpoints | `CheckpointChunk` | T = 1024–4096B, R = policy-driven | Chunked snapshot objects; rebuild from marker stream if lost |
| Indices | `IndexSegment` (Page, Object, Manifest) | T = 1280–4096B, R = 20% default | Decode or rebuild-from-marker-scan |
| Page storage | `PageHistory` | T = page_size, R = per-group | Decode from group symbols; on-the-fly repair on read |

**Concurrency plane (memory):**

| Subsystem | ECS Role | Notes |
|-----------|----------|-------|
| MVCC page history | `PageHistory` objects (patch chains) | Bounded by GC horizon; compressed via intent log + structured patches |
| Conflict reduction | Intent logs as small ECS objects | Replayed deterministically for rebase merge |
| SSI witness plane | `ReadWitness` / `WriteWitness` / `WitnessIndexSegment` / `DependencyEdge` / `CommitProof` | The serialization graph is itself a fountain-coded stream (see §5.6.4 and §5.7) |

**Replication plane (network):**

| Subsystem | Transport Primitive | Notes |
|-----------|-------------------|-------|
| Symbol streaming | `SymbolSink`/`SymbolStream` | Symbol-native, not file-native |
| Anti-entropy | ObjectId set reconciliation (IBLT) | O(Δ) reconciliation of ObjectId sets; fallback to segment hash scan |
| Bootstrap | `CheckpointChunk` symbol streaming | Late-join = collect K symbols |
| Multipath | `MultipathAggregator` | Any K symbols from any path suffice |

**Anti-entropy via IBLT (recommended):**

Naive set reconciliation ("send me your ObjectIds") is O(|A|) bandwidth and
often dominates catch-up cost. Use an **Invertible Bloom Lookup Table (IBLT)**
to reconcile the symmetric difference in O(Δ) where Δ = |A Δ B|.

Protocol sketch:

1. Replica A builds an IBLT over its ObjectId set in the reconciliation scope
   (e.g., "all objects reachable since checkpoint C", or "all objects in marker
   segments [S..tip]").
2. A sends the IBLT to replica B.
3. B subtracts its own ObjectIds from the received IBLT and attempts to peel
   (decode) the remaining cells.
4. On success, B obtains the missing ObjectIds and requests any needed symbols.
5. If peeling fails (Δ larger than configured capacity), B requests a larger
   IBLT (or falls back to a segment-hash scan).

This is correctness-preserving: failure to peel is not silent; it simply
degrades to a slower fallback.

**Observability plane (alien-artifact explainability):**

| Subsystem | Mechanism | Notes |
|-----------|-----------|-------|
| Repair auditing | `DecodeProof` artifacts | Attached to lab traces when repair occurs |
| Schedule exploration | `LabRuntime` deterministic trace | Reproducible concurrency bugs from a single seed |
| Invariant monitoring | e-process monitors | MVCC invariants, memory bounds, replication divergence |
| Model checking | `TLA+ export` of traces | Bounded model checking of commit/replication/recovery |

**Wild but aligned experiments (encouraged, feature-gated):**
- **Symbol-level RAID on a single machine:** Distribute symbols across multiple local devices/paths; any `K` reconstructs. RAID-like redundancy without strict striping constraints.
- **Integrity sweeps as information theory:** Periodically sample symbols and attempt partial decodes; use e-process monitors to detect elevated corruption rates early (before data loss becomes possible).

**Rule:** If a new feature persists bytes or ships bytes, it MUST declare its
ECS object type, symbol policy, and repair story before implementation begins.

#### 3.5.8 Decode Proofs (Auditable Repair)

Asupersync includes a `DecodeProof` facility
(`asupersync::raptorq::proof`). We exploit this in two critical ways:

- In **lab runtime**: every decode that repairs corruption MUST produce a
  proof artifact attached to the test trace. This makes repair operations
  auditable and reproducible.
- In **replication**: a replica MAY demand proof artifacts for suspicious
  objects (e.g., repeated decode failures), enabling explainable "why did we
  reject this commit?" answers.

`DecodeProof` records:
- The set of symbol ESIs received.
- Which symbols were repair vs source.
- The intermediate decoder state at success/failure.
- Timing metadata under `LabRuntime` (deterministic virtual time).

This is the "alien artifact" stance on repair: we do not merely fix things;
we produce a mathematical witness that the fix is correct.

#### 3.5.9 Deterministic Encoding (Seed Derivation from ObjectId)

If `ObjectId` is content-derived, symbol generation MUST be deterministic:
- The set of source symbols is deterministic by definition (payload chunking).
- Repair symbol generation MUST be deterministic for a given ObjectId and
  config.

**Practical rule:**
- Derive any internal "repair schedule seed" from `ObjectId`:
  `seed = xxh3_64(object_id_bytes)`.
- Wire it through `RaptorQConfig` or sender construction as needed.

This makes "the object" a platonic mathematical entity: any replica can
regenerate missing repair symbols (within policy) without coordination.

#### 3.5.10 Symbol Size Policy (Object-Type-Aware, Measured)

Symbol size is a major performance lever:
- Too small: too many symbols, higher metadata overhead, more routing work.
- Too large: worse cache behavior, higher per-symbol loss impact, more wasted
  decode work.

We choose symbol size per object type, with sane defaults and benchmark-driven
tuning:

| Object Type | Default Symbol Size | Rationale |
|------------|-------------------|-----------|
| `CommitCapsule` | `min(page_size, 4096)` | Aligns encoding with page boundaries; `u16`-bounded |
| `IndexSegment` | 1280–4096 bytes | Metadata-heavy; smaller symbols reduce tail loss impact |
| `CheckpointChunk` | 1024–4096 bytes | MTU-aware (prefer <=1366 on UDP); large objects use larger K/more blocks rather than huge T |
| `PageHistory` | page_size (4096) | Natural alignment with page boundaries |

All sizing is versioned in `RootManifest` so replicas decode correctly.
Benchmarks MUST drive tuning decisions; these defaults are starting points.

#### 3.5.11 Tiered Storage ("Bottomless", Native Mode)

Native mode's ECS design naturally produces an immutable history of
content-addressed objects (CommitCapsules, index segments, witness evidence).
Tiered storage makes this history effectively "bottomless" by offloading cold
objects to remote storage while preserving correctness and predictability.

**Tiers (normative):**

1. **L1 (hot):** in-memory caches (ARC for decoded objects + hot pages).
2. **L2 (warm):** local append-only symbol logs under `ecs/symbols/` and
   `ecs/markers/` (default source of truth on a single machine).
3. **L3 (cold):** remote object storage (S3/R2/Blob) keyed by `ObjectId` (and
   optionally by `(ObjectId, ESI)` for symbol-addressable fetch).

**Remote durability modes:**

- `PRAGMA durability = local`: L2 is sufficient for the durability contract.
  L3 is optional (purely archival / time-travel enabling).
- `PRAGMA durability = quorum(M)`: L3 (or replica peers) participate in the
  durability contract; commit is not successful until the configured quorum
  acknowledges enough symbols to make decode succeed.

**Remote tier integration (asupersync RemoteCap + idempotency, normative):**

- L3 fetch/upload MUST require `RemoteCap` in `Cx` (§4.19.1). Without RemoteCap,
  any attempt to fetch from L3 MUST fail with an explicit error and MUST NOT
  perform network I/O.
- Remote operations MUST be expressed as named computations (`ComputationName`,
  no closure shipping; §4.19.2) so the set of remotely-executable behaviors is
  explicit and auditable.
- Remote fetch/upload MUST be idempotent under retries (§4.19.4). Requests MUST
  carry an IdempotencyKey derived from request bytes and MUST include `ecs_epoch`
  (§4.18.3) to prevent mixed-epoch ambiguity.
- Multi-step workflows (segment eviction, compaction publish) MUST use the Saga
  discipline (§4.19.5): either the saga completes, or its compensations leave
  the system in a state equivalent to "the saga never happened."

**Eviction policy (normative):**

- Local symbol logs are immutable once rotated. Eviction operates at the
  granularity of rotated log segments, not individual objects.
- A local segment MAY be evicted from L2 only if:
  1. Every object referenced by any `CommitMarker` that is still reachable under
     the configured retention/time-travel policy is retrievable from L3 (or
     other replicas) with enough symbols to satisfy decode, and
  2. The segment is not needed for any in-flight read/repair operation (tracked
     via asupersync-style obligations / leases).
- Eviction MUST be cancel-safe: if cancellation occurs during segment upload or
  bookkeeping, the system MUST either (a) keep the segment locally, or (b) prove
  the segment is fully retrievable remotely before deleting local bytes.

**Fetch-on-demand read path:**

When decoding an object and L2 does not contain enough valid symbols:

1. Attempt local systematic fast path (§3.5.2) if systematic run placement was
   successful.
2. Otherwise request missing symbols from L3 (or peers) under a `Cx` budget:
   - Fetch source symbols first (`esi = 0..K_source-1`), then repairs as needed.
   - Prefer range reads that return contiguous systematic runs when the remote
     store supports it (reduces request count and tail latency).
3. Decode and (in lab/debug) emit `DecodeProof` (§3.5.8).
4. Populate L1 and (optionally) write back repaired symbols into L2 as a
   self-healing cache fill.

**Retention interaction:**

Tiered storage is orthogonal to GC horizons:
- MVCC/witness GC horizons (§5.6.4.8) control what must be kept for correctness
  of *current* operations.
- Retention policy controls how much historical state is kept for time travel,
  audit, and forensic replay (Section 12.17). Default policy in V1 is:
  retain full commit history, with cold history eligible for L3-only residence.

#### 3.5.12 Adaptive Redundancy (Anytime-Valid Durability Autopilot)

Static redundancy assumptions are a correctness risk: media, firmware, filesystems,
and networks do not keep stable loss/corruption rates over time. FrankenSQLite
therefore treats RaptorQ redundancy as a **control loop** with formal guarantees:
we monitor symbol health with anytime-valid tests, and we raise redundancy when
evidence indicates the durability budget is being violated.

**Key enabling fact (RaptorQ + ECS):** Repair symbol generation is deterministic
for a given `(ObjectId, config)` (§3.5.3, §3.5.9). Therefore redundancy is
**appendable**: we can publish additional repair symbols for an existing object
later without changing its ObjectId or rewriting the object bytes. This is a
uniquely powerful "self-hardening" lever compared to traditional WAL designs.

##### 3.5.12.1 Durability Budgets (Per Object Type, Normative Defaults)

For each ECS object class, the engine defines:
- `p_symbol_budget`: maximum acceptable symbol corruption probability per record.
- `epsilon_loss_budget`: maximum acceptable probability that an object becomes
  undecodable given the symbol budget and redundancy policy.
- `slack_symbols`: additive decode slack per source block (V1 default `+2`).

Markers and commit proofs are special:
- `CommitMarker` and `CommitProof` MUST use conservative budgets (smaller objects
  are dominated by rounding + additive slack; the policy MUST clamp to avoid
  under-provisioning; §3.5.3).

##### 3.5.12.2 Anytime-Valid Monitoring (e-Process, Optional Stopping Safe)

Every time we validate or decode a symbol record, we obtain a Bernoulli
observation:

- `X = 1` if record failed integrity (`frame_xxh3` mismatch, auth failure, etc.)
- `X = 0` otherwise

We maintain an e-process monitor of the null hypothesis `H0: p <= p0`
where `p0 = p_symbol_budget`. If the e-value exceeds `1/alpha`, the monitor
rejects with a provable false-alarm bound (Ville's inequality).

```rust
// Symbol corruption monitor (anytime-valid).
let sym_corruption = EProcess::new("INV-SYMBOL-CORRUPTION: p <= p0",
    EProcessConfig {
        p0: 1e-6,        // budget for symbol corruption probability
        lambda: 0.5,     // moderate bet; tune by calibration
        alpha: 1e-6,     // extremely low false alarm (durability is sacred)
        max_evalue: 1e18,
    });

// Each verified record yields one observation.
sym_corruption.observe(record_is_corrupt as u8);
```

**Rule:** Monitoring MUST be separated from the hot path: observations are
batched and recorded as part of decode/verification bookkeeping, not as an
unbounded per-record logging stream.

###### 3.5.12.2.1 Living Corruption-Rate Estimates (Bayes + Anytime-Valid Bounds)

The e-process provides an anytime-valid *budget test* ("is p <= p0 still
plausible?"). Separately, the system SHOULD maintain a living estimate of `p`
for explainability and for decision-theoretic policy tuning (§4.17).

**Bayesian posterior (recommended, explainability):**

For each object class and storage tier, maintain bounded counters:
- `n_ok`: count of verified-clean symbol records (`X=0`)
- `n_bad`: count of corrupt symbol records (`X=1`)

Assume a Beta prior `p ~ Beta(α0, β0)` (conjugate to Bernoulli). The posterior is:

```
p | data ~ Beta(α0 + n_bad, β0 + n_ok)
E[p | data] = (α0 + n_bad) / (α0 + β0 + n_ok + n_bad)
```

This posterior MUST be surfaced for diagnostics (PRAGMA / evidence ledger) as:
- posterior mean `E[p|data]`
- an upper credible bound `p_cred_hi` (e.g., 99.9% quantile)

**Anytime-valid conservative bound (required for safety decisions):**

For safety-critical actions (reducing redundancy, relaxing repair budgets, or
reporting a "durability bound" as a guarantee), the engine MUST use an
anytime-valid conservative bound `p_upper` that remains correct under optional
stopping.

One valid construction is to derive `p_upper` by inverting an e-process into a
confidence sequence (martingale inversion).

**Important:** Bayesian credible bounds are not anytime-valid under optional
stopping; they MUST be treated as diagnostics only and MUST NOT be used as
formal guarantees for safety-critical policy decisions.

**Rule:** `PolicyController` MAY use the Bayesian posterior for expected-loss
ranking, but it MUST treat e-process budgets as hard guardrails (§4.17).

##### 3.5.12.3 Autopilot Policy (Raise Redundancy, Repair Hardening)

When `INV-SYMBOL-CORRUPTION` rejects (evidence that p exceeded the budget),
FrankenSQLite MUST enter a **durability hardening mode**:

1. **Raise redundancy for new objects:** increase `raptorq_overhead` for affected
   object classes (CommitCapsule / IndexSegment / PageHistory) up to a configured
   maximum. Default policy:
   `overhead := min(overhead_max, max(overhead_min, overhead * 2))`.
2. **Retroactive hardening (background):** For recently reachable objects (under
   retention), generate and persist additional deterministic repair symbols
   for each object up to the new redundancy policy. This is safe because it is
   union-only: adding symbols cannot invalidate prior decodes.
3. **Escalate integrity sweeps:** increase sweep frequency and widen sampling
   (more objects, more buckets) until the monitor stops accumulating evidence
   of excess corruption.
4. **Emit explainable evidence:** record an evidence ledger entry (§4.16.1)
   describing the rejection, the policy change, and the set of objects hardened.

**Graceful degradation (required):** If retroactive hardening cannot decode an
object (insufficient surviving symbols), the engine MUST surface a
"durability contract violated" diagnostic with decode proofs, and it MUST
halt any operation that would otherwise claim durable commit ordering for
unverifiable objects (markers are the atomic truth).

##### 3.5.12.4 Why This Is Alien-Artifact Quality

- **Formal safety guarantees:** false-alarm probability is bounded under optional stopping.
- **Explainability:** decisions carry evidence ledgers and (in lab) decode proofs.
- **Self-healing:** redundancy increases are append-only, deterministic, and auditable.
- **Graceful degradation:** the system does not pretend; it either repairs or emits proofs.

### 3.6 Native Indexing: RaptorQ-Coded Index Segments

Classic SQLite uses a separate WAL-index structure (shm) to avoid scanning the WAL. FrankenSQLite's Native Mode goes further: the index itself is a stream of self-healing ECS objects.

#### 3.6.1 What The Index Must Answer

Given `(pgno, snapshot)` we need:
1. The newest committed version `V` such that `V.commit_seq <= snapshot.high`.
2. A pointer to the bytes (or intent replay recipe) to materialize `V`.

#### 3.6.2 VersionPointer (The Atom of Lookup)

```
VersionPointer {
  commit_seq: u64,
  patch_object: ObjectId,     // ECS object containing the patch/intent
  patch_kind: PatchKind,      // FullImage | IntentLog | SparseXor
  base_hint: Option<ObjectId> // optional "base image" hint for fast materialization
}
```

The pointer is stable and replicable: it references content-addressed objects, not physical offsets.

#### 3.6.3 IndexSegment Types

We use multiple segment kinds, all ECS objects:

1.  **PageVersionIndexSegment**: Maps `Pgno -> VersionPointer` for a specific commit range. Includes bloom filters for fast "not present" checks.
2.  **ObjectLocatorSegment**: Maps `ObjectId -> Vec<SymbolLogOffset>`. An accelerator for finding symbols on disk. Rebuildable by scanning symbol logs.
3.  **ManifestSegment**: Maps `commit_seq` ranges to `IndexSegment` object IDs. Used for bootstrapping.

#### 3.6.4 Lookup Algorithm (Read Path)

To read page `P` under snapshot `S`:

1.  **Check Cache:** Consult ARC cache for a visible committed version.
2.  **Check Filter:** Consult Version Presence Filter (Bloom/Quotient). If "no versions", read base page.
3.  **Index Scan:** Scan `PageVersionIndexSegment`s backwards from `S.high` until a visible version is found.
4.  **Fetch & Materialize:**
    - Fetch the `patch_object` (repairing via RaptorQ if needed).
    - If it's a full image, return it.
    - If it's a patch/intent, apply it to the base page (recursively if needed).

#### 3.6.5 Segment Construction (Background, Deterministic)

The **Segment Builder** consumes the commit marker stream:
- Accumulates `Pgno -> VersionPointer` updates in memory.
- Periodically flushes a new `PageVersionIndexSegment` object covering `[start_seq, end_seq]`.
- Construction is **deterministic**: stable map iteration order, stable encoding. This ensures all replicas build identical index segments.

#### 3.6.6 Repair and Rebuild

Because IndexSegments are ECS objects:
- **Repair:** Missing/corrupt segments are repaired by decoding from surviving symbols (local or remote).
- **Rebuild:** If a segment is irretrievably lost, it is rebuilt by re-scanning the commit marker stream and capsules.
- **Diagnostics:** "Index unrebuildable but commit markers exist" is a critical integrity failure.

#### 3.6.7 Boldness Constraint

Coded index segments ship in V1. They are not a "Phase 9 nice-to-have." The
index is part of the fundamental ECS thesis: if durability, storage, and
transport are all object-based and symbol-native, then the index MUST be too.
Fallbacks (e.g., linear marker-stream scan for lookup) exist only as emergency
escape hatches, activated only after conformance/performance data proves a need.

---

## 4. Asupersync Deep Integration

Asupersync is not just "a blocking pool and some channels." It is a
formally-specified async runtime with capabilities that map precisely to
FrankenSQLite's needs:

### 4.1 Cx (Capability Context) -- Everywhere

Every FrankenSQLite operation accepts `&Cx`. This enables:

- **Cooperative cancellation**: Long-running queries check `cx.is_cancel_requested()`
  and MUST call `cx.checkpoint()` at explicit yield points (e.g., VDBE instruction
  boundaries, symbol decode loops, long scans). `checkpoint()` is the canonical
  cancellation observation point in asupersync and also records progress for
  "stalled task" detection. FrankenSQLite maps `ErrorKind::Cancelled` to the most
  precise SQLite error code for the context (default: `SQLITE_INTERRUPT`).
- **Deadline propagation (budgets)**: time budgets are expressed as `Budget` deadlines
  and enforced via region/scope budgets. Budgets are a **product lattice with
  mixed meet/join** (deadline + poll quota + cost quota + priority): resource
  constraints (deadline/poll/cost) tighten by `min` (meet), while priority
  propagates by `max` (join — higher priority is more urgent). When tightening a
  budget, callers MUST compute `effective = cx.budget().meet(child)` and then use
  `cx.scope_with_budget(effective)` so child scopes cannot loosen parent budgets.
  Cancellation cleanup MUST use a bounded cleanup budget (`Budget::MINIMAL` or a
  stricter budget derived from it).
- **Compile-time capability narrowing**: Functions that should not perform I/O
  accept a narrowed `&Cx<CapsWithoutIo>`. Pure layers (parser/planner) accept
  capability sets without `IO`, `REMOTE`, and typically without `SPAWN`. Narrowing
  is zero-cost via `cx.restrict::<NewCaps>()` and is monotone (`SubsetOf`), so the
  type system prevents capability escalation.

#### 4.1.1 Ambient Authority Prohibition (Audit Gate)

Deterministic testing, capability security, and cancel-correctness all collapse
if code can silently reach around `Cx` (ambient authority). Therefore:

**Rule (INV-NO-AMBIENT-AUTHORITY):** FrankenSQLite crates MUST NOT call ambient
side-effect APIs directly. In particular, database crates MUST NOT call:
- `std::time::SystemTime::now()` / `Instant::now()` (use Cx time/budget clocks),
- ambient RNG (`rand::thread_rng()` / `getrandom`) (use Cx randomness),
- direct filesystem/network APIs (`std::fs`, `std::net`) (use VFS + RemoteCap),
- spawning (`std::thread::spawn`, tokio, etc.) (use asupersync regions/scopes).

**Enforcement (required):**
- Time/randomness/I/O MUST flow through `Cx` + VFS/Remote traits.
- The workspace SHOULD use a compile-time audit gate (asupersync
  `audit::ambient` pattern: define "pristine modules" and deny disallowed
  symbols) so violations are caught in CI, not in production.

**Integration pattern:**
```rust
fn execute_query(cx: &Cx, stmt: &PreparedStatement) -> Result<Rows> {
    for (pc, opcode) in stmt.program.iter().enumerate() {
        cx.checkpoint_with(format!("vdbe pc={pc} opcode={opcode:?}"))?;
        dispatch_opcode(cx, opcode)?;
    }
}
```

**Capability narrowing through the call stack:**

Asupersync's `Cx` type carries a phantom type parameter `Caps` that encodes
which capabilities are available. The capability set is a fixed-width vector
of booleans `[SPAWN, TIME, RANDOM, IO, REMOTE]` represented via const generics
as `CapSet<SPAWN, TIME, RANDOM, IO, REMOTE>`. The subset relation is the
pointwise `<=` ordering: `false <= false`, `false <= true`, `true <= true`.
Narrowing (dropping capabilities) always succeeds; widening (gaining
capabilities) is a compile-time error because the missing impl
`(Bit<true>, Bit<false>)` prevents it.

This means FrankenSQLite can express precise contracts at every layer boundary:

```rust
use asupersync::cx::{Cx, cap};

// Type aliases for FrankenSQLite-specific capability profiles
type FullCaps = cap::All;                                   // Connection level: everything
type StorageCaps = cap::CapSet<false, true, false, true, false>;  // VFS: time + I/O, no spawn/remote
type ComputeCaps = cap::None;                               // Parser/planner: pure computation

/// Connection::execute_query has full capabilities.
/// It is the outermost entry point from the public API.
pub fn execute_query(cx: &Cx<FullCaps>, sql: &str) -> Result<Rows> {
    let compute_cx = cx.restrict::<ComputeCaps>();
    let ast = parse_sql(&compute_cx, sql)?;          // restrict to ComputeCaps
    let plan = plan_query(&compute_cx, &ast)?;       // restrict to ComputeCaps
    let program = codegen(&compute_cx, &plan)?;      // restrict to ComputeCaps
    execute_program(cx, &program)                     // full caps: needs I/O for page reads
}

/// The parser accepts only ComputeCaps. It cannot perform I/O.
/// This is a compile-time guarantee, not a runtime check.
fn parse_sql(cx: &Cx<ComputeCaps>, sql: &str) -> Result<Ast> {
    cx.checkpoint()?;  // cancellation is always available
    // cx.blocking_io(...)  -- COMPILE ERROR: ComputeCaps lacks IO
    let lexer = Lexer::new(sql);
    Parser::parse(cx, lexer)
}

/// The VFS layer accepts StorageCaps: it can do I/O and timers
/// but cannot spawn tasks or make remote calls.
fn read_page(cx: &Cx<StorageCaps>, file: &mut impl VfsFile, pgno: PageNumber) -> Result<PageData> {
    cx.checkpoint()?;
    let offset = u64::from(pgno.get() - 1) * u64::from(page_size);
    let mut buf = vec![0u8; page_size as usize];
    // NOTE: `VfsFile` is synchronous (SQLite-compatible). Callers running on
    // asupersync worker threads MUST offload the actual read to the blocking pool.
    file.read(&mut buf, offset)?;
    Ok(PageData::from(buf))
}
```

**Cx flows through the full call stack:**

```
Connection::execute(cx: &Cx<All>).await
  -> VDBE::run(cx: &Cx<All>)
    -> BtreeCursor::move_to(cx: &Cx<StorageCaps>)
      -> MvccPager::get_page(cx: &Cx<StorageCaps>)
        -> ArcCache::fetch(cx: &Cx<StorageCaps>)
          -> VfsFile::read(buf, offset)     // synchronous SQLite-compatible VFS method
            -> asupersync::runtime::spawn_blocking_io(|| { pread64(...) })
```

At each level, capabilities can only be narrowed, never widened. The VDBE
has full capabilities (it orchestrates I/O for page reads). When it calls
down to the pager, it narrows to `StorageCaps`. When the parser is invoked
(a pure computation), it narrows to `ComputeCaps`. This means a bug in the
parser that accidentally tries to do I/O is caught at compile time, not at
runtime.

### 4.2 Lab Runtime + Lab Reactor -- Deterministic Testing

Asupersync provides **deterministic testing primitives** that FrankenSQLite uses
as the foundation for concurrency verification:

- `asupersync::lab::LabRuntime`: deterministic scheduling, virtual time, oracle suite,
  trace certificates, replay capture, and (optional) chaos injection.
- `asupersync::runtime::reactor::LabReactor`: a **virtual readiness reactor**
  (tokens + injected events) for deterministic testing of async I/O readiness.

**Critical clarification (merge-canon rule):** these lab primitives do **not**
magically virtualize filesystem syscalls. Determinism is about *task scheduling,
virtual time, cancellation injection, and trace equivalence classes*. Disk fault
injection is provided by the FrankenSQLite harness via an explicit VFS wrapper
(described below).

**Why this matters for MVCC testing:**
- Run 100 concurrent transactions with deterministic interleaving.
- Reproduce any race condition by replaying the same seed + schedule certificate.
- Systematically explore interleavings via DPOR-style explorers (Section 4.4, Section 17.4).
- Inject cancellation at every await point to prove cancel-safety (below).
- Inject *storage* faults via a deterministic `FaultInjectingVfs` wrapper (below).

#### 4.2.1 The Real LabRuntime Skeleton (Actual Asupersync API)

```rust
use asupersync::lab::{LabConfig, LabRuntime};
use asupersync::types::Budget;

let mut runtime = LabRuntime::new(LabConfig::new(0xDEAD_BEEF).worker_count(4).max_steps(100_000));
let region = runtime.state.create_root_region(Budget::INFINITE);

let (t1_id, _t1) = runtime.state.create_task(region, Budget::INFINITE, async move {
    // Inside tasks, `Cx::current()` is set by the runtime (capabilities, cancellation, budgets).
    // let cx = asupersync::cx::Cx::current().expect("cx");
    // ... run test logic ...
    1_u64
}).expect("create task");

runtime.scheduler.lock().unwrap().schedule(t1_id, 0);

let report = runtime.run_until_quiescent_with_report();
assert!(report.oracle_report.all_passed(), "oracle failures:\n{}", report.oracle_report);
assert!(report.invariant_violations.is_empty(), "lab invariants: {:?}", report.invariant_violations);
```

#### 4.2.2 Systematic Cancellation Injection (Actual Asupersync API)

Cancellation can strike at any `.await`. FrankenSQLite MUST be cancel-correct:
no leaked locks, no leaked obligations, no half-commits.

```rust
use asupersync::lab::{lab, InjectionStrategy, InstrumentedFuture};

#[test]
fn mvcc_commit_is_cancel_safe() {
    let report = lab(42)
        .with_cancellation_injection(InjectionStrategy::AllPoints)
        .with_all_oracles()
        .run(|injector| InstrumentedFuture::new(async {
            // ... run a representative MVCC commit scenario ...
        }, injector));

    assert!(report.all_passed(), "Cancellation failures:\n{}", report);
}
```

#### 4.2.3 FrankenSQLite Harness: FsLab + FaultInjectingVfs (Adds What Asupersync Does Not)

To keep the spec examples readable *and* remain truthful to asupersync APIs,
FrankenSQLite defines harness utilities in `crates/fsqlite-harness/`:

- `fsqlite_harness::lab::FsLab`: a small wrapper around `LabRuntime` that provides
  ergonomic `run(|cx| async { ... })` and `spawn(name, |cx| async { ... })` helpers.
- `fsqlite_harness::vfs::FaultInjectingVfs`: deterministic disk fault injection
  for SQLite-style VFS calls (torn writes, partial writes, fsync loss, power-cut).

These wrappers are **FrankenSQLite functionality**, built on the asupersync lab runtime.

**Complete scenario (canonical): snapshot isolation under deterministic scheduling**

```rust
#[test]
fn snapshot_isolation_holds_under_specific_interleaving() {
    let mut lab = fsqlite_harness::lab::FsLab::new(0xDEAD_BEEF)
        .worker_count(4)
        .max_steps(100_000);

    let report = lab.run(|cx| async move {
        let db = Database::open_in_memory(cx).await.unwrap();
        db.execute(cx, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await.unwrap();
        db.execute(cx, "INSERT INTO t VALUES(1, 100)").await.unwrap();
        db.execute(cx, "INSERT INTO t VALUES(2, 200)").await.unwrap();

        let db1 = db.clone();
        let t1 = lab.spawn("reader", move |cx| async move {
            let txn = db1.begin_concurrent(cx).await.unwrap();
            let val1 = txn.query_one(cx, "SELECT val FROM t WHERE id=1").await.unwrap();
            assert_eq!(val1, 100);

            cx.checkpoint_with("yield to let writer commit")?;
            fsqlite_harness::yield_now().await; // harness-level deterministic yield helper

            let val1_again = txn.query_one(cx, "SELECT val FROM t WHERE id=1").await.unwrap();
            assert_eq!(val1_again, 100, "snapshot isolation violated!");
            txn.commit(cx).await.unwrap();
            Ok::<_, FrankenError>(())
        });

        let db2 = db.clone();
        let t2 = lab.spawn("writer", move |cx| async move {
            let txn = db2.begin_concurrent(cx).await.unwrap();
            txn.execute(cx, "UPDATE t SET val=999 WHERE id=1").await.unwrap();
            txn.commit(cx).await.unwrap();
            Ok::<_, FrankenError>(())
        });

        t1.await.unwrap();
        t2.await.unwrap();
        Ok::<_, FrankenError>(())
    });

    assert!(report.oracle_report.all_passed(), "oracle failures:\n{}", report.oracle_report);
    assert!(report.invariant_violations.is_empty(), "lab invariants: {:?}", report.invariant_violations);
}
```

**Canonical storage fault tests (FrankenSQLite harness VFS wrapper):**

```rust
#[test]
fn wal_survives_torn_write_at_frame_3() {
    let mut lab = fsqlite_harness::lab::FsLab::new(42).max_steps(50_000);
    let report = lab.run(|cx| async move {
        let vfs = fsqlite_harness::vfs::FaultInjectingVfs::new(UnixVfs::new());
        vfs.inject_fault(FaultSpec::torn_write("*.wal").at_offset_bytes(32 + 2 * (24 + 4096)).valid_bytes(17));

        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        // ... perform a 5-page transaction that writes 5 WAL frames ...
        drop(db); // crash

        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        db.execute(cx, "PRAGMA integrity_check").await.unwrap();
        Ok::<_, FrankenError>(())
    });

    assert!(report.oracle_report.all_passed(), "oracle failures:\n{}", report.oracle_report);
}

#[test]
fn power_loss_during_wal_commit_preserves_atomicity() {
    let mut lab = fsqlite_harness::lab::FsLab::new(7777).max_steps(50_000);
    let report = lab.run(|cx| async move {
        let vfs = fsqlite_harness::vfs::FaultInjectingVfs::new(UnixVfs::new());
        vfs.inject_fault(FaultSpec::power_cut("*.wal").after_nth_sync(1));

        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        db.execute(cx, "CREATE TABLE t(x INTEGER)").await.unwrap();
        db.execute(cx, "INSERT INTO t VALUES(1)").await.unwrap();
        let _ = db.execute(cx, "INSERT INTO t VALUES(2)").await; // interrupted

        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        let count: i64 = db.query_one(cx, "SELECT count(*) FROM t").await.unwrap();
        assert_eq!(count, 1, "uncommitted transaction must not be visible after crash");
        Ok::<_, FrankenError>(())
    });

    assert!(report.oracle_report.all_passed(), "oracle failures:\n{}", report.oracle_report);
}
```

### 4.3 E-Processes -- Anytime-Valid Invariant Monitoring

E-processes (based on Ville's inequality) provide statistically rigorous
runtime monitoring that can be checked at ANY point during execution, not just
at the end of a test.

**For MVCC, monitor these invariants as e-processes:**
- **INV-1 (Monotonicity)**: TxnId (begin ids) and CommitSeq (commit clock) are strictly increasing
- **INV-2 (Lock Exclusivity)**: No two active transactions hold the same page lock
- **INV-3 (Version Chain Order)**: Versions are ordered by descending CommitSeq
- **INV-4 (Write Set Consistency)**: Write set only contains locked pages
- **INV-5 (Snapshot Stability)**: A transaction's snapshot (`high` field) is immutable after capture
- **INV-6 (Commit Atomicity)**: Committed transaction's pages all become visible
- **INV-7 (Serialized Mode Exclusivity)**: At most one serialized writer active at any time

If an e-process detects a violation, it provides a **proof certificate** that
the invariant was violated, including the exact sequence of operations that
caused it. This is not a test that passes or fails -- it's a continuously
running formal monitor.

**Formal definition of an e-process:**

An **e-process** `(E_t)_{t >= 0}` is a sequence of random variables adapted
to a filtration `(F_t)` such that:

1. `E_0 = 1` (starts at one)
2. `E_t >= 0` for all `t` (non-negative)
3. `E[E_t | F_{t-1}] <= E_{t-1}` (supermartingale under the null hypothesis H_0)

The null hypothesis H_0 asserts that the invariant holds (violation probability
is at most `p_0`, typically 0.001). Each observation `X_t` is binary: 1 if a
violation is detected, 0 otherwise.

**Key property (Ville's inequality):** For any stopping time `tau` and
significance level `alpha`:

```
P_{H_0}(exists t : E_t >= 1/alpha) <= alpha
```

This means you can **peek at any time** and reject H_0 (conclude the invariant
is systematically violated) if `E_t >= 1/alpha`, without inflating the type-I
error rate. No correction for multiple testing over time is needed. This is
the fundamental advantage over classical hypothesis testing.

**The betting martingale update rule:**

```
E_t = E_{t-1} * (1 + lambda * (X_t - p_0))
```

where:
- `lambda` is the bet size, constrained to `(-1/(1-p_0), 1/p_0)` for non-negativity
- `X_t` is the observation (1 = violation, 0 = no violation)
- `p_0` is the null hypothesis violation rate (e.g., 0.001)

Under H_0, `E[X_t] = p_0`, so `E[E_t | E_{t-1}] = E_{t-1}` (martingale).
Under the alternative H_1 (actual violation rate `p_1 > p_0`), the e-process
grows exponentially at rate `KL(p_1 || p_0)` per observation, where KL is the
Kullback-Leibler divergence.

**Alien-artifact upgrade (recommended): Mixture e-processes (no hand-tuned λ)**

The fixed-λ betting martingale is valid but brittle: power depends strongly on
choosing λ well, and we generally do not know the true violation rate `p_1`
ahead of time.

Key fact: any **nonnegative mixture** of valid e-processes is itself a valid
e-process (by linearity of expectation). Therefore we can run a small grid of
λ strategies in parallel and sum them:

```
E_mix(t) := Σ_j w_j * E_{λ_j}(t),   w_j >= 0, Σ_j w_j = 1
```

where each `E_{λ_j}` updates as `E_t = E_{t-1} * (1 + λ_j (X_t - p0))`.

**Practical implementation (normative guidance):**
- Choose `λ_j` on a log grid spanning "sensitive to rare violations" → "sensitive
  to frequent violations" (e.g., 16–64 values).
- Maintain `log(E_{λ_j})` and compute the mixture in log-space (log-sum-exp) for
  numerical stability.
- Alarm when `E_mix(t) >= 1/alpha` (same Ville guarantee; optional stopping safe).

This gives near-oracle power across a wide range of `p_1` without per-invariant
hand-tuning, while preserving the same statistical guarantee under H0.

**Multiple invariants (family-wise error control):**

FrankenSQLite runs *many* monitors (INV-1..INV-7, INV-SSI-FP, symbol survival,
replication divergence, etc.). If each monitor independently alarms at level
`alpha_i`, naive use can inflate the global false-alarm rate.

E-values make global control simple and optional-stopping-safe:

- **Alpha budget (union bound, simplest):** choose per-monitor levels `alpha_i`
  such that `sum_i alpha_i <= alpha_total`. Each monitor rejects when
  `E_i(t) >= 1/alpha_i`. Then the probability that *any* monitor ever rejects
  under the global null is `<= alpha_total`.

- **E-value aggregation (adaptive, recommended):** choose weights `w_i >= 0`
  with `sum_i w_i = 1` and define the **arithmetic mean**:

  ```
  E_global(t) := Σ_i w_i * E_i(t)
  ```

  By linearity of conditional expectation, `E_global(t)` is a valid e-process
  (nonneg supermartingale with `E_global(0) = 1`) under the global null
  **regardless of dependence** between monitors — no independence assumption
  required (Vovk & Wang 2021, §4). This is critical because MVCC invariant
  monitors observe the same transactions and are therefore correlated.
  Alarm when `E_global(t) >= 1/alpha_total` (Ville's inequality; optional
  stopping safe). The resulting certificate includes the top contributing
  monitors by `w_i * E_i(t)` share (an "evidence ledger").

  *Note:* The weighted geometric mean `Π_i E_i(t)^{w_i}` would be tighter
  but requires conditional independence of the monitors, which does not hold
  here. The arithmetic mean is the standard dependence-robust aggregation.

This gives rigorous "peek-anytime" monitoring *across the whole system* rather
than per-invariant ad hoc thresholds.

**Concrete e-process definitions for MVCC invariants:**

```rust
use asupersync::lab::oracle::eprocess::{EProcess, EProcessConfig};

/// Create e-processes for all MVCC invariants.
///
/// CALIBRATION NOTE (Alien-Artifact Discipline):
/// Each invariant has qualitatively different violation characteristics.
/// Using identical (p0, lambda, alpha) for all is wrong:
///   - INV-1 (monotonicity) is enforced by AtomicU64 fetch_add. A violation
///     implies a hardware fault. p0 should be ~10^-15.
///   - INV-SSI-FP (false positive rate) has an EXPECTED baseline of ~0.5-5%.
///     p0 = 0.001 would trigger false alarms constantly.
///
/// Per-invariant power analysis: for a monitor with p0 and lambda, the
/// expected detection delay (observations to reject H0) when the true
/// violation rate is p1 is:
///   N_detect ≈ log(1/alpha) / KL(p1 || p0)
/// where KL is the Kullback-Leibler divergence.
fn create_mvcc_monitors() -> Vec<EProcess> {
    vec![
        // INV-1: Monotonicity. Enforced by hardware atomics; any violation is a
        // catastrophic bug. For this class of invariant, the correct response is
        // to fail-fast on the first observed violation (assert/panic), while the
        // e-process provides an auditable, anytime-valid *ledger* for long-running
        // fuzz/lab traces (optional stopping safe).
        EProcess::new("INV-1: TxnId/CommitSeq Monotonicity", EProcessConfig {
            p0: 1e-9, lambda: 0.999, alpha: 1e-6, max_evalue: 1e18,
        }),
        // INV-2: Lock Exclusivity. CAS-enforced; violation = logic bug.
        EProcess::new("INV-2: Lock Exclusivity", EProcessConfig {
            p0: 1e-9, lambda: 0.999, alpha: 1e-6, max_evalue: 1e18,
        }),
        // INV-3: Version Chain Order. Depends on correct insert ordering.
        // A bug here is subtle (wrong version served). Moderate sensitivity.
        EProcess::new("INV-3: Version Chain Order", EProcessConfig {
            p0: 1e-6, lambda: 0.9, alpha: 0.001, max_evalue: 1e15,
        }),
        // INV-4: Write Set Consistency. Lock-before-write invariant.
        EProcess::new("INV-4: Write Set Consistency", EProcessConfig {
            p0: 1e-6, lambda: 0.9, alpha: 0.001, max_evalue: 1e15,
        }),
        // INV-5: Snapshot Stability. Read-set immutability during txn.
        EProcess::new("INV-5: Snapshot Stability", EProcessConfig {
            p0: 1e-6, lambda: 0.9, alpha: 0.001, max_evalue: 1e15,
        }),
        // INV-6: Commit Atomicity. All-or-nothing page visibility.
        EProcess::new("INV-6: Commit Atomicity", EProcessConfig {
            p0: 1e-6, lambda: 0.9, alpha: 0.001, max_evalue: 1e15,
        }),
        // INV-7: Serialized Mode Exclusivity. Global mutex correctness.
        EProcess::new("INV-7: Serialized Mode Exclusivity", EProcessConfig {
            p0: 1e-9, lambda: 0.999, alpha: 1e-6, max_evalue: 1e18,
        }),
    ]
}
```

**Example: the Lock Exclusivity e-process (INV-2):**

The Lock Exclusivity invariant states: for any page P, at most one active
transaction holds a lock. We define the observation function:

```rust
/// Check INV-2 at the current instant.
/// Returns true (violation) if any page has two holders, false otherwise.
struct ActiveTxnInfo {
    state: TxnState,
    page_locks: Vec<PageNumber>,
}

fn observe_lock_exclusivity(
    lock_table: &PageLockTable,
    active_transactions: &HashMap<TxnId, ActiveTxnInfo>,
) -> bool {
    // The lock table maps PageNumber -> TxnId.
    // By construction, HashMap allows only one value per key.
    // But we additionally verify against the per-transaction lock sets:
    let mut page_holders: HashMap<PageNumber, Vec<TxnId>> = HashMap::new();
    for (txn_id, txn) in active_transactions {
        if txn.state == TxnState::Active {
            for &pgno in &txn.page_locks {
                page_holders.entry(pgno).or_default().push(*txn_id);
            }
        }
    }
    for (pgno, holders) in &page_holders {
        if holders.len() > 1 {
            return true; // VIOLATION
        }
    }
    // Cross-check internal consistency: every lock_table entry must be present
    // in the transaction's lock set (no "ghost" or leaked locks).
    for (&pgno, &holder) in lock_table.iter() {
        let Some(txn) = active_transactions.get(&holder) else {
            return true; // VIOLATION (lock held by unknown txn)
        };
        if txn.state != TxnState::Active || !txn.page_locks.contains(&pgno) {
            return true; // VIOLATION (lock_table and txn lock set disagree)
        }
    }
    false // no violation
}

// In the test loop, after each operation:
let violated = observe_lock_exclusivity(&lock_table, &active_transactions);
inv2_eprocess.observe(violated);
if inv2_eprocess.rejected {
    panic!(
        "INV-2 violated: e-value {} >= threshold {} after {} observations",
        inv2_eprocess.e_value(),
        inv2_eprocess.config.threshold(),
        inv2_eprocess.observations,
    );
}
```

After 1000 operations with no violations, `E_1000 ~ 1.0` (fluctuates around 1
due to the martingale property). If a bug causes even a single violation, the
e-value jumps by a factor of `(1 + lambda * (1 - p_0))`. For INV-2's actual
config (lambda=0.999, p_0=1e-9, alpha=1e-6), this is approximately `2.0`,
and the rejection threshold is `1/alpha = 1,000,000`. Each violation roughly
doubles the e-value; ~20 violations (log2(10^6) ≈ 20) are sufficient to
cross the threshold, even intermixed with millions of non-violations.
(Pedagogical shorthand: with lambda=0.5, p_0=0.001, alpha=0.05, the jump
would be ~1.5 with threshold 20 -- but those are not the actual INV-2 params.)

### 4.4 Mazurkiewicz Trace Monoid -- Systematic Interleaving

Standard concurrency testing relies on random interleaving, which may miss
rare but critical orderings. Asupersync's Mazurkiewicz trace implementation
systematically explores ALL distinct interleavings (up to commutativity of
independent operations).

**For MVCC:** Given a scenario with N transactions each performing M operations,
the trace monoid enumerates all non-equivalent orderings and verifies that:
- Snapshot isolation holds for every ordering
- First-committer-wins correctly identifies conflicts
- GC never reclaims a version needed by an active transaction

This provides exhaustive coverage that random testing cannot match.

**Formal definition:**

A **trace monoid** `M(Sigma, I)` is defined over:
- An **alphabet** `Sigma` of actions (e.g., `read_page(T1, P1)`, `write_page(T2, P3)`)
- A symmetric, irreflexive **independence relation** `I` on `Sigma x Sigma`

Two actions `a, b` are **independent** (written `(a, b) in I`) if swapping
their order does not change observable behavior. Two words (sequences of
actions) `w_1` and `w_2` are **trace-equivalent** (written `w_1 =_I w_2`)
if one can be transformed into the other by repeatedly swapping adjacent
independent actions.

The trace monoid is the quotient `M(Sigma, I) = Sigma* / =_I`.

**Independence relation for MVCC operations:**

| Action A | Action B | Independent? | Reason |
|----------|----------|-------------|--------|
| `read_page(T1, P1)` | `read_page(T2, P2)` | Yes, if P1 != P2 | Different pages, read-read |
| `read_page(T1, P1)` | `read_page(T2, P1)` | Yes | Read-read on same page (MVCC: each sees own snapshot) |
| `read_page(T1, P1)` | `write_page(T2, P1)` | **No** | T2's write might change what T1 sees (dependent) |
| `write_page(T1, P1)` | `write_page(T2, P2)` | Yes, if P1 != P2 | Different pages |
| `write_page(T1, P1)` | `write_page(T2, P1)` | **No** | Same page conflict |
| `commit(T1)` | `commit(T2)` | **No** | Serialized through write coordinator |
| `begin(T1)` | `begin(T2)` | **No** | Snapshot capture depends on ordering |
| `read_page(T1, P1)` | `commit(T2)` | **No** if P1 in T2.write_set | Commit publishes versions |

**How the trace monoid quotients out commutative reorderings:**

Given a concrete execution trace (a total order of events), the trace monoid
identifies which events could have been reordered without affecting the outcome.
Two traces that differ only in the order of independent events belong to the
same equivalence class. Asupersync computes the **Foata normal form** -- a
canonical representative where events are organized into layers of mutually
independent events, with a deterministic sort within each layer.

**Concrete example: 2 transactions, 3 operations each:**

Setup: T1 reads P1, writes P2, commits. T2 reads P3, writes P4, commits.
Pages are all distinct, so T1 and T2's reads/writes are independent of each
other (only commits are dependent due to coordinator serialization).

```
Alphabet Sigma = {
    a1 = read(T1, P1),   a2 = write(T1, P2),   a3 = commit(T1),
    b1 = read(T2, P3),   b2 = write(T2, P4),   b3 = commit(T2)
}

Independence relation I (symmetric pairs):
    (a1, b1), (a1, b2),          -- T1's read of P1 independent of T2's ops on P3,P4
    (a2, b1), (a2, b2),          -- T1's write of P2 independent of T2's ops on P3,P4
    (b1, a1), (b1, a2),          -- (symmetric)
    (b2, a1), (b2, a2),          -- (symmetric)

Dependent pairs (NOT in I):
    (a3, b3)                     -- commits are serialized
    (a1, a2), (a2, a3), (a1, a3) -- same-transaction ordering preserved
    (b1, b2), (b2, b3), (b1, b3) -- same-transaction ordering preserved
```

The distinct traces (equivalence classes) are determined by the relative
ordering of the two commit operations and the per-transaction operation order:

```
Trace class 1 (T1 commits first):
  Foata normal form: [a1, b1] [a2, b2] [a3] [b3]
  Layer 0: {a1, b1} -- both reads, mutually independent
  Layer 1: {a2, b2} -- both writes, mutually independent
  Layer 2: {a3}     -- T1 commits
  Layer 3: {b3}     -- T2 commits (depends on a3 via coordinator)

Trace class 2 (T2 commits first):
  Foata normal form: [a1, b1] [a2, b2] [b3] [a3]
  Layer 0-1: same as above
  Layer 2: {b3}     -- T2 commits first
  Layer 3: {a3}     -- T1 commits second
```

Only 2 distinct equivalence classes, despite 6! / constraints = many possible
linearizations. The explorer verifies MVCC invariants hold for both classes
rather than testing hundreds of redundant interleavings.

### 4.5 Two-Phase MPSC Channels -- Write Coordinator

The write coordinator uses asupersync's cancel-safe two-phase MPSC channel:

```
Phase 1 (Reserve): Writer reserves a slot in the commit pipeline
  - If cancelled before commit: slot automatically released (cancel-safe)
Phase 2 (Commit): Writer submits its write set for validation + WAL append
  - Coordinator validates, appends to WAL, responds via oneshot

Benefits over a simple Mutex:
  - Backpressure: pipeline capacity limits in-flight commits
  - Cancel-safety: if a transaction is interrupted mid-commit, no state leak
  - Ordering: commits are processed FIFO, providing fairness
```

**The two-phase API in detail:**

```rust
use asupersync::channel::mpsc;
use asupersync::cx::Cx;

// Create a bounded channel with capacity 16 (max in-flight commits)
let (tx, rx) = mpsc::channel::<CommitRequest>(16);

// Writer side (one per writing transaction):
async fn submit_commit(cx: &Cx, tx: &mpsc::Sender<CommitRequest>, req: CommitRequest) -> Result<()> {
    // Phase 1: Reserve a slot. This awaits if the channel is full (backpressure).
    // If the task is cancelled while waiting, the permit is never created -- no leak.
    let permit: mpsc::SendPermit<CommitRequest> = tx.reserve(cx).await?;

    // Between reserve() and send(), the slot is held but no data occupies it.
    // If we are cancelled here (e.g., client disconnects), dropping the permit
    // automatically releases the slot. This is the cancel-safety guarantee.

    // Phase 2: Commit the data into the reserved slot. This is synchronous
    // and cannot fail (the slot is already reserved).
    permit.send(req);
    // Alternatively: permit.abort() to explicitly release without sending.

    Ok(())
}
```

**Tracked variant (recommended for safety-critical channels):**
In lab mode (and optionally in production for the commit pipeline), FrankenSQLite
SHOULD wrap critical senders with asupersync's obligation-tracked session layer
(`asupersync::channel::session::TrackedSender`). Dropping a reserved permit
without `send()` or `abort()` is then structurally detected (fail-fast in lab;
diagnostic escalation in production; §4.13.1).

**Cancel-safety: why this matters for database commits:**

Consider the sequence of operations during a `COMMIT`:

1. B-tree modifications are complete (pages modified in write set)
2. CommitRequest is sent to the write coordinator
3. Coordinator validates the write set
4. Coordinator appends frames to WAL
5. Coordinator responds via oneshot channel
6. Transaction marks pages as committed in version store

If the task is cancelled between steps 1 and 2, the traditional approach
(simple `tx.send(req).await`) has a race: the message might be half-sent,
or the send future might be dropped while the message is being moved into
the channel buffer. With two-phase MPSC:

- Cancel between `reserve()` and `send()`: the `SendPermit` is dropped,
  which automatically releases the reserved slot. No orphaned state.
- Cancel during `reserve()` awaiting backpressure: the waiter is removed
  from the wait queue. No slot was ever reserved.

This means a cancelled transaction never leaves ghost entries in the commit
pipeline, never consumes a slot without producing a message, and never
causes the coordinator to hang waiting for a message that will never arrive.

**Backpressure: bounded channel capacity limits in-flight commits:**

The channel capacity (default: 16) limits the number of transactions that can
be simultaneously in the commit pipeline.

**Derivation (Little's Law):** The channel capacity C must satisfy
`C >= lambda * t_commit` where `lambda` is the peak commit arrival rate and
`t_commit` is the mean commit processing time (validate + WAL append + fsync
amortization). For the throughput model in Section 17.2:
- Group commit with batch size N=50, fsync cost 2ms:
  `t_commit ≈ 2ms / 50 = 40us` per transaction (amortized).
- At peak 37,000 commits/sec: `C >= 37000 * 40e-6 ≈ 1.5`.
- At burst 4x peak (148K/sec): `C >= 148000 * 40e-6 ≈ 6`.
- With safety margin 2.5x for jitter: `C = 6 * 2.5 = 15 ≈ 16`.

The default of 16 is therefore well-calibrated: it absorbs bursts at 4x
sustained peak without stalling senders, while bounding memory to 16 write
sets. Adjustable via `PRAGMA fsqlite.commit_channel_capacity`.

This provides:
- **Memory boundedness**: At most C write sets are buffered, bounding the
  coordinator's memory usage regardless of the number of concurrent writers.
- **Latency signal**: When the channel is full, new committers block on
  `reserve()`, signaling commit pipeline saturation. This naturally
  throttles new write transactions.
- **Fair queuing**: FIFO ordering of reserve waiters ensures long-waiting
  transactions are served first, preventing starvation.
- **Optimal batch size:** The group commit batch size N interacts with C:
  the coordinator drains `min(C, available)` commits per fsync. The optimal
  N minimizes `t_fsync / N + t_validate * N` (fsync amortization vs.
  validation latency). For `t_fsync = 2ms, t_validate = 5us`:
  `N_opt = sqrt(t_fsync / t_validate) = sqrt(400) = 20`. The capacity of 16
  is below this optimum, so the system naturally batches up to 16 per fsync
  under saturation, which is near-optimal.

**Alien-artifact upgrade (recommended): Conformal control for batch size**

The derivation above uses point estimates. In reality, `t_fsync` and
`t_validate` are random variables with heavy tails and regime shifts
(queue depth, background compaction, device health).

The coordinator SHOULD therefore choose `N` using conservative, distribution-free
upper quantiles *within the current BOCPD regime* (§4.8):

1. Maintain bounded calibration windows (ring buffers) of recent measurements:
   - `fsync_samples = {t_fsync_i}` from completed batches
   - `validate_samples = {t_validate_i}` from per-commit validation
2. Compute conformal upper quantiles (split conformal; §4.7):
   - `q_fsync := Q_{1-α}(fsync_samples)`
   - `q_validate := Q_{1-α}(validate_samples)`
3. Choose:

```
N_conformal = clamp(round(sqrt(q_fsync / q_validate)), 1, C)
```

**Operational rules (normative):**
- On a BOCPD regime shift for fsync/validate streams, the calibration windows
  MUST reset (new regime, new quantiles).
- `N` MUST change with hysteresis (e.g., require a 2-step improvement) to avoid
  thrash; policy changes MUST be logged in the evidence ledger (§4.16.1).
- Under `LabRuntime`, the decision MUST be deterministic for a fixed
  (seed, trace): calibration uses the lab clock (not wall clock).

### 4.6 Sheaf-Theoretic Consistency Checking (Optional, Speculative)

Sheaf-theoretic consistency is an optional formal lens for checking that local
observations are globally consistent (the sheaf condition). FrankenSQLite can
implement this check *in the harness* on top of the lab runtime:

- Each transaction's local view (its snapshot) is a "section" over its
  read set
- The sheaf condition requires that overlapping sections agree: if T1 and T2
  both read page P, and both see it through their respective snapshots, the
  versions they see must be consistent with the global version chain

This provides a formal framework for verifying that MVCC visibility rules
produce globally consistent views.

**Concrete example:**

```rust
// In a lab test, after running N concurrent transactions:
let sections: Vec<Section> = completed_txns.iter().map(|txn| {
    Section {
        domain: txn.read_set.clone(),
        assignment: txn.observed_versions.clone(),  // PageNumber -> (TxnId, PageData)
    }
}).collect();

// Check the sheaf condition: overlapping sections must agree.
//
// NOTE: This is a lab-only verification lens. We do NOT require a bespoke
// `fsqlite_harness::sheaf` module in V1. Instead, the harness SHOULD adapt
// asupersync's sheaf utilities (or an equivalent formally-defined checker) to
// operate on recorded MVCC observations.
let result = asupersync::trace::distributed::sheaf::check_consistency(
    &sections,
    &global_version_chains,
);
assert!(result.is_consistent(), "Sheaf violation: {}", result.obstruction());
```

### 4.7 Conformal Calibration -- Distribution-Free Confidence (Oracles + Perf)

Conformal prediction is used in two **distinct** ways:

1. **Oracle anomaly detection (asupersync-native):** calibrate on `OracleReport`s
   from deterministic lab runs and produce prediction sets for invariant-level
   behavior (distribution-free, finite-sample coverage).
2. **Numeric performance regression detection (FrankenSQLite harness):** treat
   throughput/latency/memory as first-class metrics, but gate changes using the
   Extreme Optimization Loop (baseline → profile → one-lever change → isomorphism
   proof → verify) rather than pretending a single conformal wrapper replaces
   benchmarking statistics.

#### 4.7.1 Oracle Calibrator (Actual Asupersync API)

Asupersync's `ConformalCalibrator` consumes `OracleReport` (not raw floats):

```rust
use asupersync::lab::{ConformalCalibrator, ConformalConfig, LabConfig, LabRuntime};
use asupersync::types::Budget;

let mut cal = ConformalCalibrator::new(ConformalConfig {
    alpha: 0.05,                  // 95% coverage guarantee
    min_calibration_samples: 50,   // require ≥50 seeds before predicting
});

// Calibration: many deterministic seeds, same scenario.
for seed in 0..100_u64 {
    let mut rt = LabRuntime::new(LabConfig::new(seed));
    let root = rt.state.create_root_region(Budget::INFINITE);
    let (task_id, _handle) = rt.state.create_task(root, Budget::INFINITE, async move {
        // ... run a representative FrankenSQLite harness scenario ...
    }).expect("create task");
    rt.scheduler.lock().unwrap().schedule(task_id, 0);
    let rep = rt.run_until_quiescent_with_report();
    cal.calibrate(&rep.oracle_report);
}

// Prediction: after a code change, new oracle report should remain conforming.
let mut rt = LabRuntime::new(LabConfig::new(101));
let root = rt.state.create_root_region(Budget::INFINITE);
let (task_id, _handle) = rt.state.create_task(root, Budget::INFINITE, async move {
    // ... same scenario ...
}).expect("create task");
rt.scheduler.lock().unwrap().schedule(task_id, 0);
let rep = rt.run_until_quiescent_with_report();

if let Some(pred) = cal.predict(&rep.oracle_report) {
    for ps in &pred.prediction_sets {
        if !ps.conforming {
            panic!("Oracle anomaly: {} score={} threshold={}", ps.invariant, ps.score, ps.threshold);
        }
    }
}
```

**Order-statistic intuition (why `min_calibration_samples` matters):**
The conformal threshold is the `ceil((1-α)(n+1))`-th order statistic of
calibration scores. With small `n`, thresholds are too permissive and regressions
slip through. `n >= 50` is a pragmatic minimum; phase gates typically run 100+
seeds for tighter bounds.

#### 4.7.2 Performance Regression Discipline (Extreme Optimization Loop)

Performance metrics are not oracle invariants. For throughput/latency changes,
FrankenSQLite MUST follow the Extreme Optimization Loop (baseline, profile, one lever,
isomorphism proof, verify). Asupersync's benchmarking guide is the reference template
for this workflow (Criterion baselines + smoke artifacts + opportunity scoring).

**Non-negotiable gate:** only land optimizations with OpportunityScore ≥ 2.0:
`score = impact * confidence / effort`.

### 4.8 Bayesian Online Change-Point Detection (BOCPD)

Database workloads are non-stationary. A write-heavy analytical job may start
at 2 AM, a bulk import may spike contention, or a schema migration may
temporarily change the page access pattern. Static thresholds for MVCC tuning
parameters (GC frequency, version chain length limit, witness-plane hot/cold
index compaction policy) will be wrong for at least one regime.

BOCPD (Adams & MacKay, 2007) detects regime shifts in real time by maintaining
a posterior distribution over the **run length** `r_t` (number of observations
since the last change point):

```
P(r_t | x_{1:t}) ∝ Σ_{r_{t-1}} P(x_t | r_t, x_{t-r_t:t-1}) * P(r_t | r_{t-1}) * P(r_{t-1} | x_{1:t-1})
```

(Note the summation over `r_{t-1}`: the previous run length must be
marginalized out. Without this sum, `r_{t-1}` would be a free variable.
This is the standard Adams & MacKay (2007) recursion.)

where:
- `P(x_t | r_t, ...)` is the predictive probability under the current regime
  (modeled as a conjugate Normal-Gamma for throughput, Beta-Binomial for abort rates)
- `P(r_t | r_{t-1})` encodes the hazard function (probability of a change point
  at each step; geometric hazard with `H = 1/250` for ~250-observation regimes)

**What we monitor with BOCPD:**

| Stream | Conjugate model | Action on change point |
|--------|----------------|----------------------|
| Commit throughput (ops/sec) | Normal-Gamma | Log regime shift, adjust GC frequency |
| SSI abort rate | Beta-Binomial | If rate jumps, log warning for DBA; if rate drops, consider relaxing version chain limits |
| Page contention (locks/sec) | Normal-Gamma | Adjust witness-plane refinement and hot-index pressure controls |
| Version chain length | Normal-Gamma | Tighten/loosen GC watermarks |

**Why BOCPD, not fixed-window averages:**
- No window size to tune (the algorithm infers the regime length).
- Exact posterior inference via the run-length recursion (no MCMC needed).
- Naturally handles multiple change points.
- Computational cost: O(t) per update in the naive implementation, but
  pruning low-probability run lengths keeps practical cost O(1) amortized.

**Integration:**

```rust
// NOTE: BOCPD is a FrankenSQLite harness component (not provided by asupersync).
use fsqlite_harness::drift::bocpd::{BocpdMonitor, BocpdConfig, HazardFunction};

// CALIBRATION NOTE (Alien-Artifact Discipline):
// All parameters below have explicit derivations. None are magic numbers.
let throughput_monitor = BocpdMonitor::new(BocpdConfig {
    hazard: HazardFunction::Geometric { h: 1.0 / 250.0 },
    // H = 1/250: Expected regime length = 250 observations.
    // At 1 observation/sec (commit batch rate), this is ~4 minutes.
    // Derived from: typical database workload phase duration is 1-30 min
    // (OLTP burst, batch import, maintenance window). 4 min is the geometric
    // mean. Sensitivity: H in [1/100, 1/1000] shifts detection delay by
    // ~2x but does not change qualitative behavior (false alarm rate stays
    // below 1/yr for all H in this range).
    model: ConjugateModel::NormalGamma {
        mu_0: 0.0,       // prior mean: 0 (uninformative; learns from first observations)
        kappa_0: 0.01,   // very weak prior on mean (0.01 pseudo-observations)
        alpha_0: 0.5,    // Jeffreys prior on variance (minimally informative)
        beta_0: 0.5,     // Jeffreys prior (matches alpha_0 for conjugacy)
        // WHY Jeffreys priors: the previous version hard-coded mu_0=50000 and
        // beta_0=1000, encoding a specific hardware assumption. Jeffreys priors
        // are objective/uninformative: the BOCPD adapts to whatever throughput
        // the actual hardware delivers within the first ~20 observations.
    },
    change_point_threshold: 0.5,
    // Threshold = 0.5: posterior P(r_t = 0) > 0.5 triggers detection.
    // This is the Bayes-optimal decision threshold under symmetric loss
    // (cost of false alarm = cost of missed change point). If actions taken
    // on detection are cheap (log + adjust GC), the threshold could be
    // lowered to 0.3 for earlier detection at the cost of more false alarms.
    // The actual cost ratio is L_false_alarm / L_delayed_detection ≈ 0.1
    // (adjusting GC is cheap, but delayed detection causes memory pressure),
    // giving optimal threshold ≈ L_fa / (L_fa + L_dd) = 0.1/1.1 ≈ 0.09.
    // We use 0.5 (conservative) because V1 BOCPD actions are advisory only.
});

// Feed observations from the MVCC commit path:
throughput_monitor.observe(current_throughput);
if throughput_monitor.change_point_detected() {
    let new_regime = throughput_monitor.current_regime_stats();
    log::warn!("Workload regime shift detected: throughput {} -> {} ops/sec",
               previous_regime.mean, new_regime.mean);
    gc_scheduler.adjust_frequency(new_regime.mean);
}
```

**Monitoring stack (merged, canonical):**

- **Layer 0 (asupersync deadline monitor):** adaptive deadline warnings and "no progress"
  detection based on `Cx::checkpoint*` and task-type labeling via `Cx::set_task_type("...")`.
- **Layer 1 (e-processes):** anytime-valid evidence of invariant violations (sound false-alarm control).
- **Layer 2 (conformal):** distribution-free anomaly detection on *oracle reports* across seeds.
- **Optional Layer 3 (BOCPD harness):** regime-shift detection on workload streams; used to retune
  heuristics (GC watermarks, eviction aggressiveness) and explain performance changes.

**Deadline monitoring (actual asupersync builder API):**

```rust
use asupersync::runtime::RuntimeBuilder;
use std::time::Duration;

let rt = RuntimeBuilder::low_latency()
    .deadline_monitoring(|m| {
        m.enabled(true)
            .check_interval(Duration::from_secs(1))
            .checkpoint_timeout(Duration::from_secs(30))
            .adaptive_enabled(true)
            .adaptive_warning_percentile(0.90)
            .adaptive_min_samples(10)
            .adaptive_fallback_threshold(Duration::from_secs(30))
            .on_warning(|w| eprintln!("deadline warning: {w:?}"))
    })
    .build()
    .expect("runtime");
```

### 4.9 TLA+ Export -- Model Checking

Asupersync ships a **trace-driven** TLA+ exporter: `asupersync::trace::TlaExporter`.
It can:

- Export a **concrete behavior** (a sequence of states) from a `Vec<TraceEvent>`.
- Export a **parametric skeleton** for a model-checkable spec structure.

FrankenSQLite adopts the same pattern for MVCC protocols:

1. Instrument MVCC commit/checkpoint/GC with a domain trace (`MvccTraceEvent`).
2. Run deterministic scenarios in the harness (LabRuntime seeds + schedule certs).
3. Export both:
   - A concrete behavior module for debugging ("what actually happened")
   - A spec skeleton for TLC checks ("what can happen in bounded models")

**Concrete example (asupersync runtime trace export):**

```rust
use asupersync::trace::{TraceEvent, TlaExporter};

let events: Vec<TraceEvent> = /* captured from a deterministic run */;
let exporter = TlaExporter::from_trace(&events);
let behavior = exporter.export_behavior("AsupersyncRuntimeBehavior");
let skeleton = TlaExporter::export_spec_skeleton("AsupersyncRuntimeModel");
```

**Concrete example (FrankenSQLite MVCC trace export; harness feature):**

```rust
use fsqlite_harness::tla::{MvccTlaExporter, MvccTraceEvent};

let mvcc_events: Vec<MvccTraceEvent> = /* captured from MVCC commit scenarios */;
let exporter = MvccTlaExporter::from_trace(&mvcc_events);
let behavior = exporter.export_behavior("MvccCommitBehavior");
let skeleton = MvccTlaExporter::export_spec_skeleton("MvccCommitModel");
```

### 4.10 BlockingPool Integration

All file I/O in FrankenSQLite is dispatched to asupersync's blocking pool,
ensuring that the async runtime's worker threads are never blocked by
synchronous system calls.

**Hard rule (workspace invariant): `unsafe` is forbidden.**

Therefore, FrankenSQLite MUST NOT transmit raw pointers or borrowed slices
across a `spawn_blocking` boundary. The correct, safe, zero-allocation pattern
is: **owned pooled buffers** moved into the blocking closure and returned by
value (RAII on drop).

We use asupersync's blocking helpers:
- `asupersync::runtime::spawn_blocking`
- `asupersync::runtime::spawn_blocking_io`

**I/O buffer model (normative):**

- `PageBuf`: owned, page-sized buffer handle that is `Send + 'static`.
  Drop returns the underlying allocation to a pool (even if the task is cancelled).
- `PageBufPool`: bounded pool keyed by `page_size`. This is FrankenSQLite
  infrastructure (in `fsqlite-pager`), not an asupersync feature.

This achieves all goals simultaneously:
- no `unsafe`
- no heap allocation on the hot path (pool reuse)
- no extra memcpy in the common path (pager consumes `PageBuf` directly)
- cancellation safety: if a task is cancelled mid-I/O, the buffer is dropped and
  deterministically returned to the pool.

**How file I/O is dispatched (canonical pattern):**

```rust
use asupersync::cx::Cx;
use asupersync::runtime::spawn_blocking_io;

/// Read exactly one database page into an owned pool buffer (no memcpy).
///
/// NOTE: Uses `std::os::unix::fs::FileExt::read_exact_at` (safe API),
/// NOT raw `pread`/`RawFd` which would require `unsafe`.
async fn read_page(cx: &Cx, pool: &PageBufPool, file: &Arc<File>, offset: u64) -> Result<PageBuf> {
    cx.checkpoint()?; // observe cancellation before scheduling blocking work

    let mut buf = pool.acquire(); // PageBuf (owned, RAII -> pool on drop)
    let file = Arc::clone(file);

    // Move the owned buffer into the blocking closure. This is safe and `unsafe`-free.
    let buf: PageBuf = spawn_blocking_io(move || {
        // FileExt::read_exact_at is safe Rust; no `unsafe` needed.
        file.read_exact_at(buf.as_mut_slice(), offset)?;
        Ok(buf)
    })
    .await?;

    Ok(buf)
}
```

**Cancel semantics (asupersync):**

`spawn_blocking*` is *soft-cancel*: dropping the future requests cancellation of
the pool task, but the underlying OS syscall may still run to completion. This is
acceptable because all FrankenSQLite durable effects are guarded by:
- the reserve/commit publication protocol (ECS symbol logs, witness plane)
- commit markers as the atomic visibility point

So a cancelled task can never publish a partial commit as durable.

**Pool sizing:**

The blocking pool uses a min/max thread model:

- **Minimum threads: 1** -- always at least one blocking thread available for
  immediate dispatch, avoiding cold-start latency on the first I/O operation.
- **Maximum threads: derived from storage class** -- not a fixed constant.
  The optimal thread count follows from Little's Law (`L = lambda * W`):

  | Storage class | Mean service time W | Optimal threads at 10K IOPS |
  |---------------|--------------------|-----------------------------|
  | HDD (7200rpm) | ~8ms (seek+rotate) | 80 (but serialized by arm)  |
  | SATA SSD      | ~100us             | 1-2                         |
  | NVMe SSD      | ~15us              | 1-2 (kernel parallelism)    |

  For single-file database workloads, HDD and SATA SSD serialize requests
  internally (single command queue). The benefit of >1 thread is overlap
  with CPU work (CRC computation while another read is in-flight), not
  increased I/O bandwidth. NVMe devices support multiple hardware queues
  and internal parallelism, so additional threads yield actual I/O
  concurrency. Defaults: **HDD: 2**, **SATA SSD: 2**, **NVMe: 4**.
  Auto-detected via `statfs()` heuristic; overridable with
  `PRAGMA fsqlite.blocking_pool_threads`.

- **Idle timeout: 10 seconds (derived from survival analysis)** -- minimizes
  `L_spawn * P(arrival < t) + L_idle * t * P(no_arrival < t)` where
  `L_spawn ≈ 50us` (thread creation cost) and `L_idle ≈ 8MB` (stack memory
  per idle thread). For bursty I/O with exponential inter-arrival times,
  the optimal timeout ranges 5-30s. The BOCPD workload monitor (Section 4.8)
  adjusts this adaptively when it detects a regime shift in I/O arrival rate.

**How this interacts with async callers:**

The async-to-blocking bridge works as follows:

1. Async task calls `asupersync::runtime::spawn_blocking*(closure)`, which returns a `Future`.
2. The closure is placed on the blocking pool's work queue.
3. A blocking pool thread picks up the closure and executes it.
4. When the closure completes, the result is sent back via an internal oneshot
   channel, waking the async task.
5. The async task receives the result and continues.

This ensures that:
- The async runtime's worker threads (which drive the VDBE, parser, planner)
  are never blocked by disk I/O.
- File I/O operations are still cancellable: if the async task is cancelled,
  the blocking operation runs to completion (cannot interrupt `pread64`), but
  the result is discarded and the async task is cleaned up.
- In lab runs, the runtime typically omits a blocking pool; `spawn_blocking*`
  falls back to executing the closure inline (see asupersync implementation),
  preserving determinism by avoiding real threads.

---

### 4.11 Structured Concurrency (Regions) -- Database Lifetime and Quiescence

FrankenSQLite adopts asupersync's region tree as the **non-negotiable lifetime
model** for all concurrency:

- Every background worker, coordinator, replicator, and long-lived service MUST
  run as a region-owned task/actor.
- No task may outlive the `Database` root region. There are no detached tasks.
- `Database::close()` MUST close the root region and await **quiescence**.

This is not cosmetic. It is the structural guarantee that makes shutdown,
cancellation, and failure handling predictable: no orphan tasks, no "still
flushing in the background", no half-finished repairs.

**Normative region tree (conceptual):**

```
DbRootRegion
  - WriteCoordinatorRegion          (native marker sequencer + compat WAL path)
  - SymbolStoreRegion               (local symbol logs + tiered storage fetch)
  - ReplicationRegion               (stream symbols; anti-entropy; membership)
  - CheckpointGcRegion              (checkpointer, compactor, GC horizon)
  - ObservabilityRegion             (deadline monitor, task inspector, metrics)

PerConnectionRegion (child of DbRootRegion)
  - QueryExecution tasks
  - Cursor prefetch tasks (bounded; optional)

PerTransactionRegion (child of PerConnectionRegion)
  - Encode/persist capsule tasks (native mode)
  - Witness publication tasks
  - Validation tasks
```

**Rule (INV-REGION-QUIESCENCE):** A region MUST NOT report closed until:
- all child tasks are completed,
- all finalizers have run,
- all obligations are resolved (Committed/Aborted, not Reserved/Leaked).

**Practical consequence:** Closing the database is a protocol, not a `drop`:
on close we request cancellation, drain, finalize, then return. Any subsystem
that cannot prove bounded drain is a spec violation.

### 4.12 Cancellation Is a Protocol (Request → Drain → Finalize) + Masking

Asupersync cancellation is **not** "drop the future". It is a multi-phase
protocol with explicit checkpoints, bounded drain, and finalizers.

**Task cancellation state machine (asupersync oracle model):**

```
Created/Running → CancelRequested → Cancelling → Finalizing → Completed(Cancelled)
```

**Rules:**
- **INV-CANCEL-PROPAGATES:** Region cancellation MUST propagate to all descendant
  regions; a parent cannot be cancelled while a child remains uncancelled.
- **INV-CANCEL-IDEMPOTENT:** Multiple cancel requests MUST be monotone: the
  strongest cancel reason wins (it cannot get weaker).
- **INV-LOSERS-DRAIN:** Any combinator that returns early (race/timeout/hedge)
  MUST cancel and drain losers to completion before returning.

#### 4.12.1 Checkpoints (Where Cancellation Is Observed)

FrankenSQLite MUST place `cx.checkpoint()` / `cx.checkpoint_with(...)` at yield
points that bound the "amount of uninterruptible work" between observations:

- VDBE instruction boundaries (each opcode tick).
- B-tree descent loops (every node visit).
- RaptorQ decode/encode loops (every fixed number of symbol operations).
- Any loop over user data (every N rows; N derived from budget poll_quota).

**Rule:** Any cancellation-unaware hot loop is a bug. Cancelling a query must
bound cleanup and bound latency, not "maybe if it hits an await".

#### 4.12.2 Masked Critical Sections (Cx::masked, MAX_MASK_DEPTH)

Asupersync supports bounded cancellation deferral via `Cx::masked(...)`:
while masked, `checkpoint()` returns `Ok(())` even if cancellation is requested.

Masking exists for **short, atomic publication steps** that must not be
interrupted once started (two-phase effects):
- Completing a reserved send/commit.
- Publishing a marker after allocating `commit_seq`.
- Releasing a set of resources in a required order.

Asupersync enforces **INV-MASK-BOUNDED**: mask depth MUST be finite and bounded
(`MAX_MASK_DEPTH = 64`). Exceeding the bound is a correctness failure (panic in
lab; fatal diagnostic in production).

**Rule:** FrankenSQLite MUST NOT use masking for long operations (remote fetch,
bulk decode, long scans). Masking MAY wrap tiny durability-critical steps
(e.g., marker publication + local fsync barriers in the commit section), but
every masked section MUST remain explicitly bounded (poll quota + leak-free
obligation discipline).

#### 4.12.3 Commit Sections (Bounded Masking for Two-Phase Protocols)

For protocol steps that are logically atomic but involve multiple operations,
FrankenSQLite SHOULD use an asupersync commit section helper (`commit_section`
semantics) that:
- masks cancellation while the section is in progress,
- enforces a poll quota bound (bounded deferral),
- guarantees finalizers run even on cancellation.

**Normative usage sites:**
- In the WriteCoordinator: once FCW validation passes and `commit_seq` is
  allocated, proof+marker publication MUST run as a commit section so the
  sequencer cannot emit "half a commit" under cancellation.
- In witness publication: once a reservation is committed, the commit must
  complete or the reservation must abort deterministically.

### 4.13 Obligations (Linear Resources) -- No Leaks, No Ghosts

Asupersync models cancellation-safe effects using **obligations** (linear
resources) with a two-phase lifecycle:

```
Reserved  ──commit──▶  Committed
    │
    └─abort/drop──▶  Aborted

(Bug) Reserved ──drop without resolution──▶ Leaked  (detected by oracles)
```

Obligations are what turn "best-effort cleanup" into a structural invariant.

**Rule (INV-NO-OBLIGATION-LEAKS):** Every reserved obligation MUST reach a
terminal state (Committed or Aborted). Leaked obligations are correctness bugs:
fail-fast in lab; diagnostic escalation in production.

**FrankenSQLite MUST treat the following as obligations:**
- Commit pipeline `SendPermit` reservations (two-phase MPSC).
- Commit response delivery (reply obligation on oneshot/session replies).
- TxnSlot acquisition + renewal (lease obligations; abort on expiry).
- Witness-plane reservation tokens for symbol/object publication (reserve/commit).
- Any "name/registration" in shared state that could go stale on crash.

#### 4.13.1 Tracked Two-Phase Channels for Safety-Critical Protocols

For safety-critical internal messaging, FrankenSQLite SHOULD use asupersync's
obligation-tracked session channels (`asupersync::channel::session`) rather than
raw MPSC/oneshot, so dropping a permit without resolution is structurally
detected:

- Lab mode: leaks MUST fail fast (panic-on-leak).
- Production: leaks MUST be trace-visible (log + metrics) and MUST trigger
  escalation (close the offending region/connection) rather than silently
  continuing.

**Rule:** It is acceptable for non-critical telemetry channels to use policies
like `send_evict_oldest`, but commit ordering, durability publication, and
cross-process coordination MUST NOT drop messages.

#### 4.13.2 Obligation Leak Response Policy (Lab vs Production)

FrankenSQLite inherits asupersync's stance:
- **Lab runtime default:** obligation leak is a test failure (panic) because it
  indicates a cancel-safety or protocol bug.
- **Production default:** obligation leak is a correctness incident: emit a
  diagnostic bundle (trace + obligation ledger), fail the affected connection,
  and keep the database process alive if and only if invariants for durability
  objects are not violated.

### 4.14 Supervision (Spork/OTP-Style) for Database Services

Long-lived services (sequencers, replicators, checkpoint workers) MUST be
supervised. "Spawn a loop and hope" is forbidden.

Asupersync supervision provides:
- Strategies: `Stop`, `Restart(config)`, `Escalate`.
- Restart budgets: `max_restarts` in a sliding `window`, with backoff.
- Budget-aware restarts (cost quota, min remaining time, min poll quota).
- Monotone severity: outcomes cannot be "downgraded" by supervision.

**Rule (INV-SUPERVISION-MONOTONE):**
- `Outcome::Panicked` MUST NOT be restarted (programming error). Stop/escalate.
- `Outcome::Cancelled` MUST stop (external directive / shutdown).
- `Outcome::Err` MAY restart if the error is classified transient and restart
  budget allows.

**FrankenSQLite supervision tree (normative):**
- `DbRootSupervisor` owns:
  - `WriteCoordinator`: `Escalate` on Err/Panicked (sequencer correctness is core).
  - `SymbolStore`: `Restart` on transient I/O; `Escalate` on integrity faults.
  - `Replicator`: `Restart` with exponential backoff; `Stop` when remote disabled.
  - `CheckpointerGc`: `Restart` (bounded) on transient errors; escalate if repeated.
  - `IntegritySweeper` (optional): `Stop` on error; does not gate core function.

This structure ensures: a component crash becomes an explainable, bounded event
with a deterministic restart policy, not a silent hang or memory leak.

### 4.15 Resilience Combinators (Backpressure, Isolation, Graceful Degradation)

FrankenSQLite MUST leverage asupersync's cancel-safe combinators to keep the
system robust under load and partial failure:

- `pipeline`: staged commit capsule publication and replication with backpressure.
- `bulkhead`: isolate heavy work (encode/decode/compaction/remote fetch) with
  bounded parallelism so it cannot starve the sequencer or VDBE.
- `rate_limit`: cap background work (GC/compaction/sweeps) to preserve p99 query
  latency.
- `retry`: budget-aware retries for transient I/O (with jitter/backoff).
- `circuit_breaker`: open/half-open/closed policy for remote tier fetch; prevent
  retry storms when remote is degraded.
- `hedge` / `first_ok`: latency reduction for symbol fetch (start backup after
  delay; first success wins).
- `bracket`: acquire/use/release wrappers so resource cleanup is guaranteed
  under cancellation (file handles, leases, reservations).

**Rule:** Any use of these combinators MUST preserve INV-LOSERS-DRAIN and
INV-NO-OBLIGATION-LEAKS; the loser branches must drain and all obligations must
resolve even when the winner returns early.

### 4.16 Observability and Diagnostics (Task Inspector, Explainable Failures)

FrankenSQLite MUST surface asupersync-native diagnostics for production and lab:

- Task inspector: live visibility into blocked reasons, budget usage, mask depth,
  held obligations, and cancellation status.
- Diagnostics: structured explanations for cancellation propagation and blocked
  tasks (why are we stuck? who holds what?).
- Deterministic repro bundles: when `ASUPERSYNC_TEST_ARTIFACTS_DIR` is set in
  harness runs, failures MUST emit a repro manifest and trace artifacts that
  recreate the schedule and cancellation points.

This is a direct consequence of the "no vibes" philosophy: if something times
out or aborts, the system must be able to explain why with evidence.

#### 4.16.1 Evidence Ledger (Galaxy-Brain Explainability, Deterministic)

Asupersync supports emitting an **evidence ledger**: a bounded, deterministic
record of *why* a cancellation/race/scheduler decision occurred (trace-backed,
replay-stable). FrankenSQLite MUST leverage this to make core events
explainable:

- cancellation propagation (who cancelled whom, and why)
- race/timeout/hedge winner selection (and loser drain proofs)
- scheduler choices under deadlines/budgets (lane + tie-break)
- commit/abort decisions (FCW conflicts, SSI pivot aborts, merge eligibility)

**Minimum ledger entry schema (normative):**

```text
EvidenceEntry := {
  decision_id : u64,
  kind        : { cancel, race, scheduler, commit },
  context     : { task_id: u64, region_id: u64, lane: {Cancel, Timed, Ready} },
  candidates  : Vec<Candidate>,
  constraints : Vec<Constraint>,
  chosen      : CandidateId,
  rationale   : Vec<Reason>,
  witnesses   : Vec<TraceEventId>,
}
```

**Determinism requirements:**
- Field ordering MUST be deterministic.
- Candidate ordering MUST be deterministic (stable by `(score desc, id asc)`).
- Witness references MUST be stable under replay (trace event ids or hashes).
- Ledger size MUST be bounded (ring buffer + spill-to-artifacts in lab mode).

**Emission policy (required):**
- **Lab:** evidence ledger MUST be emitted for any failing test, any SSI abort,
  and any commit abort due to FCW/SSI/merge.
- **Production:** evidence ledger SHOULD be sampleable and gated (PRAGMA or
  env). It MUST NOT impose unbounded overhead or allocate on hot paths.

### 4.17 Policy Controller (Expected Loss + Anytime-Valid Guardrails + BOCPD)

Many parameters in FrankenSQLite are **policies**, not correctness axioms:
redundancy overhead, checkpoint cadence, background compaction rate limits,
busy timeouts, and which SAFE merge rungs are worth attempting under budget.
Hard-coded thresholds are brittle because workloads and environments shift.

FrankenSQLite therefore defines an optional but recommended `PolicyController`
service that tunes *non-correctness* knobs using principled math, with explicit
guarantees and explainability.

**Non-negotiable safety rule:** `PolicyController` MUST NOT change correctness
semantics (e.g., isolation level, enabling LAB_UNSAFE merges, bypassing
invariant checks). It only tunes performance/reliability knobs within the
pre-defined safe envelope.

**Inputs (normative):**
- **Anytime-valid monitors (e-processes):** guardrail budgets on failure/violation
  rates under optional stopping (§4.3).
- **Conformal budgets:** distribution-free performance bounds over oracle reports
  across seeds (§4.7).
- **Regime detection (BOCPD):** change-point posterior for workload/health streams
  (§4.8).
- **Local telemetry:** latency histograms, queue depths, symbol fetch success,
  merge accept/reject counts, etc. (All telemetry is advisory; correctness never
  depends on it.)

**Monitoring is also a policy (VOI budgeting, recommended):**

Some measurements are cheap (counters, lightweight histograms). Others are
expensive and must be budgeted (integrity sweeps, row-level replay to classify
SSI false positives, deep B-tree invariant audits).

The controller SHOULD schedule *optional* monitors by Value of Information
(VOI), under explicit CPU/I/O budgets:

```
VOI(m) = E[ ΔLoss(m) | evidence ] - Cost(m)
```

Correctness-critical monitors (durability budgets, MVCC invariants) have
effectively infinite VOI and MUST remain always-on; VOI only gates additional,
diagnostic, or high-cost measurements.

**Typical knobs (non-exhaustive):**
- redundancy overhead / repair slack (§3.5.12),
- group-commit batch size N (conformal; §4.5),
- retry/backoff control (optimal stopping; §18.8),
- transaction max duration D (memory boundedness; §5.5) and lease sizing (§5.6.2),
- background GC/compaction scheduling (§7.13).

**Decision rule (normative): expected loss minimization**

For each policy knob `k`, define a finite action set `A_k` (candidate settings)
and a loss matrix `L(a, state)` reflecting asymmetric costs (e.g., data loss risk
is vastly more expensive than extra redundancy bytes). The controller chooses:

```
a* = argmin_{a in A_k}  E[ L(a, state) | evidence ]
```

where `evidence` is the current monitor state (e-process trajectories, conformal
alerts, BOCPD regime posterior, telemetry).

**Guardrails (normative):**
- The controller MUST NOT take an action that violates an active e-process budget.
  Example: decreasing `raptorq_overhead` is forbidden while the symbol-loss
  monitor rejects `H0: p <= p_budget` (§3.5.3).
- If BOCPD detects a regime shift with posterior `P(change) > threshold`, the
  controller MAY retune action sets and priors, but it MUST emit an evidence
  ledger entry describing the change-point and the new policy choice (§4.16.1).

**Explainability (required):**

Every automatic policy change MUST emit an evidence ledger entry that includes:
- the knob name and prior setting,
- the candidate actions evaluated,
- the expected loss for each candidate,
- the winning action and the top contributing evidence (e.g., e-value threshold
  crossing, change-point posterior spike, conformal alert).

**Determinism (required in lab):**

Under `LabRuntime`, `PolicyController` decisions MUST be deterministic for a
given trace + seed: no dependence on wall-clock, hash randomization, or
unordered iteration. Any randomization MUST be explicit, seeded, and recorded
in the evidence ledger.

### 4.18 Epochs (Asupersync EpochClock) -- Validity Windows and Coordination

Asupersync provides an epoch model (`asupersync::epoch`) for time-bounded
distributed operations. FrankenSQLite adopts epochs as the explicit mechanism
for cross-process and cross-host transitions that must not be "half applied":

- durability quorum membership changes,
- remote tier endpoint changes,
- symbol authentication key rotations,
- compaction publication generations (optional, advisory).

**Definition (normative):**
- `ecs_epoch : EpochId` is a monotone `u64` stored durably in `RootManifest.ecs_epoch`
  (§3.5.5) and mirrored in `SharedMemoryLayout.ecs_epoch` (§5.6.1).
- `ecs_epoch` increments only under a serialized coordinator decision. Epochs
  MUST NOT be reused.

#### 4.18.1 SymbolValidityWindow (Normative Default)

The engine defines a SymbolValidityWindow:

```
SymbolValidityWindow := [0, RootManifest.ecs_epoch]
```

This is a fail-closed policy for future epochs:
- Symbols/segments tagged with `epoch_id > RootManifest.ecs_epoch` MUST be rejected
  as misconfiguration or replay from an incompatible future configuration.
- Past epochs are accepted by default (time travel + full-history retention).

Implementations MAY tighten the lower bound (reject very old epochs) only if
the retention policy does not require decoding historical objects from those
epochs.

#### 4.18.2 Epoch-Scoped Symbol Authentication Key Derivation (Required)

When symbol authentication (`auth_tag`) is enabled (§3.5.2), the verification
key MUST be derived as a deterministic function of `(master_key, ecs_epoch)`:

```
K_epoch = BLAKE3_KEYED(master_key, "fsqlite:symbol-auth:epoch:v1" || le_u64(ecs_epoch))
```

**Master key source (normative):**
- **Production (preferred):** If page encryption is enabled, `master_key` MUST
  be derived from the database's encryption `DEK` with domain separation (so a
  page-encryption key cannot be misused as a transport-auth key without
  intent):
  `master_key = BLAKE3_KEYED(DEK, "fsqlite:symbol-auth-master:v1")`.
- **Production (no encryption):** If page encryption is disabled and
  `symbol_auth = on`, the caller MUST provide a `SymbolAuthMasterKeyCap`
  (or equivalent) through `Cx`. Without an explicit key capability, enabling
  `symbol_auth` MUST fail (no ambient keys).
- **Lab runtime:** `master_key` MUST be derived deterministically from the seed
  so traces are replay-stable.

This aligns with asupersync's "no ambient keys" principle: keys are provided
through capabilities, and derivation is deterministic (lab-replayable).

**Rule:** Auth failures MUST fail closed: invalid/missing `auth_tag` on a symbol
record MUST cause the symbol to be rejected for decoding (it MAY still be used
as a corruption signal for redundancy autopilot (§3.5.12)).

#### 4.18.3 Epoch-Scoped Remote Durability Configuration (Required)

If durability depends on remote acknowledgements (`PRAGMA durability = quorum(M)`),
the durability configuration is epoch-scoped:
- Requests MUST carry `ecs_epoch` and peers MUST reject requests outside their
  SymbolValidityWindow (preventing mixed-quorum ambiguity).
- CommitMarkers implicitly bind to the epoch in effect at the time their
  referent symbols were made durable.

#### 4.18.4 Epoch Transition Barrier (Quiescence Without Stop-The-World)

Epoch transitions that affect correctness-critical policy (quorum membership,
symbol auth master key) MUST establish a quiescence point so no single commit
straddles two epochs.

FrankenSQLite MUST implement this as an asupersync-style barrier:
- Coordinator creates an `EpochBarrier(current_epoch, participants=N, timeout)`.
- Participants are the region-owned services: WriteCoordinator, SymbolStore,
  Replicator, CheckpointerGc.
- Each participant arrives only after draining in-flight work that would bind to
  the old epoch (e.g., commits in the sequencer queue; ongoing remote uploads).
- If the barrier triggers with `AllArrived`, the coordinator increments
  `RootManifest.ecs_epoch`, publishes it durably, then updates
  `SharedMemoryLayout.ecs_epoch` with `Release`.
- If the barrier triggers by `Timeout` or `Cancelled`, the transition MUST abort
  (remain in the old epoch) unless explicitly forced by an operator command.

This is the non-vibes way to do configuration changes: either everyone arrived
and the epoch advanced, or it did not.

### 4.19 Remote Effects (Asupersync Remote) -- Named Computations, Leases, Idempotency, Sagas

Tiered storage (L3) and replication are fundamentally remote effects. FrankenSQLite
adopts asupersync's remote contract so remote behavior is cancellable, bounded,
and auditable rather than ad-hoc.

#### 4.19.1 Explicit Remote Capability (Required)

All remote operations MUST require `RemoteCap` in `Cx`. Without it:
- No network I/O can occur (compile-time or runtime refusal).
- Native mode still functions under `durability = local` (remote is optional).

This prevents silent network I/O from arbitrary SQL code paths and makes the
system testable (lab contexts simply omit RemoteCap).

#### 4.19.2 Named Computations (No Closure Shipping, Required for Auditing)

Remote work MUST be specified by a `ComputationName` plus serialized input bytes.
The runtime must never serialize arbitrary closures.

Normative remote computation names (minimum set):
- `symbol_get_range(object_id, esi_lo, esi_hi, ecs_epoch)`
- `symbol_put_batch(object_id, symbols[], ecs_epoch)`
- `segment_put(segment_id, bytes, ecs_epoch)`
- `segment_stat(segment_id, ecs_epoch)`

#### 4.19.3 Lease-Backed Liveness (Required)

Remote handles MUST be lease-backed: if a lease expires, the local region MUST
escalate (cancel, retry, or fail), and the event MUST be trace-visible.

This is how we avoid "hung remote fetch" as an unbounded tail-latency failure.

#### 4.19.4 Idempotency (Required)

All remote requests that might be retried MUST include an IdempotencyKey:

```
IdempotencyKey = Trunc128(BLAKE3("fsqlite:remote:v1" || request_bytes))
```

Remote receivers MUST deduplicate by IdempotencyKey (asupersync IdempotencyStore
semantics):
- Duplicate with same computation name + inputs returns the recorded outcome.
- Duplicate with same key but different computation inputs is a conflict and MUST
  be rejected.

#### 4.19.5 Sagas for Multi-Step Publication (Compaction, Eviction, Required)

Any multi-step remote+local workflow that would otherwise leave partial state
on cancellation/crash MUST be expressed as a Saga (forward steps + deterministic
compensations). This is required for:
- L2 segment eviction to L3 (upload -> verify -> retire local),
- compaction publish (write new segments -> publish -> update locators/manifests).

Sagas are deterministic and replayable: given the same inputs, the same sequence
of steps and compensations occurs, and evidence is recorded for debugging.

#### 4.19.6 Networking Stack (Asupersync net) + Deterministic VirtualTcp (Required)

When remote effects are enabled, FrankenSQLite MUST use asupersync's cancel-safe
network stack (TCP + TLS + HTTP/2 where applicable) so that:

- cancellation is not a "drop the future" footgun (losers drain; obligations resolve),
- deadlines/budgets bound network I/O and handshake time,
- transport behavior is deterministic in lab mode.

**Production transport requirements:**

- **TLS by default:** Remote effects over the network SHOULD use TLS via rustls.
  Plaintext transport is permitted only when explicitly configured for local
  development and MUST be gated by an explicit capability/config knob.
- **Handshake + protocol timeouts:** Remote handshakes and protocol parsing MUST
  be budgeted and time-bounded (deadline or explicit timeouts).
- **HTTP/2 hard limits (if HTTP/2 is used):**
  - `max_concurrent_streams = 256` (default),
  - `max_header_list_size = 65536` (64 KiB),
  - `continuation_timeout_ms = 5000`,
  - absolute header fragment cap `256 KiB`.
  These prevent stream exhaustion and header-compression bombs from turning
  tiered storage into a DoS vector.
- **Message size caps:** Any remote RPC framing MUST enforce max send/recv sizes
  (default: 4 MiB) and reject larger messages deterministically.

**Deterministic network testing requirements:**

- In lab tests, the remote transport MUST be swappable to `VirtualTcp` (in-memory,
  deterministic, no kernel sockets). This is required to make replication and
  tiered-storage behaviors reproducible and DPOR-explorable under `LabRuntime`.
- The harness MUST provide a "drop/reorder/corrupt" virtual network shim to
  simulate lossy replication while preserving deterministic replay (loss patterns
  derive from the lab seed and are trace-visible).

### 4.20 Scheduler Priority Lanes (Cancel / Timed / Ready) -- Tail Latency Control

Asupersync's scheduler is lane-aware: cancellation work should run immediately;
deadline work should respect EDF; background work should not steal p99 latency.

FrankenSQLite MUST map its work to lanes via `Cx` budgets and task labeling:

- **Cancel lane (highest priority):** cancellation/drain/finalizers, obligation
  completion, rollback/cleanup, and coordinator responses to cancellations.
  These tasks MUST not be starved by background work.
- **Timed lane (EDF):** user queries with explicit deadlines, commit publication
  (marker append + response), and tiered-storage reads required for foreground
  queries.
- **Ready lane:** background GC, compaction, checkpointing, anti-entropy, and
  statistics updates (these MUST be `rate_limit`ed / `bulkhead`ed; §4.15).

**Normative rule:** any long-running loop in foreground work MUST checkpoint
frequently and SHOULD call `cx.set_task_type("...")` once at task start so
deadline monitors and perf dashboards can bucket behavior by task class.

## 5. MVCC Formal Model (Revised)

This section supersedes `MVCC_SPECIFICATION.md` with corrections for the
isolation level analysis, checksum performance, and multi-process semantics.

### 5.1 Core Types

```
TxnId       := u64                          -- monotonically increasing logical id allocated at BEGIN (AtomicU64)
TxnEpoch    := u32                          -- increments when a TxnSlotId is reused (prevents stale slot interpretation)
TxnToken    := (txn_id: TxnId, txn_epoch: TxnEpoch)

CommitSeq   := u64                          -- monotonically increasing commit sequence (assigned at COMMIT by the sequencer)
SchemaEpoch := u64                          -- increments on schema/layout changes (DDL, VACUUM, etc.)
PageNumber  := NonZeroU32                   -- 1-based page number
PageData    := Vec<u8>                      -- page content, length = page_size

Snapshot := {
    high            : CommitSeq,            -- all commits with commit_seq <= high are visible
    schema_epoch    : SchemaEpoch,          -- schema version at BEGIN (prevents intent replay across schema changes)
}

-- Schema epoch discipline:
-- - `schema_epoch` increments on any committed schema or physical-layout change
--   (DDL, VACUUM, etc.).
-- - A transaction MUST NOT perform intent-log replay / rebase merge if its
--   `snapshot.schema_epoch` differs from the current schema epoch.
-- - A write transaction that reaches COMMIT with a stale schema epoch MUST
--   abort with `SQLITE_SCHEMA` (caller must reprepare/retry under the new
--   schema).

PageVersion := {
    pgno       : PageNumber,
    commit_seq : CommitSeq,                 -- 0 for uncommitted/private versions (only in a txn write_set)
    created_by : TxnToken,                  -- creator identity (debug/audit); not used for visibility
    data       : PageData,                  -- or sparse XOR delta (Section 3.4.4)
    prev_idx   : Option<VersionIdx>,        -- index into VersionArena (NOT Box pointer)
}

-- NOTE: The XXH3-128 hash for integrity checking (Section 7.2) is stored
-- in CachedPage, NOT in PageVersion. CachedPage wraps PageVersion and adds
-- the hash field. PageVersion is the version-chain payload; CachedPage is
-- the buffer pool entry.

-- PERFORMANCE (Extreme Optimization Discipline):
-- Version chains MUST NOT use heap-allocated linked lists (Box<PageVersion>).
-- Pointer-chasing through N heap allocations at N random addresses is the
-- worst possible pattern for CPU cache utilization (Section 1.5 mandates
-- "no pointer chasing in hot paths").
--
-- Instead, all PageVersion nodes live in a VersionArena: a dense,
-- append-only arena. VersionIdx is a u32 "slot number" interpreted as:
--   chunk = idx / ARENA_CHUNK
--   off   = idx % ARENA_CHUNK
-- Traversing a version chain of length L touches L entries in dense chunk
-- storage (mostly sequential memory access within chunks).
--
-- CONCURRENCY (normative): The VersionArena MUST define an explicit
-- synchronization regime. V1 uses a single-writer / multi-reader model:
-- - Single-writer: only the commit sequencer/coordinator allocates and frees
--   VersionIdx slots (publication already has a serialized step; §5.9).
-- - Multi-reader: readers may resolve VersionIdx concurrently.
-- - Synchronization: VersionArena MUST be wrapped in a readers-writer lock
--   (e.g., `parking_lot::RwLock`). Readers MUST dereference VersionIdx only
--   while holding a read guard; the coordinator mutates the arena only while
--   holding a write guard. Implementations MUST NOT hand out raw pointers or
--   references that outlive the guard.
--
-- MEMORY STABILITY (normative): The arena MUST be chunked so that appending new
-- versions cannot reallocate/move previously published PageVersion storage.
--
-- Theorem 5 (Section 5.5) bounds version chain length to R * D + 1 where
-- R is the write rate and D is the duration above the GC horizon. For
-- typical workloads (R=100 writes/sec, D=0.1s), chains are <= 11 entries.
-- The per-page version chain head table (mapping PageNumber -> VersionIdx)
-- can use SmallVec<[VersionIdx; 8]> to inline the most recent chain heads
-- without heap allocation; when a page has more than 8 retained versions,
-- the overflow indices are already in the arena's dense chunk storage.
--
-- Reclamation: when GC advances the horizon and prunes old versions, arena
-- slots are added to a free list for reuse. Because VersionIdx dereference is
-- required to occur under a VersionArena read guard (see CONCURRENCY above),
-- GC runs under the write guard and cannot race any reader dereference.

ARENA_CHUNK := 4096  -- power-of-two recommended (fast div/mod; cache-friendly)

VersionArena := {
    chunks    : Vec<Vec<PageVersion>>, -- each chunk reserves ARENA_CHUNK and never grows beyond it
    free_list : Vec<VersionIdx>,       -- recycled slots from GC
    high_water: VersionIdx,            -- bump pointer for new allocations
}

PageLockTable := ShardedHashMap<PageNumber, TxnId>  -- exclusive write locks
    -- Sharded by PageNumber hash into N shards (N = 64 default).
    -- Each shard is a parking_lot::Mutex<HashMap<PageNumber, TxnId>>.
    -- Shard count is a power of two for fast modular arithmetic (pgno & (N-1)).
    --
    -- CONTENTION MODEL (Alien-Artifact Discipline):
    -- With W concurrent writers and S shards, the probability that at least
    -- two writers contend on the same shard follows the birthday problem:
    --   P(collision) ≈ 1 - e^(-W*(W-1) / (2*S))
    -- For S=64, W=16: P ≈ 1 - e^(-240/128) ≈ 0.85 (85% chance of at least
    -- one collision). For S=64, W=8: P ≈ 0.36. For S=64, W=4: P ≈ 0.09.
    --
    -- Under ZIPFIAN page access (which Section 17.3 uses for benchmarking),
    -- collisions are WORSE because hot pages cluster into hot shards.
    -- The effective shard count is S_eff = S / skew_factor where skew_factor
    -- depends on the Zipfian parameter s (for s=1.0, roughly 4x concentration
    -- on the top 10% of shards, so S_eff ≈ 16 for S=64).
    --
    -- The expected lock hold time per shard access is ~50ns (HashMap lookup
    -- under parking_lot::Mutex). Expected wait time when contended:
    --   E[wait] ≈ (W/S) * t_hold ≈ (16/64) * 50ns = 12.5ns (uniform)
    --   E[wait] ≈ (W/S_eff) * t_hold ≈ (16/16) * 50ns = 50ns (Zipfian)
    --
    -- S=64 is adequate for W <= 32 under uniform access, W <= 16 under
    -- Zipfian. For higher concurrency, increase S to 256 (via PRAGMA).
    -- Monitored at runtime via the BOCPD contention stream (Section 4.8).

SSIWitnessPlane := (see §5.6.4)
    -- The RaptorQ-native witness plane replaces any ephemeral SIREAD lock table:
    -- it captures read/write evidence as witness keys and publishes durable
    -- ECS objects plus a shared-memory hot index (no false negatives).

Transaction := {
    txn_id      : TxnId,
    txn_epoch   : TxnEpoch,
    snapshot    : Snapshot,
    write_set   : HashMap<PageNumber, PageVersion>, -- private versions (commit_seq = 0)
    intent_log  : Vec<IntentOp>,            -- semantic operation log for rebase merge
    page_locks  : HashSet<PageNumber>,
    state       : {Active, Committed{commit_seq}, Aborted{reason}},
    mode        : {Serialized, Concurrent},
    serialized_write_lock_held: bool,       -- true iff this txn currently holds the global write mutex

    -- Witness-plane SSI evidence (Section 5.6.4):
    read_keys   : HashSet<WitnessKey>,
    write_keys  : HashSet<WitnessKey>,

    -- SSI state (computed at commit for Concurrent mode):
    has_in_rw   : bool,    -- some other txn R read a key that this txn later wrote (R -rw-> this; incoming edge)
    has_out_rw  : bool,    -- this txn read a key that some other txn W later wrote (this -rw-> W; outgoing edge)
}

IntentOp := (see §5.10.1)

CommitIndex := ShardedHashMap<PageNumber, CommitSeq>
    -- Maps each page to the latest commit_seq that modified it.
    -- Used by First-Committer-Wins validation without scanning commit history.

CommitLog := AppendOnlyVec<CommitRecord>
    -- Ordered by commit time (CommitSeq). Append is O(1).
    -- Lookup by CommitSeq: since CommitSeq is monotonically increasing and
    --   assigned sequentially, offset = commit_seq - base_commit_seq gives
    --   direct index, O(1).
    -- GC truncates the front when all transactions below the horizon
    --   have been reclaimed, using a VecDeque or circular buffer.
    -- NOT BTreeMap: TxnIds are assigned at BEGIN and committed in arbitrary
    --   order, so a BTreeMap<TxnId, _> would not be sorted by commit time.
    --   A dense array ordered by CommitSeq is strictly superior for monotonic
    --   keys, with O(1) append and cache-friendly sequential access.

CommitRecord := {
    txn_id     : TxnId,
    commit_seq : CommitSeq,                    -- explicit for robustness after GC truncation
    pages      : SmallVec<[PageNumber; 8]>,    -- most commits touch few pages
    timestamp  : Instant,
}
```

### 5.2 Invariants

**INV-1 (Monotonicity):** TxnIds (begin ids) and `CommitSeq` (commit clock) are
strictly monotonically increasing.

```
Formal (begin ids): forall T1, T2 :
    begin(T1) happens-before begin(T2) => T1.txn_id < T2.txn_id

Formal (commit clock): forall C1, C2 :
    commit(C1) happens-before commit(C2) => commit_seq(C1) < commit_seq(C2)
```

*Enforcement:* `TxnManager::next_txn_id` is an `AtomicU64` incremented with
`fetch_add(1, SeqCst)`. Sequential consistency ordering guarantees that no two
calls return the same value, and the values are strictly increasing.
`CommitSeq` is assigned only by the commit sequencer in the serialized commit
section, so committed transactions have a strict total order.

**Native mode (marker stream):** `CommitSeq` allocation MUST be gap-free and
MUST be derived from the physical marker stream tip under the marker-append
lock (§3.5.4.1). Implementations MUST NOT allocate `CommitSeq` from an
in-memory counter that can advance without a durably appended marker record,
because a crash after allocation but before persistence would create a gap and
break O(1) marker indexing.

**Compatibility mode (WAL):** `CommitSeq` advances only after the WAL commit
record is durably published (post-fsync). The engine MAY cache the current
high-water `CommitSeq` in an `AtomicU64` for snapshot capture, but it MUST
never get ahead of the durable publication point.

*Violation consequence:* If TxnIds are reused or non-monotone, snapshot
visibility becomes undefined. A transaction could see a "future" version as
old, or fail to see a committed version. This leads to phantom reads, lost
updates, and corrupted query results.

---

**INV-2 (Lock Exclusivity):** For any page P, at most one active transaction
holds a lock: `|{T : T.state = Active AND P IN T.page_locks}| <= 1`.

```
Formal: forall P : forall T1, T2 :
    T1.state = Active AND T2.state = Active AND T1 != T2
    => NOT (P in T1.page_locks AND P in T2.page_locks)
```

*Enforcement:* `PageLockTable::try_acquire(pgno, txn_id)` performs an atomic
check-and-set under a mutex. If `pgno` is already mapped to a different
`txn_id`, the call returns `Err(SQLITE_BUSY)` without modifying the table.
The lock is only inserted if the page is unlocked or already held by the
same transaction (idempotent re-acquire).

*Violation consequence:* Two transactions simultaneously modifying the same page
would produce two conflicting `PageVersion` entries. The version chain would
have a fork (two versions with different `created_by` but the same `prev`),
breaking INV-3. The resulting page data depends on which commit runs last,
leading to lost updates.

---

**INV-3 (Version Chain Order):** If `V.prev = Some(V')`, then
`V.commit_seq > V'.commit_seq`.

```
Formal: forall P, V, V' :
    V in version_chain(P) AND V.prev = Some(V')
    => V.commit_seq > V'.commit_seq
```

*Enforcement:* Versions are published to the version store during commit, in
the order of commit (`CommitSeq` is assigned by the sequencer at commit time).
The `publish()` operation prepends the new version to the head of the chain,
setting its `prev` to the current head. Since each committed transaction has a
strictly increasing `commit_seq`, the ordering holds.

*Violation consequence:* Version resolution walks the chain from newest to
oldest, returning the first visible version. If the chain is mis-ordered,
`resolve()` might return an older version when a newer one should be visible,
or skip a version entirely. This breaks snapshot isolation.

---

**INV-4 (Write Set Consistency):** `forall P in T.write_set.keys(): P in T.page_locks`.

```
Formal: forall T, P : P in T.write_set.keys() => P in T.page_locks
```

*Enforcement:* `write_page()` acquires the page lock before inserting into the
write set. The lock acquisition is the first operation; if it fails, the write
set is not modified.

*Violation consequence:* A page in the write set without a lock means another
transaction could also write the same page (since no lock prevents it). Both
transactions would attempt to publish conflicting versions during commit,
bypassing the first-committer-wins check.

---

**INV-5 (Snapshot Stability):** A transaction's snapshot is immutable.

```
Formal: forall T : T.snapshot at time t = T.snapshot at time t' for all t' > t
    where t is the time of begin(T)
```

*Enforcement:* The `Snapshot` struct is stored by value inside the `Transaction`
struct. No mutable references to `T.snapshot` are ever created after
`capture_snapshot()` returns. The `Snapshot` type does not implement
interior mutability.

*Violation consequence:* If a snapshot changes during a transaction, reads at
different times could see different versions of the same page, breaking
the repeatable-read guarantee that snapshot isolation provides.

---

**INV-6 (Commit Atomicity):** All-or-nothing visibility.

```
Formal: forall T, S :
    if T.state = Committed then
        (forall P in T.write_set.keys(): visible(T.write_set[P], S))
        OR (forall P in T.write_set.keys(): NOT visible(T.write_set[P], S))
```

*Enforcement:* Version publishing and commit log insertion happen while the
coordinator holds the commit pipeline. All versions are published, then the
commit marker is appended (Native mode) or the WAL commit record is appended
(Compatibility mode). The marker/record is the atomic "this commit exists"
visibility point:
- Until the marker/record is durable, the commit is not considered committed,
  so none of the transaction's versions are visible.
- Once durable, the commit's versions share a single `commit_seq` and become
  visible simultaneously to any snapshot with `snapshot.high >= commit_seq`.

*Memory ordering constraint (normative):* The published `commit_seq`
high-water mark MUST be stored with `Release` ordering AFTER all version chain updates
for the committing transaction are visible (i.e., version chain head pointers
are updated with at least `Release` stores). Readers MUST load `commit_seq`
with `Acquire` ordering before traversing version chains. This ensures that
a reader who observes the new `commit_seq` value is guaranteed to see the
corresponding version chain entries. Without this ordering, a reader could
take a snapshot that includes `commit_seq = N` but traverse stale version
chains that do not yet reflect commit N's versions, violating INV-6.

*Cross-process note:* The Acquire/Release ordering above governs the in-process
buffer pool and the shared-memory `commit_seq` publication point (§5.6.1).
Cross-process *data* visibility is mode-specific:

- **Compatibility mode:** WAL frames are durable on disk before the WAL index
  (shared memory) is updated (§11.9). A reader in another process loads the WAL
  index with Acquire semantics and then reads WAL frames from the file, which
  are guaranteed to be present because the writer flushed them before updating
  the index.
- **Native mode:** Referents (capsule symbols + `CommitProof`) are made durable
  before the marker is made durable (FSYNC_1 then marker then FSYNC_2; §7.11.2).
  After the marker is durable, the sequencer publishes `shm.commit_seq` with a
  Release store; other processes capture snapshots via Acquire loads and can
  safely decode the commit’s referents via the marker stream and ECS logs.

*Violation consequence:* Partial visibility means a reader could see some of
a transaction's writes but not others, observing an inconsistent state. For
example, a transfer between two accounts might show the debit but not the
credit, temporarily "losing" money.

---

**INV-7 (Serialized Mode):** If `T.mode = Serialized`, then T holds the
global write mutex for the duration of its write operations. At most one
Serialized-mode writer holds the mutex at any time. DEFERRED (read-only)
Serialized transactions do not acquire the mutex until their first write.

```
Formal: forall T1, T2 :
    T1.serialized_write_lock_held
    AND T2.serialized_write_lock_held
    AND T1 != T2
    => FALSE
```

*Enforcement:* The global write mutex is acquired either at `BEGIN IMMEDIATE /
BEGIN EXCLUSIVE` or at the first write attempt of a DEFERRED Serialized
transaction (§5.4). Once acquired, it is held until `commit()` or `abort()`
releases it. Since Rust's `Mutex` allows at most one holder, at most one
Serialized writer can be active.

*Violation consequence:* If two Serialized writers run simultaneously,
the system no longer provides SERIALIZABLE isolation in Serialized mode. This
breaks backward compatibility with C SQLite's guarantee that writers are
serialized.

### 5.3 Visibility Predicate

```
visible(V, S) :=
    V.commit_seq != 0
    AND V.commit_seq <= S.high

resolve(P, S) :=
    first V in version_chain(P) where visible(V, S)
    // Falls back to on-disk baseline if no committed version found

resolve_for_txn(P, T) -> Option<VersionIdx> :=
    // Returns the VersionArena index of the base version for a write.
    // Used by write_page() to set PageVersion.prev_idx.
    if P in T.write_set: return T.write_set[P].prev_idx
    let V = resolve(P, T.snapshot)
    if V exists: return Some(V.arena_idx)   // arena index of the resolved version
    else: return None                       // page only exists on disk (no version chain entry)
```

**Complete worked example (commit-seq snapshots):**

Assume a database with one page `P1`. The global commit clock starts at
`commit_seq = 0` (on-disk baseline).

```
Time  Action
----  ------
t0    T1 begins   (txn_id=1, snapshot.high=0)
t1    T2 begins   (txn_id=2, snapshot.high=0)
t2    T1 writes P1 (private write_set version; not committed)
t3    T3 begins   (txn_id=3, snapshot.high=0)
t4    T1 commits  (commit_seq=1; publishes V1 with commit_seq=1)
t5    T2 writes P1 (private write_set version)
t6    T4 begins   (txn_id=4, snapshot.high=1)  -- sees V1
t7    T2 commits  -> FAILS FCW: base_version(P1).commit_seq=1 > snapshot.high=0
t8    T5 begins   (txn_id=5, snapshot.high=1)
t9    T3 writes P1 (private write_set version)
t10   T3 commits  -> FAILS FCW: base_version(P1).commit_seq=1 > snapshot.high=0
t11   T5 writes P1 (private write_set version)
t12   T5 commits  (commit_seq=2; publishes V2 with commit_seq=2)
```

What each transaction sees when reading `P1`:

- `T1` before own write: on-disk baseline; after own write: its private version.
- `T2` sees only on-disk baseline throughout (snapshot.high=0), even after `T1` commits.
- `T4` sees `V1` (snapshot.high=1).
- `T5` sees `V1` before writing; then sees its private version after writing.

### 5.4 Transaction Lifecycle

**Begin:**
```
BeginKind := {Deferred, Immediate, Exclusive, Concurrent}

begin(manager, begin_kind) -> Transaction:
    txn_id = manager.next_txn_id.fetch_add(1, SeqCst)
    txn_epoch = 0  // in-process; cross-process uses TxnSlot.epoch (Section 5.6.2)
    snapshot = Snapshot {
        high: manager.commit_seq.load(Acquire),
        schema_epoch: manager.schema_epoch.load(),
    }
    serialized_write_lock_held = false
    mode = if begin_kind == Concurrent { Concurrent } else { Serialized }
    if begin_kind == Immediate || begin_kind == Exclusive:
        // Writer-intent at BEGIN (SQLite IMMEDIATE/EXCLUSIVE semantics).
        manager.global_write_mutex.lock()
        serialized_write_lock_held = true
    return Transaction {
        txn_id, txn_epoch, snapshot, mode,
        serialized_write_lock_held,
        ...
    }
```

**Read (both modes):**
```
read_page(T, pgno) -> PageData:
    // NOTE: SSI witnesses are emitted by semantic layers (VDBE/B-tree),
    // not by raw pager reads. See §5.6.4.3.
    if pgno in T.write_set: return T.write_set[pgno].data
    return resolve(pgno, T.snapshot).data
```

**Write:**
```
write_page(manager, T, pgno, new_data) -> Result<()>:
    if T.mode == Serialized:
        // DEFERRED upgrade: if we haven't taken writer exclusion yet, take it now.
        // This preserves concurrent readers (SQLite DEFERRED behavior).
        if !T.serialized_write_lock_held:
            manager.global_write_mutex.lock()  // exact SQLite compat (writer only)
            T.serialized_write_lock_held = true
        // No page lock needed (mutex provides writer exclusion), but still track
        // in write_set for WAL append.
    else: // Concurrent mode
        lock_result = page_lock_table.try_acquire(pgno, T.txn_id)
        if lock_result = AlreadyHeld(other): return Err(SQLITE_BUSY)
        T.page_locks.insert(pgno)

        // NOTE: SSI witnesses are emitted by semantic layers (VDBE/B-tree) that
        // know which logical keys are being written. See §5.6.4.3.

    base = resolve_for_txn(pgno, T)
    T.write_set.insert(pgno, PageVersion {
        pgno,
        commit_seq: 0,
        created_by: (T.txn_id, T.txn_epoch),
        data: new_data,
        prev_idx: base,
    })
    Ok(())
```

**Commit:**
```
commit(manager, T) -> Result<()>:
    // Schema epoch check (merge safety; see §5.10).
    if current_schema_epoch() != T.snapshot.schema_epoch:
        abort(T)
        return Err(SQLITE_SCHEMA)

    if T.mode == Concurrent:
        // Step 1: SSI validation (serializable by default).
        // Witness-plane candidate discovery + refinement + pivot abort rule lives in §5.7.
        // This procedure emits `DependencyEdge` / `AbortWitness` / `CommitProof` artifacts in Native mode.
        ssi_validate_and_publish(T)?  // returns Err(SQLITE_BUSY_SNAPSHOT) if pivot

    // Merge-Retry Loop:
    // The Coordinator is the source of truth. If it rejects us with Conflict, we
    // must retry the merge logic with the authoritative conflict info it provides.
    loop:
        if T.mode == Concurrent:
            // Step 2: First-committer-wins (FCW) validation + merge policy (§5.10.4).
            // NOTE: On first pass, this uses local CommitIndex. On retry, it uses
            // the Conflict info returned by the coordinator.
            for pgno in T.write_set.keys():
                if commit_index.latest_commit_seq(pgno) > T.snapshot.high:
                    // Base drift detected: this would be an abort under strict FCW.
                    //
                    // If `PRAGMA fsqlite.write_merge = SAFE`, attempt the strict safety
                    // ladder in §5.10.4 (deterministic rebase + structured patch merge).
                    // Raw byte-disjoint XOR merge is forbidden for SQLite structured pages.
                    if !try_resolve_conflict_via_merge_policy(T, pgno):
                        abort(T)
                        return Err(SQLITE_BUSY_SNAPSHOT)  // retryable conflict

        // Step 3: Persist + publish using the selected commit protocol (Section 7).
        // The commit sequencer assigns commit_seq and appends the atomic marker/record.
        response = write_coordinator.publish(T)
        match response:
            Ok(commit_seq) =>
                T.state = Committed{commit_seq}
                release_page_locks(T)
                if T.mode == Serialized && T.serialized_write_lock_held:
                    manager.global_write_mutex.unlock()
                return Ok(())

            Conflict(pages, seq) =>
                // The coordinator saw a conflict our local index missed (stale cache).
                // Update local knowledge and RETRY the merge loop.
                commit_index.update_from_coordinator(pages, seq)
                continue // Loop back to Step 2 to attempt merge with new info

            Aborted(code) =>
                abort(T)
                return Err(code)

            IoError(e) =>
                abort(T)
                return Err(SQLITE_IOERR)
```

**Transaction state machine:**

```
                    +--------+
                    | Active |
                    +--------+
                   /          \
          commit()/            \abort() or
         succeeds              validation fails
                /                \
    +-----------+              +---------+
    | Committed |              | Aborted |
    +-----------+              +---------+

State transitions:
  Active -> Committed:  Only via successful commit validation + durable commit marker/record append
  Active -> Aborted:    Via explicit ROLLBACK, commit validation failure,
                        SQLITE_BUSY on page lock, or SQLITE_INTERRUPT
  Committed -> *:       Terminal state (no further transitions)
  Aborted -> *:         Terminal state (no further transitions)

All transitions are irreversible. A committed transaction cannot be
rolled back; an aborted transaction cannot be retried (a new transaction
must be started).
```

**Concurrent mode vs Serialized mode side-by-side:**

```
                    Serialized Mode              Concurrent Mode
                    ===============              ===============

BEGIN:              Capture snapshot             Capture snapshot
                    No mutex for DEFERRED        No global lock
                    (take global_write_mutex at
                     BEGIN IMMEDIATE/EXCLUSIVE
                     or on first write)

READ:               resolve(P, snapshot)         resolve(P, snapshot)
                    (identical)                  (identical)

WRITE:              No page lock needed          try_acquire page lock
                    (mutex provides exclusion)   Return SQLITE_BUSY if held
                    Add to write_set             Add to write_set

COMMIT:             No validation needed         SSI check: abort if pivot
                    (mutex ensures serial)       First-committer-wins check
                    WAL append                   FCW check + merge ladder (§5.10)
                    Release global_write_mutex   WAL append
                    (if held)                    Release page locks

ABORT:              Release global_write_mutex   Release all page locks
                    (if held)
                    Discard write_set            Discard write_set
                                                 Witness evidence is monotonic; aborted
                                                 witnesses are ignored and later GC'd
                                                 by safe horizons (§5.6.4.8)

CONCURRENCY:        One writer at a time         Multiple writers in parallel
                    (exact SQLite behavior)      (conflict on same page only)

ISOLATION:          SERIALIZABLE                 SERIALIZABLE (Page-SSI)
                    (trivially, by serializing)  (conservative rw-antidependency
                                                  tracking; write skew prevented)
                                                 PRAGMA fsqlite.serializable=OFF
                                                  downgrades to SI (opt-in only)

USE CASE:           DROP-in SQLite replacement   Applications that opt in
                    Legacy applications          to concurrent writes
```

**How savepoints interact with MVCC:**

Savepoints are a B-tree-level mechanism, NOT an MVCC-level mechanism. The
MVCC layer does not know about savepoints. Here is why:

- `SAVEPOINT name` records the current state of the B-tree modifications
  (specifically, the set of pages in the write set and their pre-modification
  data).
- `ROLLBACK TO name` undoes B-tree modifications back to the savepoint by
  restoring the recorded page states within the write set.
- `RELEASE name` discards the savepoint record.

All of this happens within a single MVCC transaction. The transaction's
`txn_id`, `snapshot`, and `page_locks` are unaffected by savepoint operations.
Page locks acquired after a savepoint are NOT released on `ROLLBACK TO` --
they are held until the enclosing transaction commits or aborts. This is
because releasing a page lock mid-transaction would allow another transaction
to acquire it, potentially violating first-committer-wins when the outer
transaction later tries to re-write the page.

**SSI witness interaction:** Similarly, SSI witness keys (read/write
registrations in the hot plane) are NOT rolled back on `ROLLBACK TO`. Once a
read or write is registered in the `HotWitnessIndex` bitset, it remains set
for the lifetime of the enclosing transaction. This is a safe overapproximation:
retaining stale witness entries can only increase false positive aborts, never
cause false negatives (missed anomalies). Rolling back witnesses would risk
missing a genuine rw-antidependency if the transaction later re-reads or
re-writes the same pages.

### 5.5 Safety Proofs

**Theorem 1 (Deadlock Freedom):** The MVCC system is deadlock-free.

**Proof:** A deadlock requires a cycle in the wait-for graph. Our system has
no wait-for graph because `try_acquire()` never blocks -- it returns
`Err(SQLITE_BUSY)` immediately if the lock is held by another transaction.
Since no transaction ever waits for another transaction to release a lock,
no cycle can form. QED.

**Structural guarantee:** This is not a detection-based approach (like timeout
or cycle detection in a wait-for graph). Deadlocks are *structurally impossible*
because the `try_acquire` operation is non-blocking by construction.

---

**Theorem 2 (Snapshot Isolation in Concurrent Mode):** Every Concurrent-mode
transaction observes a consistent snapshot -- it never sees partial results
of concurrent transactions.

**Proof:** Let `T_r` be a reading transaction with snapshot `S_r` and
`S_r.high = h`. Let `T_w` be any other transaction that commits at
`commit_seq(T_w) = c`.

All versions produced by `T_w` share the same `commit_seq = c` (commit assigns
a single sequence number for the transaction and publishes all its page
versions under that number).

By the visibility predicate (§5.3), for any version `V_i` produced by `T_w`:

```
visible(V_i, S_r) = (c != 0) AND (c <= h)
```

This condition is identical for every `V_i` from `T_w`, so `T_r` sees either
ALL of `T_w`'s committed writes or NONE of them. Since `S_r` is immutable
(INV-5), this visibility decision does not change during `T_r`'s lifetime.

QED.

---

**Theorem 3 (No Lost Updates / FCW Safety):** If two Concurrent-mode
transactions `T_1` and `T_2` both attempt to write the same page `P`, then the
system either:
1. aborts one transaction, or
2. commits both via a deterministic merge/rebase such that the final state is
   equivalent to some serial order.

**Proof:** We consider two exhaustive sub-cases based on the temporal ordering
of their page lock acquisitions.

**Case A (Concurrent lock contention):** `T_1` and `T_2` both attempt
`write_page(P)` while both are Active. Without loss of generality, suppose
`T_1` calls `try_acquire(P)` first and succeeds. When `T_2` subsequently
calls `try_acquire(P)`, it finds the lock held by `T_1` and receives
`Err(SQLITE_BUSY)`. `T_2` cannot write `P` at all and therefore cannot commit a
conflicting write to `P`.

**Case B (Sequential writes + snapshot conflict):** `T_1` acquires the lock on
`P`, writes `P`, and commits first, releasing the lock. `T_2` then acquires the
lock on `P` (now free) and writes `P`. Let `commit_seq(T_1) = c1` and let
`T_2.snapshot.high = h2` (captured at `BEGIN`).

- If `c1 <= h2`, then `T_2`'s snapshot already includes `T_1`'s commit. `T_2`
  is effectively writing after `T_1` in serial order, so committing `T_2` does
  not lose `T_1`'s update.
- If `c1 > h2`, then `T_1` committed after `T_2`'s snapshot. The First-Committer-Wins
  check detects `commit_index[P] = c1 > h2` and therefore requires either:
  - a deterministic rebase/merge (§5.10) that incorporates `T_1`'s committed
    state into `T_2`'s final deltas, or
  - abort/retry of `T_2`.

In all cases, the system prevents "last writer wins" lost updates: either one
transaction aborts, or both commit with an explicitly justified merge that is
equivalent to serial execution.

QED.

---

**Theorem 4 (GC Safety):** Garbage collection never removes a version that
any active or future transaction could need.

**Proof:** Define the safe GC horizon in commit-seq space:

```
safe_gc_seq := min(T.snapshot.high for all active transactions T)
if no active transactions: safe_gc_seq := latest_commit_seq
```

Because `CommitSeq` is monotonic and snapshots are immutable (INV-5),
`safe_gc_seq` is a correct global lower bound: every active transaction has
`snapshot.high >= safe_gc_seq`.

**Reclaimability predicate:** A committed version `V` of page `P` with
`V.commit_seq = c` is reclaimable iff there exists a newer committed version
`V'` in the version chain such that:

```
c < V'.commit_seq <= safe_gc_seq
```

That is: `V'` is at least as new as the newest version that the oldest active
snapshot could ever see.

For any active transaction `T_a`, since `V'.commit_seq <= safe_gc_seq <= T_a.snapshot.high`,
`visible(V', T_a.snapshot)` is true (§5.3). Because the version chain is ordered
by descending `commit_seq` (INV-3), `resolve(P, T_a.snapshot)` returns `V'` or a
newer version, never `V`. Thus no active transaction can need `V`.

For any future transaction `T_f`, `T_f.snapshot.high >= latest_commit_seq >= safe_gc_seq`,
so the same argument applies: `V'` is visible and dominates `V` in the chain.

Therefore, reclaiming `V` cannot affect the result of `resolve(P, S)` for any
active or future snapshot.

QED.

---

**Theorem 5 (Memory Boundedness):** Under steady-state load with maximum
transaction duration `D` and commit rate `R` (commits per second), the
maximum number of retained versions per page is bounded by `R * D + 1`.

**Proof:** Define `safe_gc_seq = min(active snapshot.high)` (Theorem 4).
Under steady state, the oldest active transaction began at most `D` seconds
ago, so `safe_gc_seq` lags the head of the commit clock by at most `R * D`
commits (in `D` seconds, at most `R * D` commits can occur).

Each committed transaction can create at most one version per page. Therefore
the version chain for any page contains at most `R * D` versions with
`commit_seq > safe_gc_seq`, plus one version at or below the horizon (the
newest version visible to the oldest active snapshot). All older versions are
reclaimable by GC Safety (Theorem 4). Total retained versions per page:
`R * D + 1`. QED.

**Practical implication (example numbers):** With `D = 5s` (configured maximum
active snapshot duration; `PRAGMA fsqlite.txn_max_duration_ms`) and
`R = 1000 commits/s`, at most 5001 versions per page. At 4KB per full-page
version, this is ~20MB per extremely hot page. In practice:
- most transactions touch a small subset of pages,
- older history is stored as patches/intent logs (§5.10.6), not full images,
- GC prunes aggressively once the horizon advances.

**Alien-artifact correction (required):** `D` is not a "nice-to-have estimate".
It is a contractual bound on how long the oldest active snapshot can hold the
GC horizon back. If `D` is unbounded, memory boundedness is unprovable.

Therefore:
- The engine MUST define a configured `txn_max_duration` for Concurrent mode
  (and any mode that retains MVCC history), and it MUST enforce it by aborting
  transactions that exceed it.
- The default `txn_max_duration` SHOULD be derived from survival analysis of
  transaction durations (Kaplan-Meier with right censoring) and updated only on
  BOCPD regime shifts, with evidence-ledger justification (§4.17).

**Caveat (non-steady-state):** Theorem 5 assumes constant `R` and bounded `D`.
Under burst workloads (high `R`) or under a policy that permits larger `D`, the
bound grows proportionally. If version chain length exceeds the configured
threshold, the adaptive GC controller (§5.6.3) increases GC frequency and the
PolicyController MAY tighten `txn_max_duration` (never loosen under active
memory pressure).

**Deriving `txn_max_duration` (survival analysis, recommended):**

`txn_max_duration` is a policy knob that SHOULD be derived from measured
transaction durations, per BOCPD regime.

1. Record `duration_ms = end_time - begin_time` for each completed transaction.
2. Treat active transactions at the end of the observation window as
   right-censored samples (they survived at least `now - begin_time`).
3. Maintain a Kaplan-Meier estimator `S(t) = P(duration > t)` for the current
   regime (reset on BOCPD change-point).
4. Choose `txn_max_duration` as a high quantile of the survival function
   (e.g., `Q_0.999`), plus a fixed safety margin; clamp to operator limits.

This adapts to workloads with heavy-tailed durations without guessing a single
global constant.

---

**Theorem 6 (Liveness):** Every transaction either commits or aborts in
finite time, assuming:
(a) The application eventually calls COMMIT or ROLLBACK for every transaction.
(b) The write coordinator processes requests in finite time.
(c) Durable commit I/O completes in finite time (WAL I/O in Compatibility mode;
    symbol-log + marker I/O in Native mode).

**Proof:** We show that every transaction makes progress through its lifecycle
without unbounded blocking.

**Begin:** `fetch_add` on an `AtomicU64` completes in O(1). Snapshot capture is
O(1): it reads the current `commit_seq` and `schema_epoch` and stores them in
the immutable `Snapshot` (INV-5). For `BeginKind::Immediate` / `Exclusive`,
acquiring `global_write_mutex` may wait, but only for the duration of another
Serialized writer, which by inductive hypothesis completes in finite time. For
`BeginKind::Deferred`, no mutex is acquired at BEGIN; the mutex MAY be acquired
later at the first write attempt under the same bound.

**Read:** `resolve()` walks the version chain, which has bounded length
(Theorem 5). Each visibility check is O(1) (`commit_seq <= snapshot.high`). Total
time is bounded.

**Write:** In Concurrent mode, `try_acquire` is non-blocking (returns immediately
with `Ok` or `Err`). Copy-on-write is O(page_size). In Serialized mode, the first
write in a DEFERRED transaction may acquire `global_write_mutex` (upgrade); this
wait is bounded as in Begin. Total time is bounded.

**Commit (Concurrent mode):** Commit-time checks are bounded:
- SSI witness-plane candidate discovery is O(#buckets + #candidates) (hot index
  lookups + optional refinement; §5.7).
- First-committer-wins checks consult `CommitIndex` in O(write_set_size).
Durability completes in finite time (assumption c) and the sequencer publishes
the atomic commit marker/record. Lock release is O(page_locks_size). Total time
is bounded.

**Commit (Serialized mode):** No SSI/FCW validation is needed (serialization
prevents concurrent writers). Durability and publication are the same atomic
marker/record append as above. Mutex release is O(1). Total time is bounded.

**Abort:** Discard write set O(write_set_size), release locks O(page_locks_size).
Total time is bounded.

Therefore, every transaction that begins will eventually reach either the
Committed or Aborted terminal state, assuming the application and I/O
subsystem cooperate. QED.

### 5.6 Multi-Process Semantics

FrankenSQLite provides MVCC concurrency both within a single process (via
in-memory lock tables and version chains) and across processes (via a
shared-memory coordination region). The in-process path is the fast path;
the cross-process path adds ~100ns per lock operation due to mmap-based
atomics.

**Architecture:** Each database file `foo.db` (or `foo.db.fsqlite/` in Native
mode) has an associated shared-memory file `foo.db.fsqlite-shm` that
provides the cross-process coordination plane. This is analogous to SQLite's
WAL-index shared memory but extended for MVCC.

#### 5.6.1 Shared-Memory Coordination Region

The shared-memory file is structured as a fixed-size header followed by
an array of TxnSlots:

```
SharedMemoryLayout := {
    magic            : [u8; 8],        -- "FSQLSHM\0"
    version          : u32,            -- layout version (1)
    page_size        : u32,            -- database page size
    max_txn_slots    : u32,            -- capacity of TxnSlot array (default: 256)
                                       -- Derivation: 256 = max_processes * max_concurrent_txn_per_process.
                                       -- Typical: 16 processes * 16 concurrent queries = 256 slots.
                                       -- Memory cost: 256 * sizeof(TxnSlot) ≈ 256 * 128B = 32KB.
                                       -- Exceeding capacity returns SQLITE_BUSY (not silent failure).
    next_txn_id      : AtomicU64,      -- global TxnId counter (fetch_add)
    commit_seq       : AtomicU64,      -- published commit_seq high-water mark (latest DURABLE commit)
                                       -- NOTE: This is NOT a commit_seq allocator.
                                       -- Native mode: advanced by the marker sequencer only AFTER the
                                       -- marker record is durably appended (§7.11.2) and MUST match the
                                       -- physical marker stream tip (§3.5.4.1). It MUST NOT get ahead of
                                       -- the marker stream (no gaps).
    schema_epoch     : AtomicU64,      -- monotonic schema epoch (mirror of RootManifest.schema_epoch)
    ecs_epoch        : AtomicU64,      -- monotonic ECS coordination epoch (mirror of RootManifest.ecs_epoch)
    gc_horizon       : AtomicU64,      -- safe GC horizon commit_seq (min active begin_seq) across all processes
    lock_table_offset: u64,            -- byte offset to PageLockTable region
    witness_offset   : u64,            -- byte offset to SSI witness plane (HotWitnessIndex)
    txn_slot_offset  : u64,            -- byte offset to TxnSlot array
    checksum         : u64,            -- xxhash3 of header fields
    _padding         : [u8; 64],       -- align to cache line
    // --- TxnSlot array follows at txn_slot_offset ---
    // --- PageLockTable region follows at lock_table_offset ---
    // --- SSI witness plane follows at witness_offset ---
}
```

The shared-memory file is created on first access and mapped by every
process that opens the database. All fields after the header use atomic
operations.

**Memory ordering (normative):**
- `commit_seq` stores MUST use `Release` ordering at the commit publication
  point; `commit_seq` loads for snapshot capture MUST use `Acquire` ordering.
- Other fields MAY use `SeqCst` when simplicity is worth the cost; otherwise
  use `Acquire/Release` where required by invariants and `Relaxed` only for
  diagnostics counters.

**Initialization and reconciliation (normative):**
- On database open, implementations MUST set `shm.commit_seq` to the current
  durable commit clock tip:
  - **Native mode:** the physical marker stream tip (§3.5.4.1).
  - **Compatibility mode:** the durable WAL-visible state (WAL index + frames).
- On database open (native mode), implementations MUST set `shm.ecs_epoch` from
  `RootManifest.ecs_epoch` so cross-process services share the same epoch-scoped
  remote/key policy configuration (§4.18). Loads/stores MUST use Acquire/Release.
- If the shared-memory file already exists, implementations MUST reconcile
  `shm.commit_seq` against the durable tip and MUST NOT allow `shm.commit_seq`
  to remain ahead of durable reality (that would make snapshots reference
  non-existent commits).

#### 5.6.2 TxnSlot: Per-Transaction Cross-Process State

```
TxnSlot := {
    txn_id          : AtomicU64,     -- 0 = slot is free
    txn_epoch       : AtomicU32,     -- increments when the slot is acquired (prevents stale slot-id interpretation)
    pid             : AtomicU32,     -- owning process ID
    pid_birth       : AtomicU64,     -- process "birth" identifier to prevent PID reuse bugs during cleanup
                                   -- Unix: process start time (platform-specific monotonic ticks)
                                   -- Windows: process creation time
    lease_expiry    : AtomicU64,     -- Unix timestamp (seconds) of lease expiry
    begin_seq       : AtomicU64,     -- CommitSeq observed at BEGIN (snapshot backbone for SSI overlap)
    commit_seq      : AtomicU64,     -- CommitSeq when committed; 0 if not committed
    snapshot_high   : AtomicU64,     -- snapshot high commit sequence; equals begin_seq for commit-seq snapshots.
                                   -- Intentionally redundant with begin_seq: this field exists for debug/audit
                                   -- diagnostics (inspect snapshot boundaries via shm tools without deriving
                                   -- from begin_seq). Implementations MUST populate it from the same
                                   -- commit_seq.load(Acquire) used for begin_seq to avoid GC safety issues.
    state           : AtomicU8,      -- 0=Free, 1=Active, 2=Committing, 3=Committed, 4=Aborted
    mode            : AtomicU8,      -- 0=Serialized, 1=Concurrent
    witness_epoch   : AtomicU32,     -- witness-plane epoch pinned at BEGIN CONCURRENT (§5.6.4.8)
    has_in_rw       : AtomicBool,    -- SSI: has incoming rw-antidependency
    has_out_rw      : AtomicBool,    -- SSI: has outgoing rw-antidependency
    marked_for_abort: AtomicBool,    -- SSI: eager pivot abort signal (optimization)
    write_set_pages : AtomicU32,     -- count of pages in write set (for GC sizing)
    claiming_timestamp: AtomicU64,   -- unix timestamp when Phase 1 CAS set TXN_ID_CLAIMING;
                                   -- used by cleanup to detect stuck CLAIMING slots (§5.6.2).
                                   -- Written AFTER the successful Phase 1 CAS; zeroed when slot is freed.
    _padding        : [u8; 48],      -- pad to 128 bytes (two cache lines; prevents false sharing between adjacent slots)
                                   -- Layout: 80 bytes of fields with repr(C) alignment (2B gap after mode
                                   -- for witness_epoch align, 1B gap after marked_for_abort for
                                   -- write_set_pages align) + 48B padding = 128B total.
}
```

**Platform requirement (normative):** Concurrent mode relies on 64-bit atomic
operations in shared memory (`AtomicU64` in the `FSQLSHM` header/TxnSlots). This
requires a target that supports 64-bit atomics (`cfg(target_has_atomic = "64")`
in Rust, or equivalent). If 64-bit atomics are unavailable, `BEGIN CONCURRENT`
MUST be rejected (or not compiled), and only Serialized mode is supported.

**Slot lifecycle:**
1. **Acquire (atomic, TOCTOU-safe):** Process scans TxnSlot array for a slot
   with `txn_id == 0`. The acquisition is a **two-phase protocol**:

   **Phase 1 (claim):** CAS `txn_id` from 0 to a **sentinel value**
   `TXN_ID_CLAIMING := u64::MAX`. This is an atomic claim that prevents
   other processes from racing on the same slot. If the CAS fails, try the
   next slot.

   **Reserved sentinels (normative):**
   - `TXN_ID_CLAIMING := u64::MAX` means "slot is claimed but not yet published".
   - `TXN_ID_CLEANING := u64::MAX - 1` means "cleanup owns this slot and is
     resetting fields"; acquisitions MUST treat it as non-free.
   These sentinel values are never valid TxnIds.

   **Phase 2 (initialize):** With the slot exclusively claimed (no other
   process can acquire it because `txn_id != 0`), initialize all fields.
   **Required ordering:** fields that scanners rely on (`begin_seq`,
   `snapshot_high`, `mode`, `state`) MUST be initialized before Phase 3 publish.
   In particular, `begin_seq`/`snapshot_high` MUST be set before the slot can
   influence GC and witness-epoch advancement decisions (§5.6.4.8, §5.6.5).

   Minimum required initialization:
   - increment `txn_epoch` (wrap permitted),
   - `snap = shm.commit_seq.load(Acquire)`,
   - set `begin_seq = snap` and `snapshot_high = snap` (from the SAME `snap`),
   - set `mode` (Serialized or Concurrent) for this transaction,
   - clear `commit_seq = 0`, clear SSI flags/counters (`has_in_rw/has_out_rw/marked_for_abort/write_set_pages = 0`),
   - set `pid`, `pid_birth`, `lease_expiry`, and `state = Active`.
   If `mode == Concurrent`, set `witness_epoch = HotWitnessIndex.epoch.load(Acquire)` so all
   witness-plane registrations for the transaction are pinned to a single epoch
   generation (prevents reader-induced epoch livelock; §5.6.4.8).

   **Phase 3 (publish):** Store the real TxnId into `txn_id` with Release
   ordering. Only after this store is the slot visible to other processes as
   a live transaction.

   **Why sentinel:** Without the two-phase protocol, there is a TOCTOU window
   between the CAS(0 → real_txn_id) and the field initialization. During this
   window, cleanup_orphaned_slots() or another reader could observe a slot
   with a valid txn_id but uninitialized begin_seq / pid / lease_expiry,
   leading to incorrect cleanup decisions or stale snapshot computations.
2. **Renew lease:** While active, process periodically updates `lease_expiry`
   to `now + LEASE_DURATION` (default: 30 seconds). This is a simple
   atomic store.

   **Derivation of LEASE_DURATION (correctness):** The TxnSlot lease is a
   crash-detection heartbeat, not a transaction deadline. Healthy processes
   renew leases periodically; long transactions are safe as long as renewals
   continue. Therefore LEASE sizing does NOT depend on transaction duration.

   LEASE trades off:
   - shorter LEASE: faster crash cleanup (orphaned slots cleared sooner)
   - longer LEASE: lower risk of false-orphan detection during pauses
     (scheduler stalls, stop-the-world events, overload)

   **Recommended sizing (alien-artifact):**
   - define a renewal cadence `renew_every = LEASE/3` (runtime timer; deterministic
     in lab),
   - measure the distribution of *renewal gaps* (time between successful
     renewals) per BOCPD regime,
   - set `LEASE_DURATION` to a high quantile of that gap distribution plus a
     safety margin (e.g., p99.999 + 2s), so healthy processes do not expire.

   Adjustable via `PRAGMA fsqlite.txn_lease_seconds`.

   **Separately (memory boundedness):** Theorem 5 depends on a bound `D` on how
   long the *oldest active snapshot* can remain active. This is a different
   knob (`PRAGMA fsqlite.txn_max_duration_ms`, default derived from survival
   analysis of transaction durations; §5.5). LEASE does not enforce `D` by
   itself; it only enables crash cleanup.
3. **Commit/Abort:** Set `state` to Committed or Aborted. Release page locks.
   On commit: set `commit_seq = assigned_commit_seq`. For Concurrent-mode
   transactions with SSI enabled, insert a `CommittedReaderEntry` into the
   `RecentlyCommittedReadersIndex` (§5.6.2.1) BEFORE freeing the slot.

   **Freeing discipline (normative):** Before setting `txn_id = 0`, the owner
   MUST clear snapshot/epoch fields so a future claimer cannot transiently expose
   stale values under `TXN_ID_CLAIMING`:
   - `begin_seq = 0`, `snapshot_high = 0`, `witness_epoch = 0`,
   - clear SSI flags/counters (`has_in_rw/has_out_rw/marked_for_abort/write_set_pages = 0`),
   - set `state = Free` and clear/zero other metadata as desired (pid/lease, etc.).
   The `txn_id.store(0, Release)` MUST be the final write that publishes the slot
   as free.
   (The next acquirer increments `txn_epoch`, so stale slot references are rejected.)

**Lease-based crash cleanup:** If a process crashes, its TxnSlots become
orphaned (lease expires, and the owning process is no longer alive). Any process can detect
this and clean up:

```
const CLAIMING_TIMEOUT_SECS: u64 = 5;  // no valid Phase 1->Phase 2 takes 5s

cleanup_orphaned_slots():
    now = unix_timestamp()
    for slot in txn_slots:
        if slot.txn_id == TXN_ID_CLEANING:
            // Another process is resetting this slot. If it crashed mid-reset,
            // the slot can become permanently stuck. Treat this like CLAIMING:
            // if CLEANING persists beyond the timeout, reclaim and free.
            if slot.claiming_timestamp == 0:
                slot.claiming_timestamp.CAS(0, now)
                continue
            if now - slot.claiming_timestamp > CLAIMING_TIMEOUT_SECS:
                // Best-effort reclaim: clear fields again and free the slot.
                slot.state = Free
                slot.mode = Serialized
                slot.begin_seq = 0
                slot.snapshot_high = 0
                slot.witness_epoch = 0
                slot.has_in_rw = false
                slot.has_out_rw = false
                slot.marked_for_abort = false
                slot.write_set_pages = 0
                slot.pid = 0
                slot.pid_birth = 0
                slot.lease_expiry = 0
                slot.claiming_timestamp = 0
                slot.txn_id = 0  // Free the slot (Release ordering, LAST)
            continue
        if slot.txn_id == TXN_ID_CLAIMING:
            // Slot is being claimed by another process (Phase 1 of acquire).
            //
            // CRITICAL: If a process crashes between Phase 1 (CAS 0 ->
            // TXN_ID_CLAIMING) and Phase 2 (write pid/lease_expiry), the
            // slot's pid/pid_birth/lease_expiry fields are STALE (they
            // belong to the previous occupant, or are zero for a fresh slot).
            // We MUST NOT rely on those fields for CLAIMING-state cleanup.
            //
            // Instead, use a dedicated timeout: if the slot has been in
            // CLAIMING state for longer than CLAIMING_TIMEOUT_SECS, the
            // claimer is presumed dead. 5 seconds is orders of magnitude
            // longer than any valid Phase 1 -> Phase 2 transition (~μs).
            if slot.claiming_timestamp == 0:
                // The claimer writes claiming_timestamp after the CAS (§5.6.2).
                // If it crashed immediately after claiming, the timestamp may
                // still be 0. Seed the timeout clock without touching other fields.
                slot.claiming_timestamp.CAS(0, now)
                continue
            if now - slot.claiming_timestamp > CLAIMING_TIMEOUT_SECS:
                // Transition to CLEANING before clearing fields so we do not race
                // with a new claimer that could otherwise observe/clobber state.
                if slot.txn_id.CAS(TXN_ID_CLAIMING, TXN_ID_CLEANING):
                    // Clear snapshot/epoch fields as well: a future claimer must not
                    // observe stale begin_seq/witness_epoch under TXN_ID_CLAIMING (§5.6.5, §5.6.4.8).
                    slot.state = Free
                    slot.mode = Serialized
                    slot.begin_seq = 0
                    slot.snapshot_high = 0
                    slot.witness_epoch = 0
                    slot.has_in_rw = false
                    slot.has_out_rw = false
                    slot.marked_for_abort = false
                    slot.write_set_pages = 0
                    slot.pid = 0
                    slot.pid_birth = 0
                    slot.lease_expiry = 0
                    slot.claiming_timestamp = 0
                    slot.txn_id = 0  // Free the slot (Release ordering, LAST)
                continue  // skip the lease/liveness check — fields are stale
            else:
                continue  // CLAIMING recently; give the claimer time
        if slot.txn_id != 0 AND slot.lease_expiry < now:
            // Lease expired -- check whether the owning process still exists.
            // IMPORTANT: PID reuse is real; liveness checks MUST defend against it.
            if !process_alive(slot.pid, slot.pid_birth):
                // Process crashed. Abort its transaction.
                // ATOMICITY: CAS txn_id from current value to 0 to prevent
                // double-cleanup if two processes race. If CAS fails, another
                // process already cleaned this slot — skip it.
                old_txn_id = slot.txn_id.load()
                if !slot.txn_id.CAS(old_txn_id, TXN_ID_CLAIMING):
                    continue  // someone else is cleaning this slot
                release_page_locks_for(old_txn_id)
                slot.state = Free
                slot.mode = Serialized
                slot.commit_seq = 0
                slot.begin_seq = 0
                slot.snapshot_high = 0
                slot.witness_epoch = 0
                slot.has_in_rw = false
                slot.has_out_rw = false
                slot.marked_for_abort = false
                slot.write_set_pages = 0
                slot.pid = 0             // Zero stale metadata to prevent
                slot.pid_birth = 0       // future CLAIMING-crash scenarios
                slot.lease_expiry = 0    // from reading ghost values.
                slot.claiming_timestamp = 0
                slot.txn_id = 0    // Free the slot (Release ordering, LAST)
```

The three-phase acquire protocol (Phase 1) MUST set `claiming_timestamp`
**after** the successful CAS, not before:

```
// Phase 1: claim the slot
if !slot.txn_id.CAS(0, TXN_ID_CLAIMING):
    continue  // slot taken by another process
// CAS succeeded — we exclusively own this slot now.
slot.claiming_timestamp = unix_timestamp()  // set AFTER CAS
// Phase 2: initialize fields (pid, pid_birth, lease_expiry, etc.)
// Phase 3: publish real TxnId
```

**Rationale:** Writing `claiming_timestamp` before the CAS is a race: if the
CAS fails (slot already claimed by another process), we have corrupted that
process's `claiming_timestamp`. Since the CAS establishes exclusive ownership,
all field writes (including `claiming_timestamp`) MUST occur after the CAS
succeeds.

`process_alive(pid, pid_birth)` is platform-specific:
- **MUST return `false` immediately if `pid == 0`.** On Unix, `kill(0, 0)`
  signals the calling process's entire process group (POSIX §3.3.2), not
  PID 0. It returns success because the group exists, which would
  incorrectly prevent cleanup of zero-initialized or freed slots.
- Unix: use `kill(pid, 0)` to check existence (treat `EPERM` as "alive"), AND
  verify the process start time matches `pid_birth` (prevents PID reuse bugs).
- Windows: check process handle liveness and creation time.

#### 5.6.2.1 Recently Committed Readers (SSI Incoming Edge Coverage)

**Problem (normative):** The SSI incoming-edge discovery procedure (§5.7.3
step 3) uses the hot plane (`HotWitnessIndex` bitsets intersected with
`active_slots_bitset`) to find readers that read keys the committing
transaction wrote. Once a reader `R` commits and frees its TxnSlot
(`txn_id = 0`), `R` is invisible in the hot plane. If `R` read a key that
the committing transaction `T` later wrote, the rw-antidependency edge
`R -rw-> T` goes undetected. This can:

- suppress `T.has_in_rw` (making a pivot commit possible when it should abort), and/or
- suppress the **T3 rule** for a dangerous structure `X -rw-> R -rw-> T` when `R`
  is the pivot but has already committed (and thus cannot be aborted).

This is the symmetric counterpart to the outgoing-edge gap (committed writers
invisible in the hot plane), which is solved by consulting the `commit_index`.
PostgreSQL SSI solves the incoming-edge problem by retaining SIREAD locks for
committed transactions until all concurrent transactions have finished.

**Solution (normative):** FrankenSQLite MUST maintain a
`RecentlyCommittedReadersIndex` that retains committed transactions' SSI read
evidence until it is safe to discard.

```
RecentlyCommittedReadersIndex := {
    entries: Vec<CommittedReaderEntry>,
    gc_horizon: CommitSeq,             -- entries with commit_seq <= gc_horizon are prunable
}

CommittedReaderEntry := {
    txn_id      : TxnId,
    begin_seq   : CommitSeq,           -- snapshot.high at BEGIN
    commit_seq  : CommitSeq,           -- assigned at commit
    has_in_rw   : bool,                -- SSI incoming flag at commit time
    read_witness_summary : WitnessPageSet,  -- pages (or witness keys) read by this txn
}

WitnessPageSet := RoaringBitmap<u32>   -- page numbers; sound superset of keys read
```

**Lifecycle:**

1. **On commit:** After a Concurrent-mode transaction `R` passes SSI validation
   and commits, the engine MUST insert a `CommittedReaderEntry` into the index
   BEFORE freeing `R`'s TxnSlot. The entry captures `R`'s read witness summary
   (a page-level bitmap, sufficient for incoming-edge discovery; cell/byte-range
   refinement uses the cold plane) and `R`'s SSI flags at commit time.

2. **During incoming-edge discovery:** `discover_incoming_edges(T, write_wits)`
   MUST, in addition to querying the hot plane, scan the
   `RecentlyCommittedReadersIndex` for entries where:
   - `entry.commit_seq > T.begin_seq` (R committed after T's snapshot), AND
   - `entry.read_witness_summary` overlaps with T's write set pages.
   Each matching entry produces a candidate incoming edge `R -rw-> T`. If
   `entry.has_in_rw` is true, then allowing `T` to commit would complete a
   dangerous structure where the pivot (`R`) is already committed and cannot be
   aborted. Therefore `T` MUST abort with `SQLITE_BUSY_SNAPSHOT` (the T3 rule
   for committed pivots; §5.7.3 step 6).

3. **GC:** An entry is safe to prune when no active transaction has
   `begin_seq <= entry.begin_seq`. Equivalently, when the oldest active
   snapshot's `high >= entry.commit_seq`, any future committer's incoming-edge
   check cannot produce an edge with `entry` (the committer's snapshot already
   includes `entry`'s commit, so `entry` is not concurrent). The GC horizon
   tracks `min(active snapshot.high)` and prunes entries whose `commit_seq`
   is at or below it.

**Cross-process (shared memory):** The index MUST be accessible to all
processes. In multi-process deployments, it resides in the
`foo.db.fsqlite-shm` shared memory region as a fixed-capacity circular buffer
of `CommittedReaderEntry` records. Overflow beyond capacity forces the oldest
entries to be pruned early; if this risks correctness (pruned entries'
`commit_seq > min(active snapshot.high)`), the engine MUST fall back to
aborting the committing transaction with `SQLITE_BUSY_SNAPSHOT` rather than
allowing a potential false negative.

**Memory bound:** Under steady state with commit rate `R` and maximum
transaction duration `D`, the index holds at most `R * D` entries (same
bound as version chain length; Theorem 5). In shared memory this MUST be
implemented as a fixed byte-capacity ring (records are eviction-prone by design):
if inserting a new `CommittedReaderEntry` would exceed capacity (or would force
eviction of entries that are still required for correctness), the engine MUST
abort the committing transaction with `SQLITE_BUSY_SNAPSHOT` rather than risk a
false negative.

#### 5.6.3 Cross-Process Page Lock Table

The shared-memory PageLockTable is a fixed-size hash table (not the
in-process sharded HashMap). It uses open addressing with linear probing:

```
SharedPageLockTable := {
    capacity            : u32,        -- power-of-2 (default: 65536)
    rebuild_pid         : AtomicU32,  -- 0 = no rebuild in progress
    rebuild_pid_birth   : AtomicU64,  -- used to defend against PID reuse
    rebuild_lease_expiry: AtomicU64,  -- unix timestamp (seconds); same semantics as TxnSlot lease
    rebuild_epoch       : AtomicU32,  -- increments on successful rebuild (debug + stale detection)
    entries             : [PageLockEntry; capacity],
}

PageLockEntry := {
    page_number : AtomicU32,         -- 0 = empty slot, else page number
    owner_txn   : AtomicU64,         -- TxnId that holds the exclusive lock (0 = unlocked)
}
```

**Representation notes (normative):**
- `page_number == 0` means "empty slot".
- `owner_txn == 0` means "not currently locked".
- **Key stability (normative):** `page_number` MUST NOT be deleted/tombstoned as
  part of normal `release()`. Keys are cleared only during a **lock-quiescent**
  rebuild (§5.6.3.1), i.e., after freezing new acquisitions and draining all
  outstanding lock holders. This avoids key-deletion races in a lock-free
  linear-probing table where `(page_number, owner_txn)` are separate atomics.

**Acquire (linear probing with atomic insertion):**

0. If the table is under rebuild (`rebuild_lease_expiry >= now` AND owner is alive),
   `try_acquire` MUST fail **immediately** with `SQLITE_BUSY`. It MUST NOT
   block/spin inside the lock-table implementation (deadlock freedom; §5.5
   Theorem 1). `release` MUST continue to function during rebuild drain (see
   §5.6.3.1).
   **Liveness rule (normative):** If a transaction currently holds *any* page
   locks and observes `SQLITE_BUSY` due to rebuild, it MUST abort/rollback
   (releasing its page locks) rather than waiting, so the rebuild can drain to
   lock-quiescence. Busy-timeout waiting, if enabled, MUST occur only when the
   caller holds no page locks.
1. Start at `idx = hash(page_number) & (capacity - 1)`.
2. Probe:
   - If `entries[idx].page_number == page_number`:
     - CAS `owner_txn` from 0 -> requesting TxnId. On success: lock acquired.
     - On failure: return `SQLITE_BUSY`.
   - If `entries[idx].page_number == 0` (empty):
     - CAS `page_number` from 0 -> `page_number` to claim the slot.
       If CAS fails, do NOT advance: re-read the same slot and continue probing.
       (The winner may have inserted `page_number` here; advancing can create
       duplicate keys in a lock-free open-addressing table.)
     - Then CAS `owner_txn` from 0 -> requesting TxnId.
       **MUST NOT** `store()` here: after the key is published, another process
       may observe `(page_number=P, owner_txn=0)` and acquire via CAS. A plain
       store would clobber that winner.
       If this CAS fails, another process raced and acquired the lock; return
       `SQLITE_BUSY`. The acquirer MUST NOT continue
       probing to insert a second copy of `page_number` elsewhere.
   - Else: advance `idx = (idx + 1) & (capacity - 1)`.

This insertion discipline is required: inserting by writing `owner_txn` alone
is incorrect because it would create entries with no discoverable key.

**Release (key-stable, race-free):**

- Locate the entry for `page_number` by probing from its hash.
- CAS `owner_txn` from `releasing TxnId` -> 0 (Release ordering).
- MUST NOT modify `page_number` during normal release. Key deletion in a
  lock-free linear-probing table with separate `(page_number, owner_txn)` atomics
  is not safe; rebuild under lock-quiescence is the only supported removal mechanism
  (§5.6.3.1).

Because keys persist, the table can saturate in long-running workloads. The
lease-based rebuild protocol (§5.6.3.1) clears the table at a proven lock-quiescence
point to reclaim capacity and bound probe lengths.

This is simpler than the in-process sharded HashMap but provides the same
semantics: exclusive write locks per page, immediate failure on contention.

##### 5.6.3.1 Table Rebuild (Lease + Lock-Quiescence Barrier)

The shared-memory lock table is fixed-capacity in V1; "rebuild" means "clear
the table to empty", not "resize".

**Why rebuild is needed:** Because keys are not deleted during normal operation
(§5.6.3), the number of distinct pages ever locked since the last rebuild can
approach `capacity`, causing long probe chains and eventually making insertion
for new page numbers impossible. Rebuild resets the load factor and restores
short probe lengths.

**Who rebuilds:** To avoid a thundering herd, rebuild SHOULD be initiated by
the commit sequencer (the process that currently sequences commit publication
and advances `gc_horizon`; §5.6.5). Any process MAY initiate rebuild if the
sequencer is unavailable, but only one rebuild may be in progress.

**Trigger conditions (any are sufficient):**
- `N/C > 0.70` where `N` counts `page_number != 0` entries, OR
- repeated `SQLITE_BUSY` due to the load-factor guard for >100ms, OR
- (optional) an e-process monitor over probe lengths rejects a configured budget.

**Rebuild lease acquisition (shared memory):**
- A process acquires the rebuild lease by CASing `rebuild_pid` from 0 to its PID,
  then writing `rebuild_pid_birth` and `rebuild_lease_expiry = now + T`.
- If `rebuild_pid != 0` but `rebuild_lease_expiry < now` AND the owning process
  is dead (PID + birth mismatch; §5.6.2), another process MAY steal the lease.
- Lease duration `T` SHOULD be short (default 5s) and renewed while rebuilding.

**Protocol (normative):**
1. Acquire rebuild lease.
2. Freeze new acquisitions: while rebuild lease is active, `try_acquire` MUST
   fail with `SQLITE_BUSY` (non-blocking; busy-timeout is a caller policy).
   `release` MUST continue to function.
   **Deadlock avoidance (normative):** If a transaction already holds one or
   more page locks, it MUST NOT busy-wait/spin on this `SQLITE_BUSY` while
   continuing to hold those locks. It MUST abort/retry (releasing its locks)
   so the drain can reach lock-quiescence. Busy-timeout waiting is permitted
   only for transactions that currently hold **zero** page locks.
3. Drain to **lock-quiescence**: wait until there are no outstanding lock
   holders in the PageLockTable, i.e., until:
   - `forall entry in entries: entry.owner_txn == 0`.
   Read-only transactions MUST NOT block rebuild: Concurrent-mode reads do not
   touch the PageLockTable. During drain, the rebuilder SHOULD run
   `cleanup_orphaned_slots()` so orphaned holders cannot stall the drain.
4. Clear table: set every entry to empty (`page_number = 0`, `owner_txn = 0`).
   This is safe because (a) new acquisitions are frozen and (b) drain observed
   `owner_txn == 0` for all entries, so no acquire/release operation can race
   with key clearing.
5. Increment `rebuild_epoch` and release the lease (`rebuild_pid = 0`).

**Cancellation safety:** Once drain observes lock-quiescence and clearing begins,
the rebuild MUST run to completion (mask cancellation) so the lease is released
and the table is not left partially cleared.

**Load factor analysis (Extreme Optimization Discipline):**

Linear probing has expected probe length (Knuth, Vol. 3):
- **Successful search:** `0.5 * (1 + 1/(1 - alpha))`
- **Unsuccessful search (insert):** `0.5 * (1 + (1/(1 - alpha))^2)`

where `alpha = N/C` is the load factor (N = `page_number != 0` entries,
C = capacity). The often-cited `1/(1 - alpha)` is for **uniform (random)
probing**, not linear probing; linear probing suffers from primary clustering
which makes it worse. Worst-case probe chain length grows as `O(log C)` with
high probability for uniform hashing, but under Zipfian page access, primary
clustering degrades performance further:

| Load factor | Unsuccessful probes (linear) | Unsuccessful probes (Zipfian s=1) |
|-------------|-----------------------------|------------------------------------|
| 0.25        | 1.39                        | ~2.0                               |
| 0.50        | 2.50                        | ~5.0                               |
| 0.75        | 8.50                        | ~20.0                              |
| 0.90        | 50.50                       | ~100+                              |

**Maximum load factor policy:** If `N > 0.70 * C`, new lock acquisitions
return `SQLITE_BUSY` rather than degrading to pathological probe chains.
With C=65536 and the 70% limit, this allows up to 45,875 distinct page numbers
in the table before requiring rebuild. This is a capacity budget, not a limit
on concurrent transactions; a single transaction can touch many pages.

**Alternative: Robin Hood hashing.** If Zipfian clustering proves
problematic, Robin Hood hashing bounds the variance of probe lengths
(maximum probe length difference between any two entries is O(log log C))
while maintaining the same shared-memory-friendly fixed-size layout.

#### 5.6.4 RaptorQ-Native SSI Witness Plane (Cross-Process + Distributed)

SQLite-compatible multi-process SSI cannot rely on in-process hash tables:
the read/write dependency evidence must survive:

- Multiple OS processes mapping the same database
- Crashes mid-transaction and mid-publication
- Torn writes, partial persistence, and partial replication
- Reordering and loss in symbol-native transport

FrankenSQLite solves this by making the SSI dependency graph itself part of
the ECS substrate:

- Reads and writes are published as **witness objects** (`ReadWitness`, `WriteWitness`).
- Candidate discovery is accelerated by a **hierarchical hot index** in shared memory.
- The durable truth is a **cold plane** of ECS objects (`WitnessDelta`,
  `WitnessIndexSegment`, `DependencyEdge`, `CommitProof`).

The result is a witness plane with the same posture as the rest of ECS:
if bytes go missing, we decode; if processes crash, we ignore uncommitted
artifacts; if shared memory is corrupted, we rebuild from symbol logs.

##### 5.6.4.1 Non-Negotiable Requirements

1. **No false negatives (candidate discoverability):** If transaction `R` reads
   a `WitnessKey K` and an overlapping transaction `W` writes `K`, then during
   SSI validation of either party we MUST be able to discover `R` as a
   candidate for `K` at *some configured hierarchy level* (refinement may be
   required to confirm intersection).
2. **Cross-process:** Works when multiple OS processes attach to the same DB
   file and share only the shared-memory region + ECS logs.
3. **Distributed-ready:** Evidence is ECS objects, so symbol-native replication
   can carry the dependency graph, not just the data pages.
4. **Self-healing:** If a subset of witness symbols are missing/corrupt within
   tolerance, decoding MUST reconstruct them (or surface an explicit "durability
   contract violated" diagnostic with decode proofs in lab/debug).
5. **Monotonic updates:** Hot-plane index updates are unions only (set bits /
   insert IDs). Clearing is performed only by epoch swap under a provably safe
   GC horizon (see §5.6.4.8 and §5.6.5).

##### 5.6.4.2 Transaction Identity for Witnesses: TxnToken

TxnSlots are reused. Any data structure that references slot IDs must prevent
stale interpretation. Therefore every cross-process SSI artifact identifies
transactions by a `TxnToken`:

```
TxnToken := (txn_id: TxnId, txn_epoch: TxnEpoch)
```

`TxnEpoch` is stored in `TxnSlot.txn_epoch` and is incremented on every slot
acquisition (wrap permitted). Any lookup of a slot-derived candidate MUST
validate that the slot's `(txn_id, txn_epoch)` matches the token being
considered. This permits false positives (stale bits) but forbids false
negatives (missing candidates).

##### 5.6.4.3 WitnessKey (Granularity Without Correctness Risk)

SSI tracks rw-antidependencies over a canonical key space:

```text
WitnessKey =
  | Page(pgno: u32)
  | Cell(btree_root_pgno: u32, cell_tag: u32)
  | ByteRange(page: u32, start: u16, len: u16)
  | KeyRange(btree_root_pgno: u32, lo: Key, hi: Key)   // optional, advanced
  | Custom(namespace: u32, bytes: [u8])
```

**Correctness rule:** It is always valid to fall back to `Page(pgno)` even if
higher-resolution keys exist. Finer keys exist to reduce false positives and
unlock safe merge/refinement (§5.10), never to preserve correctness.

**Implementation directive (critical for deterministic rebase/merge):**
The SSI witness plane is fed by *semantic* operations (VDBE/B-tree), not raw
pager I/O. Implementations MUST NOT register `WitnessKey::Page(pgno)` reads just
because a cursor traversed or inspected a B-tree page. Doing so makes almost all
writers appear read-dependent on the pages they modify, collapsing safe merge
and deterministic rebase (§5.10.2) back to abort/retry.

Instead, the B-tree/VDBE MUST register witnesses at key granularity:
- **Point read / uniqueness check (including "negative read"):**
  `WitnessKey::Cell(btree_root_pgno, cell_tag(key_bytes))`
- **Point write (insert/delete/update by key):**
  `WitnessKey::Cell(btree_root_pgno, cell_tag(key_bytes))`
- `btree_root_pgno` is the SQLite B-tree root page number for the table or
  index (stable namespace; see §11.11 `sqlite_master.rootpage`).

`cell_tag(key_bytes)` MUST be deterministic and stable across processes. A
recommended derivation is:
`cell_tag = low32(xxh3_64("fsqlite:witness:cell:v1" || le_u32(btree_root_pgno) || canonical_key_bytes))`.

##### 5.6.4.4 RangeKey: Hierarchical Buckets Over WitnessKey Hash Space

We index the witness key space via a prefix tree over hashes:

1. Canonical-encode `WitnessKey` bytes.
2. Compute `KeyHash := xxh3_64(WitnessKeyBytes)`.
3. For each configured level `L`, derive `RangeKey(L, prefix_bits)` as the top
   `p_L` bits of `KeyHash`.

Default hierarchy (tunable, stored in config and recorded in manifests so
replicas interpret evidence consistently):

- Level L0: `p0 = 12` (4096 buckets)
- Level L1: `p1 = 20` (~1,048,576 buckets, allocated lazily in hot plane)
- Level L2: `p2 = 28` (deep refinement for hotspots)

This is intentionally *not* an interval tree over page numbers: hashing avoids
contiguous hotspot clustering (e.g., root pages) collapsing into a single range
node.

##### 5.6.4.5 Hot Plane (Shared Memory): HotWitnessIndex

The hot plane is an accelerator for candidate discovery. It is not the source
of truth.

Shared memory stores a fixed-size hash table mapping `(level, prefix)` to a
bucket entry with **monotonic bitsets** of active TxnSlots:

```
HotWitnessIndex := {
    capacity : u32,       -- power-of-2; sized for expected hot buckets
    epoch    : AtomicU32, -- current witness epoch (monotonic)
    entries  : [HotWitnessBucketEntry; capacity],
    overflow : HotWitnessBucketEntry, -- always-present catch-all (no false negatives)
}

HotWitnessBucketEntry := {
    level        : AtomicU8,      -- 0xFF = empty
    prefix       : AtomicU32,     -- packed prefix bits (interpretation depends on level)
    epoch_lock   : AtomicU32,     -- 0 = unlocked; non-zero = locked (spinlock for epoch install + clear)
    epoch_a      : AtomicU32,     -- epoch tag for (readers_a, writers_a)
    readers_a    : [AtomicU64; W],-- bit i = TxnSlotId i is a reader in epoch_a
    writers_a    : [AtomicU64; W],
    epoch_b      : AtomicU32,     -- epoch tag for (readers_b, writers_b)
    readers_b    : [AtomicU64; W],-- bit i = TxnSlotId i is a reader in epoch_b
    writers_b    : [AtomicU64; W],
}
```

Where `W = ceil(max_txn_slots / 64)`.

**Helper views (conceptual, but required semantics):**

```
readers_for_epoch(bucket, e):
  if bucket.epoch_a == e: return bucket.readers_a
  if bucket.epoch_b == e: return bucket.readers_b
  return all_zeros

writers_for_epoch(bucket, e):
  if bucket.epoch_a == e: return bucket.writers_a
  if bucket.epoch_b == e: return bucket.writers_b
  return all_zeros
```

**Update on read/write (monotonic, race-free):**

- Every Concurrent-mode transaction pins a `witness_epoch` at begin
  (`TxnSlot.witness_epoch`; §5.6.2 and §5.6.4.8). All witness-plane
  registrations for the transaction MUST target that pinned epoch.
- On read of key `K` by slot `s`, set bit `s` in the bucket buffer tagged with
  `epoch == TxnSlot[s].witness_epoch` for all configured levels' buckets for `K`
  (L0/L1/L2), or `overflow` if allocation fails.
- On write of key `K` by slot `s`, set bit `s` in the corresponding `writers_*`
  buffers similarly.

**Epoch discipline (required to avoid false negatives):**

- At any time, there are at most two *live* epochs in the hot plane:
  `cur = HotWitnessIndex.epoch` and `prev = cur - 1` (because epoch advancement
  is constrained by §5.6.4.8).
- Updaters MUST load `target_epoch = TxnSlot[s].witness_epoch` with `Acquire`.
- Fast path: if `epoch_a == target_epoch` or `epoch_b == target_epoch`, no lock
  is needed; set the relevant bit using `fetch_or` in that buffer.
- Slow path (install + clear): if neither buffer is tagged with `target_epoch`,
  the updater MUST acquire `epoch_lock` and install `target_epoch` into one
  buffer by:
  - clearing that buffer's `readers_*[*]` and `writers_*[*]` to 0, then
  - storing the corresponding `epoch_* = target_epoch` with `Release`, then
  - releasing `epoch_lock`.
  Install MUST NOT overwrite the other live epoch's buffer (if present); any
  buffer tagged with neither `cur` nor `prev` is stale and may be reused.

**`epoch_lock` acquisition (normative):**

- Acquire with a CAS loop: `CAS(0 → 1, Acquire, Relaxed)` and bounded backoff.
- Release with `store(0, Release)`.
- Lock acquisition MUST be cancellation/budget-aware. If the updater cannot
  acquire the lock within its budget, it MUST fall back to setting the bit in
  `HotWitnessIndex.overflow` so candidate discoverability is preserved.

If a bucket cannot be allocated due to hot-index capacity pressure, the update
MUST be applied to `HotWitnessIndex.overflow` for the corresponding kind
(read/write). This preserves the "no false negatives" requirement at the cost
of higher false positive rate.

**Staleness handling:** Bits are never cleared per transaction. Candidates are
filtered by:
- Current `TxnSlot.txn_id != 0` (slot is active)
- `TxnSlot.txn_epoch` matches the `TxnToken` being considered (prevents stale slot-id misbind)

##### 5.6.4.6 Cold Plane (ECS Objects): Durable, Replicable Truth

In Native mode, the witness plane's cold truth is stored as ECS objects (thus
RaptorQ-encodable, repairable, and replicable):

- `ReadWitness` / `WriteWitness`: per-transaction, per-bucket evidence with a
  sound `KeySummary` (no false negatives for its coverage claim).
- `WitnessDelta`: monotonic participation updates (`Present` union) used to
  rebuild/compact index segments.
- `WitnessIndexSegment`: compacted `readers` / `writers` roaring bitmaps for a
  `(level, prefix)` over a commit sequence range, rebuildable from deltas.
- `DependencyEdge`: explicit rw-antidependency edges (mandatory for explainability).
- `CommitProof`: proof-carrying commit artifact referencing witnesses, segments,
  and edges used to validate serializability.

In Compatibility mode, the cold plane is still required, but is stored as an
ECS-style symbol log sidecar under the database's `.fsqlite/` directory (not
inside the SQLite `.db` file) to preserve strict file-format compatibility.

Canonical object structures are specified in §5.7 (SSI algorithm and witness
objects), and they participate in ECS deterministic encoding rules (§3.5).

##### 5.6.4.7 Publication Protocol (Cancel-Safe, Crash-Resilient)

Witness/edge/proof publication MUST be correct under cancellation at any `.await`
point and under process crash at any instruction boundary:

1. **Reserve:** obtain a durable append reservation in the symbol log (or
   equivalent) and a linear reservation token.
2. **Write:** write object symbol records (systematic + repair as configured).
3. **Commit:** atomically publish the reservation token so the object becomes
   visible to readers.
4. **Abort:** if cancelled before commit, dropping the reservation token MUST
   make the partial publication unreachable and GC-able.

This mirrors asupersync's two-phase discipline (reserve/commit) used to prevent
silent drops, but is applied to persistent ECS publication rather than in-memory
channels.

**Marker discipline:** A transaction is committed iff its `CommitMarker` exists
and is published. Witness objects may exist for aborted transactions and are
ignored once the transaction's abort is known (slot state and/or marker stream).

##### 5.6.4.8 Witness GC and Bucket Epochs

Witness evidence is retained until it is provably irrelevant:

- Define `oldest_active_begin_seq := min(TxnSlot.begin_seq for all active slots)`.
- Define `safe_gc_seq := oldest_active_begin_seq`.

Any witness/edge/proof that references only transactions with `commit_seq < safe_gc_seq`
is eligible for cold-plane compaction/pruning (subject to retention policy for
debuggability).

The hot plane uses **bucket epochs**:
- `HotWitnessIndex.epoch` is a monotonically increasing global generation number.
  It is a **performance accelerator**, not the source of truth.
- The hot plane MUST be **double-buffered** per bucket (two epoch-tagged bitset
  buffers; §5.6.4.5). This allows advancing epochs without requiring "zero
  Concurrent-mode transactions", preventing reader-induced writer starvation.
- **Pinned epoch (normative):** Every Concurrent-mode transaction MUST pin
  `TxnSlot.witness_epoch = HotWitnessIndex.epoch.load(Acquire)` at BEGIN and MUST
  target that epoch for all witness-plane registrations. This ensures a
  transaction's discoverability does not depend on global epoch changes while it
  is active.
- **Safe epoch advancement (normative):** Let `cur = HotWitnessIndex.epoch.load(Acquire)`
  and `old = cur - 1`. Advancing `epoch` from `cur` to `cur+1` necessarily
  drops/reuses buffers tagged `old`. Therefore epoch advancement is permitted
  iff there are **no** TxnSlots with:
  - `mode == Concurrent`, and
  - `state` in {Active, Committing}, and
  - `txn_id != 0` (including `TXN_ID_CLAIMING`; Phase 2 pins `witness_epoch` before publish), and
  - `witness_epoch == old`.
  This does not require a moment of zero active transactions; it requires only
  that the *oldest* epoch has drained.
- **Bucket refresh:** When an updater needs to set a bit for `target_epoch` and
  the bucket has no buffer tagged with that epoch, it refreshes a stale buffer
  under `epoch_lock` by clearing it and publishing the new `epoch_*` tag with
  Release semantics (§5.6.4.5). Candidate discovery MUST consult both live
  epochs (`cur` and `cur-1`) plus `overflow`.

This yields bounded memory and bounded per-operation cost without per-txn clears.

##### 5.6.4.9 Distributed Mode: Proof-Carrying Replication (Normative Hook)

Because witness evidence (`ReadWitness`/`WriteWitness`/`DependencyEdge`) and
validation summaries (`CommitProof`) are ECS objects, they are **replicable by
symbols** just like pages and capsules.

Normative replication hook:
- Any replica that can receive/apply a `CommitMarker` MUST be able to fetch the
  marker-referenced `CommitProof` and (transitively) the witness-plane objects
  needed to replay validation.
- Replicas MAY enforce a policy of **proof-carrying commits**: accept a remote
  commit only if the referenced evidence objects decode and the local replay of
  validation reaches the same conclusion under the same policy knobs.

This does not require leaderless operation, but it removes "trust me" from the
distributed story: commits can carry replayable evidence.

##### 5.6.4.10 Deterministic Verification Gates (Required)

The witness plane MUST be verified under cancellation/crash/loss using
asupersync LabRuntime:
- deterministic scenarios: §17.4.1
- no-false-negatives property tests: §17.4.2

#### 5.6.5 GC Coordination

The `gc_horizon` in shared memory is a monotonically *increasing* safe-point
in CommitSeq space: `min(begin_seq)` across all active transactions. Since
`begin_seq` is derived from the monotonically increasing published `commit_seq`
high-water mark (§5.6.1),
this horizon never decreases. To avoid races and partial views across
processes, `gc_horizon` is authoritative only when advanced by the commit
sequencer. Other processes treat it as read-only state.

**GC scheduling policy (Alien-Artifact Discipline):**

"Periodically" is not a specification. The GC frequency is derived from:

```
f_gc = min(f_max, max(f_min, version_chain_pressure / target_chain_length))
```

where:
- `f_max = 100 Hz` (never GC more often than every 10ms -- diminishing returns)
- `f_min = 1 Hz` (always GC at least once per second -- safety floor)
- `version_chain_pressure` = observed mean version chain length (BOCPD-tracked)
- `target_chain_length` = 8 (from Theorem 5: R*D+1 for R=100, D=0.07s ≈ 8)

**Who runs GC:** The commit coordinator runs `raise_gc_horizon()` after each
group commit batch, piggy-backing on the commit critical section. This
avoids the thundering-herd problem (multiple processes scanning TxnSlots
simultaneously). Cross-process coordination: only the process that holds
the WAL write lock (the coordinator) runs GC. Other processes observe the
updated `gc_horizon` on their next read.

```
raise_gc_horizon():
    // Default: if no active transactions exist, the safe point is the latest
    // commit sequence number.
    old_horizon = shm.gc_horizon.load(Acquire)
    global_min_begin_seq = shm.commit_seq.load(Acquire)
    for slot in txn_slots:
        tid = slot.txn_id.load(Acquire)
        if tid == 0:
            continue
        if tid == TXN_ID_CLAIMING || tid == TXN_ID_CLEANING:
            // CRITICAL: A claiming slot may already have captured its snapshot
            // (Phase 2 initializes begin_seq/snapshot_high), but has not yet
            // published a real txn_id. Advancing gc_horizon while a slot is in
            // CLAIMING can prune versions that the soon-to-be-active transaction
            // will require. Therefore, do not advance the horizon while ANY slot
            // is in CLAIMING state.
            global_min_begin_seq = min(global_min_begin_seq, old_horizon)
            continue
        global_min_begin_seq = min(global_min_begin_seq, slot.begin_seq.load(Acquire))
    new_horizon = max(old_horizon, global_min_begin_seq)  // monotonic
    shm.gc_horizon.store(new_horizon, Release)
```

#### 5.6.6 Compatibility: Legacy Interop and File-Lock Fallback

Legacy SQLite processes do not understand `foo.db.fsqlite-shm`. They coordinate
only via the standard SQLite lock regime (`foo.db-shm` WAL-index locks and
database-file byte locks). This creates a strict interop boundary:

- When `foo.db.fsqlite-shm` is used (the default fast path), FrankenSQLite MUST
  run the Hybrid SHM protocol (§5.6.7). This supports **legacy readers** but
  MUST exclude legacy writers (a legacy writer would bypass `.fsqlite-shm` and
  can corrupt the WAL).
- If `foo.db.fsqlite-shm` cannot be used, FrankenSQLite falls back to standard
  SQLite file locking (single-writer). This fallback can interoperate with
  legacy writers, but it has no multi-writer MVCC and no SSI.

##### 5.6.6.1 Legacy Writer Exclusion (Required When Using `foo.db.fsqlite-shm`)

**Problem:** If a legacy writer can acquire SQLite's standard write locks while
FrankenSQLite is operating in Compatibility mode with `foo.db.fsqlite-shm`, it
can write pages without participating in MVCC coordination or witness
registration.

**Rule (normative):** In Compatibility mode, whenever `foo.db.fsqlite-shm` is
in use, the system MUST hold a legacy-writer exclusion lock that prevents a
standard SQLite process from becoming a writer.

**WAL mode (required for Compatibility mode):**
- The exclusion lock MUST be `WAL_WRITE_LOCK` on the legacy WAL-index shared
  memory (`foo.db-shm`).
- The lock MUST be held for the coordinator's lifetime (Hybrid SHM protocol,
  §5.6.7). Releasing it creates a window for a legacy writer.
- Legacy readers remain permitted: `WAL_WRITE_LOCK` blocks writers, not readers.

**Coordinator note (multi-process):** Because `WAL_WRITE_LOCK` is exclusive,
Compatibility mode with `foo.db.fsqlite-shm` requires a single cross-process
commit sequencer while the exclusion lock is held. In multi-process deployments,
other processes MUST route commit publication through the sequencer (shared
memory two-phase queue) so the lock is not released to legacy writers between
commits.

If the exclusion lock cannot be acquired, the database open MUST fail with
`SQLITE_BUSY` (or wait per busy-timeout), because the Hybrid SHM protocol
cannot be made safe without excluding legacy writers.

##### 5.6.6.2 No-SHM Fallback (File Locks Only)

When shared-memory coordination is not available (e.g., `foo.db.fsqlite-shm`
cannot be created due to filesystem restrictions), FrankenSQLite falls back
to C SQLite's file-level locking protocol:

- `WAL_WRITE_LOCK` for single-writer mutual exclusion
- Standard WAL reader marks for snapshot isolation
- No multi-writer MVCC, no SSI

This ensures FrankenSQLite works on any filesystem that supports advisory
file locks, degrading gracefully from multi-writer to single-writer.

#### 5.6.7 Compatibility Mode: Hybrid SHM Coordination Protocol

**Problem statement:** Compatibility Mode (§2.4 Layer 1) produces standard
SQLite database and WAL files readable by C SQLite. But FrankenSQLite uses
`foo.db.fsqlite-shm` (the `FSQLSHM` layout, §5.6.1) for MVCC coordination,
while C SQLite uses `foo.db-shm` (standard WAL-index, §11.10). Without a
bridging protocol, two failures arise:

1. **Legacy readers cannot find new frames.** C SQLite locates WAL frames
   via hash tables in `foo.db-shm`. If FrankenSQLite only updates
   `foo.db.fsqlite-shm`, a C SQLite reader's `mxFrame` is stale.

2. **Legacy writers corrupt data.** Nothing prevents C SQLite from acquiring
   `WAL_WRITE_LOCK` on `foo.db-shm` and writing concurrently, since
   FrankenSQLite's coordinator uses a different lock domain. Two
   uncoordinated writers appending to the same WAL = silent corruption.

**Normative protocol (MUST for Compatibility Mode):**

When `foo.db.fsqlite-shm` is in use, the Write Coordinator MUST also
maintain the standard `foo.db-shm` WAL-index:

1. **Exclude legacy writers (startup).** Acquire `WAL_WRITE_LOCK` (byte 120
   of `foo.db-shm`, §2.1) and hold it for the coordinator's lifetime. This
   prevents C SQLite from entering WAL-write mode. The lock MUST be held
   even when no FrankenSQLite transaction is active — releasing creates a
   window for a legacy writer.

2. **Update WAL-index hash tables (on commit).** After appending WAL frames
   (§5.9.2 `WALAppend`), the coordinator MUST update `foo.db-shm`:
   - Insert each frame's `(page_number, frame_index)` into the hash table.
   - Update `mxFrame` in both `WalIndexHdr` copies.
   - Update `aFrameCksum`, `aSalt`, `aCksum` in both header copies.
   - Use the dual-copy protocol (write copy 1, then copy 2) so lock-free
     readers see a consistent snapshot.

3. **Maintain reader marks + reader locks.** FrankenSQLite readers MUST
   participate in SQLite's WAL reader protocol, not just its metadata:
   - Slot claim + mark update MUST follow SQLite's lock discipline:
     - Acquire `WAL_READ_LOCK(i)` **EXCLUSIVE** (byte `aLock[3+i]` in `foo.db-shm`; §11.10),
       then write/update `aReadMark[i]` while holding the EXCLUSIVE lock.
     - Downgrade to a **SHARED** `WAL_READ_LOCK(i)` for the full lifetime of the
       snapshot, releasing it only when the snapshot ends.
     This matches SQLite's invariants: the lock (not just the mark value) is what
     legacy checkpointers consult to decide which marks are live.
   This is non-negotiable: legacy checkpointers consult the read locks to decide
   which `aReadMark` entries are live. Updating `aReadMark` without holding the
   matching `WAL_READ_LOCK(i)` can cause overwritten frames and silent corruption.
   If no `WAL_READ_LOCK(i)` slot is available (cannot obtain EXCLUSIVE on any
   slot that would be required for the snapshot), the reader MUST return
   `SQLITE_BUSY` (or wait per busy-timeout).

   **Interop limitation (explicit):** The legacy WAL-index format provides only
   5 reader marks/locks (`aReadMark[0..4]`). This bounds the number of distinct
   concurrently-active WAL snapshots that can be represented to legacy tooling.
   FrankenSQLite's "hundreds of readers" story applies to Native mode and to
   non-legacy coordination; Compatibility mode inherits this legacy constraint.

4. **Checkpoint coordination.** Checkpoint logic (§7.5) MUST update
   `nBackfill` in the standard `WalCkptInfo` during backfill.

**Ordering:** The standard WAL-index update (step 2) MUST happen after
`wal.sync()` and before `publish_versions()` in the group commit sequence.
If a C SQLite reader sees a new `mxFrame`, the frames must already be
durable on disk.

**Native Mode:** This protocol does NOT apply to Native Mode (§2.4 Layer 3),
which uses ECS-based commit streams, not standard WAL files.

### 5.7 SSI Algorithm Specification (Witness Plane, Proof-Carrying)

Serializable Snapshot Isolation (SSI) extends Snapshot Isolation to detect and
prevent the write skew anomaly. SSI ships as the default isolation mode for
`BEGIN CONCURRENT` (Layer 2 of Section 2.4).

In FrankenSQLite, SSI is implemented on top of the **RaptorQ-native witness
plane** (§5.6.4): read/write dependency evidence is stored as ECS objects and
indexed by a hierarchical hot index (shared memory) plus a compacted cold index
(ECS). This makes SSI:

- Cross-process safe (multiple OS processes)
- Distributed-ready (proof-carrying replication is possible)
- Self-healing (witness evidence is fountain-coded and repairable)
- Explainable (explicit `DependencyEdge` + `CommitProof` artifacts)

**Formal definition of rw-antidependencies (witness-key space):**

An rw-antidependency edge `R -rw-> W` exists iff:

1. `R` and `W` are **concurrent**: neither committed before the other's
   snapshot was taken (`W.commit_seq > R.begin_seq` AND `R.commit_seq > W.begin_seq`,
   or equivalently, their active lifetimes overlap in the commit-sequence order).
   The relevant notion is snapshot-based concurrency, not wall-clock overlap.
2. There exists a `WitnessKey K` such that `R` read `K` under its snapshot and
   `W` wrote `K` with a commit not visible to `R`'s snapshot (i.e.,
   `W.commit_seq > R.begin_seq`).

`WitnessKey` is the canonical "thing you read or wrote" key space (§5.6.4.3).
Falling back to `Page(pgno)` is always correct; finer keys reduce false positives
and enable merge (§5.10).

**Witness plane integration contract (required hooks):**

Every read path that participates in serializability MUST register a key, and
every write path MUST register keys at the finest available granularity:

```
register_read(key: WitnessKey)
register_write(key: WitnessKey)
emit_witnesses() -> (read_witnesses: Vec<ObjectId>, write_witnesses: Vec<ObjectId>)
```

`emit_witnesses()` publishes `ReadWitness` / `WriteWitness` objects (ECS) and
updates the hot-plane `HotWitnessIndex` buckets (shared memory) as a monotonic
union.

#### 5.7.1 Witness Objects (Canonical ECS Schemas)

The witness plane is defined by a small family of **canonical ECS objects**
whose encoding is deterministic (§3.5) and whose publication is cancel-safe
(reserve/write/commit; §5.6.4.7).

These structures are *normative*; field order and canonicalization rules follow
the ECS encoding rules:
- integer endianness: little-endian
- maps/sets: sorted by canonical byte representation
- bitmaps: canonical roaring encoding (stable container ordering)

```text
KeyHash := u64
CommitSeq := u64

KeySummary :=
  | ExactKeys(keys: Vec<WitnessKey>)                  // sorted by canonical bytes
  | HashedKeySet(hashes: Vec<KeyHash>)                // sorted ascending
  | PageBitmap(pages: RoaringBitmap<u32>)             // page numbers
  | CellBitmap(cells: RoaringBitmap<u64>)             // (page<<32) | cell_tag
  | ByteRangeList(ranges: Vec<(u32, u16, u16)>)       // (page, start, len), sorted
  | Chunked(chunks: Vec<KeySummaryChunk>)             // for large sets; each chunk is sound

ReadWitness := {
  txn          : TxnToken
  begin_seq    : CommitSeq
  level        : u8
  range_prefix : u32
  key_summary  : KeySummary        // sound: no false negatives for its coverage claim
  emitted_at   : LogicalTime       // from asupersync logical clock (optional in minimal builds)
}

WriteWitness := {
  txn          : TxnToken
  begin_seq    : CommitSeq
  level        : u8
  range_prefix : u32
  key_summary  : KeySummary
  emitted_at   : LogicalTime
  write_kind   : { Intent, Final } // Final is required before commit validation
}

WitnessDelta := {
  txn          : TxnToken
  begin_seq    : CommitSeq
  kind         : { Read, Write }
  level        : u8
  range_prefix : u32
  participation: { Present }       // union-only CRDT update (no removals)
  refinement   : Option<KeySummary>
}

WitnessIndexSegment := {
  segment_id        : u64
  level             : u8
  range_prefix      : u32
  readers           : RoaringBitmap<u64>  // TxnId
  writers           : RoaringBitmap<u64>  // TxnId
  epochs            : Option<EpochSnapshot> // optional epoch table snapshot for slot reuse validation
  covered_begin_seq : CommitSeq
  covered_end_seq   : CommitSeq
}

DependencyEdge := {
  kind            : { RWAntiDependency }
  from            : TxnToken
  to              : TxnToken
  key_basis       : { level: u8, range_prefix: u32, refinement: Option<KeySummaryDigest> }
  observed_by     : TxnToken
  observation_seq : CommitSeq
}

CommitProof := {
  txn                : TxnToken
  begin_seq          : CommitSeq
  commit_seq         : CommitSeq
  has_in_rw          : bool
  has_out_rw         : bool
  read_witnesses     : Vec<ObjectId>
  write_witnesses    : Vec<ObjectId>
  index_segments_used: Vec<ObjectId>
  edges_emitted      : Vec<ObjectId>
  merge_witnesses    : Vec<ObjectId>
  abort_policy       : { AbortPivot, AbortYoungest, Custom }
}

AbortWitness := {
  txn            : TxnToken
  begin_seq      : CommitSeq
  abort_seq      : CommitSeq              // observation ordering stamp (not a commit)
  reason         : { SSIPivot, Cancelled, Other }
  edges_observed : Vec<ObjectId>
}

MergeWitness := {
  // Specified in §5.10 (merge artifacts are ECS objects and RaptorQ-encodable).
}
```

**Soundness rule (KeySummary):** A `KeySummary` MUST NOT have false negatives
for the subset it claims to cover. False positives are allowed and are reduced
by refinement (cell/byte-range keys) and merge (§5.10).

**CommitProof meaning:** `CommitProof` is a *replayable proof*, not a
cryptographic proof: it contains enough evidence references to deterministically
re-run SSI validation and reach the same decision (commit vs abort) given the
same witness plane.

#### 5.7.2 Candidate Discovery (Hot Plane) and Refinement (Cold Plane)

SSI validation needs to discover candidate overlaps without scanning all active
transactions. The witness plane does this in two stages:

1. **Hot-plane candidate discovery:** shared-memory `HotWitnessIndex` bitsets
   provide a superset of candidates in O(1) per bucket.
2. **Cold-plane refinement (optional):** decode `ReadWitness`/`WriteWitness`
   refinements (or `WitnessIndexSegment`s) to confirm actual key intersection and
   reduce false positives.

**Incoming rw-antidependency discovery** (`R -rw-> T`):

- Let `cur = HotWitnessIndex.epoch.load(Acquire)` and `prev = cur - 1`.
- For each `WriteWitness` bucket of `T`, query the bucket's reader bitsets for
  **both live epochs** (`cur` and `prev`) and OR them:
  `readers = readers_for_epoch(cur) ∪ readers_for_epoch(prev)`.
  Then intersect with `active_slots_bitset`.
- Map slots to `TxnToken` via `TxnSlotTable`, validating `txn_epoch` matches.
- If refinement is enabled, confirm `ReadSet(R) ∩ WriteSet(T) ≠ ∅` at the finest
  available key granularity; otherwise treat bucket overlap as conflict.

**Outgoing rw-antidependency discovery** (`T -rw-> W`) is symmetric using
the union of `writers_for_epoch(cur) ∪ writers_for_epoch(prev)`.

**Theorem (No False Negatives, hot plane -- active transactions only):**

If a transaction `R` is **active** (holds its TxnSlot) and registers a read
`WitnessKey K`, then `R` is discoverable as a reader candidate for `K` at
commit time for any overlapping writer `T` because:
- registration updates every configured level bucket for `K` (or `overflow`)
  by setting `readers_for_epoch(R.witness_epoch)[R.slot_id]` (union-only),
- epoch advancement is constrained so every active transaction has
  `witness_epoch ∈ {cur, cur-1}` (§5.6.4.8), and candidate discovery queries both
  live epochs for those same buckets (and `overflow`),
- stale bits are filtered by `(txn_id, txn_epoch)` validation.

Thus false negatives are forbidden by construction for active readers; false
positives are bounded and reduced by refinement.

**Scope limitation:** This theorem covers only transactions that still hold
their TxnSlot. Once `R` commits and frees its slot (`txn_id = 0`), `R`'s
hot-plane evidence becomes stale (slot reuse changes `txn_epoch`). For
committed readers, the `RecentlyCommittedReadersIndex` (§5.6.2.1) provides
the required coverage. The combination of hot plane (active readers) +
`RecentlyCommittedReadersIndex` (committed readers) is required for complete
incoming-edge coverage.

#### 5.7.3 Commit-Time SSI Validation (Proof-Carrying)

SSI validation runs as part of the commit pipeline (see §7.11 in Native mode).
It produces explicit evidence artifacts:
- `DependencyEdge` objects for observed rw-antidependencies
- `CommitProof` for commits
- `AbortWitness` for SSI aborts

This makes concurrency behavior deterministic, auditable, and replicable.

**Normative commit-time procedure (conceptual pseudocode):**

```text
ssi_validate_and_publish(T):
  // Preconditions:
  // - T has registered read/write WitnessKeys during execution.
  // - T holds the necessary page locks / write intents for its write set.

  // 1) Emit witnesses (ECS) + update hot index (SHM).
  //    This must happen before the read-only fast path because read witnesses
  //    are needed even for read-only transactions (other writers use them for
  //    rw-antidependency discovery).
  (read_wits, write_wits) = T.emit_witnesses()

  // 2) Fast path: read-only transactions (empty write set) can never be the
  //    pivot in a dangerous structure (pivot requires both incoming AND outgoing
  //    rw edges, and outgoing requires a write). Skip SSI validation entirely.
  if T.write_set is empty:
      return (read_wits, [], [], [])

  // 3) Discover incoming and outgoing rw-antidependencies.
  //    IMPORTANT: The hot plane (SHM bitset) only tracks ACTIVE transactions.
  //    Transactions that committed and freed their TxnSlot are no longer visible
  //    in the hot plane. To avoid false negatives:
  //
  //    - discover_outgoing_edges MUST also consult the commit_index (CommitLog)
  //      for transactions that committed AFTER T.begin_seq (i.e., transactions
  //      whose writes are not visible to T's snapshot but which wrote keys that
  //      T read). Without this, a committed-and-freed writer's rw-antidependency
  //      with T goes undetected.
  //
  //    - discover_incoming_edges MUST also consult the recently_committed_readers
  //      index (§5.6.2.1) for transactions that committed AFTER T.begin_seq and
  //      whose read witnesses overlap with T's write set. Without this, a
  //      committed-and-freed reader's rw-antidependency with T goes undetected,
  //      potentially allowing a dangerous structure (X -> R_committed -> T) to
  //      be missed entirely. This is the symmetric problem to the outgoing-edge
  //      gap: PostgreSQL retains SIREAD locks until all concurrent transactions
  //      have finished; FrankenSQLite MUST provide equivalent coverage.
  in_edges  = discover_incoming_edges(T, write_wits)   // checks hot plane + recently_committed_readers
  out_edges = discover_outgoing_edges(T, read_wits)    // checks hot plane + commit_index

  T.has_in_rw  = (in_edges not empty)
  T.has_out_rw = (out_edges not empty)

  // 4) Refinement and merge escape hatch (optional but canonical).
  // - Refinement confirms true intersection at finer WitnessKey granularity.
  // - Merge (§5.10) can transform "same page" conflicts into commuting merges,
  //   tightening witness precision and dropping spurious edges.
  (in_edges, out_edges, merge_witnesses) = refine_and_maybe_merge(T, in_edges, out_edges)

  T.has_in_rw  = (in_edges not empty)
  T.has_out_rw = (out_edges not empty)

  // 5) Pivot rule (conservative, sound default):
  //    T is the pivot (T2 in T1->T2->T3): abort T.
  if T.has_in_rw && T.has_out_rw:
     publish AbortWitness(T, edges = in_edges ∪ out_edges)
     abort T with SQLITE_BUSY_SNAPSHOT

  // 6) T3 rule (Cahill/Ports §3.2, "near-miss" check):
  //    When T commits, it may complete a dangerous structure where some other
  //    transaction R is the pivot. Specifically, if:
  //      - R already has an incoming rw edge (R.has_in_rw = true), and
  //      - T wrote a key/page that R read (creating R -rw-> T, i.e. R now has
  //        an outgoing rw edge),
  //    then R is a confirmed pivot in `X -rw-> R -rw-> T`.
  //
  //    - If R is still active: mark R for abort (eager optimization).
  //    - If R already committed: R cannot be aborted, so T MUST abort.
  //
  //    Sources of `in_edges` include:
  //    - active readers discovered from the hot plane (TxnSlots), and
  //    - committed readers discovered via `RecentlyCommittedReadersIndex` (§5.6.2.1),
  //      which carries `has_in_rw` from the reader's commit-time state.
  for R in in_edges.source_txns():           // R -rw-> T (R read, T wrote)
      if R.is_active():
          R.has_out_rw = true                // R now has an outgoing rw edge to T
          if R.has_in_rw:
              R.marked_for_abort = true      // R is pivot; abort it
      else:
          if R.has_in_rw:
              publish AbortWitness(T, edges = in_edges ∪ out_edges)   // committed pivot implies abort T (conservative)
              abort T with SQLITE_BUSY_SNAPSHOT

  // 7) Publish edges + return evidence references for CommitProof.
  edge_ids = publish DependencyEdge objects for (in_edges ∪ out_edges)
  return (read_wits, write_wits, edge_ids, merge_witnesses)
```

**Correctness rule:** Skipping refinement is always safe (over-approx); it only
increases abort rate. Merge never weakens correctness: it replaces false
conflicts with commuting composition and tightens witness precision so SSI sees
fewer spurious edges.

**The dangerous structure:**

SSI detects serialization anomalies by identifying "dangerous structures" --
patterns of rw-antidependency edges that imply a cycle in the serialization
graph.

The dangerous structure is two consecutive rw-antidependency edges:

```
T1 -rw-> T2 -rw-> T3
```

where:
- `T1` read something that `T2` later wrote (T1 -rw-> T2)
- `T2` read something that `T3` later wrote (T2 -rw-> T3)
- At least one of `T1` or `T3` has already committed

This implies a potential cycle: `T1` must precede `T2` (because `T1` did not
see `T2`'s write), `T2` must precede `T3` (same reason), but if `T3` committed
before `T1`'s snapshot, then `T3` should precede `T1` in the serial order,
closing the cycle. The condition `(T1 committed OR T3 committed)` ensures that
the cycle is unavoidable -- if both are still active, the system could still
reorder them to avoid the anomaly.

Formally, the dangerous structure exists when:
```
exists T1, T2, T3 :
    rw_edge(T1, T2) AND rw_edge(T2, T3)
    AND T2.has_in_rw AND T2.has_out_rw
    AND (T1 committed OR T3 committed)
```

`T2` is called the **pivot** -- it sits in the middle of the two rw edges.

**Per-transaction state for SSI:**

```
Transaction (SSI extensions) := {
    ...existing fields...
    has_in_rw       : bool,                -- some other txn R read a key that this txn later wrote (R -rw-> this; incoming edge)
    has_out_rw      : bool,                -- this txn read a key that some other txn W later wrote (this -rw-> W; outgoing edge)
    rw_in_from      : HashSet<TxnToken>,   -- (optional) sources of incoming edges
    rw_out_to       : HashSet<TxnToken>,   -- (optional) targets of outgoing edges
    edges_emitted   : Vec<ObjectId>,       -- edges emitted/observed during validation
    marked_for_abort: bool,                -- eager abort optimization
}
```

**Pivot abort rule (normative default):**

After `ssi_validate_and_publish(T)` computes `has_in_rw` and `has_out_rw` for
the committing transaction `T` (§5.7.3), `T` MUST abort if both are true,
unless refinement + merge (§5.10) eliminate one side of the rw evidence.

**Note (deliberate overapproximation):** The formal dangerous structure
definition (above) additionally requires `(T1 committed OR T3 committed)`.
The pivot abort rule omits this check intentionally: at T2's commit time,
the committed status of T1 and T3 may change concurrently (race window).
The conservative rule `has_in_rw AND has_out_rw` is a strict overapproximation
that trades a bounded increase in false positive aborts for the elimination of
a subtle TOCTOU race. The decision-theoretic analysis (below) shows this
overapproximation is cost-effective given the asymmetric loss ratio.

**Eager abort marking (optional optimization):**

When a committing transaction observes an edge that makes some other active
transaction a pivot (both flags true), the observer MAY set
`TxnSlot.marked_for_abort = true` for that pivot. This is an optimization to
abort early (reducing wasted work), not a correctness requirement. Correctness
comes from the pivot abort rule enforced at the pivot's own commit time.

**When to abort T2 (the pivot) vs T3 (the unsafe): Decision-Theoretic Policy**

The abort victim selection policy is not arbitrary; it minimizes the **Expected Loss** of the system.

Let `L(T)` be the cost of aborting transaction `T` (approximated by `T.write_set.len()` + `T.duration`).
We have a potential dangerous structure `T1 -> T2 -> T3`. To break the cycle, we must abort `T2` or `T3`.

**Policy:**
1. **Safety First:** If the cycle is confirmed (T1 and T3 both committed), we *must* abort `T2` (the active pivot). Loss is irrelevant; correctness is mandatory.
2. **Optimistic Victim Selection:** If the cycle is only *potential* (e.g., T1 is active, T3 is committed), we compare expected losses:
   - Option A: Abort T2 now. Cost = `L(T2)`.
   - Option B: Wait. Risk = `P(T1 commits) * Cost(later abort)`.
   - **Alien Rule:** If `L(T2) << L(T3)` (T2 is tiny, T3 is huge), we may preferentially abort T2 *even if it is not yet strictly necessary*, to protect the "heavy" transaction T3 from a future forced abort.

FrankenSQLite uses the conservative approach initially (abort pivot T2) but exposes hook points for this cost-based victim selection.

**PostgreSQL's experience: false positive rate and overhead:**

Based on the PostgreSQL 9.1+ implementation (Ports, 2012):
- **False positive abort rate:** ~0.5% of transactions aborted unnecessarily
  under typical OLTP workloads. This is acceptable because the cost of a
  false positive (retry the transaction) is much lower than the cost of a
  missed anomaly (data corruption).
- **Overhead:** 3–7% throughput reduction on OLTP benchmarks (TPC-C, RUBiS);
  10–20% on synthetic microbenchmarks (SIBENCH) before read-only optimizations
  (Ports & Grittner, VLDB 2012). Overhead comes from maintaining
  *read-dependency evidence* (Postgres: SIREAD locks) and checking for dangerous
  structures. In FrankenSQLite the analogous costs are witness registration,
  hot-index bitset updates, witness object publication, and (optional)
  refinement.
- **Memory:** PostgreSQL's SIREAD lock table grows roughly with
  `active_txns * read_granules`. In FrankenSQLite:
  - hot plane memory is bounded by `TxnSlot` count and hot bucket capacity
    (bitsets over slots, plus overflow bucket),
  - cold plane witness objects are append-only but GC-able by `safe_gc_seq`
    horizons (§5.6.4.8).

**How SSI maps to page granularity in FrankenSQLite:**

SSI at page granularity is coarser than PostgreSQL's row-level SSI. This means:
- **More false positives:** Two transactions that read and write different
  rows on the same page will appear to have an rw-antidependency even if
  they are logically independent. The false positive rate will be higher than
  PostgreSQL's 0.5%.
- **Less overhead:** Fewer SIREAD lock entries (one per page, not one per
  row). The witness-key set is smaller and candidate discovery is cheaper.
- **Mitigation:** Witness refinement + merge (§5.10) can refine page-level
  conflicts to `Cell(btree_root_pgno, cell_tag)` and/or `ByteRange(page, start, len)`,
  reducing false positives while preserving correctness.

**Decision-Theoretic SSI Abort Policy (Alien-Artifact Discipline).**

The abort-vs-commit decision is an instance of expected loss minimization
under posterior uncertainty. Rather than hard-coding the conservative rule
as a boolean, we frame it as a Bayesian decision:

**State space:** For a committing transaction T with `has_in_rw` and
`has_out_rw` both true, the true state `S` is either:
- `S = anomaly`: The dangerous structure represents a genuine serialization
  anomaly. Committing T would violate serializability.
- `S = safe`: The dangerous structure is a false positive (the rw edges are
  at different rows on the same page, or the cycle is broken by commit
  ordering). Aborting T wastes work.

**Loss matrix:**

```
             | commit (a=0)  | abort (a=1)  |
-------------+---------------+--------------+
S = anomaly  |   L_miss      |   0          |
S = safe     |   0           |   L_fp       |
```

where:
- `L_miss` = cost of a missed anomaly (data corruption, silent write skew).
  Extremely high; set to 1000 (arbitrary units).
- `L_fp` = cost of a false positive abort (transaction retried, wasted CPU).
  Low; set to 1 (the retry succeeds on the next attempt almost always).

**Optimal decision:** Abort if:

```
E[Loss | commit] = P(anomaly | evidence) * L_miss + P(safe | evidence) * 0
E[Loss | abort]  = P(anomaly | evidence) * 0     + P(safe | evidence) * L_fp

Abort when E[Loss | commit] > E[Loss | abort]:
  P(anomaly) * L_miss > (1 - P(anomaly)) * L_fp

=> abort if P(anomaly | evidence) > L_fp / (L_fp + L_miss)
=> abort if P(anomaly | evidence) > 1/1001 ≈ 0.001
```

With `L_miss/L_fp = 1000`, the threshold is vanishingly small. This
*mathematically justifies* the conservative approach: even a 0.1% chance of
a genuine anomaly is enough to warrant aborting, because the asymmetry
between data corruption and a retry is enormous.

**Sensitivity analysis (the threshold is robust):**

| L_miss/L_fp | Abort threshold    | Practical effect          |
|-------------|-------------------|---------------------------|
| 10          | 0.091 (9.1%)      | Permissive: allow some risk |
| 100         | 0.0099 (1.0%)     | Still conservative         |
| 1,000       | 0.00099 (0.1%)    | V1 default                 |
| 10,000      | 0.0001 (0.01%)    | Ultra-conservative         |
| 100,000     | 0.00001 (0.001%)  | Paranoid                   |

The threshold is insensitive to the exact loss ratio: varying L_miss/L_fp
across 4 orders of magnitude (100 to 100,000) keeps the threshold below
1%. Since the conservative Page-SSI rule fires on any `has_in_rw &&
has_out_rw` (which implies P(anomaly|evidence) >> 1% for genuine dangerous
structures), the abort decision is the same across the entire reasonable
range. The decision is **robust to mis-specification of the loss ratio**,
which is exactly what the alien-artifact discipline demands: the conclusion
should not depend on precise knowledge of hard-to-estimate quantities.

**Why this matters beyond "just use the conservative rule":**
1. It provides a formal framework for the Layer 3 refinement (Section 2.4,
   bullet 4). When cell/byte-range witness refinement is added (i.e., witnesses
   include `Cell(btree_root_pgno, cell_tag)` and/or `ByteRange(page, start, len)` keys),
   `P(anomaly|evidence)` drops for same-page-different-row conflicts, and the
   decision framework naturally produces fewer aborts without changing the
   threshold.
2. It enables **adaptive victim selection**. If merge (§5.10) resolves the
   apparent conflict to a successful commuting merge,
   the posterior `P(anomaly|evidence)` drops to zero for the write-side
   contribution, and the decision can flip from abort to commit.
3. It makes the abort policy **auditable**: every abort decision can log
   `P(anomaly|evidence)`, the evidence components, and the loss ratio,
   enabling postmortem analysis of abort storms.

**E-process monitoring of SSI false positive rate:**

The SSI false positive rate is monitored as an e-process (INV-SSI-FP):

```rust
// SSI False Positive Rate e-process
let ssi_fp_monitor = EProcess::new("INV-SSI-FP: SSI False Positive Rate",
    EProcessConfig {
        p0: 0.05,        // null: false positive rate <= 5%
        lambda: 0.3,     // moderate bet (page granularity is inherently coarser)
        alpha: 0.01,     // reject at 1% significance
        max_evalue: 1e12,
    });

// On each SSI abort, retrospectively check if it was a true positive
// by replaying the conflicting transactions at row granularity.
// X_t = 1 if the abort was a false positive (row-level replay succeeds
//        without anomaly), 0 if it was a genuine anomaly.
ssi_fp_monitor.observe(is_false_positive);
```

If the e-process exceeds `1/alpha = 100`, the false positive rate is
significantly above the 5% budget. This triggers an alert (not an
automatic response) suggesting that cell/byte-range witness refinement should
be prioritized for the hot pages causing the most false positives.

**Conformal calibration of page-level coarseness overhead:**

The throughput overhead of page-level SSI (relative to row-level) is
bounded using conformal prediction rather than parametric assumptions:

```rust
let coarseness_calibrator = ConformalCalibrator::new(ConformalConfig {
    alpha: 0.05,  // 95% coverage: page-level overhead is within this band
    min_calibration_samples: 30,
});

// Calibrate: run identical workload under row-level (simulated) and
// page-level SSI, measure abort rate difference.
for trial in 0..50 {
    let delta_abort_rate = page_level_abort_rate(trial) - row_level_abort_rate(trial);
    coarseness_calibrator.observe(delta_abort_rate);
}

// At runtime: is the current coarseness penalty within the calibrated band?
let current_delta = measure_current_abort_delta();
assert!(coarseness_calibrator.is_conforming(current_delta),
    "Page-level SSI coarseness penalty ({:.1}%) outside 95% prediction band",
    current_delta * 100.0);
```

This provides a **distribution-free** bound on how much worse page-level
SSI is compared to the theoretical row-level ideal, without assuming any
particular workload distribution.

**PAC-Bayes bound on page-level SSI false positives (harness methodology, recommended):**

The spec must not merely claim "page-level false positives will be higher". The
harness SHOULD produce a quantified, high-probability bound on the page-level
false-positive rate within each BOCPD regime.

Let `X_i = 1` if an SSI abort is classified as a false positive by row-level
replay, and `X_i = 0` otherwise. Treat samples as exchangeable across lab seeds
within a regime. Maintain a prior `P` over the false-positive probability
`p_fp` (e.g., `Beta(α0, β0)`) and a posterior `Q` after observing `n` samples.

Apply a PAC-Bayes bound to obtain an upper bound `p_fp_hi` such that, with
probability at least `1-δ` over the lab sample draw:
- the true regime false-positive rate satisfies `p_fp <= p_fp_hi`.

This bound (and the chosen `(α0, β0, δ)`) MUST be emitted in harness reports
alongside the e-process and conformal results, and it SHOULD gate the default
false-positive budget for `INV-SSI-FP` when sufficient evidence is available.

**Interaction with BEGIN CONCURRENT:**

SSI is an enhancement to `BEGIN CONCURRENT` (Concurrent mode). When SSI is
enabled:
- `BEGIN CONCURRENT` provides SERIALIZABLE isolation (not just SI).
- Applications that previously tolerated write skew under SI will see
  occasional `SQLITE_BUSY_SNAPSHOT` aborts for transactions that would have
  produced non-serializable results.
- `BEGIN` / `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` continue to use Serialized
  mode (global write mutex), which is trivially serializable and does not
need SSI.

#### 5.7.4 Witness Refinement Policy (VOI-Driven, Bounded)

Page-granularity SSI is deliberately conservative. To reduce false positive
aborts without weakening correctness, the witness plane supports **refinement**
to finer witness keys (`Cell`, `ByteRange`, hashed sets, or exact keys).

Refinement has a cost:
- more bytes in `ReadWitness` / `WriteWitness` / `WitnessDelta`
- more encode/decode work
- more candidate confirmation work during commit validation

So refinement MUST be budgeted and targeted.

**Non-negotiable correctness rule:** Refinement is an optimization layer only.
If refinement is disabled or budget-exhausted, the system MUST still be sound
(it may abort more often, but must not miss true conflicts; §5.6.4.1).

##### 5.7.4.1 VOI Model (Expected Loss Minimization)

We choose refinement using **Value of Information (VOI)**:
refine where the expected reduction in false abort cost exceeds the expected
CPU/bytes cost of refinement.

For a given `RangeKey` bucket `b`, define:
- `c_b`: estimated rate of "bucket overlap observations" per unit time
  (how often this bucket participates in candidate conflicts).
- `fp_b`: estimated probability that a bucket overlap is a false positive
  at page granularity (no true key intersection).
- `Δfp_b`: estimated reduction in false positive probability if we refine
  this bucket (page → cell/byte-range, or add key summaries).
- `L_abort`: expected cost of aborting a transaction (duration + write set
  cost; measured and tracked).
- `Cost_refine_b`: bytes + CPU cost to emit and later decode refinement for `b`.

Then the VOI score is:

```
Benefit_b = c_b * Δfp_b * L_abort
VOI_b     = Benefit_b - Cost_refine_b
```

**Rule:** The engine SHOULD refine buckets with `VOI_b > 0`, subject to a per-txn
refinement budget (bytes + CPU) derived from the commit budget (`Cx::budget`).

##### 5.7.4.2 Practical Policy (V1 Defaults)

1. **Always register Page keys:** Hot index is always updated at `Page(pgno)` so
   candidate discoverability is never lost.
2. **Emit refined keys only for hotspots:** Maintain per-bucket statistics from:
   - `INV-SSI-FP` (false positive rate monitor; §5.7.3)
   - conflict heatmaps (`DependencyEdge` aggregation; bucket frequency)
   - merge outcomes (`MergeWitness` success rate by bucket/page)
3. **Refine in descending VOI order** until `refinement_budget_bytes` is exhausted.
4. **Refinement types priority (recommended):**
   - `CellBitmap` (best for B-tree leaf/interior ops when cell tags exist)
   - `ByteRangeList` (best when page patches are sparse/disjoint)
   - `HashedKeySet` (medium: cheaper than exact keys, good for large sets)
   - `ExactKeys` (only for tiny sets; most precise)

##### 5.7.4.3 How Refinement Is Published (Objects + Hot Plane)

Refinement MUST appear only in durable ECS objects:
- `ReadWitness.key_summary` / `WriteWitness.key_summary` for the refined set
- and/or `WitnessDelta.refinement` for compact per-bucket participation updates

Hot-plane `HotWitnessIndex` remains bucket participation only (bitsets).
Refinement is consulted only after candidate discovery (cold-plane decode),
and only to reduce false positives.

##### 5.7.4.4 Explaining Refinement Decisions (Evidence Ledger)

When refinement is enabled, the commit pipeline SHOULD emit an evidence ledger
entry (§4.16.1) showing:
- which buckets were refined,
- the VOI scores and budget constraints,
- which candidate conflicts were eliminated by refinement,
- and whether merge (§5.10) tightened witness precision.

### 5.8 Conflict Detection and Resolution Detail

**Page lock table implementation:**

```rust
const LOCK_TABLE_SHARDS: usize = 64;  // power of two for fast modular arithmetic

pub struct PageLockTable {
    shards: [parking_lot::Mutex<HashMap<PageNumber, TxnId>>; LOCK_TABLE_SHARDS],
}

impl PageLockTable {
    fn shard(&self, pgno: PageNumber) -> &parking_lot::Mutex<HashMap<PageNumber, TxnId>> {
        &self.shards[pgno.get() as usize & (LOCK_TABLE_SHARDS - 1)]
    }

    /// Attempt to acquire exclusive lock on a page.
    /// Returns Ok(()) if acquired or already held by this txn.
    /// Returns Err(SQLITE_BUSY) if held by another txn.
    pub fn try_acquire(&self, pgno: PageNumber, txn_id: TxnId) -> Result<()> {
        let mut table = self.shard(pgno).lock();
        match table.entry(pgno) {
            Entry::Vacant(e) => {
                e.insert(txn_id);
                Ok(())
            }
            Entry::Occupied(e) => {
                if *e.get() == txn_id {
                    Ok(())  // idempotent re-acquire
                } else {
                    Err(FrankenError::Busy)  // held by another transaction
                }
            }
        }
    }

    /// Release a page lock. Panics if not held by this txn.
    pub fn release(&self, pgno: PageNumber, txn_id: TxnId) {
        let mut table = self.shard(pgno).lock();
        match table.entry(pgno) {
            Entry::Occupied(e) if *e.get() == txn_id => {
                e.remove();
            }
            _ => panic!("releasing lock not held by txn {}", txn_id),
        }
    }

    /// Release all locks held by a transaction.
    /// Iterates the per-transaction lock set, touching only relevant shards.
    pub fn release_all(&self, locks: &HashSet<PageNumber>, txn_id: TxnId) {
        // Iterate per-page; each page touches its own shard lock.
        // A production implementation MAY group by shard to reduce lock acquisitions.
        for pgno in locks {
            let mut table = self.shard(*pgno).lock();
            if let Entry::Occupied(e) = table.entry(*pgno) {
                if *e.get() == txn_id {
                    e.remove();
                }
            }
        }
    }
}
```

Note: `release_all` iterates the per-transaction lock set (typically tens
of entries), not the entire lock table (which could be thousands of entries
under high concurrency). This is O(W) where W is the transaction's write
set size, not O(L) where L is total locked pages.

**Commit validation algorithm:**

The first-committer-wins check determines whether any page in the committing
transaction's write set was also modified by a transaction that committed
after the snapshot was taken.

```
validate_commit(T, commit_index) -> Result<()>:
    // A commit is "after" our snapshot iff it has commit_seq > snapshot.high.
    //
    // Therefore FCW reduces to: for every page we wrote, ensure the latest
    // committed writer of that page is not newer than our snapshot.
    for pgno in T.write_set.keys():
        if commit_index.latest_commit_seq(pgno) > T.snapshot.high:
            // Attempt deterministic merge/rebase (Section 5.10).
            if algebraic_merge_possible(T, pgno):
                perform_merge(T, pgno)
            else:
                return Err(SQLITE_BUSY_SNAPSHOT)  // retryable conflict

    Ok(())  // no conflicts, commit proceeds
```

**Interaction between Serialized and Concurrent mode transactions:**

Serialized mode exists for strict SQLite behavioral compatibility: it MUST
provide single-writer semantics. Therefore, a Serialized-mode writer is
exclusive with respect to Concurrent-mode writers.

**Normative rules:**

- While a Serialized-mode transaction is Active (holding the global write mutex):
  - Concurrent transactions MAY `BEGIN` and may read normally.
  - Any Concurrent-mode attempt to acquire a page write lock MUST fail with
    `SQLITE_BUSY` (or wait under the configured busy-timeout), because allowing
    concurrent writers would violate the SQLite single-writer contract.
- While any Concurrent-mode writer is Active (holds any page locks):
  - Acquiring the Serialized writer exclusion (i.e., `BEGIN IMMEDIATE`,
    `BEGIN EXCLUSIVE`, or DEFERRED upgrade on first write) MUST fail with
    `SQLITE_BUSY` (or wait under busy-timeout). It MUST NOT proceed to write
    without excluding Concurrent writers. (DEFERRED read-only begins remain
    permitted; only the writer upgrade is excluded.)

**Implementation hook (cross-process):** The shared-memory coordination region
maintains a single `serialized_writer` indicator (token + lease) that is set
when a Serialized transaction acquires writer exclusion (at `BEGIN IMMEDIATE /
EXCLUSIVE` or at DEFERRED upgrade on first write) and is cleared at commit/abort.
Concurrent-mode write paths MUST check this indicator before acquiring page locks.

This design avoids a correctness pitfall where a Serialized writer could modify
pages without participating in page-level exclusion, which would undermine
First-Committer-Wins and make conflict behavior timing-dependent.

**External interop hook (Compatibility mode):** Concurrent-mode exclusion is
meaningless if a legacy SQLite writer can bypass `.fsqlite-shm` entirely.
Therefore, Compatibility mode with `foo.db.fsqlite-shm` MUST exclude legacy
writers via the Hybrid SHM protocol (§5.6.6.1, §5.6.7). It is forbidden to run
multi-writer MVCC while legacy writers are permitted.

### 5.9 Write Coordinator Detail

The write coordinator is a single background task that serializes the **commit
sequencing** critical section. Its responsibilities differ by operating mode:

- **Compatibility mode (WAL path):** The coordinator serializes validation,
  WAL append, fsync/group-commit, version publishing, and commit-log insertion.
- **Native mode (ECS path):** The coordinator is a **tiny-marker sequencer**:
  it never moves page payload bytes. Writers persist `CommitCapsule` objects
  (bulk I/O) concurrently; the coordinator validates, allocates `commit_seq`,
  persists a small `CommitProof`, and appends a tiny `CommitMarker` (§7.11).

This split is structural: it prevents "one sequencing thread moves all bytes"
from becoming the scalability ceiling on modern NVMe.

**Multi-process note (normative):** In a multi-process deployment, "the
coordinator" is a **role**, not necessarily a thread in every process:
- Exactly one process MUST hold the coordinator role for a database at a time
  (lease-backed; same posture as TxnSlot leases).
- In Compatibility mode with `foo.db.fsqlite-shm` (Hybrid SHM protocol, §5.6.7),
  this is REQUIRED to uphold the legacy writer exclusion lock (§5.6.6.1): the
  coordinator holds `WAL_WRITE_LOCK` for its lifetime and sequences WAL appends.
- Other processes MUST route commit publication through the coordinator (shared
  memory two-phase queue; asupersync reserve/commit discipline). The in-process
  channel examples below define the **message schemas**; the transport differs.

#### 5.9.1 Native Mode Sequencer (Tiny Marker Path)

**State machine (native mode):**

```
                     +-------+
            +------->| Idle  |<----------+
            |        +-------+           |
            |            |               |
            |  recv(PublishRequest)      |
            |            |               |
            |            v               |
            |      +-----------+         |
            |      | Validate  |         |
            |      +-----------+         |
            |        |       |           |
            |   pass |       | fail      |
            |        v       v           |
            |  +-----------+  +-------+  |
            |  | Seq+Proof |  | Abort |--+
            |  +-----------+  +-------+
            |        |
            |  +-----------+
            |  | Marker IO |
            |  +-----------+
            |        |
            |  respond(Ok)
            |        |
            +--------+

Validate:   First-committer-wins + any global constraints using write-set summaries
Seq+Proof:  Allocate commit_seq; publish CommitProof (small ECS object)
Marker IO:  Append CommitMarker (tiny) to marker stream (atomic visibility point)
```

**PublishRequest (native mode):**

```rust
pub struct PublishRequest {
    pub txn: TxnToken,
    pub begin_seq: u64,
    pub capsule_object_id: ObjectId,
    pub capsule_digest: [u8; 32],        // e.g., BLAKE3-256 of capsule bytes (audit/sanity)
    pub write_set_summary: RoaringBitmap<u32>, // page numbers (no false negatives)
    pub read_witnesses: Vec<ObjectId>,
    pub write_witnesses: Vec<ObjectId>,
    pub edge_ids: Vec<ObjectId>,
    pub merge_witnesses: Vec<ObjectId>,
    pub abort_policy: AbortPolicy,
    pub response_tx: oneshot::Sender<PublishResponse>,
}

pub enum PublishResponse {
    Ok { commit_seq: u64, marker_object_id: ObjectId },
    Conflict { conflicting_pages: Vec<PageNumber>, conflicting_commit_seq: u64 },
    Aborted { code: ErrorCode }, // e.g., SQLITE_BUSY_SNAPSHOT, SQLITE_INTERRUPT
    IoError { error: FrankenError },
}
```

**Critical rule:** In native mode the coordinator MUST NOT decode the full
capsule during validation; it operates on `write_set_summary` and coordinator
indexes. This is required for scalability and for keeping the serialized
section "tiny."

#### 5.9.2 Compatibility Mode Coordinator (WAL Path)

**Full state machine for the coordinator:**

```
                     +-------+
            +------->| Idle  |<----------+
            |        +-------+           |
            |            |               |
            |  recv(CommitRequest)       |
            |            |               |
            |            v               |
            |      +-----------+         |
            |      | Validate  |         |
            |      +-----------+         |
            |        |       |           |
            |   pass |       | fail      |
            |        v       v           |
            |  +---------+  +-------+    |
            |  |WALAppend|  | Abort |----+
            |  +---------+  +-------+
            |        |          ^
            |   sync |          | I/O error
            |        v          |
            |   +---------+    |
            |   | Publish |--->+
            |   +---------+
            |        |
            |  respond(Ok)
            |        |
            +--------+

States:
  Idle:       Waiting for next CommitRequest on MPSC channel.
  Validate:   Running first-committer-wins check on the request's write set.
  WALAppend:  Writing page frames to WAL file (systematic only); enqueue FEC job.
  Publish:    Inserting versions into version store and commit record into commit log.
  Abort:      Notifying the requester of failure; cleaning up partial state.
```

**Compatibility-mode CommitRequest and CommitResponse types:**

```rust
/// Sent by a committing transaction to the write coordinator (compatibility/WAL path).
pub struct CommitRequest {
    /// Transaction ID of the committing transaction.
    pub txn_id: TxnId,
    /// Transaction mode (Serialized or Concurrent).
    pub mode: TxnMode,
    /// Pages to be committed: page number -> new page data.
    pub write_set: HashMap<PageNumber, PageData>,
    /// Intent log for deterministic rebase merge (Section 5.10).
    pub intent_log: Vec<IntentOp>,
    /// Page locks held (for release after commit).
    pub page_locks: HashSet<PageNumber>,
    /// Snapshot of the committing transaction (for validation).
    pub snapshot: Snapshot,
    /// SSI state: has_in_rw and has_out_rw flags (pre-checked by caller,
    /// but coordinator may re-validate if needed).
    pub has_in_rw: bool,
    pub has_out_rw: bool,
    /// Snapshot of the WAL FEC policy for this commit group (Section 3.4.1).
    /// Captured at BEGIN/COMMIT time so policy changes cannot race the encoder.
    pub wal_fec_r: u8,
    /// Oneshot channel for the coordinator's response.
    pub response_tx: oneshot::Sender<CommitResponse>,
}

/// Sent by the write coordinator back to the committing transaction (compatibility/WAL path).
pub enum CommitResponse {
    /// Commit succeeded. All versions published, WAL synced.
    Ok {
        /// WAL offset where the commit record was written.
        wal_offset: u64,
        /// Commit sequence number (monotonically increasing).
        commit_seq: u64,
    },
    /// Commit failed due to a page conflict.
    Conflict {
        /// The page(s) that conflicted.
        conflicting_pages: Vec<PageNumber>,
        /// The transaction that already committed the conflicting page(s).
        conflicting_txn: TxnId,
    },
    /// Commit failed due to an I/O error during WAL append.
    IoError {
        error: FrankenError,
    },
}
```

**Throughput model with derivation:**

The coordinator processes commits sequentially. Each commit involves:

1. **Validation**: Check CommitIndex for first-committer-wins conflicts.
   - Cost: O(W) where W = write set size. Each page requires one
     CommitIndex hash lookup: ~50ns. Typical: W = 10 pages. Total: 10 * 50ns = 500ns.
   - Let `T_validate` denote this cost.

2. **WAL append (systematic)**: Write page frames sequentially to `.wal`, then
   `fsync` (durable). Repair symbols are pipelined to a background encoder
   thread (§3.4.1) and MUST NOT extend the WAL write critical section.
   - Cost: W frames * (24 bytes header + page_size data).
   - Typical: 10 frames * 4120 bytes = ~40KB sequential write.
   - SSD sequential write throughput: ~2 GB/s. Time: 40KB / 2GB/s = 20us.
   - Plus fsync: ~50us on modern NVMe (group commit amortizes this).
   - Let `T_wal` denote the synchronous `.wal` cost.

   **Background FEC cost (out of critical path):** Generating and appending `.wal-fec`
   repair symbols consumes CPU and sequential write bandwidth but is not included
   in `T_commit`. The system MUST bound and monitor "repair lag" (time from durable
   commit to repairable group) and prioritize encoder catch-up under sustained load.

3. **Version publishing + commit log**: In-memory operations.
   - Cost: O(W) hash insertions. Typical: 10 * 100ns = 1us.
   - Let `T_publish` denote this cost.

Total per-commit latency:
```
T_commit = T_validate + T_wal + T_publish
         = 0.5us + 70us + 1us
         = ~71.5us
```

Throughput (single coordinator, no batching):
```
Throughput = 1 / T_commit = 1 / 71.5us ~ 14,000 commits/sec
```

With group commit batching (amortize fsync across N concurrent commits):
```
T_commit_batched = T_validate + T_wal_write + T_fsync/N + T_publish
                 = 0.5us + 20us + 50us/N + 1us

For N = 10:  T_commit = 26.5us  -> Throughput ~ 37,700 commits/sec
For N = 50:  T_commit = 22.5us  -> Throughput ~ 44,400 commits/sec
```

**Batching optimization: coalescing multiple commits into a single WAL sync:**

The coordinator implements group commit to amortize the fsync cost:

```
Coordinator main loop (with batching):

loop:
    // Drain all available requests (non-blocking after first)
    batch = Vec::new()
    first_request = commit_channel.recv().await   // blocking wait for first
    batch.push(first_request)

    // Drain additional pending requests (non-blocking)
    while let Ok(request) = commit_channel.try_recv():
        batch.push(request)
        if batch.len() >= MAX_BATCH_SIZE:
            break

    // Phase 1: Validate all requests in the batch
    valid = Vec::new()
    for request in batch:
        match validate(request):
            Ok(()) => valid.push(request),
            Err(conflict) => request.response_tx.send(CommitResponse::Conflict(conflict)),

    // Phase 2: Append all valid commits to WAL (one sequential write)
    wal_offsets = wal.append_batch(&valid)   // single write() call for all frames

    // Phase 3: Single fsync for the entire batch
    wal.sync()

    // Phase 4: Publish all versions and respond
    for (request, offset) in valid.iter().zip(wal_offsets):
        publish_versions(request)
        insert_commit_record(request)
        request.response_tx.send(CommitResponse::Ok { wal_offset: offset, ... })
```

The batching optimization transforms the throughput model from:

```
Without batching:  N commits * (T_write + T_fsync) = N * 70us
With batching:     N * T_write + 1 * T_fsync = N * 20us + 50us
```

For a batch of 10 commits: 250us total vs 700us, a 2.8x improvement. The
larger the batch (more concurrent committers), the greater the amortization
benefit. This is the standard group commit optimization used by PostgreSQL,
MySQL InnoDB, and other production databases.

**Interaction with the two-phase MPSC channel:**

The write coordinator receives `CommitRequest` messages from the MPSC channel's
receiver end (`rx`). The bounded capacity of the channel (default: 16) provides
natural batching: when the coordinator is busy processing a batch, new commit
requests accumulate in the channel buffer. When the coordinator finishes and
calls `try_recv()` to drain pending requests, it collects all buffered requests
into the next batch.

If the channel buffer fills up (16 in-flight commits), additional committers
block on `tx.reserve(cx).await`, which provides backpressure. This prevents
unbounded memory growth from write set buffering and naturally rate-limits
the commit pipeline when the WAL I/O is the bottleneck.

### 5.10 Safe Write Merging and Intent Logs

Page-level MVCC can conflict on hot pages (B-tree root, internal nodes during
splits, hot leaf pages). Safe write merging reduces false conflicts
**without** upgrading to row-level MVCC metadata (which would break file format
and cost space).

**The insight:** Many "same-page conflicts" in B-tree workloads involve
logically independent operations (e.g., two inserts into distinct keys that
happen to land on the same leaf page). Instead of treating these as fatal
conflicts, we attempt to **merge** them.

**Two merge planes:**

1. **Logical plane (preferred):** Merge *intent-level* B-tree operations that
   commute (e.g., inserts into distinct keys).
2. **Physical plane (fallback):** Merge *structured page patches* keyed by
   stable identifiers (e.g., `cell_key_digest`) with explicit invariant checks.
   Raw byte-disjoint XOR merge is forbidden for SQLite structured pages (§3.4.5).

#### 5.10.1 Intent Logs (Semantic Operations)

Each writing transaction records an `intent_log: Vec<IntentOp>` alongside its
materialized page deltas. Intent operations are:

```
IntentOp := {
  // The schema epoch captured at transaction begin. This prevents replaying
  // semantic intents against a different schema/physical layout.
  schema_epoch: u64,

  // A semantic footprint for this intent op, used to justify (or forbid)
  // deterministic rebase / merge by construction.
  footprint: IntentFootprint,

  op: IntentOpKind,
}

IntentFootprint := {
  // Semantic reads that the correctness of this op depends on (e.g., predicate
  // reads, uniqueness checks). Structural reads (B-tree descent, page layout)
  // do not belong here; those are accounted for by `structural` (below).
  reads : Vec<SemanticKeyRef>,

  // Semantic writes performed by this op (the logical keys it creates/updates/deletes).
  writes: Vec<SemanticKeyRef>,

  // Structural side-effects that make the op non-commutative (split/merge/overflow/freelist).
  structural: StructuralEffects,
}

SemanticKeyRef := {
  // Stable identifier of the logical object being accessed.
  btree: { TableId | IndexId },
  kind : { TableRow, IndexEntry },

  // 128-bit stable digest of the key bytes with domain separation:
  // key_digest = Trunc128(BLAKE3("fsqlite:btree:key:v1" || kind || btree_id || canonical_key_bytes))
  key_digest: [u8; 16],
}

StructuralEffects := bitflags {
  NONE               = 0,
  PAGE_SPLIT         = 1 << 0,
  PAGE_MERGE         = 1 << 1,
  BALANCE_MULTI_PAGE = 1 << 2,
  OVERFLOW_ALLOC     = 1 << 3,
  OVERFLOW_MUTATE    = 1 << 4,
  FREELIST_MUTATE    = 1 << 5,
  POINTER_MAP_MUTATE = 1 << 6,
  DEFRAG_MOVE_CELLS  = 1 << 7,
}

// 0-based column index in the table schema (as used by the VDBE `Column` opcode).
ColumnIdx := u16

IntentOpKind ::=
  | Insert { table: TableId, key: RowId, record: Vec<u8> }
  | Delete { table: TableId, key: RowId }
  | Update { table: TableId, key: RowId, new_record: Vec<u8> }
  | IndexInsert { index: IndexId, key: Vec<u8>, rowid: RowId }
  | IndexDelete { index: IndexId, key: Vec<u8>, rowid: RowId }
  | UpdateExpression {
      table: TableId,
      key: RowId,
      column_updates: Vec<(ColumnIdx, RebaseExpr)>,
  }

// Simplified, serializable expression AST for replayable column updates.
// Each variant is a pure, deterministic computation that can be re-evaluated
// against a different base row during rebase.
RebaseExpr ::=
  | ColumnRef(ColumnIdx)              // read current value of column N from the base row
  | Literal(SqliteValue)              // constant: integer, real, text, blob, null
  | BinaryOp {
      op: { Add | Sub | Mul | Div | Rem | BitAnd | BitOr | ShiftL | ShiftR
          | Eq | Ne | Lt | Le | Gt | Ge | And | Or },
      lhs: Box<RebaseExpr>,
      rhs: Box<RebaseExpr>,
  }
  | UnaryOp {
      op: { Neg | BitNot | Not },
      operand: Box<RebaseExpr>,
  }
  | FunctionCall {
      name: String,                  // canonical uppercase; MUST be deterministic (§8.2)
      args: Vec<RebaseExpr>,
  }
  | Cast {
      operand: Box<RebaseExpr>,
      target_affinity: TypeAffinity,
  }
  | Case {
      operand: Option<Box<RebaseExpr>>,
      when_clauses: Vec<(RebaseExpr, RebaseExpr)>,
      else_clause: Option<Box<RebaseExpr>>,
  }
  | Coalesce(Vec<RebaseExpr>)         // COALESCE(a, b, ...): first non-NULL
  | NullIf {
      lhs: Box<RebaseExpr>,
      rhs: Box<RebaseExpr>,
  }
  | Concat {                          // || operator: text concatenation
      operands: Vec<RebaseExpr>,
  }
```

**Expression safety analysis (normative):**

```
fn expr_is_rebase_safe(expr: &Expr) -> Option<RebaseExpr>
```

Walks the resolved AST expression tree and attempts to lower it into a `RebaseExpr`.
Returns `None` (rejecting the expression) if any of the following are encountered:
- Subqueries (scalar, `EXISTS`, `IN (SELECT ...)`)
- Non-deterministic functions (any `ScalarFunction` where `is_deterministic()` returns
  `false`; see §8.2 line ~9573)
- Aggregate functions or window functions
- Correlated column references (references to tables other than the UPDATE target)
- `RANDOM()`, `LAST_INSERT_ROWID()`, or any function with session/connection state dependency
- User-defined functions not registered with the `SQLITE_DETERMINISTIC` flag

When `expr_is_rebase_safe` returns `Some(rebase_expr)`, the expression is guaranteed
to be a pure function of the target row's column values and constants, and can be
safely re-evaluated against any base row version.

Intent logs are *small* (typically tens of entries) and encode/replicate
efficiently as ECS objects. They are the preferred merge substrate because
they carry semantic information that byte-level patches lack.

`schema_epoch` is captured at `BEGIN` from `RootManifest.schema_epoch` (or the
shared-memory mirror `SharedMemoryLayout.schema_epoch`) and stored in the
transaction snapshot. Every `IntentOp` MUST carry that snapshot epoch. Any
attempt to replay semantic intents across a schema epoch boundary MUST abort
with `SQLITE_SCHEMA` (see §5.10.4).

#### 5.10.2 Deterministic Rebase (The Big Win)

When a txn `U` reaches commit and discovers a page in `write_set(U)` has been
updated since its snapshot, we attempt **deterministic rebase**:

1. **Schema epoch check (required):** If `current_schema_epoch != U.snapshot.schema_epoch`,
   abort with `SQLITE_SCHEMA` (cannot rebase across DDL/VACUUM).
2. **Detect base drift:** `base_version(pgno)` for U's write set changed since
   its snapshot.
3. **Attempt rebase:** Take U's intent log and replay it against the *current*
   committed snapshot, producing new page deltas.
4. **If replay succeeds** without violating B-tree invariants or constraints:
   commit proceeds with the rebased page deltas.
5. **If replay fails** (true conflict, constraint violation): abort/retry.

**Safety Constraint (Refined Read-Dependency Check):** Rebase safety depends on
distinguishing two categories of reads:

- **Blocking reads:** Reads recorded in `footprint.reads` — values the transaction
  consumed for decisions NOT captured in replayable expressions. A blocking read
  creates a stale dependency on the snapshot base. If any `IntentOp.footprint.reads`
  is non-empty, rebase MUST NOT proceed (replaying against a different base creates
  a Lost Update / Write Skew).
- **Expression reads:** Column reads embedded in `RebaseExpr` within an
  `UpdateExpression` intent. These are NOT recorded in `footprint.reads` because
  the read is captured in the expression AST itself. During rebase, the expression
  is re-evaluated against the new committed base row, so no stale dependency exists.

**Rebase rule (normative):** Rebase proceeds when ALL of:
1. `footprint.reads` is empty for every `IntentOp` in the transaction's intent log, AND
2. `footprint.structural == NONE` for every `IntentOp`.

`UpdateExpression` ops do NOT add their implicit column reads to `footprint.reads`
because the reads are captured in `RebaseExpr` and will be replayed. The compiler
MUST ensure this invariant (see codegen rules below).

**Rebase algorithm for `UpdateExpression` (normative):**

For each `UpdateExpression { table, key, column_updates }` in the intent log during
rebase replay:
1. Read the target row from the new committed base by `key` (rowid lookup).
2. If the row was deleted in the new base → abort (true conflict; the row no longer
   exists and the expression cannot be evaluated).
3. For each `(col_idx, rebase_expr)` in `column_updates`: evaluate `rebase_expr`
   against the new base row's column values. `ColumnRef(i)` resolves to column `i`
   of the new base row, not the original snapshot row.
4. Type affinity coercion follows standard SQLite rules (§3.1 of the SQLite file
   format documentation). NULL propagation follows SQL semantics.
5. Produce the updated row record from the new base row with the evaluated column
   updates applied. Emit as a page delta.
6. **Constraint checks (normative):** The replay engine MUST enforce the same
   row-level constraint semantics as normal execution for the updated row:
   - **NOT NULL** constraints for the target table.
   - **CHECK** constraints for the target table (a CHECK that evaluates to
     false fails; true or NULL passes, per SQLite semantics).
   If any constraint fails, rebase MUST abort (true conflict/violation).
7. **Index regeneration (critical):** Any `IndexDelete`/`IndexInsert` ops in the
   original intent log that are associated with this `UpdateExpression` (same
   `table`, same `rowid`) carry stale key bytes derived from the original snapshot
   and MUST be discarded during rebase. Instead, the rebase engine MUST regenerate
   index operations from the schema:
   a. For each secondary index covering any column in `column_updates`:
      compute the old index key from the *new base row's* column values and the
      new index key from the *updated row's* column values.
   b. If old key ≠ new key: emit `IndexDelete(index, old_key, rowid)` followed by
      `IndexInsert(index, new_key, rowid)`.
   c. If old key == new key: no index operation needed (the column change did not
      affect this index's key).
   The rebase engine has access to the schema (needed for affinity coercion in
   step 4) and MUST use it to enumerate affected indexes.

**VDBE codegen rules for `UpdateExpression` emission (normative):**

The code generator emits an `UpdateExpression` intent (instead of a materialized
`Update` with the row read in `footprint.reads`) when ALL of:
- The target table has no triggers (BEFORE/AFTER/INSTEAD).
- **Foreign keys (V1 restriction):** The target table MUST NOT participate in
  any foreign key constraints (as child or parent). Foreign key enforcement
  requires additional semantic reads/writes that are not represented in
  `RebaseExpr` and would otherwise be bypassed by commit-time replay. If any
  foreign keys apply, the statement MUST fall back to a materialized `Update`.
- **CHECK constraints (V1 restriction):** If the target table has CHECK
  constraints, each CHECK expression MUST be accepted by `expr_is_rebase_safe()`
  so it can be re-evaluated deterministically during replay (step 6 above).
  Otherwise, fall back to a materialized `Update`.
- The WHERE clause resolves to a point lookup by rowid or integer primary key
  (not a range scan, not a secondary index scan with multiple candidates).
- No SET clause targets the rowid or INTEGER PRIMARY KEY column. Modifying the
  primary key changes the row's position in the B-tree (semantically a DELETE +
  INSERT) and cannot be expressed as a column-level `UpdateExpression`.
- All SET expressions pass `expr_is_rebase_safe()` (§5.10.1): pure, deterministic,
  no subqueries, no non-deterministic functions.
- The transaction has no prior explicit read of the same row (via SELECT or
  otherwise) that would have already recorded a `SemanticKeyRef` in
  `footprint.reads` for this key. If such a read exists, the stale-dependency
  is already established and `UpdateExpression` cannot remove it.

Otherwise, the VDBE falls back to a materialized `Update` with the row read
recorded in `footprint.reads` (blocking rebase for this transaction).

This is "merge by re-execution", not "merge by bytes". It gives us *row-level
concurrency effects* without storing row-level MVCC metadata.

**Determinism requirement:** The replay engine MUST be deterministic for a
given `(intent_log, base_snapshot)`. Under `LabRuntime`, identical inputs yield
identical outputs across all seeds. No dependence on wall-clock, iteration
order, or hash randomization.

**Compatibility note (byte layout):** Rebase output pages MUST be valid SQLite
file format and pass post-merge invariant checks. They are NOT required to be
byte-identical to what C SQLite would have produced for the same logical
operations. Conformance is defined on observable behavior (query results and
integrity checks), not on matching legacy cell-placement heuristics (see risk
R6, §21).

**Structural scope restriction (normative):** Deterministic rebase is permitted
only for a restricted, proven-safe subset of B-tree operations. A rebase attempt
MUST reject (and fall back to the next merge ladder step, §5.10.4) if replay
would require any of:
- page split/merge/balance across multiple pages,
- overflow allocation or overflow chain mutation,
- freelist trunk/leaf mutation beyond the leaf page itself, or
- any non-deterministic tie-breaking (HashMap iteration, wall-clock time).

These are correctness constraints (not version limitations): the structural
operations above interact with global page-allocation state that cannot be safely
replayed against a different base without full B-tree re-traversal.

#### 5.10.3 Physical Merge: Structured Page Patches

Physical merge is the fallback when a commit sees **base drift** (FCW conflict)
and deterministic rebase (§5.10.2) is not applicable or does not succeed.

**Encoding vs correctness:** Pages and deltas are still byte vectors and may be
encoded using XOR/`GF(256)` deltas (useful for history compression). However,
for SQLite file-format pages, **merge eligibility is never decided by raw
byte-range disjointness** (§3.4.5).

Instead, physical merge is expressed as a `StructuredPagePatch` whose operations
are keyed by stable identifiers rather than physical offsets.

**Implementation model (normative):** For SQLite structured pages, physical merge
MUST be implemented as `parse -> merge -> repack` (deterministic). It MUST NOT be
implemented as "apply two byte patches to the same base page", even when the
byte ranges appear disjoint.

**Lens law (normative):** Let `parse_k` / `repack_k` be the parser and canonical
repacker for SQLite page kind `k` (e.g., B-tree leaf table). SAFE physical merge
MUST operate on the parsed object:

```
obj_base = parse_k(bytes_base)
obj'     = merge_obj(obj_base, patch_a, patch_b, ...)   // semantic keys, not offsets
bytes'   = repack_k(obj')                               // canonical layout
```

The repacker MUST be canonical: `repack_k(parse_k(bytes))` yields a canonical
layout that is stable across processes and replays for equivalent semantic
content (no "layout by chance").

In SAFE mode, `StructuredPagePatch` for B-tree leaf pages MUST contain only
semantic cell operations (`cell_ops` keyed by `cell_key_digest`). Header and
free-space layout is derived by repacking; patches MUST NOT encode physical
cell offsets as merge inputs.

**StructuredPagePatch (normative representation):**

```
StructuredPagePatch {
  header_ops: Vec<HeaderOp>,         -- derived during repack (SHOULD be empty for SAFE B-tree leaf merges)
  cell_ops: Vec<CellOp>,            -- mergeable when disjoint by cell_key
  free_ops: Vec<FreeSpaceOp>,       -- derived during repack (SHOULD be empty for SAFE B-tree leaf merges)
  raw_xor_ranges: Vec<RangeXorPatch>, -- forbidden for SQLite structured pages; debug-only for opaque pages
}
```

`cell_ops` are keyed by a stable identifier (`cell_key_digest` derived from
rowid/index key), not by raw offsets. This enables safe merges even when the
page layout shifts during a concurrent split.

**Normative safety constraints:**

1. For any SQLite file-format structured page kind (including B-tree, overflow,
   freelist, pointer-map), `raw_xor_ranges` MUST be empty under all SAFE builds
   and under `PRAGMA fsqlite.write_merge = SAFE`.
2. `raw_xor_ranges` MAY be used only for pages explicitly designated **opaque**
   by the engine (not SQLite file-format pages), and only when
   `PRAGMA fsqlite.write_merge = LAB_UNSAFE`. This is a lab/debug facility, not
   a correctness mechanism.
3. A `StructuredPagePatch` merge MUST treat `header_ops` as non-commutative:
   if both patches include header mutations that cannot be serialized without
   ambiguity, the merge MUST reject and fall back to abort/retry.
4. `free_ops` are conservative: if either patch includes non-empty `free_ops`,
   the merge MUST reject unless the implementation can prove safe composition
   by construction (provable via proptest over randomized free-list states).

#### 5.10.4 Commit-Time Merge Policy (Strict Safety Ladder)

When txn `U` reaches commit, for each page in `write_set(U)`:

1. If base unchanged since snapshot → OK (no merge needed).
2. Else, apply `PRAGMA fsqlite.write_merge`:
   - `OFF`: Abort/retry (strict FCW).
   - `SAFE`: Attempt merge in strict priority order:
     1. **Schema epoch check (required):** If `current_schema_epoch != U.snapshot.schema_epoch`,
        abort with `SQLITE_SCHEMA` (merging across DDL/VACUUM boundaries is forbidden).
     2. **Deterministic rebase replay** (preferred):
        - MUST verify `U` has no `ReadWitness` covering this page/key (see §5.10.2).
        - If safe, replay `IntentOp` against current base. For `UpdateExpression`
          ops, re-evaluate column expressions against the new base row (§5.10.2).
        - Handles both pure blind writes (`Insert`/`Delete`/`Update` with empty
          `footprint.reads`) and expression-based updates (`UpdateExpression`).
     3. **Structured page patch merge** (if ops are disjoint by semantic key, e.g., `cell_key_digest`)
     4. **Abort/retry** (no safe merge found)
   - `LAB_UNSAFE`: Perform the SAFE ladder above. If it fails, the engine MAY
     additionally merge `raw_xor_ranges` for explicitly-opaque pages only; it
     MUST still reject raw XOR merging for SQLite structured pages (§3.4.5).

This yields a strict safety ladder: we only take merges we can justify.

#### 5.10.5 What Must Be Proven

Runnable proofs (proptest + DPOR), not prose:

- **B-tree invariants** hold after replay/merge: ordering, cell count bounds,
  free space accounting, overflow chain validity.
- **Patch algebra invariants:** `apply(p, merge(a,b)) == apply(apply(p,a), b)`
  when mergeable. Commutativity for declared commutative ops.
- **Determinism:** Identical `(intent_log, base_snapshot)` yields identical
  replay outcome under `LabRuntime` across seeds.
- **UpdateExpression determinism:** `evaluate_rebase_expr(expr, row)` is
  deterministic for a given `(expr, row)` pair. Verified by proptest over
  randomized `RebaseExpr` trees and row values.
- **Expression safety:** `expr_is_rebase_safe` correctly rejects all
  non-deterministic and side-effectful expressions. Verified by exhaustive
  enumeration of rejected expression kinds (subqueries, non-deterministic
  functions, aggregates, window functions, correlated references).

#### 5.10.6 MVCC History Compression: PageHistory Objects

Storing full page images per version is not acceptable long-term:

- **Newest committed version:** full page image (for fast reads).
- **Older versions:** patches (intent logs and/or structured patches).
- **Hot pages:** Encode patch chains as ECS **PageHistory objects** so history
  itself is repairable and remote replicas can fetch "just enough symbols" to
  reconstruct a needed historical version.

This is how MVCC avoids eating memory under real write concurrency.

#### 5.10.7 Intent Footprints and Commutativity (Trace-Normalized Merge)

The merge ladder (§5.10.4) is safe only when it preserves a **semantic
serialization order**. The key mechanism is `IntentOp.footprint`
(`IntentFootprint`, §5.10.1): merges are permitted only when the engine can
prove (by construction) that the involved intents **commute**.

We make "commute" concrete via a trace-monoid independence relation (see the
formal definition of trace monoids and Foata normal form in §4.4).

**Independence relation on intents (normative):**

Two intent ops `a, b` are independent (written `(a, b) in I_intent`) iff:

- `a.schema_epoch == b.schema_epoch`, and
- `a.footprint.structural == NONE` and `b.footprint.structural == NONE`, and
- `Writes(a) ∩ Writes(b) = ∅`, and
- `Writes(a) ∩ Reads(b) = ∅` and `Writes(b) ∩ Reads(a) = ∅`.

For SAFE merges, an additional restriction applies:
- `Reads(a)` and `Reads(b)` MUST both be empty. (For `UpdateExpression` ops, the
  implicit column reads are captured in `RebaseExpr` and are NOT in `footprint.reads`,
  so this condition is satisfied.)

**`UpdateExpression` commutativity refinement:** `SemanticKeyRef` is row-granularity,
so two `UpdateExpression` ops on the same `(table, key)` have `Writes(a) ∩ Writes(b)
≠ ∅` at the `SemanticKeyRef` level. This refinement overrides the base `Writes`
disjointness condition for pairs of `UpdateExpression` ops with a column-level check:
two `UpdateExpression` ops `a, b` targeting the same `(table, key)` are independent
iff their `column_updates` sets are disjoint by `ColumnIdx`:
- `columns_written(a) ∩ columns_written(b) = ∅`

where `columns_written(x) := { col_idx | (col_idx, _) in x.column_updates }`.
If any column index overlaps, the ops are NOT independent (the writes conflict
on a sub-row granularity) and the merge MUST reject for that pair.

An `UpdateExpression` and a materialized `Update` (or `Delete`) targeting the same
key are NEVER independent (the materialized op replaces the entire row).

Here `Reads(x)` and `Writes(x)` refer to the sets of `SemanticKeyRef` in
`x.footprint`.

**Canonical merge order (normative):**

When a merge is allowed, the engine MUST execute it using a deterministic normal
form derived from the trace monoid:

- Define `Sigma_intent` as the alphabet of intent ops (identified by stable
  `op_digest := Trunc128(BLAKE3("fsqlite:intent:v1" || canonical_intent_bytes))`).
- Order independent ops using the Foata normal form layering; within each layer,
  sort stably by `(btree_id, kind, key_digest, op_kind, op_digest)`.

This is the *exact* order that must be recorded in the merge certificate
(§5.10.8).

**Mergeable intent classes (normative):**

SAFE merging is deliberately narrow. A merge attempt MUST reject unless all
involved intents are from this set:

- `Insert/Delete/Update` on table B-tree leaf pages for distinct `RowId` keys,
  with no overflow and no multi-page balance.
- `UpdateExpression` on table B-tree leaf pages, subject to the column-disjointness
  rule above. Two `UpdateExpression` ops on the same `RowId` with disjoint
  `ColumnIdx` sets are independent; overlapping column sets are not.
- `IndexInsert/IndexDelete` on index B-tree leaf pages for distinct index keys,
  with no overflow and no multi-page balance.

Any op with `footprint.structural != NONE` MUST be treated as non-commutative
and MUST not be merged; abort/retry is the only safe path.

**Key identity alignment (required):**

`StructuredPagePatch.cell_ops[*].cell_key_digest` MUST be derived from the same
domain-separated semantic key digest as `SemanticKeyRef.key_digest`. The merge
machinery MUST NOT treat physical offsets as identity.

#### 5.10.8 Merge Certificates (Proof-Carrying Merge)

Any commit that is accepted via a merge path (deterministic rebase §5.10.2 and/or
structured patch merge §5.10.3) MUST produce a verifiable **MergeCertificate**
and MUST attach it to the commit's proof payload:

- **Native mode:** `CommitProof` MUST include the certificate (referenced by the
  marker record as `proof_object_id`; §3.5.4.1).
- **Compatibility mode:** the certificate MUST be emitted to the evidence ledger
  (§4.16.1) and MUST be available to the harness; implementations MAY persist
  it to a sidecar for forensic replay.

**MergeCertificate schema (normative):**

```text
MergeCertificate := {
  merge_kind        : { rebase, structured_patch, rebase+patch },
  base_commit_seq   : u64,
  schema_epoch      : u64,
  pages             : Vec<PageNumber>,
  intent_op_digests : Vec<[u8;16]>,          -- op digests involved in the merge
  footprint_digest  : [u8;16],               -- digest over all IntentFootprints
  normal_form       : Vec<[u8;16]>,          -- op digests in canonical order used
  post_state        : {
    page_hashes          : Vec<(PageNumber, [u8;16])>,  -- hash of repacked bytes
    btree_invariant_hash : [u8;16],
  },
  verifier_version  : u32,
}
```

**Verification (normative):**

Given `(base snapshot, intents, certificate)`, a verifier MUST be able to:

1. Recompute all `op_digest` values from canonical intent encodings.
2. Recompute `footprint_digest` from the included `IntentFootprint` values.
3. Check that the `normal_form` is a valid trace-monoid normal form under
   `I_intent` (§5.10.7) for the involved intents.
4. Re-execute `parse -> merge -> repack` for affected pages and re-run B-tree
   invariants; compare `page_hashes` and `btree_invariant_hash`.

**Circuit breaker (normative):**

If any merge verification fails, the system MUST treat it as a correctness
incident. In production, the engine MUST:

- disable SAFE merging for the current database epoch (`PRAGMA fsqlite.write_merge = OFF`),
- emit an evidence ledger entry with the failing check and the certificate id,
- escalate supervision for the component that produced the certificate (§4.14).

In lab mode, it MUST fail fast (test failure).

---

## 6. Buffer Pool: ARC Cache

### 6.1 Why ARC, Not LRU

LRU fails catastrophically for database workloads: a single table scan evicts
the entire working set. ARC (Adaptive Replacement Cache, Megiddo & Modha,
FAST '03) auto-tunes between recency and frequency. The original paper proves
that ARC's 2c-entry directory always contains the c pages LRU(c) would retain,
and that ARC self-tunes to capture both recency and frequency. It dominates
LRU across all tested workloads. (The Sleator-Tarjan competitive ratio for any deterministic paging algorithm
including LRU is k — the cache size — not 2. ARC's theoretical contribution is
adaptive self-tuning, not a tighter worst-case bound.)

**Patent note:** The ARC patent (US 6,996,676 B2, Megiddo & Modha, filed 2002,
expired Feb 2024) has expired, so implementing ARC and its practical variants
(e.g., CAR) is legally safe.

ARC's advantage over LRU is not marginal -- it is structural. Consider three
canonical database access patterns:

1. **Scan-then-point**: A reporting query scans an entire table (touching every
   page once), followed by OLTP point queries on a hot set of 100 pages. Under
   LRU, the scan evicts all 100 hot pages. Under ARC, the scan pages enter T1
   but never promote to T2; the hot pages remain in T2 untouched.

2. **Frequency skew**: 10% of pages receive 90% of accesses (Zipfian). LRU
   cannot distinguish between a page accessed once recently and one accessed
   1000 times. ARC promotes frequently-accessed pages to T2, protecting them
   from recency-only eviction.

3. **Loop patterns**: A query repeatedly scans a working set slightly larger
   than cache. LRU achieves 0% hit rate (every access is a miss). ARC detects
   the looping pattern via ghost hits in B1 and adjusts p to retain a portion
   of the loop, achieving partial hit rate.

### 6.2 MVCC-Aware ARC Data Structures

Standard ARC keys on page number. Our variant keys on `(PageNumber, CommitSeq)`
because multiple versions coexist.

```rust
/// Cache key: MVCC-aware page identity.
/// Multiple versions of the same page coexist when concurrent transactions
/// hold different snapshots. `commit_seq = 0` represents the on-disk baseline.
#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub struct CacheKey {
    pub pgno: PageNumber,
    pub commit_seq: CommitSeq,
}

/// A cached page with metadata for eviction decisions.
pub struct CachedPage {
    pub key: CacheKey,
    pub data: PageData,
    pub ref_count: AtomicU32,     // pinned by active operations
    pub dirty: AtomicBool,        // modified but not yet flushed to WAL
    pub xxh3: Xxh3Hash,           // integrity hash of data at load time
    pub byte_size: usize,         // actual memory (for variable-size deltas)
    pub wal_frame: Option<u32>,   // WAL frame number if from WAL
}

/// The MVCC-aware ARC cache.
///
/// IMPLEMENTATION NOTE (Extreme Optimization Discipline):
/// The Megiddo & Modha (FAST '03) ARC algorithm is specified here as the
/// logical model. The PHYSICAL implementation SHOULD use the CAR (Clock
/// with Adaptive Replacement) variant by Bansal & Modha (FAST '04),
/// which replaces the four LinkedHashMaps with two circular clock buffers
/// and two ghost hash sets:
///
///   - T1 clock: contiguous array of CachedPage slots with reference bits.
///     Scanning for eviction is a sequential memory sweep (cache-friendly).
///   - T2 clock: same structure for frequency-favored pages.
///   - B1/B2: remain as HashSets of CacheKey (metadata only, small).
///
/// Why CAR over linked-list ARC:
///   - LinkedHashMap has 2 pointers per entry (prev/next) plus HashMap
///     overhead. For 2000-page cache: 32KB wasted on link pointers alone.
///   - Every ARC operation (insert, promote, evict) mutates linked list
///     pointers scattered across heap — L1/L2 cache pollution.
///   - CAR's clock hand sweep is a sequential scan over a dense array —
///     the CPU prefetcher handles it. Hit rate is comparable to ARC
///     (shown empirically in the FAST '04 paper across all tested workloads).
///   - Arc<CachedPage> indirection adds another pointer chase. Instead,
///     use inline CachedPage in the clock array with a pinned flag.
///     Pinned pages are simply skipped by the clock hand (not removed
///     from the array, avoiding ABA problems).
///
/// The struct below is the LOGICAL specification. The physical layout uses
/// clock buffers internally but exposes identical semantics.
///
/// CONCURRENCY: All ArcCache operations (REQUEST, REPLACE, promote, evict)
/// mutate multiple internal collections atomically. The cache MUST be
/// protected by a `Mutex<ArcCache>` (or `parking_lot::Mutex` for fast
/// uncontended paths). Individual CachedPage fields (ref_count, dirty) use
/// atomics for lock-free read-side access, but structural mutations to
/// T1/T2/B1/B2/p/index require the mutex. With the CAR physical
/// implementation, the mutex-held critical section is short (clock sweep
/// is sequential over a dense array).
pub struct ArcCache {
    /// T1: pages accessed exactly once recently (recency-favored).
    /// Physical: CircularClockBuffer<CachedPage> with reference bit.
    t1: LinkedHashMap<CacheKey, Arc<CachedPage>>,
    /// T2: pages accessed two or more times recently (frequency-favored).
    /// Physical: CircularClockBuffer<CachedPage> with reference bit.
    t2: LinkedHashMap<CacheKey, Arc<CachedPage>>,
    /// B1: ghost entries evicted from T1 (metadata only, no page data).
    b1: LinkedHashSet<CacheKey>,
    /// B2: ghost entries evicted from T2 (metadata only, no page data).
    b2: LinkedHashSet<CacheKey>,
    /// Adaptive parameter: target size for T1. Range [0, capacity].
    p: usize,
    /// Maximum number of pages in T1 + T2 combined.
    capacity: usize,
    /// Total bytes consumed by cached page data (for memory accounting).
    total_bytes: usize,
    /// Maximum bytes allowed (derived from PRAGMA cache_size).
    max_bytes: usize,
    /// Lookup index: HashMap<CacheKey, SlotIdx> for O(1) cache probes.
    /// SlotIdx encodes which clock (T1 or T2) and the array index.
    index: HashMap<CacheKey, SlotIdx>,
}
```

**Eviction constraints:**
1. Never evict a pinned page (`ref_count > 0`)
2. Never evict a dirty page without flushing it to the WAL first
3. Prefer superseded versions (newer committed version exists and is visible
   to all active snapshots)

### 6.3 Full ARC Algorithm: REPLACE Subroutine

The REPLACE subroutine selects a victim page for eviction. It chooses between
T1 and T2 based on the adaptive parameter p and a tie-breaking rule when the
target key was found in B2.

```
REPLACE(cache, target_key):
  // target_key is the page that triggered this replacement (for tie-breaking)
  rotations_t1 = 0
  rotations_t2 = 0
  loop:
    // Safety valve (MUST be checked FIRST).
    // If we have proven there is no evictable victim in either list (all pinned
    // and/or unflushable), we are overcommitted. Allow temporary growth beyond
    // capacity rather than deadlock.
    //
    // CRITICAL: It is not sufficient to count rotations across (T1+T2) while
    // always selecting the same list. A pinned/failing preferred list MUST NOT
    // prevent eviction from the other list.
    if rotations_t1 >= |T1| AND rotations_t2 >= |T2|:
      capacity_overflow += 1
      return  // caller inserts without evicting

    prefer_t1 = |T1| > 0 AND (|T1| > p OR (|T1| == p AND target_key IN B2))

    if prefer_t1 AND rotations_t1 < |T1|:
      // Evict the LRU page of T1 (recency list)
      candidate = T1.front()
      if candidate.ref_count > 0:
        T1.rotate_front_to_back()  // skip pinned; try next
        rotations_t1 += 1
        continue
      if candidate.dirty:
        if flush_dirty_page(candidate).is_err():
          // WAL write failed (disk full, I/O error). Skip this candidate
          // rather than evicting an unflushed dirty page (data loss).
          // flush_dirty_page() restores the dirty flag on failure (§6.6).
          T1.rotate_front_to_back()
          rotations_t1 += 1
          continue
      (evicted_key, evicted_page) = T1.pop_front()
      B1.push_back(evicted_key)    // remember in ghost list
      total_bytes -= evicted_page.byte_size
      return
    else if |T2| > 0 AND rotations_t2 < |T2|:
      // Evict the LRU page of T2 (frequency list)
      candidate = T2.front()
      if candidate.ref_count > 0:
        T2.rotate_front_to_back()
        rotations_t2 += 1
        continue
      if candidate.dirty:
        if flush_dirty_page(candidate).is_err():
          T2.rotate_front_to_back()
          rotations_t2 += 1
          continue
      (evicted_key, evicted_page) = T2.pop_front()
      B2.push_back(evicted_key)
      total_bytes -= evicted_page.byte_size
      return
    else:
      // Preferred list is exhausted (all pinned/failed candidates) or empty.
      // Fall through and retry: the safety valve will eventually trigger.
      continue
```

**Dirty flush under mutex (design note):** The `flush_dirty_page()` calls above
execute WAL I/O while the cache mutex is held, blocking all concurrent cache
operations for the duration of the write. This is a deliberate simplicity
trade-off: the alternative (releasing the mutex, flushing, re-acquiring) would
require complex state management to handle pages that change during the
unlocked window. For the typical case (4096-byte page, NVMe SSD), a single
`write_frame` completes in ~4μs — acceptable latency for the mutex critical
section. If profiling reveals this as a bottleneck under heavy concurrent load,
a "flush queue" approach (mark candidate as flush-pending, release mutex, flush
asynchronously, re-acquire and evict) can be added without changing the
logical algorithm.

### 6.4 Full ARC Algorithm: REQUEST Subroutine

```
REQUEST(cache, key: CacheKey) -> Result<Arc<CachedPage>>:

  // Case I: Cache hit in T1 or T2
  if key IN T1:
    page = T1.remove(key)
    T2.push_back(key, page)       // promote to frequency list
    page.ref_count.fetch_add(1)
    return Ok(page)

  if key IN T2:
    page = T2.move_to_back(key)   // refresh MRU position
    page.ref_count.fetch_add(1)
    return Ok(page)

  // Case II: Ghost hit in B1 (recently evicted from T1)
  if key IN B1:
    // Evidence that T1 is too small. Increase p to favor recency.
    delta = max(1, |B2| / |B1|)
    p = min(p + delta, capacity)
    REPLACE(cache, key)
    B1.remove(key)
    page = fetch_from_storage(key.pgno, key.commit_seq)
    T2.push_back(key, page)       // enters T2 (second lifetime access)
    total_bytes += page.byte_size
    page.ref_count.fetch_add(1)
    return Ok(page)

  // Case III: Ghost hit in B2 (recently evicted from T2)
  if key IN B2:
    // Evidence that T2 is too small. Decrease p to favor frequency.
    delta = max(1, |B1| / |B2|)
    p = max(p.saturating_sub(delta), 0)
    REPLACE(cache, key)
    B2.remove(key)
    page = fetch_from_storage(key.pgno, key.commit_seq)
    T2.push_back(key, page)
    total_bytes += page.byte_size
    page.ref_count.fetch_add(1)
    return Ok(page)

  // Case IV: Complete miss (not in T1, T2, B1, or B2)
  let L1 = |T1| + |B1|
  let L2 = |T2| + |B2|

  if L1 == capacity:
    if |T1| < capacity:
      B1.pop_front()              // discard oldest ghost from B1
      REPLACE(cache, key)
    else:
      // T1 is full, B1 is empty. Evict LRU of T1 directly.
      // CRITICAL: Do NOT add evicted key to B1 here. Adding to B1 would
      // push |B1| to 1 while |T1| remains at capacity, violating the
      // invariant L1 = |T1| + |B1| ≤ capacity. The evicted key is simply
      // discarded (it was never in a ghost list, so the page leaves the
      // cache entirely — no ghost metadata is preserved).
      rotations = 0
      candidate = T1.front()
      while candidate.ref_count > 0 OR candidate.dirty:
        if rotations >= |T1|:
          // Safety valve: all T1 pages are pinned or unflushed.
          // Allow temporary over-capacity rather than spinning forever.
          capacity_overflow += 1
          break  // skip eviction, insert will exceed capacity
        if candidate.dirty AND candidate.ref_count == 0:
          if flush_dirty_page(candidate).is_err():
            T1.rotate_front_to_back()
            rotations += 1
            candidate = T1.front()
            continue                  // retry with next candidate
          break  // flush succeeded, candidate is now clean and evictable
        T1.rotate_front_to_back()
        rotations += 1
        candidate = T1.front()
      if rotations < |T1| AND candidate.ref_count == 0:
        (evicted_key, _) = T1.pop_front()
        // No B1.push_back — intentionally omitted (see above)
        total_bytes -= _.byte_size
  else if L1 < capacity AND L1 + L2 >= capacity:
    if L1 + L2 >= 2 * capacity:
      B2.pop_front()              // discard oldest ghost from B2
    REPLACE(cache, key)
  // else: cache has room, no eviction needed

  page = fetch_from_storage(key.pgno, key.commit_seq)
  T1.push_back(key, page)         // new pages always enter T1
  total_bytes += page.byte_size
  page.ref_count.fetch_add(1)
  return Ok(page)
```

**Complexity:** Each cache operation is O(1) amortized. Ghost lists consume
16 bytes per CacheKey (`PageNumber`: 4B + 4B alignment padding + `CommitSeq`:
8B) plus container overhead (hash table bucket pointer + linked list links ≈
24 bytes per entry in a `LinkedHashSet`). At `capacity` entries **each** (B1
and B2): 2 × 2000 entries × ~40 bytes = ~160 KB total ghost list overhead —
still negligible compared to page data
(~8 MiB for a 2000-page cache).

#### 6.4.1 Optional: p-Update as Online Learning (Research Note)

The ghost-hit signal (`key ∈ B1` vs `key ∈ B2`) is a signed feedback signal
about whether recency (`T1`) or frequency (`T2`) is undersized. This can be
framed as an online learning / optimal control problem: choose `p_t` over time
to minimize cache miss loss under non-stationary workloads.

A simple OCO-style controller would update:

```
p_{t+1} = clamp(p_t + η_t * s_t, 0, capacity)
s_t = +1 for B1 hit, -1 for B2 hit
```

With standard assumptions, diminishing `η_t` yields a no-regret guarantee in
the abstract OCO model. However, ARC/CAR's known properties rely on the
canonical update rules above. Any alternative `p` controller therefore MUST be
treated as a harness experiment until it is proven to preserve ARC invariants
and performance dominance.

### 6.5 MVCC Adaptation: (PageNumber, CommitSeq) Keying with Ghost Lists

**Ghost list semantics change.** When a ghost entry `(pgno, old_commit_seq)` is
in B1 and a request arrives for `(pgno, new_commit_seq)`, this is NOT a ghost
hit -- it is a different version. Ghost hits only occur on exact
`(pgno, commit_seq)` match. This is correct because different versions have
genuinely different access patterns.

**Version coalescing in ghost lists.** Ghost lists may accumulate many entries
for the same page number with different commit sequence values. To bound ghost list size,
when the GC horizon advances, prune ghost entries whose commit sequence is below the
new horizon:

```
prune_ghosts(cache, gc_horizon: CommitSeq):
  B1.retain(|k| k.commit_seq >= gc_horizon)
  B2.retain(|k| k.commit_seq >= gc_horizon)
```

**Capacity accounting.** Each `(pgno, commit_seq)` pair counts as one entry. A
heavily-versioned page consumes multiple cache slots. Under high write
contention, the effective number of distinct pages cached decreases. This is
correct: the cache prioritizes versions actively needed over breadth.

### 6.6 Eviction: Pinned Pages and Dirty Flush Protocol

**All pages pinned scenario.** If REPLACE scans all of T1 and T2 without
finding an unpinned, clean page, the cache is overcommitted. Resolution:

1. Temporarily grow capacity by 1 (`capacity_overflow += 1`).
2. Log a warning: the application has too many concurrent pinned pages.
3. On the next `unpin()` call, decrement `capacity_overflow` and trigger
   eviction if needed.

This is a safety valve, not the normal path. In practice, pinned page count
is bounded by `(concurrent_cursors * max_btree_depth)`, which is typically
under 200 even for heavy workloads.

**Dirty page flush protocol:**

```rust
fn flush_dirty_page(page: &CachedPage, wal: &WalWriter) -> Result<()> {
    // Atomically claim the flush to prevent double-flush from concurrent evictors
    if page.dirty.compare_exchange(true, false, SeqCst, SeqCst).is_ok() {
        match wal.write_frame(page.key.pgno, &page.data) {
            Ok(()) => Ok(()),
            Err(e) => {
                // WAL write failed (e.g., disk full). Restore dirty flag.
                page.dirty.store(true, SeqCst);
                Err(e)
            }
        }
    } else {
        Ok(()) // already flushed by another thread
    }
}
```

### 6.7 MVCC Version Coalescing

When a newer committed version of a page is visible to ALL active snapshots,
older versions are reclaimable. The cache proactively drops them.

**Coalescing triggers:**
- During REPLACE (opportunistic: check if candidate is superseded)
- After GC horizon advances (batch scan)
- On `PRAGMA shrink_memory`

```
coalesce_versions(cache, pgno, gc_horizon):
  versions = all cached entries where key.pgno == pgno
  sort versions by commit_seq descending

  kept_committed = false
  for key in versions:
    if key.commit_seq != 0 AND key.commit_seq <= gc_horizon:
      if !kept_committed:
        kept_committed = true   // keep newest committed below horizon
        continue
      // Superseded: remove if not pinned
      if let Some(page) = remove_from_t1_or_t2(key):
        if page.ref_count == 0:
          total_bytes -= page.byte_size
          // Do NOT add to ghost list (version is permanently dead)
        else:
          re_insert(key, page)  // pinned; try again later
```

### 6.8 Snapshot Visibility (CommitSeq, O(1))

FrankenSQLite uses commit-seq snapshots (§5): `Snapshot.high` is the latest
committed `CommitSeq` visible to the transaction. Therefore version visibility
checks during version-chain traversal are O(1) and do not require an `in_flight`
set or Bloom filter.

**Visibility fast path (committed versions only):**

```rust
fn is_visible(version_commit_seq: CommitSeq, snapshot: &Snapshot) -> bool {
    version_commit_seq != 0 && version_commit_seq <= snapshot.high
}
```

Uncommitted/private versions (`commit_seq = 0`) are never visible through MVCC
resolution; they are visible only via the owning transaction's private
`write_set` (self-visibility).

### 6.9 Memory Accounting (System-Wide, No Surprise OOM)

Every subsystem that stores variable-size state MUST have:
- A strict byte budget.
- A policy for reclamation under pressure.
- Metrics exported for harness + benchmarks.

We do not accept unbounded growth of ANY of the following:

| Subsystem | Budget Source | Reclamation Policy |
|-----------|-------------|-------------------|
| ARC page cache | `PRAGMA cache_size` | ARC eviction (§6.3–6.4) |
| MVCC page version chains | GC horizon (min active snapshot) | Coalescing + version drop (§6.7) |
| SSI witness plane (hot index + evidence caches) | Hot: fixed SHM layout; Cold: fixed byte budgets | Hot: epoch swap (§5.6.4.8); Cold: LRU + rebuild from ECS; evidence GC by safe horizons |
| Symbol caches (decoded objects) | Fixed byte budget, configurable | LRU eviction |
| Index segment caches | Fixed byte budget | LRU eviction; rebuild from ECS on miss |
| Bloom/quotient filters | O(n) where n = active pages with versions | Rebuilt on GC horizon advance |

**Cache-specific accounting:**

The cache tracks total byte consumption, not just page count, because MVCC
version chain compression (sparse XOR deltas, Section 3.4.4) produces
variable-size entries. A full page = 4096 bytes; a sparse delta may be ~200.

**Dual eviction trigger:** Eviction fires when EITHER page count exceeds
capacity OR `total_bytes` exceeds `max_bytes`. This prevents memory exhaustion
when many full-size pages are cached alongside compact deltas.

```rust
fn should_evict(&self) -> bool {
    (self.t1.len() + self.t2.len() > self.capacity)
        || (self.total_bytes > self.max_bytes)
}
```

### 6.10 Configuration: PRAGMA cache_size Mapping

```
PRAGMA cache_size = N:
    if N > 0:
        cache.capacity = N
        cache.max_bytes = N * page_size
    if N < 0:
        cache.max_bytes = |N| * 1024    // |N| KiB
        cache.capacity = cache.max_bytes / page_size
    if N == 0:
        // PRAGMA cache_size = 0 sets the cache size to 0 pages. There is
        // NO special "reset to default" logic in SQLite; the compile-time
        // default (SQLITE_DEFAULT_CACHE_SIZE = -2000) is only applied at
        // database open time.
        cache.capacity = 0
        cache.max_bytes = 0
```

**Default:** Compile-time default is -2000 (= 2000 KiB). For 4096-byte pages
this yields 500 pages (2 MiB); for 1024-byte pages, 2000 pages. Ghost lists
limited to `capacity` entries each (~72 KB overhead for 2000 entries, see §6.4).

**Resize protocol (runtime change):**
1. Set new capacity and max_bytes.
2. If `|T1| + |T2| > new_capacity`: repeatedly call REPLACE until within
   limits (dirty pages are flushed during this process).
3. Trim ghost lists: `B1.truncate(new_capacity)`, `B2.truncate(new_capacity)`.
4. Clamp p to `[0, new_capacity]`.

### 6.11 Performance Analysis

| Workload | P (pages) | W (hot) | C (cache) | H (LRU) | H (ARC) |
|----------|-----------|---------|-----------|---------|---------|
| OLTP point queries | 100K | 500 | 2000 | 0.96 | 0.97 |
| Mixed OLTP + scan | 100K | 500 | 2000 | 0.60 | 0.85 |
| Full table scan | 100K | 100K | 2000 | 0.02 | 0.02 |
| Zipfian (s=1.0) | 100K | N/A | 2000 | 0.82 | 0.89 |
| MVCC 8 writers | 100K | 800 | 2000 | 0.55 | 0.78 |

ARC's advantage is most pronounced in mixed workloads. The T2 list protects
frequently-accessed pages from scan pollution. Under MVCC with multiple
writers, ARC naturally separates hot current versions (T2) from cold
superseded versions (evicted or coalesced).

### 6.12 Warm-Up Behavior

**Phase 1 -- Cold start (0 to ~50% full):** All misses. p=0. No adaptation.

**Phase 2 -- Learning (~50-100% full):** First evictions. Ghost lists populate.
p adapts toward workload. Hit rate climbs 20-60%.

**Phase 3 -- Steady state (full):** p converged. Hit rate at expected value.
Reached after approximately 3x capacity accesses.

**Pre-warming (optional, `PRAGMA cache_warm = ON`):** On database open, read
pages referenced in WAL index into T1 (limited to half capacity). Also read
root pages of all tables/indexes from sqlite_master.

---

## 7. Checksums and Integrity

### 7.1 SQLite Native Checksum Algorithm

The WAL uses a custom 64-bit checksum (two u32 accumulators) for frame
integrity. This must be implemented exactly for file format compatibility.

**Algorithm (from wal.c):**

```rust
/// Compute SQLite WAL checksum, chaining from (s1_init, s2_init).
///
/// `big_end_cksum` is `(magic & 1) != 0` from the WAL header (wal.c): it records
/// whether the WAL creator machine was big-endian.
///
/// SQLite computes `nativeCksum = (bigEndCksum == SQLITE_BIGENDIAN)` and calls:
/// `walChecksumBytes(nativeCksum, ...)`. When `nativeCksum == 0`, it
/// BYTESWAP32's each u32 word before accumulating. This is equivalent to:
/// `native_cksum = (big_end_cksum == cfg!(target_endian = "big"))`.
pub fn wal_checksum(
    data: &[u8],
    s1_init: u32,
    s2_init: u32,
    big_end_cksum: bool,
) -> (u32, u32) {
    assert!(data.len() % 8 == 0);
    let mut s1 = s1_init;
    let mut s2 = s2_init;
    let native_cksum = big_end_cksum == cfg!(target_endian = "big");

    for chunk in data.chunks_exact(8) {
        let (a, b) = if native_cksum {
            // nativeCksum=1: read u32 words in native byte order (no swap)
            (
                u32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
                u32::from_ne_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]),
            )
        } else {
            // nativeCksum=0: BYTESWAP32 each u32 before accumulating
            (
                u32::from_ne_bytes([chunk[3], chunk[2], chunk[1], chunk[0]]),
                u32::from_ne_bytes([chunk[7], chunk[6], chunk[5], chunk[4]]),
            )
        };
        s1 = s1.wrapping_add(a).wrapping_add(s2);
        s2 = s2.wrapping_add(b).wrapping_add(s1);
    }
    (s1, s2)
}
```

**Endianness determination from WAL magic:**
- `0x377f0682` (bit 0 = 0): `bigEndCksum = 0` (created on a little-endian machine).
  - On little-endian readers: `nativeCksum = 1` (no swap).
  - On big-endian readers: `nativeCksum = 0` (BYTESWAP32 each u32).
- `0x377f0683` (bit 0 = 1): `bigEndCksum = 1` (created on a big-endian machine).
  - On big-endian readers: `nativeCksum = 1` (no swap).
  - On little-endian readers: `nativeCksum = 0` (BYTESWAP32 each u32).

The magic is always read via big-endian `u32` decoding (matching SQLite's
`sqlite3Get4byte`). The caller passes `big_end_cksum = (magic & 1) != 0` to this
function; this function derives `native_cksum` exactly as SQLite does:
`nativeCksum = (bigEndCksum == SQLITE_BIGENDIAN)`.

FrankenSQLite writes WAL files using native byte order for performance.

**Cumulative chaining:** Each frame's checksum chains from the previous:
```
WAL header checksum: (hdr_cksum1, hdr_cksum2) = wal_checksum(header[0..24], 0, 0, big_end_cksum)
Frame 0 checksum = wal_checksum(frame0_hdr[0..8] ++ page0_data, hdr_cksum1, hdr_cksum2, big_end_cksum)
Frame N checksum = wal_checksum(frameN_hdr[0..8] ++ pageN_data, s1_{N-1}, s2_{N-1}, big_end_cksum)
```

This creates a hash chain: modifying any frame invalidates all subsequent
checksums, detecting both random corruption and truncation.

### 7.2 XXH3 Integration

For internal integrity checks not requiring WAL format compatibility,
FrankenSQLite uses XXH3-128 from `xxhash-rust`. Throughput: ~50 GB/s on
x86-64 with AVX2 (~80ns per 4096-byte page).

**Storage:**

```rust
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Xxh3Hash {
    pub low: u64,
    pub high: u64,
}

impl Xxh3Hash {
    pub fn compute(data: &[u8]) -> Self {
        let h = xxhash_rust::xxh3::xxh3_128(data);
        Self { low: h as u64, high: (h >> 64) as u64 }
    }
    pub fn verify(&self, data: &[u8]) -> bool { *self == Self::compute(data) }
}
```

**Where XXH3 is used:**

1. **Buffer pool:** On page read from disk, compute and store XXH3-128.
   Reverify on `get_page()` from cache when `PRAGMA integrity_check_cache = ON`.
2. **MVCC version chain:** Each PageVersion carries an XXH3-128 hash.
3. **Checkpoint:** Verify XXH3 before writing page from WAL to database file.
4. **PRAGMA integrity_check:** Full verification of all pages.

**Collision probability:** 2^-128 (~3e-39). For non-adversarial corruption
detection this is vastly sufficient.

### 7.3 CRC-32C for RaptorQ

RaptorQ repair symbols carry CRC-32C checksums (4-byte overhead per symbol).

**Hardware acceleration:** CRC-32C has native instruction support on:
- x86-64: SSE4.2 `crc32` instruction (~20 GB/s)
- ARM: ACLE CRC extension `__crc32cd` instruction (~15 GB/s)
- Software fallback: table-based Sarwate algorithm (~2 GB/s)

**Detection strategy:**

```rust
/// CRC-32C computation. We use the `crc32c` crate (NOT `crc32fast`, which
/// computes CRC-32/ISO-HDLC, a different polynomial). CRC-32C (Castagnoli,
/// polynomial 0x1EDC6F41) is what SSE4.2's `crc32` instruction computes
/// natively and what protocols like iSCSI, ext4, and btrfs use.
///
/// The `crc32c` crate auto-detects SSE4.2 / ARMv8 CRC at runtime and
/// falls back to a software table implementation. It uses unsafe internally
/// for SIMD intrinsics, but our workspace only forbids unsafe in *our*
/// crates, not dependencies.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
```

**Verification point:** CRC-32C is checked on each repair symbol BEFORE
passing it to the RaptorQ decoder. A corrupted symbol with valid CRC-32C has
probability ~2^-32 of going undetected (adequate for repair symbols that are
themselves redundant).

### 7.3.1 Three-Tier Hash Strategy (Explicit Separation of Concerns)

We separate three concerns with three hash functions:

| Tier | Purpose | Hash | Speed | Where |
|------|---------|------|-------|-------|
| **Hot-path integrity** | Detect torn writes / bitrot on every page access | **XXH3-128** | ~50 GB/s | Buffer pool, MVCC version chain, cache reads |
| **Content identity** | Stable, collision-resistant addressing for ECS objects | **BLAKE3** (truncated to 128 bits) | ~5 GB/s | `ObjectId` derivation, commit capsule identity |
| **Authenticity / security** | Cryptographic authentication at trust boundaries | `asupersync::security::SecurityContext` | Key-dependent | Replication transport, authenticated symbols |

**Policy:**
- We do NOT use SHA-256 on hot paths. It is too slow for per-page integrity.
- We do NOT use XXH3 for content addressing. It is not cryptographic.
- We do NOT roll our own crypto. Security uses asupersync's vetted primitives.
- BLAKE3 is the bridge: fast enough for object-granularity identity, strong
  enough for collision resistance in this context. Note: 128-bit truncation
  gives ~2^64 birthday-bound collision resistance, not 2^128. This is
  adequate for the expected object population (< 2^40 objects per database)
  but means ObjectId should NOT be relied upon as a security guarantee
  against adversarial collision attacks.

### 7.4 Page-Level Integrity

**On-disk pages:** Standard SQLite format has NO per-page checksums. Corruption
is detected only by structural checks or `PRAGMA integrity_check`.

**Optional FrankenSQLite enhancement:** When `PRAGMA page_checksum = ON`, the
reserved space at the end of each page stores an XXH3-128 hash:

```
Page layout: [data: page_size - 16 bytes] [xxh3: 16 bytes]
Header byte offset 20 set to 16 (reserved space = 16).
```

This is compatible with C SQLite (reserved space is opaque to it). Default is
OFF for maximum compatibility.

**Interoperability Warning:** While C SQLite can *read* databases with reserved
space checksums (it ignores the bytes), it will *write* zeros (or preserved garbage)
to the reserved space when modifying pages. This invalidates the FrankenSQLite
checksum. Therefore, if `PRAGMA page_checksum = ON` is used, the database should
be treated as **Read-Only** by legacy C SQLite clients to avoid "corruption"
reports when FrankenSQLite next reads the modified pages.

**Verification points in the hot path:**
- Every disk read: compute XXH3, store in CachedPage
- Every cache read (optional): reverify XXH3
- Before WAL write: verify dirty page XXH3 matches
- Before checkpoint write: verify page XXH3

### 7.5 WAL Frame Integrity: Cumulative Checksum Chain

The WAL checksum chain provides these properties:

**Append-only integrity:** Inserting or modifying any frame invalidates all
subsequent checksums. Detects both corruption and tampering.

**Torn write detection:** A partial write produces an invalid checksum at
the torn frame. During recovery, frames are read sequentially; the first
invalid checksum marks the valid WAL end.

**Recovery procedure:**

```
recover_wal(wal_file):
  read and verify wal_header checksum
  if invalid: WAL is entirely corrupt; use database file only

  (s1, s2) = (wal_header.cksum1, wal_header.cksum2)
  valid_frames = 0

  loop:
    read frame_header (24 bytes) + page_data (page_size bytes)
    if EOF: break

    // Verify salts match WAL header
    if frame.salt1 != wal_header.salt1 OR frame.salt2 != wal_header.salt2:
      break  // stale frame from previous WAL generation

    // Verify cumulative checksum
    (expected_s1, expected_s2) = wal_checksum(
      frame_header[0..8] ++ page_data, s1, s2, native
    )
    if frame.cksum1 != expected_s1 OR frame.cksum2 != expected_s2:
      break  // torn write or corruption

    (s1, s2) = (frame.cksum1, frame.cksum2)
    valid_frames += 1

  // Only committed transactions (last frame has db_size > 0) are replayed
```

**Critical implication (normative for self-healing):** Because the checksum is
*cumulative*, once a mismatch occurs at frame `i` the WAL format alone cannot
validate frames `i+1..` (their expected checksum depends on the checksum state
after frame `i`). Therefore, any "self-healing WAL" design MUST provide an
independent random-access validation mechanism for source frames. FrankenSQLite
does this by storing per-source `xxh3_64(page_data)` hashes in `.wal-fec`
(`WalFecGroupMeta.source_page_xxh3`; §3.4.1), which allows identifying which
source symbols are safe to feed into a decoder even when the cumulative chain is
broken.

### 7.6 Double-Write Prevention

SQLite's WAL design prevents double-write corruption through:

1. **Cumulative checksums** (Section 7.5): torn writes produce invalid checksums.
2. **Salt values:** Each WAL generation has unique random salts. After
   checkpoint RESTART/TRUNCATE, old frames are rejected by salt mismatch.
3. **Commit frame marker:** A frame with non-zero `db_size` field marks a
   transaction boundary. Partial transactions (no valid commit frame) are
   discarded during recovery.
4. **Tightly-packed frames:** WAL frames are NOT sector-aligned; each frame
   (24-byte header + page_size bytes) follows the previous with no padding.
   Torn writes are detected by the cumulative checksum chain, not by
   alignment. (Contrast with rollback journal, where the header IS padded
   to sector size.)

**FrankenSQLite addition:** RaptorQ repair symbols (Section 3.4.1) turn
"detect and discard" into "detect and repair" -- corrupted frames within a
commit group are reconstructed if sufficient repair symbols survive.

### 7.7 PRAGMA integrity_check Implementation

**Level 1 -- Page-level:** Read every page. For pages identified as B-tree
pages (via Level 4 cross-reference), verify page type flag is valid (0x02,
0x05, 0x0A, 0x0D) and verify header fields are in range. Overflow pages,
freelist trunk/leaf pages, lock-byte pages, and pointer map pages have
different structures and MUST NOT be checked against B-tree type flags.
If page checksums enabled, verify XXH3 for all page types.

**Level 2 -- B-tree structural:** Cell pointers within bounds and non-
overlapping. Cell content within cell content area. Interior child pointers
reference valid pages. Keys sorted within each page. Keys in child subtrees
bounded by parent keys. Freeblock list well-formed (no cycles). Fragmented
byte count matches actual fragmentation.

**Level 3 -- Record format:** Header varints valid. Serial types not 10 or 11.
Payload sizes match serial type declarations. Overflow chains well-formed.

**Level 4 -- Cross-reference:** Every page accounted for (B-tree, freelist, or
pointer-map). No page in multiple B-trees. Freelist structure consistent.
Pointer map entries match actual parents (auto-vacuum mode).

**Level 5 -- Schema:** sqlite_master readable. All entries parseable. Root page
numbers match existing B-trees. For each index, verify entries match table data.

**Output:** List of error strings, or the single string "ok" if no issues
found. Matches C SQLite behavior exactly.

### 7.8 Error Recovery by Checksum Type

**WAL frame checksum mismatch:** Frame is at or beyond the valid WAL end under
SQLite's cumulative checksum rule (§7.5). Normal recovery truncates the WAL at
the first mismatch. FrankenSQLite MUST attempt repair first *if* a matching
`.wal-fec` group exists:
- Locate `WalFecGroupMeta` for the affected commit group (§3.4.1).
- Validate candidate source frames using `source_page_xxh3` (random-access; does
  not depend on the broken checksum chain).
- Combine surviving sources + repair symbols and decode if `>= K`.
- If repair succeeds, treat the group as committed and persist the repair by
  checkpointing and resetting/truncating the WAL (so the corruption does not
  require re-repair on every boot).
- If repair fails, truncate the WAL before the damaged group (transaction lost).

**XXH3 internal mismatch (buffer pool):** Return `SQLITE_CORRUPT` to caller.
Log page number, expected hash, actual hash. Evict page from cache. If page
exists in WAL, retry from WAL. Otherwise corruption is persistent.

**CRC-32C mismatch (RaptorQ symbol):** Exclude corrupted symbol from decoding
set. If `|surviving| >= K` total symbols (source + repair combined, where K
is the source symbol count), decoding proceeds. Otherwise the commit group
is unrecoverable.

**Database file corruption (found by integrity_check):** Reported as diagnostic
text. If WAL version exists, it supersedes the corrupt page. Otherwise
corruption is permanent without backups.

### 7.9 Crash Model (Explicit Contract)

FrankenSQLite assumes the following failure model. Every durability and
recovery mechanism is designed against these six points:

1. **Process crash at any point.** No code path is crash-immune. Any operation
   may be interrupted between any two instructions.
2. **`fsync()` is a durability barrier** for data and metadata as documented by
   the OS. We trust the OS's fsync contract but nothing weaker.
3. **Writes can be reordered** unless constrained by fsync barriers. The OS and
   storage hardware may reorder writes freely between fsync calls.
4. **Torn writes exist at sector granularity.** A sector write (typically 512B
   or 4KB) is atomic, but writes spanning multiple sectors can be partially
   completed. Tests simulate multiple sector sizes (512, 1024, 4096).
5. **Bitrot and corruption may exist.** Silent data corruption in storage media
   is a real threat. Checksums (Section 7) detect it; RaptorQ (Section 3)
   repairs it within the configured tolerance budget.
6. **File metadata durability may require directory `fsync()`.** Platform-
   dependent. Our VFS MUST model this. Tests MUST include directory fsync
   simulation.

**Self-healing durability contract:**

> If the commit protocol reports "durable", then the system MUST be able to
> reconstruct the committed data exactly during recovery, even if some
> fraction of locally stored symbols are missing or corrupted within the
> configured tolerance budget.

This is the operational meaning of "self-healing": we do not merely *detect*
corruption; we *repair* it by RaptorQ decoding.

**Durability policy (exposed via PRAGMA):**

- `PRAGMA durability = local` (default): Enough RaptorQ symbols persisted to
  local storage such that decode will succeed under the local corruption budget.
- `PRAGMA durability = quorum(M)`: Enough symbols persisted across M of N
  replicas to survive node loss budgets (see replication in Section 3.4.2).
- `PRAGMA raptorq_overhead = <percent>`: Controls repair symbol budget
  (default: 20% overhead, meaning 1.2x source symbols are stored).

### 7.10 Two Operating Modes

FrankenSQLite supports two operating modes to balance innovation with
verifiability:

**Compatibility Mode (Oracle-Friendly):**
- Purpose: Prove SQL/API correctness against C SQLite 3.52.0.
- DB file is standard SQLite format.
- WAL frames are standard SQLite WAL frames.
- Legacy SQLite readers MAY attach concurrently.
- Legacy writers are excluded whenever `foo.db.fsqlite-shm` is in use (Hybrid
  SHM protocol, §5.6.7). To interoperate with legacy writers, run without
  `foo.db.fsqlite-shm` (file-lock fallback, §5.6.6.2), which disables
  multi-writer MVCC and SSI.
- We may write *extra* sidecars (`.wal-fec` for WAL repair symbols, `.db-fec`
  for page-group repair symbols, `.idx-fec` for index repair) but the core `.db` stays SQLite-compatible when
  checkpointed.
- This is the default mode for conformance testing.

**Native Mode (RaptorQ-First):**
- Purpose: Maximum concurrency + durability + replication.
- Primary durable state is an ECS commit stream (CommitCapsule objects encoded
  as RaptorQ symbols).
- CommitCapsule contains: `snapshot_basis`, `intent_log` and/or `page_deltas`,
  `read_set_digest`, `write_set_digest`, plus **SSI witness-plane evidence
  references** (§5.7): `ReadWitness`/`WriteWitness` ObjectIds, emitted
  `DependencyEdge` ObjectIds, and optional `MergeWitness` ObjectIds.
  (Commit ordering is provided by the marker stream; the capsule does not embed
  `commit_seq` so content addressing is not polluted by an ordering artifact.)
- CommitMarker is the atomic "this commit exists" record: `commit_seq`,
  `commit_time_unix_ns`, `capsule_object_id`, `proof_object_id`, `prev_marker`,
  `integrity_hash`. `commit_time_unix_ns` MUST be monotonic non-decreasing with
  `commit_seq` (see §12.17).
- **Atomicity rule:** A commit is committed iff its marker is durable. Recovery
  ignores any capsule without a committed marker.
- Checkpointing materializes a canonical `.db` for compatibility export, but
  the source-of-truth is the commit stream.
- Both modes are supported by the **same SQL/API layer**. Conformance harness
  validates behavior, not internal format.

**Mode selection:** `PRAGMA fsqlite.mode = compatibility | native` (default:
compatibility). Mode is per-database, not per-connection. Switching from
Compatibility to Native requires building an initial commit stream from the
existing `.db` + WAL state. Switching from Native to Compatibility requires
materializing a checkpoint `.db` from the commit stream. Both conversions
are explicit operations (not automatic on reconnect).

### 7.11 Native Mode Commit Protocol (High-Concurrency Path)

The Native-mode commit protocol decouples **Bulk Durability** (payload bytes)
from **Ordering** (the marker stream):

- Writers persist `CommitCapsule` payloads concurrently (bulk I/O off the
  critical section).
- A single sequencer (WriteCoordinator) serializes only the tiny ordering step:
  validation + `commit_seq` allocation + `CommitMarker` append.

This avoids a structural bottleneck where one thread must move every byte of
every transaction while also sequencing commits. The serialized section MUST
never be responsible for writing page payloads; it operates on ObjectIds,
digests, and compact write-set summaries.

#### 7.11.1 Writer Path (Concurrent, Bulk I/O)

1. **Finalize (local):** Finalize the write set (pages and/or intent log).
2. **Validate (SSI, local):** Run SSI validation using the witness plane (§5.7).
   This phase MAY emit `DependencyEdge` objects and MAY perform merge (§5.10),
   producing `MergeWitness` objects when successful. If SSI aborts, publish
   `AbortWitness` and return `SQLITE_BUSY_SNAPSHOT`.
3. **Publish witness evidence (pre-marker):** Publish `ReadWitness` /
   `WriteWitness` objects, emitted `DependencyEdge` objects, and any
   `MergeWitness` objects using the cancel-safe two-phase publication protocol
   (§5.6.4.7). These objects are not considered "committed" until referenced by
   a committed marker, but publication MUST occur before marker publication.
4. **Build capsule:** Construct `CommitCapsuleBytes(T)` deterministically from
   intent log, page deltas, snapshot basis, and the witness-plane ObjectId
   references from step (3).
5. **Encode:** RaptorQ-encode capsule bytes into symbols (systematic + repair).
6. **Write capsule symbols (CONCURRENT I/O):** The committing transaction
   writes symbols to local symbol log files (and optionally streams to replicas).
   This happens **before** acquiring the commit sequencing critical section:
   - Local: write ≥ `K_source + R` symbols (where `K_source` = source symbols,
     `R` = repair symbols per §3.4.3) to the current symbol log segment.
     The writer does NOT fsync here; actual local durability is deferred to the
     coordinator's FSYNC_1 (§7.11.2 step 4) for group-commit batching.
   - Quorum: persist/ack ≥ `K_source + R` symbols across M replicas. Remote
     replicas MUST fsync before acking (remote durability is not deferred).
7. **Submit to WriteCoordinator:** Send a tiny publish request over a two-phase
   MPSC channel (§4.5) containing:
   - `capsule_object_id` (16B)
   - `capsule_digest` (for sanity-checking / audit)
   - `write_set_summary` (page numbers / witness keys sufficient for FCW validation; no false negatives)
   - `witness_refs`: the `ReadWitness`/`WriteWitness` ids
   - `edge_ids` and `merge_witness_ids`
   - `txn_token`, `begin_seq`, and abort-policy metadata
   Then await the coordinator response.

#### 7.11.2 WriteCoordinator Loop (Serialized, Tiny I/O)

For each publish request:

1. **Validation (FCW):** Perform First-Committer-Wins validation using
   `write_set_summary` against the coordinator's commit index (or equivalent).
   Validation MUST NOT require decoding the entire capsule.
   This step is cancellable: if the database is shutting down, the coordinator
   MAY respond `Aborted { SQLITE_INTERRUPT }` before entering the commit section.

   **SSI Re-validation (Race Protection):** If the requesting transaction's
   `TxnSlot.mode == Concurrent` (looked up via `request.txn`),
   the coordinator MUST re-check the `TxnSlot.has_in_rw` and `TxnSlot.has_out_rw`
   flags (or `SSI_Epoch`) for the requesting transaction. A concurrent commit
   could have created a Dangerous Structure after the writer's local validation.
   If `has_in_rw && has_out_rw` (and `request.abort_policy != Custom`), the
   coordinator MUST abort with `SQLITE_BUSY_SNAPSHOT`.

2. **Allocate `commit_seq` (gap-free, marker-tip-derived):** Assign the next
   commit sequence number by deriving it from the physical marker stream tip,
   inside the same cross-process serialized section used to append the marker
   record (§3.5.4.1). This prevents commit sequence gaps under crash:
   a `commit_seq` is not "consumed" unless a marker record is actually written.
   Also assign `commit_time_unix_ns` as a monotonic timestamp:
   `commit_time_unix_ns := max(now_unix_ns(), last_commit_time_unix_ns + 1)`.
   Steps (2)–(8) form the sequencer's **commit section**: once `commit_seq` is
   allocated, the coordinator MUST NOT observe cancellation until the marker is
   durable and the requester has been responded to. Implement using bounded
   masking (`Cx::masked` / commit_section semantics; §4.12.2–§4.12.3).
3. **Persist `CommitProof` (small):** Build and publish a `CommitProof` ECS
   object containing `commit_seq` and evidence references. Record its
   `proof_object_id`.
4. **FSYNC barrier (pre-marker, group commit point):** Issue `fdatasync` (or
   platform equivalent) on the current symbol log segment file(s) and proof
   object storage. This is the group-commit durability point: writers from
   step 6 of §7.11.1 wrote symbols without fsyncing; this single fdatasync
   makes all pending capsule symbols AND the CommitProof durable BEFORE the
   marker references them. Without this barrier, write reordering (common on
   NVMe with volatile write caches) can make the marker durable while its
   referents are not — an irrecoverable corruption on crash.
   If multiple publish requests are batched (§4.5), a single fdatasync covers
   all of their capsule symbols.
5. **Persist marker (tiny):** Append a `CommitMarkerRecord` (§3.5.4.1) to the
   marker stream. This is the atomic "this commit exists" step (fixed-size,
   88 bytes in V1). `prev_marker_id` links to the previous marker, and
   `marker_id` is the integrity hash of the record.
6. **FSYNC barrier (post-marker):** Issue `fdatasync` on the marker stream.
   The client MUST NOT receive a success response until this completes.
7. **Publish commit_seq:** Update the shared-memory `commit_seq` high-water mark
   (§5.6.1) with a `Release` store of the committed `commit_seq`. This MUST
   occur only after the marker is durable (step 6), so other processes never
   observe a `commit_seq` that does not exist in the marker stream.
8. **Respond:** Notify the client of success (or conflict/abort).

#### 7.11.3 Background Work (Not in Critical Section)

- Index segments and caches update asynchronously.

**Critical ordering (TWO fsync barriers, normative):**

```
capsule symbols [written (not fsynced) by committing txn, step 6 of §7.11.1]
    → CommitProof [written, step 3 above]
    → FSYNC_1 (step 4)      ← group-commit: ensures capsule + proof are durable
    → marker [persisted, step 5]
    → FSYNC_2 (step 6)      ← ensures marker is durable
    → shm.commit_seq publish (step 7)
    → client response (step 8)
```

Both barriers are **mandatory**:
- **FSYNC_1** prevents "committed marker, lost data" — the worst-case native
  mode failure. If the marker is durable but the capsule or proof is not
  decodable, the core durability contract is violated and recovery cannot
  proceed.
- **FSYNC_2** prevents "client thinks committed, marker not persisted" — a
  durability violation that silently loses transactions on crash.

**Performance note:** The two-fsync cost (~100-200μs on NVMe) is amortized
by batching multiple commits per WriteCoordinator iteration (§4.5). The
optimal batch size derivation (§4.5) already accounts for `t_fsync`.

### 7.12 Native Mode Recovery Algorithm

1. Load `RootManifest` via `ecs/root` (§3.5.5).
2. Locate the latest checkpoint (if any) and its manifest.
3. Scan marker stream from the checkpoint tip forward (or from genesis).
4. For each marker:
   - Fetch/decode referenced capsule (repairing via RaptorQ if needed).
   - Apply capsule to state (materialize page deltas or replay intent log).
5. Rebuild/refresh index segments and caches as needed.

**Correctness requirement:** If recovery encounters a committed marker, it
MUST eventually be able to decode the capsule (within configured budgets), or
else it MUST surface a "durability contract violated" diagnostic with decode
proofs attached (lab/debug builds).

### 7.13 ECS Storage Reclamation (Compaction)

Native Mode's append-only symbol logs (`ecs/symbols/*.log`) grow indefinitely.
To reclaim storage, the system runs a **Mark-and-Compact** process.

**Compaction Signals (candidate triggers):**
- **Space amplification:** `total_log_size / live_data_size` exceeds a policy
  threshold (default: 2.0).
- **Time interval:** `PRAGMA fsqlite.auto_compact_interval`.
- **Manual:** `PRAGMA fsqlite.compact` (MUST run regardless of policy).

**Policy rule (recommended):** The *timing* and *rate limiting* of background
compaction SHOULD be selected by `PolicyController` via expected loss (§4.17),
not by a single fixed threshold.

#### 7.13.1 Workload-Adaptive Compaction Policy (MDP, Recommended)

Compaction has a real opportunity cost: it consumes I/O and CPU and competes
with foreground reads/writes. The optimal time to compact depends on the
current workload regime (read-heavy vs write-heavy), which FrankenSQLite already
tracks via BOCPD (§4.8).

Model compaction scheduling as a finite-state Markov Decision Process (MDP):

- **State:** `S = (space_amp_bucket, read_regime, write_regime, compaction_debt)`
  - `space_amp_bucket`: discretized `total_log_size/live_data_size`
  - `read_regime`, `write_regime`: BOCPD regime labels
  - `compaction_debt`: whether deferred compaction work is accumulating
- **Actions:** `A = {Defer, CompactNow(rate_limit)}` where `rate_limit` is chosen
  from a small discrete set (e.g., {low, medium, high}).
- **Cost:** per time step,

  ```
  Cost(S, a) =
    w_space * space_amp
  + w_read  * read_rate_regime * read_amp(space_amp)
  + w_write * write_rate_regime * write_interference(a)
  + w_cpu   * compaction_cpu(a)
  ```

  Weights `w_*` are explicit policy constants and MUST be recorded in evidence
  ledger entries when policy is applied.

- **Transition:** `space_amp` tends to increase under write-heavy regimes and
  decrease under compaction actions; regime transitions are driven by BOCPD.

**Implementation guidance (normative):**
- Solve the MDP offline over a small discretized grid and embed the resulting
  policy as a deterministic lookup table (no floating-point instability).
- On BOCPD regime shifts, the controller MAY switch to a different precomputed
  policy table and MUST emit an evidence ledger entry describing the regime
  change and chosen action.
- If the policy is unavailable, fall back to the default threshold signal
  (`space_amp > 2.0`) with conservative rate limiting (graceful degradation).

**Compaction Algorithm (Background Task, Crash-Safe):**

Compaction MUST be:
- cancel-safe (safe at any `.await`),
- crash-safe (safe at any instruction boundary),
- cross-process safe (multiple processes may be reading),
- non-disruptive to p99 latency (rate-limited background region).

**Saga requirement (normative):** Compaction MUST be implemented as a Saga
(`asupersync::remote::Saga` semantics; §4.19.5) even when all I/O is local.
Each phase that could leave partial state MUST have a deterministic
compensation:
- If cancellation occurs before publication, temporary segments remain ignored
  and may be garbage-collected later.
- If cancellation occurs after new segments are durable but before locator/root
  update, the system MUST either complete publication or roll back pointers to
  a coherent pre-compaction view.

1.  **Mark Phase (Identify Live Symbols):**
    -   Start from `RootManifest` and active `CommitMarker` stream.
    -   Trace all reachable `CommitCapsule` objects.
    -   From capsules, trace all reachable `PageHistory` objects (up to GC horizon).
    -   From witness plane, trace reachable `ReadWitness`/`WriteWitness`/`IndexSegment` objects.
    -   Build a `BloomFilter` of live `ObjectId`s.

2.  **Compact Phase (Rewrite Logs):**
    -   Create new symbol log segment(s) using **temporary names**:
        `segment-XXXXXX.log.compacting` (never overwrite an existing segment).
    -   Scan old symbol logs. For each symbol record:
        -   If `ObjectId` is in live set (check Bloom + exact check): copy to new log.
        -   Else: discard (dead object).
    -   `fdatasync()` new segment files (and directory fsync if required by VFS).
    -   Write a new `cache/object_locator.cache.tmp` built from the rewritten logs.

3.  **Publish Phase (Two-Phase, Normative Ordering):**
    -   Atomically publish compacted segments:
        - `rename(segment-*.log.compacting, segment-*.log)`
        - `fsync(ecs/symbols/ dir)` if required by VFS.
    -   Atomically publish the new locator (MUST be AFTER segment publish):
        - `fdatasync(cache/object_locator.cache.tmp)`
        - `rename(cache/object_locator.cache.tmp, cache/object_locator.cache)`
        - `fsync(cache/ dir)` if required by VFS.
    -   **Ordering rule:** old segments MUST NOT be retired until both the new
        segments and the new locator are durable. This prevents a crash from
        leaving the system with neither a valid locator nor the old segments
        that the old locator points at.

4.  **Retire Phase (Space Reclamation):**
    -   Old segments are retired only once no active readers depend on them.
        This is tracked via segment leases / obligations (asupersync-style):
        a reader that may dereference an `ObjectLocator` entry holds a lease on
        the referenced segment(s).
    -   Unix: old segments MAY be unlinked once retired; open handles remain valid.
    -   Windows: deletion of open files is not supported; old segments MUST be
        renamed to `segment-*.log.retired` and deleted only after all handles
        are closed (lease set empty).

**Safety argument (sketch):**
- Compaction never mutates an existing segment; it only creates new segments.
- Publication is two-phase: until published, new segments/locator are ignored;
  after published, old segments are retained until reader leases drain.
- Therefore, at all times there exists at least one complete set of symbol logs
  sufficient to decode any reachable object under the retention policy.

---

## 8. Architecture: Crate Map and Dependencies

### 8.1 Workspace Structure

23 crates under `crates/`, plus supporting directories:

```
frankensqlite/
  Cargo.toml                     # Workspace root
  rust-toolchain.toml            # channel = "nightly"
  AGENTS.md                      # Agent guidelines
  COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md

  crates/
    fsqlite-types/               # PageNumber, SqliteValue, TxnId, Opcode, limits
    fsqlite-error/               # FrankenError, ErrorCode
    fsqlite-vfs/                 # Vfs/VfsFile traits, MemoryVfs, UnixVfs
    fsqlite-pager/               # Page cache, journal, state machine
    fsqlite-wal/                 # WAL frames, index, checkpoint, recovery
    fsqlite-mvcc/                # Page versioning, snapshots, conflicts, GC
    fsqlite-btree/               # B-tree: cursor, cell, balance, overflow, freelist
    fsqlite-ast/                 # SQL AST nodes
    fsqlite-parser/              # Lexer + recursive descent parser
    fsqlite-planner/             # Name resolution, WHERE, join ordering, cost
    fsqlite-vdbe/                # Bytecode VM (190+ opcodes), Mem values, sort
    fsqlite-func/                # Built-in scalar/aggregate/window functions
    fsqlite-ext-fts3/            # FTS3/FTS4
    fsqlite-ext-fts5/            # FTS5
    fsqlite-ext-rtree/           # R-tree + geopoly
    fsqlite-ext-json/            # JSON1
    fsqlite-ext-session/         # Session/changeset
    fsqlite-ext-icu/             # ICU collation
    fsqlite-ext-misc/            # generate_series, dbstat, csv, etc.
    fsqlite-core/                # Connection, prepare, schema, codegen
    fsqlite/                     # Public API facade
    fsqlite-cli/                 # Interactive shell (frankentui)
    fsqlite-harness/             # Conformance test runner

  conformance/                   # Golden output fixtures
  tests/                         # Workspace integration tests
  benches/                       # Criterion benchmarks
  fuzz/                          # Fuzz targets
  legacy_sqlite_code/            # C source reference
```

### 8.2 Dependency Layers

```
Layer 0 (leaves):     fsqlite-types    fsqlite-error
Layer 1 (storage):    fsqlite-vfs      fsqlite-ast
Layer 2 (cache):      fsqlite-pager    fsqlite-parser     fsqlite-func
Layer 3 (log+mvcc):   fsqlite-wal      fsqlite-mvcc       fsqlite-planner
Layer 4 (btree):      fsqlite-btree
Layer 5 (vm):         fsqlite-vdbe
Layer 6 (ext):        fsqlite-ext-{fts3,fts5,rtree,json,session,icu,misc}
Layer 7 (core):       fsqlite-core
Layer 8 (api):        fsqlite
Layer 9 (apps):       fsqlite-cli      fsqlite-harness
```

**Layering rationale (V1.7 errata):**

- **fsqlite-mvcc moved from Layer 6 to Layer 3.** The B-tree layer (L4) needs
  the `MvccPager` trait for page access. If MVCC stayed at L6, this would be
  a layer inversion (L4 depending on L6). The `MvccPager` *trait definition*
  lives in `fsqlite-pager` (L2); `fsqlite-mvcc` (L3) *implements* it. This
  way `fsqlite-btree` (L4) depends only on `fsqlite-pager` (L2) for the
  trait, and `fsqlite-core` (L7) wires the concrete implementation.

- **fsqlite-wal does NOT depend on fsqlite-pager** (breaking the cycle).
  Instead, `fsqlite-pager` defines a `CheckpointPageWriter` trait. During
  checkpoint, `fsqlite-wal` receives a `&dyn CheckpointPageWriter` callback
  from `fsqlite-core`, which provides page cache access without creating a
  compile-time crate dependency from wal -> pager. Both crates depend on
  `fsqlite-vfs` and `fsqlite-types` (L0-L1) without cycles.

### 8.3 Per-Crate Detailed Descriptions

**`fsqlite-types`** (~3,500 LOC estimated)

The foundational types crate with zero internal dependencies.

Key types and modules:
- `page.rs`: `PageNumber` (NonZeroU32), `PageData` (Vec<u8>), `PageSize` (validated power of 2)
- `value.rs`: `SqliteValue` enum (Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>))
- `opcode.rs`: `Opcode` enum with all 190+ VDBE opcodes, plus `OpcodeInfo` metadata
- `serial.rs`: `SerialType` (u64), serial type encoding/decoding, content size formulas
- `record.rs`: `Record` struct, `RecordHeader`, serialization/deserialization
- `txn.rs`: `TxnId` (u64 newtype), `TxnMode` enum (Deferred, Immediate, Exclusive, Concurrent)
- `flags.rs`: `OpenFlags`, `SyncFlags`, `AccessFlags`, `LockLevel` (bitflags)
- `limits.rs`: SQLite limits (SQLITE_MAX_LENGTH, SQLITE_MAX_COLUMN, etc.)
- `affinity.rs`: `TypeAffinity` enum, affinity determination from type names
- `collation.rs`: `CollationId`, built-in collation identifiers (BINARY, NOCASE, RTRIM)

Public API surface: ~80 types, all `#[derive(Debug, Clone)]`, most `Copy` where possible.

**`fsqlite-error`** (~800 LOC estimated)

Error types using `thiserror` derive.

Key types:
- `error.rs`: `FrankenError` enum (~40 variants mapping to SQLite error codes)
- `code.rs`: `ErrorCode` enum (SQLITE_OK, SQLITE_ERROR, SQLITE_BUSY, ..., ~30 primary codes)
- `extended.rs`: Extended error codes (SQLITE_BUSY_RECOVERY, SQLITE_BUSY_SNAPSHOT, etc.)
- `result.rs`: `type Result<T> = std::result::Result<T, FrankenError>`

Every variant carries context: the operation that failed, the page or table involved,
and optionally a source error (for I/O errors wrapping std::io::Error).

**`fsqlite-vfs`** (~2,500 LOC estimated)

Virtual filesystem abstraction. Equivalent to sqlite3_vfs + sqlite3_io_methods.

Modules:
- `traits.rs`: `Vfs` and `VfsFile` trait definitions
- `memory.rs`: `MemoryVfs` -- fully in-memory VFS for testing. Stores file data
  in `HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>`. Supports concurrent access.
- `unix.rs`: `UnixVfs` -- POSIX VFS using asupersync blocking I/O. File locking
  via `fcntl(F_SETLK)`. Implements all 5 SQLite lock levels (NONE, SHARED,
  RESERVED, PENDING, EXCLUSIVE).
- `flags.rs`: `VfsOpenFlags` (READONLY, READWRITE, CREATE, etc.)

Dependency rationale: depends on `fsqlite-types` for `PageNumber`, `OpenFlags`;
depends on `fsqlite-error` for `Result`. Uses `asupersync` for blocking I/O
pool in `UnixVfs`.

**`fsqlite-pager`** (~4,000 LOC estimated)

Page cache and transaction state machine. The core I/O layer.

Modules:
- `pager.rs`: `Pager` struct (the main type). State machine:
  `Open -> Reader -> Writer -> Error`. Manages database file handle, journal
  file, and the ARC cache. (WAL operations are in `fsqlite-wal`; the pager
  defines the `MvccPager` trait and `CheckpointPageWriter` trait but does
  not depend on `fsqlite-wal`.)
- `cache.rs`: `ArcCache` implementation (Section 6). Full ARC algorithm with
  MVCC-aware eviction.
- `page_ref.rs`: `PageRef` (RAII guard that pins a page in cache, decrements
  ref_count on drop).
- `journal.rs`: Rollback journal creation, page journaling, hot journal
  detection and rollback.
- `state.rs`: `PagerState` enum, transition validation.
- `header.rs`: Database header parsing and writing (100-byte header at offset 0).

Dependency rationale: needs `fsqlite-vfs` for file I/O; needs `fsqlite-types`
for `PageNumber`, `PageData`; needs `fsqlite-error` for error handling.

**`fsqlite-wal`** (~3,500 LOC estimated)

Write-ahead log implementation.

Modules:
- `wal.rs`: `Wal` struct. WAL file header parsing/writing. Frame append.
  Cumulative checksum computation (Section 7.1).
- `frame.rs`: `WalFrame` struct (24-byte header + page data). Frame
  serialization/deserialization.
- `index.rs`: `WalIndex` -- shared-memory hash table for page-to-frame lookup.
  Hash tables with linear probing, reader marks, lock bytes.
- `checkpoint.rs`: Checkpoint logic (PASSIVE, FULL, RESTART, TRUNCATE).
  Reads frames from WAL, writes pages to database file, resets WAL.
- `recovery.rs`: WAL recovery on database open. Validates checksum chain,
  replays committed transactions. RaptorQ self-healing integration.
- `raptorq.rs`: RaptorQ repair symbol generation for WAL commit groups.
  Encoding on commit, decoding during recovery.

Dependency rationale: needs `fsqlite-vfs` for WAL file and SHM file access;
needs `asupersync` for RaptorQ codec. Does NOT depend on `fsqlite-pager`
(V1.7 errata: the previous wal -> pager edge created a compile-time cycle;
checkpoint page-write access is now injected at runtime via
`&dyn CheckpointPageWriter`, defined in `fsqlite-pager`, passed by
`fsqlite-core` during checkpoint orchestration).

**`fsqlite-mvcc`** (~3,000 LOC estimated)

MVCC version management, the heart of the concurrency innovation.

Modules:
- `manager.rs`: `MvccManager` -- coordinates transactions, version store,
  page lock table, commit index, witness plane hooks, and GC.
- `snapshot.rs`: `Snapshot` struct (`high: CommitSeq`, `schema_epoch: SchemaEpoch`).
  `capture_snapshot()` logic. Visibility predicate (`commit_seq <= snapshot.high`).
- `version.rs`: `PageVersion` struct and version chains (arena-backed indices;
  ordered by `commit_seq`).
- `lock_table.rs`: `PageLockTable` (HashMap<PageNumber, TxnId>). `try_acquire`,
  `release`, `release_all`.
- `transaction.rs`: `Transaction` struct. Lifecycle: Active -> Committed/Aborted.
  Write set, intent log, witness keys, page locks.
- `commit.rs`: Commit validation (FCW via `CommitIndex` + merge ladder). Commit
  publication via WriteCoordinator (WAL group commit in Compatibility mode;
  tiny-marker sequencing in Native mode).
- `gc.rs`: Garbage collection. Horizon computation, version chain pruning,
  reclaimability predicate.
- `coordinator.rs`: `WriteCoordinator` -- wraps asupersync two-phase MPSC
  channel. Serializes the commit sequencing critical section: WAL appends in
  Compatibility mode; tiny marker/proof writes in Native mode.

Dependency rationale: needs `fsqlite-wal` for WAL append; needs `fsqlite-pager`
for page cache; needs `parking_lot` for fast Mutex/RwLock on hot-path
structures; needs `asupersync` for channels and RaptorQ.

**`fsqlite-btree`** (~5,000 LOC estimated)

B-tree storage engine. The most complex crate after `fsqlite-vdbe`.

Modules:
- `cursor.rs`: `BtCursor` with page stack traversal (max depth 20 for 4KB
  pages, max depth 40 for 512-byte pages). Position save/restore for cursor
  stability across modifications.
- `cell.rs`: Cell format parsing. `IntKeyCell` (table leaf), `BlobKeyCell`
  (index leaf), `InteriorCell`. Varint decoding for payload size and rowid.
- `balance.rs`: Page splitting and merging. `balance_nonroot` (redistribution
  among siblings), `balance_deeper` (new root creation on root overflow),
  `balance_quick` (fast-path append to rightmost leaf).
- `overflow.rs`: Overflow page chain management. Read/write payload spanning
  multiple overflow pages. Chain creation, traversal, and freeing.
- `free_list.rs`: Free page management. Trunk/leaf structure. Allocate from
  freelist or grow file. Deallocate to freelist.
- `payload.rs`: `BtreePayload` -- unified read/write abstraction for cell
  payloads that may span local storage + overflow pages.
- `table.rs`: Table B-tree operations (intkey). Create table, drop table,
  row count.
- `index.rs`: Index B-tree operations (blobkey). Create index, drop index.

Dependency rationale: needs `fsqlite-pager` (via `MvccPager` trait) for page
access; needs `fsqlite-types` for `PageNumber`, `SerialType`, cell format types.

**`fsqlite-ast`** (~2,000 LOC estimated)

SQL abstract syntax tree node types.

Modules:
- `stmt.rs`: Top-level `Statement` enum (Select, Insert, Update, Delete,
  CreateTable, CreateIndex, CreateView, CreateTrigger, Drop, AlterTable,
  Attach, Detach, Begin, Commit, Rollback, Savepoint, Release, Pragma,
  Vacuum, Reindex, Analyze, Explain).
- `expr.rs`: `Expr` enum (~30 variants: Literal, Column, BinaryOp, UnaryOp,
  Between, In, Like, Case, Cast, Exists, Subquery, FunctionCall, Aggregate,
  Window, Collate, Raise, JsonAccess, etc.)
- `select.rs`: `SelectStatement`, `SelectCore`, `CompoundOp`, `JoinClause`,
  `JoinType`, `OrderingTerm`, `LimitClause`, `WithClause`, `Cte`.
- `table_ref.rs`: `TableRef` enum (Named, Subquery, JoinExpr, FunctionCall).
- `ddl.rs`: `ColumnDef`, `TableConstraint`, `IndexedColumn`, `ForeignKeyClause`.
- `literal.rs`: `Literal` enum (Integer, Float, String, Blob, Null, True, False, CurrentTime, CurrentDate, CurrentTimestamp).
- `operator.rs`: `BinaryOp`, `UnaryOp` enums with all SQL operators.
- `span.rs`: `Span` (byte offset range in source text) for error reporting.

All AST nodes carry `Span` for source location.

**`fsqlite-parser`** (~4,500 LOC estimated)

SQL lexer and recursive descent parser.

Modules:
- `lexer.rs`: Tokenizer. Token types enum (~150 variants). Memchr-accelerated
  scanning for string delimiters and comment markers. Line/column tracking.
- `parser.rs`: Recursive descent parser. One method per grammar production.
  Pratt precedence for expression parsing.
- `keyword.rs`: Perfect hash for 150+ SQL keywords (generated at build time
  or via phf crate).
- `error.rs`: Parse error types with source span, expected tokens, recovery hints.

**`fsqlite-planner`** (~3,000 LOC estimated)

Query planning and optimization.

Modules:
- `resolve.rs`: Name resolution. Table alias binding, column reference
  resolution, star expansion, subquery scoping.
- `where_clause.rs`: WHERE clause analysis. Extracting index-usable terms,
  range constraints, OR optimization.
- `join.rs`: Join ordering. Greedy algorithm for > 6 tables, exhaustive
  search for <= 6.
- `cost.rs`: Cost model. Estimated I/O per access path. Index selectivity
  estimation from sqlite_stat1/stat4.
- `index.rs`: Index usability determination. Which indexes can serve a
  given WHERE clause. Covering index detection.
- `plan.rs`: `QueryPlan` output type. Access path per table, join order,
  estimated cost.

**`fsqlite-vdbe`** (~6,000 LOC estimated)

The bytecode virtual machine. Largest crate by estimated LOC.

Modules:
- `vm.rs`: Fetch-execute loop. `VdbeExec` struct. Match-based opcode dispatch.
  Program counter management, jump resolution.
- `mem.rs`: `Mem` (sqlite3_value). Multi-representation storage (integer + text
  cached simultaneously). Type affinity application. Comparison with collation.
- `cursor.rs`: `VdbeCursor` wrapping `BtCursor`. Deferred seek, cached row
  decoding, pseudo-table support.
- `program.rs`: `VdbeProgram` (Vec<VdbeOp>). Register allocation metadata.
  Coroutine state.
- `op.rs`: `VdbeOp` struct (opcode, p1, p2, p3, p4, p5). `P4` enum variants.
- `sort.rs`: External merge sort for ORDER BY. Sorter cursor.
- `compare.rs`: Record comparison with collation sequences. Key comparison
  for index lookups.
- `func_dispatch.rs`: Function call dispatch. Scalar, aggregate, window.
- `subtype.rs`: Subtype management (for JSON functions).

**`fsqlite-func`** (~2,500 LOC estimated)

Built-in functions (~80 total).

Modules:
- `scalar.rs`: ~60 scalar functions (abs, char, hex, instr, length, lower, etc.)
- `aggregate.rs`: ~12 aggregate functions (avg, count, sum, group_concat, etc.)
- `window.rs`: ~11 window functions (row_number, rank, lag, lead, etc.)
- `math.rs`: Math functions (acos, sin, sqrt, log, etc.)
- `info.rs`: sqlite_version, changes, total_changes, last_insert_rowid
- `registry.rs`: `FunctionRegistry` -- maps (name, arg_count) to function impl

**`fsqlite-ext-json`** (~2,000 LOC)
JSON1 extension. json(), json_extract(), json_set(), json_remove(), json_type(),
json_valid(), json_each/json_tree virtual tables, JSONB binary format, -> and ->> operators.

**`fsqlite-ext-fts5`** (~4,000 LOC)
Full-text search v5. Porter stemmer, unicode61 tokenizer, inverted index, BM25
ranking, highlight/snippet auxiliary functions, custom tokenizer API.

**`fsqlite-ext-fts3`** (~2,000 LOC)
FTS3/4 compatibility layer. matchinfo(), offsets(), snippet(). Largely wraps FTS5.

**`fsqlite-ext-rtree`** (~2,000 LOC)
R-tree spatial index. R*-tree insertion, nearest-neighbor search. Geopoly extension.

**`fsqlite-ext-session`** (~1,500 LOC)
Session extension. Changeset/patchset generation, application, and inversion.

**`fsqlite-ext-icu`** (~800 LOC)
ICU collation integration. Unicode-aware comparison, case folding, FTS tokenizer.

**`fsqlite-ext-misc`** (~1,500 LOC)
Miscellaneous: generate_series, dbstat, dbpage, csv virtual table, decimal,
uuid, ieee754, carray.

**`fsqlite-core`** (~5,000 LOC estimated)

The orchestration layer that wires everything together.

Modules:
- `connection.rs`: `Connection` struct. Open/close, ATTACH/DETACH, schema cache,
  auto-commit state, busy handler, authorization callback.
- `prepare.rs`: SQL compilation pipeline: parse -> resolve -> plan -> codegen.
  Statement cache (LRU of prepared statements, keyed by SQL text hash).
- `schema.rs`: Schema loading from sqlite_master. Table, Index, View, Trigger
  objects. Schema cookie validation and reload.
- `codegen.rs`: AST-to-VDBE code generation. SELECT, INSERT, UPDATE, DELETE
  compilation. Expression codegen. Subquery/CTE coroutine generation.
- `pragma.rs`: PRAGMA command implementation (~80 pragmas).
- `auth.rs`: Authorization callback dispatch.
- `vtab.rs`: Virtual table module registration and lifecycle.

**`fsqlite`** (~1,000 LOC estimated)

Public API facade. `Database` is the primary user-facing type (wraps
`Connection` from `fsqlite-core` with convenience methods). Re-exports:

```rust
/// `Database` wraps `Connection` with `open()`, `open_in_memory()`, etc.
/// This is the canonical public type; `Connection` is the internal name.
pub struct Database(Connection);
pub use fsqlite_core::{Statement, Row, Transaction};
pub use fsqlite_types::{SqliteValue, PageNumber};
pub use fsqlite_error::{FrankenError, ErrorCode, Result};
pub use fsqlite_vfs::{Vfs, VfsFile, MemoryVfs};
```

Adds convenience methods: `Connection::open()`, `Connection::open_in_memory()`,
`Connection::execute(cx, sql).await`, `Connection::query_row(cx, sql).await`.

**`fsqlite-cli`** (~2,000 LOC estimated)
Interactive shell using frankentui. Dot-commands (.tables, .schema, .mode, .import,
.dump, .headers, .separator). Output modes (column, csv, json, line, list, table).
Tab completion, syntax highlighting, history.

**`fsqlite-harness`** (~1,500 LOC estimated)
Conformance test runner. Runs identical SQL against FrankenSQLite and C sqlite3.
Compares output row-by-row. Error code matching. Golden file management.

### 8.4 Dependency Edges with Rationale

| From | To | Rationale |
|------|----|-----------|
| fsqlite-vfs | fsqlite-types | OpenFlags, PageNumber |
| fsqlite-vfs | fsqlite-error | Result type |
| fsqlite-pager | fsqlite-vfs | File I/O |
| fsqlite-pager | fsqlite-types | PageNumber, PageData |
| fsqlite-wal | fsqlite-vfs | WAL file + SHM file access |
| fsqlite-wal | fsqlite-types | PageNumber, frame types |
| ~~fsqlite-wal~~ | ~~fsqlite-pager~~ | ~~REMOVED (V1.7): was "page cache during checkpoint" -- created a compile-time cycle. Checkpoint now receives `&dyn CheckpointPageWriter` at runtime from fsqlite-core.~~ |
| fsqlite-mvcc | fsqlite-wal | WAL append during commit |
| fsqlite-mvcc | fsqlite-pager | Page cache (via MvccPager trait impl), CheckpointPageWriter impl |
| fsqlite-mvcc | fsqlite-types | TxnId, PageNumber, CommitSeq, Snapshot |
| fsqlite-mvcc | parking_lot | Fast Mutex for lock table (hot path) |
| fsqlite-mvcc | asupersync | Two-phase MPSC channel, RaptorQ codec |
| fsqlite-btree | fsqlite-pager | Page access (via MvccPager trait defined in fsqlite-pager) |
| fsqlite-btree | fsqlite-types | Cell formats, SerialType |
| fsqlite-ast | fsqlite-types | SqliteValue (for AST literals) |
| fsqlite-parser | fsqlite-ast | Produces AST nodes |
| fsqlite-parser | fsqlite-types | Token types, keyword IDs |
| fsqlite-parser | memchr | SIMD byte scanning in lexer |
| fsqlite-planner | fsqlite-ast | Consumes AST, produces plan |
| fsqlite-planner | fsqlite-types | Column metadata, affinities |
| fsqlite-vdbe | fsqlite-btree | B-tree cursor operations |
| fsqlite-vdbe | fsqlite-pager | Direct page access for some opcodes |
| fsqlite-vdbe | fsqlite-func | Function dispatch (ScalarFunction, AggregateFunction, etc.) |
| fsqlite-vdbe | fsqlite-types | Opcode enum, Mem values |
| fsqlite-func | fsqlite-types | SqliteValue args and return |
| fsqlite-core | (all above) | Orchestration layer |
| fsqlite | fsqlite-core | Public API wraps core |
| fsqlite-cli | fsqlite | Uses public API |
| fsqlite-cli | frankentui | TUI framework |
| fsqlite-harness | fsqlite | Uses public API for testing |

### 8.5 Feature Flags

```toml
# Status: not yet implemented in Cargo manifests.
#
# Feature flags MUST live on a real package manifest (e.g. `crates/fsqlite/Cargo.toml`),
# not the workspace root (which is a virtual manifest). The target shape is:
#
# crates/fsqlite/Cargo.toml (planned)
[features]
default = ["json", "fts5", "rtree"]

json = ["dep:fsqlite-ext-json"]
fts5 = ["dep:fsqlite-ext-fts5"]
fts3 = ["dep:fsqlite-ext-fts3"]
rtree = ["dep:fsqlite-ext-rtree"]
session = ["dep:fsqlite-ext-session"]
icu = ["dep:fsqlite-ext-icu"]
misc = ["dep:fsqlite-ext-misc"]

# Enables FrankenSQLite's RaptorQ-backed repair/replication hooks.
# Note: asupersync's RaptorQ module is not feature-gated upstream; this flag
# controls FrankenSQLite integration code only.
raptorq = []

# MVCC is core; use runtime configuration to choose default transaction behavior.
mvcc = []
```

### 8.6 Build Configuration

```toml
[workspace.package]
edition = "2024"
license = "MIT"
repository = "https://github.com/Dicklesworthstone/frankensqlite"
rust-version = "1.85"

[workspace.lints.rust]
unsafe_code = "forbid"            # No unsafe anywhere in workspace

[workspace.lints.clippy]
pedantic = { level = "deny", priority = -1 }
nursery = { level = "deny", priority = -1 }
cast_precision_loss = { level = "allow", priority = 1 }
doc_markdown = { level = "allow", priority = 1 }
missing_const_for_fn = { level = "allow", priority = 1 }
uninlined_format_args = { level = "allow", priority = 1 }
missing_errors_doc = { level = "allow", priority = 1 }
missing_panics_doc = { level = "allow", priority = 1 }
module_name_repetitions = { level = "allow", priority = 1 }
must_use_candidate = { level = "allow", priority = 1 }
option_if_let_else = { level = "allow", priority = 1 }

[profile.release]
opt-level = "z"        # Default release optimizes for size; use release-perf for throughput characterization
lto = true             # Whole-program optimization
codegen-units = 1      # Single codegen unit for maximum optimization
panic = "abort"        # No unwinding overhead
strip = true           # Strip debug info from release binary

[profile.release-perf]
inherits = "release"
opt-level = 3          # Throughput characterization/profile runs

[profile.dev]
opt-level = 1          # Mild optimization for acceptable test speed
```

---

## 9. Trait Hierarchy

**Cx Everywhere Rule:** Every trait method that touches I/O, acquires locks,
or could block MUST accept `&Cx` (asupersync's capability context) as its
first parameter. This enables:
- **Cancellation:** Any operation can be cancelled by the caller's context.
- **Deadline propagation:** Timeout budgets flow through the entire call chain.
- **Capability narrowing:** Callers can restrict what callees are allowed to do.

The `Cx` parameter appears in VFS, MvccPager, and any async-capable method.
Pure computation (e.g., `CollationFunction::compare`, `ScalarFunction::call`
for CPU-only work) does not take `Cx`. When in doubt, include `Cx`.

**Sealed trait discipline (internal invariants):**

Some traits are *implementation-internal* interfaces that encode MVCC safety
invariants and layering constraints. These traits MUST be **sealed** so
downstream crates cannot provide alternate implementations that violate
invariants or bypass required checks.

- **Open extension points (user-implementable):** `Vfs`, `VfsFile`,
  `ScalarFunction`, `AggregateFunction`, `WindowFunction`, `VirtualTable`,
  `VirtualTableCursor`, `CollationFunction`, `Authorizer`.
- **Internal-only (sealed):** `MvccPager`, `BtreeCursorOps` (and any similar
  trait whose implementations must preserve engine invariants).

**Sealing pattern (Rust):**
```rust
mod sealed { pub trait Sealed {} } // private to the defining crate

pub trait MvccPager: sealed::Sealed + Send + Sync { /* ... */ }
```

Because the `sealed` module is private, only the defining crate can implement
the trait. Test mocks for sealed traits live alongside the trait definition
(and are exported as values/types for other crates to use in tests).

### 9.1 Storage Traits

```rust
/// Virtual filesystem abstraction.
/// Equivalent to sqlite3_vfs in C SQLite.
///
/// # Thread Safety
/// Implementations must be Send + Sync because a single VFS instance is shared
/// across all connections in a process. The VFS itself is stateless (or
/// internally synchronized); individual file handles carry mutable state.
///
/// # Error Handling
/// All methods return `Result<T, FrankenError>`. I/O errors are wrapped in
/// `FrankenError::IoError(std::io::Error)`. Permission errors map to
/// `FrankenError::CantOpen` or `FrankenError::Auth`.
pub trait Vfs: Send + Sync {
    /// The file handle type produced by this VFS.
    type File: VfsFile;

    /// Open a file at the given path with the specified flags.
    ///
    /// `path` is None for temporary files (the VFS chooses a path).
    /// Returns the opened file handle and the flags that were actually used
    /// (some flags may be modified, e.g., READWRITE downgraded to READONLY).
    ///
    /// # Errors
    /// - `FrankenError::CantOpen` if the file cannot be opened.
    /// - `FrankenError::IoError` for underlying I/O failures.
    fn open(&self, cx: &Cx, path: Option<&Path>, flags: VfsOpenFlags)
        -> Result<(Self::File, VfsOpenFlags)>;

    /// Delete a file. If `sync_dir` is true, also sync the directory
    /// containing the file to ensure the deletion is durable.
    ///
    /// # Errors
    /// - `FrankenError::IoError` if deletion fails.
    /// - Not an error if the file does not exist.
    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()>;

    /// Check whether a file exists or has specific properties.
    ///
    /// `flags` determines what to check:
    /// - `AccessFlags::EXISTS`: file exists
    /// - `AccessFlags::READWRITE`: file exists and is read-write
    /// - `AccessFlags::READ`: file exists and is readable
    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool>;

    /// Convert a relative path to an absolute (canonical) path.
    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf>;

    /// Fill `buf` with random bytes. Used for WAL salt generation.
    fn randomness(&self, cx: &Cx, buf: &mut [u8]);

    /// Return the current time as a Julian day number (fractional days
    /// since noon, November 24, 4714 BC, proleptic Gregorian calendar).
    fn current_time(&self, cx: &Cx) -> f64;
}

/// An open file handle within a VFS.
/// Equivalent to sqlite3_file + sqlite3_io_methods in C SQLite.
///
/// # Thread Safety
/// Send + Sync because file handles may be shared across threads (e.g.,
/// the WAL file is accessed by both readers and the write coordinator).
/// Implementations must use internal synchronization for mutable state.
///
/// # Lifetime
/// A VfsFile is owned by the component that opened it (Pager, Wal).
/// It is closed when dropped or when `close()` is called explicitly.
pub trait VfsFile: Send + Sync {
    /// Close the file handle and release all resources.
    /// After close(), no other methods may be called.
    fn close(&mut self, cx: &Cx) -> Result<()>;

    /// Read `buf.len()` bytes from the file at the given byte offset.
    /// Returns the number of bytes actually read (may be less than
    /// buf.len() if the file is shorter than offset + buf.len()).
    /// Short reads zero-fill the remainder of buf.
    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` to the file at the given byte offset.
    /// The file is extended if necessary.
    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()>;

    /// Truncate the file to exactly `size` bytes.
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()>;

    /// Sync file contents to durable storage.
    /// `flags`: SYNC_NORMAL or SYNC_FULL (FULL also syncs metadata).
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()>;

    /// Return the current file size in bytes.
    fn file_size(&self, cx: &Cx) -> Result<u64>;

    /// Acquire or upgrade a file lock.
    /// Lock levels: NONE < SHARED < RESERVED < PENDING < EXCLUSIVE.
    /// Locks are advisory; they coordinate concurrent access between
    /// processes but do not prevent direct file I/O.
    ///
    /// # Errors
    /// - `FrankenError::Busy` if the lock cannot be acquired (another
    ///   process holds a conflicting lock).
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()>;

    /// Release or downgrade a file lock.
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()>;

    /// Check whether another process holds a RESERVED lock.
    /// Used to determine if a write transaction is in progress elsewhere.
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool>;

    /// Return the sector size of the underlying storage device.
    /// Typically 512 (HDD) or 4096 (SSD). Used for WAL frame alignment.
    fn sector_size(&self) -> u32;

    /// Return device characteristics flags.
    /// Bit flags indicating device properties: IOCAP_ATOMIC, IOCAP_SAFE_APPEND,
    /// IOCAP_SEQUENTIAL, etc. Used to optimize sync behavior.
    fn device_characteristics(&self) -> u32;

    // --- Shared-memory methods (required for WAL mode) ---

    /// Map a region of shared memory. `region` is a 0-based index of 32KB
    /// regions. If `extend` is true and the region does not exist, create it.
    /// Returns a safe `ShmRegion` handle wrapping the mapped region.
    ///
    /// # Safety note
    /// The underlying mmap returns a raw pointer. The `ShmRegion` wrapper
    /// (defined in `fsqlite-vfs`) encapsulates the `unsafe` dereference
    /// behind a safe API (`read_u32(offset)`, `write_u32(offset, val)`,
    /// `as_slice()`, `as_mut_slice()`). The `fsqlite-vfs` crate is listed
    /// in the `unsafe_code` exception set (see §1.4) alongside the
    /// prefetch helper crate, because VFS implementations inherently
    /// require platform-specific unsafe operations (mmap, mlock, madvise).
    /// (Equivalent to sqlite3_io_methods.xShmMap)
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool)
        -> Result<ShmRegion>;

    /// Acquire or release a shared-memory lock.
    /// `offset` and `n` define a range of lock slots.
    /// `flags`: SHM_LOCK | (SHM_SHARED | SHM_EXCLUSIVE).
    /// (Equivalent to sqlite3_io_methods.xShmLock)
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32)
        -> Result<()>;

    /// Memory barrier for shared memory -- ensures all prior SHM writes are
    /// visible to other processes before subsequent reads.
    /// (Equivalent to sqlite3_io_methods.xShmBarrier)
    fn shm_barrier(&self);

    /// Unmap all shared-memory regions. If `delete` is true, also delete
    /// the underlying SHM file.
    /// (Equivalent to sqlite3_io_methods.xShmUnmap)
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()>;
}

/// MVCC-aware page access. The primary interface for B-tree and VDBE layers.
///
/// # Thread Safety
/// Send + Sync. Multiple transactions from different threads call into the
/// same MvccPager concurrently. The implementation uses internal locking
/// (version store RwLock, page lock table Mutex) for synchronization.
///
/// # Lifetime Relationships
/// The MvccPager outlives all Transactions it creates. Transaction holds
/// a reference (via Arc) to the MvccPager's internal state.
///
/// **Type placement note:** The `Transaction` type referenced below MUST be
/// defined in `fsqlite-pager` (or `fsqlite-types`), NOT in `fsqlite-mvcc`.
/// Otherwise `fsqlite-pager` (L2) would depend on `fsqlite-mvcc` (L3),
/// creating a circular dependency since `fsqlite-mvcc` depends on
/// `fsqlite-pager`. The concrete `Transaction` struct in `fsqlite-mvcc`
/// implements a pager-level `TransactionHandle` trait defined here.
mod sealed { pub trait Sealed {} } // private to the defining crate

pub trait MvccPager: sealed::Sealed + Send + Sync {
    /// Begin a new transaction with the specified mode.
    /// Serialized mode acquires the global write mutex immediately.
    /// Concurrent mode does not acquire any locks until write_page().
    fn begin(&self, cx: &Cx, mode: TxnMode) -> Result<Transaction>;

    /// Read a page within a transaction. Returns a pinned page reference.
    /// The page is resolved through: write_set -> version_chain -> disk.
    /// Tracks the page in the transaction's read set and registers a `WitnessKey`
    /// in the SSI witness plane (register_read; §5.7).
    fn get_page(&self, cx: &Cx, txn: &Transaction, pgno: PageNumber) -> Result<PageRef>;

    /// Write a page within a transaction.
    /// In Concurrent mode, acquires a page lock (returns SQLITE_BUSY if held),
    /// and updates SSI rw-antidependency state.
    /// In Serialized mode, the global mutex is already held.
    fn write_page(&self, cx: &Cx, txn: &mut Transaction, pgno: PageNumber, data: PageData) -> Result<()>;

    /// Allocate a new page (from freelist or by growing the file).
    fn allocate_page(&self, cx: &Cx, txn: &mut Transaction) -> Result<PageNumber>;

    /// Mark a page as free (add to freelist).
    fn free_page(&self, cx: &Cx, txn: &mut Transaction, pgno: PageNumber) -> Result<()>;

    /// Commit the transaction. SSI validation (abort if pivot),
    /// first-committer-wins check, merge ladder (§5.10) (rebase + structured patch),
    /// WAL append,
    /// version publishing, witness-plane evidence publication/proof emission,
    /// lock release.
    /// Returns SQLITE_BUSY_SNAPSHOT on SSI abort or conflict.
    fn commit(&self, cx: &Cx, txn: Transaction) -> Result<()>;

    /// Abort the transaction. Discards write set, releases locks,
    /// and leaves monotonic witness evidence to be ignored and GC'd by horizons.
    /// Never fails (panics on poisoned mutex, which is unrecoverable anyway).
    fn rollback(&self, cx: &Cx, txn: Transaction);
}

/// Cursor operations over a B-tree.
///
/// SQLite has two fundamentally different B-tree types:
/// - **Table B-trees** (intkey): keyed by i64 rowid. `pKey` is NULL;
///   `nKey` carries the rowid. Leaf cells store a rowid + record payload.
/// - **Index B-trees** (blobkey): keyed by a serialized record (the index
///   columns concatenated in SQLite record format). `pKey` points to the
///   serialized key; `nKey` is its byte length.
///
/// C SQLite exposes separate functions for the two types:
/// `sqlite3BtreeTableMoveTo(BtCursor*, i64 intKey)` vs
/// `sqlite3BtreeIndexMoveto(BtCursor*, UnpackedRecord*)`.
/// We mirror this split to prevent type confusion.
///
/// # Thread Safety
/// NOT Send or Sync. A cursor is bound to a single transaction and
/// should only be used from one thread at a time. The VDBE execution
/// loop is single-threaded per statement.
pub trait BtreeCursorOps: sealed::Sealed {
    // --- Seek methods (type-specific) ---

    /// Position an *index* cursor at or near the given serialized key.
    /// Returns the cursor's final position relative to the key.
    /// (Equivalent to sqlite3BtreeIndexMoveto)
    fn index_move_to(&mut self, cx: &Cx, key: &[u8]) -> Result<CursorPosition>;

    /// Position a *table* cursor at or near the given rowid.
    /// Returns the cursor's final position relative to the rowid.
    /// (Equivalent to sqlite3BtreeTableMoveTo)
    fn table_move_to(&mut self, cx: &Cx, rowid: i64) -> Result<CursorPosition>;

    // --- Navigation (cursor-type-agnostic) ---

    /// Position the cursor at the first (smallest key) entry.
    /// Returns false if the tree is empty. (VDBE: OP_Rewind)
    fn first(&mut self, cx: &Cx) -> Result<bool>;

    /// Position the cursor at the last (largest key) entry.
    /// Returns false if the tree is empty. (VDBE: OP_Last)
    fn last(&mut self, cx: &Cx) -> Result<bool>;

    /// Advance to the next entry. Returns false if no more entries.
    fn next(&mut self, cx: &Cx) -> Result<bool>;

    /// Move to the previous entry. Returns false if at the beginning.
    fn prev(&mut self, cx: &Cx) -> Result<bool>;

    // --- Mutation (type-specific) ---

    /// Insert a serialized record into an *index* B-tree.
    /// The key is the full serialized record (index columns + rowid).
    /// May trigger page splits (balance operations).
    fn index_insert(&mut self, cx: &Cx, key: &[u8]) -> Result<()>;

    /// Insert a row into a *table* (intkey) B-tree.
    /// `rowid` is the integer primary key; `data` is the record payload
    /// (header + body per §11 record format).
    /// May trigger page splits (balance operations).
    fn table_insert(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()>;

    /// Delete the entry at the cursor's current position.
    /// May trigger page merges.
    fn delete(&mut self, cx: &Cx) -> Result<()>;

    // --- Accessors ---

    /// Read the full cell payload of the current entry.
    /// For table B-trees: returns the record payload (not the rowid).
    /// For index B-trees: returns the serialized key.
    fn payload(&self) -> Result<&[u8]>;

    /// Read the rowid of the current entry.
    /// For table B-trees: the integer primary key.
    /// For index B-trees: extracted from the trailing field of the
    /// serialized key (index records always end with the rowid).
    fn rowid(&self) -> Result<i64>;

    /// Return true if the cursor is positioned past the last entry.
    fn eof(&self) -> bool;
}

/// Callback trait for WAL checkpoint: the WAL layer calls this to write
/// checkpointed pages back to the database file. Defined in `fsqlite-pager`
/// to break the pager<->wal compile-time cycle. `fsqlite-wal` receives
/// `&dyn CheckpointPageWriter` at runtime from `fsqlite-core` (§8.2.5).
pub trait CheckpointPageWriter: Send {
    /// Write `data` to page `pgno` in the database file.
    /// Called during checkpoint to transfer WAL frames back to the main DB.
    fn write_page(&mut self, cx: &Cx, pgno: PageNumber, data: &[u8]) -> Result<()>;

    /// Truncate the database file to `n_pages` pages.
    /// Called when the WAL contains a commit record with a smaller DB size.
    fn truncate(&mut self, cx: &Cx, n_pages: u32) -> Result<()>;

    /// Sync the database file to durable storage after checkpoint writes.
    fn sync(&mut self, cx: &Cx) -> Result<()>;
}
```

### 9.2 Function Traits

```rust
/// A scalar function (deterministic or non-deterministic).
/// Equivalent to xFunc in sqlite3_create_function.
///
/// # Thread Safety
/// Send + Sync because function objects are shared across connections
/// and may be called concurrently by different VDBE executions.
pub trait ScalarFunction: Send + Sync {
    /// Invoke the function with the given arguments.
    /// Returns the result value, or an error.
    ///
    /// # Errors
    /// - `FrankenError::Error` with a message for domain errors (e.g., abs(-9223372036854775808))
    /// - `FrankenError::TooBig` if result exceeds SQLITE_MAX_LENGTH
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue>;

    /// Whether this function is deterministic (same inputs always produce same output).
    /// Deterministic functions can be optimized (e.g., constant folding).
    fn is_deterministic(&self) -> bool { true }

    /// Number of arguments. -1 means variadic.
    fn num_args(&self) -> i32;

    /// Function name (for error messages and EXPLAIN output).
    fn name(&self) -> &str;
}

/// An aggregate function with step/finalize semantics.
/// Equivalent to xStep + xFinal in sqlite3_create_function.
///
/// TYPE ERASURE NOTE: The FunctionRegistry stores
/// `Arc<dyn AggregateFunction<State = Box<dyn Any + Send>>>`. Since
/// `Box<dyn Any + Send>` does NOT implement `Default`, we use a factory
/// method `initial_state()` instead of the `Default` bound. Concrete
/// implementations use a type-erasing wrapper (`AggregateAdapter<F>`)
/// that internally creates the concrete state type and wraps it in
/// `Box<dyn Any + Send>`.
pub trait AggregateFunction: Send + Sync {
    /// Aggregate accumulator state. Created via `initial_state()` at the
    /// start of each aggregation group.
    type State: Send;

    /// Create initial accumulator state for a new aggregation group.
    /// Replaces `Default::default()` to support type-erased storage.
    fn initial_state(&self) -> Self::State;

    /// Process one row. Called once per row in the group.
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Produce the final result for the group.
    /// Consumes the state (the accumulator is no longer needed).
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;

    fn num_args(&self) -> i32;
    fn name(&self) -> &str;
}

/// A window function with step/inverse/value/finalize semantics.
/// Equivalent to xStep + xInverse + xValue + xFinal.
pub trait WindowFunction: Send + Sync {
    type State: Send;  // uses initial_state() factory, same as AggregateFunction

    /// Create initial accumulator state for a new window partition/group.
    fn initial_state(&self) -> Self::State;

    /// Add a row to the window frame.
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Remove a row from the window frame (for sliding windows).
    /// This is the key difference from aggregate: window functions must
    /// support efficient removal of rows that have left the frame.
    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Return the current value of the window function without consuming state.
    /// Called after each step/inverse to produce the result for the current row.
    fn value(&self, state: &Self::State) -> Result<SqliteValue>;

    /// Produce the final value and consume the state.
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;

    fn num_args(&self) -> i32;
    fn name(&self) -> &str;
}
```

### 9.3 Extension Traits

```rust
/// A virtual table implementation.
/// Equivalent to sqlite3_module in C SQLite.
///
/// # Thread Safety
/// Send + Sync. A virtual table module is registered once and shared.
/// Individual table instances may have mutable state protected by
/// internal locks.
pub trait VirtualTable: Send + Sync {
    type Cursor: VirtualTableCursor;

    /// Create a new virtual table (called for CREATE VIRTUAL TABLE).
    /// Distinct from `connect`: `create` may create backing storage.
    /// Default: delegates to `connect` (eponymous tables).
    fn create(db: &Database, args: &[&str]) -> Result<Self> where Self: Sized {
        Self::connect(db, args)
    }

    /// Connect to an existing virtual table (called on subsequent opens).
    fn connect(db: &Database, args: &[&str]) -> Result<Self> where Self: Sized;

    /// Inform the planner about available indexes and their estimated costs.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;

    /// Open a new cursor for scanning the virtual table.
    fn open(&self) -> Result<Self::Cursor>;

    /// Disconnect from the virtual table (drop the instance).
    fn disconnect(&mut self) -> Result<()>;

    /// Destroy the virtual table and its backing storage (DROP VIRTUAL TABLE).
    /// Default: delegates to `disconnect`.
    fn destroy(&mut self) -> Result<()> { self.disconnect() }

    /// Perform INSERT, UPDATE, or DELETE on the virtual table.
    /// `args[0]` = old rowid (None for INSERT), `args[1]` = new rowid,
    /// `args[2..]` = column values. Returns the new rowid for INSERT.
    /// Without this method, virtual tables are read-only.
    fn update(&mut self, _args: &[SqliteValue]) -> Result<Option<i64>> {
        Err(FrankenError::ReadOnly)
    }

    /// Begin a transaction on this virtual table.
    fn begin(&mut self) -> Result<()> { Ok(()) }

    /// Sync phase of two-phase commit.
    fn sync(&mut self) -> Result<()> { Ok(()) }

    /// Commit the current transaction.
    fn commit(&mut self) -> Result<()> { Ok(()) }

    /// Rollback the current transaction.
    fn rollback(&mut self) -> Result<()> { Ok(()) }

    /// Rename the virtual table.
    fn rename(&mut self, _new_name: &str) -> Result<()> {
        Err(FrankenError::Unsupported)
    }

    /// Create a savepoint (n = savepoint depth).
    fn savepoint(&mut self, _n: i32) -> Result<()> { Ok(()) }

    /// Release a savepoint.
    fn release(&mut self, _n: i32) -> Result<()> { Ok(()) }

    /// Rollback to a savepoint.
    fn rollback_to(&mut self, _n: i32) -> Result<()> { Ok(()) }
}

/// A cursor for iterating over a virtual table.
pub trait VirtualTableCursor: Send {
    /// Begin a scan with the given filter parameters.
    /// `idx_num` and `idx_str` come from best_index().
    fn filter(&mut self, idx_num: i32, idx_str: Option<&str>,
              args: &[SqliteValue]) -> Result<()>;

    /// Advance to the next row. Call after filter() and between rows.
    fn next(&mut self) -> Result<()>;

    /// Return true if the cursor has moved past the last row.
    fn eof(&self) -> bool;

    /// Write the value of column `col` into the context.
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()>;

    /// Return the rowid of the current row.
    fn rowid(&self) -> Result<i64>;
}
```

### 9.4 Collation and Authorization Traits

```rust
/// A collation function for string comparison.
/// Equivalent to sqlite3_create_collation.
///
/// The collation determines the sort order for text values.
/// Built-in collations: BINARY (memcmp), NOCASE (case-insensitive ASCII),
/// RTRIM (ignore trailing spaces).
pub trait CollationFunction: Send + Sync {
    /// Compare two strings according to this collation.
    /// Returns Ordering::Less, Equal, or Greater.
    ///
    /// The inputs are UTF-8 encoded byte slices.
    /// The comparison must be deterministic, antisymmetric, and transitive.
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    /// Collation name (e.g., "BINARY", "NOCASE", "my_collation").
    fn name(&self) -> &str;
}

/// Authorization callback.
/// Equivalent to sqlite3_set_authorizer.
///
/// Called during SQL compilation (not execution) to approve or deny
/// each operation. Used for sandboxing untrusted SQL.
pub trait Authorizer: Send + Sync {
    /// Called for each operation during SQL compilation.
    /// Returns AuthResult::Ok to allow, Deny to reject with error,
    /// or Ignore to silently replace the result with NULL.
    ///
    /// `action` identifies the operation (READ, INSERT, DELETE, etc.).
    /// `arg1` and `arg2` provide context (table name, column name, etc.).
    /// `db_name` is the database name ("main", "temp", etc.).
    /// `trigger` is the name of the trigger if called from within one.
    fn authorize(
        &self,
        action: AuthAction,
        arg1: Option<&str>,
        arg2: Option<&str>,
        db_name: Option<&str>,
        trigger: Option<&str>,
    ) -> AuthResult;
}

/// Authorization action codes.
pub enum AuthAction {
    CreateIndex,
    CreateTable,
    CreateTempIndex,
    CreateTempTable,
    CreateTempTrigger,
    CreateTempView,
    CreateTrigger,
    CreateView,
    Delete,
    DropIndex,
    DropTable,
    DropTempIndex,
    DropTempTable,
    DropTempTrigger,
    DropTempView,
    DropTrigger,
    DropView,
    Insert,
    Pragma,
    Read,
    Select,
    Transaction,
    Update,
    Attach,
    Detach,
    AlterTable,
    Reindex,
    Analyze,
    CreateVtable,
    DropVtable,
    Function,
    Savepoint,
    Recursive,
}

pub enum AuthResult {
    Ok,
    Deny,
    Ignore,
}
```

### 9.5 Function Registry

```rust
/// Registry for scalar, aggregate, and window functions.
/// Supports both built-in functions and user-registered functions.
///
/// Functions are looked up by (name, arg_count). If an exact arg_count
/// match is not found, a variadic version (arg_count = -1) is tried.
pub struct FunctionRegistry {
    scalars: HashMap<FunctionKey, Arc<dyn ScalarFunction>>,
    aggregates: HashMap<FunctionKey, Arc<dyn AggregateFunction<State = Box<dyn Any + Send>>>>,
    windows: HashMap<FunctionKey, Arc<dyn WindowFunction<State = Box<dyn Any + Send>>>>,
}

#[derive(Hash, Eq, PartialEq)]
struct FunctionKey {
    name: String,      // case-insensitive (stored as uppercase)
    num_args: i32,     // -1 for variadic
}

impl FunctionRegistry {
    /// Register a scalar function. Overwrites any existing function
    /// with the same name and argument count.
    pub fn register_scalar(&mut self, func: Arc<dyn ScalarFunction>) { ... }

    /// Register an aggregate function.
    pub fn register_aggregate<F: AggregateFunction + 'static>(&mut self, func: F) { ... }

    /// Register a window function.
    pub fn register_window<F: WindowFunction + 'static>(&mut self, func: F) { ... }

    /// Look up a scalar function by name and argument count.
    /// Returns None if not found (caller should raise "no such function" error).
    pub fn find_scalar(&self, name: &str, num_args: i32)
        -> Option<Arc<dyn ScalarFunction>> { ... }

    /// Look up an aggregate function.
    pub fn find_aggregate(&self, name: &str, num_args: i32)
        -> Option<Arc<dyn AggregateFunction<State = Box<dyn Any + Send>>>> { ... }

    /// Look up a window function.
    pub fn find_window(&self, name: &str, num_args: i32)
        -> Option<Arc<dyn WindowFunction<State = Box<dyn Any + Send>>>> { ... }
}
```

### 9.6 Trait Composition: How Layers Connect

**Vfs + VfsFile -> Pager:** The Pager owns a `Box<dyn VfsFile>` for the database
file. It opens the file via `Vfs::open()` during connection setup.

**Pager + Wal -> MvccPager:** The MvccPager wraps both. `get_page()` checks the
version store first, then falls through to Pager (which checks WAL via WalIndex,
then reads from database file).

**MvccPager -> BtCursor:** Cursor calls `pager.get_page()` during traversal.
All page access goes through MVCC version resolution transparently.

**BtCursor -> VdbeCursor -> VDBE:** VDBE opcodes like `OpenRead` create
VdbeCursors wrapping BtCursors. `Column` extracts fields via cursor.

**VDBE + FunctionRegistry -> Execution:** `Function`/`PureFunc` opcodes look
up functions in the registry, call `invoke()`/`step()`/`finalize()`.

### 9.7 Mock Implementations for Testing

Each trait has a mock implementation for unit testing:

- `MockVfs` / `MockVfsFile`: Records all calls, returns configurable responses.
  Used in pager tests to simulate I/O errors.
- `MockMvccPager`: Returns pre-configured page data for given `(pgno, txn_id)`.
  Used in B-tree tests to isolate from MVCC.
- `MockBtreeCursor`: Returns pre-configured rows. Used in VDBE tests.
- `MockScalarFunction`: Returns a fixed value. Used in codegen tests.

For sealed internal traits (e.g., `MvccPager`), mocks MUST live in the defining
crate (the one that defines the private `sealed` supertrait). Other crates use
the exported mock types/values rather than implementing the trait themselves.

---

## 10. Query Pipeline

```
SQL text
  |
  v
Lexer (memchr-accelerated, zero-copy token spans)
  |
  v
Parser (recursive descent, Pratt precedence for expressions)
  |
  v
AST (strongly typed enum hierarchy)
  |
  v
Name Resolution (table/column binding, * expansion)
  |
  v
Query Planning (index selection, cost estimation, join ordering)
  |
  v
VDBE Bytecode Generation (register-based VM, 190+ opcodes)
  |
  v
Execution (fetch-execute loop, match-based dispatch)
  |
  v
Results (iterator of Row, each row is a slice of SqliteValue)
```

### 10.1 Lexer Detail

The lexer converts SQL text into a stream of tokens. Each token carries a
`TokenType` discriminant and a `Span` (byte offset range in source).

**Token type enum (~150 variants):**

```rust
pub enum TokenType {
    // Literals
    Integer,          // 42, -7, 0xFF
    Float,            // 3.14, 1e10, .5
    String,           // 'hello' (single-quoted only at the lexer level)
    Blob,             // X'CAFE', x'00ff'
    Variable,         // ?1, :name, @name, $name

    // Identifiers and keywords
    Id,               // unquoted identifier
    QuotedId,         // "quoted identifier" or [bracketed identifier] or `backtick`
                      // NOTE: "hello" is ALWAYS QuotedId at the lexer level, matching
                      // C SQLite's tokenizer (tokenize.c:413 emits TK_ID for all
                      // double-quoted tokens). The DQS (double-quoted string) legacy
                      // behavior — where an unresolvable "identifier" is reinterpreted
                      // as a string literal — is handled in name resolution (resolve.c),
                      // NOT the lexer. QuotedId tokens carry an EP_DblQuoted-equivalent
                      // flag so the resolver can apply DQS fallback when enabled.

    // Keywords (each is its own variant for fast matching)
    KwAbort, KwAction, KwAdd, KwAfter, KwAll, KwAlter, KwAlways,
    KwAnalyze, KwAnd, KwAs, KwAsc, KwAttach, KwAutoincrement,
    KwBefore, KwBegin, KwBetween, KwBy,
    KwCascade, KwCase, KwCast, KwCheck, KwCollate, KwColumn,
    KwCommit, KwConflict, KwConstraint, KwCreate, KwCross,
    KwCurrentDate, KwCurrentTime, KwCurrentTimestamp, KwConcurrent,
    KwDatabase, KwDefault, KwDeferrable, KwDeferred, KwDelete,
    KwDesc, KwDetach, KwDistinct, KwDo, KwDrop,
    KwEach, KwElse, KwEnd, KwEscape, KwExcept, KwExclusive,
    KwExists, KwExplain,
    KwFail, KwFilter, KwFirst, KwFollowing, KwFor, KwForeign, KwFrom, KwFull,
    KwGenerated, KwGlob, KwGroup, KwGroups,
    KwHaving,
    KwIf, KwIgnore, KwImmediate, KwIn, KwIndex, KwIndexed,
    KwInitially, KwInner, KwInsert, KwInstead, KwIntersect, KwInto, KwIs, KwIsnull,
    KwJoin,
    KwKey,
    KwLast, KwLeft, KwLike, KwLimit,
    KwMatch, KwMaterialized,
    KwNatural, KwNo, KwNot, KwNothing, KwNotnull, KwNull, KwNulls,
    KwOf, KwOffset, KwOn, KwOr, KwOrder, KwOuter, KwOver,
    KwPartition, KwPlan, KwPragma, KwPreceding, KwPrimary,
    KwQuery,
    KwRaise, KwRange, KwRecursive, KwReferences, KwRegexp, KwReindex,
    KwRelease, KwRename, KwReplace, KwRestrict, KwReturning, KwRight,
    KwRollback, KwRow, KwRows, KwRowid,
    KwSavepoint, KwSelect, KwSet, KwStrict,
    KwTable, KwTemp, KwTemporary, KwThen, KwTies, KwTo, KwTransaction, KwTrigger,
    KwUnbounded, KwUnion, KwUnique, KwUpdate, KwUsing,
    KwVacuum, KwValues, KwView, KwVirtual,
    KwWhen, KwWhere, KwWindow, KwWith, KwWithout,

    // Operators and punctuation
    Plus, Minus, Star, Slash, Percent,             // + - * / %
    Ampersand, Pipe, Tilde,                        // & | ~
    ShiftLeft, ShiftRight,                         // << >>
    Eq, Ne, Lt, Le, Gt, Ge,                        // = != < <= > >=
    EqEq, BangEq, LtGt,                           // == != <>
    Dot, Comma, Semicolon,                         // . , ;
    LeftParen, RightParen,                         // ( )
    Arrow, DoubleArrow,                            // -> ->>
    Concat,                                        // ||

    // Special
    Eof,              // end of input
    Error,            // lexer error (unterminated string, invalid character)

    // Whitespace and comments (not emitted to parser; consumed internally)
    // Whitespace, LineComment, BlockComment
}
```

**String/number/blob literal parsing:**

- **String literals:** Delimited by single quotes. Embedded quotes are escaped
  by doubling (`''`). The lexer uses `memchr` to find the closing quote
  efficiently. Scans forward from the opening quote; on finding `'`, checks
  if the next character is also `'` (escaped) or not (end of string).

- **Number literals:** Integer or float. The lexer recognizes:
  - Decimal integers: `[0-9]+`
  - Hex integers: `0x[0-9a-fA-F]+`
  - Floats: `[0-9]*\.[0-9]+([eE][+-]?[0-9]+)?` or `[0-9]+[eE][+-]?[0-9]+`
  - The token type is `Integer` or `Float` based on the presence of `.` or `e/E`.

- **Blob literals:** `X'[0-9a-fA-F]*'` or `x'...'`. Must have even number of
  hex digits. Odd count produces an `Error` token.

**Error tokens:** When the lexer encounters invalid input (unterminated string,
invalid hex in blob literal, unrecognized character), it emits an `Error` token
with a diagnostic message and the offending byte range. The parser can then
produce a user-friendly error with source location.

**Line/column tracking:** The lexer maintains `line: u32` and `col: u32`
counters, incremented on each newline. Every `Token` carries a `Span` with
byte offsets and the `(line, col)` at the token start. This enables error
messages like: `line 3, column 15: expected ')' but found ','`.

### 10.2 Parser Detail

Hand-written recursive descent, NOT a generated parser. Uses C SQLite's
`parse.y` (~1,900+ production lines; the full file including Lemon directives
and semantic actions is ~76 KB) as the authoritative grammar reference for
production rules. Note: C SQLite uses a Lemon LALR(1) generated parser —
the switch to recursive descent is a deliberate FrankenSQLite design choice
for better Rust ergonomics, error recovery, and debuggability.

**Structure:** One method per grammar production. Each method consumes tokens
from the lexer and returns an AST node. Methods are named after the grammar
production they implement.

**Key parsing methods:**

```
parse_statement()              -> Statement
  parse_select_stmt()          -> SelectStatement
    parse_with_clause()        -> WithClause
    parse_select_core()        -> SelectCore
      parse_result_columns()   -> Vec<ResultColumn>
      parse_from_clause()      -> Option<TableRef>
        parse_join_clause()    -> JoinClause
      parse_where_clause()     -> Option<Expr>
      parse_group_by()         -> Option<GroupBy>
      parse_having()           -> Option<Expr>
      parse_window_clause()    -> Vec<WindowDef>
    parse_compound_op()        -> CompoundOp (UNION, INTERSECT, EXCEPT)
    parse_order_by()           -> Vec<OrderingTerm>
    parse_limit()              -> Option<LimitClause>
  parse_insert_stmt()          -> InsertStatement
    parse_upsert_clause()      -> Option<UpsertClause>
    parse_returning()          -> Option<Vec<ResultColumn>>
  parse_update_stmt()          -> UpdateStatement
  parse_delete_stmt()          -> DeleteStatement
  parse_create_table_stmt()    -> CreateTableStatement
    parse_column_def()         -> ColumnDef
    parse_table_constraint()   -> TableConstraint
  parse_create_index_stmt()    -> CreateIndexStatement
  parse_create_view_stmt()     -> CreateViewStatement
  parse_create_trigger_stmt()  -> CreateTriggerStatement
  parse_drop_stmt()            -> DropStatement
  parse_alter_table_stmt()     -> AlterTableStatement
  parse_begin_stmt()           -> BeginStatement
  parse_commit_stmt()          -> CommitStatement
  parse_rollback_stmt()        -> RollbackStatement
  parse_pragma_stmt()          -> PragmaStatement
  parse_explain_stmt()         -> ExplainStatement
  parse_expr()                 -> Expr (Pratt precedence)
    parse_prefix()             -> Expr (unary, literal, paren, subquery, case, cast, ...)
    parse_infix()              -> Expr (binary ops, BETWEEN, IN, LIKE, COLLATE, ...)
```

**Pratt precedence table for expressions:**

| Precedence | Operators | Associativity |
|------------|-----------|---------------|
| 1 (lowest) | OR | Left |
| 2 | AND | Left |
| 3 | NOT (prefix) | Right |
| 4 | =, ==, !=, <>, IS, IS NOT, IN, LIKE, GLOB, BETWEEN, MATCH, REGEXP, ISNULL, NOTNULL, NOT NULL | Left |
| 5 | <, <=, >, >= | Left |
| 6 | ESCAPE | Right |
| 7 | &, \|, <<, >> | Left |
| 8 | +, - | Left |
| 9 | *, /, % | Left |
| 10 | \|\| (concat), ->, ->> (JSON) | Left |
| 11 | COLLATE | Left |
| 12 (highest) | ~ (bitwise not), + (unary), - (unary) | Right |

**NOTE:** Equality/membership operators (level 4) and relational operators
(level 5) are at SEPARATE precedence levels, matching C SQLite's `parse.y`
(`%left IS MATCH LIKE_KW BETWEEN IN ... NE EQ` then `%left GT LE LT GE`).
This means `a = b < c` parses as `a = (b < c)`, NOT `(a = b) < c`.
ESCAPE is `%right` (not `%left`) per `parse.y`.

**Error recovery strategy:** On parse error, the parser:
1. Records the error (token, expected alternatives, source span).
2. Attempts to synchronize by skipping tokens until a "synchronization point"
   is found (semicolon, EOF, or a keyword that starts a new statement).
3. Continues parsing the next statement.
4. Returns all collected errors along with whatever AST was successfully parsed.

This allows the parser to report multiple errors in a single pass rather than
stopping at the first error.

### 10.3 AST Node Types

```rust
/// Top-level statement.
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateView(CreateViewStatement),
    CreateTrigger(CreateTriggerStatement),
    CreateVirtualTable(CreateVirtualTableStatement),
    Drop(DropStatement),
    AlterTable(AlterTableStatement),
    Begin(BeginStatement),
    Commit,
    Rollback(RollbackStatement),
    Savepoint(String),
    Release(String),
    Attach(AttachStatement),
    Detach(String),
    Pragma(PragmaStatement),
    Vacuum(VacuumStatement),
    Reindex(Option<QualifiedName>),
    Analyze(Option<QualifiedName>),
    Explain { query_plan: bool, stmt: Box<Statement> },
}

pub struct SelectStatement {
    pub with: Option<WithClause>,
    pub body: SelectBody,
    pub order_by: Vec<OrderingTerm>,
    pub limit: Option<LimitClause>,
}

pub struct SelectBody {
    pub select: SelectCore,
    pub compounds: Vec<(CompoundOp, SelectCore)>,
}

pub struct SelectCore {
    pub distinct: Distinct,
    pub columns: Vec<ResultColumn>,
    pub from: Option<TableRef>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub windows: Vec<WindowDef>,
}

pub enum Expr {
    Literal(Literal, Span),
    Column(ColumnRef, Span),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr>, span: Span },
    UnaryOp { op: UnaryOp, expr: Box<Expr>, span: Span },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, not: bool, span: Span },
    In { expr: Box<Expr>, set: InSet, not: bool, span: Span },
    Like { expr: Box<Expr>, pattern: Box<Expr>, escape: Option<Box<Expr>>, op: LikeOp, span: Span },
    Case { operand: Option<Box<Expr>>, whens: Vec<(Expr, Expr)>, else_: Option<Box<Expr>>, span: Span },
    Cast { expr: Box<Expr>, type_name: TypeName, span: Span },
    Exists { subquery: Box<SelectStatement>, not: bool, span: Span },
    Subquery(Box<SelectStatement>, Span),
    FunctionCall { name: String, args: Vec<Expr>, distinct: bool, filter: Option<Box<Expr>>, over: Option<WindowSpec>, span: Span },
    Collate { expr: Box<Expr>, collation: String, span: Span },
    IsNull { expr: Box<Expr>, not: bool, span: Span },
    Raise { action: RaiseAction, message: Option<String>, span: Span },
    JsonAccess { expr: Box<Expr>, path: Box<Expr>, arrow: JsonArrow, span: Span },
    RowValue(Vec<Expr>, Span),         // row value: (a, b) for multi-column comparisons (SQLite 3.15+)
    Placeholder(PlaceholderType, Span),
}
```

### 10.4 Name Resolution

Name resolution transforms raw AST identifiers into fully-resolved references.

**Table alias binding:** When a FROM clause contains `table AS alias`, the
resolver creates a binding `alias -> table_schema`. Subsequent column references
can use either the table name or the alias.

**Column reference resolution:** For a reference like `t.col`:
1. Search the current scope's table aliases for `t`.
2. If found, verify `col` exists in that table's schema.
3. If `t` is omitted, search all tables in the FROM clause for a column
   named `col`. If found in exactly one table, resolve. If found in multiple
   tables, report "ambiguous column name" error.

**Star expansion:** `SELECT *` expands to all columns of all tables in the FROM
clause. `SELECT t.*` expands to all columns of table `t`.

**Subquery scoping:** Each subquery creates a new scope. Inner scopes can
reference outer scope columns (correlated subqueries). The resolver tracks
a stack of scopes. A column reference first checks the innermost scope, then
walks outward.

### 10.5 Query Planning

**Cost model:** The planner estimates cost for each access path, primarily
in page reads. When `ANALYZE` statistics are available (`sqlite_stat1`,
`sqlite_stat4`), the planner uses actual row counts and distribution data;
otherwise it falls back to heuristic estimates.

```
Full table scan:              cost = N_pages(table)
Index scan (range):           cost = log2(N_pages(index)) + selectivity * N_pages(index) + selectivity * N_pages(table)
Index scan (equality):        cost = log2(N_pages(index)) + 1
Covering index scan:          cost = log2(N_pages(index)) + selectivity * N_pages(index)
Rowid lookup:                 cost = log2(N_pages(table))
```

Note: These are simplified cost formulas for initial implementation. C SQLite's
cost model is more nuanced, incorporating CPU cost estimates and per-row lookup
cost for non-covering index scans.

**Index usability:** For each WHERE term, the planner determines if an index
can satisfy it:
- Equality (`col = expr`): usable if `col` is the leftmost column of an index.
- Range (`col > expr`, `col BETWEEN`): usable as the rightmost constraint.
- IN (`col IN (...)`): usable, expanded to multiple equality probes.
- LIKE (`col LIKE 'prefix%'`): usable if prefix is constant.

**Join ordering:** For N tables:
- N <= 8: exhaustive search of all N! orderings (at most 40,320).
- N > 8: greedy algorithm -- at each step, add the table that has the lowest
  estimated join cost with the already-joined set.
(Note: C SQLite's NGQP uses a bounded best-first search that maintains the
`mxChoice` best partial paths at each step — it is O(mxChoice*N), not
exhaustive. `mxChoice` varies by join complexity (1 for simple, up to
`SQLITE_DEFAULT_MXCHOICE=18` for multi-table). The term "N Nearest Neighbors"
is not used in the SQLite source; the algorithm is implemented in
`wherePathSolver()` in `where.c`. The simpler exhaustive/greedy split here is
FrankenSQLite's initial strategy.)

### 10.6 Code Generation

The opcode traces below are **illustrative only**: the exact sequences vary by
schema, indexes, triggers, and optimizer choices. They exist to convey shape,
not to be byte-for-byte identical to C SQLite.

**SELECT -> VDBE opcodes:**
```
SELECT col FROM table WHERE rowid = ?
  Init       0, <end>
  Transaction 0, 0           # begin read transaction
  Variable   1, 1            # load bind parameter ?1 into r1
  OpenRead   0, <root>, 0    # open cursor 0 on table
  SeekRowid  0, <notfound>, 1  # seek to rowid in r1
  Column     0, <col_idx>, 2   # extract column into r2
  ResultRow  2, 1              # emit r2 as result
  <notfound>:
  Close      0
  Halt       0, 0
  <end>:
```

**INSERT -> VDBE opcodes:**
```
INSERT INTO table VALUES (?, ?)
  Init       0, <end>
  Transaction 0, 1           # begin write transaction
  OpenWrite  0, <root>, 0    # open cursor 0 for writing
  NewRowid   0, 1            # generate new rowid into r1
  Variable   1, 2            # bind param 1 -> r2
  Variable   2, 3            # bind param 2 -> r3
  MakeRecord 2, 2, 4         # pack r2..r3 into record r4
  Insert     0, 4, 1         # insert record r4 with rowid r1
  Close      0
  Halt       0, 0
  <end>:
```

**UPDATE -> VDBE opcodes:**
```
-- Schema: CREATE TABLE t(a, b)
UPDATE t SET b = ? WHERE rowid = ?
  Init       0, <end>
  Transaction 0, 1           # begin write transaction
  Variable   1, 1            # bind new value for b -> r1
  Variable   2, 2            # bind rowid -> r2
  OpenWrite  0, <root>, 2    # open cursor 0 for writing (2 columns)
  NotExists  0, <done>, 2    # if rowid r2 not found, skip
  Column     0, 0, 3         # read existing col a into r3
  Copy       1, 4            # new col b value (from r1) into r4
  MakeRecord 3, 2, 5         # pack r3..r4 (ALL columns) into record r5
  Insert     0, 5, 2, REPLACE  # overwrite record at rowid r2 with r5
  <done>:
  Close      0
  Halt       0, 0
  <end>:
```

**DELETE -> VDBE opcodes:**
```
DELETE FROM table WHERE rowid = ?
  Init       0, <end>
  Transaction 0, 1           # begin write transaction
  Variable   1, 1            # bind rowid -> r1
  OpenWrite  0, <root>, 0    # open cursor 0 for writing
  NotExists  0, <done>, 1    # if rowid r1 not found, skip
  Delete     0, 0            # delete row at current cursor position
  <done>:
  Close      0
  Halt       0, 0
  <end>:
```

### 10.7 VDBE Instruction Format

```rust
pub struct VdbeOp {
    pub opcode: Opcode,    // u8, one of 190+ opcodes
    pub p1: i32,           // first operand (register, cursor, or literal)
    pub p2: i32,           // second operand (jump target, register, etc.)
    pub p3: i32,           // third operand
    pub p4: P4,            // extended operand
    pub p5: u16,           // flags. C SQLite declares this as u16; most
                           // opcode flag masks fit in the low 8 bits, but
                           // some newer opcodes may use the full 16 bits.
                           // Match C SQLite's per-opcode P5 usage exactly.
}

pub enum P4 {
    None,
    Int32(i32),
    Int64(i64),
    Real(f64),
    String(String),
    Blob(Vec<u8>),
    FuncDef(Arc<dyn ScalarFunction>),
    CollSeq(Arc<dyn CollationFunction>),
    KeyInfo(KeyInfo),        // column sort orders for index comparison
    Mem(Mem),                // pre-loaded register value
    Vtab(Arc<dyn VirtualTable>),
    Table(TableInfo),        // table metadata for Insert/Update
    Subprogram(VdbeProgram), // trigger sub-program
}
```

**Jump resolution:** During code generation, forward jumps target unknown
addresses. The codegen uses a label system: `emit_label()` returns a `Label`
handle, and `resolve_label(label, address)` patches all instructions that
reference that label. All labels must be resolved before execution begins.

**Register allocation:** Registers are numbered starting at 1. The codegen
allocates registers sequentially via `alloc_reg()` and `alloc_regs(n)`.
Temporary registers (used within a single opcode sequence) are allocated from
a pool and returned after use. Persistent registers (for result columns,
cursor positions) are allocated once and held for the statement's lifetime.

### 10.8 Coroutines

Subqueries and CTEs use the VDBE coroutine mechanism:

```
// InitCoroutine P1=r_yield, P2=<done>, P3=<cte_body>
// Sets r_yield = &cte_body, then falls through (P2=0) or jumps to P2.
// Typical layout (P2=0, fall-through to outer query):

InitCoroutine  r_yield, 0, <cte_body>
  // ... outer query: Yield r_yield pulls next row from CTE body
  //     (Yield swaps PCs: saves current PC into r_yield, jumps to old r_yield)
  Goto <done>
<cte_body>:
  // ... CTE body: produces rows, each ending with Yield r_yield
  //     (Yield swaps PCs back to outer query)
  EndCoroutine r_yield    // final return to outer query; marks exhaustion
<done>:
```

The `Yield` opcode swaps program counters between the outer query and the
coroutine: it saves the current PC into `r_yield` and jumps to the previously
saved PC. `EndCoroutine` performs one final swap back to the caller. This
allows the CTE to produce rows on-demand without materializing the entire
result set into a temporary table. (Note: the exact layout varies by
compilation phase; WITH RECURSIVE and subquery flattening may use different
P2 targets.)

---

## 11. File Format Compatibility

FrankenSQLite reads and writes standard SQLite database files. This section
specifies every format detail needed for byte-level compatibility.

### 11.1 Database Header (100 bytes at offset 0)

Every field with exact byte offset, valid values, and what FrankenSQLite sets:

```
Offset  Size  Field                    Valid Values              FrankenSQLite Default
------  ----  -----                    ------------              ---------------------
  0      16   Magic string             "SQLite format 3\000"     Same (required)
 16       2   Page size                512,1024,2048,4096,       4096
                                       8192,16384,32768,
                                       1 (means 65536)
 18       1   Write version            1=journal, 2=WAL          2 (WAL mode default)
 19       1   Read version             1=journal, 2=WAL          2
 20       1   Reserved space/page      0..255                    0 (or 16 if page_checksum=ON)
                                      (constraint: usable_size = page_size -
                                       reserved_space must be >= 480)
 21       1   Max embed payload frac   64 (MUST be 64)           64
 22       1   Min embed payload frac   32 (MUST be 32)           32
 23       1   Leaf payload fraction    32 (MUST be 32)           32
 24       4   File change counter      any u32                   Incremented when the database header
                                      (offset 24)                is written (rollback-journal commit;
                                                                  checkpoint writing page 1). In WAL mode,
                                                                  this is NOT forced on every commit.
 28       4   Database size (pages)    0 or actual count         Actual count
                                      (only valid when offset 92 == offset 24;
                                       otherwise compute from file size)
 32       4   First freelist trunk     0 or page number          0 (empty freelist initially)
 36       4   Total freelist pages     count                     0
 40       4   Schema cookie            any u32                   Incremented on schema change
 44       4   Schema format number     1,2,3,4                   4 (current)
 48       4   Suggested cache size     PRAGMA default_cache_size  0 (use runtime default)
 52       4   Largest root b-tree page 0 or page# (auto-vacuum)  0
 56       4   Text encoding            1=UTF8, 2=UTF16le,        1 (UTF-8)
                                       3=UTF16be
 60       4   User version             any u32                   0
 64       4   Incremental vacuum       0 or non-zero             0
 68       4   Application ID           any u32                   0
 72      20   Reserved                 all zeros                 All zeros
 92       4   Version-valid-for        change counter value       Updated alongside offset 24; when equal,
                                                                  header fields like "db size pages" are valid
 96       4   SQLite version number    X*1000000+Y*1000+Z        3052000 (3.52.0)
```

**Page size encoding:** The value 1 at offset 16-17 encodes a page size of
65536 (since 65536 does not fit in a u16). All other values are the literal
page size. Must be a power of 2 in the range [512, 65536].

**FrankenSQLite version number:** At offset 96, FrankenSQLite writes 3052000
(representing 3.52.0) to indicate compatibility with SQLite 3.52.0.

**Write/read version forward compatibility (offsets 18-19):** When opening a
database, if the read version (offset 19) exceeds the maximum version the
library understands (currently 2 = WAL), the database MUST be refused with
`SQLITE_CANTOPEN`. If only the write version (offset 18) exceeds the maximum,
the database MUST be opened read-only. This mechanism allows future SQLite
format extensions (e.g., WAL2) to prevent older libraries from corrupting
databases they cannot fully understand.

### 11.2 B-Tree Page Layout

**Page structure (top to bottom within a page):**

```
[Page header: 8 or 12 bytes]
[Cell pointer array: 2 * num_cells bytes]
[Unallocated space: variable]
[Cell content area: grows backward from end of page]
[Reserved space: reserved_per_page bytes at very end]
```

**Page header field layout:**

```
Offset  Size  Field
  0       1   Page type: 0x02 (index interior), 0x05 (table interior),
              0x0A (index leaf), 0x0D (table leaf)
  1       2   First freeblock offset (big-endian u16; 0 if no freeblocks)
  3       2   Number of cells on this page (big-endian u16)
  5       2   Cell content area start offset (big-endian u16; 0 means 65536)
  7       1   Fragmented free bytes count
  8       4   Right-most child pointer (INTERIOR PAGES ONLY; absent on leaf)
```

Interior pages (0x02, 0x05) have a 12-byte header; leaf pages (0x0A, 0x0D)
have an 8-byte header. The extra 4 bytes on interior pages hold the
right-most child page number.

**Page 1 special case:** Page 1 has the 100-byte database header before the
B-tree page header. Cell pointer offsets on page 1 account for this prefix.
The usable start of page 1 is at byte 100.

**Cell pointer array:** Immediately after the page header. Each entry is a
2-byte big-endian u16 offset pointing to the start of a cell within the page.
The offsets are relative to the start of the page. Cells are stored from the
end of the page growing backward.

**Unallocated space:** Between the end of the cell pointer array and the start
of the cell content area. This is contiguous free space available for new cells.

**Freeblock list:** Within the cell content area, deleted cells form a linked
list of freeblocks. Each freeblock starts with a 2-byte pointer to the next
freeblock (0 if last) and a 2-byte size. Minimum freeblock size is 4 bytes.

**Fragmented bytes:** The page header byte at offset 7 counts bytes of space
lost to fragmentation -- individual 1-3 byte gaps between cells or at the
end of freeblocks that are too small to form their own freeblock entry (the
minimum freeblock size is 4 bytes, so gaps of 1-3 bytes cannot be tracked).
The total number of bytes in fragments may not exceed 60 in a well-formed
page. If an insertion would cause the count to exceed 60, the page is
defragmented first (cells are compacted toward the end of the page).

### 11.2.1 Varint Encoding

SQLite uses a specific variable-length integer encoding throughout cell
formats and record headers. This is NOT protobuf varint, NOT LEB128. The
encoding is a custom Huffman-like scheme with a maximum length of 9 bytes:

```
Bytes  Value range                    Encoding
  1    0 to 127                       0xxxxxxx (high bit clear)
  2    128 to 16383                   1xxxxxxx 0xxxxxxx
  3    16384 to 2097151               1xxxxxxx 1xxxxxxx 0xxxxxxx
  ...  (pattern continues)
  8    up to 2^56 - 1                 1xxxxxxx * 7 then 0xxxxxxx
  9    up to 2^64 - 1                 1xxxxxxx * 8 then xxxxxxxx (full byte)
```

**Decoding algorithm:**
- For the first 8 bytes: if the high bit is set, the lower 7 bits contribute
  to the result and the next byte is read. If the high bit is clear, the
  lower 7 bits are the final contribution.
- The 9th byte (if reached) contributes all 8 bits (no continuation bit).
- Maximum encoded value: a full u64 (2^64 - 1).
- The result is an unsigned 64-bit integer. For signed values (e.g., rowid),
  it is cast to i64 (two's complement).

**Encoding algorithm:** Encode the least number of bytes needed. Values 0-127
use 1 byte; values 128-16383 use 2 bytes; and so on up to 9 bytes for values
>= 2^56.

**Critical difference from protobuf/LEB128:** In protobuf varints, each byte
contributes 7 bits with the high bit as continuation, for ALL bytes. In
SQLite varints, the 9th byte contributes ALL 8 bits. This means SQLite
varints can encode a full 64-bit value in exactly 9 bytes, whereas protobuf
would need 10 bytes.

### 11.3 Cell Formats

**Table leaf cell (page type 0x0D):**
```
[payload_size: varint]    -- total bytes of payload
[rowid: varint]           -- integer primary key
[payload: bytes]          -- first local_bytes bytes (see §11.4 overflow calc)
[overflow_pgno: u32BE]    -- only if payload overflows
```

**Table interior cell (page type 0x05):**
```
[left_child: u32BE]       -- 4-byte page number of left child
[rowid: varint]           -- divider key (integer)
```

**Index leaf cell (page type 0x0A):**
```
[payload_size: varint]    -- total bytes of payload
[payload: bytes]          -- first local_bytes bytes (see §11.4 overflow calc)
[overflow_pgno: u32BE]    -- only if payload overflows
```

**Index interior cell (page type 0x02):**
```
[left_child: u32BE]       -- 4-byte page number of left child
[payload_size: varint]    -- total bytes of payload
[payload: bytes]          -- first local_bytes bytes (see §11.4 overflow calc)
[overflow_pgno: u32BE]    -- only if payload overflows
```

### 11.4 Overflow Pages

**When overflow occurs:**

```
usable = page_size - reserved_per_page

Table leaf:
  max_local = usable - 35
  min_local = (usable - 12) * 32 / 255 - 23

Index (leaf and interior):
  max_local = (usable - 12) * 64 / 255 - 23
  min_local = (usable - 12) * 32 / 255 - 23

if payload_size <= max_local: all local, no overflow
else:
  local = min_local + (payload_size - min_local) % (usable - 4)
  if local > max_local: local = min_local
  overflow_bytes = payload_size - local
```

For 4096-byte page, 0 reserved: table leaf max_local = 4061, index max_local = 1002.
(Index: `(usable - 12) * 64 / 255 - 23` = `4084 * 64 / 255 - 23` = `1025 - 23` = 1002,
using integer division: `4084 * 64 = 261376`, `261376 / 255 = 1025` (truncated; remainder 1).)

**Overflow page format:**
```
Offset  Size          Description
  0       4           Next overflow page number (0 if last)
  4       usable-4    Payload data
```

### 11.5 Freelist

**Trunk page format:**
```
Offset  Size    Description
  0       4     Next trunk page number (0 if last)
  4       4     Number of leaf page numbers (K)
  8       4*K   Array of leaf page numbers
```

Max leaves per trunk = (usable - 8) / 4 = 1022 for 4096-byte pages.

Header offset 32 = first trunk page; offset 36 = total freelist page count.

### 11.6 Pointer Map (Auto-Vacuum)

**Entry format (5 bytes per page):**
```
Byte 0:     Type code:
              1 = PTRMAP_ROOTPAGE  (root page of a B-tree; parent = 0)
              2 = PTRMAP_FREEPAGE  (page on freelist; parent = 0)
              3 = PTRMAP_OVERFLOW1 (first overflow page; parent = B-tree page holding the cell)
              4 = PTRMAP_OVERFLOW2 (subsequent overflow page; parent = preceding overflow page)
              5 = PTRMAP_BTREE     (non-root B-tree page; parent = B-tree parent page)
Bytes 1-4:  Parent page number (u32 BE). Meaning varies by type (see above).
```

**Location:** First pointer map page is always page 2.
entries_per_page = usable / 5. Group size = entries_per_page + 1.
Pointer map pages at: 2, 2+group_size, 2+2*group_size, ...

For 4096 pages: 819 entries/page, group size 820, pages at 2, 822, 1642, ...

### 11.7 Record Format Detail

**Structure:** `[header_size: varint] [serial_types: varint...] [data: bytes...]`

The header_size varint includes itself. Serial types encode both type and size.

**Serial types:**

| Value | Type | Content Bytes |
|-------|------|---------------|
| 0 | NULL | 0 |
| 1 | 8-bit signed int | 1 |
| 2 | 16-bit big-endian signed int | 2 |
| 3 | 24-bit big-endian signed int | 3 |
| 4 | 32-bit big-endian signed int | 4 |
| 5 | 48-bit big-endian signed int | 6 |
| 6 | 64-bit big-endian signed int | 8 |
| 7 | IEEE 754 64-bit float (BE) | 8 |
| 8 | Integer constant 0 | 0 |
| 9 | Integer constant 1 | 0 |
| 10,11 | Reserved (internal use) | - |
| N >= 12, even | BLOB of (N-12)/2 bytes | (N-12)/2 |
| N >= 13, odd | TEXT of (N-13)/2 bytes | (N-13)/2 |

**Worked example:** Row `(42, "hello", 3.14, NULL, X'CAFE')`:

Serial types: 1 (42 fits i8), 23 (5*2+13), 7 (float), 0 (NULL), 16 (2*2+12).
Header: [06, 01, 17, 07, 00, 10] (6 bytes total including size varint).
Data: [2A] [68 65 6C 6C 6F] [40 09 1E B8 51 EB 85 1F] [] [CA FE].
Total: 22 bytes.

### 11.8 WAL Header (32 bytes)

```
Offset  Size  Description
  0       4   Magic: 0x377F0682 (bigEndCksum=0, LE machine) or
              0x377F0683 (bigEndCksum=1, BE machine). See §7.1.
  4       4   Format version: 3007000 (constant for all WAL1 databases;
              indicates the WAL format introduced in SQLite 3.7.0)
  8       4   Page size
 12       4   Checkpoint sequence number
 16       4   Salt-1
 20       4   Salt-2
 24       4   Checksum-1 (of bytes 0..23)
 28       4   Checksum-2 (of bytes 0..23)
```

### 11.9 WAL Frame Header (24 bytes)

```
Offset  Size  Description
  0       4   Page number
  4       4   For commit frames: db size in pages. Otherwise 0.
  8       4   Salt-1 (must match WAL header)
 12       4   Salt-2 (must match WAL header)
 16       4   Cumulative checksum-1
 20       4   Cumulative checksum-2
```

### 11.9.1 WAL Checksum Algorithm

The WAL uses a custom double-accumulator checksum (NOT CRC-32, NOT xxHash).
The canonical implementation is in **§7.1** (`wal_checksum()`). This section
specifies the checksum chain — how that function is applied to the WAL header
and frame sequence.

**Checksum chain:**
1. **WAL header checksum:** `wal_checksum(header_bytes[0..24], 0, 0, big_end_cksum)` →
   stored at header bytes 24..32.
2. **First frame:** `wal_checksum(frame_header[0..8] ++ page_data, hdr_cksum1, hdr_cksum2, big_end_cksum)`
   → stored at frame header bytes 16..24. (Note: only the first 8 bytes of
   the frame header are checksummed, NOT bytes 8..16 which contain the salt.)
3. **Subsequent frames:** use the previous frame's `(cksum1, cksum2)` as the seed.
   Each frame's checksum covers itself AND all prior frames (cumulative).

**Validation:** During recovery, walk frames sequentially. A frame is valid iff
its recomputed checksum matches the stored values AND its salt matches the WAL
header salt. The first frame that fails either check terminates the valid
prefix of the WAL.

### 11.10 WAL Index (wal-index / SHM)

**Byte order:** Unlike the main database file and WAL file (big-endian),
all WAL-index (SHM) header fields are stored in **native byte order** of the
host machine (except salt values copied verbatim from the WAL header). This
is because the SHM is not involved in crash recovery and does not need to be
portable across architectures.

```
Header (136 bytes):
  [0..48]:   WalIndexHdr (first copy):
               iVersion(u32) = 3007000 (MUST match),
               unused(u32), iChange(u32), isInit(u8),
               bigEndCksum(u8), szPage(u16), mxFrame(u32), nPage(u32),
               aFrameCksum[2](u32), aSalt[2](u32), aCksum[2](u32)
  [48..96]:  WalIndexHdr (second copy -- lock-free reads: reader reads
               both copies, uses them only if they match)
  [96..136]: WalCkptInfo (40 bytes total):
               nBackfill(u32) at offset 96
               aReadMark[5](u32) at offsets 100-119 (5 reader marks, 20 bytes)
               aLock[8](u8) at offsets 120-127 (8 SHM lock slots, 1 byte each)
               nBackfillAttempted(u32) at offset 128
               notUsed0(u32) at offset 132

Hash table segments (32 KB each):
  Physical layout: page-number array (u32[4096]) at bytes [0..16384) and
  hash table (ht_slot[8192], u16) at bytes [16384..32768) in ALL segments.
  In the first segment, the 136-byte header overlaps the first 34 u32
  page-number slots, leaving 4062 usable entries (wal.c compile-time assert).

  First segment:  covers up to 4062 frames.
                  [0..136):       Header (overlaps first 34 page-number slots)
                  [136..16384):   Page number array: 4062 entries x 4 bytes
                  [16384..32768): Hash table: 8192 slots x 2 bytes
  Subsequent:     covers up to 4096 frames (full 32 KB region).
                  [0..16384):     Page number array: 4096 entries x 4 bytes
                  [16384..32768): Hash table: 8192 slots x 2 bytes
  Hash function: (page_number * 383) & 8191, linear probing.
  -- NOT simple modulo. The prime multiplier 383 (HASHTABLE_HASH_1 in C
  -- SQLite) provides much better distribution for sequential page numbers.
  -- Using `page_number % 8192` would produce a working but incompatible
  -- wal-index when sharing SHM files with C SQLite in multi-process mode.
```

**Reader marks:** Byte offsets 100-119 contain 5 reader marks (u32 each, 20 bytes total).
Each reader mark records the WAL frame count at the time a reader began.
This prevents checkpoint from overwriting frames still needed by active readers.

**WAL-index lock slot mapping (required for Hybrid SHM interop):**
- `aLock[0]` (byte 120) = `WAL_WRITE_LOCK` (exclusive; writer exclusion)
- `aLock[1]` (byte 121) = `WAL_CKPT_LOCK`
- `aLock[2]` (byte 122) = `WAL_RECOVER_LOCK`
- `aLock[3 + i]` (bytes 123..127) = `WAL_READ_LOCK(i)` for `i in 0..4`

These bytes are lockable file regions. Their *values* are not used as a
coordination protocol; correctness depends on the OS-level locks taken on these
byte offsets.

### 11.11 sqlite_master Table

Every database contains a `sqlite_master` table (page 1 root) with this schema:

```sql
CREATE TABLE sqlite_master (
    type TEXT,      -- 'table', 'index', 'view', 'trigger'
    name TEXT,      -- object name
    tbl_name TEXT,  -- associated table name (for indexes/triggers: the table)
    rootpage INT,   -- root B-tree page number (0 for views/triggers)
    sql TEXT        -- CREATE statement text (NULL for auto-indexes)
);
```

For the temp database, the equivalent is `sqlite_temp_master`.

On database creation, FrankenSQLite creates page 1 as a table leaf page
containing zero rows in sqlite_master. The first `CREATE TABLE` inserts a
row into sqlite_master with the CREATE statement text.

### 11.12 Encoding

**Default:** UTF-8 (text encoding = 1 at header offset 56).

**UTF-16 alternatives:** UTF-16le (2) and UTF-16be (3) are supported. The
encoding is set at database creation and cannot be changed afterward. When
UTF-16 is used, all text stored in the database is UTF-16 encoded, and text
comparisons use UTF-16 collation.

**How encoding affects comparison:** The BINARY collation uses `memcmp` on
the raw bytes. For UTF-8, this produces correct Unicode code point ordering.
For UTF-16, byte-order matters (LE vs BE). NOCASE collation always operates
on Unicode code points regardless of encoding.

### 11.13 Page Size Constraints

- Minimum: 512 bytes
- Maximum: 65536 bytes
- Must be a power of 2
- The value 1 at header offset 16-17 encodes 65536 (since 65536 > u16::MAX)
- Page size is set at database creation and cannot be changed except by
  `PRAGMA page_size = N; VACUUM;` (only when NOT in WAL mode) or `VACUUM INTO`
- FrankenSQLite default: 4096 (matches modern filesystem block size and SSD page size)

### 11.13.1 Lock-Byte Page (Pending Byte)

For databases larger than 1 GiB, the page containing byte offset
0x40000000 (1,073,741,824 — the POSIX advisory "pending byte") is reserved
for file locking and MUST NOT store B-tree content. For 4096-byte pages this
is page `(0x40000000 / 4096) + 1 = 262145`. SQLite skips this page during
allocation (`allocateBtreePage()` in btree.c).

FrankenSQLite MUST replicate this behavior:
- Never allocate this page for B-tree storage or freelist use.
- On `PRAGMA integrity_check`, verify this page is not referenced by any
  B-tree pointer.
- The exact page number depends on page size: `(0x40000000 / page_size) + 1`.

This is critical for multi-process locking compatibility: if a B-tree page
occupies the lock-byte region, concurrent readers using POSIX `fcntl()` locks
will corrupt it.

### 11.14 Rollback Journal Format

FrankenSQLite must support rollback journal mode for reading databases not
in WAL mode. The rollback journal file (`<database>-journal`) format:

```
Journal Header (padded to sector boundary):
  Offset  Size  Description
    0       8   Magic: {0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7}
    8       4   Page count (-1 means compute from file size)
   12       4   Random nonce for checksum
   16       4   Initial database size in pages (before this transaction)
   20       4   Sector size (header padded to this boundary)
   24       4   Page size

Journal Page Records (repeated page_count times):
  [4 bytes: page number (u32 BE)]
  [page_size bytes: original page content before modification]
  [4 bytes: checksum]
```

**Checksum:** `nonce + data[page_size-200] + data[page_size-400] + ... + data[k]`
where k is the smallest value `> 0` (strictly positive) in the arithmetic
sequence. The loop condition is `while( i > 0 )`, so `data[0]` is **never**
sampled (pager.c `pager_cksum()`). Each `data[i]` reads a single `u8` byte,
accumulated into a `u32` sum. For 4096-byte pages: 19 bytes summed (offsets
3896, 3696, ..., 296, 96).

**Hot journal recovery:** On open, if a journal file exists, is non-empty,
and the database's reserved lock is not held, it is a "hot journal." Recovery
plays back original pages from the journal, then deletes it.

**Journal modes:** DELETE (default), TRUNCATE, PERSIST, MEMORY, WAL, OFF.
`PRAGMA journal_mode` switches modes. WAL-to-rollback: checkpoint all WAL
frames, delete WAL and SHM files, update header bytes 18-19 from 2 to 1.

## 12. SQL Coverage

FrankenSQLite implements the full SQLite 3.52.0 SQL dialect. This section
specifies every supported syntactic form with semantic details sufficient
to drive parser, planner, and VDBE codegen implementation.

### 12.1 SELECT

The SELECT statement is the most complex production in the SQLite grammar.
The full syntax tree is:

```sql
SELECT [DISTINCT | ALL] result-column [, result-column]*
  FROM table-or-subquery [join-clause]*
  [WHERE expr]
  [GROUP BY expr [, expr]* [HAVING expr]]
  [WINDOW window-defn [, window-defn]*]
  [ORDER BY ordering-term [, ordering-term]*]
  [LIMIT expr [OFFSET expr | , expr]]
```

**result-column** forms:
- `*` -- all columns from all tables in FROM
- `table-name.*` -- all columns from a specific table
- `expr [AS alias]` -- computed expression with optional alias

**FROM clause** table sources:
- Table name: `FROM t1`
- Table alias: `FROM t1 AS a`
- Indexed hint: `FROM t1 INDEXED BY idx_name` or `FROM t1 NOT INDEXED`
- Subquery: `FROM (SELECT ...) AS sub`
- Table-valued function: `FROM json_each(col)` or `FROM generate_series(1,100)`
- Multiple tables (implicit CROSS JOIN): `FROM t1, t2`

**JOIN types** (all produce VDBE nested-loop opcodes; Bloom filter opcodes
`OP_FilterAdd`/`OP_Filter` may additionally be emitted for early rejection,
but SQLite has no hash join):
- `INNER JOIN ... ON expr` / `JOIN ... ON expr`
- `LEFT [OUTER] JOIN ... ON expr`
- `RIGHT [OUTER] JOIN ... ON expr` (SQLite 3.39+)
- `FULL [OUTER] JOIN ... ON expr` (SQLite 3.39+)
- `CROSS JOIN` (optimizer will not reorder)
- `NATURAL JOIN` (implicit ON using shared column names)
- `... USING (col1, col2)` (explicit shared columns)

**Compound SELECT operators** (vertically combine result sets):
- `UNION` -- deduplicate
- `UNION ALL` -- keep duplicates
- `INTERSECT` -- rows present in both
- `EXCEPT` -- rows in left but not right

Compound operators bind left-to-right. ORDER BY and LIMIT apply to the
entire compound result, not individual SELECT arms. Column names come from
the first (leftmost) SELECT.

**Common Table Expressions (CTEs):**
```sql
WITH [RECURSIVE]
  cte_name [(col1, col2, ...)] AS [NOT MATERIALIZED | MATERIALIZED] (
    select-stmt
  ) [, ...]
SELECT ... FROM cte_name ...
```

Recursive CTEs use `UNION ALL` (keeps duplicates) or `UNION` (discards
duplicates, providing implicit cycle detection) between the base case and
the recursive step. The recursive step may reference `cte_name` exactly once.
When using `UNION ALL`, cycle detection is not automatic; use `LIMIT` to
prevent infinite recursion.
`MATERIALIZED` forces the CTE to be evaluated once and stored as a temp
table. `NOT MATERIALIZED` allows the optimizer to inline the CTE as a
subquery (default behavior for non-recursive CTEs referenced once).

**Window functions:**
```sql
SELECT func(args) OVER (
  [PARTITION BY expr [, expr]*]
  [ORDER BY ordering-term [, ordering-term]*]
  [frame-spec]
)

frame-spec :=
  { RANGE | ROWS | GROUPS }
  { BETWEEN frame-bound AND frame-bound | frame-bound }

frame-bound :=
  UNBOUNDED PRECEDING
  | expr PRECEDING
  | CURRENT ROW
  | expr FOLLOWING
  | UNBOUNDED FOLLOWING

EXCLUDE := EXCLUDE { NO OTHERS | CURRENT ROW | GROUP | TIES }
```

Default frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` when
ORDER BY is present; `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING` when ORDER BY is absent.

**FILTER clause** (SQLite 3.30+): Both aggregate and window function calls
support an optional `FILTER (WHERE expr)` clause that restricts which rows
are fed to the function:
```sql
SELECT count(*) FILTER (WHERE status = 'active') FROM users;
SELECT sum(amount) FILTER (WHERE type = 'credit') OVER (ORDER BY date) FROM txns;
```
The FILTER clause is semantically equivalent to wrapping the argument in a
CASE expression: `sum(CASE WHEN type='credit' THEN amount END)`, but is more
readable and is required for SQL standard conformance.

**NULLS FIRST / NULLS LAST** (SQLite 3.30+): The `ordering-term` syntax is:
```sql
ordering-term := expr [COLLATE collation-name] [ASC | DESC] [NULLS {FIRST | LAST}]
```
Default: `NULLS FIRST` for ASC, `NULLS LAST` for DESC (SQLite's historical
behavior; NULLs sort as smaller than any other value). Specifying `NULLS LAST`
with ASC or `NULLS FIRST` with DESC overrides this default.

**Date/time keyword constants:** `current_time`, `current_date`, and
`current_timestamp` are special keyword tokens that parse as zero-argument
built-in functions and return the current time as text strings in UTC:
- `current_time` -> `'HH:MM:SS'`
- `current_date` -> `'YYYY-MM-DD'`
- `current_timestamp` -> `'YYYY-MM-DD HH:MM:SS'`

These are evaluated once per statement (not per row) via `sqlite3StmtCurrentTime()`
(which calls `sqlite3OsCurrentTimeInt64()` in the VFS).

**DISTINCT processing:** Implemented via a temporary B-tree index for
deduplication. The VDBE uses `OP_Found` / `OP_NotFound` on the temp
index to check for duplicates before emitting rows.

**LIMIT and OFFSET:** LIMIT takes a non-negative integer expression. OFFSET
takes a non-negative integer expression. The alternative form
`LIMIT offset, count` (offset as **first** argument, count as second —
following MySQL convention) is supported for backward compatibility.
SQLite's documentation calls this ordering "counter-intuitive" and
recommends the explicit `OFFSET` keyword form instead. Negative LIMIT means unlimited. Negative OFFSET
is treated as zero.

### 12.2 INSERT

```sql
INSERT [OR conflict-clause] INTO table-name [(col-list)]
  { VALUES (expr, ...) [, (expr, ...)]* | select-stmt | DEFAULT VALUES }
  [upsert-clause]
  [RETURNING result-column [, result-column]*]
```

**Conflict resolution clauses** (OR keyword forms):
- `INSERT OR ABORT` -- default, abort current statement on conflict
- `INSERT OR ROLLBACK` -- rollback entire transaction on conflict
- `INSERT OR FAIL` -- abort statement but keep prior changes from same statement
- `INSERT OR IGNORE` -- silently skip conflicting row
- `INSERT OR REPLACE` -- delete existing conflicting row, then insert new

**UPSERT (ON CONFLICT):**
```sql
INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)
  ON CONFLICT (a) DO UPDATE SET b = excluded.b, c = excluded.c
  WHERE excluded.c > t1.c;

INSERT INTO t1 (a, b) VALUES (1, 2)
  ON CONFLICT (a) DO NOTHING;

-- Multiple ON CONFLICT clauses (SQLite 3.35+):
INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)
  ON CONFLICT (a) DO UPDATE SET b = excluded.b
  ON CONFLICT (b) DO NOTHING;
```

The `excluded` pseudo-table refers to the row that would have been inserted.
The conflict target `(column-list)` must match a UNIQUE index or PRIMARY KEY.
An optional WHERE clause on the conflict target restricts which index to match.
The DO UPDATE SET clause can reference both `excluded.*` and the original
table columns.

**RETURNING clause** (SQLite 3.35+): Returns the rows actually inserted,
including any default values and autoincrement values. The returned values
reflect BEFORE-trigger modifications (since those run before the DML) but
do NOT reflect AFTER-trigger modifications. Each returned row has columns
matching the result-column list.

**Multi-row VALUES:** `VALUES (1,'a'), (2,'b'), (3,'c')` inserts three
rows atomically within the same statement. The VDBE generates a loop over
the value lists.

**INSERT from SELECT:** `INSERT INTO t1 SELECT * FROM t2 WHERE ...`
streams rows from the SELECT result directly into the B-tree insert path.

**DEFAULT VALUES:** `INSERT INTO t1 DEFAULT VALUES` inserts a single row
using the DEFAULT expression for every column (NULL if no DEFAULT defined).

### 12.3 UPDATE

```sql
UPDATE [OR conflict-clause] table-name
  SET col = expr [, col = expr]*
  [FROM table-or-subquery [, table-or-subquery]*]
  [WHERE expr]
  [ORDER BY ordering-term [, ordering-term]*]
  [LIMIT expr [OFFSET expr]]
  [RETURNING result-column [, result-column]*]
```

**UPDATE FROM** (SQLite 3.33+): The FROM clause provides additional tables
for the SET expressions and WHERE clause, enabling UPDATE-with-JOIN:
```sql
UPDATE inventory SET quantity = inventory.quantity - orders.qty
  FROM orders
  WHERE inventory.product_id = orders.product_id
    AND orders.status = 'pending';
```
When the FROM clause is present and a row in the target table joins with
multiple rows from the FROM tables, the update is applied once with an
arbitrarily chosen matching row (implementation-defined which).

**ORDER BY + LIMIT on UPDATE:** Non-standard but SQLite-supported. Useful
for "update the top N rows" patterns:
```sql
UPDATE log SET processed = 1
  ORDER BY created_at ASC
  LIMIT 100;
```
Requires that ORDER BY columns identify a unique ordering; otherwise
the set of updated rows is non-deterministic.

### 12.4 DELETE

```sql
DELETE FROM table-name
  [WHERE expr]
  [ORDER BY ordering-term [, ordering-term]*]
  [LIMIT expr [OFFSET expr]]
  [RETURNING result-column [, result-column]*]
```

**ORDER BY + LIMIT on DELETE:** Same non-standard extension as UPDATE:
```sql
DELETE FROM log
  ORDER BY created_at ASC
  LIMIT 1000;
```

**Truncate optimization:** `DELETE FROM table_name` without WHERE is
optimized to drop and recreate the B-tree root page rather than deleting
rows one by one, unless triggers or foreign keys prevent it.

### 12.5 DDL: CREATE TABLE

```sql
CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] [schema.]table-name (
  column-def [, column-def | table-constraint]*
) [WITHOUT ROWID] [STRICT];

CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] [schema.]table-name
  AS select-stmt;
```

**Column definition:**
```sql
column-name [type-name] [column-constraint]*

column-constraint :=
  PRIMARY KEY [ASC | DESC] [conflict-clause] [AUTOINCREMENT]
  | NOT NULL [conflict-clause]
  | UNIQUE [conflict-clause]
  | CHECK (expr)
  | DEFAULT (expr) | DEFAULT literal | DEFAULT signed-number
  | COLLATE collation-name
  | REFERENCES foreign-table [(foreign-column)] [foreign-key-clause]
  | [GENERATED ALWAYS] AS (expr) [STORED | VIRTUAL]
```

**Table constraints:**
```sql
table-constraint :=
  PRIMARY KEY (indexed-column [, indexed-column]*) [conflict-clause]
  | UNIQUE (indexed-column [, indexed-column]*) [conflict-clause]
  | CHECK (expr)
  | FOREIGN KEY (column [, column]*) REFERENCES foreign-table
      [(column [, column]*)] [foreign-key-clause]
```

**Conflict clause** on constraints: `ON CONFLICT {ROLLBACK | ABORT | FAIL | IGNORE | REPLACE}`.

**Type affinity** is determined from the declared type name using these rules
(applied in order, first match wins):
1. Contains "INT" -> INTEGER affinity
2. Contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity
3. Contains "BLOB" or no type name -> BLOB affinity (NONE)
4. Contains "REAL", "FLOA", or "DOUB" -> REAL affinity
5. Otherwise -> NUMERIC affinity

**WITHOUT ROWID tables:** The table uses an index B-tree (clustered on
PRIMARY KEY) instead of a table B-tree. Requires an explicit PRIMARY KEY.
Implications: no `rowid` pseudo-column, no `AUTOINCREMENT`, `INTEGER
PRIMARY KEY` is NOT an alias for `rowid`, sort order is determined by the
PRIMARY KEY declaration including COLLATE and ASC/DESC.

**STRICT tables** (SQLite 3.37+): Column type names are restricted to
exactly INT, INTEGER, REAL, TEXT, BLOB, or ANY. Type checking is enforced
on INSERT/UPDATE: a TEXT value cannot be stored in an INT column. ANY
columns accept any type without coercion.

**Generated columns** (SQLite 3.31+):
- `VIRTUAL`: Computed on read, not stored on disk. Cannot be indexed
  directly (but expression indexes can reference the underlying expression).
- `STORED`: Computed on INSERT/UPDATE, stored on disk. Can be indexed.
- Generated columns cannot reference other generated columns that come
  later in the column definition list.

**AUTOINCREMENT:** Only valid on `INTEGER PRIMARY KEY`. Guarantees that
rowids are never reused (uses the `sqlite_sequence` system table to track
the highest ever allocated). Without AUTOINCREMENT, rowids may be reused
after DELETE.

**Foreign key clause details:**
```sql
REFERENCES parent-table [(parent-column)]
  [ON DELETE {SET NULL | SET DEFAULT | CASCADE | RESTRICT | NO ACTION}]
  [ON UPDATE {SET NULL | SET DEFAULT | CASCADE | RESTRICT | NO ACTION}]
  [MATCH {SIMPLE | PARTIAL | FULL}]
  [[NOT] DEFERRABLE [INITIALLY DEFERRED | INITIALLY IMMEDIATE]]
```

**Note on MATCH:** SQLite parses `MATCH` clauses but does not enforce them.
All foreign key constraints are handled as if `MATCH SIMPLE` were specified,
regardless of the declared match type. FrankenSQLite inherits this behavior.

Foreign key enforcement requires `PRAGMA foreign_keys = ON` (off by
default for backward compatibility).

### 12.6 DDL: CREATE INDEX

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] [schema.]index-name
  ON table-name (indexed-column [, indexed-column]*)
  [WHERE expr];

indexed-column := { column-name | expr } [COLLATE collation-name] [ASC | DESC]
```

**Partial indexes:** The WHERE clause restricts which rows appear in the
index. The query planner can only use a partial index if the query's WHERE
clause implies the index's WHERE clause. Example:
```sql
CREATE INDEX idx_active ON users(email) WHERE active = 1;
-- Usable by: SELECT * FROM users WHERE active = 1 AND email = ?
-- NOT usable by: SELECT * FROM users WHERE email = ?
```

**Expression indexes:** Index on computed expressions, not just column names:
```sql
CREATE INDEX idx_lower_email ON users(lower(email));
-- Usable by: SELECT * FROM users WHERE lower(email) = ?
```

The VDBE computes the expression for each row during index construction and
maintenance. The planner matches query expressions against index expressions
using structural equality of the AST after normalization.

### 12.7 DDL: CREATE VIEW

```sql
CREATE [TEMP | TEMPORARY] VIEW [IF NOT EXISTS] [schema.]view-name
  [(column-alias [, column-alias]*)]
  AS select-stmt;
```

Views are expanded inline during query compilation (they are not
materialized unless wrapped in a CTE with `MATERIALIZED`). Column aliases,
if provided, override the column names from the SELECT. Views can reference
CTEs, including recursive CTEs, to create recursive views.

Views are read-only unless an INSTEAD OF trigger is defined.

### 12.8 DDL: CREATE TRIGGER

```sql
CREATE [TEMP | TEMPORARY] TRIGGER [IF NOT EXISTS] [schema.]trigger-name
  {BEFORE | AFTER | INSTEAD OF}
  {DELETE | INSERT | UPDATE [OF column [, column]*]}
  ON table-name
  [FOR EACH ROW]
  [WHEN expr]
BEGIN
  dml-statement; [dml-statement; ...]
END;
```

**Trigger timing:**
- `BEFORE`: Fires before the DML operation. Can modify or prevent the
  operation by raising an error via `RAISE()`.
- `AFTER`: Fires after the DML operation has completed.
- `INSTEAD OF`: Only valid on views. Replaces the DML operation entirely.

**OLD and NEW pseudo-tables:**
- `INSERT` triggers: `NEW` refers to the inserted row. `OLD` is not available.
- `DELETE` triggers: `OLD` refers to the deleted row. `NEW` is not available.
- `UPDATE` triggers: Both `OLD` (pre-update) and `NEW` (post-update) are
  available.

**WHEN clause:** The trigger body only executes if the WHEN expression
evaluates to true. The WHEN clause can reference `OLD` and `NEW`.

**Trigger body:** May contain multiple DML statements (INSERT, UPDATE,
DELETE, SELECT). Each statement can reference `OLD`, `NEW`, and
`RAISE(IGNORE)`, `RAISE(ROLLBACK, msg)`, `RAISE(ABORT, msg)`,
`RAISE(FAIL, msg)`.

**Recursive triggers:** Enabled by `PRAGMA recursive_triggers = ON`.
When enabled, a trigger can cause itself to fire again. Maximum recursion
depth is controlled by `SQLITE_MAX_TRIGGER_DEPTH` (default 1000).

**Implementation directive (Rust safety):** Trigger execution MUST NOT use Rust
call-stack recursion. It MUST be implemented with an explicit, heap-allocated
frame stack (e.g., a `Vec<VdbeFrame>` of nested VDBE frames/subprograms) so the
depth limit is enforced deterministically and cannot cause a stack overflow in
safe Rust. In addition to the depth limit, the engine MUST enforce a
capability-budgeted memory ceiling for nested frames via `Cx` (e.g., total
register-file bytes across frames); exceeding the budget MUST fail cleanly
(`SQLITE_NOMEM` or `SQLITE_LIMIT`), not crash.

### 12.9 DDL: Other

**ALTER TABLE:**
```sql
ALTER TABLE table-name RENAME TO new-table-name;
ALTER TABLE table-name RENAME COLUMN old-name TO new-name;
ALTER TABLE table-name ADD COLUMN column-def;
ALTER TABLE table-name DROP COLUMN column-name;
```

DROP COLUMN (SQLite 3.35+) always rewrites the table to purge the dropped
column's data. The command fails if the column is part of the PRIMARY KEY,
has a UNIQUE constraint, is referenced by an index, appears in a CHECK or
foreign key constraint, or is the only column in the table.

**DROP statements:**
```sql
DROP TABLE [IF EXISTS] [schema.]table-name;
DROP INDEX [IF EXISTS] [schema.]index-name;
DROP VIEW [IF EXISTS] [schema.]view-name;
DROP TRIGGER [IF EXISTS] [schema.]trigger-name;
```

### 12.10 Transaction Control

```sql
BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE | CONCURRENT] [TRANSACTION];
COMMIT [TRANSACTION];
END [TRANSACTION];           -- synonym for COMMIT
ROLLBACK [TRANSACTION];

SAVEPOINT savepoint-name;
RELEASE [SAVEPOINT] savepoint-name;
ROLLBACK [TRANSACTION] TO [SAVEPOINT] savepoint-name;
```

**Transaction modes:**
- `DEFERRED` (default): No locks acquired until first read/write.
- `IMMEDIATE`: Acquires a RESERVED lock immediately (blocks other writers).
- `EXCLUSIVE`: Acquires an EXCLUSIVE lock immediately (blocks readers too,
  in rollback journal mode; equivalent to IMMEDIATE in WAL mode).
- `CONCURRENT`: FrankenSQLite extension. Enters MVCC concurrent writer mode
  with Snapshot Isolation. Multiple CONCURRENT transactions can write
  simultaneously to different pages. Conflict on the same page results in
  `SQLITE_BUSY_SNAPSHOT` for the second committer.

**Savepoints** form a stack. `RELEASE X` commits all work since `SAVEPOINT X`
and removes X and all more recent savepoints from the stack. `ROLLBACK TO X`
undoes all work since `SAVEPOINT X` but leaves X on the stack (allowing
further work within the same savepoint scope).

### 12.11 ATTACH / DETACH

```sql
ATTACH [DATABASE] expr AS schema-name;
DETACH [DATABASE] schema-name;
```

`expr` evaluates to a filename string. The attached database gets the schema
name and its tables are accessible as `schema-name.table-name`. The main
database is always named `main`. The temp database is always named `temp`.
Maximum 10 attached databases by default (`SQLITE_MAX_ATTACHED`). Cross-database
transactions are atomic only in rollback journal mode (not WAL mode in
standard SQLite; FrankenSQLite MUST support cross-database atomic WAL
transactions via two-phase commit across attached database WAL files).

### 12.12 EXPLAIN and EXPLAIN QUERY PLAN

```sql
EXPLAIN statement;
EXPLAIN QUERY PLAN statement;
```

**EXPLAIN** returns the VDBE bytecode program as a result set with columns:
`addr`, `opcode`, `p1`, `p2`, `p3`, `p4`, `p5`, `comment`. Each row is one
VDBE instruction. This is the primary debugging tool for understanding query
execution.

**EXPLAIN QUERY PLAN** returns a high-level description of the query plan
with columns: `id`, `parent`, `notused`, `detail`. The `detail` column
contains human-readable text describing scan order, index usage, and sort
operations. Tree structure is encoded via `id`/`parent` relationships.

### 12.13 VACUUM

```sql
VACUUM [schema-name];
VACUUM [schema-name] INTO filename;
```

`VACUUM` rebuilds the database file, reclaiming free pages and defragmenting.
It works by creating a new database, copying all content, then replacing the
original. `VACUUM INTO` writes the rebuilt database to a new file without
modifying the original, functioning as a compact backup.

### 12.14 Other Statements

```sql
REINDEX [collation-name | [schema.]table-or-index-name];
ANALYZE [schema-name | table-or-index-name];
PRAGMA [schema.]pragma-name [= value | (value)];
```

`ANALYZE` populates `sqlite_stat1` and optionally `sqlite_stat4` tables with
index statistics used by the query planner for cost estimation. `REINDEX`
rebuilds indexes after collation sequence changes.

### 12.15 Expression Syntax

Full expression grammar including all operators by precedence (highest first):

| Precedence | Operators | Assoc |
|-----------|-----------|-------|
| 1 (highest) | `~` (bitwise NOT), unary `+`, unary `-` | Right |
| 2 | `COLLATE` (postfix collation override) | Left |
| 3 | `\|\|` (string concat), `->`, `->>` (JSON extract) | Left |
| 4 | `*`, `/`, `%` | Left |
| 5 | `+`, `-` | Left |
| 6 | `<<`, `>>`, `&`, `\|` | Left |
| 7 | `ESCAPE` | Right |
| 8 | `<`, `<=`, `>`, `>=` | Left |
| 9 | `=`, `==`, `!=`, `<>`, `IS`, `IS NOT`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`, `IN`, `LIKE`, `GLOB`, `MATCH`, `REGEXP`, `BETWEEN`, `ISNULL`, `NOTNULL`, `NOT NULL` | Left |
| 10 | `NOT` (prefix logical negation -- lower than comparisons: `NOT x = y` parses as `NOT (x = y)`) | Right |
| 11 | `AND` | Left |
| 12 (lowest) | `OR` | Left |

**IMPORTANT:** `NOT` is NOT at the same precedence as unary `~`/`+`/`-`.
In SQLite, `NOT x = y` means `NOT (x = y)`, not `(NOT x) = y`. This
matches the SQLite source grammar (parse.y) and documentation. Unary
operators bind tighter than `COLLATE`: `-x COLLATE NOCASE` parses as
`(-x) COLLATE NOCASE`, not `-(x COLLATE NOCASE)`.

**Special expression forms:**
- `CAST(expr AS type-name)` -- explicit type conversion
- `CASE [expr] WHEN expr THEN expr [ELSE expr] END` -- conditional
- `EXISTS (select-stmt)` -- subquery existence test
- `expr [NOT] IN (select-stmt | expr-list)` -- membership test
- `expr [NOT] BETWEEN expr AND expr` -- range test
- `expr COLLATE collation-name` -- collation override
- `expr [NOT] LIKE pattern [ESCAPE char]` -- pattern match (% and _)
- `expr [NOT] GLOB pattern` -- case-sensitive glob (* and ?)
- `RAISE(IGNORE | ROLLBACK,msg | ABORT,msg | FAIL,msg)` -- trigger only
- `expr -> path` -- JSON extract (returns JSON)
- `expr ->> path` -- JSON extract (returns SQL value)

### 12.16 Type Affinity Rules

Five affinities: TEXT, NUMERIC, INTEGER, REAL, BLOB.

**Affinity determination from declared type** (first match wins):
1. Type name contains "INT" -> INTEGER
2. Type name contains "CHAR", "CLOB", or "TEXT" -> TEXT
3. Type name contains "BLOB" or is empty -> BLOB
4. Type name contains "REAL", "FLOA", or "DOUB" -> REAL
5. Otherwise -> NUMERIC

**Comparison affinity rules** (applied before comparison; determines which
operand gets type coercion -- per SQLite documentation `datatype3.html`):

1. If one operand has INTEGER, REAL, or NUMERIC affinity and the other has
   TEXT or BLOB/NONE affinity: apply numeric affinity to the TEXT/BLOB
   operand only. (The numeric operand is already in numeric form.)
2. If one operand has TEXT affinity and the other has BLOB/NONE affinity
   (and neither has numeric affinity): apply TEXT affinity to the BLOB/NONE
   operand only.
3. Otherwise (both have the same affinity class, or both have BLOB/NONE):
   no affinity conversion is applied.

**Key distinction from a common misreading:** affinity is applied to the
operand that needs conversion, not to both. If both operands already share
an affinity class, no coercion occurs.

### 12.17 Time Travel Queries (Native Mode Extension)

Native mode persists an immutable commit stream (capsules + markers). This
enables **time travel** queries that evaluate reads against a historical commit
sequence.

**Syntax (extension):**

Time travel is expressed on table references:

```sql
SELECT ... FROM my_table FOR SYSTEM_TIME AS OF '2023-10-27 10:00:00';
SELECT ... FROM my_table FOR SYSTEM_TIME AS OF COMMITSEQ 1234567;
```

**Semantics (normative):**

1. Determine `target_commit_seq`:
   - If `AS OF COMMITSEQ N`, then `target_commit_seq := N`.
   - Otherwise parse the `time-string` using SQLite-compatible datetime rules
     (same inputs accepted by `unixepoch(...)`) and convert to
     `target_time_unix_ns`.
     Then binary-search commit sequence space using random-access marker reads
     (§3.5.4.1) for the greatest marker with:
     `marker.commit_time_unix_ns <= target_time_unix_ns`, and set
     `target_commit_seq := marker.commit_seq`.
2. Create a synthetic read-only snapshot `S` with `S.high = target_commit_seq`.
3. Execute the query using the normal MVCC resolution rules:
   `resolve(P, S)` returns the newest committed version with
   `version.commit_seq <= S.high` (§3.6, §5).

**Restrictions (V1):**

- Time travel is read-only. Any attempt to execute `INSERT/UPDATE/DELETE/DDL`
  in a time-travel context MUST fail with `SQLITE_ERROR` (or a more specific
  SQLite-compatible error code when applicable).

**Retention and tiered storage:**

- If the retention policy has pruned the requested historical state, time
  travel MUST fail with an explicit error indicating "history not retained".
- With tiered storage enabled (§3.5.11), older commit capsules and index
  segments MAY reside only in remote storage; the engine MUST fetch symbols on
  demand under `Cx` budgets and decode/repair as usual.

---

## 13. Built-in Functions

FrankenSQLite implements all built-in functions from SQLite 3.52.0. All
functions follow SQLite's NULL propagation rule: if any argument is NULL,
the result is NULL, unless the function is specifically documented to handle
NULL differently.

### 13.1 Core Scalar Functions

**abs(X)** -> integer or real. Returns the absolute value of X. If X is
NULL, returns NULL. If X is the integer -9223372036854775808 (minimum i64),
an integer overflow error is raised because the result cannot be represented
as a positive i64. If X is a string that looks numeric, it is coerced.

**char(X1, X2, ..., XN)** -> text. Returns a string composed of characters
with Unicode code points X1 through XN. NULL arguments are silently skipped.

**coalesce(X, Y, ...)** -> any. Returns the first non-NULL argument. If all
arguments are NULL, returns NULL. Short-circuits: arguments after the first
non-NULL are not evaluated.

**concat(X, Y, ...)** -> text (SQLite 3.44+). Concatenates all arguments
as text. NULL arguments are treated as empty strings (unlike `||` which
propagates NULL).

**concat_ws(SEP, X, Y, ...)** -> text (SQLite 3.44+). Concatenates with
separator. NULL arguments are skipped entirely (no double separators).

**format(FORMAT, ...)** / **printf(FORMAT, ...)** -> text. SQL-specific
printf with format specifiers:
- `%d` -- integer (truncates floating point)
- `%f` -- floating point (default 6 decimal places)
- `%e` / `%E` -- scientific notation
- `%g` / `%G` -- shorter of %f and %e
- `%s` -- string (NULL renders as empty string)
- `%q` -- string with single-quotes doubled (for SQL literals)
- `%Q` -- like %q but wraps in single quotes, NULL renders as `NULL` (unquoted)
- `%w` -- like %q but wraps in double quotes (for identifiers)
- `%c` -- character from integer code point
- `%n` -- no-op (deliberately disabled for security; does NOT write to memory)
- `%z` -- same as %s (compatibility)
- `%%` -- literal percent sign
Width, precision, and flag modifiers (`-`, `+`, ` `, `0`) are supported.

**glob(PATTERN, STRING)** -> integer (0 or 1). Case-sensitive glob match.
`*` matches any sequence, `?` matches any single character, `[...]` matches
character classes. This is the function form of the `GLOB` operator.

**hex(X)** -> text. Returns the hexadecimal rendering of X. If X is a blob,
each byte becomes two hex characters. If X is text, the UTF-8 bytes are
rendered. If X is a number, it is first converted to its UTF-8 text
representation, then those bytes are hex-encoded (NOT the raw IEEE-754 bits).

**iif(B1, V1 [, B2, V2, ...] [, ELSE])** -> any. Three-argument form is
equivalent to `CASE WHEN B1 THEN V1 ELSE ELSE END`. Multi-condition form
(SQLite 3.49+) evaluates B1, B2, ... in order, returning the first Vn where
Bn is true. Short-circuits evaluation. **`if()`** is an alias (SQLite 3.48+).
Two-argument `iif(COND, X)` returns NULL when COND is false (SQLite 3.48+).

**ifnull(X, Y)** -> any. Returns X if X is not NULL, otherwise Y.
Equivalent to `coalesce(X, Y)`.

**instr(X, Y)** -> integer. Returns the 1-based position of the first
occurrence of Y in X, or 0 if not found. If either argument is NULL,
returns NULL. For blob arguments, operates on bytes; for text, operates
on characters.

**last_insert_rowid()** -> integer. Returns the rowid of the most recent
successful INSERT on the same database connection.

**length(X)** -> integer. For text: number of characters (not bytes). For
blob: number of bytes. For NULL: NULL. For numbers: length of text
representation.

**like(PATTERN, STRING [, ESCAPE])** -> integer. Case-insensitive pattern
match. `%` matches any sequence, `_` matches any single character. Optional
ESCAPE character. This is the function form of the `LIKE` operator.

**likelihood(X, P)** -> any. Returns X unchanged. Hints to the query
planner that X is true with probability P (0.0 to 1.0). P must be a
compile-time constant.

**likely(X)** -> any. Equivalent to `likelihood(X, 0.9375)`.

**unlikely(X)** -> any. Equivalent to `likelihood(X, 0.0625)`.

**lower(X)** -> text. Converts ASCII characters to lowercase. For full
Unicode case folding, the ICU extension is required.

**upper(X)** -> text. Converts ASCII characters to uppercase.

**ltrim(X [, Y])** -> text. Removes characters in Y from the left of X.
Default Y is spaces.

**rtrim(X [, Y])** -> text. Removes characters in Y from the right of X.

**trim(X [, Y])** -> text. Removes characters in Y from both sides of X.

**max(X, Y, ...)** -> any. Returns the argument with the maximum value.
Uses the standard SQLite comparison rules. **If ANY argument is NULL,
returns NULL immediately** (this is the scalar multi-argument form; the
aggregate `max(X)` over a column ignores NULLs per SQL standard). When
used as a scalar function (not aggregate), handles 2+ arguments.

**min(X, Y, ...)** -> any. Returns the argument with the minimum value.
Same NULL semantics as scalar `max()`: **if ANY argument is NULL, returns
NULL immediately**. The aggregate `min(X)` over a column ignores NULLs.

**nullif(X, Y)** -> any. Returns NULL if X = Y, otherwise returns X.

**octet_length(X)** -> integer (SQLite 3.43+). Returns the number of bytes
in the UTF-8 encoding of X. For numeric values, X is first converted to its
text representation. This is equivalent to `length(CAST(X AS BLOB))` and
differs from `length(X)` for UTF-8 text (`length` counts characters, not bytes).

**quote(X)** -> text. Returns X in a form suitable for inclusion in SQL.
Text is single-quoted with internal quotes doubled. Blobs become `X'hex'`.
NULL becomes the string `NULL`. Numbers are rendered as-is.

**random()** -> integer. Returns a pseudo-random 64-bit signed integer.
Uses a PRNG seeded from the system entropy source at connection open.

**randomblob(N)** -> blob. Returns an N-byte blob of pseudo-random data.

**replace(X, Y, Z)** -> text. Replaces every occurrence of Y in X with Z.
If Y is empty string, returns X unchanged.

**round(X [, N])** -> real. Rounds X to N decimal places (default 0).
Uses round half away from zero (e.g., round(2.5) = 3.0, round(-2.5) = -3.0).
This is NOT banker's rounding.

**sign(X)** -> integer. Returns -1, 0, or +1 for negative, zero, or
positive X. Returns NULL for NULL. Returns NULL for non-numeric strings.

**soundex(X)** -> text. Returns the Soundex encoding of X as a 4-character
string (letter + 3 digits). Returns `?000` for empty or NULL input.

**substr(X, START [, LENGTH])** / **substring(X, START [, LENGTH])** -> text
or blob.

- 1-based indexing for `START > 0`.
- `START = 0` is a historical quirk (SQLite default behavior): if LENGTH is
  provided and `LENGTH > 0`, the function returns `max(LENGTH - 1, 0)` elements
  from the start; if LENGTH is omitted, it behaves like `START = 1`.
- Negative START counts from the end.
- If LENGTH is omitted, returns from START to the end.
- If LENGTH is negative, returns `abs(LENGTH)` characters (or bytes for BLOB)
  immediately preceding START (to the left), excluding the element at START.

**typeof(X)** -> text. Returns `"null"`, `"integer"`, `"real"`, `"text"`,
or `"blob"`.

**subtype(X)** -> integer. Returns the subtype of X as an integer tag
(`sqlite3_value_subtype(X)`). Unlike most scalar functions, `subtype()` does
NOT propagate NULL: `subtype(NULL) = 0` (the same value used for "no subtype").

**unhex(X [, Y])** -> blob (SQLite 3.41+). Decodes hex string X into blob.
Y specifies characters to ignore (e.g., spaces, dashes). Returns NULL if X
contains invalid hex characters (after removing Y characters).

**unicode(X)** -> integer. Returns the Unicode code point of the first
character of text X.

**unistr(X)** -> text (SQLite 3.45+; `SQLITE_ENABLE_UNISTR_FUNCTION` in C builds).
Interprets `\uXXXX` and `\UXXXXXXXX` escape sequences in X.

**zeroblob(N)** -> blob. Returns a blob consisting of N zero bytes.
Efficiently represented internally without allocating N bytes.

**sqlite_version()** -> text. Returns the version string (e.g., "3.52.0").
For compatibility, FrankenSQLite SHOULD report its claimed SQLite feature
compatibility target (so application feature detection works). It MAY also
expose an engine-specific version via a separate function.

**sqlite_source_id()** -> text. Returns source identification string.

**sqlite_compileoption_used(X)** -> integer (0 or 1). Returns 1 if compile
option X was used, else 0.

**sqlite_compileoption_get(N)** -> text or NULL. Returns the Nth compile-time
option string. Returns NULL if N is out of range.

**changes()** -> integer. Returns the number of rows modified by the most
recent INSERT, UPDATE, or DELETE on the same connection.

**total_changes()** -> integer. Returns the total number of rows modified
since the connection was opened.

**sqlite_offset(X)** -> integer (`SQLITE_ENABLE_OFFSET_SQL_FUNC` in C builds).
Returns the byte offset of the value for column X within the underlying record
payload. Only meaningful within a query; requires that X be a direct column
reference (not an expression).

### 13.2 Math Functions (SQLite 3.35+)

In C SQLite, these require the `-DSQLITE_ENABLE_MATH_FUNCTIONS` compile flag
(enabled by default since 3.35.0). FrankenSQLite always includes them.

All math functions return NULL for NULL input. For domain errors (e.g.,
sqrt of negative), the behavior depends on the function.

**acos(X)** -> real. Arc cosine. Domain: [-1, 1]. Returns NULL for out-of-domain.
**acosh(X)** -> real. Inverse hyperbolic cosine. Domain: [1, +inf).
**asin(X)** -> real. Arc sine. Domain: [-1, 1].
**asinh(X)** -> real. Inverse hyperbolic sine. Domain: all reals.
**atan(X)** -> real. Arc tangent. Domain: all reals.
**atan2(Y, X)** -> real. Two-argument arc tangent. Returns angle in radians.
**atanh(X)** -> real. Inverse hyperbolic tangent. Domain: (-1, 1).
**ceil(X)** / **ceiling(X)** -> integer or real. Smallest integer >= X.
Returns INTEGER if X is INTEGER; otherwise returns a REAL with an integral
value (e.g., `ceil(1.2) = 2.0`).
**cos(X)** -> real. Cosine (X in radians).
**cosh(X)** -> real. Hyperbolic cosine.
**degrees(X)** -> real. Converts radians to degrees.
**exp(X)** -> real. e raised to the power X. Overflow returns +Inf.
**floor(X)** -> integer or real. Largest integer <= X.
Returns INTEGER if X is INTEGER; otherwise returns a REAL with an integral value.
**ln(X)** -> real. Natural logarithm. Domain: (0, +inf). Returns NULL for X <= 0.
**log(X)** / **log10(X)** -> real. Base-10 logarithm.
**log(B, X)** -> real. Base-B logarithm. Computed as ln(X)/ln(B).
**log2(X)** -> real. Base-2 logarithm.
**mod(X, Y)** -> real or integer. Remainder of X/Y. Returns NULL if Y is 0.
**pi()** -> real. Returns 3.141592653589793.
**pow(X, Y)** / **power(X, Y)** -> real. X raised to the power Y.
**radians(X)** -> real. Converts degrees to radians.
**(sign(X) is a core scalar, not a math function -- see §13.1.)**
**sin(X)** -> real. Sine (X in radians).
**sinh(X)** -> real. Hyperbolic sine.
**sqrt(X)** -> real. Square root. Returns NULL for negative X.
**tan(X)** -> real. Tangent (X in radians).
**tanh(X)** -> real. Hyperbolic tangent.
**trunc(X)** -> integer or real. Truncates toward zero.
Returns INTEGER if X is INTEGER; otherwise returns a REAL with an integral value.

**NaN and Inf handling (normative):** SQLite stores IEEE-754 doubles as REAL.
`+Inf` and `-Inf` are valid REAL values and can be produced by overflow
(e.g., `exp(1000)` yields `Inf`). Division by zero yields NULL (not Inf/NaN).

FrankenSQLite MUST match SQLite observable behavior:
- propagate `+Inf` / `-Inf` as REAL values when SQLite does,
- normalize NaN results to NULL (and avoid surfacing NaN as a stored value).

### 13.3 Date/Time Functions

All date/time functions accept time strings in ISO-8601 format and optional
modifiers. The time string formats recognized are:
- `YYYY-MM-DD`
- `YYYY-MM-DD HH:MM`
- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DD HH:MM:SS.SSS`
- `YYYY-MM-DDTHH:MM:SS.SSS` (T separator)
- `HH:MM`, `HH:MM:SS`, `HH:MM:SS.SSS` (date defaults to 2000-01-01)
- `DDDDDDDDDD` (Julian day number as float)
- `now` (current date/time)

**Modifiers** (applied left to right):
- `NNN days`, `NNN hours`, `NNN minutes`, `NNN seconds`, `NNN months`, `NNN years`
- `start of month`, `start of year`, `start of day`
- `weekday N` (advance to next day-of-week, 0=Sunday)
- `unixepoch` (interpret input as Unix timestamp)
- `julianday` (interpret input as Julian day)
- `auto` (auto-detect unix epoch vs Julian day)
- `localtime` (convert to local time)
- `utc` (convert to UTC)
- `subsec` / `subsecond` (include fractional seconds in output)

**date(time-string, modifier, ...)** -> text. Returns `YYYY-MM-DD`.
**time(time-string, modifier, ...)** -> text. Returns `HH:MM:SS`.
**datetime(time-string, modifier, ...)** -> text. Returns `YYYY-MM-DD HH:MM:SS`.
**julianday(time-string, modifier, ...)** -> real. Returns Julian day number.
**unixepoch(time-string, modifier, ...)** -> integer. Returns Unix timestamp.
**strftime(format, time-string, modifier, ...)** -> text. Format specifiers:
`%d` day (01-31), `%e` day with leading space (SQLite 3.44+),
`%f` fractional seconds SS.SSS, `%H` hour 00-23, `%I` hour 01-12 (SQLite 3.44+),
`%j` day of year 001-366, `%J` Julian day number,
`%k` hour 0-23 with leading space (SQLite 3.44+),
`%l` hour 1-12 with leading space (SQLite 3.44+),
`%m` month 01-12, `%M` minute 00-59,
`%p` AM/PM (SQLite 3.44+), `%P` am/pm lowercase (SQLite 3.44+),
`%R` equivalent to `%H:%M` (SQLite 3.44+),
`%s` Unix timestamp, `%S` seconds 00-59,
`%T` equivalent to `%H:%M:%S` (SQLite 3.44+),
`%u` ISO 8601 weekday 1-7 Mon=1 (SQLite 3.44+),
`%w` day of week 0-6 Sun=0, `%W` week of year 00-53,
`%G` ISO 8601 year (SQLite 3.44+), `%g` 2-digit ISO year (SQLite 3.44+),
`%V` ISO 8601 week number 01-53 (SQLite 3.44+),
`%Y` year, `%%` literal %.
**timediff(time1, time2)** -> text (SQLite 3.43+). Returns the difference
as `+YYYY-MM-DD HH:MM:SS.SSS`.

### 13.4 Aggregate Functions

**avg(X)** -> real. Average of non-NULL values. Returns NULL for empty set.
Internally accumulates sum and count separately to avoid precision loss.

**count(*)** -> integer. Counts all rows (including NULLs).
**count(X)** -> integer. Counts non-NULL values of X.

**group_concat(X [, SEP] [ORDER BY ...])** -> text. Concatenates non-NULL
values with separator (default `,`). Without an ORDER BY clause, the
concatenation order is arbitrary. Since SQLite 3.44+, an ORDER BY clause
can be specified directly inside the function call to control concatenation
order: `group_concat(name, ', ' ORDER BY name)`. This is distinct from
the SELECT-level ORDER BY (which orders result rows, not aggregated values).

**string_agg(X, SEP [ORDER BY ...])** -> text (SQLite 3.44+). SQL-standard
alias for `group_concat(X, SEP)`. Supports the same in-aggregate ORDER BY
clause: `string_agg(name, ', ' ORDER BY name DESC)`.

**max(X)** -> any. Returns maximum non-NULL value. For aggregate use
(single argument).

**min(X)** -> any. Returns minimum non-NULL value.

**sum(X)** -> integer or real. Sum of non-NULL values. Returns **NULL** for
empty set (use `total()` for a guaranteed non-NULL 0.0 result). Raises an
integer overflow error if the sum exceeds i64 range.

**total(X)** -> real. Always returns a float (0.0 for empty set). Never
overflows (uses double precision). Use `total()` instead of `sum()` when
you need a guaranteed non-NULL result.

**median(X)** -> real (SQLite 3.51+, requires SQLITE_ENABLE_PERCENTILE which
is enabled by default in amalgamation builds since 3.51.0). Equivalent to
`percentile_cont(X, 0.5)`. Returns the interpolated median of non-NULL values.

**percentile(Y, P)** -> real (SQLite 3.51+). Returns the P-th percentile of
non-NULL values in Y, where P is a percentage in the range 0.0 to 100.0.
Uses linear interpolation between adjacent values.

**percentile_cont(Y, P)** -> real (SQLite 3.51+). Continuous percentile per
SQL standard. P is a fraction in the range 0.0 to 1.0. Interpolates between
adjacent input values.

**percentile_disc(Y, P)** -> any (SQLite 3.51+). Discrete percentile per SQL
standard. P is a fraction in the range 0.0 to 1.0. Returns an actual input
value (no interpolation).

### 13.5 Window Functions

All aggregate functions can also be used as window functions. In addition,
the following are window-function-only:

**row_number()** -> integer. Sequential number of each row in its partition,
starting from 1. No frame clause needed.

**rank()** -> integer. Rank with gaps. Rows with equal ORDER BY values get
the same rank; the next distinct value gets rank = number of preceding rows + 1.

**dense_rank()** -> integer. Rank without gaps. Next distinct value gets
the previous rank + 1.

**percent_rank()** -> real. `(rank - 1) / (partition_rows - 1)`. Returns
0.0 for partitions with one row.

**cume_dist()** -> real. Cumulative distribution: `row_number / partition_rows`
where `row_number` is the row_number() of the **last peer** in the current
peer group. All rows with the same ORDER BY value get the same cume_dist.
For partition [1,2,2,3]: cume_dist values are 0.25, 0.75, 0.75, 1.0.

**ntile(N)** -> integer. Distributes rows into N roughly equal groups,
numbered 1 through N.

**lag(X [, offset [, default]])** -> any. Returns the value of X from the
row `offset` rows before the current row in the partition. Default offset is
1. Default default is NULL.

**lead(X [, offset [, default]])** -> any. Returns the value of X from the
row `offset` rows after the current row.

**first_value(X)** -> any. Returns X from the first row in the window frame.

**last_value(X)** -> any. Returns X from the last row in the window frame.
Note: with the default frame (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW`), this always returns the current row's value. Use `ROWS BETWEEN
UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` for the true last value.

**nth_value(X, N)** -> any. Returns X from the Nth row (1-based) in the
window frame. Returns NULL if the frame has fewer than N rows.

**Frame interaction details:** The `inverse` method on the WindowFunction
trait is called when rows exit the frame (for ROWS and GROUPS modes). This
enables O(1) amortized per-row computation for functions like sum and count
over sliding windows, rather than recomputing from scratch.

### 13.6 COLLATE Interaction

Collation affects ordering/comparison semantics, not raw string processing.

**Functions affected by collation:** `min` / `max` (scalar and aggregate) use
SQLite's comparison rules and therefore respect collation.

**Functions NOT affected by collation:** `instr`, `replace`, `LIKE`, and `GLOB`
do not use collation; they implement their own byte/character and/or
case-folding rules.

When operands have different collations for a comparison, SQLite's normal
collation selection rules apply:
1. Explicit `COLLATE` clause wins (if multiple explicit collations appear,
   the leftmost one wins)
2. Column collation from the schema
3. Default `BINARY` collation

Built-in collations: `BINARY` (memcmp), `NOCASE` (ASCII case-insensitive),
`RTRIM` (ignores trailing spaces).

---

## 14. Extensions

Each extension resides in its own crate under `crates/fsqlite-ext-*` and is
independently feature-gated. Extensions are compiled in (not dynamically
loaded), controlled by Cargo features on the `fsqlite` facade crate.

This is intentional: extensions carry different optional dependency sets
(sometimes heavy), and separate crates improve dependency isolation and
incremental builds (changing JSON should not force rebuilding FTS5, ICU, etc.).

### 14.1 JSON1 (`fsqlite-ext-json`)

JSON1 provides comprehensive JSON manipulation within SQL. SQLite 3.45+
introduces JSONB, an internal binary format that avoids re-parsing JSON on
every function call.

#### 14.1.1 Scalar Functions

**json(X)** -> text. Validates and minifies JSON text X. **Throws an error**
(not NULL) if X is not well-formed JSON or JSONB. Converts JSONB to text JSON.

**json_valid(X [, FLAGS])** -> integer. Returns 1 if X is well-formed according
to FLAGS, 0 otherwise. FLAGS bitmask (SQLite 3.45+, default 0x01):
- 0x01: Accept RFC-8259 canonical JSON text
- 0x02: Accept JSON5 text extensions
- 0x04: Accept JSONB blob (superficial check)
- 0x08: Accept JSONB blob (strict format verification)

**json_type(X [, PATH])** -> text. Returns the type of the JSON value at
PATH as one of: `"null"`, `"true"`, `"false"`, `"integer"`, `"real"`,
`"text"`, `"array"`, `"object"`. Returns SQL NULL if PATH does not exist.

**json_extract(X, PATH, ...)** -> any. Extracts value(s) from JSON. Single
path: returns SQL value (text for strings, integer/real for numbers, NULL for
JSON null). Multiple paths: returns a JSON array of the extracted values.
PATH syntax: `$` for root, `.key` for object member, `[N]` for array element
(0-based), `[#-N]` for array element from end.

**X -> PATH** (alias for json_extract with single path, returning JSON text)
**X ->> PATH** (alias for json_extract with single path, returning SQL value)

The `->>` operator is the most commonly used. `json_extract` and `->>` both
unwrap JSON strings to SQL text, JSON numbers to SQL integers/reals, and
JSON null to SQL NULL. The `->` operator preserves JSON typing (returns JSON
text for string values, including the surrounding quotes).

**json_set(X, PATH, VALUE, ...)** -> text. Sets values at paths. Creates
new keys if they do not exist. Overwrites existing values. PATH/VALUE
arguments come in pairs.

**json_insert(X, PATH, VALUE, ...)** -> text. Like json_set but does NOT
overwrite existing values. Only creates new keys/elements.

**json_replace(X, PATH, VALUE, ...)** -> text. Like json_set but does NOT
create new keys. Only overwrites existing values.

**json_remove(X, PATH, ...)** -> text. Removes elements at the specified
paths. Array elements are removed and the array is compacted.

**json_patch(X, Y)** -> text. Implements RFC 7396 JSON Merge Patch.
Recursively merges Y into X. NULL values in Y delete keys in X.

**json_quote(X)** -> text. Converts SQL value X to its JSON representation.
Text becomes a JSON string (with escaping), integer/real become JSON numbers,
NULL becomes JSON `null`, blob becomes JSON text via hex encoding.

**json_array(X, ...)** -> text. Returns a JSON array containing all arguments.

**json_object(KEY, VALUE, ...)** -> text. Returns a JSON object. Arguments
are key/value pairs. Keys must be text.

**jsonb(X)** -> blob. Converts JSON text X to the JSONB binary format.
Throws an error if X is not well-formed JSON. The inverse of `json(X)`.

**json_array_length(X [, PATH])** -> integer. Returns the number of elements
in the JSON array X (or at PATH within X). Returns 0 for `[]`, NULL if
X is not an array or PATH does not exist.

**json_error_position(X)** -> integer (SQLite 3.42+). Returns 0 if X is
well-formed JSON, or the 1-based character position of the first syntax
error. Useful for diagnosing malformed JSON without a try/catch.

**json_pretty(X [, INDENT])** -> text (SQLite 3.46+). Returns a
pretty-printed version of JSON text X. INDENT defaults to 4 spaces;
pass a string to use custom indentation (e.g., `json_pretty(X, char(9))`
for tabs).

**JSONB variants:** Every JSON1 scalar function that returns JSON text has
a corresponding `jsonb_*` variant that returns JSONB blob instead:
`jsonb_extract`, `jsonb_set`, `jsonb_insert`, `jsonb_replace`,
`jsonb_remove`, `jsonb_patch`, `jsonb_array`, `jsonb_object`,
`jsonb_group_array`, `jsonb_group_object`. These avoid the
text→JSONB→text round-trip when the result will be stored or passed to
another JSON function.

#### 14.1.2 Aggregate Functions

**json_group_array(X)** -> text. Returns a JSON array containing X from all
rows in the group. NULL values are included as JSON `null`.

**json_group_object(KEY, VALUE)** -> text. Returns a JSON object with
key/value pairs from all rows. Duplicate keys result in the last value winning.

#### 14.1.3 Table-Valued Functions

**json_each(X [, PATH])** -> virtual table. Iterates over the top-level
elements of the JSON array or object at PATH. Columns:
- `key`: array index (integer) or object key (text)
- `value`: the element value (SQL type)
- `type`: JSON type name
- `atom`: the element value (always as SQL type, NULL for arrays/objects)
- `id`: unique integer ID for this element within the JSON
- `parent`: ID of the parent element
- `fullkey`: full path to this element (e.g., `$.store.book[0].title`)
- `path`: path to the parent (e.g., `$.store.book[0]`)

**json_tree(X [, PATH])** -> virtual table. Like json_each but recursively
descends into nested arrays and objects. Same column schema as json_each.

#### 14.1.4 JSONB Binary Format

JSONB is a binary encoding of JSON stored as a BLOB. Structure:
- Each node is a header byte (4-bit type + 4-bit size-of-payload-size),
  followed by the payload size (0, 1, 2, 4, or 8 bytes), followed by payload.
- Node types (lower 4 bits of first header byte):
  null(0x0), true(0x1), false(0x2), int(0x3), int5(0x4), float(0x5),
  float5(0x6), text(0x7), textj(0x8), text5(0x9), textraw(0xA),
  array(0xB), object(0xC). Types 0xD–0xF are reserved.
  Upper 4 bits of the first header byte encode payload size category.
- Arrays and objects store their children as concatenated child nodes.
- JSONB is typically 5–10% smaller than text JSON and avoids parsing
  overhead on every function call.

Functions that produce JSON output also accept and produce JSONB when the
input is JSONB, preserving the binary format through chains of function
calls. Use `json(X)` to convert JSONB to text, or `jsonb(X)` to convert
text to JSONB.

### 14.2 FTS5 (`fsqlite-ext-fts5`)

FTS5 (Full-Text Search version 5) provides efficient full-text search over
large text corpora using an inverted index architecture.

#### 14.2.1 Table Creation

```sql
CREATE VIRTUAL TABLE docs USING fts5(
  title,
  body,
  content=external_table,     -- external content table
  content_rowid=id,           -- rowid column in external content table
  tokenize='porter unicode61', -- tokenizer pipeline
  prefix='2,3',               -- prefix indexes for 2 and 3 character prefixes
  detail=full                 -- posting list detail level
);
```

**detail levels:**
- `full` (default): Stores column number and token position. Supports all queries.
- `column`: Stores only column number. Position-dependent queries (NEAR, phrase)
  not supported.
- `none`: Stores only docid. Neither column filters nor position queries supported.

#### 14.2.2 Tokenizer API

FTS5 tokenizers implement a trait that receives text and emits tokens:

```rust
pub trait Fts5Tokenizer: Send + Sync {
    fn tokenize(
        &self,
        text: &str,
        flags: TokenizeFlags,
        callback: &mut dyn FnMut(token: &str, start: usize, end: usize) -> Result<()>,
    ) -> Result<()>;
}
```

Built-in tokenizers:
- `unicode61`: Unicode-aware tokenization with diacritics removal. Configurable
  separators and token characters.
- `ascii`: ASCII-only tokenization. Faster but handles only ASCII text.
- `porter`: Porter stemming wrapper. Applied after another tokenizer:
  `tokenize='porter unicode61'`.
- `trigram`: Splits text into 3-character sequences. Enables substring search
  (`LIKE '%pattern%'`) via FTS.

Custom tokenizer registration:
```rust
db.create_fts5_tokenizer("my_tokenizer", MyTokenizer::new())?;
```

#### 14.2.3 Inverted Index Structure

FTS5 stores its index in a shadow table `{table}_data` as a segment-based
structure (similar to an LSM tree):

**Segments:** Each segment is a sorted run of term/doclist pairs. New
documents are initially written to a small in-memory segment, then flushed.
Background merge operations combine small segments into larger ones (tiered
compaction).

**Term format:** Terms are stored as prefix-compressed byte strings. Each
leaf page contains a sorted sequence of terms with their associated doclists.

**Doclist format:** For each term, the doclist is a sequence of:
- Varint-encoded docid deltas (difference from previous docid)
- For each docid, a position list: column number + offset pairs
- Position lists are varint-encoded with column number deltas and offset deltas

**Segment merge:** Merging reads from multiple input segments, deduplicates
docids, and writes a new output segment. The merge process is incremental
and can be performed during queries (auto-merge) or explicitly via
`INSERT INTO fts_table(fts_table) VALUES('merge=N')` where N is the number
of pages to merge.

#### 14.2.4 Query Syntax

FTS5 queries are passed as the right-hand side of the MATCH operator:

```sql
SELECT * FROM docs WHERE docs MATCH 'search terms';
SELECT * FROM docs('search terms');  -- shorthand
```

Query language:
- **Implicit AND:** `word1 word2` matches documents containing both words
- **OR:** `word1 OR word2`
- **NOT:** `word1 NOT word2` (binary operator only — matches documents
  containing word1 but not word2; unlike FTS3/4, unary `NOT word1` is a
  syntax error in FTS5; see `fts5parse.y` where NOT is `%left` with
  production `expr NOT expr`)
- **Phrase:** `"exact phrase"` matches consecutive tokens
- **Prefix:** `pref*` matches any token starting with "pref"
- **NEAR:** `NEAR(word1 word2, 10)` matches when word1 and word2 appear
  within 10 tokens of each other
- **Column filter:** `title : search` restricts search to the title column
- **Caret initial token:** `^word` matches word only at the start of a column
- **Grouping:** Parentheses for complex boolean expressions

#### 14.2.5 Ranking and Auxiliary Functions

**Built-in ranking:** BM25 (Okapi BM25). Automatically available as a
ranking function:
```sql
SELECT *, rank FROM docs WHERE docs MATCH 'query' ORDER BY rank;
-- rank is automatically BM25 score (lower = better match)
```

**Custom ranking functions** are registered via:
```rust
db.create_fts5_function("my_rank", my_ranking_function)?;
```

**Built-in auxiliary functions:**
- `highlight(fts_table, col_idx, open_tag, close_tag)` -- returns text with
  matching tokens wrapped in open/close tags
- `snippet(fts_table, col_idx, open_tag, close_tag, ellipsis, max_tokens)` --
  returns a short snippet around matching tokens
- `bm25(fts_table, w1, w2, ...)` -- BM25 score with per-column weights

#### 14.2.6 Content Tables

**Internal content:** FTS5 stores its own copy of the content (default).

**External content:** `content=table_name` references an external table.
FTS5 does not store document text. The external table must be kept in sync
manually (using triggers or explicit management).

**Contentless:** `content=''` stores no content at all. Only the inverted
index is maintained. `highlight()` and `snippet()` are not available.
Useful for pure search-and-retrieve-rowid workloads.

**Contentless-delete (SQLite 3.43+):** `content='' content_rowid=id` with
`contentless_delete=1`. Like contentless but supports DELETE operations,
maintaining a delete-marker tombstone in the index.

**fts5vocab:** Shadow virtual table for inspecting the FTS5 index vocabulary:
```sql
CREATE VIRTUAL TABLE vocab USING fts5vocab(docs, 'row');    -- per-row stats
CREATE VIRTUAL TABLE vocab USING fts5vocab(docs, 'col');    -- per-column stats
CREATE VIRTUAL TABLE vocab USING fts5vocab(docs, 'instance'); -- every occurrence
```
Columns: `term`, `doc` (document count), `cnt` (total occurrences),
`col` (column name, for 'col'/'instance' types).

#### 14.2.7 Configuration Options

FTS5 configuration is modified via special INSERT commands:

```sql
-- Merge control
INSERT INTO docs(docs) VALUES('merge=500');      -- merge up to 500 pages
INSERT INTO docs(docs) VALUES('automerge=8');     -- auto-merge threshold (2-16, default 4)
INSERT INTO docs(docs) VALUES('crisismerge=16');  -- crisis merge threshold (default 2× automerge)
INSERT INTO docs(docs) VALUES('usermerge=4');     -- manual merge segment count

-- Storage tuning
INSERT INTO docs(docs) VALUES('pgsz=4096');       -- leaf page size in bytes (default 1000)
INSERT INTO docs(docs) VALUES('hashsize=131072'); -- hash table size for pending terms (default 1MB)

-- Maintenance
INSERT INTO docs(docs) VALUES('rebuild');          -- rebuild entire index from content
INSERT INTO docs(docs) VALUES('optimize');         -- merge all segments into one
INSERT INTO docs(docs) VALUES('integrity-check'); -- verify index integrity
INSERT INTO docs(docs) VALUES('delete-all');       -- delete all entries
```

**secure-delete (SQLite 3.44+):** `INSERT INTO docs(docs) VALUES('secure-delete=1')`
causes DELETE operations to physically remove content from the index (not just
mark as deleted), preventing deleted content from appearing in `integrity-check`
or being recoverable from the database file.

### 14.3 FTS3/FTS4 (`fsqlite-ext-fts3`)

FTS3 and FTS4 are the predecessors to FTS5. They share an implementation
crate because FTS4 is a backward-compatible extension of FTS3.

**Key differences from FTS5:**
- FTS3/4 uses a different segment structure (B-tree based, not LSM-like)
- Query syntax differs: AND is explicit, not implicit
- FTS4 adds `matchinfo()`, `offsets()`, `content=` tables, `compress=`/`uncompress=`
- FTS3/4 uses `SELECT ... WHERE column MATCH 'query'` (column-level match)
  vs FTS5's table-level match

**matchinfo(X, FORMAT)** returns a blob of 32-bit unsigned integers encoding
match statistics. FORMAT string controls what is included:
- `p`: Number of matchable phrases
- `c`: Number of user-defined columns
- `n`: Number of rows in the FTS table
- `a`: Average number of tokens per column per row
- `l`: Length of the current row in tokens per column
- `s`: Longest common subsequence of phrase tokens
- `x`: 3 values per phrase/column pair: hits in this row, hits in all rows,
  number of rows containing hits

**offsets(X)** returns a text string listing the byte offsets of all matches:
`"col_num term_num byte_offset byte_length col_num term_num ..."`.

**compress/uncompress (FTS4 only):** Custom compression functions for stored
content: `CREATE VIRTUAL TABLE t USING fts4(content, compress=zlib_compress, uncompress=zlib_uncompress)`.

### 14.4 R*-Tree (`fsqlite-ext-rtree`)

R*-Tree (Beckmann et al., SIGMOD 1990) provides efficient spatial indexing
for multi-dimensional data. SQLite uses the R*-tree variant, not the original
R-tree of Guttman (1984).

```sql
CREATE VIRTUAL TABLE demo_index USING rtree(
  id,              -- integer primary key
  minX, maxX,      -- first dimension bounds
  minY, maxY       -- second dimension bounds
  -- up to 5 dimensions (10 coordinate columns)
);
```

**Dimension limits:** 1 to 5 dimensions (2 to 10 coordinate columns).
Coordinates are stored as 32-bit floats by default. Use `rtree_i32` for
32-bit integers instead.

**Query types:**
```sql
-- Range query: find all entries overlapping a bounding box
SELECT * FROM demo_index WHERE minX <= 100 AND maxX >= 50
                           AND minY <= 200 AND maxY >= 100;

-- Custom geometry callback
SELECT * FROM demo_index WHERE id MATCH my_geometry(50, 100, 30);
```

**Custom geometry callbacks** implement the `RtreeGeometry` trait:
```rust
pub trait RtreeGeometry: Send + Sync {
    fn query_func(&self, bbox: &[f64]) -> Result<RtreeQueryResult>;
    // Returns: Include, Exclude, or PartiallyContained
}
```

The R-tree query engine calls the geometry callback for each node in the
tree during descent, pruning branches where the callback returns `Exclude`.

**Geopoly extension:** Built on top of R*-tree, provides polygon operations:
- `geopoly_overlap(P1, P2)` -- test if two polygons overlap
- `geopoly_within(P1, P2)` -- test if P1 is within P2
- `geopoly_area(P)` -- compute polygon area
- `geopoly_blob(P)` -- convert GeoJSON to internal binary format
- `geopoly_json(P)` -- convert internal format to GeoJSON
- `geopoly_svg(P)` -- render polygon as SVG path
- `geopoly_bbox(P)` -- bounding box of polygon
- `geopoly_contains_point(P, X, Y)` -- point-in-polygon test
- `geopoly_group_bbox(P)` -- aggregate bounding box
- `geopoly_regular(X, Y, R, N)` -- regular N-gon at center (X,Y) radius R
- `geopoly_ccw(P)` -- ensure counter-clockwise winding
- `geopoly_xform(P, A, B, C, D, E, F)` -- affine transformation

Polygons are stored as binary blobs in the format: 4-byte header (type +
vertex count) followed by pairs of 32-bit float coordinates.

### 14.5 Session (`fsqlite-ext-session`)

The Session extension records changes to a database and represents them
as changesets or patchsets that can be applied to other databases.

#### 14.5.1 Changeset Format

A changeset is a binary blob with the following layout:
```
For each modified table:
  'T' byte (0x54)
  Number of columns (varint)
  For each column: 0x00 (not part of PK) or 0x01 (part of PK)
  Table name (nul-terminated string)

  For each changed row:
    Operation byte: SQLITE_INSERT (18), SQLITE_DELETE (9), SQLITE_UPDATE (23)

    For DELETE:
      Old values: one value per column (serial-type encoded)

    For INSERT:
      New values: one value per column (serial-type encoded)

    For UPDATE:
      Old values: one per column (undefined for non-PK columns that didn't change)
      New values: one per column (undefined for columns that didn't change)
```

Each value is encoded as: a single type byte (0x00=undefined, 0x01=integer,
0x02=real, 0x03=text, 0x04=blob, 0x05=null) followed by the value data
(varint-length-prefixed for text and blob, 8-byte big-endian for integer
and real).

#### 14.5.2 Conflict Resolution

When applying a changeset, conflicts are resolved via a callback:
```rust
pub enum ConflictAction {
    OmitChange,     // skip this change
    Replace,        // overwrite conflicting row
    Abort,          // abort the entire apply operation
}

pub enum ConflictType {
    Data,           // row exists but values differ from expected
    NotFound,       // row to update/delete does not exist
    Conflict,       // unique constraint violation
    Constraint,     // other constraint violation
    ForeignKey,     // foreign key constraint
}
```

#### 14.5.3 Patchset Differences

A patchset is a more compact format that omits the old values for UPDATE
operations (only stores new values and PK). Patchsets cannot detect
conflicts as precisely as changesets (cannot verify that the old row matched)
but are significantly smaller for tables with many columns.

### 14.6 ICU (`fsqlite-ext-icu`)

The ICU extension provides Unicode-aware string operations.

**Collation creation:**
```sql
SELECT icu_load_collation('de_DE', 'german');
-- Now: SELECT * FROM t ORDER BY name COLLATE german;
```

This creates a collation from an ICU locale identifier. The collation
uses ICU's `ucol_strcoll` for comparison, providing linguistically
correct sort order for the specified language.

**Case folding:** `icu_upper(X, LOCALE)` and `icu_lower(X, LOCALE)` provide
locale-aware case conversion (unlike the built-in `upper`/`lower` which
handle ASCII only).

**FTS tokenizer integration:** The ICU tokenizer `icu` can be used with
FTS3/4/5 for language-aware word breaking:
```sql
CREATE VIRTUAL TABLE docs USING fts5(body, tokenize='icu zh_CN');
```

This uses ICU's `UBreakIterator` with word-break rules appropriate for
the specified locale, which is critical for CJK languages where words
are not delimited by spaces.

### 14.7 Miscellaneous (`fsqlite-ext-misc`)

**generate_series(START, STOP [, STEP])** -> virtual table. Generates a
sequence of integers from START to STOP with optional STEP (default 1).
Columns: `value`, `start`, `stop`, `step`. Commonly used in joins:
```sql
SELECT value FROM generate_series(1, 100);
SELECT date(d.value) FROM generate_series(
  unixepoch('2024-01-01'), unixepoch('2024-12-31'), 86400
) AS d;
```

**dbstat** -> virtual table. Reports B-tree page usage statistics:
```sql
SELECT name, path, pageno, pagetype, ncell, payload, unused, mx_payload
  FROM dbstat WHERE aggregate=FALSE;
```
Columns provide per-page details: page number, type (leaf/internal), number
of cells, total payload bytes, unused bytes, maximum cell payload. The
`aggregate` hidden column controls whether to show per-page or per-table
aggregated statistics.

**dbpage** -> virtual table. Provides direct read/write access to database
pages:
```sql
SELECT data FROM dbpage WHERE pgno = 1;  -- read page 1
UPDATE dbpage SET data = X'...' WHERE pgno = 5;  -- write page 5 (dangerous!)
```

**csv** -> virtual table. Reads CSV files as virtual tables:
```sql
CREATE VIRTUAL TABLE temp.csv_data USING csv(
  filename='data.csv',
  header=YES,
  columns=4
);
```

**decimal** -> extension for arbitrary-precision decimal arithmetic:
- `decimal(X)` -- convert to decimal text representation
- `decimal_add(X, Y)`, `decimal_sub(X, Y)`, `decimal_mul(X, Y)` --
  arbitrary precision arithmetic
- `decimal_sum(X)` -- aggregate sum with arbitrary precision
- `decimal_cmp(X, Y)` -- comparison returning -1, 0, or +1

Decimal values are represented internally as strings to avoid floating-point
precision loss. This is useful for financial calculations.

**uuid** -> UUID generation functions:
- `uuid()` -- generate random UUID v4
- `uuid_str(X)` -- convert UUID blob to string representation
- `uuid_blob(X)` -- convert UUID string to 16-byte blob

---

## 15. Exclusions (What We Are NOT Building)

FrankenSQLite deliberately excludes the following components. Each exclusion
has a technical rationale; none are omitted from laziness.

**Amalgamation build system.** The C SQLite amalgamation (`sqlite3.c`) is a
single-file build artifact produced by concatenating ~150 source files. Its
purpose is simplifying C compilation. Rust's Cargo workspace with 23 crates
provides superior modularity, parallel compilation, and dependency tracking.
There is no analog of the amalgamation in a Rust project.

**TCL test harness.** C SQLite's test suite is driven by TCL scripts
(~90,000+ lines). These scripts are deeply intertwined with the C API
(`sqlite3_exec`, `sqlite3_step`, etc.) and cannot be meaningfully ported.
Instead, FrankenSQLite uses: (1) native Rust `#[test]` modules, (2) proptest
for property-based testing, (3) the conformance harness that compares SQL
output against C sqlite3 golden files, and (4) asupersync's lab reactor
for deterministic concurrency tests. This strategy provides equivalent or
superior coverage without the TCL dependency.

**LEMON parser generator.** C SQLite uses a custom LALR(1) parser generator
called LEMON to produce `parse.c` from `parse.y`. FrankenSQLite uses a
hand-written recursive descent parser with Pratt precedence for expressions.
Rationale: better error messages with precise source span reporting,
simpler maintenance, no build-time code generation step, and the `parse.y`
grammar serves as an authoritative reference even without LEMON.

**Loadable extension API (.so/.dll).** C SQLite supports dynamically loading
extensions via `sqlite3_load_extension()`. This requires a C-compatible ABI
and `dlopen`/`LoadLibrary` calls. FrankenSQLite instead compiles all
extensions directly into the binary, controlled by Cargo features. This
eliminates an entire class of security vulnerabilities (arbitrary code
loading) and simplifies deployment. Users who need custom extensions implement
Rust traits and recompile.

**Legacy file format quirks (schema format < 4).** Schema format number 4
has been the default since SQLite 3.3.0 (2006). Formats 1-3 have minor
differences in how DESC indexes and boolean handling work. Supporting these
would add complexity for a format that no actively maintained database uses.
FrankenSQLite requires schema format 4 and rejects databases with older formats
with a clear error message.

**Obsolete VFS implementations.** C SQLite ships VFS backends for OS/2,
VxWorks, Windows CE, and other legacy platforms. FrankenSQLite provides
`UnixVfs` (POSIX), `WindowsVfs` (Win32), and `MemoryVfs` (in-memory).
Other platforms can be supported via the `Vfs` trait.

**Shared-cache mode.** C SQLite's shared-cache mode allows multiple
connections within the same process to share a single page cache and use
table-level locking. It has been deprecated since SQLite 3.41.0 (2023) and
is widely considered a source of subtle bugs. FrankenSQLite's MVCC system
supersedes shared-cache entirely: multiple connections within a process
share the MVCC version chains and benefit from page-level concurrency, which
is strictly superior.

**NOTE:** `WindowsVfs` is NOT an exclusion -- it is in-scope (listed under
§15 for completeness of the VFS discussion). Windows file locking uses
`LockFileEx`/`UnlockFileEx` instead of `fcntl`, and shared memory uses
`CreateFileMapping` instead of `mmap`. `WindowsVfs` implements the same
`Vfs` trait as `UnixVfs`. Platform-specific code is isolated behind
`#[cfg(target_os)]` gates.

**Multiplexor VFS.** C SQLite's multiplexor shards large databases across
multiple files to work around filesystem limitations (e.g., FAT32 4GB limit).
Modern filesystems do not have these limitations. Excluded.

**SEE (SQLite Encryption Extension).** C SQLite's commercial encryption
extension is not ported. Instead, FrankenSQLite provides page-level
encryption using the reserved-space-per-page field in the database header:
- **Envelope encryption (DEK/KEK):**
  - On database creation, generate a random 256-bit **Data Encryption Key**
    `DEK` (requires `Cx` random capability).
  - `PRAGMA key = 'passphrase'` derives a **Key Encryption Key** `KEK` via
    Argon2id with a per-database random salt and explicit parameters recorded in
    metadata.
  - Store `wrap(DEK, KEK)` as durable metadata:
    - Native mode: in ECS metadata (e.g., `RootManifest`-reachable object).
    - Compatibility mode: in the `.fsqlite/` sidecar directory (SQLite file
      format is not a crypto keystore; do not overload unrelated header bytes).
  - **Instant rekey (O(1)):** `PRAGMA rekey = 'new_passphrase'` re-derives `KEK'`
    and rewrites only `wrap(DEK, KEK')`. Bulk page data is not re-encrypted.
  - **Transitioning from Plaintext:** Enabling encryption on an existing database
    (`PRAGMA key = ...` where none existed) requires `reserved_bytes >= 40`.
    Standard SQLite databases have 0 reserved bytes. Therefore, the first encryption
    enablement MUST trigger a full `VACUUM` to rewrite pages with the new layout.
    Subsequent rekeys are O(1).

- **Page algorithm:** Pages are encrypted with **XChaCha20-Poly1305** using the
  `DEK` (AEAD).

- **Nonce:** A fresh 24-byte random nonce is generated for every page write.
  Random nonces eliminate global counters and remain safe under VM snapshot
  reverts, process crashes, forks, and distributed writers. Collision
  probability is negligible at any realistic write volume.

- **Storage in reserved bytes:** The per-page nonce (24B) and Poly1305 tag (16B)
  are stored in the page reserved space (requires `reserved_bytes >= 40`).

- **AAD (swap resistance):** AEAD additional authenticated data MUST include
  `(page_number, database_id, page_type_tag)` so ciphertext cannot be replayed or
  swapped across pages or databases without detection.

- **Key management API:** Retain the familiar SQLite-style API surface:
  `PRAGMA key` / `PRAGMA rekey`. The underlying scheme is not SEE-compatible
  byte-for-byte; it is compatible at the SQL interface level.

- **Encrypt-then-code:** Encryption is orthogonal to ECS: encrypted pages are
  encoded as ECS symbols with encryption applied before RaptorQ encoding
  (encrypt-then-code).

---

## 16. Implementation Phases

### Phase 1: Bootstrap and Spec Extraction [COMPLETE]

**Deliverables:**
- `Cargo.toml` workspace root with 23 crate entries
- `crates/fsqlite-types/src/lib.rs`: `PageNumber` (NonZeroU32), `SqliteValue`
  enum (Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>)),
  `Opcode` enum (190+ variants), limits module (`SQLITE_MAX_LENGTH`,
  `SQLITE_MAX_SQL_LENGTH`, etc.), serial type encoding/decoding, bitflags
- `crates/fsqlite-error/src/lib.rs`: `FrankenError` enum (~40 variants
  mapping to SQLite error codes), `ErrorCode` constants, `Display`/`Error`
  impls, conversion from `std::io::Error`
- Spec documents: `AGENTS.md`, `PROPOSED_ARCHITECTURE.md`,
  `PLAN_TO_PORT_SQLITE_TO_RUST.md`, `EXISTING_SQLITE_STRUCTURE.md`

**Acceptance criteria:**
- `cargo check --workspace` passes with zero errors
- `cargo clippy --workspace --all-targets -- -D warnings` passes
- 77 tests all green covering: SqliteValue type conversions, PageNumber
  construction (reject zero), all Opcode display names, limit constant
  values matching C SQLite, serial type round-trip for all type categories
- Every error variant has a distinct ErrorCode and meaningful Display output
- Conformance harness infrastructure: Oracle runner can execute SQL against
  C SQLite and capture results in JSON fixture format (Section 17.7)
- At least 10 basic conformance fixtures captured from Oracle

**Dependencies:** None (first phase).

**Risk areas:** Getting the Opcode enum right -- there are 190+ opcodes and
their numeric values must match C SQLite for EXPLAIN output compatibility.
Mitigation: extract opcode list mechanically from `opcodes.h`.

**Estimated complexity:** ~3,000 LOC across fsqlite-types, fsqlite-error,
and fsqlite-harness bootstrap.

### Phase 2: Core Types and Storage Foundation [IN PROGRESS]

**Deliverables:**
- `crates/fsqlite-vfs/src/lib.rs`: `Vfs` and `VfsFile` traits
- `crates/fsqlite-vfs/src/memory.rs`: `MemoryVfs` implementation (in-memory
  file system with `HashMap<String, Arc<Mutex<Vec<u8>>>>`)
- `crates/fsqlite-types/src/record.rs`: Record format serialization and
  deserialization (varint header, serial types, data payload)
- `crates/fsqlite-vfs/src/unix.rs`: `UnixVfs` with POSIX file operations and
  `fcntl`-based locking (5-level: NONE, SHARED, RESERVED, PENDING, EXCLUSIVE)

**Acceptance criteria:**
- MemoryVfs: create file, write, read-back, truncate, file_size all correct
- MemoryVfs: concurrent read/write from multiple threads (using Arc clone)
- Record format: encode/decode round-trip for NULL, integers (all 6 sizes),
  float, text, blob, constant 0, constant 1
- Record format: proptest with arbitrary SqliteValue vectors up to 100 columns
- Record format: edge case -- empty record (zero columns), single NULL column,
  maximum-size text (1GB), varint boundary values (127, 128, 16383, 16384)
- UnixVfs: create/open/read/write/delete on real filesystem via tempfile
- UnixVfs: lock escalation NONE -> SHARED -> RESERVED -> EXCLUSIVE
- UnixVfs: two processes cannot both hold EXCLUSIVE (test via fork or separate
  process spawn)
- Target: 200+ tests

**Dependencies:** Phase 1 complete.

**Risk areas:** Unix file locking semantics are notoriously tricky. POSIX
`fcntl` locks are per-process (not per-file-descriptor), meaning two fds
to the same file in the same process share locks. SQLite works around this
with a global lock table (`unixInodeInfo`). We need an equivalent.

**Estimated complexity:** ~4,000 LOC across fsqlite-vfs and fsqlite-types.

### Phase 3: B-Tree and SQL Parser

**Deliverables:**
- `crates/fsqlite-btree/src/cursor.rs`: `BtCursor` with page-stack
  traversal (max depth 20 for 4KB pages; with interior page fanout
  ~300-400 for table B-trees, capacity vastly exceeds any practical
  database size even at depth 5-6)
- `crates/fsqlite-btree/src/cell.rs`: Cell parsing for all 4 page types
  (INTKEY table leaf/interior, BLOBKEY index leaf/interior), overflow
  detection, local payload calculation
- `crates/fsqlite-btree/src/balance.rs`: Page splitting algorithms --
  `balance_nonroot` (redistribute cells among siblings, typically 3-way
  split), `balance_deeper` (root overflow, increase tree depth by 1)
- `crates/fsqlite-btree/src/overflow.rs`: Overflow page chain read/write,
  following chain links, allocating overflow pages from freelist
- `crates/fsqlite-btree/src/freelist.rs`: Trunk + leaf freelist page
  management, allocation (prefer leaf pages from first trunk), deallocation
- `crates/fsqlite-btree/src/payload.rs`: `BtreePayload` abstraction for
  reading across page boundaries (local + overflow)
- `crates/fsqlite-ast/src/lib.rs`: Complete AST type hierarchy --
  `Statement`, `SelectStatement`, `InsertStatement`, `UpdateStatement`,
  `DeleteStatement`, `CreateTableStatement`, `Expr`, `JoinClause`,
  `OrderingTerm`, `WindowDefn`, etc.
- `crates/fsqlite-parser/src/lexer.rs`: Token enum, memchr-accelerated
  scanning for string literals and comments, keyword classification
- `crates/fsqlite-parser/src/parser.rs`: Recursive descent with Pratt
  precedence for expressions, all statement types from Section 12
- `crates/fsqlite-parser/src/keyword.rs`: Perfect hash (or PHF crate) for
  150+ SQL keywords with O(1) lookup

**Acceptance criteria:**
- B-tree: Insert 10,000 random i64 keys, verify all retrievable via cursor
- B-tree: Insert 10,000 sequential keys, delete 5,000 random subset, verify
  remaining 5,000 present and in order
- B-tree: Insert keys that force overflow pages (payload > page_size/4),
  verify read-back
- B-tree: Insert/delete pattern that causes tree depth to increase to 3 and
  then decrease back to 2
- B-tree: Freelist correctly tracks freed pages and reuses them on insert
- B-tree: Proptest -- random mix of insert/delete/lookup operations,
  invariant: cursor iteration always returns keys in sorted order
- Parser: Parse all statement types from Section 12 (at least one test per
  subsection)
- Parser: Expression precedence: `1 + 2 * 3` parses as `1 + (2 * 3)`
- Parser: All join types, CTE syntax, window function syntax
- Parser: Round-trip property test: parse -> pretty-print -> re-parse
  produces identical AST for 1000 generated SQL statements
- Parser: Error recovery: invalid SQL produces error with line:column span
- Parser: Keywords as identifiers in non-reserved positions (e.g., column
  named `order` in `SELECT "order" FROM t`)
- Target: 500+ tests

**Dependencies:** Phase 2 complete (B-tree depends on VFS for page I/O,
parser depends on types for AST nodes).

**Risk areas:**
- B-tree balance is the most algorithmically complex code in SQLite.
  `balance_nonroot` alone is ~800 lines of C (lines 8230-9033 in btree.c). Incorrect balancing causes
  silent data corruption. Mitigation: extensive proptest with invariant
  checking after every operation (cell count, key ordering, child pointers,
  freespace accounting).
- Parser completeness: SQLite's grammar has many context-sensitive corners
  (e.g., `REPLACE` is both a keyword and a function name). Mitigation: use
  `parse.y` as the authoritative reference, test every production.

**Estimated complexity:** ~12,000 LOC (btree: 5,000, parser: 4,000, ast: 3,000).

### Phase 4: VDBE and Query Pipeline

**Deliverables:**
- `crates/fsqlite-vdbe/src/engine.rs`: Fetch-execute loop, match-based
  opcode dispatch, register file (Vec<Mem>)
- `crates/fsqlite-vdbe/src/mem.rs`: `Mem` type (SQLite's runtime value with
  type, affinity, encoding), comparison with collation, arithmetic
- `crates/fsqlite-vdbe/src/opcodes/`: Implementation modules for the 50+
  critical opcodes: Init, Goto, Halt, Integer, String8, Null, Blob,
  ResultRow, MakeRecord, Column, Rowid, OpenRead, OpenWrite, Rewind, Next,
  Prev, SeekGE, SeekGT, SeekLE, SeekLT, Found, NotFound, Insert, Delete,
  NewRowid, IdxInsert, IdxDelete, Transaction, AutoCommit, CreateBtree,
  Destroy, Clear, Noop, Explain, TableLock, ReadCookie, SetCookie, etc.
- `crates/fsqlite-vdbe/src/sorter.rs`: External merge sort for ORDER BY
- `crates/fsqlite-planner/src/resolve.rs`: Name resolution (table/column
  binding, `*` expansion, alias resolution)
- `crates/fsqlite-planner/src/codegen.rs`: AST-to-VDBE code generation for
  SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
- `crates/fsqlite-core/src/connection.rs`: Connection state, schema cache,
  prepared statement management
- `crates/fsqlite/src/lib.rs`: Public API: `Connection::open()`,
  `connection.prepare()`, `stmt.execute()`, `stmt.query()`, `Row`, etc.

**Acceptance criteria:**
- End-to-end: `CREATE TABLE t(a INTEGER, b TEXT); INSERT INTO t VALUES(1,'hello'); SELECT * FROM t;` returns `[(1, "hello")]`
- End-to-end: `SELECT 1+2, 'abc'||'def', typeof(3.14)` returns `[(3, "abcdef", "real")]`
- End-to-end: INSERT with multiple rows, SELECT with WHERE, ORDER BY, LIMIT
- End-to-end: UPDATE with SET and WHERE, verify changed rows
- End-to-end: DELETE with WHERE, verify deleted rows gone
- End-to-end: EXPLAIN produces correct opcode listing
- VDBE: All comparison operators with type affinity coercion
- VDBE: NULL handling (NULL = NULL is NULL, NULL IS NULL is true)
- VDBE: CASE expression evaluation
- VDBE: Subquery (EXISTS, IN, scalar subquery)
- Sorter: ORDER BY correctly sorts 100,000 rows in-memory, correctly
  spills to disk for 1,000,000 rows
- Target: 1,000+ tests

**Dependencies:** Phase 3 complete (VDBE needs btree for storage, codegen
needs parser for AST).

**Risk areas:** Codegen is the glue layer where parser output meets VDBE
input. Getting register allocation right is subtle (SQLite uses a complex
register assignment algorithm to minimize register pressure). Mitigation:
start with naive one-register-per-expression, optimize later.

**Estimated complexity:** ~18,000 LOC (vdbe: 8,000, planner: 4,000,
core: 3,000, public api: 1,000, func: 2,000).

### Phase 5: Persistence, WAL, and Transactions

**Deliverables:**
- `crates/fsqlite-pager/src/pager.rs`: Pager state machine (OPEN, READER,
  WRITER, SYNCED, ERROR), journal/WAL mode switching
- `crates/fsqlite-pager/src/journal.rs`: Rollback journal (hot journal
  detection, playback on recovery)
- `crates/fsqlite-wal/src/wal.rs`: WAL file creation, frame append, frame
  read, checksum computation (SQLite's custom algorithm)
- `crates/fsqlite-wal/src/index.rs`: WAL index (shared memory hash table
  for page-to-frame lookup)
- `crates/fsqlite-wal/src/checkpoint.rs`: PASSIVE, FULL, RESTART, TRUNCATE
  checkpoint modes
- `crates/fsqlite-wal/src/recovery.rs`: WAL recovery on open (detect valid
  frames by checksum chain, discard torn tail)
- `crates/fsqlite-wal/src/raptorq.rs`: Self-healing WAL with RaptorQ repair
  symbols (Section 3.4.1)
- Transaction support: BEGIN/COMMIT/ROLLBACK, savepoint stack
- Page-level encryption (§15, replacing SEE): XChaCha20-Poly1305 with envelope
  DEK/KEK, Argon2id key derivation, nonce/tag in reserved bytes, AAD swap
  resistance, PRAGMA key/rekey API

**Acceptance criteria:**
- Persistence: Create table, insert data, close connection, reopen, data
  present
- Journal mode: Write data, simulate crash (truncate mid-write), reopen,
  hot journal detection and playback, data consistent
- WAL mode: Multiple readers concurrent with one writer, readers see
  consistent snapshots
- WAL checksum: Corrupt one byte of a frame, verify checksum detects it
- WAL recovery: Append 100 frames, truncate last frame (simulate torn write),
  recovery discards torn frame, prior 99 frames intact
- RaptorQ WAL: Append 10 frames + 2 repair symbols, corrupt 2 source frames,
  verify recovery reconstructs all 10 frames
- Checkpoint: Verify all 4 modes move frames back to database file correctly
- Savepoints: SAVEPOINT, RELEASE, ROLLBACK TO with nested savepoints
- Round-trip: Create database with FrankenSQLite, read with C sqlite3 (and
  vice versa), verify data integrity
- Encryption: PRAGMA key creates encrypted database, data unreadable without
  key, PRAGMA rekey changes passphrase without re-encrypting pages, AAD
  prevents page swaps across databases
- Target: 1,500+ tests

**Dependencies:** Phase 4 complete (persistence needs pager under VDBE).

**Risk areas:** WAL checksum compatibility is critical for file format
interop. The checksum algorithm is non-standard and byte-order-dependent.
Mitigation: generate test WAL files with C SQLite and verify FrankenSQLite
reads them correctly. Encryption nonce management must be correct under
concurrent writers and crash recovery.

**Estimated complexity:** ~12,000 LOC (pager: 3,000, wal: 5,000,
raptorq integration: 2,000, encryption: 2,000).

### Phase 6: MVCC Concurrent Writers with SSI

**Deliverables:**
- `crates/fsqlite-mvcc/src/txn.rs`: Transaction type with TxnId, Snapshot,
  TxnEpoch, write_set, intent_log, page_locks, mode (Serialized/Concurrent),
  witness-key sets (read_keys/write_keys), SSI state (has_in_rw/has_out_rw)
- `crates/fsqlite-mvcc/src/snapshot.rs`: Snapshot capture (`high = commit_seq at BEGIN`,
  `schema_epoch` at BEGIN), visibility predicate (`commit_seq <= snapshot.high`)
- `crates/fsqlite-mvcc/src/version_chain.rs`: Page version chains, GF(256)
  delta encoding via RaptorQ (Section 3.4.4)
- `crates/fsqlite-mvcc/src/lock_table.rs`: Sharded PageLockTable (64 shards)
  with try_acquire (non-blocking) and release_all
- `crates/fsqlite-mvcc/src/witness_plane.rs`: SSI witness plane integration:
  witness-key registration (`register_read`/`register_write`), shared-memory
  `HotWitnessIndex` updates, cold-plane witness object emission (`ReadWitness`,
  `WriteWitness`, `WitnessDelta`) and index-segment compaction hooks
- `crates/fsqlite-mvcc/src/ssi.rs`: SSI validation on top of the witness plane
  (conservative pivot abort rule with optional refinement + merge), plus
  `DependencyEdge` / `CommitProof` / `AbortWitness` artifacts for explainability
- `crates/fsqlite-mvcc/src/conflict.rs`: First-committer-wins validation,
  merge policy ladder (Section 5.10): deterministic rebase via intent logs and
  structured page patch merge; explicit prohibition of raw byte-disjoint XOR
  merge for SQLite structured pages (§3.4.5)
- `crates/fsqlite-mvcc/src/gc.rs`: Garbage collection -- horizon computation
  (min active begin_seq), version chain trimming, witness-plane GC horizons and
  bucket epoch advance (§5.6.4.8), memory bound enforcement
- `crates/fsqlite-mvcc/src/coordinator.rs`: Write coordinator using
  asupersync two-phase MPSC channel, commit serialization for WAL append
- `crates/fsqlite-pager/src/cache.rs`: ARC cache with (PageNumber, CommitSeq)
  keys, MVCC-aware eviction constraints (pinned, dirty, superseded). Lives in
  fsqlite-pager (L2) because the MvccPager trait is defined there; CommitSeq
  is imported from fsqlite-types.
- `crates/fsqlite-pager/src/mvcc_pager.rs`: MvccPager trait definition;
  implementation in fsqlite-mvcc (L3) bridges B-tree layer to MVCC, Cx threading

**Acceptance criteria:**
- Serialized mode: Exact C SQLite behavior -- single writer, SERIALIZABLE
  isolation, `BEGIN IMMEDIATE` blocks other writers
- Concurrent mode: Two transactions writing to different pages both commit
  successfully
- Concurrent mode: Two transactions writing to the same page with a
  non-mergeable conflict, second committer gets `SQLITE_BUSY_SNAPSHOT`
- Concurrent mode: 100 threads each insert 100 rows into separate rowid
  ranges, all 10,000 rows present after all commits
- Snapshot isolation: Long-running reader (started before writer) does not
  see writer's changes even after writer commits
- Snapshot isolation: Reader started after writer commits sees all changes
- Merge safety: SQLite structured pages MUST NOT be merged by raw byte-range
  XOR; include a regression test for the B-tree lost-update counterexample
  (cell move/defrag vs update at old offset) that must abort or resolve
  semantically (never a silent lost update)
- GC: Sustained write load of 1,000 transactions, memory usage bounded by
  O(active_transactions * pages_per_transaction), not O(total_transactions)
- GC: Version chain length never exceeds active transaction count + 1
- Version chain compression: Pages with small diffs (< 10% changed) use
  sparse XOR delta encoding, space savings > 80%
- SSI: Write skew pattern (two txns read overlapping data, write disjoint
  pages based on reads) -- at least one txn aborted under default mode
- SSI: PRAGMA fsqlite.serializable=OFF allows both to commit (SI mode)
- SSI: has_in_rw/has_out_rw flags correctly set for known rw-antidependency
  patterns
- Rebase merge: Two transactions insert distinct keys into the same leaf
  page -- rebase succeeds, both commit
- Rebase merge: Two transactions update the same key -- rebase fails,
  second committer aborts
- Roaring Bitmap: Visibility checks with 100 in-flight transactions have
  zero false positives (exact, not probabilistic)
- ARC cache: Sequential scan does not evict frequently-accessed index pages
  (ARC adaptation test)
- Lab runtime: All above tests run under deterministic scheduling with
  same results across 100 different seeds
- Mazurkiewicz traces: 3-transaction scenario (T1 writes page A, T2 writes
  page B, T3 writes both A and B) -- all 6 possible commit orderings
  verified for correct conflict detection
- E-process monitors: INV-1 through INV-7 monitored continuously during
  100-thread stress test, zero violations
- Target: 2,000+ tests

**Dependencies:** Phase 5 complete (MVCC sits atop WAL and pager).

**Risk areas:** This is the hardest phase. Specific risks:
- Snapshot capture must be atomic with respect to concurrent commits.
  A non-atomic snapshot can miss a commit, violating SI. Mitigation: hold
  a read lock on active_transactions during snapshot capture.
- GC must not reclaim versions needed by any active transaction. Mitigation:
  formal proof in Section 5.5, e-process monitoring at runtime.
- Merge ladder correctness (intent replay + structured patches) is subtle:
  a naive byte-range merge can silently lose writes on B-tree pages.
  Mitigation: explicit counterexample tests + proptest/DPOR asserting that
  any accepted merge is observationally equivalent to some serial ordering
  of the intent ops, and passes integrity_check post-commit.
- ARC cache interaction with MVCC versioning adds complexity to eviction
  decisions. Mitigation: start with simple LRU, upgrade to ARC once basic
  MVCC works.

**Estimated complexity:** ~15,000 LOC.

### Phase 7: Advanced Query Planner, Full VDBE, SQL Features

**Deliverables:**
- Full WHERE optimization: index scan selection, range narrowing, OR
  optimization via temp index, LIKE prefix optimization, skip-scan
  for composite indexes with leading column not constrained
- Join ordering: cost-based with cardinality estimation from sqlite_stat1,
  greedy algorithm for > 8 tables, exhaustive search for <= 8 tables
- All 190+ VDBE opcodes implemented
- Window function execution: frame management, ROWS/RANGE/GROUPS modes,
  EXCLUDE clause, partition-by sorting
- CTE execution: materialized and non-materialized, recursive with cycle
  detection via LIMIT
- Trigger compilation and execution: BEFORE/AFTER/INSTEAD OF, OLD/NEW
  access, recursive triggers
- Foreign key enforcement: deferred and immediate checking, CASCADE actions
- View expansion and INSTEAD OF trigger routing
- ALTER TABLE: RENAME, ADD COLUMN, DROP COLUMN (with table rewrite)
- VACUUM: full database rebuild, INTO variant
- REINDEX: rebuild specified or all indexes
- ANALYZE: populate sqlite_stat1 with sample-based statistics

**Acceptance criteria:**
- Index selection: query with equality on indexed column uses index scan
  (verified via EXPLAIN QUERY PLAN)
- Index selection: query with range (BETWEEN, <, >) uses index scan with
  proper bounds
- Partial index: query with matching WHERE clause uses partial index
- Expression index: query with matching expression uses expression index
- Join ordering: 4-table join selects optimal order (smallest intermediate
  result first)
- Window functions: row_number, rank, dense_rank, lag, lead, sum OVER
  with ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING all produce correct results
- CTE: recursive CTE generating Fibonacci sequence (first 20 terms)
- Trigger: BEFORE INSERT trigger that validates data, AFTER DELETE trigger
  that logs to audit table
- Foreign keys: INSERT into child table with non-existent parent FK fails,
  CASCADE DELETE removes child rows
- VACUUM INTO: creates identical but defragmented copy
- Target: 3,000+ tests

**Dependencies:** Phase 6 complete.

**Risk areas:** The WHERE optimizer is the most complex part of the query
planner. C SQLite's `where.c` is ~7,800 lines. Cost estimation without
statistics (before ANALYZE) relies on heuristics that must match C SQLite's
behavior for conformance.

**Estimated complexity:** ~20,000 LOC.

### Phase 8: Extensions

**Deliverables:** All extensions from Section 14, each in its own crate.

**Acceptance criteria per extension:**
- JSON1: All functions from Section 14.1 with JSONB round-trip, json_each
  and json_tree virtual table queries
- FTS5: Tokenize 100K documents, full-text search with BM25 ranking,
  highlight and snippet, prefix queries
- FTS3/4: matchinfo blob format matches C SQLite output
- R*-Tree: 2D spatial index with 100K entries, range query, custom geometry
- Session: Generate changeset from modifications, apply to second database,
  verify identical content
- ICU: Create collation from locale, ORDER BY uses locale-correct sorting
- Misc: generate_series(1,1000000) performs in < 1 second

**Dependencies:** Phase 7 complete (extensions use virtual table API).

**Estimated complexity:** ~25,000 LOC.

### Phase 9: CLI, Conformance, Benchmarks, Replication

**Deliverables:**
- `crates/fsqlite-cli/`: Interactive shell using frankentui, dot-commands
  (`.tables`, `.schema`, `.mode`, `.headers`, `.import`, `.dump`), output
  modes (column, csv, json, table, markdown), tab completion, syntax
  highlighting, command history
- `crates/fsqlite-harness/`: Conformance test runner, golden file comparison
- `conformance/`: 1,000+ SQL test files with golden output from C sqlite3
- `benches/`: Criterion benchmark suite (see Section 17.8 for regression methodology)
- Fountain-coded replication: UDP-based symbol emission, receiver assembly,
  changeset application
- Snapshot shipping: full database transfer via RaptorQ encoding

**Acceptance criteria:**
- CLI: All sqlite3 dot-commands that have meaningful equivalents
- Conformance: **100% parity target** across all golden files (with any
  intentional divergences explicitly documented and annotated in the harness)
- Benchmarks: single-writer within 3x of C SQLite, multi-writer (non-
  contended) shows linear scaling up to 4 cores
- Replication: 10% packet loss, database replicates correctly within 1.2x
  of no-loss time (RaptorQ overhead)
- Target: 4,000+ tests

**Dependencies:** Phase 8 complete.

**Estimated complexity:** ~10,000 LOC.

---

## 17. Testing Strategy

### 17.1 Unit Tests (Per-Crate)

Every public function and every non-trivial private function has at least
one `#[test]`. Trait dependencies are mocked using hand-written mock
implementations (not a mocking framework) to keep tests understandable.

**Concrete test scenarios by crate:**

**fsqlite-types:**
- SqliteValue: comparison between Integer(3) and Real(3.0) returns Equal
- SqliteValue: Text("123") coerced to Integer context yields Integer(123)
- PageNumber: construction from 0 returns error
- Opcode: all 190+ variants have distinct u8 values
- Serial type: round-trip encode/decode for every serial type category

**fsqlite-vfs:**
- MemoryVfs: write 1MB, read back, verify byte-for-byte identity
- MemoryVfs: truncate from 1MB to 512KB, verify file_size and read
- UnixVfs: create in temp directory, write, close, reopen, read back
- UnixVfs: delete non-existent file returns appropriate error
- UnixVfs: two concurrent readers on same file see consistent data

**fsqlite-btree:**
- Test: insert 10K random i64 keys, delete 5K random subset, verify
  remaining 5K are all present and in sorted order via cursor iteration
- Test: insert keys forcing tree depth to 4, verify cursor traversal
  visits all keys
- Test: overflow page chain for 100KB payload, read back complete
- Test: freelist reclaims pages, verify via dbstat-equivalent accounting

### 17.2 Property-Based Tests (proptest)

**B-tree invariants:**
```rust
proptest! {
    #[test]
    fn btree_maintains_order(ops in vec(btree_op(), 0..10000)) {
        let mut tree = BTree::new(MemoryPager::new(4096));
        let mut reference = BTreeMap::new();
        for op in ops {
            match op {
                Op::Insert(k, v) => { tree.insert(k, v); reference.insert(k, v); }
                Op::Delete(k) => { tree.delete(k); reference.remove(&k); }
            }
        }
        // Invariant: cursor iteration matches reference
        let tree_entries: Vec<_> = tree.cursor().collect();
        let ref_entries: Vec<_> = reference.into_iter().collect();
        assert_eq!(tree_entries, ref_entries);
    }
}
```

**Parser round-trip:**
```rust
proptest! {
    #[test]
    fn parse_roundtrip(sql in arbitrary_select()) {
        let ast1 = parse(&sql).unwrap();
        let sql2 = ast1.to_sql_string();
        let ast2 = parse(&sql2).unwrap();
        assert_eq!(ast1, ast2);
    }
}
```

**Record format:**
```rust
proptest! {
    #[test]
    fn record_roundtrip(values in vec(arbitrary_sqlite_value(), 0..100)) {
        let encoded = encode_record(&values);
        let decoded = decode_record(&encoded);
        assert_eq!(values, decoded);
    }
}
```

**MVCC linearizability:**
```rust
proptest! {
    #[test]
    fn mvcc_snapshot_isolation(
        txns in vec(arbitrary_txn_ops(), 2..16),
        seed in any::<u64>()
    ) {
        let mut lab = fsqlite_harness::lab::FsLab::new(seed).worker_count(4).max_steps(200_000);
        let report = lab.run(|cx| async move {
            let db = Database::open_in_memory(cx).await.unwrap();
            // Execute all transactions concurrently under deterministic lab scheduling.
            // Verify: every committed transaction's reads are consistent with its snapshot,
            // every aborted transaction had a real conflict.
            Ok::<_, FrankenError>(())
        });
        prop_assert!(report.oracle_report.all_passed(), "oracle failures:\n{}", report.oracle_report);
    }
}
```

### 17.3 Deterministic Concurrency Tests (Lab Runtime)

All MVCC tests run under asupersync's lab runtime via `fsqlite-harness`'s `FsLab`
wrapper (Section 4.2.3). Setup:

```rust
#[test]
fn mvcc_two_writers_different_pages() {
    let seed = 0xDEADBEEF_u64;
    let mut lab = fsqlite_harness::lab::FsLab::new(seed).worker_count(4).max_steps(200_000);

    let report = lab.run(|cx| async move {
        let db = Database::open_in_memory(cx).await.unwrap();
        db.execute(cx, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").await.unwrap();

        let (tx1_done, tx2_done) = (fsqlite_harness::oneshot(), fsqlite_harness::oneshot());

        // Transaction 1: insert into low rowids
        let db1 = db.clone();
        let t1 = lab.spawn("writer.low", move |cx| async move {
            let txn = db1.begin_concurrent(cx).await.unwrap();
            for i in 1..=100 { txn.execute(cx, "INSERT INTO t VALUES(?,?)", (i, "a")).await.unwrap(); }
            txn.commit(cx).await.unwrap();
            tx1_done.send(());
            Ok::<_, FrankenError>(())
        });

        // Transaction 2: insert into high rowids
        let db2 = db.clone();
        let t2 = lab.spawn("writer.high", move |cx| async move {
            let txn = db2.begin_concurrent(cx).await.unwrap();
            for i in 1001..=1100 { txn.execute(cx, "INSERT INTO t VALUES(?,?)", (i, "b")).await.unwrap(); }
            txn.commit(cx).await.unwrap();
            tx2_done.send(());
            Ok::<_, FrankenError>(())
        });

        t1.await.unwrap();
        t2.await.unwrap();
        fsqlite_harness::join(tx1_done.recv(), tx2_done.recv()).await;
        let count: i64 = db.query_one(cx, "SELECT count(*) FROM t").await.unwrap();
        assert_eq!(count, 200);
    });

    assert!(report.oracle_report.all_passed(), "oracle failures:\n{}", report.oracle_report);
}
```

**Seed management:** Each test uses a fixed seed for reproducibility.
CI runs each concurrency test with 100 different seeds. A failing seed is
recorded in the test failure message for exact replay.

**Deterministic repro artifacts (asupersync-native):**

When `ASUPERSYNC_TEST_ARTIFACTS_DIR` is set, any failing deterministic lab run
MUST emit a self-contained repro bundle (so "flake" bugs become one-command
reproductions).

**Directory layout (required on failure):**
```
$ASUPERSYNC_TEST_ARTIFACTS_DIR/
  {test_id}/
    repro_manifest.json
    event_log.txt
    failed_assertions.json
    trace.async          # optional (if trace capture enabled)
    inputs.bin           # optional (if failure depends on input bytes)
```

**Seed taxonomy (required):** each repro manifest MUST record:
- `test_seed` (root)
- derived seeds:
  - `schedule_seed` (scheduler RNG)
  - `entropy_seed` (Cx randomness)
  - `fault_seed` (fault injection)
  - `fuzz_seed` (property generators)

**Derivation rule (normative):**
`derived = H(test_seed || purpose_tag || scope_id)` where `H` is a stable 64-bit
hash (xxh3_64 or SplitMix64), `purpose_tag` is ASCII (`"schedule"`, `"entropy"`,
`"fault"`, `"fuzz"`), and `scope_id` is a stable scenario identifier.

**`repro_manifest.json` minimum schema (required):**
```json
{
  "schema_version": 1,
  "test_id": "mvcc_two_writers_different_pages",
  "seed": 3735928559,
  "scenario_id": "mvcc_two_writers_different_pages",
  "config_hash": "sha256:...",
  "trace_fingerprint": "sha256:...",
  "input_digest": "sha256:...",
  "oracle_violations": ["obligation_leak", "cancel_protocol"],
  "passed": false
}
```

**Replay workflow (required):**
1. Load `repro_manifest.json`.
2. Re-run with `ASUPERSYNC_SEED=<seed>` and the same scenario/test id.
3. If `trace.async` exists, replay directly; any divergence MUST produce a
   divergence artifact with the first mismatched event.

**Fault injection:** The lab reactor supports injecting I/O failures:
```rust
let vfs = fsqlite_harness::vfs::FaultInjectingVfs::new(UnixVfs::new());
vfs.inject_fault(FaultSpec::partial_write("test.db-wal").at_offset_bytes(4096).bytes_written(2048).after_count(50));
```

### 17.4 Systematic Interleaving (Mazurkiewicz Traces)

**Concrete 3-transaction scenario:**

```
T1: BEGIN CONCURRENT; INSERT INTO t VALUES(1,'a'); COMMIT;
T2: BEGIN CONCURRENT; INSERT INTO t VALUES(2,'b'); COMMIT;
T3: BEGIN CONCURRENT; INSERT INTO t VALUES(3,'c'); COMMIT;

Operations (simplified):
  T1_w(page_A), T1_commit
  T2_w(page_B), T2_commit
  T3_w(page_A), T3_w(page_B), T3_commit

Independence relation:
  T1_w(A) independent of T2_w(B)  -- different pages
  T1_w(A) dependent on T3_w(A)    -- same page
  T2_w(B) dependent on T3_w(B)    -- same page

Distinct traces (non-equivalent orderings):
  1. T1_w(A), T1_commit, T2_w(B), T2_commit, T3_w(A), T3_w(B), T3_commit
     -> T3 sees T1's commit on page A: conflict if T3 also wrote A
  2. T1_w(A), T2_w(B), T1_commit, T2_commit, T3_w(A), T3_w(B), T3_commit
     -> Same outcome for T3
  3. T3_w(A), T3_w(B), T3_commit, T1_w(A), T1_commit, T2_w(B), T2_commit
     -> T1 sees T3's commit on page A: T1 conflict
  4. T1_w(A), T3_w(A), ...
     -> T3 gets SQLITE_BUSY immediately (page lock conflict)
  ... (enumerate all distinct orderings)

Verification for each trace:
  - If T_x committed: all its rows visible in final state
  - If T_x aborted: none of its rows visible
  - Total rows = sum of committed transactions' insert counts
  - No phantom rows
```

The Mazurkiewicz trace explorer generates all non-equivalent orderings
(typically tens to low hundreds for 3-5 transaction scenarios) and verifies
invariants for each. This is feasible for small scenarios and provides
exhaustive coverage that random testing cannot guarantee.

#### 17.4.1 SSI Witness Plane Deterministic Scenarios (Required)

The harness MUST include deterministic lab scenarios that specifically stress
SSI witness publication + candidate discovery under cancellation, crashes, and
loss (the correctness posture is: false positives allowed, false negatives
forbidden; §5.6.4.1).

Required scenarios (minimum set):
- **Two writers, disjoint pages:** both commit; no FCW/SSI aborts.
- **Two writers, same page, disjoint cell tags:** merge ladder succeeds (§5.10)
  and emits `MergeWitness`; SSI does not emit spurious edges at refined
  granularity.
- **Classic write skew:** must abort under default SSI (`BEGIN CONCURRENT`),
  and must succeed under explicitly non-serializable mode (if enabled).
- **Multi-process lease expiry + slot reuse:** reuse a TxnSlotId and validate
  that `TxnEpoch` prevents stale hot-index bits from binding to a new txn.
- **Missing/late symbol records during witness decode:** randomly drop/reorder
  witness-plane symbol records and require decode recovery from repair symbols
  (or an explicit "durability contract violated" error with `DecodeProof`).

#### 17.4.2 No-False-Negatives Property Tests (Witness Plane)

Property (normative):
For any execution schedule, if transaction `R` read key `K` and an overlapping
transaction `W` wrote key `K`, then during validation of either party, the
witness plane MUST make it possible to discover `R` as a candidate for `K` at
some configured index level (refinement may be required to confirm).

The property test harness MUST:
- randomly generate witness-key reads/writes across multiple RangeKey levels,
- randomly drop symbol records (local and simulated network),
- randomly crash/cancel publishers mid-stream (reserve/write without commit),
- verify candidate discoverability still holds (no false negatives).

#### 17.4.3 Tiered Storage + Remote Idempotency + Saga Cancellation Scenarios (Required)

Because tiered storage and remote durability are correctness-relevant (not just
"performance features"), the harness MUST include deterministic lab scenarios
for the remote plane (§3.5.11, §4.18–§4.19):

- **Idempotent remote fetch:** Issue duplicate `symbol_get_range` requests with
  the same IdempotencyKey and verify the receiver returns identical outcomes
  (dedup), with no double-accounting of durability acks.
- **Idempotent remote upload:** Retry `symbol_put_batch` after injected timeouts;
  verify the receiver records exactly one durable publication per IdempotencyKey.
- **Eviction saga cancel-safety:** Cancel the eviction saga at each await point
  (upload, verify, local retire) and verify the post-state is coherent: either
  (a) the segment remains locally present, or (b) the segment is provably
  retrievable from L3 and local retirement has occurred. No "half-evicted" state.
- **Epoch transition quiescence:** Trigger an epoch transition while concurrent
  commits are in flight; verify the epoch barrier prevents any commit from
  straddling epochs when the transition affects quorum/key policy (§4.18.4).

### 17.5 Runtime Invariant Monitoring (E-Processes)

E-process configuration for MVCC invariants:

| Invariant | Test statistic | Threshold | Alert condition |
|-----------|---------------|-----------|-----------------|
| INV-1 (Monotonicity) | Consecutive TxnId difference | >= 1 | Any difference < 1 |
| INV-2 (Lock Exclusivity) | Max concurrent holders per page | <= 1 | Any count > 1 |
| INV-3 (Version Chain Order) | Chain order violations per 1K ops | 0 | Any violation |
| INV-4 (Write Set Consistency) | Unlocked writes per 1K ops | 0 | Any unlocked write |
| INV-5 (Snapshot Stability) | Snapshot mutation events per txn | 0 | Any snapshot.high change during a transaction's lifetime |
| INV-6 (Commit Atomicity) | Partial visibility observations | 0 | Any partial observation |
| INV-7 (Serialized Mode Exclusivity) | Concurrent serialized writers | <= 1 | Any count > 1 |
| INV-SSI-FP (SSI False Positives) | Abort false positive rate | <= 0.05 | E_t >= 100 (1/alpha) |

**Hard invariants vs. statistical metrics:** INV-1 through INV-7 are hard
invariants (must NEVER be violated). For these, simple `assert!` or
`debug_assert!` checks with zero tolerance are more appropriate than
e-processes: assertions have zero false-alarm rate, zero computational
overhead in release builds, and immediate failure with a stack trace.
E-processes are the correct tool for **statistical** quality metrics like
INV-SSI-FP (where the null hypothesis is "false positive rate <= threshold"
and we need sequential monitoring to detect drift). Using e-processes for
hard invariants adds unnecessary complexity and introduces a non-zero false
alarm rate (alpha).

**Recommendation:** Use `debug_assert!` for INV-1 through INV-7 in
production code. Reserve e-processes for INV-SSI-FP and other rate-based
metrics where sequential hypothesis testing adds genuine value.

### 17.6 Fuzz Test Specifications

**SQL parser fuzz target:**
```rust
// fuzz/fuzz_targets/sql_parser.rs
fuzz_target!(|data: &[u8]| {
    if let Ok(sql) = std::str::from_utf8(data) {
        let _ = fsqlite_parser::parse(sql);
        // Must not panic, must not loop forever
    }
});
```

**Grammar-based SQL fuzzing:** Use `arbitrary` crate to generate structured
SQL from the grammar, not just random bytes. This achieves deeper coverage:
```rust
#[derive(Arbitrary)]
enum FuzzStatement {
    Select(FuzzSelect),
    Insert(FuzzInsert),
    // ...
}

impl FuzzStatement {
    fn to_sql(&self) -> String { ... }
}

fuzz_target!(|stmt: FuzzStatement| {
    let sql = stmt.to_sql();
    let result = db.execute(&sql);
    // Must not panic, must not corrupt database
    // If Ok, verify with PRAGMA integrity_check
});
```

**Other fuzz targets:**
- `record_decoder`: arbitrary bytes -> `decode_record()` -> must not panic
- `btree_page_decoder`: arbitrary 4096-byte pages -> page parser -> no panic
- `wal_frame_decoder`: arbitrary frame bytes -> frame parser -> no panic
- `json_parser`: arbitrary bytes -> `json_valid()` returns 0 or 1, no panic
- `raptorq_decoder`: valid encoding with random bit flips -> decoder either
  succeeds with correct output or returns error, never silent corruption

### 17.7 Conformance Testing

**Principle:** Conformance is not Phase 9. It starts in Phase 1, and it is how
we keep the project honest while being radically innovative internally.

> We are allowed to change *how* it works. We are not allowed to change *what
> it does* (unless explicitly approved).

**The Oracle:** C SQLite 3.52.0 built from `legacy_sqlite_code/`. The harness
MUST be able to run the Oracle in-process or via a small runner binary, execute
SQL statements, and capture results deterministically.

**Categories:**
- DDL: CREATE/DROP/ALTER for tables, indexes, views, triggers (100+ tests)
- DML: INSERT/UPDATE/DELETE with all clause variants (200+ tests)
- Expressions: arithmetic, string ops, type coercion, NULL handling (150+ tests)
- Functions: every built-in function with edge cases (200+ tests)
- Transactions: BEGIN/COMMIT/ROLLBACK, savepoints, isolation (100+ tests)
- Edge cases: empty tables, MAX_LENGTH values, Unicode, zero-length blobs (100+ tests)
- Extensions: JSON1, FTS5, R*-Tree basic operations (100+ tests)
- Concurrency regression: write skew patterns (must abort under default
  serializable mode in `BEGIN CONCURRENT`)

**What we compare (not just rows):**
- Result rows (including NULL behavior)
- Type affinity where observable
- Error code + extended error code (normalized)
- Affected-row counts (`changes()`, `total_changes()`)
- `last_insert_rowid()` where relevant
- Transaction boundary effects (commit/rollback, savepoints)

**JSON fixture format (self-describing):**

```json
{
  "name": "insert-and-select",
  "steps": [
    { "op": "open", "flags": "readwrite_create", "pragmas": ["journal_mode=WAL"] },
    { "op": "exec", "sql": "CREATE TABLE t(x INTEGER);" },
    { "op": "exec", "sql": "INSERT INTO t VALUES (1),(2),(3);" },
    { "op": "query", "sql": "SELECT x FROM t ORDER BY x;",
      "expect": { "rows": [["1"],["2"],["3"]] } }
  ]
}
```

JSON fixtures are generated by the Oracle runner and consumed by Rust tests.
Harness MUST support multi-step cases (transactions, temp objects, pragmas).
Results are string-normalized by default; type-aware comparison is opt-in.

**SQLLogicTest (SLT) ingestion:** The harness MUST also consume SQLLogicTest
files for broad SQL coverage. SLT provides thousands of pre-existing test
queries with expected results.

**Normalization rules (avoid false failures):**
- Unordered SELECT results: compare as multisets when SQL has no ORDER BY.
- Floating-point: compare exact strings (default) or tolerance mode where
  explicitly requested.
- Error messages: compare error codes; messages are normalized (Oracle's exact
  phrasing is not stable across versions).

**Golden output discipline:** Every optimization or refactor must preserve
golden outputs unless we explicitly document an intentional divergence and
add a harness annotation explaining why it is acceptable.

**Golden file format (simple text):**
```
-- test: insert_returning
-- description: INSERT with RETURNING clause
INSERT INTO t VALUES(1, 'a') RETURNING rowid, *;
-- expected:
-- 1|1|a
```

### 17.8 Performance Regression Detection

**Performance Discipline (Extreme Optimization):**
We operate under a strict loop: Baseline -> Profile -> Prove behavior unchanged (oracle) -> Implement -> Re-measure.
**Non-negotiable rule:** We do not optimize "from vibes". We optimize from profiles and budgets.

**Benchmarks We Must Have Early (from CODEX):**

*Micro:*
- **Page read path:** Resolve visible version (varying chain lengths 0, 1, 10).
- **Delta apply:** Cost of merging intent logs or applying patches.
- **SSI overhead:** Cost of witness-key registration + hot-index updates + refinement + pivot detection.
- **RaptorQ:** Encode/decode throughput for typical capsule sizes (1-4 KB).
- **Coded Index:** Lookup latency vs direct pointer chase.

*Macro:*
- **Multi-writer scaling:** Throughput vs N concurrent writers (1 to 64).
- **Conflict rate:** Abort rate vs Zipf skew parameter.
- **Scan vs Random:** Cache policy sensitivity (ARC vs LRU).
- **Replication:** Convergence time under 5%, 10%, 25% packet loss.

**Statistical methodology (split conformal + e-process; distribution-free):**

We do not assume normality. We treat performance as heavy-tailed and
schedule-sensitive, and we use distribution-free calibration and anytime-valid
monitors from asupersync's lab toolkit.

1. **Baseline establishment:** Run each benchmark scenario across
   `N_base >= ceil(M / alpha_total)` deterministic schedule seeds and record
   the chosen statistic (median/p95/p99 latency, throughput, alloc counts,
   syscall counts). For the canonical configuration (M=12 metrics,
   alpha_total=0.01, Bonferroni), this requires `N_base >= 1200`. For a
   faster development loop, use a relaxed configuration (M=12,
   alpha_total=0.10) requiring only `N_base >= 120`.
   **Rationale:** split conformal prediction at per-metric alpha requires
   `n >= ceil(1/alpha) - 1` calibration samples. Under Bonferroni with M
   metrics, per-metric alpha = alpha_total/M. With too few samples (e.g.,
   n=30 at alpha=0.0008), the conformal bound degenerates to the sample
   maximum, achieving only `n/(n+1)` coverage (~96.8% for n=30) — far
   below the 99.92% required by the per-metric alpha.
2. **Split conformal "no regression" bound (distribution-free):** For each
   metric, compute an upper prediction bound `U_alpha` from baseline samples
   using split conformal quantiles (as in `asupersync::lab::conformal`):
   under the exchangeability assumption across seeds, a fresh baseline run is
   `<= U_alpha` with probability `>= 1 - alpha`.
3. **Candidate measurement:** Run the same scenario across `N_cand >= 10`
   schedule seeds and compute the same statistic.
4. **Gate (normative):** A metric is a regression if `cand_stat > U_alpha`
   (or if a ratio vs baseline median exceeds a declared budget). Budgets and
   `alpha` MUST be recorded in the perf smoke report (§17.8.4).
5. **Anytime-valid regression monitor (optional but canonical):** Define
   per-run exceedance `X_i := 1[cand_i > U_alpha]` and wrap it in an e-process
   monitor. This supports optional stopping while controlling false alarms
   (Ville's inequality; asupersync `EProcess`).
6. **Multiple testing policy (required):** Allocate `alpha_total` across
   metrics using Bonferroni (`alpha = alpha_total / M`) or an alpha-investing
   policy. The policy and `M` MUST be recorded alongside results.

#### 17.8.1 Extreme Optimization Loop (Mandatory, Operational)

All performance work MUST follow this loop (one lever per commit):

1. **BASELINE:** capture p50/p95/p99 + throughput + alloc counts for a named scenario.
2. **PROFILE:** CPU profile and (if relevant) allocation + syscall census.
3. **PROVE:** golden outputs unchanged + isomorphism proof (Section 17.9).
4. **IMPLEMENT:** one optimization lever only (no drive-by refactors).
5. **VERIFY:** re-measure vs baseline; store artifacts; re-run golden checks.
6. **REPEAT:** re-profile (hotspots move).

The loop is strict because database performance is heavy-tailed and non-linear:
optimizing the wrong 5% burns engineering time and typically regresses p99.

#### 17.8.2 Deterministic Measurement Discipline (Seeds + Fingerprints)

**Rule:** Every benchmark scenario MUST be reproducible:
- fixed `seed`,
- fixed scenario parameters,
- recorded environment (at least `RUSTFLAGS`, feature flags, mode),
- recorded `git_sha`.

For concurrent scenarios, we additionally require a **schedule fingerprint**
(Foata fingerprint / trace fingerprint when available) so a profile can be
replayed and diffed without "it got a different interleaving".

This is where asupersync buys real alpha: it turns perf debugging into a
repeatable experiment rather than a noisy ritual.

#### 17.8.3 Opportunity Matrix (Gate: Score >= 2.0)

Before implementing any optimization, we MUST score it:

| Hotspot (func:line) | Impact (1-5) | Confidence (1-5) | Effort (1-5) | Score |
|---------------------|--------------|------------------|--------------|-------|
| example             | 4            | 4                | 2            | 8.0   |

`Score = (Impact * Confidence) / Effort`

Only land changes with `Score >= 2.0`. If you cannot name the hotspot, your
confidence is 0 and the score is 0.

#### 17.8.4 Baseline Artifact Layout (Normative)

FrankenSQLite MUST store perf artifacts under `baselines/` (git-tracked when
small; otherwise stored as CI artifacts with a stable path):

```
baselines/
  criterion/              # Criterion summary baselines (JSON)
  hyperfine/              # CLI microbench baselines (JSON)
  alloc_census/           # heaptrack/valgrind reports
  syscalls/               # strace summaries
  smoke/                  # end-to-end perf smoke reports (JSON)
```

Each artifact MUST include:
- `generated_at` (ISO-8601),
- `command`,
- `seed`,
- `git_sha`,
- scenario id / config hash.

This is the minimal discipline needed to make "it got slower" actionable.

**Perf smoke report schema (required):**

The perf smoke report in `baselines/smoke/` is the canonical manifest for a
measurement run (it ties together baselines, environment, and statistical
gates). Minimum schema:

```json
{
  "generated_at": "2026-02-07T00:00:00Z",
  "scenario_id": "mvcc_100_writers_zipf_s_0_99",
  "command": "cargo bench --bench mvcc_stress",
  "seed": "3735928559",
  "trace_fingerprint": "sha256:...",
  "git_sha": "deadbeef...",
  "config_hash": "sha256:...",
  "alpha_total": 0.01,
  "alpha_policy": "bonferroni",
  "metric_count": 12,
  "artifacts": {
    "criterion_dir": "target/criterion",
    "baseline_path": "baselines/criterion/baseline_20260207_000000.json",
    "latest_path": "baselines/criterion/baseline_latest.json"
  },
  "env": {
    "RUSTFLAGS": "-C force-frame-pointers=yes"
  },
  "system": {
    "os": "linux",
    "arch": "x86_64",
    "kernel": "Linux-6.x"
  }
}
```

#### 17.8.5 Profiling Cookbook (Copy/Paste, Required Fields)

**CPU profiling (Linux):**
```bash
RUSTFLAGS="-C force-frame-pointers=yes" \
cargo flamegraph --bench <bench_name> -- --bench
```

**CLI microbench baseline (hyperfine):**
```bash
hyperfine \
  --warmup 3 \
  --runs 10 \
  --export-json baselines/hyperfine/<scenario>.json \
  '<command>'
```

**Allocation profiling (heaptrack):**
```bash
heaptrack <binary_or_bench_invocation>
```

**Syscall census (strace):**
```bash
strace -f -c -o baselines/syscalls/<scenario>.txt <command>
```

**Mandatory metadata to record in perf notes / smoke report:**
- `git rev-parse HEAD`
- scenario id + parameters
- seed(s)
- `RUSTFLAGS` and feature flags
- platform (`uname -a`)

#### 17.8.6 Golden Checksums for Perf Changes (Behavior Lock)

For any perf-only change, we MUST produce a quick behavior lock:

```bash
# Capture (baseline commit)
sha256sum -b golden_outputs/* > golden_checksums.txt

# Verify (candidate commit)
sha256sum -c golden_checksums.txt
```

The golden outputs are the same ones used by the conformance harness
(Section 17.7): query results, error codes, and any spec-required artifacts
(`CommitMarker`/`CommitProof`/`AbortWitness`) for scenarios that exercise them.

### 17.9 Isomorphism Proof Template (Required For Optimizations)

For every performance optimization that touches query execution or data storage, the PR description MUST include this proof template:

```
Change: <description of optimization>
- Ordering preserved:     [yes/no] (+why)
- Tie-breaking unchanged: [yes/no] (+why)
- Float behavior:         [identical / N/A]
- RNG seeds:              [unchanged / N/A]
- Oracle fixtures:        PASS (list reference case IDs)
```

This ensures we stay fast without drifting from parity. "It feels faster" is not an acceptable justification.

---

## 18. Probabilistic Conflict Model

### 18.1 Problem Statement

Given N concurrent writing transactions, each touching W pages uniformly at
random from a database of P total pages, what is the probability that at
least two transactions conflict (write to the same page)?

### 18.2 Pairwise Conflict Probability

Consider two transactions T1 and T2, each writing W pages chosen uniformly
at random (without replacement) from P total pages.

The probability that T1 and T2 do NOT conflict is the probability that T2's
W pages are all disjoint from T1's W pages:

```
P(no conflict between T1, T2)
  = C(P-W, W) / C(P, W)
  = product_{i=0}^{W-1} (P - W - i) / (P - i)
```

For W << P, this approximates to:

```
P(no conflict) ~ ((P-W)/P)^W ~ e^(-W^2/P)
P(conflict between T1, T2) ~ 1 - e^(-W^2/P)
```

### 18.3 Birthday Paradox Connection

This is exactly the birthday paradox. If each transaction writes W pages
out of P, and we treat each written page as a "birthday" in a year with
P "days", the probability that any two of N transactions share a page is:

```
P(any conflict among N txns) ~ 1 - e^{-N(N-1)W^2 / (2P)}
```

This is the birthday paradox with `N(N-1)/2` pairwise comparisons, each
having `W^2/P` collision probability per pair. The N(N-1) term (not N^2)
reflects that a transaction cannot conflict with itself.

**Interpreting the threshold:** Conflicts become *non-negligible* (not
"likely") near `N * W ~ sqrt(P)`. For P = 1,000,000 pages:

- N=10, W=100 (N*W=1,000): exponent = 10*9*10000/(2*1e6) = 0.045,
  so P(conflict) ~ 4.4% — noticeable but not probable.
- N=10, W=370 (N*W=3,700): exponent ~ 0.62, P(conflict) ~ 46%.
- N=100, W=10: exponent = 100*99*100/(2*1e6) = 0.495,
  P(conflict) ~ 39%.

For P(conflict) > 50%, the exponent must exceed ln(2) ~ 0.693, requiring
`N(N-1)W^2 > 1.386P`. The sqrt(P) threshold marks where conflicts start
appearing at low single-digit percentages, not where they become probable.

### 18.4 Non-Uniform Page Access: Zipf Distribution

Real workloads are NOT uniform. B-tree access patterns follow approximately
a Zipf distribution because:

1. **Root page:** Every B-tree operation reads the root page. For writes
   that modify the tree structure (splits, merges), the root page is also
   written.

2. **Internal pages:** Higher-level internal pages are accessed more
   frequently than lower-level ones (they fan out to many leaf pages).

3. **Hot leaf pages:** In many workloads, recent inserts cluster on a few
   "hot" leaf pages (e.g., auto-increment keys always hit the rightmost leaf).

For Zipf-distributed access with parameter s, the probability of accessing
page ranked k is:

```
p(k) = (1/k^s) / H(P,s)

where H(P,s) = sum_{i=1}^{P} 1/i^s  (generalized harmonic number)
```

#### 18.4.1 Estimating the Zipf Parameter s Online (Policy Input)

The conflict model depends on the skew parameter `s`. It MUST NOT be treated as
a magic constant. The engine SHOULD estimate `s` online from observed page
access frequencies and feed the estimate into:
- shard sizing for lock tables and hot indices,
- contention predictions (abort rate expectations),
- BOCPD regime detection for skew shifts.

**Data collection (bounded, deterministic):**
- Maintain a bounded heavy-hitters summary of page accesses (top-K pages) per
  time window (e.g., 10 seconds), plus an "other" bucket.
- Rank pages by observed frequency within the window (ties broken by pgno).

**Estimator (recommended): discrete Zipf MLE**

For ranks `k = 1..K` with counts `c_k`, let `n = Σ_k c_k`. The Zipf log-likelihood is:

```
ℓ(s) = Σ_{k=1}^{K} c_k * (-s log k - log H(K,s))
```

Solve `dℓ/ds = 0` with a bounded Newton step (few iterations; clamp `s` to
`[0.1, 2.0]`):

```
f(s)    = - Σ c_k log k - n * (H'(K,s)/H(K,s))
H'(K,s) = - Σ_{i=1}^{K} (log i)/i^s
```

**Regime awareness:** Run the estimator per BOCPD regime (reset counts on regime
change). Emit `(s_hat, window_n, regime_id)` into telemetry and the evidence
ledger when used for policy decisions.

With s ~ 0.8-1.2 (typical for database workloads), the conflict probability
increases significantly compared to the uniform model. For N concurrent
transactions each writing W pages drawn from the Zipf distribution, the
probability that no two transactions collide on page k is approximately:

```
P(no conflict on page k) ~ e^{-C(N,2) * p(k)^2}

P(any conflict) ~ 1 - product_{k=1}^{P} e^{-C(N,2) * p(k)^2}
                = 1 - e^{-C(N,2) * sum_k p(k)^2}
```

where C(N,2) = N(N-1)/2 is the number of transaction pairs and p(k) is
the probability each transaction writes page k. For the uniform model,
`sum_k p(k)^2 = P * (W/P)^2 = W^2/P`, recovering the formula in §18.3.
For Zipf, `sum_k p(k)^2 = sum_k 1/(k^{2s} * H(P,s)^2)` is dominated by
the hot pages (small k), concentrating conflict mass on the root and
upper-level B-tree pages.

**Numerical comparison (P=1M, N=10, W=100):**
- Uniform: P(conflict) ~ 4.4%
- Zipf s=1.0: P(conflict) ~ 15-25% (hot-page concentration effect)

### 18.5 B-Tree Hotspot Analysis

Specific B-tree operations that create conflict hotspots:

**Root page modifications:** When a B-tree root page splits, the root is
rewritten. Any concurrent transaction also writing to the same B-tree will
conflict on the root page, even if it targets a completely different key
range. Root splits are rare for large trees (depth 3+ trees split the root
only when growing from depth d to d+1) but catastrophic for concurrency
when they happen.

**Page splitting as conflict amplifier:** A single INSERT that causes a leaf
page split modifies: (1) the leaf page being split, (2) the new sibling
leaf page, (3) the parent internal page (to add the new child pointer), and
potentially (4) the parent's parent if the parent also splits. A single
INSERT can touch 2-4 pages, increasing the effective W per transaction.

**Index maintenance:** Each INSERT into a table with K indexes modifies
~1 + K pages (one table leaf + one leaf per index), multiplied by split
probability. A table with 5 indexes has an effective W per INSERT of ~6
in the no-split case, ~12-20 in the split case.

### 18.6 Empirical Validation Methodology

To validate the probabilistic model against actual conflict rates:

1. **Instrumentation:** Add counters to the MVCC commit path:
   - `conflicts_detected`: total FCW base-drift conflicts (commit-index says base changed)
   - `conflicts_merged_rebase`: conflicts resolved by deterministic rebase (intent replay)
   - `conflicts_merged_structured`: conflicts resolved by structured patch merge
   - `conflicts_aborted`: conflicts that caused transaction abort/retry
   - `total_commits`: total commit attempts
   - `pages_per_commit`: histogram of write set sizes

2. **Benchmark workloads:**
   - Uniform random: INSERT with random keys into large table
   - Sequential: INSERT with auto-increment keys
   - Zipf: INSERT with Zipf-distributed keys (s = 0.99)
   - Mixed: 80% read, 20% write across 4 tables

3. **Comparison:** Plot actual conflict rate vs model prediction. Expected
   result: uniform model matches uniform workload within 10%, Zipf model
   matches skewed workloads within 20%.

### 18.7 Impact of Safe Write Merging

Safe write merging (§5.10; §3.4.5) reduces aborts by converting some FCW
base-drift conflicts into successful commits *only when the underlying intent
operations commute*.

**Worked example (semantic, not byte offsets):**
- Two concurrent transactions `T1` and `T2` both INSERT distinct keys that land
  on the same leaf page.
- Without merge: `T2` hits FCW base drift at commit and aborts/retries.
- With merge ladder (`PRAGMA fsqlite.write_merge = SAFE`): `T2` rebases its
  `IntentOp::Insert` against the current committed snapshot (or merges a
  `StructuredPagePatch` keyed by `cell_key_digest`), producing a page that
  contains both inserts.

Note that the physical byte regions touched by the two inserts may overlap
(cell pointer array growth, free space accounting, defragmentation). SAFE merge
is possible anyway because the merge predicate is **semantic disjointness**,
not byte disjointness.

**Effective abort reduction model:**

Let `f_merge` be the empirically-measured fraction of detected FCW conflicts
that are resolved by the SAFE merge ladder (rebase + structured patches). Then:

```
P_abort_eff ≈ P_conflict * (1 - f_merge)
```

`f_merge` is workload-dependent and must be measured (see §18.6), not assumed.

### 18.8 Throughput Model

The committed transactions per second (TPS) under contention:

```
TPS = N * (1 - P_abort) * (1 / T_txn)

where:
  N = number of concurrent writers
  P_abort = probability a transaction must abort and retry
  T_txn = average transaction duration (seconds)
```

P_abort depends on the conflict rate and the number of retries:

```
P_abort_first_attempt ~ 1 - product_{j != i} (1 - P(conflict with T_j))
P_abort_after_K_retries ~ P_abort_first^K  (geometric retry)
```

For the typical case (medium DB, moderate writers):
- P = 100,000 pages, W = 50 pages/txn, N = 8 writers
- P(pairwise conflict) ~ 1 - e^(-50^2/100000) ~ 0.025
- P(any conflict for one txn) ~ 1 - (1-0.025)^7 ~ 0.16
- With safe merge ladder resolving f_merge=0.40 of detected conflicts (empirical): effective P_abort ~ 0.10
- With one retry: P_abort ~ 0.01
- TPS ~ 8 * 0.99 / T_txn

This shows that for medium-to-large databases, MVCC concurrent writers
achieve near-linear scaling up to ~8 writers. Beyond that, conflict rates
grow quadratically (birthday paradox) and throughput plateaus.

**Retry policy (required for completeness):**

Retries are a policy problem with explicit tradeoffs: extra retries reduce
abort rate but increase tail latency and can amplify contention. Hard-coded
"max retries" and fixed backoff constants are brittle.

**Normative model:** Retry control MUST be framed as expected-loss minimization
under uncertainty, bounded by the caller's timeout (`PRAGMA busy_timeout`) and
`Cx` deadline (§4.17).

Define:
- `T_budget`: remaining time budget (ms)
- `C_try`: cost of one retry attempt (validation + potential write amplification)
- `C_fail`: cost of surfacing `SQLITE_BUSY` to the application
- `p_succ(t | evidence)`: probability the next attempt succeeds if we wait `t`
  before retrying (estimated per BOCPD regime from conflict telemetry; default
  priors allowed)

The controller chooses an action `a ∈ {FailNow} ∪ {RetryAfter(t)}` minimizing:

```
E[Loss(FailNow)]         = C_fail
E[Loss(RetryAfter(t))]   = t + C_try + (1 - p_succ(t)) * C_fail
```

The argmin yields an **optimal stopping** rule ("retry while the expected
benefit exceeds the marginal cost"). With a Beta-Bernoulli model for success
probability and a fixed per-attempt cost, the optimal policy corresponds to a
Gittins-index threshold rule; implementations MAY use the index directly or a
deterministic approximation.

**Starvation / fairness (required):**
- The controller MUST NOT grant retried transactions priority over new ones.
- If a single transaction experiences repeated conflicts under remaining budget
  (starvation), the controller MAY escalate by switching that transaction to a
  brief serialized/advisory mode for progress. This is a policy action that
  MUST be recorded in the evidence ledger.

If `T_budget` is exhausted, the engine MUST stop retrying and return
`SQLITE_BUSY` (or `SQLITE_INTERRUPT` if cancelled).

---

## 19. C SQLite Behavioral Reference

For the complete behavior extraction from C SQLite source (data structures,
SQL grammar, all 190+ VDBE opcodes, B-tree page format, WAL format, all
PRAGMA commands, all built-in functions, extension APIs, error codes, locking
protocol, transaction semantics, virtual table interface, threading model,
and limits), see `EXISTING_SQLITE_STRUCTURE.md`.

That document is the authoritative behavioral spec. Implementation should
consult ONLY that document for C SQLite behavior, not the C source code
directly (per the porting methodology: extract spec from legacy, implement
from spec, never translate line-by-line).

**Key behavioral quirks that differ from naive expectations:**

- **Type affinity is advisory, not enforced** (except STRICT tables). You
  can store a TEXT value in an INTEGER column. The affinity only affects
  type coercion during comparison and storage, not rejection.

- **NULL handling in UNIQUE constraints:** SQLite allows multiple NULL
  values in a UNIQUE column (NULL != NULL). This differs from some other
  databases.

- **ORDER BY on compound SELECT:** ORDER BY at the end of a compound
  SELECT (UNION, EXCEPT, INTERSECT) uses column numbers or aliases from
  the FIRST select, not the last.

- **Integer overflow wraps silently** in some contexts. The `sum()`
  aggregate raises an error on overflow, but arithmetic expressions like
  `9223372036854775807 + 1` promote to REAL (floating point) rather than
  wrapping.

- **AUTOINCREMENT vs rowid reuse:** Without AUTOINCREMENT, deleted rowids
  CAN be reused. `max(rowid)+1` is used for new rows, but if the maximum
  rowid is `MAX_ROWID` (2^63-1; see `vdbe.c` `OP_NewRowid`'s `MAX_ROWID`),
  SQLite tries random rowids.

- **LIKE is case-insensitive for ASCII only.** The built-in LIKE does not
  handle Unicode case folding. `'a' LIKE 'A'` is true, but `'ä' LIKE 'Ä'`
  is false without ICU.

- **Empty string vs NULL:** `''` (empty string) is NOT NULL. `length('')`
  returns 0, not NULL. `'' IS NULL` is false.

- **Deterministic vs non-deterministic functions:** Functions like
  `random()`, `changes()`, and `last_insert_rowid()` are non-deterministic
  and are re-evaluated for each row. The query planner cannot factor them
  out of loops.

---

## 20. Key Reference Files

### C SQLite Source (for spec extraction only)

| File | Purpose | Lines | What to Extract |
|------|---------|-------|-----------------|
| `sqliteInt.h` | Main internal header | 5,882 | All struct definitions (Btree, BtCursor, Pager, Wal, Vdbe, Mem, Table, Index, Column, Expr, Select, etc.), all `#define` constants, all function prototypes. This is the Rosetta Stone. |
| `btree.c` | B-tree engine | 11,568 | Page format parsing, cell format, cursor movement algorithms (moveToChild, moveToRoot, moveToLeftmost, moveToRightmost), insert/delete with rebalancing, overflow page management, freelist operations. Focus on `balance_nonroot` (~800 lines, lines 8230-9033) as the most complex function. |
| `pager.c` | Page cache | 7,834 | Pager state machine (OPEN, READER, WRITER_LOCKED, WRITER_CACHEMOD, WRITER_DBMOD, WRITER_FINISHED, ERROR), journal format, hot journal detection, page reference counting, cache eviction policy. |
| `wal.c` | WAL subsystem | 4,621 | WAL header/frame format, checksum algorithm implementation, WAL index (wal-index) hash table structure, checkpoint algorithm, the critical `WAL_WRITE_LOCK` in `sqlite3WalBeginWriteTransaction` (line numbers shift by SQLite version) that FrankenSQLite replaces with MVCC. |
| `vdbe.c` | VDBE interpreter | 9,316 | The giant switch statement dispatching all opcodes. Each case is the authoritative definition of what that opcode does. Extract: register manipulation, cursor operations, comparison semantics, NULL handling per opcode. |
| `select.c` | SELECT compilation | 8,972 | How SELECT is compiled to VDBE opcodes: result column processing, FROM clause flattening, subquery handling, compound SELECT, DISTINCT, ORDER BY, LIMIT. |
| `where.c` | WHERE optimization | 7,858 | Index selection algorithm, cost estimation, OR optimization, skip-scan, automatic index creation. The `WhereTerm`, `WhereLoop`, and `WherePath` structures define the optimizer's search space. |
| `parse.y` | LEMON grammar | 2,160 | The authoritative SQL grammar. Every production rule defines a valid SQL construct. Use as the reference for the recursive descent parser. |
| `tokenize.c` | SQL tokenizer | 899 | Token types, keyword recognition, string/number/blob literal parsing, comment handling. |
| `func.c` | Built-in functions | 3,461 | Implementation of all scalar and aggregate functions. Edge case behaviors (NULL handling, type coercion, overflow) are defined here. |
| `expr.c` | Expression handling | 7,702 | Expression compilation, affinity computation, collation resolution, constant folding. |
| `build.c` | DDL processing | 5,815 | CREATE TABLE/INDEX/VIEW/TRIGGER compilation, schema modification, type affinity determination from type name strings. |

### Asupersync Modules

| Module | What FrankenSQLite Uses | Why It Matters |
|--------|----------------------|----------------|
| `src/raptorq/` | RFC 6330 codec | WAL self-healing, replication, version chain compression. The core innovation enabler. |
| `src/sync/` | Mutex, RwLock, Condvar | MVCC lock table, version chain access, global write mutex for serialized mode. |
| `src/channel/mpsc.rs` | Two-phase MPSC | Write coordinator commit pipeline with cancel-safety and backpressure. |
| `src/channel/oneshot.rs` | Oneshot response | Commit response delivery from coordinator to committing transaction. |
| `src/cx/` | Capability context | Threading through every function for cancellation, deadlines, and capability narrowing. |
| `src/lab/runtime.rs` | Deterministic runtime | Reproducible concurrency testing, fault injection, virtual time. |
| `src/lab/explorer.rs` | DPOR + Mazurkiewicz traces | Systematic schedule exploration for small critical concurrency scenarios. |
| `src/obligation/eprocess.rs` | E-process core | Anytime-valid monitoring for invariant violations under optional stopping. |
| `src/lab/oracle/eprocess.rs` | E-process oracle | Test harness + certificates for e-process monitoring. |
| `src/lab/conformal.rs` | Distribution-free stats | Benchmark regression detection without parametric assumptions. |
| `src/database/sqlite.rs` | API reference | FrankenSQLite's public API mirrors asupersync's SQLite wrapper API for familiarity. |

### Project Documents

| Document | Purpose | When to Consult |
|----------|---------|-----------------|
| `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md` | Source of truth | Always. This document supersedes all others. |
| `EXISTING_SQLITE_STRUCTURE.md` | C SQLite behavior | When implementing any feature: look up the C behavior first, then implement from the spec. |
| `docs/rfc6330.txt` | RaptorQ specification | When implementing RaptorQ integration (WAL, replication, version chains). |
| `AGENTS.md` | Coding guidelines | Before every coding session: review style, testing, and documentation requirements. |
| `MVCC_SPECIFICATION.md` | MVCC formal model (legacy) | Historical reference only. Section 5 of this document supersedes it with corrections. |
| `PROPOSED_ARCHITECTURE.md` | Architecture overview (legacy) | Historical reference. Section 8 of this document supersedes the crate map. |

---

## 21. Risk Register, Open Questions, and Future Work

### 21.0 Risk Register (With Mitigations)

**R1. SSI abort rate too high (Page-SSI is conservative).**
Mitigations:
- Refine witness keys from page → (page, range/cell tag) to reduce false positives.
- Add safe snapshot optimizations for read-only transactions.
- Intent-level rebase (Section 5.10.2) turns page conflicts into merges,
  reducing effective conflict rate by 30-60%.
- PostgreSQL's measured false positive rate is ~0.5% at row granularity; our
  page granularity will be higher, but merge compensation helps.

**R2. RaptorQ overhead dominates CPU.**
Mitigations:
- Choose symbol sizing policy based on object type (capsules: small symbols
  for fast commit; checkpoints: large symbols for throughput).
- Cache decoded objects aggressively (ARC cache).
- Profile and tune encoder/decoder hot paths (one lever per change, per
  the Extreme Optimization methodology).

**R3. Append-only storage grows without bound.**
Mitigations:
- Checkpoint, GC, and compaction are first-class (Section 5.5 for MVCC GC/version chain trimming, Section 7.13 for ECS compaction, Section 7.9 for the crash contract).
- Enforce budgets for MVCC history, SSI witness plane, symbol caches.
- Safe GC horizon = min(active `begin_seq`) (Theorem 4) bounds version chain length (Theorem 5).

**R4. Bootstrapping chicken-and-egg (need index to find symbols, need symbols
to decode index).**
Mitigations:
- Symbol records are self-describing (header + OTI).
- One tiny mutable root pointer per database.
- Rebuild-from-scan is always possible as a fallback.

**R5. Multi-process MVCC coordination is complex.**
Mitigations:
- Shared-memory coordination protocol specified (Section 5.6.1).
- Lease-based TxnSlot cleanup handles process crashes without blocking.
- Both in-process and cross-process MVCC are validated in Phase 6.
- Explicit tests for multi-process behaviors required before shipping.

**R6. File format compatibility vs "do it right".**
Mitigations:
- Compatibility Mode (Section 7.10) treats SQLite `.db/.wal` as the standard
  format for conformance.
- Native Mode is the innovation layer.
- Conformance harness validates observable behavior, not byte-identical layout.

**R7. Mergeable writes become a correctness minefield.**
Mitigations:
- Strict merge ladder (Section 5.10.4): only take merges we can justify.
- Proptest invariants + DPOR tests (Section 5.10.5).
- Start with deterministic rebase replay for a small op subset (inserts/updates
  on leaf pages), grow coverage guided by conflict benchmarks.

**R8. Distributed mode correctness is hard.**
Mitigations:
- Symbol-native replication uses "leader commit clock" as the default mode.
- Use sheaf checks + TLA+ export for bounded model checking.
- Replication protocol is ECS-native: ObjectId set reconciliation + anti-entropy.
- Implementation phased: single-node first, then multi-node (Phase 9).

### 21.1 Open Questions (With How We Answer Them)

**Q1. Multi-process writers:** What is the performance envelope for cross-process
concurrent writes?
*Answer plan:* Implement shared-memory coordination (Section 5.6.1); benchmark
contention vs in-process baseline; tune TxnSlot count and lease intervals.

**Q2. How far do we go with range/cell refinement for SSI witness keys?**
*Answer plan:* Start page-only; collect abort witnesses; refine only when
abort rate is proven unacceptable by benchmark.

**Q3. Symbol sizing policy per object type (capsule vs checkpoint vs index).**
*Answer plan:* Benchmark encode/decode throughput vs object sizes; pick
defaults; expose PRAGMA overrides for experiments.

**Q4. Where to checkpoint for compatibility `.db` without bottlenecking writes?**
*Answer plan:* Background checkpoint with ECS chunks; measure; keep export
optional.

**Q5. Which B-tree operations can be replayed deterministically for rebase merge?**
*Answer plan:* Implement inserts/updates on leaf pages first; grow coverage
guided by conflict benchmarks.

**Q6. Do we need B-link style concurrency for hot-page split/merge contention?**
*Answer plan:* Benchmark workloads that hammer the same index/table. If
internal-page conflicts dominate, add an internal "structure modification"
protocol (ephemeral metadata, not file format changes) inspired by B-link
trees: optimistic descent + right-sibling guidance + deterministic retry.

### 21.2 Cross-Process MVCC (Implementation Notes)

Cross-process MVCC is specified in Section 5.6.1. Implementation notes:
- Phase 6 validates both in-process and cross-process MVCC correctness
  (the Phase 6 gates in §22 explicitly test multi-process lease expiry
  and TxnSlot reuse; Phase 7 is "Advanced Query Planner", not cross-process)
- Key challenge: benchmarking the mmap-based TxnSlot array vs in-process atomics
- Lease-based cleanup must be stress-tested under process crash scenarios

### 21.3 Write-Ahead-Log Multiplexing

For very high write throughput (>100K TPS), a single WAL file becomes the
bottleneck (sequential append to one file). WAL multiplexing shards WAL
frames across multiple files:
- WAL file selected by `hash(page_number) % num_wal_files`
- Each WAL file has its own checkpoint state
- Commit requires atomic append to all WAL files touched by the transaction
  (2PC across WAL files; crash recovery replays prepared-but-uncommitted
  entries using a global commit marker in the primary WAL)
- Potential improvement in sustained write throughput on NVMe SSDs
  with high queue depth (actual speedup depends on workload page
  distribution and device parallelism; requires benchmarking)

### 21.4 Distributed Consensus Integration

For multi-node deployments, integrate Raft or Paxos for replicated state:
- WAL entries as the replicated log
- Leader handles all writes, followers handle reads (read replicas)
- Snapshot shipping (Section 3.4.3) for new follower initialization
- RaptorQ-coded replication (Section 3.4.2) for steady-state log shipping
- Challenge: linearizable reads require either reading from leader or
  implementing read leases

### 21.5 GPU-Accelerated RaptorQ Encoding

For bulk operations (full database backup, large changeset replication),
RaptorQ encoding is CPU-bound. GPU acceleration via compute shaders:
- GF(256) arithmetic maps well to SIMD/GPU (each symbol byte independent)
- Matrix multiplication for intermediate symbol generation is embarrassingly
  parallel
- Expected speedup: 10-50x for large source blocks (K > 10,000)
- Framework: wgpu for cross-platform GPU compute

### 21.6 Persistent Memory (PMEM) VFS

CXL-attached persistent memory (and legacy Intel Optane DCPMM) enable
byte-addressable persistent storage. A PMEM VFS would:
- Memory-map the database file directly to PMEM
- Eliminate the WAL entirely (copy-on-write page updates with 8-byte
  atomic pointer swings for crash consistency)
- Use `clflush`/`clwb` instructions for cache line persistence
- MVCC version chains stored directly in PMEM with epoch-based reclamation
- Expected latency reduction: 10-100x for small transactions (eliminate
  WAL write + fsync)

### 21.7 Vectorized VDBE Execution

Current VDBE processes one row at a time (Volcano model). Vectorized
execution processes batches of rows through each operator:
- Column-at-a-time processing enables SIMD utilization
- Better CPU cache behavior (fewer instruction cache misses)
- Applicable to full table scans, aggregations, and nested-loop joins
- Expected speedup: 2-5x for analytical queries, negligible for point lookups
- Challenge: must maintain row-at-a-time semantics for triggers and
  RETURNING clause

### 21.8 Column-Store Hybrid for Analytical Queries

For mixed OLTP/OLAP workloads, a column-store representation alongside
the row-store B-tree:
- Column groups stored in separate B-trees per column
- Automatic materialization of frequently-scanned columns
- RLE and dictionary compression for low-cardinality columns
- Query planner selects row-store or column-store based on query pattern
- Challenge: maintaining consistency between row-store and column-store
  under concurrent writes

### 21.9 Erasure-Coded Page Storage (Implementation Notes)

Section 3.4.6 fully specifies erasure-coded page storage. Implementation notes:
- Modified page allocation: allocate G pages as a group
- Repair page storage: in the ECS object store (Native mode) or in a
  `foo.db-fec` sidecar file (Compatibility mode)
- Read path: attempt source page first, fall back to erasure recovery
- Group size selection: benchmark G=32, G=64, G=128 to find the optimal
  balance of space overhead vs recovery capability per workload

### 21.10 Time Travel Queries and Tiered Symbol Storage (Implementation Notes)

Native mode's source of truth is an immutable commit stream (`CommitCapsule` +
`CommitMarker`). The canonical spec already includes:
- Time travel queries (§12.17: `FOR SYSTEM_TIME AS OF ...` including a `COMMITSEQ` form), and
- Tiered symbol storage (§3.5.11: L1/L2/L3 with fetch-on-demand under capability).

The remaining work is operational: explicit retention policy, predictable
latency control, and failure-mode hardening (idempotency + sagas; §4.19).

- **Retention policy:** Time travel is only meaningful within a configured
  history window. GC/compaction MUST remain free to drop old history unless a
  retention policy pins it.
- **Addressing:** The stable history coordinate is `commit_seq`. Timestamp-based
  APIs require persisting `commit_time` metadata per commit and an index to map
  time → `commit_seq` (under `LabRuntime`, this uses deterministic virtual time).
- **SQL surface:** FrankenSQLite supports `FOR SYSTEM_TIME AS OF` (§12.17) as
  the primary interface, plus `... AS OF COMMITSEQ <n>` as a stable coordinate
  that avoids timestamp ambiguity.
- **Tiered SymbolStore:** `SymbolStore` SHOULD remain pluggable with an optional
  cold backend (object storage). Remote fetch MUST require an explicit capability
  (`RemoteCap`; §4.19.1) and MUST be paired with caching and prefetching so query
  latency remains predictable.

---

## 22. Verification Gates

Every phase must pass all applicable gates before proceeding to the next.

### Universal Gates (All Phases)

1. `cargo check --workspace` -- zero errors, zero warnings
2. `cargo clippy --workspace --all-targets -- -D warnings` -- zero warnings
   with pedantic + nursery lints
3. `cargo fmt --all -- --check` -- all code formatted
4. `cargo test --workspace` -- all tests pass, no ignored tests without
   documented reason
5. `cargo doc --workspace --no-deps` -- all public items documented, no
   broken doc links

### Phase-Specific Gates

**Phase 2 gates:**
- MemoryVfs passes all VFS trait contract tests
- Record format round-trip proptest with 10,000 iterations, zero failures
- Zero `unsafe` blocks in any crate

**Phase 3 gates:**
- B-tree proptest: 10,000-operation random sequence, invariants hold
- B-tree: cursor iteration after random ops matches BTreeMap reference
- Parser: 95% coverage of `parse.y` grammar productions
- Parser fuzz: 1 hour of fuzzing with zero panics

**Phase 4 gates:**
- End-to-end: 20 SQL conformance tests (basic DDL + DML) pass
- VDBE: EXPLAIN output for basic queries matches expected opcode sequence
- Sorter: correctly sorts 100,000 rows

**Phase 5 gates:**
- File format: database created by FrankenSQLite readable by C sqlite3
- File format: database created by C sqlite3 readable by FrankenSQLite
- WAL recovery: 100 crash-recovery scenarios with zero data loss
- RaptorQ WAL: recovery succeeds with up to R corrupted frames (R = repair
  symbol count)

**Phase 6 gates:**
- MVCC stress test: 100 concurrent writers, 100 operations each, all
  committed rows present, no phantom rows
- SSI: write skew patterns produce abort under default serializable mode;
  same patterns succeed under PRAGMA fsqlite.serializable=OFF
- SSI: no false negatives (no write skew anomaly escapes detection in
  3-transaction Mazurkiewicz trace exploration)
- SSI witness plane: multi-process lease expiry + TxnSlot reuse does not cause
  stale hot-index bits to bind to a new `TxnToken` (TxnEpoch validation holds)
- SSI witness plane: witness objects/segments decode under injected symbol
  loss/reordering (repair-path succeeds or emits explicit `DecodeProof`)
- Snapshot isolation: verified via Mazurkiewicz trace exploration for
  3-transaction scenarios (all non-equivalent orderings)
- E-process monitors: INV-1 through INV-7, zero violations over 1M operations
- GC memory bound: memory usage under sustained load stays within 2x of
  minimum theoretical (active transactions * pages per transaction * page size)
- Serialized mode: behavior identical to C SQLite for single-writer test suite
- Rebase merge: 1,000 merge attempts with distinct-key inserts on same page,
  zero false rejections
- Structured merge safety: 1,000 merge attempts with commuting, cell-key-disjoint
  operations on the same page, no lost updates; negative tests for the B-tree
  lost-update counterexample (cell move/defrag vs update at old offset) are
  never accepted
- Crash model: 100 crash-recovery scenarios validating self-healing durability
  contract (Section 7.9)

**Phase 7 gates:**
- Query planner: EXPLAIN QUERY PLAN shows index usage for indexed queries
- Window functions: 50 conformance tests matching C SQLite output
- CTE: recursive CTE terminates correctly with LIMIT

**Phase 8 gates:**
- JSON1: json_valid/json_extract/json_set pass 200 conformance tests
- FTS5: full-text search returns relevant results for 100 test queries
- R*-Tree: spatial query returns correct results for 50 bounding box queries

**Phase 9 gates:**
- Conformance: **100% parity target** across 1,000+ golden files (with any
  intentional divergences explicitly documented and annotated in the harness)
- Benchmarks: single-writer within 3x of C SQLite
- Benchmarks: no regression (candidate statistic <= conformal upper bound U_alpha with alpha=0.01, per §17.8 methodology) compared to Phase 8
- Replication: database replicates correctly under 10% packet loss within 1.2x of no-loss time (matches §16, Phase 9 acceptance criteria)

---

## 23. Summary: What Makes FrankenSQLite Alien

FrankenSQLite is not an incremental improvement on SQLite. It is a
ground-up reimagination of what an embedded database engine can be when
built on near-optimal erasure coding, formal verification, and modern
language guarantees.

**1. MVCC with Serializable Concurrent Writers (In-Process and Cross-Process).**
The single biggest limitation of SQLite -- the WAL_WRITE_LOCK that serializes
all writers -- is replaced with page-level MVCC versioning and Serializable
Snapshot Isolation (SSI). Applications choose their isolation level: Serialized
mode for exact backward compatibility, Concurrent mode for true multi-writer
parallelism with full SERIALIZABLE guarantees (not merely Snapshot Isolation).
The conservative Page-SSI rule prevents write skew by default; safe write
merging (intent replay + structured page patch merge) and deterministic rebase reduce conflict rates on hot
pages without row-level MVCC metadata. Cross-process MVCC uses a shared-memory
coordination region with lease-based crash cleanup. The layered approach means
zero risk for existing applications and serializable concurrency for
applications that opt in.

**2. RaptorQ-Pervasive Architecture with ECS Substrate.** Fountain codes are
not bolted on as an afterthought. They are woven into every layer: the WAL
uses RaptorQ repair symbols for self-healing durability that survives torn
writes without double-write journaling. The replication protocol is
fountain-coded for bandwidth-optimal, UDP-based, multicast-capable data
transfer over lossy networks. Version chains use XOR delta encoding (stored as ECS objects, erasure-coded
for durability) for near-optimal compression. Conflict resolution uses semantic write merging (intent
replay + structured page patches keyed by stable identifiers); XOR/`GF(256)` is an encoding for patch/history
objects, not a merge correctness criterion. The Erasure-Coded
Stream (ECS) substrate provides content-addressed, self-describing,
deterministic object storage with BLAKE3 ObjectIds and self-healing repair
symbols. The result: data loss becomes a mathematical near-impossibility
rather than a failure mode to mitigate.

**3. Asupersync Deep Integration.** Every operation threads a Cx capability
context for type-safe cancellation and deadline propagation. The lab reactor
enables fully deterministic concurrency testing with reproducible scheduling
and precise fault injection. E-processes provide anytime-valid statistical
invariant monitoring based on Ville's inequality. Mazurkiewicz traces
systematically enumerate all non-equivalent interleavings of concurrent
transactions for exhaustive verification. Conformal calibration provides
distribution-free confidence intervals for benchmark regression detection.
Sheaf-theoretic consistency checking formally verifies that MVCC snapshot
views are globally consistent.

**4. Safe Rust, No Compromises.** `unsafe_code = "forbid"` at workspace
level. Clippy pedantic and nursery lints at deny level. If it compiles,
it is free of undefined behavior, data races, and use-after-free. The
entire database engine -- including the B-tree, VDBE, MVCC system, and
all extensions -- is memory-safe by construction.

**5. Full Compatibility.** FrankenSQLite reads and writes standard SQLite
database files. It targets **100% behavioral parity** against golden-file
tests comparing output with C sqlite3 for the supported surface. Any
intentional divergence MUST be explicitly documented and annotated in the
harness with rationale. The SQL dialect, type affinity system, VDBE
instruction set, file format, and WAL format all match SQLite 3.52.0. It aims
to be a near-drop-in replacement for the sqlite3 CLI and library, targeting
**100% parity** while deliberately omitting deprecated
or security-sensitive features (loadable extensions, shared-cache mode, legacy
schema formats 1-3; see §15).

**6. Formal Verification Depth.** The MVCC system is specified with formal
invariants (INV-1 through INV-7), safety proofs (deadlock freedom, snapshot
isolation, serializable mode, first-committer-wins, GC safety), SSI
correctness argument (conservative rw-antidependency rule prevents cycles),
and a probabilistic conflict model validated empirically. The testing strategy
combines property-based testing, deterministic concurrency testing, systematic
interleaving exploration, anytime-valid statistical monitoring, grammar-based
fuzzing, and conformance testing against the reference implementation starting
from Phase 1 (not deferred to Phase 9). An explicit crash model, risk
register, and operating mode duality (Compatibility vs Native) ensure the
system is both innovative and verifiable. This is not aspirational -- these
tools exist in asupersync and are integrated into the test infrastructure.
The monitoring stack is layered: BOCPD detects workload regime shifts (Section
4.8), e-processes detect invariant violations within any regime (Section 4.3),
and conformal calibration provides distribution-free performance bounds
(Section 4.7). SSI abort decisions are grounded in decision-theoretic expected
loss minimization with explicit asymmetric loss matrices (Section 5.7).

**7. Information-Theoretic Guarantees (Alien-Artifact Formal Theorems).**
FrankenSQLite's durability and repair contracts are not heuristic. They rest
on provable information-theoretic foundations:

**Theorem (Durability Bound).** For an ECS object encoded as K source symbols
with R repair symbols, and a local corruption model where each symbol is
independently corrupted with probability p, the probability that the object
is unrecoverable is:

```
P(loss) <= sum_{i=R+1}^{K+R} C(K+R, i) * p^i * (1-p)^(K+R-i)
```

This bound holds for any `p`. FrankenSQLite treats `p` as a budgeted and
monitored parameter (§3.5.12): e-processes provide anytime-valid guardrails,
and the system maintains living estimates of `p` (Bayesian posterior for
explainability plus a conservative `p_upper` for decisions).

When the engine reports a "durability bound" as a guarantee, it MUST plug
`p_upper` (not a point estimate) into the theorem, so the bound is conservative
under optional stopping.

Plugging in a representative design point (R ≈ 0.2K, p = 10^-4), this tail
probability is extremely small for moderate K. Concrete orders of magnitude
(using the leading term of the binomial tail):
- K=4, R=1 (n=5): P(loss) ≈ C(5,2) p^2 ≈ 1e-7
- K=16, R=4 (n=20): P(loss) ≈ C(20,5) p^5 ≈ 1.6e-16

Small-K objects are dominated by integer rounding and additive decode slack;
the engine clamps symbol policies per §3.5.3.

**Theorem (Repair Completeness).** For any ECS object, if the local symbol
store retains at least K valid symbols (out of K+R stored), the original
object bytes are recoverable exactly. The `DecodeProof` artifact witnesses
the reconstruction: it records the specific symbol subset used and the
decoder's intermediate state, constituting a mathematical certificate of
correct repair.

**Monitoring via e-processes + living bounds:** The failure probability envelope
is not merely a design-time calculation. At runtime:
- e-process monitors track whether symbol corruption exceeds the configured
  budget under optional stopping (§3.5.12),
- and the system SHOULD export a "living durability estimate" per object class:
  `(p_posterior, p_upper, P_loss(p_upper))` for the current epoch/regime.

If evidence indicates `p` drifted above budget (media degradation, firmware
bugs, correlated failures), the e-process alarm fires *before* data loss
becomes possible, and the redundancy autopilot hardens by publishing additional
repair symbols (§3.5.12.3).

FrankenSQLite demonstrates that embedded databases need not sacrifice
concurrency for simplicity, durability for performance, or safety for speed.
By building on near-optimal erasure coding (RaptorQ), formal
verification techniques (e-processes, Mazurkiewicz traces, sheaf theory),
and the memory safety guarantees of Rust, it sets a new standard for what
an embedded database engine can achieve.

---

*Document version: 1.28 (Round 11 audit: ARC p-update online-learning framing added (research note; canonical ARC update remains normative); Round 10 audit: version-chain delta compression corrected: use sparse XOR deltas between adjacent page versions (RaptorQ remains the durability/repair layer for delta objects); prior rounds: forbid raw byte-disjoint XOR write merging for SQLite structured pages; specify safe merge ladder (intent-log deterministic rebase + structured patch parse/merge/repack + merge certificates); built-in function semantics audited/corrected (ceil/floor/trunc return types, NaN/Inf handling, octet_length bytes, substr negative length, COLLATE interaction, compileoption funcs); VFS trait examples corrected to include `&Cx`; risk register compaction cross-reference fixed.)*
*Last updated: 2026-02-07*
*Status: Authoritative Specification*
