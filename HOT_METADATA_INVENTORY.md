# Hot-Metadata Inventory — bd-db300.5.3.2.1

> Code-indexed inventory of every metadata class on the hot path.
> Drives primitive selection in bd-db300.5.3.2.2.

---

## M1: Pager Published State (`PublishedPagerState`)

**File:** `crates/fsqlite-pager/src/pager.rs:1234-1251`
**Struct:** `PublishedPagerState`
**Scope:** Per-pager (shared across all connections to same database file)

**Fields (all atomic):**
| Field | Type | Line |
|-------|------|------|
| `sequence` | `AtomicU64` | 1239 |
| `visible_commit_seq` | `AtomicU64` | 1240 |
| `db_size` | `AtomicU32` | 1241 |
| `journal_mode` | `AtomicU8` | 1242 |
| `freelist_count` | `AtomicUsize` | 1243 |
| `checkpoint_active` | `AtomicBool` | 1244 |
| `page_set_size` | `AtomicUsize` | 1245 |
| `publish_lock` | `Mutex<()>` | 1236 |
| `sequence_gate` | `Mutex<()>` | 1237 |
| `sequence_cv` | `Condvar` | 1238 |
| `pages` | `ShardedPublishedPages` | 1250 |

**Read path:** `PagerPublishedSnapshot::read()` — seqlock pattern: load `sequence` (must be even), load all fields, re-check `sequence`. Retry on odd or mismatch.
**Write path:** `publish_committed_state()` — takes `publish_lock`, bumps `sequence` odd, writes all fields, bumps `sequence` even, notifies `sequence_cv`.
**Read/write ratio:** ~100:1 (every connection begin reads; only commit writes)
**Reader retryability:** YES — seqlock retry is bounded (typically 0-1 retries)
**Invalidation trigger:** Every successful commit (`txn.commit()` in pager)
**Reclamation difficulty:** LOW — atomic overwrites, no allocation
**Topology sensitivity:** HIGH — `publish_lock` Mutex and `sequence` atomic are cross-core contention points. Under c8+ with remote NUMA, the Mutex + Condvar wake creates cross-node cache-line bouncing on `publish_lock`, `sequence`, and `sequence_cv`.
**Weak cells:** c4/hot-page (publish_lock contention); c1 (Mutex overhead even uncontested)

---

## M2: Connection Pager Publication Binding (`BoundPagerPublication`)

**File:** `crates/fsqlite-core/src/connection.rs:3009-3012`
**Struct:** `BoundPagerPublication`
**Scope:** Per-connection (local, not shared)

**Fields:**
| Field | Type | Line |
|-------|------|------|
| `snapshot` | `PagerPublishedSnapshot` | 3010 |
| `read_retry_count` | `u64` | 3011 |

**Read path:** Created during `refresh_memdb_if_stale_with_publication()` (connection.rs:5187). Every autocommit statement on file-backed databases reads M1 → produces M2.
**Write path:** Constructed once per statement entry; not updated.
**Read/write ratio:** Write-once-read-many within a statement. Constructed ~1x per statement for file-backed DBs. **Skipped entirely for :memory:** (the `is_memory()` fast-path bypasses publication binding).
**Reader retryability:** N/A — local struct
**Invalidation trigger:** Each new statement creates a fresh binding. Stale if `visible_commit_seq` has advanced between creation and use.
**Reclamation difficulty:** ZERO — stack-allocated Copy type
**Topology sensitivity:** NONE — per-connection local data
**Weak cells:** c1/c4 file-backed (refresh + Mutex entry on M1 per-statement); **not a bottleneck for :memory:**

---

## M3: Schema Cookie & Generation

**Files:**
- `crates/fsqlite-core/src/connection.rs:3147` — `schema_cookie: RefCell<u32>`
- `crates/fsqlite-core/src/connection.rs:3150` — `schema_generation: Cell<u64>`
- `crates/fsqlite-core/src/connection.rs:1668-1676` — `PreparedStatementTemplate` caches both
**Scope:** Per-connection

**Read path:**
- Every `PreparedStatement::execute/query` checks `conn.schema_cookie() != self.schema_cookie || conn.schema_generation() != self.schema_generation` (connection.rs:1960-1961).
- Cached read/write transaction reuse checks `cached_write_txn_cookie == schema_cookie` (connection.rs:11375, 11385).
**Write path:**
- DDL operations (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW`) increment `schema_cookie` and bump `schema_generation`.
- Cross-connection schema reload bumps `schema_generation` without changing `schema_cookie`.
**Read/write ratio:** ~1000:1 (every statement reads; only DDL writes)
**Reader retryability:** YES — on mismatch, PreparedStatement invalidates and re-prepares
**Invalidation trigger:** DDL operations, cross-connection schema reload
**Reclamation difficulty:** ZERO — scalar values
**Topology sensitivity:** NONE — per-connection RefCell/Cell, no cross-thread sharing
**Weak cells:** Not a bottleneck. Constant-time check per statement.

---

## M4: Cached Read Snapshot

**File:** `crates/fsqlite-core/src/connection.rs:3050-3053`
**Fields:**
| Field | Type | Line |
|-------|------|------|
| `cached_read_snapshot` | `RefCell<Option<Box<dyn TransactionHandle>>>` | 3050 |
| `cached_read_snapshot_cookie` | `Cell<u32>` | 3053 |
**Scope:** Per-connection

**Read path:** `ensure_autocommit_txn_mode_with_cx()` checks if available + cookie matches (connection.rs:11399-11413). If match → take ownership, skip `pager.begin()`.
**Write path:** `resolve_autocommit_txn_with_capture_and_cx()` parks read-only txn (connection.rs:11580-11601). Invalidated by any write operation.
**Read/write ratio:** ~1:1 (parked after each read stmt, consumed by next)
**Reader retryability:** YES — on stale cookie, invalidates and creates fresh txn
**Invalidation trigger:** Any write commit, DDL, explicit BEGIN, schema change
**Reclamation difficulty:** LOW — `rollback()` on the transaction handle, then drop
**Topology sensitivity:** NONE — per-connection local
**Weak cells:** c1 read-heavy (eliminates pager.begin per-statement). Already measured as effective.

---

## M5: Cached Write Transaction (`:memory:` fast-path)

**File:** `crates/fsqlite-core/src/connection.rs:3060-3062`
**Fields:**
| Field | Type | Line |
|-------|------|------|
| `cached_write_txn` | `RefCell<Option<Box<dyn TransactionHandle>>>` | 3060 |
| `cached_write_txn_cookie` | `Cell<u32>` | 3062 |
**Scope:** Per-connection, `:memory:` only

**Read path:** `ensure_autocommit_txn_mode_with_cx()` checks `is_memory() && mode != ReadOnly && cached_write_txn.is_some() && cookie match` (connection.rs:11372-11384). If match → reuse, skip `pager.begin()` + Mutex.
**Write path:** `resolve_autocommit_txn_with_capture_and_cx()` calls `txn.commit_and_retain()` → parks transaction (connection.rs:11672-11700). Parks via `pager.rs:4652-4809`.
**Read/write ratio:** ~1:1 (parked after each write, consumed by next)
**Reader retryability:** YES — stale cookie → invalidate + fresh begin
**Invalidation trigger:** DDL, explicit BEGIN, schema change (`invalidate_cached_write_txn` at connection.rs:11513)
**Reclamation difficulty:** LOW — `commit()` then drop
**Topology sensitivity:** NONE — per-connection local, :memory: only
**Weak cells:** c1 :memory: write-heavy (this IS the fix for the 59x pager ceremony overhead). Counters: `FSQLITE_CACHED_WRITE_TXN_REUSES`, `FSQLITE_CACHED_WRITE_TXN_PARKS`.

---

## M6: PagerInner Committed State

**File:** `crates/fsqlite-pager/src/pager.rs:296-330`
**Struct:** `PagerInner<F: VfsFile>` (behind `Mutex<PagerInner>`)
**Scope:** Per-pager (shared via `Arc<Mutex<PagerInner>>`)

**Key hot fields:**
| Field | Type | Line | Hot-path role |
|-------|------|------|---------------|
| `commit_seq` | `CommitSeq` | 325 | ABA detection, staleness check |
| `db_size` | `u32` | 302 | Page-count header maintenance |
| `writer_active` | `bool` | 306 | Writer exclusion gate |
| `active_transactions` | `u32` | 308 | Reference counting |
| `checkpoint_active` | `bool` | 310 | Checkpoint exclusion |
| `freelist` | `Vec<PageNumber>` | 315 | Allocation source |
| `committed_db_file_size_bytes` | `u64` | 327 | Staleness detection |

**Read path:** `pager.begin()` takes the Mutex, reads `commit_seq`, `writer_active`, `active_transactions`. Every txn open/close touches this Mutex.
**Write path:** `txn.commit()` takes the Mutex, updates `commit_seq`, `db_size`, `writer_active`, freelist.
**Read/write ratio:** ~2:1 (begin + commit both take Mutex; reads dominate slightly)
**Reader retryability:** NO — `Mutex::lock()` is blocking
**Invalidation trigger:** Every commit, rollback, or checkpoint
**Reclamation difficulty:** LOW — scalar fields. `freelist` Vec may hold pages but is cheap to clear.
**Topology sensitivity:** **VERY HIGH** — this Mutex is THE contention bottleneck at c4+. Every pager.begin() and pager.commit() serializes through this lock. The cached read/write transaction fast-paths (M4, M5) exist specifically to avoid this Mutex.
**Weak cells:** c1/c4 file-backed write-heavy (Mutex contention on every statement pair). This is the primary target for M5 (:memory:) and for the "immutable metadata publication" structural pillar.

---

## M7: WAL Frame Count & Generation

**File:** `crates/fsqlite-wal/src/wal.rs:82-99`
**Struct:** `WalFile<F: VfsFile>`
**Scope:** Per-WAL-handle (one per connection's WAL access)

**Key fields:**
| Field | Type | Line |
|-------|------|------|
| `header` | `WalHeader` | 86 |
| `running_checksum` | `SqliteWalChecksum` | 88 |
| `frame_count` | `usize` | 90 |
| `last_commit_frame` | `Option<usize>` | 92 |
| `frame_scratch` | `Vec<u8>` | 98 |

**Read path:** `refresh()` (wal.rs:107) reads disk WAL header + frames to resync `frame_count` and `running_checksum`. Called from `PagerInner::probe_visible_commit_seq()`.
**Write path:** `append_frames()` (wal.rs:1001) appends frames, updates `frame_count`, `running_checksum`, `last_commit_frame`.
**Read/write ratio:** ~1:1 for active writers; read-only connections call `refresh()` to discover new frames
**Reader retryability:** YES — `refresh()` re-reads from disk on generation change or frame-count advance
**Invalidation trigger:** New frame append by any writer, checkpoint reset (generation change)
**Reclamation difficulty:** MEDIUM — `frame_scratch` Vec is reused, not reallocated. Generation changes require full re-scan.
**Topology sensitivity:** LOW for per-handle fields. The WAL FILE is shared, but each handle has its own `frame_count`/`running_checksum` copy resynchronized via `refresh()`.
**Weak cells:** c4/c8 file-backed (refresh overhead when multiple writers commit between reads)

---

## M8: WAL Generation Identity

**File:** `crates/fsqlite-wal/src/wal.rs:59-65`
**Struct:** `WalGenerationIdentity`
**Scope:** Per-WAL-handle (derived from WAL header)

**Fields:**
| Field | Type | Line |
|-------|------|------|
| `checkpoint_seq` | `u32` | 62 |
| `salts` | `WalSalts` | 64 |

**Read path:** Compared during `refresh()` to detect generation changes (wal.rs:161). If salts or checkpoint_seq changed → full rebuild of frame_count and running_checksum.
**Write path:** Set on WAL creation or checkpoint reset.
**Read/write ratio:** ~100:1 (checked on every refresh; changes only on checkpoint)
**Reader retryability:** YES — generation change triggers full WAL rescan
**Invalidation trigger:** Checkpoint completion (WAL reset)
**Reclamation difficulty:** ZERO — Copy struct, 12 bytes
**Topology sensitivity:** NONE — per-handle local copy
**Weak cells:** Not a direct bottleneck. Checkpoint frequency determines generation turnover.

---

## M9: MemDB Visible Commit Seq (Staleness Gate)

**File:** `crates/fsqlite-core/src/connection.rs:3191`
**Field:** `memdb_visible_commit_seq: RefCell<CommitSeq>`
**Scope:** Per-connection

**Read path:** `refresh_memdb_if_stale()` compares against M1's `visible_commit_seq` to decide whether to reload MemDatabase from pager (connection.rs:5199-5237).
**Write path:** `advance_commit_clock()` updates after each local commit (connection.rs:~4967).
**Read/write ratio:** ~10:1 (checked per-statement on file-backed; updated per local commit)
**Reader retryability:** N/A — comparison triggers reload
**Invalidation trigger:** Another connection's commit advancing the global `visible_commit_seq` beyond this connection's cached value
**Reclamation difficulty:** ZERO — u64
**Topology sensitivity:** NONE — per-connection local. But the *comparison target* (M1) is shared.
**Weak cells:** c4 file-backed (every statement reads M1 to check staleness → Mutex/seqlock cost)

---

## M10: Cached VDBE Engine

**File:** `crates/fsqlite-core/src/connection.rs:3063-3065` (approx, near `cached_vdbe_engine`)
**Field:** `cached_vdbe_engine: RefCell<Option<VdbeEngine>>`
**Scope:** Per-connection

**Read path:** Each statement execution tries to take the cached engine (avoids 21+ collection allocs).
**Write path:** After execution, engine is `reset()` and parked.
**Read/write ratio:** 1:1 (take → execute → park)
**Reader retryability:** N/A — local
**Invalidation trigger:** None explicit — always reused. Engine `reset()` clears state but retains allocations.
**Reclamation difficulty:** LOW — `reset()` clears Vecs/HashMaps in-place
**Topology sensitivity:** NONE — per-connection local
**Weak cells:** c1 (eliminates VdbeEngine allocation per-statement). Already effective.

---

## M11: Concurrent Registry & Lock Table

**Files:**
- `crates/fsqlite-core/src/connection.rs:3173` — `concurrent_registry: Arc<Mutex<ConcurrentRegistry>>`
- `crates/fsqlite-core/src/connection.rs:3184` — `concurrent_lock_table: Arc<InProcessPageLockTable>`
- `crates/fsqlite-core/src/connection.rs:3186` — `concurrent_commit_index: Arc<CommitIndex>`
**Scope:** Per-database (shared across connections)

**Read path:** `begin_concurrent()` takes `concurrent_registry` Mutex, allocates session. `InProcessPageLockTable` fast-path uses CAS on flat array (core_types.rs:589-607).
**Write path:** `begin_concurrent()` + `commit()` both mutate registry. Page locks use CAS.
**Read/write ratio:** ~1:1 (each txn begin + commit touches registry)
**Reader retryability:** Page locks: YES (CAS retry). Registry: NO (Mutex blocks).
**Invalidation trigger:** Session completion releases locks and registry entry
**Reclamation difficulty:** MEDIUM — session tracking, page lock cleanup on abort
**Topology sensitivity:** **HIGH** — Registry Mutex is global contention point. `InProcessPageLockTable` CAS on fast array causes cache-line bouncing under c8+ hot-page contention.
**Weak cells:** c4/c8 hot-page (page lock CAS contention). **Bypassed entirely for :memory:** via `is_memory()` fast-path.

---

## M12: Parse Cache & Compiled Statement Cache

**Files:**
- Connection struct fields (not individually named in struct; accessed via `prepare()`)
- `crates/fsqlite-parser/src/parser.rs` — parser output caching
**Scope:** Per-connection

**Read/write ratio:** ~10:1 (PreparedStatement re-execution checks cache hit; misses trigger parse+compile)
**Reader retryability:** YES — miss triggers full re-parse
**Invalidation trigger:** Schema cookie/generation change (M3)
**Reclamation difficulty:** LOW — LRU eviction
**Topology sensitivity:** NONE — per-connection
**Weak cells:** c1 with high statement variety (cache miss rate). Not a bottleneck for parameterized prepared statements.

---

## Summary: Contention Heat Map

| Class | Shared? | Lock type | c1 impact | c4 impact | c8 impact | :memory: bypassed? |
|-------|---------|-----------|-----------|-----------|-----------|-------------------|
| M1 (PublishedPagerState) | Per-pager | Seqlock + Mutex | Moderate | **High** | **Very High** | Yes (:memory: skips publication) |
| M2 (BoundPagerPublication) | Local | None | Low | Low | Low | Yes |
| M3 (Schema cookie) | Local | RefCell | Negligible | Negligible | Negligible | No |
| M4 (Cached read snapshot) | Local | RefCell | **Positive** | Positive | Positive | No |
| M5 (Cached write txn) | Local | RefCell | **Positive** | Positive | N/A (:memory: only) | N/A |
| M6 (PagerInner) | Per-pager | **Mutex** | **High** | **Very High** | **Very High** | Partially (M5 avoids) |
| M7 (WAL frame count) | Per-handle | None | Low | Moderate | Moderate | N/A (no WAL for :memory:) |
| M8 (WAL generation) | Per-handle | None | Negligible | Negligible | Negligible | N/A |
| M9 (Staleness gate) | Local | RefCell | Low | Moderate | Moderate | No |
| M10 (Cached VDBE) | Local | RefCell | **Positive** | Positive | Positive | No |
| M11 (Concurrent registry) | Per-database | Mutex + CAS | Bypassed | **High** | **Very High** | Yes (:memory: bypasses) |
| M12 (Parse cache) | Local | RefCell | Low | Low | Low | No |

---

## Primary Targets for Primitive Selection (bd-db300.5.3.2.2)

**Target 1: M6 (PagerInner Mutex)** — The single biggest hot-path serialization point for file-backed databases. Every `pager.begin()` and `pager.commit()` holds this Mutex. Candidate primitives: RCU for read-side fields, split Mutex (read-state vs write-state), seqlock for commit_seq/db_size.

**Target 2: M1 (PublishedPagerState)** — Already uses seqlock pattern but adds `publish_lock` Mutex on the write side. Under c8+, `sequence_cv` Condvar wake creates cross-node traffic. Candidate: eliminate Condvar in favor of polling, or epoch-based publication.

**Target 3: M11 (ConcurrentRegistry Mutex)** — Global Mutex for session lifecycle. Candidate: sharded registry per-core, or lock-free session allocation with epoch-based reclamation.

---

## Logging Contract for Later Verification

Each metadata access on the hot path should emit (when tracing is enabled):

```
trace_id, metadata_class, operation (read|write|retry|invalidate|park|reuse),
lock_wait_ns, retry_count, stale_detected, topology_node_id,
caller_file, caller_line
```

For M1 specifically (highest interference risk):
```
publication_write_count, read_retry_count, sequence_value,
visible_commit_seq, publish_lock_wait_ns
```

These fields are required by bd-db300.7.5.5 (regime atlas) and bd-db300.7.5.6 (shadow-oracle verification).
