# Hot-Path Buffer Ownership & Mutability Contract (T6.7.11.1 / bd-1dp9.6.7.11.1)

Status: contract v1 (grounds the copy-reduction work in T6.7.11.2 and later).

Maps the ownership and mutability boundaries for database-page bytes as they
flow along the steady-state hot path — Pager ↔ B-Tree ↔ VDBE on the read side,
and VDBE → B-Tree → Pager → WAL on the write side — so copy reduction can
proceed without breaking snapshot isolation or introducing unsound aliasing.

This is a *contract*, not a refactor: it states, for each interface, what the
buffer's ownership is today, who may mutate it, where a copy is currently
forced and why, and which of those copies are safe to remove versus load-bearing
for correctness.

## 1. Scope

In scope — the page-byte carrying interfaces on the steady-state path:
- Pager read boundary: how a committed page is handed to a reader.
- Pager write/staging boundary: how a dirty page is obtained and published.
- B-Tree cell/payload/record access handed to the VDBE.
- VDBE record decode → `SqliteValue` materialization + the storage-cursor
  decode cache.
- WAL frame-append boundary.

Out of scope: control-plane structures (locks, gates, registries), schema
objects, and MVCC version-chain bookkeeping except where they gate page-byte
aliasing.

## 2. Ownership taxonomy

Every page-byte-carrying value at a boundary is exactly one of:

| Kind | Meaning | May the holder mutate? | May it outlive the producer? |
|------|---------|------------------------|------------------------------|
| **Owned** | A `Vec<u8>` / `Box<[u8]>` the holder solely owns. | Yes | Yes |
| **BorrowGuarded** | `&[u8]` valid only while a lock guard / cursor position is held. | No | No — tied to the guard's lifetime. |
| **ArcShared** | `Arc<[u8]>` / `Arc<PageBytes>` — immutable, shared by many readers. | No (frozen) | Yes — refcounted. |
| **Pooled** | A buffer leased from a reuse pool (page cache / S3-FIFO); recycled when its lease ends. | Yes, while leased | No — must not escape the lease. |
| **CoWForked** | A write buffer forked from a shared page on first write. | Yes (private copy) | Yes |

The copy-reduction target is to move boundaries from **Owned (forced copy)** to
**ArcShared** or **BorrowGuarded** wherever the snapshot-isolation invariant
(§3) still holds.

## 3. Invariants that MUST hold (any refactor preserves these)

- **I1 — Snapshot immutability.** Bytes a reader observes for its snapshot must
  not change under it. A committed page reachable from a reader's snapshot is
  frozen for that reader's lifetime; a writer that needs to change it must
  CoW-fork, never mutate in place.
- **I2 — No pooled buffer escapes its lease.** A `Pooled` buffer must not be
  aliased by any value that outlives the lease; recycling a still-referenced
  buffer is a use-after-free-equivalent data corruption.
- **I3 — No aliasing of an in-flight write buffer with a reader snapshot.** A
  page being mutated on the write path (B-Tree split/insert, dirty staging) must
  not share bytes with any committed snapshot until it is atomically published.
- **I4 — WAL frame stability.** Bytes handed to WAL append must stay stable and
  unaliased until the frame is durable (checksum computed over the exact bytes
  that reach disk).
- **I5 — Decode-cache coherence.** A decode cache that retains borrowed page
  bytes must be invalidated the moment the underlying slot is mutated (a
  same-slot write), or must hold Owned/ArcShared bytes.

## 4. Per-boundary ownership map

The linchpin is **`PageData`** (`fsqlite-types/src/lib.rs:318`), a lazy
copy-on-write page handle: `Owned { bytes: Vec<u8>, shared: OnceLock<Arc<[u8]>> }`
until the first `clone()` promotes it to `Shared(Arc<[u8]>)`. `as_bytes()`
(`:438`) is a shared read view; `as_bytes_mut()` (`:458`) forces CoW via
`Arc::make_mut` and bumps an `image_token` (`:462`) so `(page_no, image_token)`
caches detect the mutation. This single type carries the contract: **the pager
owns immutable Arc images; the B-Tree reads through borrows/`Cow` and mutates
only through CoW-forced private copies, re-publishing by value.**

| # | Boundary (interface) | Buffer type | Kind | Who may mutate | Copy forced? |
|---|---|---|---|---|---|
| B1 | Pager READ — `TransactionHandle::get_page → PageData` (`pager.rs:21939`, `traits.rs:933`) | `PageData` | ArcShared (owned/immutable, never a lock-borrow) | nobody (frozen to reader) | only at I/O materialize (disk `read_page_copy`→`Vec` `:22137`; WAL `from_vec` `:22123`); `get_copy().to_vec()` avoided on hot path |
| B2 | Pager WRITE staging — `write_page(&[u8])` (`pager.rs:22199`) / `write_page_data(PageData)` (`:22242`) | `PageBuf` (or adopted `PageData`) | Pooled, single-owner while staged | writing txn only, pre-publish | `write_page` copies caller slice → `PageBuf` (`:18343`); `write_page_data` adopts w/o copy |
| B3 | Pager PUBLISH — `into_published_page` → `PublishedPagerState.pages` (`pager.rs:17501`, `:11504`) | `PageData` | ArcShared immutable | publisher (seqlock + `publish_lock`) | one copy `PageBuf`→`Arc` for Buffered pages (`:17509`); Owned moved |
| B4 | B-Tree ← Pager — `PageReader::read_page_data → PageData` (`cursor.rs:321`) into `StackEntry.page_data` (`cursor.rs:864`) | `PageData` | ArcShared CoW clone | pager owns; btree only via CoW | no (Arc bump) |
| B5 | B-Tree cell/payload — `read_cell_payload → Cow<[u8]>` (`cursor.rs:4694`); `local_payload` borrow (`cell.rs:740`) | `Cow<'a,[u8]>` | BorrowGuarded (into `page_data`); Owned on overflow | read-only | zero-copy for local; overflow reassembles; owned `payload()` API `.to_vec()` (`cursor.rs:11340`) |
| B6 | B-Tree overflow assembly — `read_overflow_chain → Vec<u8>` (`overflow.rs:38`) | `Vec<u8>` | Owned (fresh) | n/a | yes — non-contiguous span reassembly (prefix variant early-stops, still owns) |
| B7 | B-Tree MUTATE — `PageData::as_bytes_mut` then `write_page_data` (`cursor.rs:6842/6875`) | `&mut [u8]` (CoW) → `PageData` | CoWForked private buffer | btree, on its own copy | CoW on first write; per-cell `.to_vec()` on split (`balance.rs:618`) |
| B8 | Cursor → VDBE record — `payload_into(&mut Vec<u8>)` → cursor-local `payload_buf` (`engine.rs:4065`, `:4358`) | `Vec<u8>` | Owned, cursor-local, reused | owning `StorageCursor` (`&mut`) | yes — page→owned buf so record survives page borrow/cursor move |
| B9 | Record → values — `SqliteValue::Text(SmallText)`/`Blob(Arc<[u8]>)` (`record.rs:2945`, `value.rs:386`) | owned; wide TEXT/BLOB ArcShared | Owned / ArcShared | decode scratch owner | yes — value must outlive cursor position (wide values Arc-shared → later reads O(1)) |
| B10 | VDBE decode cache — `RecordDecodeScratch{header_offsets,values,decoded_mask}` (`record.rs:980`) | owned, per-cursor, never shared | Owned | owning `StorageCursor` | invalidated on any `position_stamp` change incl. same-slot `row_image_epoch` bump (**I5**, `engine.rs:18940`, `cursor.rs:1215`) |
| B11 | WAL append — `append_frame(page_data: &[u8])` → `frame_scratch: Vec<u8>` (`wal.rs:724`, `:194`) | `&[u8]` in → owned `frame_scratch` | Borrowed in; Owned frame buf | `WalFile` (`&mut self`) | yes — header+page contiguity + in-place checksum + single batched write (**I4**) |

### 4.1 Pager read boundary (B1)
`get_page` resolves write-set → per-txn read cache (pins the snapshot, GH#129)
→ published plane (seqlock-rechecked) → shared cache (`get_shared` = one Arc,
then Arc clones) → WAL/disk (owned `Vec`). It **never** returns a `&[u8]` into a
lock guard or pooled `PageBuf`; the lock is dropped before the `PageData` is
returned. Kind = **ArcShared**; I1 holds structurally.

### 4.2 Pager write/staging + publish boundary (B2, B3)
`write_page(&[u8])` copies into a pooled single-owner `PageBuf`
(`StagedPage::Buffered`); `write_page_data(PageData)` adopts an owned image with
no second copy. `try_overwrite_bytes_in_place` mutates a staged buffer only while
`published` is empty and the backing is single-owner (I3). Commit publishes each
staged page as an immutable Arc `PageData` under `publish_lock` + a bumped
sequence — never editing an existing published image (I1).

### 4.3 B-Tree cell / payload / record boundary (B4–B7)
The cursor holds a cloned CoW `PageData` in `StackEntry.page_data`. `CellRef`
(`cell.rs:441`) is an offset/size descriptor that owns no bytes;
`read_cell_payload` returns `Cow::Borrowed` into the page for local payloads and
`Cow::Owned` only for overflow. Mutation is always `as_bytes_mut()` (CoW) →
`clone()` staged snapshot → `write_page_data`; the sole in-place fast path
(`try_mutate_staged_page_data`, `cursor.rs:390`) is gated to **unpublished
write-set pages** (I3).

### 4.4 VDBE record-decode + storage-cursor decode cache (B8–B10)
`payload_into` copies the record into the cursor's reused owned `payload_buf`,
decoupling it from the page borrow the instant it returns; decode then
materializes owned `SqliteValue`s (wide TEXT/BLOB as `Arc<str>`/`Arc<[u8]>`, so
register-write clones are O(1) refcount bumps). The `decoded_mask` gates lazy
per-column re-decode within a stable position; **I5** is already enforced by
`test_storage_cursor_same_slot_write_mutation_invalidates_cached_text`.

### 4.5 WAL frame-append boundary (B11)
Append borrows `&[u8]` and copies into the WAL-owned reused `frame_scratch`; the
checksum is computed over the assembled contiguous frame and frozen into header
bytes 16..24, then written in one I/O. **I4** requires this copy: the
checksummed bytes and the written bytes must be one immutable snapshot the pager
is free to mutate/evict concurrently.

## 5. Copy-forcing boundaries (AC #4)

| Copy site | Boundary | Reason | Invariant it protects | Reducible? |
|---|---|---|---|---|
| disk `read_page_copy`→`Vec`, WAL `from_vec` | B1 | I/O materialization from backend into memory | — (mechanical) | No — genuine I/O read. |
| `ShardedPageCache::get_copy().to_vec()` | B1 | owned-`Vec` cache API | — | **Already avoided** on the hot path via `get_shared` (Arc). Audit remaining callers. |
| `PageBuf`→`Arc` at publish | B3 | freeze pooled buffer into shared image; `PageBuf` must return to pool | I1, I2 | **Candidate** — a pool-returning `Arc` (custom allocator/`Arc<PageBuf>`) could avoid the memcpy; must keep I2 (lease non-escape). |
| `write_page(&[u8])` copy-in | B2 | caller only lends the bytes | I3 | **Partly** — callers holding an owned `PageData` should use `write_page_data` (already zero-copy). |
| owned `payload()` `.to_vec()` | B5 | owned-`Vec` API when a local cell is borrowable | — | **Yes** — route callers to `rowid_and_payload_cow` / `payload_into` to keep the `Cow::Borrowed`. |
| overflow reassembly `Vec` | B6 | payload spans non-contiguous overflow pages | — (physical layout) | No for the full payload; the prefix-bounded variant already early-stops. |
| `payload_into` page→`payload_buf` | B8 | record must survive the page borrow across the next `Next`/write | I1, I5 | **Candidate** — a record could borrow the Arc-shared page by keeping the `PageData` clone alive for the register lifetime; gated by I5 (decode cache) and pins the page longer. |
| decode → owned `SqliteValue` | B9 | value outlives the cursor position (register lifetime) | I5 | Partly done — wide values are Arc-shared; short values are inline (cheap). |
| WAL `frame_scratch` copy | B11 | checksum-over-contiguous-frame + in-flight-write stability | I4 | No — load-bearing for durability correctness. |
| split per-cell `.to_vec()` + fresh page `Vec` | B7 | building new pages during balance | I1, I3 | Low priority — split is a rare, structurally copy-heavy path. |

**Reduction priorities for T6.7.11.2**, in order of payoff × safety: (1) the
owned `payload()` `.to_vec()` (B5) — pure API friction, no invariant risk;
(2) the publish `PageBuf`→`Arc` memcpy (B3) — one per committed page, needs an
I2-safe pool-returning Arc; (3) the `payload_into` copy (B8) — largest but gated
by I5 and page-pinning trade-offs. B11, B6, and the I/O reads are load-bearing
and out of scope for reduction.

## 6. Structured copy-boundary diagnostics (AC #4)

**Partly already present.** The static copy-boundary map is §5 above. At runtime,
the B-Tree already instruments its copy sites — `record_owned_payload_materialization`
(overflow/owned-`payload` copies) and `record_local_payload_copy`
(`cursor.rs:11339, 4648, 10699`, `overflow.rs:49`) — and the pager cache
distinguishes `get_shared` (Arc) from `get_copy` (`.to_vec`). The plan below
unifies these under one target so a single profile run attributes every hot-path
page-byte copy to a boundary in §4's B1–B11 scheme.

A `tracing` target `fsqlite_pager::copy_boundary` (and peers in btree/vdbe/wal)
emits one event per forced page-byte copy on the hot path, so a profile run can
attribute copies to boundaries:

- fields: `boundary` (enum: pager_read | overflow_reassembly | record_materialize
  | wal_frame | decode_cache_fill), `page` (page number), `bytes` (copied length),
  `reason` (enum: snapshot_isolation | lock_lifetime | overflow_span |
  checksum_stability | value_outlives_cursor).
- gated behind the existing metrics-off-hot-path flag so it is zero-cost when
  disabled (mirror `test_vdbe_metrics_can_be_disabled_off_hot_path`).
- a `copy_boundary_snapshot()` accessor returns per-boundary copy counts + bytes
  for a test/bench to assert "boundary X forced N copies of B bytes".

## 7. Invariant-guarding tests (AC #2)

Deterministic tests (MemoryVfs, no wall-clock) that fail if a refactor breaks an
invariant:

- **T-I1 snapshot immutability:** open a read snapshot over page N, commit a
  writer that modifies page N, assert the reader still sees the original bytes
  (the writer CoW-forked, did not mutate in place).
- **T-I2 pooled-lease non-escape:** (compile-time where possible via lifetimes;
  runtime) assert a pooled page buffer's contents are unchanged across a
  recycle-triggering allocation while a lease is held.
- **T-I3 write/read disjointness:** during a B-Tree split, assert the committed
  snapshot's bytes for the split page are unchanged until publish.
- **T-I4 WAL frame stability:** mutate a page after handing it to WAL append;
  assert the appended frame's checksum matches the bytes-at-append, not the
  mutated bytes.
- **T-I5 decode-cache coherence:** already covered by
  `test_storage_cursor_same_slot_write_mutation_invalidates_cached_text`
  (fsqlite-vdbe) — this contract adopts it as the I5 guard.

## 8. How this guides implementation (AC #3)

T6.7.11.2 (hot-path copy reduction) may only convert an **Owned (forced copy)**
boundary to **ArcShared**/**BorrowGuarded** when: (a) the source page is on the
committed (immutable) snapshot (I1), (b) no pooled buffer would escape its lease
(I2), and (c) the value's required lifetime does not exceed the borrow's (I5).
Every such conversion must be accompanied by the matching §7 invariant test
staying green and a §6 diagnostic showing the copy count drop.
