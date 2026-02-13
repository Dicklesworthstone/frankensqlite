//! Criterion micro-benchmarks for MVCC operations (bd-t6sv2.2).
//!
//! Benchmarks:
//! - Page lock acquire/release latency (InProcessPageLockTable)
//! - Version arena alloc/get/free cycle
//! - CommitIndex update/lookup
//! - FCW validation (clean path, conflict path)
//! - Conflict observer overhead (on vs off)
//! - GC prune_page_chain (version chain garbage collection)
//! - SSI edge discovery

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use fsqlite_mvcc::{
    ActiveTxnView, BeginKind, CommitIndex, CommittedWriterInfo, ConcurrentRegistry, DiscoveredEdge,
    GcTodo, InProcessPageLockTable, TransactionManager, VersionArena, VersionIdx,
    discover_incoming_edges, discover_outgoing_edges, gc_tick, prune_page_chain,
    validate_first_committer_wins,
};
use fsqlite_observability::{ConflictObserver, MetricsObserver};
use fsqlite_types::{
    CommitSeq, PageData, PageNumber, PageNumberBuildHasher, PageSize, PageVersion, SchemaEpoch,
    Snapshot, TxnEpoch, TxnId, TxnToken, VersionPointer, WitnessKey,
};

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn page(n: u32) -> PageNumber {
    PageNumber::new(n).unwrap()
}

fn txn(n: u64) -> TxnId {
    TxnId::new(n).unwrap()
}

fn make_page_version(pgno: u32, seq: u64) -> PageVersion {
    PageVersion {
        pgno: page(pgno),
        commit_seq: CommitSeq::new(seq),
        created_by: TxnToken::new(txn(seq), TxnEpoch::new(1)),
        data: PageData::zeroed(PageSize::default()),
        prev: None,
    }
}

fn seeded_scan_manager(start_page: u32, end_page: u32) -> TransactionManager {
    let mut manager = TransactionManager::new(PageSize::DEFAULT);
    manager.set_txn_max_duration_ms(u64::MAX);

    let mut writer = manager
        .begin(BeginKind::Immediate)
        .expect("seed writer should begin");
    for raw_page in start_page..=end_page {
        manager
            .write_page(
                &mut writer,
                page(raw_page),
                PageData::zeroed(PageSize::default()),
            )
            .expect("seed write should succeed");
    }
    manager
        .commit(&mut writer)
        .expect("seed commit should succeed");
    manager
}

fn checksum_visible_rows(rows: &[(PageNumber, Option<PageData>)]) -> u64 {
    let mut checksum = 0_u64;
    for (_, page_data) in rows {
        if let Some(page_data) = page_data {
            for byte in page_data.as_bytes() {
                checksum = checksum.wrapping_add(u64::from(*byte));
            }
        }
    }
    checksum
}

fn consume_rows_like_oltp(rows: &[(PageNumber, Option<PageData>)]) -> u64 {
    let mut folded = 0_u64;
    for _ in 0..4 {
        folded = folded.rotate_left(7) ^ checksum_visible_rows(rows);
    }
    folded
}

// ---------------------------------------------------------------------------
// Lock table benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: acquire and release a page lock (uncontended).
fn bench_lock_acquire_release(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_table/acquire_release");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    group.bench_function("uncontended", |b| {
        let table = InProcessPageLockTable::new();
        let p = page(42);
        let t = txn(1);
        b.iter(|| {
            table.try_acquire(black_box(p), black_box(t)).unwrap();
            table.release(black_box(p), black_box(t));
        });
    });

    group.finish();
}

/// Benchmark: acquire locks on many distinct pages (scaling).
fn bench_lock_acquire_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_table/acquire_scaling");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_pages in &[100_u32, 1000, 10_000] {
        group.throughput(Throughput::Elements(u64::from(n_pages)));
        group.bench_with_input(BenchmarkId::new("pages", n_pages), &n_pages, |b, &count| {
            b.iter_batched(
                InProcessPageLockTable::new,
                |table| {
                    let t = txn(1);
                    for i in 1..=count {
                        table.try_acquire(page(i), t).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark: lock contention (lock held by another txn).
fn bench_lock_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_table/contention");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    group.bench_function("contended", |b| {
        let table = InProcessPageLockTable::new();
        let p = page(42);
        let holder = txn(1);
        let requester = txn(2);
        table.try_acquire(p, holder).unwrap();

        b.iter(|| {
            let result = table.try_acquire(black_box(p), black_box(requester));
            black_box(result).ok();
        });
    });

    group.finish();
}

/// Benchmark: conflict observer overhead (with vs without observer).
fn bench_lock_observer_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_table/observer_overhead");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    // Without observer: acquire contended lock.
    group.bench_function("no_observer", |b| {
        let table = InProcessPageLockTable::new();
        let p = page(42);
        table.try_acquire(p, txn(1)).unwrap();

        b.iter(|| {
            let _ = table.try_acquire(black_box(p), black_box(txn(2)));
        });
    });

    // With observer: acquire contended lock (observer callback fires).
    group.bench_function("with_observer", |b| {
        let obs = Arc::new(MetricsObserver::new(1024));
        let table = InProcessPageLockTable::with_observer(obs as Arc<dyn ConflictObserver>);
        let p = page(42);
        table.try_acquire(p, txn(1)).unwrap();

        b.iter(|| {
            let _ = table.try_acquire(black_box(p), black_box(txn(2)));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Version arena benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: version arena alloc/get cycle.
fn bench_arena_alloc_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("version_arena/alloc_get");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_versions in &[100_u32, 1000, 10_000] {
        group.throughput(Throughput::Elements(u64::from(n_versions)));
        group.bench_with_input(
            BenchmarkId::new("versions", n_versions),
            &n_versions,
            |b, &count| {
                b.iter_batched(
                    VersionArena::new,
                    |mut arena| {
                        let mut indices = Vec::with_capacity(count as usize);
                        for i in 1..=u64::from(count) {
                            #[allow(clippy::cast_possible_truncation)]
                            let idx = arena.alloc(make_page_version(i as u32, i));
                            indices.push(idx);
                        }
                        // Read back all versions.
                        for idx in &indices {
                            black_box(arena.get(*idx));
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: version arena alloc/free/realloc cycle (free list).
fn bench_arena_free_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("version_arena/free_list");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1000));

    group.bench_function("alloc_free_realloc_1000", |b| {
        b.iter_batched(
            || {
                let mut arena = VersionArena::new();
                let indices: Vec<_> = (1..=1000_u32)
                    .map(|i| arena.alloc(make_page_version(i, u64::from(i))))
                    .collect();
                (arena, indices)
            },
            |(mut arena, indices)| {
                // Free all.
                for idx in &indices {
                    arena.free(*idx);
                }
                // Realloc (hits free list).
                for i in 1..=1000_u32 {
                    black_box(arena.alloc(make_page_version(i, u64::from(i))));
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Commit index benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: commit index update and lookup.
fn bench_commit_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_index");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_pages in &[100_u32, 1000, 10_000] {
        group.throughput(Throughput::Elements(u64::from(n_pages)));

        // Update benchmark.
        group.bench_with_input(
            BenchmarkId::new("update", n_pages),
            &n_pages,
            |b, &count| {
                b.iter_batched(
                    CommitIndex::new,
                    |index| {
                        for i in 1..=count {
                            index.update(page(i), CommitSeq::new(u64::from(i)));
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Lookup benchmark (pre-populated).
        group.bench_with_input(
            BenchmarkId::new("lookup", n_pages),
            &n_pages,
            |b, &count| {
                let index = CommitIndex::new();
                for i in 1..=count {
                    index.update(page(i), CommitSeq::new(u64::from(i)));
                }
                b.iter(|| {
                    for i in 1..=count {
                        black_box(index.latest(page(i)));
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// FCW validation benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: first-committer-wins validation (clean path, no conflicts).
fn bench_fcw_clean(c: &mut Criterion) {
    let mut group = c.benchmark_group("fcw_validation/clean");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_pages in &[10_u32, 100, 1000] {
        group.throughput(Throughput::Elements(u64::from(n_pages)));
        group.bench_with_input(BenchmarkId::new("pages", n_pages), &n_pages, |b, &count| {
            b.iter_batched(
                || {
                    let commit_index = CommitIndex::new();
                    for i in 1..=count {
                        commit_index.update(page(i), CommitSeq::new(1));
                    }
                    let mut registry = ConcurrentRegistry::new();
                    let snapshot = Snapshot::new(CommitSeq::new(1), SchemaEpoch::ZERO);
                    let session_id = registry.begin_concurrent(snapshot).unwrap();

                    let lock_table = InProcessPageLockTable::new();
                    let handle = registry.get_mut(session_id).unwrap();
                    for i in 1..=count {
                        let data = PageData::zeroed(PageSize::default());
                        fsqlite_mvcc::concurrent_write_page(
                            handle,
                            &lock_table,
                            session_id,
                            page(i),
                            data,
                        )
                        .unwrap();
                    }
                    (registry, session_id, commit_index)
                },
                |(registry, session_id, commit_index)| {
                    let handle = registry.get(session_id).unwrap();
                    black_box(validate_first_committer_wins(handle, &commit_index));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// Benchmark: FCW validation with conflicts on every page.
fn bench_fcw_conflict(c: &mut Criterion) {
    let mut group = c.benchmark_group("fcw_validation/conflict");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_pages in &[10_u32, 100, 1000] {
        group.throughput(Throughput::Elements(u64::from(n_pages)));
        group.bench_with_input(BenchmarkId::new("pages", n_pages), &n_pages, |b, &count| {
            b.iter_batched(
                || {
                    let commit_index = CommitIndex::new();
                    for i in 1..=count {
                        commit_index.update(page(i), CommitSeq::new(5));
                    }
                    let mut registry = ConcurrentRegistry::new();
                    let snapshot = Snapshot::new(CommitSeq::new(1), SchemaEpoch::ZERO);
                    let session_id = registry.begin_concurrent(snapshot).unwrap();

                    let lock_table = InProcessPageLockTable::new();
                    let handle = registry.get_mut(session_id).unwrap();
                    for i in 1..=count {
                        let data = PageData::zeroed(PageSize::default());
                        fsqlite_mvcc::concurrent_write_page(
                            handle,
                            &lock_table,
                            session_id,
                            page(i),
                            data,
                        )
                        .unwrap();
                    }
                    (registry, session_id, commit_index)
                },
                |(registry, session_id, commit_index)| {
                    let handle = registry.get(session_id).unwrap();
                    black_box(validate_first_committer_wins(handle, &commit_index));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// GC benchmarks
// ---------------------------------------------------------------------------

/// Build a version chain of `depth` versions for page `pgno` in the arena.
///
/// Returns the chain head index. Versions are linked via `prev` pointers
/// with commit sequences from `depth` down to 1.
fn build_version_chain(
    arena: &mut VersionArena,
    chain_heads: &mut HashMap<PageNumber, VersionIdx, PageNumberBuildHasher>,
    pgno: PageNumber,
    depth: u32,
) {
    let mut prev_ptr = None;
    // Build from oldest (seq=1) to newest (seq=depth), chaining prev pointers.
    for seq in 1..=depth {
        let version = PageVersion {
            pgno,
            commit_seq: CommitSeq::new(u64::from(seq)),
            created_by: TxnToken::new(txn(u64::from(seq)), TxnEpoch::new(1)),
            data: PageData::zeroed(PageSize::default()),
            prev: prev_ptr,
        };
        let idx = arena.alloc(version);
        prev_ptr = Some(VersionPointer::new(
            (u64::from(idx.chunk()) << 32) | u64::from(idx.offset()),
        ));
        // The last insertion is the chain head.
        chain_heads.insert(pgno, idx);
    }
}

/// Benchmark: GC prune_page_chain at various chain depths.
fn bench_gc_prune(c: &mut Criterion) {
    let mut group = c.benchmark_group("gc/prune_page_chain");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &chain_depth in &[5_u32, 20, 100] {
        group.throughput(Throughput::Elements(u64::from(chain_depth)));
        group.bench_with_input(
            BenchmarkId::new("depth", chain_depth),
            &chain_depth,
            |b, &depth| {
                b.iter_batched(
                    || {
                        let mut arena = VersionArena::new();
                        let mut chain_heads =
                            HashMap::with_hasher(PageNumberBuildHasher::default());
                        build_version_chain(&mut arena, &mut chain_heads, page(1), depth);
                        (arena, chain_heads)
                    },
                    |(mut arena, mut chain_heads)| {
                        // Prune everything below horizon (keep only the newest).
                        let horizon = CommitSeq::new(u64::from(depth) - 1);
                        black_box(prune_page_chain(
                            page(1),
                            horizon,
                            &mut arena,
                            &mut chain_heads,
                        ));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: gc_tick with multiple pages queued.
fn bench_gc_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("gc/gc_tick");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_pages in &[10_u32, 100, 500] {
        let chain_depth = 10_u32;
        group.throughput(Throughput::Elements(u64::from(n_pages)));
        group.bench_with_input(BenchmarkId::new("pages", n_pages), &n_pages, |b, &count| {
            b.iter_batched(
                || {
                    let mut arena = VersionArena::new();
                    let mut chain_heads = HashMap::with_hasher(PageNumberBuildHasher::default());
                    let mut todo = GcTodo::new();
                    for i in 1..=count {
                        build_version_chain(&mut arena, &mut chain_heads, page(i), chain_depth);
                        todo.enqueue(page(i));
                    }
                    (todo, arena, chain_heads)
                },
                |(mut todo, mut arena, mut chain_heads)| {
                    let horizon = CommitSeq::new(u64::from(chain_depth) - 1);
                    black_box(gc_tick(&mut todo, horizon, &mut arena, &mut chain_heads));
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// SSI tracking overhead benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: incremental overhead of range-scan tracking over baseline reads.
///
/// Baseline path performs identical page reads without the explicit
/// `record_range_scan` pass. Tracked path calls `read_page_range`, which
/// reuses page reads and adds predicate witness/read-set range capture.
fn bench_range_scan_tracking_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("ssi/range_scan_tracking_overhead");
    group.sample_size(24);
    group.measurement_time(Duration::from_secs(10));

    for &pages_per_scan in &[32_u32, 128, 256] {
        let start_page = 10_u32;
        let end_page = start_page + pages_per_scan - 1;
        group.throughput(Throughput::Elements(u64::from(pages_per_scan)));

        group.bench_with_input(
            BenchmarkId::new("baseline_reads", pages_per_scan),
            &pages_per_scan,
            |b, &_| {
                let manager = seeded_scan_manager(start_page, end_page);
                b.iter(|| {
                    let mut reader = manager
                        .begin(BeginKind::Concurrent)
                        .expect("reader begin should succeed");
                    let mut rows = Vec::with_capacity(usize::try_from(pages_per_scan).unwrap_or(0));
                    for raw_page in start_page..=end_page {
                        rows.push((
                            page(raw_page),
                            manager.read_page(&mut reader, page(raw_page)),
                        ));
                    }
                    black_box(consume_rows_like_oltp(&rows));
                    manager.abort(&mut reader);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("range_tracking", pages_per_scan),
            &pages_per_scan,
            |b, &_| {
                let manager = seeded_scan_manager(start_page, end_page);
                b.iter(|| {
                    let mut reader = manager
                        .begin(BeginKind::Concurrent)
                        .expect("reader begin should succeed");
                    let rows =
                        manager.read_page_range(&mut reader, page(start_page), page(end_page));
                    black_box(consume_rows_like_oltp(&rows));
                    manager.abort(&mut reader);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Concurrent writer contention macro-benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: concurrent writer lifecycle (begin → write → FCW validate → commit/abort).
///
/// Simulates `n_writers` concurrent handles contending for `n_shared_pages` pages.
/// Each writer writes to a mix of shared (contended) and private (uncontended) pages.
fn bench_concurrent_writer_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writers/lifecycle");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    for &(n_writers, n_shared_pages) in &[(2_u32, 10_u32), (4, 50), (8, 100)] {
        let label = format!("{n_writers}w_{n_shared_pages}shared");
        let private_pages_per_writer = 20_u32;
        let total_ops = u64::from(n_writers) * u64::from(n_shared_pages + private_pages_per_writer);
        group.throughput(Throughput::Elements(total_ops));

        group.bench_function(&label, |b| {
            b.iter_batched(
                || {
                    let commit_index = CommitIndex::new();
                    let lock_table = InProcessPageLockTable::new();

                    // Pre-populate commit index at seq=1 for shared pages.
                    for i in 1..=n_shared_pages {
                        commit_index.update(page(i), CommitSeq::new(1));
                    }

                    (commit_index, lock_table)
                },
                |(commit_index, lock_table)| {
                    let mut registry = ConcurrentRegistry::new();
                    let snapshot = Snapshot::new(CommitSeq::new(1), SchemaEpoch::ZERO);

                    // Begin concurrent sessions.
                    let session_ids: Vec<u64> = (0..n_writers)
                        .map(|_| registry.begin_concurrent(snapshot).unwrap())
                        .collect();

                    // Each writer writes to shared pages + private pages.
                    for (writer_idx, &session_id) in session_ids.iter().enumerate() {
                        let handle = registry.get_mut(session_id).unwrap();

                        // Write to shared pages (contention zone).
                        for i in 1..=n_shared_pages {
                            let data = PageData::zeroed(PageSize::default());
                            // Ignore lock contention errors — expected for shared pages.
                            let _ = fsqlite_mvcc::concurrent_write_page(
                                handle,
                                &lock_table,
                                session_id,
                                page(i),
                                data,
                            );
                        }

                        // Write to private pages (no contention).
                        #[allow(clippy::cast_possible_truncation)]
                        let base = n_shared_pages + (writer_idx as u32) * private_pages_per_writer;
                        for i in 1..=private_pages_per_writer {
                            let data = PageData::zeroed(PageSize::default());
                            let _ = fsqlite_mvcc::concurrent_write_page(
                                handle,
                                &lock_table,
                                session_id,
                                page(base + i),
                                data,
                            );
                        }
                    }

                    // First writer commits successfully (FCW clean for its write set).
                    let first_session = session_ids[0];
                    let first_handle = registry.get(first_session).unwrap();
                    let fcw_result = validate_first_committer_wins(first_handle, &commit_index);
                    black_box(&fcw_result);

                    // Remaining writers validate FCW (may see conflicts on shared pages).
                    for &session_id in &session_ids[1..] {
                        let handle = registry.get(session_id).unwrap();
                        let result = validate_first_committer_wins(handle, &commit_index);
                        black_box(&result);
                    }

                    // Abort all (cleanup locks).
                    for &session_id in &session_ids {
                        let handle = registry.get_mut(session_id).unwrap();
                        fsqlite_mvcc::concurrent_abort(handle, &lock_table, session_id);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark: hotspot contention — all writers target the same pages.
///
/// This is the worst-case scenario for lock contention. Measures the raw
/// cost of detecting and handling page-level conflicts.
fn bench_hotspot_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writers/hotspot");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    for &n_writers in &[2_u32, 4, 8] {
        let hotspot_pages = 5_u32; // all writers hit the same 5 pages
        let total_ops = u64::from(n_writers) * u64::from(hotspot_pages);
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::new("writers", n_writers),
            &n_writers,
            |b, &writers| {
                b.iter_batched(
                    || {
                        let obs = Arc::new(MetricsObserver::new(256));
                        let lock_table = InProcessPageLockTable::with_observer(
                            obs.clone() as Arc<dyn ConflictObserver>
                        );
                        let commit_index = CommitIndex::new();
                        for i in 1..=hotspot_pages {
                            commit_index.update(page(i), CommitSeq::new(1));
                        }
                        (lock_table, commit_index, obs)
                    },
                    |(lock_table, commit_index, obs)| {
                        let mut registry = ConcurrentRegistry::new();
                        let snapshot = Snapshot::new(CommitSeq::new(1), SchemaEpoch::ZERO);

                        let session_ids: Vec<u64> = (0..writers)
                            .map(|_| registry.begin_concurrent(snapshot).unwrap())
                            .collect();

                        // All writers try to lock the same hotspot pages.
                        for &session_id in &session_ids {
                            let handle = registry.get_mut(session_id).unwrap();
                            for i in 1..=hotspot_pages {
                                let data = PageData::zeroed(PageSize::default());
                                let _ = fsqlite_mvcc::concurrent_write_page(
                                    handle,
                                    &lock_table,
                                    session_id,
                                    page(i),
                                    data,
                                );
                            }
                        }

                        // FCW validation for all.
                        for &session_id in &session_ids {
                            let handle = registry.get(session_id).unwrap();
                            black_box(validate_first_committer_wins(handle, &commit_index));
                        }

                        // Verify observer captured contention events.
                        let snap = obs.metrics().snapshot();
                        black_box(&snap);

                        // Cleanup.
                        for &session_id in &session_ids {
                            let handle = registry.get_mut(session_id).unwrap();
                            fsqlite_mvcc::concurrent_abort(handle, &lock_table, session_id);
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// SSI edge discovery benchmarks
// ---------------------------------------------------------------------------

/// Lightweight mock implementing `ActiveTxnView` for benchmarks.
/// Uses `AtomicBool` for interior-mutable flag setters.
struct BenchActiveTxn {
    token: TxnToken,
    begin_seq: CommitSeq,
    read_keys: Vec<WitnessKey>,
    write_keys: Vec<WitnessKey>,
    has_in_rw: AtomicBool,
    has_out_rw: AtomicBool,
}

impl BenchActiveTxn {
    fn new(id: u64, begin: u64, reads: Vec<WitnessKey>, writes: Vec<WitnessKey>) -> Self {
        Self {
            token: TxnToken::new(txn(id), TxnEpoch::new(1)),
            begin_seq: CommitSeq::new(begin),
            read_keys: reads,
            write_keys: writes,
            has_in_rw: AtomicBool::new(false),
            has_out_rw: AtomicBool::new(false),
        }
    }
}

impl ActiveTxnView for BenchActiveTxn {
    fn token(&self) -> TxnToken {
        self.token
    }
    fn begin_seq(&self) -> CommitSeq {
        self.begin_seq
    }
    fn is_active(&self) -> bool {
        true
    }
    fn read_keys(&self) -> &[WitnessKey] {
        &self.read_keys
    }
    fn write_keys(&self) -> &[WitnessKey] {
        &self.write_keys
    }
    fn has_in_rw(&self) -> bool {
        self.has_in_rw.load(Ordering::Relaxed)
    }
    fn has_out_rw(&self) -> bool {
        self.has_out_rw.load(Ordering::Relaxed)
    }
    fn set_has_in_rw(&self, val: bool) {
        self.has_in_rw.store(val, Ordering::Relaxed);
    }
    fn set_has_out_rw(&self, val: bool) {
        self.has_out_rw.store(val, Ordering::Relaxed);
    }
    fn set_marked_for_abort(&self, _val: bool) {}
}

/// Benchmark: SSI incoming edge discovery with N active readers.
fn bench_ssi_incoming_edges(c: &mut Criterion) {
    let mut group = c.benchmark_group("ssi_edge_discovery/incoming");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_readers in &[4_u32, 8, 16] {
        group.throughput(Throughput::Elements(u64::from(n_readers)));
        group.bench_with_input(
            BenchmarkId::new("active_readers", n_readers),
            &n_readers,
            |b, &count| {
                b.iter_batched(
                    || {
                        // The committing txn writes pages 1..=10.
                        let write_keys: Vec<WitnessKey> =
                            (1..=10_u32).map(|i| WitnessKey::Page(page(i))).collect();
                        let committer = TxnToken::new(txn(1000), TxnEpoch::new(1));

                        // N active readers each read overlapping pages (1..=5).
                        let readers: Vec<BenchActiveTxn> = (0..count)
                            .map(|i| {
                                let reads =
                                    (1..=5_u32).map(|p| WitnessKey::Page(page(p))).collect();
                                BenchActiveTxn::new(u64::from(i) + 100, 1, reads, Vec::new())
                            })
                            .collect();
                        (committer, write_keys, readers)
                    },
                    |(committer, write_keys, readers)| {
                        let reader_views: Vec<&dyn ActiveTxnView> =
                            readers.iter().map(|r| r as &dyn ActiveTxnView).collect();
                        let edges: Vec<DiscoveredEdge> = discover_incoming_edges(
                            committer,
                            CommitSeq::new(10),
                            &write_keys,
                            &reader_views,
                            &[],
                        );
                        black_box(edges);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: SSI outgoing edge discovery with N committed writers.
fn bench_ssi_outgoing_edges(c: &mut Criterion) {
    let mut group = c.benchmark_group("ssi_edge_discovery/outgoing");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &n_writers in &[4_u32, 8, 16] {
        group.throughput(Throughput::Elements(u64::from(n_writers)));
        group.bench_with_input(
            BenchmarkId::new("committed_writers", n_writers),
            &n_writers,
            |b, &count| {
                b.iter_batched(
                    || {
                        // The committing txn reads pages 1..=10.
                        let read_keys: Vec<WitnessKey> =
                            (1..=10_u32).map(|i| WitnessKey::Page(page(i))).collect();
                        let committer = TxnToken::new(txn(1000), TxnEpoch::new(1));

                        // N committed writers each modified overlapping pages.
                        let committed_writers: Vec<CommittedWriterInfo> = (0..count)
                            .map(|i| CommittedWriterInfo {
                                token: TxnToken::new(txn(u64::from(i) + 200), TxnEpoch::new(1)),
                                commit_seq: CommitSeq::new(u64::from(i) + 5),
                                pages: (1..=5_u32).map(page).collect(),
                            })
                            .collect();
                        (committer, read_keys, committed_writers)
                    },
                    |(committer, read_keys, committed_writers)| {
                        let edges: Vec<DiscoveredEdge> = discover_outgoing_edges(
                            committer,
                            CommitSeq::new(2),
                            &read_keys,
                            &[],
                            &committed_writers,
                        );
                        black_box(edges);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Multi-threaded lock contention benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: real multi-threaded lock table contention.
///
/// N threads each acquire/release locks on a shared page pool. Half the pages
/// overlap across threads (contention), half are thread-private (parallelism).
fn bench_multi_thread_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_table/multi_thread_contention");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    for &n_threads in &[2_u32, 4, 8] {
        let ops_per_thread = 50_u32;
        group.throughput(Throughput::Elements(
            u64::from(n_threads) * u64::from(ops_per_thread),
        ));
        group.bench_with_input(
            BenchmarkId::new("threads", n_threads),
            &n_threads,
            |b, &threads| {
                b.iter_batched(
                    || Arc::new(InProcessPageLockTable::new()),
                    |table| {
                        let barrier = Arc::new(Barrier::new(threads as usize));
                        let handles: Vec<_> = (0..threads)
                            .map(|t| {
                                let tbl = Arc::clone(&table);
                                let bar = Arc::clone(&barrier);
                                let tid = txn(u64::from(t) + 1);
                                std::thread::spawn(move || {
                                    bar.wait();
                                    // Shared pages 1..=25 (contention).
                                    for i in 1..=25_u32 {
                                        let _ = tbl.try_acquire(page(i), tid);
                                    }
                                    // Private pages (no contention).
                                    let base = 1000 + t * 100;
                                    for i in 1..=25_u32 {
                                        let _ = tbl.try_acquire(page(base + i), tid);
                                    }
                                    // Release all.
                                    for i in 1..=25_u32 {
                                        tbl.release(page(i), tid);
                                    }
                                    for i in 1..=25_u32 {
                                        tbl.release(page(base + i), tid);
                                    }
                                })
                            })
                            .collect();
                        for h in handles {
                            h.join().unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups
// ---------------------------------------------------------------------------

criterion_group!(
    name = lock_table;
    config = criterion_config();
    targets =
        bench_lock_acquire_release,
        bench_lock_acquire_scaling,
        bench_lock_contention,
        bench_lock_observer_overhead
);

criterion_group!(
    name = version_arena;
    config = criterion_config();
    targets =
        bench_arena_alloc_get,
        bench_arena_free_list
);

criterion_group!(
    name = commit_index;
    config = criterion_config();
    targets =
        bench_commit_index
);

criterion_group!(
    name = fcw_validation;
    config = criterion_config();
    targets =
        bench_fcw_clean,
        bench_fcw_conflict
);

criterion_group!(
    name = gc;
    config = criterion_config();
    targets =
        bench_gc_prune,
        bench_gc_tick
);

criterion_group!(
    name = concurrent_writers;
    config = criterion_config();
    targets =
        bench_concurrent_writer_lifecycle,
        bench_hotspot_contention
);

criterion_group!(
    name = ssi_edge_discovery;
    config = criterion_config();
    targets =
        bench_ssi_incoming_edges,
        bench_ssi_outgoing_edges
);

criterion_group!(
    name = ssi_tracking;
    config = criterion_config();
    targets =
        bench_range_scan_tracking_overhead
);

criterion_group!(
    name = multi_thread;
    config = criterion_config();
    targets =
        bench_multi_thread_contention
);

criterion_main!(
    lock_table,
    version_arena,
    commit_index,
    fcw_validation,
    gc,
    concurrent_writers,
    ssi_edge_discovery,
    ssi_tracking,
    multi_thread
);
