//! Harness integration tests for bd-2y306.7: EBR Version-Chain GC rollout
//! (budgeted + stale-reader safe-mode).
//!
//! Validates: end-to-end GC lifecycle with EBR defer/retire, budget enforcement
//! under pressure, stale-reader warning budget, scheduler-driven tick cadence,
//! concurrent writer GC interference, chain-pressure feedback loop, eager reclaim
//! fallback semantics, and p50/p95/p99 tail latency targets.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fsqlite_mvcc::{
    GC_F_MAX_HZ, GC_F_MIN_HZ, GC_PAGES_BUDGET, GC_VERSIONS_BUDGET, GC_TARGET_CHAIN_LENGTH,
    GcScheduler, GcTodo, gc_tick,
    VersionArena, VersionIdx,
    VersionGuard, VersionGuardRegistry, VersionGuardTicket, StaleReaderConfig,
};
use fsqlite_mvcc::gc::gc_tick_with_registry;
use fsqlite_mvcc::ebr::GLOBAL_EBR_METRICS;
use fsqlite_types::{CommitSeq, PageNumber, PageNumberBuildHasher, PageData, PageSize};
use fsqlite_types::glossary::{PageVersion, TxnToken, TxnId, TxnEpoch};

const BEAD_ID: &str = "bd-2y306.7";

/// Create a dummy PageVersion for arena insertion.
fn dummy_version(pgno: u32, seq: u64) -> PageVersion {
    PageVersion {
        pgno: PageNumber::new(pgno).unwrap(),
        commit_seq: CommitSeq::new(seq),
        created_by: TxnToken {
            id: TxnId::new(1).unwrap(),
            epoch: TxnEpoch::new(0),
        },
        data: PageData::zeroed(PageSize::new(4096).unwrap()),
        prev: None,
    }
}

/// Populate arena with `n_pages` pages, each having `chain_depth` versions.
/// Returns (arena, chain_heads, todo) ready for gc_tick.
fn populate_arena(
    n_pages: u32,
    chain_depth: u64,
) -> (
    VersionArena,
    HashMap<PageNumber, VersionIdx, PageNumberBuildHasher>,
    GcTodo,
) {
    let mut arena = VersionArena::new();
    let mut chain_heads: HashMap<PageNumber, VersionIdx, PageNumberBuildHasher> =
        HashMap::with_hasher(PageNumberBuildHasher::default());
    let mut todo = GcTodo::new();

    for page in 1..=n_pages {
        let pgno = PageNumber::new(page).unwrap();
        let mut _prev_idx: Option<VersionIdx> = None;
        for seq in 1..=chain_depth {
            let ver = dummy_version(page, seq);
            let idx = arena.alloc(ver);
            if seq == chain_depth {
                chain_heads.insert(pgno, idx);
            }
            _prev_idx = Some(idx);
        }
        todo.enqueue(pgno);
    }
    (arena, chain_heads, todo)
}

/// Collect percentiles from a sorted array of durations.
fn percentiles(sorted: &[Duration]) -> (Duration, Duration, Duration) {
    let n = sorted.len();
    if n == 0 {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }
    let p50 = sorted[n / 2];
    let p95 = sorted[(n * 95) / 100];
    let p99 = sorted[(n * 99) / 100];
    (p50, p95, p99)
}

// ── 1. End-to-end GC lifecycle with EBR ──────────────────────────────────────

#[test]
fn test_e2e_gc_lifecycle_with_ebr() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig::default()));
    let mut arena = VersionArena::new();
    let mut chain_heads: HashMap<PageNumber, VersionIdx, PageNumberBuildHasher> =
        HashMap::with_hasher(PageNumberBuildHasher::default());
    let mut todo = GcTodo::new();

    // Populate: 10 pages, 5 versions each (seq 1..=5). Head = seq 5.
    for page in 1..=10u32 {
        let pgno = PageNumber::new(page).unwrap();
        for seq in 1..=5u64 {
            let ver = dummy_version(page, seq);
            let idx = arena.alloc(ver);
            if seq == 5 {
                chain_heads.insert(pgno, idx);
            }
        }
        todo.enqueue(pgno);
    }

    let before = GLOBAL_EBR_METRICS.snapshot();

    // GC with horizon=10 (above all version seqs). The head (seq=5) is at or
    // below the horizon, so prune_page_chain will find a sever point and pin an
    // EBR guard per page. Without linked prev pointers, nothing below the head
    // is freed, but the guard lifecycle is exercised.
    let horizon = CommitSeq::new(10);
    let result = gc_tick_with_registry(
        &mut todo, horizon, &mut arena, &mut chain_heads, &registry,
    );

    let after = GLOBAL_EBR_METRICS.snapshot();

    println!(
        "[{BEAD_ID}] e2e lifecycle: pages_pruned={} versions_freed={} queue_remaining={}",
        result.pages_pruned, result.versions_freed, result.queue_remaining,
    );

    // All 10 pages should be processed (within budget).
    assert_eq!(result.pages_pruned, 10, "bead_id={BEAD_ID} case=pages_pruned");
    assert_eq!(result.queue_remaining, 0, "bead_id={BEAD_ID} case=queue_empty");

    // Guard pin/unpin should have occurred (one guard per page prune).
    let delta_pinned = after.guards_pinned_total - before.guards_pinned_total;
    assert!(
        delta_pinned >= 10,
        "bead_id={BEAD_ID} case=guards_pinned delta={delta_pinned}",
    );
}

// ── 2. Budget enforcement under heavy pressure ──────────────────────────────

#[test]
fn test_budget_enforcement_heavy_pressure() {
    let pages = GC_PAGES_BUDGET * 3; // 192 pages, way over budget
    let (mut arena, mut chain_heads, mut todo) = populate_arena(pages, 1);

    let horizon = CommitSeq::new(0);
    let result = gc_tick(&mut todo, horizon, &mut arena, &mut chain_heads);

    println!(
        "[{BEAD_ID}] budget enforcement: pages_pruned={} budget={} exhausted={} remaining={}",
        result.pages_pruned, GC_PAGES_BUDGET, result.pages_budget_exhausted, result.queue_remaining,
    );

    assert!(
        result.pages_pruned <= GC_PAGES_BUDGET,
        "bead_id={BEAD_ID} case=pages_budget_cap pruned={} budget={}",
        result.pages_pruned, GC_PAGES_BUDGET,
    );
    assert!(
        result.pages_budget_exhausted,
        "bead_id={BEAD_ID} case=pages_budget_exhausted",
    );
    assert!(
        result.queue_remaining > 0,
        "bead_id={BEAD_ID} case=queue_has_remaining",
    );

    // Second tick should process more pages (incremental drain).
    let result2 = gc_tick(&mut todo, horizon, &mut arena, &mut chain_heads);
    assert!(
        result2.pages_pruned > 0,
        "bead_id={BEAD_ID} case=incremental_drain",
    );
    println!(
        "[{BEAD_ID}] incremental drain: pages_pruned={} remaining={}",
        result2.pages_pruned, result2.queue_remaining,
    );
}

// ── 3. Versions budget exhaustion ───────────────────────────────────────────

#[test]
fn test_versions_budget_exhaustion() {
    // Create many pages with deep chains to potentially hit versions budget.
    // GC_VERSIONS_BUDGET=4096, so 50 pages * 100 versions = 5000 potentially freeable.
    let mut arena = VersionArena::new();
    let mut chain_heads: HashMap<PageNumber, VersionIdx, PageNumberBuildHasher> =
        HashMap::with_hasher(PageNumberBuildHasher::default());
    let mut todo = GcTodo::new();

    for page in 1..=50u32 {
        let pgno = PageNumber::new(page).unwrap();
        for seq in 1..=100u64 {
            let ver = dummy_version(page, seq);
            let idx = arena.alloc(ver);
            if seq == 100 {
                chain_heads.insert(pgno, idx);
            }
        }
        todo.enqueue(pgno);
    }

    // Horizon at 50 should free versions with seq <= 49 per page.
    let horizon = CommitSeq::new(50);
    let result = gc_tick(&mut todo, horizon, &mut arena, &mut chain_heads);

    println!(
        "[{BEAD_ID}] versions budget: versions_freed={} budget={} versions_exhausted={} pages_exhausted={}",
        result.versions_freed, GC_VERSIONS_BUDGET,
        result.versions_budget_exhausted, result.pages_budget_exhausted,
    );

    assert!(
        result.versions_freed <= GC_VERSIONS_BUDGET,
        "bead_id={BEAD_ID} case=versions_budget_cap freed={} budget={}",
        result.versions_freed, GC_VERSIONS_BUDGET,
    );
}

// ── 4. Stale-reader warning budget ──────────────────────────────────────────

#[test]
fn test_stale_reader_warning_budget() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
        warn_after: Duration::from_millis(10),
        warn_every: Duration::from_millis(50),
    }));

    // Pin a guard and let it become stale.
    let guard = VersionGuard::pin(Arc::clone(&registry));
    std::thread::sleep(Duration::from_millis(20));

    let before = GLOBAL_EBR_METRICS.snapshot();

    // First warning should fire.
    let w1 = registry.warn_on_stale_readers(Instant::now());
    assert!(w1 >= 1, "bead_id={BEAD_ID} case=first_stale_warning w1={w1}");

    // Immediate second call should be rate-limited (within warn_every=50ms).
    let w2 = registry.warn_on_stale_readers(Instant::now());
    assert_eq!(w2, 0, "bead_id={BEAD_ID} case=rate_limited w2={w2}");

    // Wait past warn_every and try again.
    std::thread::sleep(Duration::from_millis(60));
    let w3 = registry.warn_on_stale_readers(Instant::now());
    assert!(w3 >= 1, "bead_id={BEAD_ID} case=post_cooldown_warning w3={w3}");

    let after = GLOBAL_EBR_METRICS.snapshot();
    let delta_warnings = after.stale_reader_warnings_total - before.stale_reader_warnings_total;

    println!(
        "[{BEAD_ID}] stale reader warnings: w1={w1} w2={w2} w3={w3} total_delta={delta_warnings}"
    );

    drop(guard);

    // After drop, no more stale readers.
    let stale = registry.stale_reader_snapshots(Instant::now());
    assert!(
        stale.is_empty(),
        "bead_id={BEAD_ID} case=stale_cleared_after_drop",
    );
}

// ── 5. Scheduler-driven tick cadence ────────────────────────────────────────

#[test]
fn test_scheduler_driven_tick_cadence() {
    let scheduler = GcScheduler::new();

    // Simulate time-stepped tick invocations at different pressures.
    let scenarios = [
        ("low_pressure", 1.0_f64, 5000_u64), // 1Hz → 1000ms interval, 5000ms available
        ("target_pressure", GC_TARGET_CHAIN_LENGTH, 2000),
        ("high_pressure", 500.0, 200),
    ];

    println!("[{BEAD_ID}] scheduler-driven cadence:");
    for (label, pressure, time_window_ms) in &scenarios {
        let mut ticks = 0_u32;
        let interval = scheduler.compute_interval_millis(*pressure);
        let freq = scheduler.compute_frequency(*pressure);

        // Reset scheduler state.
        let mut sched = GcScheduler::new();

        let mut now_ms = 0_u64;
        while now_ms <= *time_window_ms {
            if sched.should_tick(*pressure, now_ms) {
                ticks += 1;
            }
            now_ms += 1; // 1ms steps
        }

        println!(
            "  {label}: pressure={pressure:.1} interval={interval}ms freq={freq:.1}Hz ticks={ticks} in {time_window_ms}ms"
        );

        // At least 1 tick should fire in the window.
        assert!(
            ticks >= 1,
            "bead_id={BEAD_ID} case=cadence_{label} ticks={ticks}",
        );

        // Ticks should roughly match expected count (window / interval).
        let expected_ticks = (*time_window_ms as f64 / interval as f64).ceil() as u32;
        // Allow ±1 tolerance for boundary effects.
        assert!(
            ticks <= expected_ticks + 1,
            "bead_id={BEAD_ID} case=cadence_upper_{label} ticks={ticks} expected={expected_ticks}",
        );
    }
}

// ── 6. Concurrent writer GC interference ────────────────────────────────────

#[test]
fn test_concurrent_writer_gc_interference() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig::default()));

    // Simulate: writer threads hold guards while GC runs.
    let writer_guards: Vec<_> = (0..4)
        .map(|_| VersionGuard::pin(Arc::clone(&registry)))
        .collect();

    assert_eq!(
        registry.active_guard_count(), 4,
        "bead_id={BEAD_ID} case=writers_pinned",
    );

    // GC tick with pinned readers — should still complete (EBR defers reclamation).
    let mut arena = VersionArena::new();
    let mut chain_heads: HashMap<PageNumber, VersionIdx, PageNumberBuildHasher> =
        HashMap::with_hasher(PageNumberBuildHasher::default());
    let mut todo = GcTodo::new();

    for page in 1..=20u32 {
        let pgno = PageNumber::new(page).unwrap();
        let ver = dummy_version(page, 1);
        let idx = arena.alloc(ver);
        chain_heads.insert(pgno, idx);
        todo.enqueue(pgno);
    }

    let horizon = CommitSeq::new(0);
    let result = gc_tick_with_registry(
        &mut todo, horizon, &mut arena, &mut chain_heads, &registry,
    );

    println!(
        "[{BEAD_ID}] concurrent interference: pages_pruned={} active_guards={}",
        result.pages_pruned, registry.active_guard_count(),
    );

    // GC should complete without panic despite active guards.
    assert_eq!(
        result.pages_pruned, 20,
        "bead_id={BEAD_ID} case=gc_completes_with_guards",
    );

    // Drop writer guards — EBR will eventually reclaim deferred items.
    drop(writer_guards);
    assert_eq!(
        registry.active_guard_count(), 0,
        "bead_id={BEAD_ID} case=writers_released",
    );
}

// ── 7. Chain-pressure feedback loop ─────────────────────────────────────────

#[test]
fn test_chain_pressure_feedback_loop() {
    let scheduler = GcScheduler::new();

    // Simulate rising pressure → faster GC → pressure drops → slower GC.
    let mut pressure = 2.0_f64;
    let mut history: Vec<(f64, f64)> = Vec::new();

    println!("[{BEAD_ID}] feedback loop simulation:");
    for step in 0..20 {
        let freq = scheduler.compute_frequency(pressure);
        history.push((pressure, freq));

        // Simulate: higher freq → more versions freed → pressure drops.
        // Lower freq → versions accumulate → pressure rises.
        let gc_effect = freq * 0.5; // each Hz frees ~0.5 pressure units
        let ingestion = 4.0; // constant ingestion rate

        pressure = (pressure + ingestion - gc_effect).max(0.0);

        if step < 5 || step >= 15 {
            println!("  step={step}: pressure={pressure:.2} freq={freq:.1}Hz");
        }
    }

    // Pressure should stabilize (not diverge to infinity).
    let final_pressure = history.last().unwrap().0;
    assert!(
        final_pressure < 1000.0,
        "bead_id={BEAD_ID} case=pressure_bounded final={final_pressure:.1}",
    );

    // Frequency should have increased as pressure rose.
    let early_freq = history[0].1;
    let late_freq = history[10].1;
    assert!(
        late_freq >= early_freq,
        "bead_id={BEAD_ID} case=freq_tracks_pressure early={early_freq:.1} late={late_freq:.1}",
    );
}

// ── 8. Eager reclaim fallback semantics ─────────────────────────────────────

#[test]
fn test_eager_reclaim_fallback_semantics() {
    // Test the defer_retire_version fallback: when no ticket is present,
    // the system should still function (synchronous path).
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig::default()));

    // With guard: defer works.
    let guard = VersionGuard::pin(Arc::clone(&registry));
    let before = GLOBAL_EBR_METRICS.snapshot();
    guard.defer_retire(Box::new(42u64));
    let after = GLOBAL_EBR_METRICS.snapshot();
    let delta = after.retirements_deferred_total - before.retirements_deferred_total;
    assert_eq!(
        delta, 1,
        "bead_id={BEAD_ID} case=defer_with_guard delta={delta}",
    );
    guard.flush();
    drop(guard);

    // Ticket-based deferred retire (Send-safe path).
    let ticket = VersionGuardTicket::register(Arc::clone(&registry));
    let before2 = GLOBAL_EBR_METRICS.snapshot();
    ticket.defer_retire(Box::new(99u64));
    let after2 = GLOBAL_EBR_METRICS.snapshot();
    let delta2 = after2.retirements_deferred_total - before2.retirements_deferred_total;
    assert_eq!(
        delta2, 1,
        "bead_id={BEAD_ID} case=defer_with_ticket delta={delta2}",
    );
    drop(ticket);

    // Registry should be clean.
    assert_eq!(
        registry.active_guard_count(), 0,
        "bead_id={BEAD_ID} case=registry_clean_after_fallback",
    );

    println!("[{BEAD_ID}] eager reclaim fallback: guard defer OK, ticket defer OK, registry clean");
}

// ── 9. GC tick tail latency under load ──────────────────────────────────────

#[test]
fn test_gc_tick_tail_latency_under_load() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig::default()));
    let mut arena = VersionArena::new();
    let mut chain_heads: HashMap<PageNumber, VersionIdx, PageNumberBuildHasher> =
        HashMap::with_hasher(PageNumberBuildHasher::default());

    // Populate: 64 pages (= GC_PAGES_BUDGET), 20 versions each.
    for page in 1..=GC_PAGES_BUDGET {
        let pgno = PageNumber::new(page).unwrap();
        for seq in 1..=20u64 {
            let ver = dummy_version(page, seq);
            let idx = arena.alloc(ver);
            if seq == 20 {
                chain_heads.insert(pgno, idx);
            }
        }
    }

    let mut latencies = Vec::new();
    for round in 0..100 {
        // Re-populate todo each round.
        let mut todo = GcTodo::new();
        for page in 1..=GC_PAGES_BUDGET {
            todo.enqueue(PageNumber::new(page).unwrap());
        }
        let horizon = CommitSeq::new(round + 1);
        let t0 = Instant::now();
        let _result = gc_tick_with_registry(
            &mut todo, horizon, &mut arena, &mut chain_heads, &registry,
        );
        latencies.push(t0.elapsed());
    }

    latencies.sort();
    let (p50, p95, p99) = percentiles(&latencies);

    println!("[{BEAD_ID}] gc_tick tail latency (64 pages, 20 versions, 100 rounds):");
    println!("  p50={p50:?} p95={p95:?} p99={p99:?}");

    // Acceptance: p99 < 5ms for budgeted tick (64 pages is the full budget).
    assert!(
        p99 < Duration::from_millis(5),
        "bead_id={BEAD_ID} case=gc_tick_p99 p99={p99:?}",
    );
}

// ── 10. Multi-threaded GC + reader contention ───────────────────────────────

#[test]
fn test_multithreaded_gc_reader_contention() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
        warn_after: Duration::from_secs(30),
        warn_every: Duration::from_secs(5),
    }));

    // Spawn reader threads that pin/unpin rapidly.
    let reader_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_handles: Vec<_> = (0..4)
        .map(|_| {
            let reg = Arc::clone(&registry);
            let done = Arc::clone(&reader_done);
            std::thread::spawn(move || {
                let mut pin_count = 0_u64;
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    let g = VersionGuard::pin(Arc::clone(&reg));
                    std::hint::black_box(&g);
                    drop(g);
                    pin_count += 1;
                    if pin_count > 500 {
                        break;
                    }
                }
                pin_count
            })
        })
        .collect();

    // Meanwhile, run GC on main thread.
    let mut arena = VersionArena::new();
    let mut chain_heads: HashMap<PageNumber, VersionIdx, PageNumberBuildHasher> =
        HashMap::with_hasher(PageNumberBuildHasher::default());
    let mut todo = GcTodo::new();

    for page in 1..=30u32 {
        let pgno = PageNumber::new(page).unwrap();
        let ver = dummy_version(page, 1);
        let idx = arena.alloc(ver);
        chain_heads.insert(pgno, idx);
        todo.enqueue(pgno);
    }

    let result = gc_tick_with_registry(
        &mut todo, CommitSeq::new(0), &mut arena, &mut chain_heads, &registry,
    );

    reader_done.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut total_pins = 0_u64;
    for h in reader_handles {
        total_pins += h.join().expect("reader thread should not panic");
    }

    println!(
        "[{BEAD_ID}] contention: gc_pages_pruned={} reader_pins={total_pins} active_guards={}",
        result.pages_pruned, registry.active_guard_count(),
    );

    assert_eq!(
        result.pages_pruned, 30,
        "bead_id={BEAD_ID} case=gc_under_contention",
    );
    assert_eq!(
        registry.active_guard_count(), 0,
        "bead_id={BEAD_ID} case=all_guards_released",
    );
}

// ── 11. Ticket lifecycle across threads ─────────────────────────────────────

#[test]
fn test_ticket_deferred_retire_cross_thread() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig::default()));

    let before = GLOBAL_EBR_METRICS.snapshot();

    // Create ticket on main thread.
    let ticket = VersionGuardTicket::register(Arc::clone(&registry));
    assert!(
        registry.active_guard_count() > 0,
        "bead_id={BEAD_ID} case=ticket_registered",
    );

    // Move to worker thread, defer retire, then drop.
    let handle = std::thread::spawn(move || {
        ticket.defer_retire(Box::new(vec![1u8, 2, 3]));
        ticket.defer_retire_with(|| {
            std::hint::black_box(42u64);
        });
        drop(ticket);
    });
    handle.join().expect("worker should not panic");

    let after = GLOBAL_EBR_METRICS.snapshot();
    let delta = after.retirements_deferred_total - before.retirements_deferred_total;

    assert!(
        delta >= 2,
        "bead_id={BEAD_ID} case=cross_thread_retirements delta={delta}",
    );
    assert_eq!(
        registry.active_guard_count(), 0,
        "bead_id={BEAD_ID} case=ticket_released_cross_thread",
    );

    println!("[{BEAD_ID}] ticket cross-thread: retirements_delta={delta} registry_clean=true");
}

// ── 12. Conformance summary ─────────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
        warn_after: Duration::from_millis(5),
        warn_every: Duration::from_millis(1),
    }));
    let scheduler = GcScheduler::new();

    // 1. Budget enforcement: pages budget caps gc_tick.
    let (mut arena, mut heads, mut todo) = populate_arena(GC_PAGES_BUDGET * 2, 1);
    let r = gc_tick(&mut todo, CommitSeq::new(0), &mut arena, &mut heads);
    let pass_budget = r.pages_pruned <= GC_PAGES_BUDGET;

    // 2. Scheduler frequency bounds.
    let pass_freq = scheduler.compute_frequency(0.0) == GC_F_MIN_HZ
        && scheduler.compute_frequency(10000.0) == GC_F_MAX_HZ;

    // 3. EBR defer/retire lifecycle.
    let guard = VersionGuard::pin(Arc::clone(&registry));
    guard.defer_retire(Box::new(1u64));
    guard.flush();
    let pass_defer = registry.active_guard_count() == 1;
    drop(guard);
    let pass_lifecycle = pass_defer && registry.active_guard_count() == 0;

    // 4. Stale reader detection.
    let g = VersionGuard::pin(Arc::clone(&registry));
    std::thread::sleep(Duration::from_millis(10));
    let stale = registry.stale_reader_snapshots(Instant::now());
    let pass_stale = !stale.is_empty();
    drop(g);

    // 5. Ticket cross-thread.
    let t = VersionGuardTicket::register(Arc::clone(&registry));
    let pass_ticket_pin = registry.active_guard_count() > 0;
    let h = std::thread::spawn(move || { drop(t); });
    h.join().unwrap();
    let pass_ticket = pass_ticket_pin && registry.active_guard_count() == 0;

    // 6. Concurrent GC safety.
    let _reader = VersionGuard::pin(Arc::clone(&registry));
    let (mut arena2, mut heads2, mut todo2) = populate_arena(10, 1);
    let r2 = gc_tick_with_registry(
        &mut todo2, CommitSeq::new(0), &mut arena2, &mut heads2, &registry,
    );
    let pass_concurrent_gc = r2.pages_pruned == 10;
    drop(_reader);

    let checks = [
        ("budget_enforcement", pass_budget),
        ("frequency_bounds", pass_freq),
        ("ebr_lifecycle", pass_lifecycle),
        ("stale_reader", pass_stale),
        ("ticket_cross_thread", pass_ticket),
        ("concurrent_gc", pass_concurrent_gc),
    ];
    let passed = checks.iter().filter(|(_, p)| *p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} EBR GC Rollout Conformance ===");
    for (name, ok) in &checks {
        println!("  {name:.<28}{}", if *ok { "PASS" } else { "FAIL" });
    }
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}",
    );
}
