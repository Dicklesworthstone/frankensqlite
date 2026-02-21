//! Harness integration tests for bd-2uza4.3: Page cooling/heating state machine
//! with eviction integration.
//!
//! Validates: HOT/COOLING/COLD transitions, access-based re-heating, root
//! pinning, eviction protocol, cooling scan, concurrent access, and metrics.

use std::sync::Arc;

use fsqlite_btree::cooling::{
    CoolingConfig, CoolingStateMachine, cooling_metrics_snapshot,
};
use fsqlite_btree::swizzle::PageTemperature;

const BEAD_ID: &str = "bd-2uza4.3";

// ── 1. Full page lifecycle ───────────────────────────────────────────────

#[test]
fn test_full_page_lifecycle() {
    let csm = CoolingStateMachine::new(CoolingConfig::default());

    // Register → Cold.
    csm.register_page(1);
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Cold),
        "bead_id={BEAD_ID} case=initial_cold"
    );

    // Load → Hot.
    csm.load_page(1, 0x1000);
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Hot),
        "bead_id={BEAD_ID} case=after_load"
    );
    assert_eq!(csm.frame_addr(1), Some(0x1000));

    // Cooling scan (no access since load, count=1 < threshold=2) → Cooling.
    let result = csm.run_cooling_scan();
    assert_eq!(result.pages_cooled, 1, "bead_id={BEAD_ID} case=cooled_1");
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Cooling),
        "bead_id={BEAD_ID} case=after_cool"
    );

    // Evict → Cold.
    csm.evict_page(1).expect("evict should succeed");
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Cold),
        "bead_id={BEAD_ID} case=after_evict"
    );
    assert_eq!(csm.frame_addr(1), None);
}

// ── 2. Re-heat on access ─────────────────────────────────────────────────

#[test]
fn test_re_heat_on_access() {
    let csm = CoolingStateMachine::new(CoolingConfig {
        cooling_threshold: 2,
    });
    csm.register_page(1);
    csm.load_page(1, 0x1000);

    // Cool the page.
    csm.run_cooling_scan();
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Cooling),
        "bead_id={BEAD_ID} case=cooled"
    );

    // Access → should re-heat to Hot.
    csm.access_page(1);
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Hot),
        "bead_id={BEAD_ID} case=reheated_on_access"
    );
}

// ── 3. Re-heat on load ───────────────────────────────────────────────────

#[test]
fn test_re_heat_on_load() {
    let csm = CoolingStateMachine::new(CoolingConfig::default());
    csm.register_page(1);
    csm.load_page(1, 0x1000);
    csm.run_cooling_scan();
    assert_eq!(csm.temperature(1), Some(PageTemperature::Cooling));

    // Re-load → should re-heat.
    let was_cold = csm.load_page(1, 0x2000);
    assert!(!was_cold, "bead_id={BEAD_ID} case=load_cooling_returns_false");
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Hot),
        "bead_id={BEAD_ID} case=reheated_on_load"
    );
}

// ── 4. Root pinning ──────────────────────────────────────────────────────

#[test]
fn test_root_pinning() {
    let csm = CoolingStateMachine::new(CoolingConfig::default());

    csm.pin_root(1);
    csm.pin_root(2);
    csm.register_page(3);

    csm.load_page(1, 0x1000);
    csm.load_page(2, 0x2000);
    csm.load_page(3, 0x3000);

    // Run 5 cooling scans with no access.
    for _ in 0..5 {
        csm.run_cooling_scan();
    }

    // Pinned roots stay Hot.
    assert_eq!(
        csm.temperature(1),
        Some(PageTemperature::Hot),
        "bead_id={BEAD_ID} case=pinned_root_stays_hot_1"
    );
    assert_eq!(
        csm.temperature(2),
        Some(PageTemperature::Hot),
        "bead_id={BEAD_ID} case=pinned_root_stays_hot_2"
    );

    // Non-pinned page should be Cooling.
    assert_eq!(
        csm.temperature(3),
        Some(PageTemperature::Cooling),
        "bead_id={BEAD_ID} case=non_pinned_cooled"
    );

    // Cannot evict pinned root.
    let err = csm.evict_page(1).unwrap_err();
    assert_eq!(err, "page is a pinned root", "bead_id={BEAD_ID} case=cannot_evict_pinned");

    assert!(csm.is_pinned(1));
    assert!(!csm.is_pinned(3));
    assert_eq!(csm.pinned_count(), 2);
}

// ── 5. Eviction protocol ─────────────────────────────────────────────────

#[test]
fn test_eviction_protocol() {
    let csm = CoolingStateMachine::new(CoolingConfig::default());
    csm.register_page(1);
    csm.load_page(1, 0x1000);

    // Cannot evict Hot page.
    assert!(
        csm.evict_page(1).is_err(),
        "bead_id={BEAD_ID} case=cannot_evict_hot"
    );

    // Cool, then evict.
    csm.run_cooling_scan();
    assert!(
        csm.evict_page(1).is_ok(),
        "bead_id={BEAD_ID} case=evict_cooling_ok"
    );

    // Cannot evict already Cold page.
    assert!(
        csm.evict_page(1).is_err(),
        "bead_id={BEAD_ID} case=cannot_evict_cold"
    );

    // Cannot evict unregistered page.
    assert!(
        csm.evict_page(999).is_err(),
        "bead_id={BEAD_ID} case=cannot_evict_unregistered"
    );
}

// ── 6. Frequent access prevents cooling ──────────────────────────────────

#[test]
fn test_frequent_access_prevents_cooling() {
    let csm = CoolingStateMachine::new(CoolingConfig {
        cooling_threshold: 3,
    });
    csm.register_page(1);
    csm.load_page(1, 0x1000);

    // Access the page frequently between scans.
    for _ in 0..5 {
        csm.access_page(1);
        csm.access_page(1);
        csm.access_page(1);
        let result = csm.run_cooling_scan();
        assert_eq!(
            result.pages_cooled, 0,
            "bead_id={BEAD_ID} case=frequent_access_no_cooling"
        );
        assert_eq!(csm.temperature(1), Some(PageTemperature::Hot));
    }
}

// ── 7. Temperature counts ────────────────────────────────────────────────

#[test]
fn test_temperature_counts() {
    let csm = CoolingStateMachine::new(CoolingConfig::default());

    for pid in 1..=10 {
        csm.register_page(pid);
    }

    // All Cold initially.
    let counts = csm.temperature_counts();
    assert_eq!(counts.cold, 10, "bead_id={BEAD_ID} case=all_cold");
    assert_eq!(counts.hot, 0);
    assert_eq!(counts.cooling, 0);

    // Load 5 pages.
    for pid in 1..=5 {
        csm.load_page(pid, pid * 0x1000);
    }
    let counts = csm.temperature_counts();
    assert_eq!(counts.hot, 5, "bead_id={BEAD_ID} case=5_hot");
    assert_eq!(counts.cold, 5, "bead_id={BEAD_ID} case=5_cold");

    // Cool all.
    csm.run_cooling_scan();
    let counts = csm.temperature_counts();
    assert_eq!(counts.cooling, 5, "bead_id={BEAD_ID} case=5_cooling");
    assert_eq!(counts.cold, 5);
}

// ── 8. Concurrent access + cooling ───────────────────────────────────────

#[test]
fn test_concurrent_access_and_cooling() {
    let csm = Arc::new(CoolingStateMachine::new(CoolingConfig {
        cooling_threshold: 5,
    }));

    for pid in 0..100 {
        csm.register_page(pid);
        csm.load_page(pid, (pid + 1) * 0x100);
    }

    // Spawn access threads and a cooling thread concurrently.
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let csm = Arc::clone(&csm);
            std::thread::spawn(move || {
                for _ in 0..10 {
                    for pid in (t * 25)..((t + 1) * 25) {
                        csm.access_page(pid);
                    }
                }
            })
        })
        .chain(std::iter::once({
            let csm = Arc::clone(&csm);
            std::thread::spawn(move || {
                for _ in 0..5 {
                    csm.run_cooling_scan();
                }
            })
        }))
        .collect();

    for h in handles {
        h.join().expect("thread should not panic");
    }

    // No panics = success. Check invariants.
    let counts = csm.temperature_counts();
    assert_eq!(
        counts.hot + counts.cooling + counts.cold,
        100,
        "bead_id={BEAD_ID} case=total_pages_preserved"
    );
}

// ── 9. Metrics fidelity ──────────────────────────────────────────────────

#[test]
fn test_metrics_fidelity() {
    let before = cooling_metrics_snapshot();

    let csm = CoolingStateMachine::new(CoolingConfig::default());
    for pid in 1..=5 {
        csm.register_page(pid);
        csm.load_page(pid, pid * 0x1000);
    }

    // Scan: should cool all 5.
    csm.run_cooling_scan();

    // Re-heat 2 pages.
    csm.access_page(1);
    csm.access_page(2);

    // Evict 2 pages.
    csm.evict_page(3).ok();
    csm.evict_page(4).ok();

    let after = cooling_metrics_snapshot();

    let delta_scans = after.cooling_scans_total - before.cooling_scans_total;
    let delta_cooled = after.pages_cooled_total - before.pages_cooled_total;
    let delta_reheated = after.pages_reheated_total - before.pages_reheated_total;
    let delta_evicted = after.pages_evicted_total - before.pages_evicted_total;

    assert!(
        delta_scans >= 1,
        "bead_id={BEAD_ID} case=scan_metric delta_scans={delta_scans}"
    );
    assert!(
        delta_cooled >= 5,
        "bead_id={BEAD_ID} case=cooled_metric delta_cooled={delta_cooled}"
    );
    assert!(
        delta_reheated >= 2,
        "bead_id={BEAD_ID} case=reheated_metric delta_reheated={delta_reheated}"
    );
    assert!(
        delta_evicted >= 2,
        "bead_id={BEAD_ID} case=evicted_metric delta_evicted={delta_evicted}"
    );

    println!(
        "[{BEAD_ID}] scans={delta_scans} cooled={delta_cooled} reheated={delta_reheated} evicted={delta_evicted}"
    );
}

// ── 10. Conformance summary ──────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    let csm = CoolingStateMachine::new(CoolingConfig::default());

    // Lifecycle: Cold → Hot → Cooling → Cold.
    csm.register_page(1);
    csm.load_page(1, 0x1000);
    csm.run_cooling_scan();
    csm.evict_page(1).ok();
    let pass_lifecycle = csm.temperature(1) == Some(PageTemperature::Cold);

    // Re-heat.
    csm.register_page(2);
    csm.load_page(2, 0x2000);
    csm.run_cooling_scan();
    csm.access_page(2);
    let pass_reheat = csm.temperature(2) == Some(PageTemperature::Hot);

    // Root pinning.
    csm.pin_root(3);
    csm.load_page(3, 0x3000);
    for _ in 0..5 {
        csm.run_cooling_scan();
    }
    let pass_pinning = csm.temperature(3) == Some(PageTemperature::Hot)
        && csm.evict_page(3).is_err();

    // Eviction protocol.
    csm.register_page(4);
    csm.load_page(4, 0x4000);
    let pass_evict = csm.evict_page(4).is_err() // can't evict Hot
        && {
            csm.run_cooling_scan();
            csm.evict_page(4).is_ok()
        };

    // Counts.
    let counts = csm.temperature_counts();
    let pass_counts = counts.hot + counts.cooling + counts.cold == csm.tracked_count();

    // Concurrent safety (no panics).
    let csm2 = Arc::new(CoolingStateMachine::new(CoolingConfig::default()));
    for i in 0..10 {
        csm2.register_page(i);
        csm2.load_page(i, (i + 1) * 0x100);
    }
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let c = Arc::clone(&csm2);
            std::thread::spawn(move || {
                for i in 0..10 {
                    c.access_page(i);
                }
                c.run_cooling_scan();
            })
        })
        .collect();
    let pass_concurrent = handles.into_iter().all(|h| h.join().is_ok());

    let checks = [
        pass_lifecycle,
        pass_reheat,
        pass_pinning,
        pass_evict,
        pass_counts,
        pass_concurrent,
    ];
    let passed = checks.iter().filter(|&&p| p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} Cooling State Machine Conformance ===");
    println!("  lifecycle:   {}", if pass_lifecycle { "PASS" } else { "FAIL" });
    println!("  re-heat:     {}", if pass_reheat { "PASS" } else { "FAIL" });
    println!("  pinning:     {}", if pass_pinning { "PASS" } else { "FAIL" });
    println!("  eviction:    {}", if pass_evict { "PASS" } else { "FAIL" });
    println!("  counts:      {}", if pass_counts { "PASS" } else { "FAIL" });
    println!("  concurrent:  {}", if pass_concurrent { "PASS" } else { "FAIL" });
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}"
    );
}
