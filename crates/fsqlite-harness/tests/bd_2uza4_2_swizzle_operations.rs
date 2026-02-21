//! Harness integration tests for bd-2uza4.2: SwizzlePtr and B-tree swizzle/unswizzle operations.
//!
//! Validates: CAS swizzle/unswizzle roundtrips, SwizzleRegistry lifecycle,
//! temperature state machine, simulated traversal with mixed pointer types,
//! concurrent registry access, and metrics fidelity.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use fsqlite_btree::swizzle::{
    PageTemperature, SwizzleError, SwizzlePtr, SwizzleRegistry, SwizzleState,
    MAX_PAGE_ID, SWIZZLED_TAG,
};
use fsqlite_btree::instrumentation::{btree_metrics_snapshot, reset_btree_metrics};

const BEAD_ID: &str = "bd-2uza4.2";

// ── 1. SwizzlePtr CAS roundtrip ─────────────────────────────────────────

#[test]
fn test_swizzle_ptr_cas_roundtrip() {
    // Unswizzled -> Swizzled -> Unswizzled via CAS operations.
    let ptr = SwizzlePtr::new_unswizzled(42).unwrap();

    // Verify initial state.
    assert_eq!(
        ptr.state(Ordering::Acquire),
        SwizzleState::Unswizzled { page_id: 42 },
        "bead_id={BEAD_ID} case=initial_unswizzled"
    );
    assert!(!ptr.is_swizzled(Ordering::Acquire));

    // CAS: swizzle to frame address 0x1000.
    ptr.try_swizzle(42, 0x1000).expect("swizzle should succeed");
    assert_eq!(
        ptr.state(Ordering::Acquire),
        SwizzleState::Swizzled { frame_addr: 0x1000 },
        "bead_id={BEAD_ID} case=after_swizzle"
    );
    assert!(ptr.is_swizzled(Ordering::Acquire));

    // CAS: unswizzle back to page_id 42.
    ptr.try_unswizzle(0x1000, 42).expect("unswizzle should succeed");
    assert_eq!(
        ptr.state(Ordering::Acquire),
        SwizzleState::Unswizzled { page_id: 42 },
        "bead_id={BEAD_ID} case=after_unswizzle"
    );
    assert!(!ptr.is_swizzled(Ordering::Acquire));
}

// ── 2. CAS contention detection ──────────────────────────────────────────

#[test]
fn test_cas_contention_detection() {
    let ptr = SwizzlePtr::new_unswizzled(10).unwrap();

    // CAS with wrong expected page_id fails.
    let err = ptr.try_swizzle(99, 0x2000).unwrap_err();
    match err {
        SwizzleError::CompareExchangeFailed { .. } => {}
        other => panic!("bead_id={BEAD_ID} case=cas_wrong_expected: expected CompareExchangeFailed, got {other:?}"),
    }

    // Original state unchanged.
    assert_eq!(
        ptr.state(Ordering::Acquire),
        SwizzleState::Unswizzled { page_id: 10 },
        "bead_id={BEAD_ID} case=state_unchanged_after_failed_cas"
    );

    // Correct CAS succeeds.
    ptr.try_swizzle(10, 0x2000).expect("correct CAS should succeed");

    // Double swizzle with wrong expected frame_addr fails.
    let err = ptr.try_unswizzle(0x4000, 10).unwrap_err();
    match err {
        SwizzleError::CompareExchangeFailed { .. } => {}
        other => panic!("bead_id={BEAD_ID} case=unswizzle_wrong_addr: expected CompareExchangeFailed, got {other:?}"),
    }
}

// ── 3. SwizzleRegistry lifecycle ─────────────────────────────────────────

#[test]
fn test_registry_full_lifecycle() {
    let reg = SwizzleRegistry::new();

    // Register pages.
    for pid in 1..=10 {
        reg.register_page(pid);
    }
    assert_eq!(reg.tracked_count(), 10, "bead_id={BEAD_ID} case=tracked_count");
    assert_eq!(reg.swizzled_count(), 0, "bead_id={BEAD_ID} case=initial_swizzled_count");

    // Swizzle half of them.
    for pid in 1..=5 {
        let addr = pid * 0x1000;
        assert!(
            reg.try_swizzle(pid, addr),
            "bead_id={BEAD_ID} case=swizzle_page_{pid}"
        );
    }
    assert_eq!(reg.swizzled_count(), 5, "bead_id={BEAD_ID} case=half_swizzled");

    // Query swizzle state.
    for pid in 1..=5 {
        assert!(reg.is_swizzled(pid), "bead_id={BEAD_ID} case=page_{pid}_swizzled");
        assert_eq!(
            reg.frame_addr(pid),
            Some(pid * 0x1000),
            "bead_id={BEAD_ID} case=frame_addr_{pid}"
        );
    }
    for pid in 6..=10 {
        assert!(!reg.is_swizzled(pid), "bead_id={BEAD_ID} case=page_{pid}_not_swizzled");
        assert_eq!(reg.frame_addr(pid), None, "bead_id={BEAD_ID} case=no_addr_{pid}");
    }

    // Unswizzle 3 pages.
    for pid in 1..=3 {
        assert!(reg.try_unswizzle(pid), "bead_id={BEAD_ID} case=unswizzle_{pid}");
    }
    assert_eq!(reg.swizzled_count(), 2, "bead_id={BEAD_ID} case=after_unswizzle");
}

// ── 4. Temperature state machine ─────────────────────────────────────────

#[test]
fn test_temperature_state_machine() {
    // Valid transitions.
    let valid = [
        (PageTemperature::Hot, PageTemperature::Cooling),
        (PageTemperature::Cooling, PageTemperature::Cold),
        (PageTemperature::Cold, PageTemperature::Hot),
        (PageTemperature::Cooling, PageTemperature::Hot),
        // Self-transitions.
        (PageTemperature::Hot, PageTemperature::Hot),
        (PageTemperature::Cooling, PageTemperature::Cooling),
        (PageTemperature::Cold, PageTemperature::Cold),
    ];
    for (from, to) in valid {
        assert!(
            from.can_transition_to(to),
            "bead_id={BEAD_ID} case=valid_transition_{from:?}_to_{to:?}"
        );
        assert!(from.transition(to).is_ok());
    }

    // Invalid transitions.
    let invalid = [
        (PageTemperature::Hot, PageTemperature::Cold),
        (PageTemperature::Cold, PageTemperature::Cooling),
    ];
    for (from, to) in invalid {
        assert!(
            !from.can_transition_to(to),
            "bead_id={BEAD_ID} case=invalid_transition_{from:?}_to_{to:?}"
        );
        assert!(from.transition(to).is_err());
    }
}

// ── 5. Simulated B-tree traversal with mixed pointer types ───────────────

#[test]
fn test_simulated_btree_traversal() {
    // Simulate a B-tree with 7 nodes where some child pointers are swizzled.
    let reg = SwizzleRegistry::new();
    let page_ids = [100, 101, 102, 103, 104, 105, 106];

    for &pid in &page_ids {
        reg.register_page(pid);
    }

    // Swizzle internal nodes (hot path): 100, 101, 102.
    reg.try_swizzle(100, 0x10000);
    reg.try_swizzle(101, 0x20000);
    reg.try_swizzle(102, 0x30000);

    // Simulate traversal: for each page, check if swizzled (fast path) or
    // unswizzled (slow path = page load).
    let mut fast_path_count = 0;
    let mut slow_path_count = 0;

    for &pid in &page_ids {
        if reg.is_swizzled(pid) {
            let _frame_addr = reg.frame_addr(pid).expect("swizzled page should have frame addr");
            fast_path_count += 1;
        } else {
            // Simulate page load + swizzle.
            slow_path_count += 1;
        }
    }

    assert_eq!(fast_path_count, 3, "bead_id={BEAD_ID} case=fast_path_traversals");
    assert_eq!(slow_path_count, 4, "bead_id={BEAD_ID} case=slow_path_traversals");

    // After loading all pages, all should be available.
    for &pid in &page_ids[3..] {
        let addr = pid as u64 * 0x100;
        reg.try_swizzle(pid, addr);
    }
    assert_eq!(
        reg.swizzled_count(),
        7,
        "bead_id={BEAD_ID} case=all_swizzled_after_full_traversal"
    );
}

// ── 6. Tag-bit encoding correctness ──────────────────────────────────────

#[test]
fn test_tag_bit_encoding() {
    // Verify tag bit encoding for various page IDs.
    let test_ids: Vec<u64> = vec![0, 1, 42, 1000, MAX_PAGE_ID / 2, MAX_PAGE_ID];
    for &pid in &test_ids {
        let ptr = SwizzlePtr::new_unswizzled(pid).unwrap();
        let raw = ptr.load_raw(Ordering::Acquire);

        // Unswizzled: bit 0 should be 0.
        assert_eq!(
            raw & SWIZZLED_TAG, 0,
            "bead_id={BEAD_ID} case=unswizzled_tag_clear pid={pid}"
        );

        // Round-trip through decode.
        let state = ptr.state(Ordering::Acquire);
        assert_eq!(
            state,
            SwizzleState::Unswizzled { page_id: pid },
            "bead_id={BEAD_ID} case=roundtrip_page_id pid={pid}"
        );
    }

    // Verify swizzled encoding.
    let test_addrs: Vec<u64> = vec![0x1000, 0x2000, 0x100000, 0xFFFF_FFFE];
    for &addr in &test_addrs {
        let ptr = SwizzlePtr::new_swizzled(addr).unwrap();
        let raw = ptr.load_raw(Ordering::Acquire);

        // Swizzled: bit 0 should be 1.
        assert_eq!(
            raw & SWIZZLED_TAG, SWIZZLED_TAG,
            "bead_id={BEAD_ID} case=swizzled_tag_set addr={addr:#x}"
        );

        let state = ptr.state(Ordering::Acquire);
        assert_eq!(
            state,
            SwizzleState::Swizzled { frame_addr: addr },
            "bead_id={BEAD_ID} case=roundtrip_frame_addr addr={addr:#x}"
        );
    }

    // Unaligned address should be rejected.
    assert!(SwizzlePtr::new_swizzled(0x1001).is_err());
}

// ── 7. Concurrent registry access ────────────────────────────────────────

#[test]
fn test_concurrent_registry_access() {
    let reg = Arc::new(SwizzleRegistry::new());
    let n_pages = 100;

    // Register all pages.
    for pid in 0..n_pages {
        reg.register_page(pid);
    }

    // Spawn threads that swizzle disjoint page ranges.
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let reg = Arc::clone(&reg);
            std::thread::spawn(move || {
                let start = t * 25;
                let end = start + 25;
                for pid in start..end {
                    let addr = (pid + 1) * 0x1000;
                    reg.try_swizzle(pid, addr);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        reg.swizzled_count(),
        n_pages as usize,
        "bead_id={BEAD_ID} case=all_pages_swizzled_concurrently"
    );

    // All frame addresses correct.
    for pid in 0..n_pages {
        assert_eq!(
            reg.frame_addr(pid),
            Some((pid + 1) * 0x1000),
            "bead_id={BEAD_ID} case=concurrent_frame_addr pid={pid}"
        );
    }
}

// ── 8. Error handling edge cases ─────────────────────────────────────────

#[test]
fn test_error_handling_edge_cases() {
    // Page ID overflow.
    assert!(
        SwizzlePtr::new_unswizzled(MAX_PAGE_ID + 1).is_err(),
        "bead_id={BEAD_ID} case=page_id_overflow"
    );

    // Max valid page ID works.
    let ptr = SwizzlePtr::new_unswizzled(MAX_PAGE_ID).unwrap();
    assert_eq!(
        ptr.state(Ordering::Acquire),
        SwizzleState::Unswizzled { page_id: MAX_PAGE_ID },
        "bead_id={BEAD_ID} case=max_page_id"
    );

    // Unaligned frame address.
    assert!(
        SwizzlePtr::new_swizzled(0x1001).is_err(),
        "bead_id={BEAD_ID} case=unaligned_addr"
    );

    // Zero page ID and zero address.
    let ptr0 = SwizzlePtr::new_unswizzled(0).unwrap();
    assert_eq!(
        ptr0.state(Ordering::Acquire),
        SwizzleState::Unswizzled { page_id: 0 },
        "bead_id={BEAD_ID} case=zero_page_id"
    );

    // Registry operations on unregistered pages.
    let reg = SwizzleRegistry::new();
    assert!(!reg.try_swizzle(999, 0x1000), "bead_id={BEAD_ID} case=swizzle_unregistered");
    assert!(!reg.try_unswizzle(999), "bead_id={BEAD_ID} case=unswizzle_unregistered");
    assert!(!reg.is_swizzled(999), "bead_id={BEAD_ID} case=query_unregistered");
}

// ── 9. Metrics fidelity ──────────────────────────────────────────────────

#[test]
fn test_metrics_fidelity() {
    let before = btree_metrics_snapshot();

    let reg = SwizzleRegistry::new();
    for pid in 0..5 {
        reg.register_page(pid);
    }

    // Swizzle 3 pages (should produce 3 swizzle_in events).
    reg.try_swizzle(0, 0x1000);
    reg.try_swizzle(1, 0x2000);
    reg.try_swizzle(2, 0x3000);

    // Attempt to swizzle already-swizzled page (should produce fault).
    reg.try_swizzle(0, 0x9000);

    // Unswizzle 1 page (should produce 1 swizzle_out event).
    reg.try_unswizzle(2);

    let after = btree_metrics_snapshot();

    let delta_in = after.fsqlite_swizzle_in_total - before.fsqlite_swizzle_in_total;
    let delta_out = after.fsqlite_swizzle_out_total - before.fsqlite_swizzle_out_total;
    let delta_faults = after.fsqlite_swizzle_faults_total - before.fsqlite_swizzle_faults_total;

    assert!(
        delta_in >= 3,
        "bead_id={BEAD_ID} case=swizzle_in_metric delta_in={delta_in}"
    );
    assert!(
        delta_out >= 1,
        "bead_id={BEAD_ID} case=swizzle_out_metric delta_out={delta_out}"
    );
    assert!(
        delta_faults >= 1,
        "bead_id={BEAD_ID} case=swizzle_fault_metric delta_faults={delta_faults}"
    );

    println!(
        "[{BEAD_ID}] delta_in={delta_in} delta_out={delta_out} delta_faults={delta_faults}"
    );
}

// ── 10. Conformance summary ──────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    // SwizzlePtr CAS roundtrip.
    let ptr = SwizzlePtr::new_unswizzled(7).unwrap();
    ptr.try_swizzle(7, 0x8000).unwrap();
    ptr.try_unswizzle(0x8000, 7).unwrap();
    let pass_cas = ptr.state(Ordering::Acquire) == SwizzleState::Unswizzled { page_id: 7 };

    // Tag-bit encoding.
    let pass_tag = {
        let u = SwizzlePtr::new_unswizzled(42).unwrap();
        let s = SwizzlePtr::new_swizzled(0x2000).unwrap();
        (u.load_raw(Ordering::Acquire) & SWIZZLED_TAG == 0)
            && (s.load_raw(Ordering::Acquire) & SWIZZLED_TAG == SWIZZLED_TAG)
    };

    // Registry swizzle/unswizzle.
    let reg = SwizzleRegistry::new();
    reg.register_page(1);
    reg.try_swizzle(1, 0x1000);
    let pass_reg_swizzle = reg.is_swizzled(1) && reg.frame_addr(1) == Some(0x1000);
    reg.try_unswizzle(1);
    let pass_reg_unswizzle = !reg.is_swizzled(1) && reg.frame_addr(1).is_none();

    // Temperature state machine.
    let pass_temp = PageTemperature::Hot.transition(PageTemperature::Cooling).is_ok()
        && PageTemperature::Cooling.transition(PageTemperature::Cold).is_ok()
        && PageTemperature::Hot.transition(PageTemperature::Cold).is_err();

    // Concurrent safety (basic: no panics).
    let reg2 = Arc::new(SwizzleRegistry::new());
    for i in 0..10 { reg2.register_page(i); }
    let handles: Vec<_> = (0..4).map(|t| {
        let r = Arc::clone(&reg2);
        std::thread::spawn(move || {
            for i in 0..10 { r.try_swizzle(i, (t * 10 + i + 1) * 0x100); }
        })
    }).collect();
    let pass_concurrent = handles.into_iter().all(|h| h.join().is_ok());

    let checks = [
        pass_cas,
        pass_tag,
        pass_reg_swizzle,
        pass_reg_unswizzle,
        pass_temp,
        pass_concurrent,
    ];
    let passed = checks.iter().filter(|&&p| p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} SwizzlePtr Conformance ===");
    println!("  CAS roundtrip:    {}", if pass_cas { "PASS" } else { "FAIL" });
    println!("  tag-bit encoding: {}", if pass_tag { "PASS" } else { "FAIL" });
    println!("  registry swizzle: {}", if pass_reg_swizzle { "PASS" } else { "FAIL" });
    println!("  registry unswizzle: {}", if pass_reg_unswizzle { "PASS" } else { "FAIL" });
    println!("  temperature FSM:  {}", if pass_temp { "PASS" } else { "FAIL" });
    println!("  concurrent:       {}", if pass_concurrent { "PASS" } else { "FAIL" });
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}"
    );
}
