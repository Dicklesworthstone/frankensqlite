//! Property-based tests for bd-2uza4.5: SwizzlePtr and cooling state machine invariants.
//!
//! Uses proptest to verify correctness over random inputs: roundtrip identity,
//! tag-bit integrity, CAS safety, temperature FSM, and concurrent registry
//! invariants.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use proptest::prelude::*;

use fsqlite_btree::swizzle::{
    PageTemperature, SwizzlePtr, SwizzleRegistry, SwizzleState,
    MAX_PAGE_ID, SWIZZLED_TAG,
};

const BEAD_ID: &str = "bd-2uza4.5";

// ── Strategies ───────────────────────────────────────────────────────────

/// Generate valid page IDs (fit in 63 bits).
fn valid_page_id() -> impl Strategy<Value = u64> {
    0..=MAX_PAGE_ID
}

/// Generate valid (even) frame addresses.
fn valid_frame_addr() -> impl Strategy<Value = u64> {
    // Must be even (bit 0 clear). Use range then multiply by 2.
    (1u64..=0x7FFF_FFFF_FFFF_FFFFu64).prop_map(|v| v & !1u64).prop_filter(
        "frame_addr must be even and non-zero",
        |&v| v != 0 && v & 1 == 0,
    )
}

/// Generate a random temperature.
fn any_temperature() -> impl Strategy<Value = PageTemperature> {
    prop_oneof![
        Just(PageTemperature::Hot),
        Just(PageTemperature::Cooling),
        Just(PageTemperature::Cold),
    ]
}

// ── 1. Roundtrip correctness: unswizzled ─────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_unswizzled_roundtrip(page_id in valid_page_id()) {
        let ptr = SwizzlePtr::new_unswizzled(page_id)
            .expect("valid page_id should encode");
        let state = ptr.state(Ordering::Acquire);
        prop_assert_eq!(
            state,
            SwizzleState::Unswizzled { page_id },
            "bead_id={} case=unswizzled_roundtrip pid={}", BEAD_ID, page_id
        );
        prop_assert!(!ptr.is_swizzled(Ordering::Acquire));
    }
}

// ── 2. Roundtrip correctness: swizzled ───────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_swizzled_roundtrip(frame_addr in valid_frame_addr()) {
        let ptr = SwizzlePtr::new_swizzled(frame_addr)
            .expect("valid frame_addr should encode");
        let state = ptr.state(Ordering::Acquire);
        prop_assert_eq!(
            state,
            SwizzleState::Swizzled { frame_addr },
            "bead_id={} case=swizzled_roundtrip addr={:#x}", BEAD_ID, frame_addr
        );
        prop_assert!(ptr.is_swizzled(Ordering::Acquire));
    }
}

// ── 3. Tag-bit integrity ─────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_tag_bit_integrity_unswizzled(page_id in valid_page_id()) {
        let ptr = SwizzlePtr::new_unswizzled(page_id).unwrap();
        let raw = ptr.load_raw(Ordering::Acquire);

        // Bit 0 must be clear for unswizzled.
        prop_assert_eq!(raw & SWIZZLED_TAG, 0,
            "bead_id={} case=unswizzled_tag_bit_clear pid={}", BEAD_ID, page_id);

        // is_swizzled must agree.
        prop_assert!(!ptr.is_swizzled(Ordering::Acquire));
    }

    #[test]
    fn prop_tag_bit_integrity_swizzled(frame_addr in valid_frame_addr()) {
        let ptr = SwizzlePtr::new_swizzled(frame_addr).unwrap();
        let raw = ptr.load_raw(Ordering::Acquire);

        // Bit 0 must be set for swizzled.
        prop_assert_eq!(raw & SWIZZLED_TAG, SWIZZLED_TAG,
            "bead_id={} case=swizzled_tag_bit_set addr={:#x}", BEAD_ID, frame_addr);

        // is_swizzled must agree.
        prop_assert!(ptr.is_swizzled(Ordering::Acquire));
    }
}

// ── 4. CAS swizzle/unswizzle roundtrip ───────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn prop_cas_swizzle_unswizzle_roundtrip(
        page_id in valid_page_id(),
        frame_addr in valid_frame_addr(),
    ) {
        let ptr = SwizzlePtr::new_unswizzled(page_id).unwrap();

        // Swizzle: page_id -> frame_addr.
        ptr.try_swizzle(page_id, frame_addr).expect("swizzle should succeed");
        prop_assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Swizzled { frame_addr }
        );

        // Unswizzle: frame_addr -> page_id.
        ptr.try_unswizzle(frame_addr, page_id).expect("unswizzle should succeed");
        prop_assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Unswizzled { page_id }
        );
    }
}

// ── 5. CAS with wrong expected value fails ───────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn prop_cas_fails_on_mismatch(
        page_id in valid_page_id(),
        wrong_id in valid_page_id(),
        frame_addr in valid_frame_addr(),
    ) {
        prop_assume!(page_id != wrong_id);
        let ptr = SwizzlePtr::new_unswizzled(page_id).unwrap();

        let result = ptr.try_swizzle(wrong_id, frame_addr);
        prop_assert!(result.is_err(),
            "bead_id={} case=cas_mismatch_must_fail pid={} wrong={}", BEAD_ID, page_id, wrong_id);

        // State must be unchanged.
        prop_assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Unswizzled { page_id }
        );
    }
}

// ── 6. Temperature FSM allowed transitions ───────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_temperature_fsm_self_transitions(temp in any_temperature()) {
        // Self-transitions are always allowed.
        prop_assert!(temp.can_transition_to(temp),
            "bead_id={} case=self_transition_{:?}", BEAD_ID, temp);
        prop_assert!(temp.transition(temp).is_ok());
    }
}

#[test]
fn test_temperature_fsm_exhaustive() {
    // Exhaustively test all 9 (from, to) pairs.
    let _temps = [PageTemperature::Hot, PageTemperature::Cooling, PageTemperature::Cold];

    let expected_valid = [
        // (Hot, Hot), (Hot, Cooling)
        (PageTemperature::Hot, PageTemperature::Hot, true),
        (PageTemperature::Hot, PageTemperature::Cooling, true),
        (PageTemperature::Hot, PageTemperature::Cold, false),
        // (Cooling, Hot), (Cooling, Cooling), (Cooling, Cold)
        (PageTemperature::Cooling, PageTemperature::Hot, true),
        (PageTemperature::Cooling, PageTemperature::Cooling, true),
        (PageTemperature::Cooling, PageTemperature::Cold, true),
        // (Cold, Hot), (Cold, Cooling=false), (Cold, Cold)
        (PageTemperature::Cold, PageTemperature::Hot, true),
        (PageTemperature::Cold, PageTemperature::Cooling, false),
        (PageTemperature::Cold, PageTemperature::Cold, true),
    ];

    for (from, to, expected) in expected_valid {
        assert_eq!(
            from.can_transition_to(to),
            expected,
            "bead_id={BEAD_ID} case=fsm_exhaustive_{from:?}_to_{to:?}"
        );
    }
}

// ── 7. Page ID overflow is correctly rejected ────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_overflow_page_id_rejected(offset in 1u64..1000) {
        let bad_id = MAX_PAGE_ID.wrapping_add(offset);
        if bad_id > MAX_PAGE_ID {
            let result = SwizzlePtr::new_unswizzled(bad_id);
            prop_assert!(result.is_err(),
                "bead_id={} case=overflow_rejected id={}", BEAD_ID, bad_id);
        }
    }
}

// ── 8. Unaligned frame addresses rejected ────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn prop_unaligned_frame_addr_rejected(addr in 1u64..=u64::MAX) {
        if addr & 1 == 1 {
            let result = SwizzlePtr::new_swizzled(addr);
            prop_assert!(result.is_err(),
                "bead_id={} case=unaligned_rejected addr={:#x}", BEAD_ID, addr);
        }
    }
}

// ── 9. Registry concurrent swizzle invariant ─────────────────────────────

#[test]
fn test_registry_concurrent_swizzle_no_lost_updates() {
    // Multiple threads swizzle disjoint page sets concurrently.
    // Invariant: every page ends up swizzled exactly once.
    let reg = Arc::new(SwizzleRegistry::new());
    let n_pages_per_thread = 50;
    let n_threads = 4;

    for pid in 0..(n_pages_per_thread * n_threads) {
        reg.register_page(pid as u64);
    }

    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let reg = Arc::clone(&reg);
            std::thread::spawn(move || {
                let start = t * n_pages_per_thread;
                for pid in start..(start + n_pages_per_thread) {
                    let addr = (pid as u64 + 1) * 0x1000;
                    reg.try_swizzle(pid as u64, addr);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        reg.swizzled_count(),
        (n_pages_per_thread * n_threads) as usize,
        "bead_id={BEAD_ID} case=no_lost_updates"
    );

    // Verify each page has the correct frame address.
    for t in 0..n_threads {
        let start = t * n_pages_per_thread;
        for pid in start..(start + n_pages_per_thread) {
            let expected_addr = (pid as u64 + 1) * 0x1000;
            assert_eq!(
                reg.frame_addr(pid as u64),
                Some(expected_addr),
                "bead_id={BEAD_ID} case=correct_addr pid={pid}"
            );
        }
    }
}

// ── 10. Conformance summary ──────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    // Roundtrip: unswizzled.
    let pass_roundtrip_u = {
        let ptr = SwizzlePtr::new_unswizzled(123).unwrap();
        ptr.state(Ordering::Acquire) == SwizzleState::Unswizzled { page_id: 123 }
    };

    // Roundtrip: swizzled.
    let pass_roundtrip_s = {
        let ptr = SwizzlePtr::new_swizzled(0x4000).unwrap();
        ptr.state(Ordering::Acquire) == SwizzleState::Swizzled { frame_addr: 0x4000 }
    };

    // CAS roundtrip.
    let pass_cas = {
        let ptr = SwizzlePtr::new_unswizzled(77).unwrap();
        ptr.try_swizzle(77, 0x8000).is_ok()
            && ptr.try_unswizzle(0x8000, 77).is_ok()
            && ptr.state(Ordering::Acquire) == SwizzleState::Unswizzled { page_id: 77 }
    };

    // FSM: invalid transition rejected.
    let pass_fsm = PageTemperature::Hot.transition(PageTemperature::Cold).is_err()
        && PageTemperature::Cold.transition(PageTemperature::Cooling).is_err();

    // Overflow rejected.
    let pass_overflow = SwizzlePtr::new_unswizzled(MAX_PAGE_ID + 1).is_err();

    // Registry concurrent (basic).
    let pass_concurrent = {
        let reg = Arc::new(SwizzleRegistry::new());
        for i in 0..20 { reg.register_page(i); }
        let handles: Vec<_> = (0..4).map(|t| {
            let r = Arc::clone(&reg);
            std::thread::spawn(move || {
                for i in (t * 5)..((t + 1) * 5) {
                    r.try_swizzle(i, (i + 1) * 0x100);
                }
            })
        }).collect();
        handles.into_iter().all(|h| h.join().is_ok())
            && reg.swizzled_count() == 20
    };

    let checks = [
        pass_roundtrip_u,
        pass_roundtrip_s,
        pass_cas,
        pass_fsm,
        pass_overflow,
        pass_concurrent,
    ];
    let passed = checks.iter().filter(|&&p| p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} SwizzlePtr Property Test Conformance ===");
    println!("  roundtrip (unswizzled): {}", if pass_roundtrip_u { "PASS" } else { "FAIL" });
    println!("  roundtrip (swizzled):   {}", if pass_roundtrip_s { "PASS" } else { "FAIL" });
    println!("  CAS roundtrip:          {}", if pass_cas { "PASS" } else { "FAIL" });
    println!("  FSM invariant:          {}", if pass_fsm { "PASS" } else { "FAIL" });
    println!("  overflow rejection:     {}", if pass_overflow { "PASS" } else { "FAIL" });
    println!("  concurrent safety:      {}", if pass_concurrent { "PASS" } else { "FAIL" });
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}"
    );
}
