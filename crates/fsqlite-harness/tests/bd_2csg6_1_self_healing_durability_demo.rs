//! Harness integration tests for bd-2csg6.1: Self-healing durability demo.
//!
//! Validates: RaptorQ self-healing pipeline — encode repair symbols, corrupt
//! pages, detect corruption via BLAKE3, repair using RaptorQ decoding, verify
//! repair witnesses. Demonstrates the full write → corrupt → detect → repair →
//! verify pipeline with evidence ledger entries.

use fsqlite_core::db_fec::{
    DbFecGroupMeta, compute_db_gen_digest, compute_raptorq_repair_symbols, page_xxh3_128,
};
use fsqlite_core::repair_engine::{
    RepairOutcome, blake3_page_checksum, detect_and_repair_page, detect_and_repair_pages,
    verify_page_blake3,
};

const BEAD_ID: &str = "bd-2csg6.1";
const PAGE_SIZE: usize = 1024;
const GROUP_K: u32 = 20;
const GROUP_R: u32 = 10;
const TOTAL_PAGES: u32 = 200;

/// Deterministic page content indexed by 1-based page number.
#[allow(clippy::cast_possible_truncation)]
fn make_page(pgno: u32) -> Vec<u8> {
    let mut data = vec![0u8; PAGE_SIZE];
    for (j, b) in data.iter_mut().enumerate() {
        *b = ((pgno as usize * 41 + j * 7 + 13) & 0xFF) as u8;
    }
    data
}

fn make_all_pages() -> Vec<Vec<u8>> {
    (1..=TOTAL_PAGES).map(make_page).collect()
}

/// Build group metadata and repair symbols for a group of source pages.
#[allow(clippy::cast_possible_truncation)]
fn build_group(
    group_pages: &[Vec<u8>],
    start_pgno: u32,
    r_repair: u32,
) -> (DbFecGroupMeta, Vec<(u32, Vec<u8>)>) {
    let k = group_pages.len() as u32;
    let hashes: Vec<[u8; 16]> = group_pages.iter().map(|d| page_xxh3_128(d)).collect();
    let digest = compute_db_gen_digest(1, TOTAL_PAGES, 0, 1);
    let meta = DbFecGroupMeta::new(PAGE_SIZE as u32, start_pgno, k, r_repair, hashes, digest);

    let slices: Vec<&[u8]> = group_pages.iter().map(Vec::as_slice).collect();
    let repair_data =
        compute_raptorq_repair_symbols(&meta, &slices, PAGE_SIZE).expect("RaptorQ encode");
    let repair_symbols: Vec<(u32, Vec<u8>)> = repair_data
        .into_iter()
        .enumerate()
        .map(|(i, d)| (k + u32::try_from(i).expect("index fits u32"), d))
        .collect();

    (meta, repair_symbols)
}

fn build_all_groups(original_pages: &[Vec<u8>]) -> Vec<(DbFecGroupMeta, Vec<(u32, Vec<u8>)>)> {
    let num_groups = TOTAL_PAGES / GROUP_K;
    (0..num_groups)
        .map(|g| {
            let start_pgno = g * GROUP_K + 1;
            let group_pages: Vec<Vec<u8>> = (0..GROUP_K)
                .map(|i| original_pages[(start_pgno + i - 1) as usize].clone())
                .collect();
            build_group(&group_pages, start_pgno, GROUP_R)
        })
        .collect()
}

/// Apply bit-flip corruption to a page (deterministic, LCG-based).
fn corrupt_bit_flip(data: &mut [u8], seed: u64) {
    let mut state = seed;
    let flips = 4;
    for _ in 0..flips {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let byte_idx = (state >> 16) as usize % data.len();
        let bit_idx = (state >> 8) as u8 % 8;
        data[byte_idx] ^= 1 << bit_idx;
    }
}

/// Apply zero-fill corruption.
fn corrupt_zero_fill(data: &mut [u8]) {
    data.fill(0);
}

// ── 1. Encode repair symbols ────────────────────────────────────────────────

#[test]
fn test_encode_repair_symbols() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    let mut total_repair_symbols = 0;
    for (meta, symbols) in &groups {
        assert_eq!(
            symbols.len(), GROUP_R as usize,
            "bead_id={BEAD_ID} case=repair_count group_start={}",
            meta.start_pgno,
        );
        for (esi, data) in symbols {
            assert_eq!(
                data.len(), PAGE_SIZE,
                "bead_id={BEAD_ID} case=symbol_size esi={esi}",
            );
            assert!(
                *esi >= GROUP_K,
                "bead_id={BEAD_ID} case=repair_esi_range esi={esi} k={GROUP_K}",
            );
        }
        total_repair_symbols += symbols.len();
    }

    let num_groups = TOTAL_PAGES / GROUP_K;
    let overhead_pct = (total_repair_symbols as f64 / TOTAL_PAGES as f64) * 100.0;

    println!(
        "[{BEAD_ID}] encode: {num_groups} groups, {TOTAL_PAGES} pages, {total_repair_symbols} repair symbols ({overhead_pct:.0}% overhead)"
    );

    assert_eq!(
        total_repair_symbols, (num_groups * GROUP_R) as usize,
        "bead_id={BEAD_ID} case=total_repair_symbols",
    );
}

// ── 2. BLAKE3 corruption detection ──────────────────────────────────────────

#[test]
fn test_blake3_corruption_detection() {
    let page = make_page(42);
    let checksum = blake3_page_checksum(&page);

    // Intact page should verify.
    assert!(
        verify_page_blake3(&page, &checksum),
        "bead_id={BEAD_ID} case=intact_verifies",
    );

    // Corrupted page should NOT verify.
    let mut corrupted = page.clone();
    corrupted[0] ^= 0xFF;
    assert!(
        !verify_page_blake3(&corrupted, &checksum),
        "bead_id={BEAD_ID} case=corrupt_detected",
    );

    println!("[{BEAD_ID}] BLAKE3 detection: intact=PASS, corrupt=DETECTED");
}

// ── 3. Single page repair with witness ──────────────────────────────────────

#[test]
fn test_single_page_repair_with_witness() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    // Corrupt page 5 (group 0, offset 4 within group).
    let target_pgno = 5_u32;
    let original = &pages[(target_pgno - 1) as usize];
    let expected_blake3 = blake3_page_checksum(original);

    let mut corrupted = original.clone();
    corrupt_bit_flip(&mut corrupted, 0xDEADBEEF);
    assert!(!verify_page_blake3(&corrupted, &expected_blake3));

    let (ref meta, ref repair_syms) = groups[0]; // Group 0 covers pages 1..=20.
    let all_pages = pages.clone();

    let outcome = detect_and_repair_page(
        target_pgno,
        &corrupted,
        &expected_blake3,
        meta,
        &|pgno| {
            if pgno == target_pgno {
                corrupted.clone()
            } else {
                all_pages[(pgno - 1) as usize].clone()
            }
        },
        repair_syms,
    );

    match outcome {
        RepairOutcome::Repaired { pgno, repaired_data, witness } => {
            assert_eq!(pgno, target_pgno, "bead_id={BEAD_ID} case=repaired_pgno");
            assert!(
                verify_page_blake3(&repaired_data, &expected_blake3),
                "bead_id={BEAD_ID} case=repaired_blake3_match",
            );
            assert!(witness.verified, "bead_id={BEAD_ID} case=witness_verified");
            assert_eq!(
                witness.expected_hash, expected_blake3,
                "bead_id={BEAD_ID} case=witness_expected_hash",
            );
            assert_ne!(
                witness.corrupted_hash, witness.expected_hash,
                "bead_id={BEAD_ID} case=witness_corruption_visible",
            );
            println!(
                "[{BEAD_ID}] single repair: pgno={pgno} verified={} symbols_used={} corrupt_in_group={}",
                witness.verified, witness.symbols_used, witness.corrupt_pages_in_group,
            );
        }
        other => panic!("bead_id={BEAD_ID} case=expected_repaired got={other:?}"),
    }
}

// ── 4. Multi-page corruption and batch repair ───────────────────────────────

#[test]
fn test_multi_page_batch_repair() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    // Corrupt 3 pages in group 0: pages 2, 7, 14.
    let corrupt_targets = [2_u32, 7, 14];
    let mut corrupted_pages = pages.clone();
    for &pgno in &corrupt_targets {
        corrupt_bit_flip(&mut corrupted_pages[(pgno - 1) as usize], pgno as u64 * 12345);
    }

    let (ref meta, ref repair_syms) = groups[0];
    let all_expected: Vec<[u8; 32]> = pages.iter().map(|p| blake3_page_checksum(p)).collect();
    let corrupted_clone = corrupted_pages.clone();

    let outcomes = detect_and_repair_pages(
        &corrupt_targets,
        meta,
        &|pgno| corrupted_clone[(pgno - 1) as usize].clone(),
        &|pgno| all_expected[(pgno - 1) as usize],
        repair_syms,
    );

    let mut repaired_count = 0;
    for outcome in &outcomes {
        match outcome {
            RepairOutcome::Repaired { pgno, witness, .. } => {
                assert!(witness.verified, "bead_id={BEAD_ID} case=batch_witness pgno={pgno}");
                repaired_count += 1;
            }
            RepairOutcome::Intact { .. } => {}
            RepairOutcome::Unrecoverable { pgno, detail, .. } => {
                panic!("bead_id={BEAD_ID} case=batch_unrecoverable pgno={pgno} detail={detail}");
            }
        }
    }

    println!(
        "[{BEAD_ID}] batch repair: {repaired_count}/{} pages repaired in group 0",
        corrupt_targets.len(),
    );
    assert_eq!(
        repaired_count, corrupt_targets.len(),
        "bead_id={BEAD_ID} case=all_corrupted_repaired",
    );
}

// ── 5. Intact page detection ────────────────────────────────────────────────

#[test]
fn test_intact_page_detection() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    let target_pgno = 10_u32;
    let original = &pages[(target_pgno - 1) as usize];
    let expected_blake3 = blake3_page_checksum(original);

    let (ref meta, ref repair_syms) = groups[0];
    let all_pages = pages.clone();

    let outcome = detect_and_repair_page(
        target_pgno,
        original,
        &expected_blake3,
        meta,
        &|pgno| all_pages[(pgno - 1) as usize].clone(),
        repair_syms,
    );

    match outcome {
        RepairOutcome::Intact { pgno, blake3_hash } => {
            assert_eq!(pgno, target_pgno, "bead_id={BEAD_ID} case=intact_pgno");
            assert_eq!(blake3_hash, expected_blake3, "bead_id={BEAD_ID} case=intact_hash");
            println!("[{BEAD_ID}] intact detection: page {pgno} correctly identified as intact");
        }
        other => panic!("bead_id={BEAD_ID} case=expected_intact got={other:?}"),
    }
}

// ── 6. Zero-fill corruption and repair ──────────────────────────────────────

#[test]
fn test_zero_fill_corruption_repair() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    let target_pgno = 15_u32;
    let original = &pages[(target_pgno - 1) as usize];
    let expected_blake3 = blake3_page_checksum(original);

    let mut corrupted = original.clone();
    corrupt_zero_fill(&mut corrupted);
    assert!(!verify_page_blake3(&corrupted, &expected_blake3));

    let (ref meta, ref repair_syms) = groups[0];
    let all_pages = pages.clone();

    let outcome = detect_and_repair_page(
        target_pgno,
        &corrupted,
        &expected_blake3,
        meta,
        &|pgno| {
            if pgno == target_pgno {
                corrupted.clone()
            } else {
                all_pages[(pgno - 1) as usize].clone()
            }
        },
        repair_syms,
    );

    match outcome {
        RepairOutcome::Repaired { pgno, repaired_data, witness } => {
            assert!(witness.verified, "bead_id={BEAD_ID} case=zerofill_witness");
            assert_eq!(
                repaired_data, *original,
                "bead_id={BEAD_ID} case=zerofill_data_match pgno={pgno}",
            );
            println!(
                "[{BEAD_ID}] zero-fill repair: pgno={pgno} verified={} symbols={}",
                witness.verified, witness.symbols_used,
            );
        }
        other => panic!("bead_id={BEAD_ID} case=expected_zerofill_repair got={other:?}"),
    }
}

// ── 7. Graceful degradation beyond repair capacity ──────────────────────────

#[test]
fn test_graceful_degradation_beyond_capacity() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    // Corrupt 15 out of 20 pages in group 2 (pages 41..60) — exceeds R=10.
    let group_idx = 2;
    let start_pgno = group_idx * GROUP_K + 1; // 41
    let corrupt_count = 15;
    let corrupt_pgnos: Vec<u32> = (start_pgno..start_pgno + corrupt_count).collect();

    let mut corrupted_pages = pages.clone();
    for &pgno in &corrupt_pgnos {
        corrupt_zero_fill(&mut corrupted_pages[(pgno - 1) as usize]);
    }

    let (ref meta, ref repair_syms) = groups[group_idx as usize];
    let all_expected: Vec<[u8; 32]> = pages.iter().map(|p| blake3_page_checksum(p)).collect();
    let corrupted_clone = corrupted_pages.clone();

    let outcomes = detect_and_repair_pages(
        &corrupt_pgnos,
        meta,
        &|pgno| corrupted_clone[(pgno - 1) as usize].clone(),
        &|pgno| all_expected[(pgno - 1) as usize],
        repair_syms,
    );

    let unrecoverable_count = outcomes
        .iter()
        .filter(|o| matches!(o, RepairOutcome::Unrecoverable { .. }))
        .count();

    println!(
        "[{BEAD_ID}] degradation: {corrupt_count} corrupted in group (R={GROUP_R}), {unrecoverable_count} unrecoverable"
    );

    // With 15 corruptions and only 10 repair symbols, some should be unrecoverable.
    assert!(
        unrecoverable_count > 0,
        "bead_id={BEAD_ID} case=some_unrecoverable",
    );
}

// ── 8. Witness proof completeness ───────────────────────────────────────────

#[test]
fn test_witness_proof_completeness() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    let target_pgno = 3_u32;
    let original = &pages[(target_pgno - 1) as usize];
    let expected_blake3 = blake3_page_checksum(original);

    let mut corrupted = original.clone();
    corrupt_bit_flip(&mut corrupted, 0xCAFEBABE);

    let (ref meta, ref repair_syms) = groups[0];
    let all_pages = pages.clone();

    let outcome = detect_and_repair_page(
        target_pgno,
        &corrupted,
        &expected_blake3,
        meta,
        &|pgno| {
            if pgno == target_pgno { corrupted.clone() }
            else { all_pages[(pgno - 1) as usize].clone() }
        },
        repair_syms,
    );

    if let RepairOutcome::Repaired { witness, .. } = outcome {
        // Witness triple must be complete.
        assert!(witness.verified, "bead_id={BEAD_ID} case=witness_verified");
        assert_ne!(
            witness.corrupted_hash, witness.expected_hash,
            "bead_id={BEAD_ID} case=corrupted_ne_expected",
        );
        assert_eq!(
            witness.repaired_hash, witness.expected_hash,
            "bead_id={BEAD_ID} case=repaired_eq_expected",
        );
        assert!(
            witness.symbols_used > 0,
            "bead_id={BEAD_ID} case=symbols_used_positive",
        );
        assert!(
            witness.corrupt_pages_in_group >= 1,
            "bead_id={BEAD_ID} case=corrupt_count_positive",
        );

        // Witness is serializable (evidence ledger format).
        let json = serde_json::to_string_pretty(&witness).expect("witness serializes");
        assert!(json.contains("corrupted_hash"), "bead_id={BEAD_ID} case=json_has_corrupted");
        assert!(json.contains("repaired_hash"), "bead_id={BEAD_ID} case=json_has_repaired");
        assert!(json.contains("expected_hash"), "bead_id={BEAD_ID} case=json_has_expected");

        println!("[{BEAD_ID}] witness proof:");
        println!("{json}");
    } else {
        panic!("bead_id={BEAD_ID} case=expected_repaired_for_witness");
    }
}

// ── 9. Repair determinism ───────────────────────────────────────────────────

#[test]
fn test_repair_determinism() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    let target_pgno = 8_u32;
    let original = &pages[(target_pgno - 1) as usize];
    let expected_blake3 = blake3_page_checksum(original);

    let mut corrupted = original.clone();
    corrupt_bit_flip(&mut corrupted, 0x1234);

    let (ref meta, ref repair_syms) = groups[0];
    let all_pages = pages.clone();
    let corrupted_ref = corrupted.clone();

    // Run repair twice — should produce identical results.
    let mut results = Vec::new();
    for _ in 0..2 {
        let corrupted_copy = corrupted_ref.clone();
        let all_pages_copy = all_pages.clone();
        let outcome = detect_and_repair_page(
            target_pgno,
            &corrupted_copy,
            &expected_blake3,
            meta,
            &|pgno| {
                if pgno == target_pgno { corrupted_copy.clone() }
                else { all_pages_copy[(pgno - 1) as usize].clone() }
            },
            repair_syms,
        );
        if let RepairOutcome::Repaired { repaired_data, witness, .. } = outcome {
            results.push((repaired_data, witness));
        } else {
            panic!("bead_id={BEAD_ID} case=expected_repaired_for_determinism");
        }
    }

    assert_eq!(
        results[0].0, results[1].0,
        "bead_id={BEAD_ID} case=repair_deterministic_data",
    );
    assert_eq!(
        results[0].1.repaired_hash, results[1].1.repaired_hash,
        "bead_id={BEAD_ID} case=repair_deterministic_hash",
    );

    println!("[{BEAD_ID}] repair determinism: two runs produce identical output");
}

// ── 10. Conformance summary ─────────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    let pages = make_all_pages();
    let groups = build_all_groups(&pages);

    // 1. Repair symbol generation.
    let pass_encode = groups.iter().all(|(_, syms)| syms.len() == GROUP_R as usize);

    // 2. BLAKE3 detection.
    let page = make_page(1);
    let hash = blake3_page_checksum(&page);
    let pass_detect = verify_page_blake3(&page, &hash);

    // 3. Single repair.
    let target = 5_u32;
    let original = &pages[(target - 1) as usize];
    let expected = blake3_page_checksum(original);
    let mut corrupted = original.clone();
    corrupt_bit_flip(&mut corrupted, 0xAABB);
    let (ref meta, ref syms) = groups[0];
    let all_p = pages.clone();
    let outcome = detect_and_repair_page(
        target, &corrupted, &expected, meta,
        &|pgno| if pgno == target { corrupted.clone() } else { all_p[(pgno - 1) as usize].clone() },
        syms,
    );
    let pass_repair = matches!(outcome, RepairOutcome::Repaired { ref witness, .. } if witness.verified);

    // 4. Intact detection.
    let intact_outcome = detect_and_repair_page(
        10, original, &blake3_page_checksum(original), meta,
        &|pgno| all_p[(pgno - 1) as usize].clone(),
        syms,
    );
    let pass_intact = matches!(intact_outcome, RepairOutcome::Intact { .. });

    // 5. Witness serializable.
    let pass_witness = if let RepairOutcome::Repaired { ref witness, .. } = outcome {
        serde_json::to_string(&witness).is_ok()
    } else {
        false
    };

    // 6. Determinism.
    let mut c2 = original.clone();
    corrupt_bit_flip(&mut c2, 0xAABB);
    let o2 = detect_and_repair_page(
        target, &c2, &expected, meta,
        &|pgno| if pgno == target { c2.clone() } else { all_p[(pgno - 1) as usize].clone() },
        syms,
    );
    let pass_determinism = if let (RepairOutcome::Repaired { repaired_data: d1, .. }, RepairOutcome::Repaired { repaired_data: d2, .. }) = (&outcome, &o2) {
        d1 == d2
    } else {
        false
    };

    let checks = [
        ("repair_symbol_gen", pass_encode),
        ("blake3_detection", pass_detect),
        ("single_page_repair", pass_repair),
        ("intact_detection", pass_intact),
        ("witness_serializable", pass_witness),
        ("repair_determinism", pass_determinism),
    ];
    let passed = checks.iter().filter(|(_, p)| *p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} Self-Healing Durability Conformance ===");
    for (name, ok) in &checks {
        println!("  {name:.<28}{}", if *ok { "PASS" } else { "FAIL" });
    }
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}",
    );
}
