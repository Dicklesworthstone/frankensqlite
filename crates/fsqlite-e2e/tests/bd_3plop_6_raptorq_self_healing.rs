//! Bead bd-3plop.6: E2E test — RaptorQ self-healing with 5% random page corruption.
//!
//! Showcase test for FrankenSQLite's core differentiator: transparent page-level
//! self-healing using RaptorQ (RFC 6330) erasure coding with BLAKE3 corruption
//! detection and auditable witness proofs.
//!
//! ## Coverage
//!
//! - 5% random corruption across multiple groups → 100% repair success
//! - All four corruption variants (bit flips, zero-fill, random overwrite, header)
//! - Evidence entries with valid BLAKE3 witness proofs for every repair
//! - Permanent repair verification (re-check after repair confirms persistence)
//! - 10% corruption with 50% repair overhead → still succeeds (margin)
//! - Graceful degradation when corruption exceeds repair capacity
//! - Batch-mode repair across whole groups (intact pages correctly identified)
//! - CI-safe: deterministic seeding, completes in < 30 seconds

use std::collections::BTreeSet;

use fsqlite_core::db_fec::{
    DbFecGroupMeta, compute_db_gen_digest, compute_raptorq_repair_symbols, page_xxh3_128,
};
use fsqlite_core::repair_engine::{
    RepairOutcome, RepairWitness, blake3_page_checksum, detect_and_repair_page,
    detect_and_repair_pages,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// ── Constants ───────────────────────────────────────────────────────────

/// Page size for all tests.  1024 bytes balances realism with CI speed.
const PAGE_SIZE: usize = 1024;

/// Deterministic seed for reproducible corruption patterns.
const SEED: u64 = 0xBD_3010_0006;

/// Source pages per group (K).
const GROUP_K: u32 = 20;

/// Repair symbols per group (R = 50% of K per bead specification).
const GROUP_R: u32 = 10;

/// Number of groups in the simulated database.
const NUM_GROUPS: u32 = 10;

/// Total page count across all groups.
const TOTAL_PAGES: u32 = GROUP_K * NUM_GROUPS;

type RepairGroup = (DbFecGroupMeta, Vec<(u32, Vec<u8>)>);

// ── Corruption variants ─────────────────────────────────────────────────

/// Four corruption variants as specified in the bead acceptance criteria.
#[derive(Debug, Clone, Copy)]
enum CorruptionVariant {
    /// Random bit flips (1-8 per page) — simulates cosmic rays / bit rot.
    BitFlip,
    /// Zero-fill entire page — simulates sector failure.
    ZeroFill,
    /// Random data overwrite — simulates stray writes.
    RandomOverwrite,
    /// Header corruption — first 16 bytes randomized.
    HeaderCorrupt,
}

const ALL_VARIANTS: [CorruptionVariant; 4] = [
    CorruptionVariant::BitFlip,
    CorruptionVariant::ZeroFill,
    CorruptionVariant::RandomOverwrite,
    CorruptionVariant::HeaderCorrupt,
];

// ── Helpers ─────────────────────────────────────────────────────────────

/// Build deterministic page content indexed by 1-based page number.
#[allow(clippy::cast_possible_truncation)]
fn make_page(pgno: u32) -> Vec<u8> {
    let mut data = vec![0u8; PAGE_SIZE];
    for (j, b) in data.iter_mut().enumerate() {
        *b = ((pgno as usize * 41 + j * 7 + 13) & 0xFF) as u8;
    }
    data
}

/// Build all pages for the simulated database (1-indexed internally).
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

/// Build all groups for the simulated database.
fn build_all_groups(original_pages: &[Vec<u8>]) -> Vec<RepairGroup> {
    (0..NUM_GROUPS)
        .map(|g| {
            let start_pgno = g * GROUP_K + 1;
            let group_pages: Vec<Vec<u8>> = (0..GROUP_K)
                .map(|i| original_pages[(start_pgno + i - 1) as usize].clone())
                .collect();
            build_group(&group_pages, start_pgno, GROUP_R)
        })
        .collect()
}

/// Apply a corruption variant to page data in-place.
fn apply_corruption(data: &mut [u8], variant: CorruptionVariant, rng: &mut StdRng) {
    match variant {
        CorruptionVariant::BitFlip => {
            let flips = rng.gen_range(1..=8u32);
            for _ in 0..flips {
                let byte_idx = rng.gen_range(0..data.len());
                let bit_idx = rng.gen_range(0..8u8);
                data[byte_idx] ^= 1 << bit_idx;
            }
        }
        CorruptionVariant::ZeroFill => {
            data.fill(0);
        }
        CorruptionVariant::RandomOverwrite => {
            rng.fill(&mut data[..]);
        }
        CorruptionVariant::HeaderCorrupt => {
            let header_len = 16.min(data.len());
            for b in &mut data[..header_len] {
                *b = rng.r#gen();
            }
        }
    }
}

/// Select corruption targets: `percent`% of total pages, each assigned a
/// round-robin corruption variant to guarantee all four variants appear.
fn select_corruption_targets(percent: f64, rng: &mut StdRng) -> Vec<(u32, CorruptionVariant)> {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let count = ((f64::from(TOTAL_PAGES)) * percent / 100.0).ceil() as usize;

    let mut selected = BTreeSet::new();
    while selected.len() < count {
        let pgno = rng.gen_range(1..=TOTAL_PAGES);
        selected.insert(pgno);
    }

    selected
        .into_iter()
        .enumerate()
        .map(|(i, pgno)| (pgno, ALL_VARIANTS[i % ALL_VARIANTS.len()]))
        .collect()
}

/// Count corruption targets per group to verify distribution.
fn corruption_per_group(targets: &[(u32, CorruptionVariant)]) -> Vec<u32> {
    let mut counts = vec![0u32; NUM_GROUPS as usize];
    for &(pgno, _) in targets {
        let group_idx = (pgno - 1) / GROUP_K;
        counts[group_idx as usize] += 1;
    }
    counts
}

// ── Tests ───────────────────────────────────────────────────────────────

/// Main showcase test: 5% random corruption with all four variants, full
/// repair, evidence validation, witness proofs, and permanent repair.
#[test]
#[allow(clippy::too_many_lines)]
fn test_bd_3plop_6_five_percent_corruption_fully_repaired() {
    let mut rng = StdRng::seed_from_u64(SEED);

    // ── Step 1: Create database with known data ─────────────────────
    let original_pages = make_all_pages();
    assert_eq!(original_pages.len(), TOTAL_PAGES as usize);

    // ── Step 2: Build repair symbols (simulates checkpoint) ─────────
    let groups = build_all_groups(&original_pages);

    // ── Step 3: Corrupt 5% of pages with mixed variants ─────────────
    let mut corrupted_pages = original_pages.clone();
    let targets = select_corruption_targets(5.0, &mut rng);
    let corrupt_count = targets.len();
    assert!(
        corrupt_count >= 10,
        "5% of {TOTAL_PAGES} must be >= 10, got {corrupt_count}"
    );

    // Verify all four variants appear.
    let variant_names: BTreeSet<&str> = targets
        .iter()
        .map(|(_, v)| match v {
            CorruptionVariant::BitFlip => "bit_flip",
            CorruptionVariant::ZeroFill => "zero_fill",
            CorruptionVariant::RandomOverwrite => "random_overwrite",
            CorruptionVariant::HeaderCorrupt => "header_corrupt",
        })
        .collect();
    assert_eq!(
        variant_names.len(),
        4,
        "all four corruption variants must be exercised"
    );

    // Verify no group exceeds repair budget.
    let per_group = corruption_per_group(&targets);
    for (g, &count) in per_group.iter().enumerate() {
        assert!(
            count <= GROUP_R,
            "group {g} has {count} corrupt pages, exceeds R={GROUP_R}"
        );
    }

    // Apply corruption.
    for &(pgno, variant) in &targets {
        apply_corruption(&mut corrupted_pages[(pgno - 1) as usize], variant, &mut rng);
    }

    // Confirm corruption actually changed the pages.
    for &(pgno, _) in &targets {
        let original_hash = blake3_page_checksum(&original_pages[(pgno - 1) as usize]);
        let corrupted_hash = blake3_page_checksum(&corrupted_pages[(pgno - 1) as usize]);
        assert_ne!(
            original_hash, corrupted_hash,
            "page {pgno} must be corrupted (BLAKE3 must differ)"
        );
    }

    // ── Step 4: Repair each corrupted page and collect evidence ─────
    let mut witnesses: Vec<RepairWitness> = Vec::new();
    let mut repaired_pages = corrupted_pages.clone();
    let corrupt_set: BTreeSet<u32> = targets.iter().map(|&(p, _)| p).collect();

    for &(pgno, _) in &targets {
        let group_idx = ((pgno - 1) / GROUP_K) as usize;
        let (meta, repair_syms) = &groups[group_idx];
        let expected_blake3 = blake3_page_checksum(&original_pages[(pgno - 1) as usize]);

        // Closure reads from the corrupted snapshot (other pages may also be corrupt).
        let corrupted_snapshot = corrupted_pages.clone();
        let all_page_data = |p: u32| -> Vec<u8> { corrupted_snapshot[(p - 1) as usize].clone() };

        let outcome = detect_and_repair_page(
            pgno,
            &corrupted_pages[(pgno - 1) as usize],
            &expected_blake3,
            meta,
            &all_page_data,
            repair_syms,
        );

        match outcome {
            RepairOutcome::Repaired {
                pgno: repaired_pgno,
                repaired_data,
                witness,
            } => {
                assert_eq!(
                    repaired_data,
                    original_pages[(pgno - 1) as usize],
                    "repaired page {pgno} must exactly match original"
                );
                repaired_pages[(repaired_pgno - 1) as usize] = repaired_data;
                witnesses.push(witness);
            }
            RepairOutcome::Intact { pgno: p, .. } => {
                panic!("page {p} was corrupted but reported as intact");
            }
            RepairOutcome::Unrecoverable {
                pgno: p, detail, ..
            } => {
                panic!("page {p} should be repairable with R={GROUP_R}, got: {detail}");
            }
        }
    }

    // ── Step 5: Evidence ledger — one witness per corrupted page ─────
    assert_eq!(
        witnesses.len(),
        corrupt_count,
        "must have exactly one witness per corrupted page"
    );

    // ── Step 6: BLAKE3 witness proof validation ─────────────────────
    for witness in &witnesses {
        let pgno = witness.pgno;
        let original = &original_pages[(pgno - 1) as usize];

        // expected_hash matches pre-corruption BLAKE3.
        assert_eq!(
            witness.expected_hash,
            blake3_page_checksum(original),
            "page {pgno}: expected_hash must match original BLAKE3"
        );

        // repaired_hash matches expected_hash (successful repair).
        assert_eq!(
            witness.repaired_hash, witness.expected_hash,
            "page {pgno}: repaired_hash must equal expected_hash"
        );

        // corrupted_hash differs from expected (actual corruption detected).
        assert_ne!(
            witness.corrupted_hash, witness.expected_hash,
            "page {pgno}: corrupted_hash must differ from expected_hash"
        );

        // Witness reports verified.
        assert!(witness.verified, "page {pgno}: witness must be verified");

        // Symbols used is positive.
        assert!(
            witness.symbols_used > 0,
            "page {pgno}: symbols_used must be > 0"
        );

        // Corrupt pages in group count is reasonable.
        assert!(
            witness.corrupt_pages_in_group >= 1,
            "page {pgno}: must detect at least 1 corrupt page in group"
        );
    }

    // ── Step 7: Batch-mode — verify ALL pages in each group ─────────
    //
    // Non-corrupted pages must report Intact; corrupted pages have already
    // been individually repaired above, so here we verify the batch API
    // sees repaired data as intact when using the repaired snapshot.
    for (g, (meta, repair_syms)) in groups.iter().enumerate() {
        let start = u32::try_from(g).expect("group index fits u32") * GROUP_K + 1;
        let pgnos: Vec<u32> = (start..start + GROUP_K).collect();

        let repaired_snapshot = repaired_pages.clone();
        let all_page_data = |p: u32| -> Vec<u8> { repaired_snapshot[(p - 1) as usize].clone() };
        let expected_blake3s =
            |p: u32| -> [u8; 32] { blake3_page_checksum(&original_pages[(p - 1) as usize]) };

        let outcomes =
            detect_and_repair_pages(&pgnos, meta, &all_page_data, &expected_blake3s, repair_syms);

        for (i, outcome) in outcomes.iter().enumerate() {
            let pgno = start + u32::try_from(i).expect("index fits u32");
            assert!(
                matches!(outcome, RepairOutcome::Intact { .. }),
                "page {pgno} in group {g} must be intact after repair, got: {outcome:?}"
            );
        }
    }

    // ── Step 8: Permanent repair — re-check reports intact ──────────
    for &(pgno, _) in &targets {
        let group_idx = ((pgno - 1) / GROUP_K) as usize;
        let (meta, repair_syms) = &groups[group_idx];
        let expected_blake3 = blake3_page_checksum(&original_pages[(pgno - 1) as usize]);

        let repaired_snapshot = repaired_pages.clone();
        let all_page_data = |p: u32| -> Vec<u8> { repaired_snapshot[(p - 1) as usize].clone() };

        let outcome = detect_and_repair_page(
            pgno,
            &repaired_pages[(pgno - 1) as usize],
            &expected_blake3,
            meta,
            &all_page_data,
            repair_syms,
        );

        assert!(
            matches!(outcome, RepairOutcome::Intact { .. }),
            "page {pgno} must report intact after permanent repair"
        );
    }

    // ── Step 9: Non-corrupted pages were never touched ──────────────
    for pgno in 1..=TOTAL_PAGES {
        if !corrupt_set.contains(&pgno) {
            assert_eq!(
                repaired_pages[(pgno - 1) as usize],
                original_pages[(pgno - 1) as usize],
                "non-corrupted page {pgno} must be unchanged"
            );
        }
    }
}

/// Margin test: 10% corruption with 50% repair overhead still succeeds.
#[test]
fn test_bd_3plop_6_ten_percent_corruption_margin() {
    let mut rng = StdRng::seed_from_u64(SEED + 1);
    let original_pages = make_all_pages();
    let groups = build_all_groups(&original_pages);

    let mut corrupted_pages = original_pages.clone();
    let targets = select_corruption_targets(10.0, &mut rng);
    assert!(
        targets.len() >= 20,
        "10% of {TOTAL_PAGES} must be >= 20, got {}",
        targets.len()
    );

    // Verify per-group distribution stays within budget.
    let per_group = corruption_per_group(&targets);
    let max_per_group = per_group.iter().copied().max().unwrap_or(0);
    assert!(
        max_per_group <= GROUP_R,
        "max {max_per_group} corrupt pages in one group exceeds R={GROUP_R}; \
         re-seed or adjust test parameters"
    );

    for &(pgno, variant) in &targets {
        apply_corruption(&mut corrupted_pages[(pgno - 1) as usize], variant, &mut rng);
    }

    // Every corrupted page must be repairable.
    let mut repair_count = 0usize;
    for &(pgno, _) in &targets {
        let group_idx = ((pgno - 1) / GROUP_K) as usize;
        let (meta, repair_syms) = &groups[group_idx];
        let expected_blake3 = blake3_page_checksum(&original_pages[(pgno - 1) as usize]);

        let corrupted_snapshot = corrupted_pages.clone();
        let all_page_data = |p: u32| -> Vec<u8> { corrupted_snapshot[(p - 1) as usize].clone() };

        let outcome = detect_and_repair_page(
            pgno,
            &corrupted_pages[(pgno - 1) as usize],
            &expected_blake3,
            meta,
            &all_page_data,
            repair_syms,
        );

        match &outcome {
            RepairOutcome::Repaired { witness, .. } => {
                assert!(witness.verified, "page {pgno}: witness must be verified");
                repair_count += 1;
            }
            RepairOutcome::Unrecoverable { detail, .. } => {
                panic!(
                    "page {pgno} should be repairable at 10% corruption with R={GROUP_R}: {detail}"
                );
            }
            RepairOutcome::Intact { .. } => {
                panic!("page {pgno} was corrupted but reported intact");
            }
        }
    }

    assert_eq!(
        repair_count,
        targets.len(),
        "all 10%-corrupted pages must be repaired"
    );
}

/// Graceful degradation: corruption exceeds repair capacity.
///
/// With 60% of a group corrupted (12 out of K=20) and only R=10 repair
/// symbols, the decoder cannot reconstruct all pages.  The engine must
/// report `Unrecoverable` gracefully (no panics, descriptive errors).
#[test]
fn test_bd_3plop_6_graceful_degradation_beyond_capacity() {
    // Single group: K=20, R=10.
    let group_pages: Vec<Vec<u8>> = (1..=GROUP_K).map(make_page).collect();
    let (meta, repair_syms) = build_group(&group_pages, 1, GROUP_R);

    // Corrupt 60% of the group (12 pages).  R=10 cannot handle 12 erasures.
    let corrupt_count = 12u32;
    let mut corrupted = group_pages.clone();
    for page in corrupted.iter_mut().take(corrupt_count as usize) {
        page.fill(0xFF);
    }

    let corrupted_snapshot = corrupted.clone();
    let all_page_data = |p: u32| -> Vec<u8> { corrupted_snapshot[(p - 1) as usize].clone() };

    let mut unrecoverable_count = 0u32;

    for pgno in 1..=corrupt_count {
        let expected_blake3 = blake3_page_checksum(&group_pages[(pgno - 1) as usize]);

        let outcome = detect_and_repair_page(
            pgno,
            &corrupted[(pgno - 1) as usize],
            &expected_blake3,
            &meta,
            &all_page_data,
            &repair_syms,
        );

        match &outcome {
            RepairOutcome::Unrecoverable {
                witness, detail, ..
            } => {
                unrecoverable_count += 1;

                // Graceful: descriptive error message.
                assert!(
                    !detail.is_empty(),
                    "page {pgno}: unrecoverable detail must not be empty"
                );

                // Partial witness may be present (attempted repair that failed
                // verification, or pre-repair diagnostic).
                if let Some(w) = witness {
                    assert!(
                        !w.verified,
                        "page {pgno}: unrecoverable witness must not be verified"
                    );
                    assert_ne!(
                        w.corrupted_hash, w.expected_hash,
                        "page {pgno}: corrupted hash must differ from expected"
                    );
                }
            }
            RepairOutcome::Repaired { .. } => {
                // In theory the decoder might succeed on some pages even
                // beyond the nominal R budget if internal RaptorQ overhead
                // provides extra capacity.  We don't count these.
            }
            RepairOutcome::Intact { .. } => {
                panic!("page {pgno} was zero-filled but reported intact");
            }
        }
    }

    assert!(
        unrecoverable_count > 0,
        "with {corrupt_count}/{GROUP_K} corrupt pages and R={GROUP_R}, \
         at least some pages must be unrecoverable"
    );

    // Intact pages in the same group are still detected as intact.
    for pgno in (corrupt_count + 1)..=GROUP_K {
        let expected_blake3 = blake3_page_checksum(&group_pages[(pgno - 1) as usize]);

        let outcome = detect_and_repair_page(
            pgno,
            &corrupted[(pgno - 1) as usize],
            &expected_blake3,
            &meta,
            &all_page_data,
            &repair_syms,
        );

        assert!(
            matches!(outcome, RepairOutcome::Intact { .. }),
            "page {pgno} was not corrupted and must report intact"
        );
    }
}

/// Each corruption variant individually produces valid repair and witness.
#[test]
fn test_bd_3plop_6_each_variant_produces_valid_witness() {
    for (vi, &variant) in ALL_VARIANTS.iter().enumerate() {
        let mut rng = StdRng::seed_from_u64(SEED + 100 + vi as u64);

        // Small group: K=8, R=4.
        let pages: Vec<Vec<u8>> = (1..=8).map(make_page).collect();
        let (meta, repair_syms) = build_group(&pages, 1, 4);

        // Corrupt one page.
        let target_pgno = 3_u32;
        let mut corrupted = pages.clone();
        apply_corruption(&mut corrupted[2], variant, &mut rng);

        let expected_blake3 = blake3_page_checksum(&pages[2]);
        let corrupted_snapshot = corrupted.clone();
        let all_page_data = |p: u32| -> Vec<u8> { corrupted_snapshot[(p - 1) as usize].clone() };

        let outcome = detect_and_repair_page(
            target_pgno,
            &corrupted[2],
            &expected_blake3,
            &meta,
            &all_page_data,
            &repair_syms,
        );

        match outcome {
            RepairOutcome::Repaired {
                repaired_data,
                witness,
                ..
            } => {
                assert_eq!(
                    repaired_data, pages[2],
                    "variant {variant:?}: repaired data must match original"
                );
                assert!(
                    witness.verified,
                    "variant {variant:?}: witness must be verified"
                );
                assert_eq!(
                    witness.repaired_hash, witness.expected_hash,
                    "variant {variant:?}: repaired hash must match expected"
                );
                assert_ne!(
                    witness.corrupted_hash, witness.expected_hash,
                    "variant {variant:?}: corrupted hash must differ from expected"
                );
                assert_eq!(
                    witness.corrupt_pages_in_group, 1,
                    "variant {variant:?}: exactly one corrupt page"
                );
            }
            other => {
                panic!("variant {variant:?}: expected Repaired, got {other:?}");
            }
        }
    }
}
