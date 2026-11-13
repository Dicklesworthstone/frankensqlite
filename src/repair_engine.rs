//! §3.5 Unified RaptorQ Decode/Repair Engine with BLAKE3 Proofs (bd-n0g4q.3).
//!
//! Provides BLAKE3-based corruption detection and RaptorQ-powered automatic
//! repair with auditable witness proofs.  This is the "self-healing" core:
//!
//! 1. **Detection**: compute BLAKE3 checksum of page data; compare against
//!    expected hash from group metadata.
//! 2. **Repair**: gather available source + repair symbols, invoke RaptorQ
//!    `InactivationDecoder`, reconstruct missing source pages.
//! 3. **Witness**: emit a `RepairWitness` triple `(corrupted_hash,
//!    repaired_hash, expected_hash)` for every repair action.

use tracing::{debug, error, info, warn};

use crate::db_fec::{self, DbFecGroupMeta, RepairResult};

const BEAD_ID: &str = "bd-n0g4q.3";

// ---------------------------------------------------------------------------
// BLAKE3 page checksum
// ---------------------------------------------------------------------------

/// Compute the BLAKE3 checksum of a page (32 bytes).
#[must_use]
pub fn blake3_page_checksum(page_data: &[u8]) -> [u8; 32] {
    *blake3::hash(page_data).as_bytes()
}

/// Verify a page against its expected BLAKE3 checksum.
#[must_use]
pub fn verify_page_blake3(page_data: &[u8], expected: &[u8; 32]) -> bool {
    blake3_page_checksum(page_data) == *expected
}

// ---------------------------------------------------------------------------
// BLAKE3 witness proof
// ---------------------------------------------------------------------------

/// Auditable witness proof for a page repair action.
///
/// The triple `(corrupted_hash, repaired_hash, expected_hash)` provides
/// cryptographic evidence of what was observed, what was produced, and
/// what was expected.  This is logged to the evidence ledger (§3.5.8).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepairWitness {
    /// 1-based page number that was repaired.
    pub pgno: u32,
    /// BLAKE3 hash of the corrupted page data (before repair).
    pub corrupted_hash: [u8; 32],
    /// BLAKE3 hash of the repaired page data (after repair).
    pub repaired_hash: [u8; 32],
    /// Expected BLAKE3 hash from group metadata.
    pub expected_hash: [u8; 32],
    /// Whether the repair was verified (repaired_hash == expected_hash).
    pub verified: bool,
    /// Number of symbols consumed during RaptorQ decode.
    pub symbols_used: u32,
    /// Number of corrupt pages detected in the group.
    pub corrupt_pages_in_group: u32,
}

impl RepairWitness {
    /// Whether this witness records a successful, verified repair.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.verified
    }
}

// ---------------------------------------------------------------------------
// Repair engine outcome
// ---------------------------------------------------------------------------

/// Outcome of a detect-and-repair operation.
#[derive(Debug, Clone)]
pub enum RepairOutcome {
    /// Page was intact — no repair needed.
    Intact { pgno: u32, blake3_hash: [u8; 32] },
    /// Page was corrupted and successfully repaired.
    Repaired {
        pgno: u32,
        repaired_data: Vec<u8>,
        witness: RepairWitness,
    },
    /// Page was corrupted but repair failed (insufficient symbols or
    /// verification mismatch).
    Unrecoverable {
        pgno: u32,
        witness: Option<RepairWitness>,
        detail: String,
    },
}

// ---------------------------------------------------------------------------
// Unified detect-and-repair entry point
// ---------------------------------------------------------------------------

/// Detect corruption and attempt automatic repair for a single page.
///
/// This is the unified entry point for self-healing durability:
///
/// 1. Read the page and compute its BLAKE3 checksum.
/// 2. If it matches `expected_blake3`, the page is intact.
/// 3. If mismatched, invoke `db_fec::attempt_page_repair()` with available
///    repair symbols.
/// 4. Produce a `RepairWitness` for every repair (success or failure).
///
/// The caller provides:
/// - `target_pgno`: 1-based page number to check.
/// - `page_data`: current (possibly corrupted) page bytes.
/// - `expected_blake3`: expected BLAKE3 hash for this page.
/// - `group_meta`: db-fec group metadata for this page's group.
/// - `all_page_data`: closure to read any page in the group by pgno.
/// - `repair_symbols`: `(esi, data)` pairs from the `.db-fec` sidecar.
#[allow(clippy::too_many_lines)]
pub fn detect_and_repair_page(
    target_pgno: u32,
    page_data: &[u8],
    expected_blake3: &[u8; 32],
    group_meta: &DbFecGroupMeta,
    all_page_data: &dyn Fn(u32) -> Vec<u8>,
    repair_symbols: &[(u32, Vec<u8>)],
) -> RepairOutcome {
    // Validate target_pgno is within the group range.
    let group_end = group_meta.start_pgno + group_meta.group_size;
    if target_pgno < group_meta.start_pgno || target_pgno >= group_end {
        return RepairOutcome::Unrecoverable {
            pgno: target_pgno,
            witness: None,
            detail: format!(
                "page {target_pgno} is outside group range [{}, {})",
                group_meta.start_pgno, group_end,
            ),
        };
    }

    let actual_hash = blake3_page_checksum(page_data);

    // Fast path: page is intact.
    if actual_hash == *expected_blake3 {
        debug!(
            bead_id = BEAD_ID,
            pgno = target_pgno,
            "page intact — BLAKE3 checksum verified"
        );
        return RepairOutcome::Intact {
            pgno: target_pgno,
            blake3_hash: actual_hash,
        };
    }

    info!(
        bead_id = BEAD_ID,
        pgno = target_pgno,
        group_start = group_meta.start_pgno,
        K = group_meta.group_size,
        R = group_meta.r_repair,
        "BLAKE3 mismatch detected — initiating repair"
    );

    // Count corrupt pages in this group for the witness.
    let mut corrupt_count: u32 = 0;
    for i in 0..group_meta.group_size {
        let pgno = group_meta.start_pgno + i;
        let data = if pgno == target_pgno {
            page_data.to_vec()
        } else {
            all_page_data(pgno)
        };
        if !db_fec::verify_page_xxh3_128(&data, &group_meta.source_page_xxh3_128[i as usize]) {
            corrupt_count += 1;
        }
    }

    // Attempt RaptorQ repair.
    match db_fec::attempt_page_repair(target_pgno, group_meta, all_page_data, repair_symbols) {
        Ok((repaired_data, repair_result)) => {
            let repaired_hash = blake3_page_checksum(&repaired_data);
            let verified = repaired_hash == *expected_blake3;

            let RepairResult::Repaired { symbols_used, .. } = &repair_result else {
                // attempt_page_repair only returns Ok with RepairResult::Repaired;
                // other variants are returned via Err.  Defensive fallback.
                return RepairOutcome::Unrecoverable {
                    pgno: target_pgno,
                    witness: None,
                    detail: format!(
                        "page {target_pgno}: unexpected repair result variant: {repair_result:?}"
                    ),
                };
            };
            let symbols_used = *symbols_used;

            let witness = RepairWitness {
                pgno: target_pgno,
                corrupted_hash: actual_hash,
                repaired_hash,
                expected_hash: *expected_blake3,
                verified,
                symbols_used,
                corrupt_pages_in_group: corrupt_count,
            };

            if verified {
                info!(
                    bead_id = BEAD_ID,
                    pgno = target_pgno,
                    symbols_used,
                    corrupt_in_group = corrupt_count,
                    "page repair VERIFIED — BLAKE3 witness confirmed"
                );
                RepairOutcome::Repaired {
                    pgno: target_pgno,
                    repaired_data,
                    witness,
                }
            } else {
                warn!(
                    bead_id = BEAD_ID,
                    pgno = target_pgno,
                    "page repair produced data but BLAKE3 verification FAILED"
                );
                RepairOutcome::Unrecoverable {
                    pgno: target_pgno,
                    witness: Some(witness),
                    detail: format!("page {target_pgno}: repaired data failed BLAKE3 verification"),
                }
            }
        }
        Err(err) => {
            error!(
                bead_id = BEAD_ID,
                pgno = target_pgno,
                corrupt_in_group = corrupt_count,
                error = %err,
                "page repair FAILED — insufficient symbols"
            );
            RepairOutcome::Unrecoverable {
                pgno: target_pgno,
                witness: Some(RepairWitness {
                    pgno: target_pgno,
                    corrupted_hash: actual_hash,
                    repaired_hash: [0u8; 32],
                    expected_hash: *expected_blake3,
                    verified: false,
                    symbols_used: 0,
                    corrupt_pages_in_group: corrupt_count,
                }),
                detail: format!("{err}"),
            }
        }
    }
}

/// Batch repair: detect and repair multiple pages in a group.
///
/// Returns a `RepairOutcome` for every page in the target list.  Pages
/// that are intact are returned as `Intact`; corrupted pages are repaired
/// (or reported as unrecoverable) individually.
pub fn detect_and_repair_pages(
    target_pgnos: &[u32],
    group_meta: &DbFecGroupMeta,
    all_page_data: &dyn Fn(u32) -> Vec<u8>,
    expected_blake3s: &dyn Fn(u32) -> [u8; 32],
    repair_symbols: &[(u32, Vec<u8>)],
) -> Vec<RepairOutcome> {
    target_pgnos
        .iter()
        .map(|&pgno| {
            let data = all_page_data(pgno);
            let expected = expected_blake3s(pgno);
            detect_and_repair_page(
                pgno,
                &data,
                &expected,
                group_meta,
                all_page_data,
                repair_symbols,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_fec::{
        DbFecGroupMeta, compute_db_gen_digest, compute_raptorq_repair_symbols, page_xxh3_128,
    };

    /// Build test pages with deterministic content.
    #[allow(clippy::cast_possible_truncation)]
    fn make_test_pages(k: u32, page_size: usize) -> Vec<Vec<u8>> {
        (0..k)
            .map(|i| {
                let mut data = vec![0u8; page_size];
                for (j, b) in data.iter_mut().enumerate() {
                    *b = ((i as usize * 41 + j * 7) & 0xFF) as u8;
                }
                data
            })
            .collect()
    }

    /// Build group metadata and repair symbols for test pages.
    fn make_test_group(
        pages: &[Vec<u8>],
        page_size: u32,
        r_repair: u32,
        start_pgno: u32,
    ) -> (DbFecGroupMeta, Vec<(u32, Vec<u8>)>) {
        let k = u32::try_from(pages.len()).expect("k fits u32");
        let hashes: Vec<[u8; 16]> = pages.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, k + 1, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, start_pgno, k, r_repair, hashes, digest);

        let slices: Vec<&[u8]> = pages.iter().map(Vec::as_slice).collect();
        let repair_data =
            compute_raptorq_repair_symbols(&meta, &slices, page_size as usize).expect("encode");
        let repair_symbols: Vec<(u32, Vec<u8>)> = repair_data
            .into_iter()
            .enumerate()
            .map(|(i, d)| (k + u32::try_from(i).expect("i fits u32"), d))
            .collect();

        (meta, repair_symbols)
    }

    // -- BLAKE3 checksum tests --

    #[test]
    fn test_blake3_checksum_deterministic() {
        let data = vec![0xAB_u8; 512];
        let h1 = blake3_page_checksum(&data);
        let h2 = blake3_page_checksum(&data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_blake3_checksum_sensitive_to_content() {
        let data_a = vec![0x01_u8; 512];
        let data_b = vec![0x02_u8; 512];
        assert_ne!(blake3_page_checksum(&data_a), blake3_page_checksum(&data_b));
    }

    #[test]
    fn test_blake3_verify_page() {
        let data = vec![0x42_u8; 1024];
        let hash = blake3_page_checksum(&data);
        assert!(verify_page_blake3(&data, &hash));
        let mut corrupted = data;
        corrupted[0] ^= 0xFF;
        assert!(!verify_page_blake3(&corrupted, &hash));
    }

    // -- Intact detection --

    #[test]
    fn test_detect_intact_page() {
        let pages = make_test_pages(4, 128);
        let (meta, repair_symbols) = make_test_group(&pages, 128, 4, 2);
        let expected_hash = blake3_page_checksum(&pages[0]);

        let read_fn = |pgno: u32| -> Vec<u8> { pages[(pgno - 2) as usize].clone() };

        let outcome = detect_and_repair_page(
            2,
            &pages[0],
            &expected_hash,
            &meta,
            &read_fn,
            &repair_symbols,
        );

        assert!(matches!(outcome, RepairOutcome::Intact { pgno: 2, .. }));
    }

    // -- Single-page repair --

    #[test]
    fn test_detect_and_repair_single_corruption() {
        let pages = make_test_pages(4, 128);
        let (meta, repair_symbols) = make_test_group(&pages, 128, 4, 2);
        let expected_hash = blake3_page_checksum(&pages[1]);

        let corrupted = vec![0xFF_u8; 128];
        let read_fn = |pgno: u32| -> Vec<u8> {
            if pgno == 3 {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        let outcome = detect_and_repair_page(
            3,
            &corrupted,
            &expected_hash,
            &meta,
            &read_fn,
            &repair_symbols,
        );

        match outcome {
            RepairOutcome::Repaired {
                pgno,
                repaired_data,
                witness,
            } => {
                assert_eq!(pgno, 3);
                assert_eq!(repaired_data, pages[1]);
                assert!(witness.verified);
                assert_eq!(witness.corrupted_hash, blake3_page_checksum(&corrupted));
                assert_eq!(witness.repaired_hash, expected_hash);
                assert_eq!(witness.expected_hash, expected_hash);
                assert!(witness.corrupt_pages_in_group >= 1);
            }
            other => panic!("expected Repaired, got {other:?}"),
        }
    }

    // -- Multi-page corruption --

    #[test]
    fn test_detect_and_repair_multi_corruption() {
        let pages = make_test_pages(8, 128);
        let (meta, repair_symbols) = make_test_group(&pages, 128, 4, 2);

        let corrupted = vec![0xCC_u8; 128];
        let corrupt_pgnos = [2_u32, 3, 4]; // 3 corrupted pages (within R=4 budget)

        let read_fn = |pgno: u32| -> Vec<u8> {
            if corrupt_pgnos.contains(&pgno) {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        for &target in &corrupt_pgnos {
            let idx = (target - 2) as usize;
            let expected_hash = blake3_page_checksum(&pages[idx]);
            let outcome = detect_and_repair_page(
                target,
                &corrupted,
                &expected_hash,
                &meta,
                &read_fn,
                &repair_symbols,
            );

            match outcome {
                RepairOutcome::Repaired {
                    repaired_data,
                    witness,
                    ..
                } => {
                    assert_eq!(repaired_data, pages[idx]);
                    assert!(witness.verified);
                    assert!(witness.corrupt_pages_in_group >= 3);
                }
                other => panic!("expected Repaired for page {target}, got {other:?}"),
            }
        }
    }

    // -- Contiguous range corruption --

    #[test]
    fn test_detect_and_repair_contiguous_range() {
        let pages = make_test_pages(8, 64);
        // R=8 gives ample overhead for 4 corruptions (RaptorQ needs symbols
        // beyond the exact boundary due to potential linear dependence in the
        // binary LT encoding pattern).
        let (meta, repair_symbols) = make_test_group(&pages, 64, 8, 2);

        let corrupted = vec![0xBB_u8; 64];
        // Corrupt pages 5,6,7,8 (contiguous range, indices 3..7)
        let corrupt_pgnos = [5_u32, 6, 7, 8];

        let read_fn = |pgno: u32| -> Vec<u8> {
            if corrupt_pgnos.contains(&pgno) {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        for &target in &corrupt_pgnos {
            let idx = (target - 2) as usize;
            let expected_hash = blake3_page_checksum(&pages[idx]);
            let outcome = detect_and_repair_page(
                target,
                &corrupted,
                &expected_hash,
                &meta,
                &read_fn,
                &repair_symbols,
            );

            match outcome {
                RepairOutcome::Repaired { witness, .. } => {
                    assert!(witness.verified, "page {target} should be repaired");
                }
                other => panic!("expected Repaired for page {target}, got {other:?}"),
            }
        }
    }

    // -- Graceful degradation --

    #[test]
    fn test_graceful_degradation_beyond_repair_capacity() {
        let pages = make_test_pages(8, 64);
        let (meta, repair_symbols) = make_test_group(&pages, 64, 4, 2);

        let corrupted = vec![0xEE_u8; 64];
        // Corrupt 5 pages (exceeds R=4 budget)
        let corrupt_pgnos = [2_u32, 3, 4, 5, 6];

        let read_fn = |pgno: u32| -> Vec<u8> {
            if corrupt_pgnos.contains(&pgno) {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        let expected_hash = blake3_page_checksum(&pages[0]);
        let outcome = detect_and_repair_page(
            2,
            &corrupted,
            &expected_hash,
            &meta,
            &read_fn,
            &repair_symbols,
        );

        match outcome {
            RepairOutcome::Unrecoverable {
                pgno,
                witness,
                detail,
            } => {
                assert_eq!(pgno, 2);
                assert!(witness.is_some());
                assert!(!detail.is_empty());
                let w = witness.unwrap();
                assert!(!w.verified);
                assert!(w.corrupt_pages_in_group >= 5);
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    // -- Witness proof completeness --

    #[test]
    fn test_witness_proof_completeness() {
        let pages = make_test_pages(4, 128);
        let (meta, repair_symbols) = make_test_group(&pages, 128, 4, 2);

        let corrupted = vec![0xAA_u8; 128];
        let expected_hash = blake3_page_checksum(&pages[2]);

        let read_fn = |pgno: u32| -> Vec<u8> {
            if pgno == 4 {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        let outcome = detect_and_repair_page(
            4,
            &corrupted,
            &expected_hash,
            &meta,
            &read_fn,
            &repair_symbols,
        );

        match outcome {
            RepairOutcome::Repaired { witness, .. } => {
                // The witness triple must be complete.
                assert_ne!(witness.corrupted_hash, [0u8; 32]);
                assert_ne!(witness.repaired_hash, [0u8; 32]);
                assert_ne!(witness.expected_hash, [0u8; 32]);
                assert_ne!(witness.corrupted_hash, witness.repaired_hash);
                assert_eq!(witness.repaired_hash, witness.expected_hash);
                assert!(witness.symbols_used > 0);
                assert!(witness.is_success());
            }
            other => panic!("expected Repaired, got {other:?}"),
        }
    }

    // -- Corruption percentage boundary tests --

    /// Test repair at varying corruption levels to find the success/failure boundary.
    #[test]
    fn test_corruption_boundary_1_percent() {
        corruption_boundary_test(64, 4, 1); // 1% of 64 = ~1 page
    }

    #[test]
    fn test_corruption_boundary_5_percent() {
        // 5% of 64 = ~4 pages; R=8 gives 2x overhead for the binary LT decoder.
        corruption_boundary_test(64, 8, 5);
    }

    #[test]
    fn test_corruption_boundary_10_percent() {
        // 10% of 64 = ~7 pages; R=16 gives ~2x overhead.
        corruption_boundary_test(64, 16, 10);
    }

    #[test]
    fn test_corruption_boundary_20_percent() {
        // 20% of 64 = ~13 pages; R=32 gives ~2.5x overhead.
        corruption_boundary_test(64, 32, 20);
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn corruption_boundary_test(k: u32, r: u32, corruption_pct: u32) {
        let page_size = 64_usize;
        let pages = make_test_pages(k, page_size);
        let (meta, repair_symbols) = make_test_group(&pages, page_size as u32, r, 2);

        let num_corrupt = (f64::from(k) * f64::from(corruption_pct) / 100.0).ceil() as u32;
        let num_corrupt = num_corrupt.max(1).min(k);

        let corrupted = vec![0xDD_u8; page_size];
        let corrupt_pgnos: Vec<u32> = (2..2 + num_corrupt).collect();

        let read_fn = |pgno: u32| -> Vec<u8> {
            if corrupt_pgnos.contains(&pgno) {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        let target = corrupt_pgnos[0];
        let idx = (target - 2) as usize;
        let expected_hash = blake3_page_checksum(&pages[idx]);

        let outcome = detect_and_repair_page(
            target,
            &corrupted,
            &expected_hash,
            &meta,
            &read_fn,
            &repair_symbols,
        );

        if num_corrupt <= r {
            // Should succeed.
            match outcome {
                RepairOutcome::Repaired {
                    repaired_data,
                    witness,
                    ..
                } => {
                    assert_eq!(repaired_data, pages[idx]);
                    assert!(
                        witness.verified,
                        "repair should succeed: {num_corrupt} corrupt <= R={r}"
                    );
                }
                other => {
                    panic!("expected Repaired for {num_corrupt} corrupt (R={r}), got {other:?}")
                }
            }
        } else {
            // Should fail gracefully.
            assert!(
                matches!(outcome, RepairOutcome::Unrecoverable { .. }),
                "expected Unrecoverable for {num_corrupt} corrupt > R={r}, got {outcome:?}"
            );
        }
    }

    // -- Batch repair --

    #[test]
    fn test_batch_detect_and_repair() {
        let pages = make_test_pages(4, 128);
        let (meta, repair_symbols) = make_test_group(&pages, 128, 4, 2);

        let corrupted = vec![0xFF_u8; 128];
        let read_fn = |pgno: u32| -> Vec<u8> {
            if pgno == 3 {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        let blake3_fn = |pgno: u32| -> [u8; 32] {
            let idx = (pgno - 2) as usize;
            blake3_page_checksum(&pages[idx])
        };

        let outcomes =
            detect_and_repair_pages(&[2, 3, 4, 5], &meta, &read_fn, &blake3_fn, &repair_symbols);

        assert_eq!(outcomes.len(), 4);
        assert!(matches!(outcomes[0], RepairOutcome::Intact { .. }));
        assert!(matches!(outcomes[1], RepairOutcome::Repaired { .. }));
        assert!(matches!(outcomes[2], RepairOutcome::Intact { .. }));
        assert!(matches!(outcomes[3], RepairOutcome::Intact { .. }));
    }

    // -- Compliance gate --

    #[test]
    fn test_bd_n0g4q_3_compliance_gate() {
        assert_eq!(BEAD_ID, "bd-n0g4q.3");
        // Verify key types exist and are constructible.
        let witness = RepairWitness {
            pgno: 1,
            corrupted_hash: [0u8; 32],
            repaired_hash: [1u8; 32],
            expected_hash: [1u8; 32],
            verified: true,
            symbols_used: 5,
            corrupt_pages_in_group: 1,
        };
        assert!(witness.is_success());
    }
}
