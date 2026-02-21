//! bd-368z9: Proofs of Retrievability for durability audit (§11.11) integration tests.
//!
//! Validates PoR cryptographic storage audit primitives:
//!   1. Challenge determinism (same seed = same challenge)
//!   2. Challenge diversity (different seeds = different challenges)
//!   3. Proof correctness (same data = same witness)
//!   4. Corruption detection (flipped bit = different witness)
//!   5. Full audit pass/fail lifecycle
//!   6. Missing page detection (prover read failure)
//!   7. Multi-seed audit batch
//!   8. Metrics fidelity (delta-based)
//!   9. Large page count stress test
//!  10. Machine-readable conformance output

use fsqlite_core::por::{
    PorChallenge, GLOBAL_POR_METRICS, PorMetrics,
    compute_por_proof, run_por_audit,
};

/// Create N deterministic pages (4096 bytes each).
fn make_pages(count: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut page = vec![0u8; 4096];
            for (j, b) in page.iter_mut().enumerate() {
                #[allow(clippy::cast_possible_truncation)]
                {
                    *b = ((i as usize * 37 + j) % 256) as u8;
                }
            }
            page
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test 1: Challenge determinism
// ---------------------------------------------------------------------------

#[test]
fn test_challenge_determinism() {
    let c1 = PorChallenge::from_seed(0xDEAD, 100, 10);
    let c2 = PorChallenge::from_seed(0xDEAD, 100, 10);
    assert_eq!(c1, c2, "same seed must produce identical challenges");
    assert_eq!(c1.page_indices.len(), 10);
    assert_eq!(c1.nonce, c2.nonce);

    // Indices should be sorted and unique.
    let mut sorted = c1.page_indices.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 10, "indices must be unique");
    assert_eq!(c1.page_indices, sorted, "indices must be sorted");

    println!(
        "[PASS] Challenge determinism: seed=0xDEAD, {} indices, sorted+unique",
        c1.page_indices.len()
    );
}

// ---------------------------------------------------------------------------
// Test 2: Challenge diversity
// ---------------------------------------------------------------------------

#[test]
fn test_challenge_diversity() {
    let seeds = [1u64, 2, 42, 0xCAFE, 0xBEEF, 0xDEAD];
    let challenges: Vec<PorChallenge> = seeds
        .iter()
        .map(|&s| PorChallenge::from_seed(s, 1000, 20))
        .collect();

    // All nonces should be distinct.
    let nonces: std::collections::HashSet<[u8; 32]> =
        challenges.iter().map(|c| c.nonce).collect();
    assert_eq!(nonces.len(), seeds.len(), "all nonces must be distinct");

    // All page index sets should be distinct.
    let mut index_sets: Vec<Vec<u32>> = challenges.iter().map(|c| c.page_indices.clone()).collect();
    index_sets.sort();
    index_sets.dedup();
    assert_eq!(index_sets.len(), seeds.len(), "all index sets must differ");

    // Challenge size capped at total_pages.
    let small = PorChallenge::from_seed(42, 3, 100);
    assert_eq!(small.page_indices.len(), 3, "size capped at total_pages");

    println!(
        "[PASS] Challenge diversity: {} seeds, all nonces+indices distinct",
        seeds.len()
    );
}

// ---------------------------------------------------------------------------
// Test 3: Proof correctness
// ---------------------------------------------------------------------------

#[test]
fn test_proof_correctness() {
    let pages = make_pages(50);
    let challenge = PorChallenge::from_seed(42, 50, 10);

    let proof1 = compute_por_proof(&challenge, |i| pages.get(i as usize).cloned());
    let proof2 = compute_por_proof(&challenge, |i| pages.get(i as usize).cloned());

    assert!(proof1.is_some(), "proof should be computable");
    assert_eq!(proof1, proof2, "same data must produce same proof");

    // Witness should be 32 bytes.
    let w = proof1.unwrap().witness;
    assert_eq!(w.len(), 32);
    // Witness should not be all zeros (extremely unlikely with BLAKE3).
    assert!(w.iter().any(|&b| b != 0), "witness should not be all zeros");

    println!("[PASS] Proof correctness: deterministic witness, 32 bytes, non-zero");
}

// ---------------------------------------------------------------------------
// Test 4: Corruption detection
// ---------------------------------------------------------------------------

#[test]
fn test_corruption_detection() {
    let pages = make_pages(20);
    let challenge = PorChallenge::from_seed(42, 20, 10);

    let proof_clean = compute_por_proof(&challenge, |i| pages.get(i as usize).cloned()).unwrap();

    // Corrupt each challenged page and verify the witness changes.
    let mut detections = 0;
    for &idx in &challenge.page_indices {
        let proof_corrupt = compute_por_proof(&challenge, |i| {
            let mut p = pages.get(i as usize)?.clone();
            if i == idx {
                p[0] ^= 0xFF; // flip bits in first byte
            }
            Some(p)
        })
        .unwrap();

        if proof_corrupt.witness != proof_clean.witness {
            detections += 1;
        }
    }

    assert_eq!(
        detections,
        challenge.page_indices.len(),
        "all corrupted pages must change the witness"
    );

    println!(
        "[PASS] Corruption detection: {}/{} pages detected",
        detections,
        challenge.page_indices.len()
    );
}

// ---------------------------------------------------------------------------
// Test 5: Full audit pass/fail lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_audit_lifecycle() {
    let pages = make_pages(50);

    // Audit should pass with identical data.
    let result = run_por_audit(
        42,
        50,
        15,
        |i| pages.get(i as usize).cloned(),
        |i| pages.get(i as usize).cloned(),
    );
    assert!(result.valid, "audit should pass with identical data");
    assert_eq!(result.challenge_size, 15);
    assert!(result.duration_us < 1_000_000, "audit should be fast");

    // Audit should fail with corrupted prover.
    let result_corrupt = run_por_audit(
        42,
        50,
        15,
        |i| {
            let mut p = pages.get(i as usize)?.clone();
            p[0] ^= 0xFF; // corrupt every page
            Some(p)
        },
        |i| pages.get(i as usize).cloned(),
    );
    assert!(!result_corrupt.valid, "audit should fail with corrupted prover");

    println!(
        "[PASS] Audit lifecycle: pass (same data), fail (corrupt prover), duration={}us",
        result.duration_us
    );
}

// ---------------------------------------------------------------------------
// Test 6: Missing page detection
// ---------------------------------------------------------------------------

#[test]
fn test_missing_page_detection() {
    let pages = make_pages(10);

    // Prover returns None for all pages.
    let result = run_por_audit(
        42,
        10,
        5,
        |_| None,
        |i| pages.get(i as usize).cloned(),
    );
    assert!(!result.valid, "audit should fail when prover returns None");

    // Verifier returns None.
    let result2 = run_por_audit(
        42,
        10,
        5,
        |i| pages.get(i as usize).cloned(),
        |_| None,
    );
    assert!(!result2.valid, "audit should fail when verifier returns None");

    // Single page missing in prover.
    let challenge = PorChallenge::from_seed(42, 10, 5);
    let missing_idx = challenge.page_indices[0];
    let result3 = run_por_audit(
        42,
        10,
        5,
        |i| {
            if i == missing_idx {
                None
            } else {
                pages.get(i as usize).cloned()
            }
        },
        |i| pages.get(i as usize).cloned(),
    );
    assert!(!result3.valid, "audit should fail with single missing page");

    println!("[PASS] Missing page detection: all-missing, verifier-missing, single-missing");
}

// ---------------------------------------------------------------------------
// Test 7: Multi-seed audit batch
// ---------------------------------------------------------------------------

#[test]
fn test_multi_seed_audit_batch() {
    let pages = make_pages(100);

    let mut passes = 0u32;
    let mut fails = 0u32;

    // Run 50 audits with different seeds — all should pass.
    for seed in 0..50u64 {
        let result = run_por_audit(
            seed,
            100,
            20,
            |i| pages.get(i as usize).cloned(),
            |i| pages.get(i as usize).cloned(),
        );
        if result.valid {
            passes += 1;
        } else {
            fails += 1;
        }
    }

    assert_eq!(passes, 50, "all 50 audits should pass");
    assert_eq!(fails, 0, "no failures expected");

    println!("[PASS] Multi-seed audit batch: {passes}/50 passed, {fails} failed");
}

// ---------------------------------------------------------------------------
// Test 8: Metrics fidelity (delta-based)
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_fidelity() {
    let m_before = GLOBAL_POR_METRICS.snapshot();

    let pages = make_pages(10);

    // 3 passing audits.
    for seed in 100..103u64 {
        run_por_audit(
            seed,
            10,
            5,
            |i| pages.get(i as usize).cloned(),
            |i| pages.get(i as usize).cloned(),
        );
    }

    // 1 failing audit.
    run_por_audit(
        200,
        10,
        5,
        |_| None,
        |i| pages.get(i as usize).cloned(),
    );

    let m_after = GLOBAL_POR_METRICS.snapshot();
    let delta_audits = m_after.audits_total - m_before.audits_total;
    let delta_failures = m_after.failures_total - m_before.failures_total;

    assert!(delta_audits >= 4, "should record at least 4 audits, got {delta_audits}");
    assert!(delta_failures >= 1, "should record at least 1 failure, got {delta_failures}");

    // PorMetrics::new() / record / snapshot / reset.
    let local = PorMetrics::new();
    local.record_audit(true);
    local.record_audit(false);
    let snap = local.snapshot();
    assert_eq!(snap.audits_total, 2);
    assert_eq!(snap.failures_total, 1);
    local.reset();
    let snap2 = local.snapshot();
    assert_eq!(snap2.audits_total, 0);
    assert_eq!(snap2.failures_total, 0);

    // Display format.
    local.record_audit(true);
    let text = format!("{}", local.snapshot());
    assert!(text.contains("por_audits=1"));
    assert!(text.contains("por_failures=0"));

    println!(
        "[PASS] Metrics fidelity: delta_audits={delta_audits} delta_failures={delta_failures}, local metrics OK"
    );
}

// ---------------------------------------------------------------------------
// Test 9: Large page count stress test
// ---------------------------------------------------------------------------

#[test]
fn test_large_page_count() {
    // 10K pages, challenge 100 of them.
    let pages: Vec<Vec<u8>> = (0..10_000u32)
        .map(|i| {
            let mut p = vec![0u8; 4096];
            p[0] = (i % 256) as u8;
            p[1] = ((i >> 8) % 256) as u8;
            p
        })
        .collect();

    let result = run_por_audit(
        0xCAFEBABE,
        10_000,
        100,
        |i| pages.get(i as usize).cloned(),
        |i| pages.get(i as usize).cloned(),
    );
    assert!(result.valid, "large audit should pass");
    assert_eq!(result.challenge_size, 100);

    // Verify challenge covers a spread of the page range.
    let challenge = PorChallenge::from_seed(0xCAFEBABE, 10_000, 100);
    let min_idx = challenge.page_indices.first().copied().unwrap();
    let max_idx = challenge.page_indices.last().copied().unwrap();
    assert!(max_idx - min_idx > 100, "challenge should span a wide range");

    println!(
        "[PASS] Large page count: 10K pages, 100 challenged, range=[{min_idx},{max_idx}], passed"
    );
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    let pages = make_pages(20);

    // Property 1: Challenge determinism.
    let c1 = PorChallenge::from_seed(1, 20, 5);
    let c2 = PorChallenge::from_seed(1, 20, 5);
    let det_ok = c1 == c2;

    // Property 2: Proof matches same data.
    let proof1 = compute_por_proof(&c1, |i| pages.get(i as usize).cloned());
    let proof2 = compute_por_proof(&c1, |i| pages.get(i as usize).cloned());
    let proof_ok = proof1.is_some() && proof1 == proof2;

    // Property 3: Corruption changes witness.
    let clean = proof1.unwrap();
    let corrupt = compute_por_proof(&c1, |i| {
        let mut p = pages.get(i as usize)?.clone();
        p[0] ^= 0xFF;
        Some(p)
    })
    .unwrap();
    let corrupt_ok = clean.witness != corrupt.witness;

    // Property 4: Audit passes with same data.
    let audit = run_por_audit(
        1, 20, 5,
        |i| pages.get(i as usize).cloned(),
        |i| pages.get(i as usize).cloned(),
    );
    let audit_ok = audit.valid;

    // Property 5: Missing page fails audit.
    let fail = run_por_audit(1, 20, 5, |_| None, |i| pages.get(i as usize).cloned());
    let fail_ok = !fail.valid;

    // Property 6: Metrics tracked.
    let m = GLOBAL_POR_METRICS.snapshot();
    let metrics_ok = m.audits_total > 0;

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] Challenge determinism: same seed = same challenge");
    println!("  [CONFORM] Proof correctness: same data = same witness");
    println!("  [CONFORM] Corruption detection: flipped bits change witness");
    println!("  [CONFORM] Audit pass: identical prover/verifier");
    println!("  [CONFORM] Missing page: audit fails correctly");
    println!("  [CONFORM] Metrics: audits_total={}", m.audits_total);
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(det_ok, "challenge determinism failed");
    assert!(proof_ok, "proof correctness failed");
    assert!(corrupt_ok, "corruption detection failed");
    assert!(audit_ok, "audit pass failed");
    assert!(fail_ok, "missing page audit should fail");
    assert!(metrics_ok, "metrics not tracked");
}
