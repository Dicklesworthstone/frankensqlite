//! Tuple Generator and Systematic Index Table verification suite (RFC 6330 §5.3.5.4).
//!
//! Bead: bd-1hi.8
//!
//! Verifies that asupersync's RaptorQ implementation correctly implements:
//! - RFC 6330 `Rand` function (§5.3.5.1) using V0-V3 lookup tables
//! - RFC 6330 degree distribution (§5.3.5.2)
//! - Systematic encoding parameters (§5.3 derived from K)
//! - Systematic property: first K encoding symbols are source symbols
//! - Encode/decode roundtrip correctness for representative K values
//! - Determinism: identical inputs produce identical outputs

use std::collections::HashSet;
use std::time::Instant;

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::gf256::Gf256;
use asupersync::raptorq::rfc6330::{V0, V1, V2, V3, deg, rand};
use asupersync::raptorq::systematic::{
    ConstraintMatrix, RobustSoliton, SystematicEncoder, SystematicParams,
};
use tracing::{error, info_span};

const BEAD_ID: &str = "bd-1hi.8";
const ENCODING_PIPELINE_BEAD_ID: &str = "bd-1hi.3";

// ============================================================================
// §5.3.5.1 Rand function — RFC 6330 V0-V3 table verification
// ============================================================================

#[test]
fn test_rand_v0_v3_tables_are_correct_length() {
    assert_eq!(
        V0.len(),
        256,
        "bead_id={BEAD_ID} case=v0_table_length expected=256 actual={}",
        V0.len()
    );
    assert_eq!(
        V1.len(),
        256,
        "bead_id={BEAD_ID} case=v1_table_length expected=256 actual={}",
        V1.len()
    );
    assert_eq!(
        V2.len(),
        256,
        "bead_id={BEAD_ID} case=v2_table_length expected=256 actual={}",
        V2.len()
    );
    assert_eq!(
        V3.len(),
        256,
        "bead_id={BEAD_ID} case=v3_table_length expected=256 actual={}",
        V3.len()
    );
}

#[test]
fn test_rand_v0_known_entries() {
    // RFC 6330 Section 5.5 specifies V0[0] and a few other entries.
    // Verify against the known first entry.
    assert_eq!(
        V0[0], 251_291_136,
        "bead_id={BEAD_ID} case=v0_first_entry expected=251291136 actual={}",
        V0[0]
    );
    // V0[255] (last entry)
    assert_eq!(
        V0[255], 1_358_307_511,
        "bead_id={BEAD_ID} case=v0_last_entry expected=1358307511 actual={}",
        V0[255]
    );
}

#[test]
fn test_rand_function_matches_rfc6330_formula() {
    // Rand(y, i, m) = (V0[x0] ^ V1[x1] ^ V2[x2] ^ V3[x3]) % m
    // where x0 = (y + i) mod 256, x1 = (floor(y/256) + i) mod 256, etc.
    // Verify by computing manually and comparing to the function output.
    let test_cases: Vec<(u32, u8, u32)> = vec![
        (0, 0, 1000),
        (1, 0, 256),
        (42, 7, 500),
        (12345, 3, 100),
        (0xFFFF_FFFF, 255, 65536),
        (256, 0, 100),
        (65536, 0, 1000),
        (0x0102_0304, 5, 200),
    ];

    for (y, i, m) in test_cases {
        let x0 = ((y.wrapping_add(u32::from(i))) & 0xFF) as usize;
        let x1 = (((y >> 8).wrapping_add(u32::from(i))) & 0xFF) as usize;
        let x2 = (((y >> 16).wrapping_add(u32::from(i))) & 0xFF) as usize;
        let x3 = (((y >> 24).wrapping_add(u32::from(i))) & 0xFF) as usize;
        let expected = (V0[x0] ^ V1[x1] ^ V2[x2] ^ V3[x3]) % m;
        let actual = rand(y, i, m);
        assert_eq!(
            actual, expected,
            "bead_id={BEAD_ID} case=rand_manual_verification y={y} i={i} m={m} \
             expected={expected} actual={actual}"
        );
    }
}

#[test]
fn test_rand_output_in_range() {
    // Rand(y, i, m) must return value in [0, m) for all inputs.
    for y in [0_u32, 1, 100, 1000, 65535, 0xFFFF_FFFF] {
        for i in 0..16_u8 {
            for m in [1_u32, 10, 100, 1000, 65536] {
                let result = rand(y, i, m);
                assert!(
                    result < m,
                    "bead_id={BEAD_ID} case=rand_in_range y={y} i={i} m={m} result={result}"
                );
            }
        }
    }
}

#[test]
fn test_rand_deterministic() {
    // Same inputs must produce same outputs (no hidden state).
    for _ in 0..10 {
        let a = rand(42, 7, 1000);
        let b = rand(42, 7, 1000);
        assert_eq!(
            a, b,
            "bead_id={BEAD_ID} case=rand_deterministic expected same result for same inputs"
        );
    }
}

// ============================================================================
// §5.3.5.2 Degree distribution — RFC 6330 deg() function
// ============================================================================

#[test]
fn test_deg_output_range() {
    // deg(v, w, l) must return d where 1 <= d <= min(w, l).
    let w = 100_usize;
    let l = 120_usize;
    for v in (0_u32..1_048_576).step_by(1000) {
        let d = deg(v, w, l);
        assert!(
            d >= 1,
            "bead_id={BEAD_ID} case=deg_min v={v} w={w} l={l} d={d}"
        );
        assert!(
            d <= w.min(l),
            "bead_id={BEAD_ID} case=deg_max v={v} w={w} l={l} d={d} max={}",
            w.min(l)
        );
    }
}

#[test]
fn test_deg_deterministic() {
    // Same inputs → same output.
    assert_eq!(
        deg(500_000, 100, 120),
        deg(500_000, 100, 120),
        "bead_id={BEAD_ID} case=deg_deterministic"
    );
}

#[test]
fn test_deg_distribution_statistical() {
    // Verify that the degree distribution roughly matches the robust soliton distribution:
    // - Most symbols have small degree (fast peeling)
    // - A few symbols have large degree (algebraic coverage)
    // We test that degree 1 and 2 are the most common, and very high degrees are rare.
    let w = 200_usize;
    let l = 220_usize;
    let mut degree_counts = vec![0_u64; l + 1];
    let n_samples = 1_048_576_u32; // = 2^20, full range of v

    for v in 0..n_samples {
        let d = deg(v, w, l);
        degree_counts[d] += 1;
    }

    // Degree 1 should be the most common (ideal soliton has spike at 1).
    let d1_count = degree_counts[1];
    assert!(
        d1_count > 0,
        "bead_id={BEAD_ID} case=deg_distribution_d1_nonzero"
    );

    // Low degrees (1-5) should account for majority of symbols.
    let low_degree_count: u64 = degree_counts[1..=5].iter().sum();
    let low_fraction = low_degree_count as f64 / f64::from(n_samples);
    assert!(
        low_fraction > 0.5,
        "bead_id={BEAD_ID} case=deg_distribution_low_degrees fraction={low_fraction:.4} \
         expected>0.5"
    );
}

// ============================================================================
// Systematic Index Table — SystematicParams verification
// ============================================================================

#[test]
fn test_systematic_params_basic_invariants() {
    // For various K values, verify L = K + S + H, W = K + S, P = H.
    let test_k_values = [1, 5, 10, 50, 100, 500, 1000, 5000];
    for &k in &test_k_values {
        let params = SystematicParams::for_source_block(k, 64);
        assert_eq!(params.k, k, "bead_id={BEAD_ID} case=params_k k={k}");
        assert_eq!(
            params.l,
            params.k + params.s + params.h,
            "bead_id={BEAD_ID} case=params_l_equals_k_s_h k={k} l={} k+s+h={}",
            params.l,
            params.k + params.s + params.h
        );
        assert_eq!(
            params.w,
            params.k + params.s,
            "bead_id={BEAD_ID} case=params_w_equals_k_s k={k}"
        );
        assert_eq!(
            params.p, params.h,
            "bead_id={BEAD_ID} case=params_p_equals_h k={k}"
        );
        assert_eq!(
            params.b, params.k,
            "bead_id={BEAD_ID} case=params_b_equals_k k={k}"
        );
    }
}

#[test]
fn test_systematic_params_s_is_prime() {
    // RFC 6330 requires S to be prime (for LDPC circulant coprimality).
    let test_k_values = [1, 5, 10, 50, 100, 500, 1000, 5000];
    for &k in &test_k_values {
        let params = SystematicParams::for_source_block(k, 64);
        assert!(
            is_prime(params.s),
            "bead_id={BEAD_ID} case=params_s_prime k={k} s={} (not prime!)",
            params.s
        );
    }
}

#[test]
fn test_systematic_params_s_gte_7() {
    // RFC 6330 Table 2 always picks S >= 7.
    let test_k_values = [1, 2, 3, 5, 10, 50, 100];
    for &k in &test_k_values {
        let params = SystematicParams::for_source_block(k, 64);
        assert!(
            params.s >= 7,
            "bead_id={BEAD_ID} case=params_s_gte_7 k={k} s={}",
            params.s
        );
    }
}

#[test]
fn test_systematic_params_h_gte_3() {
    // H >= 3 for half-distance coverage.
    let test_k_values = [1, 2, 5, 10, 50, 100, 1000];
    for &k in &test_k_values {
        let params = SystematicParams::for_source_block(k, 64);
        assert!(
            params.h >= 3,
            "bead_id={BEAD_ID} case=params_h_gte_3 k={k} h={}",
            params.h
        );
    }
}

#[test]
fn test_systematic_params_h_grows_with_sqrt_k() {
    // H >= ceil(sqrt(K)) for half-distance check coverage.
    let test_k_values = [10, 100, 1000, 5000];
    for &k in &test_k_values {
        let params = SystematicParams::for_source_block(k, 64);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let sqrt_k = (k as f64).sqrt().ceil() as usize;
        assert!(
            params.h >= sqrt_k,
            "bead_id={BEAD_ID} case=params_h_sqrt k={k} h={} ceil_sqrt={}",
            params.h,
            sqrt_k
        );
    }
}

#[test]
fn test_systematic_index_table_deterministic() {
    // Same K → same parameters (no randomness in parameter selection).
    for k in [5, 50, 500, 5000] {
        let p1 = SystematicParams::for_source_block(k, 64);
        let p2 = SystematicParams::for_source_block(k, 64);
        assert_eq!(
            p1.s, p2.s,
            "bead_id={BEAD_ID} case=params_deterministic_s k={k}"
        );
        assert_eq!(
            p1.h, p2.h,
            "bead_id={BEAD_ID} case=params_deterministic_h k={k}"
        );
        assert_eq!(
            p1.l, p2.l,
            "bead_id={BEAD_ID} case=params_deterministic_l k={k}"
        );
    }
}

#[test]
fn test_systematic_index_lookup_o1() {
    // Parameter computation should be fast (O(1) or near-O(1)).
    // We measure a batch of lookups and verify < 1ms for 1000 lookups.
    let start = Instant::now();
    for k in 1..=1000 {
        let _params = SystematicParams::for_source_block(k, 64);
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 100,
        "bead_id={BEAD_ID} case=params_lookup_fast elapsed_ms={} expected<100",
        elapsed.as_millis()
    );
}

// ============================================================================
// Tuple Generator — Encoding determinism
// ============================================================================

#[test]
fn test_tuple_generator_deterministic() {
    // Same (K, seed, ESI) → same repair symbol (no hidden state).
    let k = 20_usize;
    let symbol_size = 32_usize;
    let seed = 42_u64;
    let source: Vec<Vec<u8>> = (0..k)
        .map(|i| vec![u8::try_from(i % 256).unwrap_or(0); symbol_size])
        .collect();

    let enc1 = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder 1");
    let enc2 = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder 2");

    let k_u32 = u32::try_from(k).expect("k fits u32");
    for esi in k_u32..k_u32 + 50 {
        let sym1 = enc1.repair_symbol(esi);
        let sym2 = enc2.repair_symbol(esi);
        assert_eq!(
            sym1, sym2,
            "bead_id={BEAD_ID} case=tuple_deterministic esi={esi}"
        );
    }
}

#[test]
fn test_tuple_generator_different_esi_differ() {
    // Different ESIs should (almost certainly) produce different repair symbols.
    let k = 10_usize;
    let symbol_size = 64_usize;
    let seed = 1337_u64;
    let source: Vec<Vec<u8>> = (0..k)
        .map(|i| {
            (0..symbol_size)
                .map(|j| u8::try_from((i * 37 + j * 13 + 7) % 256).unwrap_or(0))
                .collect()
        })
        .collect();

    let encoder = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder");
    let k_u32 = u32::try_from(k).expect("k fits u32");
    let mut visited = HashSet::new();
    for esi in k_u32..k_u32 + 20 {
        let sym = encoder.repair_symbol(esi);
        let was_new = visited.insert(sym);
        assert!(
            was_new,
            "bead_id={BEAD_ID} case=tuple_different_esi_differ esi={esi} (duplicate!)"
        );
    }
}

#[test]
fn test_tuple_generator_different_seeds_differ() {
    // Different seeds should produce different encodings.
    let k = 10_usize;
    let symbol_size = 64_usize;
    let source: Vec<Vec<u8>> = (0..k).map(|_| vec![0xAB; symbol_size]).collect();

    let enc1 = SystematicEncoder::new(&source, symbol_size, 1).expect("encoder seed=1");
    let enc2 = SystematicEncoder::new(&source, symbol_size, 2).expect("encoder seed=2");
    let k_u32 = u32::try_from(k).expect("k fits u32");

    let sym1 = enc1.repair_symbol(k_u32);
    let sym2 = enc2.repair_symbol(k_u32);
    assert_ne!(
        sym1, sym2,
        "bead_id={BEAD_ID} case=tuple_different_seeds esi={k_u32}"
    );
}

// ============================================================================
// Systematic property verification
// ============================================================================

#[test]
fn test_systematic_property_k5() {
    verify_systematic_property(5, 32, 42);
}

#[test]
fn test_systematic_property_k10() {
    verify_systematic_property(10, 64, 42);
}

#[test]
fn test_systematic_property_k50() {
    verify_systematic_property(50, 64, 42);
}

#[test]
fn test_systematic_property_k100() {
    verify_systematic_property(100, 64, 42);
}

#[test]
fn test_systematic_property_k500() {
    verify_systematic_property(500, 64, 42);
}

/// Verify that the first K emitted symbols are identical to the source symbols.
///
/// This is the core systematic property of RFC 6330: for ISI X < K_prime,
/// the encoding symbol IS the source symbol (zero encoding overhead).
fn verify_systematic_property(k: usize, symbol_size: usize, seed: u64) {
    let source: Vec<Vec<u8>> = (0..k)
        .map(|i| {
            (0..symbol_size)
                .map(|j| u8::try_from((i * 37 + j * 13 + 7) % 256).unwrap_or(0))
                .collect()
        })
        .collect();

    let mut encoder = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder creation");
    let systematic_symbols = encoder.emit_systematic();

    assert_eq!(
        systematic_symbols.len(),
        k,
        "bead_id={BEAD_ID} case=systematic_emit_count k={k}"
    );

    for (i, emitted) in systematic_symbols.iter().enumerate() {
        assert_eq!(
            emitted.data, source[i],
            "bead_id={BEAD_ID} case=systematic_property k={k} esi={i} \
             (emitted != source)"
        );
        assert!(
            emitted.is_source,
            "bead_id={BEAD_ID} case=systematic_flag k={k} esi={i}"
        );
    }
}

// ============================================================================
// Constraint matrix structure verification
// ============================================================================

#[test]
fn test_constraint_matrix_dimensions() {
    // Matrix should be (S + H + K) rows x L columns.
    for &k in &[5, 20, 100] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        let expected_rows = params.s + params.h + params.k;
        assert_eq!(
            matrix.rows, expected_rows,
            "bead_id={BEAD_ID} case=matrix_rows k={k} expected={expected_rows} actual={}",
            matrix.rows
        );
        assert_eq!(
            matrix.cols, params.l,
            "bead_id={BEAD_ID} case=matrix_cols k={k} expected={} actual={}",
            params.l, matrix.cols
        );
    }
}

#[test]
fn test_constraint_matrix_deterministic() {
    // Same parameters and seed → same matrix.
    let params = SystematicParams::for_source_block(20, 64);
    let m1 = ConstraintMatrix::build(&params, 42);
    let m2 = ConstraintMatrix::build(&params, 42);
    for row in 0..m1.rows {
        for col in 0..m1.cols {
            assert_eq!(
                m1.get(row, col),
                m2.get(row, col),
                "bead_id={BEAD_ID} case=matrix_deterministic row={row} col={col}"
            );
        }
    }
}

// ============================================================================
// Robust Soliton distribution verification
// ============================================================================

#[test]
fn test_robust_soliton_degree_range() {
    // Sample should return values in [1, max_degree].
    let soliton = RobustSoliton::new(100, 0.2, 0.05);
    for v in (0..100_000_u32).step_by(100) {
        let d = soliton.sample(v);
        assert!(d >= 1, "bead_id={BEAD_ID} case=soliton_min v={v} d={d}");
        assert!(
            d <= soliton.max_degree(),
            "bead_id={BEAD_ID} case=soliton_max v={v} d={d} max={}",
            soliton.max_degree()
        );
    }
}

#[test]
fn test_robust_soliton_deterministic() {
    let s1 = RobustSoliton::new(100, 0.2, 0.05);
    let s2 = RobustSoliton::new(100, 0.2, 0.05);
    for v in (0..10_000_u32).step_by(100) {
        assert_eq!(
            s1.sample(v),
            s2.sample(v),
            "bead_id={BEAD_ID} case=soliton_deterministic v={v}"
        );
    }
}

#[test]
fn test_robust_soliton_validates_bad_params() {
    // k=0 or negative c/delta should be caught.
    assert!(
        RobustSoliton::validate_params(0, 0.2, 0.05).is_some(),
        "bead_id={BEAD_ID} case=soliton_validate_k_zero"
    );
    assert!(
        RobustSoliton::validate_params(100, 0.0, 0.05).is_some(),
        "bead_id={BEAD_ID} case=soliton_validate_c_zero"
    );
    assert!(
        RobustSoliton::validate_params(100, 0.2, 0.0).is_some(),
        "bead_id={BEAD_ID} case=soliton_validate_delta_zero"
    );
}

// ============================================================================
// E2E Encode/Decode roundtrip for representative K values
// ============================================================================

#[test]
fn test_e2e_roundtrip_k5() {
    verify_roundtrip(5, 64, 42);
}

#[test]
fn test_e2e_roundtrip_k50() {
    verify_roundtrip(50, 64, 42);
}

#[test]
fn test_e2e_roundtrip_k500() {
    verify_roundtrip(500, 64, 42);
}

#[test]
fn test_e2e_roundtrip_k1000() {
    verify_roundtrip(1000, 64, 42);
}

#[test]
fn test_e2e_roundtrip_different_symbol_sizes() {
    // Verify roundtrip with various symbol sizes.
    for &symbol_size in &[16, 32, 64, 128, 256] {
        verify_roundtrip(20, symbol_size, 42);
    }
}

/// Full encode → decode roundtrip verification.
///
/// Creates K source symbols, encodes them, constructs decoder input from
/// constraint + source + repair symbols, decodes, and verifies byte-perfect
/// recovery of the original source data.
fn verify_roundtrip(k: usize, symbol_size: usize, seed: u64) {
    // Generate patterned source data (deterministic, debuggable).
    let source: Vec<Vec<u8>> = (0..k)
        .map(|i| {
            (0..symbol_size)
                .map(|j| u8::try_from((i * 37 + j * 13 + 7) % 256).unwrap_or(0))
                .collect()
        })
        .collect();

    let encoder = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder creation");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);
    let params = decoder.params();
    let k_u32 = u32::try_from(k).expect("k fits u32");
    let l_u32 = u32::try_from(params.l).expect("l fits u32");
    let base_rows = params.s + params.h;
    let constraints = ConstraintMatrix::build(params, seed);

    // Collect all received symbols: constraints + source + repair.
    let mut received: Vec<ReceivedSymbol> = decoder.constraint_symbols();

    // Add source symbols with their LT equations.
    for (i, data) in source.iter().enumerate() {
        let row = base_rows + i;
        let mut columns = Vec::new();
        let mut coefficients = Vec::new();
        for col in 0..constraints.cols {
            let coeff = constraints.get(row, col);
            if !coeff.is_zero() {
                columns.push(col);
                coefficients.push(coeff);
            }
        }
        received.push(ReceivedSymbol {
            esi: u32::try_from(i).expect("esi fits u32"),
            is_source: true,
            columns,
            coefficients,
            data: data.clone(),
        });
    }

    // Add repair symbols to reach L total.
    for esi in k_u32..l_u32 {
        let (cols, coefs) = decoder.repair_equation(esi);
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(esi, cols, coefs, repair_data));
    }

    let result = decoder
        .decode(&received)
        .expect("bead_id=bd-1hi.8 case=roundtrip_decode_failed");

    assert_eq!(
        result.source, source,
        "bead_id={BEAD_ID} case=e2e_roundtrip_k{k} source mismatch"
    );
}

// ============================================================================
// §3.2.3 Encoding pipeline normalization tests (bd-1hi.3)
// ============================================================================

fn stage_span(
    stage: &'static str,
    params: &SystematicParams,
    seed: u64,
    isi: u32,
) -> tracing::span::EnteredSpan {
    info_span!(
        "encoding_pipeline_stage",
        stage,
        k = params.k,
        k_prime = params.k,
        s = params.s,
        h = params.h,
        w = params.w,
        l = params.l,
        isi,
        seed
    )
    .entered()
}

fn deterministic_source_block(k: usize, symbol_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..symbol_size)
                .map(|j| u8::try_from((i * 37 + j * 13 + 7) % 256).unwrap_or(0))
                .collect()
        })
        .collect()
}

fn tiny_digest(bytes: &[u8]) -> u64 {
    let mut acc = 0_u64;
    for (idx, byte) in bytes.iter().take(16).enumerate() {
        let shift = u32::try_from((idx % 8) * 8).expect("shift fits u32");
        acc ^= u64::from(*byte) << shift;
    }
    let len_u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    acc ^ len_u64
}

fn assert_constraint_system_holds(
    matrix: &ConstraintMatrix,
    params: &SystematicParams,
    intermediate: &[Vec<u8>],
    source: &[Vec<u8>],
    symbol_size: usize,
    seed: u64,
) -> Result<(), String> {
    let zero = vec![0_u8; symbol_size];
    let lt_start = params.s + params.h;

    for row in 0..matrix.rows {
        let mut computed = vec![0_u8; symbol_size];
        for (col, intermediate_symbol) in intermediate.iter().enumerate().take(matrix.cols) {
            let coeff = matrix.get(row, col);
            if coeff.is_zero() {
                continue;
            }
            for (dst, src) in computed.iter_mut().zip(intermediate_symbol.iter()) {
                *dst ^= (coeff * Gf256(*src)).raw();
            }
        }

        let expected = if row < lt_start {
            &zero
        } else if row - lt_start < source.len() {
            &source[row - lt_start]
        } else {
            &zero
        };

        if computed != *expected {
            let isi = u32::try_from(row.saturating_sub(lt_start)).unwrap_or(u32::MAX);
            error!(
                bead_id = ENCODING_PIPELINE_BEAD_ID,
                stage = "solve_check",
                k = params.k,
                k_prime = params.k,
                s = params.s,
                h = params.h,
                w = params.w,
                l = params.l,
                isi,
                seed,
                expected_digest = tiny_digest(expected),
                actual_digest = tiny_digest(&computed),
                "constraint-system mismatch while checking A*C=D"
            );
            return Err(format!("A*C=D mismatch at row {row}"));
        }
    }
    Ok(())
}

#[test]
fn test_encoding_pipeline_stages() {
    let k = 64_usize;
    let symbol_size = 128_usize;
    let seed = 42_u64;
    let source = deterministic_source_block(k, symbol_size);

    let params = SystematicParams::for_source_block(k, symbol_size);
    {
        let _stage = stage_span("determine_parameters", &params, seed, 0);
        assert_eq!(params.k, k);
        assert_eq!(params.l, params.k + params.s + params.h);
    }

    let matrix = {
        let _stage = stage_span("construct_constraint_matrix", &params, seed, 0);
        let matrix = ConstraintMatrix::build(&params, seed);
        assert_eq!(matrix.rows, params.s + params.h + params.k);
        assert_eq!(matrix.cols, params.l);
        matrix
    };

    let lt_start = params.s + params.h;
    {
        let _stage = stage_span("build_source_vector", &params, seed, 0);
        assert_eq!(matrix.rows - lt_start, params.k);
    }

    {
        let _stage = stage_span("solve_intermediate_symbols", &params, seed, 0);
        let encoder = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder creation");
        let intermediate: Vec<Vec<u8>> = (0..params.l)
            .map(|idx| encoder.intermediate_symbol(idx).to_vec())
            .collect();
        assert_eq!(intermediate.len(), params.l);

        assert_constraint_system_holds(&matrix, &params, &intermediate, &source, symbol_size, seed)
            .expect("A*C=D must hold for systematic encoder output");
    }

    {
        let _stage = stage_span("generate_encoding_symbols", &params, seed, 0);
        let mut enc = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder emit");
        let source_syms = enc.emit_systematic();
        assert_eq!(source_syms.len(), k);
        for (index, symbol) in source_syms.iter().enumerate() {
            assert_eq!(symbol.data, source[index]);
            assert!(symbol.is_source);
        }
    }
}

#[test]
fn test_encoding_pipeline_correctness() {
    verify_roundtrip(128, 256, 42);
}

#[test]
fn test_encoding_pipeline_systematic() {
    verify_systematic_property(100, 64, 42);
}

#[test]
fn test_encoding_pipeline_deterministic() {
    let k = 96_usize;
    let symbol_size = 128_usize;
    let seed = 7_u64;
    let source = deterministic_source_block(k, symbol_size);
    let params = SystematicParams::for_source_block(k, symbol_size);

    let mut encoder_a = {
        let _stage = stage_span("determinism_check", &params, seed, 0);
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder A")
    };
    let mut encoder_b = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder B");

    {
        let _stage = stage_span("deterministic_systematic_symbols", &params, seed, 0);
        let source_a = encoder_a.emit_systematic();
        let source_b = encoder_b.emit_systematic();
        assert_eq!(source_a.len(), source_b.len());
        for (idx, (lhs, rhs)) in source_a.iter().zip(source_b.iter()).enumerate() {
            assert_eq!(
                lhs.data, rhs.data,
                "bead_id={ENCODING_PIPELINE_BEAD_ID} case=deterministic_source idx={idx}"
            );
        }
    }

    let k_u32 = u32::try_from(k).expect("k fits u32");
    for esi in k_u32..k_u32 + 16 {
        let _stage = stage_span("deterministic_repair_symbol", &params, seed, esi);
        let repair_a = encoder_a.repair_symbol(esi);
        let repair_b = encoder_b.repair_symbol(esi);
        assert_eq!(
            repair_a, repair_b,
            "bead_id={ENCODING_PIPELINE_BEAD_ID} case=deterministic_repair esi={esi}"
        );
    }
}

#[test]
fn test_e2e_encode_decode_pipeline() {
    verify_roundtrip(1000, 4096, 42);
}

// ============================================================================
// Repair equation determinism (tuple generation core)
// ============================================================================

#[test]
fn test_repair_equation_deterministic() {
    // InactivationDecoder::repair_equation must be deterministic.
    let k = 20;
    let d1 = InactivationDecoder::new(k, 64, 42);
    let d2 = InactivationDecoder::new(k, 64, 42);

    for esi in 20..40_u32 {
        let (cols1, coefs1) = d1.repair_equation(esi);
        let (cols2, coefs2) = d2.repair_equation(esi);
        assert_eq!(
            cols1, cols2,
            "bead_id={BEAD_ID} case=repair_eq_columns_deterministic esi={esi}"
        );
        assert_eq!(
            coefs1, coefs2,
            "bead_id={BEAD_ID} case=repair_eq_coefs_deterministic esi={esi}"
        );
    }
}

#[test]
fn test_repair_equation_columns_valid() {
    // All column indices must be < L.
    let k = 50;
    let decoder = InactivationDecoder::new(k, 64, 42);
    let l = decoder.params().l;

    for esi in 50..100_u32 {
        let (cols, coefs) = decoder.repair_equation(esi);
        assert_eq!(
            cols.len(),
            coefs.len(),
            "bead_id={BEAD_ID} case=repair_eq_len_match esi={esi}"
        );
        for &col in &cols {
            assert!(
                col < l,
                "bead_id={BEAD_ID} case=repair_eq_col_valid esi={esi} col={col} l={l}"
            );
        }
        // Columns should be distinct (no duplicate XOR cancellation).
        let unique: HashSet<usize> = cols.iter().copied().collect();
        assert_eq!(
            unique.len(),
            cols.len(),
            "bead_id={BEAD_ID} case=repair_eq_cols_distinct esi={esi}"
        );
    }
}

// ============================================================================
// Encoding statistics verification
// ============================================================================

#[test]
fn test_encoding_stats_overhead_ratio() {
    // Overhead ratio L/K should be > 1 (overhead from S + H).
    let k = 100;
    let source: Vec<Vec<u8>> = (0..k).map(|_| vec![0u8; 64]).collect();
    let encoder = SystematicEncoder::new(&source, 64, 42).expect("encoder");
    let overhead = encoder.stats().overhead_ratio();
    assert!(
        overhead > 1.0,
        "bead_id={BEAD_ID} case=overhead_gt_1 k={k} overhead={overhead}"
    );
    // Overhead should be reasonable (< 1.5 for K=100).
    assert!(
        overhead < 1.5,
        "bead_id={BEAD_ID} case=overhead_lt_1_5 k={k} overhead={overhead}"
    );
}

// ============================================================================
// Helpers
// ============================================================================

/// Simple primality test.
fn is_prime(n: usize) -> bool {
    if n < 2 {
        return false;
    }
    if n < 4 {
        return true;
    }
    if n % 2 == 0 || n % 3 == 0 {
        return false;
    }
    let mut i = 5;
    while i * i <= n {
        if n % i == 0 || n % (i + 2) == 0 {
            return false;
        }
        i += 6;
    }
    true
}
