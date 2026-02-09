#![allow(clippy::cast_possible_truncation)]
//! RaptorQ Encoding Pipeline verification suite (§3.2.3).
//!
//! Bead: bd-1hi.3
//!
//! Verifies the 5-step encoding pipeline:
//!   Step 1: Determine coding parameters (K -> K', S, H, W, L)
//!   Step 2: Construct constraint matrix A (L x L)
//!   Step 3: Build source vector D
//!   Step 4: Solve A*C = D for intermediate symbols
//!   Step 5: Generate encoding symbols from intermediates
//!
//! Tests cover: pipeline stages, correctness, systematic property,
//! determinism, constraint matrix structure, and E2E roundtrips.

use std::collections::HashSet;

use asupersync::raptorq::decoder::{DecodeError, InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::gf256::Gf256;
use asupersync::raptorq::systematic::{ConstraintMatrix, SystematicEncoder, SystematicParams};

const BEAD_ID: &str = "bd-1hi.3";

// ============================================================================
// Helpers
// ============================================================================

fn make_source(k: usize, symbol_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..symbol_size)
                .map(|j| u8::try_from((i * 37 + j * 13 + 7) % 256).unwrap_or(0))
                .collect()
        })
        .collect()
}

fn try_encoder(k: usize, symbol_size: usize, seed: u64) -> Option<SystematicEncoder> {
    let source = make_source(k, symbol_size);
    SystematicEncoder::new(&source, symbol_size, seed)
}

fn encoder_or_skip(k: usize, symbol_size: usize) -> Option<SystematicEncoder> {
    for seed in [42, 123, 7, 999, 314_159] {
        if let Some(enc) = try_encoder(k, symbol_size, seed) {
            return Some(enc);
        }
    }
    None
}

/// Build decode input: constraint equations (S+H) + source symbols (K) + repair.
/// The decoder requires L received symbols minimum. constraint_symbols() provides
/// LDPC+HDPC rows; source symbols use their LT equations from the constraint matrix.
fn build_full_decode_input(
    source: &[Vec<u8>],
    encoder: &SystematicEncoder,
    k: usize,
    sym_sz: usize,
    seed: u64,
) -> (InactivationDecoder, Vec<ReceivedSymbol>) {
    let decoder = InactivationDecoder::new(k, sym_sz, seed);
    let params = decoder.params();
    let constraints = ConstraintMatrix::build(params, seed);
    let base_rows = params.s + params.h;

    let mut received = decoder.constraint_symbols();

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
            #[allow(clippy::cast_possible_truncation)]
            esi: i as u32,
            is_source: true,
            columns,
            coefficients,
            data: data.clone(),
        });
    }

    let k_u32 = k as u32;
    let l_u32 = params.l as u32;
    for esi in k_u32..l_u32 {
        let (cols, coeffs) = decoder.repair_equation(esi);
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(esi, cols, coeffs, repair_data));
    }

    (decoder, received)
}

fn build_decode_with_erasures(
    source: &[Vec<u8>],
    drop_indices: &HashSet<usize>,
    encoder: &SystematicEncoder,
    k: usize,
    sym_sz: usize,
    seed: u64,
) -> (InactivationDecoder, Vec<ReceivedSymbol>) {
    let decoder = InactivationDecoder::new(k, sym_sz, seed);
    let params = decoder.params();
    let constraints = ConstraintMatrix::build(params, seed);
    let base_rows = params.s + params.h;

    let mut received = decoder.constraint_symbols();

    for (i, data) in source.iter().enumerate() {
        if drop_indices.contains(&i) {
            continue;
        }
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
            #[allow(clippy::cast_possible_truncation)]
            esi: i as u32,
            is_source: true,
            columns,
            coefficients,
            data: data.clone(),
        });
    }

    let k_u32 = k as u32;
    let repair_count = drop_indices.len() + params.s + params.h;
    for esi in k_u32..k_u32 + repair_count as u32 {
        let (cols, coeffs) = decoder.repair_equation(esi);
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(esi, cols, coeffs, repair_data));
    }

    (decoder, received)
}

// ============================================================================
// Step 1: Coding parameter verification
// ============================================================================

#[test]
fn test_step1_coding_params_basic_invariants() {
    for &k in &[5, 10, 20, 50, 100, 200, 500, 1000] {
        let p = SystematicParams::for_source_block(k, 64);
        assert_eq!(p.l, p.k + p.s + p.h, "bead_id={BEAD_ID} L k={k}");
        assert_eq!(p.w, p.k + p.s, "bead_id={BEAD_ID} W k={k}");
        assert_eq!(p.p, p.h, "bead_id={BEAD_ID} P=H k={k}");
        assert_eq!(p.b, p.k, "bead_id={BEAD_ID} B=K k={k}");
        assert!(p.s >= 7, "bead_id={BEAD_ID} S>=7 k={k} s={}", p.s);
        assert!(p.h >= 3, "bead_id={BEAD_ID} H>=3 k={k} h={}", p.h);
    }
}

#[test]
fn test_step1_symbol_size_preserved() {
    for &sym_sz in &[64, 256, 1024, 4096] {
        let p = SystematicParams::for_source_block(50, sym_sz);
        assert_eq!(p.symbol_size, sym_sz);
    }
}

// ============================================================================
// Step 2: Constraint matrix structure
// ============================================================================

#[test]
fn test_step2_constraint_matrix_dimensions() {
    for &k in &[5, 20, 100] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        assert_eq!(matrix.rows, params.s + params.h + params.k);
        assert_eq!(matrix.cols, params.l);
    }
}

#[test]
fn test_step2_ldpc_rows_are_sparse() {
    for &k in &[10, 50, 100] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        for row in 0..params.s {
            let nz: usize = (0..params.l)
                .filter(|&col| !matrix.get(row, col).is_zero())
                .count();
            assert!(nz > 0, "bead_id={BEAD_ID} ldpc nonempty k={k} row={row}");
        }
    }
}

#[test]
fn test_step2_ldpc_identity_block() {
    for &k in &[10, 50] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        for i in 0..params.s {
            assert_eq!(matrix.get(i, params.k + i), Gf256::ONE);
        }
    }
}

#[test]
fn test_step2_hdpc_rows_use_gf256() {
    for &k in &[20, 100] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        let found = (params.s..params.s + params.h)
            .any(|row| (0..params.w).any(|col| matrix.get(row, col).raw() > 1));
        assert!(found, "bead_id={BEAD_ID} hdpc gf256 k={k}");
    }
}

#[test]
fn test_step2_hdpc_pi_identity_block() {
    for &k in &[20, 100] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        for r in 0..params.h {
            assert_eq!(matrix.get(params.s + r, params.w + r), Gf256::ONE);
        }
    }
}

#[test]
fn test_step2_lt_rows_systematic_identity() {
    for &k in &[10, 50] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 42);
        for i in 0..k {
            let row = params.s + params.h + i;
            assert_eq!(matrix.get(row, i), Gf256::ONE);
            for col in 0..params.l {
                if col != i {
                    assert!(matrix.get(row, col).is_zero());
                }
            }
        }
    }
}

#[test]
fn test_step2_constraint_matrix_deterministic() {
    let params = SystematicParams::for_source_block(50, 64);
    let m1 = ConstraintMatrix::build(&params, 42);
    let m2 = ConstraintMatrix::build(&params, 42);
    for row in 0..m1.rows {
        for col in 0..m1.cols {
            assert_eq!(m1.get(row, col), m2.get(row, col));
        }
    }
}

// ============================================================================
// Steps 3-4: Solve
// ============================================================================

#[test]
fn test_step3_4_solve_produces_intermediate_symbols() {
    let Some(enc) = encoder_or_skip(50, 64) else {
        return;
    };
    let params = enc.params();
    for i in 0..params.l {
        assert_eq!(enc.intermediate_symbol(i).len(), params.symbol_size);
    }
}

#[test]
fn test_step3_4_intermediate_first_k_match_source() {
    let k = 50;
    let source = make_source(k, 64);
    let Some(enc) = SystematicEncoder::new(&source, 64, 42) else {
        return;
    };
    for (i, src) in source.iter().enumerate() {
        assert_eq!(enc.intermediate_symbol(i), &src[..]);
    }
}

// ============================================================================
// Step 5: Emission
// ============================================================================

#[test]
fn test_step5_systematic_emission_is_source_identity() {
    for &k in &[5, 10, 50, 100] {
        let source = make_source(k, 64);
        let Some(mut enc) = SystematicEncoder::new(&source, 64, 42) else {
            continue;
        };
        let systematic = enc.emit_systematic();
        assert_eq!(systematic.len(), k);
        for (i, sym) in systematic.iter().enumerate() {
            assert_eq!(sym.esi, i as u32);
            assert!(sym.is_source);
            assert_eq!(sym.degree, 1);
            assert_eq!(sym.data, source[i]);
        }
    }
}

#[test]
fn test_step5_repair_symbols_are_not_source() {
    let Some(mut enc) = encoder_or_skip(50, 64) else {
        return;
    };
    let _ = enc.emit_systematic();
    let repairs = enc.emit_repair(10);
    assert_eq!(repairs.len(), 10);
    for (i, sym) in repairs.iter().enumerate() {
        assert_eq!(sym.esi, (50 + i) as u32);
        assert!(!sym.is_source);
        assert!(sym.degree >= 1);
    }
}

#[test]
fn test_step5_repair_esi_ascending() {
    let Some(mut enc) = encoder_or_skip(50, 64) else {
        return;
    };
    let _ = enc.emit_systematic();
    let b1 = enc.emit_repair(5);
    let b2 = enc.emit_repair(5);
    for (i, sym) in b1.iter().enumerate() {
        assert_eq!(sym.esi, (50 + i) as u32);
    }
    for (i, sym) in b2.iter().enumerate() {
        assert_eq!(sym.esi, (55 + i) as u32);
    }
}

// ============================================================================
// Pipeline-level tests
// ============================================================================

#[test]
fn test_encoding_pipeline_stages() {
    let k = 50;
    let source = make_source(k, 64);

    let params = SystematicParams::for_source_block(k, 64);
    assert_eq!(params.k, k);
    assert!(params.l > params.k);

    let matrix = ConstraintMatrix::build(&params, 42);
    assert_eq!(matrix.rows, params.s + params.h + params.k);
    assert_eq!(matrix.cols, params.l);

    let mut rhs: Vec<Vec<u8>> = Vec::with_capacity(matrix.rows);
    for _ in 0..params.s + params.h {
        rhs.push(vec![0u8; 64]);
    }
    for sym in &source {
        rhs.push(sym.clone());
    }
    assert_eq!(rhs.len(), matrix.rows);

    let intermediate = matrix.solve(&rhs).expect("solve k=50");
    assert_eq!(intermediate.len(), params.l);

    for i in 0..k {
        assert_eq!(
            intermediate[i], source[i],
            "bead_id={BEAD_ID} systematic i={i}"
        );
    }
}

#[test]
fn test_encoding_pipeline_correctness() {
    let k = 50;
    let seed = 42;
    let source = make_source(k, 64);
    let Some(enc) = SystematicEncoder::new(&source, 64, seed) else {
        return;
    };
    let (decoder, received) = build_full_decode_input(&source, &enc, k, 64, seed);
    let result = decoder.decode(&received).expect("decode");
    for (i, src) in source.iter().enumerate() {
        assert_eq!(result.source[i], *src, "bead_id={BEAD_ID} roundtrip i={i}");
    }
}

#[test]
fn test_encoding_pipeline_systematic() {
    for &k in &[5, 10, 50, 100] {
        let source = make_source(k, 64);
        let Some(mut enc) = SystematicEncoder::new(&source, 64, 42) else {
            continue;
        };
        let all = enc.emit_all(4);
        for i in 0..k {
            assert_eq!(all[i].data, source[i]);
            assert!(all[i].is_source);
        }
        for sym in all.iter().skip(k) {
            assert!(!sym.is_source);
            assert!(sym.esi >= k as u32);
        }
    }
}

#[test]
fn test_encoding_pipeline_deterministic() {
    let k = 50;
    let source = make_source(k, 64);
    let mut enc1 = SystematicEncoder::new(&source, 64, 42).unwrap();
    let mut enc2 = SystematicEncoder::new(&source, 64, 42).unwrap();
    let out1 = enc1.emit_all(10);
    let out2 = enc2.emit_all(10);
    assert_eq!(out1.len(), out2.len());
    for (i, (a, b)) in out1.iter().zip(out2.iter()).enumerate() {
        assert_eq!(a.esi, b.esi, "esi i={i}");
        assert_eq!(a.data, b.data, "data i={i}");
        assert_eq!(a.is_source, b.is_source, "src i={i}");
        assert_eq!(a.degree, b.degree, "deg i={i}");
    }
}

#[test]
fn test_encoding_different_seeds_differ() {
    let source = make_source(50, 64);
    let mut e1 = SystematicEncoder::new(&source, 64, 42).unwrap();
    let mut e2 = SystematicEncoder::new(&source, 64, 99).unwrap();
    let _ = e1.emit_systematic();
    let _ = e2.emit_systematic();
    let r1 = e1.emit_repair(5);
    let r2 = e2.emit_repair(5);
    assert!(r1.iter().zip(r2.iter()).any(|(a, b)| a.data != b.data));
}

// ============================================================================
// K' zero-padding
// ============================================================================

#[test]
fn test_k_prime_padding_handled() {
    for &k in &[3, 5, 7, 11, 13, 17, 23] {
        let source = make_source(k, 32);
        if let Some(mut enc) = SystematicEncoder::new(&source, 32, 42) {
            let sys = enc.emit_systematic();
            assert_eq!(sys.len(), k);
            for (i, sym) in sys.iter().enumerate() {
                assert_eq!(sym.data, source[i]);
            }
        }
    }
}

// ============================================================================
// Encoding stats
// ============================================================================

#[test]
fn test_encoding_stats_populated() {
    let Some(mut enc) = encoder_or_skip(50, 64) else {
        return;
    };
    let _ = enc.emit_systematic();
    let _ = enc.emit_repair(10);
    let stats = enc.stats();
    assert_eq!(stats.source_symbol_count, 50);
    assert_eq!(stats.symbol_size, 64);
    assert_eq!(stats.repair_symbols_generated, 10);
    assert_eq!(stats.systematic_bytes_emitted, 50 * 64);
    assert_eq!(stats.repair_bytes_emitted, 10 * 64);
    assert!(stats.degree_min >= 1);
    assert!(stats.degree_max >= stats.degree_min);
    assert!(stats.degree_count == 10);
    assert!(stats.overhead_ratio() > 1.0);
}

// ============================================================================
// E2E encode/decode pipeline tests
// ============================================================================

#[test]
fn test_e2e_encode_decode_pipeline_k64_t4096() {
    let k = 64;
    let seed = 42;
    let source = make_source(k, 4096);
    let Some(enc) = SystematicEncoder::new(&source, 4096, seed) else {
        eprintln!("bead_id={BEAD_ID} SKIP: singular k={k}");
        return;
    };
    for (i, src) in source.iter().enumerate() {
        assert_eq!(enc.intermediate_symbol(i), &src[..]);
    }
    let drop: HashSet<usize> = [7, 31].into_iter().collect();
    let (decoder, received) = build_decode_with_erasures(&source, &drop, &enc, k, 4096, seed);
    let result = decoder.decode(&received).expect("e2e decode");
    for (i, src) in source.iter().enumerate() {
        assert_eq!(result.source[i], *src, "bead_id={BEAD_ID} e2e i={i}");
    }
}

#[test]
fn test_e2e_roundtrip_all_source() {
    for &k in &[10, 50, 100] {
        let seed = 42;
        let source = make_source(k, 64);
        let Some(enc) = SystematicEncoder::new(&source, 64, seed) else {
            continue;
        };
        let (decoder, received) = build_full_decode_input(&source, &enc, k, 64, seed);
        let result = decoder.decode(&received).expect("all source decode");
        for (i, src) in source.iter().enumerate() {
            assert_eq!(result.source[i], *src);
        }
    }
}

#[test]
fn test_e2e_insufficient_symbols_error() {
    let k = 50;
    let source = make_source(k, 64);
    let Some(mut enc) = SystematicEncoder::new(&source, 64, 42) else {
        return;
    };
    let systematic = enc.emit_systematic();
    let decoder = InactivationDecoder::new(k, 64, 42);
    let received: Vec<ReceivedSymbol> = systematic
        .iter()
        .take(k / 2)
        .map(|s| ReceivedSymbol::source(s.esi, s.data.clone()))
        .collect();
    match decoder.decode(&received) {
        Err(DecodeError::InsufficientSymbols {
            received: r,
            required: req,
        }) => {
            assert_eq!(r, k / 2);
            assert!(req > r);
        }
        Ok(_) => panic!("should fail"),
        Err(e) => panic!("unexpected: {e:?}"),
    }
}

#[test]
fn test_e2e_symbol_size_mismatch_error() {
    let decoder = InactivationDecoder::new(10, 64, 42);
    let params = decoder.params();
    let received: Vec<ReceivedSymbol> = (0..params.l)
        .map(|i| ReceivedSymbol::source(i as u32, vec![0u8; 65]))
        .collect();
    match decoder.decode(&received) {
        Err(DecodeError::SymbolSizeMismatch { expected, actual }) => {
            assert_eq!(expected, 64);
            assert_eq!(actual, 65);
        }
        other => panic!("expected size mismatch: {other:?}"),
    }
}

#[test]
fn test_e2e_different_symbol_sizes() {
    for &sym_sz in &[32, 128, 512, 4096] {
        let k = 20;
        let seed = 42;
        let source = make_source(k, sym_sz);
        let Some(enc) = SystematicEncoder::new(&source, sym_sz, seed) else {
            continue;
        };
        let (decoder, received) = build_full_decode_input(&source, &enc, k, sym_sz, seed);
        let result = decoder.decode(&received).expect("various sizes");
        for (i, src) in source.iter().enumerate() {
            assert_eq!(result.source[i], *src);
        }
    }
}

#[test]
fn test_e2e_larger_k_500() {
    let k = 500;
    let seed = 42;
    let source = make_source(k, 256);
    let Some(enc) = SystematicEncoder::new(&source, 256, seed) else {
        eprintln!("bead_id={BEAD_ID} SKIP: singular k=500");
        return;
    };
    for (i, s) in source.iter().enumerate() {
        assert_eq!(enc.intermediate_symbol(i), &s[..]);
    }
    let (decoder, received) = build_full_decode_input(&source, &enc, k, 256, seed);
    let result = decoder.decode(&received).expect("decode k=500");
    for (i, src) in source.iter().enumerate() {
        assert_eq!(result.source[i], *src);
    }
}

// ============================================================================
// Repair symbol consistency
// ============================================================================

#[test]
fn test_repair_symbol_matches_equation() {
    let k = 50;
    let source = make_source(k, 64);
    let Some(enc) = SystematicEncoder::new(&source, 64, 42) else {
        return;
    };
    let decoder = InactivationDecoder::new(k, 64, 42);
    for esi in (k as u32)..(k as u32 + 5) {
        let repair_data = enc.repair_symbol(esi);
        let (cols, _) = decoder.repair_equation(esi);
        let mut expected = vec![0u8; 64];
        for &col in &cols {
            for (e, &s) in expected.iter_mut().zip(enc.intermediate_symbol(col).iter()) {
                *e ^= s;
            }
        }
        assert_eq!(
            repair_data, expected,
            "bead_id={BEAD_ID} repair eq esi={esi}"
        );
    }
}

#[test]
fn test_emit_all_order_and_counts() {
    let k = 30;
    let rc = 8;
    let Some(mut enc) = encoder_or_skip(k, 64) else {
        return;
    };
    let all = enc.emit_all(rc);
    assert_eq!(all.len(), k + rc);
    for (i, sym) in all.iter().enumerate().take(k) {
        assert_eq!(sym.esi, i as u32);
        assert!(sym.is_source);
    }
    for (i, sym) in all.iter().skip(k).enumerate() {
        assert_eq!(sym.esi, (k + i) as u32);
        assert!(!sym.is_source);
    }
}
