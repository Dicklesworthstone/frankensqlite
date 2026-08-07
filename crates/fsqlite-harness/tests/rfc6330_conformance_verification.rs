//! RFC 6330 Conformance Test Suite + HDPC Matrix Verification.
//!
//! Bead: bd-1tup
//!
//! Verifies asupersync's RFC 6330 (RaptorQ) implementation at the
//! mathematical level: GF(256) arithmetic tables, LDPC/HDPC constraint
//! generation, systematic parameters, and encode/decode correctness.
//!
//! Key verification targets (per §3.1-3.3):
//!   - GF(256) field with irreducible polynomial 0x11D, generator g=2
//!   - Worked example: 0xA3 * 0x47 = 0xE1
//!   - LDPC: 3 nonzeros per source column with circulant stride
//!   - HDPC: GF(256) coefficients from GAMMA×MT product (not GF(2))
//!   - L = K' + S + H identity from the RFC systematic index table
//!   - Encode/decode roundtrip at K, K+2 symbol boundaries

use std::collections::HashSet;

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::gf256::Gf256;
use asupersync::raptorq::rfc6330;
use asupersync::raptorq::systematic::{ConstraintMatrix, SystematicEncoder, SystematicParams};

const BEAD_ID: &str = "bd-1tup";

// ============================================================================
// GF(256) reference implementations (from first principles)
// ============================================================================

/// Reduction mask for GF(256) polynomial p(x) = x^8 + x^4 + x^3 + x^2 + 1.
/// Lower 8 bits (0x1D) after subtracting x^8 from 0x11D.
const GF256_POLY: u8 = 0x1D;

/// Naive GF(256) multiplication via shift-and-XOR over the polynomial.
/// Independent of any lookup tables — computed from the definition.
fn gf256_mul_naive(mut a: u8, mut b: u8) -> u8 {
    let mut result = 0u8;
    for _ in 0..8 {
        if b & 1 != 0 {
            result ^= a;
        }
        let hi = a & 0x80;
        a <<= 1;
        if hi != 0 {
            a ^= GF256_POLY;
        }
        b >>= 1;
    }
    result
}

/// Build reference EXP table: EXP[i] = g^i where g=2 (primitive element).
/// Extended to 512 entries for mod-free log-sum lookup (EXP[a+b] works
/// without reduction when a,b < 255).
fn build_reference_exp_table() -> Vec<u8> {
    let mut exp = vec![0u8; 512];
    let mut val = 1u8;
    for i in 0..255 {
        exp[i] = val;
        exp[i + 255] = val; // mirror for mod-free lookup
        let hi = val & 0x80;
        val <<= 1;
        if hi != 0 {
            val ^= GF256_POLY;
        }
    }
    exp[255] = 1; // g^255 = g^0 = 1 (multiplicative order)
    exp[510] = 1;
    exp
}

/// Build reference LOG table from EXP: LOG[a] = k such that g^k = a.
/// LOG[0] = 0 (sentinel — log of zero is undefined).
fn build_reference_log_table(exp: &[u8]) -> Vec<u8> {
    let mut log = vec![0u8; 256];
    for (i, &e) in exp.iter().enumerate().take(255) {
        log[e as usize] = u8::try_from(i).expect("index < 255");
    }
    log
}

/// Reference multiplication via log/exp: a*b = EXP[LOG[a] + LOG[b]].
fn gf256_mul_via_log(a: u8, b: u8, log: &[u8], exp: &[u8]) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let log_sum = log[a as usize] as usize + log[b as usize] as usize;
    exp[log_sum]
}

/// Reference inverse: inv(a) = EXP[255 - LOG[a]].
fn gf256_inv_ref(a: u8, log: &[u8], exp: &[u8]) -> u8 {
    assert_ne!(a, 0, "cannot invert zero");
    exp[255 - log[a as usize] as usize]
}

/// Simple primality test.
fn is_prime(n: usize) -> bool {
    if n < 2 {
        return false;
    }
    if n < 4 {
        return true;
    }
    if n.is_multiple_of(2) || n.is_multiple_of(3) {
        return false;
    }
    let mut i = 5;
    while i * i <= n {
        if n.is_multiple_of(i) || n.is_multiple_of(i + 2) {
            return false;
        }
        i += 6;
    }
    true
}

// ============================================================================
// Test helpers for encode/decode
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

/// Build a full set of received symbols for the decoder.
///
/// Uses the critical pattern: constraint_symbols() (S+H LDPC/HDPC equations)
/// + source symbols with the decoder's canonical systematic identity equation + repair symbols.
fn build_full_decode_input(
    source: &[Vec<u8>],
    encoder: &SystematicEncoder,
    k: usize,
    sym_sz: usize,
    seed: u64,
) -> (InactivationDecoder, Vec<ReceivedSymbol>) {
    let decoder = InactivationDecoder::new(k, sym_sz, seed);
    let params = decoder.params();

    let mut received = decoder.constraint_symbols();

    for (i, data) in source.iter().enumerate() {
        received.push(ReceivedSymbol::source(
            u32::try_from(i).expect("ESI fits u32"),
            data.clone(),
        ));
    }

    #[allow(clippy::cast_possible_truncation)]
    let k_u32 = k as u32;
    #[allow(clippy::cast_possible_truncation)]
    let l_u32 = params.l as u32;
    for esi in k_u32..l_u32 {
        let (cols, coeffs) = decoder
            .repair_equation_rfc6330(esi)
            .expect("repair symbol should have an RFC6330 repair equation");
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(esi, cols, coeffs, repair_data));
    }

    (decoder, received)
}

/// Build decode input with some source symbols erased and replaced by repair.
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

    let mut received = decoder.constraint_symbols();

    for (i, data) in source.iter().enumerate() {
        if drop_indices.contains(&i) {
            continue;
        }
        received.push(ReceivedSymbol::source(
            u32::try_from(i).expect("ESI fits u32"),
            data.clone(),
        ));
    }

    #[allow(clippy::cast_possible_truncation)]
    let k_u32 = k as u32;
    let repair_count = drop_indices.len() + params.s + params.h;
    #[allow(clippy::cast_possible_truncation)]
    for esi in k_u32..k_u32 + repair_count as u32 {
        let (cols, coeffs) = decoder
            .repair_equation_rfc6330(esi)
            .expect("repair symbol should have an RFC6330 repair equation");
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(esi, cols, coeffs, repair_data));
    }

    (decoder, received)
}

// ============================================================================
// GF(256) Field Verification Tests (INV-GF256-CORRECT)
// ============================================================================

/// Verify EXP[LOG[a]] = a for all nonzero a, and LOG[EXP[k]] = k for all k.
/// Cross-checks reference tables against Gf256::ALPHA.pow(k).
#[test]
fn test_gf256_log_exp_tables_roundtrip() {
    let _ = BEAD_ID;
    let ref_exp = build_reference_exp_table();
    let ref_log = build_reference_log_table(&ref_exp);

    // For every nonzero a (1..=255): EXP[LOG[a]] == a
    for a in 1u8..=255 {
        let log_a = ref_log[a as usize];
        let roundtrip = ref_exp[log_a as usize];
        assert_eq!(roundtrip, a, "EXP[LOG[{a}]] = {roundtrip}, expected {a}");
    }

    // For every k (0..=254): LOG[EXP[k]] == k
    for k in 0u8..=254 {
        let exp_k = ref_exp[k as usize];
        let roundtrip = ref_log[exp_k as usize];
        assert_eq!(roundtrip, k, "LOG[EXP[{k}]] = {roundtrip}, expected {k}");
    }

    // EXP[0] == 1 (g^0 = 1)
    assert_eq!(ref_exp[0], 1, "EXP[0] should be 1");

    // EXP[255] == EXP[0] == 1 (g^255 = 1, order of generator is 255)
    assert_eq!(ref_exp[255], 1, "EXP[255] should be 1");

    // Cross-check: Gf256::ALPHA.pow(k) matches reference EXP[k]
    for k in 0..=254u8 {
        let from_api = Gf256::ALPHA.pow(k).raw();
        let from_ref = ref_exp[k as usize];
        assert_eq!(
            from_api, from_ref,
            "Gf256::ALPHA.pow({k}) = {from_api}, ref EXP[{k}] = {from_ref}"
        );
    }
}

/// Verify the worked example 0xA3 * 0x47 = 0xE1 with intermediate values.
/// Also: mul(a,1)=a, mul(a,0)=0, mul(a,inv(a))=1 for all a.
#[test]
fn test_gf256_multiplication_worked_example() {
    let _ = BEAD_ID;
    let ref_exp = build_reference_exp_table();
    let ref_log = build_reference_log_table(&ref_exp);

    // Worked example: 0xA3 * 0x47 = 0xE1
    let result = Gf256::new(0xA3) * Gf256::new(0x47);
    assert_eq!(
        result.raw(),
        0xE1,
        "0xA3 * 0x47 = {:#04X}, expected 0xE1",
        result.raw()
    );

    // Intermediate values:
    assert_eq!(ref_log[0xA3], 91, "LOG[0xA3] should be 91");
    assert_eq!(ref_log[0x47], 253, "LOG[0x47] should be 253");
    assert_eq!((91 + 253) % 255, 89, "log sum mod 255 should be 89");
    assert_eq!(ref_exp[89], 0xE1, "EXP[89] should be 0xE1");

    // Cross-check with naive polynomial multiplication
    assert_eq!(gf256_mul_naive(0xA3, 0x47), 0xE1);

    // mul(a, 1) == a for all a
    for a in 0..=255u8 {
        assert_eq!((Gf256::new(a) * Gf256::ONE).raw(), a);
    }

    // mul(a, 0) == 0 for all a
    for a in 0..=255u8 {
        assert_eq!((Gf256::new(a) * Gf256::ZERO).raw(), 0);
    }

    // mul(a, inverse(a)) == 1 for all nonzero a
    for a in 1..=255u8 {
        let product = Gf256::new(a) * Gf256::new(a).inv();
        assert_eq!(
            product.raw(),
            1,
            "mul({a}, inv({a})) = {}, expected 1",
            product.raw()
        );
    }
}

/// Verify field axioms for 1000 random triples: associativity,
/// distributivity, commutativity, and self-inverse under addition.
#[test]
fn test_gf256_field_axioms() {
    let _ = BEAD_ID;
    let mut rng_state = 0x1234_5678u64;
    let mut next_byte = || -> u8 {
        rng_state = rng_state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        #[allow(clippy::cast_possible_truncation)]
        {
            (rng_state >> 33) as u8
        }
    };

    for trial in 0..1000 {
        let a = Gf256::new(next_byte());
        let b = Gf256::new(next_byte());
        let c = Gf256::new(next_byte());

        // Commutativity: a*b == b*a
        assert_eq!(
            (a * b).raw(),
            (b * a).raw(),
            "trial {trial}: commutativity failed for {}, {}",
            a.raw(),
            b.raw()
        );

        // Associativity: (a*b)*c == a*(b*c)
        assert_eq!(
            ((a * b) * c).raw(),
            (a * (b * c)).raw(),
            "trial {trial}: associativity failed for {}, {}, {}",
            a.raw(),
            b.raw(),
            c.raw()
        );

        // Distributivity: a*(a+b) == a*a + a*b (addition is XOR)
        let lhs = a * (a + b);
        let aa = a * a;
        let ab = a * b;
        let rhs = aa + ab;
        assert_eq!(
            lhs.raw(),
            rhs.raw(),
            "trial {trial}: distributivity failed for {}, {}",
            a.raw(),
            b.raw()
        );

        // Self-inverse under addition: a + a == 0
        assert_eq!(
            (a + a).raw(),
            0,
            "trial {trial}: a+a != 0 for a={}",
            a.raw()
        );
    }
}

/// Exhaustive 65,536-pair check: Gf256 multiplication matches naive
/// polynomial multiplication for every (a, b) pair.
#[test]
fn test_gf256_mul_tables_consistency() {
    let _ = BEAD_ID;
    for a in 0..=255u8 {
        for b in 0..=255u8 {
            let from_api = (Gf256::new(a) * Gf256::new(b)).raw();
            let from_naive = gf256_mul_naive(a, b);
            assert_eq!(
                from_api, from_naive,
                "({a:#04X}, {b:#04X}): API={from_api:#04X}, naive={from_naive:#04X}"
            );
        }
    }
}

/// Exhaustive check: log/exp multiplication matches naive for all pairs.
#[test]
fn test_gf256_log_exp_mul_consistency() {
    let _ = BEAD_ID;
    let ref_exp = build_reference_exp_table();
    let ref_log = build_reference_log_table(&ref_exp);

    for a in 0..=255u8 {
        for b in 0..=255u8 {
            let from_logexp = gf256_mul_via_log(a, b, &ref_log, &ref_exp);
            let from_naive = gf256_mul_naive(a, b);
            assert_eq!(
                from_logexp, from_naive,
                "({a:#04X}, {b:#04X}): logexp={from_logexp:#04X}, naive={from_naive:#04X}"
            );
        }
    }
}

/// Generator g=2 must have multiplicative order exactly 255.
#[test]
fn test_gf256_generator_order_is_255() {
    let _ = BEAD_ID;
    let g = Gf256::ALPHA;
    let mut val = Gf256::ONE;
    for k in 1..255u32 {
        val *= g;
        assert_ne!(val.raw(), 1, "generator has order {k}, expected 255");
    }
    val *= g; // k = 255
    assert_eq!(val.raw(), 1, "g^255 should be 1");
}

/// Verify inverse properties: API matches reference, double-inverse is identity.
#[test]
fn test_gf256_inverse_properties() {
    let _ = BEAD_ID;
    let ref_exp = build_reference_exp_table();
    let ref_log = build_reference_log_table(&ref_exp);

    for a in 1..=255u8 {
        let inv_api = Gf256::new(a).inv().raw();
        let inv_ref = gf256_inv_ref(a, &ref_log, &ref_exp);
        assert_eq!(inv_api, inv_ref, "inverse mismatch for {a}");

        // Double inverse: inv(inv(a)) == a
        let double_inv = Gf256::new(inv_api).inv().raw();
        assert_eq!(double_inv, a, "inv(inv({a})) = {double_inv}");
    }
}

// ============================================================================
// Systematic Parameter Verification Tests (INV-L-FORMULA, INV-SYSTEMATIC-TABLE)
// ============================================================================

/// Verify parameter computation for representative K values:
/// K' comes from the RFC systematic index table, L=K'+S+H, and W/P/B/H are
/// internally consistent nonzero values from that table.
#[test]
fn test_systematic_parameter_computation() {
    let _ = BEAD_ID;
    let test_ks = [1, 4, 5, 10, 50, 100, 500, 1000, 5000, 10000];
    let sym_sz = 128;

    for &k in &test_ks {
        let params = SystematicParams::for_source_block(k, sym_sz);

        assert_eq!(params.k, k, "K={k}: params.k mismatch");
        assert!(
            params.k_prime >= k,
            "K={k}: K'={} must be >= K",
            params.k_prime
        );
        assert_eq!(
            params.l,
            params.k_prime + params.s + params.h,
            "K={k}: L != K'+S+H"
        );
        assert!(is_prime(params.s), "K={k}: S={} not prime", params.s);
        assert!(params.s >= 7, "K={k}: S={} < 7", params.s);

        assert!(params.h > 0, "K={k}: H must be nonzero");

        assert!(params.w <= params.l, "K={k}: W > L");
        assert_eq!(params.p, params.l - params.w, "K={k}: P != L-W");
        assert_eq!(params.b, params.w - params.s, "K={k}: B != W-S");
        assert_eq!(params.symbol_size, sym_sz, "K={k}: symbol_size mismatch");
    }
}

/// Verify L = K' + S + H for K=1..500 and selected large K values.
#[test]
fn test_parameter_l_equals_k_prime_plus_s_plus_h() {
    let _ = BEAD_ID;
    let sym_sz = 64;

    for k in 1..=500 {
        let params = SystematicParams::for_source_block(k, sym_sz);
        assert_eq!(
            params.l,
            params.k_prime + params.s + params.h,
            "K={k}: L={} != K'+S+H={}",
            params.l,
            params.k_prime + params.s + params.h
        );
    }

    for &k in &[1000, 2000, 5000, 10000, 20000, 50000] {
        let params = SystematicParams::for_source_block(k, sym_sz);
        assert_eq!(
            params.l,
            params.k_prime + params.s + params.h,
            "K={k}: L != K'+S+H"
        );
    }
}

/// Exhaustive check that S is prime for K=1..1000.
#[test]
fn test_systematic_s_is_always_prime() {
    let _ = BEAD_ID;
    let sym_sz = 64;
    for k in 1..=1000 {
        let params = SystematicParams::for_source_block(k, sym_sz);
        assert!(is_prime(params.s), "K={k}: S={} not prime", params.s);
    }
}

// ============================================================================
// Constraint Matrix Structure Tests (INV-HDPC-GF256)
// ============================================================================

/// Verify LDPC rows: each source column j has exactly 3 nonzeros at the
/// positions given by the circulant stride formula. LDPC entries are GF(2).
#[test]
#[allow(clippy::too_many_lines)]
fn test_ldpc_constraint_structure() {
    let _ = BEAD_ID;
    let k = 10;
    let sym_sz = 128;
    let seed = 42;
    let params = SystematicParams::for_source_block(k, sym_sz);
    let matrix = ConstraintMatrix::build(&params, seed);
    let s = params.s;

    assert_eq!(matrix.rows, params.s + params.h + params.k_prime);
    assert_eq!(matrix.cols, params.l);

    // Each source column j (0..K) has exactly 3 nonzero entries in LDPC rows
    for j in 0..k {
        let mut nonzero_rows = Vec::new();
        for row in 0..s {
            if !matrix.get(row, j).is_zero() {
                nonzero_rows.push(row);
            }
        }
        assert_eq!(
            nonzero_rows.len(),
            3,
            "source column {j}: {} nonzeros in LDPC, expected 3",
            nonzero_rows.len()
        );

        // Verify stride formula: a = 1 + floor(j/S), b = j % S
        // Positions: b, (b+a)%S, (b+2a)%S
        let a = 1 + j / s;
        let b_pos = j % s;
        let expected: HashSet<usize> = [b_pos, (b_pos + a) % s, (b_pos + 2 * a) % s]
            .into_iter()
            .collect();
        let actual: HashSet<usize> = nonzero_rows.into_iter().collect();
        assert_eq!(actual, expected, "column {j}: LDPC positions mismatch");
    }

    // LDPC identity block: row i has 1 at column K+i
    for i in 0..s {
        assert_eq!(
            matrix.get(i, k + i).raw(),
            1,
            "LDPC identity[{i}][{}]",
            k + i
        );
    }

    // All LDPC entries must be in GF(2): 0 or 1
    for row in 0..s {
        for col in 0..params.l {
            let val = matrix.get(row, col).raw();
            assert!(
                val == 0 || val == 1,
                "LDPC[{row}][{col}] = {val} not in GF(2)"
            );
        }
    }
}

/// Verify HDPC rows use GF(256) coefficients (not just GF(2)),
/// have the PI symbol identity block, and proper dimensions.
#[test]
fn test_hdpc_matrix_dimensions_and_gf256_entries() {
    let _ = BEAD_ID;
    for &k in &[6, 10, 20, 50] {
        let sym_sz = 128;
        let seed = 42;
        let params = SystematicParams::for_source_block(k, sym_sz);
        let matrix = ConstraintMatrix::build(&params, seed);
        let s = params.s;
        let h = params.h;
        let w = params.w;

        // HDPC identity block: row S+r has 1 at column K'+S+r.
        let hdpc_identity_start = params.k_prime + params.s;
        for r in 0..h {
            assert_eq!(
                matrix.get(s + r, hdpc_identity_start + r).raw(),
                1,
                "K={k}: HDPC identity[{r}] at col {}",
                hdpc_identity_start + r
            );
        }

        // INV-HDPC-GF256: at least one HDPC entry must be outside {0, 1}
        let mut found_non_binary = false;
        for r in 0..h {
            for c in 0..w {
                let val = matrix.get(s + r, c).raw();
                if val > 1 {
                    found_non_binary = true;
                    break;
                }
            }
            if found_non_binary {
                break;
            }
        }
        assert!(
            found_non_binary,
            "K={k}: HDPC must have GF(256) entries outside {{0,1}}"
        );
    }
}

/// Verify HDPC rows contain varied GF(256) values from GAMMA×MT construction.
#[test]
fn test_hdpc_gamma_alpha_variety() {
    let _ = BEAD_ID;
    let k = 10;
    let sym_sz = 128;
    let seed = 42;
    let params = SystematicParams::for_source_block(k, sym_sz);
    let matrix = ConstraintMatrix::build(&params, seed);
    let s = params.s;
    let h = params.h;
    let w = params.w;

    // Collect unique nonzero HDPC coefficients in columns 0..W
    let mut unique_values: HashSet<u8> = HashSet::new();
    for r in 0..h {
        for c in 0..w {
            let val = matrix.get(s + r, c).raw();
            if val != 0 {
                unique_values.insert(val);
            }
        }
    }

    // GAMMA×MT product should produce multiple distinct GF(256) values
    assert!(
        unique_values.len() > 1,
        "HDPC should have multiple distinct nonzero values, got {unique_values:?}"
    );
}

/// Constraint matrix is square (L×L) for all tested K values.
#[test]
fn test_constraint_matrix_dimensions() {
    let _ = BEAD_ID;
    for &k in &[4, 10, 50, 100] {
        let params = SystematicParams::for_source_block(k, 64);
        let matrix = ConstraintMatrix::build(&params, 12345);

        assert_eq!(
            matrix.rows,
            params.s + params.h + params.k_prime,
            "K={k}: rows"
        );
        assert_eq!(matrix.cols, params.l, "K={k}: cols");
        assert_eq!(matrix.rows, matrix.cols, "K={k}: matrix not square");
    }
}

/// LT rows for systematic encoding match the RFC tuple expansion for ESI 0..K'-1.
#[test]
fn test_lt_rows_match_rfc_tuple_rows() {
    let _ = BEAD_ID;
    let k = 20;
    let params = SystematicParams::for_source_block(k, 64);
    let matrix = ConstraintMatrix::build(&params, 99);
    let lt_start = params.s + params.h;

    for i in 0..params.k_prime {
        let expected_cols: HashSet<usize> = rfc6330::repair_indices_for_esi(
            params.j,
            params.w,
            params.p,
            u32::try_from(i).expect("K' row index must fit in u32"),
        )
        .into_iter()
        .collect();
        for c in 0..params.l {
            let expected = u8::from(expected_cols.contains(&c));
            let actual = matrix.get(lt_start + i, c).raw();
            assert_eq!(actual, expected, "LT[{i}][{c}]");
        }
    }
}

// ============================================================================
// RFC 6330 V-Table and Function Tests
// ============================================================================

/// RFC 6330 rand function is deterministic and output is in [0, m).
#[test]
fn test_rfc6330_rand_determinism_and_range() {
    let _ = BEAD_ID;
    for y in [0u32, 1, 100, 65535, 0xFFFF_FFFF] {
        for i in 0..8u8 {
            let r1 = rfc6330::rand(y, i, 1000);
            let r2 = rfc6330::rand(y, i, 1000);
            assert_eq!(r1, r2, "rand({y}, {i}, 1000) not deterministic");
            assert!(r1 < 1000);

            for m in [1u32, 10, 256, 65536] {
                let r = rfc6330::rand(y, i, m);
                assert!(r < m, "rand({y}, {i}, {m}) = {r} >= {m}");
            }
        }
    }
}

/// RFC 6330 degree function: output >= 1 and <= 30 (table-driven).
#[test]
fn test_rfc6330_deg_bounds() {
    let _ = BEAD_ID;
    let cases = [0u32, 100_000, 500_000, 1_000_000];
    for &v in &cases {
        let d = rfc6330::deg(v);
        assert!(d >= 1, "deg({v}) = {d}, must be >= 1");
        assert!(d <= 30, "deg({v}) = {d}, must be <= 30");
    }
}

// ============================================================================
// Encode/Decode Roundtrip Tests (INV-DECODE-PROBABILITY)
// ============================================================================

#[allow(clippy::similar_names)]
fn encode_decode_roundtrip(k: usize, sym_sz: usize, seed: u64) {
    let source = make_source(k, sym_sz);
    let encoder = SystematicEncoder::new(&source, sym_sz, seed)
        .unwrap_or_else(|| panic!("K={k}: encoder construction failed"));

    let (dec, received) = build_full_decode_input(&source, &encoder, k, sym_sz, seed);
    let result = dec
        .decode(&received)
        .unwrap_or_else(|e| panic!("K={k}: decode failed: {e:?}"));

    for (i, (orig, got)) in source.iter().zip(result.source.iter()).enumerate() {
        assert_eq!(orig, got, "K={k}: symbol {i} mismatch");
    }
}

#[test]
fn test_encode_decode_roundtrip_k4() {
    encode_decode_roundtrip(4, 64, 42);
}

#[test]
fn test_encode_decode_roundtrip_k10() {
    encode_decode_roundtrip(10, 128, 42);
}

#[test]
fn test_encode_decode_roundtrip_k50() {
    encode_decode_roundtrip(50, 128, 42);
}

#[test]
fn test_encode_decode_roundtrip_k100() {
    encode_decode_roundtrip(100, 256, 42);
}

#[test]
fn test_encode_decode_roundtrip_k500() {
    encode_decode_roundtrip(500, 128, 42);
}

/// Decode with erasures: drop 2 source symbols and replace with repair.
#[test]
fn test_encode_decode_with_erasures() {
    let _ = BEAD_ID;
    let k = 64;
    let sym_sz = 128;
    let seed = 42;
    let source = make_source(k, sym_sz);
    let encoder =
        SystematicEncoder::new(&source, sym_sz, seed).expect("encoder construction failed");

    let drop_indices: HashSet<usize> = [5, 30].into_iter().collect();
    let (dec, received) =
        build_decode_with_erasures(&source, &drop_indices, &encoder, k, sym_sz, seed);

    let result = dec.decode(&received).expect("decode with erasures failed");
    for (i, (orig, got)) in source.iter().zip(result.source.iter()).enumerate() {
        assert_eq!(orig, got, "K={k}: symbol {i} mismatch (erasure test)");
    }
}

/// Verify systematic emission: emitted symbols match source data exactly.
#[test]
fn test_systematic_emission_matches_source() {
    let _ = BEAD_ID;
    for &k in &[4, 10, 50, 100] {
        let sym_sz = 128;
        let seed = 42;
        let source = make_source(k, sym_sz);
        let mut encoder = SystematicEncoder::new(&source, sym_sz, seed).expect("encoder failed");

        let systematic = encoder.emit_systematic();
        assert_eq!(systematic.len(), k, "K={k}: wrong systematic count");

        for (i, sym) in systematic.iter().enumerate() {
            assert!(sym.is_source, "K={k}: symbol {i} not marked source");
            #[allow(clippy::cast_possible_truncation)]
            {
                assert_eq!(sym.esi, i as u32, "K={k}: wrong ESI for symbol {i}");
            }
            assert_eq!(&sym.data, &source[i], "K={k}: systematic[{i}] != source");
        }
    }
}

/// Encoding is deterministic: same input+seed produces identical output.
#[test]
fn test_encoding_determinism() {
    let _ = BEAD_ID;
    let k = 30;
    let sym_sz = 128;
    let seed = 42;
    let source = make_source(k, sym_sz);

    let mut enc1 = SystematicEncoder::new(&source, sym_sz, seed).expect("enc1 failed");
    let mut enc2 = SystematicEncoder::new(&source, sym_sz, seed).expect("enc2 failed");

    let all1 = enc1.emit_all(10);
    let all2 = enc2.emit_all(10);

    assert_eq!(all1.len(), all2.len());
    for (i, (s1, s2)) in all1.iter().zip(all2.iter()).enumerate() {
        assert_eq!(s1.esi, s2.esi, "symbol {i}: ESI mismatch");
        assert_eq!(s1.data, s2.data, "symbol {i}: data mismatch");
        assert_eq!(
            s1.is_source, s2.is_source,
            "symbol {i}: source flag mismatch"
        );
    }
}

// ============================================================================
// E2E Conformance Test
// ============================================================================

/// End-to-end RFC 6330 conformance: verify parameter computation, systematic
/// emission, constraint matrix structure, and decode roundtrip for multiple
/// K values and seeds.
#[test]
fn test_e2e_rfc6330_conformance_vectors() {
    let _ = BEAD_ID;
    let configs: Vec<(usize, usize, Vec<u64>)> = vec![
        (4, 64, vec![0, 42, 12345]),
        (10, 128, vec![0, 42, 99999]),
        (50, 256, vec![42, 77777]),
        (100, 128, vec![42, 31337]),
    ];

    for (k, sym_sz, seeds) in &configs {
        for &seed in seeds {
            let source = make_source(*k, *sym_sz);

            // 1. Encoder constructs successfully
            let encoder = SystematicEncoder::new(&source, *sym_sz, seed)
                .unwrap_or_else(|| panic!("K={k}, seed={seed}: encoder failed"));

            // 2. Verify parameter invariants
            let params = encoder.params();
            assert_eq!(params.l, params.k_prime + params.s + params.h);
            assert!(is_prime(params.s));

            // 3. Constraint matrix is square
            let matrix = ConstraintMatrix::build(params, seed);
            assert_eq!(matrix.rows, matrix.cols);

            // 4. HDPC has GF(256) entries
            let mut has_gf256 = false;
            for r in 0..params.h {
                for c in 0..params.w {
                    if matrix.get(params.s + r, c).raw() > 1 {
                        has_gf256 = true;
                        break;
                    }
                }
                if has_gf256 {
                    break;
                }
            }
            assert!(has_gf256, "K={k}, seed={seed}: no GF(256) HDPC entries");

            // 5. Encode/decode roundtrip
            let (dec, received) = build_full_decode_input(&source, &encoder, *k, *sym_sz, seed);
            let result = dec
                .decode(&received)
                .unwrap_or_else(|e| panic!("K={k}, seed={seed}: decode failed: {e:?}"));

            for (i, (orig, got)) in source.iter().zip(result.source.iter()).enumerate() {
                assert_eq!(orig, got, "K={k}, seed={seed}: symbol {i} mismatch");
            }
        }
    }
}
