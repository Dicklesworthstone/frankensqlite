//! FrankenSQLite verification harness.
//!
//! This crate is intentionally not "just tests": it contains reusable
//! verification tooling (trace exporters, schedule exploration harnesses, etc.)
//! that other crates can call into from their own tests.

pub mod adversarial_search;
pub mod backlog_quality_gate;
pub mod benchmark_corpus;
pub mod ci_coverage_gate;
pub mod ci_gate_matrix;
pub mod closure_wave;
pub mod commit_pipeline;
pub mod concurrent_writer_parity;
pub mod confidence_gates;
pub mod corpus_ingest;
pub mod crash_recovery_parity;
pub mod differential_v2;
pub mod drift_monitor;
pub mod durability_matrix;
pub mod e2e_log_schema;
pub mod e2e_logging_init;
pub mod e2e_orchestrator;
pub mod e2e_traceability;
pub mod eprocess;
pub mod evidence_index;
pub mod execution_waves;
pub mod extension_parity_matrix;
pub mod failure_bundle;
pub mod fault_profiles;
pub mod fault_vfs;
pub mod fixture_discovery;
pub mod forensics_navigator;
pub mod fslab;
pub mod impact_graph;
pub mod isomorphism_proof;
pub mod lane_selector;
pub mod lock_txn_parity;
pub mod log;
pub mod log_schema_validator;
pub mod maintenance_parity;
pub mod metamorphic;
pub mod mismatch_minimizer;
pub mod no_mock_critical_path_gate;
pub mod no_mock_evidence;
pub mod oracle;
pub mod parity_evidence_matrix;
pub mod parity_invariant_catalog;
pub mod parity_taxonomy;
pub mod perf_loop;
pub mod performance_regression_detector;
pub mod planner_vdbe_closure;
pub mod ratchet_policy;
pub mod realdb_e2e_logging;
pub mod release_certificate;
pub mod replay_harness;
pub mod replay_triage;
pub mod scheduler;
pub mod score_engine;
pub mod seed_taxonomy;
pub mod semantic_gap_map;
pub mod soak_executor;
pub mod soak_profiles;
pub mod spec_to_beads_audit;
pub mod sql_pipeline_optimization;
pub mod sql_semantic_differential;
pub mod supervision;
pub mod tcl_conformance;
pub mod test_diagnostics;
pub mod tla;
pub mod toolchain_determinism;
pub mod unit_fixtures;
pub mod unit_matrix;
pub mod validation_manifest;
pub mod verification_contract_enforcement;
pub mod verification_gates;
pub mod wal_journal_parity;

#[cfg(test)]
mod sql_pipeline_suites;
#[cfg(test)]
mod storage_unit_suites;

#[cfg(test)]
mod gf256_verification {
    use std::sync::OnceLock;

    use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
    use asupersync::raptorq::gf256::{Gf256, gf256_addmul_slice, gf256_mul_slice};
    use asupersync::raptorq::systematic::{ConstraintMatrix, SystematicEncoder};
    use proptest::prelude::*;

    const BEAD_ID: &str = "bd-1hi.1";

    /// Full irreducible polynomial: x^8 + x^4 + x^3 + x^2 + 1.
    const POLY_FULL: u16 = 0x11D;
    /// Reduction mask (POLY_FULL without the x^8 term).
    const POLY_REDUCTION: u8 = 0x1D;
    /// Generator g = 2 (polynomial x).
    const GENERATOR: u8 = 0x02;

    #[derive(Debug, Clone)]
    struct Gf256Oracle {
        log: [u8; 256],
        exp: [u8; 512],
    }

    impl Gf256Oracle {
        fn global() -> &'static Self {
            static ORACLE: OnceLock<Gf256Oracle> = OnceLock::new();
            ORACLE.get_or_init(Self::new)
        }

        fn new() -> Self {
            let exp = build_exp_table();
            let log = build_log_table();
            Self { log, exp }
        }

        fn log(&self, a: u8) -> u8 {
            self.log[usize::from(a)]
        }

        fn exp(&self, k: usize) -> u8 {
            self.exp[k]
        }

        fn mul_logexp(&self, a: u8, b: u8) -> u8 {
            if a == 0 || b == 0 {
                return 0;
            }
            let idx = usize::from(self.log(a)) + usize::from(self.log(b));
            self.exp(idx)
        }

        fn inv(&self, a: u8) -> u8 {
            assert_ne!(a, 0, "cannot invert zero in GF(256)");
            let log_a = usize::from(self.log(a));
            self.exp(255 - log_a)
        }

        fn div(&self, a: u8, b: u8) -> u8 {
            assert_ne!(b, 0, "division by zero in GF(256)");
            self.mul_logexp(a, self.inv(b))
        }

        fn pow(&self, a: u8, exp: u16) -> u8 {
            if exp == 0 {
                return 1;
            }
            if a == 0 {
                return 0;
            }
            let log_a = u16::from(self.log(a));
            let log_result = (log_a * exp) % 255;
            self.exp(usize::from(log_result))
        }

        fn addmul_slice(&self, dst: &mut [u8], src: &[u8], c: u8) {
            assert_eq!(dst.len(), src.len(), "slice length mismatch");
            if c == 0 {
                return;
            }
            for (d, s) in dst.iter_mut().zip(src.iter()) {
                *d ^= self.mul_logexp(*s, c);
            }
        }
    }

    #[track_caller]
    fn fail_mismatch(case: &str, a: u8, b: u8, expected: u8, actual: u8, source: &str) {
        assert_eq!(
            actual, expected,
            "bead_id={BEAD_ID} case={case} a=0x{a:02X} b=0x{b:02X} expected=0x{expected:02X} actual=0x{actual:02X} source={source}",
        );
    }

    fn gf256_mul_const(mut a: u8, mut b: u8) -> u8 {
        // Polynomial multiplication with reduction by the irreducible polynomial.
        let mut acc = 0_u8;
        let mut i = 0_u8;
        while i < 8 {
            if (b & 1) != 0 {
                acc ^= a;
            }
            let hi = a & 0x80;
            a <<= 1;
            if hi != 0 {
                a ^= POLY_REDUCTION;
            }
            b >>= 1;
            i += 1;
        }
        acc
    }

    fn build_exp_table() -> [u8; 512] {
        let mut table = [0_u8; 512];
        let mut val: u16 = 1;
        for i in 0..255 {
            let v = u8::try_from(val).expect("exp table value fits in u8");
            table[i] = v;
            table[i + 255] = v; // mirror for mod-free lookup
            val <<= 1;
            if val & 0x100 != 0 {
                val ^= POLY_FULL;
            }
        }
        table[255] = 1;
        table[510] = 1;
        table
    }

    fn build_log_table() -> [u8; 256] {
        let mut table = [0_u8; 256];
        let mut val: u16 = 1;
        for i in 0_u16..=254 {
            let i_u8 = u8::try_from(i).expect("log table index fits in u8");
            table[usize::from(val)] = i_u8;
            val <<= 1;
            if val & 0x100 != 0 {
                val ^= POLY_FULL;
            }
        }
        table
    }

    fn poly_degree(poly: u16) -> Option<u32> {
        if poly == 0 {
            return None;
        }
        Some((u16::BITS - 1).saturating_sub(poly.leading_zeros()))
    }

    fn poly_mod(mut dividend: u16, divisor: u16) -> u16 {
        let div_deg = poly_degree(divisor).expect("divisor must be non-zero");
        while let Some(dvd_deg) = poly_degree(dividend) {
            if dvd_deg < div_deg {
                break;
            }
            let shift = dvd_deg - div_deg;
            dividend ^= divisor << shift;
        }
        dividend
    }

    // --- RFC 6330 §5.7 / spec §3.2.1 verification cases ---

    #[test]
    fn test_gf256_add_is_xor() {
        for a in 0_u8..=u8::MAX {
            for b in 0_u8..=u8::MAX {
                let expected = a ^ b;
                let actual = (Gf256(a) + Gf256(b)).0;
                if actual != expected {
                    fail_mismatch("add_is_xor", a, b, expected, actual, "asupersync");
                }
            }
        }
    }

    #[test]
    fn test_gf256_sub_is_xor() {
        for a in 0_u8..=u8::MAX {
            for b in 0_u8..=u8::MAX {
                let expected = a ^ b;
                let actual = (Gf256(a) - Gf256(b)).0;
                if actual != expected {
                    fail_mismatch("sub_is_xor", a, b, expected, actual, "asupersync");
                }
            }
        }
    }

    #[test]
    fn test_gf256_mul_zero_annihilator() {
        for a in 0_u8..=u8::MAX {
            let expected = 0_u8;
            let actual1 = (Gf256(a) * Gf256(0)).0;
            let actual2 = (Gf256(0) * Gf256(a)).0;
            if actual1 != expected {
                fail_mismatch(
                    "mul_zero_annihilator",
                    a,
                    0,
                    expected,
                    actual1,
                    "asupersync",
                );
            }
            if actual2 != expected {
                fail_mismatch(
                    "mul_zero_annihilator",
                    0,
                    a,
                    expected,
                    actual2,
                    "asupersync",
                );
            }
        }
    }

    #[test]
    fn test_gf256_mul_worked_example_a3_47_e1() {
        let o = Gf256Oracle::global();
        let a = 0xA3;
        let b = 0x47;
        let expected = 0xE1;

        let oracle = o.mul_logexp(a, b);
        assert_eq!(
            oracle, expected,
            "bead_id={BEAD_ID} case=worked_example oracle mismatch"
        );

        let actual = (Gf256(a) * Gf256(b)).0;
        if actual != expected {
            fail_mismatch(
                "worked_example_a3_47_e1",
                a,
                b,
                expected,
                actual,
                "asupersync",
            );
        }
    }

    #[test]
    fn test_gf256_log_exp_roundtrip_nonzero() {
        let o = Gf256Oracle::global();
        for a in 1_u8..=u8::MAX {
            let idx = usize::from(o.log(a));
            let got = o.exp(idx);
            if got != a {
                fail_mismatch("log_exp_roundtrip", a, 0, a, got, "oracle (exp[log[a]])");
            }
        }
    }

    #[test]
    fn test_gf256_exp_table_extended_range() {
        let o = Gf256Oracle::global();

        // Mirror property: EXP[k] == EXP[k + 255] for k in 0..255.
        for k in 0..255 {
            let a = o.exp(k);
            let b = o.exp(k + 255);
            assert_eq!(
                a, b,
                "bead_id={BEAD_ID} case=exp_mirror k={k} exp=0x{a:02X} exp_mirror=0x{b:02X}"
            );
        }

        // Verify max log sum is within the 512-entry extended table.
        let mut max = 0_usize;
        for a in 1_u8..=u8::MAX {
            for b in 1_u8..=u8::MAX {
                let idx = usize::from(o.log(a)) + usize::from(o.log(b));
                max = max.max(idx);
            }
        }
        assert!(
            max <= 508,
            "bead_id={BEAD_ID} case=max_log_sum expected<=508 got={max}"
        );
    }

    #[test]
    fn test_gf256_generator_order_255() {
        let o = Gf256Oracle::global();
        let g = GENERATOR;

        assert_eq!(
            o.pow(g, 255),
            1,
            "bead_id={BEAD_ID} case=generator_order pow(g,255)!=1"
        );
        for k in 1_u16..=254 {
            assert_ne!(
                o.pow(g, k),
                1,
                "bead_id={BEAD_ID} case=generator_order g^{k} unexpectedly 1"
            );
        }

        // Cross-check via asupersync's GF implementation.
        assert_eq!(
            Gf256::ALPHA.pow(255).0,
            1,
            "bead_id={BEAD_ID} case=asupersync_generator_order alpha^255!=1"
        );
    }

    #[test]
    fn test_gf256_inverse_property() {
        let o = Gf256Oracle::global();
        for a in 1_u8..=u8::MAX {
            let inv = o.inv(a);
            let expected = 1_u8;
            let actual = o.mul_logexp(a, inv);
            if actual != expected {
                fail_mismatch("inverse_property", a, inv, expected, actual, "oracle");
            }
            let actual_as = (Gf256(a) * Gf256(inv)).0;
            if actual_as != expected {
                fail_mismatch(
                    "inverse_property",
                    a,
                    inv,
                    expected,
                    actual_as,
                    "asupersync",
                );
            }
        }
    }

    #[test]
    fn test_gf256_division_identity() {
        let o = Gf256Oracle::global();
        for a in 0_u8..=u8::MAX {
            for b in 1_u8..=u8::MAX {
                let prod = o.mul_logexp(a, b);
                let got = o.div(prod, b);
                if got != a {
                    fail_mismatch("division_identity", a, b, a, got, "oracle");
                }
                let prod_as = (Gf256(a) * Gf256(b)).0;
                let got_as = (Gf256(prod_as) / Gf256(b)).0;
                if got_as != a {
                    fail_mismatch("division_identity", a, b, a, got_as, "asupersync");
                }
            }
        }
    }

    #[test]
    fn test_gf256_mul_tables_match_logexp() {
        let o = Gf256Oracle::global();
        for a in 0_u8..=u8::MAX {
            for b in 0_u8..=u8::MAX {
                let expected = o.mul_logexp(a, b);
                let actual = gf256_mul_const(a, b);
                if actual != expected {
                    fail_mismatch(
                        "mul_tables_match_logexp",
                        a,
                        b,
                        expected,
                        actual,
                        "oracle_const",
                    );
                }
                let actual_as = (Gf256(a) * Gf256(b)).0;
                if actual_as != expected {
                    fail_mismatch(
                        "mul_tables_match_logexp",
                        a,
                        b,
                        expected,
                        actual_as,
                        "asupersync_mul_field",
                    );
                }
            }
        }
    }

    #[test]
    fn test_gf256_mul_slice_tables_cover_all_pairs() {
        // Exercises asupersync's MUL_TABLES fast-path: gf256_mul_slice() uses a precomputed 256x256
        // table when dst.len() >= 64. We drive it with a 256-byte slice covering all octets.
        let o = Gf256Oracle::global();

        let base: Vec<u8> = (0_u8..=u8::MAX).collect();
        for c in 0_u8..=u8::MAX {
            let mut data = base.clone();
            gf256_mul_slice(&mut data, Gf256(c));
            for (&x, &got) in base.iter().zip(data.iter()) {
                let expected = o.mul_logexp(x, c);
                if got != expected {
                    fail_mismatch(
                        "mul_slice_tables",
                        x,
                        c,
                        expected,
                        got,
                        "asupersync_mul_slice",
                    );
                }
            }
        }
    }

    #[test]
    fn test_gf256_addmul_slice_tables_cover_all_pairs() {
        // Similar to mul_slice, but validate addmul slice multiply table usage.
        let o = Gf256Oracle::global();

        let src: Vec<u8> = (0_u8..=u8::MAX).collect();
        for c in 0_u8..=u8::MAX {
            let mut dst = vec![0_u8; src.len()];
            gf256_addmul_slice(&mut dst, &src, Gf256(c));
            for (&x, &got) in src.iter().zip(dst.iter()) {
                let expected = o.mul_logexp(x, c);
                if got != expected {
                    fail_mismatch(
                        "addmul_slice_tables",
                        x,
                        c,
                        expected,
                        got,
                        "asupersync_addmul_slice",
                    );
                }
            }
        }
    }

    #[test]
    fn test_gf256_distributive_over_add_exhaustive() {
        let o = Gf256Oracle::global();
        for a in 0_u8..=u8::MAX {
            for b in 0_u8..=u8::MAX {
                for c in 0_u8..=u8::MAX {
                    let lhs = o.mul_logexp(a, b ^ c);
                    let rhs = o.mul_logexp(a, b) ^ o.mul_logexp(a, c);
                    assert_eq!(
                        lhs, rhs,
                        "bead_id={BEAD_ID} case=distributive a=0x{a:02X} b=0x{b:02X} c=0x{c:02X} lhs=0x{lhs:02X} rhs=0x{rhs:02X}",
                    );
                }
            }
        }
    }

    #[test]
    fn test_gf256_irreducible_polynomial_has_no_small_factors() {
        // Verify POLY_FULL has no monic factors of degree 1..=4 over GF(2).
        // This is a simple (but complete) check for irreducibility for degree-8 polynomials.
        for deg in 1_u16..=4 {
            let max_tail = 1_u16 << deg;
            for tail in 0_u16..max_tail {
                let divisor = (1_u16 << deg) | tail;
                let rem = poly_mod(POLY_FULL, divisor);
                assert_ne!(
                    rem, 0,
                    "bead_id={BEAD_ID} case=irreducible factor_found deg={deg} divisor=0x{divisor:03X}"
                );
            }
        }
    }

    // --- proptest properties (seed must be surfaced by proptest on failure) ---

    proptest! {
        #[test]
        fn prop_gf256_mul_associative(a in 1_u8..=255, b in 1_u8..=255, c in 1_u8..=255) {
            let o = Gf256Oracle::global();
            let lhs = o.mul_logexp(o.mul_logexp(a, b), c);
            let rhs = o.mul_logexp(a, o.mul_logexp(b, c));
            prop_assert_eq!(
                lhs, rhs,
                "bead_id={} case=prop_associative a=0x{:02X} b=0x{:02X} c=0x{:02X}",
                BEAD_ID,
                a,
                b,
                c
            );
        }

        #[test]
        fn prop_gf256_distributive_over_add(a in any::<u8>(), b in any::<u8>(), c in any::<u8>()) {
            let o = Gf256Oracle::global();
            let lhs = o.mul_logexp(a, b ^ c);
            let rhs = o.mul_logexp(a, b) ^ o.mul_logexp(a, c);
            prop_assert_eq!(
                lhs, rhs,
                "bead_id={} case=prop_distributive a=0x{:02X} b=0x{:02X} c=0x{:02X}",
                BEAD_ID,
                a,
                b,
                c
            );
        }
    }

    // --- E2E: oracle-generated repair symbols + asupersync decode roundtrip ---

    #[test]
    fn test_e2e_raptorq_roundtrip_uses_gf256_tables() {
        let o = Gf256Oracle::global();
        let k = 8_usize;
        let symbol_size = 64_usize;
        let seed = 42_u64;

        // Patterned source data (stable, easy to debug).
        let source: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                (0..symbol_size)
                    .map(|j| {
                        let v = (i * 37 + j * 13 + 7) % 256;
                        u8::try_from(v).expect("patterned source byte")
                    })
                    .collect()
            })
            .collect();

        let encoder = SystematicEncoder::new(&source, symbol_size, seed).expect("encoder init");
        let decoder = InactivationDecoder::new(k, symbol_size, seed);
        let params = decoder.params();
        let l = params.l;
        let k_u32 = u32::try_from(k).expect("k is small and fits u32");
        let l_u32 = u32::try_from(l).expect("L is small in this test and fits u32");
        let base_rows = params.s + params.h;
        let constraints = ConstraintMatrix::build(params, seed);

        // Start with the LDPC/HDPC constraint symbols (zero RHS).
        let mut received: Vec<ReceivedSymbol> = decoder.constraint_symbols();

        // Add source symbols with their LT equations from the constraint matrix.
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
                esi: u32::try_from(i).expect("esi fits u32 for this test"),
                is_source: true,
                columns,
                coefficients,
                data: data.clone(),
            });
        }

        // Add enough repair symbols to reach L total.
        for esi in k_u32..l_u32 {
            let (cols, coefs) = decoder.repair_equation(esi);
            assert_eq!(
                cols.len(),
                coefs.len(),
                "bead_id={BEAD_ID} case=e2e_repair_equation_shape_mismatch esi={esi} cols_len={} coefs_len={}",
                cols.len(),
                coefs.len(),
            );

            // Build repair symbol bytes from the encoder's intermediate symbols using the oracle GF(256).
            let mut repair = vec![0_u8; symbol_size];
            for (col, coef) in cols.iter().copied().zip(coefs.iter().copied()) {
                let sym = encoder.intermediate_symbol(col);
                o.addmul_slice(&mut repair, sym, coef.0);
            }

            // Cross-check a few symbols against asupersync's own encoder output (sanity).
            if esi < k_u32 + 4 {
                let expected = encoder.repair_symbol(esi);
                assert_eq!(
                    repair, expected,
                    "bead_id={BEAD_ID} case=e2e_repair_symbol_mismatch esi={esi}"
                );
            }

            received.push(ReceivedSymbol::repair(esi, cols, coefs, repair));
        }

        let decode_outcome = decoder.decode(&received).expect("decode should succeed");
        assert_eq!(
            decode_outcome.source, source,
            "bead_id={BEAD_ID} case=e2e_roundtrip source mismatch"
        );
    }
}
