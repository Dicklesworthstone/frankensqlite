//! GF(256) arithmetic verification suite (RFC 6330) against asupersync.
//!
//! Bead: bd-1hi.1

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};

use asupersync::raptorq::gf256::{Gf256, gf256_add_slice, gf256_addmul_slice, gf256_mul_slice};
use asupersync::raptorq::{RaptorQReceiverBuilder, RaptorQSenderBuilder};
use asupersync::runtime::{Runtime, RuntimeBuilder};
use asupersync::transport::error::{SinkError, StreamError};
use asupersync::transport::sink::SymbolSink;
use asupersync::transport::stream::SymbolStream;
use asupersync::types::{ObjectId, ObjectParams};
use asupersync::{Cx, RaptorQConfig};
use proptest::prelude::*;

const BEAD_ID: &str = "bd-1hi.1";

fn test_cx() -> Cx {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        RuntimeBuilder::current_thread()
            .build()
            .expect("test runtime should build")
    });
    runtime.block_on(async { Cx::current().expect("runtime block_on should install Cx") })
}

// RFC 6330 §5.7: p(x) = x^8 + x^4 + x^3 + x^2 + 1 = 0x11D.
const POLY_FULL: u16 = 0x11D;
// Reduction mask (low 8 bits after subtracting x^8), used in xtime-style multiply.
const POLY_REDUCTION: u8 = 0x1D;

#[track_caller]
fn fail_u8_case(case: &'static str, a: u8, b: u8, expected: u8, actual: u8, source: &'static str) {
    assert_eq!(
        actual, expected,
        "bead_id={BEAD_ID} case={case} a=0x{a:02X} b=0x{b:02X} source={source}",
    );
}

fn gf256_mul_const(mut a: u8, mut b: u8) -> u8 {
    let mut acc = 0_u8;
    for _ in 0..8 {
        if (b & 1) != 0 {
            acc ^= a;
        }
        let hi = a & 0x80;
        a = a.wrapping_shl(1);
        if hi != 0 {
            a ^= POLY_REDUCTION;
        }
        b >>= 1;
    }
    acc
}

fn build_exp_table() -> [u8; 512] {
    let mut table = [0_u8; 512];
    let mut val: u16 = 1;
    for i in 0_usize..255 {
        let v = u8::try_from(val).expect("GF(256) element fits in u8");
        table[i] = v;
        table[i + 255] = v; // mirror for mod-free lookup
        val <<= 1;
        if (val & 0x100) != 0 {
            val ^= 0x100 | u16::from(POLY_REDUCTION);
        }
    }
    // Wrap slots that are commonly used in mod-free schemes.
    table[255] = 1;
    table[510] = 1;
    table
}

fn build_log_table(exp: &[u8; 512]) -> [u8; 256] {
    let mut log = [0_u8; 256];
    for (i, &v) in exp.iter().enumerate().take(255) {
        log[usize::from(v)] = u8::try_from(i).expect("i < 255");
    }
    log
}

fn mul_logexp(a: u8, b: u8, log: &[u8; 256], exp: &[u8; 512]) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let idx = usize::from(log[usize::from(a)]) + usize::from(log[usize::from(b)]);
    exp[idx]
}

fn poly_deg(p: u16) -> i32 {
    if p == 0 {
        return -1;
    }
    let lz = p.leading_zeros();
    i32::try_from(u16::BITS - 1 - lz).expect("fits in i32")
}

fn poly_mod(mut dividend: u16, divisor: u16) -> u16 {
    let divisor_deg = poly_deg(divisor);
    assert!(divisor_deg >= 0, "divisor must be non-zero");
    while dividend != 0 && poly_deg(dividend) >= divisor_deg {
        let shift = poly_deg(dividend) - divisor_deg;
        let shift_u32 = u32::try_from(shift).expect("non-negative");
        dividend ^= divisor << shift_u32;
    }
    dividend
}

#[test]
fn test_gf256_add_is_xor() {
    for a in 0_u8..=u8::MAX {
        for b in 0_u8..=u8::MAX {
            let expected = a ^ b;
            let actual = (Gf256(a) + Gf256(b)).raw();
            if actual != expected {
                fail_u8_case("add_is_xor", a, b, expected, actual, "asupersync");
            }
        }
    }
}

#[test]
fn test_gf256_sub_is_xor() {
    for a in 0_u8..=u8::MAX {
        for b in 0_u8..=u8::MAX {
            let expected = a ^ b;
            let actual = (Gf256(a) - Gf256(b)).raw();
            if actual != expected {
                fail_u8_case("sub_is_xor", a, b, expected, actual, "asupersync");
            }
        }
    }
}

#[test]
fn test_gf256_mul_zero_annihilator() {
    for a in 0_u8..=u8::MAX {
        let fa = Gf256(a);
        assert_eq!((fa * Gf256::ZERO).raw(), 0);
        assert_eq!((Gf256::ZERO * fa).raw(), 0);
    }
}

#[test]
fn test_gf256_mul_worked_example_a3_47_e1() {
    let a = 0xA3_u8;
    let b = 0x47_u8;
    let expected = 0xE1_u8;
    let actual = (Gf256(a) * Gf256(b)).raw();
    assert_eq!(actual, expected);
}

#[test]
fn test_gf256_log_exp_roundtrip_nonzero() {
    let exp = build_exp_table();
    let log = build_log_table(&exp);

    for a in 1_u8..=u8::MAX {
        let got = exp[usize::from(log[usize::from(a)])];
        assert_eq!(got, a, "roundtrip failed for a=0x{a:02x}");
    }

    // RFC worked example intermediate values (normative).
    assert_eq!(log[0xA3], 91);
    assert_eq!(log[0x47], 253);
    assert_eq!(exp[89], 0xE1);
}

#[test]
fn test_gf256_exp_table_extended_range() {
    let exp = build_exp_table();
    for i in 0_usize..=255 {
        assert_eq!(exp[i], exp[i + 255], "mirror mismatch at i={i}");
    }
}

#[test]
fn test_gf256_generator_order_255() {
    let alpha = Gf256::ALPHA;
    let mut acc = Gf256::ONE;
    for k in 1_u16..=255 {
        acc *= alpha;
        if k < 255 {
            assert_ne!(acc, Gf256::ONE, "generator has smaller order at k={k}");
        } else {
            assert_eq!(acc, Gf256::ONE, "generator order should be 255");
        }
    }
}

#[test]
fn test_gf256_inverse_property() {
    for a in 1_u8..=u8::MAX {
        let fa = Gf256(a);
        let inv = fa.inv();
        assert_eq!((fa * inv).raw(), 1, "a=0x{a:02x}");
    }
}

#[test]
fn test_gf256_division_identity() {
    for a in 0_u8..=u8::MAX {
        for b in 1_u8..=u8::MAX {
            let fa = Gf256(a);
            let fb = Gf256(b);
            let got = ((fa * fb) / fb).raw();
            if got != a {
                fail_u8_case("division_identity", a, b, a, got, "asupersync");
            }
        }
    }
}

#[test]
fn test_gf256_mul_tables_match_logexp() {
    let exp = build_exp_table();
    let log = build_log_table(&exp);

    for a in 0_u8..=u8::MAX {
        for b in 0_u8..=u8::MAX {
            let expected = mul_logexp(a, b, &log, &exp);
            let actual = (Gf256(a) * Gf256(b)).raw();
            if actual != expected {
                fail_u8_case("mul_match_logexp", a, b, expected, actual, "asupersync");
            }
        }
    }
}

#[test]
fn test_gf256_mul_tables_match_mul_const() {
    for a in 0_u8..=u8::MAX {
        for b in 0_u8..=u8::MAX {
            let expected = gf256_mul_const(a, b);
            let actual = (Gf256(a) * Gf256(b)).raw();
            if actual != expected {
                fail_u8_case("mul_match_mul_const", a, b, expected, actual, "asupersync");
            }
        }
    }
}

#[test]
fn test_gf256_mul_const_matches_logexp() {
    let exp = build_exp_table();
    let log = build_log_table(&exp);

    for a in 0_u8..=u8::MAX {
        for b in 0_u8..=u8::MAX {
            let expected = mul_logexp(a, b, &log, &exp);
            let actual = gf256_mul_const(a, b);
            if actual != expected {
                fail_u8_case(
                    "mul_const_match_logexp",
                    a,
                    b,
                    expected,
                    actual,
                    "test_only",
                );
            }
        }
    }
}

#[test]
fn test_gf256_poly_0x11d_irreducible_over_gf2() {
    // If an 8th-degree polynomial is reducible over GF(2), it has a factor of degree <= 4.
    for degree in 1_u32..=4 {
        let start = 1_u16 << degree; // monic polynomial of exact degree
        let end = 1_u16 << (degree + 1);
        for f in start..end {
            let rem = poly_mod(POLY_FULL, f);
            assert_ne!(
                rem, 0,
                "bead_id={BEAD_ID} case=poly_irreducible degree={degree} factor=0b{f:b}"
            );
        }
    }
}

#[test]
fn test_gf256_bulk_slice_ops_match_scalar() {
    // Small slice path (log/exp) and large slice path (mul tables) should match scalar math.
    let mut small = (0_u8..32).collect::<Vec<u8>>();
    let mut small_expected = small.clone();
    let c = Gf256(0xB7);
    gf256_mul_slice(&mut small, c);
    for x in &mut small_expected {
        *x = (Gf256(*x) * c).raw();
    }
    assert_eq!(small, small_expected);

    let mut large = (0_u8..=u8::MAX).collect::<Vec<u8>>();
    let mut large_expected = large.clone();
    gf256_mul_slice(&mut large, c);
    for x in &mut large_expected {
        *x = (Gf256(*x) * c).raw();
    }
    assert_eq!(large, large_expected);

    let mut dst = vec![0_u8; 256];
    let src = (0_u8..=u8::MAX).collect::<Vec<u8>>();
    let mut dst_addmul_expected = dst.clone();
    gf256_addmul_slice(&mut dst, &src, c);
    for (d, s) in dst_addmul_expected.iter_mut().zip(src.iter()) {
        *d ^= (Gf256(*s) * c).raw();
    }
    assert_eq!(dst, dst_addmul_expected);

    let mut dst_add = vec![0_u8; 256];
    let mut dst_add_expected = dst_add.clone();
    gf256_add_slice(&mut dst_add, &src);
    for (d, s) in dst_add_expected.iter_mut().zip(src.iter()) {
        *d ^= *s;
    }
    assert_eq!(dst_add, dst_add_expected);
}

proptest! {
    #[test]
    fn prop_gf256_mul_associative(a in any::<u8>(), b in any::<u8>(), c in any::<u8>()) {
        let fa = Gf256(a);
        let fb = Gf256(b);
        let fc = Gf256(c);
        prop_assert_eq!((fa * fb) * fc, fa * (fb * fc));
    }

    #[test]
    fn prop_gf256_distributive_over_add(a in any::<u8>(), b in any::<u8>(), c in any::<u8>()) {
        let fa = Gf256(a);
        let fb = Gf256(b);
        let fc = Gf256(c);
        prop_assert_eq!(fa * (fb + fc), (fa * fb) + (fa * fc));
    }
}

#[derive(Debug)]
struct VecSink {
    symbols: Vec<asupersync::types::Symbol>,
}

impl VecSink {
    fn new() -> Self {
        Self {
            symbols: Vec::new(),
        }
    }
}

impl SymbolSink for VecSink {
    fn poll_send(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        symbol: asupersync::security::authenticated::AuthenticatedSymbol,
    ) -> Poll<Result<(), SinkError>> {
        self.symbols.push(symbol.into_symbol());
        Poll::Ready(Ok(()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct VecStream {
    q: VecDeque<asupersync::security::authenticated::AuthenticatedSymbol>,
}

impl VecStream {
    fn new(symbols: Vec<asupersync::types::Symbol>) -> Self {
        let q = symbols
            .into_iter()
            .map(|s| {
                asupersync::security::authenticated::AuthenticatedSymbol::from_parts(
                    s,
                    asupersync::security::AuthenticationTag::zero(),
                )
            })
            .collect();
        Self { q }
    }
}

impl SymbolStream for VecStream {
    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<asupersync::security::authenticated::AuthenticatedSymbol, StreamError>>>
    {
        match self.q.pop_front() {
            Some(sym) => Poll::Ready(Some(Ok(sym))),
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.q.len(), Some(self.q.len()))
    }

    fn is_exhausted(&self) -> bool {
        self.q.is_empty()
    }
}

#[test]
fn test_e2e_raptorq_roundtrip_uses_gf256_tables() {
    let cx = test_cx();

    let mut config = RaptorQConfig::default();
    // Keep the object in a single block for determinism.
    config.encoding.max_block_size = 64 * 1024;
    config.encoding.repair_overhead = 1.10;

    let object_id = ObjectId::new_for_test(1);
    let mut data = Vec::with_capacity(10_000);
    for i in 0_u32..10_000 {
        let b = u8::try_from(i.wrapping_mul(31) % 256).expect("mod 256 fits u8");
        data.push(b ^ 0xA5);
    }

    let mut sender = RaptorQSenderBuilder::new()
        .config(config.clone())
        .transport(VecSink::new())
        .build()
        .expect("sender build");
    let _outcome = sender
        .send_object(&cx, object_id, &data)
        .expect("send_object");

    let symbols = std::mem::take(&mut sender.transport_mut().symbols);
    assert!(
        !symbols.is_empty(),
        "bead_id={BEAD_ID} case=raptorq_no_symbols"
    );

    let k = data
        .len()
        .div_ceil(usize::from(config.encoding.symbol_size));
    let params = ObjectParams::new(
        object_id,
        u64::try_from(data.len()).expect("len fits u64"),
        config.encoding.symbol_size,
        1,
        u16::try_from(k).expect("k fits u16"),
    );

    let mut receiver = RaptorQReceiverBuilder::new()
        .config(config)
        .source(VecStream::new(symbols))
        .build()
        .expect("receiver build");
    let got = receiver
        .receive_object(&cx, &params)
        .expect("receive_object")
        .data;

    assert_eq!(got, data);
}
