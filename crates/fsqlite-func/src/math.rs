//! SQLite 3.35+ math functions (§13.2).
//!
//! All 30 math functions: trig, hyperbolic, rounding, log/exp, and misc.
//! Always included in FrankenSQLite (no compile flag needed).
//!
//! # NaN / Inf semantics
//! - NaN results are normalized to NULL.
//! - +Inf / -Inf are valid REAL values and propagate.
//! - Division by zero (mod) returns NULL.
#![allow(
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::suboptimal_flops,
    clippy::unnecessary_wraps,
    clippy::match_same_arms,
    clippy::items_after_statements,
    clippy::float_cmp
)]

use fsqlite_error::Result;
use fsqlite_types::SqliteValue;

use crate::{FunctionRegistry, ScalarFunction};

// ── Helpers ───────────────────────────────────────────────────────────────

/// Coerce a `SqliteValue` to `f64`.  Returns `None` for `Null` and for
/// non-numeric text (SQLite math functions return NULL for these cases,
/// per `sqlite3_value_numeric_type()` semantics in C SQLite).
fn to_f64(v: &SqliteValue) -> Option<f64> {
    match v {
        SqliteValue::Null => None,
        SqliteValue::Integer(i) => Some(*i as f64),
        SqliteValue::Float(f) => Some(*f),
        // SQLite trims leading/trailing whitespace via sqlite3AtoF before
        // numeric conversion.  Non-numeric text produces NULL (not 0.0).
        SqliteValue::Text(s) => {
            let trimmed = s.trim();
            // Reject non-finite (NaN/Inf) — sqlite3AtoF doesn't recognize them.
            trimmed.parse::<f64>().ok().filter(|f| f.is_finite())
        }
        SqliteValue::Blob(_) => Some(0.0),
    }
}

/// Wrap an `f64` result: NaN → NULL, otherwise Float.
fn wrap(v: f64) -> SqliteValue {
    if v.is_nan() {
        SqliteValue::Null
    } else {
        SqliteValue::Float(v)
    }
}

/// One-arg math function that returns NULL on domain error (NaN).
fn unary_math(args: &[SqliteValue], f: fn(f64) -> f64) -> Result<SqliteValue> {
    let Some(x) = to_f64(&args[0]) else {
        return Ok(SqliteValue::Null);
    };
    Ok(wrap(f(x)))
}

/// One-arg math with explicit domain check.  Returns NULL if `domain` is false.
fn unary_domain(
    args: &[SqliteValue],
    domain: fn(f64) -> bool,
    f: fn(f64) -> f64,
) -> Result<SqliteValue> {
    let Some(x) = to_f64(&args[0]) else {
        return Ok(SqliteValue::Null);
    };
    if !domain(x) {
        return Ok(SqliteValue::Null);
    }
    Ok(wrap(f(x)))
}

// ── Trigonometric ─────────────────────────────────────────────────────────

pub struct AcosFunc;

impl ScalarFunction for AcosFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| (-1.0..=1.0).contains(&x), f64::acos)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "acos"
    }
}

pub struct AsinFunc;

impl ScalarFunction for AsinFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| (-1.0..=1.0).contains(&x), f64::asin)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "asin"
    }
}

pub struct AtanFunc;

impl ScalarFunction for AtanFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::atan)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "atan"
    }
}

pub struct Atan2Func;

impl ScalarFunction for Atan2Func {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(y) = to_f64(&args[0]) else {
            return Ok(SqliteValue::Null);
        };
        let Some(x) = to_f64(&args[1]) else {
            return Ok(SqliteValue::Null);
        };
        Ok(wrap(y.atan2(x)))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "atan2"
    }
}

pub struct CosFunc;

impl ScalarFunction for CosFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::cos)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "cos"
    }
}

pub struct SinFunc;

impl ScalarFunction for SinFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::sin)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sin"
    }
}

pub struct TanFunc;

impl ScalarFunction for TanFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::tan)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "tan"
    }
}

// ── Hyperbolic ────────────────────────────────────────────────────────────

pub struct AcoshFunc;

impl ScalarFunction for AcoshFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| x >= 1.0, f64::acosh)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "acosh"
    }
}

pub struct AsinhFunc;

impl ScalarFunction for AsinhFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::asinh)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "asinh"
    }
}

pub struct AtanhFunc;

impl ScalarFunction for AtanhFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // Domain: (-1, 1) — open interval, atanh(1) and atanh(-1) are ±Inf
        // but C sqlite returns NULL for these edge cases.
        unary_domain(args, |x| x > -1.0 && x < 1.0, f64::atanh)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "atanh"
    }
}

pub struct CoshFunc;

impl ScalarFunction for CoshFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::cosh)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "cosh"
    }
}

pub struct SinhFunc;

impl ScalarFunction for SinhFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::sinh)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sinh"
    }
}

pub struct TanhFunc;

impl ScalarFunction for TanhFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::tanh)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "tanh"
    }
}

// ── Rounding ──────────────────────────────────────────────────────────────
//
// ceil/floor/trunc preserve INTEGER type for INTEGER input.

pub struct CeilFunc;

impl ScalarFunction for CeilFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match &args[0] {
            SqliteValue::Null => Ok(SqliteValue::Null),
            SqliteValue::Integer(i) => Ok(SqliteValue::Integer(*i)),
            other => {
                let Some(x) = to_f64(other) else {
                    return Ok(SqliteValue::Null);
                };
                Ok(wrap(x.ceil()))
            }
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "ceil"
    }
}

pub struct FloorFunc;

impl ScalarFunction for FloorFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match &args[0] {
            SqliteValue::Null => Ok(SqliteValue::Null),
            SqliteValue::Integer(i) => Ok(SqliteValue::Integer(*i)),
            other => {
                let Some(x) = to_f64(other) else {
                    return Ok(SqliteValue::Null);
                };
                Ok(wrap(x.floor()))
            }
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "floor"
    }
}

pub struct TruncFunc;

impl ScalarFunction for TruncFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match &args[0] {
            SqliteValue::Null => Ok(SqliteValue::Null),
            SqliteValue::Integer(i) => Ok(SqliteValue::Integer(*i)),
            other => {
                let Some(x) = to_f64(other) else {
                    return Ok(SqliteValue::Null);
                };
                Ok(wrap(x.trunc()))
            }
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "trunc"
    }
}

// ── Logarithmic / Exponential ─────────────────────────────────────────────

pub struct LnFunc;

impl ScalarFunction for LnFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| x > 0.0, f64::ln)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "ln"
    }
}

/// `log(X)` — base-10 logarithm (single arg).
pub struct Log10Func;

impl ScalarFunction for Log10Func {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| x > 0.0, f64::log10)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "log10"
    }
}

/// `log(X)` with 1 arg = base-10.  `log(B, X)` with 2 args = base-B.
pub struct LogFunc;

impl ScalarFunction for LogFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() == 1 {
            // log(X) = log10(X)
            return Log10Func.invoke(args);
        }
        // log(B, X) = ln(X) / ln(B)
        let Some(b) = to_f64(&args[0]) else {
            return Ok(SqliteValue::Null);
        };
        let Some(x) = to_f64(&args[1]) else {
            return Ok(SqliteValue::Null);
        };
        if b <= 0.0 || b == 1.0 || x <= 0.0 {
            return Ok(SqliteValue::Null);
        }
        Ok(wrap(x.ln() / b.ln()))
    }

    fn num_args(&self) -> i32 {
        -1 // variadic: 1 or 2 args
    }

    fn name(&self) -> &str {
        "log"
    }
}

pub struct Log2Func;

impl ScalarFunction for Log2Func {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| x > 0.0, f64::log2)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "log2"
    }
}

pub struct ExpFunc;

impl ScalarFunction for ExpFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // exp overflow produces +Inf (valid REAL), so no domain check needed.
        unary_math(args, f64::exp)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "exp"
    }
}

pub struct PowFunc;

impl ScalarFunction for PowFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(x) = to_f64(&args[0]) else {
            return Ok(SqliteValue::Null);
        };
        let Some(y) = to_f64(&args[1]) else {
            return Ok(SqliteValue::Null);
        };
        Ok(wrap(x.powf(y)))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "pow"
    }
}

pub struct SqrtFunc;

impl ScalarFunction for SqrtFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_domain(args, |x| x >= 0.0, f64::sqrt)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sqrt"
    }
}

// ── Other ─────────────────────────────────────────────────────────────────

pub struct DegreesFunc;

impl ScalarFunction for DegreesFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::to_degrees)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "degrees"
    }
}

pub struct RadiansFunc;

impl ScalarFunction for RadiansFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        unary_math(args, f64::to_radians)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "radians"
    }
}

pub struct ModFunc;

impl ScalarFunction for ModFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(x) = to_f64(&args[0]) else {
            return Ok(SqliteValue::Null);
        };
        let Some(y) = to_f64(&args[1]) else {
            return Ok(SqliteValue::Null);
        };
        if y == 0.0 {
            return Ok(SqliteValue::Null);
        }
        Ok(wrap(x % y))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "mod"
    }
}

pub struct PiFunc;

impl ScalarFunction for PiFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Float(std::f64::consts::PI))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "pi"
    }
}

// ── Registration ──────────────────────────────────────────────────────────

/// Register all §13.2 math functions into the given registry.
pub fn register_math_builtins(registry: &mut FunctionRegistry) {
    // Trigonometric
    registry.register_scalar(AcosFunc);
    registry.register_scalar(AsinFunc);
    registry.register_scalar(AtanFunc);
    registry.register_scalar(Atan2Func);
    registry.register_scalar(CosFunc);
    registry.register_scalar(SinFunc);
    registry.register_scalar(TanFunc);

    // Hyperbolic
    registry.register_scalar(AcoshFunc);
    registry.register_scalar(AsinhFunc);
    registry.register_scalar(AtanhFunc);
    registry.register_scalar(CoshFunc);
    registry.register_scalar(SinhFunc);
    registry.register_scalar(TanhFunc);

    // Rounding
    registry.register_scalar(CeilFunc);
    registry.register_scalar(FloorFunc);
    registry.register_scalar(TruncFunc);

    // Logarithmic / Exponential
    registry.register_scalar(LnFunc);
    registry.register_scalar(LogFunc);
    registry.register_scalar(Log10Func);
    registry.register_scalar(Log2Func);
    registry.register_scalar(ExpFunc);
    registry.register_scalar(PowFunc);
    registry.register_scalar(SqrtFunc);

    // Other
    registry.register_scalar(DegreesFunc);
    registry.register_scalar(RadiansFunc);
    registry.register_scalar(ModFunc);
    registry.register_scalar(PiFunc);

    // Aliases
    // "ceiling" → same as "ceil"
    struct CeilingFunc;
    impl ScalarFunction for CeilingFunc {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            CeilFunc.invoke(args)
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "ceiling"
        }
    }
    registry.register_scalar(CeilingFunc);

    // "power" → same as "pow"
    struct PowerFunc;
    impl ScalarFunction for PowerFunc {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            PowFunc.invoke(args)
        }

        fn num_args(&self) -> i32 {
            2
        }

        fn name(&self) -> &str {
            "power"
        }
    }
    registry.register_scalar(PowerFunc);
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Tolerance for floating-point comparisons.
    const EPS: f64 = 1e-12;

    fn assert_float_eq(result: &SqliteValue, expected: f64) {
        match result {
            SqliteValue::Float(v) => {
                assert!((v - expected).abs() < EPS, "expected {expected}, got {v}");
            }
            other => panic!("expected Float({expected}), got {other:?}"),
        }
    }

    fn assert_null(result: &SqliteValue) {
        assert_eq!(result, &SqliteValue::Null, "expected NULL");
    }

    fn float(v: f64) -> SqliteValue {
        SqliteValue::Float(v)
    }

    fn int(v: i64) -> SqliteValue {
        SqliteValue::Integer(v)
    }

    fn null() -> SqliteValue {
        SqliteValue::Null
    }

    // ── Trigonometric ─────────────────────────────────────────────────

    #[test]
    fn test_acos_valid() {
        let r = AcosFunc.invoke(&[float(0.5)]).unwrap();
        assert_float_eq(&r, 0.5_f64.acos());
    }

    #[test]
    fn test_acos_domain_error() {
        assert_null(&AcosFunc.invoke(&[float(2.0)]).unwrap());
    }

    #[test]
    fn test_acos_null() {
        assert_null(&AcosFunc.invoke(&[null()]).unwrap());
    }

    #[test]
    fn test_acosh_valid() {
        let r = AcoshFunc.invoke(&[float(2.0)]).unwrap();
        assert_float_eq(&r, 2.0_f64.acosh());
    }

    #[test]
    fn test_acosh_domain_error() {
        assert_null(&AcoshFunc.invoke(&[float(0.5)]).unwrap());
    }

    #[test]
    fn test_asin_valid() {
        let r = AsinFunc.invoke(&[float(0.5)]).unwrap();
        assert_float_eq(&r, 0.5_f64.asin());
    }

    #[test]
    fn test_asin_domain_error() {
        assert_null(&AsinFunc.invoke(&[float(2.0)]).unwrap());
    }

    #[test]
    fn test_asinh_all_reals() {
        let r = AsinhFunc.invoke(&[float(1.0)]).unwrap();
        assert_float_eq(&r, 1.0_f64.asinh());
    }

    #[test]
    fn test_atan_basic() {
        let r = AtanFunc.invoke(&[float(1.0)]).unwrap();
        assert_float_eq(&r, std::f64::consts::FRAC_PI_4);
    }

    #[test]
    fn test_atan2_quadrants() {
        // Q1: atan2(1, 1) ≈ π/4
        let r = Atan2Func.invoke(&[float(1.0), float(1.0)]).unwrap();
        assert_float_eq(&r, std::f64::consts::FRAC_PI_4);

        // Q2: atan2(1, -1) ≈ 3π/4
        let r = Atan2Func.invoke(&[float(1.0), float(-1.0)]).unwrap();
        assert_float_eq(&r, 3.0 * std::f64::consts::FRAC_PI_4);

        // Q3: atan2(-1, -1) ≈ -3π/4
        let r = Atan2Func.invoke(&[float(-1.0), float(-1.0)]).unwrap();
        assert_float_eq(&r, -3.0 * std::f64::consts::FRAC_PI_4);

        // Q4: atan2(-1, 1) ≈ -π/4
        let r = Atan2Func.invoke(&[float(-1.0), float(1.0)]).unwrap();
        assert_float_eq(&r, -std::f64::consts::FRAC_PI_4);
    }

    #[test]
    fn test_atanh_valid() {
        let r = AtanhFunc.invoke(&[float(0.5)]).unwrap();
        assert_float_eq(&r, 0.5_f64.atanh());
    }

    #[test]
    fn test_atanh_domain_error() {
        // atanh(1.0) is outside the open interval (-1, 1)
        assert_null(&AtanhFunc.invoke(&[float(1.0)]).unwrap());
        assert_null(&AtanhFunc.invoke(&[float(-1.0)]).unwrap());
    }

    #[test]
    fn test_cos_zero() {
        let r = CosFunc.invoke(&[float(0.0)]).unwrap();
        assert_float_eq(&r, 1.0);
    }

    #[test]
    fn test_cosh_zero() {
        let r = CoshFunc.invoke(&[float(0.0)]).unwrap();
        assert_float_eq(&r, 1.0);
    }

    #[test]
    fn test_sin_zero() {
        let r = SinFunc.invoke(&[float(0.0)]).unwrap();
        assert_float_eq(&r, 0.0);
    }

    #[test]
    fn test_sinh_zero() {
        let r = SinhFunc.invoke(&[float(0.0)]).unwrap();
        assert_float_eq(&r, 0.0);
    }

    #[test]
    fn test_tan_zero() {
        let r = TanFunc.invoke(&[float(0.0)]).unwrap();
        assert_float_eq(&r, 0.0);
    }

    #[test]
    fn test_tanh_zero() {
        let r = TanhFunc.invoke(&[float(0.0)]).unwrap();
        assert_float_eq(&r, 0.0);
    }

    // ── Rounding ──────────────────────────────────────────────────────

    #[test]
    fn test_ceil_real() {
        let r = CeilFunc.invoke(&[float(1.2)]).unwrap();
        assert_float_eq(&r, 2.0);
    }

    #[test]
    fn test_ceil_integer_type() {
        let r = CeilFunc.invoke(&[int(5)]).unwrap();
        assert_eq!(r, SqliteValue::Integer(5));
    }

    #[test]
    fn test_ceil_negative() {
        let r = CeilFunc.invoke(&[float(-1.2)]).unwrap();
        assert_float_eq(&r, -1.0);
    }

    #[test]
    fn test_floor_real() {
        let r = FloorFunc.invoke(&[float(1.7)]).unwrap();
        assert_float_eq(&r, 1.0);
    }

    #[test]
    fn test_floor_integer_type() {
        let r = FloorFunc.invoke(&[int(5)]).unwrap();
        assert_eq!(r, SqliteValue::Integer(5));
    }

    #[test]
    fn test_floor_negative() {
        let r = FloorFunc.invoke(&[float(-1.2)]).unwrap();
        assert_float_eq(&r, -2.0);
    }

    #[test]
    fn test_trunc_positive() {
        let r = TruncFunc.invoke(&[float(2.9)]).unwrap();
        assert_float_eq(&r, 2.0);
    }

    #[test]
    fn test_trunc_negative() {
        let r = TruncFunc.invoke(&[float(-2.9)]).unwrap();
        assert_float_eq(&r, -2.0);
    }

    #[test]
    fn test_trunc_integer_type() {
        let r = TruncFunc.invoke(&[int(5)]).unwrap();
        assert_eq!(r, SqliteValue::Integer(5));
    }

    // ── Logarithmic / Exponential ─────────────────────────────────────

    #[test]
    fn test_ln_positive() {
        let r = LnFunc.invoke(&[float(std::f64::consts::E)]).unwrap();
        assert_float_eq(&r, 1.0);
    }

    #[test]
    fn test_ln_zero() {
        assert_null(&LnFunc.invoke(&[float(0.0)]).unwrap());
    }

    #[test]
    fn test_ln_negative() {
        assert_null(&LnFunc.invoke(&[float(-1.0)]).unwrap());
    }

    #[test]
    fn test_log_single_arg_base10() {
        let r = LogFunc.invoke(&[float(100.0)]).unwrap();
        assert_float_eq(&r, 2.0);
    }

    #[test]
    fn test_log_two_arg_base() {
        let r = LogFunc.invoke(&[float(2.0), float(8.0)]).unwrap();
        assert_float_eq(&r, 3.0);
    }

    #[test]
    fn test_log10_alias() {
        let r = Log10Func.invoke(&[float(1000.0)]).unwrap();
        assert_float_eq(&r, 3.0);
    }

    #[test]
    fn test_log2_basic() {
        let r = Log2Func.invoke(&[float(8.0)]).unwrap();
        assert_float_eq(&r, 3.0);
    }

    #[test]
    fn test_exp_one() {
        let r = ExpFunc.invoke(&[float(1.0)]).unwrap();
        assert_float_eq(&r, std::f64::consts::E);
    }

    #[test]
    fn test_exp_overflow() {
        let r = ExpFunc.invoke(&[float(1000.0)]).unwrap();
        match r {
            SqliteValue::Float(v) => assert!(v.is_infinite() && v > 0.0, "+Inf expected"),
            other => panic!("expected +Inf Float, got {other:?}"),
        }
    }

    #[test]
    fn test_pow_basic() {
        let r = PowFunc.invoke(&[float(2.0), float(10.0)]).unwrap();
        assert_float_eq(&r, 1024.0);
    }

    #[test]
    fn test_pow_zero_zero() {
        let r = PowFunc.invoke(&[float(0.0), float(0.0)]).unwrap();
        assert_float_eq(&r, 1.0);
    }

    #[test]
    fn test_power_alias() {
        // power is registered via register_math_builtins as an alias.
        // Test the underlying PowFunc directly.
        let r = PowFunc.invoke(&[float(3.0), float(2.0)]).unwrap();
        assert_float_eq(&r, 9.0);
    }

    #[test]
    fn test_sqrt_positive() {
        let r = SqrtFunc.invoke(&[float(144.0)]).unwrap();
        assert_float_eq(&r, 12.0);
    }

    #[test]
    fn test_sqrt_negative() {
        assert_null(&SqrtFunc.invoke(&[float(-1.0)]).unwrap());
    }

    // ── Other ─────────────────────────────────────────────────────────

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_degrees_pi() {
        let r = DegreesFunc.invoke(&[float(std::f64::consts::PI)]).unwrap();
        assert_float_eq(&r, 180.0);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_radians_180() {
        let r = RadiansFunc.invoke(&[float(180.0)]).unwrap();
        assert_float_eq(&r, std::f64::consts::PI);
    }

    #[test]
    fn test_mod_basic() {
        let r = ModFunc.invoke(&[float(10.0), float(3.0)]).unwrap();
        assert_float_eq(&r, 1.0);
    }

    #[test]
    fn test_mod_zero_divisor() {
        assert_null(&ModFunc.invoke(&[float(10.0), float(0.0)]).unwrap());
    }

    #[test]
    fn test_pi_precision() {
        let r = PiFunc.invoke(&[]).unwrap();
        assert_float_eq(&r, std::f64::consts::PI);
    }

    // ── NaN / Inf ─────────────────────────────────────────────────────

    #[test]
    fn test_nan_normalized_to_null() {
        // Any operation producing NaN must return NULL.
        // sqrt(-1) → NULL via domain check.
        assert_null(&SqrtFunc.invoke(&[float(-1.0)]).unwrap());
        // acos(2) → NULL via domain check.
        assert_null(&AcosFunc.invoke(&[float(2.0)]).unwrap());
    }

    #[test]
    fn test_inf_propagation() {
        // exp(1000) = +Inf (valid REAL)
        let r = ExpFunc.invoke(&[float(1000.0)]).unwrap();
        match r {
            SqliteValue::Float(v) => assert!(v.is_infinite()),
            other => panic!("expected Inf, got {other:?}"),
        }
    }

    #[test]
    fn test_neg_inf_propagation() {
        // exp(-1000) is tiny but not -Inf.
        // -exp(1000) would be -Inf, but we test sinh(large) which overflows.
        let r = SinhFunc.invoke(&[float(1000.0)]).unwrap();
        match r {
            SqliteValue::Float(v) => assert!(v.is_infinite() && v > 0.0),
            other => panic!("expected +Inf, got {other:?}"),
        }
        let r = SinhFunc.invoke(&[float(-1000.0)]).unwrap();
        match r {
            SqliteValue::Float(v) => assert!(v.is_infinite() && v < 0.0),
            other => panic!("expected -Inf, got {other:?}"),
        }
    }

    #[test]
    fn test_ceiling_alias() {
        // "ceiling" is registered as alias for "ceil" via register_math_builtins.
        // Here we test functionally: same result.
        let r1 = CeilFunc.invoke(&[float(1.2)]).unwrap();
        // CeilingFunc is defined inside register_math_builtins, so we test
        // via the registry instead.
        let mut reg = FunctionRegistry::new();
        register_math_builtins(&mut reg);
        let ceiling = reg.find_scalar("ceiling", 1).expect("ceiling registered");
        let r2 = ceiling.invoke(&[float(1.2)]).unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_all_null_input() {
        // All math functions return NULL for NULL input.
        let n = &[null()];
        assert_null(&AcosFunc.invoke(n).unwrap());
        assert_null(&AsinFunc.invoke(n).unwrap());
        assert_null(&AtanFunc.invoke(n).unwrap());
        assert_null(&CosFunc.invoke(n).unwrap());
        assert_null(&SinFunc.invoke(n).unwrap());
        assert_null(&TanFunc.invoke(n).unwrap());
        assert_null(&AcoshFunc.invoke(n).unwrap());
        assert_null(&AsinhFunc.invoke(n).unwrap());
        assert_null(&AtanhFunc.invoke(n).unwrap());
        assert_null(&CoshFunc.invoke(n).unwrap());
        assert_null(&SinhFunc.invoke(n).unwrap());
        assert_null(&TanhFunc.invoke(n).unwrap());
        assert_null(&CeilFunc.invoke(n).unwrap());
        assert_null(&FloorFunc.invoke(n).unwrap());
        assert_null(&TruncFunc.invoke(n).unwrap());
        assert_null(&LnFunc.invoke(n).unwrap());
        assert_null(&Log10Func.invoke(n).unwrap());
        assert_null(&Log2Func.invoke(n).unwrap());
        assert_null(&ExpFunc.invoke(n).unwrap());
        assert_null(&SqrtFunc.invoke(n).unwrap());
        assert_null(&DegreesFunc.invoke(n).unwrap());
        assert_null(&RadiansFunc.invoke(n).unwrap());
        // Two-arg functions with NULL
        assert_null(&Atan2Func.invoke(&[null(), float(1.0)]).unwrap());
        assert_null(&Atan2Func.invoke(&[float(1.0), null()]).unwrap());
        assert_null(&PowFunc.invoke(&[null(), float(1.0)]).unwrap());
        assert_null(&ModFunc.invoke(&[null(), float(1.0)]).unwrap());
        // log with NULL
        assert_null(&LogFunc.invoke(&[null()]).unwrap());
        assert_null(&LogFunc.invoke(&[null(), float(8.0)]).unwrap());
        assert_null(&LogFunc.invoke(&[float(2.0), null()]).unwrap());
    }

    #[test]
    fn test_register_math_builtins_all_present() {
        let mut reg = FunctionRegistry::new();
        register_math_builtins(&mut reg);

        let expected = [
            ("acos", 1),
            ("asin", 1),
            ("atan", 1),
            ("atan2", 2),
            ("cos", 1),
            ("sin", 1),
            ("tan", 1),
            ("acosh", 1),
            ("asinh", 1),
            ("atanh", 1),
            ("cosh", 1),
            ("sinh", 1),
            ("tanh", 1),
            ("ceil", 1),
            ("ceiling", 1),
            ("floor", 1),
            ("trunc", 1),
            ("ln", 1),
            ("log10", 1),
            ("log2", 1),
            ("exp", 1),
            ("pow", 2),
            ("power", 2),
            ("sqrt", 1),
            ("degrees", 1),
            ("radians", 1),
            ("mod", 2),
            ("pi", 0),
        ];

        for (name, arity) in expected {
            assert!(
                reg.find_scalar(name, arity).is_some(),
                "math function '{name}/{arity}' not registered"
            );
        }

        // log is variadic (-1), test lookup with 1 and 2 args
        assert!(reg.find_scalar("log", 1).is_some(), "log/1 via variadic");
        assert!(reg.find_scalar("log", 2).is_some(), "log/2 via variadic");
    }

    #[test]
    fn test_e2e_registry_invoke_math() {
        let mut reg = FunctionRegistry::new();
        register_math_builtins(&mut reg);

        // pi() through registry
        let pi = reg.find_scalar("pi", 0).unwrap();
        let r = pi.invoke(&[]).unwrap();
        assert_float_eq(&r, std::f64::consts::PI);

        // sqrt(144) through registry
        let sqrt = reg.find_scalar("sqrt", 1).unwrap();
        let r = sqrt.invoke(&[float(144.0)]).unwrap();
        assert_float_eq(&r, 12.0);

        // log(2, 8) = 3.0 through registry
        let log = reg.find_scalar("log", 2).unwrap();
        let r = log.invoke(&[float(2.0), float(8.0)]).unwrap();
        assert_float_eq(&r, 3.0);
    }
}
