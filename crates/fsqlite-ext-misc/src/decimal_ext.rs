//! Exact decimal input conversion and the additional decimal scalar functions.
//!
//! Arithmetic operands retain the extension's text-conversion semantics. Only
//! decimal() and decimal_exp() expand a REAL (or big-endian binary64 BLOB) into
//! its exact value. No floating-point arithmetic is used in that expansion.

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::FunctionRegistry;
use fsqlite_func::scalar::ScalarFunction;
use fsqlite_types::value::{SmallText, SqliteValue};

use super::{decimal_normalize, format_decimal};

// Bound exponent expansion before allocating. A short hostile input such as
// "1e9223372036854775807" must not request an unbounded allocation.
const MAX_DECIMAL_DIGITS: usize = 1_000_000;

pub fn parse_decimal(s: &str) -> Option<(bool, Vec<u8>, Vec<u8>)> {
    let s = s.trim();
    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s.strip_prefix('+').unwrap_or(s))
    };
    let (mantissa, exponent) = match s.find(['e', 'E']) {
        Some(index) => {
            let exponent = &s[index + 1..];
            let digits = exponent
                .strip_prefix('+')
                .or_else(|| exponent.strip_prefix('-'))
                .unwrap_or(exponent);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            (&s[..index], exponent.parse::<i64>().ok()?)
        }
        None => (s, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digit_count = integer.len().checked_add(fraction.len())?;
    if digit_count == 0
        || digit_count > MAX_DECIMAL_DIGITS
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let point = i64::try_from(integer.len()).ok()?.checked_add(exponent)?;
    let (leading, trailing) = if point < 0 {
        (usize::try_from(point.unsigned_abs()).ok()?, 0)
    } else {
        (0, usize::try_from(point).ok()?.saturating_sub(digit_count))
    };
    let expanded = digit_count.checked_add(leading)?.checked_add(trailing)?;
    if expanded > MAX_DECIMAL_DIGITS {
        return None;
    }
    let mut digits = Vec::with_capacity(expanded);
    digits.resize(leading, 0);
    digits.extend(integer.bytes().chain(fraction.bytes()).map(|b| b - b'0'));
    digits.resize(expanded, 0);
    let split = if point < 0 {
        0
    } else {
        usize::try_from(point).ok()?
    };
    let fraction = digits.split_off(split);
    if digits.is_empty() {
        digits.push(0);
    }
    Some((negative, digits, fraction))
}

// Little-endian base-10 digits permit carry growth at the end of the vector.
// Multiplying by a u32 uses u64 intermediates, so every step is exact.
fn multiply_small(digits: &mut Vec<u8>, multiplier: u32) {
    let mut carry = 0_u64;
    for digit in digits.iter_mut() {
        let product = u64::from(*digit) * u64::from(multiplier) + carry;
        *digit = u8::try_from(product % 10).expect("decimal digit");
        carry = product / 10;
    }
    while carry != 0 {
        digits.push(u8::try_from(carry % 10).expect("decimal digit"));
        carry /= 10;
    }
}

fn multiply_power(digits: &mut Vec<u8>, base: u32, mut exponent: u32) {
    // Both 2^12 and 5^12 fit u32. Chunking avoids a full digit traversal
    // for every bit, including at decimal_pow2's +/-20000 boundaries.
    while exponent >= 12 {
        multiply_small(digits, base.pow(12));
        exponent -= 12;
    }
    if exponent != 0 {
        multiply_small(digits, base.pow(exponent));
    }
}

fn scaled_digits(mut digits: Vec<u8>, negative: bool, scale: usize) -> String {
    if digits.len() < scale {
        digits.resize(scale, 0);
    }
    digits.reverse();
    let split = digits.len() - scale;
    format_decimal(negative, &digits[..split], &digits[split..])
}

fn exact_binary64(bits: u64) -> Option<String> {
    let exponent_bits = u16::try_from((bits >> 52) & 0x7ff).ok()?;
    if exponent_bits == 0x7ff {
        return None; // NaN and infinities have no finite decimal expansion.
    }
    let mut significand = bits & ((1_u64 << 52) - 1);
    let mut exponent = if exponent_bits == 0 {
        -1074_i32
    } else {
        significand |= 1_u64 << 52;
        i32::from(exponent_bits) - 1023 - 52
    };
    if significand == 0 {
        return Some("0".to_owned());
    }
    // Cancel powers of two before computing 5^(-exponent).
    while exponent < 0 && significand & 1 == 0 {
        significand >>= 1;
        exponent += 1;
    }
    let mut digits = significand
        .to_string()
        .bytes()
        .rev()
        .map(|b| b - b'0')
        .collect();
    let scale = if exponent < 0 {
        multiply_power(&mut digits, 5, exponent.unsigned_abs());
        usize::try_from(exponent.unsigned_abs()).ok()?
    } else {
        multiply_power(&mut digits, 2, exponent.unsigned_abs());
        0
    };
    Some(scaled_digits(digits, bits >> 63 != 0, scale))
}

pub fn value_to_decimal(value: &SqliteValue) -> Option<String> {
    match value {
        SqliteValue::Null => None,
        SqliteValue::Integer(value) => Some(value.to_string()),
        SqliteValue::Float(value) => exact_binary64(value.to_bits()),
        SqliteValue::Text(value) => decimal_normalize(value),
        SqliteValue::Blob(value) => {
            let bytes: [u8; 8] = value.as_ref().try_into().ok()?;
            exact_binary64(u64::from_be_bytes(bytes))
        }
    }
}

fn scientific(decimal: &str) -> Option<String> {
    let (negative, integer, fraction) = parse_decimal(decimal)?;
    let point = i64::try_from(integer.len()).ok()?;
    let digits = integer.into_iter().chain(fraction).collect::<Vec<_>>();
    let Some(first) = digits.iter().position(|&digit| digit != 0) else {
        return Some("+0.0e+00".to_owned());
    };
    let last = digits.iter().rposition(|&digit| digit != 0)?;
    let exponent = point - i64::try_from(first).ok()? - 1;
    let fraction = if first == last {
        "0".to_owned()
    } else {
        digits[first + 1..=last]
            .iter()
            .map(|&digit| char::from(b'0' + digit))
            .collect()
    };
    let sign = if negative { '-' } else { '+' };
    Some(format!("{sign}{}.{fraction}e{exponent:+03}", digits[first]))
}

struct DecimalExpFunc;

impl ScalarFunction for DecimalExpFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let [value] = args else {
            return Err(FrankenError::internal(
                "decimal_exp requires exactly 1 argument",
            ));
        };
        Ok(value_to_decimal(value)
            .and_then(|decimal| scientific(&decimal))
            .map_or(SqliteValue::Null, |text| {
                SqliteValue::Text(SmallText::from_string(text))
            }))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "decimal_exp"
    }
}

struct DecimalPow2Func;

impl ScalarFunction for DecimalPow2Func {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let [value] = args else {
            return Err(FrankenError::internal(
                "decimal_pow2 requires exactly 1 argument",
            ));
        };
        let SqliteValue::Integer(exponent) = value else {
            return Ok(SqliteValue::Null);
        };
        if !(-20_000..=20_000).contains(exponent) {
            return Ok(SqliteValue::Null);
        }
        let magnitude = u32::try_from(exponent.unsigned_abs()).expect("bounded exponent");
        let mut digits = vec![1];
        multiply_power(&mut digits, if *exponent < 0 { 5 } else { 2 }, magnitude);
        let scale = if *exponent < 0 {
            usize::try_from(magnitude).expect("bounded exponent")
        } else {
            0
        };
        let decimal = scaled_digits(digits, false, scale);
        Ok(scientific(&decimal).map_or(SqliteValue::Null, |text| {
            SqliteValue::Text(SmallText::from_string(text))
        }))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "decimal_pow2"
    }
}

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(DecimalExpFunc);
    registry.register_scalar(DecimalPow2Func);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DecimalAddFunc, DecimalCmpFunc, DecimalFunc, DecimalMulFunc, DecimalSubFunc};

    fn text(value: &str) -> SqliteValue {
        SqliteValue::Text(value.into())
    }

    #[test]
    fn scientific_literals_flow_through_every_decimal_operation() {
        for (input, expected) in [
            ("1.25e3", "1250"),
            ("-12.5E-2", "-0.125"),
            ("+.5e+1", "5"),
            ("12.e-1", "1.2"),
            ("-0e20", "0"),
        ] {
            assert_eq!(DecimalFunc.invoke(&[text(input)]).unwrap(), text(expected));
        }
        assert_eq!(
            DecimalAddFunc.invoke(&[text("1e30"), text("1")]).unwrap(),
            text("1000000000000000000000000000001")
        );
        assert_eq!(
            DecimalSubFunc
                .invoke(&[text("1e-3"), text("2e-3")])
                .unwrap(),
            text("-0.001")
        );
        assert_eq!(
            DecimalMulFunc
                .invoke(&[text("12.5e-3"), text("8e1")])
                .unwrap(),
            text("1")
        );
        for (left, right) in [("123e-2", "1.230"), (".5", "0.5"), ("1e-1", "0.1")] {
            assert_eq!(
                DecimalCmpFunc.invoke(&[text(left), text(right)]).unwrap(),
                SqliteValue::Integer(0)
            );
        }
        assert_eq!(
            DecimalAddFunc.invoke(&[text(".5"), text("-0.2")]).unwrap(),
            text("0.3")
        );
    }

    #[test]
    fn malformed_or_unbounded_exponents_do_not_allocate() {
        for input in [
            "",
            ".",
            "+.",
            "1e",
            "1e+",
            "1e--2",
            "1e2e3",
            "1.2.3",
            "1e 2",
            "1e9223372036854775807",
            "1e-9223372036854775808",
            "1e1000000",
        ] {
            assert!(parse_decimal(input).is_none(), "{input}");
            assert_eq!(
                DecimalAddFunc.invoke(&[text(input), text("1")]).unwrap(),
                SqliteValue::Null
            );
        }
    }

    #[test]
    fn real_conversion_is_exact_not_shortest_roundtrip_text() {
        for (value, expected) in [
            (
                47.49_f64,
                "47.49000000000000198951966012828052043914794921875",
            ),
            (
                0.1_f64,
                "0.1000000000000000055511151231257827021181583404541015625",
            ),
            (-0.5_f64, "-0.5"),
            (-0.0_f64, "0"),
            (1.0_f64, "1"),
        ] {
            assert_eq!(
                DecimalFunc.invoke(&[SqliteValue::Float(value)]).unwrap(),
                text(expected)
            );
            let blob = SqliteValue::Blob(value.to_bits().to_be_bytes().to_vec().into());
            assert_eq!(DecimalFunc.invoke(&[blob]).unwrap(), text(expected));
        }
        assert_eq!(DecimalFunc.invoke(&[text("0.1")]).unwrap(), text("0.1"));
    }

    #[test]
    fn binary64_boundaries_are_exact_and_nonfinite_values_are_null() {
        let smallest = exact_binary64(1).unwrap();
        assert_eq!(smallest.len(), 1076);
        assert!(smallest.starts_with(&format!("0.{}494065645841246544", "0".repeat(323))));
        assert!(smallest.ends_with("19718265533447265625"));
        for bits in [
            1,
            (1_u64 << 52) - 1,
            1_u64 << 52,
            f64::MAX.to_bits(),
            (-f64::MAX).to_bits(),
        ] {
            let decimal = exact_binary64(bits).unwrap();
            assert_eq!(decimal.parse::<f64>().unwrap().to_bits(), bits);
        }
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_eq!(
                DecimalFunc.invoke(&[SqliteValue::Float(value)]).unwrap(),
                SqliteValue::Null
            );
        }
        assert_eq!(
            DecimalFunc
                .invoke(&[SqliteValue::Blob(vec![0; 7].into())])
                .unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn exponential_output_and_integer_powers_are_registered() {
        let mut registry = FunctionRegistry::new();
        crate::register_misc_scalars(&mut registry);
        let exp = registry.find_scalar("decimal_exp", 1).unwrap();
        for (input, expected) in [
            ("12300", "+1.23e+04"),
            ("-0.0012", "-1.2e-03"),
            ("0", "+0.0e+00"),
        ] {
            assert_eq!(exp.invoke(&[text(input)]).unwrap(), text(expected));
        }
        let pow = registry.find_scalar("decimal_pow2", 1).unwrap();
        for (exponent, expected) in [
            (0, "+1.0e+00"),
            (10, "+1.024e+03"),
            (-3, "+1.25e-01"),
            (64, "+1.8446744073709551616e+19"),
        ] {
            assert_eq!(
                pow.invoke(&[SqliteValue::Integer(exponent)]).unwrap(),
                text(expected)
            );
        }
        for input in [
            SqliteValue::Null,
            text("2"),
            SqliteValue::Float(2.0),
            SqliteValue::Integer(20_001),
            SqliteValue::Integer(i64::MIN),
        ] {
            assert_eq!(pow.invoke(&[input]).unwrap(), SqliteValue::Null);
        }
        assert!(DecimalExpFunc.invoke(&[]).is_err());
        assert!(DecimalPow2Func.invoke(&[]).is_err());
    }
}
