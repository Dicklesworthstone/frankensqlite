//! Miscellaneous extensions: generate_series, decimal, uuid (§14.7).
//!
//! Provides three independent extension families:
//!
//! 1. **generate_series(START, STOP \[, STEP\])**: virtual table that generates
//!    a sequence of integers, commonly used in joins and CTEs.
//!
//! 2. **Decimal arithmetic**: exact string-based decimal operations that avoid
//!    floating-point precision loss. Functions: `decimal`, `decimal_add`,
//!    `decimal_sub`, `decimal_mul`, `decimal_cmp`.
//!
//! 3. **UUID generation**: `uuid()` generates random UUID v4 strings,
//!    `uuid_str` converts blob to string, `uuid_blob` converts string to blob.

use std::cmp::Ordering;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::FunctionRegistry;
use fsqlite_func::scalar::ScalarFunction;
use fsqlite_func::vtab::{ColumnContext, IndexInfo, VirtualTable, VirtualTableCursor};
use fsqlite_types::SqliteValue;
use fsqlite_types::cx::Cx;
use tracing::{debug, info};

#[must_use]
pub const fn extension_name() -> &'static str {
    "misc"
}

// ══════════════════════════════════════════════════════════════════════
// generate_series virtual table
// ══════════════════════════════════════════════════════════════════════

/// Virtual table that generates a sequence of integers.
///
/// Usage: `SELECT value FROM generate_series(1, 10)` produces rows 1..=10.
/// Optional third argument specifies step (default 1).
pub struct GenerateSeriesTable;

impl VirtualTable for GenerateSeriesTable {
    type Cursor = GenerateSeriesCursor;

    fn create(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // generate_series accepts 2-3 equality constraints on hidden columns
        // start (col=1), stop (col=2), step (col=3)
        info.estimated_cost = 1.0;
        info.estimated_rows = 1000;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(GenerateSeriesCursor {
            current: 0,
            start: 0,
            stop: 0,
            step: 1,
            done: true,
        })
    }
}

/// Cursor for iterating over a generated integer series.
pub struct GenerateSeriesCursor {
    current: i64,
    start: i64,
    stop: i64,
    step: i64,
    done: bool,
}

impl GenerateSeriesCursor {
    /// Initialize the cursor from explicit start/stop/step values.
    #[allow(clippy::similar_names)]
    pub fn init(&mut self, start: i64, stop: i64, step: i64) -> Result<()> {
        if step == 0 {
            return Err(FrankenError::internal(
                "generate_series: step cannot be zero",
            ));
        }
        self.start = start;
        self.current = start;
        self.stop = stop;
        self.step = step;
        self.done = if step > 0 { start > stop } else { start < stop };
        debug!(start, stop, step, "generate_series: initialized cursor");
        Ok(())
    }
}

impl VirtualTableCursor for GenerateSeriesCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        #[allow(clippy::similar_names)]
        let start = args.first().and_then(SqliteValue::as_integer).unwrap_or(0);
        let end = args.get(1).and_then(SqliteValue::as_integer).unwrap_or(0);
        let step = args.get(2).and_then(SqliteValue::as_integer).unwrap_or(1);
        self.init(start, end, step)
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.done {
            return Ok(());
        }
        // Saturating add to prevent overflow
        self.current = self.current.saturating_add(self.step);
        self.done = if self.step > 0 {
            self.current > self.stop
        } else {
            self.current < self.stop
        };
        Ok(())
    }

    fn eof(&self) -> bool {
        self.done
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let val = match col {
            0 => SqliteValue::Integer(self.current), // value
            1 => SqliteValue::Integer(self.start),   // start
            2 => SqliteValue::Integer(self.stop),    // stop
            3 => SqliteValue::Integer(self.step),    // step
            _ => SqliteValue::Null,
        };
        ctx.set_value(val);
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.current)
    }
}

// ══════════════════════════════════════════════════════════════════════
// Decimal extension — exact string-based arithmetic
// ══════════════════════════════════════════════════════════════════════

/// Normalize a decimal string to canonical form.
///
/// Strips leading zeros (except the one before the decimal point),
/// ensures there's at least a "0" if the integer part is empty.
fn decimal_normalize(s: &str) -> Option<String> {
    let (negative, int_digits, frac_digits) = parse_decimal(s)?;
    Some(format_decimal(negative, &int_digits, &frac_digits))
}

/// Parse a decimal string into (negative, integer_digits, fractional_digits).
fn parse_decimal(s: &str) -> Option<(bool, Vec<u8>, Vec<u8>)> {
    let s = s.trim();
    let (negative, s) = if let Some(stripped) = s.strip_prefix('-') {
        (true, stripped)
    } else if let Some(stripped) = s.strip_prefix('+') {
        (false, stripped)
    } else {
        (false, s)
    };

    if s.is_empty() {
        return None;
    }

    let (int_str, frac_str) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };

    if !int_str.is_empty() && !int_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac_str.is_empty() && !frac_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    let int_digits: Vec<u8> = int_str.bytes().map(|b| b - b'0').collect();
    let frac_digits: Vec<u8> = frac_str.bytes().map(|b| b - b'0').collect();

    Some((negative, int_digits, frac_digits))
}

/// Add two non-negative decimal digit sequences (aligned by decimal point).
///
/// Returns (integer_digits, fractional_digits) of the sum.
fn add_unsigned(int_a: &[u8], frac_a: &[u8], int_b: &[u8], frac_b: &[u8]) -> (Vec<u8>, Vec<u8>) {
    // Pad fractional parts to equal length
    let frac_len = frac_a.len().max(frac_b.len());
    let mut fa: Vec<u8> = frac_a.to_vec();
    fa.resize(frac_len, 0);
    let mut fb: Vec<u8> = frac_b.to_vec();
    fb.resize(frac_len, 0);

    // Add fractional part right-to-left
    let mut carry: u8 = 0;
    let mut frac_result = vec![0u8; frac_len];
    for i in (0..frac_len).rev() {
        let sum = fa[i] + fb[i] + carry;
        frac_result[i] = sum % 10;
        carry = sum / 10;
    }

    // Pad integer parts to equal length
    let int_len = int_a.len().max(int_b.len());
    let mut ia = vec![0u8; int_len - int_a.len()];
    ia.extend_from_slice(int_a);
    let mut ib = vec![0u8; int_len - int_b.len()];
    ib.extend_from_slice(int_b);

    // Add integer part right-to-left
    let mut int_result = vec![0u8; int_len];
    for i in (0..int_len).rev() {
        let sum = ia[i] + ib[i] + carry;
        int_result[i] = sum % 10;
        carry = sum / 10;
    }
    if carry > 0 {
        int_result.insert(0, carry);
    }

    (int_result, frac_result)
}

/// Subtract unsigned b from unsigned a (assumes a >= b).
fn sub_unsigned(int_a: &[u8], frac_a: &[u8], int_b: &[u8], frac_b: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let frac_len = frac_a.len().max(frac_b.len());
    let mut fa: Vec<u8> = frac_a.to_vec();
    fa.resize(frac_len, 0);
    let mut fb: Vec<u8> = frac_b.to_vec();
    fb.resize(frac_len, 0);

    let mut borrow: i16 = 0;
    let mut frac_result = vec![0u8; frac_len];
    for i in (0..frac_len).rev() {
        let diff = i16::from(fa[i]) - i16::from(fb[i]) - borrow;
        if diff < 0 {
            frac_result[i] = u8::try_from(diff + 10).unwrap_or(0);
            borrow = 1;
        } else {
            frac_result[i] = u8::try_from(diff).unwrap_or(0);
            borrow = 0;
        }
    }

    let int_len = int_a.len().max(int_b.len());
    let mut ia = vec![0u8; int_len - int_a.len()];
    ia.extend_from_slice(int_a);
    let mut ib = vec![0u8; int_len - int_b.len()];
    ib.extend_from_slice(int_b);

    let mut int_result = vec![0u8; int_len];
    for i in (0..int_len).rev() {
        let diff = i16::from(ia[i]) - i16::from(ib[i]) - borrow;
        if diff < 0 {
            int_result[i] = u8::try_from(diff + 10).unwrap_or(0);
            borrow = 1;
        } else {
            int_result[i] = u8::try_from(diff).unwrap_or(0);
            borrow = 0;
        }
    }

    (int_result, frac_result)
}

/// Compare two unsigned decimal values.
fn cmp_unsigned(int_a: &[u8], frac_a: &[u8], int_b: &[u8], frac_b: &[u8]) -> Ordering {
    // Compare by number of significant integer digits first
    let ia = strip_leading_zeros(int_a);
    let ib = strip_leading_zeros(int_b);

    match ia.len().cmp(&ib.len()) {
        Ordering::Equal => {}
        ord => return ord,
    }

    // Same length integer parts — compare digit by digit
    for (a, b) in ia.iter().zip(ib.iter()) {
        match a.cmp(b) {
            Ordering::Equal => {}
            ord => return ord,
        }
    }

    // Integer parts equal — compare fractional parts
    let frac_len = frac_a.len().max(frac_b.len());
    for i in 0..frac_len {
        let a = frac_a.get(i).copied().unwrap_or(0);
        let b = frac_b.get(i).copied().unwrap_or(0);
        match a.cmp(&b) {
            Ordering::Equal => {}
            ord => return ord,
        }
    }

    Ordering::Equal
}

fn strip_leading_zeros(digits: &[u8]) -> &[u8] {
    let start = digits.iter().position(|&d| d != 0).unwrap_or(digits.len());
    if start == digits.len() {
        // All zeros — return single zero
        &digits[digits.len().saturating_sub(1)..]
    } else {
        &digits[start..]
    }
}

/// Format digit vectors back to a decimal string.
fn format_decimal(negative: bool, int_digits: &[u8], frac_digits: &[u8]) -> String {
    let int_str: String = strip_leading_zeros(int_digits)
        .iter()
        .map(|d| char::from(b'0' + d))
        .collect();
    let int_str = if int_str.is_empty() {
        "0".to_owned()
    } else {
        int_str
    };

    // Trim trailing zeros from fractional part
    let frac_end = frac_digits
        .iter()
        .rposition(|&d| d != 0)
        .map_or(0, |p| p + 1);
    let frac = &frac_digits[..frac_end];

    let result = if frac.is_empty() {
        int_str
    } else {
        let frac_str: String = frac.iter().map(|d| char::from(b'0' + d)).collect();
        format!("{int_str}.{frac_str}")
    };

    if negative && result != "0" {
        format!("-{result}")
    } else {
        result
    }
}

/// Perform decimal addition: a + b.
fn decimal_add_impl(a: &str, b: &str) -> Option<String> {
    let (neg_a, int_a, frac_a) = parse_decimal(a)?;
    let (neg_b, int_b, frac_b) = parse_decimal(b)?;

    let result = match (neg_a, neg_b) {
        (false, false) => {
            let (ir, fr) = add_unsigned(&int_a, &frac_a, &int_b, &frac_b);
            format_decimal(false, &ir, &fr)
        }
        (true, true) => {
            let (ir, fr) = add_unsigned(&int_a, &frac_a, &int_b, &frac_b);
            format_decimal(true, &ir, &fr)
        }
        (false, true) => {
            // a - |b|
            match cmp_unsigned(&int_a, &frac_a, &int_b, &frac_b) {
                Ordering::Less => {
                    let (ir, fr) = sub_unsigned(&int_b, &frac_b, &int_a, &frac_a);
                    format_decimal(true, &ir, &fr)
                }
                Ordering::Equal => "0".to_owned(),
                Ordering::Greater => {
                    let (ir, fr) = sub_unsigned(&int_a, &frac_a, &int_b, &frac_b);
                    format_decimal(false, &ir, &fr)
                }
            }
        }
        (true, false) => {
            // -|a| + b = b - |a|
            match cmp_unsigned(&int_b, &frac_b, &int_a, &frac_a) {
                Ordering::Less => {
                    let (ir, fr) = sub_unsigned(&int_a, &frac_a, &int_b, &frac_b);
                    format_decimal(true, &ir, &fr)
                }
                Ordering::Equal => "0".to_owned(),
                Ordering::Greater => {
                    let (ir, fr) = sub_unsigned(&int_b, &frac_b, &int_a, &frac_a);
                    format_decimal(false, &ir, &fr)
                }
            }
        }
    };
    Some(result)
}

/// Perform decimal subtraction: a - b.
fn decimal_sub_impl(a: &str, b: &str) -> Option<String> {
    let b_str = b.trim();
    if b_str.is_empty() {
        return None;
    }
    // a - b = a + (-b)
    let neg_b = if let Some(stripped) = b_str.strip_prefix('-') {
        stripped.to_owned()
    } else if let Some(stripped) = b_str.strip_prefix('+') {
        format!("-{stripped}")
    } else {
        format!("-{b_str}")
    };
    decimal_add_impl(a, &neg_b)
}

/// Perform decimal multiplication: a * b.
fn decimal_mul_impl(a: &str, b: &str) -> Option<String> {
    let Some((neg_a, int_a, frac_a)) = parse_decimal(a) else { return None; };
    let Some((neg_b, int_b, frac_b)) = parse_decimal(b) else { return None; };

    let result_negative = neg_a != neg_b;
    let frac_places = frac_a.len() + frac_b.len();

    // Combine integer and fractional into a single digit sequence
    let mut digits_a: Vec<u8> = int_a;
    digits_a.extend_from_slice(&frac_a);
    let mut digits_b: Vec<u8> = int_b;
    digits_b.extend_from_slice(&frac_b);

    // Grade-school multiplication
    let len_a = digits_a.len();
    let len_b = digits_b.len();
    let mut product = vec![0u16; len_a + len_b];

    for (i, &da) in digits_a.iter().enumerate().rev() {
        for (j, &db) in digits_b.iter().enumerate().rev() {
            let pos = i + j + 1;
            product[pos] += u16::from(da) * u16::from(db);
            product[i + j] += product[pos] / 10;
            product[pos] %= 10;
        }
    }

    // Convert to u8
    // Each cell is guaranteed to be 0-9 after carry propagation
    let product: Vec<u8> = product
        .iter()
        .map(|&d| u8::try_from(d).unwrap_or(0))
        .collect();

    // Split at decimal point
    let total_len = product.len();
    let int_end = total_len.saturating_sub(frac_places);
    let int_digits = &product[..int_end];
    let frac_digits = &product[int_end..];

    Some(format_decimal(result_negative, int_digits, frac_digits))
}

/// Compare two decimal values, returning -1, 0, or 1.
fn decimal_cmp_impl(a: &str, b: &str) -> Option<i64> {
    let Some((neg_a, int_a, frac_a)) = parse_decimal(a) else { return None; };
    let Some((neg_b, int_b, frac_b)) = parse_decimal(b) else { return None; };

    let a_is_zero = int_a.iter().all(|&d| d == 0) && frac_a.iter().all(|&d| d == 0);
    let b_is_zero = int_b.iter().all(|&d| d == 0) && frac_b.iter().all(|&d| d == 0);

    if a_is_zero && b_is_zero {
        return Some(0);
    }

    let result = match (neg_a && !a_is_zero, neg_b && !b_is_zero) {
        (true, false) => -1,
        (false, true) => 1,
        (true, true) => {
            // Both negative — larger magnitude is smaller
            match cmp_unsigned(&int_a, &frac_a, &int_b, &frac_b) {
                Ordering::Less => 1,
                Ordering::Equal => 0,
                Ordering::Greater => -1,
            }
        }
        (false, false) => match cmp_unsigned(&int_a, &frac_a, &int_b, &frac_b) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        },
    };
    Some(result)
}

// ── Decimal scalar functions ─────────────────────────────────────────

/// `decimal(X)` — convert a value to canonical decimal text.
pub struct DecimalFunc;

impl ScalarFunction for DecimalFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(FrankenError::internal(
                "decimal requires exactly 1 argument",
            ));
        }
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let text = args[0].to_text();
        Ok(SqliteValue::Text(decimal_normalize(&text).unwrap_or_else(|| text.clone())))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "decimal"
    }
}

/// `decimal_add(X, Y)` — exact decimal addition.
pub struct DecimalAddFunc;

impl ScalarFunction for DecimalAddFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(FrankenError::internal(
                "decimal_add requires exactly 2 arguments",
            ));
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let a = args[0].to_text();
        let b = args[1].to_text();
        debug!(a = %a, b = %b, "decimal_add invoked");
        Ok(match decimal_add_impl(&a, &b) {
            Some(result) => SqliteValue::Text(result),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "decimal_add"
    }
}

/// `decimal_sub(X, Y)` — exact decimal subtraction.
pub struct DecimalSubFunc;

impl ScalarFunction for DecimalSubFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(FrankenError::internal(
                "decimal_sub requires exactly 2 arguments",
            ));
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let a = args[0].to_text();
        let b = args[1].to_text();
        debug!(a = %a, b = %b, "decimal_sub invoked");
        Ok(match decimal_sub_impl(&a, &b) {
            Some(result) => SqliteValue::Text(result),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "decimal_sub"
    }
}

/// `decimal_mul(X, Y)` — exact decimal multiplication.
pub struct DecimalMulFunc;

impl ScalarFunction for DecimalMulFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(FrankenError::internal(
                "decimal_mul requires exactly 2 arguments",
            ));
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let a = args[0].to_text();
        let b = args[1].to_text();
        debug!(a = %a, b = %b, "decimal_mul invoked");
        Ok(match decimal_mul_impl(&a, &b) {
            Some(result) => SqliteValue::Text(result),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "decimal_mul"
    }
}

/// `decimal_cmp(X, Y)` — compare two decimals, returning -1, 0, or 1.
pub struct DecimalCmpFunc;

impl ScalarFunction for DecimalCmpFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(FrankenError::internal(
                "decimal_cmp requires exactly 2 arguments",
            ));
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let a = args[0].to_text();
        let b = args[1].to_text();
        debug!(a = %a, b = %b, "decimal_cmp invoked");
        Ok(match decimal_cmp_impl(&a, &b) {
            Some(result) => SqliteValue::Integer(result),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "decimal_cmp"
    }
}

// ══════════════════════════════════════════════════════════════════════
// UUID extension
// ══════════════════════════════════════════════════════════════════════

/// Simple PRNG for UUID v4 generation (xorshift64).
///
/// Seeded from system time. Not cryptographic, but sufficient for
/// UUID v4 uniqueness guarantees.
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Generate a random UUID v4 string.
fn generate_uuid_v4() -> String {
    // Seed from a combination of pointer address and a counter
    // to ensure uniqueness across calls.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let count = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    // Use stack address as entropy source (ASLR provides randomness)
    let stack_var: u64 = 0;
    #[allow(clippy::ptr_as_ptr)]
    let addr = std::ptr::addr_of!(stack_var) as u64;
    let mut state = addr.wrapping_mul(6_364_136_223_846_793_005)
        ^ count.wrapping_mul(1_442_695_040_888_963_407);
    if state == 0 {
        state = 0x5DEE_CE66_D1A4_F87D; // fallback seed
    }

    let mut bytes = [0u8; 16];
    for chunk in bytes.chunks_exact_mut(8) {
        let val = xorshift64(&mut state);
        chunk.copy_from_slice(&val.to_le_bytes());
    }

    // Set version (4) and variant (10xx)
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // variant 10xx

    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

/// Parse a UUID string into 16 bytes.
fn uuid_str_to_blob(s: &str) -> Result<Vec<u8>> {
    let hex: String = s.chars().filter(char::is_ascii_hexdigit).collect();
    if hex.len() != 32 {
        return Err(FrankenError::internal(format!(
            "invalid UUID string: expected 32 hex digits, got {}",
            hex.len()
        )));
    }

    let mut bytes = Vec::with_capacity(16);
    for i in (0..32).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16)
            .map_err(|_| FrankenError::internal(format!("invalid hex in UUID at position {i}")))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// Format 16 bytes as a UUID string.
fn blob_to_uuid_str(bytes: &[u8]) -> Result<String> {
    if bytes.len() != 16 {
        return Err(FrankenError::internal(format!(
            "uuid_str: expected 16-byte blob, got {} bytes",
            bytes.len()
        )));
    }
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    ))
}

// ── UUID scalar functions ────────────────────────────────────────────

/// `uuid()` — generate a random UUID v4 string.
pub struct UuidFunc;

impl ScalarFunction for UuidFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !args.is_empty() {
            return Err(FrankenError::internal("uuid takes no arguments"));
        }
        let uuid = generate_uuid_v4();
        debug!(uuid = %uuid, "uuid() generated");
        Ok(SqliteValue::Text(uuid))
    }

    fn is_deterministic(&self) -> bool {
        false // each call returns a new UUID
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &'static str {
        "uuid"
    }
}

/// `uuid_str(X)` — convert a 16-byte UUID blob to its string representation.
pub struct UuidStrFunc;

impl ScalarFunction for UuidStrFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(FrankenError::internal(
                "uuid_str requires exactly 1 argument",
            ));
        }
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        match &args[0] {
            SqliteValue::Blob(b) => {
                let s = blob_to_uuid_str(b)?;
                Ok(SqliteValue::Text(s))
            }
            SqliteValue::Text(s) => {
                // If already a string, normalize it
                let blob = uuid_str_to_blob(s)?;
                let normalized = blob_to_uuid_str(&blob)?;
                Ok(SqliteValue::Text(normalized))
            }
            _ => Err(FrankenError::internal(
                "uuid_str: argument must be a blob or text",
            )),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "uuid_str"
    }
}

/// `uuid_blob(X)` — convert a UUID string to a 16-byte blob.
pub struct UuidBlobFunc;

impl ScalarFunction for UuidBlobFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(FrankenError::internal(
                "uuid_blob requires exactly 1 argument",
            ));
        }
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let Some(s) = args[0].as_text() else {
            return Err(FrankenError::internal("uuid_blob: argument must be text"));
        };
        let blob = uuid_str_to_blob(s)?;
        Ok(SqliteValue::Blob(blob))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "uuid_blob"
    }
}

// ══════════════════════════════════════════════════════════════════════
// Registration
// ══════════════════════════════════════════════════════════════════════

/// Register all miscellaneous scalar functions.
pub fn register_misc_scalars(registry: &mut FunctionRegistry) {
    info!("misc extension: registering scalar functions");
    registry.register_scalar(DecimalFunc);
    registry.register_scalar(DecimalAddFunc);
    registry.register_scalar(DecimalSubFunc);
    registry.register_scalar(DecimalMulFunc);
    registry.register_scalar(DecimalCmpFunc);
    registry.register_scalar(UuidFunc);
    registry.register_scalar(UuidStrFunc);
    registry.register_scalar(UuidBlobFunc);
}

// ══════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_name_matches_crate_suffix() {
        let expected = env!("CARGO_PKG_NAME")
            .strip_prefix("fsqlite-ext-")
            .expect("extension crates should use fsqlite-ext-* naming");
        assert_eq!(extension_name(), expected);
    }

    // ── generate_series ──────────────────────────────────────────────

    #[test]
    fn test_generate_series_basic() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(1, 5, 1).unwrap();

        let mut values = Vec::new();
        let cx = Cx::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 0).unwrap();
            if let Some(SqliteValue::Integer(v)) = ctx.take_value() {
                values.push(v);
            }
            cursor.next(&cx).unwrap();
        }
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_generate_series_step() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(0, 10, 2).unwrap();

        let mut values = Vec::new();
        let cx = Cx::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 0).unwrap();
            if let Some(SqliteValue::Integer(v)) = ctx.take_value() {
                values.push(v);
            }
            cursor.next(&cx).unwrap();
        }
        assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_generate_series_negative_step() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(5, 1, -1).unwrap();

        let mut values = Vec::new();
        let cx = Cx::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 0).unwrap();
            if let Some(SqliteValue::Integer(v)) = ctx.take_value() {
                values.push(v);
            }
            cursor.next(&cx).unwrap();
        }
        assert_eq!(values, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_generate_series_single() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(5, 5, 1).unwrap();

        let mut values = Vec::new();
        let cx = Cx::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 0).unwrap();
            if let Some(SqliteValue::Integer(v)) = ctx.take_value() {
                values.push(v);
            }
            cursor.next(&cx).unwrap();
        }
        assert_eq!(values, vec![5]);
    }

    #[test]
    fn test_generate_series_empty() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(5, 1, 1).unwrap();
        assert!(
            cursor.eof(),
            "positive step with start > stop should be empty"
        );
    }

    #[test]
    fn test_generate_series_step_zero_error() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        assert!(cursor.init(1, 10, 0).is_err());
    }

    #[test]
    fn test_generate_series_filter() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        let cx = Cx::new();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(1),
                ],
            )
            .unwrap();

        let mut values = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 0).unwrap();
            if let Some(SqliteValue::Integer(v)) = ctx.take_value() {
                values.push(v);
            }
            cursor.next(&cx).unwrap();
        }
        assert_eq!(values, vec![1, 2, 3]);
    }

    // ── decimal ──────────────────────────────────────────────────────

    #[test]
    fn test_decimal_normalize() {
        assert_eq!(decimal_normalize("1.23"), Some("1.23".to_owned()));
        assert_eq!(decimal_normalize("001.230"), Some("1.23".to_owned()));
        assert_eq!(decimal_normalize("0.0"), Some("0".to_owned()));
        assert_eq!(decimal_normalize("-1.50"), Some("-1.5".to_owned()));
        assert_eq!(decimal_normalize("42"), Some("42".to_owned()));
    }

    #[test]
    fn test_decimal_func_basic() {
        let args = [SqliteValue::Text("1.23".into())];
        let result = DecimalFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Text("1.23".into()));
    }

    #[test]
    fn test_decimal_func_null() {
        let args = [SqliteValue::Null];
        let result = DecimalFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_decimal_add() {
        let args = [
            SqliteValue::Text("1.1".into()),
            SqliteValue::Text("2.2".into()),
        ];
        let result = DecimalAddFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Text("3.3".into()));
    }

    #[test]
    fn test_decimal_add_no_fp_loss() {
        // This would be 0.30000000000000004 in floating point
        let args = [
            SqliteValue::Text("0.1".into()),
            SqliteValue::Text("0.2".into()),
        ];
        let result = DecimalAddFunc.invoke(&args).unwrap();
        assert_eq!(
            result,
            SqliteValue::Text("0.3".into()),
            "decimal_add should avoid floating-point precision loss"
        );
    }

    #[test]
    fn test_decimal_sub() {
        let args = [
            SqliteValue::Text("5.00".into()),
            SqliteValue::Text("1.23".into()),
        ];
        let result = DecimalSubFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Text("3.77".into()));
    }

    #[test]
    fn test_decimal_sub_negative_result() {
        let args = [
            SqliteValue::Text("1.0".into()),
            SqliteValue::Text("3.0".into()),
        ];
        let result = DecimalSubFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Text("-2".into()));
    }

    #[test]
    fn test_decimal_mul() {
        let args = [
            SqliteValue::Text("1.5".into()),
            SqliteValue::Text("2.5".into()),
        ];
        let result = DecimalMulFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Text("3.75".into()));
    }

    #[test]
    fn test_decimal_mul_large() {
        let args = [
            SqliteValue::Text("1.1".into()),
            SqliteValue::Text("2.0".into()),
        ];
        let result = DecimalMulFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Text("2.2".into()));
    }

    #[test]
    fn test_decimal_cmp_less() {
        let args = [
            SqliteValue::Text("1.23".into()),
            SqliteValue::Text("4.56".into()),
        ];
        let result = DecimalCmpFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Integer(-1));
    }

    #[test]
    fn test_decimal_cmp_greater() {
        let args = [
            SqliteValue::Text("4.56".into()),
            SqliteValue::Text("1.23".into()),
        ];
        let result = DecimalCmpFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_decimal_cmp_equal() {
        let args = [
            SqliteValue::Text("1.0".into()),
            SqliteValue::Text("1.0".into()),
        ];
        let result = DecimalCmpFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Integer(0));
    }

    #[test]
    fn test_decimal_cmp_negative() {
        let args = [
            SqliteValue::Text("-5".into()),
            SqliteValue::Text("3".into()),
        ];
        let result = DecimalCmpFunc.invoke(&args).unwrap();
        assert_eq!(result, SqliteValue::Integer(-1));
    }

    #[test]
    fn test_decimal_precision_financial() {
        // Common financial precision test: 19.99 * 100 = 1999
        let result = decimal_mul_impl("19.99", "100");
        assert_eq!(result, Some("1999".to_owned()));

        // Chained operations: (10.50 + 3.75) * 2 = 28.50
        let sum = decimal_add_impl("10.50", "3.75");
        assert_eq!(sum, Some("14.25".to_owned()));
        let product = decimal_mul_impl(sum.as_ref().unwrap(), "2");
        assert_eq!(product, Some("28.5".to_owned()));
    }

    // ── uuid ─────────────────────────────────────────────────────────

    #[test]
    fn test_uuid_v4_format() {
        let uuid = generate_uuid_v4();
        // UUID v4 format: 8-4-4-4-12 hex characters
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(parts.len(), 5, "UUID should have 5 dash-separated parts");
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }

    #[test]
    fn test_uuid_v4_version() {
        let uuid = generate_uuid_v4();
        // Version nibble is the first character of the third group
        let version_char = uuid.chars().nth(14).unwrap();
        assert_eq!(version_char, '4', "UUID v4 must have version nibble = 4");
    }

    #[test]
    fn test_uuid_v4_variant() {
        let uuid = generate_uuid_v4();
        // Variant bits are the first character of the fourth group
        let variant_char = uuid.chars().nth(19).unwrap();
        let variant_nibble = u8::from_str_radix(&variant_char.to_string(), 16).unwrap();
        assert!(
            (0x8..=0xB).contains(&variant_nibble),
            "UUID v4 variant bits should be 10xx, got {variant_nibble:#X}"
        );
    }

    #[test]
    fn test_uuid_uniqueness() {
        let mut uuids: Vec<String> = (0..100).map(|_| generate_uuid_v4()).collect();
        uuids.sort();
        uuids.dedup();
        assert_eq!(
            uuids.len(),
            100,
            "100 uuid() calls should produce 100 unique values"
        );
    }

    #[test]
    fn test_uuid_func() {
        let result = UuidFunc.invoke(&[]).unwrap();
        if let SqliteValue::Text(s) = result {
            assert_eq!(s.len(), 36, "UUID string should be 36 characters");
        } else {
            panic!("uuid() should return Text");
        }
    }

    #[test]
    fn test_uuid_str_blob_roundtrip() {
        let uuid_str = generate_uuid_v4();
        let blob = uuid_str_to_blob(&uuid_str).unwrap();
        assert_eq!(blob.len(), 16);
        let back = blob_to_uuid_str(&blob).unwrap();
        assert_eq!(back, uuid_str, "uuid_str(uuid_blob(X)) should roundtrip");
    }

    #[test]
    fn test_uuid_blob_length() {
        let result = UuidBlobFunc
            .invoke(&[SqliteValue::Text(generate_uuid_v4())])
            .unwrap();
        if let SqliteValue::Blob(b) = result {
            assert_eq!(b.len(), 16, "uuid_blob should return 16-byte blob");
        } else {
            panic!("uuid_blob should return Blob");
        }
    }

    #[test]
    fn test_uuid_str_func() {
        let uuid = generate_uuid_v4();
        let blob = uuid_str_to_blob(&uuid).unwrap();
        let result = UuidStrFunc.invoke(&[SqliteValue::Blob(blob)]).unwrap();
        assert_eq!(result, SqliteValue::Text(uuid));
    }

    // ── registration ─────────────────────────────────────────────────

    #[test]
    fn test_register_misc_scalars() {
        let mut registry = FunctionRegistry::new();
        register_misc_scalars(&mut registry);
        assert!(registry.find_scalar("decimal", 1).is_some());
        assert!(registry.find_scalar("decimal_add", 2).is_some());
        assert!(registry.find_scalar("decimal_sub", 2).is_some());
        assert!(registry.find_scalar("decimal_mul", 2).is_some());
        assert!(registry.find_scalar("decimal_cmp", 2).is_some());
        assert!(registry.find_scalar("uuid", 0).is_some());
        assert!(registry.find_scalar("uuid_str", 1).is_some());
        assert!(registry.find_scalar("uuid_blob", 1).is_some());
    }

    // ── generate_series: additional edge cases ───────────────────────────

    #[test]
    fn test_generate_series_large_step() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(0, 100, 50).unwrap();
        let mut values = Vec::new();
        while !cursor.eof() {
            values.push(cursor.current);
            cursor.next(&Cx::default()).unwrap();
        }
        assert_eq!(values, vec![0, 50, 100]);
    }

    #[test]
    fn test_generate_series_negative_range() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(-5, -1, 1).unwrap();
        let mut count = 0;
        while !cursor.eof() {
            count += 1;
            cursor.next(&Cx::default()).unwrap();
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_generate_series_reverse_with_wrong_step_empty() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        // start > stop with positive step → empty
        cursor.init(10, 1, 1).unwrap();
        assert!(cursor.eof());
    }

    #[test]
    fn test_generate_series_forward_with_negative_step_empty() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        // start < stop with negative step → empty
        cursor.init(1, 10, -1).unwrap();
        assert!(cursor.eof());
    }

    #[test]
    fn test_generate_series_rowid() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(42, 42, 1).unwrap();
        assert_eq!(cursor.rowid().unwrap(), 42);
    }

    #[test]
    fn test_generate_series_column_values() {
        let table = GenerateSeriesTable;
        let mut cursor = table.open().unwrap();
        cursor.init(10, 20, 5).unwrap();
        // Column 0 = value (current)
        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Integer(10)));
        // Column 2 = stop
        let mut ctx2 = ColumnContext::new();
        cursor.column(&mut ctx2, 2).unwrap();
        assert_eq!(ctx2.take_value(), Some(SqliteValue::Integer(20)));
        // Column 3 = step
        let mut ctx3 = ColumnContext::new();
        cursor.column(&mut ctx3, 3).unwrap();
        assert_eq!(ctx3.take_value(), Some(SqliteValue::Integer(5)));
        // Column out of range = Null
        let mut ctx4 = ColumnContext::new();
        cursor.column(&mut ctx4, 99).unwrap();
        assert_eq!(ctx4.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_generate_series_vtable_create_connect() {
        let cx = Cx::default();
        let _ = GenerateSeriesTable::create(&cx, &[]).unwrap();
        let _ = GenerateSeriesTable::connect(&cx, &[]).unwrap();
    }

    #[test]
    fn test_generate_series_best_index() {
        let table = GenerateSeriesTable;
        let mut info = IndexInfo::new(Vec::new(), Vec::new());
        table.best_index(&mut info).unwrap();
        assert!(info.estimated_cost > 0.0);
        assert!(info.estimated_rows > 0);
    }

    // ── Decimal: normalization edge cases ─────────────────────────────────

    #[test]
    fn test_decimal_normalize_zero() {
        assert_eq!(decimal_normalize("0"), Some("0".to_owned()));
        assert_eq!(decimal_normalize("0.0"), Some("0".to_owned()));
        assert_eq!(decimal_normalize("000.000"), Some("0".to_owned()));
    }

    #[test]
    fn test_decimal_normalize_negative_zero() {
        // Negative zero should normalize to "0"
        let result = decimal_normalize("-0.0");
        assert!(result == Some("0".to_owned()) || result == Some("-0".to_owned()));
    }

    #[test]
    fn test_decimal_normalize_integer() {
        assert_eq!(decimal_normalize("42"), Some("42".to_owned()));
        assert_eq!(decimal_normalize("00042"), Some("42".to_owned()));
    }

    #[test]
    fn test_decimal_normalize_trailing_zeros() {
        assert_eq!(decimal_normalize("1.50000"), Some("1.5".to_owned()));
        assert_eq!(decimal_normalize("3.14000"), Some("3.14".to_owned()));
    }

    // ── Decimal: arithmetic edge cases ───────────────────────────────────

    #[test]
    fn test_decimal_add_zeros() {
        assert_eq!(decimal_add_impl("0", "0"), Some("0".to_owned()));
    }

    #[test]
    fn test_decimal_add_negative_plus_positive() {
        let result = decimal_add_impl("-5", "3");
        assert_eq!(result, Some("-2".to_owned()));
    }

    #[test]
    fn test_decimal_add_positive_plus_negative() {
        let result = decimal_add_impl("3", "-5");
        assert_eq!(result, Some("-2".to_owned()));
    }

    #[test]
    fn test_decimal_sub_same_number() {
        assert_eq!(decimal_sub_impl("42.5", "42.5"), Some("0".to_owned()));
    }

    #[test]
    fn test_decimal_sub_produces_negative() {
        let result = decimal_sub_impl("1", "5");
        assert_eq!(result, Some("-4".to_owned()));
    }

    #[test]
    fn test_decimal_mul_by_zero() {
        assert_eq!(decimal_mul_impl("12345.6789", "0"), Some("0".to_owned()));
    }

    #[test]
    fn test_decimal_mul_by_one() {
        assert_eq!(decimal_mul_impl("3.14", "1"), Some("3.14".to_owned()));
    }

    #[test]
    fn test_decimal_mul_negative_times_negative() {
        let result = decimal_mul_impl("-3", "-4");
        assert_eq!(result, Some("12".to_owned()));
    }

    #[test]
    fn test_decimal_mul_small_decimals() {
        let result = decimal_mul_impl("0.001", "0.001");
        assert_eq!(result, Some("0.000001".to_owned()));
    }

    #[test]
    fn test_decimal_cmp_equal_values() {
        assert_eq!(decimal_cmp_impl("3.14", "3.14"), Some(0));
    }

    #[test]
    fn test_decimal_cmp_leading_zeros_equal() {
        assert_eq!(decimal_cmp_impl("007.50", "7.5"), Some(0));
    }

    #[test]
    fn test_decimal_cmp_negative_ordering() {
        assert_eq!(decimal_cmp_impl("-10", "-5"), Some(-1));
        assert_eq!(decimal_cmp_impl("-5", "-10"), Some(1));
    }

    // ── Decimal: scalar function null handling ───────────────────────────

    #[test]
    fn test_decimal_add_func_null_propagation() {
        let result = DecimalAddFunc
            .invoke(&[SqliteValue::Null, SqliteValue::Text("1".to_owned())])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_decimal_sub_func_null_propagation() {
        let result = DecimalSubFunc
            .invoke(&[SqliteValue::Text("1".to_owned()), SqliteValue::Null])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_decimal_mul_func_null_propagation() {
        let result = DecimalMulFunc
            .invoke(&[SqliteValue::Null, SqliteValue::Null])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_decimal_cmp_func_null_propagation() {
        let result = DecimalCmpFunc
            .invoke(&[SqliteValue::Null, SqliteValue::Text("1".to_owned())])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    // ── UUID: error cases ────────────────────────────────────────────────

    #[test]
    fn test_uuid_str_to_blob_invalid_length() {
        // Too short
        assert!(uuid_str_to_blob("abc").is_err());
    }

    #[test]
    fn test_uuid_str_to_blob_invalid_hex() {
        assert!(uuid_str_to_blob("ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ").is_err());
    }

    #[test]
    fn test_blob_to_uuid_str_wrong_length() {
        assert!(blob_to_uuid_str(&[0u8; 15]).is_err());
        assert!(blob_to_uuid_str(&[0u8; 17]).is_err());
    }

    #[test]
    fn test_uuid_func_with_args_errors() {
        let result = UuidFunc.invoke(&[SqliteValue::Integer(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_uuid_str_func_null_returns_null() {
        let result = UuidStrFunc.invoke(&[SqliteValue::Null]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_uuid_blob_func_null_returns_null() {
        let result = UuidBlobFunc.invoke(&[SqliteValue::Null]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_uuid_blob_func_non_text_errors() {
        let result = UuidBlobFunc.invoke(&[SqliteValue::Integer(42)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_uuid_str_func_normalizes_text() {
        // uuid_str with text input should normalize via blob roundtrip
        let uuid = generate_uuid_v4();
        let result = UuidStrFunc
            .invoke(&[SqliteValue::Text(uuid.clone())])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(uuid));
    }

    #[test]
    fn test_uuid_str_func_non_blob_non_text_errors() {
        let result = UuidStrFunc.invoke(&[SqliteValue::Integer(42)]);
        assert!(result.is_err());
    }

    // ── UUID: format validation ──────────────────────────────────────────

    #[test]
    fn test_uuid_all_lowercase_hex() {
        let uuid = generate_uuid_v4();
        // UUID should contain only lowercase hex and dashes
        assert!(uuid.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        assert!(!uuid.contains(|c: char| c.is_ascii_uppercase()));
    }

    #[test]
    fn test_uuid_v4_multiple_unique() {
        let uuids: Vec<String> = (0..50).map(|_| generate_uuid_v4()).collect();
        // All should be unique
        let mut sorted = uuids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), uuids.len(), "all UUIDs should be unique");
    }

    // ── Scalar function names ────────────────────────────────────────────

    #[test]
    fn test_scalar_function_names() {
        assert_eq!(DecimalFunc.name(), "decimal");
        assert_eq!(DecimalAddFunc.name(), "decimal_add");
        assert_eq!(DecimalSubFunc.name(), "decimal_sub");
        assert_eq!(DecimalMulFunc.name(), "decimal_mul");
        assert_eq!(DecimalCmpFunc.name(), "decimal_cmp");
        assert_eq!(UuidFunc.name(), "uuid");
        assert_eq!(UuidStrFunc.name(), "uuid_str");
        assert_eq!(UuidBlobFunc.name(), "uuid_blob");
    }

    #[test]
    fn test_scalar_function_arg_counts() {
        assert_eq!(DecimalFunc.num_args(), 1);
        assert_eq!(DecimalAddFunc.num_args(), 2);
        assert_eq!(DecimalSubFunc.num_args(), 2);
        assert_eq!(DecimalMulFunc.num_args(), 2);
        assert_eq!(DecimalCmpFunc.num_args(), 2);
        assert_eq!(UuidFunc.num_args(), 0);
        assert_eq!(UuidStrFunc.num_args(), 1);
        assert_eq!(UuidBlobFunc.num_args(), 1);
    }

    #[test]
    fn test_uuid_func_not_deterministic() {
        assert!(!UuidFunc.is_deterministic());
    }
}
