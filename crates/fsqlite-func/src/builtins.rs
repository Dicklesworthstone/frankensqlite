//! Built-in core scalar functions (§13.1).
//!
//! Implements 60+ SQLite scalar functions with exact NULL-propagation
//! semantics. Functions that need connection state (changes, total_changes,
//! last_insert_rowid, sqlite_offset) are registered as stubs that will be
//! wired to connection context when the VDBE is integrated.
#![allow(
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::fn_params_excessive_bools,
    clippy::items_after_statements,
    clippy::match_same_arms,
    clippy::single_match_else,
    clippy::manual_let_else,
    clippy::comparison_chain,
    clippy::suboptimal_flops,
    clippy::unnecessary_wraps,
    clippy::useless_let_if_seq,
    clippy::redundant_closure_for_method_calls,
    clippy::manual_ignore_case_cmp
)]

use std::fmt::Write as _;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;

use crate::agg_builtins::register_aggregate_builtins;
use crate::datetime::register_datetime_builtins;
use crate::math::register_math_builtins;
use crate::{FunctionRegistry, ScalarFunction};

// Thread-local storage for connection state that scalar functions need access to.
// Set by the Connection during DML operations; read by stub functions like
// last_insert_rowid(), changes(), total_changes().
thread_local! {
    static LAST_INSERT_ROWID: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
    static LAST_CHANGES: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
    static TOTAL_CHANGES: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

/// Set the last insert rowid (called by Connection after INSERT).
pub fn set_last_insert_rowid(rowid: i64) {
    LAST_INSERT_ROWID.set(rowid);
}

/// Get the current last insert rowid.
pub fn get_last_insert_rowid() -> i64 {
    LAST_INSERT_ROWID.get()
}

/// Set the last changes count (called by Connection after DML).
///
/// Also accumulates into the cumulative `total_changes` counter.
pub fn set_last_changes(count: i64) {
    LAST_CHANGES.set(count);
    TOTAL_CHANGES.set(TOTAL_CHANGES.get().saturating_add(count));
}

/// Get the current last changes count.
pub fn get_last_changes() -> i64 {
    LAST_CHANGES.get()
}

/// Get the cumulative total changes since the connection was opened.
pub fn get_total_changes() -> i64 {
    TOTAL_CHANGES.get()
}

/// Reset the cumulative total changes counter (called on new connection open).
pub fn reset_total_changes() {
    TOTAL_CHANGES.set(0);
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Standard NULL propagation: if any arg is NULL, return NULL.
fn null_propagate(args: &[SqliteValue]) -> Option<SqliteValue> {
    if args.iter().any(SqliteValue::is_null) {
        Some(SqliteValue::Null)
    } else {
        None
    }
}

/// Try to interpret a value as a numeric (integer preferred, then float).
fn coerce_numeric(v: &SqliteValue) -> SqliteValue {
    match v {
        SqliteValue::Integer(_) | SqliteValue::Float(_) => v.clone(),
        SqliteValue::Text(s) => {
            if let Ok(i) = s.parse::<i64>() {
                SqliteValue::Integer(i)
            } else if let Ok(f) = s.parse::<f64>() {
                SqliteValue::Float(f)
            } else {
                SqliteValue::Integer(0)
            }
        }
        SqliteValue::Null => SqliteValue::Null,
        SqliteValue::Blob(_) => SqliteValue::Integer(0),
    }
}

// ── abs(X) ────────────────────────────────────────────────────────────────

pub struct AbsFunc;

impl ScalarFunction for AbsFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        match coerce_numeric(&args[0]) {
            SqliteValue::Integer(i) => {
                if i == i64::MIN {
                    return Err(FrankenError::IntegerOverflow);
                }
                Ok(SqliteValue::Integer(i.abs()))
            }
            SqliteValue::Float(f) => Ok(SqliteValue::Float(f.abs())),
            other => Ok(other),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "abs"
    }
}

// ── char(X1, X2, ...) ────────────────────────────────────────────────────

pub struct CharFunc;

impl ScalarFunction for CharFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let mut result = String::new();
        for arg in args {
            // C SQLite: sqlite3_value_int(NULL) returns 0, so NULL → U+0000.
            #[allow(clippy::cast_sign_loss)]
            let cp = arg.to_integer() as u32;
            if let Some(c) = char::from_u32(cp) {
                result.push(c);
            }
        }
        Ok(SqliteValue::Text(result))
    }

    fn is_deterministic(&self) -> bool {
        true
    }

    fn num_args(&self) -> i32 {
        -1 // variadic
    }

    fn name(&self) -> &str {
        "char"
    }
}

// ── coalesce(X, Y, ...) ─────────────────────────────────────────────────

pub struct CoalesceFunc;

impl ScalarFunction for CoalesceFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // Return first non-NULL argument.
        // NOTE: Real short-circuit evaluation happens at the VDBE level.
        // At the scalar level, all args are already evaluated.
        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(SqliteValue::Null)
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "coalesce"
    }
}

// ── concat(X, Y, ...) ───────────────────────────────────────────────────

pub struct ConcatFunc;

impl ScalarFunction for ConcatFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let mut result = String::new();
        for arg in args {
            // concat treats NULL as empty string (unlike ||)
            if !arg.is_null() {
                result.push_str(&arg.to_text());
            }
        }
        Ok(SqliteValue::Text(result))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "concat"
    }
}

// ── concat_ws(SEP, X, Y, ...) ───────────────────────────────────────────

pub struct ConcatWsFunc;

impl ScalarFunction for ConcatWsFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Text(String::new()));
        }
        // C SQLite: concat_ws(NULL, ...) returns NULL when separator is NULL.
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let sep = args[0].to_text();
        let mut parts = Vec::new();
        for arg in &args[1..] {
            // NULL args are skipped entirely
            if !arg.is_null() {
                parts.push(arg.to_text());
            }
        }
        Ok(SqliteValue::Text(parts.join(&sep)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "concat_ws"
    }
}

// ── hex(X) ───────────────────────────────────────────────────────────────

pub struct HexFunc;

impl ScalarFunction for HexFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // C SQLite hex() calls sqlite3_value_blob(arg) + sqlite3_value_bytes(arg).
        // For NULL: blob returns NULL ptr, bytes returns 0, producing "" (empty string).
        // This has been consistent across all SQLite versions including 3.52.0.
        if args[0].is_null() {
            return Ok(SqliteValue::Text(String::new()));
        }
        let bytes = match &args[0] {
            SqliteValue::Blob(b) => b.clone(),
            // For non-blob: convert to text first, then hex-encode UTF-8 bytes
            other => other.to_text().into_bytes(),
        };
        let mut hex = String::with_capacity(bytes.len() * 2);
        for b in &bytes {
            let _ = write!(hex, "{b:02X}");
        }
        Ok(SqliteValue::Text(hex))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "hex"
    }
}

// ── ifnull(X, Y) ────────────────────────────────────────────────────────

pub struct IfnullFunc;

impl ScalarFunction for IfnullFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            Ok(args[1].clone())
        } else {
            Ok(args[0].clone())
        }
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "ifnull"
    }
}

// ── iif(COND, TRUE_VAL, FALSE_VAL) ──────────────────────────────────────

pub struct IifFunc;

impl ScalarFunction for IifFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // NOTE: Real short-circuit happens at VDBE level.
        let cond = &args[0];
        let is_true = match cond {
            SqliteValue::Null => false,
            SqliteValue::Integer(i) => *i != 0,
            SqliteValue::Float(f) => *f != 0.0,
            SqliteValue::Text(s) => s.parse::<f64>().is_ok_and(|f| f != 0.0),
            SqliteValue::Blob(_) => false,
        };
        if is_true {
            Ok(args[1].clone())
        } else {
            Ok(args[2].clone())
        }
    }

    fn num_args(&self) -> i32 {
        3
    }

    fn name(&self) -> &str {
        "iif"
    }
}

// ── instr(X, Y) ─────────────────────────────────────────────────────────

pub struct InstrFunc;

impl ScalarFunction for InstrFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        match (&args[0], &args[1]) {
            (SqliteValue::Blob(haystack), SqliteValue::Blob(needle)) => {
                // SQLite: empty needle returns 1, empty haystack with non-empty needle returns 0.
                if needle.is_empty() {
                    return Ok(SqliteValue::Integer(1));
                }
                if haystack.is_empty() {
                    return Ok(SqliteValue::Integer(0));
                }
                let pos = find_bytes(haystack, needle).map_or(0, |p| p + 1);
                Ok(SqliteValue::Integer(i64::try_from(pos).unwrap_or(0)))
            }
            _ => {
                // Text: character-level search.
                // SQLite: empty needle returns 1, empty haystack with non-empty needle returns 0.
                let haystack = args[0].to_text();
                let needle = args[1].to_text();
                if needle.is_empty() {
                    return Ok(SqliteValue::Integer(1));
                }
                if haystack.is_empty() {
                    return Ok(SqliteValue::Integer(0));
                }
                let pos = haystack
                    .find(&needle)
                    .map_or(0, |byte_pos| haystack[..byte_pos].chars().count() + 1);
                Ok(SqliteValue::Integer(i64::try_from(pos).unwrap_or(0)))
            }
        }
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "instr"
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ── length(X) ────────────────────────────────────────────────────────────

pub struct LengthFunc;

impl ScalarFunction for LengthFunc {
    #[allow(clippy::cast_possible_wrap)]
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let len = match &args[0] {
            SqliteValue::Text(s) => s.chars().count(),
            SqliteValue::Blob(b) => b.len(),
            // Numbers: length of text representation
            other => other.to_text().chars().count(),
        };
        Ok(SqliteValue::Integer(len as i64))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "length"
    }
}

// ── octet_length(X) ─────────────────────────────────────────────────────

pub struct OctetLengthFunc;

impl ScalarFunction for OctetLengthFunc {
    #[allow(clippy::cast_possible_wrap)]
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let len = match &args[0] {
            SqliteValue::Text(s) => s.len(),
            SqliteValue::Blob(b) => b.len(),
            other => other.to_text().len(),
        };
        Ok(SqliteValue::Integer(len as i64))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "octet_length"
    }
}

// ── lower(X) / upper(X) ─────────────────────────────────────────────────

pub struct LowerFunc;

impl ScalarFunction for LowerFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        Ok(SqliteValue::Text(args[0].to_text().to_ascii_lowercase()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "lower"
    }
}

pub struct UpperFunc;

impl ScalarFunction for UpperFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        Ok(SqliteValue::Text(args[0].to_text().to_ascii_uppercase()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "upper"
    }
}

// ── trim/ltrim/rtrim ────────────────────────────────────────────────────

pub struct TrimFunc;
pub struct LtrimFunc;
pub struct RtrimFunc;

fn trim_chars(s: &str, chars: &str) -> String {
    let char_set: Vec<char> = chars.chars().collect();
    s.trim_matches(|c: char| char_set.contains(&c)).to_owned()
}

fn ltrim_chars(s: &str, chars: &str) -> String {
    let char_set: Vec<char> = chars.chars().collect();
    s.trim_start_matches(|c: char| char_set.contains(&c))
        .to_owned()
}

fn rtrim_chars(s: &str, chars: &str) -> String {
    let char_set: Vec<char> = chars.chars().collect();
    s.trim_end_matches(|c: char| char_set.contains(&c))
        .to_owned()
}

impl ScalarFunction for TrimFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let s = args[0].to_text();
        let chars = if args.len() > 1 && !args[1].is_null() {
            args[1].to_text()
        } else {
            " ".to_owned()
        };
        Ok(SqliteValue::Text(trim_chars(&s, &chars)))
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
    }

    fn name(&self) -> &str {
        "trim"
    }
}

impl ScalarFunction for LtrimFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let s = args[0].to_text();
        let chars = if args.len() > 1 && !args[1].is_null() {
            args[1].to_text()
        } else {
            " ".to_owned()
        };
        Ok(SqliteValue::Text(ltrim_chars(&s, &chars)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "ltrim"
    }
}

impl ScalarFunction for RtrimFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let s = args[0].to_text();
        let chars = if args.len() > 1 && !args[1].is_null() {
            args[1].to_text()
        } else {
            " ".to_owned()
        };
        Ok(SqliteValue::Text(rtrim_chars(&s, &chars)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "rtrim"
    }
}

// ── nullif(X, Y) ────────────────────────────────────────────────────────

pub struct NullifFunc;

impl ScalarFunction for NullifFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0] == args[1] {
            Ok(SqliteValue::Null)
        } else {
            Ok(args[0].clone())
        }
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "nullif"
    }
}

// ── typeof(X) ────────────────────────────────────────────────────────────

pub struct TypeofFunc;

impl ScalarFunction for TypeofFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let type_name = match &args[0] {
            SqliteValue::Null => "null",
            SqliteValue::Integer(_) => "integer",
            SqliteValue::Float(_) => "real",
            SqliteValue::Text(_) => "text",
            SqliteValue::Blob(_) => "blob",
        };
        Ok(SqliteValue::Text(type_name.to_owned()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "typeof"
    }
}

// ── subtype(X) ───────────────────────────────────────────────────────────

pub struct SubtypeFunc;

impl ScalarFunction for SubtypeFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        // subtype(NULL) = 0 (does NOT propagate NULL)
        // Without subtype tags in SqliteValue, always return 0.
        Ok(SqliteValue::Integer(0))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "subtype"
    }
}

// ── replace(X, Y, Z) ────────────────────────────────────────────────────

pub struct ReplaceFunc;

impl ScalarFunction for ReplaceFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let x = args[0].to_text();
        let y = args[1].to_text();
        let z = args[2].to_text();
        if y.is_empty() {
            return Ok(SqliteValue::Text(x));
        }
        Ok(SqliteValue::Text(x.replace(&y, &z)))
    }

    fn num_args(&self) -> i32 {
        3
    }

    fn name(&self) -> &str {
        "replace"
    }
}

// ── round(X [, N]) ──────────────────────────────────────────────────────

pub struct RoundFunc;

impl ScalarFunction for RoundFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let x = args[0].to_float();
        // Clamp N to [0, 30] matching SQLite behavior.
        let n = if args.len() > 1 && !args[1].is_null() {
            args[1].to_integer().clamp(0, 30)
        } else {
            0
        };
        // Values beyond 2^52 have no fractional part — return unchanged
        if !(-4_503_599_627_370_496.0..=4_503_599_627_370_496.0).contains(&x) {
            return Ok(SqliteValue::Float(x));
        }
        // Round half away from zero (NOT banker's rounding).
        // n is in [0, 30] so the i32 cast is lossless.
        #[allow(clippy::cast_possible_truncation)]
        let factor = 10.0_f64.powi(n as i32);
        let rounded = if x >= 0.0 {
            (x * factor + 0.5).floor() / factor
        } else {
            (x * factor - 0.5).ceil() / factor
        };
        Ok(SqliteValue::Float(rounded))
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
    }

    fn name(&self) -> &str {
        "round"
    }
}

// ── sign(X) ──────────────────────────────────────────────────────────────

pub struct SignFunc;

impl ScalarFunction for SignFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        match &args[0] {
            SqliteValue::Integer(i) => Ok(SqliteValue::Integer(i.signum())),
            SqliteValue::Float(f) => {
                if *f > 0.0 {
                    Ok(SqliteValue::Integer(1))
                } else if *f < 0.0 {
                    Ok(SqliteValue::Integer(-1))
                } else {
                    Ok(SqliteValue::Integer(0))
                }
            }
            SqliteValue::Text(s) => {
                // Non-numeric strings => NULL per spec
                if let Ok(i) = s.parse::<i64>() {
                    Ok(SqliteValue::Integer(i.signum()))
                } else if let Ok(f) = s.parse::<f64>() {
                    if f > 0.0 {
                        Ok(SqliteValue::Integer(1))
                    } else if f < 0.0 {
                        Ok(SqliteValue::Integer(-1))
                    } else {
                        Ok(SqliteValue::Integer(0))
                    }
                } else {
                    Ok(SqliteValue::Null)
                }
            }
            SqliteValue::Blob(_) => Ok(SqliteValue::Null),
            SqliteValue::Null => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sign"
    }
}

// ── random() ─────────────────────────────────────────────────────────────

pub struct RandomFunc;

impl ScalarFunction for RandomFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        // Simple PRNG using thread_rng is fine for SQLite's random()
        // which is explicitly non-cryptographic.
        let val = simple_random_i64();
        Ok(SqliteValue::Integer(val))
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "random"
    }
}

/// Simple deterministic-enough PRNG for SQLite's random().
fn simple_random_i64() -> i64 {
    // Deterministic per-process PRNG (no ambient authority).
    // Not cryptographic, matching SQLite's random()/randomblob() semantics.
    //
    // splitmix64: fast, decent statistical properties, and requires only a u64 state.
    use std::sync::atomic::{AtomicU64, Ordering};

    static STATE: AtomicU64 = AtomicU64::new(0xD1B5_4A32_D192_ED03);
    let mut x = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x as i64
}

// ── randomblob(N) ────────────────────────────────────────────────────────

pub struct RandomblobFunc;

impl ScalarFunction for RandomblobFunc {
    #[allow(clippy::cast_sign_loss)]
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let n = args[0].to_integer().max(0) as usize;
        let mut buf = vec![0u8; n];
        let mut i = 0;
        while i < n {
            let rnd = simple_random_i64().to_ne_bytes();
            let to_copy = (n - i).min(8);
            buf[i..i + to_copy].copy_from_slice(&rnd[..to_copy]);
            i += to_copy;
        }
        Ok(SqliteValue::Blob(buf))
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "randomblob"
    }
}

// ── zeroblob(N) ──────────────────────────────────────────────────────────

pub struct ZeroblobFunc;

impl ScalarFunction for ZeroblobFunc {
    #[allow(clippy::cast_sign_loss)]
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // C SQLite: zeroblob(NULL) returns x'' (empty blob), not NULL.
        if args[0].is_null() {
            return Ok(SqliteValue::Blob(Vec::new()));
        }
        let n = args[0].to_integer().max(0) as usize;
        Ok(SqliteValue::Blob(vec![0u8; n]))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "zeroblob"
    }
}

// ── quote(X) ─────────────────────────────────────────────────────────────

pub struct QuoteFunc;

impl ScalarFunction for QuoteFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let result = match &args[0] {
            SqliteValue::Null => "NULL".to_owned(),
            SqliteValue::Integer(i) => i.to_string(),
            SqliteValue::Float(f) => format!("{f}"),
            SqliteValue::Text(s) => {
                let escaped = s.replace('\'', "''");
                format!("'{escaped}'")
            }
            SqliteValue::Blob(b) => {
                let mut hex = String::with_capacity(3 + b.len() * 2);
                hex.push_str("X'");
                for byte in b {
                    let _ = write!(hex, "{byte:02X}");
                }
                hex.push('\'');
                hex
            }
        };
        Ok(SqliteValue::Text(result))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "quote"
    }
}

// ── unhex(X [, Y]) ──────────────────────────────────────────────────────

pub struct UnhexFunc;

impl ScalarFunction for UnhexFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let input = args[0].to_text();
        let ignore_chars: Vec<char> = if args.len() > 1 && !args[1].is_null() {
            args[1].to_text().chars().collect()
        } else {
            Vec::new()
        };

        // Filter out ignored characters
        let filtered: String = input
            .chars()
            .filter(|c| !ignore_chars.contains(c))
            .collect();

        // Must have even number of hex digits
        if filtered.len() % 2 != 0 {
            return Ok(SqliteValue::Null);
        }

        let mut bytes = Vec::with_capacity(filtered.len() / 2);
        let chars: Vec<char> = filtered.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let hi = match hex_digit(chars[i]) {
                Some(v) => v,
                None => return Ok(SqliteValue::Null),
            };
            let lo = match hex_digit(chars[i + 1]) {
                Some(v) => v,
                None => return Ok(SqliteValue::Null),
            };
            bytes.push(hi << 4 | lo);
            i += 2;
        }
        Ok(SqliteValue::Blob(bytes))
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
    }

    fn name(&self) -> &str {
        "unhex"
    }
}

fn hex_digit(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c as u8 - b'0'),
        'a'..='f' => Some(c as u8 - b'a' + 10),
        'A'..='F' => Some(c as u8 - b'A' + 10),
        _ => None,
    }
}

// ── unicode(X) ───────────────────────────────────────────────────────────

pub struct UnicodeFunc;

impl ScalarFunction for UnicodeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let s = args[0].to_text();
        match s.chars().next() {
            Some(c) => Ok(SqliteValue::Integer(i64::from(c as u32))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "unicode"
    }
}

// ── substr(X, START [, LENGTH]) / substring() ───────────────────────────

pub struct SubstrFunc;

impl ScalarFunction for SubstrFunc {
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let is_blob = matches!(&args[0], SqliteValue::Blob(_));
        if is_blob {
            return self.invoke_blob(args);
        }

        let s = args[0].to_text();
        let chars: Vec<char> = s.chars().collect();
        let char_count = chars.len() as i64;
        let start = args[1].to_integer();
        let has_length = args.len() > 2 && !args[2].is_null();
        let length = if has_length {
            args[2].to_integer()
        } else {
            char_count + 1
        };

        // SQLite substr semantics:
        // start=0 quirk: with length>0, returns max(length-1,0) chars from start
        // negative start counts from end
        // negative length returns chars preceding start

        if length < 0 {
            // Negative length: characters BEFORE the starting position
            let abs_len = length.unsigned_abs();
            let end_pos = if start > 0 {
                (start - 1).min(char_count)
            } else if start == 0 {
                0
            } else {
                (char_count + start + 1).max(0).min(char_count)
            };
            let start_pos = (end_pos - abs_len as i64).max(0);
            let result: String = chars[start_pos as usize..end_pos as usize].iter().collect();
            return Ok(SqliteValue::Text(result));
        }

        let (begin, len) = if start > 0 {
            ((start - 1).max(0) as usize, length as usize)
        } else if start == 0 {
            // START=0 quirk: returns max(length-1, 0) chars from beginning
            (0, (length - 1).max(0) as usize)
        } else {
            // Negative start: count from end
            let effective = char_count + start; // e.g. -2 on "hello"(5) => 3
            if effective < 0 {
                let skip = effective.unsigned_abs() as usize;
                (0, length as usize - skip.min(length as usize))
            } else {
                (effective as usize, length as usize)
            }
        };

        let result: String = chars.iter().skip(begin).take(len).collect();
        Ok(SqliteValue::Text(result))
    }

    fn num_args(&self) -> i32 {
        -1 // 2 or 3 args
    }

    fn name(&self) -> &str {
        "substr"
    }
}

impl SubstrFunc {
    #[allow(clippy::unused_self)]
    fn invoke_blob(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let blob = match &args[0] {
            SqliteValue::Blob(b) => b,
            _ => return Ok(SqliteValue::Null),
        };
        let blob_len = blob.len() as i64;
        let start = args[1].to_integer();
        let has_length = args.len() > 2 && !args[2].is_null();
        let length = if has_length {
            args[2].to_integer()
        } else {
            blob_len + 1
        };

        if length < 0 {
            let abs_len = length.unsigned_abs();
            let end_pos = if start > 0 {
                (start - 1).min(blob_len)
            } else if start == 0 {
                0
            } else {
                (blob_len + start + 1).max(0).min(blob_len)
            };
            let start_pos = (end_pos - abs_len as i64).max(0);
            return Ok(SqliteValue::Blob(
                blob[start_pos as usize..end_pos as usize].to_vec(),
            ));
        }

        let (begin, len) = if start > 0 {
            ((start - 1).max(0) as usize, length as usize)
        } else if start == 0 {
            (0, (length - 1).max(0) as usize)
        } else {
            let effective = blob_len + start;
            if effective < 0 {
                let skip = effective.unsigned_abs() as usize;
                (0, length as usize - skip.min(length as usize))
            } else {
                (effective as usize, length as usize)
            }
        };

        let end = (begin + len).min(blob.len());
        Ok(SqliteValue::Blob(blob[begin..end].to_vec()))
    }
}

// ── soundex(X) ───────────────────────────────────────────────────────────

pub struct SoundexFunc;

impl ScalarFunction for SoundexFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            // SQLite returns "?000" for SOUNDEX(NULL), not NULL.
            return Ok(SqliteValue::Text("?000".to_owned()));
        }
        let s = args[0].to_text();
        Ok(SqliteValue::Text(soundex(&s)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "soundex"
    }
}

fn soundex(s: &str) -> String {
    let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());
    let first = match chars.next() {
        Some(c) => c.to_ascii_uppercase(),
        None => return "?000".to_owned(),
    };

    let code = |c: char| -> Option<char> {
        match c.to_ascii_uppercase() {
            'B' | 'F' | 'P' | 'V' => Some('1'),
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
            'D' | 'T' => Some('3'),
            'L' => Some('4'),
            'M' | 'N' => Some('5'),
            'R' => Some('6'),
            _ => None, // A, E, I, O, U, H, W, Y
        }
    };

    let mut result = String::with_capacity(4);
    result.push(first);
    let mut last_code = code(first);

    for c in chars {
        if result.len() >= 4 {
            break;
        }
        let current = code(c);
        if let Some(digit) = current {
            if current != last_code {
                result.push(digit);
            }
        }
        last_code = current;
    }

    while result.len() < 4 {
        result.push('0');
    }
    result
}

// ── scalar max(X, Y, ...) ───────────────────────────────────────────────

pub struct ScalarMaxFunc;

impl ScalarFunction for ScalarMaxFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // Scalar max: if ANY argument is NULL, returns NULL
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let mut max = &args[0];
        for arg in &args[1..] {
            if arg.partial_cmp(max) == Some(std::cmp::Ordering::Greater) {
                max = arg;
            }
        }
        Ok(max.clone())
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "max"
    }
}

// ── scalar min(X, Y, ...) ───────────────────────────────────────────────

pub struct ScalarMinFunc;

impl ScalarFunction for ScalarMinFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // Scalar min: if ANY argument is NULL, returns NULL
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let mut min = &args[0];
        for arg in &args[1..] {
            if arg.partial_cmp(min) == Some(std::cmp::Ordering::Less) {
                min = arg;
            }
        }
        Ok(min.clone())
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "min"
    }
}

// ── likelihood/likely/unlikely ──────────────────────────────────────────

pub struct LikelihoodFunc;

impl ScalarFunction for LikelihoodFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // Returns X unchanged; P is a planner hint (ignored at runtime).
        Ok(args[0].clone())
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "likelihood"
    }
}

pub struct LikelyFunc;

impl ScalarFunction for LikelyFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(args[0].clone())
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "likely"
    }
}

pub struct UnlikelyFunc;

impl ScalarFunction for UnlikelyFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(args[0].clone())
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "unlikely"
    }
}

// ── sqlite_version() ────────────────────────────────────────────────────

pub struct SqliteVersionFunc;

impl ScalarFunction for SqliteVersionFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Text("3.52.0".to_owned()))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "sqlite_version"
    }
}

// ── sqlite_source_id() ──────────────────────────────────────────────────

pub struct SqliteSourceIdFunc;

impl ScalarFunction for SqliteSourceIdFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Text(
            "FrankenSQLite 0.1.0 (compatible with SQLite 3.52.0)".to_owned(),
        ))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "sqlite_source_id"
    }
}

// ── sqlite_compileoption_used(X) ────────────────────────────────────────

pub struct SqliteCompileoptionUsedFunc;

impl ScalarFunction for SqliteCompileoptionUsedFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let opt = args[0].to_text().to_ascii_uppercase();
        // Report our known compile options
        let known = matches!(
            opt.as_str(),
            "THREADSAFE" | "ENABLE_FTS5" | "ENABLE_JSON1" | "ENABLE_RTREE"
        );
        Ok(SqliteValue::Integer(i64::from(known)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sqlite_compileoption_used"
    }
}

// ── sqlite_compileoption_get(N) ─────────────────────────────────────────

pub struct SqliteCompileoptionGetFunc;

impl ScalarFunction for SqliteCompileoptionGetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let n = args[0].to_integer();
        let options = [
            "THREADSAFE=1",
            "ENABLE_FTS5",
            "ENABLE_JSON1",
            "ENABLE_RTREE",
        ];
        #[allow(clippy::cast_sign_loss)]
        match options.get(n as usize) {
            Some(opt) => Ok(SqliteValue::Text((*opt).to_owned())),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sqlite_compileoption_get"
    }
}

// ── like(PATTERN, STRING [, ESCAPE]) ────────────────────────────────────

pub struct LikeFunc;

impl ScalarFunction for LikeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let pattern = args[0].to_text();
        let string = args[1].to_text();
        let escape = if args.len() > 2 && !args[2].is_null() {
            args[2].to_text().chars().next()
        } else {
            None
        };
        let matched = like_match(&pattern, &string, escape);
        Ok(SqliteValue::Integer(i64::from(matched)))
    }

    fn num_args(&self) -> i32 {
        -1 // 2 or 3 args
    }

    fn name(&self) -> &str {
        "like"
    }
}

/// LIKE pattern matching (case-insensitive for ASCII).
fn like_match(pattern: &str, string: &str, escape: Option<char>) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = string.chars().collect();
    like_match_inner(&pat, &txt, 0, 0, escape)
}

fn like_match_inner(
    pat: &[char],
    txt: &[char],
    mut pi: usize,
    mut ti: usize,
    escape: Option<char>,
) -> bool {
    while pi < pat.len() {
        let pc = pat[pi];

        if Some(pc) == escape {
            // Next char is literal
            pi += 1;
            if pi >= pat.len() {
                return false;
            }
            if ti >= txt.len() {
                return false;
            }
            if !ascii_iequal(pat[pi], txt[ti]) {
                return false;
            }
            pi += 1;
            ti += 1;
            continue;
        }

        match pc {
            '%' => {
                // Skip consecutive %
                while pi < pat.len() && pat[pi] == '%' {
                    pi += 1;
                }
                if pi >= pat.len() {
                    return true; // trailing % matches everything
                }
                // Try matching rest of pattern at every position
                for start in ti..=txt.len() {
                    if like_match_inner(pat, txt, pi, start, escape) {
                        return true;
                    }
                }
                return false;
            }
            '_' => {
                if ti >= txt.len() {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            _ => {
                if ti >= txt.len() {
                    return false;
                }
                if !ascii_iequal(pc, txt[ti]) {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti >= txt.len()
}

fn ascii_iequal(a: char, b: char) -> bool {
    a.to_ascii_lowercase() == b.to_ascii_lowercase()
}

// ── glob(PATTERN, STRING) ───────────────────────────────────────────────

pub struct GlobFunc;

impl ScalarFunction for GlobFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let pattern = args[0].to_text();
        let string = args[1].to_text();
        let matched = glob_match(&pattern, &string);
        Ok(SqliteValue::Integer(i64::from(matched)))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "glob"
    }
}

/// GLOB pattern matching (case-sensitive, * and ? wildcards).
fn glob_match(pattern: &str, string: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = string.chars().collect();
    glob_match_inner(&pat, &txt, 0, 0)
}

fn glob_match_inner(pat: &[char], txt: &[char], mut pi: usize, mut ti: usize) -> bool {
    while pi < pat.len() {
        match pat[pi] {
            '*' => {
                while pi < pat.len() && pat[pi] == '*' {
                    pi += 1;
                }
                if pi >= pat.len() {
                    return true;
                }
                for start in ti..=txt.len() {
                    if glob_match_inner(pat, txt, pi, start) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if ti >= txt.len() {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            '[' => {
                if ti >= txt.len() {
                    return false;
                }
                pi += 1;
                let negate = pi < pat.len() && pat[pi] == '^';
                if negate {
                    pi += 1;
                }
                let mut found = false;
                let mut first = true;
                while pi < pat.len() && (first || pat[pi] != ']') {
                    first = false;
                    if pi + 2 < pat.len() && pat[pi + 1] == '-' {
                        let lo = pat[pi];
                        let hi = pat[pi + 2];
                        if txt[ti] >= lo && txt[ti] <= hi {
                            found = true;
                        }
                        pi += 3;
                    } else {
                        if txt[ti] == pat[pi] {
                            found = true;
                        }
                        pi += 1;
                    }
                }
                if pi < pat.len() && pat[pi] == ']' {
                    pi += 1;
                }
                if found == negate {
                    return false;
                }
                ti += 1;
            }
            c => {
                if ti >= txt.len() || txt[ti] != c {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti >= txt.len()
}

// ── unistr(X) ───────────────────────────────────────────────────────────

pub struct UnistrFunc;

impl ScalarFunction for UnistrFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let input = args[0].to_text();
        let mut result = String::new();
        let chars: Vec<char> = input.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '\\' && i + 1 < chars.len() {
                if chars[i + 1] == 'u' && i + 5 < chars.len() {
                    // \uXXXX
                    let hex: String = chars[i + 2..i + 6].iter().collect();
                    if let Ok(cp) = u32::from_str_radix(&hex, 16) {
                        if let Some(c) = char::from_u32(cp) {
                            result.push(c);
                            i += 6;
                            continue;
                        }
                    }
                } else if chars[i + 1] == 'U' && i + 9 < chars.len() {
                    // \UXXXXXXXX
                    let hex: String = chars[i + 2..i + 10].iter().collect();
                    if let Ok(cp) = u32::from_str_radix(&hex, 16) {
                        if let Some(c) = char::from_u32(cp) {
                            result.push(c);
                            i += 10;
                            continue;
                        }
                    }
                }
            }
            result.push(chars[i]);
            i += 1;
        }
        Ok(SqliteValue::Text(result))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "unistr"
    }
}

// ── Connection-state stubs ──────────────────────────────────────────────
// These functions need database connection context. They are registered
// as placeholders that return NotImplemented until the VDBE integration
// provides the connection state.

pub struct ChangesFunc;

impl ScalarFunction for ChangesFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(LAST_CHANGES.get()))
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "changes"
    }
}

pub struct TotalChangesFunc;

impl ScalarFunction for TotalChangesFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(TOTAL_CHANGES.get()))
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "total_changes"
    }
}

pub struct LastInsertRowidFunc;

impl ScalarFunction for LastInsertRowidFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(LAST_INSERT_ROWID.get()))
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "last_insert_rowid"
    }
}

// ── Register all built-ins ──────────────────────────────────────────────

/// Register all core built-in scalar functions into the given registry.
#[allow(clippy::too_many_lines)]
pub fn register_builtins(registry: &mut FunctionRegistry) {
    // Math
    registry.register_scalar(AbsFunc);
    registry.register_scalar(SignFunc);
    registry.register_scalar(RoundFunc);
    registry.register_scalar(RandomFunc);
    registry.register_scalar(RandomblobFunc);
    registry.register_scalar(ZeroblobFunc);

    // String
    registry.register_scalar(LowerFunc);
    registry.register_scalar(UpperFunc);
    registry.register_scalar(LengthFunc);
    registry.register_scalar(OctetLengthFunc);
    registry.register_scalar(TrimFunc);
    registry.register_scalar(LtrimFunc);
    registry.register_scalar(RtrimFunc);
    registry.register_scalar(ReplaceFunc);
    registry.register_scalar(SubstrFunc);
    registry.register_scalar(InstrFunc);
    registry.register_scalar(CharFunc);
    registry.register_scalar(UnicodeFunc);
    registry.register_scalar(UnistrFunc);
    registry.register_scalar(HexFunc);
    registry.register_scalar(UnhexFunc);
    registry.register_scalar(QuoteFunc);
    registry.register_scalar(SoundexFunc);

    // Type
    registry.register_scalar(TypeofFunc);
    registry.register_scalar(SubtypeFunc);

    // Conditional
    registry.register_scalar(CoalesceFunc);
    registry.register_scalar(IfnullFunc);
    registry.register_scalar(NullifFunc);
    registry.register_scalar(IifFunc);

    // Multi-value
    registry.register_scalar(ConcatFunc);
    registry.register_scalar(ConcatWsFunc);
    registry.register_scalar(ScalarMaxFunc);
    registry.register_scalar(ScalarMinFunc);

    // Planner hints
    registry.register_scalar(LikelihoodFunc);
    registry.register_scalar(LikelyFunc);
    registry.register_scalar(UnlikelyFunc);

    // Pattern matching
    registry.register_scalar(LikeFunc);
    registry.register_scalar(GlobFunc);

    // Meta
    registry.register_scalar(SqliteVersionFunc);
    registry.register_scalar(SqliteSourceIdFunc);
    registry.register_scalar(SqliteCompileoptionUsedFunc);
    registry.register_scalar(SqliteCompileoptionGetFunc);

    // Connection-state stubs
    registry.register_scalar(ChangesFunc);
    registry.register_scalar(TotalChangesFunc);
    registry.register_scalar(LastInsertRowidFunc);

    // "if" is an alias for "iif" (3.48+)
    // Register same function under alternate name
    struct IfFunc;
    impl ScalarFunction for IfFunc {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            IifFunc.invoke(args)
        }

        fn num_args(&self) -> i32 {
            3
        }

        fn name(&self) -> &str {
            "if"
        }
    }
    registry.register_scalar(IfFunc);

    // "substring" is an alias for "substr"
    struct SubstringFunc;
    impl ScalarFunction for SubstringFunc {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            SubstrFunc.invoke(args)
        }

        fn num_args(&self) -> i32 {
            -1
        }

        fn name(&self) -> &str {
            "substring"
        }
    }
    registry.register_scalar(SubstringFunc);

    // "printf" is an alias for "format".
    struct PrintfFunc;
    impl ScalarFunction for PrintfFunc {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            FormatFunc.invoke(args)
        }

        fn num_args(&self) -> i32 {
            -1
        }

        fn name(&self) -> &str {
            "printf"
        }
    }
    registry.register_scalar(FormatFunc);
    registry.register_scalar(PrintfFunc);

    // §13.2 Math functions (acos, asin, atan, ceil, floor, log, pow, sqrt, etc.)
    register_math_builtins(registry);

    // §13.3 Date/time functions (date, time, datetime, julianday, unixepoch, strftime, timediff)
    register_datetime_builtins(registry);

    // §13.4 Aggregate functions (avg, count, group_concat, max, min, sum, total, etc.)
    register_aggregate_builtins(registry);
}

// ── format(FORMAT, ...) / printf(FORMAT, ...) ───────────────────────────

pub struct FormatFunc;

impl ScalarFunction for FormatFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() || args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let fmt_str = args[0].to_text();
        let params = &args[1..];
        let result = sqlite_format(&fmt_str, params)?;
        Ok(SqliteValue::Text(result))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "format"
    }
}

/// Simplified SQLite format/printf implementation.
/// Supports: %d, %f, %e, %g, %s, %q, %Q, %w, %%, %n (no-op).
fn sqlite_format(fmt: &str, params: &[SqliteValue]) -> Result<String> {
    let mut result = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    let mut param_idx = 0;

    while i < chars.len() {
        if chars[i] != '%' {
            result.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        if i >= chars.len() {
            break;
        }

        // Parse flags
        let mut left_align = false;
        let mut show_sign = false;
        let mut space_sign = false;
        let mut zero_pad = false;
        loop {
            if i >= chars.len() {
                break;
            }
            match chars[i] {
                '-' => left_align = true,
                '+' => show_sign = true,
                ' ' => space_sign = true,
                '0' => zero_pad = true,
                _ => break,
            }
            i += 1;
        }

        // Parse width
        let mut width = 0usize;
        while i < chars.len() && chars[i].is_ascii_digit() {
            width = width * 10 + (chars[i] as usize - '0' as usize);
            i += 1;
        }

        // Parse precision
        let mut precision = None;
        if i < chars.len() && chars[i] == '.' {
            i += 1;
            let mut prec = 0usize;
            while i < chars.len() && chars[i].is_ascii_digit() {
                prec = prec * 10 + (chars[i] as usize - '0' as usize);
                i += 1;
            }
            precision = Some(prec);
        }

        if i >= chars.len() {
            break;
        }

        let spec = chars[i];
        i += 1;

        match spec {
            '%' => result.push('%'),
            'n' => {} // no-op (security: never writes to memory)
            'd' | 'i' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                let formatted =
                    format_integer(val, width, left_align, show_sign, space_sign, zero_pad);
                result.push_str(&formatted);
            }
            'f' => {
                let val = params.get(param_idx).map_or(0.0, SqliteValue::to_float);
                param_idx += 1;
                let prec = precision.unwrap_or(6);
                let formatted = format_float_f(
                    val, prec, width, left_align, show_sign, space_sign, zero_pad,
                );
                result.push_str(&formatted);
            }
            'e' | 'E' => {
                let val = params.get(param_idx).map_or(0.0, SqliteValue::to_float);
                param_idx += 1;
                let prec = precision.unwrap_or(6);
                let formatted = if spec == 'e' {
                    format!("{val:.prec$e}")
                } else {
                    format!("{val:.prec$E}")
                };
                result.push_str(&pad_string(&formatted, width, left_align));
            }
            'g' | 'G' => {
                let val = params.get(param_idx).map_or(0.0, SqliteValue::to_float);
                param_idx += 1;
                let _prec = precision.unwrap_or(6);
                // Use shorter of %f and %e
                let formatted = format!("{val}");
                result.push_str(&pad_string(&formatted, width, left_align));
            }
            's' | 'z' => {
                let param = params.get(param_idx);
                param_idx += 1;
                let val = match param {
                    Some(SqliteValue::Null) | None => "(null)".to_owned(),
                    Some(v) => v.to_text(),
                };
                let truncated = if let Some(prec) = precision {
                    val.chars().take(prec).collect::<String>()
                } else {
                    val
                };
                result.push_str(&pad_string(&truncated, width, left_align));
            }
            'q' => {
                // Single-quote escaping; NULL → literal "NULL" (no quotes)
                let param = params.get(param_idx);
                param_idx += 1;
                match param {
                    Some(SqliteValue::Null) | None => result.push_str("NULL"),
                    Some(v) => {
                        let val = v.to_text();
                        let escaped = val.replace('\'', "''");
                        result.push_str(&escaped);
                    }
                }
            }
            'Q' => {
                // Like %q but wrapped in quotes, NULL -> "NULL"
                let param = params.get(param_idx);
                param_idx += 1;
                match param {
                    Some(SqliteValue::Null) | None => result.push_str("NULL"),
                    Some(v) => {
                        let val = v.to_text();
                        let escaped = val.replace('\'', "''");
                        result.push('\'');
                        result.push_str(&escaped);
                        result.push('\'');
                    }
                }
            }
            'w' => {
                // Double-quote escaping for identifiers
                let val = params
                    .get(param_idx)
                    .map(SqliteValue::to_text)
                    .unwrap_or_default();
                param_idx += 1;
                let escaped = val.replace('"', "\"\"");
                result.push('"');
                result.push_str(&escaped);
                result.push('"');
            }
            'x' | 'X' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                #[allow(clippy::cast_sign_loss)]
                let formatted = if spec == 'x' {
                    format!("{:x}", val as u64)
                } else {
                    format!("{:X}", val as u64)
                };
                let padded = if zero_pad && width > formatted.len() {
                    let pad = "0".repeat(width - formatted.len());
                    format!("{pad}{formatted}")
                } else {
                    pad_string(&formatted, width, left_align)
                };
                result.push_str(&padded);
            }
            'o' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                #[allow(clippy::cast_sign_loss)]
                let formatted = format!("{:o}", val as u64);
                let padded = if zero_pad && width > formatted.len() {
                    let pad = "0".repeat(width - formatted.len());
                    format!("{pad}{formatted}")
                } else {
                    pad_string(&formatted, width, left_align)
                };
                result.push_str(&padded);
            }
            'c' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                #[allow(clippy::cast_sign_loss)]
                if let Some(c) = char::from_u32(val as u32) {
                    result.push(c);
                }
            }
            _ => {
                // Unknown specifier: output literally
                result.push('%');
                result.push(spec);
            }
        }
        // Suppress unused warnings
        let _ = (left_align, show_sign, space_sign, zero_pad);
    }
    Ok(result)
}

fn format_integer(
    val: i64,
    width: usize,
    left_align: bool,
    show_sign: bool,
    space_sign: bool,
    zero_pad: bool,
) -> String {
    let sign = if val < 0 {
        "-".to_owned()
    } else if show_sign {
        "+".to_owned()
    } else if space_sign {
        " ".to_owned()
    } else {
        String::new()
    };
    let digits = format!("{}", val.unsigned_abs());
    let body = format!("{sign}{digits}");
    if body.len() >= width {
        return body;
    }
    let pad = width - body.len();
    if left_align {
        format!("{body}{}", " ".repeat(pad))
    } else if zero_pad {
        format!("{sign}{}{digits}", "0".repeat(pad))
    } else {
        format!("{}{body}", " ".repeat(pad))
    }
}

fn format_float_f(
    val: f64,
    prec: usize,
    width: usize,
    left_align: bool,
    show_sign: bool,
    space_sign: bool,
    zero_pad: bool,
) -> String {
    let sign = if val < 0.0 {
        "-".to_owned()
    } else if show_sign {
        "+".to_owned()
    } else if space_sign {
        " ".to_owned()
    } else {
        String::new()
    };
    let digits = format!("{:.prec$}", val.abs());
    let body = format!("{sign}{digits}");
    if body.len() >= width {
        return body;
    }
    let pad = width - body.len();
    if left_align {
        format!("{body}{}", " ".repeat(pad))
    } else if zero_pad {
        format!("{sign}{}{digits}", "0".repeat(pad))
    } else {
        format!("{}{body}", " ".repeat(pad))
    }
}

fn pad_string(s: &str, width: usize, left_align: bool) -> String {
    if s.len() >= width {
        return s.to_owned();
    }
    let pad = width - s.len();
    if left_align {
        format!("{s}{}", " ".repeat(pad))
    } else {
        format!("{}{s}", " ".repeat(pad))
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;

    fn invoke1(f: &dyn ScalarFunction, v: SqliteValue) -> Result<SqliteValue> {
        f.invoke(&[v])
    }

    fn invoke2(f: &dyn ScalarFunction, a: SqliteValue, b: SqliteValue) -> Result<SqliteValue> {
        f.invoke(&[a, b])
    }

    // ── abs ──────────────────────────────────────────────────────────────

    #[test]
    fn test_abs_positive() {
        assert_eq!(
            invoke1(&AbsFunc, SqliteValue::Integer(42)).unwrap(),
            SqliteValue::Integer(42)
        );
    }

    #[test]
    fn test_abs_negative() {
        assert_eq!(
            invoke1(&AbsFunc, SqliteValue::Integer(-42)).unwrap(),
            SqliteValue::Integer(42)
        );
    }

    #[test]
    fn test_abs_null() {
        assert_eq!(
            invoke1(&AbsFunc, SqliteValue::Null).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_abs_min_i64_overflow() {
        let err = invoke1(&AbsFunc, SqliteValue::Integer(i64::MIN)).unwrap_err();
        assert!(matches!(err, FrankenError::IntegerOverflow));
    }

    #[test]
    fn test_abs_string_coercion() {
        assert_eq!(
            invoke1(&AbsFunc, SqliteValue::Text("-7.5".to_owned())).unwrap(),
            SqliteValue::Float(7.5)
        );
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_abs_float() {
        assert_eq!(
            invoke1(&AbsFunc, SqliteValue::Float(-3.14)).unwrap(),
            SqliteValue::Float(3.14)
        );
    }

    // ── char ─────────────────────────────────────────────────────────────

    #[test]
    fn test_char_basic() {
        let f = CharFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(72),
                SqliteValue::Integer(101),
                SqliteValue::Integer(108),
                SqliteValue::Integer(108),
                SqliteValue::Integer(111),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("Hello".to_owned()));
    }

    #[test]
    fn test_char_null_skipped() {
        let f = CharFunc;
        // C SQLite: NULL → sqlite3_value_int()=0 → U+0000 (NUL byte).
        let result = f
            .invoke(&[
                SqliteValue::Integer(65),
                SqliteValue::Null,
                SqliteValue::Integer(66),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("A\0B".to_owned()));
    }

    // ── coalesce ─────────────────────────────────────────────────────────

    #[test]
    fn test_coalesce_first_non_null() {
        let f = CoalesceFunc;
        let result = f
            .invoke(&[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Integer(3),
                SqliteValue::Integer(4),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Integer(3));
    }

    // ── concat ───────────────────────────────────────────────────────────

    #[test]
    fn test_concat_null_as_empty() {
        let f = ConcatFunc;
        let result = f
            .invoke(&[
                SqliteValue::Null,
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Null,
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("hello".to_owned()));
    }

    // ── concat_ws ────────────────────────────────────────────────────────

    #[test]
    fn test_concat_ws_null_skipped() {
        let f = ConcatWsFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(",".to_owned()),
                SqliteValue::Text("a".to_owned()),
                SqliteValue::Null,
                SqliteValue::Text("b".to_owned()),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("a,b".to_owned()));
    }

    // ── hex ──────────────────────────────────────────────────────────────

    #[test]
    fn test_hex_blob() {
        let result = invoke1(&HexFunc, SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])).unwrap();
        assert_eq!(result, SqliteValue::Text("DEADBEEF".to_owned()));
    }

    #[test]
    fn test_hex_number_via_text() {
        // hex(42) encodes '42' as UTF-8 hex, not raw bits
        let result = invoke1(&HexFunc, SqliteValue::Integer(42)).unwrap();
        assert_eq!(result, SqliteValue::Text("3432".to_owned()));
    }

    // ── iif ──────────────────────────────────────────────────────────────

    #[test]
    fn test_iif_true() {
        let f = IifFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(1),
                SqliteValue::Text("yes".to_owned()),
                SqliteValue::Text("no".to_owned()),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("yes".to_owned()));
    }

    #[test]
    fn test_iif_false() {
        let f = IifFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(0),
                SqliteValue::Text("yes".to_owned()),
                SqliteValue::Text("no".to_owned()),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("no".to_owned()));
    }

    // ── ifnull ───────────────────────────────────────────────────────────

    #[test]
    fn test_ifnull_non_null() {
        assert_eq!(
            invoke2(
                &IfnullFunc,
                SqliteValue::Integer(5),
                SqliteValue::Integer(10)
            )
            .unwrap(),
            SqliteValue::Integer(5)
        );
    }

    #[test]
    fn test_ifnull_null() {
        assert_eq!(
            invoke2(&IfnullFunc, SqliteValue::Null, SqliteValue::Integer(10)).unwrap(),
            SqliteValue::Integer(10)
        );
    }

    // ── instr ────────────────────────────────────────────────────────────

    #[test]
    fn test_instr_found() {
        assert_eq!(
            invoke2(
                &InstrFunc,
                SqliteValue::Text("hello world".to_owned()),
                SqliteValue::Text("world".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(7)
        );
    }

    #[test]
    fn test_instr_not_found() {
        assert_eq!(
            invoke2(
                &InstrFunc,
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Text("xyz".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_instr_empty_needle_returns_one() {
        // SQLite: instr(X, '') returns 1 (empty string found at position 1).
        assert_eq!(
            invoke2(
                &InstrFunc,
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Text(String::new())
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    #[test]
    fn test_instr_empty_haystack_returns_zero() {
        assert_eq!(
            invoke2(
                &InstrFunc,
                SqliteValue::Text(String::new()),
                SqliteValue::Text("x".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_instr_blob_empty_needle_returns_one() {
        // SQLite: instr(X, x'') returns 1 (empty blob found at position 1).
        assert_eq!(
            invoke2(
                &InstrFunc,
                SqliteValue::Blob(vec![1, 2, 3]),
                SqliteValue::Blob(vec![])
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    // ── length ───────────────────────────────────────────────────────────

    #[test]
    fn test_length_text_chars() {
        // café is 4 characters, 5 bytes
        assert_eq!(
            invoke1(&LengthFunc, SqliteValue::Text("café".to_owned())).unwrap(),
            SqliteValue::Integer(4)
        );
    }

    #[test]
    fn test_length_blob_bytes() {
        assert_eq!(
            invoke1(&LengthFunc, SqliteValue::Blob(vec![1, 2])).unwrap(),
            SqliteValue::Integer(2)
        );
    }

    // ── octet_length ─────────────────────────────────────────────────────

    #[test]
    fn test_octet_length_multibyte() {
        // café: 'c'=1, 'a'=1, 'f'=1, 'é'=2 bytes = 5 bytes total
        assert_eq!(
            invoke1(&OctetLengthFunc, SqliteValue::Text("café".to_owned())).unwrap(),
            SqliteValue::Integer(5)
        );
    }

    // ── lower/upper ──────────────────────────────────────────────────────

    #[test]
    fn test_lower_ascii() {
        assert_eq!(
            invoke1(&LowerFunc, SqliteValue::Text("HELLO".to_owned())).unwrap(),
            SqliteValue::Text("hello".to_owned())
        );
    }

    #[test]
    fn test_upper_ascii() {
        assert_eq!(
            invoke1(&UpperFunc, SqliteValue::Text("hello".to_owned())).unwrap(),
            SqliteValue::Text("HELLO".to_owned())
        );
    }

    // ── trim/ltrim/rtrim ─────────────────────────────────────────────────

    #[test]
    fn test_trim_default() {
        let f = TrimFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Text("  hello  ".to_owned())])
                .unwrap(),
            SqliteValue::Text("hello".to_owned())
        );
    }

    #[test]
    fn test_ltrim_default() {
        let f = LtrimFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Text("  hello".to_owned())])
                .unwrap(),
            SqliteValue::Text("hello".to_owned())
        );
    }

    #[test]
    fn test_ltrim_custom() {
        let f = LtrimFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("xxhello".to_owned()),
                SqliteValue::Text("x".to_owned()),
            ])
            .unwrap(),
            SqliteValue::Text("hello".to_owned())
        );
    }

    // ── nullif ───────────────────────────────────────────────────────────

    #[test]
    fn test_nullif_equal() {
        assert_eq!(
            invoke2(
                &NullifFunc,
                SqliteValue::Integer(5),
                SqliteValue::Integer(5)
            )
            .unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_nullif_different() {
        assert_eq!(
            invoke2(
                &NullifFunc,
                SqliteValue::Integer(5),
                SqliteValue::Integer(3)
            )
            .unwrap(),
            SqliteValue::Integer(5)
        );
    }

    // ── typeof ───────────────────────────────────────────────────────────

    #[test]
    fn test_typeof_each() {
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Null).unwrap(),
            SqliteValue::Text("null".to_owned())
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Integer(1)).unwrap(),
            SqliteValue::Text("integer".to_owned())
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Float(1.0)).unwrap(),
            SqliteValue::Text("real".to_owned())
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Text("x".to_owned())).unwrap(),
            SqliteValue::Text("text".to_owned())
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Blob(vec![0])).unwrap(),
            SqliteValue::Text("blob".to_owned())
        );
    }

    // ── subtype ──────────────────────────────────────────────────────────

    #[test]
    fn test_subtype_null_returns_zero() {
        assert_eq!(
            invoke1(&SubtypeFunc, SqliteValue::Null).unwrap(),
            SqliteValue::Integer(0)
        );
    }

    // ── replace ──────────────────────────────────────────────────────────

    #[test]
    fn test_replace_basic() {
        let f = ReplaceFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("hello world".to_owned()),
                SqliteValue::Text("world".to_owned()),
                SqliteValue::Text("earth".to_owned()),
            ])
            .unwrap(),
            SqliteValue::Text("hello earth".to_owned())
        );
    }

    #[test]
    fn test_replace_empty_y() {
        let f = ReplaceFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Text(String::new()),
                SqliteValue::Text("x".to_owned()),
            ])
            .unwrap(),
            SqliteValue::Text("hello".to_owned())
        );
    }

    // ── round ────────────────────────────────────────────────────────────

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_round_half_away() {
        // round(2.5) = 3.0, round(-2.5) = -3.0
        assert_eq!(
            RoundFunc.invoke(&[SqliteValue::Float(2.5)]).unwrap(),
            SqliteValue::Float(3.0)
        );
        assert_eq!(
            RoundFunc.invoke(&[SqliteValue::Float(-2.5)]).unwrap(),
            SqliteValue::Float(-3.0)
        );
    }

    #[test]
    #[allow(clippy::float_cmp, clippy::approx_constant)]
    fn test_round_precision() {
        assert_eq!(
            RoundFunc
                .invoke(&[SqliteValue::Float(3.14159), SqliteValue::Integer(2)])
                .unwrap(),
            SqliteValue::Float(3.14)
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_round_extreme_n_clamped() {
        // N > 30 is clamped to 30 (matches C SQLite)
        assert_eq!(
            RoundFunc
                .invoke(&[SqliteValue::Float(1.5), SqliteValue::Integer(400)])
                .unwrap(),
            RoundFunc
                .invoke(&[SqliteValue::Float(1.5), SqliteValue::Integer(30)])
                .unwrap(),
        );
        // Negative N is clamped to 0 (matches C SQLite)
        assert_eq!(
            RoundFunc
                .invoke(&[SqliteValue::Float(2.5), SqliteValue::Integer(-5)])
                .unwrap(),
            SqliteValue::Float(3.0)
        );
        // i64::MAX is clamped to 30
        let result = RoundFunc
            .invoke(&[SqliteValue::Float(1.5), SqliteValue::Integer(i64::MAX)])
            .unwrap();
        if let SqliteValue::Float(v) = result {
            assert!(!v.is_nan(), "round must never return NaN");
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_round_large_value_no_fractional() {
        // Values beyond 2^52 have no fractional part — returned unchanged
        let big = 9_007_199_254_740_993.0_f64;
        assert_eq!(
            RoundFunc.invoke(&[SqliteValue::Float(big)]).unwrap(),
            SqliteValue::Float(big)
        );
        assert_eq!(
            RoundFunc.invoke(&[SqliteValue::Float(-big)]).unwrap(),
            SqliteValue::Float(-big)
        );
    }

    // ── sign ─────────────────────────────────────────────────────────────

    #[test]
    fn test_sign_positive() {
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Integer(42)).unwrap(),
            SqliteValue::Integer(1)
        );
    }

    #[test]
    fn test_sign_negative() {
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Integer(-42)).unwrap(),
            SqliteValue::Integer(-1)
        );
    }

    #[test]
    fn test_sign_zero() {
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Integer(0)).unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_sign_null() {
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Null).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_sign_non_numeric() {
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Text("abc".to_owned())).unwrap(),
            SqliteValue::Null
        );
    }

    // ── scalar max/min ───────────────────────────────────────────────────

    #[test]
    fn test_scalar_max_null() {
        let f = ScalarMaxFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(1),
                SqliteValue::Null,
                SqliteValue::Integer(3),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_scalar_max_values() {
        let f = ScalarMaxFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(3),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Integer(3));
    }

    #[test]
    fn test_scalar_min_null() {
        let f = ScalarMinFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(1),
                SqliteValue::Null,
                SqliteValue::Integer(3),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    // ── quote ────────────────────────────────────────────────────────────

    #[test]
    fn test_quote_text() {
        assert_eq!(
            invoke1(&QuoteFunc, SqliteValue::Text("it's".to_owned())).unwrap(),
            SqliteValue::Text("'it''s'".to_owned())
        );
    }

    #[test]
    fn test_quote_null() {
        assert_eq!(
            invoke1(&QuoteFunc, SqliteValue::Null).unwrap(),
            SqliteValue::Text("NULL".to_owned())
        );
    }

    #[test]
    fn test_quote_blob() {
        assert_eq!(
            invoke1(&QuoteFunc, SqliteValue::Blob(vec![0xAB])).unwrap(),
            SqliteValue::Text("X'AB'".to_owned())
        );
    }

    // ── random ───────────────────────────────────────────────────────────

    #[test]
    fn test_random_range() {
        let f = RandomFunc;
        let result = f.invoke(&[]).unwrap();
        assert!(matches!(result, SqliteValue::Integer(_)));
    }

    // ── randomblob ───────────────────────────────────────────────────────

    #[test]
    fn test_randomblob_length() {
        let result = invoke1(&RandomblobFunc, SqliteValue::Integer(16)).unwrap();
        match result {
            SqliteValue::Blob(b) => assert_eq!(b.len(), 16),
            other => unreachable!("expected blob, got {other:?}"),
        }
    }

    // ── zeroblob ─────────────────────────────────────────────────────────

    #[test]
    fn test_zeroblob_length() {
        let result = invoke1(&ZeroblobFunc, SqliteValue::Integer(100)).unwrap();
        match result {
            SqliteValue::Blob(b) => {
                assert_eq!(b.len(), 100);
                assert!(b.iter().all(|&x| x == 0));
            }
            other => unreachable!("expected blob, got {other:?}"),
        }
    }

    // ── unhex ────────────────────────────────────────────────────────────

    #[test]
    fn test_unhex_valid() {
        let result = invoke1(&UnhexFunc, SqliteValue::Text("48656C6C6F".to_owned())).unwrap();
        assert_eq!(result, SqliteValue::Blob(b"Hello".to_vec()));
    }

    #[test]
    fn test_unhex_invalid() {
        let result = invoke1(&UnhexFunc, SqliteValue::Text("ZZZZ".to_owned())).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_unhex_ignore_chars() {
        let f = UnhexFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text("48-65-6C".to_owned()),
                SqliteValue::Text("-".to_owned()),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Blob(b"Hel".to_vec()));
    }

    // ── unicode ──────────────────────────────────────────────────────────

    #[test]
    fn test_unicode_first_char() {
        assert_eq!(
            invoke1(&UnicodeFunc, SqliteValue::Text("A".to_owned())).unwrap(),
            SqliteValue::Integer(65)
        );
    }

    // ── soundex ──────────────────────────────────────────────────────────

    #[test]
    fn test_soundex_basic() {
        assert_eq!(
            invoke1(&SoundexFunc, SqliteValue::Text("Robert".to_owned())).unwrap(),
            SqliteValue::Text("R163".to_owned())
        );
    }

    // ── substr ───────────────────────────────────────────────────────────

    #[test]
    fn test_substr_basic() {
        let f = SubstrFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Integer(2),
                SqliteValue::Integer(3),
            ])
            .unwrap(),
            SqliteValue::Text("ell".to_owned())
        );
    }

    #[test]
    fn test_substr_start_zero_quirk() {
        // substr('hello', 0, 3) returns 2 chars from start
        let f = SubstrFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Integer(0),
                SqliteValue::Integer(3),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("he".to_owned()));
    }

    #[test]
    fn test_substr_negative_start() {
        // substr('hello', -2) = 'lo'
        let f = SubstrFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Integer(-2),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("lo".to_owned()));
    }

    // ── like ─────────────────────────────────────────────────────────────

    #[test]
    fn test_like_case_insensitive() {
        assert_eq!(
            invoke2(
                &LikeFunc,
                SqliteValue::Text("ABC".to_owned()),
                SqliteValue::Text("abc".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    #[test]
    fn test_like_escape() {
        let f = LikeFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text("10\\%".to_owned()),
                SqliteValue::Text("10%".to_owned()),
                SqliteValue::Text("\\".to_owned()),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_like_percent() {
        assert_eq!(
            invoke2(
                &LikeFunc,
                SqliteValue::Text("%ell%".to_owned()),
                SqliteValue::Text("Hello".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    // ── glob ─────────────────────────────────────────────────────────────

    #[test]
    fn test_glob_star() {
        assert_eq!(
            invoke2(
                &GlobFunc,
                SqliteValue::Text("*.txt".to_owned()),
                SqliteValue::Text("file.txt".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    #[test]
    fn test_glob_case_sensitive() {
        assert_eq!(
            invoke2(
                &GlobFunc,
                SqliteValue::Text("ABC".to_owned()),
                SqliteValue::Text("abc".to_owned())
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
    }

    // ── format ───────────────────────────────────────────────────────────

    #[test]
    fn test_format_specifiers() {
        let f = FormatFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text("%d %s".to_owned()),
                SqliteValue::Integer(42),
                SqliteValue::Text("hello".to_owned()),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("42 hello".to_owned()));
    }

    #[test]
    fn test_format_n_noop() {
        let f = FormatFunc;
        // %n should not crash or do anything
        let result = f
            .invoke(&[SqliteValue::Text("before%nafter".to_owned())])
            .unwrap();
        assert_eq!(result, SqliteValue::Text("beforeafter".to_owned()));
    }

    // ── sqlite_version ───────────────────────────────────────────────────

    #[test]
    fn test_sqlite_version_format() {
        let result = SqliteVersionFunc.invoke(&[]).unwrap();
        match result {
            SqliteValue::Text(v) => {
                assert_eq!(v.split('.').count(), 3, "version must be N.N.N format");
            }
            other => unreachable!("expected text, got {other:?}"),
        }
    }

    // ── register_builtins ────────────────────────────────────────────────

    #[test]
    fn test_register_builtins_all_present() {
        let mut registry = FunctionRegistry::new();
        register_builtins(&mut registry);

        // Spot-check key functions are registered
        assert!(registry.find_scalar("abs", 1).is_some());
        assert!(registry.find_scalar("typeof", 1).is_some());
        assert!(registry.find_scalar("length", 1).is_some());
        assert!(registry.find_scalar("lower", 1).is_some());
        assert!(registry.find_scalar("upper", 1).is_some());
        assert!(registry.find_scalar("hex", 1).is_some());
        assert!(registry.find_scalar("coalesce", 3).is_some());
        assert!(registry.find_scalar("concat", 2).is_some());
        assert!(registry.find_scalar("like", 2).is_some());
        assert!(registry.find_scalar("glob", 2).is_some());
        assert!(registry.find_scalar("round", 1).is_some());
        assert!(registry.find_scalar("substr", 2).is_some());
        assert!(registry.find_scalar("substring", 3).is_some());
        assert!(registry.find_scalar("sqlite_version", 0).is_some());
        assert!(registry.find_scalar("iif", 3).is_some());
        assert!(registry.find_scalar("if", 3).is_some());
        assert!(registry.find_scalar("format", 1).is_some());
        assert!(registry.find_scalar("printf", 1).is_some());
        assert!(registry.find_scalar("max", 2).is_some());
        assert!(registry.find_scalar("min", 2).is_some());
        assert!(registry.find_scalar("sign", 1).is_some());
        assert!(registry.find_scalar("random", 0).is_some());

        // Newer SQLite scalar functions (3.41+)
        assert!(registry.find_scalar("concat_ws", 3).is_some());
        assert!(registry.find_scalar("octet_length", 1).is_some());
        assert!(registry.find_scalar("unhex", 1).is_some());
        assert!(registry.find_scalar("timediff", 2).is_some());
        assert!(registry.find_scalar("unistr", 1).is_some());

        // Percentile family enabled by default.
        assert!(registry.find_aggregate("median", 1).is_some());
        assert!(registry.find_aggregate("percentile", 2).is_some());
        assert!(registry.find_aggregate("percentile_cont", 2).is_some());
        assert!(registry.find_aggregate("percentile_disc", 2).is_some());

        // Loadable extensions are not exposed as SQL function by default.
        assert!(registry.find_scalar("load_extension", 1).is_none());
        assert!(registry.find_scalar("load_extension", 2).is_none());
    }

    #[test]
    fn test_e2e_registry_invoke_through_lookup() {
        let mut registry = FunctionRegistry::new();
        register_builtins(&mut registry);

        // Look up abs, invoke it
        let abs = registry.find_scalar("ABS", 1).unwrap();
        assert_eq!(
            abs.invoke(&[SqliteValue::Integer(-42)]).unwrap(),
            SqliteValue::Integer(42)
        );

        // Look up typeof, invoke it
        let typeof_fn = registry.find_scalar("typeof", 1).unwrap();
        assert_eq!(
            typeof_fn
                .invoke(&[SqliteValue::Text("hello".to_owned())])
                .unwrap(),
            SqliteValue::Text("text".to_owned())
        );

        // Look up coalesce (variadic), invoke with 4 args
        let coalesce = registry.find_scalar("COALESCE", 4).unwrap();
        assert_eq!(
            coalesce
                .invoke(&[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Integer(42),
                    SqliteValue::Integer(99),
                ])
                .unwrap(),
            SqliteValue::Integer(42)
        );
    }

    // ── bd-13r.8: Non-Deterministic Function Evaluation Semantics ──

    #[test]
    fn test_nondeterministic_functions_flagged() {
        // These functions MUST be marked non-deterministic to prevent
        // unsafe planner optimizations (hoisting, CSE).
        assert!(!RandomFunc.is_deterministic());
        assert!(!RandomblobFunc.is_deterministic());
        assert!(!ChangesFunc.is_deterministic());
        assert!(!TotalChangesFunc.is_deterministic());
        assert!(!LastInsertRowidFunc.is_deterministic());
    }

    #[test]
    fn test_deterministic_functions_flagged() {
        // Deterministic functions are safe for constant folding/CSE.
        assert!(AbsFunc.is_deterministic());
        assert!(LengthFunc.is_deterministic());
        assert!(TypeofFunc.is_deterministic());
        assert!(UpperFunc.is_deterministic());
        assert!(LowerFunc.is_deterministic());
        assert!(HexFunc.is_deterministic());
        assert!(CoalesceFunc.is_deterministic());
        assert!(IifFunc.is_deterministic());
    }

    #[test]
    fn test_random_produces_different_values() {
        // random() should produce different values on successive calls
        // (verifying per-call evaluation, not constant folding).
        let a = RandomFunc.invoke(&[]).unwrap();
        let b = RandomFunc.invoke(&[]).unwrap();
        // With overwhelming probability, two random i64 values differ.
        // If they're ever equal, it's a 1-in-2^64 coincidence.
        assert_ne!(a.as_integer(), b.as_integer());
    }

    #[test]
    fn test_registry_nondeterministic_lookup() {
        let mut registry = FunctionRegistry::default();
        register_builtins(&mut registry);

        // Non-deterministic functions should be findable and flagged.
        let random = registry.find_scalar("random", 0).unwrap();
        assert!(!random.is_deterministic());

        let changes = registry.find_scalar("changes", 0).unwrap();
        assert!(!changes.is_deterministic());

        let lir = registry.find_scalar("last_insert_rowid", 0).unwrap();
        assert!(!lir.is_deterministic());

        // Deterministic function check.
        let abs = registry.find_scalar("abs", 1).unwrap();
        assert!(abs.is_deterministic());
    }
}
