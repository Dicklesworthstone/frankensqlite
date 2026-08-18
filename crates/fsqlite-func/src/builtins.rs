//! Built-in core scalar functions (§13.1).
//!
//! Implements 60+ SQLite scalar functions with exact NULL-propagation
//! semantics. The connection-state helpers `changes()`, `total_changes()`,
//! and `last_insert_rowid()` are projected through thread-local connection
//! state. `sqlite_offset()` remains unwired.
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

use std::borrow::Cow;
use std::fmt::Write as _;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::value::{format_sqlite_float, sql_like_cased};
use fsqlite_types::{SmallText, SqliteValue, TextEncoding};

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
    /// `PRAGMA case_sensitive_like` for the connection whose statement is
    /// currently executing on this thread. `false` (the default) folds ASCII
    /// case in LIKE; `true` makes LIKE byte-exact. Set by the Connection before
    /// each statement (see `sync_change_tracking_context`); read by `LikeFunc`
    /// and other LIKE evaluation paths so the pragma never has to be threaded
    /// through every call site.
    static CASE_SENSITIVE_LIKE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The `'now'` Julian-day value captured once for the statement currently
    /// executing on this thread. C SQLite reads the wall clock exactly once per
    /// `sqlite3_step()` and reuses it for every `'now'`/`CURRENT_*` within that
    /// statement, so `julianday('now')` is stable across the rows a single
    /// statement produces. `None` means "not captured yet this statement"; the
    /// Connection resets it to `None` at each statement start (see
    /// `sync_change_tracking_context`) and the datetime path captures it lazily
    /// on the first `'now'` use.
    static STATEMENT_NOW: std::cell::Cell<Option<f64>> = const { std::cell::Cell::new(None) };
    /// The database TEXT encoding for the connection whose statement is
    /// currently executing on this thread (bd-iubwb). `octet_length(X)` must
    /// report the byte length of X's TEXT representation *in the database
    /// encoding*, so a UTF-16 database counts each code unit as two bytes.
    /// Defaults to `Utf8` (the common case, byte-identical to today), and is
    /// projected by the Connection before each statement (see
    /// `sync_change_tracking_context`) and by the VDBE engine before every
    /// scalar-function invocation, mirroring how `CASE_SENSITIVE_LIKE` is
    /// threaded so the encoding never has to be passed through every call site.
    static STATEMENT_TEXT_ENCODING: std::cell::Cell<TextEncoding> =
        const { std::cell::Cell::new(TextEncoding::Utf8) };
}

/// Reset the captured statement `'now'` (called by the Connection at each
/// statement start so the next statement re-reads the wall clock).
pub fn reset_statement_now() {
    STATEMENT_NOW.set(None);
}

/// The `'now'` value already captured for the current statement, if any.
#[must_use]
pub fn statement_now() -> Option<f64> {
    STATEMENT_NOW.with(std::cell::Cell::get)
}

/// Record the statement `'now'` captured on its first use this statement.
pub fn set_statement_now(now_jdn: f64) {
    STATEMENT_NOW.set(Some(now_jdn));
}

/// Set the active `case_sensitive_like` flag for LIKE evaluation on this thread
/// (called by the Connection before executing a statement).
pub fn set_case_sensitive_like(case_sensitive: bool) {
    CASE_SENSITIVE_LIKE.set(case_sensitive);
}

/// Read the active `case_sensitive_like` flag for LIKE evaluation on this thread.
#[must_use]
pub fn case_sensitive_like_active() -> bool {
    CASE_SENSITIVE_LIKE.get()
}

/// Set the active database TEXT encoding for the statement on this thread.
///
/// Called by the Connection before executing a statement and by the VDBE engine
/// before invoking a scalar function. Read by `octet_length()`.
pub fn set_statement_text_encoding(encoding: TextEncoding) {
    STATEMENT_TEXT_ENCODING.set(encoding);
}

/// Read the active database TEXT encoding for the current statement's thread.
/// Defaults to [`TextEncoding::Utf8`] when nothing has been projected.
#[must_use]
pub fn statement_text_encoding() -> TextEncoding {
    STATEMENT_TEXT_ENCODING.get()
}

/// Byte length of `text` when serialized in the database `encoding`. UTF-8 is
/// the string's own byte length; UTF-16 (either endianness) is two bytes per
/// UTF-16 code unit, which counts a non-BMP scalar (a surrogate pair) as four
/// bytes exactly as SQLite does.
#[must_use]
fn text_octet_length(text: &str, encoding: TextEncoding) -> usize {
    match encoding {
        TextEncoding::Utf8 => text.len(),
        TextEncoding::Utf16le | TextEncoding::Utf16be => 2 * text.encode_utf16().count(),
    }
}

/// Connection-scoped change-tracking state projected into builtin execution context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeTrackingState {
    pub last_insert_rowid: i64,
    pub last_changes: i64,
    pub total_changes: i64,
}

/// Replace the full builtin change-tracking context.
pub fn set_change_tracking_state(state: ChangeTrackingState) {
    LAST_INSERT_ROWID.set(state.last_insert_rowid);
    LAST_CHANGES.set(state.last_changes);
    TOTAL_CHANGES.set(state.total_changes);
}

/// Read the current builtin change-tracking context for this thread.
#[must_use]
pub fn get_change_tracking_state() -> ChangeTrackingState {
    ChangeTrackingState {
        last_insert_rowid: LAST_INSERT_ROWID.get(),
        last_changes: LAST_CHANGES.get(),
        total_changes: TOTAL_CHANGES.get(),
    }
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

const SQLITE_COMPILE_OPTIONS: &[&str] = &[
    "COMPILER=rustc",
    #[cfg(feature = "ext-fts5")]
    "ENABLE_FTS5",
    #[cfg(feature = "ext-geopoly")]
    "ENABLE_GEOPOLY",
    #[cfg(feature = "ext-icu")]
    "ENABLE_ICU",
    #[cfg(feature = "ext-json")]
    "ENABLE_JSON1",
    #[cfg(feature = "ext-rtree")]
    "ENABLE_RTREE",
    "FRANKENSQLITE",
    "OMIT_LOAD_EXTENSION",
    "THREADSAFE=1",
];

/// Return the canonical compile-option surface exposed by FrankenSQLite.
#[must_use]
pub fn sqlite_compile_options() -> &'static [&'static str] {
    SQLITE_COMPILE_OPTIONS
}

fn is_sqlite_compile_option_match(query: &str, option: &str) -> bool {
    let trimmed = query.trim();
    let normalized = if trimmed
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SQLITE_"))
    {
        &trimmed[7..]
    } else {
        trimmed
    };
    if normalized.is_empty() {
        return false;
    }
    if option.eq_ignore_ascii_case(normalized) {
        return true;
    }
    option
        .get(..normalized.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(normalized))
        && option
            .as_bytes()
            .get(normalized.len())
            .is_none_or(|next| !next.is_ascii_alphanumeric() && *next != b'_')
}

/// Report whether the given SQLite-style compile-option query matches the
/// current FrankenSQLite build surface.
#[must_use]
pub fn sqlite_compileoption_used(query: &str) -> bool {
    sqlite_compile_options()
        .iter()
        .any(|option| is_sqlite_compile_option_match(query, option))
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

// ── abs(X) ────────────────────────────────────────────────────────────────

pub struct AbsFunc;

impl ScalarFunction for AbsFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        match &args[0] {
            SqliteValue::Integer(i) => {
                if *i == i64::MIN {
                    return Err(FrankenError::IntegerOverflow);
                }
                Ok(SqliteValue::Integer(i.abs()))
            }
            other => {
                let f = other.to_float();
                // Match C SQLite: abs uses `x < 0 ? -x : x`.
                // IEEE 754: -0.0 < 0.0 is false, so abs(-0.0) == -0.0.
                Ok(SqliteValue::Float(if f < 0.0 { -f } else { f }))
            }
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
            let ch = u32::try_from(arg.to_integer())
                .ok()
                .and_then(char::from_u32)
                .unwrap_or(char::REPLACEMENT_CHARACTER);
            result.push(ch);
        }
        Ok(SqliteValue::Text(SmallText::from_string(result)))
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

    fn min_args(&self) -> i32 {
        2
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
                result.push_str(text_arg(arg).as_ref());
            }
        }
        Ok(SqliteValue::Text(SmallText::from_string(result)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
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
            return Ok(SqliteValue::Text(SmallText::new("")));
        }
        // C SQLite: concat_ws(NULL, ...) returns NULL when separator is NULL.
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let sep = text_arg(&args[0]);
        let mut result = String::new();
        let mut has_part = false;
        for arg in &args[1..] {
            // C SQLite skips only NULL value arguments. Empty text is still a
            // value: `concat_ws('|','','x')` yields `'|x'`.
            if arg.is_null() {
                continue;
            }
            let part = text_arg(arg);
            if has_part {
                result.push_str(sep.as_ref());
            }
            result.push_str(part.as_ref());
            has_part = true;
        }
        Ok(SqliteValue::Text(SmallText::from_string(result)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        2
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
            return Ok(SqliteValue::Text(SmallText::new("")));
        }
        let bytes: Cow<'_, [u8]> = match &args[0] {
            SqliteValue::Blob(b) => Cow::Borrowed(b.as_ref()),
            SqliteValue::Text(text) => Cow::Borrowed(text.as_bytes_direct()),
            // For non-blob: convert to text first, then hex-encode UTF-8 bytes.
            other => Cow::Owned(other.to_text().into_bytes()),
        };
        let mut hex = String::with_capacity(bytes.len() * 2);
        for b in bytes.as_ref() {
            let _ = write!(hex, "{b:02X}");
        }
        Ok(SqliteValue::Text(SmallText::from_string(hex)))
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
        let cond = &args[0];
        // C SQLite evaluates IIF condition with sqlite3VdbeRealValue != 0.0,
        // so 0.5 is truthy (non-zero real).
        let is_true = match cond {
            SqliteValue::Null => false,
            SqliteValue::Integer(n) => *n != 0,
            SqliteValue::Float(f) => *f != 0.0,
            SqliteValue::Text(_) | SqliteValue::Blob(_) => {
                let i = cond.to_integer();
                if i != 0 { true } else { cond.to_float() != 0.0 }
            }
        };
        if is_true {
            Ok(args[1].clone())
        } else if args.len() >= 3 {
            Ok(args[2].clone())
        } else {
            // Two-argument form iif(X,Y) is shorthand for iif(X,Y,NULL),
            // i.e. CASE WHEN X THEN Y END (SQLite 3.48+).
            Ok(SqliteValue::Null)
        }
    }

    fn num_args(&self) -> i32 {
        -1 // 2 or 3 args
    }

    fn min_args(&self) -> i32 {
        2
    }

    fn max_args(&self) -> Option<i32> {
        Some(3)
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
                let haystack = text_arg(&args[0]);
                let needle = text_arg(&args[1]);
                let haystack = haystack.as_ref();
                let needle = needle.as_ref();
                if needle.is_empty() {
                    return Ok(SqliteValue::Integer(1));
                }
                if haystack.is_empty() {
                    return Ok(SqliteValue::Integer(0));
                }
                let pos = haystack
                    .find(needle)
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

fn sqlite_text_until_nul(text: &str) -> &str {
    text.split_once('\0').map_or(text, |(prefix, _)| prefix)
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
            SqliteValue::Text(s) => {
                let text = sqlite_text_until_nul(s.as_str());
                if text.is_ascii() {
                    text.len()
                } else {
                    text.chars().count()
                }
            }
            SqliteValue::Blob(b) => b.len(),
            other => {
                // Numbers: length of text representation.
                let text = other.to_text();
                let text = sqlite_text_until_nul(&text);
                if text.is_ascii() {
                    text.len()
                } else {
                    text.chars().count()
                }
            }
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
        // bd-iubwb: octet_length reports the byte length of X's TEXT rendering
        // in the DATABASE text encoding (projected via the thread-local), so a
        // UTF-16 database counts two bytes per code unit. BLOB is raw bytes,
        // regardless of encoding.
        let encoding = statement_text_encoding();
        let len = match &args[0] {
            SqliteValue::Text(s) => text_octet_length(s.as_str(), encoding),
            SqliteValue::Blob(b) => b.len(),
            other => text_octet_length(&other.to_text(), encoding),
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
        let lowered = text_arg(&args[0]).as_ref().to_ascii_lowercase();
        Ok(SqliteValue::Text(SmallText::from_string(lowered)))
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
        let upper = text_arg(&args[0]).as_ref().to_ascii_uppercase();
        Ok(SqliteValue::Text(SmallText::from_string(upper)))
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

fn trim_chars<'a>(s: &'a str, chars: &str) -> &'a str {
    let char_set: Vec<char> = chars.chars().collect();
    s.trim_matches(|c: char| char_set.contains(&c))
}

fn ltrim_chars<'a>(s: &'a str, chars: &str) -> &'a str {
    let char_set: Vec<char> = chars.chars().collect();
    s.trim_start_matches(|c: char| char_set.contains(&c))
}

fn rtrim_chars<'a>(s: &'a str, chars: &str) -> &'a str {
    let char_set: Vec<char> = chars.chars().collect();
    s.trim_end_matches(|c: char| char_set.contains(&c))
}

impl ScalarFunction for TrimFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let s = text_arg(&args[0]);
        let chars = if args.len() > 1 && !args[1].is_null() {
            text_arg(&args[1])
        } else {
            Cow::Borrowed(" ")
        };
        Ok(SqliteValue::Text(SmallText::new(trim_chars(
            s.as_ref(),
            chars.as_ref(),
        ))))
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(2)
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
        let s = text_arg(&args[0]);
        let chars = if args.len() > 1 && !args[1].is_null() {
            text_arg(&args[1])
        } else {
            Cow::Borrowed(" ")
        };
        Ok(SqliteValue::Text(SmallText::new(ltrim_chars(
            s.as_ref(),
            chars.as_ref(),
        ))))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(2)
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
        let s = text_arg(&args[0]);
        let chars = if args.len() > 1 && !args[1].is_null() {
            text_arg(&args[1])
        } else {
            Cow::Borrowed(" ")
        };
        Ok(SqliteValue::Text(SmallText::new(rtrim_chars(
            s.as_ref(),
            chars.as_ref(),
        ))))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(2)
    }

    fn name(&self) -> &str {
        "rtrim"
    }
}

// ── nullif(X, Y) ────────────────────────────────────────────────────────

pub struct NullifFunc;

impl ScalarFunction for NullifFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_collation(args, None)
    }

    fn consumes_argument_collation(&self) -> bool {
        true
    }

    fn invoke_with_collation(
        &self,
        args: &[SqliteValue],
        collation: Option<&dyn crate::collation::CollationFunction>,
    ) -> Result<SqliteValue> {
        let equal = match (&args[0], &args[1], collation) {
            (SqliteValue::Text(left), SqliteValue::Text(right), Some(collation)) => {
                collation.compare(left.as_bytes(), right.as_bytes()) == std::cmp::Ordering::Equal
            }
            _ => args[0] == args[1],
        };
        if equal {
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
        Ok(SqliteValue::Text(SmallText::new(type_name)))
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
        let x = text_arg(&args[0]);
        let y = text_arg(&args[1]);
        let z = text_arg(&args[2]);
        if y.is_empty() {
            return Ok(SqliteValue::Text(SmallText::from_string(x)));
        }

        // Prevent OOM from massive string expansion
        if z.len() > y.len() {
            let occurrences = x.matches(y.as_ref()).count();
            let final_len = x.len() + occurrences * (z.len() - y.len());
            if final_len > 1_000_000_000 {
                return Err(FrankenError::TooBig);
            }
        }

        Ok(SqliteValue::Text(SmallText::from_string(
            x.replace(y.as_ref(), z.as_ref()),
        )))
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
        // C SQLite: a NULL precision argument makes the whole call NULL
        // (`round(123.4, NULL)` → NULL), not a default of 0.
        if args.len() > 1 && args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let x = args[0].to_float();
        // Clamp N to [0, 30] matching SQLite behavior.
        let n = if args.len() > 1 {
            args[1].to_integer().clamp(0, 30)
        } else {
            0
        };
        // Values beyond 2^52 have no fractional part — return unchanged
        if !(-4_503_599_627_370_496.0..=4_503_599_627_370_496.0).contains(&x) {
            return Ok(SqliteValue::Float(x));
        }
        // SQLite uses "round half away from zero" via its custom printf, while
        // Rust's format! uses "round half to even" (IEEE 754 default). They
        // agree on every value except an exact binary tie, which the shared
        // fixed-notation helper detects and adjusts to match SQLite.
        let rounded = format_fixed_round_half_away(x, n as usize)
            .parse::<f64>()
            .unwrap_or(x);
        Ok(SqliteValue::Float(rounded))
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(2)
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
            SqliteValue::Null => Ok(SqliteValue::Null),
            SqliteValue::Integer(i) => Ok(SqliteValue::Integer(i.signum())),
            SqliteValue::Float(f) => {
                if f.is_nan() {
                    Ok(SqliteValue::Null)
                } else if *f > 0.0 {
                    Ok(SqliteValue::Integer(1))
                } else if *f < 0.0 {
                    Ok(SqliteValue::Integer(-1))
                } else {
                    Ok(SqliteValue::Integer(0))
                }
            }
            SqliteValue::Text(s) => {
                // C SQLite sign() uses sqlite3AtoF — returns NULL for non-numeric text.
                let trimmed = s.trim_matches(|ch: char| ch.is_ascii_whitespace());
                if trimmed.is_empty() {
                    return Ok(SqliteValue::Null);
                }

                // Reject literal NaN/inf/infinity keywords (case-insensitive,
                // with optional leading sign). Rust's f64::parse accepts these
                // but C SQLite's sqlite3AtoF does not. Note: numeric overflow
                // strings like "1e999" that parse to infinity ARE valid — C
                // SQLite recognises those as numeric and sign() returns 1/-1.
                let stripped = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
                if stripped.eq_ignore_ascii_case("nan")
                    || stripped.eq_ignore_ascii_case("inf")
                    || stripped.eq_ignore_ascii_case("infinity")
                {
                    return Ok(SqliteValue::Null);
                }

                // Try parsing as a number. If the string isn't a valid numeric
                // representation, return NULL (matching C SQLite behavior).
                if let Ok(f) = trimmed.parse::<f64>() {
                    // Use the already-parsed value (avoids a redundant double-parse).
                    if f > 0.0 {
                        Ok(SqliteValue::Integer(1))
                    } else if f < 0.0 {
                        Ok(SqliteValue::Integer(-1))
                    } else {
                        Ok(SqliteValue::Integer(0))
                    }
                } else if let Ok(i) = trimmed.parse::<i64>() {
                    // Handles integers that f64 can't represent exactly but i64 can.
                    Ok(SqliteValue::Integer(i.signum()))
                } else {
                    Ok(SqliteValue::Null)
                }
            }
            SqliteValue::Blob(_) => Ok(SqliteValue::Null),
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
        // C SQLite returns a one-byte blob for NULL and for all lengths below
        // one. `zeroblob()` uses different empty-blob semantics, so keep this
        // rule local to randomblob().
        let n_i64 = if args[0].is_null() {
            1
        } else {
            args[0].to_integer().max(1)
        };
        if n_i64 > 1_000_000_000 {
            return Err(FrankenError::TooBig);
        }
        let n = n_i64 as usize;
        let mut buf = vec![0u8; n];
        let mut i = 0;
        while i < n {
            let rnd = simple_random_i64().to_ne_bytes();
            let to_copy = (n - i).min(8);
            buf[i..i + to_copy].copy_from_slice(&rnd[..to_copy]);
            i += to_copy;
        }
        Ok(SqliteValue::Blob(Arc::from(buf.as_slice())))
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
            return Ok(SqliteValue::Blob(Arc::from([] as [u8; 0])));
        }
        let n_i64 = args[0].to_integer().max(0);
        if n_i64 > 1_000_000_000 {
            return Err(FrankenError::TooBig);
        }
        let n = n_i64 as usize;
        Ok(SqliteValue::Blob(Arc::from(vec![0u8; n].as_slice())))
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
        let result = quote_sql_value(&args[0], false);
        Ok(SqliteValue::Text(SmallText::from_string(result)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "quote"
    }
}

// ── unistr_quote(X) ───────────────────────────────────────────────────────

pub struct UnistrQuoteFunc;

impl ScalarFunction for UnistrQuoteFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let result = quote_sql_value(&args[0], true);
        Ok(SqliteValue::Text(SmallText::from_string(result)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "unistr_quote"
    }
}

fn quote_sql_value(value: &SqliteValue, use_unistr_quote: bool) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(i) => i.to_string(),
        SqliteValue::Float(f) => format_sqlite_float(*f),
        SqliteValue::Text(s) => quote_sql_text_literal(s.as_str(), use_unistr_quote),
        SqliteValue::Blob(b) => {
            let mut hex = String::with_capacity(3 + b.len() * 2);
            hex.push_str("X'");
            for byte in b.iter() {
                let _ = write!(hex, "{byte:02X}");
            }
            hex.push('\'');
            hex
        }
    }
}

fn quote_sql_text_literal(text: &str, use_unistr_quote: bool) -> String {
    let text = sqlite_text_until_nul(text);
    if use_unistr_quote && text.chars().any(is_unistr_control_char) {
        return unistr_quote_sql_text_literal(text);
    }

    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('\'');
    append_sql_string_literal_body(&mut quoted, text);
    quoted.push('\'');
    quoted
}

fn unistr_quote_sql_text_literal(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 12);
    quoted.push_str("unistr('");
    for ch in text.chars() {
        match ch {
            '\'' => quoted.push_str("''"),
            '\\' => quoted.push_str("\\\\"),
            _ if is_unistr_control_char(ch) => {
                let _ = write!(quoted, "\\u{:04x}", ch as u32);
            }
            _ => quoted.push(ch),
        }
    }
    quoted.push_str("')");
    quoted
}

fn append_sql_string_literal_body(out: &mut String, text: &str) {
    for ch in text.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
}

fn is_unistr_control_char(ch: char) -> bool {
    matches!(ch, '\u{0001}'..='\u{001F}')
}

// ── unhex(X [, Y]) ──────────────────────────────────────────────────────

pub struct UnhexFunc;

impl ScalarFunction for UnhexFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        if args.len() > 1 && args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let input = text_arg(&args[0]);
        let ignore_chars: Vec<char> = if args.len() > 1 {
            text_arg(&args[1])
                .chars()
                .filter(|&c| hex_digit(c).is_none())
                .collect()
        } else {
            Vec::new()
        };

        let mut bytes = Vec::with_capacity(input.len() / 2);
        let mut hi_nibble = None;
        for c in input.as_ref().chars() {
            if ignore_chars.contains(&c) {
                if hi_nibble.is_some() {
                    return Ok(SqliteValue::Null);
                }
                continue;
            }
            let digit = match hex_digit(c) {
                Some(v) => v,
                None => return Ok(SqliteValue::Null),
            };
            if let Some(hi) = hi_nibble.take() {
                bytes.push(hi << 4 | digit);
            } else {
                hi_nibble = Some(digit);
            }
        }
        if hi_nibble.is_some() {
            return Ok(SqliteValue::Null);
        }
        Ok(SqliteValue::Blob(Arc::from(bytes.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(2)
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
        if let SqliteValue::Blob(bytes) = &args[0] {
            return Ok(
                sqlite_blob_first_codepoint(bytes).map_or(SqliteValue::Null, SqliteValue::Integer)
            );
        }
        let s = text_arg(&args[0]);
        match sqlite_text_until_nul(s.as_ref()).chars().next() {
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

fn sqlite_blob_first_codepoint(bytes: &[u8]) -> Option<i64> {
    let first = *bytes.first()?;
    if first == 0 {
        return None;
    }
    let mut codepoint = match first {
        0x00..=0xBF => u32::from(first),
        0xC0..=0xDF => u32::from(first & 0x1F),
        0xE0..=0xEF => u32::from(first & 0x0F),
        0xF0..=0xF7 => u32::from(first & 0x07),
        _ => 0xFFFD,
    };

    if first >= 0xC0 && first <= 0xF7 {
        for byte in bytes
            .iter()
            .copied()
            .skip(1)
            .take_while(|byte| byte & 0xC0 == 0x80)
        {
            codepoint = codepoint
                .wrapping_shl(6)
                .wrapping_add(u32::from(byte & 0x3F));
        }
        if codepoint < 0x80
            || (codepoint & 0xFFFF_F800) == 0xD800
            || (codepoint & 0xFFFF_FFFE) == 0xFFFE
        {
            codepoint = 0xFFFD;
        }
    }

    Some(i64::from(codepoint))
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

        let text = text_arg(&args[0]);
        let s = text.as_ref();
        let ascii_fast_path = s.is_ascii();
        let len = if ascii_fast_path {
            s.len() as i64
        } else {
            s.chars().count() as i64
        };
        let has_length = args.len() > 2 && !args[2].is_null();

        let mut p1 = args[1].to_integer();
        let mut p2 = if has_length {
            args[2].to_integer()
        } else {
            1_000_000_000
        };

        // Match C SQLite's 2-phase substr algorithm exactly:
        // Phase 1: remember if length was negative, make it positive
        // Use saturating_neg to avoid panic on i64::MIN.
        let neg_p2 = p2 < 0;
        if neg_p2 {
            p2 = p2.saturating_neg();
        }

        // Phase 2: resolve start position (1-based to 0-based)
        if p1 < 0 {
            p1 = p1.saturating_add(len);
            if p1 < 0 {
                p2 = p2.saturating_add(p1);
                p1 = 0;
            }
        } else if p1 > 0 {
            p1 -= 1;
        } else if p2 > 0 {
            p2 -= 1; // start=0 quirk
        }

        // Phase 3: apply negative-length shift (move start backward)
        if neg_p2 {
            p1 = p1.saturating_sub(p2);
            if p1 < 0 {
                p2 = p2.saturating_add(p1);
                p1 = 0;
            }
        }

        if p1.saturating_add(p2) > len {
            p2 = len.saturating_sub(p1);
        }
        if p2 <= 0 {
            return Ok(SqliteValue::Text(SmallText::new("")));
        }

        if ascii_fast_path {
            let start = p1 as usize;
            let end = (p1 + p2) as usize;
            return Ok(SqliteValue::Text(SmallText::new(&s[start..end])));
        }

        let chars: Vec<char> = s.chars().collect();
        let result: String = chars[p1 as usize..(p1 + p2) as usize].iter().collect();
        Ok(SqliteValue::Text(SmallText::from_string(result)))
    }

    fn num_args(&self) -> i32 {
        -1 // 2 or 3 args
    }

    fn min_args(&self) -> i32 {
        2
    }

    fn max_args(&self) -> Option<i32> {
        Some(3)
    }

    fn name(&self) -> &str {
        "substr"
    }
}

impl SubstrFunc {
    #[allow(
        clippy::unused_self,
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap
    )]
    fn invoke_blob(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let blob = match &args[0] {
            SqliteValue::Blob(b) => b,
            _ => return Ok(SqliteValue::Null),
        };
        let len = blob.len() as i64;
        let has_length = args.len() > 2 && !args[2].is_null();

        let mut p1 = args[1].to_integer();
        let mut p2 = if has_length {
            args[2].to_integer()
        } else {
            1_000_000_000
        };

        let neg_p2 = p2 < 0;
        if neg_p2 {
            p2 = p2.saturating_neg();
        }

        if p1 < 0 {
            p1 = p1.saturating_add(len);
            if p1 < 0 {
                p2 = p2.saturating_add(p1);
                p1 = 0;
            }
        } else if p1 > 0 {
            p1 -= 1;
        } else if p2 > 0 {
            p2 -= 1;
        }

        if neg_p2 {
            p1 = p1.saturating_sub(p2);
            if p1 < 0 {
                p2 = p2.saturating_add(p1);
                p1 = 0;
            }
        }

        if p1.saturating_add(p2) > len {
            p2 = len.saturating_sub(p1);
        }
        if p2 <= 0 {
            return Ok(SqliteValue::Blob(Arc::from([] as [u8; 0])));
        }

        Ok(SqliteValue::Blob(Arc::from(
            &blob[p1 as usize..(p1 + p2) as usize],
        )))
    }
}

// ── soundex(X) ───────────────────────────────────────────────────────────

pub struct SoundexFunc;

impl ScalarFunction for SoundexFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            // SQLite returns "?000" for SOUNDEX(NULL), not NULL.
            return Ok(SqliteValue::Text(SmallText::new("?000")));
        }
        let s = text_arg(&args[0]);
        let code = soundex(s.as_ref());
        let text = std::str::from_utf8(&code).expect("Soundex output must be ASCII");
        Ok(SqliteValue::Text(SmallText::new(text)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "soundex"
    }
}

fn soundex(s: &str) -> [u8; 4] {
    let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());
    let first = match chars.next() {
        Some(c) => c.to_ascii_uppercase(),
        None => return *b"?000",
    };

    let code = |c: char| -> Option<u8> {
        match c.to_ascii_uppercase() {
            'B' | 'F' | 'P' | 'V' => Some(b'1'),
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some(b'2'),
            'D' | 'T' => Some(b'3'),
            'L' => Some(b'4'),
            'M' | 'N' => Some(b'5'),
            'R' => Some(b'6'),
            _ => None, // A, E, I, O, U, H, W, Y
        }
    };

    let mut result = *b"0000";
    result[0] = first as u8;
    let mut result_len = 1;
    let mut last_code = code(first);

    for c in chars {
        if result_len >= result.len() {
            break;
        }
        let current = code(c);
        if let Some(digit) = current
            && current != last_code
        {
            result[result_len] = digit;
            result_len += 1;
        }
        last_code = current;
    }

    result
}

// ── scalar max(X, Y, ...) ───────────────────────────────────────────────

pub struct ScalarMaxFunc;

impl ScalarFunction for ScalarMaxFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_collation(args, None)
    }

    fn consumes_argument_collation(&self) -> bool {
        true
    }

    fn invoke_with_collation(
        &self,
        args: &[SqliteValue],
        collation: Option<&dyn crate::collation::CollationFunction>,
    ) -> Result<SqliteValue> {
        // Scalar max: if ANY argument is NULL, returns NULL
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let mut max = &args[0];
        for arg in &args[1..] {
            let ordering = match (arg, max, collation) {
                (SqliteValue::Text(left), SqliteValue::Text(right), Some(collation)) => {
                    Some(collation.compare(left.as_bytes(), right.as_bytes()))
                }
                _ => arg.partial_cmp(max),
            };
            if ordering == Some(std::cmp::Ordering::Greater) {
                max = arg;
            }
        }
        Ok(max.clone())
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "max"
    }
}

// ── scalar min(X, Y, ...) ───────────────────────────────────────────────

pub struct ScalarMinFunc;

impl ScalarFunction for ScalarMinFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_collation(args, None)
    }

    fn consumes_argument_collation(&self) -> bool {
        true
    }

    fn invoke_with_collation(
        &self,
        args: &[SqliteValue],
        collation: Option<&dyn crate::collation::CollationFunction>,
    ) -> Result<SqliteValue> {
        // Scalar min: if ANY argument is NULL, returns NULL
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let mut min = &args[0];
        for arg in &args[1..] {
            let ordering = match (arg, min, collation) {
                (SqliteValue::Text(left), SqliteValue::Text(right), Some(collation)) => {
                    Some(collation.compare(left.as_bytes(), right.as_bytes()))
                }
                _ => arg.partial_cmp(min),
            };
            // SQLite's scalar min() selects the later argument on a tie. This
            // is observable when equal numeric values use different storage
            // classes or a collation considers distinct text values equal.
            if matches!(
                ordering,
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) {
                min = arg;
            }
        }
        Ok(min.clone())
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
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
        Ok(SqliteValue::Text(SmallText::new(
            fsqlite_types::FRANKENSQLITE_SQLITE_VERSION,
        )))
    }

    fn is_deterministic(&self) -> bool {
        false
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
        Ok(SqliteValue::Text(SmallText::new(
            fsqlite_types::FRANKENSQLITE_SOURCE_ID,
        )))
    }

    fn is_deterministic(&self) -> bool {
        false
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
        let query = text_arg(&args[0]);
        Ok(SqliteValue::Integer(i64::from(sqlite_compileoption_used(
            query.as_ref(),
        ))))
    }

    fn is_deterministic(&self) -> bool {
        false
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
        #[allow(clippy::cast_sign_loss)]
        match sqlite_compile_options().get(n as usize) {
            Some(opt) => Ok(SqliteValue::Text(SmallText::new(opt))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn is_deterministic(&self) -> bool {
        false
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
        let pattern = text_arg(&args[0]);
        let string = text_arg(&args[1]);
        let escape = if args.len() > 2 && !args[2].is_null() {
            Some(single_char_escape(text_arg(&args[2]).as_ref())?)
        } else {
            None
        };
        let matched = like_match(pattern.as_ref(), string.as_ref(), escape);
        Ok(SqliteValue::Integer(i64::from(matched)))
    }

    fn num_args(&self) -> i32 {
        -1 // 2 or 3 args
    }

    fn min_args(&self) -> i32 {
        2
    }

    fn max_args(&self) -> Option<i32> {
        Some(3)
    }

    fn name(&self) -> &str {
        "like"
    }
}

#[cfg(test)]
mod like_func_pragma_tests {
    use super::{LikeFunc, case_sensitive_like_active, set_case_sensitive_like};
    use crate::ScalarFunction;
    use fsqlite_types::SqliteValue;

    fn like(pattern: &str, text: &str) -> i64 {
        match LikeFunc
            .invoke(&[
                SqliteValue::Text(pattern.into()),
                SqliteValue::Text(text.into()),
            ])
            .unwrap()
        {
            SqliteValue::Integer(n) => n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    #[test]
    fn like_honors_case_sensitive_like_thread_local() {
        // Default: ASCII-case-insensitive.
        set_case_sensitive_like(false);
        assert_eq!(like("a", "A"), 1);
        assert_eq!(like("A%", "apple"), 1);
        // ON: byte-exact.
        set_case_sensitive_like(true);
        assert!(case_sensitive_like_active());
        assert_eq!(like("a", "A"), 0);
        assert_eq!(like("A%", "apple"), 0);
        assert_eq!(like("A%", "Apple"), 1);
        // Restore so other tests on this thread see the default.
        set_case_sensitive_like(false);
    }
}

fn single_char_escape(escape: &str) -> Result<char> {
    let mut chars = escape.chars();
    match (chars.next(), chars.next()) {
        (Some(ch), None) => Ok(ch),
        _ => Err(FrankenError::function_error(
            "ESCAPE expression must be a single character",
        )),
    }
}

/// LIKE pattern matching. ASCII-case-insensitive by default; byte-exact when
/// the connection has `PRAGMA case_sensitive_like = ON` (read from the
/// thread-local set by the Connection before statement execution).
fn like_match(pattern: &str, string: &str, escape: Option<char>) -> bool {
    sql_like_cased(pattern, string, escape, case_sensitive_like_active())
}

// ── glob(PATTERN, STRING) ───────────────────────────────────────────────

pub struct GlobFunc;

impl ScalarFunction for GlobFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if let Some(null) = null_propagate(args) {
            return Ok(null);
        }
        let pattern = text_arg(&args[0]);
        let string = text_arg(&args[1]);
        let matched = glob_match(pattern.as_ref(), string.as_ref());
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

fn text_arg(value: &SqliteValue) -> Cow<'_, str> {
    match value.as_text_str() {
        Some(text) => Cow::Borrowed(text),
        None => Cow::Owned(value.to_text()),
    }
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
                    // `X-Y` is a range only when Y is not the closing
                    // bracket: C SQLite's patternCompare treats a `-`
                    // immediately before `]` as a literal dash, so
                    // `[a-c-]` is {a..c, '-'} and `[^A-Za-z0-9._:-]`
                    // ends with a literal '-' rather than a `:-]` range
                    // that would swallow the class terminator.
                    if pi + 2 < pat.len() && pat[pi + 1] == '-' && pat[pi + 2] != ']' {
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
                } else {
                    // Unterminated character class: the pattern ran off the end
                    // before a closing ']'. C SQLite's patternCompare returns 0
                    // (no match) in this case, so `'a' GLOB '[a'` must be false.
                    return false;
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

const INVALID_UNISTR_ESCAPE: &str = "invalid Unicode escape";

fn decode_unistr_escape(chars: &mut std::str::Chars<'_>, digits: usize) -> Result<char> {
    let mut lookahead = chars.clone();
    let mut codepoint = 0u32;
    for _ in 0..digits {
        let Some(ch) = lookahead.next() else {
            return Err(FrankenError::function_error(INVALID_UNISTR_ESCAPE));
        };
        let Some(digit) = hex_digit(ch) else {
            return Err(FrankenError::function_error(INVALID_UNISTR_ESCAPE));
        };
        codepoint = (codepoint << 4) | u32::from(digit);
    }
    for _ in 0..digits {
        let _digit = chars.next();
    }
    char::from_u32(codepoint).ok_or_else(|| FrankenError::function_error(INVALID_UNISTR_ESCAPE))
}

impl ScalarFunction for UnistrFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args[0].is_null() {
            return Ok(SqliteValue::Null);
        }
        let input = text_arg(&args[0]);
        let mut result = String::with_capacity(input.len());
        let mut chars = input.as_ref().chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                // C SQLite: \\ is an escaped backslash literal.
                if chars.as_str().starts_with('\\') {
                    let _ = chars.next();
                    result.push('\\');
                    continue;
                }
                let digits = if chars.as_str().starts_with('+') {
                    // \+XXXXXX
                    let _plus = chars.next();
                    6
                } else if chars.as_str().starts_with('u') {
                    // \uXXXX
                    let _marker = chars.next();
                    4
                } else if chars.as_str().starts_with('U') {
                    // \UXXXXXXXX
                    let _marker = chars.next();
                    8
                } else {
                    // \XXXX
                    4
                };
                result.push(decode_unistr_escape(&mut chars, digits)?);
                continue;
            }
            result.push(ch);
        }
        Ok(SqliteValue::Text(SmallText::from_string(result)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "unistr"
    }
}

// ── Connection-state helpers ────────────────────────────────────────────
// These functions reflect connection-local counters projected into this
// thread by the connection layer around statement execution.

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
    registry.register_scalar(UnistrQuoteFunc);
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
    registry.register_slow_changing_scalar(SqliteVersionFunc);
    registry.register_slow_changing_scalar(SqliteSourceIdFunc);
    registry.register_slow_changing_scalar(SqliteCompileoptionUsedFunc);
    registry.register_slow_changing_scalar(SqliteCompileoptionGetFunc);

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
            -1 // 2 or 3 args, mirroring iif
        }

        fn min_args(&self) -> i32 {
            2
        }

        fn max_args(&self) -> Option<i32> {
            Some(3)
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

        fn min_args(&self) -> i32 {
            2
        }

        fn max_args(&self) -> Option<i32> {
            Some(3)
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
        // SQLite returns NULL (not empty text) when the format string is empty:
        // an empty format never appends to the StrAccum, so its result buffer
        // stays NULL. A non-empty format that renders to nothing (e.g.
        // printf('%s', NULL)) still yields empty TEXT, so only gate on the
        // format string being empty here.
        if fmt_str.is_empty() {
            return Ok(SqliteValue::Null);
        }
        let params = &args[1..];
        let result = sqlite_format(&fmt_str, params)?;
        Ok(SqliteValue::Text(SmallText::from_string(result)))
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
        let mut alt_form = false;
        let mut alt_form2 = false;
        let mut comma_group = false;
        loop {
            if i >= chars.len() {
                break;
            }
            match chars[i] {
                '-' => left_align = true,
                '+' => show_sign = true,
                ' ' => space_sign = true,
                '0' => zero_pad = true,
                '#' => alt_form = true,
                // SQLite's alternate-form-2 flag. For non-float conversions it has
                // no effect; for %f/%g it selects the shortest round-trip form.
                '!' => alt_form2 = true,
                // SQLite's comma flag: group the integer digits into
                // thousands separated by commas. Applies to the decimal
                // conversions %d/%i/%u and the integer part of %f; it is
                // accepted-but-inert for %e/%g/%x/%o (matches C SQLite).
                ',' => comma_group = true,
                _ => break,
            }
            i += 1;
        }

        // Parse width: a literal number, or `*` to take the width from the next
        // argument (bd-jvnwt). A negative dynamic width means left-justify with
        // its absolute value, matching C printf.
        let mut width = 0usize;
        if i < chars.len() && chars[i] == '*' {
            i += 1;
            let w = params.get(param_idx).map_or(0, SqliteValue::to_integer);
            param_idx += 1;
            if w < 0 {
                left_align = true;
                width = usize::try_from(w.unsigned_abs())
                    .unwrap_or(0)
                    .min(100_000_000);
            } else {
                width = usize::try_from(w).unwrap_or(0).min(100_000_000);
            }
        } else {
            while i < chars.len() && chars[i].is_ascii_digit() {
                width = width
                    .saturating_mul(10)
                    .saturating_add(chars[i] as usize - '0' as usize)
                    .min(100_000_000); // Prevent OOM from malicious formats
                i += 1;
            }
        }

        // Parse precision: a literal number, or `*` to take the precision from
        // the next argument (like width above). A negative dynamic precision
        // means "no precision", matching C printf.
        let mut precision = None;
        if i < chars.len() && chars[i] == '.' {
            i += 1;
            if i < chars.len() && chars[i] == '*' {
                i += 1;
                let p = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                if p >= 0 {
                    precision = Some(usize::try_from(p).unwrap_or(0).min(100_000_000));
                }
            } else {
                let mut prec = 0usize;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    prec = prec
                        .saturating_mul(10)
                        .saturating_add(chars[i] as usize - '0' as usize)
                        .min(100_000_000); // Prevent OOM from malicious formats
                    i += 1;
                }
                precision = Some(prec);
            }
        }

        if i >= chars.len() {
            break;
        }

        let spec = chars[i];
        i += 1;

        match spec {
            // A literal `%` honors the field width like any other conversion
            // (space-padded, right/left-justified): `%5%` -> "    %", `%-5%` ->
            // "%    ". The `0` flag pads with spaces (`%` is not numeric), so
            // pad_string is correct (bd-g27fn).
            '%' => result.push_str(&pad_string("%", width, left_align)),
            'n' => {} // no-op (security: never writes to memory)
            'd' | 'i' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                let formatted = format_integer(
                    val,
                    width,
                    left_align,
                    show_sign,
                    space_sign,
                    zero_pad,
                    comma_group,
                    precision,
                );
                result.push_str(&formatted);
            }
            'u' => {
                // Unsigned decimal (bd-jvnwt): reinterpret the i64 bit pattern as
                // u64, matching C/SQLite %u.
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                #[allow(clippy::cast_sign_loss)]
                let digits = apply_int_precision(&(val as u64).to_string(), precision);
                let padded = if comma_group {
                    // Zero-pad the raw digits to the field width before grouping,
                    // so the padding zeros participate in comma grouping.
                    let base = if zero_pad && width > digits.len() {
                        format!("{}{}", "0".repeat(width - digits.len()), digits)
                    } else {
                        digits
                    };
                    let grouped = group_thousands(&base);
                    if zero_pad {
                        grouped
                    } else {
                        pad_string(&grouped, width, left_align)
                    }
                } else if zero_pad && width > digits.len() {
                    format!("{}{}", "0".repeat(width - digits.len()), digits)
                } else {
                    pad_string(&digits, width, left_align)
                };
                result.push_str(&padded);
            }
            'f' => {
                let val = params.get(param_idx).map_or(0.0, SqliteValue::to_float);
                param_idx += 1;
                // C SQLite normalizes signed zero for %f/%e/%g: -0.0 renders as
                // 0 (no minus), and any sign flag then applies to +0.0
                // (bd-gh-printf-negative-zero-era4w). `-0.0 == 0.0` is true.
                let val = if val == 0.0 { 0.0 } else { val };
                let formatted = if let Some(s) =
                    nonfinite_float_str(val, width, left_align, show_sign, space_sign)
                {
                    s
                } else {
                    // Build the unsigned magnitude, honoring precision. Alt-form-2
                    // (`!`) applies the requested precision FIRST and then strips
                    // trailing fractional zeros (keeping >=1 digit, forcing ".0"
                    // when precision is 0) — matches C SQLite, e.g. '%!5.2f' 3.14159
                    // -> "3.14", '%!.3f' 1.5 -> "1.5", '%!f' 0.1 -> "0.1".
                    let prec = precision.unwrap_or(6);
                    // Round exact binary ties away from zero (C SQLite) rather
                    // than Rust's round-half-to-even, e.g. printf('%.0f', 2.5)
                    // -> "3" not "2" (bd-o1tu1).
                    let mut mag = format_fixed_round_half_away(val.abs(), prec);
                    if alt_form2 {
                        mag = altform2_trim_float(&mag);
                    }
                    if comma_group {
                        mag = group_float_integer_part(&mag);
                    }
                    let body = if val.is_sign_negative() {
                        format!("-{mag}")
                    } else {
                        mag
                    };
                    finish_float_padding(&body, width, left_align, show_sign, space_sign, zero_pad)
                };
                result.push_str(&formatted);
            }
            'e' | 'E' => {
                let val = params.get(param_idx).map_or(0.0, SqliteValue::to_float);
                param_idx += 1;
                // Normalize signed zero (bd-gh-printf-negative-zero-era4w).
                let val = if val == 0.0 { 0.0 } else { val };
                let prec = precision.unwrap_or(6);
                if let Some(s) = nonfinite_float_str(val, width, left_align, show_sign, space_sign)
                {
                    result.push_str(&s);
                } else {
                    // Round the mantissa's exact ties away from zero (C SQLite)
                    // instead of Rust's round-half-to-even, e.g.
                    // printf('%.0e', 2.5) -> "3e+00" not "2e+00" (bd-o1tu1).
                    let raw = format_sci_round_half_away(val, prec, spec == 'E');
                    // C printf always uses explicit sign and minimum 2-digit exponent
                    let mut formatted = normalize_exponent(&raw);
                    // Alternate-form-2 (`!`) strips trailing zeros from the
                    // mantissa (keeping >= 1 fractional digit), e.g. '%!e' 3.14159
                    // -> "3.14159e+00", '%!e' 5.0 -> "5.0e+00".
                    if alt_form2 {
                        formatted = altform2_trim_exp(&formatted, spec == 'e');
                    }
                    result.push_str(&finish_float_padding(
                        &formatted, width, left_align, show_sign, space_sign, zero_pad,
                    ));
                }
            }
            'g' | 'G' => {
                let val = params.get(param_idx).map_or(0.0, SqliteValue::to_float);
                param_idx += 1;
                // Normalize signed zero, incl. the alt-form path that bypasses
                // format_float_g (bd-gh-printf-negative-zero-era4w).
                let val = if val == 0.0 { 0.0 } else { val };
                let prec = precision.unwrap_or(6);
                let sig = prec.max(1);
                if let Some(s) = nonfinite_float_str(val, width, left_align, show_sign, space_sign)
                {
                    result.push_str(&s);
                } else if alt_form2 {
                    // Alternate-form-2 (`!`) on %g: format at the requested
                    // significant digits (like %g, honoring precision — 0 means 1
                    // sig fig), then ensure a decimal point with >= 1 fractional
                    // digit. Unlike a shortest-round-trip form this respects the
                    // precision-driven fixed/exponential choice: '%!.0g' 12345 ->
                    // "1.0e+04", '%!.3g' 12345 -> "1.23e+04", '%!g' 100 -> "100.0".
                    let formatted = format_float_g(val, sig, spec == 'G');
                    let alt = if formatted.contains(['e', 'E']) {
                        altform2_trim_exp(&formatted, spec == 'g')
                    } else {
                        altform2_trim_float(&formatted)
                    };
                    result.push_str(&finish_float_padding(
                        &alt, width, left_align, show_sign, space_sign, zero_pad,
                    ));
                } else {
                    let mut formatted = format_float_g(val, sig, spec == 'G');
                    // The `,` flag groups the integer part only when %g renders in
                    // fixed (non-exponential) form; SQLite leaves exponential
                    // output ungrouped ('%,g' 1234.5 -> "1,234.5"; '%,g' 1e6 ->
                    // "1e+06").
                    if comma_group && !formatted.contains(['e', 'E']) {
                        formatted = group_signed_decimal_integer_part(&formatted);
                    }
                    result.push_str(&finish_float_padding(
                        &formatted, width, left_align, show_sign, space_sign, zero_pad,
                    ));
                }
            }
            's' | 'z' => {
                let param = params.get(param_idx);
                param_idx += 1;
                let val = match param {
                    // SQLite: printf('%s', NULL) returns empty string
                    Some(SqliteValue::Null) | None => String::new(),
                    Some(v) => v.to_text(),
                };
                // C SQLite counts %s precision in BYTES. It will emit a bare
                // partial code point; we floor to the previous char boundary
                // instead (Rust strings must stay valid UTF-8), which matches
                // C SQLite whenever the cut lands on a boundary.
                let truncated = if let Some(prec) = precision {
                    if val.len() > prec {
                        let mut end = prec;
                        while end > 0 && !val.is_char_boundary(end) {
                            end -= 1;
                        }
                        val[..end].to_owned()
                    } else {
                        val
                    }
                } else {
                    val
                };
                result.push_str(&pad_string(&truncated, width, left_align));
            }
            'q' => {
                // Single-quote escaping; C SQLite emits "(NULL)" for %q with NULL.
                // Field width applies (byte-counted, like %s), space-padded and
                // right/left-justified, including the "(NULL)" case (bd-8959m).
                let param = params.get(param_idx);
                param_idx += 1;
                let escaped = match param {
                    // SQLite: printf('%q', NULL) returns literal "(NULL)"
                    Some(SqliteValue::Null) | None => "(NULL)".to_owned(),
                    Some(v) => v.to_text().replace('\'', "''"),
                };
                result.push_str(&pad_string(&escaped, width, left_align));
            }
            'Q' => {
                // Like %q but wrapped in quotes, NULL -> "NULL". Field width
                // applies to the whole rendered token (bd-8959m).
                let param = params.get(param_idx);
                param_idx += 1;
                let rendered = match param {
                    Some(SqliteValue::Null) | None => "NULL".to_owned(),
                    Some(v) => format!("'{}'", v.to_text().replace('\'', "''")),
                };
                result.push_str(&pad_string(&rendered, width, left_align));
            }
            'w' => {
                // Double-quote escaping for identifiers; NULL → empty.
                // C SQLite %w with NULL produces nothing (empty string),
                // and only escapes internal double quotes (no surrounding quotes).
                // Field width applies to the non-NULL rendering (bd-8959m); the
                // NULL case stays empty (its value semantics are a separate
                // concern from width).
                let param = params.get(param_idx);
                param_idx += 1;
                if matches!(param, Some(SqliteValue::Null) | None) {
                    // NULL: produce nothing (matches the existing behavior).
                } else {
                    let escaped = param
                        .map(SqliteValue::to_text)
                        .unwrap_or_default()
                        .replace('"', "\"\"");
                    result.push_str(&pad_string(&escaped, width, left_align));
                }
            }
            'x' | 'X' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                #[allow(clippy::cast_sign_loss)]
                let digits = apply_int_precision(
                    &if spec == 'x' {
                        format!("{:x}", val as u64)
                    } else {
                        format!("{:X}", val as u64)
                    },
                    precision,
                );
                // Alternate form (`#`) prefixes a nonzero value with 0x / 0X.
                let prefix = if alt_form && val != 0 {
                    if spec == 'x' { "0x" } else { "0X" }
                } else {
                    ""
                };
                // SQLite's printf zero-pads whenever the `0` flag is present,
                // even alongside `-` (it does NOT let `-` override `0` the way C
                // does). The digits are zero-padded to `width`; the prefix sits
                // outside that pad.
                let padded = if zero_pad && width > digits.len() {
                    let pad = "0".repeat(width - digits.len());
                    format!("{prefix}{pad}{digits}")
                } else {
                    pad_string(&format!("{prefix}{digits}"), width, left_align)
                };
                result.push_str(&padded);
            }
            'o' => {
                let val = params.get(param_idx).map_or(0, SqliteValue::to_integer);
                param_idx += 1;
                #[allow(clippy::cast_sign_loss)]
                let digits = apply_int_precision(&format!("{:o}", val as u64), precision);
                // Alternate form (`#`) prefixes a nonzero value with a leading 0.
                let prefix = if alt_form && val != 0 { "0" } else { "" };
                // As with %x, SQLite zero-pads whenever the `0` flag is present
                // (even with `-`).
                let padded = if zero_pad && width > digits.len() {
                    let pad = "0".repeat(width - digits.len());
                    format!("{prefix}{pad}{digits}")
                } else {
                    pad_string(&format!("{prefix}{digits}"), width, left_align)
                };
                result.push_str(&padded);
            }
            'c' => {
                let param = params.get(param_idx);
                param_idx += 1;
                // SQLite's printf %c renders the argument to its text form and
                // emits the first character — it does NOT interpret an integer
                // as a Unicode codepoint like C printf does (bd-47mu0). So
                // printf('%c', 65) yields '6' (first char of "65"), not 'A'.
                let text = match param {
                    Some(SqliteValue::Null) | None => String::new(),
                    Some(v) => v.to_text(),
                };
                // Field width applies, counted in CHARACTERS: the single emitted
                // char is one width unit regardless of its byte length (unlike
                // %s, which counts bytes), and the '0' flag is ignored — padding
                // is always spaces, right- or left-justified (bd-ul4c0). An empty
                // argument emits no char but is still padded to width.
                let ch = text.chars().next();
                let pad = width.saturating_sub(usize::from(ch.is_some()));
                if !left_align {
                    for _ in 0..pad {
                        result.push(' ');
                    }
                }
                if let Some(c) = ch {
                    result.push(c);
                }
                if left_align {
                    for _ in 0..pad {
                        result.push(' ');
                    }
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

// A printf integer conversion carries several independent, non-groupable flags
// (justification, sign mode, zero-pad, comma grouping) plus width and precision;
// bundling them behind a struct would only add indirection for a single caller.
#[allow(clippy::too_many_arguments)]
fn format_integer(
    val: i64,
    width: usize,
    left_align: bool,
    show_sign: bool,
    space_sign: bool,
    zero_pad: bool,
    comma_group: bool,
    precision: Option<usize>,
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
    let digits = apply_int_precision(&format!("{}", val.unsigned_abs()), precision);
    if comma_group {
        // SQLite zero-pads the raw digits up to the field width BEFORE inserting
        // the grouping commas, so the padding zeros are themselves grouped
        // (e.g. '%,08d' 1234 -> "00,001,234"). Space padding, by contrast, is
        // applied to the already-grouped value ('%,10d' 1234567 -> " 1,234,567").
        let padded_digits = if zero_pad && width > sign.len() + digits.len() {
            format!("{}{digits}", "0".repeat(width - sign.len() - digits.len()))
        } else {
            digits
        };
        let body = format!("{sign}{}", group_thousands(&padded_digits));
        if zero_pad || body.len() >= width {
            return body;
        }
        let pad = width - body.len();
        return if left_align {
            format!("{body}{}", " ".repeat(pad))
        } else {
            format!("{}{body}", " ".repeat(pad))
        };
    }
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

/// Insert a comma every three digits, counting from the right, into a string of
/// ASCII digits — SQLite's printf `,` grouping flag. Input with fewer than four
/// digits (or any non-digit byte) is returned unchanged.
fn group_thousands(digits: &str) -> String {
    if digits.len() <= 3 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return digits.to_owned();
    }
    let lead = digits.len() % 3;
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    if lead > 0 {
        out.push_str(&digits[..lead]);
    }
    let mut idx = lead;
    while idx < digits.len() {
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(&digits[idx..idx + 3]);
        idx += 3;
    }
    out
}

/// Alternate-form-2 (`!`) trailing-zero trim for a `%f` magnitude string: strip
/// trailing fractional zeros but keep at least one digit after the decimal
/// point, and add a ".0" when there is no point at all (precision 0). Matches
/// C SQLite: "5.000000" -> "5.0", "1.500" -> "1.5", "6" -> "6.0", "3.14" -> "3.14".
fn altform2_trim_float(mag: &str) -> String {
    if mag.contains('.') {
        let trimmed = mag.trim_end_matches('0');
        if trimmed.ends_with('.') {
            format!("{trimmed}0")
        } else {
            trimmed.to_owned()
        }
    } else {
        format!("{mag}.0")
    }
}

/// Apply the `,` thousands grouping to the integer part of an unsigned `%f`
/// magnitude string, e.g. "1234.500000" -> "1,234.500000".
fn group_float_integer_part(mag: &str) -> String {
    if let Some(dot) = mag.find('.') {
        format!("{}{}", group_thousands(&mag[..dot]), &mag[dot..])
    } else {
        group_thousands(mag)
    }
}

/// Like [`group_float_integer_part`] but for a possibly signed decimal string
/// (e.g. a `%g` fixed-form result such as "-1234.5" -> "-1,234.5").
fn group_signed_decimal_integer_part(s: &str) -> String {
    if let Some(rest) = s.strip_prefix('-') {
        format!("-{}", group_float_integer_part(rest))
    } else {
        group_float_integer_part(s)
    }
}

/// Apply an integer conversion's precision (SQLite/C printf: precision is the
/// MINIMUM number of digits — left-pad with zeros to reach it). Applies to
/// %d/%i/%u/%x/%X/%o. `None` (no precision) leaves the digits untouched. SQLite
/// keeps a single "0" for `%.0d` of 0 (unlike C, which yields ""), which falls
/// out naturally because the "0" digit already satisfies precision 0.
fn apply_int_precision(digits: &str, precision: Option<usize>) -> String {
    match precision {
        Some(p) if digits.len() < p => {
            format!("{}{digits}", "0".repeat(p - digits.len()))
        }
        _ => digits.to_owned(),
    }
}

/// Alternate-form-2 (`!`) trailing-zero trim for a `%e`/`%E` result: strip
/// trailing zeros from the mantissa (the part before the exponent marker),
/// keeping at least one fractional digit, then reattach the exponent, e.g.
/// "3.141590e+00" -> "3.14159e+00", "5.000000e+00" -> "5.0e+00".
fn altform2_trim_exp(s: &str, lower: bool) -> String {
    let marker = if lower { 'e' } else { 'E' };
    if let Some(pos) = s.find(marker) {
        format!("{}{}", altform2_trim_float(&s[..pos]), &s[pos..])
    } else {
        s.to_owned()
    }
}

/// C SQLite renders non-finite floats in printf as `Inf` / `-Inf` / `NaN`
/// (sign flags honored for infinities, space-padded to width, never
/// zero-padded). Returns `None` for finite values.
fn nonfinite_float_str(
    val: f64,
    width: usize,
    left_align: bool,
    show_sign: bool,
    space_sign: bool,
) -> Option<String> {
    let body = if val.is_nan() {
        "NaN".to_owned()
    } else if val.is_infinite() {
        let sign = if val < 0.0 {
            "-"
        } else if show_sign {
            "+"
        } else if space_sign {
            " "
        } else {
            ""
        };
        format!("{sign}Inf")
    } else {
        return None;
    };
    Some(pad_string(&body, width, left_align))
}

/// Apply printf sign flags and width padding to an already-formatted float
/// body (which may carry a leading `-`). Zero padding is inserted between the
/// sign and the digits, matching C printf.
fn finish_float_padding(
    body: &str,
    width: usize,
    left_align: bool,
    show_sign: bool,
    space_sign: bool,
    zero_pad: bool,
) -> String {
    let (sign, digits) = if let Some(rest) = body.strip_prefix('-') {
        ("-", rest)
    } else if show_sign {
        ("+", body)
    } else if space_sign {
        (" ", body)
    } else {
        ("", body)
    };
    let full_len = sign.len() + digits.len();
    if full_len >= width {
        return format!("{sign}{digits}");
    }
    let pad = width - full_len;
    if left_align {
        format!("{sign}{digits}{}", " ".repeat(pad))
    } else if zero_pad {
        format!("{sign}{}{digits}", "0".repeat(pad))
    } else {
        format!("{}{sign}{digits}", " ".repeat(pad))
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

/// Normalize an exponent string to match C printf: explicit sign and
/// minimum two digits (e.g. `"1.23e6"` → `"1.23e+06"`).
fn normalize_exponent(s: &str) -> String {
    let (prefix, e_char, exp_part) = if let Some(pos) = s.find('e') {
        (&s[..pos], 'e', &s[pos + 1..])
    } else if let Some(pos) = s.find('E') {
        (&s[..pos], 'E', &s[pos + 1..])
    } else {
        return s.to_owned();
    };
    let (sign, digits) = if let Some(rest) = exp_part.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = exp_part.strip_prefix('+') {
        ("+", rest)
    } else {
        ("+", exp_part)
    };
    let padded = if digits.len() < 2 {
        format!("0{digits}")
    } else {
        digits.to_owned()
    };
    format!("{prefix}{e_char}{sign}{padded}")
}

/// A finite `f64` has at most 1074 fractional decimal digits (the smallest
/// positive subnormal, `2^-1074`). Formatting with this many guard digits
/// therefore reproduces the value's EXACT decimal expansion, so a trailing
/// "…5000…0" is a genuine binary half-tie rather than a rounding artifact.
const MAX_F64_FRACTIONAL_DIGITS: usize = 1074;

/// Increment a non-negative decimal magnitude given as ASCII digit bytes (may
/// contain a single `.`, never a sign) by one unit in its last place,
/// propagating carry and prepending `1` on overflow, e.g. `"2"` → `"3"`,
/// `"9"` → `"10"`, `"9.9"` → `"10.0"`.
fn increment_decimal_digits(digits: &mut Vec<u8>) {
    let mut carry = true;
    for b in digits.iter_mut().rev() {
        if *b == b'.' {
            continue;
        }
        if carry {
            if *b == b'9' {
                *b = b'0';
            } else {
                *b += 1;
                carry = false;
                break;
            }
        }
    }
    if carry {
        digits.insert(0, b'1');
    }
}

/// True iff the non-negative magnitude `mag` is an EXACT binary half-tie at
/// `prec` fractional digits — its exact decimal expansion has digit `prec + 1`
/// equal to `5` with only zeros afterward. Rust's `format!` rounds ties to
/// even while C SQLite rounds them away from zero, so ONLY exact ties diverge;
/// every other value is already correctly rounded by `format!`.
///
/// A real tie shows the "…5000" pattern at any guard length, so a cheap guard
/// is checked first; it is confirmed against the full exact expansion only to
/// reject near-ties whose long 9-/0-runs a short guard would round into a
/// spurious "…5000" (e.g. `0.15` is really `0.14999…`, not a tie).
fn is_exact_decimal_tie(mag: f64, prec: usize) -> bool {
    fn looks_like_tie(mag: f64, prec: usize, guard: usize) -> bool {
        let full = format!("{mag:.guard$}");
        let Some(dot) = full.find('.') else {
            return false;
        };
        let rd_idx = dot + 1 + prec;
        let bytes = full.as_bytes();
        rd_idx < bytes.len()
            && bytes[rd_idx] == b'5'
            && full[rd_idx + 1..].bytes().all(|b| b == b'0')
    }
    looks_like_tie(mag, prec, prec + 18)
        && looks_like_tie(mag, prec, prec + MAX_F64_FRACTIONAL_DIGITS)
}

/// Scientific-notation analogue of [`is_exact_decimal_tie`]: true iff the
/// mantissa of `mag` (normalized to `[1, 10)`) has an exact `5` with only
/// trailing zeros at fractional digit `prec + 1`.
fn is_exact_sci_tie(mag: f64, prec: usize) -> bool {
    fn looks_like_tie(mag: f64, prec: usize, guard: usize) -> bool {
        let s = format!("{mag:.guard$e}");
        let Some((mant, _)) = s.split_once('e') else {
            return false;
        };
        let Some(dot) = mant.find('.') else {
            return false;
        };
        let rd_idx = dot + 1 + prec;
        let bytes = mant.as_bytes();
        rd_idx < bytes.len()
            && bytes[rd_idx] == b'5'
            && mant[rd_idx + 1..].bytes().all(|b| b == b'0')
    }
    looks_like_tie(mag, prec, prec + 18)
        && looks_like_tie(mag, prec, prec + MAX_F64_FRACTIONAL_DIGITS)
}

/// Format a finite float in fixed notation with `prec` fractional digits,
/// rounding exact binary half-ties AWAY FROM ZERO (matching C SQLite's
/// `printf`/`round`) instead of Rust's round-half-to-even. Non-tie values are
/// left to `format!`, which already rounds them exactly as SQLite does; only a
/// confirmed exact tie is adjusted. A leading `-` is preserved for negatives.
fn format_fixed_round_half_away(val: f64, prec: usize) -> String {
    let base = format!("{val:.prec$}");
    let mag = val.abs();
    if !is_exact_decimal_tie(mag, prec) {
        return base;
    }
    // Exact tie: round the magnitude up (away from zero) by incrementing the
    // truncated digit string, then reattach the sign.
    let src = format!("{mag:.p$}", p = prec + 2);
    let dot = src.find('.').unwrap_or(src.len());
    let rd_idx = dot + 1 + prec;
    let mut digits = src.as_bytes()[..rd_idx].to_vec();
    if digits.last() == Some(&b'.') {
        digits.pop();
    }
    increment_decimal_digits(&mut digits);
    let Ok(body) = String::from_utf8(digits) else {
        return base;
    };
    if val.is_sign_negative() {
        format!("-{body}")
    } else {
        body
    }
}

/// Format a finite float in `%e`/`%E` scientific notation with `prec`
/// fractional mantissa digits, rounding an exact mantissa half-tie AWAY FROM
/// ZERO. A carry out of `[1, 10)` renormalizes the mantissa (e.g. `9.5` at
/// precision 0 → `1e+01`) and bumps the exponent. Returns an un-normalized
/// `d.ddde{exp}` string (sign included, exponent not zero-padded) that mirrors
/// Rust's `{:e}`/`{:E}` output, so callers post-process it with
/// `normalize_exponent`/`altform2_trim_exp` exactly as before.
fn format_sci_round_half_away(val: f64, prec: usize, upper: bool) -> String {
    let base = if upper {
        format!("{val:.prec$E}")
    } else {
        format!("{val:.prec$e}")
    };
    let mag = val.abs();
    if mag == 0.0 || !is_exact_sci_tie(mag, prec) {
        return base;
    }
    let e_char = if upper { 'E' } else { 'e' };
    let src = format!("{mag:.p$e}", p = prec + 2);
    let Some((mant, exp_str)) = src.split_once('e') else {
        return base;
    };
    let mut exp: i64 = exp_str.parse().unwrap_or(0);
    let dot = mant.find('.').unwrap_or(mant.len());
    let rd_idx = dot + 1 + prec;
    let mut digits = mant.as_bytes()[..rd_idx].to_vec();
    if digits.last() == Some(&b'.') {
        digits.pop();
    }
    increment_decimal_digits(&mut digits);
    let Ok(mut mantissa) = String::from_utf8(digits) else {
        return base;
    };
    // A carry out of the `[1, 10)` mantissa produces a two-digit integer part
    // ("10" or "10.0…0"); renormalize to "1.0…0" and bump the exponent.
    let int_len = mantissa.find('.').unwrap_or(mantissa.len());
    if int_len == 2 {
        mantissa = if prec > 0 {
            format!("1.{}", "0".repeat(prec))
        } else {
            "1".to_owned()
        };
        exp += 1;
    }
    let sign = if val.is_sign_negative() { "-" } else { "" };
    format!("{sign}{mantissa}{e_char}{exp}")
}

/// Format a float using `%g`/`%G` semantics.
fn format_float_g(val: f64, sig: usize, upper: bool) -> String {
    if !val.is_finite() {
        return format!("{val}");
    }
    // C SQLite canonicalizes signed zero for %g: both +0.0 and -0.0 render as
    // "0" (no minus sign). `-0.0 == 0.0` is true, so this maps -0.0 to +0.0.
    let val = if val == 0.0 { 0.0 } else { val };
    // Round to `sig` significant digits half-away (matching C SQLite), then read
    // the resulting exponent. The rounding may carry across a power of ten
    // (e.g. `9.5` at 1 significant digit → `1e1`), and that rounded exponent —
    // not the raw one — selects fixed vs. exponential form below.
    let sci = format_sci_round_half_away(val, sig.saturating_sub(1), false);
    let exp: i32 = sci
        .rsplit_once('e')
        .and_then(|(_, e)| e.parse().ok())
        .unwrap_or(0);
    #[allow(clippy::cast_possible_wrap)]
    let formatted = if exp < -4 || exp >= sig as i32 {
        let s = if upper { sci.replace('e', "E") } else { sci };
        // Strip trailing zeros from mantissa, then normalize the exponent.
        let trimmed = if s.contains('.') {
            if let Some(e_pos) = s.find('e').or_else(|| s.find('E')) {
                let mantissa = s[..e_pos].trim_end_matches('0').trim_end_matches('.');
                format!("{mantissa}{}", &s[e_pos..])
            } else {
                s.trim_end_matches('0').trim_end_matches('.').to_owned()
            }
        } else {
            s
        };
        normalize_exponent(&trimmed)
    } else {
        let decimal_places = if exp >= 0 {
            sig.saturating_sub((exp + 1) as usize)
        } else {
            sig + exp.unsigned_abs() as usize - 1
        };
        let s = format_fixed_round_half_away(val, decimal_places);
        // Only strip trailing zeros when there is a fractional part. When
        // decimal_places == 0 (e.g. `%g` of 100000.0 -> exp 5, sig 6), `s` is
        // "100000" with no '.', and an unconditional trim would strip the
        // significant integer zeros down to "1". Mirror the exponential branch's
        // `if s.contains('.')` guard above. (C/SQLite %g never drops integer digits.)
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            s
        }
    };
    formatted
}

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

    fn assert_wrong_arg_count(registry: &FunctionRegistry, name: &str, arity: i32) {
        let function = registry
            .find_scalar(name, arity)
            .expect("known scalar name with bad arity returns erroring scalar");
        let args = vec![SqliteValue::Null; arity.max(0) as usize];
        let err = function
            .invoke(&args)
            .expect_err("wrong arity should return function error");
        let expected = format!("wrong number of arguments to function {name}()");
        assert!(
            matches!(&err, FrankenError::FunctionError(message) if message == &expected),
            "expected {expected:?}, got {err:?}"
        );
    }

    #[test]
    fn test_get_change_tracking_state_returns_thread_local_snapshot() {
        let original = get_change_tracking_state();
        let expected = ChangeTrackingState {
            last_insert_rowid: 17,
            last_changes: 23,
            total_changes: 42,
        };

        set_change_tracking_state(expected);
        assert_eq!(get_change_tracking_state(), expected);

        set_change_tracking_state(original);
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
            invoke1(&AbsFunc, SqliteValue::Text(SmallText::from_string("-7.5"))).unwrap(),
            SqliteValue::Float(7.5)
        );
    }

    #[test]
    fn test_abs_whitespace_padded_text() {
        // SQLite's abs() casts non-integers to REAL, even if they parse cleanly as integers
        assert_eq!(
            invoke1(
                &AbsFunc,
                SqliteValue::Text(SmallText::from_string("  42  "))
            )
            .unwrap(),
            SqliteValue::Float(42.0)
        );
        assert_eq!(
            invoke1(
                &AbsFunc,
                SqliteValue::Text(SmallText::from_string("  -7.5  "))
            )
            .unwrap(),
            SqliteValue::Float(7.5)
        );
        assert_eq!(
            invoke1(&AbsFunc, SqliteValue::Text(SmallText::from_string("abc"))).unwrap(),
            SqliteValue::Float(0.0)
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
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("Hello")));
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
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("A\0B")));
    }

    #[test]
    fn test_char_invalid_scalar_values_use_replacement_character() {
        let f = CharFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(-1),
                SqliteValue::Integer(65),
                SqliteValue::Integer(1_114_112),
            ])
            .unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("\u{fffd}A\u{fffd}"))
        );
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
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Null,
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("hello")));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_concat_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const TEXT_ARGS: usize = 24;
        const INVOCATIONS: usize = 50_000;
        const REPEATS: usize = 5;

        let f = ConcatFunc;
        let mut args = Vec::with_capacity(TEXT_ARGS);
        for _ in 0..TEXT_ARGS {
            args.push(SqliteValue::Text(SmallText::from_string("payload")));
        }

        let mut best_ns = u128::MAX;
        let mut result_len = 0usize;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(args.as_slice()))
                        .expect("concat benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            best_ns = best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "concat_text_args text_args={TEXT_ARGS} invocations={INVOCATIONS} repeats={REPEATS} best_ns={best_ns} result_len={result_len}"
        );
    }

    // ── concat_ws ────────────────────────────────────────────────────────

    #[test]
    fn test_concat_ws_null_skipped() {
        let f = ConcatWsFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(",")),
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Null,
                SqliteValue::Text(SmallText::from_string("b")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("a,b")));
    }

    #[test]
    fn test_concat_ws_empty_string_is_not_skipped() {
        let f = ConcatWsFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("|")),
                SqliteValue::Text(SmallText::new("")),
                SqliteValue::Text(SmallText::from_string("x")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("|x")));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_concat_ws_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const TEXT_ARGS: usize = 24;
        const INVOCATIONS: usize = 50_000;
        const REPEATS: usize = 5;

        let f = ConcatWsFunc;
        let mut args = Vec::with_capacity(TEXT_ARGS + 1);
        args.push(SqliteValue::Text(SmallText::from_string(",")));
        for _ in 0..TEXT_ARGS {
            args.push(SqliteValue::Text(SmallText::from_string("payload")));
        }

        let mut best_ns = u128::MAX;
        let mut result_len = 0usize;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(args.as_slice()))
                        .expect("concat_ws benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            best_ns = best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "concat_ws_text_args text_args={TEXT_ARGS} invocations={INVOCATIONS} repeats={REPEATS} best_ns={best_ns} result_len={result_len}"
        );
    }

    // ── hex ──────────────────────────────────────────────────────────────

    #[test]
    fn test_hex_blob() {
        let result = invoke1(
            &HexFunc,
            SqliteValue::Blob(Arc::from([0xDE, 0xAD, 0xBE, 0xEF].as_slice())),
        )
        .unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("DEADBEEF"))
        );
    }

    #[test]
    fn test_hex_number_via_text() {
        // hex(42) encodes '42' as UTF-8 hex, not raw bits
        let result = invoke1(&HexFunc, SqliteValue::Integer(42)).unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("3432")));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_hex_text_blob_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const BYTES: usize = 24;
        const INVOCATIONS: usize = 100_000;
        const REPEATS: usize = 5;

        let f = HexFunc;
        let text_args = [SqliteValue::Text(SmallText::from_string(
            "payload payload sentinel",
        ))];
        let blob_args = [SqliteValue::Blob(Arc::from([0xAB; BYTES].as_slice()))];

        let mut text_best_ns = u128::MAX;
        let mut blob_best_ns = u128::MAX;
        let mut text_result_len = 0usize;
        let mut blob_result_len = 0usize;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(text_args.as_slice()))
                        .expect("hex text benchmark invocation must succeed"),
                );
                text_result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            text_best_ns = text_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(blob_args.as_slice()))
                        .expect("hex blob benchmark invocation must succeed"),
                );
                blob_result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            blob_best_ns = blob_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "hex_text_blob_args bytes={BYTES} invocations={INVOCATIONS} repeats={REPEATS} text_best_ns={text_best_ns} blob_best_ns={blob_best_ns} text_result_len={text_result_len} blob_result_len={blob_result_len}"
        );
    }

    // ── iif ──────────────────────────────────────────────────────────────

    #[test]
    fn test_iif_true() {
        let f = IifFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("yes")),
                SqliteValue::Text(SmallText::from_string("no")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("yes")));
    }

    #[test]
    fn test_iif_false() {
        let f = IifFunc;
        let result = f
            .invoke(&[
                SqliteValue::Integer(0),
                SqliteValue::Text(SmallText::from_string("yes")),
                SqliteValue::Text(SmallText::from_string("no")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("no")));
    }

    #[test]
    fn test_iif_whitespace_padded_text_truthy() {
        // Regression: IIF('  5  ', 'yes', 'no') must return 'yes'
        // because SQLite trims text before numeric coercion.
        let f = IifFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("  5  ")),
                SqliteValue::Text(SmallText::from_string("yes")),
                SqliteValue::Text(SmallText::from_string("no")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("yes")));
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
                SqliteValue::Text(SmallText::from_string("hello world")),
                SqliteValue::Text(SmallText::from_string("world"))
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
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Text(SmallText::from_string("xyz"))
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
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Text(SmallText::new(""))
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
                SqliteValue::Text(SmallText::new("")),
                SqliteValue::Text(SmallText::from_string("x"))
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
                SqliteValue::Blob(Arc::from([1, 2, 3].as_slice())),
                SqliteValue::Blob(Arc::from([].as_slice()))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_instr_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 100_000;
        const REPEATS: usize = 5;

        let f = InstrFunc;
        let args = [
            SqliteValue::Text(SmallText::from_string("payload payload sentinel")),
            SqliteValue::Text(SmallText::from_string("sentinel")),
        ];

        let mut best_ns = u128::MAX;
        let mut result_value = 0i64;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(args.as_slice()))
                        .expect("instr benchmark invocation must succeed"),
                );
                result_value = match result {
                    SqliteValue::Integer(value) => value,
                    SqliteValue::Null
                    | SqliteValue::Float(_)
                    | SqliteValue::Text(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            best_ns = best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "instr_text_args invocations={INVOCATIONS} repeats={REPEATS} best_ns={best_ns} result_value={result_value}"
        );
    }

    // ── length ───────────────────────────────────────────────────────────

    #[test]
    fn test_length_text_chars() {
        // café is 4 characters, 5 bytes
        assert_eq!(
            invoke1(
                &LengthFunc,
                SqliteValue::Text(SmallText::from_string("café"))
            )
            .unwrap(),
            SqliteValue::Integer(4)
        );
    }

    #[test]
    fn test_length_text_stops_at_nul() {
        assert_eq!(
            invoke1(
                &LengthFunc,
                SqliteValue::Text(SmallText::from_string("A\0B"))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            invoke1(
                &LengthFunc,
                SqliteValue::Text(SmallText::from_string("\0A"))
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_length_blob_bytes() {
        assert_eq!(
            invoke1(&LengthFunc, SqliteValue::Blob(Arc::from([1, 2].as_slice()))).unwrap(),
            SqliteValue::Integer(2)
        );
    }

    // ── octet_length ─────────────────────────────────────────────────────

    #[test]
    fn test_octet_length_multibyte() {
        // café: 'c'=1, 'a'=1, 'f'=1, 'é'=2 bytes = 5 bytes total
        assert_eq!(
            invoke1(
                &OctetLengthFunc,
                SqliteValue::Text(SmallText::from_string("café"))
            )
            .unwrap(),
            SqliteValue::Integer(5)
        );
    }

    #[test]
    fn test_octet_length_honors_statement_text_encoding() {
        // Default (UTF-8): byte length is the string's own byte length.
        set_statement_text_encoding(TextEncoding::Utf8);
        assert_eq!(
            invoke1(
                &OctetLengthFunc,
                SqliteValue::Text(SmallText::from_string("abc"))
            )
            .unwrap(),
            SqliteValue::Integer(3)
        );
        assert_eq!(
            invoke1(&OctetLengthFunc, SqliteValue::Integer(12345)).unwrap(),
            SqliteValue::Integer(5)
        );

        // UTF-16le: two bytes per code unit for TEXT and rendered numerics.
        set_statement_text_encoding(TextEncoding::Utf16le);
        assert_eq!(statement_text_encoding(), TextEncoding::Utf16le);
        assert_eq!(
            invoke1(
                &OctetLengthFunc,
                SqliteValue::Text(SmallText::from_string("abc"))
            )
            .unwrap(),
            SqliteValue::Integer(6)
        );
        assert_eq!(
            invoke1(&OctetLengthFunc, SqliteValue::Integer(12345)).unwrap(),
            SqliteValue::Integer(10)
        );
        // A non-BMP scalar is a surrogate pair = two code units = four bytes.
        assert_eq!(
            invoke1(
                &OctetLengthFunc,
                SqliteValue::Text(SmallText::from_string("\u{1F600}"))
            )
            .unwrap(),
            SqliteValue::Integer(4)
        );

        // UTF-16be counts identically to UTF-16le.
        set_statement_text_encoding(TextEncoding::Utf16be);
        assert_eq!(
            invoke1(
                &OctetLengthFunc,
                SqliteValue::Text(SmallText::from_string("abc"))
            )
            .unwrap(),
            SqliteValue::Integer(6)
        );

        // BLOB stays raw bytes regardless of the database encoding.
        assert_eq!(
            invoke1(&OctetLengthFunc, SqliteValue::Blob(vec![1, 2, 3].into())).unwrap(),
            SqliteValue::Integer(3)
        );

        // Restore the default so other tests on this thread are unaffected.
        set_statement_text_encoding(TextEncoding::Utf8);
    }

    // ── lower/upper ──────────────────────────────────────────────────────

    #[test]
    fn test_lower_ascii() {
        assert_eq!(
            invoke1(
                &LowerFunc,
                SqliteValue::Text(SmallText::from_string("HELLO"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("hello"))
        );
    }

    #[test]
    fn test_upper_ascii() {
        assert_eq!(
            invoke1(
                &UpperFunc,
                SqliteValue::Text(SmallText::from_string("hello"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("HELLO"))
        );
    }

    // ── trim/ltrim/rtrim ─────────────────────────────────────────────────

    #[test]
    fn test_trim_default() {
        let f = TrimFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Text(SmallText::from_string("  hello  "))])
                .unwrap(),
            SqliteValue::Text(SmallText::from_string("hello"))
        );
    }

    #[test]
    fn test_ltrim_default() {
        let f = LtrimFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Text(SmallText::from_string("  hello"))])
                .unwrap(),
            SqliteValue::Text(SmallText::from_string("hello"))
        );
    }

    #[test]
    fn test_ltrim_custom() {
        let f = LtrimFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("xxhello")),
                SqliteValue::Text(SmallText::from_string("x")),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("hello"))
        );
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_trim_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 100_000;
        const REPEATS: usize = 5;

        let trim = TrimFunc;
        let ltrim = LtrimFunc;
        let rtrim = RtrimFunc;
        let default_args = [SqliteValue::Text(SmallText::from_string("   payload   "))];
        let custom_args = [
            SqliteValue::Text(SmallText::from_string("xxxpayloadxxx")),
            SqliteValue::Text(SmallText::from_string("x")),
        ];

        let mut trim_best_ns = u128::MAX;
        let mut ltrim_best_ns = u128::MAX;
        let mut rtrim_best_ns = u128::MAX;
        let mut custom_best_ns = u128::MAX;
        let mut result_len = 0usize;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    trim.invoke(black_box(default_args.as_slice()))
                        .expect("trim benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            trim_best_ns = trim_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    ltrim
                        .invoke(black_box(default_args.as_slice()))
                        .expect("ltrim benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            ltrim_best_ns = ltrim_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    rtrim
                        .invoke(black_box(default_args.as_slice()))
                        .expect("rtrim benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            rtrim_best_ns = rtrim_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    trim.invoke(black_box(custom_args.as_slice()))
                        .expect("custom trim benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            custom_best_ns = custom_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "trim_text_args invocations={INVOCATIONS} repeats={REPEATS} trim_best_ns={trim_best_ns} ltrim_best_ns={ltrim_best_ns} rtrim_best_ns={rtrim_best_ns} custom_best_ns={custom_best_ns} result_len={result_len}"
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
            SqliteValue::Text(SmallText::from_string("null"))
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Integer(1)).unwrap(),
            SqliteValue::Text(SmallText::from_string("integer"))
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Float(1.0)).unwrap(),
            SqliteValue::Text(SmallText::from_string("real"))
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Text(SmallText::from_string("x"))).unwrap(),
            SqliteValue::Text(SmallText::from_string("text"))
        );
        assert_eq!(
            invoke1(&TypeofFunc, SqliteValue::Blob(Arc::from([0].as_slice()))).unwrap(),
            SqliteValue::Text(SmallText::from_string("blob"))
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
                SqliteValue::Text(SmallText::from_string("hello world")),
                SqliteValue::Text(SmallText::from_string("world")),
                SqliteValue::Text(SmallText::from_string("earth")),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("hello earth"))
        );
    }

    #[test]
    fn test_replace_empty_y() {
        let f = ReplaceFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Text(SmallText::new("")),
                SqliteValue::Text(SmallText::from_string("x")),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("hello"))
        );
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_replace_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 100_000;
        const REPEATS: usize = 5;

        let f = ReplaceFunc;
        let args = [
            SqliteValue::Text(SmallText::from_string("payload payload payload")),
            SqliteValue::Text(SmallText::from_string("zz")),
            SqliteValue::Text(SmallText::from_string("replacement")),
        ];

        let mut best_ns = u128::MAX;
        let mut result_len = 0usize;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(args.as_slice()))
                        .expect("replace benchmark invocation must succeed"),
                );
                result_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            best_ns = best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "replace_text_args invocations={INVOCATIONS} repeats={REPEATS} best_ns={best_ns} result_len={result_len}"
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
        // C SQLite: math functions return NULL for strings that cannot be parsed as numeric.
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Text(SmallText::from_string("abc"))).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_sign_whitespace_padded_text() {
        // Regression: SIGN('  5  ') must return 1, not NULL.
        // SQLite trims ASCII whitespace before numeric parsing.
        assert_eq!(
            invoke1(
                &SignFunc,
                SqliteValue::Text(SmallText::from_string("  5  "))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            invoke1(
                &SignFunc,
                SqliteValue::Text(SmallText::from_string("  -3.14  "))
            )
            .unwrap(),
            SqliteValue::Integer(-1)
        );
    }

    #[test]
    fn test_sign_unicode_space_and_blob_return_null() {
        assert_eq!(
            invoke1(
                &SignFunc,
                SqliteValue::Text(SmallText::from_string("\u{00a0}123"))
            )
            .unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Blob(Arc::from(b"123".as_slice()))).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_sign_nan_inf_text_returns_null() {
        // C SQLite doesn't recognise "NaN", "inf", "Infinity" etc. as numeric —
        // sign() must return NULL for these, matching the C oracle.
        for s in &[
            "NaN",
            "nan",
            "inf",
            "-inf",
            "Infinity",
            "-Infinity",
            "INF",
            "+nan",
            "+inf",
        ] {
            assert_eq!(
                invoke1(&SignFunc, SqliteValue::Text(SmallText::from_string(*s))).unwrap(),
                SqliteValue::Null,
                "sign('{s}') should be NULL"
            );
        }
    }

    #[test]
    fn test_sign_numeric_overflow_to_infinity() {
        // "1e999" overflows to +inf in both Rust and C. C SQLite's sqlite3AtoF
        // accepts it as numeric, so sign() must return 1 (not NULL).
        assert_eq!(
            invoke1(
                &SignFunc,
                SqliteValue::Text(SmallText::from_string("1e999"))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            invoke1(
                &SignFunc,
                SqliteValue::Text(SmallText::from_string("-1e999"))
            )
            .unwrap(),
            SqliteValue::Integer(-1)
        );
        // Underflow to zero
        assert_eq!(
            invoke1(
                &SignFunc,
                SqliteValue::Text(SmallText::from_string("1e-999"))
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_sign_float_nan_returns_null() {
        // C SQLite: sign(0.0/0.0) = NULL. Float NaN must not return 0.
        assert_eq!(
            invoke1(&SignFunc, SqliteValue::Float(f64::NAN)).unwrap(),
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

    #[test]
    fn test_scalar_min_selects_later_equal_value_while_max_keeps_first() {
        let min = ScalarMinFunc;
        let max = ScalarMaxFunc;
        let numeric = [SqliteValue::Integer(1), SqliteValue::Float(1.0)];
        assert!(matches!(
            min.invoke(&numeric).unwrap(),
            SqliteValue::Float(value) if value == 1.0
        ));
        assert_eq!(max.invoke(&numeric).unwrap(), SqliteValue::Integer(1));

        let text = [
            SqliteValue::Text(SmallText::new("a")),
            SqliteValue::Text(SmallText::new("A")),
        ];
        let nocase = crate::collation::NoCaseCollation;
        assert_eq!(
            min.invoke_with_collation(&text, Some(&nocase)).unwrap(),
            SqliteValue::Text(SmallText::new("A"))
        );
        assert_eq!(
            max.invoke_with_collation(&text, Some(&nocase)).unwrap(),
            SqliteValue::Text(SmallText::new("a"))
        );
    }

    // ── quote ────────────────────────────────────────────────────────────

    #[test]
    fn test_quote_text() {
        assert_eq!(
            invoke1(
                &QuoteFunc,
                SqliteValue::Text(SmallText::from_string("it's"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("'it''s'"))
        );
    }

    #[test]
    fn test_quote_null() {
        assert_eq!(
            invoke1(&QuoteFunc, SqliteValue::Null).unwrap(),
            SqliteValue::Text(SmallText::from_string("NULL"))
        );
    }

    #[test]
    fn test_quote_blob() {
        assert_eq!(
            invoke1(&QuoteFunc, SqliteValue::Blob(Arc::from([0xAB].as_slice()))).unwrap(),
            SqliteValue::Text(SmallText::from_string("X'AB'"))
        );
    }

    #[test]
    fn test_quote_text_truncates_at_first_nul() {
        assert_eq!(
            invoke1(
                &QuoteFunc,
                SqliteValue::Text(SmallText::from_string("A\0B"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("'A'"))
        );
    }

    #[test]
    fn test_unistr_quote_plain_text_matches_quote() {
        assert_eq!(
            invoke1(
                &UnistrQuoteFunc,
                SqliteValue::Text(SmallText::from_string("it's"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("'it''s'"))
        );
    }

    #[test]
    fn test_unistr_quote_escapes_control_chars_and_backslashes() {
        assert_eq!(
            invoke1(
                &UnistrQuoteFunc,
                SqliteValue::Text(SmallText::from_string("a\nb\\c\x01d"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("unistr('a\\u000ab\\\\c\\u0001d')"))
        );
    }

    #[test]
    fn test_unistr_quote_truncates_at_first_nul_before_wrapping() {
        assert_eq!(
            invoke1(
                &UnistrQuoteFunc,
                SqliteValue::Text(SmallText::from_string("A\0\nB"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("'A'"))
        );
    }

    #[test]
    fn test_unistr_decodes_backslash_and_unicode_escapes() {
        assert_eq!(
            invoke1(
                &UnistrFunc,
                SqliteValue::Text(SmallText::from_string(
                    "a\\\\b\\u0020\\U0001f600\\0041\\+000042"
                ))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("a\\b \u{1f600}AB"))
        );
    }

    #[test]
    fn test_unistr_invalid_escape_returns_error() {
        for input in [
            "\\u12xz",
            "\\12xz",
            "\\+00xz",
            "\\",
            "\\x",
            "\\U00110000",
            "\\D800",
        ] {
            let err = invoke1(
                &UnistrFunc,
                SqliteValue::Text(SmallText::from_string(input)),
            )
            .unwrap_err();
            assert_eq!(err.to_string(), INVALID_UNISTR_ESCAPE);
        }
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_unistr_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 500_000;
        const REPEATS: usize = 7;

        let f = UnistrFunc;
        let plain_args = [SqliteValue::Text(SmallText::from_string(
            "plain unicode payload",
        ))];
        let escaped_args = [SqliteValue::Text(SmallText::from_string(
            "a\\\\b\\u0020\\u0048\\u0069\\U0001f600",
        ))];

        let mut plain_best_ns = u128::MAX;
        let mut escaped_best_ns = u128::MAX;
        let mut checksum = 0usize;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(plain_args.as_slice()))
                        .expect("unistr plain benchmark invocation must succeed"),
                );
                if let SqliteValue::Text(text) = result {
                    checksum = checksum.wrapping_add(text.len());
                }
            }
            plain_best_ns = plain_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(escaped_args.as_slice()))
                        .expect("unistr escaped benchmark invocation must succeed"),
                );
                if let SqliteValue::Text(text) = result {
                    checksum = checksum.wrapping_add(text.len());
                }
            }
            escaped_best_ns = escaped_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "unistr_text_args invocations={INVOCATIONS} repeats={REPEATS} plain_best_ns={plain_best_ns} escaped_best_ns={escaped_best_ns} checksum={checksum}"
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

    #[test]
    fn test_randomblob_null_zero_and_negative_lengths_are_one_byte() {
        for arg in [
            SqliteValue::Null,
            SqliteValue::Integer(0),
            SqliteValue::Integer(-5),
        ] {
            let result = invoke1(&RandomblobFunc, arg).unwrap();
            match result {
                SqliteValue::Blob(b) => assert_eq!(b.len(), 1),
                other => unreachable!("expected one-byte blob, got {other:?}"),
            }
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
        let result = invoke1(
            &UnhexFunc,
            SqliteValue::Text(SmallText::from_string("48656C6C6F")),
        )
        .unwrap();
        assert_eq!(result, SqliteValue::Blob(Arc::from(b"Hello".as_slice())));
    }

    #[test]
    fn test_unhex_invalid() {
        let result = invoke1(
            &UnhexFunc,
            SqliteValue::Text(SmallText::from_string("ZZZZ")),
        )
        .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_unhex_ignore_chars() {
        let f = UnhexFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("48-65-6C")),
                SqliteValue::Text(SmallText::from_string("-")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Blob(Arc::from(b"Hel".as_slice())));
    }

    #[test]
    fn test_unhex_ignore_chars_only_between_byte_pairs() {
        let f = UnhexFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("AB CD")),
                SqliteValue::Text(SmallText::from_string(" ")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Blob(Arc::from([0xAB, 0xCD])));

        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("A BCD")),
                SqliteValue::Text(SmallText::from_string(" ")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_unhex_null_ignore_argument_returns_null() {
        let f = UnhexFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("41")),
                SqliteValue::Null,
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_unhex_hex_digits_in_ignore_argument_do_not_ignore_digits() {
        let f = UnhexFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("41")),
                SqliteValue::Text(SmallText::from_string("4")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Blob(Arc::from(b"A".as_slice())));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_unhex_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 300_000;
        const REPEATS: usize = 7;

        let f = UnhexFunc;
        let plain_args = [SqliteValue::Text(SmallText::from_string(
            "48656C6C6F776F726C64",
        ))];
        let ignore_args = [
            SqliteValue::Text(SmallText::from_string("48-65-6C-6C-6F")),
            SqliteValue::Text(SmallText::from_string("-")),
        ];
        let mut plain_best_ns = u128::MAX;
        let mut ignore_best_ns = u128::MAX;
        let mut checksum = 0usize;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(plain_args.as_slice()))
                        .expect("unhex benchmark invocation must succeed"),
                );
                if let SqliteValue::Blob(blob) = result {
                    checksum = checksum.wrapping_add(blob.len());
                }
            }
            plain_best_ns = plain_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(ignore_args.as_slice()))
                        .expect("unhex ignore benchmark invocation must succeed"),
                );
                if let SqliteValue::Blob(blob) = result {
                    checksum = checksum.wrapping_add(blob.len());
                }
            }
            ignore_best_ns = ignore_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "unhex_text_args invocations={INVOCATIONS} repeats={REPEATS} plain_best_ns={plain_best_ns} ignore_best_ns={ignore_best_ns} checksum={checksum}"
        );
    }

    // ── unicode ──────────────────────────────────────────────────────────

    #[test]
    fn test_unicode_first_char() {
        assert_eq!(
            invoke1(&UnicodeFunc, SqliteValue::Text(SmallText::from_string("A"))).unwrap(),
            SqliteValue::Integer(65)
        );
    }

    #[test]
    fn test_unicode_text_stops_at_nul() {
        assert_eq!(
            invoke1(
                &UnicodeFunc,
                SqliteValue::Text(SmallText::from_string("\0A"))
            )
            .unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            invoke1(
                &UnicodeFunc,
                SqliteValue::Text(SmallText::from_string("A\0"))
            )
            .unwrap(),
            SqliteValue::Integer(65)
        );
    }

    #[test]
    fn test_unicode_blob_uses_sqlite_utf8_reader() {
        let cases: &[(&[u8], SqliteValue)] = &[
            (&[0x00, 0x41], SqliteValue::Null),
            (&[0x80], SqliteValue::Integer(128)),
            (&[0xC2, 0x80], SqliteValue::Integer(128)),
            (&[0xC2, 0x80, 0x80], SqliteValue::Integer(8192)),
            (&[0xED, 0xA0, 0x80], SqliteValue::Integer(65_533)),
            (&[0xF4, 0x90, 0x80, 0x80], SqliteValue::Integer(1_114_112)),
        ];

        for (bytes, expected) in cases {
            assert_eq!(
                invoke1(&UnicodeFunc, SqliteValue::Blob(Arc::from(*bytes))).unwrap(),
                expected.clone()
            );
        }
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_unicode_text_arg() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 1_000_000;
        const REPEATS: usize = 7;

        let f = UnicodeFunc;
        let args = [SqliteValue::Text(SmallText::from_string("Alphabet soup"))];
        let mut text_best_ns = u128::MAX;
        let mut checksum = 0i64;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(args.as_slice()))
                        .expect("unicode benchmark invocation must succeed"),
                );
                if let SqliteValue::Integer(codepoint) = result {
                    checksum = checksum.wrapping_add(codepoint);
                }
            }
            text_best_ns = text_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "unicode_text_arg invocations={INVOCATIONS} repeats={REPEATS} text_best_ns={text_best_ns} checksum={checksum}"
        );
    }

    // ── soundex ──────────────────────────────────────────────────────────

    #[test]
    fn test_soundex_basic() {
        assert_eq!(
            invoke1(
                &SoundexFunc,
                SqliteValue::Text(SmallText::from_string("Robert"))
            )
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("R163"))
        );
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_soundex_text_arg() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 1_000_000;
        const REPEATS: usize = 7;

        let f = SoundexFunc;
        let args = [SqliteValue::Text(SmallText::from_string("Robert"))];
        let mut text_best_ns = u128::MAX;
        let mut checksum = 0usize;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(args.as_slice()))
                        .expect("soundex benchmark invocation must succeed"),
                );
                if let SqliteValue::Text(text) = result {
                    checksum = checksum.wrapping_add(text.len());
                }
            }
            text_best_ns = text_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "soundex_text_arg invocations={INVOCATIONS} repeats={REPEATS} text_best_ns={text_best_ns} checksum={checksum}"
        );
    }

    // ── substr ───────────────────────────────────────────────────────────

    #[test]
    fn test_substr_basic() {
        let f = SubstrFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Integer(2),
                SqliteValue::Integer(3),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("ell"))
        );
    }

    #[test]
    fn test_substr_start_zero_quirk() {
        // substr('hello', 0, 3) returns 2 chars from start
        let f = SubstrFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Integer(0),
                SqliteValue::Integer(3),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("he")));
    }

    #[test]
    fn test_substr_negative_start() {
        // substr('hello', -2) = 'lo'
        let f = SubstrFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("hello")),
                SqliteValue::Integer(-2),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("lo")));
    }

    #[test]
    fn test_substr_negative_length() {
        let f = SubstrFunc;
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        let i = SqliteValue::Integer;
        // SUBSTR('hello', 3, -2) => 'he' (2 chars before position 3)
        assert_eq!(f.invoke(&[t("hello"), i(3), i(-2)]).unwrap(), t("he"));
        // SUBSTR('hello', 3, -5) => 'he' (clamped at start)
        assert_eq!(f.invoke(&[t("hello"), i(3), i(-5)]).unwrap(), t("he"));
        // SUBSTR('hello', 1, -1) => '' (nothing before position 1)
        assert_eq!(f.invoke(&[t("hello"), i(1), i(-1)]).unwrap(), t(""));
    }

    #[test]
    fn test_substr_negative_start_negative_length() {
        let f = SubstrFunc;
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        let i = SqliteValue::Integer;
        // SUBSTR('hello', -2, -2) => 'el' (C SQLite confirmed)
        assert_eq!(f.invoke(&[t("hello"), i(-2), i(-2)]).unwrap(), t("el"));
    }

    #[test]
    fn test_substr_edge_cases() {
        let f = SubstrFunc;
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        let i = SqliteValue::Integer;
        // Past end
        assert_eq!(f.invoke(&[t("hello"), i(6), i(2)]).unwrap(), t(""));
        // Way before start
        assert_eq!(f.invoke(&[t("hello"), i(-10), i(3)]).unwrap(), t(""));
        // Negative start covering entire string
        assert_eq!(f.invoke(&[t("hello"), i(-5), i(6)]).unwrap(), t("hello"));
        // start=0, length=1 => '' (quirk)
        assert_eq!(f.invoke(&[t("hello"), i(0), i(1)]).unwrap(), t(""));
        // start=0, negative length
        assert_eq!(f.invoke(&[t("hello"), i(0), i(-1)]).unwrap(), t(""));
        // Empty string
        assert_eq!(f.invoke(&[t(""), i(1), i(1)]).unwrap(), t(""));
    }

    #[test]
    fn test_substr_blob_negative_length() {
        let f = SubstrFunc;
        let i = SqliteValue::Integer;
        let blob = SqliteValue::Blob(Arc::from([1, 2, 3, 4, 5].as_slice()));
        // SUBSTR(X'0102030405', -2, -2) => X'0203' (matches text behavior)
        assert_eq!(
            f.invoke(&[blob, i(-2), i(-2)]).unwrap(),
            SqliteValue::Blob(Arc::from([2, 3].as_slice()))
        );
    }

    // ── like ─────────────────────────────────────────────────────────────

    #[test]
    fn test_like_case_insensitive() {
        assert_eq!(
            invoke2(
                &LikeFunc,
                SqliteValue::Text(SmallText::from_string("ABC")),
                SqliteValue::Text(SmallText::from_string("abc"))
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
                SqliteValue::Text(SmallText::from_string("10\\%")),
                SqliteValue::Text(SmallText::from_string("10%")),
                SqliteValue::Text(SmallText::from_string("\\")),
            ])
            .unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_like_escape_rejects_empty_string() {
        let err = LikeFunc
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Text(SmallText::new("")),
            ])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("ESCAPE expression must be a single character")
        );
    }

    #[test]
    fn test_like_escape_rejects_multi_character_string() {
        let err = LikeFunc
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Text(SmallText::from_string("xx")),
            ])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("ESCAPE expression must be a single character")
        );
    }

    #[test]
    fn test_like_percent() {
        assert_eq!(
            invoke2(
                &LikeFunc,
                SqliteValue::Text(SmallText::from_string("%ell%")),
                SqliteValue::Text(SmallText::from_string("Hello"))
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
                SqliteValue::Text(SmallText::from_string("*.txt")),
                SqliteValue::Text(SmallText::from_string("file.txt"))
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
                SqliteValue::Text(SmallText::from_string("ABC")),
                SqliteValue::Text(SmallText::from_string("abc"))
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_glob_unterminated_character_class_does_not_match() {
        // Regression (#257): an unterminated '[' character class never matches,
        // matching C SQLite's patternCompare which returns 0 at end-of-pattern.
        assert_eq!(
            invoke2(
                &GlobFunc,
                SqliteValue::Text(SmallText::from_string("[a")),
                SqliteValue::Text(SmallText::from_string("a"))
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
        // The properly-closed form still matches.
        assert_eq!(
            invoke2(
                &GlobFunc,
                SqliteValue::Text(SmallText::from_string("[a]")),
                SqliteValue::Text(SmallText::from_string("a"))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
    }

    #[test]
    fn test_glob_trailing_dash_in_character_class_is_literal() {
        // Regression (found via eidetic_engine_cli bd-1eeyw): C SQLite's
        // patternCompare treats a `-` immediately before `]` as a literal
        // class member, never as a range opener. The old parser consumed
        // `:-]` in `[^A-Za-z0-9._:-]` as a range from ':' to ']', swallowed
        // the class terminator, and derailed the rest of the pattern — so
        // `peer_abc/123 GLOB '*[^A-Za-z0-9._:-]*'` returned 0 and a
        // NOT-GLOB CHECK constraint admitted invalid identifiers.
        let glob = |pattern: &str, text: &str| {
            invoke2(
                &GlobFunc,
                SqliteValue::Text(SmallText::from_string(pattern)),
                SqliteValue::Text(SmallText::from_string(text)),
            )
            .unwrap()
        };
        // '/' is outside the allowed set: the negated class must match it.
        assert_eq!(
            glob("*[^A-Za-z0-9._:-]*", "peer_abc/123"),
            SqliteValue::Integer(1)
        );
        // Every allowed byte class: no negated-class match anywhere.
        assert_eq!(
            glob("*[^A-Za-z0-9._:-]*", "peer_a.b:c-"),
            SqliteValue::Integer(0)
        );
        // Positive class: trailing dash is a literal member.
        assert_eq!(glob("[a-c-]", "-"), SqliteValue::Integer(1));
        assert_eq!(glob("[a-c-]", "b"), SqliteValue::Integer(1));
        assert_eq!(glob("[a-c-]", "d"), SqliteValue::Integer(0));
        // A dash as the very first member is likewise literal.
        assert_eq!(glob("[-a]", "-"), SqliteValue::Integer(1));
        assert_eq!(glob("[-a]", "b"), SqliteValue::Integer(0));
    }

    #[test]
    fn test_iif_two_argument_form() {
        // Regression (#183): iif(X, Y) is shorthand for iif(X, Y, NULL) (3.48+).
        let f = IifFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("y")),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("y"))
        );
        assert_eq!(
            f.invoke(&[
                SqliteValue::Integer(0),
                SqliteValue::Text(SmallText::from_string("y")),
            ])
            .unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_format_g_negative_zero() {
        // Regression (#258): printf('%g', -0.0) canonicalizes to '0' (no minus).
        let f = FormatFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("%g")),
                SqliteValue::Float(-0.0),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("0"))
        );
    }

    #[test]
    fn test_format_signed_zero_all_specs() {
        // bd-gh-printf-negative-zero-era4w (#258): -0.0 normalizes to 0 for
        // %f/%e/%g and every sub-path (sign flags, width, alt-form), matching
        // C SQLite 3.46.1; real negatives keep the minus sign.
        let f = FormatFunc;
        let fmt = |spec: &str, v: f64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Float(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        // Negative zero -> canonical zero across specifiers.
        assert_eq!(fmt("%f", -0.0), "0.000000");
        assert_eq!(fmt("%e", -0.0), "0.000000e+00");
        assert_eq!(fmt("%E", -0.0), "0.000000E+00");
        assert_eq!(fmt("%G", -0.0), "0");
        // Sign flags apply to the normalized +0.0.
        assert_eq!(fmt("%+g", -0.0), "+0");
        assert_eq!(fmt("% g", -0.0), " 0");
        assert_eq!(fmt("%+f", -0.0), "+0.000000");
        // Width/precision.
        assert_eq!(fmt("%8.2f", -0.0), "    0.00");
        // Alt-form (`!`) path also normalizes.
        assert_eq!(fmt("%!g", -0.0), "0.0");
        // Arithmetic-produced negative zero (underflow) normalizes too.
        assert_eq!(fmt("%g", -1e-320 * 1e-10), "0");
        // Real negatives are UNCHANGED (regression guard).
        assert_eq!(fmt("%g", -1.5), "-1.5");
        assert_eq!(fmt("%f", -2.25), "-2.250000");
        assert_eq!(fmt("%+g", -1.5), "-1.5");
    }

    #[test]
    fn test_format_g_integer_trailing_zeros() {
        // bd-v4ujl: %g must not strip significant integer trailing zeros when the
        // value rounds to an integer (decimal_places == 0). C/SQLite 3.46.1:
        // printf('%g', 100000.0) -> "100000", never "1". Expected values below
        // are oracle-verified against sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: f64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Float(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        // The bug: integer-valued %g stripped its trailing zeros to a single digit.
        assert_eq!(fmt("%g", 100000.0), "100000");
        assert_eq!(fmt("%g", 120000.0), "120000");
        assert_eq!(fmt("%g", 250000.0), "250000");
        assert_eq!(fmt("%g", 100.0), "100");
        assert_eq!(fmt("%g", 999999.0), "999999");
        assert_eq!(fmt("%G", 100000.0), "100000");
        // Fractional %g still trims trailing zeros (regression guard).
        assert_eq!(fmt("%g", 0.5), "0.5");
        assert_eq!(fmt("%g", 1.5), "1.5");
        // Exponential branch (exp >= sig) is unaffected by the guard.
        assert_eq!(fmt("%g", 1000000.0), "1e+06");
        assert_eq!(fmt("%g", 1234560.0), "1.23456e+06");
        assert_eq!(fmt("%G", 1000000.0), "1E+06");
    }

    #[test]
    fn test_format_c_field_width_bd_ul4c0() {
        // bd-ul4c0: printf %c honors field width, counted in CHARACTERS — the
        // single emitted char is one width unit regardless of byte length
        // (unlike %s, which counts bytes) — padded with spaces (the '0' flag is
        // ignored), right- or left-justified. %c still emits the FIRST char of
        // the argument's text form (bd-47mu0). Oracle-verified vs sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: SqliteValue| -> String {
            match f
                .invoke(&[SqliteValue::Text(SmallText::from_string(spec)), v])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let txt = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        // The bug: %c emitted the char but ignored field width entirely.
        assert_eq!(fmt(">%3c<", SqliteValue::Integer(65)), ">  6<");
        assert_eq!(fmt(">%-3c<", SqliteValue::Integer(65)), ">6  <");
        // First char of the text form, then width padding.
        assert_eq!(fmt(">%5c<", txt("abc")), ">    a<");
        assert_eq!(fmt(">%-5c<", txt("abc")), ">a    <");
        // The '0' flag does NOT zero-pad %c; padding stays spaces.
        assert_eq!(fmt(">%03c<", SqliteValue::Integer(65)), ">  6<");
        // Width counts CHARACTERS, not bytes: 'é' (2 UTF-8 bytes) is one unit.
        assert_eq!(fmt(">%3c<", txt("é")), ">  é<");
        // No width => just the first char (regression guard for bd-47mu0).
        assert_eq!(fmt(">%c<", SqliteValue::Integer(65)), ">6<");
        assert_eq!(fmt(">%c<", txt("abc")), ">a<");
    }

    #[test]
    fn test_format_quote_specifiers_field_width_bd_8959m() {
        // bd-8959m: printf %q/%Q/%w honor field width (byte-counted like %s,
        // space-padded, right/left-justified). %q NULL renders "(NULL)" and %Q
        // NULL renders "NULL", both padded; %w NULL stays empty. Width is a
        // minimum (never truncates). Oracle-verified vs sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: SqliteValue| -> String {
            match f
                .invoke(&[SqliteValue::Text(SmallText::from_string(spec)), v])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let txt = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        // %q width (the bug: width was ignored) + escaping + NULL.
        assert_eq!(fmt(">%6q<", txt("ab")), ">    ab<");
        assert_eq!(fmt(">%-6q<", txt("ab")), ">ab    <");
        assert_eq!(fmt(">%8q<", SqliteValue::Null), ">  (NULL)<");
        assert_eq!(fmt(">%8q<", txt("a'b")), ">    a''b<");
        assert_eq!(fmt(">%3q<", txt("abcde")), ">abcde<"); // width is a minimum
        // %Q: quote-wrapped, width applies to the whole token.
        assert_eq!(fmt(">%6Q<", txt("ab")), ">  'ab'<");
        assert_eq!(fmt(">%-6Q<", txt("ab")), ">'ab'  <");
        assert_eq!(fmt(">%6Q<", SqliteValue::Null), ">  NULL<");
        // %w: identifier escaping, width on the non-NULL rendering.
        assert_eq!(fmt(">%6w<", txt("ab")), ">    ab<");
        assert_eq!(fmt(">%-6w<", txt("ab")), ">ab    <");
        // Byte-counted width (matches %s): 'é' is two UTF-8 bytes.
        assert_eq!(fmt(">%4q<", txt("é")), ">  é<");
        // No width => unchanged (regression guard).
        assert_eq!(fmt(">%q<", txt("ab")), ">ab<");
        assert_eq!(fmt(">%Q<", txt("ab")), ">'ab'<");
    }

    #[test]
    fn test_format_round_half_away_from_zero_bd_o1tu1() {
        // bd-o1tu1: printf/format float conversions %f/%e/%g must round exact
        // binary half-ties AWAY FROM ZERO (C SQLite) rather than Rust's
        // round-half-to-even. Non-tie values (e.g. 0.135, 1.005, 2.675, 0.15)
        // are NOT exact binary ties and MUST stay on their correctly-rounded
        // value. Every expected string below was produced by running
        // `sqlite3 :memory: "SELECT printf('<spec>', <val>);"` against stock
        // sqlite3 3.46.1 — assert exactly that, never a guess.
        let f = FormatFunc;
        let fmt = |spec: &str, v: f64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Float(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let cases: &[(&str, f64, &str)] = &[
            // %f exact ties -> away from zero.
            ("%.0f", 2.5, "3"),
            ("%.0f", 0.5, "1"),
            ("%.0f", -2.5, "-3"),
            ("%.0f", 3.5, "4"),
            ("%.0f", -0.5, "-1"),
            ("%.0f", -3.5, "-4"),
            ("%.0f", 1.5, "2"),
            ("%.2f", 0.125, "0.13"),
            ("%.2f", 0.375, "0.38"),
            ("%.2f", 0.625, "0.63"),
            ("%.2f", 2.125, "2.13"),
            ("%.2f", -0.125, "-0.13"),
            ("%.1f", 0.25, "0.3"),
            ("%.1f", 0.75, "0.8"),
            ("%.1f", 2.25, "2.3"),
            ("%.1f", -0.25, "-0.3"),
            ("%.1f", 0.05, "0.1"),
            ("%.0f", 12.5, "13"),
            ("%.2f", 12.5, "12.50"),
            // %f non-ties -> unchanged (correctly-rounded true value).
            ("%.2f", 0.135, "0.14"),
            ("%.2f", 0.35, "0.35"),
            ("%.2f", 0.15, "0.15"),
            ("%.2f", 0.85, "0.85"),
            ("%.2f", 0.95, "0.95"),
            ("%.2f", 1.005, "1.00"),
            ("%.2f", 2.675, "2.67"),
            ("%.2f", 0.005, "0.01"),
            ("%.2f", 0.015, "0.01"),
            ("%.2f", 0.025, "0.03"),
            ("%.1f", 0.35, "0.3"),
            ("%.1f", 0.15, "0.1"),
            ("%.1f", 0.135, "0.1"),
            ("%.0f", 2.675, "3"),
            ("%.0f", 0.49999, "0"),
            // Sign / width / uppercase interplay applied AFTER rounding.
            ("%+.0f", 2.5, "+3"),
            ("%8.0f", 2.5, "       3"),
            // %e exact mantissa ties -> away (carry may bump the exponent).
            ("%.0e", 2.5, "3e+00"),
            ("%.0e", 9.5, "1e+01"),
            ("%.0e", 1.5, "2e+00"),
            ("%.0e", 250.0, "3e+02"),
            ("%.0e", 0.25, "3e-01"),
            ("%.1e", 1.25, "1.3e+00"),
            ("%.1e", 12.5, "1.3e+01"),
            ("%.0E", 2.5, "3E+00"),
            // %e non-ties -> unchanged.
            ("%.1e", 0.5, "5.0e-01"),
            ("%.1e", 9.95, "9.9e+00"),
            ("%.1e", 1.005, "1.0e+00"),
            ("%.1e", 2.675, "2.7e+00"),
            ("%.1e", 1.35, "1.4e+00"),
            ("%.0e", 0.5, "5e-01"),
            ("%.0e", 9.95, "1e+01"),
            ("%.0e", 1.005, "1e+00"),
            // %g exact ties (precision is significant digits) -> away.
            ("%.1g", 0.25, "0.3"),
            ("%.1g", 2.5, "3"),
            ("%.1g", 25.0, "3e+01"),
            ("%.2g", 0.125, "0.13"),
            ("%.2g", 1.25, "1.3"),
            ("%.2g", 12.5, "13"),
            // %g non-ties -> unchanged.
            ("%.1g", 0.35, "0.3"),
            ("%.1g", 0.15, "0.1"),
            ("%.1g", 0.45, "0.5"),
            ("%.1g", 0.125, "0.1"),
            ("%.2g", 0.135, "0.14"),
            ("%.2g", 1.005, "1"),
            ("%.2g", 2.675, "2.7"),
        ];
        for (spec, v, want) in cases {
            assert_eq!(fmt(spec, *v), *want, "spec={spec} v={v}");
        }
    }

    #[test]
    fn test_round_half_away_near_ties_match_oracle_bd_o1tu1() {
        // bd-o1tu1: round() shares the fixed-notation half-away helper, so its
        // exact-tie detection must NOT misfire on near-ties whose double is not
        // a true binary half (a small guard would round e.g. 0.15's 0.14999…
        // into a spurious 0.1500…). Values below are stock sqlite3 3.46.1
        // `SELECT round(v, 1);` results.
        #[allow(clippy::float_cmp)]
        fn round1(v: f64) -> f64 {
            match RoundFunc
                .invoke(&[SqliteValue::Float(v), SqliteValue::Integer(1)])
                .unwrap()
            {
                SqliteValue::Float(x) => x,
                other => panic!("expected float, got {other:?}"),
            }
        }
        let cases: &[(f64, f64)] = &[
            (0.15, 0.1),
            (0.35, 0.3),
            (0.85, 0.8),
            (0.95, 0.9),
            (0.135, 0.1),
            (1.005, 1.0),
            (2.675, 2.7),
            // 0.25 is a genuine exact tie -> away from zero (0.3); 0.45's
            // double is 0.45000…111 so it rounds up on its true value; 2.5 has
            // no digit past precision 1 and is returned unchanged.
            (0.25, 0.3),
            (0.45, 0.5),
            (2.5, 2.5),
        ];
        for (v, want) in cases {
            #[allow(clippy::float_cmp)]
            let got = round1(*v);
            assert_eq!(got, *want, "round({v}, 1)");
        }
    }

    #[test]
    fn test_format_altform2_flag() {
        // Regression (#176): the '!' (alternate-form-2) flag is accepted. For
        // string/int conversions the value formats normally; for %f it selects
        // the shortest round-trip form with a decimal point.
        let f = FormatFunc;
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("%!5s")),
                SqliteValue::Text(SmallText::from_string("ab")),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("   ab"))
        );
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("%!d")),
                SqliteValue::Integer(3),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("3"))
        );
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("%!f")),
                SqliteValue::Float(0.1),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("0.1"))
        );
    }

    #[test]
    fn test_format_altform2_precision_and_width() {
        // The '!' (alternate-form-2) flag on %f applies the requested precision
        // FIRST and then strips trailing fractional zeros (keeping >= 1 digit;
        // ".0" is forced at precision 0), with width/sign flags applied last.
        // Frank previously used Rust's shortest round-trip form and ignored an
        // explicit precision, so '%!5.2f' 3.14159 rendered "3.14159" instead of
        // " 3.14" (probe-found divergence). Oracle: sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: f64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Float(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let cases: &[(&str, f64, &str)] = &[
            ("%!f", 0.1, "0.1"),
            ("%!5.2f", 3.14159, " 3.14"),
            ("%!.3f", 1.5, "1.5"),
            ("%!f", 3.14159, "3.14159"),
            ("%!f", 5.0, "5.0"),
            ("%!f", 5.5, "5.5"),
            ("%!.0f", 5.5, "6.0"),
            ("%!f", -0.5, "-0.5"),
            ("%+!f", 0.5, "+0.5"),
            ("%!f", 100.0, "100.0"),
            ("%!8.2f", 3.14159, "    3.14"),
            ("%!08.3f", 1.5, "000001.5"),
            ("%!10.2f", 3.14159, "      3.14"),
        ];
        for (spec, v, want) in cases {
            assert_eq!(fmt(spec, *v), *want, "spec={spec} v={v}");
        }
    }

    #[test]
    fn test_format_comma_grouping_flag() {
        // SQLite's `,` printf flag groups the integer digits into thousands.
        // Applies to %d/%i/%u and the integer part of %f; it is accepted but
        // inert for %e/%g/%x. Frank previously did not recognize `,` as a flag
        // and emitted the spec verbatim ('%,d' 1234567 -> "%,d"; probe-found).
        // Zero padding pads the raw digits BEFORE grouping ('%,08d' 1234 ->
        // "00,001,234"); space padding is applied AFTER grouping. Oracle:
        // sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: SqliteValue| -> String {
            match f
                .invoke(&[SqliteValue::Text(SmallText::from_string(spec)), v])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let int_cases: &[(&str, i64, &str)] = &[
            ("%,d", 1234567, "1,234,567"),
            ("%,d", -1234567, "-1,234,567"),
            ("%,d", 123, "123"),
            ("%,d", 1000, "1,000"),
            ("%,d", 0, "0"),
            ("%,d", -100, "-100"),
            ("%,d", 1000000, "1,000,000"),
            ("%,10d", 1234567, " 1,234,567"),
            ("%,08d", 1234, "00,001,234"),
            ("%+,d", 1234567, "+1,234,567"),
            ("%, d", 1234567, " 1,234,567"),
            ("%-,12d", 1234567, "1,234,567   "),
            ("%,i", 1234567, "1,234,567"),
            ("%,u", 1234567, "1,234,567"),
            ("%,x", 1234567, "12d687"),
        ];
        for (spec, v, want) in int_cases {
            assert_eq!(
                fmt(spec, SqliteValue::Integer(*v)),
                *want,
                "spec={spec} v={v}"
            );
        }
        let float_cases: &[(&str, f64, &str)] = &[
            ("%,f", 1234567.5, "1,234,567.500000"),
            ("%,.2f", 1234567.891, "1,234,567.89"),
            ("%,f", -1234.5, "-1,234.500000"),
            ("%,e", 1234.5, "1.234500e+03"),
            // %g is grouped only in fixed (non-exponential) form.
            ("%,g", 1234.5, "1,234.5"),
            ("%,g", 12.0, "12"),
            ("%,g", 1234567.0, "1.23457e+06"),
            ("%,g", 1000000.0, "1e+06"),
            ("%,.2g", 1234.5, "1.2e+03"),
        ];
        for (spec, v, want) in float_cases {
            assert_eq!(
                fmt(spec, SqliteValue::Float(*v)),
                *want,
                "spec={spec} v={v}"
            );
        }
    }

    #[test]
    fn test_format_integer_precision() {
        // Integer precision (%.Nd) is the MINIMUM digit count: the digits are
        // zero-padded to N, with sign/width applied outside. Applies to
        // %d/%i/%u/%x/%X/%o. Frank previously ignored precision on integers
        // ('%.3d' 5 -> "5"; probe-found divergence). Oracle: sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: i64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Integer(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let cases: &[(&str, i64, &str)] = &[
            ("%.3d", 5, "005"),
            ("%.3d", -5, "-005"),
            ("%.0d", 0, "0"),
            ("%.0d", 5, "5"),
            ("%5.3d", 42, "  042"),
            ("%-5.3d", 42, "042  "),
            ("%.3d", 12345, "12345"),
            ("%+.3d", 5, "+005"),
            ("% .3d", 5, " 005"),
            ("%08.3d", 42, "00000042"),
            ("%.3i", 9, "009"),
            ("%.3u", 7, "007"),
            ("%.3x", 10, "00a"),
            ("%.3o", 8, "010"),
        ];
        for (spec, v, want) in cases {
            assert_eq!(fmt(spec, *v), *want, "spec={spec} v={v}");
        }
    }

    #[test]
    fn test_format_altform2_exponential() {
        // The '!' flag on %e/%E strips trailing zeros from the mantissa (keeping
        // >= 1 fractional digit), then reattaches the exponent. Oracle: sqlite3
        // 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: f64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Float(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let cases: &[(&str, f64, &str)] = &[
            ("%!e", 3.14159, "3.14159e+00"),
            ("%!E", 3.14159, "3.14159E+00"),
            ("%!e", 5.0, "5.0e+00"),
            ("%!.2e", 3.14159, "3.14e+00"),
            ("%!.0e", 3.0, "3.0e+00"),
        ];
        for (spec, v, want) in cases {
            assert_eq!(fmt(spec, *v), *want, "spec={spec} v={v}");
        }
    }

    #[test]
    fn test_format_altform2_g_honors_precision() {
        // bd-g7pfx: '!' on %g formats at the requested significant digits
        // (precision 0 => 1 sig fig, so the fixed/exponential choice is honored),
        // then ensures a decimal point with >= 1 fractional digit. Previously the
        // path used shortest-round-trip and ignored precision. Oracle: sqlite3
        // 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str, v: f64| -> String {
            match f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(spec)),
                    SqliteValue::Float(v),
                ])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let cases: &[(&str, f64, &str)] = &[
            ("%!g", 12345.0, "12345.0"),
            ("%!.0g", 12345.0, "1.0e+04"),
            ("%!.1g", 12345.0, "1.0e+04"),
            ("%!.3g", 12345.0, "1.23e+04"),
            ("%!.2g", 0.000123, "0.00012"),
            ("%!g", 100.0, "100.0"),
            ("%!.0g", 5.0, "5.0"),
            ("%!g", 0.1, "0.1"),
            ("%!G", 12345.0, "12345.0"),
            ("%!.0G", 12345.0, "1.0E+04"),
        ];
        for (spec, v, want) in cases {
            assert_eq!(fmt(spec, *v), *want, "spec={spec} v={v}");
        }
    }

    // ── format ───────────────────────────────────────────────────────────

    #[test]
    fn test_format_specifiers() {
        let f = FormatFunc;
        let result = f
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("%d %s")),
                SqliteValue::Integer(42),
                SqliteValue::Text(SmallText::from_string("hello")),
            ])
            .unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("42 hello"))
        );
    }

    #[test]
    fn test_format_n_noop() {
        let f = FormatFunc;
        // %n should not crash or do anything
        let result = f
            .invoke(&[SqliteValue::Text(SmallText::from_string("before%nafter"))])
            .unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("beforeafter"))
        );
    }

    #[test]
    fn test_format_literal_percent_honors_width() {
        // bd-g27fn: a literal `%` conversion honors the field width, space-padded
        // and right/left-justified (the `0` flag pads with spaces since `%` is
        // non-numeric). Oracle: sqlite3 3.46.1.
        let f = FormatFunc;
        let fmt = |spec: &str| -> String {
            match f
                .invoke(&[SqliteValue::Text(SmallText::from_string(spec))])
                .unwrap()
            {
                SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        assert_eq!(fmt("%%"), "%");
        assert_eq!(fmt("%5%"), "    %");
        assert_eq!(fmt("%-5%"), "%    ");
        assert_eq!(fmt("%05%"), "    %");
        assert_eq!(fmt("[%3%]"), "[  %]");
    }

    #[test]
    fn test_format_alternate_form_hex_octal() {
        // bd-w54bm: `#` flag prefixes 0x/0X (hex) or 0 (octal) for nonzero values.
        let cases: &[(&str, i64, &str)] = &[
            ("%#x", 255, "0xff"),
            ("%#X", 255, "0XFF"),
            ("%#o", 64, "0100"),
            ("%#x", 0, "0"),        // zero gets no prefix
            ("%#o", 0, "0"),        // zero gets no prefix
            ("%#5x", 255, " 0xff"), // prefix counts toward space pad
            ("%#8x", 255, "    0xff"),
            ("%#08x", 255, "0x000000ff"), // zero pad pads digits, prefix outside
            ("%-#8x", 255, "0xff    "),   // '-' (no '0') -> space pad, left aligned
            ("%-08x", 255, "000000ff"),   // '-' does NOT override '0' in SQLite
            ("%#08o", 64, "000000100"),
            ("%#x", -1, "0xffffffffffffffff"),
        ];
        for (fmt, arg, want) in cases {
            let f = FormatFunc;
            let result = f
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(*fmt)),
                    SqliteValue::Integer(*arg),
                ])
                .unwrap();
            assert_eq!(
                result,
                SqliteValue::Text(SmallText::from_string((*want).to_owned())),
                "format({fmt:?}, {arg})"
            );
        }
    }

    #[test]
    fn test_format_empty_string_is_null() {
        // bd-13ivh: an empty format string yields NULL (the StrAccum is never
        // touched), while a non-empty format that renders to nothing still
        // yields empty TEXT.
        let f = FormatFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Text(SmallText::from_string(""))])
                .unwrap(),
            SqliteValue::Null
        );
        // Non-empty format rendering to empty output is still TEXT, not NULL.
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text(SmallText::from_string("%s")),
                SqliteValue::Null,
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string(String::new()))
        );
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

    #[test]
    fn test_sqlite_compileoption_used_matches_sqlite_prefix_and_value_options() {
        let func = SqliteCompileoptionUsedFunc;
        assert_eq!(
            invoke1(
                &func,
                SqliteValue::Text(SmallText::from_string("THREADSAFE"))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
        let expected_icu_enabled = i64::from(cfg!(feature = "ext-icu"));
        assert_eq!(
            invoke1(
                &func,
                SqliteValue::Text(SmallText::from_string("SQLITE_ENABLE_ICU"))
            )
            .unwrap(),
            SqliteValue::Integer(expected_icu_enabled)
        );
        assert_eq!(
            invoke1(
                &func,
                SqliteValue::Text(SmallText::from_string("sqlite_enable_icu"))
            )
            .unwrap(),
            SqliteValue::Integer(expected_icu_enabled)
        );
        assert_eq!(
            invoke1(
                &func,
                SqliteValue::Text(SmallText::from_string("OMIT_LOAD_EXTENSION"))
            )
            .unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            invoke1(
                &func,
                SqliteValue::Text(SmallText::from_string("ENABLE_FTS3"))
            )
            .unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            invoke1(&func, SqliteValue::Null).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_compileoption_used_text_args() {
        use std::hint::black_box;
        use std::time::Instant;

        const INVOCATIONS: usize = 1_000_000;
        const REPEATS: usize = 7;

        let f = SqliteCompileoptionUsedFunc;
        let present_args = [SqliteValue::Text(SmallText::from_string(
            "SQLITE_ENABLE_ICU",
        ))];
        let absent_args = [SqliteValue::Text(SmallText::from_string(
            "ENABLE_NOT_PRESENT",
        ))];

        let mut present_best_ns = u128::MAX;
        let mut absent_best_ns = u128::MAX;
        let mut checksum = 0i64;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(present_args.as_slice()))
                        .expect("compileoption present benchmark invocation must succeed"),
                );
                if let SqliteValue::Integer(value) = result {
                    checksum = checksum.wrapping_add(value);
                }
            }
            present_best_ns = present_best_ns.min(started.elapsed().as_nanos());

            let started = Instant::now();
            for _ in 0..INVOCATIONS {
                let result = black_box(
                    f.invoke(black_box(absent_args.as_slice()))
                        .expect("compileoption absent benchmark invocation must succeed"),
                );
                if let SqliteValue::Integer(value) = result {
                    checksum = checksum.wrapping_add(value);
                }
            }
            absent_best_ns = absent_best_ns.min(started.elapsed().as_nanos());
        }

        println!(
            "compileoption_used_text_args invocations={INVOCATIONS} repeats={REPEATS} present_best_ns={present_best_ns} absent_best_ns={absent_best_ns} checksum={checksum}"
        );
    }

    #[test]
    fn test_sqlite_compileoption_get_enumerates_canonical_option_list() {
        let func = SqliteCompileoptionGetFunc;
        for (index, option) in sqlite_compile_options().iter().enumerate() {
            assert_eq!(
                invoke1(&func, SqliteValue::Integer(index as i64)).unwrap(),
                SqliteValue::Text(SmallText::new(option))
            );
        }
        assert_eq!(
            invoke1(&func, SqliteValue::Integer(-1)).unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            invoke1(
                &func,
                SqliteValue::Integer(sqlite_compile_options().len() as i64)
            )
            .unwrap(),
            SqliteValue::Null
        );
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
        assert!(registry.find_scalar("unistr_quote", 1).is_some());

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
    fn test_register_builtins_rejects_invalid_variadic_arities() {
        let mut registry = FunctionRegistry::new();
        register_builtins(&mut registry);

        for (name, too_few, valid, too_many) in [
            ("coalesce", 1, 2, None),
            ("concat", 0, 1, None),
            ("concat_ws", 1, 2, None),
            ("trim", 0, 1, Some(3)),
            ("ltrim", 0, 1, Some(3)),
            ("rtrim", 0, 1, Some(3)),
            ("round", 0, 1, Some(3)),
            ("unhex", 0, 1, Some(3)),
            ("substr", 1, 2, Some(4)),
            ("substring", 1, 2, Some(4)),
            ("max", 0, 1, None),
            ("min", 0, 1, None),
        ] {
            assert_wrong_arg_count(&registry, name, too_few);
            assert!(
                registry.find_scalar(name, valid).is_some(),
                "{name}/{valid} should resolve"
            );
            if let Some(arity) = too_many {
                assert_wrong_arg_count(&registry, name, arity);
            }
        }

        assert!(registry.find_scalar("char", 0).is_some());
        assert!(registry.find_scalar("format", 0).is_some());
        assert!(registry.find_scalar("printf", 0).is_some());
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
                .invoke(&[SqliteValue::Text(SmallText::from_string("hello"))])
                .unwrap(),
            SqliteValue::Text(SmallText::from_string("text"))
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
        assert!(!SqliteVersionFunc.is_deterministic());
        assert!(!SqliteSourceIdFunc.is_deterministic());
        assert!(!SqliteCompileoptionUsedFunc.is_deterministic());
        assert!(!SqliteCompileoptionGetFunc.is_deterministic());
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

        for (name, num_args) in [
            ("sqlite_version", 0),
            ("sqlite_source_id", 0),
            ("sqlite_compileoption_used", 1),
            ("sqlite_compileoption_get", 1),
        ] {
            assert_eq!(
                registry.scalar_is_deterministic(name, num_args),
                Some(false),
                "{name} must publish non-deterministic registry metadata"
            );
        }

        // Deterministic function check.
        let abs = registry.find_scalar("abs", 1).unwrap();
        assert!(abs.is_deterministic());
    }

    #[test]
    fn test_registry_builtin_query_constancy_metadata() {
        use crate::{ScalarQueryConstancy, ScalarSchemaSafety};

        let mut registry = FunctionRegistry::default();
        register_builtins(&mut registry);

        for (name, num_args) in [
            ("sqlite_version", 0),
            ("sqlite_source_id", 0),
            ("sqlite_compileoption_used", 1),
            ("sqlite_compileoption_get", 1),
        ] {
            let resolved = registry.resolve_scalar(name, num_args).unwrap();
            assert_eq!(resolved.schema_safety(), ScalarSchemaSafety::Never);
            assert_eq!(
                resolved.query_constancy(),
                ScalarQueryConstancy::SlowChanging,
                "{name}/{num_args} must match SQLite's slow-changing metadata"
            );
        }

        for (name, num_args) in [
            ("date", 0),
            ("time", 0),
            ("datetime", 0),
            ("julianday", 0),
            ("unixepoch", 0),
            ("strftime", 1),
            ("timediff", 2),
        ] {
            let resolved = registry.resolve_scalar(name, num_args).unwrap();
            assert_eq!(
                resolved.schema_safety(),
                ScalarSchemaSafety::DateTimeConditional
            );
            assert_eq!(
                resolved.query_constancy(),
                ScalarQueryConstancy::SlowChanging,
                "{name}/{num_args} must be query-constant despite conditional schema safety"
            );
        }

        for (name, num_args) in [
            ("random", 0),
            ("randomblob", 1),
            ("changes", 0),
            ("total_changes", 0),
            ("last_insert_rowid", 0),
        ] {
            assert_eq!(
                registry
                    .resolve_scalar(name, num_args)
                    .unwrap()
                    .query_constancy(),
                ScalarQueryConstancy::Volatile,
                "{name}/{num_args} must remain volatile"
            );
        }

        for (name, num_args) in [("abs", 1), ("like", 2), ("like", 3), ("glob", 2)] {
            assert_eq!(
                registry
                    .resolve_scalar(name, num_args)
                    .unwrap()
                    .query_constancy(),
                ScalarQueryConstancy::Constant,
                "{name}/{num_args} must remain constant"
            );
        }

        for (name, num_args) in [
            ("sqlite_version", 1),
            ("sqlite_compileoption_used", 0),
            ("like", 1),
            ("like", 4),
            ("glob", 1),
        ] {
            assert_eq!(
                registry
                    .resolve_scalar(name, num_args)
                    .unwrap()
                    .query_constancy(),
                ScalarQueryConstancy::Volatile,
                "{name}/{num_args} wrong-arity sentinel must fail closed"
            );
        }
    }
}
