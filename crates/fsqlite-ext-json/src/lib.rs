//! JSON1 foundations for `fsqlite-ext-json` (`bd-3cvl`).
//!
//! This module currently provides:
//! - JSON validation/minification (`json`, `json_valid`)
//! - JSONB encode/decode helpers (`jsonb`, `jsonb_*`, `json_valid` JSONB flags)
//! - JSON type inspection (`json_type`)
//! - JSON path extraction with SQLite-like single vs multi-path semantics (`json_extract`)
//! - JSON value constructors and aggregates (`json_quote`, `json_array`, `json_object`,
//!   `json_group_array`, `json_group_object`)
//! - mutators (`json_set`, `json_insert`, `json_array_insert`, `json_replace`, `json_remove`, `json_patch`)
//! - formatting and diagnostics (`json_pretty`, `json_error_position`, `json_array_length`)
//!
//! Path support in this slice:
//! - `$` root
//! - `$.key` object member
//! - `$."key.with.dots"` quoted object member
//! - `$[N]` array index
//! - `$[#]` append pseudo-index
//! - `$[#-N]` reverse array index

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::{
    AggregateFunction, ColumnContext, FunctionRegistry, IndexInfo, JSON_SUBTYPE, ScalarFunction,
    VirtualTable, VirtualTableCursor, WindowFunction,
};
use fsqlite_types::{SmallText, SqliteValue, cx::Cx};
use serde_json::Number;

/// Maximum inline UTF-8 byte length stored inside [`InlineStr`] without heap
/// allocation. With `Box<str>` (16 bytes) for the heap variant, `InlineStr` is
/// 24 bytes — the same size as `String` — while eliminating heap allocations
/// for virtually all JSON object keys, float literals, and short string values.
const INLINE_STR_CAP: usize = 22;

#[derive(Clone)]
enum InlineStr {
    Inline { len: u8, buf: [u8; INLINE_STR_CAP] },
    Heap(Box<str>),
}

impl InlineStr {
    #[inline]
    fn new(s: &str) -> Self {
        if s.len() <= INLINE_STR_CAP {
            let mut buf = [0u8; INLINE_STR_CAP];
            buf[..s.len()].copy_from_slice(s.as_bytes());
            Self::Inline {
                len: u8::try_from(s.len()).expect("len <= INLINE_STR_CAP fits in u8"),
                buf,
            }
        } else {
            Self::Heap(Box::from(s))
        }
    }

    #[inline]
    fn from_string(s: String) -> Self {
        if s.len() <= INLINE_STR_CAP {
            Self::new(&s)
        } else {
            Self::Heap(s.into_boxed_str())
        }
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline { len, buf } => &buf[..usize::from(*len)],
            Self::Heap(text) => text.as_bytes(),
        }
    }

    #[inline]
    fn as_str(&self) -> &str {
        match self {
            Self::Inline { len, buf } => std::str::from_utf8(&buf[..usize::from(*len)])
                .expect("InlineStr invariant: valid UTF-8"),
            Self::Heap(text) => text,
        }
    }
}

impl std::ops::Deref for InlineStr {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for InlineStr {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for InlineStr {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for InlineStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for InlineStr {}

impl PartialEq<str> for InlineStr {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<&str> for InlineStr {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl std::hash::Hash for InlineStr {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Match `str::hash` verbatim so `Borrow<str>` hash invariants hold.
        std::hash::Hash::hash(self.as_str(), state);
    }
}

impl std::fmt::Debug for InlineStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_str(), f)
    }
}

impl From<String> for InlineStr {
    #[inline]
    fn from(value: String) -> Self {
        Self::from_string(value)
    }
}

impl From<&str> for InlineStr {
    #[inline]
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Fast non-cryptographic hasher for large JSON objects (`>= INDEX_THRESHOLD`
/// members), avoiding SipHash overhead while keeping key order in `Object`.
#[derive(Debug, Default, Clone)]
struct FxBuildHasher;

struct FxHasher {
    hash: u64,
}

impl std::hash::BuildHasher for FxBuildHasher {
    type Hasher = FxHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        FxHasher { hash: 0 }
    }
}

impl std::hash::Hasher for FxHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        const K: u64 = 0x517c_c1b7_2722_0a95;
        let mut hash = self.hash;
        let (chunks, rem) = bytes.as_chunks::<8>();
        for &chunk in chunks {
            let word = u64::from_ne_bytes(chunk);
            hash = (hash.rotate_left(5) ^ word).wrapping_mul(K);
        }
        if !rem.is_empty() {
            let mut tail = 0u64;
            for (i, &b) in rem.iter().enumerate() {
                tail |= u64::from(b) << (i * 8);
            }
            hash = (hash.rotate_left(5) ^ tail).wrapping_mul(K);
        }
        self.hash = hash;
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        const K: u64 = 0x517c_c1b7_2722_0a95;
        self.hash = (self.hash.rotate_left(5) ^ u64::from(i)).wrapping_mul(K);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        const K: u64 = 0x517c_c1b7_2722_0a95;
        self.hash = (self.hash.rotate_left(5) ^ (i as u64)).wrapping_mul(K);
    }
}

/// JSON objects are ordered sequences of members, not maps: SQLite retains
/// repeated labels and a path selects the first matching member. Keep this
/// representation private so every parser, renderer and edit shares that rule.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    Number(Number),
    /// Exact decimal digits when a `Number` would round them or lose `-0`.
    IntegerText(String),
    /// Exact float literal or SQLite-formatted REAL text (`1.50`, `1E2`,
    /// `9e999`, `1.0e+300`, `0.33333333333333332`). Carried as its own variant
    /// so genuine strings never need sentinel escaping (bd-oahj7).
    FloatText(InlineStr),
    HexInteger(HexInteger),
    String(InlineStr),
    Array(Vec<Self>),
    Object(Object),
}

/// SQLite's INT5 uses the unsigned hexadecimal magnitude to choose the SQL
/// storage class, even for negative values. Keep the original token so a
/// JSONB round trip retains that distinction at the signed integer boundary.
#[derive(Debug, Clone, PartialEq)]
struct HexInteger {
    token: String,
    magnitude: Option<u64>,
}

impl HexInteger {
    fn parse_value(token: &str) -> Option<Value> {
        let unsigned = token.strip_prefix('-').unwrap_or(token);
        let digits = unsigned
            .strip_prefix("0x")
            .or_else(|| unsigned.strip_prefix("0X"))?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let magnitude = u64::from_str_radix(digits, 16).ok();
        if let Some(n) = magnitude {
            if !token.starts_with('-') {
                return Some(Value::Number(n.into()));
            }
            if n == 0 {
                return Some(Value::IntegerText("-0".to_owned()));
            }
            if let Ok(int) = i64::try_from(n) {
                return Some(Value::Number((-int).into()));
            }
        }
        Some(Value::HexInteger(Self {
            token: token.to_owned(),
            magnitude,
        }))
    }

    fn write_text(&self, out: &mut String) {
        if self.token.starts_with('-') {
            out.push('-');
        }
        if let Some(magnitude) = self.magnitude {
            let _ = std::fmt::Write::write_fmt(out, format_args!("{magnitude}"));
        } else {
            out.push_str("9.0e999");
        }
    }

    fn sql_value(&self) -> Result<SqliteValue> {
        let magnitude = self.magnitude.ok_or_else(malformed_json)?;
        let negative = self.token.starts_with('-');
        Ok(if let Ok(int) = i64::try_from(magnitude) {
            SqliteValue::Integer(if negative { -int } else { int })
        } else {
            let real = magnitude as f64;
            SqliteValue::Float(if negative { -real } else { real })
        })
    }
}

impl Value {
    fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Object(object) => object.get(key),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&Vec<Self>> {
        match self {
            Self::Array(array) => Some(array),
            _ => None,
        }
    }

    fn as_array_mut(&mut self) -> Option<&mut Vec<Self>> {
        match self {
            Self::Array(array) => Some(array),
            _ => None,
        }
    }

    fn as_object_mut(&mut self) -> Option<&mut Object> {
        match self {
            Self::Object(object) => Some(object),
            _ => None,
        }
    }

    const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// Boxed first-member key index for large JSON objects (`>= Object::INDEX_THRESHOLD`),
/// keeping `Object` at 32 bytes (`Value` at 40 bytes) so small/medium objects pay
/// zero hashing or inline size overhead.
#[derive(Debug, Clone)]
struct ObjectKeyIndex(HashMap<InlineStr, usize, FxBuildHasher>);

/// Members retain their source order, including duplicate labels. Large
/// objects (`>= INDEX_THRESHOLD` members) also keep a boxed non-SipHash
/// first-member index so `Object` stays 32 bytes (`Value` stays 40 bytes) and
/// typical small/medium objects pay zero hashing or key-copy overhead.
#[derive(Debug, Clone, Default)]
struct Object {
    members: Vec<(InlineStr, Value)>,
    first_by_key: Option<Box<ObjectKeyIndex>>,
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        self.members == other.members
    }
}

impl Object {
    const INDEX_THRESHOLD: usize = 64;

    const fn new() -> Self {
        Self {
            members: Vec::new(),
            first_by_key: None,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            members: Vec::with_capacity(capacity),
            first_by_key: None,
        }
    }

    fn len(&self) -> usize {
        self.members.len()
    }

    fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn iter(&self) -> std::slice::Iter<'_, (InlineStr, Value)> {
        self.members.iter()
    }

    fn first_index(&self, key: &str) -> Option<usize> {
        match &self.first_by_key {
            Some(index) => index.0.get(key).copied(),
            None => {
                let target = key.as_bytes();
                self.members
                    .iter()
                    .position(|(label, _)| label.as_bytes() == target)
            }
        }
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.first_index(key).map(|index| &self.members[index].1)
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.first_index(key)
            .map(|index| &mut self.members[index].1)
    }

    fn contains_key(&self, key: &str) -> bool {
        self.first_index(key).is_some()
    }

    /// Parsing always appends: even an existing label names another member.
    fn append(&mut self, key: impl Into<InlineStr>, value: Value) {
        let key = key.into();
        if let Some(index) = &mut self.first_by_key {
            index.0.entry(key.clone()).or_insert(self.members.len());
        }
        self.members.push((key, value));
        if self.first_by_key.is_none() && self.members.len() >= Self::INDEX_THRESHOLD {
            self.rebuild_index();
        }
    }

    /// Edits replace the first matching member without disturbing duplicates.
    fn insert(&mut self, key: impl Into<InlineStr>, value: Value) {
        let key = key.into();
        if let Some(prior) = self.get_mut(key.as_str()) {
            *prior = value;
        } else {
            self.append(key, value);
        }
    }

    /// Removing the first member exposes the next duplicate to path lookup.
    fn shift_remove(&mut self, key: &str) {
        if let Some(index) = self.first_index(key) {
            self.members.remove(index);
            if self.first_by_key.is_some() {
                self.rebuild_index();
            }
        }
    }

    fn rebuild_index(&mut self) {
        if self.members.len() < Self::INDEX_THRESHOLD {
            self.first_by_key = None;
            return;
        }
        let mut index = HashMap::with_capacity_and_hasher(self.members.len(), FxBuildHasher);
        for (position, (key, _)) in self.members.iter().enumerate() {
            index.entry(key.clone()).or_insert(position);
        }
        self.first_by_key = Some(Box::new(ObjectKeyIndex(index)));
    }
}

impl IntoIterator for Object {
    type Item = (InlineStr, Value);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.members.into_iter()
    }
}

impl<'a> IntoIterator for &'a Object {
    type Item = &'a (InlineStr, Value);
    type IntoIter = std::slice::Iter<'a, (InlineStr, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        self.members.iter()
    }
}

const JSON_VALID_DEFAULT_FLAGS: u8 = 0x01;
const JSON_VALID_RFC_8259_FLAG: u8 = 0x01;
const JSON_VALID_JSON5_FLAG: u8 = 0x02;
const JSON_VALID_JSONB_SUPERFICIAL_FLAG: u8 = 0x04;
const JSON_VALID_JSONB_STRICT_FLAG: u8 = 0x08;
const JSON_PRETTY_DEFAULT_INDENT_WIDTH: usize = 4;
/// Declared columns for `json_each` / `json_tree`. The trailing `json` and
/// `root` input columns are HIDDEN and must not appear in a star projection.
pub const JSON_TABLE_COLUMN_NAMES: [&str; 10] = [
    "key", "value", "type", "atom", "id", "parent", "fullkey", "path", "json", "root",
];

const JSONB_NULL_TYPE: u8 = 0x0;
const JSONB_TRUE_TYPE: u8 = 0x1;
const JSONB_FALSE_TYPE: u8 = 0x2;
const JSONB_INT_TYPE: u8 = 0x3;
const JSONB_INT5_TYPE: u8 = 0x4;
const JSONB_FLOAT_TYPE: u8 = 0x5;
const JSONB_FLOAT5_TYPE: u8 = 0x6;
const JSONB_TEXT_TYPE: u8 = 0x7;
const JSONB_TEXT_JSON_TYPE: u8 = 0x8;
const JSONB_TEXT5_TYPE: u8 = 0x9;
const JSONB_TEXTRAW_TYPE: u8 = 0xA;
const JSONB_ARRAY_TYPE: u8 = 0xB;
const JSONB_OBJECT_TYPE: u8 = 0xC;

/// Historical sentinel prefix kept for unit tests verifying that user strings
/// containing arbitrary control bytes and NULs remain plain JSON strings.
#[cfg(test)]
const NUMBER_TEXT_TAG: &str = "\u{0}\u{0}fsqlite\u{1}nonfinite\u{0}";

/// Build a `Value::FloatText` carrying exact numeric text (`9e999`, `9.0e+999`,
/// `1.0e+300`, `0.33333333333333332`, `1.50`, `1E2`, ...).
#[inline]
fn number_text_value(raw: &str) -> Value {
    Value::FloatText(InlineStr::new(raw))
}

/// Construct a genuine JSON string value.
#[inline]
fn json_string_value(text: String) -> Value {
    Value::String(InlineStr::from_string(text))
}

/// Decimal integer tokens that cannot round-trip through `serde_json::Number`'s
/// `i64`/`u64` representation (including `-0`).
fn integer_needs_text(raw: &str) -> bool {
    raw == "-0"
        || (raw.len() >= 20
            && raw
                .strip_prefix('-')
                .unwrap_or(raw)
                .bytes()
                .all(|b| b.is_ascii_digit())
            && is_strict_json_number(raw)
            && raw.parse::<i64>().is_err()
            && raw.parse::<u64>().is_err())
}

/// Whether `text` is a fixed-point decimal that is already the shortest
/// round-trip form of its value, which is what Rust's `{:?}` prints for it.
#[cfg(test)]
fn is_shortest_fixed_decimal(text: &[u8]) -> bool {
    let digits = text.strip_prefix(b"-").unwrap_or(text);
    let Some(dot) = digits.iter().position(|&byte| byte == b'.') else {
        return false;
    };
    let (int, frac) = (&digits[..dot], &digits[dot + 1..]);
    if int.is_empty()
        || frac.is_empty()
        || int.len() > 16
        || (int.len() > 1 && int[0] == b'0')
        || !int.iter().all(u8::is_ascii_digit)
        || !frac.iter().all(u8::is_ascii_digit)
        || (frac.len() > 1 && frac.last() == Some(&b'0'))
    {
        return false;
    }
    let significant = if int == b"0" {
        let leading_zeros = frac.iter().take_while(|&&byte| byte == b'0').count();
        if leading_zeros == frac.len() {
            return true;
        }
        if leading_zeros > 3 {
            return false;
        }
        frac.len() - leading_zeros
    } else if frac == b"0" {
        int.len()
    } else {
        int.len() + frac.len()
    };
    significant <= 15
}

/// RFC 8259 `number`: `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
fn is_strict_json_number(raw: &str) -> bool {
    let bytes = raw.strip_prefix('-').unwrap_or(raw).as_bytes();
    let int_len = bytes.iter().take_while(|b| b.is_ascii_digit()).count();
    if int_len == 0 || (int_len > 1 && bytes[0] == b'0') {
        return false;
    }
    let mut rest = &bytes[int_len..];
    if let Some(fraction) = rest.strip_prefix(b".") {
        let digits = fraction.iter().take_while(|b| b.is_ascii_digit()).count();
        if digits == 0 {
            return false;
        }
        rest = &fraction[digits..];
    }
    if let Some(exponent) = rest.strip_prefix(b"e").or_else(|| rest.strip_prefix(b"E")) {
        let exponent = exponent
            .strip_prefix(b"+")
            .or_else(|| exponent.strip_prefix(b"-"))
            .unwrap_or(exponent);
        return !exponent.is_empty() && exponent.iter().all(u8::is_ascii_digit);
    }
    rest.is_empty()
}

/// Whether `input` can hold a float token: a `.`, or an exponent marker right
/// after a digit.
#[cfg(test)]
fn may_hold_float_token(input: &str) -> bool {
    let bytes = input.as_bytes();
    memchr::memchr(b'.', bytes).is_some()
        || memchr::memchr2_iter(b'e', b'E', bytes)
            .any(|at| at > 0 && bytes[at - 1].is_ascii_digit())
}

/// A wide integer has at least 19 consecutive digits (negative i64 overflow
/// starts at 19). Used by the JSON5 fallback to know when to recover wide
/// integer spellings from the input.
fn may_hold_integer_text(input: &str) -> bool {
    if input.contains("-0") {
        return true;
    }
    let mut bytes = input.as_bytes();
    while let Some(window) = bytes.get(..19) {
        let Some(last_non_digit) = window.iter().rposition(|b| !b.is_ascii_digit()) else {
            return true;
        };
        bytes = &bytes[last_non_digit + 1..];
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSegment {
    Key(SmallText),
    Index(usize),
    Append,
    FromEnd(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditMode {
    Set,
    Insert,
    Replace,
}

/// Parse and minify JSON text.
///
/// Returns a canonical minified JSON string or a `FunctionError` if invalid.
pub fn json(input: &str) -> Result<String> {
    // Strict RFC-8259 JSON: validate with zero allocations, then lexically
    // minify so every source token (number literals, string escapes, duplicate
    // keys) is preserved verbatim (bd-6b0pe / bd-p2xrc / bd-oahj7).
    if validate_json_text(input) {
        return Ok(minify_json_text(input));
    }
    // JSON5 fallback (bd-qear2): stock SQLite 3.42+ accepts JSON5 — unquoted
    // object keys, single-quoted strings, trailing commas, // and /* */
    // comments, hex integers, leading/trailing decimal points, Infinity/NaN —
    // and canonicalizes it to standard JSON. Parse ONLY to validate, then
    // translate LEXICALLY (bd-4gqeu). On total failure surface the strict
    // error ("malformed JSON").
    if parse_json5_text(input).is_err() {
        return Err(malformed_json());
    }
    Ok(translate_json5_to_json(input))
}

/// Validate JSON text under flags compatible with SQLite `json_valid`.
///
/// Supported flags:
/// - `0x01`: strict RFC-8259 JSON text
/// - `0x02`: JSON5 text
/// - `0x04`: superficial JSONB check
/// - `0x08`: strict JSONB parse
#[must_use]
pub fn json_valid(input: &str, flags: Option<u8>) -> i64 {
    json_valid_blob(input.as_bytes(), flags)
}

/// Validate binary JSONB payloads and/or JSON text (when UTF-8).
#[must_use]
pub fn json_valid_blob(input: &[u8], flags: Option<u8>) -> i64 {
    let effective_flags = flags.unwrap_or(JSON_VALID_DEFAULT_FLAGS);
    if effective_flags == 0 {
        return 0;
    }

    let allow_json = effective_flags & JSON_VALID_RFC_8259_FLAG != 0;
    let allow_json5 = effective_flags & JSON_VALID_JSON5_FLAG != 0;
    let allow_jsonb_superficial = effective_flags & JSON_VALID_JSONB_SUPERFICIAL_FLAG != 0;
    let allow_jsonb_strict = effective_flags & JSON_VALID_JSONB_STRICT_FLAG != 0;

    if (allow_json || allow_json5)
        && let Ok(text) = std::str::from_utf8(input)
    {
        if validate_json_text(text) {
            return 1;
        }
        if allow_json5 && parse_json5_text(text).is_ok() {
            return 1;
        }
    }

    if allow_jsonb_strict && decode_jsonb_root(input).is_ok() {
        return 1;
    }
    if allow_jsonb_superficial && is_superficially_valid_jsonb(input) {
        return 1;
    }

    0
}

/// Convert JSON or JSON5 text into stock-compatible JSONB bytes.
pub fn jsonb(input: &str) -> Result<Vec<u8>> {
    translate_json_text_to_jsonb(input)
}

/// Convert JSONB bytes back into minified JSON text.
pub fn json_from_jsonb(input: &[u8]) -> Result<String> {
    let value = decode_jsonb_root(input)?;
    Ok(encode_json_text(&value))
}

fn encode_json_text(value: &Value) -> String {
    let mut out = String::with_capacity(64);
    write_canonical_json_text(value, &mut out);
    out
}

/// Minify already-validated JSON *text* the way stock `json()` does: strip only
/// insignificant (inter-token) whitespace and preserve every token verbatim.
///
/// Stock SQLite's `json()` never rewrites the parsed source — number literals
/// keep their exact form (`1e2`, `1E2`, `1e+2`, `1e02` are all distinct;
/// `1.50` keeps its trailing zero), string escapes are preserved (`"a\/b"`
/// stays `\/`, not `/`), and duplicate object keys are kept. Round-tripping
/// through `serde_json::Value` loses all of that (exponents are normalised to
/// Lexically translate KNOWN-VALID JSON5 text to stock-canonical JSON text
/// (bd-4gqeu). Token-level like stock's `jsonTranslateTextToBlob`, so it
/// preserves what a value-tree round-trip destroys: duplicate object keys,
/// key order, exponent literals (`1e2`, `1E+3`), and string-escape spellings.
///
/// Oracle-pinned transformations (sqlite3 3.46.1):
/// - comments, trailing commas, and JSON5 whitespace stripped; structure
///   minified
/// - single-quoted strings/keys -> double-quoted; a bare `"` inside becomes
///   `\"`; `\'` becomes a bare `'`
/// - bare identifier keys quoted; `Infinity`/`+Infinity` -> `9e999`,
///   `-Infinity` -> `-9e999`, `NaN` (any sign) -> `null`
/// - `\xHH` -> `\u00HH` (hex digits verbatim), `\0` -> the `0000` escape
///   spelling, and `\v` -> the `0009` escape spelling — reproducing stock's
///   quirk (the VALUE is U+000B but its text translator spells `0009`);
///   JSON-legal escapes
///   (`\" \\ \/ \b \f \n \r \t \uXXXX`) pass through verbatim; escaped line
///   continuations are removed
/// - hex integers -> decimal (`0xFFFFFFFFFFFFFFFF` -> full u64 magnitude);
///   a leading `+` is dropped; `.5`/`-.5` gain a leading `0`; `5.`/`1.e3`
///   gain a `0` after the dot; all other number tokens pass verbatim
///
/// The caller MUST have validated `input` as well-formed JSON5 first; this
/// function never rejects, it only transforms.
#[allow(clippy::too_many_lines)]
fn translate_json5_to_json(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    // A structural comma is held back until the next value/key token proves it
    // was not a trailing comma.
    let mut pending_comma = false;
    let flush_comma = |out: &mut String, pending: &mut bool| {
        if *pending {
            out.push(',');
            *pending = false;
        }
    };

    fn is_json5_ws(ch: char) -> bool {
        ch.is_whitespace() || ch == '\u{FEFF}'
    }

    while let Some(ch) = chars.next() {
        match ch {
            c if is_json5_ws(c) => {}
            '/' => match chars.peek() {
                Some('/') => {
                    for c in chars.by_ref() {
                        if c == '\n' {
                            break;
                        }
                    }
                }
                Some('*') => {
                    chars.next();
                    let mut prev = '\0';
                    for c in chars.by_ref() {
                        if prev == '*' && c == '/' {
                            break;
                        }
                        prev = c;
                    }
                }
                // Unreachable in valid JSON5; pass through.
                _ => out.push('/'),
            },
            ',' => pending_comma = true,
            '}' | ']' => {
                // A comma directly before a closer was trailing: drop it.
                pending_comma = false;
                out.push(ch);
            }
            '{' | '[' => {
                flush_comma(&mut out, &mut pending_comma);
                out.push(ch);
            }
            ':' => out.push(':'),
            '"' | '\'' => {
                flush_comma(&mut out, &mut pending_comma);
                translate_json5_string(ch, &mut chars, &mut out);
            }
            c if c.is_alphabetic() || c == '_' || c == '$' || c == '\\' => {
                flush_comma(&mut out, &mut pending_comma);
                let mut ident = String::new();
                ident.push(c);
                while let Some(&n) = chars.peek() {
                    if n.is_alphanumeric() || n == '_' || n == '$' || n == '\\' {
                        ident.push(n);
                        chars.next();
                    } else {
                        break;
                    }
                }
                match ident.as_str() {
                    "true" | "false" | "null" => out.push_str(&ident),
                    "Infinity" => out.push_str("9e999"),
                    "NaN" => out.push_str("null"),
                    // Any other bare identifier in valid JSON5 is an object key.
                    _ => {
                        out.push('"');
                        out.push_str(&ident);
                        out.push('"');
                    }
                }
            }
            '+' | '-' | '.' | '0'..='9' => {
                flush_comma(&mut out, &mut pending_comma);
                let mut sign_negative = false;
                let mut first = ch;
                if first == '+' || first == '-' {
                    sign_negative = first == '-';
                    match chars.next() {
                        Some(next) => first = next,
                        None => break,
                    }
                }
                if first == 'I' || first == 'N' {
                    // Signed Infinity / NaN: consume the identifier tail.
                    while let Some(&n) = chars.peek() {
                        if n.is_alphabetic() {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if first == 'I' {
                        if sign_negative {
                            out.push('-');
                        }
                        out.push_str("9e999");
                    } else {
                        out.push_str("null");
                    }
                    continue;
                }
                let mut token = String::new();
                token.push(first);
                while let Some(&n) = chars.peek() {
                    // In valid JSON5 a number token is delimited by
                    // whitespace, a comment, `,`, `]`, or `}` — none of which
                    // are number chars — so a greedy read is safe.
                    if n.is_ascii_hexdigit() || matches!(n, 'x' | 'X' | '.' | '+' | '-') {
                        token.push(n);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if sign_negative {
                    out.push('-');
                }
                out.push_str(&translate_json5_number(&token));
            }
            // Unreachable in valid JSON5; pass through.
            other => out.push(other),
        }
    }
    out
}

/// Translate the body of one JSON5 string (opened with `delim`, which has
/// already been consumed) into a canonical double-quoted JSON string.
fn translate_json5_string(
    delim: char,
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    out: &mut String,
) {
    out.push('"');
    while let Some(ch) = chars.next() {
        if ch == delim {
            break;
        }
        match ch {
            '\\' => match chars.next() {
                Some('\'') => out.push('\''),
                Some('v') => out.push_str("\\u0009"),
                Some('0') => out.push_str("\\u0000"),
                Some('x') => {
                    out.push_str("\\u00");
                    for _ in 0..2 {
                        if let Some(h) = chars.next() {
                            out.push(h);
                        }
                    }
                }
                Some('u') => {
                    out.push_str("\\u");
                    for _ in 0..4 {
                        if let Some(h) = chars.next() {
                            out.push(h);
                        }
                    }
                }
                // Escaped line continuations disappear ('\r' swallows a
                // following '\n' so CRLF drops as one).
                Some('\r') => {
                    if chars.peek() == Some(&'\n') {
                        chars.next();
                    }
                }
                Some('\n' | '\u{2028}' | '\u{2029}') => {}
                Some(c @ ('"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't')) => {
                    out.push('\\');
                    out.push(c);
                }
                // Unreachable in stock-valid JSON5; keep the char.
                Some(other) => out.push(other),
                None => break,
            },
            // A bare `"` inside a single-quoted string must be escaped.
            '"' => out.push_str("\\\""),
            other => out.push(other),
        }
    }
    out.push('"');
}

/// Canonicalize one unsigned JSON5 number token (sign already emitted).
fn translate_json5_number(token: &str) -> String {
    if let Some(hex) = token
        .strip_prefix("0x")
        .or_else(|| token.strip_prefix("0X"))
    {
        return u64::from_str_radix(hex, 16)
            .map_or_else(|_| "9.0e999".to_owned(), |value| value.to_string());
    }
    let mut out = String::with_capacity(token.len() + 2);
    let mut chars = token.chars().peekable();
    if chars.peek() == Some(&'.') {
        out.push('0');
    }
    while let Some(ch) = chars.next() {
        out.push(ch);
        if ch == '.' && !chars.peek().is_some_and(char::is_ascii_digit) {
            out.push('0');
        }
    }
    out
}

/// `e+NN`, `\/` is unescaped, duplicate keys are dropped), so the text-input
/// path lexically minifies the source instead of re-serialising a parsed tree.
/// The caller MUST have validated `input` as well-formed JSON first.
fn minify_json_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    for ch in input.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else if matches!(ch, ' ' | '\t' | '\n' | '\r') {
            // Insignificant whitespace between tokens — drop it.
        } else {
            out.push(ch);
            if ch == '"' {
                in_string = true;
            }
        }
    }
    out
}

/// Append `text` as a double-quoted RFC-8259 JSON string literal directly into
/// `out` without allocating an intermediate `String`.
fn write_json_escaped_string(text: &str, out: &mut String) {
    out.push('"');
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'"' || b == b'\\' || b < 0x20 {
            if start < i {
                out.push_str(&text[start..i]);
            }
            match b {
                b'"' => out.push_str("\\\""),
                b'\\' => out.push_str("\\\\"),
                0x08 => out.push_str("\\b"),
                0x0C => out.push_str("\\f"),
                b'\n' => out.push_str("\\n"),
                b'\r' => out.push_str("\\r"),
                b'\t' => out.push_str("\\t"),
                control => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    let buf = [
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        HEX[usize::from(control >> 4)],
                        HEX[usize::from(control & 0x0f)],
                    ];
                    out.push_str(
                        std::str::from_utf8(&buf).expect("ASCII hex escape is valid UTF-8"),
                    );
                }
            }
            i += 1;
            start = i;
        } else {
            i += 1;
        }
    }
    if start < bytes.len() {
        out.push_str(&text[start..]);
    }
    out.push('"');
}

/// Minified JSON writer with stock-SQLite-canonical float text: `{:?}` is
/// Rust's shortest round-trip form ("1e300", "-0.0", "0.1"), whereas
/// `serde_json::to_string` renders exponents with a non-canonical '+'
/// ("1e+300") that diverges from stock `json()` output (bd-t75hg).
fn write_canonical_json_text(value: &Value, out: &mut String) {
    use std::fmt::Write as _;

    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::IntegerText(digits) => out.push_str(digits),
        Value::FloatText(raw) => out.push_str(raw),
        Value::HexInteger(integer) => integer.write_text(out),
        Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                let _ = write!(out, "{int}");
            } else if let Some(uint) = number.as_u64() {
                let _ = write!(out, "{uint}");
            } else if let Some(float) = number.as_f64().filter(|value| value.is_finite()) {
                let _ = write!(out, "{float:?}");
            } else {
                let _ = write!(out, "{number}");
            }
        }
        Value::String(text) => {
            write_json_escaped_string(text, out);
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json_text(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, (key, item)) in map.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_json_escaped_string(key, out);
                out.push(':');
                write_canonical_json_text(item, out);
            }
            out.push('}');
        }
    }
}

fn parse_json_input_blob(input: &[u8]) -> Result<Value> {
    match decode_jsonb_root(input) {
        Ok(value) => Ok(value),
        Err(_) => match std::str::from_utf8(input) {
            Ok(text) => parse_json_value_lenient(text),
            Err(_) => Err(malformed_json()),
        },
    }
}

fn json_type_value(root: &Value, path: Option<&str>) -> Result<Option<&'static str>> {
    let target = match path {
        Some(path_expr) => resolve_path(root, path_expr)?,
        None => Some(root),
    };
    Ok(target.map(json_type_name))
}

fn json_extract_value(root: &Value, paths: &[&str]) -> Result<SqliteValue> {
    if paths.is_empty() {
        return Err(FrankenError::function_error(
            "json_extract requires at least one path",
        ));
    }

    if paths.len() == 1 {
        return json_extract_single_path(root, paths[0]);
    }

    let mut out = Vec::with_capacity(paths.len());
    for path_expr in paths {
        let selected = resolve_path(root, path_expr)?;
        out.push(selected.cloned().unwrap_or(Value::Null));
    }

    let encoded = encode_json_text(&Value::Array(out));
    Ok(SqliteValue::Text(SmallText::from_string(encoded)))
}

fn jsonb_extract_value(root: &Value, paths: &[&str]) -> Result<Vec<u8>> {
    if paths.is_empty() {
        return Err(FrankenError::function_error(
            "jsonb_extract requires at least one path",
        ));
    }

    let output = if paths.len() == 1 {
        jsonb_extract_single_path_value(root, paths[0])?
    } else {
        let mut values = Vec::with_capacity(paths.len());
        for path_expr in paths {
            values.push(
                resolve_path(root, path_expr)?
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        Value::Array(values)
    };

    encode_jsonb_root(&output)
}

fn json_arrow_value(root: &Value, path: &str) -> Result<SqliteValue> {
    let selected = resolve_path(root, path)?;
    let Some(value) = selected else {
        return Ok(SqliteValue::Null);
    };
    let encoded = encode_json_text(value);
    Ok(SqliteValue::Text(SmallText::from_string(encoded)))
}

fn json_extract_single_path(root: &Value, path: &str) -> Result<SqliteValue> {
    let selected = resolve_path(root, path)?;
    selected.map_or(Ok(SqliteValue::Null), json_to_sqlite_scalar)
}

fn jsonb_extract_single_path_value(root: &Value, path: &str) -> Result<Value> {
    Ok(resolve_path(root, path)?.cloned().unwrap_or(Value::Null))
}

fn json_array_length_value(root: &Value, path: Option<&str>) -> Result<Option<usize>> {
    let target = match path {
        Some(path_expr) => resolve_path(root, path_expr)?,
        None => Some(root),
    };
    Ok(match target {
        Some(Value::Array(array)) => Some(array.len()),
        Some(_) => Some(0),
        None => None,
    })
}

fn json_error_position_blob(input: &[u8]) -> usize {
    if decode_jsonb_root(input).is_ok() {
        return 0;
    }
    match std::str::from_utf8(input) {
        Ok(text) => json_error_position(text),
        Err(_) => 1,
    }
}

fn json_pretty_value(root: &Value, indent: Option<&str>) -> String {
    let indent_unit = match indent {
        Some(indent) => indent.to_owned(),
        None => " ".repeat(JSON_PRETTY_DEFAULT_INDENT_WIDTH),
    };
    let mut out = String::new();
    write_pretty_value(root, &indent_unit, 0, &mut out);
    out
}

fn edit_json_paths_value(
    mut edited: Value,
    pairs: &[(&str, SqliteValue)],
    mode: EditMode,
) -> Result<Value> {
    for (path, value) in pairs {
        let segments = parse_path(path)?;
        let replacement = sqlite_to_json(value)?;
        apply_edit(&mut edited, &segments, replacement, mode);
    }
    Ok(edited)
}

/// Like [`edit_json_paths_value`], but honors each value argument's subtype: a
/// value carrying `JSON_SUBTYPE` (e.g. produced by `json(...)`) is embedded as a
/// JSON subtree instead of being stringified. `arg_subtypes` is indexed parallel
/// to `args`; the value for the (path, value) pair beginning at `idx` is
/// `args[idx + 1]` with subtype `arg_subtypes[idx + 1]`. (GH #233)
fn edit_json_paths_value_with_subtypes(
    name: &str,
    mut edited: Value,
    args: &[SqliteValue],
    arg_subtypes: &[u32],
    start: usize,
    mode: EditMode,
) -> Result<Value> {
    let mut idx = start;
    while idx + 1 < args.len() {
        // Stock skips a pair whose PATH is NULL.
        if args[idx].is_null() {
            idx += 2;
            continue;
        }
        let segments = parse_path(path_arg(name, args, idx)?)?;
        let subtype = arg_subtypes.get(idx + 1).copied().unwrap_or(0);
        let replacement = sqlite_to_json_with_subtype(&args[idx + 1], subtype)?;
        apply_edit(&mut edited, &segments, replacement, mode);
        idx += 2;
    }
    Ok(edited)
}

fn json_remove_value(mut edited: Value, paths: &[&str]) -> Result<Option<Value>> {
    for path in paths {
        let segments = parse_path(path)?;
        if segments.is_empty() {
            return Ok(None);
        }
        remove_at_path(&mut edited, &segments);
    }
    Ok(Some(edited))
}

fn json_patch_value(root: Value, patch: Value) -> Value {
    merge_patch(root, patch)
}

/// Return JSON type name at the root or an optional path.
///
/// Returns `None` when the path does not resolve.
pub fn json_type(input: &str, path: Option<&str>) -> Result<Option<&'static str>> {
    if let FastLookup::Resolved(result) = try_strict_json_type(input, path)? {
        return Ok(result);
    }
    let root = parse_json_value_lenient(input)?;
    json_type_value(&root, path)
}

/// Extract JSON value(s) by path, following SQLite single vs multi-path behavior.
///
/// - One path: return SQL-native value (text unwrapped, number typed, JSON null -> SQL NULL)
/// - Multiple paths: return JSON array text of extracted values (missing paths become `null`)
/// Preserve the original JSON token spellings when extracting multiple paths.
///
/// SQLite constructs the result array from the selected source tokens. Parsing
/// those tokens into a value tree first changes observable JSON TEXT: for
/// example, a string containing `\\u0074` must keep that escape and a number
/// written as `1E+2` must not silently become `100.0`. Reuse the strict
/// source-span scanner, translating valid JSON5 lexically before scanning.
///
/// A bounded fast path avoids repeatedly scanning huge inputs for many paths.
/// Unsupported path forms retain the full resolver and its error semantics.
fn try_lexical_json_extract_multi(input: &str, paths: &[&str]) -> Result<Option<String>> {
    const MAX_LEXICAL_PATHS: usize = 8;
    const MAX_LEXICAL_INPUT_BYTES: usize = 65_536;
    if paths.len() > MAX_LEXICAL_PATHS || input.len() > MAX_LEXICAL_INPUT_BYTES {
        return Ok(None);
    }
    // Check the path shapes before doing any JSON work. The full resolver
    // handles quoted/escaped keys and all other extended JSON path forms.
    let Some(fast_paths) = paths
        .iter()
        .map(|path| parse_fast_path(path))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let normalized;
    let source = if validate_json_text(input) {
        input
    } else {
        // Validation is mandatory before the lexical JSON5 translator, which
        // deliberately assumes syntactically valid input.
        parse_json5_text(input).map_err(|_| malformed_json())?;
        normalized = translate_json5_to_json(input);
        normalized.as_str()
    };
    let mut output = String::with_capacity(source.len().min(4096));
    output.push('[');
    for (index, path) in fast_paths.iter().enumerate() {
        let span = extract_strict_json_span(source, path.as_slice());
        let selected = match span {
            StrictScanSpan::Missing => "null".to_owned(),
            StrictScanSpan::Found(span) => {
                minify_json_text(&source[span.start..span.end])
            }
            // Some valid JSON5 identifiers cannot be translated into a
            // strict token stream; let the existing value resolver handle it.
            StrictScanSpan::NotStrictJson => return Ok(None),
        };
        if index != 0 {
            output.push(',');
        }
        output.push_str(&selected);
    }
    output.push(']');
    Ok(Some(output))
}

pub fn json_extract(input: &str, paths: &[&str]) -> Result<SqliteValue> {
    if paths.len() > 1
        && let Some(encoded) = try_lexical_json_extract_multi(input, paths)?
    {
        return Ok(SqliteValue::Text(SmallText::from_string(encoded)));
    }
    if paths.len() == 1
        && let Some((value, _)) = try_strict_json_extract_single(input, paths[0])?
    {
        return Ok(value);
    }
    let root = parse_json_value_lenient(input)?;
    json_extract_value(&root, paths)
}

/// JSONB variant of `json_extract`.
///
/// The extracted JSON subtree is always returned as JSONB bytes.
pub fn jsonb_extract(input: &str, paths: &[&str]) -> Result<Vec<u8>> {
    let root = parse_json_value_lenient(input)?;
    jsonb_extract_value(&root, paths)
}

/// Extract with `->` semantics: always returns JSON text for the selected node.
///
/// Missing paths yield SQL NULL.
pub fn json_arrow(input: &str, path: &str) -> Result<SqliteValue> {
    if let Some(value) = try_strict_json_arrow(input, path)? {
        return Ok(value);
    }
    let root = parse_json_value_lenient(input)?;
    json_arrow_value(&root, path)
}

/// Extract with `->>` semantics: returns SQL-native value.
pub fn json_double_arrow(input: &str, path: &str) -> Result<SqliteValue> {
    json_extract(input, &[path])
}

/// Return the array length at root or path.
///
/// Matches SQLite JSON1 semantics: a missing path returns SQL NULL, while an
/// existing non-array target returns 0.
pub fn json_array_length(input: &str, path: Option<&str>) -> Result<Option<usize>> {
    if let FastLookup::Resolved(result) = try_strict_json_array_length(input, path)? {
        return Ok(result);
    }
    let root = parse_json_value_lenient(input)?;
    json_array_length_value(&root, path)
}

/// Maximum JSON nesting depth matching SQLite's `JSON_MAX_DEPTH` (`sqlite3.c`).
const STOCK_JSON_MAX_DEPTH: usize = 1000;

/// Return 0 for valid JSON or JSON5, otherwise a 1-based UTF-8 character
/// position matching stock SQLite's `jsonErrorFunc` / `jsonConvertTextToBlob`
/// / `jsonTranslateTextToBlob` (`sqlite3.c`).
#[must_use]
pub fn json_error_position(input: &str) -> usize {
    let bytes = input.as_bytes();
    let mut scanner = StockJsonErrorScanner {
        bytes,
        i_err: 0,
        depth: 0,
        last_was_text: false,
    };
    let end = scanner.scan_value(0);
    if end > 0 {
        let mut tail = usize::try_from(end).unwrap_or(bytes.len());
        while stock_json_is_space(stock_byte_at(bytes, tail)) {
            tail += 1;
        }
        if stock_byte_at(bytes, tail) != 0 {
            tail += stock_json5_whitespace(bytes, tail);
            if stock_byte_at(bytes, tail) != 0 {
                // Match SQLite's `jsonConvertTextToBlob`: trailing garbage
                // returns an error without modifying `pParse->iErr`.
                return stock_byte_offset_to_char_pos(bytes, scanner.i_err);
            }
        }
        return 0;
    }
    stock_byte_offset_to_char_pos(bytes, scanner.i_err)
}

#[inline]
fn stock_byte_at(bytes: &[u8], idx: usize) -> u8 {
    bytes.get(idx).copied().unwrap_or(0)
}

#[inline]
const fn stock_json_is_space(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | b'\r' | b' ')
}

#[inline]
const fn stock_json_is_ok(b: u8) -> bool {
    b >= 0x20 && b != b'"' && b != b'\'' && b != b'\\'
}

#[inline]
const fn stock_json_id1(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'$' || b == b'_' || b >= 0x80
}

#[inline]
const fn stock_json_id2(b: u8) -> bool {
    stock_json_id1(b) || b.is_ascii_digit()
}

#[inline]
fn stock_is_2hex(bytes: &[u8], idx: usize) -> bool {
    stock_byte_at(bytes, idx).is_ascii_hexdigit()
        && stock_byte_at(bytes, idx + 1).is_ascii_hexdigit()
}

#[inline]
fn stock_is_4hex(bytes: &[u8], idx: usize) -> bool {
    stock_is_2hex(bytes, idx) && stock_is_2hex(bytes, idx + 2)
}

#[inline]
fn stock_is_4hex_u(bytes: &[u8], idx: usize) -> bool {
    stock_byte_at(bytes, idx) == b'u' && stock_is_4hex(bytes, idx + 1)
}

#[inline]
fn stock_starts_with(bytes: &[u8], idx: usize, needle: &[u8]) -> bool {
    bytes
        .get(idx..idx + needle.len())
        .is_some_and(|slice| slice == needle)
}

#[inline]
fn stock_starts_with_ci(bytes: &[u8], idx: usize, needle: &[u8]) -> bool {
    bytes
        .get(idx..idx + needle.len())
        .is_some_and(|slice| slice.eq_ignore_ascii_case(needle))
}

fn stock_byte_offset_to_char_pos(bytes: &[u8], i_err: usize) -> usize {
    let mut pos = 0usize;
    let limit = i_err.min(bytes.len());
    for &b in &bytes[..limit] {
        if b == 0 {
            break;
        }
        if (b & 0xc0) != 0x80 {
            pos += 1;
        }
    }
    pos + 1
}

/// Port of SQLite's `json5Whitespace()` (`sqlite3.c`).
fn stock_json5_whitespace(bytes: &[u8], start: usize) -> usize {
    let mut n = 0usize;
    loop {
        let pos = start + n;
        match stock_byte_at(bytes, pos) {
            0x09 | 0x0a | 0x0b | 0x0c | 0x0d | 0x20 => {
                n += 1;
            }
            b'/' => {
                let next = stock_byte_at(bytes, pos + 1);
                if next == b'*' && stock_byte_at(bytes, pos + 2) != 0 {
                    let mut j = n + 3;
                    loop {
                        let cur = stock_byte_at(bytes, start + j);
                        if cur == b'/' && stock_byte_at(bytes, start + j - 1) == b'*' {
                            n = j + 1;
                            break;
                        }
                        if cur == 0 {
                            return n;
                        }
                        j += 1;
                    }
                } else if next == b'/' {
                    let mut j = n + 2;
                    loop {
                        let c = stock_byte_at(bytes, start + j);
                        if c == 0 {
                            break;
                        }
                        if c == b'\n' || c == b'\r' {
                            break;
                        }
                        if c == 0xe2
                            && stock_byte_at(bytes, start + j + 1) == 0x80
                            && matches!(stock_byte_at(bytes, start + j + 2), 0xa8 | 0xa9)
                        {
                            j += 2;
                            break;
                        }
                        j += 1;
                    }
                    n = j;
                    if stock_byte_at(bytes, start + n) != 0 {
                        n += 1;
                    }
                } else {
                    return n;
                }
            }
            0xc2 if stock_byte_at(bytes, pos + 1) == 0xa0 => {
                n += 2;
            }
            0xe1 if stock_byte_at(bytes, pos + 1) == 0x9a
                && stock_byte_at(bytes, pos + 2) == 0x80 =>
            {
                n += 3;
            }
            0xe2 => {
                let b1 = stock_byte_at(bytes, pos + 1);
                let b2 = stock_byte_at(bytes, pos + 2);
                if (b1 == 0x80 && ((0x80..=0x8a).contains(&b2) || matches!(b2, 0xa8 | 0xa9 | 0xaf)))
                    || (b1 == 0x81 && b2 == 0x9f)
                {
                    n += 3;
                } else {
                    return n;
                }
            }
            0xe3 if stock_byte_at(bytes, pos + 1) == 0x80
                && stock_byte_at(bytes, pos + 2) == 0x80 =>
            {
                n += 3;
            }
            0xef if stock_byte_at(bytes, pos + 1) == 0xbb
                && stock_byte_at(bytes, pos + 2) == 0xbf =>
            {
                n += 3;
            }
            _ => return n,
        }
    }
}

/// Zero-allocation scanner mirroring SQLite's `jsonTranslateTextToBlob`
/// (`sqlite3.c`) solely for `json_error_position(X)` offset parity.
struct StockJsonErrorScanner<'a> {
    bytes: &'a [u8],
    i_err: usize,
    depth: usize,
    last_was_text: bool,
}

impl StockJsonErrorScanner<'_> {
    #[inline]
    fn byte(&self, idx: usize) -> u8 {
        stock_byte_at(self.bytes, idx)
    }

    fn scan_value(&mut self, mut i: usize) -> isize {
        loop {
            match self.byte(i) {
                b'{' => return self.scan_object(i),
                b'[' => return self.scan_array(i),
                b'\'' | b'"' => return self.scan_string(i),
                b't' => {
                    if stock_starts_with(self.bytes, i, b"true")
                        && !self.byte(i + 4).is_ascii_alphanumeric()
                    {
                        self.last_was_text = false;
                        return isize::try_from(i + 4).unwrap_or(isize::MAX);
                    }
                    self.i_err = i;
                    return -1;
                }
                b'f' => {
                    if stock_starts_with(self.bytes, i, b"false")
                        && !self.byte(i + 5).is_ascii_alphanumeric()
                    {
                        self.last_was_text = false;
                        return isize::try_from(i + 5).unwrap_or(isize::MAX);
                    }
                    self.i_err = i;
                    return -1;
                }
                b'+' | b'.' | b'-' | b'0'..=b'9' => return self.scan_number(i),
                b'}' => {
                    self.i_err = i;
                    return -2;
                }
                b']' => {
                    self.i_err = i;
                    return -3;
                }
                b',' => {
                    self.i_err = i;
                    return -4;
                }
                b':' => {
                    self.i_err = i;
                    return -5;
                }
                0 => return 0,
                0x09 | 0x0a | 0x0d | 0x20 => {
                    i += 1;
                    while stock_json_is_space(self.byte(i)) {
                        i += 1;
                    }
                }
                0x0b | 0x0c | b'/' | 0xc2 | 0xe1 | 0xe2 | 0xe3 | 0xef => {
                    let ws = stock_json5_whitespace(self.bytes, i);
                    if ws > 0 {
                        i += ws;
                    } else {
                        self.i_err = i;
                        return -1;
                    }
                }
                b'n' => {
                    if stock_starts_with(self.bytes, i, b"null")
                        && !self.byte(i + 4).is_ascii_alphanumeric()
                    {
                        self.last_was_text = false;
                        return isize::try_from(i + 4).unwrap_or(isize::MAX);
                    }
                    return self.scan_nan_inf(i);
                }
                _ => return self.scan_nan_inf(i),
            }
        }
    }

    fn scan_object(&mut self, i: usize) -> isize {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.i_err = i;
            self.depth -= 1;
            return -1;
        }
        let mut j = i + 1;
        loop {
            let mut x = self.scan_value(j);
            if x <= 0 {
                if x == -2 {
                    j = self.i_err;
                    break;
                }
                j += stock_json5_whitespace(self.bytes, j);
                let b = self.byte(j);
                if stock_json_id1(b) || (b == b'\\' && stock_is_4hex_u(self.bytes, j + 1)) {
                    let mut k = j + 1;
                    loop {
                        let bk = self.byte(k);
                        if (stock_json_id2(bk) && stock_json5_whitespace(self.bytes, k) == 0)
                            || (bk == b'\\' && stock_is_4hex_u(self.bytes, k + 1))
                        {
                            k += 1;
                        } else {
                            break;
                        }
                    }
                    self.last_was_text = true;
                    x = isize::try_from(k).unwrap_or(isize::MAX);
                } else {
                    if x != -1 {
                        self.i_err = j;
                    }
                    self.depth -= 1;
                    return -1;
                }
            }
            if !self.last_was_text {
                self.i_err = j;
                self.depth -= 1;
                return -1;
            }
            j = usize::try_from(x).unwrap_or(self.bytes.len());
            if self.byte(j) == b':' {
                j += 1;
            } else {
                let mut found_colon = false;
                if stock_json_is_space(self.byte(j)) {
                    j += 1;
                    while stock_json_is_space(self.byte(j)) {
                        j += 1;
                    }
                    if self.byte(j) == b':' {
                        j += 1;
                        found_colon = true;
                    }
                }
                if !found_colon {
                    let sep = self.scan_value(j);
                    if sep != -5 {
                        if sep != -1 {
                            self.i_err = j;
                        }
                        self.depth -= 1;
                        return -1;
                    }
                    j = self.i_err + 1;
                }
            }
            let val = self.scan_value(j);
            if val <= 0 {
                if val != -1 {
                    self.i_err = j;
                }
                self.depth -= 1;
                return -1;
            }
            j = usize::try_from(val).unwrap_or(self.bytes.len());
            if self.byte(j) == b',' {
                j += 1;
                continue;
            }
            if self.byte(j) == b'}' {
                break;
            }
            if stock_json_is_space(self.byte(j)) {
                j += 1;
                while stock_json_is_space(self.byte(j)) {
                    j += 1;
                }
                if self.byte(j) == b',' {
                    j += 1;
                    continue;
                }
                if self.byte(j) == b'}' {
                    break;
                }
            }
            let next = self.scan_value(j);
            if next == -4 {
                j = self.i_err + 1;
                continue;
            }
            if next == -2 {
                j = self.i_err;
                break;
            }
            self.i_err = j;
            self.depth -= 1;
            return -1;
        }
        self.depth -= 1;
        self.last_was_text = false;
        isize::try_from(j + 1).unwrap_or(isize::MAX)
    }

    fn scan_array(&mut self, i: usize) -> isize {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.i_err = i;
            self.depth -= 1;
            return -1;
        }
        let mut j = i + 1;
        loop {
            let x = self.scan_value(j);
            if x <= 0 {
                if x == -3 {
                    j = self.i_err;
                    break;
                }
                if x != -1 {
                    self.i_err = j;
                }
                self.depth -= 1;
                return -1;
            }
            j = usize::try_from(x).unwrap_or(self.bytes.len());
            if self.byte(j) == b',' {
                j += 1;
                continue;
            }
            if self.byte(j) == b']' {
                break;
            }
            if stock_json_is_space(self.byte(j)) {
                j += 1;
                while stock_json_is_space(self.byte(j)) {
                    j += 1;
                }
                if self.byte(j) == b',' {
                    j += 1;
                    continue;
                }
                if self.byte(j) == b']' {
                    break;
                }
            }
            let next = self.scan_value(j);
            if next == -4 {
                j = self.i_err + 1;
                continue;
            }
            if next == -3 {
                j = self.i_err;
                break;
            }
            self.i_err = j;
            self.depth -= 1;
            return -1;
        }
        self.depth -= 1;
        self.last_was_text = false;
        isize::try_from(j + 1).unwrap_or(isize::MAX)
    }

    fn scan_string(&mut self, i: usize) -> isize {
        let delim = self.byte(i);
        let mut j = i + 1;
        loop {
            while stock_json_is_ok(self.byte(j)) {
                j += 1;
            }
            let c = self.byte(j);
            if c == delim {
                break;
            }
            if c == b'\\' {
                j += 1;
                let esc = self.byte(j);
                let is_std_escape =
                    matches!(esc, b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't')
                        || (esc == b'u' && stock_is_4hex(self.bytes, j + 1));
                let is_json5_escape = matches!(esc, b'\'' | b'v' | b'\n')
                    || (esc == b'0' && !self.byte(j + 1).is_ascii_digit())
                    || (esc == 0xe2
                        && self.byte(j + 1) == 0x80
                        && matches!(self.byte(j + 2), 0xa8 | 0xa9))
                    || (esc == b'x' && stock_is_2hex(self.bytes, j + 1));
                if is_std_escape || is_json5_escape {
                    // Valid escape sequence.
                } else if esc == b'\r' {
                    if self.byte(j + 1) == b'\n' {
                        j += 1;
                    }
                } else {
                    self.i_err = j;
                    return -1;
                }
            } else if c == 0 {
                self.i_err = j;
                return -1;
            }
            j += 1;
        }
        self.last_was_text = true;
        isize::try_from(j + 1).unwrap_or(isize::MAX)
    }

    fn scan_number(&mut self, i: usize) -> isize {
        let c = self.byte(i);
        let mut is_float = false;
        if c == b'.' {
            if self.byte(i + 1).is_ascii_digit() {
                is_float = true;
            } else {
                self.i_err = i;
                return -1;
            }
        } else if c <= b'0' {
            if c == b'0' {
                if matches!(self.byte(i + 1), b'x' | b'X') && self.byte(i + 2).is_ascii_hexdigit() {
                    let mut j = i + 3;
                    while self.byte(j).is_ascii_hexdigit() {
                        j += 1;
                    }
                    self.last_was_text = false;
                    return isize::try_from(j).unwrap_or(isize::MAX);
                }
                if self.byte(i + 1).is_ascii_digit() {
                    self.i_err = i + 1;
                    return -1;
                }
            } else {
                // '+' or '-'
                if !self.byte(i + 1).is_ascii_digit() {
                    if matches!(self.byte(i + 1), b'I' | b'i')
                        && stock_starts_with_ci(self.bytes, i + 1, b"inf")
                    {
                        let len = if stock_starts_with_ci(self.bytes, i + 4, b"inity") {
                            9
                        } else {
                            4
                        };
                        self.last_was_text = false;
                        return isize::try_from(i + len).unwrap_or(isize::MAX);
                    }
                    if self.byte(i + 1) != b'.' {
                        self.i_err = i;
                        return -1;
                    }
                } else if self.byte(i + 1) == b'0' {
                    if self.byte(i + 2).is_ascii_digit() {
                        self.i_err = i + 1;
                        return -1;
                    }
                    if matches!(self.byte(i + 2), b'x' | b'X')
                        && self.byte(i + 3).is_ascii_hexdigit()
                    {
                        let mut j = i + 4;
                        while self.byte(j).is_ascii_hexdigit() {
                            j += 1;
                        }
                        self.last_was_text = false;
                        return isize::try_from(j).unwrap_or(isize::MAX);
                    }
                }
            }
        }

        let mut seen_e = false;
        let mut j = i + 1;
        loop {
            let cur = self.byte(j);
            if cur.is_ascii_digit() {
                j += 1;
                continue;
            }
            if cur == b'.' {
                if is_float {
                    self.i_err = j;
                    return -1;
                }
                is_float = true;
                j += 1;
                continue;
            }
            if matches!(cur, b'e' | b'E') {
                if self.byte(j - 1) < b'0'
                    && !(self.byte(j - 1) == b'.'
                        && j >= i + 2
                        && self.byte(j - 2).is_ascii_digit())
                {
                    self.i_err = j;
                    return -1;
                }
                if seen_e {
                    self.i_err = j;
                    return -1;
                }
                is_float = true;
                seen_e = true;
                let mut exp_c = self.byte(j + 1);
                if matches!(exp_c, b'+' | b'-') {
                    j += 1;
                    exp_c = self.byte(j + 1);
                }
                if !exp_c.is_ascii_digit() {
                    self.i_err = j;
                    return -1;
                }
                j += 1;
                continue;
            }
            break;
        }
        if self.byte(j - 1) < b'0'
            && !(self.byte(j - 1) == b'.' && j >= i + 2 && self.byte(j - 2).is_ascii_digit())
        {
            self.i_err = j;
            return -1;
        }
        self.last_was_text = false;
        isize::try_from(j).unwrap_or(isize::MAX)
    }

    fn scan_nan_inf(&mut self, i: usize) -> isize {
        const NAN_INF_NAMES: &[&[u8]] = &[b"inf", b"infinity", b"NaN", b"QNaN", b"SNaN"];
        let c = self.byte(i);
        for &name in NAN_INF_NAMES {
            if !c.eq_ignore_ascii_case(&name[0]) {
                continue;
            }
            if !stock_starts_with_ci(self.bytes, i, name) {
                continue;
            }
            if self.byte(i + name.len()).is_ascii_alphanumeric() {
                continue;
            }
            self.last_was_text = false;
            return isize::try_from(i + name.len()).unwrap_or(isize::MAX);
        }
        self.i_err = i;
        -1
    }
}

/// Pretty-print JSON with default 4-space indentation or custom indent token.
pub fn json_pretty(input: &str, indent: Option<&str>) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    Ok(json_pretty_value(&root, indent))
}

/// Quote a SQL value as JSON.
pub fn json_quote(value: &SqliteValue) -> Result<String> {
    match value {
        SqliteValue::Null => Ok("null".to_owned()),
        SqliteValue::Integer(i) => Ok(i.to_string()),
        v @ SqliteValue::Float(f) => {
            if f.is_finite() {
                Ok(v.to_text())
            } else if f.is_nan() {
                // Only NaN maps to JSON null (SQLite stores NaN as NULL).
                Ok("null".to_owned())
            } else if *f > 0.0 {
                // +Inf/-Inf render as the numeric literal 9.0e+999 / -9.0e+999
                // (stock `json_quote(1e999)` -> `9.0e+999`, GH#212).
                Ok("9.0e+999".to_owned())
            } else {
                Ok("-9.0e+999".to_owned())
            }
        }
        SqliteValue::Text(text) => {
            let mut out = String::with_capacity(text.len() + 2);
            write_json_escaped_string(text, &mut out);
            Ok(out)
        }
        // Stock `jsonAppendSqlValue`: a JSONB blob is rendered as JSON text.
        SqliteValue::Blob(bytes) => json_from_jsonb(bytes)
            .map_err(|_| FrankenError::function_error("JSON cannot hold BLOB values")),
    }
}

/// Convert a SQL value to a JSON value, honouring the JSON subtype.
///
/// When `subtype == JSON_SUBTYPE`, a TEXT (or UTF-8 BLOB/JSONB) argument is
/// the textual rendering of an existing JSON value (e.g. the result of
/// `json('[2,3]')`), so it is parsed and embedded verbatim instead of being
/// quoted as a string. This mirrors C SQLite, which keys this off
/// `sqlite3_value_subtype()`.
fn sqlite_to_json_with_subtype(value: &SqliteValue, subtype: u32) -> Result<Value> {
    if subtype == JSON_SUBTYPE {
        match value {
            SqliteValue::Text(text) => return parse_json_text(text),
            SqliteValue::Blob(bytes) => {
                if let Ok(value) = decode_jsonb_root(bytes) {
                    return Ok(value);
                }
                if let Ok(text) = std::str::from_utf8(bytes) {
                    return parse_json_text(text);
                }
            }
            _ => {}
        }
    }
    sqlite_to_json(value)
}

/// Build a JSON array from SQL values.
pub fn json_array(values: &[SqliteValue]) -> Result<String> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(sqlite_to_json(value)?);
    }
    Ok(encode_json_text(&Value::Array(out)))
}

/// Build a JSON array from SQL values, embedding JSON-subtyped arguments as
/// parsed JSON values rather than quoting them as strings.
pub fn json_array_with_subtypes(values: &[SqliteValue], subtypes: &[u32]) -> Result<String> {
    let mut out = Vec::with_capacity(values.len());
    for (i, value) in values.iter().enumerate() {
        let subtype = subtypes.get(i).copied().unwrap_or(0);
        out.push(sqlite_to_json_with_subtype(value, subtype)?);
    }
    Ok(encode_json_text(&Value::Array(out)))
}

/// Serialize an ordered key/value list as JSON object text.
///
/// Preserves argument order AND duplicate labels verbatim — stock SQLite
/// semantics (`json_object('a',1,'a',2)` -> `{"a":1,"a":2}`).
fn encode_json_object_members(members: &[(String, Value)]) -> String {
    let mut out = String::with_capacity(64);
    out.push('{');
    for (index, (key, value)) in members.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_escaped_string(key, &mut out);
        out.push(':');
        write_canonical_json_text(value, &mut out);
    }
    out.push('}');
    out
}

/// Build a JSON object from alternating key/value arguments, embedding
/// JSON-subtyped values as parsed JSON rather than quoting them as strings.
///
/// Duplicate labels are preserved verbatim in argument order, matching stock
/// SQLite.
pub fn json_object_with_subtypes(args: &[SqliteValue], subtypes: &[u32]) -> Result<String> {
    if !args.len().is_multiple_of(2) {
        return Err(FrankenError::function_error(
            "json_object() requires an even number of arguments",
        ));
    }

    let mut members = Vec::with_capacity(args.len() / 2);
    let mut idx = 0;
    while idx < args.len() {
        let key = match &args[idx] {
            SqliteValue::Text(text) => text.to_string(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_object() labels must be TEXT",
                ));
            }
        };
        let subtype = subtypes.get(idx + 1).copied().unwrap_or(0);
        members.push((key, sqlite_to_json_with_subtype(&args[idx + 1], subtype)?));
        idx += 2;
    }

    Ok(encode_json_object_members(&members))
}

/// Build a JSONB object from alternating key/value SQL arguments while
/// preserving duplicate labels and argument order.
///
/// Encoding through `serde_json::Value::Object` would collapse duplicate keys,
/// which is observable because SQLite's JSONB object format permits duplicates.
pub fn jsonb_object_with_subtypes(args: &[SqliteValue], subtypes: &[u32]) -> Result<Vec<u8>> {
    if !args.len().is_multiple_of(2) {
        return Err(FrankenError::function_error(
            "json_object() requires an even number of arguments",
        ));
    }

    let mut payload = Vec::new();
    let mut idx = 0;
    while idx < args.len() {
        let key = match &args[idx] {
            SqliteValue::Text(text) => text.as_ref(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_object() labels must be TEXT",
                ));
            }
        };
        append_jsonb_string(key, &mut payload)?;
        let subtype = subtypes.get(idx + 1).copied().unwrap_or(0);
        let value = sqlite_to_json_with_subtype(&args[idx + 1], subtype)?;
        encode_jsonb_value(&value, &mut payload)?;
        idx += 2;
    }

    let mut out = Vec::new();
    append_jsonb_node(JSONB_OBJECT_TYPE, &payload, &mut out)?;
    Ok(out)
}

/// Build a JSON object from alternating key/value SQL arguments.
///
/// Duplicate labels are preserved verbatim in argument order, matching stock
/// SQLite (`json_object('a',1,'a',2)` -> `{"a":1,"a":2}`).
pub fn json_object(args: &[SqliteValue]) -> Result<String> {
    if !args.len().is_multiple_of(2) {
        return Err(FrankenError::function_error(
            "json_object() requires an even number of arguments",
        ));
    }

    let mut members = Vec::with_capacity(args.len() / 2);
    let mut idx = 0;
    while idx < args.len() {
        let key = match &args[idx] {
            SqliteValue::Text(text) => text.to_string(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_object() labels must be TEXT",
                ));
            }
        };
        members.push((key, sqlite_to_json(&args[idx + 1])?));
        idx += 2;
    }

    Ok(encode_json_object_members(&members))
}

/// Build JSONB from SQL values.
pub fn jsonb_array(values: &[SqliteValue]) -> Result<Vec<u8>> {
    let json_text = json_array(values)?;
    jsonb(&json_text)
}

/// Build JSONB object from alternating key/value SQL arguments.
pub fn jsonb_object(args: &[SqliteValue]) -> Result<Vec<u8>> {
    jsonb_object_with_subtypes(args, &[])
}

/// JSONB variant of [`json_array_with_subtypes`].
pub fn jsonb_array_with_subtypes(values: &[SqliteValue], subtypes: &[u32]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len());
    for (i, value) in values.iter().enumerate() {
        let subtype = subtypes.get(i).copied().unwrap_or(0);
        out.push(sqlite_to_json_with_subtype(value, subtype)?);
    }
    encode_jsonb_root(&Value::Array(out))
}

/// Aggregate rows into a JSON array, preserving SQL NULL as JSON null.
pub fn json_group_array(values: &[SqliteValue]) -> Result<String> {
    json_array(values)
}

/// JSONB variant of `json_group_array`.
pub fn jsonb_group_array(values: &[SqliteValue]) -> Result<Vec<u8>> {
    let json_text = json_group_array(values)?;
    jsonb(&json_text)
}

/// The label a `json_group_object` row stores; `None` for a NULL label.
///
/// Stock's jsonObjectStep reads the label with `sqlite3_value_text`, so a
/// number becomes its text, then measures it with `sqlite3Strlen30`, so the
/// label ends at its first NUL. (`json_object` keeps the whole label.) A row
/// with a NULL label adds no member. The connection's grouped aggregate
/// evaluator builds these objects too, and reads its labels through this.
#[must_use]
pub fn group_object_label(label: &SqliteValue) -> Option<String> {
    if label.is_null() {
        return None;
    }
    let mut text = label.to_text();
    if let Some(nul) = text.find('\0') {
        text.truncate(nul);
    }
    Some(text)
}

/// Aggregate key/value pairs into a JSON object.
///
/// Duplicate keys are preserved verbatim in row order, matching stock SQLite
/// (rows `('a',1),('a',2)` -> `{"a":1,"a":2}`).
pub fn json_group_object(entries: &[(SqliteValue, SqliteValue)]) -> Result<String> {
    let mut members = Vec::with_capacity(entries.len());
    for (key_value, value) in entries {
        let Some(label) = group_object_label(key_value) else {
            continue;
        };
        members.push((label, sqlite_to_json(value)?));
    }
    Ok(encode_json_object_members(&members))
}

/// JSONB variant of `json_group_object`.
pub fn jsonb_group_object(entries: &[(SqliteValue, SqliteValue)]) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    for (key_value, value) in entries {
        let Some(label) = group_object_label(key_value) else {
            continue;
        };
        append_jsonb_string(&label, &mut payload)?;
        encode_jsonb_value(&sqlite_to_json(value)?, &mut payload)?;
    }
    let mut out = Vec::new();
    append_jsonb_node(JSONB_OBJECT_TYPE, &payload, &mut out)?;
    Ok(out)
}

/// Set JSON values at path(s), creating object keys when missing.
pub fn json_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    let edited = edit_json_paths_value(root, pairs, EditMode::Set)?;
    Ok(encode_json_text(&edited))
}

/// JSONB variant of `json_set`.
pub fn jsonb_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let blob = jsonb(input)?;
    edit_jsonb_blob_pairs(blob, pairs, JsonbEditMode::Set)
}

/// Insert JSON values at path(s) only when path does not already exist.
pub fn json_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    let edited = edit_json_paths_value(root, pairs, EditMode::Insert)?;
    Ok(encode_json_text(&edited))
}

/// JSONB variant of `json_insert`.
pub fn jsonb_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let blob = jsonb(input)?;
    edit_jsonb_blob_pairs(blob, pairs, JsonbEditMode::Insert)
}

/// Insert values before array elements named by PATH.
///
/// Unlike `json_insert`, the final path component must identify an array
/// position. An index equal to the array length appends; an index beyond the
/// end is a no-op. Missing object parents may be created when the eventual
/// target is an array position.
pub fn json_array_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    let mut edited = root;
    for (path, value) in pairs {
        let segments = parse_path(path)?;
        apply_array_insert(&mut edited, &segments, sqlite_to_json(value)?, path)?;
    }
    Ok(encode_json_text(&edited))
}

/// JSONB variant of `json_array_insert`.
pub fn jsonb_array_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let blob = jsonb(input)?;
    edit_jsonb_blob_pairs(blob, pairs, JsonbEditMode::ArrayInsert)
}

/// Replace JSON values at path(s) only when path already exists.
pub fn json_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    let edited = edit_json_paths_value(root, pairs, EditMode::Replace)?;
    Ok(encode_json_text(&edited))
}

/// JSONB variant of `json_replace`.
pub fn jsonb_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let blob = jsonb(input)?;
    edit_jsonb_blob_pairs(blob, pairs, JsonbEditMode::Replace)
}

/// Remove JSON values at path(s). Array removals compact the array.
pub fn json_remove(input: &str, paths: &[&str]) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    let Some(edited) = json_remove_value(root, paths)? else {
        return Ok("null".to_owned());
    };
    Ok(encode_json_text(&edited))
}

/// JSONB variant of `json_remove`.
pub fn jsonb_remove(input: &str, paths: &[&str]) -> Result<Vec<u8>> {
    let blob = jsonb(input)?;
    let Some(edited) = remove_jsonb_blob_paths(blob, paths)? else {
        return encode_jsonb_root(&Value::Null);
    };
    Ok(edited)
}

/// Apply RFC 7396 JSON Merge Patch.
pub fn json_patch(input: &str, patch: &str) -> Result<String> {
    let root = parse_json_value_lenient(input)?;
    let patch_value = parse_json_value_lenient(patch)?;
    let merged = json_patch_value(root, patch_value);
    Ok(encode_json_text(&merged))
}

/// JSONB variant of `json_patch`.
pub fn jsonb_patch(input: &str, patch: &str) -> Result<Vec<u8>> {
    let root = parse_json_value_lenient(input)?;
    let patch_value = parse_json_value_lenient(patch)?;
    let merged = json_patch_value(root, patch_value);
    encode_jsonb_root(&merged)
}

/// Row shape produced by `json_each` and `json_tree`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonTableRow {
    /// Object key, array index, or NULL (root/scalar).
    pub key: SqliteValue,
    /// Value column: scalars are SQL-native, objects/arrays are JSON text.
    pub value: SqliteValue,
    /// One of: null, true, false, integer, real, text, array, object.
    pub type_name: &'static str,
    /// Scalar atom or NULL for arrays/objects.
    pub atom: SqliteValue,
    /// Stable row identifier within the result set.
    pub id: i64,
    /// Parent row id (NULL at root/top-level).
    pub parent: SqliteValue,
    /// Absolute JSON path for this row.
    pub fullkey: String,
    /// Parent container path (or same as fullkey for root/scalar rows).
    pub path: String,
}

/// Table-valued `json_each`: iterate immediate children at root or `path`.
pub fn json_each(input: &str, path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let blob = translate_json_text_to_jsonb(input)?;
    json_table_rows_from_jsonb(&blob, path, false)
}

/// Table-valued `json_each` over TEXT JSON or JSONB bytes.
pub fn json_each_blob(input: &[u8], path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let blob = json_blob_to_jsonb(input)?;
    json_table_rows_from_jsonb(&blob, path, false)
}

/// Table-valued `json_tree`: recursively iterate subtree at root or `path`.
pub fn json_tree(input: &str, path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let blob = translate_json_text_to_jsonb(input)?;
    json_table_rows_from_jsonb(&blob, path, true)
}

/// Table-valued `json_tree` over TEXT JSON or JSONB bytes.
pub fn json_tree_blob(input: &[u8], path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let blob = json_blob_to_jsonb(input)?;
    json_table_rows_from_jsonb(&blob, path, true)
}

/// Virtual table module for `json_each`.
pub struct JsonEachVtab;

/// Cursor for `json_each` virtual table scans.
#[derive(Default)]
pub struct JsonEachCursor {
    rows: Vec<JsonTableRow>,
    pos: usize,
    hidden_columns: Option<JsonTableHiddenColumns>,
}

impl VirtualTable for JsonEachVtab {
    type Cursor = JsonEachCursor;

    fn hidden_column_names(_args: &[&str]) -> Vec<String> {
        ["json", "root"].into_iter().map(str::to_owned).collect()
    }

    fn column_info(_args: &[&str]) -> Vec<(String, char)> {
        JSON_TABLE_COLUMN_NAMES
            .into_iter()
            .map(|name| (name.to_owned(), 'A'))
            .collect()
    }

    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.estimated_cost = 100.0;
        info.estimated_rows = 100;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(JsonEachCursor::default())
    }
}

impl VirtualTableCursor for JsonEachCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        // Re-filtering may fail; the previous scan must not remain visible.
        self.rows.clear();
        self.pos = 0;
        self.hidden_columns = None;
        if let Some((blob, hidden)) = parse_json_table_filter_args("json_each", args)? {
            self.rows = json_table_rows_from_jsonb(&blob, Some(hidden.root.as_str()), false)?;
            self.hidden_columns = Some(hidden);
        }
        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.pos < self.rows.len() {
            self.pos += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let Some(row) = self.rows.get(self.pos) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };
        write_json_table_column(row, self.hidden_columns.as_ref(), ctx, col);
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        json_table_cursor_rowid(self.pos, self.rows.len())
    }
}

/// Virtual table module for `json_tree`.
pub struct JsonTreeVtab;

/// Cursor for `json_tree` virtual table scans.
#[derive(Default)]
pub struct JsonTreeCursor {
    rows: Vec<JsonTableRow>,
    pos: usize,
    hidden_columns: Option<JsonTableHiddenColumns>,
}

impl VirtualTable for JsonTreeVtab {
    type Cursor = JsonTreeCursor;

    fn hidden_column_names(_args: &[&str]) -> Vec<String> {
        ["json", "root"].into_iter().map(str::to_owned).collect()
    }

    fn column_info(_args: &[&str]) -> Vec<(String, char)> {
        JSON_TABLE_COLUMN_NAMES
            .into_iter()
            .map(|name| (name.to_owned(), 'A'))
            .collect()
    }

    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.estimated_cost = 200.0;
        info.estimated_rows = 1_000;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(JsonTreeCursor::default())
    }
}

impl VirtualTableCursor for JsonTreeCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        self.rows.clear();
        self.pos = 0;
        self.hidden_columns = None;
        if let Some((blob, hidden)) = parse_json_table_filter_args("json_tree", args)? {
            self.rows = json_table_rows_from_jsonb(&blob, Some(hidden.root.as_str()), true)?;
            self.hidden_columns = Some(hidden);
        }
        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.pos < self.rows.len() {
            self.pos += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let Some(row) = self.rows.get(self.pos) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };
        write_json_table_column(row, self.hidden_columns.as_ref(), ctx, col);
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        json_table_cursor_rowid(self.pos, self.rows.len())
    }
}

fn json_table_cursor_rowid(pos: usize, row_count: usize) -> Result<i64> {
    if pos >= row_count {
        return Ok(0);
    }
    i64::try_from(pos).map_err(|error| {
        FrankenError::function_error(format!("JSON table rowid overflow: {error}"))
    })
}

fn parse_json_text(input: &str) -> Result<Value> {
    let mut parser = StrictJsonParser::new(input);
    parser.skip_ws();
    let value = parser.parse_value()?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err(malformed_json());
    }
    Ok(value)
}

/// Zero-allocation strict RFC-8259 validator (bd-oahj7).
fn validate_json_text(input: &str) -> bool {
    let mut parser = StrictJsonParser::new(input);
    parser.skip_ws();
    if parser.validate_value().is_err() {
        return false;
    }
    parser.skip_ws();
    parser.pos == parser.bytes.len()
}

#[derive(Clone, Copy)]
enum FastPathStep<'a> {
    Key(&'a str),
    Index(usize),
}

const MAX_FAST_PATH_STEPS: usize = 8;

struct FastPath<'a> {
    steps: [FastPathStep<'a>; MAX_FAST_PATH_STEPS],
    len: usize,
}

impl<'a> FastPath<'a> {
    #[inline]
    fn as_slice(&self) -> &[FastPathStep<'a>] {
        &self.steps[..self.len]
    }
}

#[derive(Clone, Copy)]
enum ExtractedKind {
    Null,
    True,
    False,
    Int,
    Float,
    String { has_escapes: bool },
    Array { len: usize },
    Object,
}

#[derive(Clone, Copy)]
struct ExtractedSpan {
    kind: ExtractedKind,
    start: usize,
    end: usize,
}

enum StrictExtractOutcome<T> {
    Matched(T),
    NotStrictJson,
    UnsupportedPath,
}

fn parse_fast_path(path: &str) -> Option<FastPath<'_>> {
    let bytes = path.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return None;
    }
    let mut steps = [FastPathStep::Index(0); MAX_FAST_PATH_STEPS];
    let mut len = 0usize;
    let mut idx = 1usize;

    while idx < bytes.len() {
        if len >= MAX_FAST_PATH_STEPS {
            return None;
        }
        match bytes[idx] {
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return None;
                }
                if bytes[idx] == b'"' {
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() {
                        let b = bytes[idx];
                        if b == b'\\' {
                            // Escaped quoted path keys fall back to the full resolver.
                            return None;
                        }
                        if b == b'"' {
                            break;
                        }
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return None;
                    }
                    steps[len] = FastPathStep::Key(&path[start..idx]);
                    len += 1;
                    idx += 1;
                } else {
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                        idx += 1;
                    }
                    if start == idx {
                        return None;
                    }
                    steps[len] = FastPathStep::Key(&path[start..idx]);
                    len += 1;
                }
            }
            b'[' => {
                idx += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b']' {
                    if !bytes[idx].is_ascii_digit() {
                        return None;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() || start == idx {
                    return None;
                }
                let index = parse_path_index(&path[start..idx])?;
                steps[len] = FastPathStep::Index(index);
                len += 1;
                idx += 1;
            }
            _ => return None,
        }
    }

    Some(FastPath { steps, len })
}

enum StrictScanSpan {
    Found(ExtractedSpan),
    Missing,
    NotStrictJson,
}

enum FastLookup<T> {
    Resolved(Option<T>),
    Fallback,
}

fn extract_strict_json_span(input: &str, path: &[FastPathStep<'_>]) -> StrictScanSpan {
    let mut parser = StrictJsonParser::new(input);
    let mut found = None;
    parser.skip_ws();
    if parser.extract_value(path, &mut found).is_err() {
        return StrictScanSpan::NotStrictJson;
    }
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return StrictScanSpan::NotStrictJson;
    }
    match found {
        Some(span) => StrictScanSpan::Found(span),
        None => StrictScanSpan::Missing,
    }
}

#[inline]
fn parse_integer_value(raw: &str) -> Value {
    if raw == "-0" {
        return Value::IntegerText("-0".to_owned());
    }
    let bytes = raw.as_bytes();
    let (neg, digits) = if bytes.first() == Some(&b'-') {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.len() <= 18 {
        let mut acc: i64 = 0;
        for &b in digits {
            acc = acc * 10 + i64::from(b - b'0');
        }
        return Value::Number(Number::from(if neg { -acc } else { acc }));
    }
    if let Ok(int) = raw.parse::<i64>() {
        return Value::Number(Number::from(int));
    }
    if !neg && let Ok(uint) = raw.parse::<u64>() {
        return Value::Number(Number::from(uint));
    }
    Value::IntegerText(raw.to_owned())
}

#[inline]
fn parse_integer_to_sqlite(raw: &str) -> SqliteValue {
    if raw == "-0" {
        return SqliteValue::Integer(0);
    }
    let bytes = raw.as_bytes();
    let (neg, digits) = if bytes.first() == Some(&b'-') {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.len() <= 18 {
        let mut acc: i64 = 0;
        for &b in digits {
            acc = acc * 10 + i64::from(b - b'0');
        }
        return SqliteValue::Integer(if neg { -acc } else { acc });
    }
    if let Ok(int) = raw.parse::<i64>() {
        return SqliteValue::Integer(int);
    }
    SqliteValue::Float(raw.parse::<f64>().unwrap_or(0.0))
}

#[inline]
fn hex_nibble(b: u8) -> Option<u16> {
    match b {
        b'0'..=b'9' => Some(u16::from(b - b'0')),
        b'a'..=b'f' => Some(u16::from(b - b'a' + 10)),
        b'A'..=b'F' => Some(u16::from(b - b'A' + 10)),
        _ => None,
    }
}

#[inline]
fn parse_hex4(bytes: &[u8], pos: usize) -> Option<u16> {
    let slice = bytes.get(pos..pos + 4)?;
    let h0 = hex_nibble(slice[0])?;
    let h1 = hex_nibble(slice[1])?;
    let h2 = hex_nibble(slice[2])?;
    let h3 = hex_nibble(slice[3])?;
    Some((h0 << 12) | (h1 << 8) | (h2 << 4) | h3)
}

fn decode_validated_json_string(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0usize;
    let mut start = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            i += 1;
            continue;
        }
        if start < i {
            out.push_str(&raw[start..i]);
        }
        i += 1;
        match bytes[i] {
            b'"' => out.push('"'),
            b'\\' => out.push('\\'),
            b'/' => out.push('/'),
            b'b' => out.push('\u{0008}'),
            b'f' => out.push('\u{000C}'),
            b'n' => out.push('\n'),
            b'r' => out.push('\r'),
            b't' => out.push('\t'),
            b'u' => {
                let unit = parse_hex4(bytes, i + 1).expect("validated \\uXXXX escape");
                i += 4;
                if (0xD800..=0xDBFF).contains(&unit) {
                    let low = parse_hex4(bytes, i + 3).expect("validated surrogate pair");
                    i += 6;
                    let code =
                        0x10000 + ((u32::from(unit) - 0xD800) << 10) + (u32::from(low) - 0xDC00);
                    out.push(char::from_u32(code).expect("valid Unicode scalar"));
                } else {
                    out.push(char::from_u32(u32::from(unit)).expect("valid Unicode scalar"));
                }
            }
            _ => unreachable!("validated JSON escape"),
        }
        i += 1;
        start = i;
    }
    if start < bytes.len() {
        out.push_str(&raw[start..]);
    }
    out
}

/// Single-pass strict RFC-8259 parser, validator, and streaming single-path
/// extractor (bd-oahj7). Preserves float literal text verbatim without a
/// second byte pass or `serde_json` string-rewriting round trip.
struct StrictJsonParser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> StrictJsonParser<'a> {
    #[inline]
    const fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            depth: 0,
        }
    }

    #[inline]
    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn scan_string_span(&mut self) -> Result<(usize, usize, bool)> {
        if self.bytes.get(self.pos) != Some(&b'"') {
            return Err(malformed_json());
        }
        self.pos += 1;
        let content_start = self.pos;
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b == b'"' {
                let content_end = self.pos;
                self.pos += 1;
                return Ok((content_start, content_end, false));
            }
            if b == b'\\' {
                return self.finish_validate_escaped_string(content_start);
            }
            if b < 0x20 {
                return Err(malformed_json());
            }
            self.pos += 1;
        }
        Err(malformed_json())
    }

    fn finish_validate_escaped_string(
        &mut self,
        content_start: usize,
    ) -> Result<(usize, usize, bool)> {
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b == b'"' {
                let content_end = self.pos;
                self.pos += 1;
                return Ok((content_start, content_end, true));
            }
            if b < 0x20 {
                return Err(malformed_json());
            }
            self.pos += 1;
            if b == b'\\' {
                let Some(&esc) = self.bytes.get(self.pos) else {
                    return Err(malformed_json());
                };
                match esc {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                        self.pos += 1;
                    }
                    b'u' => {
                        self.pos += 1;
                        let unit = parse_hex4(self.bytes, self.pos).ok_or_else(malformed_json)?;
                        self.pos += 4;
                        if (0xD800..=0xDBFF).contains(&unit) {
                            if self.bytes.get(self.pos..self.pos + 2) != Some(b"\\u") {
                                return Err(malformed_json());
                            }
                            self.pos += 2;
                            let low =
                                parse_hex4(self.bytes, self.pos).ok_or_else(malformed_json)?;
                            if !(0xDC00..=0xDFFF).contains(&low) {
                                return Err(malformed_json());
                            }
                            self.pos += 4;
                        } else if (0xDC00..=0xDFFF).contains(&unit) {
                            return Err(malformed_json());
                        }
                    }
                    _ => return Err(malformed_json()),
                }
            }
        }
        Err(malformed_json())
    }

    fn parse_string(&mut self) -> Result<InlineStr> {
        let (start, end, has_escapes) = self.scan_string_span()?;
        if !has_escapes {
            Ok(InlineStr::new(&self.input[start..end]))
        } else {
            Ok(InlineStr::from_string(decode_validated_json_string(
                &self.input[start..end],
            )))
        }
    }

    fn scan_number_span(&mut self) -> Result<(usize, usize, bool)> {
        let start = self.pos;
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1;
        }
        match self.bytes.get(self.pos).copied() {
            Some(b'0') => {
                self.pos += 1;
                if self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                    return Err(malformed_json());
                }
            }
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                    self.pos += 1;
                }
            }
            _ => return Err(malformed_json()),
        }
        let mut is_float = false;
        if self.bytes.get(self.pos) == Some(&b'.') {
            is_float = true;
            self.pos += 1;
            if !self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                return Err(malformed_json());
            }
            while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                self.pos += 1;
            }
        }
        if matches!(self.bytes.get(self.pos), Some(b'e' | b'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.bytes.get(self.pos), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                return Err(malformed_json());
            }
            while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
                self.pos += 1;
            }
        }
        Ok((start, self.pos, is_float))
    }

    fn parse_value(&mut self) -> Result<Value> {
        match self.bytes.get(self.pos).copied() {
            Some(b'n') => {
                if self.bytes.get(self.pos..self.pos + 4) == Some(b"null") {
                    self.pos += 4;
                    Ok(Value::Null)
                } else {
                    Err(malformed_json())
                }
            }
            Some(b't') => {
                if self.bytes.get(self.pos..self.pos + 4) == Some(b"true") {
                    self.pos += 4;
                    Ok(Value::Bool(true))
                } else {
                    Err(malformed_json())
                }
            }
            Some(b'f') => {
                if self.bytes.get(self.pos..self.pos + 5) == Some(b"false") {
                    self.pos += 5;
                    Ok(Value::Bool(false))
                } else {
                    Err(malformed_json())
                }
            }
            Some(b'"') => self.parse_string().map(Value::String),
            Some(b'-' | b'0'..=b'9') => {
                let (start, end, is_float) = self.scan_number_span()?;
                let raw = &self.input[start..end];
                if is_float {
                    Ok(Value::FloatText(InlineStr::new(raw)))
                } else {
                    Ok(parse_integer_value(raw))
                }
            }
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            _ => Err(malformed_json()),
        }
    }

    fn parse_array(&mut self) -> Result<Value> {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.depth -= 1;
            return Err(malformed_json());
        }
        self.pos += 1; // '['
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b']') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(Value::Array(Vec::new()));
        }
        let mut items = Vec::with_capacity(4);
        loop {
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.bytes.get(self.pos).copied() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                }
                Some(b']') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(Value::Array(items));
                }
                _ => {
                    self.depth -= 1;
                    return Err(malformed_json());
                }
            }
        }
    }

    fn parse_object(&mut self) -> Result<Value> {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.depth -= 1;
            return Err(malformed_json());
        }
        self.pos += 1; // '{'
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(Value::Object(Object::new()));
        }
        let mut object = Object::with_capacity(8);
        loop {
            let key = self.parse_string()?;
            self.skip_ws();
            if self.bytes.get(self.pos) != Some(&b':') {
                self.depth -= 1;
                return Err(malformed_json());
            }
            self.pos += 1;
            self.skip_ws();
            let value = self.parse_value()?;
            object.append(key, value);
            self.skip_ws();
            match self.bytes.get(self.pos).copied() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                }
                Some(b'}') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(Value::Object(object));
                }
                _ => {
                    self.depth -= 1;
                    return Err(malformed_json());
                }
            }
        }
    }

    fn validate_value(&mut self) -> Result<()> {
        match self.bytes.get(self.pos).copied() {
            Some(b'n') => {
                if self.bytes.get(self.pos..self.pos + 4) == Some(b"null") {
                    self.pos += 4;
                    Ok(())
                } else {
                    Err(malformed_json())
                }
            }
            Some(b't') => {
                if self.bytes.get(self.pos..self.pos + 4) == Some(b"true") {
                    self.pos += 4;
                    Ok(())
                } else {
                    Err(malformed_json())
                }
            }
            Some(b'f') => {
                if self.bytes.get(self.pos..self.pos + 5) == Some(b"false") {
                    self.pos += 5;
                    Ok(())
                } else {
                    Err(malformed_json())
                }
            }
            Some(b'"') => {
                let _ = self.scan_string_span()?;
                Ok(())
            }
            Some(b'-' | b'0'..=b'9') => {
                let _ = self.scan_number_span()?;
                Ok(())
            }
            Some(b'[') => {
                let _ = self.validate_array_len()?;
                Ok(())
            }
            Some(b'{') => self.validate_object(),
            _ => Err(malformed_json()),
        }
    }

    fn validate_value_with_kind(&mut self) -> Result<ExtractedKind> {
        match self.bytes.get(self.pos).copied() {
            Some(b'n') => {
                if self.bytes.get(self.pos..self.pos + 4) == Some(b"null") {
                    self.pos += 4;
                    Ok(ExtractedKind::Null)
                } else {
                    Err(malformed_json())
                }
            }
            Some(b't') => {
                if self.bytes.get(self.pos..self.pos + 4) == Some(b"true") {
                    self.pos += 4;
                    Ok(ExtractedKind::True)
                } else {
                    Err(malformed_json())
                }
            }
            Some(b'f') => {
                if self.bytes.get(self.pos..self.pos + 5) == Some(b"false") {
                    self.pos += 5;
                    Ok(ExtractedKind::False)
                } else {
                    Err(malformed_json())
                }
            }
            Some(b'"') => {
                let (_, _, has_escapes) = self.scan_string_span()?;
                Ok(ExtractedKind::String { has_escapes })
            }
            Some(b'-' | b'0'..=b'9') => {
                let (_, _, is_float) = self.scan_number_span()?;
                Ok(if is_float {
                    ExtractedKind::Float
                } else {
                    ExtractedKind::Int
                })
            }
            Some(b'[') => {
                let len = self.validate_array_len()?;
                Ok(ExtractedKind::Array { len })
            }
            Some(b'{') => {
                self.validate_object()?;
                Ok(ExtractedKind::Object)
            }
            _ => Err(malformed_json()),
        }
    }

    fn validate_array_len(&mut self) -> Result<usize> {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.depth -= 1;
            return Err(malformed_json());
        }
        self.pos += 1; // '['
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b']') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(0);
        }
        let mut count = 0usize;
        loop {
            self.validate_value()?;
            count += 1;
            self.skip_ws();
            match self.bytes.get(self.pos).copied() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                }
                Some(b']') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(count);
                }
                _ => {
                    self.depth -= 1;
                    return Err(malformed_json());
                }
            }
        }
    }

    fn validate_object(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.depth -= 1;
            return Err(malformed_json());
        }
        self.pos += 1; // '{'
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(());
        }
        loop {
            let _ = self.scan_string_span()?;
            self.skip_ws();
            if self.bytes.get(self.pos) != Some(&b':') {
                self.depth -= 1;
                return Err(malformed_json());
            }
            self.pos += 1;
            self.skip_ws();
            self.validate_value()?;
            self.skip_ws();
            match self.bytes.get(self.pos).copied() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                }
                Some(b'}') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                _ => {
                    self.depth -= 1;
                    return Err(malformed_json());
                }
            }
        }
    }

    #[inline]
    fn key_matches(&self, start: usize, end: usize, has_escapes: bool, target: &str) -> bool {
        if !has_escapes {
            &self.bytes[start..end] == target.as_bytes()
        } else {
            decode_validated_json_string(&self.input[start..end]) == target
        }
    }

    fn extract_value(
        &mut self,
        steps: &[FastPathStep<'_>],
        found: &mut Option<ExtractedSpan>,
    ) -> Result<()> {
        let Some((&step, rest)) = steps.split_first() else {
            let start = self.pos;
            let kind = self.validate_value_with_kind()?;
            *found = Some(ExtractedSpan {
                kind,
                start,
                end: self.pos,
            });
            return Ok(());
        };
        match (self.bytes.get(self.pos).copied(), step) {
            (Some(b'{'), FastPathStep::Key(target_key)) => {
                self.extract_object(target_key, rest, found)
            }
            (Some(b'['), FastPathStep::Index(target_idx)) => {
                self.extract_array(target_idx, rest, found)
            }
            _ => self.validate_value(),
        }
    }

    fn extract_object(
        &mut self,
        target_key: &str,
        rest: &[FastPathStep<'_>],
        found: &mut Option<ExtractedSpan>,
    ) -> Result<()> {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.depth -= 1;
            return Err(malformed_json());
        }
        self.pos += 1; // '{'
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(());
        }
        let mut matched = false;
        loop {
            let (ks, ke, has_escapes) = self.scan_string_span()?;
            self.skip_ws();
            if self.bytes.get(self.pos) != Some(&b':') {
                self.depth -= 1;
                return Err(malformed_json());
            }
            self.pos += 1;
            self.skip_ws();
            if !matched && self.key_matches(ks, ke, has_escapes, target_key) {
                matched = true;
                self.extract_value(rest, found)?;
            } else {
                self.validate_value()?;
            }
            self.skip_ws();
            match self.bytes.get(self.pos).copied() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                }
                Some(b'}') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                _ => {
                    self.depth -= 1;
                    return Err(malformed_json());
                }
            }
        }
    }

    fn extract_array(
        &mut self,
        target_idx: usize,
        rest: &[FastPathStep<'_>],
        found: &mut Option<ExtractedSpan>,
    ) -> Result<()> {
        self.depth += 1;
        if self.depth > STOCK_JSON_MAX_DEPTH {
            self.depth -= 1;
            return Err(malformed_json());
        }
        self.pos += 1; // '['
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b']') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(());
        }
        let mut elem_idx = 0usize;
        loop {
            if elem_idx == target_idx {
                self.extract_value(rest, found)?;
            } else {
                self.validate_value()?;
            }
            elem_idx += 1;
            self.skip_ws();
            match self.bytes.get(self.pos).copied() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                }
                Some(b']') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                _ => {
                    self.depth -= 1;
                    return Err(malformed_json());
                }
            }
        }
    }
}

fn extracted_span_to_sqlite_scalar(
    input: &str,
    span: ExtractedSpan,
) -> Result<(SqliteValue, Option<u32>)> {
    let slice = &input[span.start..span.end];
    match span.kind {
        ExtractedKind::Null => Ok((SqliteValue::Null, None)),
        ExtractedKind::True => Ok((SqliteValue::Integer(1), None)),
        ExtractedKind::False => Ok((SqliteValue::Integer(0), None)),
        ExtractedKind::Int => Ok((parse_integer_to_sqlite(slice), None)),
        ExtractedKind::Float => Ok((
            SqliteValue::Float(slice.parse::<f64>().unwrap_or(0.0)),
            None,
        )),
        ExtractedKind::String { has_escapes: false } => Ok((
            SqliteValue::Text(SmallText::new(&input[span.start + 1..span.end - 1])),
            None,
        )),
        ExtractedKind::String { has_escapes: true } => Ok((
            SqliteValue::Text(SmallText::from_string(decode_validated_json_string(
                &input[span.start + 1..span.end - 1],
            ))),
            None,
        )),
        ExtractedKind::Array { .. } | ExtractedKind::Object => {
            let sub = parse_json_text(slice)?;
            let encoded = encode_json_text(&sub);
            Ok((
                SqliteValue::Text(SmallText::from_string(encoded)),
                Some(JSON_SUBTYPE),
            ))
        }
    }
}

fn strict_extract_single_with_subtype(
    input: &str,
    path: &str,
) -> Result<StrictExtractOutcome<(SqliteValue, Option<u32>)>> {
    let Some(fast_path) = parse_fast_path(path) else {
        return Ok(StrictExtractOutcome::UnsupportedPath);
    };
    match extract_strict_json_span(input, fast_path.as_slice()) {
        StrictScanSpan::NotStrictJson => Ok(StrictExtractOutcome::NotStrictJson),
        StrictScanSpan::Missing => Ok(StrictExtractOutcome::Matched((SqliteValue::Null, None))),
        StrictScanSpan::Found(span) => Ok(StrictExtractOutcome::Matched(
            extracted_span_to_sqlite_scalar(input, span)?,
        )),
    }
}

fn try_strict_json_extract_single(
    input: &str,
    path: &str,
) -> Result<Option<(SqliteValue, Option<u32>)>> {
    match strict_extract_single_with_subtype(input, path)? {
        StrictExtractOutcome::Matched(val) => Ok(Some(val)),
        StrictExtractOutcome::NotStrictJson => {
            let root = parse_json5_text(input).map_err(|_| malformed_json())?;
            let selected = resolve_path(&root, path)?;
            let subtype = matches!(selected, Some(Value::Array(_) | Value::Object(_)))
                .then_some(JSON_SUBTYPE);
            Ok(Some((
                selected.map_or(Ok(SqliteValue::Null), json_to_sqlite_scalar)?,
                subtype,
            )))
        }
        StrictExtractOutcome::UnsupportedPath => Ok(None),
    }
}

fn try_strict_json_arrow(input: &str, path: &str) -> Result<Option<SqliteValue>> {
    let Some(fast_path) = parse_fast_path(path) else {
        return Ok(None);
    };
    let span = match extract_strict_json_span(input, fast_path.as_slice()) {
        StrictScanSpan::NotStrictJson => {
            let root = parse_json5_text(input).map_err(|_| malformed_json())?;
            return json_arrow_value(&root, path).map(Some);
        }
        StrictScanSpan::Missing => return Ok(Some(SqliteValue::Null)),
        StrictScanSpan::Found(span) => span,
    };
    let slice = &input[span.start..span.end];
    let rendered = match span.kind {
        ExtractedKind::Null
        | ExtractedKind::True
        | ExtractedKind::False
        | ExtractedKind::Int
        | ExtractedKind::Float
        | ExtractedKind::String { has_escapes: false } => SqliteValue::Text(SmallText::new(slice)),
        ExtractedKind::String { has_escapes: true }
        | ExtractedKind::Array { .. }
        | ExtractedKind::Object => {
            let sub = parse_json_text(slice)?;
            let encoded = encode_json_text(&sub);
            SqliteValue::Text(SmallText::from_string(encoded))
        }
    };
    Ok(Some(rendered))
}

fn try_strict_json_type(input: &str, path: Option<&str>) -> Result<FastLookup<&'static str>> {
    let scan = match path {
        None => extract_strict_json_span(input, &[]),
        Some(path_expr) => {
            let Some(fast_path) = parse_fast_path(path_expr) else {
                return Ok(FastLookup::Fallback);
            };
            extract_strict_json_span(input, fast_path.as_slice())
        }
    };
    match scan {
        StrictScanSpan::NotStrictJson => {
            let root = parse_json5_text(input).map_err(|_| malformed_json())?;
            Ok(FastLookup::Resolved(json_type_value(&root, path)?))
        }
        StrictScanSpan::Missing => Ok(FastLookup::Resolved(None)),
        StrictScanSpan::Found(span) => Ok(FastLookup::Resolved(Some(match span.kind {
            ExtractedKind::Null => "null",
            ExtractedKind::True => "true",
            ExtractedKind::False => "false",
            ExtractedKind::Int => "integer",
            ExtractedKind::Float => "real",
            ExtractedKind::String { .. } => "text",
            ExtractedKind::Array { .. } => "array",
            ExtractedKind::Object => "object",
        }))),
    }
}

fn try_strict_json_array_length(input: &str, path: Option<&str>) -> Result<FastLookup<usize>> {
    let scan = match path {
        None => extract_strict_json_span(input, &[]),
        Some(path_expr) => {
            let Some(fast_path) = parse_fast_path(path_expr) else {
                return Ok(FastLookup::Fallback);
            };
            extract_strict_json_span(input, fast_path.as_slice())
        }
    };
    match scan {
        StrictScanSpan::NotStrictJson => {
            let root = parse_json5_text(input).map_err(|_| malformed_json())?;
            Ok(FastLookup::Resolved(json_array_length_value(&root, path)?))
        }
        StrictScanSpan::Missing => Ok(FastLookup::Resolved(None)),
        StrictScanSpan::Found(span) => Ok(FastLookup::Resolved(Some(match span.kind {
            ExtractedKind::Array { len } => len,
            _ => 0,
        }))),
    }
}

/// Deserialize JSON5 without letting serde_json turn infinite floats into null.
struct Json5Value(Value);

impl<'de> serde::Deserialize<'de> for Json5Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(Json5ValueVisitor).map(Self)
    }
}

struct Json5ValueVisitor;

impl<'de> serde::de::DeserializeSeed<'de> for Json5ValueVisitor {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Json5ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON5 value")
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let value = if value.is_nan() {
            Value::Null
        } else if value.is_infinite() {
            number_text_value(if value.is_sign_negative() {
                "-9e999"
            } else {
                "9e999"
            })
        } else {
            Value::Number(Number::from_f64(value).expect("finite JSON number"))
        };
        Ok(value)
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(InlineStr::new(value)))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(json_string_value(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(Self)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut values = Object::new();
        while let Some(key) = object.next_key::<String>()? {
            let value = object.next_value_seed(Self)?;
            values.append(key, value);
        }
        Ok(Value::Object(values))
    }
}

/// Skip JSON5 whitespace and comments outside string literals.
fn skip_json5_trivia(input: &str, mut pos: usize) -> usize {
    let bytes = input.as_bytes();
    loop {
        while pos < bytes.len() {
            let ch = input[pos..].chars().next().expect("in-bounds char");
            if ch.is_whitespace() || ch == '\u{feff}' {
                pos += ch.len_utf8();
            } else {
                break;
            }
        }
        if bytes.get(pos..pos + 2) == Some(b"//") {
            pos += 2;
            while pos < bytes.len() && !matches!(bytes[pos], b'\r' | b'\n') {
                pos += 1;
            }
        } else if bytes.get(pos..pos + 2) == Some(b"/*") {
            pos += 2;
            while pos + 1 < bytes.len() && &bytes[pos..pos + 2] != b"*/" {
                pos += 1;
            }
            pos = (pos + 2).min(bytes.len());
        } else {
            return pos;
        }
    }
}

/// SQLite's JSON5 scanner treats bare JSON keywords and nonfinite-number
/// spellings as value tokens, not as object-key identifiers. The json5 crate
/// accepts these as unquoted keys, so reject that specific mismatch after
/// syntax validation. Quoted keys and keywords used as values remain valid.
fn json5_has_reserved_unquoted_key(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        pos = skip_json5_trivia(input, pos);
        if pos >= bytes.len() {
            break;
        }
        if matches!(bytes[pos], b'"' | b'\'') {
            let quote = bytes[pos];
            pos += 1;
            while pos < bytes.len() {
                if bytes[pos] == b'\\' {
                    pos = (pos + 2).min(bytes.len());
                } else if bytes[pos] == quote {
                    pos += 1;
                    break;
                } else {
                    pos += 1;
                }
            }
            continue;
        }
        if bytes[pos].is_ascii_alphabetic() || matches!(bytes[pos], b'_' | b'$') {
            let start = pos;
            pos += 1;
            while pos < bytes.len()
                && (bytes[pos].is_ascii_alphanumeric() || matches!(bytes[pos], b'_' | b'$'))
            {
                pos += 1;
            }
            let word = &input[start..pos];
            let reserved = matches!(word, "true" | "false" | "null")
                || ["inf", "infinity", "nan", "qnan", "snan"]
                    .iter()
                    .any(|keyword| word.eq_ignore_ascii_case(keyword));
            if reserved && bytes.get(skip_json5_trivia(input, pos)) == Some(&b':') {
                return true;
            }
        } else {
            pos += input[pos..].chars().next().expect("in-bounds char").len_utf8();
        }
    }
    false
}

fn parse_json5_text(input: &str) -> Result<Value> {
    let Json5Value(mut value) = json5::from_str(input)
        .map_err(|error| FrankenError::function_error(format!("invalid JSON5 input: {error}")))?;
    if json5_has_reserved_unquoted_key(input) {
        return Err(malformed_json());
    }
    if may_hold_integer_text(input) || input.contains("0x") || input.contains("0X") {
        let mut remaining = input;
        restore_json5_integer_text(&mut value, &mut remaining);
    }
    Ok(value)
}

/// The JSON5 deserializer rounds a wide integer before calling our serde
/// visitor. Recover its decimal spelling from the validated input. Members
/// (including duplicate keys) are stored in source order, so consuming one
/// token per key and per scalar leaf keeps tokens aligned with the value tree.
fn restore_json5_integer_text(value: &mut Value, input: &mut &str) {
    match value {
        Value::Array(items) => {
            for item in items {
                restore_json5_integer_text(item, input);
            }
        }
        Value::Object(object) => {
            for (_, item) in &mut object.members {
                let _label = next_json5_atom(input);
                restore_json5_integer_text(item, input);
            }
        }
        _ => {
            if let Some(token) = next_json5_atom(input) {
                let digits = token.strip_prefix('+').unwrap_or(token);
                if integer_needs_text(digits) {
                    *value = Value::IntegerText(digits.to_owned());
                } else if let Some(integer) = HexInteger::parse_value(digits) {
                    *value = integer;
                }
            }
        }
    }
}

/// Return a complete string, bare identifier, or scalar token from valid
/// JSON5. Quoted text and comments are opaque, including any digits inside.
fn next_json5_atom<'a>(input: &mut &'a str) -> Option<&'a str> {
    let separator =
        |c| json5::char::is_json5_whitespace(c) || matches!(c, '[' | ']' | '{' | '}' | ',' | ':');
    loop {
        *input = input.trim_start_matches(separator);
        if let Some(comment) = input.strip_prefix("//") {
            let end = comment
                .find(json5::char::is_json5_line_terminator)
                .unwrap_or(comment.len());
            *input = &comment[end..];
        } else if let Some(comment) = input.strip_prefix("/*") {
            *input = comment.split_once("*/")?.1;
        } else {
            break;
        }
    }
    let mut chars = input.char_indices();
    let (_, first) = chars.next()?;
    let end = if matches!(first, '\'' | '"') {
        loop {
            let (at, c) = chars.next()?;
            if c == first {
                break at + 1;
            }
            if c == '\\' {
                chars.next()?;
            }
        }
    } else {
        input
            .find(|c| separator(c) || c == '/')
            .unwrap_or(input.len())
    };
    let (token, tail) = input.split_at(end);
    *input = tail;
    Some(token)
}

/// Parse JSON text, accepting JSON5 (unquoted keys, single quotes, trailing
/// commas, // and /* */ comments, hex integers, leading/trailing decimal
/// points) as a fallback when strict RFC-8259 parsing fails. The
/// value-consuming json functions (json_extract, json_type, json_each,
/// json_tree, …) re-serialize/extract from the parsed tree, so JSON5 input is
/// transparently canonicalized — matching stock SQLite 3.42+ (bd-qear2). Strict
/// JSON is unaffected (the strict parse is tried first). JSON5 infinities use
/// the existing non-finite numeric carriers, while NaN becomes JSON null.
fn parse_json_value_lenient(input: &str) -> Result<Value> {
    // Numbers that must keep their text travel as tagged carriers, as in
    // `parse_json_text` (GH#212, bd-yalqc, bd-1nxwh). If the strict parse
    // fails, fall back to JSON5; on total failure report stock's
    // "malformed JSON".
    parse_json_text(input).or_else(|_| parse_json5_text(input).map_err(|_| malformed_json()))
}

fn encode_jsonb_root(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_jsonb_value(value, &mut out)?;
    Ok(out)
}

fn encode_jsonb_value(value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => append_jsonb_node(JSONB_NULL_TYPE, &[], out),
        Value::Bool(true) => append_jsonb_node(JSONB_TRUE_TYPE, &[], out),
        Value::Bool(false) => append_jsonb_node(JSONB_FALSE_TYPE, &[], out),
        Value::IntegerText(digits) => append_jsonb_node(JSONB_INT_TYPE, digits.as_bytes(), out),
        Value::FloatText(raw) => append_jsonb_node(JSONB_FLOAT_TYPE, raw.as_bytes(), out),
        Value::HexInteger(integer) => {
            append_jsonb_node(JSONB_INT5_TYPE, integer.token.as_bytes(), out)
        }
        Value::Number(number) => {
            // bd-t75hg: SQLite JSONB stores numeric payloads as the ASCII
            // text of the number (RFC-8259 text for INT/FLOAT), never as
            // fixed-width binary — a stock reader decodes binary payloads
            // as garbage digits.
            if number.is_i64() || number.is_u64() {
                append_jsonb_node(JSONB_INT_TYPE, number.to_string().as_bytes(), out)
            } else if let Some(float) = number.as_f64().filter(|value| value.is_finite()) {
                append_jsonb_node(JSONB_FLOAT_TYPE, format!("{float:?}").as_bytes(), out)
            } else {
                append_jsonb_node(JSONB_FLOAT_TYPE, number.to_string().as_bytes(), out)
            }
        }
        Value::String(text) => append_jsonb_string(text, out),
        Value::Array(array) => {
            let mut payload = Vec::new();
            for item in array {
                encode_jsonb_value(item, &mut payload)?;
            }
            append_jsonb_node(JSONB_ARRAY_TYPE, &payload, out)
        }
        Value::Object(object) => {
            let mut payload = Vec::new();
            for (key, item) in object {
                append_jsonb_string(key, &mut payload)?;
                encode_jsonb_value(item, &mut payload)?;
            }
            append_jsonb_node(JSONB_OBJECT_TYPE, &payload, out)
        }
    }
}

/// Encode a string (value or object key) the way stock SQLite does: plain
/// `TEXT` (0x7) when the payload needs no JSON escaping, otherwise `TEXTJ`
/// (0x8) with the payload carrying RFC-8259 escape sequences. Emitting raw
/// bytes under `TEXT` for a string containing quotes/control characters
/// would make stock's `json()` render invalid JSON (bd-t75hg).
fn append_jsonb_string(text: &str, out: &mut Vec<u8>) -> Result<()> {
    if text
        .bytes()
        .all(|byte| byte != b'"' && byte != b'\\' && byte >= 0x20)
    {
        return append_jsonb_node(JSONB_TEXT_TYPE, text.as_bytes(), out);
    }
    let mut escaped = String::with_capacity(text.len() + 8);
    for ch in text.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\u{0008}' => escaped.push_str("\\b"),
            '\u{000C}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => escaped.push(other),
        }
    }
    append_jsonb_node(JSONB_TEXT_JSON_TYPE, escaped.as_bytes(), out)
}

fn append_jsonb_node(node_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    // SQLite JSONB header: lower 4 bits = element type; upper 4 bits encode
    // the payload size DIRECTLY for sizes 0..=11, and the values 12/13/14/15
    // mean the size follows in 1/2/4/8 big-endian bytes (bd-t75hg — the
    // previous scheme wrote a length byte for every non-empty payload, which
    // stock misparses as payload content).
    let len = payload.len();
    if len <= 11 {
        let header = (u8::try_from(len).expect("len <= 11 fits in u8") << 4) | node_type;
        out.push(header);
    } else if let Ok(len8) = u8::try_from(len) {
        out.push((12 << 4) | node_type);
        out.push(len8);
    } else if let Ok(len16) = u16::try_from(len) {
        out.push((13 << 4) | node_type);
        out.extend_from_slice(&len16.to_be_bytes());
    } else if let Ok(len32) = u32::try_from(len) {
        out.push((14 << 4) | node_type);
        out.extend_from_slice(&len32.to_be_bytes());
    } else {
        let len64 = u64::try_from(len).map_err(|error| {
            FrankenError::function_error(format!("jsonb payload too large: {error}"))
        })?;
        out.push((15 << 4) | node_type);
        out.extend_from_slice(&len64.to_be_bytes());
    }
    out.extend_from_slice(payload);
    Ok(())
}

fn decode_jsonb_root(input: &[u8]) -> Result<Value> {
    let (value, consumed) = decode_jsonb_value(input)?;
    if consumed != input.len() {
        return Err(FrankenError::function_error(
            "invalid JSONB: trailing bytes",
        ));
    }
    Ok(value)
}

fn decode_jsonb_value(input: &[u8]) -> Result<(Value, usize)> {
    let (node_type, payload, consumed) = decode_jsonb_node(input)?;
    let value = match node_type {
        JSONB_NULL_TYPE => {
            if !payload.is_empty() {
                return Err(FrankenError::function_error("invalid JSONB null payload"));
            }
            Value::Null
        }
        JSONB_TRUE_TYPE => {
            if !payload.is_empty() {
                return Err(FrankenError::function_error("invalid JSONB true payload"));
            }
            Value::Bool(true)
        }
        JSONB_FALSE_TYPE => {
            if !payload.is_empty() {
                return Err(FrankenError::function_error("invalid JSONB false payload"));
            }
            Value::Bool(false)
        }
        JSONB_INT_TYPE | JSONB_INT5_TYPE => {
            // bd-t75hg: numeric payloads are the ASCII text of the number.
            // INT5 additionally admits JSON5 forms (hex, leading '+').
            let text = std::str::from_utf8(payload).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB integer payload: {error}"))
            })?;
            let normalized = if node_type == JSONB_INT5_TYPE {
                text.strip_prefix('+').unwrap_or(text)
            } else {
                text
            };
            if integer_needs_text(normalized) {
                return Ok((Value::IntegerText(normalized.to_owned()), consumed));
            }
            if node_type == JSONB_INT5_TYPE
                && let Some(integer) = HexInteger::parse_value(normalized)
            {
                return Ok((integer, consumed));
            }
            let number = if let Ok(int) = normalized.parse::<i64>() {
                Some(Number::from(int))
            } else if let Ok(uint) = normalized.parse::<u64>() {
                Some(Number::from(uint))
            } else {
                // Preserve the existing numeric fallback for non-decimal INT5
                // spellings. Valid wide decimal integers were handled above.
                normalized.parse::<f64>().ok().and_then(Number::from_f64)
            };
            Value::Number(number.ok_or_else(|| {
                FrankenError::function_error("invalid JSONB integer payload text")
            })?)
        }
        JSONB_FLOAT_TYPE | JSONB_FLOAT5_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB float payload: {error}"))
            })?;
            if node_type == JSONB_FLOAT_TYPE {
                // bd-1nxwh / bd-oahj7: a strict JSONB FLOAT payload stores its
                // exact ASCII numeric text. Keep it directly as `FloatText`,
                // matching `parse_json_text` so wire round trips preserve every
                // digit and exponent spelling without formatting overhead.
                if !is_strict_json_number(text) && text.parse::<f64>().is_err() {
                    return Err(FrankenError::function_error(
                        "invalid JSONB float payload text",
                    ));
                }
                number_text_value(text)
            } else {
                let normalized = text.strip_prefix('+').unwrap_or(text);
                let float = normalized.parse::<f64>().map_err(|error| {
                    FrankenError::function_error(format!(
                        "invalid JSONB float payload text: {error}"
                    ))
                })?;
                if float.is_finite() {
                    Value::Number(Number::from_f64(float).ok_or_else(|| {
                        FrankenError::function_error("invalid JSONB float payload")
                    })?)
                } else {
                    number_text_value(normalized)
                }
            }
        }
        JSONB_TEXT_TYPE | JSONB_TEXTRAW_TYPE | JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE => {
            json_string_value(decode_jsonb_text(node_type, payload)?)
        }
        JSONB_ARRAY_TYPE => {
            let mut cursor = 0usize;
            let mut values = Vec::new();
            while cursor < payload.len() {
                let (item, used) = decode_jsonb_value(&payload[cursor..])?;
                values.push(item);
                cursor += used;
            }
            Value::Array(values)
        }
        JSONB_OBJECT_TYPE => {
            let mut cursor = 0usize;
            let mut map = Object::new();
            while cursor < payload.len() {
                let (key_type, key_payload, key_used) = decode_jsonb_node(&payload[cursor..])?;
                cursor += key_used;
                // Labels are raw strings, not escaped string values. Decode
                // their wire type directly: a numeric carrier must never
                // make a non-text key look like a valid Value::String.
                let key = decode_jsonb_text(key_type, key_payload)?;
                if cursor >= payload.len() {
                    return Err(FrankenError::function_error(
                        "invalid JSONB object missing value",
                    ));
                }
                let (item, used) = decode_jsonb_value(&payload[cursor..])?;
                cursor += used;
                map.append(key, item);
            }
            Value::Object(map)
        }
        _ => {
            return Err(FrankenError::function_error("invalid JSONB node type"));
        }
    };

    Ok((value, consumed))
}

/// Decode raw text for either a JSONB string value or an object label. Only
/// values subsequently enter the escaped internal string representation.
fn decode_jsonb_text(node_type: u8, payload: &[u8]) -> Result<String> {
    match node_type {
        JSONB_TEXT_TYPE | JSONB_TEXTRAW_TYPE => {
            String::from_utf8(payload.to_vec()).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB text payload: {error}"))
            })
        }
        JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE => {
            // TEXTJ carries RFC-8259 escapes; TEXT5 adds JSON5 escapes.
            let text = std::str::from_utf8(payload).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB text payload: {error}"))
            })?;
            decode_jsonb_escaped_text(text, node_type == JSONB_TEXT5_TYPE)
        }
        _ => Err(FrankenError::function_error(
            "invalid JSONB object key payload",
        )),
    }
}

/// Resolve TEXTJ/TEXT5 escape sequences to the raw string. TEXT5 adds the
/// JSON5 forms (`\'`, `\v`, `\0`, `\xNN`, escaped line continuations) on top
/// of the RFC-8259 set.
fn decode_jsonb_escaped_text(text: &str, json5: bool) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escape) = chars.next() else {
            return Err(FrankenError::function_error(
                "invalid JSONB text escape: trailing backslash",
            ));
        };
        match escape {
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            '/' => out.push('/'),
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000C}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'u' => {
                let code: String = chars.by_ref().take(4).collect();
                if code.len() != 4 {
                    return Err(FrankenError::function_error(
                        "invalid JSONB \\u escape: truncated",
                    ));
                }
                let unit = u32::from_str_radix(&code, 16).map_err(|error| {
                    FrankenError::function_error(format!("invalid JSONB \\u escape: {error}"))
                })?;
                if (0xD800..=0xDBFF).contains(&unit) {
                    let mut look = chars.clone();
                    if look.next() == Some('\\') && look.next() == Some('u') {
                        let low_code: String = look.by_ref().take(4).collect();
                        if low_code.len() == 4
                            && let Ok(low) = u32::from_str_radix(&low_code, 16)
                            && (0xDC00..=0xDFFF).contains(&low)
                        {
                            chars = look;
                            let combined = 0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
                            out.push(char::from_u32(combined).unwrap_or('\u{FFFD}'));
                            continue;
                        }
                    }
                }
                // Lone surrogates are replaced, matching lossy JSON readers.
                out.push(char::from_u32(unit).unwrap_or('\u{FFFD}'));
            }
            '\'' if json5 => out.push('\''),
            'v' if json5 => out.push('\u{000B}'),
            '0' if json5 => out.push('\u{0000}'),
            'x' if json5 => {
                let code: String = chars.by_ref().take(2).collect();
                let unit = u8::from_str_radix(&code, 16).map_err(|error| {
                    FrankenError::function_error(format!("invalid JSONB \\x escape: {error}"))
                })?;
                out.push(char::from(unit));
            }
            '\n' | '\u{2028}' | '\u{2029}' if json5 => {}
            '\r' if json5 => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
            }
            other if json5 => out.push(other),
            other => {
                return Err(FrankenError::function_error(format!(
                    "invalid JSONB text escape: \\{other}"
                )));
            }
        }
    }
    Ok(out)
}

fn decode_jsonb_node(input: &[u8]) -> Result<(u8, &[u8], usize)> {
    if input.is_empty() {
        return Err(FrankenError::function_error("invalid JSONB: empty payload"));
    }

    let header = input[0];
    // SQLite JSONB header: lower 4 bits = element type; upper 4 bits are the
    // payload size itself for 0..=11, or 12/13/14/15 meaning the size follows
    // in 1/2/4/8 big-endian bytes (bd-t75hg).
    let node_type = header & 0x0f;
    let size_nibble = usize::from(header >> 4);
    let (len_size, direct_len) = match size_nibble {
        0..=11 => (0usize, Some(size_nibble)),
        12 => (1, None),
        13 => (2, None),
        14 => (4, None),
        _ => (8, None),
    };
    if !matches!(
        node_type,
        JSONB_NULL_TYPE
            | JSONB_TRUE_TYPE
            | JSONB_FALSE_TYPE
            | JSONB_INT_TYPE
            | JSONB_INT5_TYPE
            | JSONB_FLOAT_TYPE
            | JSONB_FLOAT5_TYPE
            | JSONB_TEXT_TYPE
            | JSONB_TEXT_JSON_TYPE
            | JSONB_TEXT5_TYPE
            | JSONB_TEXTRAW_TYPE
            | JSONB_ARRAY_TYPE
            | JSONB_OBJECT_TYPE
    ) {
        return Err(FrankenError::function_error("invalid JSONB node type"));
    }

    if input.len() < 1 + len_size {
        return Err(FrankenError::function_error(
            "invalid JSONB: truncated payload length",
        ));
    }

    let len_end = 1 + len_size;
    let payload_len = if let Some(direct) = direct_len {
        direct
    } else {
        decode_jsonb_payload_len(&input[1..len_end])?
    };
    let total = 1 + len_size + payload_len;
    if input.len() < total {
        return Err(FrankenError::function_error(
            "invalid JSONB: truncated payload",
        ));
    }

    Ok((node_type, &input[1 + len_size..total], total))
}

fn decode_jsonb_payload_len(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    if !matches!(bytes.len(), 1 | 2 | 4 | 8) {
        return Err(FrankenError::function_error(
            "invalid JSONB length encoding size",
        ));
    }

    let mut raw = [0u8; 8];
    raw[8 - bytes.len()..].copy_from_slice(bytes);
    let payload_len = u64::from_be_bytes(raw);
    usize::try_from(payload_len).map_err(|error| {
        FrankenError::function_error(format!("JSONB payload length overflow: {error}"))
    })
}

/// Stock SQLite's `JSON_MAX_DEPTH` for JSONB path traversal and edits.
const JSONB_EDIT_MAX_DEPTH: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonbEditMode {
    Delete,
    Replace,
    Insert,
    Set,
    ArrayInsert,
}

impl JsonbEditMode {
    const fn creates_missing(self) -> bool {
        matches!(self, Self::Insert | Self::Set | Self::ArrayInsert)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonbLookupOutcome {
    Matched,
    NotFound,
}

/// Decode a JSONB element header without checking `header_len + payload_len <= bytes.len()`.
///
/// Needed by `JsonbBlobEditor::after_edit_size_adjust`, where a child edit has
/// already resized the blob while the parent's header still holds its previous
/// payload size.
fn decode_jsonb_header(bytes: &[u8]) -> Option<(u8, usize, usize)> {
    let header = *bytes.first()?;
    let node_type = header & 0x0f;
    if node_type > JSONB_OBJECT_TYPE {
        return None;
    }
    let size_nibble = usize::from(header >> 4);
    let (extra, payload_len) = match size_nibble {
        0..=11 => (0, size_nibble),
        12 => (1, usize::from(*bytes.get(1)?)),
        13 => {
            let b: [u8; 2] = bytes.get(1..3)?.try_into().ok()?;
            (2, usize::from(u16::from_be_bytes(b)))
        }
        14 => {
            let b: [u8; 4] = bytes.get(1..5)?.try_into().ok()?;
            (4, usize::try_from(u32::from_be_bytes(b)).ok()?)
        }
        _ => {
            let b: [u8; 8] = bytes.get(1..9)?.try_into().ok()?;
            (8, usize::try_from(u64::from_be_bytes(b)).ok()?)
        }
    };
    Some((node_type, 1 + extra, payload_len))
}

/// Decode a JSONB element header at `offset` and verify that the entire node
/// lies within `blob`.
fn jsonb_node_bounds(blob: &[u8], offset: usize) -> Option<(u8, usize, usize)> {
    let slice = blob.get(offset..)?;
    let (node_type, hdr_len, payload_len) = decode_jsonb_header(slice)?;
    let total = hdr_len.checked_add(payload_len)?;
    if total > slice.len() {
        return None;
    }
    Some((node_type, hdr_len, payload_len))
}

/// Exact safe-Rust equivalent of SQLite's `jsonBlobOverwrite`: when replacing
/// an existing JSONB node with a shorter node (`1 <= expand_by <= 8`), expand
/// the replacement's header size field so the overwrite keeps the blob's exact
/// byte length unchanged.
fn jsonb_overwrite(out: &mut [u8], ins: &[u8], expand_by: usize) -> bool {
    let Some(&first) = ins.first() else {
        return false;
    };
    if (first & 0x0f) <= JSONB_FALSE_TYPE {
        return false;
    }
    let (sz_hdr, new_hdr) = match first >> 4 {
        0..=11 => match expand_by {
            1 | 2 | 4 | 8 => (1usize, expand_by + 1),
            _ => return false,
        },
        12 => match expand_by {
            1 | 3 | 7 => (2usize, expand_by + 2),
            _ => return false,
        },
        13 => match expand_by {
            2 | 6 => (3usize, expand_by + 3),
            _ => return false,
        },
        14 => match expand_by {
            4 => (5usize, 9usize),
            _ => return false,
        },
        _ => return false,
    };
    let size_bits = match new_hdr {
        2 => 0xc0_u8,
        3 => 0xd0_u8,
        5 => 0xe0_u8,
        9 => 0xf0_u8,
        _ => return false,
    };
    let Some(payload) = ins.get(sz_hdr..) else {
        return false;
    };
    if out.len() != new_hdr + payload.len() {
        return false;
    }
    out[0] = (first & 0x0f) | size_bits;
    let mut sz_payload = u64::try_from(payload.len()).unwrap_or(0);
    for idx in (1..new_hdr).rev() {
        out[idx] = u8::try_from(sz_payload & 0xff).expect("masked byte fits in u8");
        sz_payload >>= 8;
    }
    out[new_hdr..].copy_from_slice(payload);
    true
}

/// Update the encoded payload size of the JSONB node at `offset`, returning
/// the signed byte-length delta of the header itself (`sqlite3.c`:
/// `jsonBlobChangePayloadSize`).
fn jsonb_change_payload_size(blob: &mut Vec<u8>, offset: usize, new_payload_len: usize) -> isize {
    let header = blob[offset];
    let node_type = header & 0x0f;
    let n_extra: usize = match header >> 4 {
        0..=11 => 0,
        12 => 1,
        13 => 2,
        14 => 4,
        _ => 8,
    };
    let n_needed: usize = if new_payload_len <= 11 {
        0
    } else if new_payload_len <= 0xff {
        1
    } else if new_payload_len <= 0xffff {
        2
    } else if u32::try_from(new_payload_len).is_ok() {
        4
    } else {
        8
    };
    let insert_at = offset + 1;
    if n_needed > n_extra {
        let grow = n_needed - n_extra;
        blob.splice(insert_at..insert_at, std::iter::repeat_n(0_u8, grow));
    } else if n_needed < n_extra {
        let shrink = n_extra - n_needed;
        blob.drain(insert_at..insert_at + shrink);
    }
    match n_needed {
        0 => {
            let nibble = u8::try_from(new_payload_len).expect("<= 11 fits in u8");
            blob[offset] = node_type | (nibble << 4);
        }
        1 => {
            blob[offset] = node_type | 0xc0;
            blob[offset + 1] = u8::try_from(new_payload_len).expect("<= 0xff fits in u8");
        }
        2 => {
            blob[offset] = node_type | 0xd0;
            let bytes = u16::try_from(new_payload_len)
                .expect("<= 0xffff fits in u16")
                .to_be_bytes();
            blob[offset + 1..offset + 3].copy_from_slice(&bytes);
        }
        4 => {
            blob[offset] = node_type | 0xe0;
            let bytes = u32::try_from(new_payload_len)
                .expect("fits in u32")
                .to_be_bytes();
            blob[offset + 1..offset + 5].copy_from_slice(&bytes);
        }
        _ => {
            blob[offset] = node_type | 0xf0;
            let bytes = u64::try_from(new_payload_len)
                .expect("fits in u64")
                .to_be_bytes();
            blob[offset + 1..offset + 9].copy_from_slice(&bytes);
        }
    }
    isize::try_from(n_needed).expect("header extra bytes fit in isize")
        - isize::try_from(n_extra).expect("header extra bytes fit in isize")
}

/// Count elements in the JSONB array at `i_root` (`sqlite3.c`: `jsonbArrayCount`).
fn jsonb_array_count(blob: &[u8], i_root: usize) -> Option<u64> {
    let (_, hdr_len, payload_len) = jsonb_node_bounds(blob, i_root)?;
    let mut pos = i_root + hdr_len;
    let end = pos + payload_len;
    let mut count = 0_u64;
    while pos < end {
        let (_, child_hdr, child_payload) = jsonb_node_bounds(blob, pos)?;
        pos = pos.checked_add(child_hdr)?.checked_add(child_payload)?;
        count += 1;
    }
    if pos == end { Some(count) } else { None }
}

/// Compare a path key against an object label in a JSONB blob (`sqlite3.c`:
/// `jsonLabelCompare`).
fn jsonb_label_matches(
    key_bytes: &[u8],
    raw_key: bool,
    label_type: u8,
    label_payload: &[u8],
) -> Result<bool> {
    let raw_label = matches!(label_type, JSONB_TEXT_TYPE | JSONB_TEXTRAW_TYPE);
    if raw_key && raw_label {
        return Ok(key_bytes == label_payload);
    }
    let left_str = std::str::from_utf8(key_bytes).map_err(|_| malformed_json())?;
    let left = if raw_key {
        Cow::Borrowed(left_str)
    } else {
        Cow::Owned(decode_jsonb_escaped_text(left_str, true)?)
    };
    let right = decode_jsonb_text(label_type, label_payload)?;
    Ok(left.as_ref() == right.as_str())
}

/// Parse one `.key` or `."quoted"` path segment the way SQLite's
/// `jsonLookupStep` does (`sqlite3.c:216959-216983`), returning
/// `(key_bytes, raw_key, remaining_tail)`.
fn jsonb_parse_path_object_key<'a>(
    tail: &'a str,
    full_path: &str,
) -> Result<(&'a [u8], bool, &'a str)> {
    let bytes = tail.as_bytes();
    debug_assert_eq!(bytes.first(), Some(&b'.'));
    if bytes.get(1) == Some(&b'"') {
        let mut idx = 2usize;
        while idx < bytes.len() && bytes[idx] != b'"' {
            if bytes[idx] == b'\\' && idx + 1 < bytes.len() {
                idx += 2;
            } else {
                idx += 1;
            }
        }
        if idx >= bytes.len() {
            return Err(bad_json_path(full_path));
        }
        let key_bytes = &bytes[2..idx];
        let raw_key = !key_bytes.contains(&b'\\');
        Ok((key_bytes, raw_key, &tail[idx + 1..]))
    } else {
        let mut idx = 1usize;
        while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
            idx += 1;
        }
        if idx == 1 {
            return Err(bad_json_path(full_path));
        }
        Ok((&bytes[1..idx], true, &tail[idx..]))
    }
}

/// Parse one `[N]`, `[#]`, or `[#-N]` path segment the way SQLite's
/// `jsonLookupStep` does (`sqlite3.c:217056-217090`). Returns `Ok(None)` when
/// `#-N` reaches before index 0 (`JSON_LOOKUP_NOTFOUND`).
fn jsonb_parse_path_array_index<'a>(
    tail: &'a str,
    array_count: u64,
    full_path: &str,
) -> Result<Option<(u64, &'a str)>> {
    let bytes = tail.as_bytes();
    debug_assert_eq!(bytes.first(), Some(&b'['));
    let mut idx = 1usize;
    let mut kk = 0_u64;
    while let Some(&b) = bytes.get(idx) {
        if !b.is_ascii_digit() {
            break;
        }
        if kk < 0xffff_ffff {
            kk = kk * 10 + u64::from(b - b'0');
        }
        idx += 1;
    }
    if idx >= 2 && bytes.get(idx) == Some(&b']') {
        return Ok(Some((kk, &tail[idx + 1..])));
    }
    if bytes.get(1) == Some(&b'#') {
        kk = array_count;
        idx = 2;
        if bytes.get(2) == Some(&b'-') && bytes.get(3).is_some_and(u8::is_ascii_digit) {
            let mut nn = 0_u64;
            idx = 3;
            while let Some(&b) = bytes.get(idx) {
                if !b.is_ascii_digit() {
                    break;
                }
                if nn < 0xffff_ffff {
                    nn = nn * 10 + u64::from(b - b'0');
                }
                idx += 1;
            }
            if nn > kk {
                return Ok(None);
            }
            kk -= nn;
        }
        if bytes.get(idx) != Some(&b']') {
            return Err(bad_json_path(full_path));
        }
        return Ok(Some((kk, &tail[idx + 1..])));
    }
    Err(bad_json_path(full_path))
}

/// In-place JSONB blob editor mirroring SQLite 3.54.1's `jsonInsertIntoBlob` /
/// `jsonLookupStep` / `jsonBlobEdit` (`bd-b0i0o`).
struct JsonbBlobEditor {
    blob: Vec<u8>,
    delta: isize,
    depth: usize,
}

impl JsonbBlobEditor {
    const fn new(blob: Vec<u8>) -> Self {
        Self {
            blob,
            delta: 0,
            depth: 0,
        }
    }

    fn blob_edit(&mut self, i_del: usize, n_del: usize, ins: &[u8]) {
        let d = isize::try_from(ins.len()).expect("insert len fits in isize")
            - isize::try_from(n_del).expect("delete len fits in isize");
        if (-8..0).contains(&d)
            && !ins.is_empty()
            && let Ok(expand_by) = usize::try_from(-d)
            && jsonb_overwrite(&mut self.blob[i_del..i_del + n_del], ins, expand_by)
        {
            return;
        }
        self.blob.splice(i_del..i_del + n_del, ins.iter().copied());
        self.delta += d;
    }

    fn after_edit_size_adjust(&mut self, i_root: usize) -> Result<()> {
        let (_, _, old_sz) =
            decode_jsonb_header(&self.blob[i_root..]).ok_or_else(malformed_json)?;
        let adjusted = isize::try_from(old_sz)
            .ok()
            .and_then(|sz| sz.checked_add(self.delta))
            .and_then(|sz| usize::try_from(sz).ok())
            .ok_or_else(malformed_json)?;
        let hdr_delta = jsonb_change_payload_size(&mut self.blob, i_root, adjusted);
        self.delta += hdr_delta;
        Ok(())
    }

    fn create_edit_substructure(
        &self,
        mode: JsonbEditMode,
        ins: &[u8],
        tail: &str,
        full_path: &str,
    ) -> Result<Option<Vec<u8>>> {
        if tail.is_empty() {
            return Ok(Some(ins.to_vec()));
        }
        let next_depth = self.depth + 1;
        if next_depth >= JSONB_EDIT_MAX_DEPTH {
            return Err(FrankenError::function_error("JSON path too deep"));
        }
        let empty = if tail.starts_with('.') {
            JSONB_OBJECT_TYPE
        } else {
            JSONB_ARRAY_TYPE
        };
        let mut child = Self {
            blob: vec![empty],
            delta: 0,
            depth: next_depth,
        };
        match child.lookup_step(0, tail, 0, mode, ins, full_path)? {
            JsonbLookupOutcome::Matched => Ok(Some(child.blob)),
            JsonbLookupOutcome::NotFound => Ok(None),
        }
    }

    fn apply_path_edit(&mut self, path: &str, mode: JsonbEditMode, ins: &[u8]) -> Result<()> {
        let Some(tail) = path.strip_prefix('$') else {
            return Err(bad_json_path(path));
        };
        self.delta = 0;
        self.depth = 0;
        if tail.is_empty() {
            if matches!(mode, JsonbEditMode::Set | JsonbEditMode::Replace) {
                let old_len = self.blob.len();
                self.blob_edit(0, old_len, ins);
            }
            return Ok(());
        }
        let _ = self.lookup_step(0, tail, 0, mode, ins, path)?;
        Ok(())
    }

    fn lookup_step(
        &mut self,
        i_root: usize,
        tail: &str,
        i_label: usize,
        mode: JsonbEditMode,
        ins: &[u8],
        full_path: &str,
    ) -> Result<JsonbLookupOutcome> {
        if tail.is_empty() {
            let (_, hdr_len, payload_len) =
                jsonb_node_bounds(&self.blob, i_root).ok_or_else(malformed_json)?;
            let mut del_start = i_root;
            let mut del_len = hdr_len + payload_len;
            match mode {
                JsonbEditMode::Delete => {
                    if i_label > 0 {
                        del_len += i_root - i_label;
                        del_start = i_label;
                    }
                    self.blob_edit(del_start, del_len, &[]);
                }
                JsonbEditMode::Insert => {}
                JsonbEditMode::ArrayInsert => {
                    if !full_path.ends_with(']') {
                        return Err(not_an_array_element(full_path));
                    }
                    self.blob_edit(i_root, 0, ins);
                }
                JsonbEditMode::Set | JsonbEditMode::Replace => {
                    self.blob_edit(del_start, del_len, ins);
                }
            }
            return Ok(JsonbLookupOutcome::Matched);
        }

        if tail.starts_with('.') {
            self.lookup_step_object(i_root, tail, mode, ins, full_path)
        } else if tail.starts_with('[') {
            self.lookup_step_array(i_root, tail, mode, ins, full_path)
        } else {
            Err(bad_json_path(full_path))
        }
    }

    fn lookup_step_object(
        &mut self,
        i_root: usize,
        tail: &str,
        mode: JsonbEditMode,
        ins: &[u8],
        full_path: &str,
    ) -> Result<JsonbLookupOutcome> {
        let (key_bytes, raw_key, rest) = jsonb_parse_path_object_key(tail, full_path)?;
        let (root_type, root_hdr, root_payload) =
            jsonb_node_bounds(&self.blob, i_root).ok_or_else(malformed_json)?;
        if root_type != JSONB_OBJECT_TYPE {
            return Ok(JsonbLookupOutcome::NotFound);
        }
        let mut pos = i_root + root_hdr;
        let end = pos + root_payload;
        while pos < end {
            let (label_type, label_hdr, label_len) =
                jsonb_node_bounds(&self.blob, pos).ok_or_else(malformed_json)?;
            if !matches!(
                label_type,
                JSONB_TEXT_TYPE | JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE | JSONB_TEXTRAW_TYPE
            ) {
                return Err(malformed_json());
            }
            let label_start = pos + label_hdr;
            let val_pos = label_start + label_len;
            if val_pos >= end {
                return Err(malformed_json());
            }
            if jsonb_label_matches(
                key_bytes,
                raw_key,
                label_type,
                &self.blob[label_start..val_pos],
            )? {
                let (_, val_hdr, val_len) =
                    jsonb_node_bounds(&self.blob, val_pos).ok_or_else(malformed_json)?;
                if val_pos + val_hdr + val_len > end {
                    return Err(malformed_json());
                }
                if self.depth + 1 >= JSONB_EDIT_MAX_DEPTH {
                    return Err(FrankenError::function_error("JSON path too deep"));
                }
                self.depth += 1;
                let outcome = self.lookup_step(val_pos, rest, pos, mode, ins, full_path);
                self.depth -= 1;
                let outcome = outcome?;
                if self.delta != 0 {
                    self.after_edit_size_adjust(i_root)?;
                }
                return Ok(outcome);
            }
            let (_, val_hdr, val_len) =
                jsonb_node_bounds(&self.blob, val_pos).ok_or_else(malformed_json)?;
            pos = val_pos + val_hdr + val_len;
        }
        if pos > end {
            return Err(malformed_json());
        }
        if mode.creates_missing() {
            if mode == JsonbEditMode::ArrayInsert && !rest.ends_with(']') {
                return Err(not_an_array_element(full_path));
            }
            let Some(sub_blob) = self.create_edit_substructure(mode, ins, rest, full_path)? else {
                return Ok(JsonbLookupOutcome::NotFound);
            };
            let label_type = if raw_key {
                JSONB_TEXTRAW_TYPE
            } else {
                JSONB_TEXT5_TYPE
            };
            let mut combined = Vec::with_capacity(key_bytes.len() + sub_blob.len() + 9);
            append_jsonb_node(label_type, key_bytes, &mut combined)?;
            combined.extend_from_slice(&sub_blob);
            self.blob_edit(pos, 0, &combined);
            if self.delta != 0 {
                self.after_edit_size_adjust(i_root)?;
            }
            return Ok(JsonbLookupOutcome::Matched);
        }
        Ok(JsonbLookupOutcome::NotFound)
    }

    fn lookup_step_array(
        &mut self,
        i_root: usize,
        tail: &str,
        mode: JsonbEditMode,
        ins: &[u8],
        full_path: &str,
    ) -> Result<JsonbLookupOutcome> {
        let (root_type, root_hdr, root_payload) =
            jsonb_node_bounds(&self.blob, i_root).ok_or_else(malformed_json)?;
        if root_type != JSONB_ARRAY_TYPE {
            return Ok(JsonbLookupOutcome::NotFound);
        }
        let array_count = jsonb_array_count(&self.blob, i_root).ok_or_else(malformed_json)?;
        let Some((mut kk, rest)) = jsonb_parse_path_array_index(tail, array_count, full_path)?
        else {
            return Ok(JsonbLookupOutcome::NotFound);
        };
        let mut pos = i_root + root_hdr;
        let end = pos + root_payload;
        while pos < end {
            if kk == 0 {
                if self.depth + 1 >= JSONB_EDIT_MAX_DEPTH {
                    return Err(FrankenError::function_error("JSON path too deep"));
                }
                self.depth += 1;
                let outcome = self.lookup_step(pos, rest, 0, mode, ins, full_path);
                self.depth -= 1;
                let outcome = outcome?;
                if self.delta != 0 {
                    self.after_edit_size_adjust(i_root)?;
                }
                return Ok(outcome);
            }
            kk -= 1;
            let (_, child_hdr, child_len) =
                jsonb_node_bounds(&self.blob, pos).ok_or_else(malformed_json)?;
            pos += child_hdr + child_len;
        }
        if pos > end {
            return Err(malformed_json());
        }
        if kk > 0 {
            return Ok(JsonbLookupOutcome::NotFound);
        }
        if mode.creates_missing() {
            let Some(sub_blob) = self.create_edit_substructure(mode, ins, rest, full_path)? else {
                return Ok(JsonbLookupOutcome::NotFound);
            };
            self.blob_edit(pos, 0, &sub_blob);
            if self.delta != 0 {
                self.after_edit_size_adjust(i_root)?;
            }
            return Ok(JsonbLookupOutcome::Matched);
        }
        Ok(JsonbLookupOutcome::NotFound)
    }
}

/// Encode a single SQL argument into JSONB bytes for `jsonb_set` /
/// `jsonb_insert` / `jsonb_replace` / `jsonb_array_insert`, matching SQLite
/// 3.54.1's `jsonFunctionArgToBlob` (`sqlite3.c:217356-217427`).
///
/// Plain SQL `TEXT` (without `JSON_SUBTYPE`) is stored as `JSONB_TEXTRAW`
/// (`0xA`). `JSON_SUBTYPE` text is parsed into standard JSONB (`TEXT`/`TEXTJ`),
/// and valid JSONB `BLOB` values are embedded verbatim.
fn jsonb_value_arg_to_blob(value: &SqliteValue, subtype: u32) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        SqliteValue::Null => {
            append_jsonb_node(JSONB_NULL_TYPE, &[], &mut out)?;
        }
        SqliteValue::Blob(bytes) => {
            if decode_jsonb_root(bytes).is_ok() {
                return Ok(bytes.to_vec());
            }
            if subtype == JSON_SUBTYPE
                && let Ok(text) = std::str::from_utf8(bytes)
            {
                return encode_jsonb_root(&parse_json_text(text)?);
            }
            return Err(FrankenError::function_error("JSON cannot hold BLOB values"));
        }
        SqliteValue::Text(text) => {
            if subtype == JSON_SUBTYPE {
                return encode_jsonb_root(&parse_json_value_lenient(text)?);
            }
            append_jsonb_node(JSONB_TEXTRAW_TYPE, text.as_bytes(), &mut out)?;
        }
        SqliteValue::Integer(i) => {
            append_jsonb_node(JSONB_INT_TYPE, i.to_string().as_bytes(), &mut out)?;
        }
        SqliteValue::Float(f) => {
            if f.is_nan() {
                append_jsonb_node(JSONB_NULL_TYPE, &[], &mut out)?;
            } else if f.is_infinite() {
                let text = if *f > 0.0 {
                    b"9e999".as_slice()
                } else {
                    b"-9e999".as_slice()
                };
                append_jsonb_node(JSONB_FLOAT_TYPE, text, &mut out)?;
            } else {
                let formatted = fsqlite_types::value::format_sqlite_float(*f);
                append_jsonb_node(JSONB_FLOAT_TYPE, formatted.as_bytes(), &mut out)?;
            }
        }
    }
    Ok(out)
}

/// Convert the target document argument (`argv[0]`) of a `jsonb_*` edit
/// function into an editable JSONB byte buffer, preserving existing JSONB
/// blobs verbatim so untouched `TEXTRAW` / `TEXT5` / header encodings survive
/// chained edits.
fn json_arg_to_jsonb_blob(name: &str, args: &[SqliteValue], index: usize) -> Result<Vec<u8>> {
    match args.get(index) {
        Some(SqliteValue::Blob(bytes)) => {
            if decode_jsonb_root(bytes).is_ok() {
                Ok(bytes.to_vec())
            } else {
                let text = std::str::from_utf8(bytes).map_err(|_| malformed_json())?;
                jsonb(text)
            }
        }
        Some(SqliteValue::Text(text)) => jsonb(text),
        Some(SqliteValue::Integer(i)) => {
            let mut out = Vec::new();
            append_jsonb_node(JSONB_INT_TYPE, i.to_string().as_bytes(), &mut out)?;
            Ok(out)
        }
        Some(SqliteValue::Float(f)) => encode_jsonb_root(&float_to_json(*f)?),
        Some(other) => Err(FrankenError::function_error(format!(
            "{name} argument {} must be TEXT or BLOB, got {}",
            index + 1,
            other.typeof_str()
        ))),
        None => Err(FrankenError::function_error(format!(
            "{name} missing argument {}",
            index + 1
        ))),
    }
}

fn edit_jsonb_blob_pairs(
    blob: Vec<u8>,
    pairs: &[(&str, SqliteValue)],
    mode: JsonbEditMode,
) -> Result<Vec<u8>> {
    let mut editor = JsonbBlobEditor::new(blob);
    for (path, value) in pairs {
        let ins = jsonb_value_arg_to_blob(value, 0)?;
        editor.apply_path_edit(path, mode, &ins)?;
    }
    Ok(editor.blob)
}

fn edit_jsonb_blob_with_subtypes(
    name: &str,
    blob: Vec<u8>,
    args: &[SqliteValue],
    arg_subtypes: &[u32],
    start: usize,
    mode: JsonbEditMode,
) -> Result<Vec<u8>> {
    let mut editor = JsonbBlobEditor::new(blob);
    let mut idx = start;
    while idx + 1 < args.len() {
        if args[idx].is_null() {
            idx += 2;
            continue;
        }
        let path = path_arg(name, args, idx)?;
        if !path.starts_with('$') {
            return Err(bad_json_path(path));
        }
        let subtype = arg_subtypes.get(idx + 1).copied().unwrap_or(0);
        let ins = jsonb_value_arg_to_blob(&args[idx + 1], subtype)?;
        editor.apply_path_edit(path, mode, &ins)?;
        idx += 2;
    }
    Ok(editor.blob)
}

fn remove_jsonb_blob_paths(blob: Vec<u8>, paths: &[&str]) -> Result<Option<Vec<u8>>> {
    let mut editor = JsonbBlobEditor::new(blob);
    for path in paths {
        if !path.starts_with('$') {
            return Err(bad_json_path(path));
        }
        if path.len() == 1 {
            return Ok(None);
        }
        editor.apply_path_edit(path, JsonbEditMode::Delete, &[])?;
    }
    Ok(Some(editor.blob))
}

fn is_superficially_valid_jsonb(input: &[u8]) -> bool {
    if input.is_empty() {
        return false;
    }
    let header = input[0];
    // SQLite JSONB header (bd-t75hg): lower 4 bits = element type; upper 4
    // bits are the payload size for 0..=11, or 12/13/14/15 meaning the size
    // follows in 1/2/4/8 big-endian bytes.
    let node_type = header & 0x0f;
    let size_nibble = usize::from(header >> 4);
    let (len_size, direct_len) = match size_nibble {
        0..=11 => (0usize, Some(size_nibble)),
        12 => (1, None),
        13 => (2, None),
        14 => (4, None),
        _ => (8, None),
    };
    if node_type > JSONB_OBJECT_TYPE {
        return false;
    }
    if input.len() < 1 + len_size {
        return false;
    }
    let len_end = 1 + len_size;
    let payload_len = if let Some(direct) = direct_len {
        direct
    } else {
        let Ok(decoded) = decode_jsonb_payload_len(&input[1..len_end]) else {
            return false;
        };
        decoded
    };
    1 + len_size + payload_len == input.len()
}

#[allow(clippy::too_many_lines)]
fn parse_path(path: &str) -> Result<Vec<PathSegment>> {
    let bytes = path.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return Err(bad_json_path(path));
    }

    let mut idx = 1;
    let mut segments = Vec::new();
    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return Err(bad_json_path(path));
                }
                // ... rest of the dot case remains same ...

                if bytes[idx] == b'"' {
                    let quoted_start = idx;
                    idx += 1;
                    let mut escaped = false;
                    while idx < bytes.len() {
                        let byte = bytes[idx];
                        if escaped {
                            escaped = false;
                            idx += 1;
                            continue;
                        }
                        if byte == b'\\' {
                            escaped = true;
                            idx += 1;
                            continue;
                        }
                        if byte == b'"' {
                            break;
                        }
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(bad_json_path(path));
                    }
                    let quoted_key = &path[quoted_start..=idx];
                    let key = serde_json::from_str::<String>(quoted_key)
                        .map_err(|_| bad_json_path(path))?;
                    idx += 1; // closing quote
                    segments.push(PathSegment::Key(SmallText::from_string(key)));
                } else {
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                        idx += 1;
                    }
                    if start == idx {
                        return Err(bad_json_path(path));
                    }
                    segments.push(PathSegment::Key(SmallText::new(&path[start..idx])));
                }
            }
            b'[' => {
                idx += 1;
                let start = idx;
                let mut escaped = false;
                while idx < bytes.len() {
                    let byte = bytes[idx];
                    if escaped {
                        escaped = false;
                        idx += 1;
                        continue;
                    }
                    if byte == b'\\' {
                        escaped = true;
                        idx += 1;
                        continue;
                    }
                    if byte == b']' {
                        break;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(bad_json_path(path));
                }
                let segment_text = &path[start..idx];
                idx += 1;

                if segment_text == "#" {
                    segments.push(PathSegment::Append);
                } else if let Some(rest) = segment_text.strip_prefix("#-") {
                    let from_end = parse_path_index(rest).ok_or_else(|| bad_json_path(path))?;
                    // `#-0` is one past the last element, the same slot as `#`.
                    segments.push(if from_end == 0 {
                        PathSegment::Append
                    } else {
                        PathSegment::FromEnd(from_end)
                    });
                } else {
                    let index =
                        parse_path_index(segment_text).ok_or_else(|| bad_json_path(path))?;
                    segments.push(PathSegment::Index(index));
                }
            }
            _ => {
                return Err(bad_json_path(path));
            }
        }
    }

    Ok(segments)
}

fn parse_quoted_path_key<'a>(path: &'a str, bytes: &[u8], idx: &mut usize) -> Result<Cow<'a, str>> {
    let quoted_start = *idx;
    *idx += 1;
    let mut escaped = false;
    let mut saw_escape = false;

    while *idx < bytes.len() {
        let byte = bytes[*idx];
        if escaped {
            escaped = false;
            *idx += 1;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            saw_escape = true;
            *idx += 1;
            continue;
        }
        if byte == b'"' {
            let raw = &path[(quoted_start + 1)..*idx];
            let key = if saw_escape {
                let quoted_key = &path[quoted_start..=*idx];
                Cow::Owned(
                    serde_json::from_str::<String>(quoted_key).map_err(|_| bad_json_path(path))?,
                )
            } else {
                Cow::Borrowed(raw)
            };
            *idx += 1;
            return Ok(key);
        }
        *idx += 1;
    }

    Err(bad_json_path(path))
}

/// An array subscript read the way stock reads one: ASCII digits only, and a
/// number too large for any array saturates, so it finds nothing instead of
/// being a bad path.
fn parse_path_index(text: &str) -> Option<usize> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(text.bytes().fold(0usize, |index, byte| {
        index
            .saturating_mul(10)
            .saturating_add(usize::from(byte - b'0'))
    }))
}

fn resolve_path<'a>(root: &'a Value, path: &str) -> Result<Option<&'a Value>> {
    let bytes = path.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return Err(bad_json_path(path));
    }

    let mut idx = 1;
    let mut cursor = root;

    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return Err(bad_json_path(path));
                }

                let next = if bytes[idx] == b'"' {
                    let key = parse_quoted_path_key(path, bytes, &mut idx)?;
                    cursor.get(key.as_ref())
                } else {
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                        idx += 1;
                    }
                    if start == idx {
                        return Err(bad_json_path(path));
                    }
                    cursor.get(&path[start..idx])
                };

                let Some(resolved) = next else {
                    return Ok(None);
                };
                cursor = resolved;
            }
            b'[' => {
                idx += 1;
                let start = idx;
                let mut escaped = false;
                while idx < bytes.len() {
                    let byte = bytes[idx];
                    if escaped {
                        escaped = false;
                        idx += 1;
                        continue;
                    }
                    if byte == b'\\' {
                        escaped = true;
                        idx += 1;
                        continue;
                    }
                    if byte == b']' {
                        break;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(bad_json_path(path));
                }
                let segment_text = &path[start..idx];
                idx += 1;

                if segment_text == "#" {
                    return Ok(None);
                }
                if let Some(rest) = segment_text.strip_prefix("#-") {
                    let from_end = parse_path_index(rest).ok_or_else(|| bad_json_path(path))?;
                    // `#-0` is one past the last element, like `#`: nothing there.
                    if from_end == 0 {
                        return Ok(None);
                    }
                    let Some(array) = cursor.as_array() else {
                        return Ok(None);
                    };
                    if from_end > array.len() {
                        return Ok(None);
                    }
                    cursor = &array[array.len() - from_end];
                    continue;
                }

                let index = parse_path_index(segment_text).ok_or_else(|| bad_json_path(path))?;
                let Some(array) = cursor.as_array() else {
                    return Ok(None);
                };
                let Some(next) = array.get(index) else {
                    return Ok(None);
                };
                cursor = next;
            }
            _ => {
                return Err(bad_json_path(path));
            }
        }
    }

    Ok(Some(cursor))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonbTextStep {
    Value(usize),
    Eof,
    EndObject(usize),
    EndArray(usize),
    Comma(usize),
    Colon(usize),
}

const fn is_json_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

const fn is_json_id1(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'$' || b == b'_' || b >= 0x80
}

const fn is_json_id2(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'$' || b == b'_' || b >= 0x80
}

const fn is_json_ok_byte(b: u8) -> bool {
    b >= 0x20 && b != b'"' && b != b'\'' && b != b'\\'
}

fn is_2hex(bytes: &[u8], pos: usize) -> bool {
    bytes
        .get(pos..pos + 2)
        .is_some_and(|s| s[0].is_ascii_hexdigit() && s[1].is_ascii_hexdigit())
}

fn is_4hex(bytes: &[u8], pos: usize) -> bool {
    is_2hex(bytes, pos) && is_2hex(bytes, pos + 2)
}

fn eq_ascii_ignore_case_at(bytes: &[u8], pos: usize, needle: &[u8]) -> bool {
    bytes
        .get(pos..pos + needle.len())
        .is_some_and(|slice| slice.eq_ignore_ascii_case(needle))
}

/// Skip JSON5 whitespace and comments (`sqlite3.c`: `json5Whitespace`).
fn json5_whitespace_len(bytes: &[u8]) -> usize {
    let mut n = 0usize;
    let len = bytes.len();
    while n < len {
        match bytes[n] {
            0x09 | 0x0a | 0x0b | 0x0c | 0x0d | 0x20 => n += 1,
            b'/' => {
                if bytes.get(n + 1) == Some(&b'*') && n + 2 < len {
                    let mut j = n + 3;
                    while j < len && !(bytes[j] == b'/' && bytes[j - 1] == b'*') {
                        j += 1;
                    }
                    if j >= len {
                        break;
                    }
                    n = j + 1;
                } else if bytes.get(n + 1) == Some(&b'/') {
                    let mut j = n + 2;
                    while j < len {
                        let c = bytes[j];
                        if c == b'\n' || c == b'\r' {
                            break;
                        }
                        if c == 0xe2
                            && bytes.get(j + 1) == Some(&0x80)
                            && matches!(bytes.get(j + 2), Some(0xa8 | 0xa9))
                        {
                            j += 2;
                            break;
                        }
                        j += 1;
                    }
                    n = if j < len { j + 1 } else { j };
                } else {
                    break;
                }
            }
            0xc2 if bytes.get(n + 1) == Some(&0xa0) => n += 2,
            0xe1 if bytes.get(n + 1) == Some(&0x9a) && bytes.get(n + 2) == Some(&0x80) => n += 3,
            0xe2 if (bytes.get(n + 1) == Some(&0x80)
                && matches!(bytes.get(n + 2), Some(0x80..=0x8a | 0xa8 | 0xa9 | 0xaf)))
                || (bytes.get(n + 1) == Some(&0x81) && bytes.get(n + 2) == Some(&0x9f)) =>
            {
                n += 3;
            }
            0xe3 if bytes.get(n + 1) == Some(&0x80) && bytes.get(n + 2) == Some(&0x80) => n += 3,
            0xef if bytes.get(n + 1) == Some(&0xbb) && bytes.get(n + 2) == Some(&0xbf) => n += 3,
            _ => break,
        }
    }
    n
}

fn append_jsonb_header_placeholder(
    node_type: u8,
    estimated_payload_len: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    if estimated_payload_len <= 11 {
        let nibble = u8::try_from(estimated_payload_len).expect("<= 11 fits in u8");
        out.push((nibble << 4) | node_type);
    } else if let Ok(len8) = u8::try_from(estimated_payload_len) {
        out.push((12 << 4) | node_type);
        out.push(len8);
    } else if let Ok(len16) = u16::try_from(estimated_payload_len) {
        out.push((13 << 4) | node_type);
        out.extend_from_slice(&len16.to_be_bytes());
    } else if let Ok(len32) = u32::try_from(estimated_payload_len) {
        out.push((14 << 4) | node_type);
        out.extend_from_slice(&len32.to_be_bytes());
    } else {
        let len64 = u64::try_from(estimated_payload_len).map_err(|error| {
            FrankenError::function_error(format!("jsonb payload too large: {error}"))
        })?;
        out.push((15 << 4) | node_type);
        out.extend_from_slice(&len64.to_be_bytes());
    }
    Ok(())
}

/// Lexically translate JSON / JSON5 text directly into stock SQLite 3.54.1
/// JSONB bytes (`sqlite3.c`: `jsonConvertTextToBlob` +
/// `jsonTranslateTextToBlob`).
///
/// Preserves exact raw source token encodings (`TEXT` / `TEXTJ` / `TEXT5`,
/// `INT` / `INT5`, `FLOAT` / `FLOAT5`, minimal container headers), so every
/// JSONB node byte offset (`json_each.id`, `json_tree.id`, `json_tree.parent`)
/// matches SQLite 3.54.1 byte-for-byte.
fn translate_json_text_to_jsonb(input: &str) -> Result<Vec<u8>> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len().saturating_add(8));
    let mut depth = 0usize;
    let JsonbTextStep::Value(mut idx) = translate_json_text_step(bytes, 0, &mut out, &mut depth)?
    else {
        return Err(malformed_json());
    };
    while idx < bytes.len() && is_json_space(bytes[idx]) {
        idx += 1;
    }
    if idx < bytes.len() {
        idx += json5_whitespace_len(&bytes[idx..]);
        if idx < bytes.len() {
            return Err(malformed_json());
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_lines)]
fn translate_json_text_step(
    bytes: &[u8],
    mut i: usize,
    out: &mut Vec<u8>,
    depth: &mut usize,
) -> Result<JsonbTextStep> {
    let len = bytes.len();
    loop {
        let Some(&ch) = bytes.get(i) else {
            return Ok(JsonbTextStep::Eof);
        };
        match ch {
            b'{' => {
                let i_this = out.len();
                append_jsonb_header_placeholder(JSONB_OBJECT_TYPE, len - i, out)?;
                *depth += 1;
                if *depth > STOCK_JSON_MAX_DEPTH {
                    return Err(malformed_json());
                }
                let i_start = out.len();
                let mut j = i + 1;
                loop {
                    let i_blob = out.len();
                    let next_after_key = match translate_json_text_step(bytes, j, out, depth) {
                        Ok(JsonbTextStep::Value(x)) => x,
                        Ok(JsonbTextStep::EndObject(close_idx)) => {
                            j = close_idx;
                            break;
                        }
                        Ok(_) => return Err(malformed_json()),
                        Err(_) => {
                            out.truncate(i_blob);
                            j += json5_whitespace_len(&bytes[j..]);
                            let mut op = JSONB_TEXT_TYPE;
                            let Some(&first) = bytes.get(j) else {
                                return Err(malformed_json());
                            };
                            let starts_key = if is_json_id1(first) {
                                true
                            } else if first == b'\\'
                                && bytes.get(j + 1) == Some(&b'u')
                                && is_4hex(bytes, j + 2)
                            {
                                op = JSONB_TEXT_JSON_TYPE;
                                true
                            } else {
                                false
                            };
                            if !starts_key {
                                return Err(malformed_json());
                            }
                            let mut k = j + 1;
                            while k < len {
                                let b = bytes[k];
                                if is_json_id2(b) && json5_whitespace_len(&bytes[k..]) == 0 {
                                    k += 1;
                                } else if b == b'\\'
                                    && bytes.get(k + 1) == Some(&b'u')
                                    && is_4hex(bytes, k + 2)
                                {
                                    op = JSONB_TEXT_JSON_TYPE;
                                    k += 1;
                                } else {
                                    break;
                                }
                            }
                            append_jsonb_node(op, &bytes[j..k], out)?;
                            k
                        }
                    };
                    let key_type = out.get(i_blob).copied().unwrap_or(0) & 0x0f;
                    if !(JSONB_TEXT_TYPE..=JSONB_TEXTRAW_TYPE).contains(&key_type) {
                        return Err(malformed_json());
                    }
                    j = next_after_key;
                    if bytes.get(j) == Some(&b':') {
                        j += 1;
                    } else {
                        while bytes.get(j).is_some_and(|&b| is_json_space(b)) {
                            j += 1;
                        }
                        if bytes.get(j) == Some(&b':') {
                            j += 1;
                        } else {
                            let JsonbTextStep::Colon(colon_idx) =
                                translate_json_text_step(bytes, j, out, depth)?
                            else {
                                return Err(malformed_json());
                            };
                            j = colon_idx + 1;
                        }
                    }
                    let JsonbTextStep::Value(after_val) =
                        translate_json_text_step(bytes, j, out, depth)?
                    else {
                        return Err(malformed_json());
                    };
                    j = after_val;
                    if bytes.get(j) == Some(&b',') {
                        j += 1;
                        continue;
                    }
                    if bytes.get(j) == Some(&b'}') {
                        break;
                    }
                    while bytes.get(j).is_some_and(|&b| is_json_space(b)) {
                        j += 1;
                    }
                    if bytes.get(j) == Some(&b',') {
                        j += 1;
                        continue;
                    }
                    if bytes.get(j) == Some(&b'}') {
                        break;
                    }
                    match translate_json_text_step(bytes, j, out, depth)? {
                        JsonbTextStep::Comma(comma_idx) => {
                            j = comma_idx + 1;
                        }
                        JsonbTextStep::EndObject(close_idx) => {
                            j = close_idx;
                            break;
                        }
                        _ => return Err(malformed_json()),
                    }
                }
                let payload_len = out.len() - i_start;
                let _ = jsonb_change_payload_size(out, i_this, payload_len);
                *depth -= 1;
                return Ok(JsonbTextStep::Value(j + 1));
            }
            b'[' => {
                let i_this = out.len();
                append_jsonb_header_placeholder(JSONB_ARRAY_TYPE, len - i, out)?;
                *depth += 1;
                if *depth > STOCK_JSON_MAX_DEPTH {
                    return Err(malformed_json());
                }
                let i_start = out.len();
                let mut j = i + 1;
                loop {
                    match translate_json_text_step(bytes, j, out, depth)? {
                        JsonbTextStep::Value(after_val) => j = after_val,
                        JsonbTextStep::EndArray(close_idx) => {
                            j = close_idx;
                            break;
                        }
                        _ => return Err(malformed_json()),
                    }
                    if bytes.get(j) == Some(&b',') {
                        j += 1;
                        continue;
                    }
                    if bytes.get(j) == Some(&b']') {
                        break;
                    }
                    while bytes.get(j).is_some_and(|&b| is_json_space(b)) {
                        j += 1;
                    }
                    if bytes.get(j) == Some(&b',') {
                        j += 1;
                        continue;
                    }
                    if bytes.get(j) == Some(&b']') {
                        break;
                    }
                    match translate_json_text_step(bytes, j, out, depth)? {
                        JsonbTextStep::Comma(comma_idx) => {
                            j = comma_idx + 1;
                        }
                        JsonbTextStep::EndArray(close_idx) => {
                            j = close_idx;
                            break;
                        }
                        _ => return Err(malformed_json()),
                    }
                }
                let payload_len = out.len() - i_start;
                let _ = jsonb_change_payload_size(out, i_this, payload_len);
                *depth -= 1;
                return Ok(JsonbTextStep::Value(j + 1));
            }
            b'"' | b'\'' => {
                let delim = ch;
                let mut opcode = JSONB_TEXT_TYPE;
                let mut j = i + 1;
                loop {
                    while j < len && is_json_ok_byte(bytes[j]) {
                        j += 1;
                    }
                    let Some(&c) = bytes.get(j) else {
                        return Err(malformed_json());
                    };
                    if c == delim {
                        break;
                    }
                    if c == b'\\' {
                        j += 1;
                        let Some(&esc) = bytes.get(j) else {
                            return Err(malformed_json());
                        };
                        if matches!(esc, b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't')
                            || (esc == b'u' && is_4hex(bytes, j + 1))
                        {
                            if opcode == JSONB_TEXT_TYPE {
                                opcode = JSONB_TEXT_JSON_TYPE;
                            }
                        } else if matches!(esc, b'\'' | b'v' | b'\n')
                            || (esc == b'0' && !bytes.get(j + 1).is_some_and(u8::is_ascii_digit))
                            || (esc == 0xe2
                                && bytes.get(j + 1) == Some(&0x80)
                                && matches!(bytes.get(j + 2), Some(0xa8 | 0xa9)))
                            || (esc == b'x' && is_2hex(bytes, j + 1))
                        {
                            opcode = JSONB_TEXT5_TYPE;
                        } else if esc == b'\r' {
                            if bytes.get(j + 1) == Some(&b'\n') {
                                j += 1;
                            }
                            opcode = JSONB_TEXT5_TYPE;
                        } else {
                            return Err(malformed_json());
                        }
                    } else if c <= 0x1f {
                        if c == 0 {
                            return Err(malformed_json());
                        }
                        opcode = JSONB_TEXT5_TYPE;
                    } else if c == b'"' {
                        opcode = JSONB_TEXT5_TYPE;
                    }
                    j += 1;
                }
                append_jsonb_node(opcode, &bytes[i + 1..j], out)?;
                return Ok(JsonbTextStep::Value(j + 1));
            }
            b't' => {
                if bytes.get(i..i + 4) == Some(b"true")
                    && !bytes.get(i + 4).is_some_and(u8::is_ascii_alphanumeric)
                {
                    out.push(JSONB_TRUE_TYPE);
                    return Ok(JsonbTextStep::Value(i + 4));
                }
                return Err(malformed_json());
            }
            b'f' => {
                if bytes.get(i..i + 5) == Some(b"false")
                    && !bytes.get(i + 5).is_some_and(u8::is_ascii_alphanumeric)
                {
                    out.push(JSONB_FALSE_TYPE);
                    return Ok(JsonbTextStep::Value(i + 5));
                }
                return Err(malformed_json());
            }
            b'+' | b'-' | b'.' | b'0'..=b'9' => {
                return translate_json_number_to_jsonb(bytes, i, out);
            }
            b'}' => return Ok(JsonbTextStep::EndObject(i)),
            b']' => return Ok(JsonbTextStep::EndArray(i)),
            b',' => return Ok(JsonbTextStep::Comma(i)),
            b':' => return Ok(JsonbTextStep::Colon(i)),
            0x09 | 0x0a | 0x0d | 0x20 => {
                i += 1;
                while i < len && is_json_space(bytes[i]) {
                    i += 1;
                }
            }
            0x0b | 0x0c | b'/' | 0xc2 | 0xe1 | 0xe2 | 0xe3 | 0xef => {
                let ws = json5_whitespace_len(&bytes[i..]);
                if ws == 0 {
                    return Err(malformed_json());
                }
                i += ws;
            }
            b'n' if bytes.get(i..i + 4) == Some(b"null")
                && !bytes.get(i + 4).is_some_and(u8::is_ascii_alphanumeric) =>
            {
                out.push(JSONB_NULL_TYPE);
                return Ok(JsonbTextStep::Value(i + 4));
            }
            _ => {
                const NAN_INF: &[(&[u8], bool)] = &[
                    (b"inf", true),
                    (b"infinity", true),
                    (b"nan", false),
                    (b"qnan", false),
                    (b"snan", false),
                ];
                for &(needle, is_inf) in NAN_INF {
                    if eq_ascii_ignore_case_at(bytes, i, needle)
                        && !bytes
                            .get(i + needle.len())
                            .is_some_and(u8::is_ascii_alphanumeric)
                    {
                        if is_inf {
                            append_jsonb_node(JSONB_FLOAT_TYPE, b"9e999", out)?;
                        } else {
                            out.push(JSONB_NULL_TYPE);
                        }
                        return Ok(JsonbTextStep::Value(i + needle.len()));
                    }
                }
                return Err(malformed_json());
            }
        }
    }
}

fn translate_json_number_to_jsonb(
    bytes: &[u8],
    i: usize,
    out: &mut Vec<u8>,
) -> Result<JsonbTextStep> {
    let c = bytes[i];
    let mut t: u8 = 0x00; // bit 0x01: JSON5, bit 0x02: FLOAT
    let mut seen_e = false;
    let skip_prefix = if c == b'.' {
        if !bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            return Err(malformed_json());
        }
        t = 0x03;
        true
    } else {
        false
    };

    let j = if !skip_prefix && c == b'0' {
        if matches!(bytes.get(i + 1), Some(b'x' | b'X'))
            && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit)
        {
            t = 0x01;
            let mut k = i + 3;
            while bytes.get(k).is_some_and(u8::is_ascii_hexdigit) {
                k += 1;
            }
            k
        } else if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            return Err(malformed_json());
        } else {
            scan_decimal_number(bytes, i, &mut t, &mut seen_e)?
        }
    } else if !skip_prefix && (c == b'-' || c == b'+') {
        if !bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            if eq_ascii_ignore_case_at(bytes, i + 1, b"inf") {
                if c == b'-' {
                    append_jsonb_node(JSONB_FLOAT_TYPE, b"-9e999", out)?;
                } else {
                    append_jsonb_node(JSONB_FLOAT_TYPE, b"9e999", out)?;
                }
                let advance = if eq_ascii_ignore_case_at(bytes, i + 4, b"inity") {
                    9
                } else {
                    4
                };
                return Ok(JsonbTextStep::Value(i + advance));
            }
            if bytes.get(i + 1) == Some(&b'.') {
                t |= 0x01;
                scan_decimal_number(bytes, i, &mut t, &mut seen_e)?
            } else {
                return Err(malformed_json());
            }
        } else if bytes.get(i + 1) == Some(&b'0') {
            if bytes.get(i + 2).is_some_and(u8::is_ascii_digit) {
                return Err(malformed_json());
            }
            if matches!(bytes.get(i + 2), Some(b'x' | b'X'))
                && bytes.get(i + 3).is_some_and(u8::is_ascii_hexdigit)
            {
                t |= 0x01;
                let mut k = i + 4;
                while bytes.get(k).is_some_and(u8::is_ascii_hexdigit) {
                    k += 1;
                }
                k
            } else {
                scan_decimal_number(bytes, i, &mut t, &mut seen_e)?
            }
        } else {
            scan_decimal_number(bytes, i, &mut t, &mut seen_e)?
        }
    } else {
        scan_decimal_number(bytes, i, &mut t, &mut seen_e)?
    };

    let start = if bytes[i] == b'+' { i + 1 } else { i };
    append_jsonb_node(JSONB_INT_TYPE + t, &bytes[start..j], out)?;
    Ok(JsonbTextStep::Value(j))
}

fn scan_decimal_number(bytes: &[u8], i: usize, t: &mut u8, seen_e: &mut bool) -> Result<usize> {
    let mut j = i + 1;
    while let Some(&b) = bytes.get(j) {
        if b.is_ascii_digit() {
            j += 1;
            continue;
        }
        if b == b'.' {
            if (*t & 0x02) != 0 {
                return Err(malformed_json());
            }
            *t |= 0x02;
            j += 1;
            continue;
        }
        if b == b'e' || b == b'E' {
            if bytes[j - 1] < b'0' {
                if bytes[j - 1] == b'.' && j >= i + 2 && bytes[j - 2].is_ascii_digit() {
                    *t |= 0x01;
                } else {
                    return Err(malformed_json());
                }
            }
            if *seen_e {
                return Err(malformed_json());
            }
            *t |= 0x02;
            *seen_e = true;
            let mut next_b = bytes.get(j + 1).copied().unwrap_or(0);
            if next_b == b'+' || next_b == b'-' {
                j += 1;
                next_b = bytes.get(j + 1).copied().unwrap_or(0);
            }
            if !next_b.is_ascii_digit() {
                return Err(malformed_json());
            }
            j += 1;
            continue;
        }
        break;
    }
    if bytes[j - 1] < b'0' {
        if bytes[j - 1] == b'.' && j >= i + 2 && bytes[j - 2].is_ascii_digit() {
            *t |= 0x01;
        } else {
            return Err(malformed_json());
        }
    }
    Ok(j)
}

fn json_blob_to_jsonb(input: &[u8]) -> Result<Cow<'_, [u8]>> {
    if decode_jsonb_root(input).is_ok() {
        return Ok(Cow::Borrowed(input));
    }
    let text = std::str::from_utf8(input).map_err(|_| malformed_json())?;
    Ok(Cow::Owned(translate_json_text_to_jsonb(text)?))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonbLookupError {
    NotFound,
    BadPath,
    TooDeep,
    Malformed,
}

/// Read-only path lookup over a JSONB blob (`sqlite3.c`: `jsonLookupStep`).
/// Returns `(value_offset, label_offset)` where `label_offset == 0` for root
/// or array elements and `label_offset > 0` for object members.
fn jsonb_lookup_read(
    blob: &[u8],
    i_root: usize,
    tail: &str,
    i_label: usize,
    depth: usize,
    full_path: &str,
) -> std::result::Result<(usize, usize), JsonbLookupError> {
    if tail.is_empty() {
        return Ok((i_root, i_label));
    }
    if tail.starts_with('.') {
        let (key_bytes, raw_key, rest) =
            jsonb_parse_path_object_key(tail, full_path).map_err(|_| JsonbLookupError::BadPath)?;
        let (root_type, root_hdr, root_payload) =
            jsonb_node_bounds(blob, i_root).ok_or(JsonbLookupError::Malformed)?;
        if root_type != JSONB_OBJECT_TYPE {
            return Err(JsonbLookupError::NotFound);
        }
        let mut pos = i_root + root_hdr;
        let end = pos + root_payload;
        while pos < end {
            let (label_type, label_hdr, label_len) =
                jsonb_node_bounds(blob, pos).ok_or(JsonbLookupError::Malformed)?;
            if !(JSONB_TEXT_TYPE..=JSONB_TEXTRAW_TYPE).contains(&label_type) {
                return Err(JsonbLookupError::Malformed);
            }
            let label_start = pos + label_hdr;
            let val_pos = label_start + label_len;
            if val_pos >= end {
                return Err(JsonbLookupError::Malformed);
            }
            let matched =
                jsonb_label_matches(key_bytes, raw_key, label_type, &blob[label_start..val_pos])
                    .map_err(|_| JsonbLookupError::Malformed)?;
            let (_, val_hdr, val_len) =
                jsonb_node_bounds(blob, val_pos).ok_or(JsonbLookupError::Malformed)?;
            if val_pos + val_hdr + val_len > end {
                return Err(JsonbLookupError::Malformed);
            }
            if matched {
                if depth + 1 >= JSONB_EDIT_MAX_DEPTH {
                    return Err(JsonbLookupError::TooDeep);
                }
                return jsonb_lookup_read(blob, val_pos, rest, pos, depth + 1, full_path);
            }
            pos = val_pos + val_hdr + val_len;
        }
        if pos > end {
            return Err(JsonbLookupError::Malformed);
        }
        Err(JsonbLookupError::NotFound)
    } else if tail.starts_with('[') {
        let (root_type, root_hdr, root_payload) =
            jsonb_node_bounds(blob, i_root).ok_or(JsonbLookupError::Malformed)?;
        if root_type != JSONB_ARRAY_TYPE {
            return Err(JsonbLookupError::NotFound);
        }
        let array_count = jsonb_array_count(blob, i_root).ok_or(JsonbLookupError::Malformed)?;
        let Some((mut kk, rest)) = jsonb_parse_path_array_index(tail, array_count, full_path)
            .map_err(|_| JsonbLookupError::BadPath)?
        else {
            return Err(JsonbLookupError::NotFound);
        };
        let mut pos = i_root + root_hdr;
        let end = pos + root_payload;
        while pos < end {
            if kk == 0 {
                if depth + 1 >= JSONB_EDIT_MAX_DEPTH {
                    return Err(JsonbLookupError::TooDeep);
                }
                return jsonb_lookup_read(blob, pos, rest, 0, depth + 1, full_path);
            }
            kk -= 1;
            let (_, child_hdr, child_len) =
                jsonb_node_bounds(blob, pos).ok_or(JsonbLookupError::Malformed)?;
            pos += child_hdr + child_len;
        }
        if pos > end {
            return Err(JsonbLookupError::Malformed);
        }
        Err(JsonbLookupError::NotFound)
    } else {
        Err(JsonbLookupError::BadPath)
    }
}

const fn jsonb_type_name(node_type: u8) -> &'static str {
    match node_type {
        JSONB_TRUE_TYPE => "true",
        JSONB_FALSE_TYPE => "false",
        JSONB_INT_TYPE | JSONB_INT5_TYPE => "integer",
        JSONB_FLOAT_TYPE | JSONB_FLOAT5_TYPE => "real",
        JSONB_TEXT_TYPE | JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE | JSONB_TEXTRAW_TYPE => "text",
        JSONB_ARRAY_TYPE => "array",
        JSONB_OBJECT_TYPE => "object",
        _ => "null",
    }
}

/// Convert a single scalar JSONB node into its SQL value (`sqlite3.c`:
/// `jsonReturnFromBlob`).
#[allow(clippy::cast_precision_loss)]
fn jsonb_node_to_sqlite_scalar(blob: &[u8], offset: usize) -> Result<SqliteValue> {
    let (node_type, hdr_len, payload_len) =
        jsonb_node_bounds(blob, offset).ok_or_else(malformed_json)?;
    let payload = &blob[offset + hdr_len..offset + hdr_len + payload_len];
    match node_type {
        JSONB_NULL_TYPE => {
            if !payload.is_empty() {
                return Err(malformed_json());
            }
            Ok(SqliteValue::Null)
        }
        JSONB_TRUE_TYPE => {
            if !payload.is_empty() {
                return Err(malformed_json());
            }
            Ok(SqliteValue::Integer(1))
        }
        JSONB_FALSE_TYPE => {
            if !payload.is_empty() {
                return Err(malformed_json());
            }
            Ok(SqliteValue::Integer(0))
        }
        JSONB_INT_TYPE | JSONB_INT5_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            if text.is_empty() {
                return Err(malformed_json());
            }
            let normalized = if node_type == JSONB_INT5_TYPE {
                text.strip_prefix('+').unwrap_or(text)
            } else {
                text
            };
            if node_type == JSONB_INT5_TYPE {
                let (negative, unsigned) = normalized
                    .strip_prefix('-')
                    .map_or((false, normalized), |rest| (true, rest));
                if let Some(digits) = unsigned
                    .strip_prefix("0x")
                    .or_else(|| unsigned.strip_prefix("0X"))
                {
                    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(malformed_json());
                    }
                    let magnitude =
                        u64::from_str_radix(digits, 16).map_err(|_| malformed_json())?;
                    return Ok(if let Ok(int) = i64::try_from(magnitude) {
                        SqliteValue::Integer(if negative { -int } else { int })
                    } else {
                        let real = magnitude as f64;
                        SqliteValue::Float(if negative { -real } else { real })
                    });
                }
            }
            if normalized == "-0" {
                return Ok(SqliteValue::Integer(0));
            }
            if let Ok(int) = normalized.parse::<i64>() {
                return Ok(SqliteValue::Integer(int));
            }
            let real = normalized.parse::<f64>().map_err(|_| malformed_json())?;
            Ok(SqliteValue::Float(real))
        }
        JSONB_FLOAT_TYPE | JSONB_FLOAT5_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            if text.is_empty() {
                return Err(malformed_json());
            }
            let normalized = if node_type == JSONB_FLOAT5_TYPE {
                text.strip_prefix('+').unwrap_or(text)
            } else {
                text
            };
            let real = normalized.parse::<f64>().map_err(|_| malformed_json())?;
            Ok(SqliteValue::Float(real))
        }
        JSONB_TEXT_TYPE | JSONB_TEXTRAW_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            Ok(SqliteValue::Text(text.into()))
        }
        JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE => {
            let decoded = decode_jsonb_text(node_type, payload)?;
            Ok(SqliteValue::Text(decoded.into()))
        }
        _ => Err(malformed_json()),
    }
}

/// Render a JSONB node directly into canonical JSON text preserving raw source
/// spellings (`sqlite3.c`: `jsonTranslateBlobToText`).
#[allow(clippy::too_many_lines)]
fn jsonb_translate_node_to_json_text(
    blob: &[u8],
    offset: usize,
    out: &mut String,
    depth: usize,
) -> Result<usize> {
    let (node_type, hdr_len, sz) = jsonb_node_bounds(blob, offset).ok_or_else(malformed_json)?;
    let start = offset + hdr_len;
    let end = start + sz;
    let payload = &blob[start..end];
    match node_type {
        JSONB_NULL_TYPE => {
            out.push_str("null");
            return Ok(offset + 1);
        }
        JSONB_TRUE_TYPE => {
            out.push_str("true");
            return Ok(offset + 1);
        }
        JSONB_FALSE_TYPE => {
            out.push_str("false");
            return Ok(offset + 1);
        }
        JSONB_INT_TYPE | JSONB_FLOAT_TYPE => {
            if sz == 0 {
                return Err(malformed_json());
            }
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            out.push_str(text);
        }
        JSONB_INT5_TYPE => {
            if sz == 0 {
                return Err(malformed_json());
            }
            let mut k = 2usize;
            if payload[0] == b'-' {
                out.push('-');
                k += 1;
            } else if payload[0] == b'+' {
                k += 1;
            }
            if k > sz {
                return Err(malformed_json());
            }
            let mut u = 0_u64;
            let mut overflow = false;
            for &b in &payload[k..] {
                let Some(digit) = hex_nibble(b) else {
                    return Err(malformed_json());
                };
                if (u >> 60) != 0 {
                    overflow = true;
                } else {
                    u = u * 16 + u64::from(digit);
                }
            }
            if overflow {
                out.push_str("9.0e999");
            } else {
                let _ = std::fmt::Write::write_fmt(out, format_args!("{u}"));
            }
        }
        JSONB_FLOAT5_TYPE => {
            if sz == 0 {
                return Err(malformed_json());
            }
            let mut k = if payload[0] == b'-' {
                out.push('-');
                if sz <= 1 {
                    return Err(malformed_json());
                }
                1
            } else {
                0usize
            };
            if payload[k] == b'.' {
                out.push('0');
            }
            while k < sz {
                let b = payload[k];
                out.push(char::from(b));
                if b == b'.' && !payload.get(k + 1).is_some_and(u8::is_ascii_digit) {
                    out.push('0');
                }
                k += 1;
            }
        }
        JSONB_TEXT_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            if payload
                .iter()
                .all(|&b| b >= 0x20 && b != b'"' && b != b'\\')
            {
                out.push('"');
                out.push_str(text);
                out.push('"');
            } else {
                write_json_escaped_string(text, out);
            }
        }
        JSONB_TEXT_JSON_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            out.push('"');
            out.push_str(text);
            out.push('"');
        }
        JSONB_TEXT5_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            let pbytes = text.as_bytes();
            out.push('"');
            let mut idx = 0usize;
            while idx < pbytes.len() {
                let run_start = idx;
                while idx < pbytes.len() && (is_json_ok_byte(pbytes[idx]) || pbytes[idx] == b'\'') {
                    idx += 1;
                }
                if idx > run_start {
                    out.push_str(&text[run_start..idx]);
                    if idx >= pbytes.len() {
                        break;
                    }
                }
                let b = pbytes[idx];
                if b == b'"' {
                    out.push_str("\\\"");
                    idx += 1;
                } else if b <= 0x1f {
                    match b {
                        0x08 => out.push_str("\\b"),
                        0x0c => out.push_str("\\f"),
                        b'\n' => out.push_str("\\n"),
                        b'\r' => out.push_str("\\r"),
                        b'\t' => out.push_str("\\t"),
                        other => {
                            let _ = std::fmt::Write::write_fmt(out, format_args!("\\u{other:04x}"));
                        }
                    }
                    idx += 1;
                } else {
                    if idx + 1 >= pbytes.len() {
                        return Err(malformed_json());
                    }
                    match pbytes[idx + 1] {
                        b'\'' => {
                            out.push('\'');
                            idx += 2;
                        }
                        b'v' => {
                            out.push_str("\\u000b");
                            idx += 2;
                        }
                        b'x' => {
                            if idx + 3 >= pbytes.len() {
                                return Err(malformed_json());
                            }
                            out.push_str("\\u00");
                            out.push_str(&text[idx + 2..idx + 4]);
                            idx += 4;
                        }
                        b'0' => {
                            out.push_str("\\u0000");
                            idx += 2;
                        }
                        b'\r' => {
                            idx += if pbytes.get(idx + 2) == Some(&b'\n') {
                                3
                            } else {
                                2
                            };
                        }
                        b'\n' => idx += 2,
                        0xe2 => {
                            if idx + 3 >= pbytes.len()
                                || pbytes[idx + 2] != 0x80
                                || !matches!(pbytes[idx + 3], 0xa8 | 0xa9)
                            {
                                return Err(malformed_json());
                            }
                            idx += 4;
                        }
                        _ => {
                            out.push_str(&text[idx..idx + 2]);
                            idx += 2;
                        }
                    }
                }
            }
            out.push('"');
        }
        JSONB_TEXTRAW_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|_| malformed_json())?;
            write_json_escaped_string(text, out);
        }
        JSONB_ARRAY_TYPE => {
            if depth >= STOCK_JSON_MAX_DEPTH {
                return Err(malformed_json());
            }
            out.push('[');
            let mut pos = start;
            while pos < end {
                pos = jsonb_translate_node_to_json_text(blob, pos, out, depth + 1)?;
                out.push(',');
            }
            if pos > end {
                return Err(malformed_json());
            }
            if sz > 0 {
                out.pop();
            }
            out.push(']');
        }
        JSONB_OBJECT_TYPE => {
            if depth >= STOCK_JSON_MAX_DEPTH {
                return Err(malformed_json());
            }
            out.push('{');
            let mut pos = start;
            let mut count = 0usize;
            while pos < end {
                pos = jsonb_translate_node_to_json_text(blob, pos, out, depth + 1)?;
                out.push(if (count & 1) != 0 { ',' } else { ':' });
                count += 1;
            }
            if (count & 1) != 0 || pos > end {
                return Err(malformed_json());
            }
            if sz > 0 {
                out.pop();
            }
            out.push('}');
        }
        _ => return Err(malformed_json()),
    }
    Ok(end)
}

#[derive(Debug, Clone, Copy)]
struct JsonbParentFrame {
    i_head: usize,
    i_value: usize,
    i_end: usize,
    i_key: i64,
    n_path: usize,
}

fn json_skip_label(blob: &[u8], e_type: u8, cur_i: usize) -> Result<usize> {
    if e_type == JSONB_OBJECT_TYPE {
        let (_, hdr_len, sz) = jsonb_node_bounds(blob, cur_i).ok_or_else(malformed_json)?;
        let next = cur_i + hdr_len + sz;
        if next >= blob.len() {
            Ok(cur_i)
        } else {
            Ok(next)
        }
    } else {
        Ok(cur_i)
    }
}

/// Append the current element's path segment onto `path_buf` (`sqlite3.c`:
/// `jsonAppendPathName`).
fn json_append_path_name(
    path_buf: &mut String,
    blob: &[u8],
    e_type: u8,
    cur_i: usize,
    array_key: i64,
) -> Result<()> {
    if e_type == JSONB_ARRAY_TYPE {
        let _ = std::fmt::Write::write_fmt(path_buf, format_args!("[{array_key}]"));
    } else {
        let (_, hdr_len, sz) = jsonb_node_bounds(blob, cur_i).ok_or_else(malformed_json)?;
        let raw = &blob[cur_i + hdr_len..cur_i + hdr_len + sz];
        let raw_str = std::str::from_utf8(raw).map_err(|_| malformed_json())?;
        let need_quote = raw.is_empty()
            || !raw[0].is_ascii_alphabetic()
            || raw.iter().any(|&b| !b.is_ascii_alphanumeric());
        if need_quote {
            path_buf.push_str(".\"");
            path_buf.push_str(raw_str);
            path_buf.push('"');
        } else {
            path_buf.push('.');
            path_buf.push_str(raw_str);
        }
    }
    Ok(())
}

/// Effective `path` prefix length for `json_each`/`json_tree` (`sqlite3.c`:
/// `jsonEachPathLength`).
fn json_each_path_length(
    blob: &[u8],
    path: &str,
    rowid: usize,
    recursive: bool,
    cur_i: usize,
) -> usize {
    let bytes = path.as_bytes();
    let mut n = bytes.len();
    if rowid == 0 && recursive && n >= 2 {
        while n > 1 {
            n -= 1;
            if bytes[n] == b'[' || bytes[n] == b'.' {
                let prefix_tail = &path[1..n];
                if let Ok((x, _)) = jsonb_lookup_read(blob, 0, prefix_tail, 0, 0, &path[..n])
                    && let Some((_, hdr_len, _)) = jsonb_node_bounds(blob, x)
                    && x + hdr_len == cur_i
                {
                    break;
                }
            }
        }
    }
    n
}

fn sqlite_atoi64_prefix(text: &str) -> i64 {
    let bytes = text.as_bytes();
    let mut idx = 0usize;
    while bytes.get(idx).is_some_and(u8::is_ascii_whitespace) {
        idx += 1;
    }
    let mut neg = false;
    if bytes.get(idx) == Some(&b'-') {
        neg = true;
        idx += 1;
    } else if bytes.get(idx) == Some(&b'+') {
        idx += 1;
    }
    let mut acc: u64 = 0;
    let mut saw_digit = false;
    while let Some(&b) = bytes.get(idx) {
        if !b.is_ascii_digit() {
            break;
        }
        saw_digit = true;
        acc = acc.saturating_mul(10).saturating_add(u64::from(b - b'0'));
        idx += 1;
    }
    if !saw_digit {
        return 0;
    }
    if neg {
        if acc >= i64::MIN.unsigned_abs() {
            i64::MIN
        } else {
            -i64::try_from(acc).unwrap_or(i64::MAX)
        }
    } else {
        i64::try_from(acc).unwrap_or(i64::MAX)
    }
}

/// Walk a JSONB document for `json_each` (`recursive = false`) or `json_tree`
/// (`recursive = true`), matching SQLite 3.54.1's `jsonEachFilter`,
/// `jsonEachNext`, and `jsonEachColumn` (`sqlite3.c:219160-219569`).
#[allow(clippy::too_many_lines)]
fn json_table_rows_from_jsonb(
    blob: &[u8],
    path: Option<&str>,
    recursive: bool,
) -> Result<Vec<JsonTableRow>> {
    let mut path_buf = String::new();
    let (i, mut cur_i, mut e_type) = if let Some(z_root) = path {
        if !z_root.starts_with('$') {
            return Err(bad_json_path(z_root));
        }
        path_buf.push_str(z_root);
        if z_root.len() == 1 {
            (0usize, 0usize, 0u8)
        } else {
            match jsonb_lookup_read(blob, 0, &z_root[1..], 0, 0, z_root) {
                Ok((val_idx, label_idx)) => {
                    if label_idx > 0 {
                        (val_idx, label_idx, JSONB_OBJECT_TYPE)
                    } else {
                        (val_idx, val_idx, JSONB_ARRAY_TYPE)
                    }
                }
                Err(JsonbLookupError::NotFound) => return Ok(Vec::new()),
                Err(JsonbLookupError::BadPath) => return Err(bad_json_path(z_root)),
                Err(JsonbLookupError::TooDeep) => {
                    return Err(FrankenError::function_error("JSON path too deep"));
                }
                Err(JsonbLookupError::Malformed) => return Err(malformed_json()),
            }
        }
    } else {
        path_buf.push('$');
        (0usize, 0usize, 0u8)
    };
    let n_root = path_buf.len();

    let (root_val_type, root_hdr, root_sz) =
        jsonb_node_bounds(blob, i).ok_or_else(malformed_json)?;
    let i_end = i + root_hdr + root_sz;
    let mut parents: Vec<JsonbParentFrame> = Vec::new();
    if !recursive && (root_val_type == JSONB_ARRAY_TYPE || root_val_type == JSONB_OBJECT_TYPE) {
        cur_i = i + root_hdr;
        e_type = root_val_type;
        parents.push(JsonbParentFrame {
            i_head: cur_i,
            i_value: i,
            i_end,
            i_key: 0,
            n_path: path_buf.len(),
        });
    }

    let mut rows = Vec::new();
    let mut rowid = 0usize;
    while cur_i < i_end {
        let val_i = json_skip_label(blob, e_type, cur_i)?;
        let (val_type, val_hdr, val_sz) =
            jsonb_node_bounds(blob, val_i).ok_or_else(malformed_json)?;
        let path_len = json_each_path_length(blob, &path_buf, rowid, recursive, cur_i);

        let key = if parents.is_empty() {
            if n_root == 1 {
                SqliteValue::Null
            } else {
                let n = n_root.saturating_sub(path_len);
                if n == 0 {
                    SqliteValue::Null
                } else if path_buf.as_bytes()[path_len] == b'[' {
                    SqliteValue::Integer(sqlite_atoi64_prefix(
                        &path_buf[path_len + 1..path_len + n],
                    ))
                } else if path_buf.as_bytes().get(path_len + 1) == Some(&b'"') {
                    let slice = path_buf
                        .get(path_len + 2..path_len + n - 1)
                        .unwrap_or_default();
                    SqliteValue::Text(slice.into())
                } else {
                    SqliteValue::Text(path_buf[path_len + 1..path_len + n].into())
                }
            }
        } else if e_type == JSONB_OBJECT_TYPE {
            jsonb_node_to_sqlite_scalar(blob, cur_i)?
        } else {
            SqliteValue::Integer(parents.last().expect("parent frame present").i_key)
        };

        let is_container = val_type == JSONB_ARRAY_TYPE || val_type == JSONB_OBJECT_TYPE;
        let (value, atom) = if is_container {
            let mut rendered = String::with_capacity(val_sz.saturating_add(4));
            let _ = jsonb_translate_node_to_json_text(blob, val_i, &mut rendered, 0)?;
            (
                SqliteValue::Text(SmallText::from_string(rendered)),
                SqliteValue::Null,
            )
        } else {
            let scalar = jsonb_node_to_sqlite_scalar(blob, val_i)?;
            (scalar.clone(), scalar)
        };

        let id = i64::try_from(cur_i).map_err(|error| {
            FrankenError::function_error(format!("json_each id overflow: {error}"))
        })?;
        let parent = if recursive && let Some(parent_frame) = parents.last() {
            let parent_id = i64::try_from(parent_frame.i_head).map_err(|error| {
                FrankenError::function_error(format!("json_tree parent id overflow: {error}"))
            })?;
            SqliteValue::Integer(parent_id)
        } else {
            SqliteValue::Null
        };

        let n_base = path_buf.len();
        if let Some(parent_frame) = parents.last() {
            json_append_path_name(&mut path_buf, blob, e_type, cur_i, parent_frame.i_key)?;
        }
        let fullkey = path_buf.clone();
        path_buf.truncate(n_base);
        let row_path = path_buf[..path_len].to_owned();

        rows.push(JsonTableRow {
            key,
            value,
            type_name: jsonb_type_name(val_type),
            atom,
            id,
            parent,
            fullkey,
            path: row_path,
        });

        // Advance cursor (`sqlite3.c`: `jsonEachNext`).
        if recursive {
            let mut level_change = if is_container {
                let n_path = path_buf.len();
                if e_type != 0
                    && let Some(parent_frame) = parents.last()
                {
                    json_append_path_name(&mut path_buf, blob, e_type, cur_i, parent_frame.i_key)?;
                }
                parents.push(JsonbParentFrame {
                    i_head: cur_i,
                    i_value: val_i,
                    i_end: val_i + val_hdr + val_sz,
                    i_key: -1,
                    n_path,
                });
                cur_i = val_i + val_hdr;
                true
            } else {
                cur_i = val_i + val_hdr + val_sz;
                false
            };
            while parents.last().is_some_and(|frame| cur_i >= frame.i_end) {
                let popped = parents.pop().expect("parent frame checked");
                path_buf.truncate(popped.n_path);
                level_change = true;
            }
            if level_change {
                e_type = parents.last().map_or(0, |frame| blob[frame.i_value] & 0x0f);
            }
        } else {
            cur_i = val_i + val_hdr + val_sz;
        }
        if e_type == JSONB_ARRAY_TYPE
            && let Some(parent_frame) = parents.last_mut()
        {
            parent_frame.i_key += 1;
        }
        rowid += 1;
    }

    Ok(rows)
}

struct JsonTableHiddenColumns {
    json: SqliteValue,
    root: SmallText,
}

/// The JSONB input and PATH of a json_each/json_tree scan, or `None` when the
/// scan has no rows. As in stock's `jsonEachFilter`, that is a missing or NULL
/// input (checked before the PATH, so `json_each(NULL, 'x')` is empty) or a
/// NULL PATH (checked after the input parses, so malformed input still
/// errors). `FROM t, json_each(t.tags)` thus skips rows whose `tags` is NULL.
fn parse_json_table_filter_args(
    name: &str,
    args: &[SqliteValue],
) -> Result<Option<(Vec<u8>, JsonTableHiddenColumns)>> {
    if args.len() > 2 {
        return Err(FrankenError::function_error(format!(
            "too many arguments on {name}() - max 2"
        )));
    }
    let Some(input_arg) = args.first() else {
        return Ok(None);
    };
    let (input_blob, json) = match input_arg {
        SqliteValue::Null => return Ok(None),
        // JSON5 input accepted (bd-qear2); the hidden input retains the original text.
        SqliteValue::Text(input_text) => {
            (translate_json_text_to_jsonb(input_text)?, input_arg.clone())
        }
        SqliteValue::Blob(input_blob) => match decode_jsonb_root(input_blob) {
            Ok(_) => (input_blob.to_vec(), input_arg.clone()),
            Err(_) => {
                // A text JSON blob is coerced to TEXT, while actual JSONB
                // retains BLOB storage.
                let text = std::str::from_utf8(input_blob).map_err(|_| malformed_json())?;
                (
                    translate_json_text_to_jsonb(text)?,
                    SqliteValue::Text(SmallText::from_arc_bytes(Arc::clone(input_blob))),
                )
            }
        },
        // A bare SQL numeric is a JSON number (C SQLite convention), mirroring
        // the scalar `json_arg_value` path: json_each(1.5) yields a single row
        // with type='real', atom=1.5; json_tree(7) yields type='integer'. A
        // non-finite REAL renders as the numeric literal 9.0e+999 / -9.0e+999
        // (NaN -> null), so json_each(9e999) yields a single row (GH#212).
        SqliteValue::Integer(i) => {
            let mut out = Vec::new();
            append_jsonb_node(JSONB_INT_TYPE, i.to_string().as_bytes(), &mut out)?;
            (out, SqliteValue::Text(input_arg.to_text().into()))
        }
        SqliteValue::Float(f) => {
            let mut out = Vec::new();
            if f.is_nan() {
                append_jsonb_node(JSONB_NULL_TYPE, &[], &mut out)?;
            } else if f.is_infinite() {
                let literal = if *f > 0.0 {
                    b"9.0e+999".as_slice()
                } else {
                    b"-9.0e+999".as_slice()
                };
                append_jsonb_node(JSONB_FLOAT_TYPE, literal, &mut out)?;
            } else {
                let formatted = fsqlite_types::value::format_sqlite_float(*f);
                append_jsonb_node(JSONB_FLOAT_TYPE, formatted.as_bytes(), &mut out)?;
            }
            (out, SqliteValue::Text(input_arg.to_text().into()))
        }
    };

    let root = match args.get(1) {
        None => SmallText::from("$"),
        Some(SqliteValue::Null) => return Ok(None),
        Some(_) => SmallText::from(path_arg(name, args, 1)?),
    };

    Ok(Some((input_blob, JsonTableHiddenColumns { json, root })))
}
fn write_json_table_column(
    row: &JsonTableRow,
    hidden: Option<&JsonTableHiddenColumns>,
    ctx: &mut ColumnContext,
    col: i32,
) {
    let value = match col {
        0 => row.key.clone(),
        1 => row.value.clone(),
        2 => SqliteValue::Text(row.type_name.into()),
        3 => row.atom.clone(),
        4 => SqliteValue::Integer(row.id),
        5 => row.parent.clone(),
        6 => SqliteValue::Text(row.fullkey.as_str().into()),
        7 => SqliteValue::Text(row.path.as_str().into()),
        8 => hidden.map_or(SqliteValue::Null, |columns| columns.json.clone()),
        9 => hidden.map_or(SqliteValue::Null, |columns| {
            SqliteValue::Text(columns.root.clone())
        }),
        _ => SqliteValue::Null,
    };
    ctx.set_value(value);
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(true) => "true",
        Value::Bool(false) => "false",
        Value::IntegerText(_) | Value::HexInteger(_) => "integer",
        Value::FloatText(_) => "real",
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                "integer"
            } else {
                "real"
            }
        }
        Value::String(_) => "text",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn json_to_sqlite_scalar(value: &Value) -> Result<SqliteValue> {
    Ok(match value {
        Value::HexInteger(integer) => return integer.sql_value(),
        Value::Null => SqliteValue::Null,
        Value::Bool(true) => SqliteValue::Integer(1),
        Value::Bool(false) => SqliteValue::Integer(0),
        Value::IntegerText(raw) if raw == "-0" => SqliteValue::Integer(0),
        Value::IntegerText(raw) => {
            // SQLite returns a REAL to SQL for JSON integers outside its i64
            // range. Keep the exact digits until this conversion is requested.
            SqliteValue::Float(raw.parse().expect("validated decimal integer"))
        }
        Value::FloatText(raw) => {
            // Exact float text parses to its IEEE-754 value (including ±Inf for
            // overflowing literals like `9e999`, GH#212, bd-1nxwh).
            SqliteValue::Float(raw.parse::<f64>().unwrap_or(0.0))
        }
        Value::Number(number) => {
            if let Some(i) = number.as_i64() {
                SqliteValue::Integer(i)
            } else if let Some(u) = number.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    SqliteValue::Integer(i)
                } else {
                    SqliteValue::Float(u as f64)
                }
            } else {
                let float = number
                    .as_f64()
                    .or_else(|| number.to_string().parse::<f64>().ok())
                    .unwrap_or(0.0);
                SqliteValue::Float(float)
            }
        }
        Value::String(text) => SqliteValue::Text(SmallText::new(text)),
        Value::Array(_) | Value::Object(_) => {
            // Canonical writer (not `serde_json::to_string`): stock-canonical
            // floats and exact FloatText rendering (GH#212).
            SqliteValue::Text(SmallText::from_string(encode_json_text(value)))
        }
    })
}

/// Convert a SQL REAL to a JSON value exactly as stock SQLite does:
/// finite floats become JSON numbers; +Inf/-Inf render as the numeric literal
/// `9.0e+999` / `-9.0e+999`; NaN becomes JSON null (SQLite stores NaN as NULL).
fn float_to_json(f: f64) -> Result<Value> {
    if f.is_nan() {
        return Ok(Value::Null);
    }
    if f.is_infinite() {
        // +-Inf renders as the stock numeric literal `9.0e+999` / `-9.0e+999`
        // (GH#212). `serde_json::Number` cannot hold a non-finite value, so the
        // constructed literal is carried as `FloatText` (bd-yalqc, bd-oahj7);
        // the canonical `9.0e+999` form is preserved through render.
        return Ok(number_text_value(if f > 0.0 {
            "9.0e+999"
        } else {
            "-9.0e+999"
        }));
    }
    // bd-1nxwh: stock writes a REAL that enters JSON as an SQL value with its
    // REAL-to-TEXT conversion (`%!.17g`): 1e300 is `1.0e+300`, 1e-5 is
    // `1.0e-05`, 1.0/3 is `0.33333333333333332`, 42.333333333333 is
    // `42.333333333333002`, -0.0 is `0.0`. A JSON `Number` renders in Rust's
    // shortest form, which matches that only for an integral value below
    // 1e15 (`3.0`, `150.0`). Any other REAL is carried as `FloatText`.
    if f.fract() != 0.0 || f.abs() >= 1e15 || (f == 0.0 && f.is_sign_negative()) {
        return Ok(number_text_value(
            &fsqlite_types::value::format_sqlite_float(f),
        ));
    }
    Number::from_f64(f).map(Value::Number).ok_or_else(|| {
        FrankenError::function_error("failed to convert floating-point value to JSON")
    })
}

fn sqlite_to_json(value: &SqliteValue) -> Result<Value> {
    match value {
        SqliteValue::Null => Ok(Value::Null),
        SqliteValue::Integer(i) => Ok(Value::Number(Number::from(*i))),
        SqliteValue::Float(f) => float_to_json(*f),
        SqliteValue::Text(text) => Ok(Value::String(InlineStr::new(text))),
        // Stock `jsonAppendSqlValue`: a JSONB blob is embedded as the JSON
        // value it encodes; any other blob is an error.
        SqliteValue::Blob(bytes) => decode_jsonb_root(bytes)
            .map_err(|_| FrankenError::function_error("JSON cannot hold BLOB values")),
    }
}

fn write_pretty_value(value: &Value, indent: &str, depth: usize, out: &mut String) {
    match value {
        Value::Array(array) => {
            if array.is_empty() {
                out.push_str("[]");
                return;
            }

            out.push('[');
            out.push('\n');
            for (idx, item) in array.iter().enumerate() {
                out.push_str(&indent.repeat(depth + 1));
                write_pretty_value(item, indent, depth + 1, out);
                if idx + 1 < array.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&indent.repeat(depth));
            out.push(']');
        }
        Value::Object(object) => {
            if object.is_empty() {
                out.push_str("{}");
                return;
            }

            out.push('{');
            out.push('\n');
            for (idx, (key, item)) in object.iter().enumerate() {
                out.push_str(&indent.repeat(depth + 1));
                write_json_escaped_string(key, out);
                out.push_str(": ");
                write_pretty_value(item, indent, depth + 1, out);
                if idx + 1 < object.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&indent.repeat(depth));
            out.push('}');
        }
        _ => {
            // Scalars pretty-print identically to their minified form; the
            // canonical writer keeps floats stock-canonical and renders
            // FloatText verbatim (GH#212, bd-yalqc).
            write_canonical_json_text(value, out);
        }
    }
}

fn not_an_array_element(path: &str) -> FrankenError {
    FrankenError::function_error(format!(
        "not an array element: '{}'",
        path.replace('\'', "''")
    ))
}

fn array_insert_scaffold(next: Option<&PathSegment>) -> Value {
    match next {
        Some(PathSegment::Index(_) | PathSegment::Append | PathSegment::FromEnd(_)) => {
            Value::Array(Vec::new())
        }
        _ => Value::Object(Object::new()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrayInsertOutcome {
    Applied,
    Noop,
}

fn apply_array_insert(
    root: &mut Value,
    segments: &[PathSegment],
    new_value: Value,
    path: &str,
) -> Result<()> {
    if segments.is_empty() || !matches!(root, Value::Object(_) | Value::Array(_)) {
        return Ok(());
    }

    let original = root.clone();
    match apply_array_insert_inner(root, segments, new_value, path)? {
        ArrayInsertOutcome::Applied => Ok(()),
        ArrayInsertOutcome::Noop => {
            // Parent scaffolding is speculative. Stock leaves the document
            // unchanged when a later segment cannot be reached.
            *root = original;
            Ok(())
        }
    }
}

fn apply_array_insert_inner(
    cursor: &mut Value,
    segments: &[PathSegment],
    new_value: Value,
    path: &str,
) -> Result<ArrayInsertOutcome> {
    let Some((segment, rest)) = segments.split_first() else {
        return Ok(ArrayInsertOutcome::Noop);
    };

    if rest.is_empty() {
        return match segment {
            PathSegment::Key(_) => {
                if matches!(cursor, Value::Object(_)) {
                    Err(not_an_array_element(path))
                } else {
                    Ok(ArrayInsertOutcome::Noop)
                }
            }
            PathSegment::Index(index) => {
                let Value::Array(array) = cursor else {
                    return Ok(ArrayInsertOutcome::Noop);
                };
                if *index > array.len() {
                    return Ok(ArrayInsertOutcome::Noop);
                }
                array.insert(*index, new_value);
                Ok(ArrayInsertOutcome::Applied)
            }
            PathSegment::Append => {
                let Value::Array(array) = cursor else {
                    return Ok(ArrayInsertOutcome::Noop);
                };
                array.push(new_value);
                Ok(ArrayInsertOutcome::Applied)
            }
            PathSegment::FromEnd(from_end) => {
                let Value::Array(array) = cursor else {
                    return Ok(ArrayInsertOutcome::Noop);
                };
                if *from_end == 0 {
                    array.push(new_value);
                    return Ok(ArrayInsertOutcome::Applied);
                }
                if *from_end > array.len() {
                    return Ok(ArrayInsertOutcome::Noop);
                }
                let index = array.len() - *from_end;
                array.insert(index, new_value);
                Ok(ArrayInsertOutcome::Applied)
            }
        };
    }

    let next = rest.first();
    match segment {
        PathSegment::Key(key) => {
            let Value::Object(object) = cursor else {
                return Ok(ArrayInsertOutcome::Noop);
            };
            if !object.contains_key(key.as_ref()) {
                object.insert(key.to_string(), array_insert_scaffold(next));
            }
            let child = object
                .get_mut(key.as_ref())
                .expect("array-insert parent was present or just created");
            // Existing NULL/scalar parents are not rewritten. This is a
            // deliberate difference from json_insert/json_set.
            if !matches!(child, Value::Object(_) | Value::Array(_)) {
                return Ok(ArrayInsertOutcome::Noop);
            }
            apply_array_insert_inner(child, rest, new_value, path)
        }
        PathSegment::Index(index) => {
            let Value::Array(array) = cursor else {
                return Ok(ArrayInsertOutcome::Noop);
            };
            if *index > array.len() {
                return Ok(ArrayInsertOutcome::Noop);
            }
            if *index == array.len() {
                array.push(array_insert_scaffold(next));
            }
            let child = array
                .get_mut(*index)
                .expect("array-insert index was present or just appended");
            if !matches!(child, Value::Object(_) | Value::Array(_)) {
                return Ok(ArrayInsertOutcome::Noop);
            }
            apply_array_insert_inner(child, rest, new_value, path)
        }
        PathSegment::Append => {
            let Value::Array(array) = cursor else {
                return Ok(ArrayInsertOutcome::Noop);
            };
            array.push(array_insert_scaffold(next));
            let child = array.last_mut().expect("array-insert scaffold appended");
            apply_array_insert_inner(child, rest, new_value, path)
        }
        PathSegment::FromEnd(from_end) => {
            let Value::Array(array) = cursor else {
                return Ok(ArrayInsertOutcome::Noop);
            };
            if *from_end == 0 || *from_end > array.len() {
                return Ok(ArrayInsertOutcome::Noop);
            }
            let index = array.len() - *from_end;
            let child = array.get_mut(index).expect("array-insert index checked");
            if !matches!(child, Value::Object(_) | Value::Array(_)) {
                return Ok(ArrayInsertOutcome::Noop);
            }
            apply_array_insert_inner(child, rest, new_value, path)
        }
    }
}

fn edit_json_array_insert_with_subtypes(
    name: &str,
    mut edited: Value,
    args: &[SqliteValue],
    arg_subtypes: &[u32],
    start: usize,
) -> Result<Value> {
    let mut idx = start;
    while idx + 1 < args.len() {
        if args[idx].is_null() {
            idx += 2;
            continue;
        }
        let path = path_arg(name, args, idx)?;
        let segments = parse_path(path)?;
        let subtype = arg_subtypes.get(idx + 1).copied().unwrap_or(0);
        let replacement = sqlite_to_json_with_subtype(&args[idx + 1], subtype)?;
        apply_array_insert(&mut edited, &segments, replacement, path)?;
        idx += 2;
    }
    Ok(edited)
}

fn apply_edit(root: &mut Value, segments: &[PathSegment], new_value: Value, mode: EditMode) {
    if segments.is_empty() {
        match mode {
            EditMode::Set | EditMode::Replace => *root = new_value,
            EditMode::Insert => {}
        }
        return;
    }
    if !matches!(root, Value::Object(_) | Value::Array(_)) {
        // Match SQLite JSON1 semantics: non-root path edits are no-ops when
        // the document root is a scalar value.
        return;
    }

    let (parent_segments, last) = segments.split_at(segments.len() - 1);
    let Some(last_segment) = last.first() else {
        return;
    };
    // Only multi-segment paths can speculatively scaffold intermediate parents,
    // so single-segment edits (`$.x`, `$[0]`) never need a full-tree clone.
    let original = if parent_segments.is_empty() {
        None
    } else {
        Some(root.clone())
    };
    let Some(parent) = resolve_parent_for_edit(root, parent_segments, Some(last_segment), mode)
    else {
        if let Some(original) = original {
            *root = original;
        }
        return;
    };

    let applied = match (parent, last_segment) {
        (Value::Object(object), PathSegment::Key(key)) => match mode {
            EditMode::Set => {
                object.insert(key.as_ref(), new_value);
                true
            }
            EditMode::Insert => {
                if object.contains_key(key.as_ref()) {
                    false
                } else {
                    object.append(key.as_ref(), new_value);
                    true
                }
            }
            EditMode::Replace => {
                if let Some(prior) = object.get_mut(key.as_ref()) {
                    *prior = new_value;
                    true
                } else {
                    false
                }
            }
        },
        (Value::Array(array), PathSegment::Index(index)) => {
            apply_array_edit(array, *index, new_value, mode)
        }
        (Value::Array(array), PathSegment::Append) => {
            if matches!(mode, EditMode::Set | EditMode::Insert) {
                array.push(new_value);
                true
            } else {
                false
            }
        }
        (Value::Array(array), PathSegment::FromEnd(from_end)) => {
            if *from_end == 0 || *from_end > array.len() {
                false
            } else {
                let index = array.len() - *from_end;
                apply_array_edit(array, index, new_value, mode)
            }
        }
        _ => false,
    };

    if !applied && let Some(original) = original {
        *root = original;
    }
}

fn apply_array_edit(
    array: &mut Vec<Value>,
    index: usize,
    new_value: Value,
    mode: EditMode,
) -> bool {
    if index > array.len() {
        return false;
    }

    if index == array.len() {
        if matches!(mode, EditMode::Set | EditMode::Insert) {
            array.push(new_value);
            return true;
        }
        return false;
    }

    match mode {
        EditMode::Set | EditMode::Replace => {
            array[index] = new_value;
            true
        }
        EditMode::Insert => false,
    }
}

fn remove_at_path(root: &mut Value, segments: &[PathSegment]) {
    if segments.is_empty() {
        *root = Value::Null;
        return;
    }

    let (parent_segments, last) = segments.split_at(segments.len() - 1);
    let Some(last_segment) = last.first() else {
        return;
    };
    let Some(parent) = resolve_path_mut(root, parent_segments) else {
        return;
    };

    match (parent, last_segment) {
        (Value::Object(object), PathSegment::Key(key)) => {
            // Preserve member order and remove only the first matching label.
            object.shift_remove(key.as_ref());
        }
        (Value::Array(array), PathSegment::Index(index)) if *index < array.len() => {
            array.remove(*index);
        }
        (Value::Array(array), PathSegment::FromEnd(from_end)) => {
            if *from_end == 0 || *from_end > array.len() {
                return;
            }
            let index = array.len() - *from_end;
            array.remove(index);
        }
        _ => {}
    }
}

fn resolve_path_mut<'a>(root: &'a mut Value, segments: &[PathSegment]) -> Option<&'a mut Value> {
    let mut cursor = root;

    for segment in segments {
        match segment {
            PathSegment::Key(key) => {
                let next = cursor.as_object_mut()?.get_mut(key.as_ref())?;
                cursor = next;
            }
            PathSegment::Index(index) => {
                let next = cursor.as_array_mut()?.get_mut(*index)?;
                cursor = next;
            }
            PathSegment::FromEnd(from_end) => {
                let array = cursor.as_array_mut()?;
                if *from_end == 0 || *from_end > array.len() {
                    return None;
                }
                let index = array.len() - *from_end;
                let next = array.get_mut(index)?;
                cursor = next;
            }
            PathSegment::Append => return None,
        }
    }

    Some(cursor)
}

fn resolve_parent_for_edit<'a>(
    root: &'a mut Value,
    segments: &[PathSegment],
    tail_hint: Option<&PathSegment>,
    mode: EditMode,
) -> Option<&'a mut Value> {
    fn scaffold_for_next_segment(next: Option<&PathSegment>) -> Value {
        match next {
            Some(PathSegment::Index(_) | PathSegment::Append | PathSegment::FromEnd(_)) => {
                Value::Array(Vec::new())
            }
            _ => Value::Object(Object::new()),
        }
    }

    let mut cursor = root;

    for (idx, segment) in segments.iter().enumerate() {
        let next_segment = segments.get(idx + 1).or_else(|| {
            if idx + 1 == segments.len() {
                tail_hint
            } else {
                None
            }
        });
        match segment {
            PathSegment::Key(key) => {
                // An existing JSON null is a real value, not a missing path
                // component. Stock json_set/json_insert never replace it with
                // a container merely to continue descending (bd-8ouj3).
                let object = match cursor {
                    Value::Object(o) => o,
                    _ => return None,
                };

                if !object.contains_key(key.as_ref()) {
                    if !matches!(mode, EditMode::Set | EditMode::Insert) {
                        return None;
                    }
                    object.insert(key.to_string(), scaffold_for_next_segment(next_segment));
                }
                cursor = object
                    .get_mut(key.as_ref())
                    .expect("just inserted or checked");
            }
            PathSegment::Index(index) => {
                // Preserve an existing JSON null instead of treating it as an
                // absent array that may be scaffolded (bd-8ouj3).
                let array = match cursor {
                    Value::Array(a) => a,
                    _ => return None,
                };
                if *index > array.len() {
                    return None;
                }
                if *index == array.len() {
                    if !matches!(mode, EditMode::Set | EditMode::Insert) {
                        return None;
                    }
                    array.push(scaffold_for_next_segment(next_segment));
                }
                cursor = array.get_mut(*index).expect("just pushed or checked");
            }
            PathSegment::Append => {
                // Existing JSON null is not an implicit empty array.
                let array = match cursor {
                    Value::Array(a) => a,
                    _ => return None,
                };
                if !matches!(mode, EditMode::Set | EditMode::Insert) {
                    return None;
                }
                array.push(scaffold_for_next_segment(next_segment));
                cursor = array.last_mut().expect("just pushed");
            }
            PathSegment::FromEnd(from_end) => {
                let array = match cursor {
                    Value::Array(a) => a,
                    _ => return None,
                };
                if *from_end == 0 || *from_end > array.len() {
                    return None;
                }
                let index = array.len() - *from_end;
                cursor = array.get_mut(index).expect("checked length");
            }
        }
    }

    Some(cursor)
}

fn merge_patch(target: Value, patch: Value) -> Value {
    match patch {
        Value::Object(patch_map) => {
            let mut target_map = match target {
                Value::Object(map) => map,
                _ => Object::new(),
            };

            for (key, patch_value) in patch_map {
                if patch_value.is_null() {
                    // Order-preserving removal (see remove_at_path): RFC 7396 /
                    // SQLite json_patch drops a null-valued key while leaving the
                    // remaining members in order.
                    target_map.shift_remove(&key);
                    continue;
                }
                if let Some(prior) = target_map.get_mut(&key) {
                    let old_val = std::mem::replace(prior, Value::Null);
                    *prior = merge_patch(old_val, patch_value);
                } else {
                    target_map.insert(key, merge_patch(Value::Null, patch_value));
                }
            }

            Value::Object(target_map)
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Scalar function registration
// ---------------------------------------------------------------------------

fn invalid_arity(name: &str, expected: &str, got: usize) -> FrankenError {
    FrankenError::function_error(format!("{name} expects {expected}; got {got} argument(s)"))
}

/// Stock's error for input that does not parse as JSON (text or JSONB).
fn malformed_json() -> FrankenError {
    FrankenError::function_error("malformed JSON")
}

/// Stock's error for a PATH it cannot parse: `bad JSON path: %Q`.
fn bad_json_path(path: &str) -> FrankenError {
    FrankenError::function_error(format!("bad JSON path: '{}'", path.replace('\'', "''")))
}

/// Stock's arity rule for json_set/json_insert/json_replace and their jsonb_
/// forms: any odd count (a lone argument returns the input), else an error
/// that names the json_ form.
fn check_edit_arity(name: &str, args: &[SqliteValue]) -> Result<()> {
    if args.len().is_multiple_of(2) {
        let base = name
            .trim_start_matches("jsonb_")
            .trim_start_matches("json_");
        return Err(FrankenError::function_error(format!(
            "json_{base}() needs an odd number of arguments"
        )));
    }
    Ok(())
}

/// A PATH argument read the way stock reads one (`sqlite3_value_text`): a
/// BLOB is its UTF-8 bytes, and a number is its text, which never starts with
/// `$` and so is a bad path.
fn path_arg<'a>(name: &str, args: &'a [SqliteValue], index: usize) -> Result<&'a str> {
    match args.get(index) {
        Some(SqliteValue::Text(text)) => Ok(&**text),
        Some(SqliteValue::Blob(bytes)) => {
            std::str::from_utf8(bytes).map_err(|_| bad_json_path(&String::from_utf8_lossy(bytes)))
        }
        Some(other @ (SqliteValue::Integer(_) | SqliteValue::Float(_))) => {
            Err(bad_json_path(&other.to_text()))
        }
        _ => text_arg(name, args, index),
    }
}

fn text_arg<'a>(name: &str, args: &'a [SqliteValue], index: usize) -> Result<&'a str> {
    match args.get(index) {
        Some(SqliteValue::Text(text)) => Ok(&**text),
        Some(other) => Err(FrankenError::function_error(format!(
            "{name} argument {} must be TEXT, got {}",
            index + 1,
            other.typeof_str()
        ))),
        None => Err(FrankenError::function_error(format!(
            "{name} missing argument {}",
            index + 1
        ))),
    }
}

fn json_arg_value(name: &str, args: &[SqliteValue], index: usize) -> Result<Value> {
    match args.get(index) {
        // JSON5 input is accepted (bd-qear2): the shared arg-parser for every
        // value-consuming scalar json function (json_extract, json_type,
        // json_set, json_remove, json_patch, …) canonicalizes it via the parsed
        // tree. Strict JSON is unaffected (strict parse is tried first).
        Some(SqliteValue::Text(text)) => parse_json_value_lenient(text),
        Some(SqliteValue::Blob(bytes)) => parse_json_input_blob(bytes),
        // A bare SQL numeric value is interpreted as a JSON number (C SQLite's
        // JSON-argument convention): e.g. json_type(123) -> 'integer',
        // json_type(1.5) -> 'real', json_type(9e999) -> 'real'. A non-finite
        // REAL renders as the numeric literal 9.0e+999 / -9.0e+999 (NaN -> null),
        // matching stock (GH#212).
        Some(SqliteValue::Integer(i)) => Ok(Value::Number((*i).into())),
        Some(SqliteValue::Float(f)) => float_to_json(*f),
        Some(other) => Err(FrankenError::function_error(format!(
            "{name} argument {} must be TEXT or BLOB, got {}",
            index + 1,
            other.typeof_str()
        ))),
        None => Err(FrankenError::function_error(format!(
            "{name} missing argument {}",
            index + 1
        ))),
    }
}

fn optional_flags_arg(name: &str, args: &[SqliteValue], index: usize) -> Result<Option<u8>> {
    let Some(value) = args.get(index) else {
        return Ok(None);
    };
    match u8::try_from(value.to_integer()) {
        Ok(flags @ 1..=15) => Ok(Some(flags)),
        _ => Err(FrankenError::function_error(format!(
            "FLAGS parameter to {name}() must be between 1 and 15"
        ))),
    }
}

fn usize_to_i64(name: &str, value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        FrankenError::function_error(format!("{name} result does not fit in i64: {value}"))
    })
}

fn collect_path_args<'a>(
    name: &str,
    args: &'a [SqliteValue],
    start: usize,
) -> Result<Vec<&'a str>> {
    let mut out = Vec::with_capacity(args.len().saturating_sub(start));
    for idx in start..args.len() {
        out.push(path_arg(name, args, idx)?);
    }
    Ok(out)
}

fn normalize_arrow_path_arg(value: &SqliteValue) -> Cow<'_, str> {
    // Stock's abbreviated paths: an INTEGER N is `$[N]` (`$[#-N]` when
    // negative); any other value is read as text, where `$...` is a full path,
    // `[N]` means `$[N]`, and anything else is an object label.
    let path = match value {
        SqliteValue::Integer(index_value) if *index_value >= 0 => {
            return Cow::Owned(format!("$[{index_value}]"));
        }
        SqliteValue::Integer(index_value) => {
            return Cow::Owned(format!("$[#-{}]", index_value.unsigned_abs()));
        }
        SqliteValue::Text(path) => Cow::Borrowed(&**path),
        other => Cow::Owned(other.to_text()),
    };
    if path.starts_with('$') || path.is_empty() {
        return path;
    }
    if path.len() >= 3 && path.starts_with('[') && path.ends_with(']') {
        return Cow::Owned(format!("${path}"));
    }
    let mut quoted = String::with_capacity(path.len() + 4);
    quoted.push_str("$.");
    write_json_escaped_string(path.as_ref(), &mut quoted);
    Cow::Owned(quoted)
}

fn invoke_json_arrow(name: &str, args: &[SqliteValue], double_arrow: bool) -> Result<SqliteValue> {
    if args.len() != 2 {
        return Err(invalid_arity(name, "exactly 2 arguments", args.len()));
    }
    if args.iter().any(SqliteValue::is_null) {
        return Ok(SqliteValue::Null);
    }

    let path = normalize_arrow_path_arg(&args[1]);
    if let SqliteValue::Text(text) = &args[0] {
        if double_arrow {
            if let Some((value, _)) = try_strict_json_extract_single(text, path.as_ref())? {
                return Ok(value);
            }
        } else if let Some(value) = try_strict_json_arrow(text, path.as_ref())? {
            return Ok(value);
        }
    }
    let input = json_arg_value(name, args, 0)?;
    if double_arrow {
        json_extract_value(&input, &[path.as_ref()])
    } else {
        json_arrow_value(&input, path.as_ref())
    }
}

pub struct JsonFunc;

impl ScalarFunction for JsonFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        // TEXT input: stock json() minifies whitespace but preserves every
        // token of the source verbatim (number literals incl. exponent form,
        // string escapes like `\/`, duplicate keys). Parse only to validate,
        // then lexically minify the source rather than re-serialising a parsed
        // `serde_json::Value` (which normalises exponents, unescapes `\/`, and
        // drops duplicate keys) — bd-6b0pe / bd-p2xrc.
        if let SqliteValue::Text(text) = &args[0] {
            return Ok(SqliteValue::Text(json(text)?.into()));
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let encoded = encode_json_text(&input);
        Ok(SqliteValue::Text(SmallText::from_string(encoded)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "json"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbFunc;

impl ScalarFunction for JsonbFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let blob = encode_jsonb_root(&input)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "jsonb"
    }
}

pub struct JsonValidFunc;

impl ScalarFunction for JsonValidFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        let flags = optional_flags_arg(self.name(), args, 1)?;
        let value = match &args[0] {
            SqliteValue::Null => return Ok(SqliteValue::Null),
            SqliteValue::Text(text) => json_valid(text, flags),
            SqliteValue::Blob(bytes) => json_valid_blob(bytes, flags),
            // C SQLite validates a bare SQL numeric via sqlite3_value_text():
            // it renders to its JSON numeric text and is checked against the
            // requested flags. So it is valid JSON/JSON5 (flags 1/2) but is NOT
            // a JSONB blob (flags 4/8 -> 0), and a non-finite REAL renders as
            // non-JSON text (e.g. json_valid(9e999) -> 0).
            SqliteValue::Integer(n) => json_valid(&n.to_string(), flags),
            SqliteValue::Float(f) if f.is_finite() => json_valid(&f.to_string(), flags),
            SqliteValue::Float(_) => 0,
        };
        Ok(SqliteValue::Integer(value))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_valid"
    }
}

pub struct JsonTypeFunc;

impl ScalarFunction for JsonTypeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if let SqliteValue::Text(text) = &args[0] {
            let fast_path = if args.len() == 2 {
                if matches!(args[1], SqliteValue::Null) {
                    None
                } else {
                    path_arg(self.name(), args, 1).ok().map(Some)
                }
            } else {
                Some(None)
            };
            if let Some(path) = fast_path
                && let FastLookup::Resolved(kind) = try_strict_json_type(text, path)?
            {
                return Ok(match kind {
                    Some(kind) => SqliteValue::Text(SmallText::new(kind)),
                    None => SqliteValue::Null,
                });
            }
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let path = if args.len() == 2 {
            if matches!(args[1], SqliteValue::Null) {
                return Ok(SqliteValue::Null);
            }
            Some(path_arg(self.name(), args, 1)?)
        } else {
            None
        };
        Ok(match json_type_value(&input, path)? {
            Some(kind) => SqliteValue::Text(SmallText::new(kind)),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_type"
    }
}

pub struct JsonExtractFunc;

impl JsonExtractFunc {
    /// `json_extract` plus the subtype stock gives its result: an extracted
    /// array or object, and the array built for several paths, are JSON; a
    /// scalar (even a string that looks like JSON) is not.
    fn extract(&self, args: &[SqliteValue]) -> Result<(SqliteValue, Option<u32>)> {
        if args.len() < 2 {
            return Ok((SqliteValue::Null, None));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok((SqliteValue::Null, None));
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok((SqliteValue::Null, None));
        }
        if args.len() == 2
            && let SqliteValue::Text(text) = &args[0]
            && let Ok(path) = path_arg(self.name(), args, 1)
            && let Some(result) = try_strict_json_extract_single(text, path)?
        {
            return Ok(result);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        if args.len() == 2 {
            let path = path_arg(self.name(), args, 1)?;
            let selected = resolve_path(&input, path)?;
            let subtype = matches!(selected, Some(Value::Array(_) | Value::Object(_)))
                .then_some(JSON_SUBTYPE);
            return Ok((
                selected.map_or(Ok(SqliteValue::Null), json_to_sqlite_scalar)?,
                subtype,
            ));
        }
        let paths = collect_path_args(self.name(), args, 1)?;
        Ok((json_extract_value(&input, &paths)?, Some(JSON_SUBTYPE)))
    }
}

impl ScalarFunction for JsonExtractFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(self.extract(args)?.0)
    }

    fn result_subtype_varies(&self) -> bool {
        true
    }

    fn invoke_with_result_subtype(
        &self,
        args: &[SqliteValue],
        _arg_subtypes: &[u32],
    ) -> Result<(SqliteValue, Option<u32>)> {
        self.extract(args)
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_extract"
    }
}

pub struct JsonbExtractFunc;

impl ScalarFunction for JsonbExtractFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 2 {
            return Ok(SqliteValue::Null);
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        if args.len() == 2 {
            let path = path_arg(self.name(), args, 1)?;
            let node = jsonb_extract_single_path_value(&input, path)?;
            // Stock jsonb_extract returns JSONB only where json_extract would
            // return a JSON array/object; a single path resolving to a JSON
            // scalar (or a missing path -> NULL) returns the SQL scalar value,
            // same as json_extract (bd-elre4).
            return match &node {
                Value::Array(_) | Value::Object(_) => Ok(SqliteValue::Blob(Arc::from(
                    encode_jsonb_root(&node)?.as_slice(),
                ))),
                _ => json_to_sqlite_scalar(&node),
            };
        }
        let paths = collect_path_args(self.name(), args, 1)?;
        let blob = jsonb_extract_value(&input, &paths)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_extract"
    }
}

pub struct JsonArrowFunc;

impl ScalarFunction for JsonArrowFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, false)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "json_arrow"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonDoubleArrowFunc;

impl ScalarFunction for JsonDoubleArrowFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, true)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "json_double_arrow"
    }
}

pub struct JsonArrowOperatorFunc;

impl ScalarFunction for JsonArrowOperatorFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, false)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "->"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonDoubleArrowOperatorFunc;

impl ScalarFunction for JsonDoubleArrowOperatorFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, true)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "->>"
    }
}

pub struct JsonArrayFunc;

impl ScalarFunction for JsonArrayFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let s = json_array(args)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_array_with_subtypes(args, arg_subtypes)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_array"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbArrayFunc;

impl ScalarFunction for JsonbArrayFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let blob = jsonb_array(args)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_array_with_subtypes(args, arg_subtypes)?;
        let blob = jsonb(&s)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_array"
    }
}

pub struct JsonObjectFunc;

impl ScalarFunction for JsonObjectFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let s = json_object(args)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_object_with_subtypes(args, arg_subtypes)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_object"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbObjectFunc;

impl ScalarFunction for JsonbObjectFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let blob = jsonb_object(args)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let blob = jsonb_object_with_subtypes(args, arg_subtypes)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_object"
    }
}

pub struct JsonQuoteFunc;

impl ScalarFunction for JsonQuoteFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        // A value returned by another JSON function is already JSON: stock
        // copies it verbatim (`json_quote(json('[1]'))` is `[1]`, not a string).
        let s = match &args[0] {
            SqliteValue::Text(text) if arg_subtypes.first() == Some(&JSON_SUBTYPE) => {
                text.to_string()
            }
            value => json_quote(value)?,
        };
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "json_quote"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonSetFunc;

impl ScalarFunction for JsonSetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            input,
            args,
            arg_subtypes,
            1,
            EditMode::Set,
        )?;
        let encoded = encode_json_text(&edited);
        Ok(SqliteValue::Text(SmallText::from_string(encoded)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_set"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbSetFunc;

impl ScalarFunction for JsonbSetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let blob = json_arg_to_jsonb_blob(self.name(), args, 0)?;
        let edited = edit_jsonb_blob_with_subtypes(
            self.name(),
            blob,
            args,
            arg_subtypes,
            1,
            JsonbEditMode::Set,
        )?;
        Ok(SqliteValue::Blob(Arc::from(edited.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_set"
    }
}

pub struct JsonInsertFunc;

impl ScalarFunction for JsonInsertFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            input,
            args,
            arg_subtypes,
            1,
            EditMode::Insert,
        )?;
        let encoded = encode_json_text(&edited);
        Ok(SqliteValue::Text(SmallText::from_string(encoded)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_insert"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbInsertFunc;

impl ScalarFunction for JsonbInsertFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let blob = json_arg_to_jsonb_blob(self.name(), args, 0)?;
        let edited = edit_jsonb_blob_with_subtypes(
            self.name(),
            blob,
            args,
            arg_subtypes,
            1,
            JsonbEditMode::Insert,
        )?;
        Ok(SqliteValue::Blob(Arc::from(edited.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_insert"
    }
}

pub struct JsonArrayInsertFunc;

impl ScalarFunction for JsonArrayInsertFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited =
            edit_json_array_insert_with_subtypes(self.name(), input, args, arg_subtypes, 1)?;
        Ok(SqliteValue::Text(SmallText::from_string(encode_json_text(
            &edited,
        ))))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_array_insert"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbArrayInsertFunc;

impl ScalarFunction for JsonbArrayInsertFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let blob = json_arg_to_jsonb_blob(self.name(), args, 0)?;
        let edited = edit_jsonb_blob_with_subtypes(
            self.name(),
            blob,
            args,
            arg_subtypes,
            1,
            JsonbEditMode::ArrayInsert,
        )?;
        Ok(SqliteValue::Blob(Arc::from(edited.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_array_insert"
    }
}

pub struct JsonReplaceFunc;

impl ScalarFunction for JsonReplaceFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            input,
            args,
            arg_subtypes,
            1,
            EditMode::Replace,
        )?;
        let encoded = encode_json_text(&edited);
        Ok(SqliteValue::Text(SmallText::from_string(encoded)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_replace"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbReplaceFunc;

impl ScalarFunction for JsonbReplaceFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        self.invoke_with_arg_subtypes(args, &[])
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        check_edit_arity(self.name(), args)?;
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let blob = json_arg_to_jsonb_blob(self.name(), args, 0)?;
        let edited = edit_jsonb_blob_with_subtypes(
            self.name(),
            blob,
            args,
            arg_subtypes,
            1,
            JsonbEditMode::Replace,
        )?;
        Ok(SqliteValue::Blob(Arc::from(edited.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_replace"
    }
}

pub struct JsonRemoveFunc;

impl ScalarFunction for JsonRemoveFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        if args.len() == 1 {
            // With just the JSON argument, validate and return minified.
            let encoded = encode_json_text(&input);
            return Ok(SqliteValue::Text(SmallText::from_string(encoded)));
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }
        let paths = collect_path_args(self.name(), args, 1)?;
        let Some(edited) = json_remove_value(input, &paths)? else {
            return Ok(SqliteValue::Null);
        };
        let encoded = encode_json_text(&edited);
        Ok(SqliteValue::Text(SmallText::from_string(encoded)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_remove"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbRemoveFunc;

impl ScalarFunction for JsonbRemoveFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Ok(SqliteValue::Null);
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let blob = json_arg_to_jsonb_blob(self.name(), args, 0)?;
        if args.len() == 1 {
            return Ok(SqliteValue::Blob(Arc::from(blob.as_slice())));
        }
        let mut editor = JsonbBlobEditor::new(blob);
        for arg in &args[1..] {
            if arg.is_null() {
                return Ok(SqliteValue::Null);
            }
            let path_text = arg.to_text();
            if !path_text.starts_with('$') {
                return Err(bad_json_path(&path_text));
            }
            if path_text.len() == 1 {
                return Ok(SqliteValue::Null);
            }
            editor.apply_path_edit(&path_text, JsonbEditMode::Delete, &[])?;
        }
        Ok(SqliteValue::Blob(Arc::from(editor.blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_remove"
    }
}

pub struct JsonPatchFunc;

impl ScalarFunction for JsonPatchFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(invalid_arity(
                self.name(),
                "exactly 2 arguments",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) || matches!(args[1], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let patch = json_arg_value(self.name(), args, 1)?;
        let merged = json_patch_value(input, patch);
        let encoded = encode_json_text(&merged);
        Ok(SqliteValue::Text(SmallText::from_string(encoded)))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "json_patch"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbPatchFunc;

impl ScalarFunction for JsonbPatchFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(invalid_arity(
                self.name(),
                "exactly 2 arguments",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) || matches!(args[1], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let patch = json_arg_value(self.name(), args, 1)?;
        let merged = json_patch_value(input, patch);
        let blob = encode_jsonb_root(&merged)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "jsonb_patch"
    }
}

pub struct JsonArrayLengthFunc;

impl ScalarFunction for JsonArrayLengthFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if let SqliteValue::Text(text) = &args[0] {
            let fast_path = if args.len() == 2 {
                if matches!(args[1], SqliteValue::Null) {
                    None
                } else {
                    path_arg(self.name(), args, 1).ok().map(Some)
                }
            } else {
                Some(None)
            };
            if let Some(path) = fast_path
                && let FastLookup::Resolved(len) = try_strict_json_array_length(text, path)?
            {
                return Ok(match len {
                    Some(len) => SqliteValue::Integer(usize_to_i64(self.name(), len)?),
                    None => SqliteValue::Null,
                });
            }
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let path = if args.len() == 2 {
            if matches!(args[1], SqliteValue::Null) {
                return Ok(SqliteValue::Null);
            }
            Some(path_arg(self.name(), args, 1)?)
        } else {
            None
        };
        Ok(match json_array_length_value(&input, path)? {
            Some(len) => SqliteValue::Integer(usize_to_i64(self.name(), len)?),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_array_length"
    }
}

pub struct JsonErrorPositionFunc;

impl ScalarFunction for JsonErrorPositionFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let position = match &args[0] {
            SqliteValue::Text(text) => json_error_position(text),
            SqliteValue::Blob(bytes) => json_error_position_blob(bytes),
            // A bare SQL numeric is valid JSON (a JSON number), so there is no
            // parse error: json_error_position(1) = json_error_position(1.5)
            // = json_error_position(9e999) = 0 (C SQLite convention; a
            // non-finite REAL renders as the numeric literal 9.0e+999, GH#212).
            SqliteValue::Integer(_) | SqliteValue::Float(_) => 0,
            other => {
                return Err(FrankenError::function_error(format!(
                    "{} argument 1 must be TEXT or BLOB, got {}",
                    self.name(),
                    other.typeof_str()
                )));
            }
        };
        Ok(SqliteValue::Integer(usize_to_i64(self.name(), position)?))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "json_error_position"
    }
}

pub struct JsonPrettyFunc;

impl ScalarFunction for JsonPrettyFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let indent = if args.len() == 2 {
            if matches!(args[1], SqliteValue::Null) {
                None
            } else {
                Some(text_arg(self.name(), args, 1)?)
            }
        } else {
            None
        };
        let s = json_pretty_value(&input, indent);
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_pretty"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// json_group_array(X) / json_group_object(NAME, VALUE) — JSON aggregates
// ═══════════════════════════════════════════════════════════════════════════

/// `json_group_array(X)` — aggregate argument values into a JSON array.
///
/// SQL NULL becomes JSON `null`. Reuses `json_array` so the element
/// encoding is identical to the scalar constructor.
pub struct JsonGroupArrayFunc;

impl AggregateFunction for JsonGroupArrayFunc {
    /// Each accumulated element is paired with its subtype tag (0 = untagged) so
    /// a JSON-subtyped argument (`json_object('a',1)`) is embedded as a nested
    /// value at finalize instead of being quoted as a string (bd-76x57).
    type State = Vec<(SqliteValue, u32)>;
    fn initial_state(&self) -> Self::State {
        Vec::new()
    }
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.push((args[0].clone(), 0));
        Ok(())
    }
    fn step_with_arg_subtypes(
        &self,
        state: &mut Self::State,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<()> {
        state.push((args[0].clone(), arg_subtypes.first().copied().unwrap_or(0)));
        Ok(())
    }
    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        let (values, subtypes): (Vec<SqliteValue>, Vec<u32>) = state.into_iter().unzip();
        Ok(SqliteValue::Text(SmallText::from_string(
            json_array_with_subtypes(&values, &subtypes)?,
        )))
    }
    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
    fn num_args(&self) -> i32 {
        1
    }
    fn name(&self) -> &str {
        "json_group_array"
    }
}

/// `json_group_object(NAME, VALUE)` — aggregate pairs into a JSON object.
///
/// Reuses `json_object`: the flattened `[k0, v0, k1, v1, …]` buffer feeds
/// `json_object` directly, matching its key/value encoding and validation.
pub struct JsonGroupObjectFunc;

impl AggregateFunction for JsonGroupObjectFunc {
    /// Flattened `[(k0,st0), (v0,stv0), (k1,st1), …]` — each key/value keeps its
    /// subtype tag so a JSON-subtyped value is embedded rather than quoted at
    /// finalize (bd-76x57).
    type State = Vec<(SqliteValue, u32)>;
    fn initial_state(&self) -> Self::State {
        Vec::new()
    }
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        self.step_with_arg_subtypes(state, args, &[])
    }
    fn step_with_arg_subtypes(
        &self,
        state: &mut Self::State,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<()> {
        let Some(label) = group_object_label(&args[0]) else {
            return Ok(());
        };
        state.push((SqliteValue::Text(SmallText::from_string(label)), 0));
        state.push((args[1].clone(), arg_subtypes.get(1).copied().unwrap_or(0)));
        Ok(())
    }
    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        let (values, subtypes): (Vec<SqliteValue>, Vec<u32>) = state.into_iter().unzip();
        Ok(SqliteValue::Text(SmallText::from_string(
            json_object_with_subtypes(&values, &subtypes)?,
        )))
    }
    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
    fn num_args(&self) -> i32 {
        2
    }
    fn name(&self) -> &str {
        "json_group_object"
    }
}

/// `jsonb_group_array(X)` — JSONB aggregate variant of `json_group_array`.
///
/// The state retains argument subtypes so nested JSON values are embedded
/// rather than quoted, matching the text aggregate's subtype semantics.
pub struct JsonbGroupArrayFunc;

impl AggregateFunction for JsonbGroupArrayFunc {
    type State = Vec<(SqliteValue, u32)>;

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.push((args[0].clone(), 0));
        Ok(())
    }

    fn step_with_arg_subtypes(
        &self,
        state: &mut Self::State,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<()> {
        state.push((args[0].clone(), arg_subtypes.first().copied().unwrap_or(0)));
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        let mut values = Vec::with_capacity(state.len());
        for (value, subtype) in state {
            values.push(sqlite_to_json_with_subtype(&value, subtype)?);
        }
        let blob = encode_jsonb_root(&Value::Array(values))?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "jsonb_group_array"
    }
}

/// `jsonb_group_object(NAME, VALUE)` — JSONB aggregate variant of
/// `json_group_object`.
///
/// Object members are encoded directly into the JSONB payload so duplicate
/// labels remain in row order instead of being collapsed by a map round-trip.
pub struct JsonbGroupObjectFunc;

impl AggregateFunction for JsonbGroupObjectFunc {
    type State = Vec<(String, SqliteValue, u32)>;

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        self.step_with_arg_subtypes(state, args, &[])
    }

    fn step_with_arg_subtypes(
        &self,
        state: &mut Self::State,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<()> {
        let Some(label) = group_object_label(&args[0]) else {
            return Ok(());
        };
        state.push((
            label,
            args[1].clone(),
            arg_subtypes.get(1).copied().unwrap_or(0),
        ));
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        let mut payload = Vec::new();
        for (key, value, subtype) in state {
            append_jsonb_string(&key, &mut payload)?;
            let json_value = sqlite_to_json_with_subtype(&value, subtype)?;
            encode_jsonb_value(&json_value, &mut payload)?;
        }
        let mut blob = Vec::new();
        append_jsonb_node(JSONB_OBJECT_TYPE, &payload, &mut blob)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "jsonb_group_object"
    }
}

/// One row of a JSON aggregate's window frame: the object label (`None` for a
/// row whose label is NULL), the value, and the value's subtype.
type JsonWindowRow = (Option<SqliteValue>, SqliteValue, u32);

/// `json_group_array`, `json_group_object` and their `jsonb_` forms as window
/// functions (bd-ffwsd).
///
/// The frame is a queue of rows and the inverse removes the oldest, as
/// stock's jsonGroupInverse removes the first element. A row
/// whose label is NULL keeps its place in the queue but adds no member, as
/// stock's jsonObjectStep keeps a placeholder for it.
pub struct JsonGroupWindowFunc {
    name: &'static str,
    object: bool,
    jsonb: bool,
}

impl JsonGroupWindowFunc {
    const ALL: [Self; 4] = [
        Self {
            name: "json_group_array",
            object: false,
            jsonb: false,
        },
        Self {
            name: "json_group_object",
            object: true,
            jsonb: false,
        },
        Self {
            name: "jsonb_group_array",
            object: false,
            jsonb: true,
        },
        Self {
            name: "jsonb_group_object",
            object: true,
            jsonb: true,
        },
    ];
}

impl WindowFunction for JsonGroupWindowFunc {
    type State = VecDeque<JsonWindowRow>;

    fn initial_state(&self) -> Self::State {
        VecDeque::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        self.step_with_arg_subtypes(state, args, &[])
    }

    fn step_with_arg_subtypes(
        &self,
        state: &mut Self::State,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<()> {
        if !self.object {
            state.push_back((
                None,
                args[0].clone(),
                arg_subtypes.first().copied().unwrap_or(0),
            ));
            return Ok(());
        }
        // The label is read as for the aggregate.
        let label = group_object_label(&args[0])
            .map(|text| SqliteValue::Text(SmallText::from_string(text)));
        state.push_back((
            label,
            args[1].clone(),
            arg_subtypes.get(1).copied().unwrap_or(0),
        ));
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.pop_front();
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let mut values = Vec::with_capacity(state.len() * 2);
        let mut subtypes = Vec::with_capacity(state.len() * 2);
        for (label, value, subtype) in state {
            if self.object {
                let Some(label) = label else {
                    continue;
                };
                values.push(label.clone());
                subtypes.push(0);
            }
            values.push(value.clone());
            subtypes.push(*subtype);
        }
        Ok(match (self.object, self.jsonb) {
            (false, false) => SqliteValue::Text(SmallText::from_string(json_array_with_subtypes(
                &values, &subtypes,
            )?)),
            (true, false) => SqliteValue::Text(SmallText::from_string(json_object_with_subtypes(
                &values, &subtypes,
            )?)),
            (false, true) => {
                SqliteValue::Blob(Arc::from(jsonb_array_with_subtypes(&values, &subtypes)?))
            }
            (true, true) => {
                SqliteValue::Blob(Arc::from(jsonb_object_with_subtypes(&values, &subtypes)?))
            }
        })
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        if self.object { 2 } else { 1 }
    }

    fn name(&self) -> &str {
        self.name
    }
}

/// Register JSON1 scalar functions into a `FunctionRegistry`.
pub fn register_json_scalars(registry: &mut FunctionRegistry) {
    registry.register_scalar(JsonFunc);
    registry.register_scalar(JsonbFunc);
    registry.register_scalar(JsonValidFunc);
    registry.register_scalar(JsonTypeFunc);
    registry.register_scalar(JsonExtractFunc);
    registry.register_scalar(JsonbExtractFunc);
    registry.register_scalar(JsonArrowFunc);
    registry.register_scalar(JsonDoubleArrowFunc);
    registry.register_scalar(JsonArrowOperatorFunc);
    registry.register_scalar(JsonDoubleArrowOperatorFunc);
    registry.register_scalar(JsonArrayFunc);
    registry.register_scalar(JsonbArrayFunc);
    registry.register_scalar(JsonObjectFunc);
    registry.register_scalar(JsonbObjectFunc);
    registry.register_scalar(JsonQuoteFunc);
    registry.register_scalar(JsonSetFunc);
    registry.register_scalar(JsonbSetFunc);
    registry.register_scalar(JsonInsertFunc);
    registry.register_scalar(JsonbInsertFunc);
    registry.register_scalar(JsonArrayInsertFunc);
    registry.register_scalar(JsonbArrayInsertFunc);
    registry.register_scalar(JsonReplaceFunc);
    registry.register_scalar(JsonbReplaceFunc);
    registry.register_scalar(JsonRemoveFunc);
    registry.register_scalar(JsonbRemoveFunc);
    registry.register_scalar(JsonPatchFunc);
    registry.register_scalar(JsonbPatchFunc);
    registry.register_scalar(JsonArrayLengthFunc);
    registry.register_scalar(JsonErrorPositionFunc);
    registry.register_scalar(JsonPrettyFunc);
    registry.register_aggregate(JsonGroupArrayFunc);
    registry.register_aggregate(JsonGroupObjectFunc);
    registry.register_aggregate(JsonbGroupArrayFunc);
    registry.register_aggregate(JsonbGroupObjectFunc);
    for window in JsonGroupWindowFunc::ALL {
        registry.register_window(window);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_func::FunctionRegistry;

    fn stock_json_rows(
        stock: &rusqlite::Connection,
        sql: &str,
        args: &[SqliteValue],
    ) -> rusqlite::Result<Vec<Vec<SqliteValue>>> {
        let parameters: Vec<rusqlite::types::Value> = args
            .iter()
            .map(|value| match value {
                SqliteValue::Null => rusqlite::types::Value::Null,
                SqliteValue::Integer(value) => rusqlite::types::Value::Integer(*value),
                SqliteValue::Float(value) => rusqlite::types::Value::Real(*value),
                SqliteValue::Text(value) => rusqlite::types::Value::Text(value.to_string()),
                SqliteValue::Blob(value) => rusqlite::types::Value::Blob(value.to_vec()),
            })
            .collect();
        let mut statement = stock.prepare(sql)?;
        let width = statement.column_count();
        let rows = statement
            .query_map(rusqlite::params_from_iter(parameters), |row| {
                (0..width)
                    .map(|index| {
                        row.get::<_, rusqlite::types::Value>(index)
                            .map(|value| match value {
                                rusqlite::types::Value::Null => SqliteValue::Null,
                                rusqlite::types::Value::Integer(value) => {
                                    SqliteValue::Integer(value)
                                }
                                rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                                rusqlite::types::Value::Text(value) => {
                                    SqliteValue::Text(value.into())
                                }
                                rusqlite::types::Value::Blob(value) => {
                                    SqliteValue::Blob(value.into())
                                }
                            })
                    })
                    .collect()
            })?
            .collect();
        rows
    }

    fn stock_json_scalar(
        stock: &rusqlite::Connection,
        sql: &str,
        args: &[SqliteValue],
    ) -> SqliteValue {
        stock_json_rows(stock, sql, args)
            .unwrap()
            .remove(0)
            .remove(0)
    }

    fn normalized_jsonb(stock: &rusqlite::Connection, value: SqliteValue) -> SqliteValue {
        if matches!(value, SqliteValue::Blob(_)) {
            // Stock independently decodes our output. JSONB permits several
            // string encodings; membership and values must be identical.
            stock_json_scalar(stock, "SELECT json(?1)", &[value])
        } else {
            value
        }
    }

    #[test]
    fn duplicate_members_match_live_sqlite_oracle() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let cases = [
            ("json", vec![]),
            ("jsonb", vec![]),
            ("json_type", vec![text("$.a")]),
            ("json_valid", vec![SqliteValue::Integer(10)]),
            ("json_extract", vec![text("$.a")]),
            ("json_extract", vec![text("$.a"), text("$.missing")]),
            ("jsonb_extract", vec![text("$")]),
            ("->", vec![text("$.a")]),
            ("->>", vec![text("$.a")]),
            ("json_set", vec![text("$.a"), SqliteValue::Integer(9)]),
            ("jsonb_set", vec![text("$.a"), SqliteValue::Integer(9)]),
            ("json_replace", vec![text("$.a"), SqliteValue::Integer(9)]),
            ("jsonb_replace", vec![text("$.a"), SqliteValue::Integer(9)]),
            (
                "json_insert",
                vec![
                    text("$.a"),
                    SqliteValue::Integer(9),
                    text("$.new"),
                    SqliteValue::Integer(4),
                ],
            ),
            (
                "jsonb_insert",
                vec![
                    text("$.a"),
                    SqliteValue::Integer(9),
                    text("$.new"),
                    SqliteValue::Integer(4),
                ],
            ),
            ("json_remove", vec![text("$.a")]),
            ("json_remove", vec![text("$.a"), text("$.a")]),
            ("jsonb_remove", vec![text("$.a")]),
            ("json_patch", vec![text(r#"{"a":null}"#)]),
            ("json_patch", vec![text(r#"{"a":8,"a":9}"#)]),
            ("jsonb_patch", vec![text(r#"{"a":null,"a":9}"#)]),
            ("json_pretty", vec![]),
        ];
        let mut documents = vec![
            r#"{"a":1,"b":0,"a":2}"#.to_owned(),
            r#"{"a":{"x":1,"x":2},"b":0,"a":3}"#.to_owned(),
            r#"{"a":1.50,"a":1E2}"#.to_owned(),
            r#"{"a":null,"a":[],"":1,"":2}"#.to_owned(),
            "{a:1,b:0,a:2,}".to_owned(),
            r#"{"a":1,"b":[2,3]}"#.to_owned(),
            "{}".to_owned(),
        ];
        let mut large = vec![r#""a":1"#.to_owned()];
        large.extend((0..32).map(|index| format!(r#""k{index}":{index}"#)));
        large.push(r#""a":2"#.to_owned());
        documents.push(format!("{{{}}}", large.join(",")));
        for document in documents {
            let stock_binary = stock_json_scalar(&stock, "SELECT jsonb(?1)", &[text(&document)]);
            for source in [
                text(&document),
                SqliteValue::Blob(document.as_bytes().into()),
                stock_binary,
            ] {
                for (name, tail) in &cases {
                    let mut args = vec![source.clone()];
                    args.extend(tail.iter().cloned());
                    let sql = if matches!(*name, "->" | "->>") {
                        format!("SELECT ?1 {name} ?2")
                    } else {
                        let parameters: Vec<_> =
                            (1..=args.len()).map(|index| format!("?{index}")).collect();
                        format!("SELECT {name}({})", parameters.join(","))
                    };
                    let expected = stock_json_scalar(&stock, &sql, &args);
                    let actual = registry
                        .find_scalar(name, args.len() as i32)
                        .unwrap()
                        .invoke(&args)
                        .unwrap();
                    assert_eq!(
                        std::mem::discriminant(&actual),
                        std::mem::discriminant(&expected),
                        "{sql}: {args:?}"
                    );
                    assert_eq!(
                        normalized_jsonb(&stock, actual),
                        normalized_jsonb(&stock, expected),
                        "{sql}: {args:?}"
                    );
                }
                for recursive in [false, true] {
                    let name = if recursive { "json_tree" } else { "json_each" };
                    let actual = match (&source, recursive) {
                        (SqliteValue::Text(input), false) => json_each(input, None),
                        (SqliteValue::Text(input), true) => json_tree(input, None),
                        (SqliteValue::Blob(input), false) => json_each_blob(input, None),
                        (SqliteValue::Blob(input), true) => json_tree_blob(input, None),
                        _ => unreachable!(),
                    }
                    .unwrap();
                    let actual: Vec<Vec<_>> = actual
                        .into_iter()
                        .map(|row| {
                            vec![
                                row.key,
                                row.value,
                                text(row.type_name),
                                row.atom,
                                SqliteValue::Text(row.fullkey.into()),
                                SqliteValue::Text(row.path.into()),
                            ]
                        })
                        .collect();
                    let sql = format!("SELECT key,value,type,atom,fullkey,path FROM {name}(?1)");
                    assert_eq!(
                        actual,
                        stock_json_rows(&stock, &sql, &[source.clone()]).unwrap(),
                        "{sql}: {source:?}"
                    );
                }
                let canonical = JsonFunc.invoke(&[source.clone()]).unwrap();
                for name in ["json_array", "jsonb_array"] {
                    let actual = registry
                        .find_scalar(name, 1)
                        .unwrap()
                        .invoke_with_arg_subtypes(&[canonical.clone()], &[JSON_SUBTYPE])
                        .unwrap();
                    let expected = stock_json_scalar(
                        &stock,
                        &format!("SELECT {name}(json(?1))"),
                        &[source.clone()],
                    );
                    assert_eq!(
                        normalized_jsonb(&stock, actual),
                        normalized_jsonb(&stock, expected),
                        "{name}: {source:?}"
                    );
                }
            }
        }
        for document in [r#"{"a":1,"b":2}"#, r#"{"a":1,"a":2}"#] {
            assert_eq!(
                JsonbFunc.invoke(&[text(document)]).unwrap(),
                stock_json_scalar(&stock, "SELECT jsonb(?1)", &[text(document)])
            );
        }
        // Escape spellings denote the same key for lookup, but both members
        // survive. Preserving a label's original escape spelling in rendered
        // JSON and fullkey is a separate lexical-format concern.
        let escaped = text(r#"{"a":1,"\u0061":2}"#);
        let escaped_binary = stock_json_scalar(&stock, "SELECT jsonb(?1)", &[escaped.clone()]);
        for source in [escaped, escaped_binary] {
            let args = [source.clone(), text("$.a")];
            assert_eq!(
                JsonExtractFunc.invoke(&args).unwrap(),
                stock_json_scalar(&stock, "SELECT json_extract(?1,?2)", &args)
            );
            let rows = match &source {
                SqliteValue::Text(input) => json_each(input, None),
                SqliteValue::Blob(input) => json_each_blob(input, None),
                _ => unreachable!(),
            }
            .unwrap();
            let rows: Vec<Vec<_>> = rows
                .into_iter()
                .map(|row| vec![row.key, row.value])
                .collect();
            assert_eq!(
                rows,
                stock_json_rows(&stock, "SELECT key,value FROM json_each(?1)", &[source]).unwrap()
            );
        }
        for document in [r#"{"a":1,"a":}"#, r#"{"a":1,"a":2"#] {
            let args = [text(document), text("$.a")];
            assert!(JsonExtractFunc.invoke(&args).is_err());
            assert!(stock_json_rows(&stock, "SELECT json_extract(?1,?2)", &args).is_err());
        }
    }

    #[test]
    fn duplicate_aggregate_members_match_live_sqlite_oracle() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        stock
            .execute_batch(
                "CREATE TABLE entries(k,v); INSERT INTO entries VALUES \
            ('a',1),('a',2),('b',3),(char(0)||'a',4),(char(0)||'b',5)",
            )
            .unwrap();
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let entries = [("a", 1), ("a", 2), ("b", 3), ("\0a", 4), ("\0b", 5)];
        for name in ["json_group_object", "jsonb_group_object"] {
            let aggregate = registry.find_aggregate(name, 2).unwrap();
            let mut state = aggregate.initial_state();
            for (key, value) in entries {
                aggregate
                    .step(&mut state, &[text(key), SqliteValue::Integer(value)])
                    .unwrap();
            }
            let actual = aggregate.finalize(state).unwrap();
            let expected =
                stock_json_scalar(&stock, &format!("SELECT {name}(k,v) FROM entries"), &[]);
            assert_eq!(
                normalized_jsonb(&stock, actual.clone()),
                normalized_jsonb(&stock, expected.clone()),
                "{name}"
            );
            assert_eq!(
                JsonFunc.invoke(&[actual]).unwrap(),
                JsonFunc.invoke(&[expected]).unwrap()
            );
        }
        for name in ["json_group_array", "jsonb_group_array"] {
            let aggregate = registry.find_aggregate(name, 1).unwrap();
            let mut state = aggregate.initial_state();
            let document = text(r#"{"a":1,"a":2}"#);
            for _ in entries {
                aggregate
                    .step_with_arg_subtypes(&mut state, &[document.clone()], &[JSON_SUBTYPE])
                    .unwrap();
            }
            let actual = aggregate.finalize(state).unwrap();
            let expected = stock_json_scalar(
                &stock,
                &format!("SELECT {name}(json(?1)) FROM entries"),
                &[document],
            );
            assert_eq!(
                normalized_jsonb(&stock, actual.clone()),
                normalized_jsonb(&stock, expected.clone()),
                "{name}"
            );
            assert_eq!(
                JsonFunc.invoke(&[actual]).unwrap(),
                JsonFunc.invoke(&[expected]).unwrap()
            );
        }
    }

    #[test]
    fn duplicate_object_members_survive_parsers_jsonb_and_table_walks() {
        let canonical = r#"{"a":{"x":1,"x":2},"b":0,"a":3}"#;
        for input in [canonical, "{a:{x:1,x:2},b:0,a:3}"] {
            let binary = jsonb(input).unwrap();
            assert_eq!(json_from_jsonb(&binary).unwrap(), canonical);
            assert_eq!(
                json_extract(input, &["$.a.x"]).unwrap(),
                SqliteValue::Integer(1)
            );
            assert_eq!(
                json_arrow(input, "$.a").unwrap(),
                SqliteValue::Text(r#"{"x":1,"x":2}"#.into())
            );
            for rows in [
                json_each(input, None).unwrap(),
                json_each_blob(&binary, None).unwrap(),
            ] {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0].key, SqliteValue::Text("a".into()));
                assert_eq!(rows[0].value, SqliteValue::Text(r#"{"x":1,"x":2}"#.into()));
                assert_eq!(rows[1].key, SqliteValue::Text("b".into()));
                assert_eq!(rows[2].key, SqliteValue::Text("a".into()));
                assert_eq!(rows[2].value, SqliteValue::Integer(3));
            }
            for rows in [
                json_tree(input, None).unwrap(),
                json_tree_blob(&binary, None).unwrap(),
            ] {
                assert_eq!(rows.len(), 6);
                assert_eq!(rows[2].fullkey, "$.a.x");
                assert_eq!(rows[3].fullkey, "$.a.x");
                assert_eq!(rows[2].value, SqliteValue::Integer(1));
                assert_eq!(rows[3].value, SqliteValue::Integer(2));
            }
        }
        // Different escape spellings of a label identify the same first key.
        let escaped = r#"{"a":1,"\u0061":2}"#;
        assert_eq!(json_each(escaped, None).unwrap().len(), 2);
        assert_eq!(
            json_extract(escaped, &["$.a"]).unwrap(),
            SqliteValue::Integer(1)
        );
        for malformed in [r#"{"a":1,"a":}"#, r#"{"a":1,"a":2"#] {
            assert!(json_extract(malformed, &["$.a"]).is_err());
            assert!(jsonb(malformed).is_err());
        }
    }

    #[test]
    fn duplicate_object_edits_address_only_the_first_member() {
        let input = r#"{"a":1,"b":0,"a":2}"#;
        let replacement = [("$.a", SqliteValue::Integer(9))];
        for edited in [
            json_set(input, &replacement).unwrap(),
            json_replace(input, &replacement).unwrap(),
            json_from_jsonb(&jsonb_set(input, &replacement).unwrap()).unwrap(),
            json_from_jsonb(&jsonb_replace(input, &replacement).unwrap()).unwrap(),
        ] {
            assert_eq!(edited, r#"{"a":9,"b":0,"a":2}"#);
        }
        assert_eq!(json_insert(input, &replacement).unwrap(), input);
        assert_eq!(json_remove(input, &["$.a"]).unwrap(), r#"{"b":0,"a":2}"#);
        assert_eq!(json_remove(input, &["$.a", "$.a"]).unwrap(), r#"{"b":0}"#);
        assert_eq!(
            json_set(
                r#"{"a":{"x":1,"x":2},"a":3}"#,
                &[("$.a.x", SqliteValue::Integer(7))]
            )
            .unwrap(),
            r#"{"a":{"x":7,"x":2},"a":3}"#
        );
        for (target, patch, expected) in [
            (input, r#"{"a":null}"#, r#"{"b":0,"a":2}"#),
            (input, r#"{"a":9,"a":10}"#, r#"{"a":10,"b":0,"a":2}"#),
            (input, r#"{"a":null,"a":9}"#, r#"{"b":0,"a":9}"#),
            ("{}", r#"{"a":1,"a":2}"#, r#"{"a":2}"#),
        ] {
            assert_eq!(json_patch(target, patch).unwrap(), expected);
            assert_eq!(
                json_from_jsonb(&jsonb_patch(target, patch).unwrap()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn duplicate_lookup_remains_correct_across_large_object_edits() {
        let mut members = vec![r#""a":1"#.to_owned()];
        members.extend((0..32).map(|index| format!(r#""k{index}":{index}"#)));
        members.extend([r#""a":2"#.to_owned(), r#""a":3"#.to_owned()]);
        let mut document = format!("{{{}}}", members.join(","));
        for value in 1..=3 {
            assert_eq!(
                json_extract(&document, &["$.a"]).unwrap(),
                SqliteValue::Integer(value)
            );
            assert_eq!(
                json_extract(&document, &["$.k31"]).unwrap(),
                SqliteValue::Integer(31)
            );
            document = json_remove(&document, &["$.a"]).unwrap();
        }
        assert_eq!(
            json_extract(&document, &["$.a"]).unwrap(),
            SqliteValue::Null
        );
        // Shrink below the indexed-object threshold, then grow through it again.
        for index in 0..24 {
            document = json_remove(&document, &[&format!("$.k{index}")]).unwrap();
        }
        for index in 0..24 {
            document = json_set(
                &document,
                &[(&format!("$.k{index}"), SqliteValue::Integer(index))],
            )
            .unwrap();
        }
        for index in 0..32 {
            assert_eq!(
                json_extract(&document, &[&format!("$.k{index}")]).unwrap(),
                SqliteValue::Integer(index)
            );
        }
        assert_eq!(
            json_from_jsonb(&jsonb(&document).unwrap()).unwrap(),
            document
        );
        // The unique-label binary encoding remains exactly the same format.
        assert_eq!(
            jsonb(r#"{"a":1,"b":2}"#).unwrap(),
            b"\x8c\x17a\x131\x17b\x132"
        );
        assert_eq!(
            jsonb(r#"{"a":1,"a":2}"#).unwrap(),
            b"\x8c\x17a\x131\x17a\x132"
        );
    }

    #[test]
    fn reserved_number_tag_strings_remain_text() {
        for repetitions in 1..=3 {
            for suffix in ["", "9e999", "1.50", "null,0", "\"\\\n", "ordinary"] {
                let raw = format!("{}{suffix}", NUMBER_TEXT_TAG.repeat(repetitions));
                let quoted = serde_json::to_string(&raw).unwrap();
                let value = SqliteValue::Text(raw.clone().into());
                assert_eq!(json_type(&quoted, None).unwrap(), Some("text"));
                assert_eq!(json_extract(&quoted, &["$"]).unwrap(), value);
                assert_eq!(
                    json_arrow(&quoted, "$").unwrap(),
                    SqliteValue::Text(quoted.clone().into())
                );
                assert_eq!(json(&quoted).unwrap(), quoted);
                assert_eq!(json_set(&quoted, &[]).unwrap(), quoted);
                assert_eq!(json_pretty(&quoted, None).unwrap(), quoted);
                assert_eq!(json_quote(&value).unwrap(), quoted);
                let array = json_array(&[value]).unwrap();
                assert_eq!(array, format!("[{quoted}]"));
                assert_eq!(json_array_length(&array, None).unwrap(), Some(1));
                let mut round_trip = quoted.clone();
                for _ in 0..3 {
                    round_trip = json_from_jsonb(&jsonb(&round_trip).unwrap()).unwrap();
                    assert_eq!(round_trip, quoted);
                }
            }
        }
        for raw in [
            "\0ordinary".to_owned(),
            "\0\0fsqlite".to_owned(),
            format!("ordinary{NUMBER_TEXT_TAG}9e999"),
        ] {
            let quoted = serde_json::to_string(&raw).unwrap();
            assert_eq!(
                json_extract(&quoted, &["$"]).unwrap(),
                SqliteValue::Text(raw.into())
            );
            assert_eq!(json_set(&quoted, &[]).unwrap(), quoted);
        }
    }

    #[test]
    fn reserved_number_tag_values_and_keys_coexist_with_real_numbers() {
        let raw = format!("{NUMBER_TEXT_TAG}null,0");
        let quoted = serde_json::to_string(&raw).unwrap();
        let doc = format!(r#"{{{quoted}:{quoted},"n":1e+02,"big":9e999}}"#);
        let path = format!("$.{quoted}");
        assert_eq!(json_type(&doc, Some(&path)).unwrap(), Some("text"));
        assert_eq!(
            json_extract(&doc, &[&path]).unwrap(),
            SqliteValue::Text(raw.clone().into())
        );
        assert_eq!(json_type(&doc, Some("$.n")).unwrap(), Some("real"));
        assert_eq!(
            json_extract(&doc, &["$.n"]).unwrap(),
            SqliteValue::Float(100.0)
        );
        assert_eq!(
            json_extract(&doc, &["$.big"]).unwrap(),
            SqliteValue::Float(f64::INFINITY)
        );
        assert_eq!(json_set(&doc, &[]).unwrap(), doc);
        assert_eq!(json_from_jsonb(&jsonb(&doc).unwrap()).unwrap(), doc);
        for rows in [
            json_each(&doc, None).unwrap(),
            json_each_blob(&jsonb(&doc).unwrap(), None).unwrap(),
        ] {
            assert_eq!(rows[0].key, SqliteValue::Text(raw.clone().into()));
            assert_eq!(rows[0].value, SqliteValue::Text(raw.clone().into()));
            assert_eq!(rows[0].atom, SqliteValue::Text(raw.clone().into()));
            assert_eq!(rows[0].type_name, "text");
        }
        let tree = json_tree(&doc, None).unwrap();
        assert_eq!(tree[1].value, SqliteValue::Text(raw.into()));
        assert_eq!(tree[1].type_name, "text");

        // A different escape spelling still denotes the same original text.
        let escaped = r#""\u0000\u0000\u0066sqlite\u0001nonfinite\u00009e999""#;
        assert_eq!(
            json_extract(escaped, &["$"]).unwrap(),
            SqliteValue::Text(format!("{NUMBER_TEXT_TAG}9e999").into())
        );
    }

    #[test]
    fn reserved_number_tag_json5_strings_are_not_numeric_carriers() {
        let raw = format!("{NUMBER_TEXT_TAG}9e999");
        let quoted = serde_json::to_string(&raw).unwrap();
        let input = format!("{{s:{quoted},n:Infinity,}}");
        assert_eq!(json_type(&input, Some("$.s")).unwrap(), Some("text"));
        assert_eq!(
            json_extract(&input, &["$.s"]).unwrap(),
            SqliteValue::Text(raw.clone().into())
        );
        assert_eq!(
            json_set(&input, &[]).unwrap(),
            format!(r#"{{"s":{quoted},"n":9e999}}"#)
        );
        let single_quoted = r"'\0\0fsqlite\x01nonfinite\x009e999'";
        assert_eq!(
            json_extract(single_quoted, &["$"]).unwrap(),
            SqliteValue::Text(raw.into())
        );
        assert_eq!(
            json_from_jsonb(&jsonb(single_quoted).unwrap()).unwrap(),
            quoted
        );
    }

    #[test]
    fn reserved_number_tag_constructors_mutations_and_subtypes_preserve_strings() {
        let raw = format!("{NUMBER_TEXT_TAG}null,0");
        let quoted = serde_json::to_string(&raw).unwrap();
        let value = SqliteValue::Text(raw.clone().into());
        let array = format!("[{quoted}]");
        let object = format!("{{{quoted}:{quoted}}}");
        assert_eq!(json_group_array(&[value.clone()]).unwrap(), array);
        assert_eq!(
            json_from_jsonb(&jsonb_group_array(&[value.clone()]).unwrap()).unwrap(),
            array
        );
        assert_eq!(
            json_object(&[value.clone(), value.clone()]).unwrap(),
            object
        );
        assert_eq!(
            json_from_jsonb(&jsonb_object(&[value.clone(), value.clone()]).unwrap()).unwrap(),
            object
        );
        // The group-object label ends at its first NUL, as stock's
        // jsonObjectStep measures it (sqlite3Strlen30): the tag starts with
        // NUL, so the label is "". The value keeps every byte.
        let group_object = format!("{{\"\":{quoted}}}");
        assert_eq!(
            json_group_object(&[(value.clone(), value.clone())]).unwrap(),
            group_object
        );
        assert_eq!(
            json_from_jsonb(&jsonb_group_object(&[(value.clone(), value.clone())]).unwrap())
                .unwrap(),
            group_object
        );
        for edited in [
            json_set("[]", &[("$[#]", value.clone())]).unwrap(),
            json_insert("[]", &[("$[#]", value.clone())]).unwrap(),
            json_replace("[0]", &[("$[0]", value.clone())]).unwrap(),
            json_array_insert("[]", &[("$[0]", value.clone())]).unwrap(),
        ] {
            assert_eq!(edited, array);
        }
        let subtyped = [SqliteValue::Text(quoted.into())];
        assert_eq!(
            json_array_with_subtypes(&subtyped, &[JSON_SUBTYPE]).unwrap(),
            array
        );
        assert_eq!(
            json_from_jsonb(&jsonb_array_with_subtypes(&subtyped, &[JSON_SUBTYPE]).unwrap())
                .unwrap(),
            array
        );
        for window in JsonGroupWindowFunc::ALL {
            let mut state = window.initial_state();
            let args = if window.object {
                vec![value.clone(), value.clone()]
            } else {
                vec![value.clone()]
            };
            window.step(&mut state, &args).unwrap();
            let output = window.value(&state).unwrap();
            let output = match output {
                SqliteValue::Text(text) => text.to_string(),
                SqliteValue::Blob(bytes) => json_from_jsonb(&bytes).unwrap(),
                other => panic!("unexpected JSON window value: {other:?}"),
            };
            assert_eq!(&output, if window.object { &group_object } else { &array });
            window.inverse(&mut state, &args).unwrap();
            window.step(&mut state, &args).unwrap();
            assert_eq!(state.len(), 1);
        }
    }

    #[test]
    fn reserved_number_tag_jsonb_text_nodes_and_labels_remain_text() {
        let raw = format!("{NUMBER_TEXT_TAG}{NUMBER_TEXT_TAG}9e999");
        let quoted = serde_json::to_string(&raw).unwrap();
        for node_type in [
            JSONB_TEXT_TYPE,
            JSONB_TEXT_JSON_TYPE,
            JSONB_TEXT5_TYPE,
            JSONB_TEXTRAW_TYPE,
        ] {
            let payload = if matches!(node_type, JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE) {
                &quoted.as_bytes()[1..quoted.len() - 1]
            } else {
                raw.as_bytes()
            };
            let mut node = Vec::new();
            append_jsonb_node(node_type, payload, &mut node).unwrap();
            assert_eq!(json_from_jsonb(&node).unwrap(), quoted);
            let row = json_each_blob(&node, None).unwrap().remove(0);
            assert_eq!(row.type_name, "text");
            assert_eq!(row.value, SqliteValue::Text(raw.clone().into()));
            let mut object_payload = node.clone();
            object_payload.extend_from_slice(&node);
            let mut object = Vec::new();
            append_jsonb_node(JSONB_OBJECT_TYPE, &object_payload, &mut object).unwrap();
            assert_eq!(
                json_from_jsonb(&object).unwrap(),
                format!("{{{quoted}:{quoted}}}")
            );
            let row = json_each_blob(&object, None).unwrap().remove(0);
            assert_eq!(row.key, SqliteValue::Text(raw.clone().into()));
            assert_eq!(row.value, SqliteValue::Text(raw.clone().into()));
        }
        for (node_type, payload) in [
            (JSONB_FLOAT_TYPE, b"1e2".as_slice()),
            (JSONB_FLOAT_TYPE, b"9e999".as_slice()),
            (JSONB_INT_TYPE, b"1".as_slice()),
            (JSONB_NULL_TYPE, b"".as_slice()),
            (JSONB_ARRAY_TYPE, b"".as_slice()),
        ] {
            let mut payload_nodes = Vec::new();
            append_jsonb_node(node_type, payload, &mut payload_nodes).unwrap();
            append_jsonb_node(JSONB_NULL_TYPE, &[], &mut payload_nodes).unwrap();
            let mut object = Vec::new();
            append_jsonb_node(JSONB_OBJECT_TYPE, &payload_nodes, &mut object).unwrap();
            assert!(json_from_jsonb(&object).is_err());
            assert_eq!(
                json_valid_blob(&object, Some(JSON_VALID_JSONB_STRICT_FLAG)),
                0
            );
        }
    }

    #[test]
    fn reserved_number_tag_rewrite_preserves_malformed_string_errors() {
        let prefix = r#""\u0000\u0000fsqlite\u0001nonfinite\u0000"#;
        for input in [
            format!("{prefix}9e999"),
            format!("{prefix}\\q\""),
            format!("{prefix}\\\""),
            format!("{prefix}9e999\" trailing"),
            format!("{prefix}null,0\":1"),
            format!("[{prefix}9e999\",]"),
        ] {
            assert!(parse_json_text(&input).is_err(), "{input}");
            assert_eq!(json_valid(&input, None), 0, "{input}");
        }
    }

    #[test]
    fn number_text_rewrite_rejects_malformed_numbers() {
        let mut tokens = vec![
            "01e999".to_owned(),
            "-01e999".to_owned(),
            "00e999".to_owned(),
            "01.0e999".to_owned(),
            "01.50".to_owned(),
        ];
        tokens.push(format!("0{}", "9".repeat(400)));
        tokens.push(format!("-0{}", "9".repeat(400)));
        for token in tokens {
            for input in [
                token.clone(),
                format!("[{token}]"),
                format!(r#"{{"n":{token}}}"#),
            ] {
                assert!(parse_json_text(&input).is_err(), "{input}");
                assert_eq!(json_valid(&input, None), 0, "{input}");
                assert_eq!(
                    json_valid(&input, Some(JSON_VALID_JSON5_FLAG)),
                    0,
                    "{input}"
                );
                assert!(json(&input).is_err(), "{input}");
                assert!(json_extract(&input, &["$"]).is_err(), "{input}");
                assert!(jsonb(&input).is_err(), "{input}");
            }
        }
    }

    #[test]
    fn number_text_rewrite_never_quotes_an_unquoted_object_key() {
        let tokens = [
            "1e2".to_owned(),
            "1.50".to_owned(),
            "9e999".to_owned(),
            "-9e999".to_owned(),
            "9".repeat(400),
        ];
        for token in tokens {
            for input in [
                format!("{{{token}:0}}"),
                format!("[{{{token} \t\r\n:0}}]"),
                format!(r#"{{"first":1,{token}:0}}"#),
            ] {
                assert!(parse_json_text(&input).is_err(), "{input}");
                assert_eq!(json_valid(&input, None), 0, "{input}");
                assert_eq!(
                    json_valid(&input, Some(JSON_VALID_JSON5_FLAG)),
                    0,
                    "{input}"
                );
                assert!(json(&input).is_err(), "{input}");
                assert!(json_extract(&input, &["$"]).is_err(), "{input}");
                assert!(jsonb(&input).is_err(), "{input}");
            }

            // Quoted numeric keys and numeric values remain legal, including
            // a preserved number beside a key that looks like the same token.
            for input in [
                token.clone(),
                format!("[{token}]"),
                format!(r#"{{"{token}":{token}}}"#),
            ] {
                assert_eq!(json_valid(&input, None), 1, "{input}");
                assert_eq!(json(&input).unwrap(), input);
            }
        }
    }

    #[test]
    fn overflowing_json5_numbers_still_use_the_json5_fallback() {
        for (input, expected, negative) in
            [("9.e999", "9.0e999", false), ("-9.e999", "-9.0e999", true)]
        {
            assert!(parse_json_text(input).is_err(), "{input}");
            assert_eq!(json_valid(input, None), 0, "{input}");
            assert_eq!(json_valid(input, Some(JSON_VALID_JSON5_FLAG)), 1, "{input}");
            assert_eq!(json(input).unwrap(), expected);
            let SqliteValue::Float(value) = json_extract(input, &["$"]).unwrap() else {
                panic!("{input} must extract as a REAL");
            };
            assert!(value.is_infinite(), "{input}");
            assert_eq!(value.is_sign_negative(), negative, "{input}");
        }
        // Keep strict JSON numbers, including ones that overflow f64, in
        // their source spelling instead of rejecting or quoting them.
        for input in ["9e999", "-9e999", "9.0e+999", "1.50", "1E+2"] {
            assert_eq!(json_valid(input, None), 1, "{input}");
            assert_eq!(json(input).unwrap(), input);
            let encoded = jsonb(input).unwrap();
            assert_eq!(json_from_jsonb(&encoded).unwrap(), input);
        }
    }

    #[test]
    fn json5_nonfinite_scalars_preserve_sign_and_jsonb_round_trip() {
        for (input, negative) in [
            ("Infinity", false),
            ("+Infinity", false),
            ("-Infinity", true),
        ] {
            let value = parse_json5_text(input).expect("JSON5 infinity parses");
            assert_eq!(
                value,
                number_text_value(if negative { "-9e999" } else { "9e999" })
            );
            let SqliteValue::Float(number) = json_to_sqlite_scalar(&value).unwrap() else {
                panic!("{input} must extract as a REAL");
            };
            assert!(number.is_infinite());
            assert_eq!(number.is_sign_negative(), negative);
            let encoded = encode_jsonb_root(&value).expect("infinity encodes as JSONB");
            assert_eq!(
                decode_jsonb_root(&encoded).expect("infinity JSONB decodes"),
                value
            );
        }
        for input in ["NaN", "+NaN", "-NaN"] {
            assert_eq!(parse_json5_text(input).unwrap(), Value::Null);
        }
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_number_text_values_survive_nested_containers() {
        let value = parse_json5_text("{a:[Infinity,{b:-Infinity,c:NaN}],s:'Infinity'}")
            .expect("nested JSON5 non-finite values parse");
        for (path, expected) in [
            ("$.a[0]", number_text_value("9e999")),
            ("$.a[1].b", number_text_value("-9e999")),
            ("$.a[1].c", Value::Null),
            ("$.s", Value::String(InlineStr::new("Infinity"))),
        ] {
            assert_eq!(resolve_path(&value, path).unwrap(), Some(&expected));
        }
        assert_eq!(
            encode_json_text(&value),
            r#"{"a":[9e999,{"b":-9e999,"c":null}],"s":"Infinity"}"#
        );
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_nonfinite_visitor_preserves_keys_strings_and_finite_types() {
        let value = parse_json5_text(
            r"{
                Infinity:1, NaN:'NaN',
                u:18446744073709551615, i:-9223372036854775808,
                z:-0.0, v:'\v', s:'\\Infinity', b:true, n:null, a:[]
            }",
        )
        .expect("ordinary JSON5 values keep their representation");
        for (key, expected) in [
            ("Infinity", Value::Number(1_u64.into())),
            ("NaN", Value::String(InlineStr::new("NaN"))),
            ("u", Value::Number(u64::MAX.into())),
            ("i", Value::Number(i64::MIN.into())),
            ("v", Value::String(InlineStr::new("\u{000B}"))),
            ("s", Value::String(InlineStr::new("\\Infinity"))),
            ("b", Value::Bool(true)),
            ("n", Value::Null),
            ("a", Value::Array(Vec::new())),
        ] {
            assert_eq!(value.get(key), Some(&expected), "{key}");
        }
        let Some(Value::Number(zero)) = value.get("z") else {
            panic!("negative zero remains a finite number");
        };
        assert_eq!(zero.as_f64().unwrap().to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_public_read_helpers_accept_relaxed_input() {
        let input = "{a:[1,0x10,],s:'ok',}";
        assert_eq!(
            json_from_jsonb(&jsonb(input).unwrap()).unwrap(),
            r#"{"a":[1,16],"s":"ok"}"#
        );
        assert_eq!(
            json_from_jsonb(&jsonb_extract(input, &["$.a"]).unwrap()).unwrap(),
            "[1,16]"
        );
        assert_eq!(
            json_arrow(input, "$.s").unwrap(),
            SqliteValue::Text(r#""ok""#.into())
        );
        assert_eq!(json_arrow(input, "$.missing").unwrap(), SqliteValue::Null);
        assert_eq!(json_array_length(input, Some("$.a")).unwrap(), Some(2));
        assert_eq!(json_array_length(input, Some("$.s")).unwrap(), Some(0));
        assert_eq!(json_array_length(input, Some("$.missing")).unwrap(), None);
        assert_eq!(
            json_pretty("{a:0x10,}", Some("  ")).unwrap(),
            "{\n  \"a\": 16\n}"
        );
        for valid in [input, "9e999", "[9e999]", "{a:Infinity,b:NaN,}"] {
            assert_eq!(json_error_position(valid), 0, "{valid}");
            for arg in [
                SqliteValue::Text(valid.into()),
                SqliteValue::Blob(Arc::from(valid.as_bytes())),
            ] {
                assert_eq!(
                    JsonErrorPositionFunc.invoke(&[arg]).unwrap(),
                    SqliteValue::Integer(0),
                    "{valid}"
                );
            }
        }
        assert_eq!(json_valid(input, None), 0);
        assert_eq!(json_valid(input, Some(JSON_VALID_JSON5_FLAG)), 1);
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_public_edit_helpers_accept_documents_and_patches() {
        let input = "{a:1,b:[2,3,],}";
        let pairs = [
            ("$.a", SqliteValue::Integer(9)),
            ("$.c", SqliteValue::Text("{d:4}".into())),
        ];
        let check = |text: Result<String>, binary: Result<Vec<u8>>, expected: &str| {
            let text = text.unwrap();
            assert_eq!(text, expected);
            assert_eq!(json_valid(&text, None), 1);
            assert_eq!(json_from_jsonb(&binary.unwrap()).unwrap(), expected);
        };
        // Value arguments remain literal strings even when they resemble JSON5.
        check(
            json_set(input, &pairs),
            jsonb_set(input, &pairs),
            r#"{"a":9,"b":[2,3],"c":"{d:4}"}"#,
        );
        check(
            json_insert(input, &pairs),
            jsonb_insert(input, &pairs),
            r#"{"a":1,"b":[2,3],"c":"{d:4}"}"#,
        );
        check(
            json_replace(input, &pairs),
            jsonb_replace(input, &pairs),
            r#"{"a":9,"b":[2,3]}"#,
        );
        let insertion = [("$.b[1]", SqliteValue::Integer(7))];
        check(
            json_array_insert(input, &insertion),
            jsonb_array_insert(input, &insertion),
            r#"{"a":1,"b":[2,7,3]}"#,
        );
        check(
            json_remove(input, &["$.a"]),
            jsonb_remove(input, &["$.a"]),
            r#"{"b":[2,3]}"#,
        );
        let patch = "{a:null,b:[0x10,],c:'added',}";
        check(
            json_patch(input, patch),
            jsonb_patch(input, patch),
            r#"{"b":[16],"c":"added"}"#,
        );
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_blob_inputs_support_scalar_and_table_consumers() {
        let text = |input: &str| SqliteValue::Text(input.into());
        let blob = |input: &str| SqliteValue::Blob(Arc::from(input.as_bytes()));
        let input = "{a:[1,0x10,],s:'ok',}";
        let canonical = r#"{"a":[1,16],"s":"ok"}"#;
        assert_eq!(JsonFunc.invoke(&[blob(input)]).unwrap(), text(canonical));
        assert_eq!(
            JsonbFunc.invoke(&[blob(input)]).unwrap(),
            SqliteValue::Blob(Arc::from(jsonb(canonical).unwrap()))
        );
        assert_eq!(
            JsonExtractFunc
                .invoke(&[blob(input), text("$.a[1]")])
                .unwrap(),
            SqliteValue::Integer(16)
        );
        assert_eq!(
            JsonArrayLengthFunc
                .invoke(&[blob(input), text("$.a")])
                .unwrap(),
            SqliteValue::Integer(2)
        );
        assert_eq!(
            JsonPatchFunc
                .invoke(&[blob(input), blob("{s:'updated',}")])
                .unwrap(),
            text(r#"{"a":[1,16],"s":"updated"}"#)
        );
        assert_eq!(
            json_each_blob(input.as_bytes(), Some("$.a")).unwrap(),
            json_each(input, Some("$.a")).unwrap()
        );
        assert_eq!(
            json_tree_blob(input.as_bytes(), None).unwrap(),
            json_tree(input, None).unwrap()
        );
        assert_eq!(json_valid_blob(input.as_bytes(), None), 0);
        assert_eq!(
            json_valid_blob(input.as_bytes(), Some(JSON_VALID_JSON5_FLAG)),
            1
        );
        // This is both JSONB integer 456 and UTF-8 JSON text "3456".
        // JSONB decoding must retain precedence over the text fallback.
        assert_eq!(
            JsonExtractFunc.invoke(&[blob("3456"), text("$")]).unwrap(),
            SqliteValue::Integer(456)
        );
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_consumers_preserve_malformed_input_errors() {
        for input in ["{a:[1,,]}", "{a:'unterminated}", "[0x]", "{a:1} trailing"] {
            assert!(jsonb(input).is_err(), "{input}");
            assert!(jsonb_extract(input, &["$"]).is_err(), "{input}");
            assert!(json_arrow(input, "$").is_err(), "{input}");
            assert!(json_array_length(input, None).is_err(), "{input}");
            assert!(json_pretty(input, None).is_err(), "{input}");
            assert!(json_set(input, &[]).is_err(), "{input}");
            assert!(jsonb_set(input, &[]).is_err(), "{input}");
            assert!(json_patch("{}", input).is_err(), "{input}");
            assert!(jsonb_patch("{}", input).is_err(), "{input}");
            assert!(json_error_position(input) > 0, "{input}");
            assert!(
                JsonFunc
                    .invoke(&[SqliteValue::Blob(Arc::from(input.as_bytes()))])
                    .is_err(),
                "{input}"
            );
            assert!(json_each_blob(input.as_bytes(), None).is_err(), "{input}");
        }
        let invalid_utf8 = [0xff];
        assert!(
            JsonExtractFunc
                .invoke(&[
                    SqliteValue::Blob(Arc::from(invalid_utf8.as_slice())),
                    SqliteValue::Text("$".into()),
                ])
                .is_err()
        );
        assert!(json_tree_blob(&invalid_utf8, None).is_err());
    }

    #[test]
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_number_text_values_do_not_accept_malformed_input() {
        for input in [
            "Infinityx",
            "[Infinity,,NaN]",
            "{a:Infinity b:1}",
            "[-Infinity",
        ] {
            assert!(parse_json5_text(input).is_err(), "accepted {input:?}");
        }
    }

    #[test]
    // The JSON5 literals ('{a:...}') look like format args to clippy.
    #[allow(clippy::literal_string_with_formatting_args)]
    fn json5_lexical_translation_matches_stock_oracle() {
        // bd-4gqeu: every expectation below is pinned against sqlite3 3.46.1
        // `SELECT json('<input>')` unless noted. The translator must be
        // token-faithful: duplicate keys, key order, and exponent literals
        // survive.
        let cases: &[(&str, &str)] = &[
            // Fidelity the old serde round-trip destroyed:
            ("{a:1,a:2}", r#"{"a":1,"a":2}"#),
            ("{b:1,a:2}", r#"{"b":1,"a":2}"#),
            ("{a:1e2,}", r#"{"a":1e2}"#),
            ("[1E+3]", "[1E+3]"),
            // Structure: comments, trailing commas, whitespace.
            ("[1,/*c*/2,]", "[1,2]"),
            ("{a:1//c\n,b:2}", r#"{"a":1,"b":2}"#),
            ("{ a : 1 }", r#"{"a":1}"#),
            // Strings and escapes (escape SPELLINGS are preserved/translated,
            // never decoded to their characters).
            ("{a:'hi'}", r#"{"a":"hi"}"#),
            (r"{a:'it\'s'}", r#"{"a":"it's"}"#),
            ("{'k':1}", r#"{"k":1}"#),
            ("['q\"d']", r#"["q\"d"]"#),
            (r"['\x41']", r#"["\u0041"]"#),
            (r"['s\u0041']", r#"["s\u0041"]"#),
            (r"['a\tb']", r#"["a\tb"]"#),
            (r"['\/']", r#"["\/"]"#),
            // Stock spells \v with the 0009 escape even though the value is U+000B.
            (r"['\v']", r#"["\u0009"]"#),
            (r"['\x0b']", r#"["\u000b"]"#),
            ("['line\\\ncont']", r#"["linecont"]"#),
            // Numbers.
            ("[0x1A]", "[26]"),
            ("[-0x10]", "[-16]"),
            ("[0xFFFFFFFFFFFFFFFF]", "[18446744073709551615]"),
            ("[.5e2]", "[0.5e2]"),
            ("[1.e3]", "[1.0e3]"),
            ("[-.5]", "[-0.5]"),
            ("[+.5]", "[0.5]"),
            ("{a:+3}", r#"{"a":3}"#),
            // Non-finite.
            ("{a:Infinity}", r#"{"a":9e999}"#),
            ("[+Infinity]", "[9e999]"),
            ("[-Infinity]", "[-9e999]"),
            ("{a:NaN}", r#"{"a":null}"#),
        ];
        for (input, expected) in cases {
            assert_eq!(
                json(input).unwrap_or_else(|e| panic!("json({input:?}) errored: {e}")),
                *expected,
                "json5 translation mismatch for {input:?}"
            );
        }
        // Strict-JSON inputs must still take the token-preserving minify path.
        assert_eq!(json(r#"{"a": 1e2}"#).unwrap(), r#"{"a":1e2}"#);
    }

    #[test]
    fn test_json_fn_oracle_edges_2026_08() {
        // Oracle: sqlite3 3.46.1. json_extract path semantics: a SINGLE path
        // returns the natural SQL value (JSON string unquoted, number as int/real,
        // true->1, json null / missing path -> SQL NULL); MULTIPLE paths return a
        // JSON array where a missing key is `null`; `$[#-1]` indexes from the end.
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        let i = SqliteValue::Integer;
        let f = SqliteValue::Float;
        let nul = SqliteValue::Null;
        let run = |func: &dyn ScalarFunction, args: &[SqliteValue]| -> SqliteValue {
            func.invoke(args).unwrap()
        };

        // json_extract — single path returns the natural SQL value.
        assert_eq!(run(&JsonExtractFunc, &[t(r#"{"a":1}"#), t("$.a")]), i(1));
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":1}"#), t("$.b")]),
            nul.clone()
        );
        assert_eq!(run(&JsonExtractFunc, &[t("[10,20,30]"), t("$[0]")]), i(10));
        assert_eq!(
            run(&JsonExtractFunc, &[t("[10,20,30]"), t("$[#-1]")]),
            i(30)
        );
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":"x"}"#), t("$.a")]),
            t("x")
        );
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":null}"#), t("$.a")]),
            nul.clone()
        );
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":1.5}"#), t("$.a")]),
            f(1.5)
        );
        assert_eq!(run(&JsonExtractFunc, &[t(r#"{"a":true}"#), t("$.a")]), i(1));
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":1e3}"#), t("$.a")]),
            f(1000.0)
        );
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":{"b":2}}"#), t("$.a")]),
            t(r#"{"b":2}"#)
        );
        assert_eq!(
            run(
                &JsonExtractFunc,
                &[t(r#"{"x":[1,{"y":9}]}"#), t("$.x[1].y")]
            ),
            i(9)
        );
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":1}"#), t("$")]),
            t(r#"{"a":1}"#)
        );
        assert_eq!(run(&JsonExtractFunc, &[t("5"), t("$")]), i(5));
        assert_eq!(run(&JsonExtractFunc, &[t(r#""hi""#), t("$")]), t("hi"));
        assert_eq!(
            run(&JsonExtractFunc, &[t("[1,2,3]"), t("$[5]")]),
            nul.clone()
        );
        // multiple paths -> JSON array; a missing key becomes json `null`.
        assert_eq!(
            run(
                &JsonExtractFunc,
                &[t(r#"{"a":1,"b":2}"#), t("$.a"), t("$.b")]
            ),
            t("[1,2]")
        );
        assert_eq!(
            run(&JsonExtractFunc, &[t(r#"{"a":1}"#), t("$.a"), t("$.b")]),
            t("[1,null]")
        );

        // json_type / json_valid.
        assert_eq!(run(&JsonTypeFunc, &[t("[1,2]")]), t("array"));
        assert_eq!(
            run(&JsonTypeFunc, &[t(r#"{"a":1}"#), t("$.a")]),
            t("integer")
        );
        assert_eq!(run(&JsonTypeFunc, &[t("[1,2]"), t("$[9]")]), nul.clone());
        assert_eq!(run(&JsonValidFunc, &[t("{bad}")]), i(0));
        assert_eq!(run(&JsonValidFunc, &[t(r#"{"a":1}"#)]), i(1));

        // json_quote / json_array / json_array_length / json().
        assert_eq!(run(&JsonQuoteFunc, &[f(3.0)]), t("3.0"));
        assert_eq!(
            run(&JsonArrayFunc, &[i(1), t("x"), nul.clone()]),
            t(r#"[1,"x",null]"#)
        );
        assert_eq!(run(&JsonArrayLengthFunc, &[t("[1,2,3]")]), i(3));
        assert_eq!(run(&JsonArrayLengthFunc, &[t(r#"{"a":1}"#)]), i(0));
        assert_eq!(run(&JsonFunc, &[t("  [1,  2]  ")]), t("[1,2]"));
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct JsonTableRowStructure {
        key: String,
        value: String,
        type_name: &'static str,
        atom: String,
        id: i64,
        parent: String,
        fullkey: String,
        path: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct JsonTableJsonbStructure {
        each: Vec<JsonTableRowStructure>,
        tree: Vec<JsonTableRowStructure>,
    }

    fn json_value_structure(value: &SqliteValue) -> String {
        format!("{value:?}")
    }

    fn json_table_row_structure(row: &JsonTableRow) -> JsonTableRowStructure {
        JsonTableRowStructure {
            key: json_value_structure(&row.key),
            value: json_value_structure(&row.value),
            type_name: row.type_name,
            atom: json_value_structure(&row.atom),
            id: row.id,
            parent: json_value_structure(&row.parent),
            fullkey: row.fullkey.clone(),
            path: row.path.clone(),
        }
    }

    #[test]
    fn test_register_json_scalars_registers_jsonb_aggregates_bd_5ul61() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);

        for name in [
            "json_group_array",
            "json_group_object",
            "jsonb_group_array",
            "jsonb_group_object",
        ] {
            assert!(
                registry.contains_aggregate(name),
                "missing aggregate registration for {name}"
            );
        }

        let array = JsonbGroupArrayFunc;
        let mut array_state = array.initial_state();
        array
            .step(&mut array_state, &[SqliteValue::Integer(1)])
            .unwrap();
        array
            .step(&mut array_state, &[SqliteValue::Integer(2)])
            .unwrap();
        let SqliteValue::Blob(array_blob) = array.finalize(array_state).unwrap() else {
            panic!("jsonb_group_array must return a BLOB");
        };
        assert_eq!(
            decode_jsonb_root(&array_blob).unwrap(),
            Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())])
        );

        let object = JsonbGroupObjectFunc;
        let mut object_state = object.initial_state();
        object
            .step(
                &mut object_state,
                &[
                    SqliteValue::Text(SmallText::from_string("k")),
                    SqliteValue::Integer(7),
                ],
            )
            .unwrap();
        let SqliteValue::Blob(object_blob) = object.finalize(object_state).unwrap() else {
            panic!("jsonb_group_object must return a BLOB");
        };
        assert_eq!(json_from_jsonb(&object_blob).unwrap(), r#"{"k":7}"#);
    }

    #[test]
    fn test_register_json_scalars_registers_core_functions() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);

        for name in [
            "json",
            "jsonb",
            "json_valid",
            "json_type",
            "json_extract",
            "jsonb_extract",
            "json_arrow",
            "json_double_arrow",
            "->",
            "->>",
            "json_set",
            "jsonb_set",
            "json_array_insert",
            "jsonb_array_insert",
            "json_remove",
            "jsonb_remove",
            "json_array",
            "jsonb_array",
            "json_object",
            "jsonb_object",
            "json_quote",
            "json_patch",
            "jsonb_patch",
        ] {
            assert!(
                registry.contains_scalar(name),
                "missing registration for {name}"
            );
        }
    }

    #[test]
    fn test_registered_json_extract_scalar_executes() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_extract", 2)
            .expect("json_extract should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1,"b":[2,3]}"#)),
                SqliteValue::Text(SmallText::from_string("$.b[1]")),
            ])
            .unwrap();
        assert_eq!(out, SqliteValue::Integer(3));
    }

    // Conformance guard: pins a batch of JSON scalar behaviors to the C SQLite
    // oracle (verified against sqlite3 3.46.1) and reports every divergence in
    // one run. Anchors the bd-p2xrc fix — stock json() minifies whitespace but
    // never rewrites a number literal, so parsed reals keep their exact source
    // text (json('1.50') -> '1.50', json('0.3333333333333333') round-trips,
    // json('1.5e+3') keeps its exponent) while a real constructed from a SQL
    // value renders canonically (json_array(1.0) -> '[1.0]'). Residual
    // unsigned/uppercase-exponent exactness (json('1e2') -> '1e+2') is bd-6b0pe.
    #[test]
    fn test_json_scalar_conformance_oracle_bd_p2xrc() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s.to_string()));
        let call = |name: &str, args: Vec<SqliteValue>| -> SqliteValue {
            registry
                .find_scalar(name, args.len() as i32)
                .unwrap_or_else(|| panic!("missing scalar {name}/{}", args.len()))
                .invoke(&args)
                .unwrap_or_else(|e| panic!("{name} invoke failed: {e:?}"))
        };
        let as_text = |v: &SqliteValue| -> String {
            match v {
                SqliteValue::Text(s) => s.as_ref().to_string(),
                SqliteValue::Integer(i) => i.to_string(),
                SqliteValue::Float(f) => format!("{f}"),
                SqliteValue::Null => "NULL".to_string(),
                other => format!("{other:?}"),
            }
        };
        let mut fails: Vec<String> = Vec::new();
        macro_rules! ck {
            ($label:expr, $got:expr, $expect:expr) => {{
                let g = as_text(&$got);
                if g != $expect {
                    fails.push(format!("{}: got {:?} expected {:?}", $label, g, $expect));
                }
            }};
        }
        // JSON-string / text / integer results (exact, version-independent)
        ck!(
            "json_min",
            call("json", vec![t(" { \"a\" : 1 , \"b\" : [ 1,2 ] } ")]),
            "{\"a\":1,\"b\":[1,2]}"
        );
        ck!(
            "json_type",
            call("json_type", vec![t("{\"a\":1}")]),
            "object"
        );
        ck!(
            "json_type_path",
            call("json_type", vec![t("{\"a\":[1,2]}"), t("$.a")]),
            "array"
        );
        ck!(
            "json_type_int",
            call("json_type", vec![t("123")]),
            "integer"
        );
        ck!("json_type_real", call("json_type", vec![t("1.5")]), "real");
        ck!("json_type_true", call("json_type", vec![t("true")]), "true");
        ck!("json_type_null", call("json_type", vec![t("null")]), "null");
        ck!(
            "jx_obj",
            call("json_extract", vec![t("{\"a\":1,\"b\":2}"), t("$.a")]),
            "1"
        );
        ck!(
            "jx_multi",
            call(
                "json_extract",
                vec![t("{\"a\":1,\"b\":2}"), t("$.a"), t("$.b")]
            ),
            "[1,2]"
        );
        ck!(
            "jx_whole",
            call("json_extract", vec![t("{\"a\":1}"), t("$")]),
            "{\"a\":1}"
        );
        ck!(
            "jx_text",
            call("json_extract", vec![t("{\"a\":\"x\"}"), t("$.a")]),
            "x"
        );
        ck!(
            "jx_arr",
            call("json_extract", vec![t("[10,20,30]"), t("$[1]")]),
            "20"
        );
        ck!(
            "jx_neg",
            call("json_extract", vec![t("[10,20,30]"), t("$[#-1]")]),
            "30"
        );
        ck!(
            "jx_nested_obj",
            call("json_extract", vec![t("{\"a\":{\"b\":9}}"), t("$.a")]),
            "{\"b\":9}"
        );
        ck!(
            "arrow",
            call("->", vec![t("{\"a\":{\"b\":5}}"), t("$.a")]),
            "{\"b\":5}"
        );
        ck!(
            "arrow2",
            call("->>", vec![t("{\"a\":{\"b\":5}}"), t("$.a.b")]),
            "5"
        );
        ck!(
            "arrow_str",
            call("->", vec![t("{\"a\":\"hi\"}"), t("$.a")]),
            "\"hi\""
        );
        ck!(
            "arrow2_str",
            call("->>", vec![t("{\"a\":\"hi\"}"), t("$.a")]),
            "hi"
        );
        ck!(
            "json_array",
            call(
                "json_array",
                vec![
                    SqliteValue::Integer(1),
                    t("x"),
                    SqliteValue::Null,
                    SqliteValue::Float(2.5)
                ]
            ),
            "[1,\"x\",null,2.5]"
        );
        ck!(
            "json_object",
            call(
                "json_object",
                vec![t("a"), SqliteValue::Integer(1), t("b"), t("y")]
            ),
            "{\"a\":1,\"b\":\"y\"}"
        );
        ck!(
            "json_object_dup",
            call(
                "json_object",
                vec![
                    t("a"),
                    SqliteValue::Integer(1),
                    t("a"),
                    SqliteValue::Integer(2)
                ]
            ),
            "{\"a\":1,\"a\":2}"
        );
        ck!(
            "json_quote_num",
            call("json_quote", vec![SqliteValue::Float(3.0)]),
            "3.0"
        );
        ck!(
            "json_quote_str",
            call("json_quote", vec![t("a\"b")]),
            "\"a\\\"b\""
        );
        ck!(
            "json_valid_ok",
            call("json_valid", vec![t("{\"a\":1}")]),
            "1"
        );
        ck!(
            "json_valid_bad",
            call("json_valid", vec![t("{\"a\":}")]),
            "0"
        );
        ck!("json_valid_trail", call("json_valid", vec![t("{} ")]), "1");
        ck!(
            "json_arrlen",
            call("json_array_length", vec![t("[1,2,3]")]),
            "3"
        );
        ck!(
            "json_arrlen_path",
            call("json_array_length", vec![t("{\"a\":[1,2,3,4]}"), t("$.a")]),
            "4"
        );
        ck!(
            "json_set",
            call(
                "json_set",
                vec![
                    t("{\"a\":1}"),
                    t("$.a"),
                    SqliteValue::Integer(99),
                    t("$.b"),
                    SqliteValue::Integer(2)
                ]
            ),
            "{\"a\":99,\"b\":2}"
        );
        ck!(
            "json_insert_exist",
            call(
                "json_insert",
                vec![t("{\"a\":1}"), t("$.a"), SqliteValue::Integer(99)]
            ),
            "{\"a\":1}"
        );
        ck!(
            "json_replace_miss",
            call(
                "json_replace",
                vec![t("{\"a\":1}"), t("$.b"), SqliteValue::Integer(2)]
            ),
            "{\"a\":1}"
        );
        ck!(
            "json_remove",
            call("json_remove", vec![t("{\"a\":1,\"b\":2}"), t("$.a")]),
            "{\"b\":2}"
        );
        ck!(
            "json_patch",
            call(
                "json_patch",
                vec![t("{\"a\":1,\"b\":2}"), t("{\"b\":null,\"c\":3}")]
            ),
            "{\"a\":1,\"c\":3}"
        );
        ck!(
            "json_bignum",
            call("json", vec![t("{\"a\":12345678901234567890}")]),
            "{\"a\":12345678901234567890}"
        );
        ck!(
            "json_neg_zero",
            call("json_extract", vec![t("[-0]"), t("$[0]")]),
            "0"
        );
        // ── deeper batch ──
        ck!(
            "jx_true",
            call("json_extract", vec![t("{\"a\":true}"), t("$.a")]),
            "1"
        );
        ck!(
            "jx_false",
            call("json_extract", vec![t("{\"a\":false}"), t("$.a")]),
            "0"
        );
        ck!(
            "jx_json_null",
            call("json_extract", vec![t("{\"a\":null}"), t("$.a")]),
            "NULL"
        );
        ck!(
            "jx_deep",
            call(
                "json_extract",
                vec![t("{\"a\":{\"b\":{\"c\":7}}}"), t("$.a.b.c")]
            ),
            "7"
        );
        ck!(
            "jx_quoted_key",
            call("json_extract", vec![t("{\"a b\":1}"), t("$.\"a b\"")]),
            "1"
        );
        ck!(
            "jx_i64max",
            call("json_extract", vec![t("[9223372036854775807]"), t("$[0]")]),
            "9223372036854775807"
        );
        ck!(
            "arr_of_jsontext",
            call("json_array", vec![t("[1,2]")]),
            "[\"[1,2]\"]"
        );
        ck!(
            "arr_of_real",
            call("json_array", vec![SqliteValue::Float(1.0)]),
            "[1.0]"
        );
        ck!(
            "arr_of_real2",
            call("json_array", vec![SqliteValue::Float(2.50)]),
            "[2.5]"
        );
        ck!(
            "obj_val_real",
            call("json_object", vec![t("a"), SqliteValue::Float(1.0)]),
            "{\"a\":1.0}"
        );
        ck!("json_scalar_int", call("json", vec![t("5")]), "5");
        ck!("json_scalar_str", call("json", vec![t("\"hi\"")]), "\"hi\"");
        ck!("json_scalar_real", call("json", vec![t("1.50")]), "1.50");
        ck!("json_num_1500", call("json", vec![t("1.500")]), "1.500");
        ck!("json_num_eplus", call("json", vec![t("1.5e+3")]), "1.5e+3");
        ck!("json_num_eneg", call("json", vec![t("1e-7")]), "1e-7");
        ck!("json_num_leadzero", call("json", vec![t("0.10")]), "0.10");
        ck!("json_num_neg", call("json", vec![t("-1.50")]), "-1.50");
        ck!("json_num_neg0real", call("json", vec![t("-0.0")]), "-0.0");
        ck!(
            "json_num_16dig",
            call("json", vec![t("0.3333333333333333")]),
            "0.3333333333333333"
        );
        ck!(
            "json_num_obj",
            call("json", vec![t("{\"a\":1.50,\"b\":2.0}")]),
            "{\"a\":1.50,\"b\":2.0}"
        );
        ck!(
            "json_num_arr_ws",
            call("json", vec![t("[1.50, 2.30]")]),
            "[1.50,2.30]"
        );
        ck!(
            "json_pretty",
            call("json_pretty", vec![t("{\"a\":1,\"b\":[2,3]}")]),
            "{\n    \"a\": 1,\n    \"b\": [\n        2,\n        3\n    ]\n}"
        );
        ck!(
            "json_err_pos_ok",
            call("json_error_position", vec![t("{\"a\":1}")]),
            "0"
        );
        ck!(
            "json_err_pos_bad",
            call("json_error_position", vec![t("{\"a\":}")]),
            "6"
        );
        ck!(
            "json_valid_2arg1",
            call("json_valid", vec![t("{\"a\":1}"), SqliteValue::Integer(1)]),
            "1"
        );
        ck!(
            "json_valid_2arg6",
            call("json_valid", vec![t("{\"a\":1}"), SqliteValue::Integer(6)]),
            "1"
        );
        ck!(
            "json_valid_5x",
            call("json_valid", vec![t("{\"x\":"), SqliteValue::Integer(6)]),
            "0"
        );
        ck!(
            "arrow_int_key",
            call("->", vec![t("[1,2,3]"), SqliteValue::Integer(0)]),
            "1"
        );
        ck!(
            "arrow2_int_key",
            call("->>", vec![t("[1,2,3]"), SqliteValue::Integer(2)]),
            "3"
        );
        ck!(
            "json_set_create",
            call(
                "json_set",
                vec![t("{\"a\":1}"), t("$.b.c"), SqliteValue::Integer(5)]
            ),
            "{\"a\":1,\"b\":{\"c\":5}}"
        );
        ck!(
            "json_set_append",
            call(
                "json_set",
                vec![t("{\"a\":[1,2]}"), t("$.a[#]"), SqliteValue::Integer(3)]
            ),
            "{\"a\":[1,2,3]}"
        );
        ck!(
            "json_arrlen_obj",
            call("json_array_length", vec![t("{\"a\":1}")]),
            "0"
        );
        ck!(
            "json_remove_arr_idx",
            call("json_remove", vec![t("[1,2,3]"), t("$[1]")]),
            "[1,3]"
        );
        ck!(
            "json_insert_arr",
            call(
                "json_insert",
                vec![t("[1,2]"), t("$[#]"), SqliteValue::Integer(3)]
            ),
            "[1,2,3]"
        );
        ck!(
            "json_quote_null",
            call("json_quote", vec![SqliteValue::Null]),
            "null"
        );
        ck!(
            "json_obj_num_key",
            call("json_object", vec![t("1"), SqliteValue::Integer(2)]),
            "{\"1\":2}"
        );

        // Float results: compare numerically (float→text is oracle-version-sensitive)
        let mut ck_num = |label: &str, got: SqliteValue, expect: f64| match got {
            SqliteValue::Float(f) if (f - expect).abs() < 1e-9 => {}
            SqliteValue::Integer(i) if (i as f64 - expect).abs() < 1e-9 => {}
            other => fails.push(format!("{label}: got {other:?} expected ~{expect}")),
        };
        ck_num(
            "json_deep_num",
            call("json_extract", vec![t("{\"a\":1.50}"), t("$.a")]),
            1.5,
        );
        ck_num(
            "json_e_notation",
            call("json_extract", vec![t("[1e3]"), t("$[0]")]),
            1000.0,
        );

        assert!(
            fails.is_empty(),
            "json oracle divergences:\n{}",
            fails.join("\n")
        );
    }

    #[test]
    fn test_json_text_minify_preserves_source_bd_6b0pe() {
        // Stock json() on TEXT input strips only insignificant whitespace and
        // preserves every token of the source verbatim (verified vs sqlite3
        // 3.46.1). Round-tripping through serde_json::Value used to normalise
        // exponents (1e2 -> 1e+2), unescape `\/`, and drop duplicate keys.
        for (input, expect) in [
            // exponent notation preserved exactly (bd-6b0pe)
            ("1e2", "1e2"),
            ("1E2", "1E2"),
            ("1e+2", "1e+2"),
            ("1e02", "1e02"),
            ("[1e+2, 1E2, 1e2]", "[1e+2,1E2,1e2]"),
            (r#"{"a":1.5E+3}"#, r#"{"a":1.5E+3}"#),
            // mantissa/precision still preserved (bd-p2xrc)
            ("1.50", "1.50"),
            ("2.500000", "2.500000"),
            // string escapes preserved verbatim, not canonicalised
            (r#"{"a":"a\/b"}"#, r#"{"a":"a\/b"}"#),
            (r#"["a\tb"]"#, r#"["a\tb"]"#),
            // in-string whitespace kept; inter-token whitespace stripped
            (r#"[ "a  b" , 1 ]"#, r#"["a  b",1]"#),
            (
                r#"{ "a" : [ 1 , 2 ] , "b" : "c" }"#,
                r#"{"a":[1,2],"b":"c"}"#,
            ),
            // duplicate object keys preserved (stock does not dedup on parse)
            (r#"{"a":1,"a":2}"#, r#"{"a":1,"a":2}"#),
            (r#"{"x":1e2,"x":3e4}"#, r#"{"x":1e2,"x":3e4}"#),
        ] {
            assert_eq!(
                json(input).unwrap(),
                expect,
                "json({input:?}) must minify whitespace but preserve the source verbatim"
            );
        }
        // Genuinely-malformed input (invalid even as JSON5) is still rejected.
        assert!(json("{\"a\":}").is_err());
        assert!(json("[1 2]").is_err());
        // A trailing comma is valid JSON5, now accepted and canonicalized to
        // standard JSON (bd-qear2): `[1,]` -> `[1]`.
        assert_eq!(json("[1,]").unwrap(), "[1]");
    }

    #[test]
    fn test_json_blob_render_uses_stock_float_format_bd_p2xrc_revert() {
        // json(BLOB) decodes JSONB (numeric payloads are ASCII text) and
        // re-renders. Floats must use stock's shortest form (`{float:?}`),
        // NOT the arbitrary_precision `to_string()` which inserts a '+' into
        // the exponent ("1e300" -> "1e+300") and breaks byte-compatible JSONB
        // round-trip through stock sqlite3 (the bd-p2xrc-revert regression
        // that RED'd the release gate). Text-input preservation stays in
        // minify_json_text (bd-6b0pe) and is unaffected.
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let jsonb_of = |s: &str| -> Vec<u8> {
            match registry
                .find_scalar("jsonb", 1)
                .unwrap()
                .invoke(&[SqliteValue::Text(SmallText::from_string(s.to_string()))])
                .unwrap()
            {
                SqliteValue::Blob(b) => b.to_vec(),
                other => panic!("jsonb not blob: {other:?}"),
            }
        };
        let json_of_blob = |bytes: Vec<u8>| -> String {
            match registry
                .find_scalar("json", 1)
                .unwrap()
                .invoke(&[SqliteValue::Blob(Arc::from(bytes.as_slice()))])
                .unwrap()
            {
                SqliteValue::Text(t) => t.as_ref().to_string(),
                other => panic!("json not text: {other:?}"),
            }
        };
        assert_eq!(json_of_blob(jsonb_of("1e300")), "1e300");
        assert_eq!(json_of_blob(jsonb_of("-0.0")), "-0.0");
        assert_eq!(
            json_of_blob(jsonb_of("[1.5,-0.0,1e300,9223372036854775807]")),
            "[1.5,-0.0,1e300,9223372036854775807]"
        );
    }

    #[test]
    fn test_registered_json_arrow_scalar_normalizes_label_shorthand() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_arrow", 2)
            .expect("json_arrow should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a.b":1,"a":{"b":2}}"#)),
                SqliteValue::Text(SmallText::from_string("a.b")),
            ])
            .expect("json_arrow should normalize bare labels");
        assert_eq!(out, SqliteValue::Text(SmallText::from_string("1")));
        assert_eq!(func.result_subtype(), Some(JSON_SUBTYPE));

        let operator = registry
            .find_scalar("->", 2)
            .expect("JSON arrow operator should be registered");
        assert_eq!(operator.result_subtype(), Some(JSON_SUBTYPE));
    }

    #[test]
    fn test_registered_json_double_arrow_scalar_normalizes_integer_shorthand() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_double_arrow", 2)
            .expect("json_double_arrow should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("[10,20,30]")),
                SqliteValue::Integer(1),
            ])
            .expect("json_double_arrow should normalize integer indexes");
        assert_eq!(out, SqliteValue::Integer(20));
    }

    #[test]
    fn test_json_array_insert_stock_semantics_bd_5ul61() {
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s.to_owned()));
        let func = JsonArrayInsertFunc;

        let invoke = |args: Vec<SqliteValue>| -> SqliteValue {
            func.invoke(&args).expect("json_array_insert succeeds")
        };

        assert_eq!(
            invoke(vec![t("[1,2]"), t("$[1]"), SqliteValue::Integer(9)]),
            t("[1,9,2]")
        );
        assert_eq!(
            invoke(vec![t("[1,2]"), t("$[2]"), SqliteValue::Integer(9)]),
            t("[1,2,9]")
        );
        assert_eq!(
            invoke(vec![t("[1,2]"), t("$[3]"), SqliteValue::Integer(9)]),
            t("[1,2]")
        );
        assert_eq!(
            invoke(vec![t("[1,2]"), t("$"), SqliteValue::Integer(9)]),
            t("[1,2]")
        );
        assert_eq!(
            invoke(vec![t("[1,2]"), SqliteValue::Null, SqliteValue::Integer(9)]),
            t("[1,2]")
        );
        assert_eq!(
            invoke(vec![t("{}"), t("$.a[0]"), SqliteValue::Integer(9)]),
            t(r#"{"a":[9]}"#)
        );
        assert_eq!(
            invoke(vec![t(r#"{"a":1}"#), t("$.a.b"), SqliteValue::Integer(9)]),
            t(r#"{"a":1}"#)
        );

        let error = func
            .invoke(&[t("{}"), t("$.a"), SqliteValue::Integer(9)])
            .expect_err("terminal object label must be rejected");
        assert!(
            error.to_string().contains("not an array element: '$.a'"),
            "unexpected error: {error}"
        );

        let embedded = func
            .invoke_with_arg_subtypes(&[t("[]"), t("$[0]"), t("[2,3]")], &[0, 0, JSON_SUBTYPE])
            .unwrap();
        assert_eq!(embedded, t("[[2,3]]"));
    }

    #[test]
    fn test_jsonb_array_insert_returns_jsonb_bd_5ul61() {
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s.to_owned()));
        let func = JsonbArrayInsertFunc;
        let SqliteValue::Blob(blob) = func
            .invoke(&[t("[1,2]"), t("$[1]"), SqliteValue::Integer(9)])
            .unwrap()
        else {
            panic!("jsonb_array_insert must return a BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[1,9,2]");
    }

    #[test]
    fn test_registered_json_set_scalar_executes() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_set", 3)
            .expect("json_set should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                SqliteValue::Text(SmallText::from_string("$.b")),
                SqliteValue::Integer(2),
            ])
            .unwrap();
        assert_eq!(
            out,
            SqliteValue::Text(SmallText::from_string(r#"{"a":1,"b":2}"#))
        );
    }

    #[test]
    fn test_registered_jsonb_scalar_executes() -> Result<()> {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("jsonb", 1)
            .expect("jsonb should be registered");
        let out = func
            .invoke(&[SqliteValue::Text(SmallText::from_string(r#"{"a":[1,2]}"#))])
            .expect("jsonb should encode to JSONB");
        let SqliteValue::Blob(blob) = out else {
            return Err(FrankenError::function_error("jsonb should return BLOB"));
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":[1,2]}"#);
        Ok(())
    }

    #[test]
    fn test_registered_json_extract_accepts_jsonb_blob_input() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_extract", 2)
            .expect("json_extract should be registered");
        let input = jsonb(r#"{"a":{"b":7}}"#).unwrap();
        let out = func
            .invoke(&[
                SqliteValue::Blob(Arc::from(input)),
                SqliteValue::Text(SmallText::from_string("$.a.b")),
            ])
            .expect("json_extract should accept JSONB blob input");
        assert_eq!(out, SqliteValue::Integer(7));
    }

    #[test]
    fn test_registered_jsonb_set_accepts_jsonb_blob_input() -> Result<()> {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("jsonb_set", 3)
            .expect("jsonb_set should be registered");
        let input = jsonb(r#"{"a":1}"#).unwrap();
        let out = func
            .invoke(&[
                SqliteValue::Blob(Arc::from(input)),
                SqliteValue::Text(SmallText::from_string("$.b")),
                SqliteValue::Integer(9),
            ])
            .expect("jsonb_set should accept JSONB blob input");
        let SqliteValue::Blob(blob) = out else {
            return Err(FrankenError::function_error("jsonb_set should return BLOB"));
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":1,"b":9}"#);
        Ok(())
    }

    // bd-7h73c: jsonb_set/jsonb_insert/jsonb_replace must honor the JSON subtype
    // on value args (results of json(), json_array(), ->, ...) and embed the value
    // as a JSON subtree rather than stringify it — matching the text json_* twins
    // and stock SQLite (verified sqlite3 3.46.1:
    // json(jsonb_set('{}','$.a',json_array(1,2))) = {"a":[1,2]}).
    #[test]
    fn test_jsonb_set_honors_json_subtype_on_value() {
        let out = JsonbSetFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("{}")),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                ],
                &[0, 0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_set should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":[1,2]}"#);
    }

    #[test]
    fn test_jsonb_set_without_subtype_stringifies_value() {
        // Negative control: a plain TEXT value (subtype 0) stays a JSON string.
        let out = JsonbSetFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("{}")),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                ],
                &[0, 0, 0],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_set should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":"[1,2]"}"#);
    }

    #[test]
    fn test_jsonb_insert_honors_json_subtype_on_value() {
        let out = JsonbInsertFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                    SqliteValue::Text(SmallText::from_string("$.b")),
                    SqliteValue::Text(SmallText::from_string("[3,4]")),
                ],
                &[0, 0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_insert should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":1,"b":[3,4]}"#);
    }

    #[test]
    fn test_jsonb_replace_honors_json_subtype_on_value() {
        let out = JsonbReplaceFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                    SqliteValue::Text(SmallText::from_string(r#"{"n":5}"#)),
                ],
                &[0, 0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_replace should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":{"n":5}}"#);
    }

    // bd-obb21: jsonb_array/jsonb_object must honor the JSON subtype on value
    // args (same class as bd-7h73c) — embed as a JSON subtree, not stringify
    // (verified sqlite3 3.46.1: json(jsonb_array(json_array(1,2),3)) = [[1,2],3];
    // json(jsonb_object('a',json_array(1,2))) = {"a":[1,2]}).
    #[test]
    fn test_jsonb_array_honors_json_subtype_on_value() {
        let out = JsonbArrayFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                    SqliteValue::Integer(3),
                ],
                &[JSON_SUBTYPE, 0],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_array should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[[1,2],3]");
    }

    #[test]
    fn test_jsonb_object_honors_json_subtype_on_value() {
        let out = JsonbObjectFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("a")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                ],
                &[0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_object should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":[1,2]}"#);
    }

    #[test]
    fn test_registered_json_pretty_accepts_jsonb_blob_input() -> Result<()> {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_pretty", 1)
            .expect("json_pretty should be registered");
        let input = jsonb(r#"{"a":[1,2]}"#).unwrap();
        let out = func
            .invoke(&[SqliteValue::Blob(Arc::from(input))])
            .expect("json_pretty should accept JSONB blob input");
        let SqliteValue::Text(pretty) = out else {
            return Err(FrankenError::function_error(
                "json_pretty should return TEXT",
            ));
        };
        assert!(pretty.contains('\n'));
        assert!(pretty.contains("\"a\""));
        Ok(())
    }

    #[test]
    fn test_registered_json_error_position_accepts_jsonb_blob_input() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_error_position", 1)
            .expect("json_error_position should be registered");
        let input = jsonb(r#"{"a":1}"#).unwrap();
        let out = func
            .invoke(&[SqliteValue::Blob(Arc::from(input))])
            .expect("json_error_position should accept JSONB blob input");
        assert_eq!(out, SqliteValue::Integer(0));
    }

    #[test]
    fn test_json_valid_text() {
        assert_eq!(json(r#"{"a":1}"#).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn test_json_invalid_error() {
        let err = json("not json").unwrap_err();
        assert!(matches!(err, FrankenError::FunctionError(_)));
    }

    #[test]
    fn test_json_valid_flags_default() {
        assert_eq!(json_valid(r#"{"a":1}"#, None), 1);
        assert_eq!(json_valid("not json", None), 0);
    }

    #[test]
    fn test_json_valid_flags_json5() {
        let json5_text = concat!("{", "a:1", "}");
        assert_eq!(json_valid(json5_text, Some(JSON_VALID_JSON5_FLAG)), 1);
        assert_eq!(json_valid(json5_text, Some(JSON_VALID_RFC_8259_FLAG)), 0);
    }

    #[test]
    fn test_json_valid_flags_strict() {
        assert_eq!(json_valid("invalid", Some(JSON_VALID_RFC_8259_FLAG)), 0);
    }

    #[test]
    fn test_json_valid_flags_jsonb() {
        let payload = jsonb(r#"{"a":[1,2,3]}"#).unwrap();
        assert_eq!(
            json_valid_blob(&payload, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            1
        );
        assert_eq!(
            json_valid_blob(&payload, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            1
        );
        // Trailing byte changes total blob size — both superficial and strict
        // reject it since header + payload_len != blob_len.
        let mut trailing = payload.clone();
        trailing.push(0xFF);
        assert_eq!(
            json_valid_blob(&trailing, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            0
        );
        assert_eq!(
            json_valid_blob(&trailing, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            0
        );

        // Corrupt an interior header byte — top-level header still valid
        // (superficial passes) but deep decode fails (strict rejects).
        // Byte 2 is the first sub-element header inside the object payload;
        // 0xFF has node_type=0x0F (invalid) so strict parsing fails.
        let mut corrupted = payload;
        assert!(corrupted.len() > 3);
        corrupted[2] = 0xFF;
        assert_eq!(
            json_valid_blob(&corrupted, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            1
        );
        assert_eq!(
            json_valid_blob(&corrupted, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            0
        );
    }

    #[test]
    fn test_json_type_object() {
        assert_eq!(json_type(r#"{"a":1}"#, None).unwrap(), Some("object"));
    }

    #[test]
    fn test_json_type_path() {
        assert_eq!(
            json_type(r#"{"a":1}"#, Some("$.a")).unwrap(),
            Some("integer")
        );
    }

    #[test]
    fn test_json_type_missing_path() {
        assert_eq!(json_type(r#"{"a":1}"#, Some("$.b")).unwrap(), None);
    }

    #[test]
    fn test_json_extract_single() {
        let result = json_extract(r#"{"a":1}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_json_extract_multiple() {
        let result = json_extract(r#"{"a":1,"b":2}"#, &["$.a", "$.b"]).unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("[1,2]")));
    }

    #[test]
    fn test_json_extract_string_unwrap() {
        let result = json_extract(r#"{"a":"hello"}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("hello")));
    }

    #[test]
    fn test_arrow_preserves_json() {
        let result = json_arrow(r#"{"a":"hello"}"#, "$.a").unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string(r#""hello""#))
        );
    }

    #[test]
    fn test_double_arrow_unwraps() {
        let result = json_double_arrow(r#"{"a":"hello"}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("hello")));
    }

    #[test]
    fn test_json_extract_array_index() {
        let result = json_extract("[10,20,30]", &["$[1]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(20));
    }

    #[test]
    fn test_json_extract_quoted_key_segment() {
        let result = json_extract(r#"{"a.b":1}"#, &["$.\"a.b\""]).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_json_extract_from_end() {
        let result = json_extract("[10,20,30]", &["$[#-1]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(30));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_json_extract_deep_single_path() {
        use std::hint::black_box;
        use std::time::Instant;

        const ROWS: usize = 200_000;
        const REPEATS: usize = 5;
        const JSON: &str = r#"{"a":{"b":{"c":{"d":[{"e":123},{"e":456}]}}}}"#;
        const PATH: &str = "$.a.b.c.d[1].e";

        let func = JsonExtractFunc;
        let args = [
            SqliteValue::Text(SmallText::from_string(JSON)),
            SqliteValue::Text(SmallText::from_string(PATH)),
        ];
        let mut best_ns = u128::MAX;
        let mut last_result = SqliteValue::Null;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..ROWS {
                last_result = black_box(
                    func.invoke(black_box(&args))
                        .expect("json_extract benchmark invocation must succeed"),
                );
            }
            let elapsed_ns = started.elapsed().as_nanos();
            if elapsed_ns < best_ns {
                best_ns = elapsed_ns;
            }
        }

        println!(
            "json_extract_deep_single_path rows={ROWS} repeats={REPEATS} best_ns={best_ns} last_result={last_result:?}"
        );
    }

    #[test]
    fn test_jsonb_extract_returns_blob() {
        let blob = jsonb_extract(r#"{"a":"hello"}"#, &["$.a"]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#""hello""#);
    }

    #[test]
    fn test_json_quote_text() {
        assert_eq!(
            json_quote(&SqliteValue::Text(SmallText::from_string("hello"))).unwrap(),
            r#""hello""#
        );
    }

    #[test]
    fn test_json_quote_null() {
        assert_eq!(json_quote(&SqliteValue::Null).unwrap(), "null");
    }

    #[test]
    fn test_json_array_basic() {
        let out = json_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("two")),
            SqliteValue::Null,
        ])
        .unwrap();
        assert_eq!(out, r#"[1,"two",null]"#);
    }

    #[test]
    fn test_json_object_basic() {
        let out = json_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
            SqliteValue::Text(SmallText::from_string("two")),
        ])
        .unwrap();
        assert_eq!(out, r#"{"a":1,"b":"two"}"#);
    }

    #[test]
    fn test_jsonb_roundtrip() {
        let blob = jsonb(r#"{"a":1,"b":[2,3]}"#).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":1,"b":[2,3]}"#);
    }

    #[test]
    fn test_jsonb_array_variant() {
        let blob = jsonb_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("two")),
            SqliteValue::Null,
        ])
        .unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"[1,"two",null]"#);
    }

    #[test]
    fn test_jsonb_object_variant() {
        let blob = jsonb_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
            SqliteValue::Text(SmallText::from_string("two")),
        ])
        .unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":1,"b":"two"}"#);
    }

    #[test]
    fn test_json_array_length() {
        assert_eq!(json_array_length("[1,2,3]", None).unwrap(), Some(3));
        assert_eq!(json_array_length("[]", None).unwrap(), Some(0));
        assert_eq!(json_array_length(r#"{"a":1}"#, None).unwrap(), Some(0));
    }

    #[test]
    fn test_json_array_length_path() {
        assert_eq!(
            json_array_length(r#"{"a":[1,2,3]}"#, Some("$.a")).unwrap(),
            Some(3)
        );
    }

    #[test]
    fn test_json_array_length_not_array() {
        assert_eq!(
            json_array_length(r#"{"a":1}"#, Some("$.a")).unwrap(),
            Some(0)
        );
        assert_eq!(json_array_length(r#""text""#, None).unwrap(), Some(0));
    }

    #[test]
    fn test_json_error_position_valid() {
        assert_eq!(json_error_position(r#"{"a":1}"#), 0);
    }

    #[test]
    fn test_json_error_position_invalid() {
        assert!(json_error_position(r#"{"a":}"#) > 0);
    }

    #[test]
    fn test_json_pretty_default() {
        let output = json_pretty(r#"{"a":1}"#, None).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("    \"a\""));
    }

    #[test]
    fn test_json_pretty_custom_indent() {
        let output = json_pretty(r#"{"a":1}"#, Some("\t")).unwrap();
        assert!(output.contains("\n\t\"a\""));
    }

    #[test]
    fn test_json_set_create() {
        let out = json_set(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_json_set_nested_path_create() {
        let out = json_set("{}", &[("$.a.b", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":{"b":1}}"#);
    }

    #[test]
    fn test_json_set_nested_array_path_create() {
        let out = json_set("{}", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[1]}"#);
    }

    #[test]
    fn test_json_set_nested_append_path_create() {
        let out = json_set("{}", &[("$.a[#]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[1]}"#);
    }

    #[test]
    fn test_json_set_nested_array_object_create() {
        let out = json_set("{}", &[("$.a[0].b", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[{"b":1}]}"#);
    }

    #[test]
    fn test_json_set_nested_array_index_out_of_range_does_not_scaffold() {
        let out = json_set("{}", &[("$.a[1]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn test_json_set_nested_from_end_does_not_scaffold() {
        let out = json_set("{}", &[("$.a[#-1]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn test_json_set_scalar_root_with_array_path_is_noop() {
        let out = json_set("null", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, "null");
    }

    #[test]
    fn test_json_set_existing_null_value_with_array_path_is_noop() {
        let out = json_set(r#"{"a":null}"#, &[("$.a[1]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":null}"#);
    }

    #[test]
    fn test_json_edits_do_not_descend_through_existing_null_bd_8ouj3() {
        let cases = [
            (r#"{"a":null}"#, "$.a.b.c", r#"{"a":null}"#),
            ("[null]", "$[0][0][0]", "[null]"),
            (r#"{"a":null}"#, "$.a[0].c", r#"{"a":null}"#),
            (r#"{"a":{"b":null}}"#, "$.a.b[0]", r#"{"a":{"b":null}}"#),
        ];

        for (input, path, expected) in cases {
            assert_eq!(
                json_set(input, &[(path, SqliteValue::Integer(1))]).unwrap(),
                expected,
                "json_set path {path}"
            );
            assert_eq!(
                json_insert(input, &[(path, SqliteValue::Integer(1))]).unwrap(),
                expected,
                "json_insert path {path}"
            );
            assert_eq!(
                json_replace(input, &[(path, SqliteValue::Integer(1))]).unwrap(),
                expected,
                "json_replace path {path}"
            );

            for output in [
                jsonb_set(input, &[(path, SqliteValue::Integer(1))]).unwrap(),
                jsonb_insert(input, &[(path, SqliteValue::Integer(1))]).unwrap(),
                jsonb_replace(input, &[(path, SqliteValue::Integer(1))]).unwrap(),
            ] {
                assert_eq!(
                    json_from_jsonb(&output).unwrap(),
                    expected,
                    "JSONB edit path {path}"
                );
            }
        }
    }

    #[test]
    fn test_json_edits_still_scaffold_missing_path_bd_8ouj3() {
        // Negative control: only an *existing* JSON null blocks traversal.
        // Truly missing parents still scaffold exactly as before.
        assert_eq!(
            json_set("{}", &[("$.a.b", SqliteValue::Integer(1))]).unwrap(),
            r#"{"a":{"b":1}}"#
        );
        assert_eq!(
            json_insert("{}", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap(),
            r#"{"a":[1]}"#
        );
    }

    #[test]
    fn test_json_set_overwrite() {
        let out = json_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":2}"#);
    }

    #[test]
    fn test_json_insert_no_overwrite() {
        let out = json_insert(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_json_insert_create() {
        let out = json_insert(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_json_insert_nested_path_create() {
        let out = json_insert("{}", &[("$.a.b", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":{"b":1}}"#);
    }

    #[test]
    fn test_json_insert_nested_array_path_create() {
        let out = json_insert("{}", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[1]}"#);
    }

    #[test]
    fn test_json_replace_overwrite() {
        let out = json_replace(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":2}"#);
    }

    #[test]
    fn test_json_replace_no_create() {
        let out = json_replace(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_json_remove_key() {
        let out = json_remove(r#"{"a":1,"b":2}"#, &["$.a"]).unwrap();
        assert_eq!(out, r#"{"b":2}"#);
    }

    #[test]
    fn test_json_remove_array_compact() {
        let out = json_remove("[1,2,3]", &["$[1]"]).unwrap();
        assert_eq!(out, "[1,3]");
    }

    #[test]
    fn test_json_remove_preserves_key_order() {
        // Removing a middle key must leave the remaining members in their
        // original order (SQLite parity). A swap_remove would move the LAST key
        // ("d") into the hole -> {"a":1,"d":4,"c":3}. Needs 3+ keys to catch;
        // test_json_remove_key with 2 keys cannot distinguish swap vs shift.
        let out = json_remove(r#"{"a":1,"b":2,"c":3,"d":4}"#, &["$.b"]).unwrap();
        assert_eq!(out, r#"{"a":1,"c":3,"d":4}"#);
        // Removing the first key likewise preserves the tail order.
        let out2 = json_remove(r#"{"a":1,"b":2,"c":3}"#, &["$.a"]).unwrap();
        assert_eq!(out2, r#"{"b":2,"c":3}"#);
    }

    #[test]
    fn test_json_patch_null_remove_preserves_order() {
        // RFC 7396 / SQLite json_patch drops a null-valued key while keeping the
        // remaining members in order (merge_patch must shift_remove, not remove).
        let out = json_patch(r#"{"a":1,"b":2,"c":3,"d":4}"#, r#"{"b":null}"#).unwrap();
        assert_eq!(out, r#"{"a":1,"c":3,"d":4}"#);
    }

    #[test]
    fn test_json_patch_merge() {
        let out = json_patch(r#"{"a":1,"b":2}"#, r#"{"b":3,"c":4}"#).unwrap();
        assert_eq!(out, r#"{"a":1,"b":3,"c":4}"#);
    }

    #[test]
    fn test_json_patch_delete() {
        let out = json_patch(r#"{"a":1,"b":2}"#, r#"{"b":null}"#).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_jsonb_set_variant() {
        let blob = jsonb_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(9))]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":9}"#);
    }

    #[test]
    fn test_jsonb_insert_variant() {
        let blob = jsonb_insert(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_jsonb_replace_variant() {
        let blob = jsonb_replace(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(5))]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":5}"#);
    }

    #[test]
    fn test_jsonb_remove_variant() {
        let blob = jsonb_remove(r#"{"a":1,"b":2}"#, &["$.a"]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"b":2}"#);
    }

    #[test]
    fn test_jsonb_patch_variant() {
        let blob = jsonb_patch(r#"{"a":1,"b":2}"#, r#"{"b":7}"#).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":1,"b":7}"#);
    }

    #[test]
    fn test_json_group_array_includes_nulls() {
        let out = json_group_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Null,
            SqliteValue::Integer(3),
        ])
        .unwrap();
        assert_eq!(out, "[1,null,3]");
    }

    #[test]
    fn test_json_group_array_basic() {
        let out = json_group_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Integer(2),
            SqliteValue::Integer(3),
        ])
        .unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_group_object_basic() {
        let out = json_group_object(&[
            (
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Integer(1),
            ),
            (
                SqliteValue::Text(SmallText::from_string("b")),
                SqliteValue::Integer(2),
            ),
        ])
        .unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
    }

    // bd-55eq3: stock json_group_object also keeps duplicate labels verbatim in
    // row order (`{"a":1,"a":2,"b":3}`, verified against sqlite3 3.46.1), not
    // last-wins.
    #[test]
    fn test_json_group_object_duplicate_keys_kept_verbatim() {
        let out = json_group_object(&[
            (
                SqliteValue::Text(SmallText::from_string("k")),
                SqliteValue::Integer(1),
            ),
            (
                SqliteValue::Text(SmallText::from_string("k")),
                SqliteValue::Integer(2),
            ),
        ])
        .unwrap();
        assert_eq!(out, r#"{"k":1,"k":2}"#);
    }

    /// Stock's jsonObjectStep measures the label with `sqlite3Strlen30`, so
    /// every group-object form ends a label at its first NUL; `json_object`
    /// keeps the whole label.
    #[test]
    fn test_group_object_labels_end_at_the_first_nul() {
        let text = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        let rows = [
            (text("a\0b"), SqliteValue::Integer(1)),
            (text("\0hidden"), SqliteValue::Integer(2)),
            (SqliteValue::Null, SqliteValue::Integer(3)),
            (text("plain"), SqliteValue::Integer(4)),
        ];
        let expected = r#"{"a":1,"":2,"plain":4}"#;
        assert_eq!(json_group_object(&rows).unwrap(), expected);
        assert_eq!(
            json_from_jsonb(&jsonb_group_object(&rows).unwrap()).unwrap(),
            expected
        );

        let aggregate = JsonGroupObjectFunc;
        let mut state = aggregate.initial_state();
        for (label, value) in &rows {
            aggregate
                .step(&mut state, &[label.clone(), value.clone()])
                .unwrap();
        }
        assert_eq!(
            aggregate.finalize(state).unwrap(),
            SqliteValue::Text(SmallText::from_string(expected))
        );

        let aggregate = JsonbGroupObjectFunc;
        let mut state = aggregate.initial_state();
        for (label, value) in &rows {
            aggregate
                .step(&mut state, &[label.clone(), value.clone()])
                .unwrap();
        }
        let SqliteValue::Blob(blob) = aggregate.finalize(state).unwrap() else {
            panic!("jsonb_group_object must return a BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), expected);

        for window in JsonGroupWindowFunc::ALL
            .iter()
            .filter(|window| window.object)
        {
            let mut state = window.initial_state();
            for (label, value) in &rows {
                window
                    .step(&mut state, &[label.clone(), value.clone()])
                    .unwrap();
            }
            let rendered = match window.value(&state).unwrap() {
                SqliteValue::Text(text) => text.to_string(),
                SqliteValue::Blob(blob) => json_from_jsonb(&blob).unwrap(),
                other => panic!("{} returned {other:?}", window.name),
            };
            assert_eq!(rendered, expected, "{}", window.name);
        }

        assert_eq!(
            json_object(&[text("a\0b"), SqliteValue::Integer(1)]).unwrap(),
            r#"{"a\u0000b":1}"#
        );
    }

    #[test]
    fn test_jsonb_group_array_and_object_variants() {
        let array_blob = jsonb_group_array(&[SqliteValue::Integer(1), SqliteValue::Null]).unwrap();
        assert_eq!(json_from_jsonb(&array_blob).unwrap(), "[1,null]");

        let object_blob = jsonb_group_object(&[(
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(7),
        )])
        .unwrap();
        assert_eq!(json_from_jsonb(&object_blob).unwrap(), r#"{"a":7}"#);
    }

    #[test]
    fn test_json_each_array() {
        let rows = json_each("[10,20]", None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, SqliteValue::Integer(0));
        assert_eq!(rows[1].key, SqliteValue::Integer(1));
        assert_eq!(rows[0].value, SqliteValue::Integer(10));
        assert_eq!(rows[1].value, SqliteValue::Integer(20));
    }

    #[test]
    fn test_json_each_object() {
        let rows = json_each(r#"{"a":1,"b":2}"#, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, SqliteValue::Text(SmallText::from_string("a")));
        assert_eq!(rows[1].key, SqliteValue::Text(SmallText::from_string("b")));
        assert_eq!(rows[0].value, SqliteValue::Integer(1));
        assert_eq!(rows[1].value, SqliteValue::Integer(2));
    }

    #[test]
    fn test_json_each_path() {
        let rows = json_each(r#"{"a":{"b":1,"c":2}}"#, Some("$.a")).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "$.a");
        assert_eq!(rows[1].path, "$.a");
    }

    #[test]
    fn test_json_each_nested_value_is_json_text() {
        let rows = json_each(r#"{"a":[1,2]}"#, None).unwrap();
        assert_eq!(
            rows[0].value,
            SqliteValue::Text(SmallText::from_string("[1,2]"))
        );
        assert_eq!(rows[0].atom, SqliteValue::Null); // arrays have null atom
    }

    #[test]
    fn test_json_each_blob_accepts_text_json_and_jsonb() {
        let text_rows = json_each_blob(br#"{"a":[10,20],"b":30}"#, Some("$.a")).unwrap();
        let jsonb_input = jsonb(r#"{"a":[10,20],"b":30}"#).unwrap();
        let jsonb_rows = json_each_blob(&jsonb_input, Some("$.a")).unwrap();

        assert_eq!(jsonb_rows, text_rows);
        assert_eq!(
            jsonb_rows
                .iter()
                .map(|row| row.value.clone())
                .collect::<Vec<_>>(),
            vec![SqliteValue::Integer(10), SqliteValue::Integer(20)]
        );
    }

    #[test]
    fn test_json_tree_recursive() {
        let rows = json_tree(r#"{"a":{"b":1}}"#, None).unwrap();
        assert!(rows.iter().any(|row| row.fullkey == "$.a"));
        assert!(rows.iter().any(|row| row.fullkey == "$.a.b"));
    }

    #[test]
    fn test_json_tree_columns() {
        let rows = json_tree(r#"{"a":{"b":1}}"#, None).unwrap();
        let row = rows
            .iter()
            .find(|candidate| candidate.fullkey == "$.a.b")
            .expect("nested row should exist");
        assert_eq!(row.key, SqliteValue::Text(SmallText::from_string("b")));
        assert_eq!(row.value, SqliteValue::Integer(1));
        assert_eq!(row.type_name, "integer");
        assert_eq!(row.atom, SqliteValue::Integer(1));
        assert_eq!(row.path, "$.a");
    }

    #[test]
    fn test_json_tree_blob_accepts_jsonb_path() {
        let jsonb_input = jsonb(r#"{"a":{"b":[1,2]},"c":3}"#).unwrap();
        let rows = json_tree_blob(&jsonb_input, Some("$.a")).unwrap();

        assert_eq!(rows.first().map(|row| row.fullkey.as_str()), Some("$.a"));
        assert!(
            rows.iter()
                .any(|row| row.fullkey == "$.a.b[1]" && row.value == SqliteValue::Integer(2))
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.fullkey == "$.a.b[1]")
                .map(|row| row.path.as_str()),
            Some("$.a.b")
        );
    }

    #[test]
    fn test_json_structural_snapshot_table_functions_jsonb() -> Result<()> {
        let input = jsonb(r#"{"a":[10,{"b":20}],"c":null}"#)?;
        let each = json_each_blob(&input, Some("$.a"))?
            .iter()
            .map(json_table_row_structure)
            .collect();
        let tree = json_tree_blob(&input, Some("$.a"))?
            .iter()
            .map(json_table_row_structure)
            .collect();

        let actual = format!("{:#?}", JsonTableJsonbStructure { each, tree });
        let expected = r#"JsonTableJsonbStructure {
    each: [
        JsonTableRowStructure {
            key: "Integer(0)",
            value: "Integer(10)",
            type_name: "integer",
            atom: "Integer(10)",
            id: 5,
            parent: "Null",
            fullkey: "$.a[0]",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Integer(1)",
            value: "Text(\"{\\\"b\\\":20}\")",
            type_name: "object",
            atom: "Null",
            id: 8,
            parent: "Null",
            fullkey: "$.a[1]",
            path: "$.a",
        },
    ],
    tree: [
        JsonTableRowStructure {
            key: "Text(\"a\")",
            value: "Text(\"[10,{\\\"b\\\":20}]\")",
            type_name: "array",
            atom: "Null",
            id: 2,
            parent: "Null",
            fullkey: "$.a",
            path: "$",
        },
        JsonTableRowStructure {
            key: "Integer(0)",
            value: "Integer(10)",
            type_name: "integer",
            atom: "Integer(10)",
            id: 5,
            parent: "Integer(2)",
            fullkey: "$.a[0]",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Integer(1)",
            value: "Text(\"{\\\"b\\\":20}\")",
            type_name: "object",
            atom: "Null",
            id: 8,
            parent: "Integer(2)",
            fullkey: "$.a[1]",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Text(\"b\")",
            value: "Integer(20)",
            type_name: "integer",
            atom: "Integer(20)",
            id: 9,
            parent: "Integer(8)",
            fullkey: "$.a[1].b",
            path: "$.a[1]",
        },
    ],
}"#;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_json_tree_vtab_cursor_scan() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":{"b":1}}"#)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                ],
            )
            .unwrap();

        let mut fullkeys = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 6).unwrap();
            let fullkey = ctx.take_value().unwrap();
            if let SqliteValue::Text(text) = fullkey {
                fullkeys.push(text);
            }
            cursor.next(&cx).unwrap();
        }

        assert_eq!(
            fullkeys,
            vec![SmallText::from("$.a"), SmallText::from("$.a.b")]
        );
    }

    #[test]
    fn test_json_each_vtab_cursor_accepts_jsonb_blob_input() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        let input = jsonb(r#"{"a":[10,20]}"#).unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Blob(Arc::from(input)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                ],
            )
            .expect("json_each cursor should accept JSONB blob input");

        let mut values = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 1).unwrap();
            values.push(ctx.take_value().unwrap());
            cursor.next(&cx).unwrap();
        }

        assert_eq!(
            values,
            vec![SqliteValue::Integer(10), SqliteValue::Integer(20)]
        );
    }

    #[test]
    fn test_json_table_cursor_rowids_match_stock_and_reset_on_filter() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let cx = Cx::new();
        let mut each = JsonEachCursor::default();
        let mut tree = JsonTreeCursor::default();
        let cursors: [(&str, &mut dyn VirtualTableCursor); 2] =
            [("json_each", &mut each), ("json_tree", &mut tree)];
        let nested = r#"{"a":[10,20,{"b":[30,40]}]}"#;
        let args = [
            (SqliteValue::from("[10,20,30]"), SqliteValue::from("$")),
            (SqliteValue::from(nested), SqliteValue::from("$.a")),
            (
                SqliteValue::Blob(Arc::from(jsonb(nested).unwrap())),
                SqliteValue::from("$.a[2].b"),
            ),
            (SqliteValue::from(nested), SqliteValue::from("$.absent")),
            (SqliteValue::from("42"), SqliteValue::from("$")),
        ];

        for (name, cursor) in cursors {
            for (input, root) in &args {
                let args = [input.clone(), root.clone()];
                let expected = stock_json_rows(
                    &stock,
                    &format!("SELECT rowid, value FROM {name}(?1, ?2)"),
                    &args,
                )
                .unwrap();
                cursor.filter(&cx, 0, None, &args).unwrap();
                let mut actual = Vec::new();
                while !cursor.eof() {
                    let mut context = ColumnContext::new();
                    cursor.column(&mut context, 1).unwrap();
                    actual.push(vec![
                        SqliteValue::Integer(cursor.rowid().unwrap()),
                        context.take_value().unwrap(),
                    ]);
                    cursor.next(&cx).unwrap();
                }
                assert_eq!(actual, expected, "{name} input={input:?} root={root:?}");
            }
        }
    }

    #[test]
    fn test_json_each_cursor_past_end_returns_null_and_zero_rowid() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string("[10]"))],
            )
            .unwrap();

        assert!(!cursor.eof());
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 1).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn test_json_each_cursor_invalid_column_returns_null() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string("[10]"))],
            )
            .unwrap();

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 99).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_json_tree_vtab_cursor_accepts_jsonb_blob_input() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        let input = jsonb(r#"{"a":{"b":1}}"#).unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Blob(Arc::from(input)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                ],
            )
            .expect("json_tree cursor should accept JSONB blob input");

        let mut fullkeys = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 6).unwrap();
            let fullkey = ctx.take_value().unwrap();
            if let SqliteValue::Text(text) = fullkey {
                fullkeys.push(text);
            }
            cursor.next(&cx).unwrap();
        }

        assert_eq!(
            fullkeys,
            vec![SmallText::from("$.a"), SmallText::from("$.a.b")]
        );
    }

    #[test]
    fn test_json_tree_cursor_past_end_returns_null_and_zero_rowid() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#))],
            )
            .unwrap();

        while !cursor.eof() {
            cursor.next(&cx).unwrap();
        }

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 6).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn test_json_tree_cursor_invalid_column_returns_null() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#))],
            )
            .unwrap();

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 99).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    fn json_cursor_column(cursor: &dyn VirtualTableCursor, column: i32) -> SqliteValue {
        let mut context = ColumnContext::new();
        cursor.column(&mut context, column).unwrap();
        context.take_value().unwrap()
    }

    #[test]
    fn json_table_hidden_inputs_preserve_sqlite_storage_classes() {
        let cx = Cx::new();
        // Applications can register either factory under another name. Its
        // schema and hidden-column declaration must travel with the factory.
        let factories: [Box<dyn fsqlite_func::vtab::VtabModuleFactory>; 2] = [
            Box::new(fsqlite_func::vtab::module_factory_from::<JsonEachVtab>()),
            Box::new(fsqlite_func::vtab::module_factory_from::<JsonTreeVtab>()),
        ];
        for factory in factories {
            let args = ["custom_json", "main", "custom_json"];
            let schema = factory.column_info(&args);
            assert_eq!(schema.len(), 10);
            assert!(schema.iter().all(|(_, affinity)| *affinity == 'A'));
            assert_eq!(
                &schema[8..],
                &[("json".to_owned(), 'A'), ("root".to_owned(), 'A')]
            );
            assert_eq!(factory.hidden_column_names(&args), ["json", "root"]);
        }
        let jsonb = SqliteValue::Blob(jsonb("[1,2]").unwrap().into());
        let cases = [
            (
                SqliteValue::Text(" [1,[2,3]] ".into()),
                SqliteValue::Text(" [1,[2,3]] ".into()),
            ),
            (
                SqliteValue::Text(" {items:[1,2],} ".into()),
                SqliteValue::Text(" {items:[1,2],} ".into()),
            ),
            (SqliteValue::Integer(123), SqliteValue::Text("123".into())),
            (SqliteValue::Float(1.5), SqliteValue::Text("1.5".into())),
            (
                SqliteValue::Float(f64::INFINITY),
                SqliteValue::Text("Inf".into()),
            ),
            (
                SqliteValue::Blob(Arc::from(b"[1,2]".as_slice())),
                SqliteValue::Text("[1,2]".into()),
            ),
            (jsonb.clone(), jsonb),
        ];
        for mut cursor in [
            Box::new(JsonEachCursor::default()) as Box<dyn VirtualTableCursor>,
            Box::new(JsonTreeCursor::default()),
        ] {
            for (input, expected) in &cases {
                cursor
                    .filter(&cx, 0, None, std::slice::from_ref(input))
                    .unwrap();
                assert!(!cursor.eof(), "{input:?}");
                while !cursor.eof() {
                    assert_eq!(json_cursor_column(&*cursor, 8), *expected, "{input:?}");
                    assert_eq!(
                        json_cursor_column(&*cursor, 9),
                        SqliteValue::Text("$".into())
                    );
                    cursor.next(&cx).unwrap();
                }
            }
        }
    }

    #[test]
    fn json_table_rowids_are_zero_based_scan_ordinals_and_reset_on_refilter() {
        let cx = Cx::new();
        for mut cursor in [
            Box::new(JsonEachCursor::default()) as Box<dyn VirtualTableCursor>,
            Box::new(JsonTreeCursor::default()),
        ] {
            for root in ["$", "$.items"] {
                let input = SqliteValue::Text(r#"{"items":[1,[2,3]],"other":4}"#.into());
                cursor
                    .filter(
                        &cx,
                        0,
                        None,
                        &[input.clone(), SqliteValue::Text(root.into())],
                    )
                    .unwrap();
                let mut ordinal = 0;
                while !cursor.eof() {
                    assert_eq!(cursor.rowid().unwrap(), ordinal);
                    assert_eq!(json_cursor_column(&*cursor, 8), input);
                    assert_eq!(
                        json_cursor_column(&*cursor, 9),
                        SqliteValue::Text(root.into())
                    );
                    ordinal += 1;
                    cursor.next(&cx).unwrap();
                }
                assert!(ordinal >= 2);
                assert_eq!(cursor.rowid().unwrap(), 0);
            }
        }
    }

    #[test]
    fn json_table_refilter_failures_and_empty_inputs_clear_previous_rows() {
        let cx = Cx::new();
        for mut cursor in [
            Box::new(JsonEachCursor::default()) as Box<dyn VirtualTableCursor>,
            Box::new(JsonTreeCursor::default()),
        ] {
            for (args, expected_error) in [
                (
                    vec![SqliteValue::Text("bad".into())],
                    Some("malformed JSON"),
                ),
                (
                    vec![
                        SqliteValue::Text("[1]".into()),
                        SqliteValue::Text("bad path".into()),
                    ],
                    Some("bad JSON path"),
                ),
                (
                    vec![SqliteValue::Null, SqliteValue::Null, SqliteValue::Null],
                    Some("too many arguments"),
                ),
                (Vec::new(), None),
                (vec![SqliteValue::Null], None),
                (
                    vec![SqliteValue::Null, SqliteValue::Text("bad path".into())],
                    None,
                ),
                (
                    vec![SqliteValue::Text("[1]".into()), SqliteValue::Null],
                    None,
                ),
                (
                    vec![SqliteValue::Text("bad".into()), SqliteValue::Null],
                    Some("malformed JSON"),
                ),
            ] {
                cursor
                    .filter(&cx, 0, None, &[SqliteValue::Text("[10]".into())])
                    .unwrap();
                assert!(!cursor.eof());
                let result = cursor.filter(&cx, 0, None, &args);
                if let Some(expected_error) = expected_error {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains(expected_error), "{error}");
                } else {
                    result.unwrap();
                }
                assert!(cursor.eof(), "{args:?}");
                assert_eq!(cursor.rowid().unwrap(), 0);
                for column in [1, 8, 9] {
                    assert_eq!(json_cursor_column(&*cursor, column), SqliteValue::Null);
                }
            }
        }
    }

    #[test]
    fn test_jsonb_chain_validity() {
        let first = jsonb_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(9))]).unwrap();
        let first_text = json_from_jsonb(&first).unwrap();
        let second = jsonb_patch(&first_text, r#"{"b":2}"#).unwrap();
        assert_eq!(
            json_valid_blob(&second, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            1
        );
    }

    // -----------------------------------------------------------------------
    // json() edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_minify_whitespace() {
        assert_eq!(json("  { \"a\" : 1 }  ").unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn test_json_scalar_string() {
        assert_eq!(json(r#""hello""#).unwrap(), r#""hello""#);
    }

    #[test]
    fn test_json_scalar_number() {
        assert_eq!(json("42").unwrap(), "42");
    }

    #[test]
    fn test_json_scalar_null() {
        assert_eq!(json("null").unwrap(), "null");
    }

    #[test]
    fn test_json_scalar_bool() {
        assert_eq!(json("true").unwrap(), "true");
        assert_eq!(json("false").unwrap(), "false");
    }

    #[test]
    fn test_json_nested_structure() {
        let input = r#"{"a":{"b":[1,2,{"c":3}]}}"#;
        assert_eq!(json(input).unwrap(), input);
    }

    #[test]
    fn test_json_unicode() {
        let input = r#"{"key":"\u00fc\u00e9"}"#;
        let result = json(input).unwrap();
        // After parse/re-serialize, unicode escapes become literal chars
        assert!(result.contains("key"));
    }

    // -----------------------------------------------------------------------
    // json_valid edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_valid_zero_flags() {
        assert_eq!(json_valid(r#"{"a":1}"#, Some(0)), 0);
    }

    #[test]
    fn test_registered_json_valid_rejects_out_of_range_flags() {
        let func = JsonValidFunc;
        for flags in [0, 16, -1] {
            let err = func
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                    SqliteValue::Integer(flags),
                ])
                .expect_err("SQL json_valid must reject flags outside 1..=15");
            assert!(
                err.to_string().contains("between 1 and 15")
                    || err.to_string().contains("out of range"),
                "unexpected error for flags {flags}: {err}"
            );
        }
    }

    #[test]
    fn test_json_valid_empty_string() {
        assert_eq!(json_valid("", None), 0);
    }

    // -----------------------------------------------------------------------
    // json_type all variants
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_type_null() {
        assert_eq!(json_type("null", None).unwrap(), Some("null"));
    }

    #[test]
    fn test_json_type_true() {
        assert_eq!(json_type("true", None).unwrap(), Some("true"));
    }

    #[test]
    fn test_json_type_false() {
        assert_eq!(json_type("false", None).unwrap(), Some("false"));
    }

    #[test]
    fn test_json_type_real() {
        assert_eq!(json_type("3.14", None).unwrap(), Some("real"));
    }

    #[test]
    fn test_json_type_text() {
        assert_eq!(json_type(r#""hello""#, None).unwrap(), Some("text"));
    }

    #[test]
    fn test_json_type_array() {
        assert_eq!(json_type("[1,2]", None).unwrap(), Some("array"));
    }

    #[test]
    fn test_json_valid_bare_numeric_values() {
        // Regression (#259): bare SQL numerics are valid JSON; a non-finite REAL
        // (Inf/NaN) is not (e.g. json_valid(9e999) -> 0).
        let f = JsonValidFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123)]).unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(f64::INFINITY)]).unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(f64::NAN)]).unwrap(),
            SqliteValue::Integer(0)
        );
        // A bare numeric renders to its JSON numeric text and is checked against
        // the flags, so it is valid JSON/JSON5 (1/2) but is NOT a JSONB blob
        // (4/8 -> 0), matching C SQLite json_valid(123,4)=0 / (123,8)=0.
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(1)])
                .unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(2)])
                .unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(4)])
                .unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(8)])
                .unwrap(),
            SqliteValue::Integer(0)
        );
        // json_valid(NULL) -> NULL.
        assert_eq!(f.invoke(&[SqliteValue::Null]).unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_json_type_bare_numeric_values() {
        // Regression (#260): json_type(123) -> 'integer', json_type(1.5) -> 'real'.
        let f = JsonTypeFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123)]).unwrap(),
            SqliteValue::Text(SmallText::from_string("integer"))
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Text(SmallText::from_string("real"))
        );
    }

    #[test]
    fn test_json_table_valued_bare_numeric_input() {
        // Regression (#260): json_each/json_tree accept a bare SQL numeric as a
        // JSON number (yielding a single scalar row) instead of erroring
        // "must be TEXT or BLOB JSON". C SQLite: json_each(1.5) -> type='real'
        // atom=1.5; json_tree(7) -> type='integer'.
        let (blob_float, hidden) =
            super::parse_json_table_filter_args("json_each", &[SqliteValue::Float(1.5)])
                .unwrap()
                .unwrap();
        let v_float = super::decode_jsonb_root(&blob_float).unwrap();
        // A non-integral REAL is carried as its stock text (bd-1nxwh); it is
        // still a JSON real whose atom is 1.5.
        assert_eq!(super::json_type_name(&v_float), "real");
        assert_eq!(
            super::json_to_sqlite_scalar(&v_float).unwrap(),
            SqliteValue::Float(1.5)
        );
        // An omitted path uses the root, also exposed by the hidden column.
        assert_eq!(hidden.root, SmallText::from("$"));
        let (blob_int, _) =
            super::parse_json_table_filter_args("json_tree", &[SqliteValue::Integer(7)])
                .unwrap()
                .unwrap();
        let v_int = super::decode_jsonb_root(&blob_int).unwrap();
        assert_eq!(v_int, Value::Number(7_i64.into()));
        // GH#212 (verified against sqlite3 3.46.1): json_each(9e999) yields a
        // single row (type='real', atom=Inf), so a bare +Inf renders as the
        // numeric literal 9.0e+999 rather than erroring. The non-finite REAL is
        // carried as a tagged string (bd-yalqc).
        let (blob_inf, _) =
            super::parse_json_table_filter_args("json_each", &[SqliteValue::Float(f64::INFINITY)])
                .unwrap()
                .unwrap();
        let v_inf = super::decode_jsonb_root(&blob_inf).unwrap();
        assert_eq!(v_inf, super::number_text_value("9.0e+999"));
    }

    #[test]
    fn test_json_error_position_bare_numeric() {
        // Regression (#260): a bare finite numeric is valid JSON, so
        // json_error_position(1) = json_error_position(1.5) = 0 (no parse error).
        let f = JsonErrorPositionFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(1)]).unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Integer(0)
        );
        // NULL still short-circuits to NULL. GH#212 (verified against sqlite3
        // 3.46.1): json_error_position(9e999) = 0 — a bare +Inf renders as a
        // valid JSON numeric literal (9.0e+999), so there is no parse error.
        assert_eq!(f.invoke(&[SqliteValue::Null]).unwrap(), SqliteValue::Null);
        assert_eq!(
            f.invoke(&[SqliteValue::Float(f64::INFINITY)]).unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_json_mutation_embeds_nested_json_subtype() {
        // Regression (#233): a value carrying the JSON subtype (produced by
        // json(...)) is embedded as a JSON subtree, not stringified.
        let nested = SqliteValue::Text(SmallText::from_string(r#"{"a":2}"#));
        let subtypes = [0u32, 0, JSON_SUBTYPE];
        let embedded = SqliteValue::Text(SmallText::from_string(r#"{"x":{"a":2}}"#));

        let set_args = [
            SqliteValue::Text(SmallText::from_string("{}")),
            SqliteValue::Text(SmallText::from_string("$.x")),
            nested.clone(),
        ];
        assert_eq!(
            JsonSetFunc
                .invoke_with_arg_subtypes(&set_args, &subtypes)
                .unwrap(),
            embedded
        );
        assert_eq!(
            JsonInsertFunc
                .invoke_with_arg_subtypes(&set_args, &subtypes)
                .unwrap(),
            embedded
        );

        // json_replace only rewrites an existing key.
        let replace_args = [
            SqliteValue::Text(SmallText::from_string(r#"{"x":9}"#)),
            SqliteValue::Text(SmallText::from_string("$.x")),
            nested,
        ];
        assert_eq!(
            JsonReplaceFunc
                .invoke_with_arg_subtypes(&replace_args, &subtypes)
                .unwrap(),
            embedded
        );
    }

    // -----------------------------------------------------------------------
    // json_extract edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_extract_missing_path_null() {
        let result = json_extract(r#"{"a":1}"#, &["$.b"]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_json_extract_no_paths_error() {
        let empty: &[&str] = &[];
        assert!(json_extract(r#"{"a":1}"#, empty).is_err());

        let func = JsonExtractFunc;
        let args = vec![SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#))];
        assert_eq!(func.invoke(&args).unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_json_extract_null_value() {
        let result = json_extract(r#"{"a":null}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_json_extract_boolean() {
        let result = json_extract(r#"{"a":true}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
        let result = json_extract(r#"{"a":false}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(0));
    }

    #[test]
    fn test_json_extract_nested_array() {
        let result = json_extract(r#"{"a":[[1,2],[3,4]]}"#, &["$.a[1][0]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(3));
    }

    #[test]
    fn test_json_extract_multiple_with_missing() {
        let result = json_extract(r#"{"a":1}"#, &["$.a", "$.b"]).unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("[1,null]"))
        );
    }

    // -----------------------------------------------------------------------
    // json_arrow edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_arrow_missing_path_null() {
        let result = json_arrow(r#"{"a":1}"#, "$.b").unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_json_arrow_number() {
        let result = json_arrow(r#"{"a":42}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("42")));
    }

    #[test]
    fn test_json_arrow_null() {
        let result = json_arrow(r#"{"a":null}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("null")));
    }

    // -----------------------------------------------------------------------
    // json_array_length edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_array_length_nested_not_array() {
        assert_eq!(
            json_array_length(r#"{"a":"text"}"#, Some("$.a")).unwrap(),
            Some(0)
        );
    }

    #[test]
    fn test_json_array_length_missing_path() {
        assert_eq!(json_array_length(r#"{"a":1}"#, Some("$.b")).unwrap(), None);
    }

    // -----------------------------------------------------------------------
    // json_error_position edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_error_position_empty() {
        assert!(json_error_position("") > 0);
    }

    #[test]
    fn test_json_error_position_just_brace() {
        assert!(json_error_position("{") > 0);
    }

    // -----------------------------------------------------------------------
    // json_pretty edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_pretty_empty_array() {
        assert_eq!(json_pretty("[]", None).unwrap(), "[]");
    }

    #[test]
    fn test_json_pretty_empty_object() {
        assert_eq!(json_pretty("{}", None).unwrap(), "{}");
    }

    #[test]
    fn test_json_pretty_scalar() {
        assert_eq!(json_pretty("42", None).unwrap(), "42");
    }

    #[test]
    fn test_json_pretty_nested() {
        let result = json_pretty(r#"{"a":[1,2]}"#, None).unwrap();
        assert!(result.contains('\n'));
        assert!(result.contains("\"a\""));
    }

    // -----------------------------------------------------------------------
    // bd-6i2s required tests: json_pretty + jsonb availability
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_pretty_object() {
        let output = json_pretty(r#"{"a":1,"b":[2,3]}"#, None).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("    \"a\""));
        assert!(output.contains("    \"b\""));
    }

    #[test]
    fn test_json_pretty_array() {
        let output = json_pretty("[1,2,3]", None).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("    1"));
        assert!(output.contains("    2"));
        assert!(output.contains("    3"));
    }

    #[test]
    fn test_json_pretty_idempotent() {
        let input = r#"{"a":1,"b":[2,3]}"#;
        let first = json_pretty(input, None).unwrap();
        let second = json_pretty(&first, None).unwrap();
        assert_eq!(first, second, "json_pretty should be idempotent");
    }

    #[test]
    fn test_jsonb_functions_available() {
        let blob = jsonb_array(&[SqliteValue::Integer(1), SqliteValue::Integer(2)]).unwrap();
        assert!(
            !blob.is_empty(),
            "jsonb_array should produce non-empty output"
        );

        let blob2 = jsonb_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(9))]).unwrap();
        assert!(
            !blob2.is_empty(),
            "jsonb_set should produce non-empty output"
        );

        let blob3 = jsonb_object(&[
            SqliteValue::Text(SmallText::from_string("key")),
            SqliteValue::Integer(42),
        ])
        .unwrap();
        assert!(
            !blob3.is_empty(),
            "jsonb_object should produce non-empty output"
        );
    }

    // -----------------------------------------------------------------------
    // json_quote edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_quote_integer() {
        assert_eq!(json_quote(&SqliteValue::Integer(42)).unwrap(), "42");
        assert_eq!(json_quote(&SqliteValue::Integer(-1)).unwrap(), "-1");
    }

    #[test]
    fn test_json_quote_float() {
        #[allow(clippy::approx_constant)]
        let result = json_quote(&SqliteValue::Float(3.14)).unwrap();
        assert!(result.starts_with("3.14"));
    }

    #[test]
    fn test_json_quote_float_infinity() {
        // GH#212 (verified against sqlite3 3.46.1): stock renders +Inf/-Inf as
        // the numeric literal 9.0e+999 / -9.0e+999; only NaN maps to `null`.
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::INFINITY)).unwrap(),
            "9.0e+999"
        );
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::NEG_INFINITY)).unwrap(),
            "-9.0e+999"
        );
        assert_eq!(json_quote(&SqliteValue::Float(f64::NAN)).unwrap(), "null");
    }

    // -----------------------------------------------------------------------
    // GH#212: non-finite REALs (±Inf -> 9.0e+999 / -9.0e+999, NaN -> null).
    // Every expected string below was captured from stock sqlite3 3.46.1.
    // -----------------------------------------------------------------------

    fn text(value: &str) -> SqliteValue {
        SqliteValue::Text(SmallText::from_string(value))
    }

    #[test]
    fn test_json_array_non_finite_reals() {
        // SELECT json_array(1e999)         -> [9.0e+999]
        assert_eq!(
            json_array(&[SqliteValue::Float(f64::INFINITY)]).unwrap(),
            "[9.0e+999]"
        );
        // SELECT json_array(-1e999)        -> [-9.0e+999]
        assert_eq!(
            json_array(&[SqliteValue::Float(f64::NEG_INFINITY)]).unwrap(),
            "[-9.0e+999]"
        );
        // SELECT json_array(1.0, 2e308*10) -> [1.0,9.0e+999]
        assert_eq!(
            json_array(&[SqliteValue::Float(1.0), SqliteValue::Float(f64::INFINITY)]).unwrap(),
            "[1.0,9.0e+999]"
        );
        // SELECT json_array( (SELECT 1e999 - 1e999) ) -> [null]  (NaN)
        assert_eq!(
            json_array(&[SqliteValue::Float(f64::NAN)]).unwrap(),
            "[null]"
        );
    }

    #[test]
    fn test_json_object_non_finite_real() {
        // SELECT json_object('k', 1e999)   -> {"k":9.0e+999}
        assert_eq!(
            json_object(&[text("k"), SqliteValue::Float(f64::INFINITY)]).unwrap(),
            r#"{"k":9.0e+999}"#
        );
        // NaN value -> null.
        assert_eq!(
            json_object(&[text("k"), SqliteValue::Float(f64::NAN)]).unwrap(),
            r#"{"k":null}"#
        );
    }

    /// bd-1nxwh: the parse-side fast path that keeps a token a plain `Number`
    /// must only accept text Rust's `{:?}` prints for that value; everything
    /// else goes to the exact check. On the construct side only an integral
    /// REAL below 1e15 stays a `Number`: SQLite's text agrees with Rust's
    /// there, and nowhere else reliably (42.333333333333 is 42.333333333333002).
    #[test]
    fn test_number_text_fast_paths_agree_with_the_formatters() {
        for value in [
            0.0_f64,
            1.0,
            -7.0,
            150.0,
            1500.0,
            123_456_789.0,
            999_999_999_999_999.0,
        ] {
            assert!(value.fract() == 0.0 && value.abs() < 1e15);
            assert_eq!(
                fsqlite_types::value::format_sqlite_float(value),
                format!("{value:?}"),
                "integral {value}"
            );
            assert!(matches!(float_to_json(value), Ok(Value::Number(_))));
        }
        for (value, stock) in [
            (42.333_333_333_333, "42.333333333333002"),
            (1.0 / 3.0, "0.33333333333333332"),
            (1e300, "1.0e+300"),
            (-0.0, "0.0"),
            (0.25, "0.25"),
        ] {
            assert_eq!(
                float_to_json(value).unwrap(),
                number_text_value(stock),
                "{value}"
            );
        }
        let mut tokens: Vec<String> = Vec::new();
        for int in ["0", "1", "7", "42", "1000", "123456789", "9999999999999999"] {
            for frac in [
                "0",
                "5",
                "25",
                "001",
                "0001",
                "00001",
                "125",
                "333333333333",
                "10",
                "50",
                "123456",
                "0000001",
            ] {
                tokens.push(format!("{int}.{frac}"));
                tokens.push(format!("-{int}.{frac}"));
            }
        }
        let mut accepted = 0;
        for token in &tokens {
            if is_shortest_fixed_decimal(token.as_bytes()) {
                accepted += 1;
                let value: f64 = token.parse().unwrap();
                assert_eq!(format!("{value:?}"), *token, "Rust form of {token}");
            }
        }
        assert!(accepted > 40, "the fast path must cover the usual decimals");

        // Ordinary integers skip rewriting. A wide integer takes the exact
        // decimal path even when the document contains no float marker.
        assert!(!may_hold_float_token(r#"{"name":"e","n":[1,2],"id":17}"#));
        assert!(may_hold_float_token("[1e5]"));
        assert!(may_hold_float_token("[0.5]"));
        let huge = format!("[1{}]", "0".repeat(400));
        assert!(!may_hold_float_token(&huge));
        assert!(may_hold_integer_text(&huge));
        let parsed = parse_json_text(&huge).expect("an overflowing integer parses");
        assert_eq!(
            json_type_name(&parsed.as_array().expect("the parsed input is an array")[0]),
            "integer"
        );
        assert!(parse_json_value_lenient(&huge).is_ok());
        for token in [
            "1.50",
            "1E2",
            "1e300",
            "1.0e+300",
            "0.00001",
            "10000000000000000.0",
            "0.30000000000000004",
            "0.33333333333333332",
            "1",
            "-",
        ] {
            assert!(!is_shortest_fixed_decimal(token.as_bytes()), "{token}");
        }
    }

    #[test]
    fn stock_hex_jsonb_preserves_sql_storage_class_and_round_trips() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for raw in [
            "0x1a",
            "-0x0",
            "-0X0001",
            "+0x7fffffffffffffff",
            "-0x7fffffffffffffff",
            "0x8000000000000000",
            "-0x8000000000000000",
            "0x000ffffffffffffffff",
            "-0xffffffffffffffff",
            "0x10000000000000000",
            "-0x10000000000000000",
        ] {
            let input = format!("[{raw}]");
            let SqliteValue::Blob(stock_blob) =
                stock_json_scalar(&stock, "SELECT jsonb(?1)", &[text(&input)])
            else {
                panic!("expected JSONB for {raw}");
            };
            let args = [SqliteValue::Blob(stock_blob.clone())];
            let rendered = stock_json_scalar(&stock, "SELECT json(?1)", &args);
            assert_eq!(
                text(&json_from_jsonb(&stock_blob).unwrap()),
                rendered,
                "{raw}"
            );
            assert_eq!(text(&json(&input).unwrap()), rendered, "{raw}");
            let decoded = decode_jsonb_root(&stock_blob).unwrap();
            let rebound = SqliteValue::Blob(encode_jsonb_root(&decoded).unwrap().into());
            assert_eq!(
                stock_json_scalar(&stock, "SELECT json(?1)", &[rebound.clone()]),
                rendered,
                "{raw}"
            );
            let sql = "SELECT json_extract(?1,'$[0]')";
            let expected = stock_json_rows(&stock, sql, &args);
            let ours = json_extract_value(&decoded, &["$[0]"]);
            let tree = json_tree_blob(&stock_blob, None);
            if let Ok(rows) = expected {
                let scalar = &rows[0][0];
                for actual in [
                    ours.unwrap(),
                    json_extract(&input, &["$[0]"]).unwrap(),
                    tree.unwrap()[1].atom.clone(),
                    stock_json_scalar(&stock, sql, &[rebound]),
                ] {
                    assert_eq!(
                        std::mem::discriminant(&actual),
                        std::mem::discriminant(scalar),
                        "{raw}"
                    );
                    assert_eq!(&actual, scalar, "{raw}");
                }
            } else {
                assert!(ours.is_err() && tree.is_err(), "{raw}");
                assert!(stock_json_rows(&stock, sql, &[rebound]).is_err(), "{raw}");
            }
            assert_eq!(
                json_type_value(&decoded, Some("$[0]")).unwrap(),
                Some("integer")
            );
        }
    }

    #[test]
    fn relaxed_decimal_integers_retain_spelling_without_changing_other_json5_values() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let doc = concat!(
            "{'Infinity':1, /* 18446744073709551616, } */ ",
            r#"words:'escaped \' quote, // -0, [0]', control:'\v', "#,
            "values:[{}, [], // 111111111111111111111 \u{2028}",
            "+18446744073709551616,-9223372036854775809, -0, 0x1a, .5, Infinity, NaN,],",
            "dup:18446744073709551617, dup:-0,}",
        );
        for path in [
            "$.Infinity",
            "$.words",
            "$.control",
            "$.values[2]",
            "$.values[3]",
            "$.values[4]",
            "$.values[5]",
            "$.values[6]",
            "$.values[7]",
            "$.values[8]",
            "$.dup",
        ] {
            let value = json_extract(doc, &[path]).unwrap();
            let expected = stock_json_scalar(
                &stock,
                "SELECT json_extract(?1,?2)",
                &[text(doc), text(path)],
            );
            assert_eq!(
                std::mem::discriminant(&value),
                std::mem::discriminant(&expected),
                "{path}"
            );
            assert_eq!(value, expected, "{path}");
        }
        let value = parse_json5_text(doc).unwrap();
        let Value::Object(object) = value else {
            panic!("expected object");
        };
        let dup: Vec<_> = object
            .iter()
            .filter(|(key, _)| key == "dup")
            .map(|(_, value)| encode_json_text(value))
            .collect();
        assert_eq!(dup, ["18446744073709551617", "-0"]);
        for raw in ["+18446744073709551616", "-9223372036854775809", "-0"] {
            let input = format!("{{n:{raw},}}");
            let args = [text(&input)];
            let expected = stock_json_scalar(&stock, "SELECT json_set(?1,'$.m',1)", &args);
            let edited = json_set(&input, &[("$.m", SqliteValue::Integer(1))]).unwrap();
            assert_eq!(text(&edited), expected, "{raw}");
            let output = SqliteValue::Blob(jsonb(&input).unwrap().into());
            assert_eq!(
                stock_json_scalar(&stock, "SELECT json(?1)", &[output]),
                stock_json_scalar(&stock, "SELECT json(?1)", &args)
            );
        }
    }

    #[test]
    fn wide_decimal_integers_survive_edits_and_jsonb_round_trips() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let huge = "9".repeat(400);
        for raw in [
            "-0",
            "0",
            "-9223372036854775808",
            "-9223372036854775809",
            "9223372036854775808",
            "18446744073709551615",
            "18446744073709551616",
            "123456789012345678901234567890",
            huge.as_str(),
        ] {
            let doc = format!("[{raw}]");
            let args = [SqliteValue::Text(doc.clone().into())];
            let oracle = |sql| stock_json_scalar(&stock, sql, &args);
            let scalar = json_extract(&doc, &["$[0]"]).unwrap();
            let expected = oracle("SELECT json_extract(?1,'$[0]')");
            assert_eq!(
                std::mem::discriminant(&scalar),
                std::mem::discriminant(&expected),
                "{raw}: SQL storage class"
            );
            assert_eq!(scalar, expected, "{raw}");
            assert_eq!(json_type(&doc, Some("$[0]")).unwrap(), Some("integer"));
            let edited = json_set(&doc, &[("$[1]", SqliteValue::Integer(2))]).unwrap();
            assert_eq!(
                SqliteValue::Text(edited.into()),
                oracle("SELECT json_set(?1,'$[1]',2)"),
                "{raw}"
            );
            let blob = SqliteValue::Blob(jsonb(&doc).unwrap().into());
            let stock_read =
                stock_json_rows(&stock, "SELECT json(?1), json_type(?1,'$[0]')", &[blob]).unwrap();
            assert_eq!(stock_read, vec![vec![text(&doc), text("integer")]], "{raw}");
            let SqliteValue::Blob(stock_blob) = oracle("SELECT jsonb(?1)") else {
                panic!("expected stock JSONB for {raw}");
            };
            assert_eq!(json_from_jsonb(&stock_blob).unwrap(), doc, "{raw}");
        }
    }

    #[test]
    fn test_json_quote_non_finite_reals() {
        // SELECT json_quote(1e999)  -> 9.0e+999   (NOT null: only NaN is null)
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::INFINITY)).unwrap(),
            "9.0e+999"
        );
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::NEG_INFINITY)).unwrap(),
            "-9.0e+999"
        );
        // SELECT json_quote(1e999 - 1e999) -> null  (NaN)
        assert_eq!(json_quote(&SqliteValue::Float(f64::NAN)).unwrap(), "null");
    }

    #[test]
    fn test_json_parse_preserves_non_finite_source_text() {
        // SELECT json('[9e999]')      -> [9e999]      (source text PRESERVED)
        assert_eq!(json("[9e999]").unwrap(), "[9e999]");
        // SELECT json('[9.0e+999]')   -> [9.0e+999]
        assert_eq!(json("[9.0e+999]").unwrap(), "[9.0e+999]");
        // SELECT json('[-9e999]')     -> [-9e999]
        assert_eq!(json("[-9e999]").unwrap(), "[-9e999]");
    }

    #[test]
    fn test_json_valid_accepts_non_finite_literal_text() {
        // SELECT json_valid('[9e999]') -> 1
        assert_eq!(json_valid("[9e999]", None), 1);
        assert_eq!(json_valid("[9.0e+999]", None), 1);
        assert_eq!(json_valid("[-9e999]", None), 1);
    }

    #[test]
    fn test_json_extract_non_finite_reads_back_as_infinity() {
        // SELECT json_extract('[9.0e+999]','$[0]') -> Inf (REAL +Inf)
        let value = json_extract("[9.0e+999]", &["$[0]"]).unwrap();
        match value {
            SqliteValue::Float(f) => {
                assert!(
                    f.is_infinite() && f.is_sign_positive(),
                    "expected +Inf, got {f}"
                );
            }
            other => panic!("expected REAL +Inf, got {other:?}"),
        }
        // A parsed `9e999` (minimal source) also reads back as +Inf.
        let value = json_extract("[9e999]", &["$[0]"]).unwrap();
        assert!(matches!(value, SqliteValue::Float(f) if f.is_infinite() && f.is_sign_positive()));
        // Negative literal -> -Inf.
        let value = json_extract("[-9e999]", &["$[0]"]).unwrap();
        assert!(matches!(value, SqliteValue::Float(f) if f.is_infinite() && f.is_sign_negative()));
    }

    #[test]
    fn test_json_type_non_finite_literal_is_real() {
        // SELECT json_type('[9.0e+999]','$[0]') -> real
        assert_eq!(json_type("[9.0e+999]", Some("$[0]")).unwrap(), Some("real"));
        assert_eq!(json_type("[9e999]", Some("$[0]")).unwrap(), Some("real"));
    }

    #[test]
    fn test_jsonb_round_trip_preserves_non_finite() {
        // SELECT json(jsonb('[9e999]')) -> [9e999]  (JSONB payload round-trips)
        let blob = jsonb("[9e999]").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[9e999]");

        // Constructed via jsonb_array: +Inf stored canonically -> [9.0e+999].
        let blob = jsonb_array(&[SqliteValue::Float(f64::INFINITY)]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[9.0e+999]");

        // -Inf and NaN through the JSONB construct path.
        let blob = jsonb_array(&[SqliteValue::Float(f64::NEG_INFINITY)]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[-9.0e+999]");
        let blob = jsonb_array(&[SqliteValue::Float(f64::NAN)]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[null]");
    }

    #[test]
    fn test_json_array_embeds_parsed_non_finite_preserving_source() {
        // SELECT json_array(json('9e999'))    -> [9e999]      (source preserved)
        assert_eq!(
            json_array_with_subtypes(&[text("9e999")], &[JSON_SUBTYPE]).unwrap(),
            "[9e999]"
        );
        // SELECT json_array(json('9.0e+999')) -> [9.0e+999]
        assert_eq!(
            json_array_with_subtypes(&[text("9.0e+999")], &[JSON_SUBTYPE]).unwrap(),
            "[9.0e+999]"
        );
        // SELECT json_object('k', json('9e999')) -> {"k":9e999}
        assert_eq!(
            json_object_with_subtypes(&[text("k"), text("9e999")], &[0, JSON_SUBTYPE]).unwrap(),
            r#"{"k":9e999}"#
        );
    }

    #[test]
    fn test_json_extract_nested_container_preserves_non_finite() {
        // SELECT json_extract('{"a":[1e999]}','$.a') -> [1e999]
        assert_eq!(
            json_extract(r#"{"a":[1e999]}"#, &["$.a"]).unwrap(),
            text("[1e999]")
        );
    }

    #[test]
    fn test_json_pretty_preserves_non_finite() {
        // SELECT json_pretty('[9e999]') -> "[\n    9e999\n]"
        assert_eq!(json_pretty("[9e999]", None).unwrap(), "[\n    9e999\n]");
    }

    #[test]
    fn test_json_group_array_non_finite() {
        // Aggregate construct path shares the scalar rendering.
        assert_eq!(
            json_group_array(&[
                SqliteValue::Float(f64::INFINITY),
                SqliteValue::Float(f64::NAN),
                SqliteValue::Float(f64::NEG_INFINITY),
            ])
            .unwrap(),
            "[9.0e+999,null,-9.0e+999]"
        );
    }

    #[test]
    fn test_json_quote_blob() {
        let result = json_quote(&SqliteValue::Blob(Arc::from(vec![0xDE, 0xAD])));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("JSON cannot hold BLOB values")
        );
    }

    /// bd-reality-check-1022-llk6g.14: stock `json_quote` copies a JSON-subtyped
    /// argument verbatim, renders a JSONB blob as JSON text, and tags its
    /// result as JSON.
    #[test]
    fn test_json_quote_passes_json_values_through() {
        let text = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        let json_arg = JsonQuoteFunc
            .invoke_with_arg_subtypes(&[text(r#"{"a":1}"#)], &[JSON_SUBTYPE])
            .unwrap();
        assert_eq!(json_arg, text(r#"{"a":1}"#));
        // The same text without the subtype is an ordinary string.
        assert_eq!(
            JsonQuoteFunc.invoke(&[text(r#"{"a":1}"#)]).unwrap(),
            text(r#""{\"a\":1}""#)
        );
        let blob = SqliteValue::Blob(Arc::from(jsonb("[1, 2]").unwrap().as_slice()));
        assert_eq!(JsonQuoteFunc.invoke(&[blob]).unwrap(), text("[1,2]"));
        // x'01' is the one-byte JSONB `true`.
        let one = SqliteValue::Blob(Arc::from(vec![0x01_u8]));
        assert_eq!(JsonQuoteFunc.invoke(&[one]).unwrap(), text("true"));
        assert_eq!(JsonQuoteFunc.result_subtype(), Some(JSON_SUBTYPE));
        assert_eq!(JsonPatchFunc.result_subtype(), Some(JSON_SUBTYPE));
        assert_eq!(JsonRemoveFunc.result_subtype(), Some(JSON_SUBTYPE));
    }

    /// bd-reality-check-1022-llk6g.14: json_extract tags an extracted array or
    /// object (and the multi-path array) as JSON, but not a scalar, even a
    /// string that looks like JSON.
    #[test]
    fn test_json_extract_result_subtype_depends_on_value() {
        let text = |s: &str| SqliteValue::Text(SmallText::from_string(s));
        assert!(JsonExtractFunc.result_subtype_varies());
        let doc = text(r#"{"a":[1],"s":"[1]","n":2}"#);
        let call = |path: &[&str]| {
            let mut args = vec![doc.clone()];
            args.extend(path.iter().map(|p| text(p)));
            JsonExtractFunc
                .invoke_with_result_subtype(&args, &[])
                .unwrap()
        };
        assert_eq!(call(&["$.a"]), (text("[1]"), Some(JSON_SUBTYPE)));
        assert_eq!(call(&["$.s"]), (text("[1]"), None));
        assert_eq!(call(&["$.n"]), (SqliteValue::Integer(2), None));
        assert_eq!(call(&["$.missing"]), (SqliteValue::Null, None));
        assert_eq!(call(&["$.a", "$.n"]), (text("[[1],2]"), Some(JSON_SUBTYPE)));
    }

    #[test]
    fn test_json_quote_text_special_chars() {
        let result = json_quote(&SqliteValue::Text(SmallText::from_string("a\"b\\c"))).unwrap();
        assert!(result.contains("\\\""));
        assert!(result.contains("\\\\"));
    }

    // -----------------------------------------------------------------------
    // json_object edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_object_odd_args_error() {
        let err = json_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
        ]);
        assert!(err.is_err());
    }

    #[test]
    fn test_json_object_non_text_key_error() {
        let err = json_object(&[SqliteValue::Integer(1), SqliteValue::Integer(2)]);
        assert!(err.is_err());
    }

    #[test]
    fn test_json_object_empty() {
        assert_eq!(json_object(&[]).unwrap(), "{}");
    }

    // bd-55eq3: stock SQLite keeps duplicate labels verbatim in argument order
    // (`json_object('k',1,'k',2)` -> `{"k":1,"k":2}`, verified against sqlite3
    // 3.46.1). serde_json's `Map` collapsed them to the last value; the object
    // text is now assembled directly so both entries survive.
    #[test]
    fn test_json_object_duplicate_keys_kept_verbatim() {
        let out = json_object(&[
            SqliteValue::Text(SmallText::from_string("k")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("k")),
            SqliteValue::Integer(2),
        ])
        .unwrap();
        assert_eq!(out, r#"{"k":1,"k":2}"#);
    }

    #[test]
    fn test_json_object_duplicate_keys_preserve_interleaved_order() {
        // Matches stock: json_object('a',1,'b',2,'a',3) -> {"a":1,"b":2,"a":3}.
        let out = json_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
            SqliteValue::Integer(2),
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(3),
        ])
        .unwrap();
        assert_eq!(out, r#"{"a":1,"b":2,"a":3}"#);
    }

    #[test]
    fn test_jsonb_object_preserves_duplicate_labels_bd_55eq3() {
        let blob = jsonb_object(&[
            SqliteValue::Text(SmallText::from_string("k")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("k")),
            SqliteValue::Integer(2),
        ])
        .unwrap();

        // Stock JSONB for {"k":1,"k":2}: object payload length 8, followed by
        // TEXT("k"), INT("1"), TEXT("k"), INT("2").
        assert_eq!(
            blob,
            vec![0x8c, 0x17, b'k', 0x13, b'1', 0x17, b'k', 0x13, b'2']
        );
    }

    #[test]
    fn test_jsonb_group_object_preserves_duplicate_labels_bd_55eq3() {
        let blob = jsonb_group_object(&[
            (
                SqliteValue::Text(SmallText::from_string("k")),
                SqliteValue::Integer(1),
            ),
            (
                SqliteValue::Text(SmallText::from_string("k")),
                SqliteValue::Integer(2),
            ),
        ])
        .unwrap();
        assert_eq!(
            blob,
            vec![0x8c, 0x17, b'k', 0x13, b'1', 0x17, b'k', 0x13, b'2']
        );
    }

    #[test]
    fn test_jsonb_object_subtype_embeds_nested_value_without_collapsing_labels() {
        let blob = JsonbObjectFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("k")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                    SqliteValue::Text(SmallText::from_string("k")),
                    SqliteValue::Integer(3),
                ],
                &[0, JSON_SUBTYPE, 0, 0],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = blob else {
            panic!("jsonb_object must return a BLOB");
        };
        // Both "k" members remain, the first holding the embedded array: stock
        // 3.54.0's hex(jsonb_object('k', json('[1,2]'), 'k', 3)).
        assert_eq!(
            blob.as_ref(),
            [
                0xbc, 0x17, b'k', 0x4b, 0x13, b'1', 0x13, b'2', 0x17, b'k', 0x13, b'3'
            ]
        );
    }

    #[test]
    fn test_json_group_window_inverse_removes_oldest_row() {
        let [array, object, jsonb_array, jsonb_object] = JsonGroupWindowFunc::ALL;
        let text = |s: &str| SqliteValue::Text(SmallText::from_string(s));

        // A frame sliding over 1, json('[2]'), 3: the inverse drops the oldest
        // row, and a JSON-subtyped value stays embedded.
        let mut state = array.initial_state();
        array.step(&mut state, &[SqliteValue::Integer(1)]).unwrap();
        array
            .step_with_arg_subtypes(&mut state, &[text("[2]")], &[JSON_SUBTYPE])
            .unwrap();
        assert_eq!(array.value(&state).unwrap(), text("[1,[2]]"));
        array
            .inverse(&mut state, &[SqliteValue::Integer(1)])
            .unwrap();
        array.step(&mut state, &[SqliteValue::Integer(3)]).unwrap();
        assert_eq!(array.value(&state).unwrap(), text("[[2],3]"));

        // A NULL label adds no member but keeps its row's place, so the next
        // inverse removes that row, not the member after it.
        let mut state = object.initial_state();
        object
            .step(&mut state, &[SqliteValue::Null, SqliteValue::Integer(1)])
            .unwrap();
        object
            .step(&mut state, &[text("a"), SqliteValue::Integer(2)])
            .unwrap();
        assert_eq!(object.value(&state).unwrap(), text("{\"a\":2}"));
        object
            .inverse(&mut state, &[SqliteValue::Null, SqliteValue::Integer(1)])
            .unwrap();
        assert_eq!(object.value(&state).unwrap(), text("{\"a\":2}"));
        object
            .inverse(&mut state, &[text("a"), SqliteValue::Integer(2)])
            .unwrap();
        assert_eq!(object.value(&state).unwrap(), text("{}"));

        // An empty frame is an empty JSONB array or object.
        let empty = |f: &JsonGroupWindowFunc| f.value(&f.initial_state()).unwrap();
        assert_eq!(
            empty(&jsonb_array),
            SqliteValue::Blob(Arc::from(vec![0x0b]))
        );
        assert_eq!(
            empty(&jsonb_object),
            SqliteValue::Blob(Arc::from(vec![0x0c]))
        );
    }

    // -----------------------------------------------------------------------
    // json_set/insert/replace array index
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_set_array_element() {
        let out = json_set("[1,2,3]", &[("$[1]", SqliteValue::Integer(99))]).unwrap();
        assert_eq!(out, "[1,99,3]");
    }

    #[test]
    fn test_json_set_array_append_at_len() {
        let out = json_set("[1,2]", &[("$[2]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_insert_array_append_at_len() {
        let out = json_insert("[1,2]", &[("$[2]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_set_append_pseudo_index() {
        let out = json_set("[1,2]", &[("$[#]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_replace_append_pseudo_index_noop() {
        let out = json_replace("[1,2]", &[("$[#]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2]");
    }

    #[test]
    fn test_json_replace_array_element() {
        let out = json_replace("[1,2,3]", &[("$[0]", SqliteValue::Integer(0))]).unwrap();
        assert_eq!(out, "[0,2,3]");
    }

    #[test]
    fn test_json_set_multiple_paths() {
        let out = json_set(
            r#"{"a":1,"b":2}"#,
            &[
                ("$.a", SqliteValue::Integer(10)),
                ("$.c", SqliteValue::Integer(30)),
            ],
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["a"], 10);
        assert_eq!(parsed["c"], 30);
    }

    // -----------------------------------------------------------------------
    // json_remove edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_remove_missing_key_no_change() {
        let out = json_remove(r#"{"a":1}"#, &["$.b"]).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_json_remove_multiple_paths() {
        let out = json_remove(r#"{"a":1,"b":2,"c":3}"#, &["$.a", "$.c"]).unwrap();
        assert_eq!(out, r#"{"b":2}"#);
    }

    #[test]
    fn test_json_remove_from_end_index() {
        let out = json_remove("[1,2,3]", &["$[#-1]"]).unwrap();
        assert_eq!(out, "[1,2]");
    }

    #[test]
    fn test_registered_json_remove_root_returns_sql_null() {
        let func = JsonRemoveFunc;
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                SqliteValue::Text(SmallText::from_string("$")),
            ])
            .expect("json_remove root path should execute");
        assert_eq!(out, SqliteValue::Null);
    }

    #[test]
    fn test_registered_jsonb_remove_root_returns_sql_null() {
        let func = JsonbRemoveFunc;
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                SqliteValue::Text(SmallText::from_string("$")),
            ])
            .expect("jsonb_remove root path should execute");
        assert_eq!(out, SqliteValue::Null);
    }

    // -----------------------------------------------------------------------
    // json_patch edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_patch_non_object_replaces() {
        let out = json_patch(r#"{"a":1}"#, "42").unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn test_json_patch_nested_merge() {
        let out = json_patch(r#"{"a":{"b":1,"c":2}}"#, r#"{"a":{"b":10,"d":4}}"#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["a"]["b"], 10);
        assert_eq!(parsed["a"]["c"], 2);
        assert_eq!(parsed["a"]["d"], 4);
    }

    // -----------------------------------------------------------------------
    // json_each edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_each_scalar() {
        let rows = json_each("42", None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, SqliteValue::Null);
        assert_eq!(rows[0].value, SqliteValue::Integer(42));
        assert_eq!(rows[0].type_name, "integer");
    }

    #[test]
    fn test_json_each_empty_array() {
        let rows = json_each("[]", None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_json_each_empty_object() {
        let rows = json_each("{}", None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_json_each_missing_path() {
        let rows = json_each(r#"{"a":1}"#, Some("$.b")).unwrap();
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // json_tree edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_tree_scalar() {
        let rows = json_tree("42", None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_name, "integer");
    }

    #[test]
    fn test_json_tree_empty_array() {
        let rows = json_tree("[]", None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_name, "array");
    }

    #[test]
    fn test_json_tree_parent_ids() {
        let rows = json_tree(r#"{"a":1}"#, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].parent, SqliteValue::Null); // root
        assert_eq!(rows[1].parent, SqliteValue::Integer(rows[0].id)); // child
    }

    #[test]
    fn test_json_tree_missing_path() {
        let rows = json_tree(r#"{"a":1}"#, Some("$.b")).unwrap();
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // JSONB edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_jsonb_null() {
        let blob = jsonb("null").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "null");
    }

    #[test]
    fn test_jsonb_booleans() {
        assert_eq!(json_from_jsonb(&jsonb("true").unwrap()).unwrap(), "true");
        assert_eq!(json_from_jsonb(&jsonb("false").unwrap()).unwrap(), "false");
    }

    #[test]
    fn test_jsonb_integer() {
        let blob = jsonb("42").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "42");
    }

    #[test]
    fn test_jsonb_float() {
        let blob = jsonb("3.14").unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert!(text.starts_with("3.14"));
    }

    #[test]
    fn test_jsonb_nested_array() {
        let blob = jsonb("[[1],[2,3]]").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[[1],[2,3]]");
    }

    #[test]
    fn test_jsonb_empty_string() {
        let blob = jsonb(r#""""#).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#""""#);
    }

    #[test]
    fn test_jsonb_extract_multiple_paths() {
        let blob = jsonb_extract(r#"{"a":1,"b":2}"#, &["$.a", "$.b"]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[1,2]");
    }

    #[test]
    fn test_jsonb_extract_no_paths_error() {
        let empty: &[&str] = &[];
        assert!(jsonb_extract(r#"{"a":1}"#, empty).is_err());
    }

    #[test]
    fn test_jsonb_decode_trailing_bytes() {
        let mut blob = jsonb("42").unwrap();
        blob.push(0xFF); // trailing garbage
        assert!(json_from_jsonb(&blob).is_err());
        assert_eq!(
            json_valid_blob(&blob, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            0
        );
    }

    #[test]
    fn test_jsonb_decode_empty() {
        assert!(json_from_jsonb(&[]).is_err());
    }

    // -----------------------------------------------------------------------
    // Path parsing edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_path_invalid_no_dollar() {
        assert!(json_extract(r#"{"a":1}"#, &["a"]).is_err());
    }

    #[test]
    fn test_path_empty_key_error() {
        assert!(json_extract(r#"{"a":1}"#, &["$."]).is_err());
    }

    #[test]
    fn test_path_unclosed_bracket() {
        assert!(json_extract(r"[1,2]", &["$[0"]).is_err());
    }

    #[test]
    fn test_path_from_end_zero_is_one_past_the_end() {
        // Stock: `$[#-0]` is the slot `$[#]` names, so nothing is there to
        // extract, and json_set appends there.
        assert_eq!(
            json_extract("[1,2,3]", &["$[#-0]"]).unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            json_set("[1]", &[("$[#-0]", SqliteValue::Integer(2))]).unwrap(),
            "[1,2]"
        );
    }

    #[test]
    fn test_path_from_end_beyond_length() {
        let result = json_extract("[1,2,3]", &["$[#-10]"]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    // -----------------------------------------------------------------------
    // json_group_object edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_group_object_non_text_key_is_its_text() {
        // Stock: json_group_object(1, 2) -> {"1":2}; a NULL label drops the row.
        let out = json_group_object(&[
            (SqliteValue::Integer(1), SqliteValue::Integer(2)),
            (SqliteValue::Null, SqliteValue::Integer(3)),
        ])
        .unwrap();
        assert_eq!(out, r#"{"1":2}"#);
    }

    #[test]
    fn test_json_group_object_empty() {
        assert_eq!(json_group_object(&[]).unwrap(), "{}");
    }

    // -----------------------------------------------------------------------
    // json_array edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_array_empty() {
        assert_eq!(json_array(&[]).unwrap(), "[]");
    }

    #[test]
    fn test_json_array_with_blob() {
        let err = json_array(&[SqliteValue::Blob(Arc::from(vec![0xCA, 0xFE]))]).unwrap_err();
        assert!(err.to_string().contains("JSON cannot hold BLOB values"));
    }

    /// bd-t75hg byte-level oracle keeper: every vector below is the exact
    /// `hex(jsonb(...))` output of stock sqlite3 3.46.1, captured live on
    /// 2026-08-14. Our encoder must be byte-identical for these canonical
    /// shapes, and our decoder must read the oracle bytes back to the same
    /// value.
    #[test]
    fn test_jsonb_bytes_match_sqlite3_oracle() {
        let vectors: &[(&str, &str)] = &[
            ("1", "1331"),
            // bd-pnfrr: the bead's cited oracle — `hex(jsonb(5))` is `1335`
            // (header 0x13 = INT type 3, direct size 1; payload 0x35 = '5'),
            // never a raw fixed-width binary payload.
            ("5", "1335"),
            ("-7", "232D37"),
            ("1.5", "35312E35"),
            ("0.1", "35302E31"),
            ("-0.0", "452D302E30"),
            ("1e300", "553165333030"),
            (
                "9223372036854775807",
                "C31339323233333732303336383534373735383037",
            ),
            (
                "-9223372036854775808",
                "C3142D39323233333732303336383534373735383038",
            ),
            (
                "12345678901234567890",
                "C3143132333435363738393031323334353637383930",
            ),
            ("[1,2.5,\"x\"]", "8B133135322E351778"),
            ("{\"a\":1}", "4C17611331"),
            ("[]", "0B"),
            ("{}", "0C"),
            ("\"\"", "07"),
        ];
        for (input, expected_hex) in vectors {
            let value = parse_json_text(input).expect("oracle vector parses");
            let encoded = encode_jsonb_root(&value).expect("oracle vector encodes");
            let got_hex: String = encoded.iter().map(|b| format!("{b:02X}")).collect();
            assert_eq!(
                &got_hex, expected_hex,
                "jsonb({input}) bytes must match stock sqlite3"
            );
            let oracle_bytes: Vec<u8> = (0..expected_hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&expected_hex[i..i + 2], 16).unwrap())
                .collect();
            let decoded = decode_jsonb_root(&oracle_bytes).expect("oracle bytes decode");
            assert_eq!(
                decoded, value,
                "decoding stock sqlite3 bytes for {input} must recover the value"
            );
        }
    }

    /// bd-t75hg: escaped strings encode as TEXTJ with RFC-8259 escapes (the
    /// shape stock emits and expects), and stock-written escaped payloads
    /// decode to raw strings.
    #[test]
    fn test_jsonb_escaped_strings_round_trip_and_decode_stock_escapes() {
        let value = parse_json_text("\"a\\\"b\\\\c\\nd\"").expect("escaped string parses");
        let encoded = encode_jsonb_root(&value).expect("encodes");
        assert_eq!(
            encoded[0] & 0x0f,
            JSONB_TEXT_JSON_TYPE,
            "strings needing escapes must use TEXTJ"
        );
        let decoded = decode_jsonb_root(&encoded).expect("decodes");
        assert_eq!(decoded, value, "escaped string round-trips");

        // TEXT5 payload with JSON5 escapes decodes too (read tolerance).
        let mut text5 = Vec::new();
        let payload = b"it\\'s";
        text5.push((u8::try_from(payload.len()).unwrap() << 4) | JSONB_TEXT5_TYPE);
        text5.extend_from_slice(payload);
        let decoded = decode_jsonb_root(&text5).expect("TEXT5 decodes");
        assert_eq!(decoded, Value::String(InlineStr::new("it's")));
    }

    /// bd-t75hg: INT5/FLOAT5 numeric text (JSON5 forms) decodes numerically.
    #[test]
    fn test_jsonb_json5_numeric_payloads_decode() {
        let mut int5 = Vec::new();
        int5.push((4u8 << 4) | JSONB_INT5_TYPE);
        int5.extend_from_slice(b"0x1A");
        assert_eq!(
            decode_jsonb_root(&int5).unwrap(),
            Value::Number(Number::from(26)),
        );
        let mut float5 = Vec::new();
        float5.push((3u8 << 4) | JSONB_FLOAT5_TYPE);
        float5.extend_from_slice(b"+.5");
        assert_eq!(
            decode_jsonb_root(&float5).unwrap(),
            Value::Number(Number::from_f64(0.5).unwrap()),
        );
    }

    /// bd-oahj7: one-pass parser, compact Value/Object layout, FxHashMap large
    /// object lookup, and zero-allocation streaming single-path extraction edge cases.
    #[test]
    fn bd_oahj7_single_pass_parser_and_streaming_extract_invariants() {
        assert!(
            std::mem::size_of::<InlineStr>() <= 24,
            "InlineStr must match String size (24B), got {}",
            std::mem::size_of::<InlineStr>()
        );
        assert!(
            std::mem::size_of::<Object>() <= 32,
            "Object must stay compact with boxed index (<=32B), got {}",
            std::mem::size_of::<Object>()
        );
        assert!(
            std::mem::size_of::<Value>() <= 40,
            "Value must stay compact (<=40B), got {}",
            std::mem::size_of::<Value>()
        );

        // Large object (>= 64 members) exercises FxHashMap index + duplicate keys.
        let mut members = Vec::new();
        for i in 0..80 {
            members.push(format!(r#""key_{i:02}":{i}.50"#));
        }
        members.push(r#""key_10":999.00"#.to_owned());
        members.push(
            r#""long_heap_key_beyond_22_bytes":"long_heap_value_beyond_22_bytes""#.to_owned(),
        );
        let wide_doc = format!("{{{}}}", members.join(","));
        assert_eq!(json_valid(&wide_doc, None), 1);
        assert_eq!(
            json_extract(&wide_doc, &["$.key_10"]).unwrap(),
            SqliteValue::Float(10.5)
        );
        assert_eq!(
            json_extract(&wide_doc, &["$.key_79"]).unwrap(),
            SqliteValue::Float(79.5)
        );
        assert_eq!(
            json_extract(&wide_doc, &["$.long_heap_key_beyond_22_bytes"]).unwrap(),
            SqliteValue::Text(SmallText::new("long_heap_value_beyond_22_bytes"))
        );
        let edited = json_set(&wide_doc, &[("$.key_10", SqliteValue::Integer(42))]).unwrap();
        assert_eq!(
            json_extract(&edited, &["$.key_10"]).unwrap(),
            SqliteValue::Integer(42)
        );
        assert!(edited.contains(r#""key_79":79.50"#));
        assert!(edited.contains(r#""key_10":999.00"#));

        // Escaped keys and surrogate pairs in streaming extractor and parser.
        let escaped_doc = r#"{"\u0061":1.50,"a":2.50,"emoji":"\uD83D\uDE00","arr":[1,2,3]}"#;
        assert_eq!(json_valid(escaped_doc, None), 1);
        assert_eq!(
            json_extract(escaped_doc, &["$.a"]).unwrap(),
            SqliteValue::Float(1.5)
        );
        assert_eq!(
            json_extract(escaped_doc, &["$.emoji"]).unwrap(),
            SqliteValue::Text(SmallText::new("😀"))
        );
        assert_eq!(json_type(escaped_doc, Some("$.a")).unwrap(), Some("real"));
        assert_eq!(
            json_array_length(escaped_doc, Some("$.arr")).unwrap(),
            Some(3)
        );
        assert_eq!(
            json_array_length(escaped_doc, Some("$.a")).unwrap(),
            Some(0)
        );
        assert_eq!(
            json_array_length(escaped_doc, Some("$.nope")).unwrap(),
            None
        );

        // Lone surrogates, unescaped controls, trailing syntax errors MUST fail.
        for bad in [
            r#"{"a":"\uD83D"}"#,
            r#"{"a":"\uDC00"}"#,
            r#"{"a":"\uD83D\u0041"}"#,
            "{\"a\":\"line\nbreak\"}",
            r#"{"x":1, broken}"#,
            r#"{"x":1} trailing"#,
            r#"{"x":01}"#,
            r#"{"x":1.}"#,
            r#"{"x":.5}"#,
            r#"{"x":1e}"#,
        ] {
            assert_eq!(json_valid(bad, Some(1)), 0, "strict invalid: {bad}");
        }
        assert!(json_extract(r#"{"x":1, broken}"#, &["$.x"]).is_err());

        // Malformed JSON must error before bad path error (SQLite error precedence).
        let err = json_extract("{bad", &["not_a_path"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("malformed JSON"), "got: {err}");

        // Nesting depth boundary: 1000 allowed, 1001 rejected.
        let deep_ok = format!("{}{}", "[".repeat(1000), "]".repeat(1000));
        let deep_err = format!("{}{}", "[".repeat(1001), "]".repeat(1001));
        assert_eq!(json_valid(&deep_ok, Some(1)), 1);
        assert_eq!(json_valid(&deep_err, Some(1)), 0);
        assert!(parse_json_text(&deep_ok).is_ok());
        assert!(parse_json_text(&deep_err).is_err());
    }

    #[test]
    fn bd_c1q6u_json_each_tree_ids_parents_and_rooted_paths_match_live_sqlite_oracle() {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let cx = Cx::new();

        let cases: &[(&str, &str, Option<&str>)] = &[
            ("each_nested_array_obj", r#"[10,{"a":20},[30]]"#, None),
            ("each_plain_string", r#"["A",1]"#, None),
            ("each_unicode_escape_string", r#"["\u0041",1]"#, None),
            (
                "each_json5_literals",
                "[1.50, 1e2, +5, 0x10, .5, 5., Infinity, -Infinity, NaN]",
                None,
            ),
            (
                "each_json5_obj",
                "{a:1, 'b': 'x\"y', c: [1,2,], /* comment */}",
                None,
            ),
            (
                "each_rooted_array",
                r#"{"x":[1,{"y":2}],"z":"s"}"#,
                Some("$.x"),
            ),
            ("each_rooted_scalar", r#"{"a":5}"#, Some("$.a")),
            (
                "each_rooted_compound_first",
                r#"{"a":{"b":1,"c":2}}"#,
                Some("$.a.b"),
            ),
            (
                "each_rooted_compound_second",
                r#"{"a":{"b":1,"c":2}}"#,
                Some("$.a.c"),
            ),
            (
                "each_rooted_array_first",
                r#"{"a":[10,20,30]}"#,
                Some("$.a[0]"),
            ),
            (
                "each_rooted_array_second",
                r#"{"a":[10,20,30]}"#,
                Some("$.a[1]"),
            ),
            (
                "each_rooted_array_from_end",
                r#"{"a":[10,20,30]}"#,
                Some("$.a[#-1]"),
            ),
            (
                "each_quoted_and_special_keys",
                r#"{"a.b":{"c":1,"d":2},"k_1":3,"k-2":4,"":5,"\u0061":6}"#,
                None,
            ),
            (
                "each_rooted_quoted_first",
                r#"{"a.b":{"c":1,"d":2}}"#,
                Some(r#"$."a.b".c"#),
            ),
            (
                "each_rooted_quoted_second",
                r#"{"a.b":{"c":1,"d":2}}"#,
                Some(r#"$."a.b".d"#),
            ),
            ("each_duplicate_keys", r#"{"a":1,"a":2,"b":3}"#, None),
            ("each_root_dollar", r#"{"a":1,"b":2}"#, Some("$")),
            ("each_scalar_root", "42", None),
            ("each_empty_array", "[]", None),
            ("each_empty_object", "{}", None),
            ("each_missing_path", r#"{"a":1}"#, Some("$.missing")),
        ];

        let mut diffs = Vec::new();
        for &(label, doc, path) in cases {
            let stock_jsonb = stock_json_scalar(&stock, "SELECT jsonb(?1)", &[text(doc)]);
            for (input_kind, input_arg) in [("text", text(doc)), ("jsonb", stock_jsonb)] {
                for recursive in [false, true] {
                    let fn_name = if recursive { "json_tree" } else { "json_each" };
                    let (sql, args) = match path {
                        Some(p) => (
                            format!(
                                "SELECT rowid, key, value, type, atom, id, parent, fullkey, path FROM {fn_name}(?1, ?2)"
                            ),
                            vec![input_arg.clone(), text(p)],
                        ),
                        None => (
                            format!(
                                "SELECT rowid, key, value, type, atom, id, parent, fullkey, path FROM {fn_name}(?1)"
                            ),
                            vec![input_arg.clone()],
                        ),
                    };
                    let expected = stock_json_rows(&stock, &sql, &args).unwrap();

                    // Check direct API (`json_each`/`json_tree` and `*_blob`)
                    let direct_rows = match (&input_arg, recursive) {
                        (SqliteValue::Text(s), false) => json_each(s, path),
                        (SqliteValue::Text(s), true) => json_tree(s, path),
                        (SqliteValue::Blob(b), false) => json_each_blob(b, path),
                        (SqliteValue::Blob(b), true) => json_tree_blob(b, path),
                        _ => unreachable!(),
                    }
                    .unwrap();
                    let direct_rendered: Vec<Vec<SqliteValue>> = direct_rows
                        .into_iter()
                        .enumerate()
                        .map(|(idx, row)| {
                            vec![
                                SqliteValue::Integer(i64::try_from(idx).unwrap()),
                                row.key,
                                row.value,
                                text(row.type_name),
                                row.atom,
                                SqliteValue::Integer(row.id),
                                row.parent,
                                SqliteValue::Text(row.fullkey.into()),
                                SqliteValue::Text(row.path.into()),
                            ]
                        })
                        .collect();
                    if direct_rendered != expected {
                        diffs.push(format!(
                            "  [{label}/{input_kind}/{fn_name}/direct]\n    frank={direct_rendered:?}\n    stock={expected:?}"
                        ));
                    }

                    // Check virtual table cursor API
                    let mut vtab_rows = Vec::new();
                    if recursive {
                        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
                        let mut cursor = vtab.open().unwrap();
                        cursor.filter(&cx, 0, None, &args).unwrap();
                        while !cursor.eof() {
                            let mut row = vec![SqliteValue::Integer(cursor.rowid().unwrap())];
                            for col in 0..8 {
                                let mut ctx = ColumnContext::new();
                                cursor.column(&mut ctx, col).unwrap();
                                row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
                            }
                            vtab_rows.push(row);
                            cursor.next(&cx).unwrap();
                        }
                    } else {
                        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
                        let mut cursor = vtab.open().unwrap();
                        cursor.filter(&cx, 0, None, &args).unwrap();
                        while !cursor.eof() {
                            let mut row = vec![SqliteValue::Integer(cursor.rowid().unwrap())];
                            for col in 0..8 {
                                let mut ctx = ColumnContext::new();
                                cursor.column(&mut ctx, col).unwrap();
                                row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
                            }
                            vtab_rows.push(row);
                            cursor.next(&cx).unwrap();
                        }
                    }
                    if vtab_rows != expected {
                        diffs.push(format!(
                            "  [{label}/{input_kind}/{fn_name}/vtab]\n    frank={vtab_rows:?}\n    stock={expected:?}"
                        ));
                    }
                }
            }
        }

        // Also check bare SQL scalar inputs (`json_each(7)`, `json_tree(1.5)`).
        for (label, input_arg) in [
            ("scalar_int", SqliteValue::Integer(7)),
            ("scalar_float", SqliteValue::Float(1.5)),
            ("scalar_null", SqliteValue::Null),
        ] {
            for recursive in [false, true] {
                let fn_name = if recursive { "json_tree" } else { "json_each" };
                let sql = format!(
                    "SELECT rowid, key, value, type, atom, id, parent, fullkey, path FROM {fn_name}(?1)"
                );
                let expected = stock_json_rows(&stock, &sql, &[input_arg.clone()]).unwrap();
                let mut vtab_rows = Vec::new();
                if recursive {
                    let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
                    let mut cursor = vtab.open().unwrap();
                    cursor.filter(&cx, 0, None, &[input_arg.clone()]).unwrap();
                    while !cursor.eof() {
                        let mut row = vec![SqliteValue::Integer(cursor.rowid().unwrap())];
                        for col in 0..8 {
                            let mut ctx = ColumnContext::new();
                            cursor.column(&mut ctx, col).unwrap();
                            row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
                        }
                        vtab_rows.push(row);
                        cursor.next(&cx).unwrap();
                    }
                } else {
                    let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
                    let mut cursor = vtab.open().unwrap();
                    cursor.filter(&cx, 0, None, &[input_arg.clone()]).unwrap();
                    while !cursor.eof() {
                        let mut row = vec![SqliteValue::Integer(cursor.rowid().unwrap())];
                        for col in 0..8 {
                            let mut ctx = ColumnContext::new();
                            cursor.column(&mut ctx, col).unwrap();
                            row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
                        }
                        vtab_rows.push(row);
                        cursor.next(&cx).unwrap();
                    }
                }
                if vtab_rows != expected {
                    diffs.push(format!(
                        "  [{label}/{fn_name}/vtab]\n    frank={vtab_rows:?}\n    stock={expected:?}"
                    ));
                }
            }
        }

        assert!(
            diffs.is_empty(),
            "{} bd-c1q6u json_each/json_tree divergence(s) vs live SQLite 3.54.1:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    }
}

#[cfg(test)]
mod multi_path_lexical_parity_tests {
    use super::*;

    #[test]
    fn json_extract_preserves_escaped_string_tokens_across_json5_translation() {
        let input = r#"{first:'Infini\u0074y', second:'plain', n:1E+2}"#;
        let result = json_extract(input, &["$.first", "$.second", "$.n"]).unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::new(r#"["Infini\u0074y","plain",1E+2]"#))
        );
    }

    #[test]
    fn json_extract_preserves_strict_json_subtrees_and_missing_paths() {
        let input = r#"{ "a": { "text": "a\u002fb", "n": 1.50 }, "b": null }"#;
        let result = json_extract(input, &["$.a", "$.missing", "$.b"]).unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::new(
                r#"[{"text":"a\u002fb","n":1.50},null,null]"#
            ))
        );
    }

    #[test]
    fn json_extract_preserves_duplicate_source_values_and_whitespace() {
        let input = r#"{ "items": [ 1E+2, "a\\u0062" ], "x": -0 }"#;
        let result = json_extract(input, &["$.items", "$.x", "$.items[1]"]).unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::new(
                r#"[[1E+2,"a\\u0062"],-0,"a\\u0062"]"#
            ))
        );
    }

    #[test]
    fn json_extract_bad_path_still_uses_full_resolver() {
        assert!(json_extract(r#"{"a":1}"#, &["$.a", "$["]).is_err());
    }
}

#[cfg(test)]
mod json5_reserved_key_parity_tests {
    use super::*;

    #[test]
    fn sqlite_rejects_bare_value_tokens_in_json5_key_position() {
        for key in [
            "true", "false", "null", "nan", "NaN", "NAN", "inf",
            "Infinity", "infinity", "QNaN", "SNaN",
        ] {
            let input = format!("{{{key}:1}}");
            assert!(json(&input).is_err(), "json accepted {key}");
            assert!(json_pretty(&input, None).is_err(), "pretty accepted {key}");
            assert!(
                json_set(&input, &[("$.x", SqliteValue::Integer(2))]).is_err(),
                "set accepted {key}"
            );
        }
    }

    #[test]
    fn reserved_words_remain_valid_as_values_and_quoted_keys() {
        assert_eq!(json("{n:Infinity}").unwrap(), r#"{"n":9e999}"#);
        assert_eq!(json("{n:NaN}").unwrap(), r#"{"n":null}"#);
        assert_eq!(json("{'nan':1}").unwrap(), r#"{"nan":1}"#);
        assert_eq!(json(r#"{"nan":1}"#).unwrap(), r#"{"nan":1}"#);
        assert_eq!(json("{/* nan: 1 */ n:1}").unwrap(), r#"{"n":1}"#);
        assert_eq!(json("{nanx:1}").unwrap(), r#"{"nanx":1}"#);
        assert_eq!(json("{TRUE:1}").unwrap(), r#"{"TRUE":1}"#);
    }

    #[test]
    fn quoted_and_commented_keyword_text_is_not_a_bare_key() {
        assert_eq!(
            json(r#"{"text":"nan:1","real":NaN}"#).unwrap(),
            r#"{"text":"nan:1","real":null}"#
        );
        assert!(json("{nan /* comment */ : NaN}").is_err());
    }
}
