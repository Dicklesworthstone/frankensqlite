use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use memchr::{memchr, memchr2, memmem};

use crate::{StorageClass, StrictColumnType, StrictTypeError, TypeAffinity};

// ============================================================================
// Thread-Local Value Slab Allocator
// ============================================================================

/// Maximum number of values to keep in the thread-local pool.
///
/// 256 is chosen as a balance: large enough to cover typical row batch sizes,
/// small enough to avoid unbounded memory retention per thread.
const VALUE_POOL_CAP: usize = 256;

thread_local! {
    /// Thread-local pool of reusable `SqliteValue` objects.
    ///
    /// During hot-path execution (MakeRecord, Column decode), values are
    /// acquired from this pool instead of allocating fresh, then returned
    /// when the register is overwritten or the row changes.
    static VALUE_POOL: RefCell<Vec<SqliteValue>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ValuePoolStats {
    slab_alloc_count: usize,
    slab_return_count: usize,
    global_alloc_fallback_count: usize,
    slab_high_water_mark: usize,
}

#[cfg(test)]
impl ValuePoolStats {
    const fn new() -> Self {
        Self {
            slab_alloc_count: 0,
            slab_return_count: 0,
            global_alloc_fallback_count: 0,
            slab_high_water_mark: 0,
        }
    }
}

#[cfg(test)]
thread_local! {
    static VALUE_POOL_TEST_STATS: RefCell<ValuePoolStats> =
        const { RefCell::new(ValuePoolStats::new()) };
}

#[cfg(test)]
fn reset_value_pool_test_stats() {
    VALUE_POOL_TEST_STATS.with(|stats| *stats.borrow_mut() = ValuePoolStats::new());
}

#[cfg(test)]
fn value_pool_test_stats_snapshot() -> ValuePoolStats {
    VALUE_POOL_TEST_STATS.with(|stats| *stats.borrow())
}

#[cfg(test)]
fn record_value_pool_acquire(hit: bool) {
    VALUE_POOL_TEST_STATS.with(|stats| {
        let mut stats = stats.borrow_mut();
        if hit {
            stats.slab_alloc_count += 1;
        } else {
            stats.global_alloc_fallback_count += 1;
        }
    });
}

#[cfg(test)]
fn record_value_pool_return(pool_len: usize) {
    VALUE_POOL_TEST_STATS.with(|stats| {
        let mut stats = stats.borrow_mut();
        stats.slab_return_count += 1;
        stats.slab_high_water_mark = stats.slab_high_water_mark.max(pool_len);
    });
}

/// Acquire a value from the thread-local pool, if available.
///
/// Returns `Some(value)` if a pooled value was available, `None` otherwise.
/// The returned value may contain stale data and should be overwritten
/// with the desired variant before use.
///
/// # Example
/// ```ignore
/// let value = pool_acquire().unwrap_or(SqliteValue::Null);
/// // Overwrite with actual data
/// value = SqliteValue::Integer(42);
/// ```
#[inline]
pub fn pool_acquire() -> Option<SqliteValue> {
    let value = VALUE_POOL.with(|pool| pool.borrow_mut().pop());
    #[cfg(test)]
    record_value_pool_acquire(value.is_some());
    value
}

/// Return a value to the thread-local pool for reuse.
///
/// Values are only pooled if the pool has capacity (max 256 entries).
/// Excess values are dropped normally, preventing unbounded memory growth.
///
/// For best effect, return values just before they would be dropped,
/// allowing future `pool_acquire` calls to skip allocation.
#[inline]
pub fn pool_return(value: SqliteValue) {
    VALUE_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < VALUE_POOL_CAP {
            pool.push(value);
            #[cfg(test)]
            record_value_pool_return(pool.len());
        }
        // If pool is full, value is dropped normally
    });
}

/// Return a heap-carrying value to the thread-local pool when preserving its
/// backing allocation is likely to pay off on the next decode/write.
#[inline]
pub fn pool_return_reusable(value: SqliteValue) {
    if value_preserves_reusable_heap_storage(&value) {
        pool_return(value);
    }
}

/// Clear the thread-local value pool.
///
/// Use this to release memory when a thread's workload is complete,
/// or in test teardown to ensure deterministic behavior.
#[inline]
pub fn pool_clear() {
    VALUE_POOL.with(|pool| pool.borrow_mut().clear());
}

/// Returns the current number of values in the thread-local pool.
///
/// Useful for testing and diagnostics.
#[inline]
pub fn pool_len() -> usize {
    VALUE_POOL.with(|pool| pool.borrow().len())
}

#[inline]
fn value_preserves_reusable_heap_storage(value: &SqliteValue) -> bool {
    match value {
        SqliteValue::Text(text) => matches!(&text.repr, SmallTextRepr::HeapOwned { .. }),
        SqliteValue::Blob(bytes) => Arc::strong_count(bytes) == 1,
        _ => false,
    }
}

/// Maximum inline string length for `SmallText`.
///
/// Strings up to this length are stored inline (on the stack/in the struct)
/// without heap allocation. Longer strings fall back to `Arc<str>`.
///
/// 23 bytes inline + 1 byte for length/tag = 24 bytes total, which aligns
/// with common cache line fractions and matches Arc<str>'s pointer size.
const SMALL_TEXT_INLINE_CAP: usize = 23;

/// A small-string-optimized text value.
///
/// Stores strings ≤ 23 bytes inline without heap allocation. Longer strings
/// stay owned until the first clone, then lazily promote to `Arc<str>` for
/// shared O(1) cloning. This eliminates both malloc and refcount traffic for
/// the common single-owner path.
pub struct SmallText {
    /// Representation: either inline bytes or a lazily shared heap string.
    repr: SmallTextRepr,
}

/// Internal representation for SmallText.
enum SmallTextRepr {
    /// Inline storage: length followed by up to 23 UTF-8 bytes.
    Inline {
        len: u8,
        buf: [u8; SMALL_TEXT_INLINE_CAP],
    },
    /// Heap storage before the first clone.
    ///
    /// The `Arc<str>` is materialized lazily on demand so a single-owner value
    /// pays no refcount cost until it is actually shared.
    HeapOwned {
        text: String,
        shared: OnceLock<Arc<str>>,
    },
    /// Heap storage after the text has been shared.
    HeapShared(Arc<str>),
}

impl Clone for SmallText {
    fn clone(&self) -> Self {
        Self {
            repr: self.repr.clone(),
        }
    }
}

impl Clone for SmallTextRepr {
    fn clone(&self) -> Self {
        match self {
            Self::Inline { len, buf } => Self::Inline {
                len: *len,
                buf: *buf,
            },
            Self::HeapOwned { text, shared } => {
                let shared = Arc::clone(shared.get_or_init(|| Arc::from(text.as_str())));
                Self::HeapShared(shared)
            }
            Self::HeapShared(text) => Self::HeapShared(Arc::clone(text)),
        }
    }
}

impl SmallText {
    /// Create a new SmallText from a string slice.
    #[inline]
    pub fn new(s: &str) -> Self {
        if s.len() <= SMALL_TEXT_INLINE_CAP {
            let mut buf = [0u8; SMALL_TEXT_INLINE_CAP];
            buf[..s.len()].copy_from_slice(s.as_bytes());
            Self {
                repr: SmallTextRepr::Inline {
                    len: s.len() as u8,
                    buf,
                },
            }
        } else {
            Self {
                repr: SmallTextRepr::HeapOwned {
                    text: s.to_owned(),
                    shared: OnceLock::new(),
                },
            }
        }
    }

    /// Create from an owned String, potentially reusing its allocation.
    #[inline]
    pub fn from_string<S>(s: S) -> Self
    where
        S: Into<String> + AsRef<str>,
    {
        if s.as_ref().len() <= SMALL_TEXT_INLINE_CAP {
            Self::new(s.as_ref())
        } else {
            Self {
                repr: SmallTextRepr::HeapOwned {
                    text: s.into(),
                    shared: OnceLock::new(),
                },
            }
        }
    }

    /// Create from an `Arc<str>`, avoiding re-allocation if already heap.
    #[inline]
    pub fn from_arc(arc: Arc<str>) -> Self {
        if arc.len() <= SMALL_TEXT_INLINE_CAP {
            Self::new(&arc)
        } else {
            Self {
                repr: SmallTextRepr::HeapShared(arc),
            }
        }
    }

    /// Overwrite this string, reusing the existing heap allocation when the
    /// value is still single-owner.
    #[inline]
    pub fn overwrite(&mut self, s: &str) {
        if s.len() <= SMALL_TEXT_INLINE_CAP {
            let mut buf = [0u8; SMALL_TEXT_INLINE_CAP];
            buf[..s.len()].copy_from_slice(s.as_bytes());
            self.repr = SmallTextRepr::Inline {
                len: s.len() as u8,
                buf,
            };
            return;
        }

        match &mut self.repr {
            SmallTextRepr::HeapOwned { text, shared } => {
                text.clear();
                text.push_str(s);
                if shared.get().is_some() {
                    *shared = OnceLock::new();
                }
            }
            _ => {
                self.repr = SmallTextRepr::HeapOwned {
                    text: s.to_owned(),
                    shared: OnceLock::new(),
                };
            }
        }
    }

    /// Get the string as a slice.
    ///
    /// OPT-UTF8: the inline buffer is always valid UTF-8 by construction (see
    /// the constructors and [`Self::overwrite`]), but because
    /// `forbid(unsafe_code)` prevents `from_utf8_unchecked` we must run a
    /// validator. `simdutf8::basic::from_utf8` is a drop-in for
    /// `std::str::from_utf8` that uses runtime-dispatched SIMD and is
    /// ~3-10x faster on the ASCII-dominant TEXT payloads that make up the
    /// majority of real SQL workloads.
    #[inline]
    pub fn as_str(&self) -> &str {
        match &self.repr {
            SmallTextRepr::Inline { len, buf } => simdutf8::basic::from_utf8(&buf[..*len as usize])
                .expect("SmallText inline representation must always contain valid UTF-8"),
            SmallTextRepr::HeapOwned { text, .. } => text.as_str(),
            SmallTextRepr::HeapShared(text) => text,
        }
    }

    /// Get the raw bytes of this text value without going through `&str`.
    ///
    /// Unlike [`Self::as_str`] (which revalidates the inline buffer via
    /// `from_utf8`), this directly returns the stored bytes. The returned
    /// slice is guaranteed to be valid UTF-8 by construction: every code path
    /// that writes to a `SmallText` (`new`, `from_string`, `from_arc`,
    /// `overwrite`) sources its bytes from a `&str` or `Arc<str>`.
    ///
    /// This is useful on hot paths where a byte-wise equality check is the
    /// only operation performed — for example, the record-decode fast path
    /// that reuses an existing slot when incoming bytes match what is already
    /// there. Skipping the internal `from_utf8` of `as_str` measurably
    /// reduces per-column decode cost on INSERT/SELECT workloads.
    #[inline]
    #[must_use]
    pub fn as_bytes_direct(&self) -> &[u8] {
        match &self.repr {
            SmallTextRepr::Inline { len, buf } => &buf[..*len as usize],
            SmallTextRepr::HeapOwned { text, .. } => text.as_bytes(),
            SmallTextRepr::HeapShared(text) => text.as_bytes(),
        }
    }

    /// Get the length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            SmallTextRepr::Inline { len, .. } => *len as usize,
            SmallTextRepr::HeapOwned { text, .. } => text.len(),
            SmallTextRepr::HeapShared(text) => text.len(),
        }
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if stored inline (no heap allocation).
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(&self.repr, SmallTextRepr::Inline { .. })
    }

    /// Convert to `Arc<str>`, potentially allocating if currently inline.
    #[inline]
    pub fn into_arc(self) -> Arc<str> {
        match self.repr {
            SmallTextRepr::Inline { len, buf } => {
                // See `as_str` for the simdutf8 rationale.
                let s = simdutf8::basic::from_utf8(&buf[..len as usize])
                    .expect("SmallText inline representation must always contain valid UTF-8");
                Arc::from(s)
            }
            SmallTextRepr::HeapOwned { text, shared } => shared
                .into_inner()
                .unwrap_or_else(|| Arc::<str>::from(text)),
            SmallTextRepr::HeapShared(text) => text,
        }
    }
}

impl Default for SmallText {
    #[inline]
    fn default() -> Self {
        Self::new("")
    }
}

impl fmt::Debug for SmallText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for SmallText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

impl PartialEq for SmallText {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for SmallText {}

impl PartialOrd for SmallText {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SmallText {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for SmallText {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl From<&str> for SmallText {
    #[inline]
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for SmallText {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl From<Arc<str>> for SmallText {
    #[inline]
    fn from(arc: Arc<str>) -> Self {
        Self::from_arc(arc)
    }
}

impl AsRef<str> for SmallText {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for SmallText {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for SmallText {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

// Serde implementations for SmallText
impl serde::Serialize for SmallText {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for SmallText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::from_string(s))
    }
}

/// Scan the longest SQLite numeric prefix from a byte slice.
///
/// Recognises `[+-]? [0-9]* ('.' [0-9]*)? ([eE] [+-]? [0-9]+)?`.
/// Returns the byte offset where the prefix ends, or 0 if no numeric prefix
/// is present.
fn scan_numeric_prefix(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }

    let mut i = 0usize;
    if bytes[i] == b'+' || bytes[i] == b'-' {
        i += 1;
    }

    let mut has_digit = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        has_digit = true;
        i += 1;
    }

    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            has_digit = true;
            i += 1;
        }
    }

    if !has_digit {
        return 0;
    }

    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let exp_start = i;
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        if i < bytes.len() && bytes[i].is_ascii_digit() {
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
        } else {
            i = exp_start;
        }
    }

    i
}

/// Parse the longest numeric prefix of `b` as an integer.
#[allow(clippy::cast_possible_truncation)]
fn parse_integer_prefix_bytes(b: &[u8]) -> i64 {
    let mut start = 0;
    while start < b.len() && b[start].is_ascii_whitespace() {
        start += 1;
    }
    let trimmed = &b[start..];
    let end = scan_numeric_prefix(trimmed);
    if end == 0 {
        return 0;
    }
    // SAFETY: scan_numeric_prefix only advances over ASCII bytes (digits, +, -, ., e, E),
    // so the slice is always valid UTF-8.
    let s = std::str::from_utf8(&trimmed[..end]).unwrap_or("");
    let f = s.parse::<f64>().unwrap_or(0.0);
    #[allow(clippy::manual_clamp)]
    if f >= i64::MAX as f64 {
        i64::MAX
    } else if f <= i64::MIN as f64 {
        i64::MIN
    } else {
        f as i64
    }
}

/// Parse the longest numeric prefix of `s` as an integer.
#[allow(clippy::cast_possible_truncation)]
fn parse_integer_prefix(s: &str) -> i64 {
    parse_integer_prefix_bytes(s.as_bytes())
}

/// Parse the longest numeric prefix of `b` as a float.
fn parse_float_prefix_bytes(b: &[u8]) -> f64 {
    let mut start = 0;
    while start < b.len() && b[start].is_ascii_whitespace() {
        start += 1;
    }
    let trimmed = &b[start..];
    let end = scan_numeric_prefix(trimmed);
    if end == 0 {
        return 0.0;
    }
    // SAFETY: scan_numeric_prefix only advances over ASCII bytes (digits, +, -, ., e, E),
    // so the slice is always valid UTF-8.
    let s = std::str::from_utf8(&trimmed[..end]).unwrap_or("");
    s.parse::<f64>().unwrap_or(0.0)
}

/// Parse the longest numeric prefix of `s` as a float.
fn parse_float_prefix(s: &str) -> f64 {
    parse_float_prefix_bytes(s.as_bytes())
}

fn trim_sqlite_ascii_whitespace(s: &str) -> &str {
    s.trim_matches(|ch: char| ch.is_ascii_whitespace())
}

fn cast_text_prefix_to_numeric(s: &str) -> SqliteValue {
    let trimmed = trim_sqlite_ascii_whitespace(s);
    let end = scan_numeric_prefix(trimmed.as_bytes());
    if end == 0 {
        return SqliteValue::Integer(0);
    }

    let prefix = &trimmed[..end];
    let is_integer_syntax = !prefix
        .as_bytes()
        .iter()
        .any(|byte| matches!(*byte, b'.' | b'e' | b'E'));

    if is_integer_syntax && let Ok(value) = prefix.parse::<i64>() {
        return SqliteValue::Integer(value);
    }

    if let Ok(value) = prefix.parse::<f64>() {
        if value.is_finite()
            && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
        {
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            let truncated = value as i64;
            #[allow(clippy::float_cmp, clippy::cast_precision_loss)]
            if truncated as f64 == value {
                return SqliteValue::Integer(truncated);
            }
        }
        return SqliteValue::Float(value);
    }

    SqliteValue::Integer(0)
}

/// A dynamically-typed SQLite value.
///
/// Corresponds to C SQLite's `sqlite3_value` / `Mem` type. SQLite has five
/// fundamental storage classes: NULL, INTEGER, REAL, TEXT, and BLOB.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum SqliteValue {
    /// A NULL value.
    Null,
    /// A signed 64-bit integer.
    Integer(i64),
    /// A 64-bit IEEE floating point number.
    Float(f64),
    /// A UTF-8 text string.
    ///
    /// Uses `SmallText` for small-string optimization: strings ≤ 23 bytes
    /// are stored inline without heap allocation. Longer strings use
    /// `Arc<str>` internally for O(1) cloning.
    Text(SmallText),
    /// A binary large object.
    ///
    /// Uses `Arc<[u8]>` for the same O(1)-clone benefit as `Text`.
    Blob(Arc<[u8]>),
}

impl SqliteValue {
    /// Returns the type affinity that best describes this value.
    pub const fn affinity(&self) -> TypeAffinity {
        match self {
            Self::Null | Self::Blob(_) => TypeAffinity::Blob,
            Self::Integer(_) => TypeAffinity::Integer,
            Self::Float(_) => TypeAffinity::Real,
            Self::Text(_) => TypeAffinity::Text,
        }
    }

    /// Returns the storage class of this value.
    pub const fn storage_class(&self) -> StorageClass {
        match self {
            Self::Null => StorageClass::Null,
            Self::Integer(_) => StorageClass::Integer,
            Self::Float(_) => StorageClass::Real,
            Self::Text(_) => StorageClass::Text,
            Self::Blob(_) => StorageClass::Blob,
        }
    }

    /// Apply column type affinity coercion (advisory mode).
    ///
    /// In non-STRICT tables, affinity is advisory: values are coerced when
    /// possible but never rejected. Follows SQLite §3.4 rules from
    /// <https://www.sqlite.org/datatype3.html#type_affinity_of_a_column>.
    ///
    /// - TEXT affinity: numeric values converted to text before storing.
    /// - NUMERIC affinity: text parsed as integer/real if well-formed; exact-integer reals become integer.
    /// - INTEGER affinity: identical to NUMERIC for storage/comparison coercion (differ only in CAST).
    /// - REAL affinity: like NUMERIC, plus integers forced to float.
    /// - BLOB affinity: no conversion.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::float_cmp
    )]
    pub fn apply_affinity(self, affinity: TypeAffinity) -> Self {
        match affinity {
            TypeAffinity::Blob => self,
            TypeAffinity::Text => match self {
                Self::Null | Self::Text(_) | Self::Blob(_) => self,
                Self::Integer(_) | Self::Float(_) => {
                    let t = self.to_text();
                    Self::Text(SmallText::from_string(t))
                }
            },
            TypeAffinity::Numeric | TypeAffinity::Integer => match &self {
                Self::Text(s) => try_coerce_text_to_numeric(s.as_str()).unwrap_or(self),
                Self::Float(f) => {
                    if *f >= -9_223_372_036_854_775_808.0 && *f < 9_223_372_036_854_775_808.0 {
                        let i = *f as i64;
                        if (i as f64) == *f {
                            return Self::Integer(i);
                        }
                    }
                    self
                }
                _ => self,
            },
            TypeAffinity::Real => match &self {
                Self::Text(s) => try_coerce_text_to_numeric(s.as_str())
                    .map(|v| match v {
                        Self::Integer(i) => Self::Float(i as f64),
                        other => other,
                    })
                    .unwrap_or(self),
                Self::Integer(i) => Self::Float(*i as f64),
                _ => self,
            },
        }
    }

    /// Validate a value against a STRICT table column type.
    ///
    /// NULL is always accepted (nullability is enforced separately via NOT NULL).
    /// Returns `Ok(value)` with possible implicit coercion (REAL columns accept
    /// integers, converting them to float), or `Err` if the storage class is
    /// incompatible.
    #[allow(clippy::cast_precision_loss)]
    pub fn validate_strict(self, col_type: StrictColumnType) -> Result<Self, StrictTypeError> {
        if matches!(self, Self::Null) {
            return Ok(self);
        }
        match col_type {
            StrictColumnType::Any => Ok(self),
            StrictColumnType::Integer => match self {
                Self::Integer(_) => Ok(self),
                other => Err(StrictTypeError {
                    expected: col_type,
                    actual: other.storage_class(),
                }),
            },
            StrictColumnType::Real => match self {
                Self::Float(_) => Ok(self),
                Self::Integer(i) => Ok(Self::Float(i as f64)),
                other => Err(StrictTypeError {
                    expected: col_type,
                    actual: other.storage_class(),
                }),
            },
            StrictColumnType::Text => match self {
                Self::Text(_) => Ok(self),
                other => Err(StrictTypeError {
                    expected: col_type,
                    actual: other.storage_class(),
                }),
            },
            StrictColumnType::Blob => match self {
                Self::Blob(_) => Ok(self),
                other => Err(StrictTypeError {
                    expected: col_type,
                    actual: other.storage_class(),
                }),
            },
        }
    }

    /// Returns true if this is a NULL value.
    #[inline(always)]
    #[allow(clippy::inline_always)]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to extract an integer value.
    #[inline]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract a float value.
    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Try to extract a text reference.
    #[inline]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract a blob reference.
    #[inline]
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Convert to an integer following SQLite's type coercion rules.
    ///
    /// - NULL -> 0
    /// - Integer -> itself
    /// - Float -> truncated to i64
    /// - Text -> attempt to parse, 0 on failure
    /// - Blob -> parse bytes as numeric string, 0 on failure
    #[inline(always)]
    #[allow(clippy::inline_always)]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_integer(&self) -> i64 {
        match self {
            Self::Null => 0,
            Self::Integer(i) => *i,
            Self::Float(f) => *f as i64,
            Self::Text(s) => parse_integer_prefix(s),
            Self::Blob(b) => parse_integer_prefix_bytes(b),
        }
    }

    /// Convert to a float following SQLite's type coercion rules.
    ///
    /// - NULL -> 0.0
    /// - Integer -> as f64
    /// - Float -> itself
    /// - Text -> attempt to parse, 0.0 on failure
    /// - Blob -> parse bytes as numeric string, 0.0 on failure
    #[inline(always)]
    #[allow(clippy::inline_always)]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_float(&self) -> f64 {
        match self {
            Self::Null => 0.0,
            Self::Integer(i) => *i as f64,
            Self::Float(f) => *f,
            Self::Text(s) => parse_float_prefix(s),
            Self::Blob(b) => parse_float_prefix_bytes(b),
        }
    }

    /// Coerce a value for SQLite `sum()` accumulation.
    ///
    /// `sum()` keeps integer accumulation only for INTEGER values and text that
    /// is entirely a signed 64-bit integer literal after trimming SQLite ASCII
    /// whitespace. Other text and all blobs participate through the REAL
    /// accumulator, even when their numeric prefix is integer-looking.
    #[must_use]
    pub fn to_sum_numeric_value(&self) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::Integer(i) => Self::Integer(*i),
            Self::Float(f) => Self::Float(*f),
            Self::Text(s) => {
                let trimmed = trim_sqlite_ascii_whitespace(s.as_str());
                if let Ok(integer) = trimmed.parse::<i64>() {
                    Self::Integer(integer)
                } else {
                    Self::Float(parse_float_prefix(s))
                }
            }
            Self::Blob(b) => Self::Float(parse_float_prefix_bytes(b)),
        }
    }

    /// Borrow the inner text string without allocating.
    ///
    /// Returns `Some(&str)` for `Text` values, `None` otherwise.
    /// Use this in comparisons, LIKE patterns, and WHERE clause
    /// evaluation to avoid the clone that `to_text()` incurs.
    #[inline]
    #[must_use]
    pub fn as_text_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow the inner blob bytes without allocating.
    #[inline]
    #[must_use]
    pub fn as_blob_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Convert to text following SQLite's CAST(x AS TEXT) coercion rules.
    ///
    /// For blobs, this interprets the raw bytes as UTF-8 (with lossy
    /// replacement for invalid sequences), matching C SQLite behavior.
    /// For the SQL-literal hex format (`X'...'`), use the `Display` impl.
    pub fn to_text(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => format_sqlite_float(*f),
            Self::Text(s) => s.to_string(),
            Self::Blob(b) => String::from_utf8_lossy(b).into_owned(),
        }
    }

    /// Convert to NUMERIC using SQLite CAST semantics rather than affinity.
    ///
    /// Unlike NUMERIC affinity, CAST always produces a numeric storage class for
    /// text/blob input, using the longest leading numeric prefix or `0` when no
    /// numeric prefix exists.
    #[must_use]
    pub fn cast_to_numeric(&self) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::Integer(i) => Self::Integer(*i),
            Self::Float(f) => Self::Float(*f),
            Self::Text(s) => cast_text_prefix_to_numeric(s),
            Self::Blob(b) => cast_text_prefix_to_numeric(&String::from_utf8_lossy(b)),
        }
    }

    /// Returns the SQLite `typeof()` string for this value.
    ///
    /// Matches C sqlite3: "null", "integer", "real", "text", or "blob".
    pub const fn typeof_str(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Integer(_) => "integer",
            Self::Float(_) => "real",
            Self::Text(_) => "text",
            Self::Blob(_) => "blob",
        }
    }

    /// Returns the SQLite `length()` result for this value.
    ///
    /// - NULL → NULL (represented as None)
    /// - TEXT → character count
    /// - BLOB → byte count
    /// - INTEGER/REAL → character count of text representation
    pub fn sql_length(&self) -> Option<i64> {
        match self {
            Self::Null => None,
            Self::Text(s) => Some(i64::try_from(s.chars().count()).unwrap_or(i64::MAX)),
            Self::Blob(b) => Some(i64::try_from(b.len()).unwrap_or(i64::MAX)),
            Self::Integer(_) | Self::Float(_) => {
                let t = self.to_text();
                Some(i64::try_from(t.chars().count()).unwrap_or(i64::MAX))
            }
        }
    }

    /// Check equality for UNIQUE constraint purposes.
    ///
    /// In SQLite, NULL != NULL for uniqueness: if either value is NULL, the
    /// result is `false` (they are never considered duplicates). Non-NULL values
    /// compare by storage class ordering (same as `PartialEq`).
    pub fn unique_eq(&self, other: &Self) -> bool {
        if self.is_null() || other.is_null() {
            return false;
        }
        matches!(self.partial_cmp(other), Some(Ordering::Equal))
    }

    /// Convert a floating-point arithmetic result into a SQLite value.
    ///
    /// SQLite does not surface NaN; NaN is normalized to NULL while ±Inf remain REAL.
    fn float_result_or_null(result: f64) -> Self {
        if result.is_nan() {
            Self::Null
        } else {
            Self::Float(result)
        }
    }

    /// Mirrors C SQLite's `numericType()` (SQLite VDBE:496): returns true if this
    /// value should be treated as an integer for arithmetic purposes.
    ///
    /// Integer values are obviously integer-typed. Text/Blob values that parse
    /// as i64 are also integer-typed. Float and Null are not.
    #[inline]
    pub fn is_integer_numeric_type(&self) -> bool {
        fn text_is_integer_numeric_type(s: &str) -> bool {
            let trimmed = s.trim_start();
            let end = scan_numeric_prefix(trimmed.as_bytes());
            end > 0
                && !trimmed.as_bytes()[..end]
                    .iter()
                    .any(|byte| matches!(*byte, b'.' | b'e' | b'E'))
        }

        match self {
            Self::Integer(_) => true,
            Self::Float(_) | Self::Null => false,
            Self::Text(s) => text_is_integer_numeric_type(s),
            Self::Blob(b) => text_is_integer_numeric_type(&String::from_utf8_lossy(b)),
        }
    }

    /// Returns true if this value should be treated as a float for arithmetic.
    /// A value is "float numeric type" only if it has a numeric prefix
    /// containing '.', 'e', or 'E'. Non-numeric text/blob is NOT float
    /// (it coerces to integer 0 in C SQLite's OP_Add/Sub/Mul).
    #[inline]
    fn is_float_numeric_type(&self) -> bool {
        fn text_is_float(s: &str) -> bool {
            let trimmed = s.trim_start();
            let end = scan_numeric_prefix(trimmed.as_bytes());
            end > 0
                && trimmed.as_bytes()[..end]
                    .iter()
                    .any(|byte| matches!(*byte, b'.' | b'e' | b'E'))
        }
        match self {
            Self::Float(_) => true,
            Self::Integer(_) | Self::Null => false,
            Self::Text(s) => text_is_float(s),
            Self::Blob(b) => text_is_float(&String::from_utf8_lossy(b)),
        }
    }

    /// Add two values following SQLite's overflow semantics.
    ///
    /// - Integer + Integer: checked add; overflows promote to REAL.
    /// - Any REAL operand: float addition.
    /// - NULL propagates (NULL + x = NULL).
    /// - Text/Blob coerced via `numericType()`: if both parse as integer,
    ///   integer math is used (SQLite VDBE:1932-1934).
    #[inline(always)]
    #[allow(clippy::inline_always)]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sql_add(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Self::Null,
            (Self::Integer(a), Self::Integer(b)) => match a.checked_add(*b) {
                Some(result) => Self::Integer(result),
                None => Self::float_result_or_null(*a as f64 + *b as f64),
            },
            // If neither operand is a float-type (i.e. both are integer,
            // integer-text, or non-numeric text/blob), use integer arithmetic.
            // Non-numeric text like "hello" coerces to integer 0, not float 0.0.
            _ if !self.is_float_numeric_type() && !other.is_float_numeric_type() => {
                let a = self.to_integer();
                let b = other.to_integer();
                match a.checked_add(b) {
                    Some(result) => Self::Integer(result),
                    None => Self::float_result_or_null(a as f64 + b as f64),
                }
            }
            _ => Self::float_result_or_null(self.to_float() + other.to_float()),
        }
    }

    /// Subtract two values following SQLite's overflow semantics.
    ///
    /// Integer - Integer with overflow promotes to REAL.
    #[inline(always)]
    #[allow(clippy::inline_always)]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sql_sub(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Self::Null,
            (Self::Integer(a), Self::Integer(b)) => match a.checked_sub(*b) {
                Some(result) => Self::Integer(result),
                None => Self::float_result_or_null(*a as f64 - *b as f64),
            },
            _ if !self.is_float_numeric_type() && !other.is_float_numeric_type() => {
                let a = self.to_integer();
                let b = other.to_integer();
                match a.checked_sub(b) {
                    Some(result) => Self::Integer(result),
                    None => Self::float_result_or_null(a as f64 - b as f64),
                }
            }
            _ => Self::float_result_or_null(self.to_float() - other.to_float()),
        }
    }

    /// Multiply two values following SQLite's overflow semantics.
    ///
    /// Integer * Integer with overflow promotes to REAL.
    #[inline(always)]
    #[allow(clippy::inline_always)]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sql_mul(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Self::Null,
            (Self::Integer(a), Self::Integer(b)) => match a.checked_mul(*b) {
                Some(result) => Self::Integer(result),
                None => Self::float_result_or_null(*a as f64 * *b as f64),
            },
            (Self::Integer(a), Self::Float(b)) => Self::float_result_or_null(*a as f64 * *b),
            (Self::Float(a), Self::Integer(b)) => Self::float_result_or_null(*a * *b as f64),
            (Self::Float(a), Self::Float(b)) => Self::float_result_or_null(*a * *b),
            _ if !self.is_float_numeric_type() && !other.is_float_numeric_type() => {
                let a = self.to_integer();
                let b = other.to_integer();
                match a.checked_mul(b) {
                    Some(result) => Self::Integer(result),
                    None => Self::float_result_or_null(a as f64 * b as f64),
                }
            }
            _ => Self::float_result_or_null(self.to_float() * other.to_float()),
        }
    }

    /// The sort order key for NULL values (SQLite sorts NULLs first).
    const fn sort_class(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Integer(_) | Self::Float(_) => 1,
            Self::Text(_) => 2,
            Self::Blob(_) => 3,
        }
    }
}

/// Check if two composite UNIQUE keys are duplicates (SQLite NULL semantics).
///
/// Returns `true` only if ALL corresponding components are non-NULL and equal.
/// If ANY component in either key is NULL, the keys are NOT duplicates (per
/// SQLite's NULL != NULL rule for UNIQUE constraints).
///
/// Both slices must have the same length (panics otherwise).
pub fn unique_key_duplicates(a: &[SqliteValue], b: &[SqliteValue]) -> bool {
    assert_eq!(a.len(), b.len(), "UNIQUE key columns must match");
    a.iter().zip(b.iter()).all(|(va, vb)| va.unique_eq(vb))
}

/// Match a string against a SQL LIKE pattern with SQLite semantics.
///
/// - `%` matches zero or more characters.
/// - `_` matches exactly one character.
/// - Case-insensitive for ASCII A-Z only (no Unicode case folding without ICU).
/// - `escape` optionally specifies the escape character for literal `%`/`_`.
pub fn sql_like(pattern: &str, text: &str, escape: Option<char>) -> bool {
    sql_like_cased(pattern, text, escape, false)
}

/// Match a string against a SQL LIKE pattern, honoring the connection's
/// `PRAGMA case_sensitive_like` setting.
///
/// When `case_sensitive` is `false` (the default) this is identical to
/// [`sql_like`]: ASCII case is folded. When `case_sensitive` is `true`
/// (`PRAGMA case_sensitive_like = ON`) the literal portions of the pattern are
/// matched byte-exact — wildcards (`%`, `_`) still behave the same.
#[must_use]
pub fn sql_like_cased(
    pattern: &str,
    text: &str,
    escape: Option<char>,
    case_sensitive: bool,
) -> bool {
    if let Some((kind, literal)) = classify_sql_like_fast_path(pattern, escape) {
        return sql_like_fast_path_matches_cased(kind, literal, text, case_sensitive);
    }

    sql_like_inner(
        &pattern.chars().collect::<Vec<_>>(),
        &text.chars().collect::<Vec<_>>(),
        escape,
        0,
        0,
        case_sensitive,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlLikeFastPathKind {
    MatchAll,
    Exact,
    Prefix,
    Suffix,
    Contains,
}

impl SqlLikeFastPathKind {
    #[must_use]
    pub const fn opcode_tag(self) -> i32 {
        match self {
            Self::MatchAll => 0,
            Self::Exact => 1,
            Self::Prefix => 2,
            Self::Suffix => 3,
            Self::Contains => 4,
        }
    }

    #[must_use]
    pub const fn from_opcode_tag(tag: i32) -> Option<Self> {
        match tag {
            0 => Some(Self::MatchAll),
            1 => Some(Self::Exact),
            2 => Some(Self::Prefix),
            3 => Some(Self::Suffix),
            4 => Some(Self::Contains),
            _ => None,
        }
    }
}

#[must_use]
pub fn sql_like_fast_path_matches(kind: SqlLikeFastPathKind, literal: &str, text: &str) -> bool {
    sql_like_fast_path_matches_cased(kind, literal, text, false)
}

/// Like [`sql_like_fast_path_matches`] but honoring `case_sensitive`.
///
/// When `case_sensitive` is `true` the literal is compared byte-exact
/// (`PRAGMA case_sensitive_like = ON`); otherwise ASCII case is folded.
#[must_use]
pub fn sql_like_fast_path_matches_cased(
    kind: SqlLikeFastPathKind,
    literal: &str,
    text: &str,
    case_sensitive: bool,
) -> bool {
    match kind {
        SqlLikeFastPathKind::MatchAll => true,
        SqlLikeFastPathKind::Exact => {
            if case_sensitive {
                literal.as_bytes() == text.as_bytes()
            } else {
                ascii_ci_eq_bytes(literal.as_bytes(), text.as_bytes())
            }
        }
        SqlLikeFastPathKind::Prefix => {
            if case_sensitive {
                text.as_bytes().starts_with(literal.as_bytes())
            } else {
                ascii_ci_starts_with(text, literal)
            }
        }
        SqlLikeFastPathKind::Suffix => {
            if case_sensitive {
                text.as_bytes().ends_with(literal.as_bytes())
            } else {
                ascii_ci_ends_with(text, literal)
            }
        }
        SqlLikeFastPathKind::Contains => {
            if case_sensitive {
                literal.is_empty() || memmem::find(text.as_bytes(), literal.as_bytes()).is_some()
            } else {
                ascii_ci_contains(text, literal)
            }
        }
    }
}

/// Reusable matcher for simple LIKE patterns classified by `classify_sql_like_fast_path`.
pub struct SqlLikeFastPathMatcher<'a> {
    kind: SqlLikeFastPathKind,
    literal: &'a str,
    contains_finder: Option<memmem::Finder<'a>>,
    case_sensitive: bool,
}

impl<'a> SqlLikeFastPathMatcher<'a> {
    #[must_use]
    pub fn new(kind: SqlLikeFastPathKind, literal: &'a str) -> Self {
        Self::new_cased(kind, literal, false)
    }

    /// Build a matcher honoring `case_sensitive` (`PRAGMA case_sensitive_like`).
    #[must_use]
    pub fn new_cased(kind: SqlLikeFastPathKind, literal: &'a str, case_sensitive: bool) -> Self {
        let contains_finder = (kind == SqlLikeFastPathKind::Contains && !literal.is_empty())
            .then(|| memmem::Finder::new(literal.as_bytes()));
        Self {
            kind,
            literal,
            contains_finder,
            case_sensitive,
        }
    }

    #[must_use]
    pub fn matches(&self, text: &str) -> bool {
        if let (SqlLikeFastPathKind::Contains, Some(finder)) = (self.kind, &self.contains_finder) {
            let text_bytes = text.as_bytes();
            let needle_bytes = self.literal.as_bytes();
            if needle_bytes.len() > text_bytes.len() {
                return false;
            }
            if finder.find(text_bytes).is_some() {
                return true;
            }
            // A byte-exact substring (the finder above) is the *only* match when
            // the connection's LIKE is case-sensitive; otherwise fall back to the
            // ASCII-case-folded scan.
            if self.case_sensitive {
                return false;
            }
            return ascii_ci_contains_folded_scan(text_bytes, needle_bytes);
        }
        sql_like_fast_path_matches_cased(self.kind, self.literal, text, self.case_sensitive)
    }
}

#[must_use]
pub fn classify_sql_like_fast_path(
    pattern: &str,
    escape: Option<char>,
) -> Option<(SqlLikeFastPathKind, &str)> {
    if escape.is_some() || pattern.contains('_') {
        return None;
    }
    if !pattern.contains('%') {
        return Some((SqlLikeFastPathKind::Exact, pattern));
    }
    if pattern.chars().all(|ch| ch == '%') {
        return Some((SqlLikeFastPathKind::MatchAll, ""));
    }

    let trimmed_start = pattern.trim_start_matches('%');
    let trimmed_end = pattern.trim_end_matches('%');
    if pattern.starts_with('%') && pattern.ends_with('%') {
        let core = trimmed_start.trim_end_matches('%');
        if core.is_empty() {
            return Some((SqlLikeFastPathKind::MatchAll, ""));
        }
        if !core.contains('%') {
            return Some((SqlLikeFastPathKind::Contains, core));
        }
    }
    if !pattern.starts_with('%') && trimmed_end.len() < pattern.len() && !trimmed_end.contains('%')
    {
        return Some((SqlLikeFastPathKind::Prefix, trimmed_end));
    }
    if !pattern.ends_with('%')
        && trimmed_start.len() < pattern.len()
        && !trimmed_start.contains('%')
    {
        return Some((SqlLikeFastPathKind::Suffix, trimmed_start));
    }
    None
}

fn sql_like_inner(
    pattern: &[char],
    text: &[char],
    escape: Option<char>,
    pi: usize,
    ti: usize,
    case_sensitive: bool,
) -> bool {
    let mut pi = pi;
    let mut ti = ti;

    while pi < pattern.len() {
        let pc = pattern[pi];

        // Handle escape character.
        if Some(pc) == escape {
            pi += 1;
            if pi >= pattern.len() {
                return false; // Trailing escape is malformed.
            }
            // Match the escaped character literally.
            if ti >= text.len() || !chars_eq(pattern[pi], text[ti], case_sensitive) {
                return false;
            }
            pi += 1;
            ti += 1;
            continue;
        }

        match pc {
            '%' => {
                // Skip consecutive % wildcards.
                while pi < pattern.len() && pattern[pi] == '%' {
                    pi += 1;
                }
                // If % is at end of pattern, matches everything.
                if pi >= pattern.len() {
                    return true;
                }
                // Try matching rest of pattern at each position.
                for start in ti..=text.len() {
                    if sql_like_inner(pattern, text, escape, pi, start, case_sensitive) {
                        return true;
                    }
                }
                return false;
            }
            '_' => {
                if ti >= text.len() {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            _ => {
                if ti >= text.len() || !chars_eq(pc, text[ti], case_sensitive) {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti >= text.len()
}

/// LIKE literal-character comparison: byte-exact when `case_sensitive`
/// (`PRAGMA case_sensitive_like = ON`), otherwise ASCII case-folded (default).
#[inline]
fn chars_eq(a: char, b: char, case_sensitive: bool) -> bool {
    if case_sensitive {
        a == b
    } else {
        ascii_ci_eq(a, b)
    }
}

/// ASCII-only case-insensitive character comparison (SQLite LIKE semantics).
fn ascii_ci_eq(a: char, b: char) -> bool {
    if a == b {
        return true;
    }
    // Only fold ASCII A-Z / a-z.
    a.is_ascii() && b.is_ascii() && a.eq_ignore_ascii_case(&b)
}

#[inline]
fn ascii_fold_byte(byte: u8) -> u8 {
    byte.to_ascii_lowercase()
}

#[inline]
fn ascii_ci_eq_byte(left: u8, right: u8) -> bool {
    left == right || ((left ^ right) == 0x20 && left.is_ascii_alphabetic())
}

fn ascii_ci_eq_bytes(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut idx = 0;
    while idx < left.len() {
        if !ascii_ci_eq_byte(left[idx], right[idx]) {
            return false;
        }
        idx += 1;
    }
    true
}

fn ascii_ci_starts_with(text: &str, prefix: &str) -> bool {
    let text = text.as_bytes();
    let prefix = prefix.as_bytes();
    text.len() >= prefix.len() && ascii_ci_eq_bytes(&text[..prefix.len()], prefix)
}

fn ascii_ci_ends_with(text: &str, suffix: &str) -> bool {
    let text = text.as_bytes();
    let suffix = suffix.as_bytes();
    text.len() >= suffix.len() && ascii_ci_eq_bytes(&text[text.len() - suffix.len()..], suffix)
}

fn ascii_ci_contains(text: &str, needle: &str) -> bool {
    let text = text.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    if needle.len() > text.len() {
        return false;
    }
    if memmem::find(text, needle).is_some() {
        return true;
    }

    ascii_ci_contains_folded_scan(text, needle)
}

fn ascii_ci_contains_folded_scan(text: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > text.len() {
        return false;
    }
    let max_start = text.len() - needle.len();
    let first = needle[0];
    let first_folded = ascii_fold_byte(first);
    let first_alt = if first.is_ascii_alphabetic() {
        first_folded.to_ascii_uppercase()
    } else {
        first_folded
    };
    let mut start = 0;
    while start <= max_start {
        let rel = if first_folded == first_alt {
            memchr(first_folded, &text[start..=max_start])
        } else {
            memchr2(first_folded, first_alt, &text[start..=max_start])
        };
        let Some(rel) = rel else {
            break;
        };
        start += rel;
        if ascii_ci_eq_bytes(&text[start + 1..start + needle.len()], &needle[1..]) {
            return true;
        }
        start += 1;
    }
    false
}

/// Accumulator for SQL `sum()` aggregate with SQLite overflow semantics.
///
/// Unlike expression arithmetic (which promotes to REAL on overflow), `sum()`
/// raises an error on integer overflow only if all non-NULL inputs remain in
/// the integer accumulator. A later REAL input suppresses the overflow error
/// and returns the approximate REAL sum, matching C sqlite3 behavior.
#[derive(Debug, Clone)]
pub struct SumAccumulator {
    /// Running integer sum (if still in integer mode).
    int_sum: i64,
    /// Running float sum retained in parallel so a later REAL input can fall
    /// back without losing an integer that overflowed the exact accumulator.
    float_sum: f64,
    /// KBN compensation error term.
    float_err: f64,
    /// Whether we've seen any non-NULL value.
    has_value: bool,
    /// Whether we're in float mode (any REAL-like input).
    is_float: bool,
    /// Whether an integer overflow occurred (error condition).
    overflow: bool,
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Kahan-Babuska-Neumaier compensated summation step matching upstream
/// aggregate precision behavior.
#[inline]
fn kbn_step(sum: &mut f64, err: &mut f64, value: f64) {
    let s = *sum;
    let t = s + value;
    if s.abs() > value.abs() {
        *err += (s - t) + value;
    } else {
        *err += (value - t) + s;
    }
    *sum = t;
}

impl SumAccumulator {
    /// Create a new accumulator.
    pub const fn new() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            float_err: 0.0,
            has_value: false,
            is_float: false,
            overflow: false,
        }
    }

    /// Add a value to the running sum.
    #[allow(clippy::cast_precision_loss)]
    pub fn accumulate(&mut self, val: &SqliteValue) {
        match val.to_sum_numeric_value() {
            SqliteValue::Null | SqliteValue::Text(_) | SqliteValue::Blob(_) => {}
            SqliteValue::Integer(i) => {
                self.has_value = true;
                if !self.is_float && !self.overflow {
                    match self.int_sum.checked_add(i) {
                        Some(result) => self.int_sum = result,
                        None => self.overflow = true,
                    }
                }
                kbn_step(&mut self.float_sum, &mut self.float_err, i as f64);
            }
            SqliteValue::Float(f) => {
                self.has_value = true;
                self.is_float = true;
                kbn_step(&mut self.float_sum, &mut self.float_err, f);
            }
        }
    }

    /// Finalize the sum. Returns `Err` if integer overflow occurred while the
    /// accumulator stayed integer, `Ok(NULL)` if no non-NULL values were seen,
    /// or the sum value.
    pub fn finish(&self) -> Result<SqliteValue, SumOverflowError> {
        if !self.is_float && self.overflow {
            return Err(SumOverflowError);
        }
        if !self.has_value {
            return Ok(SqliteValue::Null);
        }
        if self.is_float {
            Ok(SqliteValue::Float(self.float_sum + self.float_err))
        } else {
            Ok(SqliteValue::Integer(self.int_sum))
        }
    }
}

/// Error returned when `sum()` encounters integer overflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SumOverflowError;

impl fmt::Display for SumOverflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("integer overflow in sum()")
    }
}

impl fmt::Display for SqliteValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(v) => f.write_str(&format_sqlite_float(*v)),
            Self::Text(s) => write!(f, "'{s}'"),
            Self::Blob(b) => {
                f.write_str("X'")?;
                for byte in b.iter() {
                    write!(f, "{byte:02X}")?;
                }
                f.write_str("'")
            }
        }
    }
}

impl PartialEq for SqliteValue {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.partial_cmp(other), Some(Ordering::Equal))
    }
}

impl Eq for SqliteValue {}

impl PartialOrd for SqliteValue {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SqliteValue {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // SQLite sort order: NULL < numeric < text < blob
        let class_a = self.sort_class();
        let class_b = other.sort_class();

        if class_a != class_b {
            return class_a.cmp(&class_b);
        }

        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b).unwrap_or_else(|| a.total_cmp(b)),
            (Self::Integer(a), Self::Float(b)) => int_float_cmp(*a, *b),
            (Self::Float(a), Self::Integer(b)) => int_float_cmp(*b, *a).reverse(),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Blob(a), Self::Blob(b)) => a.cmp(b),
            _ => unreachable!(),
        }
    }
}

impl From<i64> for SqliteValue {
    fn from(i: i64) -> Self {
        Self::Integer(i)
    }
}

impl From<i32> for SqliteValue {
    fn from(i: i32) -> Self {
        Self::Integer(i64::from(i))
    }
}

impl From<f64> for SqliteValue {
    fn from(f: f64) -> Self {
        Self::float_result_or_null(f)
    }
}

impl From<String> for SqliteValue {
    fn from(s: String) -> Self {
        // SmallText stores strings ≤ 23 bytes inline without heap allocation.
        // Longer strings use Arc<str> internally.
        Self::Text(SmallText::from_string(s))
    }
}

impl From<&str> for SqliteValue {
    fn from(s: &str) -> Self {
        Self::Text(SmallText::new(s))
    }
}

impl From<Arc<str>> for SqliteValue {
    fn from(s: Arc<str>) -> Self {
        Self::Text(SmallText::from_arc(s))
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(b: Vec<u8>) -> Self {
        // Arc::from(Vec<u8>) reuses the Vec's heap buffer via
        // Vec → Box<[u8]> → Arc<[u8]>, avoiding a redundant copy.
        Self::Blob(Arc::from(b))
    }
}

impl From<&[u8]> for SqliteValue {
    fn from(b: &[u8]) -> Self {
        Self::Blob(Arc::from(b))
    }
}

impl From<Arc<[u8]>> for SqliteValue {
    fn from(b: Arc<[u8]>) -> Self {
        Self::Blob(b)
    }
}

impl<T: Into<Self>> From<Option<T>> for SqliteValue {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

/// Try to coerce a text string to INTEGER or REAL following SQLite NUMERIC
/// affinity rules. Returns `None` if the text is not a well-formed numeric
/// literal.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::float_cmp
)]
fn try_coerce_text_to_numeric(s: &str) -> Option<SqliteValue> {
    let trimmed = trim_sqlite_ascii_whitespace(s);
    if trimmed.is_empty() {
        return None;
    }
    // Try integer first (preferred for NUMERIC affinity).
    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(SqliteValue::Integer(i));
    }
    // Try float. Reject non-finite results (NaN, Infinity) since SQLite
    // does not recognise "nan", "inf", or "infinity" as numeric literals.
    // However, it does recognize literals like "1e999" which evaluate to Inf.
    if let Ok(f) = trimmed.parse::<f64>() {
        if !f.is_finite() {
            let lower = trimmed.to_ascii_lowercase();
            if lower.contains("inf") || lower.contains("nan") {
                return None;
            }
        }
        // If the float is an exact integer value within bounds, store as integer.
        // Checking bounds prevents incorrect saturation for values >= 2^63.
        if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f) {
            #[allow(clippy::cast_possible_truncation)]
            let i = f as i64;
            #[allow(clippy::cast_precision_loss)]
            if (i as f64) == f {
                return Some(SqliteValue::Integer(i));
            }
        }
        return Some(SqliteValue::Float(f));
    }
    None
}

/// Compare an integer with a float, preserving precision for large i64 values.
///
/// Matches C SQLite's `sqlite3IntFloatCompare` algorithm. The naive
/// `(i as f64).partial_cmp(&r)` loses precision for |i| > 2^53.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub fn int_float_cmp(i: i64, r: f64) -> Ordering {
    if r.is_nan() {
        // SQLite treats NaN as NULL, and all integers are greater than NULL.
        return Ordering::Greater;
    }
    // If r is out of i64 range, the answer is obvious.
    if r < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    if r >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    // Truncate float to integer and compare integer parts.
    let y = r as i64;
    match i.cmp(&y) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        // Integer parts equal — use float comparison as tiebreaker.
        Ordering::Equal => {
            let s = i as f64;
            s.partial_cmp(&r).unwrap_or(Ordering::Equal)
        }
    }
}

/// Format a floating-point value as text matching SQLite's `%!.*g` behavior.
///
/// SQLite 3.52 and newer default REAL-to-TEXT conversion to 17 significant
/// digits through `sqlite3VdbeMemStringify()`. The `!` flag keeps a decimal
/// point so REAL text stays distinct from INTEGER text, for example `120.0`
/// rather than `120`.
#[must_use]
pub fn format_sqlite_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_owned();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            "Inf".to_owned()
        } else {
            "-Inf".to_owned()
        };
    }
    render_sqlite_float_decode(&sqlite_float_decode(f))
}

const SQLITE_FLOAT_SIGNIFICANT_DIGITS: usize = 17;
const SQLITE_FLOAT_MAX_ROUND_DIGITS: usize = 20;
const SQLITE_FLOAT_GENERIC_PRECISION: i32 = 16;
const SQLITE_POWERS_OF_TEN_FIRST: i32 = -348;
const SQLITE_POWERS_OF_TEN_LAST: i32 = 347;

#[derive(Debug)]
struct SqliteFloatDecode {
    digits: Vec<u8>,
    decimal_point: i32,
    negative: bool,
}

fn render_sqlite_float_decode(decoded: &SqliteFloatDecode) -> String {
    let exponent = decoded.decimal_point - 1;
    if !(-4..=SQLITE_FLOAT_GENERIC_PRECISION).contains(&exponent) {
        return render_sqlite_float_exponential(decoded, exponent);
    }
    render_sqlite_float_fixed(decoded, exponent)
}

fn render_sqlite_float_fixed(decoded: &SqliteFloatDecode, exponent: i32) -> String {
    let mut out = String::with_capacity(decoded.digits.len() + 8);
    if decoded.negative {
        out.push('-');
    }

    let mut precision = SQLITE_FLOAT_GENERIC_PRECISION - exponent;
    let mut digit_idx = 0usize;
    let mut e2 = decoded.decimal_point - 1;

    if e2 < 0 {
        out.push('0');
    } else {
        while e2 >= 0 {
            if let Some(&digit) = decoded.digits.get(digit_idx) {
                out.push(char::from(digit));
                digit_idx += 1;
            } else {
                out.push('0');
            }
            e2 -= 1;
        }
    }

    out.push('.');

    if e2 < -1 && precision > 0 {
        let zero_count = (-1 - e2).min(precision);
        for _ in 0..zero_count {
            out.push('0');
        }
        precision -= zero_count;
    }

    if precision > 0 {
        let digits_after_decimal =
            (decoded.digits.len().saturating_sub(digit_idx)).min(precision as usize);
        for &digit in &decoded.digits[digit_idx..digit_idx + digits_after_decimal] {
            out.push(char::from(digit));
        }
    }

    trim_sqlite_float_tail(&mut out);
    out
}

fn render_sqlite_float_exponential(decoded: &SqliteFloatDecode, exponent: i32) -> String {
    let mut out = String::with_capacity(decoded.digits.len() + 8);
    if decoded.negative {
        out.push('-');
    }

    let first = decoded.digits.first().copied().unwrap_or(b'0');
    out.push(char::from(first));
    out.push('.');
    let digits_after_decimal =
        (decoded.digits.len().saturating_sub(1)).min(SQLITE_FLOAT_GENERIC_PRECISION as usize);
    for &digit in decoded.digits.iter().skip(1).take(digits_after_decimal) {
        out.push(char::from(digit));
    }
    trim_sqlite_float_tail(&mut out);

    out.push('e');
    let mut abs_exp = exponent;
    if abs_exp < 0 {
        out.push('-');
        abs_exp = -abs_exp;
    } else {
        out.push('+');
    }
    if abs_exp >= 100 {
        out.push(char::from(b'0' + (abs_exp / 100) as u8));
        abs_exp %= 100;
    }
    out.push(char::from(b'0' + (abs_exp / 10) as u8));
    out.push(char::from(b'0' + (abs_exp % 10) as u8));
    out
}

fn trim_sqlite_float_tail(out: &mut String) {
    while out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.push('0');
    }
}

fn sqlite_float_decode(f: f64) -> SqliteFloatDecode {
    let negative = f < 0.0;
    let r = if negative { -f } else { f };
    if r == 0.0 {
        return SqliteFloatDecode {
            digits: vec![b'0'],
            decimal_point: 1,
            negative: false,
        };
    }

    let bits = r.to_bits();
    let raw_exponent = ((bits >> 52) & 0x7ff) as i32;
    let mut mantissa = bits & 0x000f_ffff_ffff_ffff;
    let binary_exponent = if raw_exponent == 0 {
        let leading = mantissa.leading_zeros();
        mantissa <<= leading;
        -1074 - leading as i32
    } else {
        mantissa = (mantissa << 11) | (1_u64 << 63);
        raw_exponent - 1086
    };

    let (decimal, decimal_exponent) = sqlite_fp2_convert10(mantissa, binary_exponent, 18);
    let mut digits = decimal.to_string().into_bytes();
    let mut digit_count = digits.len();
    let mut decimal_point = digit_count as i32 + decimal_exponent;
    let mut round_at = SQLITE_FLOAT_SIGNIFICANT_DIGITS;

    if round_at < digit_count || digit_count > SQLITE_FLOAT_MAX_ROUND_DIGITS {
        if round_at == SQLITE_FLOAT_SIGNIFICANT_DIGITS {
            round_at = sqlite_adjust_17_digit_rounding(
                r,
                &digits,
                decimal_exponent,
                digit_count,
                decimal_point,
                round_at,
            );
        }
        if digits.get(round_at).copied().unwrap_or(b'0') >= b'5' {
            let mut idx = round_at - 1;
            loop {
                digits[idx] += 1;
                if digits[idx] <= b'9' {
                    break;
                }
                digits[idx] = b'0';
                if idx == 0 {
                    digits.insert(0, b'1');
                    round_at += 1;
                    decimal_point += 1;
                    break;
                }
                idx -= 1;
            }
        }
        digit_count = round_at;
        digits.truncate(digit_count);
    }

    while digit_count > 1 && digits[digit_count - 1] == b'0' {
        digit_count -= 1;
    }
    digits.truncate(digit_count);

    SqliteFloatDecode {
        digits,
        decimal_point,
        negative,
    }
}

fn sqlite_adjust_17_digit_rounding(
    r: f64,
    digits: &[u8],
    decimal_exponent: i32,
    digit_count: usize,
    decimal_point: i32,
    round_at: usize,
) -> usize {
    if digits.len() <= SQLITE_FLOAT_SIGNIFICANT_DIGITS {
        return round_at;
    }

    if digits[15] == b'9' && digits[14] == b'9' {
        let mut keep = 14usize;
        while keep > 0 && digits[keep - 1] == b'9' {
            keep -= 1;
        }
        let candidate = if keep == 0 {
            1
        } else {
            decimal_digits_to_u64(&digits[..keep]) + 1
        };
        if r == sqlite_fp10_convert2(
            candidate,
            decimal_exponent + digit_count as i32 - keep as i32,
        ) {
            return keep + 1;
        }
    } else if decimal_point >= digit_count as i32
        || (digits[15] == b'0' && digits[14] == b'0' && digits[13] == b'0')
    {
        let mut keep = 13usize;
        while keep > 0 && digits[keep - 1] == b'0' {
            keep -= 1;
        }
        if keep > 0 {
            let candidate = decimal_digits_to_u64(&digits[..keep]);
            if r == sqlite_fp10_convert2(
                candidate,
                decimal_exponent + digit_count as i32 - keep as i32,
            ) {
                return keep + 1;
            }
        }
    }

    round_at
}

fn decimal_digits_to_u64(digits: &[u8]) -> u64 {
    digits
        .iter()
        .fold(0_u64, |acc, digit| acc * 10 + u64::from(*digit - b'0'))
}

fn sqlite_fp2_convert10(mantissa: u64, binary_exponent: i32, digits: i32) -> (u64, i32) {
    let power = digits - 1 - pwr2_to_10(binary_exponent + 63);
    let (power_hi, power_lo) = power_of_ten(power);
    let (mut high, _) = sqlite_multiply_128(mantissa, power_hi);
    let _ = power_lo;
    if digits == 18 {
        high >>= -(binary_exponent + pwr10_to_2(power) + 2) as u32;
        (high.wrapping_add((high << 1) & 2) >> 1, -power)
    } else {
        high >>= -(binary_exponent + pwr10_to_2(power) + 1) as u32;
        (high, -power)
    }
}

fn sqlite_fp10_convert2(decimal: u64, power: i32) -> f64 {
    if power < SQLITE_POWERS_OF_TEN_FIRST {
        return 0.0;
    }
    if power > SQLITE_POWERS_OF_TEN_LAST {
        return f64::INFINITY;
    }

    let bit_width = 64 - decimal.leading_zeros() as i32;
    let binary_power = pwr10_to_2(power);
    let mut exponent = 53 - bit_width - binary_power;
    if exponent > 1074 {
        if exponent >= 1130 {
            return 0.0;
        }
        exponent = 1074;
    }

    let shift = -(exponent - (64 - bit_width) + binary_power + 3);
    let shift = shift.clamp(0, 63) as u32;
    let (mut power_hi, mut power_lo) = power_of_ten(power);
    if power_lo != 0 {
        power_hi = power_hi.wrapping_add(1);
        power_lo = !power_lo;
    }

    let shifted_decimal = decimal << (64 - bit_width);
    let (mut high, low) = sqlite_multiply_128(shifted_decimal, power_hi);
    let mid1 = (low >> 32) as u32;
    let mut sticky = 1_u64;
    if (high & low_mask(shift)) == 0 {
        let (mid2_high, _) = sqlite_multiply_128(shifted_decimal, u64::from(power_lo) << 32);
        let mid2 = (mid2_high >> 32) as u32;
        sticky = u64::from(mid1.wrapping_sub(mid2) > 1);
        high = high.wrapping_sub(u64::from(mid1 < mid2));
    }

    let mut rounded = (high >> shift) | sticky;
    let adjust = u32::from(rounded >= (1_u64 << 55) - 2);
    if adjust != 0 {
        rounded = (rounded >> adjust) | (rounded & 1);
        exponent -= adjust as i32;
    }

    let mut bits = (rounded + 1 + ((rounded >> 2) & 1)) >> 2;
    if exponent <= -972 {
        return f64::INFINITY;
    }
    if (bits & (1_u64 << 52)) != 0 {
        bits = (bits & !(1_u64 << 52)) | ((1075 - exponent) as u64) << 52;
    }
    f64::from_bits(bits)
}

fn low_mask(bits: u32) -> u64 {
    if bits == 0 { 0 } else { (1_u64 << bits) - 1 }
}

fn sqlite_multiply_128(left: u64, right: u64) -> (u64, u64) {
    let product = u128::from(left) * u128::from(right);
    ((product >> 64) as u64, product as u64)
}

fn sqlite_multiply_160(high: u64, low: u32, right: u64) -> (u64, u32) {
    let product =
        u128::from(high) * u128::from(right) + ((u128::from(low) * u128::from(right)) >> 32);
    (
        (product >> 64) as u64,
        ((product >> 32) & u128::from(u32::MAX)) as u32,
    )
}

fn pwr10_to_2(power: i32) -> i32 {
    (power * 108_853) >> 15
}

fn pwr2_to_10(power: i32) -> i32 {
    (power * 78_913) >> 18
}

fn power_of_ten(power: i32) -> (u64, u32) {
    const BASE: [u64; 27] = [
        0x8000_0000_0000_0000,
        0xa000_0000_0000_0000,
        0xc800_0000_0000_0000,
        0xfa00_0000_0000_0000,
        0x9c40_0000_0000_0000,
        0xc350_0000_0000_0000,
        0xf424_0000_0000_0000,
        0x9896_8000_0000_0000,
        0xbebc_2000_0000_0000,
        0xee6b_2800_0000_0000,
        0x9502_f900_0000_0000,
        0xba43_b740_0000_0000,
        0xe8d4_a510_0000_0000,
        0x9184_e72a_0000_0000,
        0xb5e6_20f4_8000_0000,
        0xe35f_a931_a000_0000,
        0x8e1b_c9bf_0400_0000,
        0xb1a2_bc2e_c500_0000,
        0xde0b_6b3a_7640_0000,
        0x8ac7_2304_89e8_0000,
        0xad78_ebc5_ac62_0000,
        0xd8d7_26b7_177a_8000,
        0x8786_7832_6eac_9000,
        0xa968_163f_0a57_b400,
        0xd3c2_1bce_cced_a100,
        0x8459_5161_4014_84a0,
        0xa56f_a5b9_9019_a5c8,
    ];
    const SCALE: [u64; 26] = [
        0x8049_a4ac_0c58_11ae,
        0xcf42_894a_5dce_35ea,
        0xa76c_5823_38ed_2621,
        0x873e_4f75_e222_4e68,
        0xda7f_5bf5_9096_6848,
        0xb080_392c_c434_9dec,
        0x8e93_8662_882a_f53e,
        0xe658_29b3_046b_0afa,
        0xba12_1a46_50e4_ddeb,
        0x964e_858c_91ba_2655,
        0xf2d5_6790_ab41_c2a2,
        0xc428_d05a_a475_1e4c,
        0x9e74_d1b7_91e0_7e48,
        0xcccc_cccc_cccc_cccc,
        0xcecb_8f27_f420_0f3a,
        0xa70c_3c40_a64e_6c51,
        0x86f0_ac99_b4e8_dafd,
        0xda01_ee64_1a70_8de9,
        0xb01a_e745_b101_e9e4,
        0x8e41_ade9_fbeb_c27d,
        0xe5d3_ef28_2a24_2e81,
        0xb9a7_4a06_37ce_2ee1,
        0x95f8_3d0a_1fb6_9cd9,
        0xf24a_01a7_3cf2_dccf,
        0xc3b8_3581_09e8_4f07,
        0x9e19_db92_b4e3_1ba9,
    ];
    const SCALE_LO: [u32; 26] = [
        0x205b_896d,
        0x5206_4cad,
        0xaf2a_f2b8,
        0x5a77_44a7,
        0xaf39_a475,
        0xbd8d_794e,
        0x547e_b47b,
        0x0cb4_a5a3,
        0x92f3_4d62,
        0x3a6a_07f9,
        0xfae2_7299,
        0xaa97_e14c,
        0x775e_a265,
        0xcccc_cccc,
        0x0000_0000,
        0x9990_90b6,
        0x69a0_28bb,
        0xe80e_6f48,
        0x5ec0_5dd0,
        0x1458_8f14,
        0x8f16_68c9,
        0x6d95_3e2c,
        0x4abd_af10,
        0xbc63_3b39,
        0x0a86_2f81,
        0x6c07_a2c2,
    ];

    debug_assert!((SQLITE_POWERS_OF_TEN_FIRST..=SQLITE_POWERS_OF_TEN_LAST).contains(&power));

    let (group, offset) = if power < 0 {
        if power == -1 {
            return (SCALE[13], SCALE_LO[13]);
        }
        let mut group = power / 27;
        let mut offset = power % 27;
        if offset != 0 {
            group -= 1;
            offset += 27;
        }
        (group, offset)
    } else if power < 27 {
        return (BASE[power as usize], 0);
    } else {
        (power / 27, power % 27)
    };

    let scale_idx = (group + 13) as usize;
    let mut high = SCALE[scale_idx];
    if offset == 0 {
        return (high, SCALE_LO[scale_idx]);
    }

    let (scaled, mut low) = sqlite_multiply_160(high, SCALE_LO[scale_idx], BASE[offset as usize]);
    high = scaled;
    if (high & (1_u64 << 63)) == 0 {
        high = (high << 1) | u64::from(low >> 31);
        low = (low << 1) | 1;
    }
    (high, low)
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;

    struct ValuePoolTestGuard;

    impl ValuePoolTestGuard {
        fn new() -> Self {
            pool_clear();
            reset_value_pool_test_stats();
            Self
        }
    }

    impl Drop for ValuePoolTestGuard {
        fn drop(&mut self) {
            pool_clear();
            reset_value_pool_test_stats();
        }
    }

    fn log_value_pool_test_stats(test_name: &str) -> ValuePoolStats {
        let stats = value_pool_test_stats_snapshot();
        eprintln!(
            "bead_id=bd-nsvud test={test_name} slab_alloc_count={} slab_return_count={} global_alloc_fallback_count={} slab_high_water_mark={} pool_len={}",
            stats.slab_alloc_count,
            stats.slab_return_count,
            stats.global_alloc_fallback_count,
            stats.slab_high_water_mark,
            pool_len(),
        );
        stats
    }

    #[test]
    fn test_slab_basic_alloc_dealloc() {
        let _guard = ValuePoolTestGuard::new();
        const ROUND_TRIP_COUNT: usize = 100;

        assert_eq!(pool_len(), 0);
        assert_eq!(pool_acquire(), None);
        assert_eq!(
            value_pool_test_stats_snapshot(),
            ValuePoolStats {
                slab_alloc_count: 0,
                slab_return_count: 0,
                global_alloc_fallback_count: 1,
                slab_high_water_mark: 0,
            }
        );

        reset_value_pool_test_stats();
        for value in 0..ROUND_TRIP_COUNT {
            pool_return(SqliteValue::Integer(value as i64));
        }
        assert_eq!(pool_len(), ROUND_TRIP_COUNT);
        assert_eq!(
            value_pool_test_stats_snapshot(),
            ValuePoolStats {
                slab_alloc_count: 0,
                slab_return_count: ROUND_TRIP_COUNT,
                global_alloc_fallback_count: 0,
                slab_high_water_mark: ROUND_TRIP_COUNT,
            }
        );

        reset_value_pool_test_stats();
        for expected in (0..ROUND_TRIP_COUNT).rev() {
            assert_eq!(pool_acquire(), Some(SqliteValue::Integer(expected as i64)));
        }
        assert_eq!(pool_len(), 0);
        assert_eq!(
            log_value_pool_test_stats("test_slab_basic_alloc_dealloc"),
            ValuePoolStats {
                slab_alloc_count: ROUND_TRIP_COUNT,
                slab_return_count: 0,
                global_alloc_fallback_count: 0,
                slab_high_water_mark: 0,
            }
        );
    }

    #[test]
    fn test_slab_exhaustion_fallback() {
        let _guard = ValuePoolTestGuard::new();

        for value in 0..=VALUE_POOL_CAP {
            pool_return(SqliteValue::Integer(value as i64));
        }
        assert_eq!(pool_len(), VALUE_POOL_CAP);
        assert_eq!(
            value_pool_test_stats_snapshot(),
            ValuePoolStats {
                slab_alloc_count: 0,
                slab_return_count: VALUE_POOL_CAP,
                global_alloc_fallback_count: 0,
                slab_high_water_mark: VALUE_POOL_CAP,
            }
        );

        reset_value_pool_test_stats();
        for _ in 0..VALUE_POOL_CAP {
            assert!(pool_acquire().is_some());
        }
        assert_eq!(pool_acquire(), None);
        assert_eq!(pool_len(), 0);
        assert_eq!(
            log_value_pool_test_stats("test_slab_exhaustion_fallback"),
            ValuePoolStats {
                slab_alloc_count: VALUE_POOL_CAP,
                slab_return_count: 0,
                global_alloc_fallback_count: 1,
                slab_high_water_mark: 0,
            }
        );
    }

    #[test]
    fn test_slab_no_leak() {
        let _guard = ValuePoolTestGuard::new();
        const ITERATIONS: usize = 10_000;

        let (weak_tx, weak_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let worker = std::thread::spawn(move || {
            pool_clear();
            reset_value_pool_test_stats();

            let mut pooled_weak = None;
            let mut overflow_weak = None;
            for value in 0..ITERATIONS {
                let payload: Arc<[u8]> =
                    Arc::from(vec![(value % 251) as u8; 64].into_boxed_slice());
                if value == 0 {
                    pooled_weak = Some(Arc::downgrade(&payload));
                } else if value == ITERATIONS - 1 {
                    overflow_weak = Some(Arc::downgrade(&payload));
                }
                pool_return(SqliteValue::Blob(payload));
            }

            assert_eq!(
                pool_len(),
                VALUE_POOL_CAP,
                "the slab must retain at most VALUE_POOL_CAP entries",
            );
            weak_tx
                .send((
                    pooled_weak.expect("capture pooled weak handle"),
                    overflow_weak.expect("capture overflow weak handle"),
                    log_value_pool_test_stats("test_slab_no_leak"),
                ))
                .expect("send slab leak stats");
            release_rx.recv().expect("wait for release");
        });

        let (pooled_weak, overflow_weak, stats) =
            weak_rx.recv().expect("receive weak blob handles");
        assert!(
            pooled_weak.upgrade().is_some(),
            "pooled blob should remain alive while the owning thread is running"
        );
        assert!(
            overflow_weak.upgrade().is_none(),
            "values beyond VALUE_POOL_CAP should fall back to normal drop instead of staying pooled"
        );
        assert_eq!(
            stats,
            ValuePoolStats {
                slab_alloc_count: 0,
                slab_return_count: VALUE_POOL_CAP,
                global_alloc_fallback_count: 0,
                slab_high_water_mark: VALUE_POOL_CAP,
            }
        );

        release_tx.send(()).expect("release worker thread");
        worker.join().expect("join worker");

        assert!(
            pooled_weak.upgrade().is_none(),
            "thread-local slab contents must be dropped when the thread exits"
        );
    }

    #[test]
    fn test_slab_thread_local_isolation() {
        let _guard = ValuePoolTestGuard::new();

        pool_return(SqliteValue::Integer(11));
        assert_eq!(pool_len(), 1);

        let worker = std::thread::spawn(|| {
            pool_clear();
            reset_value_pool_test_stats();

            assert_eq!(pool_len(), 0, "worker thread must start with an empty slab");
            pool_return(SqliteValue::Integer(22));
            assert_eq!(pool_len(), 1);
            assert_eq!(
                value_pool_test_stats_snapshot(),
                ValuePoolStats {
                    slab_alloc_count: 0,
                    slab_return_count: 1,
                    global_alloc_fallback_count: 0,
                    slab_high_water_mark: 1,
                }
            );
            assert_eq!(pool_acquire(), Some(SqliteValue::Integer(22)));
            assert_eq!(pool_len(), 0);
        });
        worker.join().expect("join worker");

        assert_eq!(
            pool_len(),
            1,
            "worker thread slab operations must not affect the caller thread"
        );
        assert_eq!(pool_acquire(), Some(SqliteValue::Integer(11)));
        assert_eq!(pool_len(), 0);
        let stats = log_value_pool_test_stats("test_slab_thread_local_isolation");
        assert_eq!(
            stats,
            ValuePoolStats {
                slab_alloc_count: 1,
                slab_return_count: 1,
                global_alloc_fallback_count: 0,
                slab_high_water_mark: 1,
            }
        );
    }

    #[test]
    fn test_slab_zero_malloc_steady_state() {
        let _guard = ValuePoolTestGuard::new();
        const WARM_POOL_DEPTH: usize = VALUE_POOL_CAP;
        const ITERATIONS: usize = 1_000;
        const INITIAL_TEXT: &str =
            "steady-state pooled string backing store for bd-nsvud warmup payload";
        const REUSED_TEXT: &str = "steady-state pooled overwrite stays in-buffer";

        assert!(
            REUSED_TEXT.len() <= INITIAL_TEXT.len(),
            "steady-state overwrite must fit within the warmed heap allocation"
        );

        for _ in 0..WARM_POOL_DEPTH {
            pool_return(SqliteValue::Text(SmallText::new(INITIAL_TEXT)));
        }
        assert_eq!(pool_len(), WARM_POOL_DEPTH);

        reset_value_pool_test_stats();
        for _ in 0..ITERATIONS {
            let mut reused = pool_acquire().unwrap_or(SqliteValue::Null);
            let SqliteValue::Text(existing) = &mut reused else {
                panic!("warmed slab entry should remain a text value");
            };
            let original_ptr = existing.as_str().as_ptr();
            existing.overwrite(REUSED_TEXT);
            assert_eq!(
                existing.as_str().as_ptr(),
                original_ptr,
                "steady-state overwrite should reuse the warmed heap buffer",
            );
            assert_eq!(existing.as_str(), REUSED_TEXT);
            pool_return(reused);
        }

        assert_eq!(pool_len(), WARM_POOL_DEPTH);
        assert_eq!(
            log_value_pool_test_stats("test_slab_zero_malloc_steady_state"),
            ValuePoolStats {
                slab_alloc_count: ITERATIONS,
                slab_return_count: ITERATIONS,
                global_alloc_fallback_count: 0,
                slab_high_water_mark: WARM_POOL_DEPTH,
            }
        );
    }

    #[test]
    fn test_small_text_heap_clone_lazily_promotes_to_shared_arc() {
        let text = SmallText::new("this string is definitely longer than twenty three bytes");
        let SmallTextRepr::HeapOwned { shared, .. } = &text.repr else {
            panic!("long text should start in heap-owned mode");
        };
        assert!(
            shared.get().is_none(),
            "long text should not allocate Arc eagerly before cloning"
        );

        let cloned = text.clone();

        let SmallTextRepr::HeapOwned { shared, .. } = &text.repr else {
            panic!("original text should remain heap-owned after clone");
        };
        assert!(
            shared.get().is_some(),
            "first clone should materialize a shared Arc lazily"
        );
        assert!(
            matches!(cloned.repr, SmallTextRepr::HeapShared(_)),
            "cloned text should use the shared Arc representation"
        );
        assert_eq!(text.as_str(), cloned.as_str());
    }

    #[test]
    fn test_small_text_overwrite_reuses_unique_heap_buffer() {
        let mut text = SmallText::new("this string is definitely longer than twenty three bytes");
        let (original_ptr, original_capacity) = match &text.repr {
            SmallTextRepr::HeapOwned { text, shared } => {
                assert!(shared.get().is_none(), "fresh heap text should be unshared");
                (text.as_ptr(), text.capacity())
            }
            _ => panic!("long text should start in heap-owned mode"),
        };

        text.overwrite("another long string that still fits the same allocation");

        match &text.repr {
            SmallTextRepr::HeapOwned { text, shared } => {
                assert!(
                    shared.get().is_none(),
                    "overwrite should keep text single-owner"
                );
                assert_eq!(text.as_ptr(), original_ptr);
                assert_eq!(text.capacity(), original_capacity);
                assert_eq!(
                    text.as_str(),
                    "another long string that still fits the same allocation"
                );
            }
            _ => panic!("overwrite should keep long text in heap-owned mode"),
        }
    }

    #[test]
    fn test_small_text_overwrite_detaches_from_shared_arc() {
        let original = "this string is definitely longer than twenty three bytes";
        let mut text = SmallText::new(original);
        let (original_ptr, original_capacity) = match &text.repr {
            SmallTextRepr::HeapOwned { text, .. } => (text.as_ptr(), text.capacity()),
            _ => panic!("long text should start in heap-owned mode"),
        };
        let replacement = "replacement text that must not mutate the shared clone";
        assert!(
            replacement.len() <= original_capacity,
            "replacement should fit the original heap allocation for this regression",
        );
        let clone = text.clone();

        text.overwrite(replacement);

        assert_eq!(
            clone.as_str(),
            original,
            "existing shared clones must keep the original contents"
        );
        assert_eq!(text.as_str(), replacement);
        match &text.repr {
            SmallTextRepr::HeapOwned { text, shared } => {
                assert_eq!(
                    text.as_ptr(),
                    original_ptr,
                    "overwriting a cloned long string should keep the owned buffer",
                );
                assert_eq!(
                    text.capacity(),
                    original_capacity,
                    "detaching from the shared cache should preserve capacity",
                );
                assert!(
                    shared.get().is_none(),
                    "overwrite should reset the lazy shared cache after detaching"
                );
            }
            _ => panic!("overwrite should restore heap-owned mode"),
        }
    }

    #[test]
    fn test_pool_return_reusable_keeps_only_reusable_heap_storage() {
        let _guard = ValuePoolTestGuard::new();

        pool_return_reusable(SqliteValue::Text(SmallText::new("tiny")));
        assert_eq!(
            pool_len(),
            0,
            "inline text should not occupy reusable slab slots",
        );

        let owned_text = SmallText::new("this string is definitely longer than twenty three bytes");
        let _clone = owned_text.clone();
        pool_return_reusable(SqliteValue::Text(owned_text));
        assert_eq!(
            pool_len(),
            1,
            "heap-owned text should stay reusable even after serving shared clones",
        );
        assert!(matches!(pool_acquire(), Some(SqliteValue::Text(_))));
        assert_eq!(pool_len(), 0);

        let shared_text =
            Arc::<str>::from("this string is definitely longer than twenty three bytes");
        pool_return_reusable(SqliteValue::Text(SmallText::from_arc(Arc::clone(
            &shared_text,
        ))));
        assert_eq!(
            pool_len(),
            0,
            "arc-backed shared text should not enter the reusable slab",
        );

        let shared_blob = Arc::<[u8]>::from([0xCA_u8, 0xFE, 0xBA, 0xBE].as_slice());
        pool_return_reusable(SqliteValue::Blob(Arc::clone(&shared_blob)));
        assert_eq!(
            pool_len(),
            0,
            "shared blob allocations should not displace reusable slab entries",
        );

        let unique_blob = Arc::<[u8]>::from([1_u8, 2, 3, 4].as_slice());
        pool_return_reusable(SqliteValue::Blob(unique_blob));
        assert_eq!(
            pool_len(),
            1,
            "unique blob allocations should remain eligible for slab reuse",
        );
    }

    #[test]
    fn test_small_text_concurrent_clone_promotion_keeps_contents_stable() {
        let text = Arc::new(SmallText::new(
            "this string is definitely longer than twenty three bytes",
        ));
        let expected = text.as_str().to_owned();
        let SmallTextRepr::HeapOwned { shared, .. } = &text.repr else {
            panic!("long text should start in heap-owned mode");
        };
        assert!(
            shared.get().is_none(),
            "shared Arc should still be lazy before concurrent clones"
        );

        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let text = Arc::clone(&text);
            let barrier = Arc::clone(&barrier);
            let expected = expected.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..64 {
                    let cloned = (*text).clone();
                    assert_eq!(cloned.as_str(), expected);
                    assert!(
                        matches!(cloned.repr, SmallTextRepr::HeapShared(_)),
                        "concurrent clone should reuse the shared Arc representation"
                    );
                }
            }));
        }

        barrier.wait();
        for worker in workers {
            worker
                .join()
                .expect("join concurrent small-text clone worker");
        }

        let SmallTextRepr::HeapOwned { shared, .. } = &text.repr else {
            panic!("original text should remain heap-owned after clone promotion");
        };
        let shared = shared
            .get()
            .expect("concurrent clones should promote the lazy shared Arc");
        assert_eq!(shared.as_ref(), expected);
        assert_eq!(text.as_str(), expected);
    }

    #[test]
    fn null_properties() {
        let v = SqliteValue::Null;
        assert!(v.is_null());
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
        assert_eq!(v.to_text(), "");
        assert_eq!(v.to_string(), "NULL");
    }

    #[test]
    fn integer_properties() {
        let v = SqliteValue::Integer(42);
        assert!(!v.is_null());
        assert_eq!(v.as_integer(), Some(42));
        assert_eq!(v.to_integer(), 42);
        assert_eq!(v.to_float(), 42.0);
        assert_eq!(v.to_text(), "42");
    }

    #[test]
    fn float_properties() {
        let v = SqliteValue::Float(3.14);
        assert_eq!(v.as_float(), Some(3.14));
        assert_eq!(v.to_integer(), 3);
        assert_eq!(v.to_text(), "3.14");
    }

    #[test]
    fn text_properties() {
        let v = SqliteValue::Text(SmallText::new("hello"));
        assert_eq!(v.as_text(), Some("hello"));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
    }

    #[test]
    fn text_numeric_coercion() {
        let v = SqliteValue::Text(SmallText::new("123"));
        assert_eq!(v.to_integer(), 123);
        assert_eq!(v.to_float(), 123.0);

        let v = SqliteValue::Text(SmallText::new("3.14"));
        assert_eq!(v.to_integer(), 3);
        assert_eq!(v.to_float(), 3.14);
    }

    #[test]
    fn text_numeric_coercion_ignores_hex_text_prefixes() {
        let v = SqliteValue::Text(SmallText::new("0x10"));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);

        let v = SqliteValue::Blob(Arc::from(b"0x10".as_slice()));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
    }

    #[test]
    fn sum_numeric_value_preserves_sqlite_integer_text_boundary() {
        assert_eq!(
            SqliteValue::Text(SmallText::new(" +123 ")).to_sum_numeric_value(),
            SqliteValue::Integer(123)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("\u{00a0}123")).to_sum_numeric_value(),
            SqliteValue::Float(0.0)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("123\u{00a0}")).to_sum_numeric_value(),
            SqliteValue::Float(123.0)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("1.0")).to_sum_numeric_value(),
            SqliteValue::Float(1.0)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("123abc")).to_sum_numeric_value(),
            SqliteValue::Float(123.0)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("")).to_sum_numeric_value(),
            SqliteValue::Float(0.0)
        );
        assert_eq!(
            SqliteValue::Blob(Arc::from(b"123".as_slice())).to_sum_numeric_value(),
            SqliteValue::Float(123.0)
        );
    }

    #[test]
    fn test_integer_numeric_type_uses_sqlite_prefix_rules() {
        assert!(SqliteValue::Text(SmallText::new("123abc")).is_integer_numeric_type());
        assert!(SqliteValue::Blob(Arc::from(b"123a".as_slice())).is_integer_numeric_type());
        assert!(!SqliteValue::Text(SmallText::new("1.5e2abc")).is_integer_numeric_type());
        assert!(!SqliteValue::Text(SmallText::new("abc")).is_integer_numeric_type());
    }

    #[test]
    fn test_sqlite_value_integer_real_comparison_equal() {
        let int_value = SqliteValue::Integer(3);
        let real_value = SqliteValue::Float(3.0);
        assert_eq!(int_value.partial_cmp(&real_value), Some(Ordering::Equal));
        assert_eq!(real_value.partial_cmp(&int_value), Some(Ordering::Equal));
    }

    #[test]
    fn test_sqlite_value_text_to_integer_coercion() {
        let text_value = SqliteValue::Text(SmallText::new("123"));
        let coerced = text_value.apply_affinity(TypeAffinity::Integer);
        assert_eq!(coerced, SqliteValue::Integer(123));
    }

    #[test]
    fn blob_properties() {
        let v = SqliteValue::Blob(Arc::from([0xDE, 0xAD].as_slice()));
        assert_eq!(v.as_blob(), Some(&[0xDE, 0xAD][..]));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
        // to_text() interprets blob bytes as UTF-8 (matching CAST(blob AS TEXT)).
        // 0xDE 0xAD is valid UTF-8 encoding of U+07AD.
        assert_eq!(v.to_text(), "\u{07AD}");
    }

    #[test]
    fn display_formatting() {
        assert_eq!(SqliteValue::Null.to_string(), "NULL");
        assert_eq!(SqliteValue::Integer(42).to_string(), "42");
        assert_eq!(SqliteValue::Integer(-1).to_string(), "-1");
        assert_eq!(SqliteValue::Float(1.5).to_string(), "1.5");
        assert_eq!(SqliteValue::Text(SmallText::new("hi")).to_string(), "'hi'");
        assert_eq!(
            SqliteValue::Blob(Arc::from([0xCA, 0xFE].as_slice())).to_string(),
            "X'CAFE'"
        );
    }

    #[test]
    fn sort_order_null_first() {
        let null = SqliteValue::Null;
        let int = SqliteValue::Integer(0);
        let text = SqliteValue::Text(SmallText::new(""));
        let blob = SqliteValue::Blob(Arc::from(&[] as &[u8]));

        assert!(null < int);
        assert!(int < text);
        assert!(text < blob);
    }

    #[test]
    fn sort_order_integers() {
        let a = SqliteValue::Integer(1);
        let b = SqliteValue::Integer(2);
        assert!(a < b);
        assert_eq!(a.partial_cmp(&a), Some(Ordering::Equal));
    }

    #[test]
    fn sort_order_mixed_numeric() {
        let int = SqliteValue::Integer(1);
        let float = SqliteValue::Float(1.5);
        assert!(int < float);

        let int = SqliteValue::Integer(2);
        assert!(int > float);
    }

    #[test]
    fn test_int_float_precision_at_i64_boundary() {
        // i64::MAX cast to f64 rounds UP to 9223372036854775808.0.
        // The naive (i as f64) comparison would say Equal, but C SQLite
        // correctly reports i64::MAX < 9223372036854775808.0.
        let imax = SqliteValue::Integer(i64::MAX);
        let fmax = SqliteValue::Float(9_223_372_036_854_775_808.0);
        assert_eq!(
            imax.partial_cmp(&fmax),
            Some(Ordering::Less),
            "i64::MAX must be Less than 9223372036854775808.0"
        );

        // Two distinct large integers that map to the same f64.
        let a = SqliteValue::Integer(i64::MAX);
        let b = SqliteValue::Integer(i64::MAX - 1);
        let f = SqliteValue::Float(i64::MAX as f64);
        // a > b, but both should compare consistently vs the float.
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));
        // Both are less than the rounded-up float.
        assert_eq!(a.partial_cmp(&f), Some(Ordering::Less));
        assert_eq!(b.partial_cmp(&f), Some(Ordering::Less));
    }

    #[test]
    fn test_int_float_precision_symmetric() {
        // Float-vs-Integer should be the reverse of Integer-vs-Float.
        let i = SqliteValue::Integer(i64::MAX);
        let f = SqliteValue::Float(9_223_372_036_854_775_808.0);
        assert_eq!(f.partial_cmp(&i), Some(Ordering::Greater));
    }

    #[test]
    fn test_int_float_exact_representation() {
        // For exactly representable values, equality still works.
        let i = SqliteValue::Integer(42);
        let f = SqliteValue::Float(42.0);
        assert_eq!(i.partial_cmp(&f), Some(Ordering::Equal));
        assert_eq!(f.partial_cmp(&i), Some(Ordering::Equal));

        // Integer 3 vs Float 3.5 — Integer is less.
        let i = SqliteValue::Integer(3);
        let f = SqliteValue::Float(3.5);
        assert_eq!(i.partial_cmp(&f), Some(Ordering::Less));
        assert_eq!(f.partial_cmp(&i), Some(Ordering::Greater));
    }

    #[test]
    fn from_conversions() {
        assert_eq!(SqliteValue::from(42i64).as_integer(), Some(42));
        assert_eq!(SqliteValue::from(42i32).as_integer(), Some(42));
        assert_eq!(SqliteValue::from(1.5f64).as_float(), Some(1.5));
        assert_eq!(SqliteValue::from("hello").as_text(), Some("hello"));
        assert_eq!(
            SqliteValue::from(String::from("world")).as_text(),
            Some("world")
        );
        assert_eq!(SqliteValue::from(vec![1u8, 2]).as_blob(), Some(&[1, 2][..]));
        assert!(SqliteValue::from(None::<i64>).is_null());
        assert_eq!(SqliteValue::from(Some(42i64)).as_integer(), Some(42));
    }

    #[test]
    fn affinity() {
        assert_eq!(SqliteValue::Null.affinity(), TypeAffinity::Blob);
        assert_eq!(SqliteValue::Integer(0).affinity(), TypeAffinity::Integer);
        assert_eq!(SqliteValue::Float(0.0).affinity(), TypeAffinity::Real);
        assert_eq!(
            SqliteValue::Text(SmallText::new("")).affinity(),
            TypeAffinity::Text
        );
        assert_eq!(
            SqliteValue::Blob(Arc::from(&[] as &[u8])).affinity(),
            TypeAffinity::Blob
        );
    }

    #[test]
    fn null_equality() {
        // In SQLite, NULL == NULL is false, but for sorting they are equal
        let a = SqliteValue::Null;
        let b = SqliteValue::Null;
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
    }

    // ── bd-13r.1: Type Affinity Advisory + STRICT Enforcement ──

    #[test]
    fn test_storage_class_variants() {
        assert_eq!(SqliteValue::Null.storage_class(), StorageClass::Null);
        assert_eq!(
            SqliteValue::Integer(42).storage_class(),
            StorageClass::Integer
        );
        assert_eq!(SqliteValue::Float(3.14).storage_class(), StorageClass::Real);
        assert_eq!(
            SqliteValue::Text("hi".into()).storage_class(),
            StorageClass::Text
        );
        assert_eq!(
            SqliteValue::Blob(Arc::from([1u8].as_slice())).storage_class(),
            StorageClass::Blob
        );
    }

    #[test]
    fn test_type_affinity_advisory_text_into_integer_ok() {
        // INSERT TEXT "hello" into INTEGER-affinity column: text stays as text
        // (not a well-formed numeric literal).
        let val = SqliteValue::Text("hello".into());
        let coerced = val.apply_affinity(TypeAffinity::Integer);
        assert!(coerced.as_text().is_some());
        assert_eq!(coerced.as_text().unwrap(), "hello");

        // INSERT TEXT "42" into INTEGER-affinity column: coerced to integer.
        let val = SqliteValue::Text("42".into());
        let coerced = val.apply_affinity(TypeAffinity::Integer);
        assert_eq!(coerced.as_integer(), Some(42));
    }

    #[test]
    fn test_type_affinity_advisory_integer_into_text_ok() {
        // INSERT INTEGER 42 into TEXT-affinity column: coerced to text "42".
        let val = SqliteValue::Integer(42);
        let coerced = val.apply_affinity(TypeAffinity::Text);
        assert_eq!(coerced.as_text(), Some("42"));
    }

    #[test]
    fn test_type_affinity_comparison_coercion_matches_oracle() {
        // NUMERIC affinity coerces text "123" to integer.
        let val = SqliteValue::Text("123".into());
        let coerced = val.apply_affinity(TypeAffinity::Numeric);
        assert_eq!(coerced.as_integer(), Some(123));

        // NUMERIC affinity coerces text "3.14" to real.
        let val = SqliteValue::Text("3.14".into());
        let coerced = val.apply_affinity(TypeAffinity::Numeric);
        assert_eq!(coerced.as_float(), Some(3.14));

        // NUMERIC affinity leaves text "hello" as text.
        let val = SqliteValue::Text("hello".into());
        let coerced = val.apply_affinity(TypeAffinity::Numeric);
        assert!(coerced.as_text().is_some());

        // BLOB affinity never converts anything.
        let val = SqliteValue::Integer(42);
        let coerced = val.apply_affinity(TypeAffinity::Blob);
        assert_eq!(coerced.as_integer(), Some(42));

        // INTEGER affinity converts exact-integer floats to integer.
        let val = SqliteValue::Float(5.0);
        let coerced = val.apply_affinity(TypeAffinity::Integer);
        assert_eq!(coerced.as_integer(), Some(5));

        // INTEGER affinity keeps non-exact floats as float.
        let val = SqliteValue::Float(5.5);
        let coerced = val.apply_affinity(TypeAffinity::Integer);
        assert_eq!(coerced.as_float(), Some(5.5));

        // REAL affinity forces integers to float.
        let val = SqliteValue::Integer(7);
        let coerced = val.apply_affinity(TypeAffinity::Real);
        assert_eq!(coerced.as_float(), Some(7.0));

        // REAL affinity coerces text "9" to float 9.0.
        let val = SqliteValue::Text("9".into());
        let coerced = val.apply_affinity(TypeAffinity::Real);
        assert_eq!(coerced.as_float(), Some(9.0));
    }

    #[test]
    fn test_cast_to_numeric_uses_sqlite_cast_rules() {
        assert_eq!(
            SqliteValue::Text(SmallText::new("123abc")).cast_to_numeric(),
            SqliteValue::Integer(123)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("1.5e2abc")).cast_to_numeric(),
            SqliteValue::Integer(150)
        );
        assert_eq!(
            SqliteValue::Text(SmallText::new("abc")).cast_to_numeric(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            SqliteValue::Blob(Arc::from(b"123a".as_slice())).cast_to_numeric(),
            SqliteValue::Integer(123)
        );

        match SqliteValue::Text(SmallText::new("1e999")).cast_to_numeric() {
            SqliteValue::Float(value) => assert!(value.is_infinite() && value.is_sign_positive()),
            other => panic!("expected +inf REAL from NUMERIC cast, got {other:?}"),
        }
    }

    #[test]
    fn test_strict_table_rejects_text_into_integer() {
        let val = SqliteValue::Text("hello".into());
        let result = val.validate_strict(StrictColumnType::Integer);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.expected, StrictColumnType::Integer);
        assert_eq!(err.actual, StorageClass::Text);
    }

    #[test]
    fn test_strict_table_allows_exact_type() {
        // INTEGER into INTEGER column: ok.
        let val = SqliteValue::Integer(42);
        assert!(val.validate_strict(StrictColumnType::Integer).is_ok());

        // REAL into REAL column: ok.
        let val = SqliteValue::Float(3.14);
        assert!(val.validate_strict(StrictColumnType::Real).is_ok());

        // TEXT into TEXT column: ok.
        let val = SqliteValue::Text("hello".into());
        assert!(val.validate_strict(StrictColumnType::Text).is_ok());

        // BLOB into BLOB column: ok.
        let val = SqliteValue::Blob(Arc::from([1u8, 2, 3].as_slice()));
        assert!(val.validate_strict(StrictColumnType::Blob).is_ok());

        // NULL into any STRICT column: ok (nullability enforced separately).
        assert!(
            SqliteValue::Null
                .validate_strict(StrictColumnType::Integer)
                .is_ok()
        );
        assert!(
            SqliteValue::Null
                .validate_strict(StrictColumnType::Text)
                .is_ok()
        );

        // ANY accepts everything.
        let val = SqliteValue::Integer(42);
        assert!(val.validate_strict(StrictColumnType::Any).is_ok());
        let val = SqliteValue::Text("hi".into());
        assert!(val.validate_strict(StrictColumnType::Any).is_ok());
    }

    #[test]
    fn test_strict_real_accepts_integer_with_coercion() {
        // STRICT REAL column accepts INTEGER and coerces to float.
        let val = SqliteValue::Integer(42);
        let result = val.validate_strict(StrictColumnType::Real).unwrap();
        assert_eq!(result.as_float(), Some(42.0));
    }

    #[test]
    fn test_strict_rejects_wrong_storage_classes() {
        // REAL into INTEGER column: rejected.
        assert!(
            SqliteValue::Float(3.14)
                .validate_strict(StrictColumnType::Integer)
                .is_err()
        );

        // BLOB into TEXT column: rejected.
        assert!(
            SqliteValue::Blob(Arc::from([1u8].as_slice()))
                .validate_strict(StrictColumnType::Text)
                .is_err()
        );

        // INTEGER into TEXT column: rejected.
        assert!(
            SqliteValue::Integer(1)
                .validate_strict(StrictColumnType::Text)
                .is_err()
        );

        // TEXT into BLOB column: rejected.
        assert!(
            SqliteValue::Text("x".into())
                .validate_strict(StrictColumnType::Blob)
                .is_err()
        );
    }

    #[test]
    fn test_strict_column_type_parsing() {
        assert_eq!(
            StrictColumnType::from_type_name("INT"),
            Some(StrictColumnType::Integer)
        );
        assert_eq!(
            StrictColumnType::from_type_name("INTEGER"),
            Some(StrictColumnType::Integer)
        );
        assert_eq!(
            StrictColumnType::from_type_name("REAL"),
            Some(StrictColumnType::Real)
        );
        assert_eq!(
            StrictColumnType::from_type_name("TEXT"),
            Some(StrictColumnType::Text)
        );
        assert_eq!(
            StrictColumnType::from_type_name("BLOB"),
            Some(StrictColumnType::Blob)
        );
        assert_eq!(
            StrictColumnType::from_type_name("ANY"),
            Some(StrictColumnType::Any)
        );
        // Invalid type name in STRICT mode.
        assert_eq!(StrictColumnType::from_type_name("VARCHAR(255)"), None);
        assert_eq!(StrictColumnType::from_type_name("NUMERIC"), None);
    }

    #[test]
    fn test_affinity_advisory_never_rejects() {
        // Advisory affinity NEVER rejects a value. All combinations must succeed.
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(42),
            SqliteValue::Float(3.14),
            SqliteValue::Text("hello".into()),
            SqliteValue::Blob(Arc::from([0xDE, 0xAD].as_slice())),
        ];
        let affinities = [
            TypeAffinity::Integer,
            TypeAffinity::Text,
            TypeAffinity::Blob,
            TypeAffinity::Real,
            TypeAffinity::Numeric,
        ];
        for val in &values {
            for aff in &affinities {
                // apply_affinity is infallible - it always returns a value.
                let _ = val.clone().apply_affinity(*aff);
            }
        }
    }

    // ── bd-13r.2: UNIQUE NULL Semantics (NULL != NULL) ──

    #[test]
    fn test_unique_allows_multiple_nulls_single_column() {
        // In UNIQUE columns, NULL != NULL: two NULLs are never duplicates.
        let a = SqliteValue::Null;
        let b = SqliteValue::Null;
        assert!(!a.unique_eq(&b));
    }

    #[test]
    fn test_unique_allows_multiple_nulls_multi_column_partial_null() {
        // UNIQUE(a,b): (NULL,1) and (NULL,1) are NOT duplicates because
        // any NULL component makes the whole key non-duplicate.
        let row_a = [SqliteValue::Null, SqliteValue::Integer(1)];
        let row_b = [SqliteValue::Null, SqliteValue::Integer(1)];
        assert!(!unique_key_duplicates(&row_a, &row_b));

        // UNIQUE(a,b): (1,NULL) and (1,NULL) are NOT duplicates.
        let row_a = [SqliteValue::Integer(1), SqliteValue::Null];
        let row_b = [SqliteValue::Integer(1), SqliteValue::Null];
        assert!(!unique_key_duplicates(&row_a, &row_b));

        // UNIQUE(a,b): (NULL,NULL) and (NULL,NULL) are NOT duplicates.
        let row_a = [SqliteValue::Null, SqliteValue::Null];
        let row_b = [SqliteValue::Null, SqliteValue::Null];
        assert!(!unique_key_duplicates(&row_a, &row_b));
    }

    #[test]
    fn test_unique_rejects_duplicate_non_null() {
        // Two identical non-NULL values ARE duplicates.
        let a = SqliteValue::Integer(42);
        let b = SqliteValue::Integer(42);
        assert!(a.unique_eq(&b));

        // Composite: (1, "hello") and (1, "hello") ARE duplicates.
        let row_a = [SqliteValue::Integer(1), SqliteValue::Text("hello".into())];
        let row_b = [SqliteValue::Integer(1), SqliteValue::Text("hello".into())];
        assert!(unique_key_duplicates(&row_a, &row_b));

        // Different values are NOT duplicates.
        let row_a = [SqliteValue::Integer(1), SqliteValue::Text("hello".into())];
        let row_b = [SqliteValue::Integer(1), SqliteValue::Text("world".into())];
        assert!(!unique_key_duplicates(&row_a, &row_b));
    }

    #[test]
    fn test_unique_null_vs_non_null_distinct() {
        // NULL and a non-NULL value are never duplicates.
        let a = SqliteValue::Null;
        let b = SqliteValue::Integer(1);
        assert!(!a.unique_eq(&b));
        assert!(!b.unique_eq(&a));

        // Composite: (NULL, 1) and (2, 1) are not duplicates (different first element).
        let row_a = [SqliteValue::Null, SqliteValue::Integer(1)];
        let row_b = [SqliteValue::Integer(2), SqliteValue::Integer(1)];
        assert!(!unique_key_duplicates(&row_a, &row_b));
    }

    // ── bd-13r.4: Integer Overflow Semantics (Expr vs sum()) ──

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn test_integer_overflow_promotes_real_expr_add() {
        let max = SqliteValue::Integer(i64::MAX);
        let one = SqliteValue::Integer(1);
        let result = max.sql_add(&one);
        // Overflow promotes to REAL (not integer).
        assert!(result.as_integer().is_none());
        assert!(result.as_float().is_some());
        // The float value is approximately i64::MAX + 1.
        assert!(result.as_float().unwrap() >= i64::MAX as f64);
    }

    #[test]
    fn test_integer_overflow_promotes_real_expr_mul() {
        let max = SqliteValue::Integer(i64::MAX);
        let two = SqliteValue::Integer(2);
        let result = max.sql_mul(&two);
        // Overflow promotes to REAL.
        assert!(result.as_float().is_some());
    }

    #[test]
    fn test_integer_overflow_promotes_real_expr_sub() {
        let min = SqliteValue::Integer(i64::MIN);
        let one = SqliteValue::Integer(1);
        let result = min.sql_sub(&one);
        // Underflow promotes to REAL.
        assert!(result.as_float().is_some());
    }

    #[test]
    fn test_sum_overflow_errors() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Integer(i64::MAX));
        acc.accumulate(&SqliteValue::Integer(1));
        let result = acc.finish();
        assert!(result.is_err());
    }

    #[test]
    fn test_sum_overflow_then_float_returns_real() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Integer(i64::MAX));
        acc.accumulate(&SqliteValue::Integer(1));
        acc.accumulate(&SqliteValue::Float(0.5));
        let result = acc.finish().unwrap();
        assert!(matches!(result, SqliteValue::Float(_)));
    }

    #[test]
    fn test_sum_text_integer_literals_stay_integer() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Text(SmallText::new("1")));
        acc.accumulate(&SqliteValue::Text(SmallText::new("2")));
        let result = acc.finish().unwrap();
        assert_eq!(result.as_integer(), Some(3));
    }

    #[test]
    fn test_sum_non_numeric_text_returns_real_zero() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Text(SmallText::new("abc")));
        let result = acc.finish().unwrap();
        assert_eq!(result.as_float(), Some(0.0));
    }

    #[test]
    fn test_no_overflow_stays_integer() {
        // Non-overflow addition stays INTEGER.
        let a = SqliteValue::Integer(100);
        let b = SqliteValue::Integer(200);
        let result = a.sql_add(&b);
        assert_eq!(result.as_integer(), Some(300));

        // Non-overflow multiplication stays INTEGER.
        let result = SqliteValue::Integer(7).sql_mul(&SqliteValue::Integer(6));
        assert_eq!(result.as_integer(), Some(42));

        // Non-overflow subtraction stays INTEGER.
        let result = SqliteValue::Integer(50).sql_sub(&SqliteValue::Integer(8));
        assert_eq!(result.as_integer(), Some(42));
    }

    #[test]
    fn test_sum_null_only_returns_null() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Null);
        acc.accumulate(&SqliteValue::Null);
        let result = acc.finish().unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_sum_mixed_int_float() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Integer(10));
        acc.accumulate(&SqliteValue::Float(2.5));
        acc.accumulate(&SqliteValue::Integer(3));
        let result = acc.finish().unwrap();
        // Once float is seen, result is float.
        assert_eq!(result.as_float(), Some(15.5));
    }

    #[test]
    fn test_sum_integer_only() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&SqliteValue::Integer(10));
        acc.accumulate(&SqliteValue::Integer(20));
        acc.accumulate(&SqliteValue::Integer(30));
        let result = acc.finish().unwrap();
        assert_eq!(result.as_integer(), Some(60));
    }

    #[test]
    fn test_sql_arithmetic_null_propagation() {
        let n = SqliteValue::Null;
        let i = SqliteValue::Integer(42);
        assert!(n.sql_add(&i).is_null());
        assert!(i.sql_add(&n).is_null());
        assert!(n.sql_sub(&i).is_null());
        assert!(n.sql_mul(&i).is_null());
    }

    #[test]
    fn test_sql_inf_arithmetic_nan_normalized_to_null() {
        // +Inf + (-Inf) is NaN in IEEE-754 and must be normalized to NULL.
        let pos_inf = SqliteValue::Float(f64::INFINITY);
        let neg_inf = SqliteValue::Float(f64::NEG_INFINITY);
        assert!(pos_inf.sql_add(&neg_inf).is_null());

        // +Inf - +Inf is also NaN and must normalize to NULL.
        assert!(pos_inf.sql_sub(&pos_inf).is_null());
    }

    #[test]
    fn test_sql_mul_zero_times_inf_normalized_to_null() {
        // 0 * +Inf is NaN in IEEE-754 and must be normalized to NULL.
        let zero = SqliteValue::Float(0.0);
        let pos_inf = SqliteValue::Float(f64::INFINITY);
        assert!(zero.sql_mul(&pos_inf).is_null());
        assert!(
            SqliteValue::Integer(0).sql_mul(&pos_inf).is_null(),
            "mixed INTEGER/REAL multiplication should preserve NaN-to-NULL semantics"
        );
    }

    #[test]
    fn test_sql_mul_mixed_int_float_stays_real() {
        let left = SqliteValue::Integer(10);
        let right = SqliteValue::Float(0.25);
        assert_eq!(left.sql_mul(&right).as_float(), Some(2.5));
        assert_eq!(right.sql_mul(&left).as_float(), Some(2.5));
    }

    #[test]
    fn test_sql_inf_propagates_when_not_nan() {
        let pos_inf = SqliteValue::Float(f64::INFINITY);
        let one = SqliteValue::Integer(1);
        let add_result = pos_inf.sql_add(&one);
        assert!(
            matches!(add_result, SqliteValue::Float(v) if v.is_infinite() && v.is_sign_positive()),
            "expected +Inf propagation, got {add_result:?}"
        );

        let neg_inf = SqliteValue::Float(f64::NEG_INFINITY);
        let sub_result = neg_inf.sql_sub(&one);
        assert!(
            matches!(sub_result, SqliteValue::Float(v) if v.is_infinite() && v.is_sign_negative()),
            "expected -Inf propagation, got {sub_result:?}"
        );
    }

    #[test]
    fn test_from_f64_nan_normalizes_to_null() {
        let value = SqliteValue::from(f64::NAN);
        assert!(value.is_null());
    }

    #[test]
    fn test_inf_comparisons_against_finite_values() {
        let pos_inf = SqliteValue::Float(f64::INFINITY);
        let neg_inf = SqliteValue::Float(f64::NEG_INFINITY);
        let finite_hi = SqliteValue::Float(1.0e308);
        let finite_lo = SqliteValue::Float(-1.0e308);

        assert_eq!(pos_inf.partial_cmp(&finite_hi), Some(Ordering::Greater));
        assert_eq!(neg_inf.partial_cmp(&finite_lo), Some(Ordering::Less));
    }

    // ── bd-13r.7: Empty String vs NULL Semantics ──

    #[test]
    fn test_empty_string_is_not_null() {
        let empty = SqliteValue::Text(SmallText::new(""));
        // '' IS NULL → false.
        assert!(!empty.is_null());
        // '' IS NOT NULL → true (expressed as !is_null).
        assert!(!empty.is_null());
        // NULL IS NULL → true.
        assert!(SqliteValue::Null.is_null());
    }

    #[test]
    fn test_length_empty_string_zero() {
        let empty = SqliteValue::Text(SmallText::new(""));
        assert_eq!(empty.sql_length(), Some(0));
    }

    #[test]
    fn test_typeof_empty_string_text() {
        let empty = SqliteValue::Text(SmallText::new(""));
        assert_eq!(empty.typeof_str(), "text");
        // NULL has typeof "null".
        assert_eq!(SqliteValue::Null.typeof_str(), "null");
    }

    #[test]
    fn test_empty_string_comparisons() {
        let empty1 = SqliteValue::Text(SmallText::new(""));
        let empty2 = SqliteValue::Text(SmallText::new(""));
        // '' = '' → true.
        assert_eq!(empty1.partial_cmp(&empty2), Some(std::cmp::Ordering::Equal));

        // '' = NULL → NULL (comparison with NULL yields None/unknown).
        // In our PartialOrd, NULL and TEXT are different sort classes,
        // so NULL < TEXT (they are not equal).
        let null = SqliteValue::Null;
        assert_ne!(empty1.partial_cmp(&null), Some(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_typeof_all_variants() {
        assert_eq!(SqliteValue::Null.typeof_str(), "null");
        assert_eq!(SqliteValue::Integer(0).typeof_str(), "integer");
        assert_eq!(SqliteValue::Float(0.0).typeof_str(), "real");
        assert_eq!(SqliteValue::Text("x".into()).typeof_str(), "text");
        assert_eq!(
            SqliteValue::Blob(Arc::from(&[] as &[u8])).typeof_str(),
            "blob"
        );
    }

    #[test]
    fn test_sql_length_all_types() {
        // NULL → NULL (None).
        assert_eq!(SqliteValue::Null.sql_length(), None);
        // TEXT → character count.
        assert_eq!(SqliteValue::Text("hello".into()).sql_length(), Some(5));
        assert_eq!(SqliteValue::Text(SmallText::new("")).sql_length(), Some(0));
        // BLOB → byte count.
        assert_eq!(
            SqliteValue::Blob(Arc::from([1u8, 2, 3].as_slice())).sql_length(),
            Some(3)
        );
        // INTEGER → length of text representation.
        assert_eq!(SqliteValue::Integer(42).sql_length(), Some(2));
        // REAL → length of text representation.
        assert_eq!(SqliteValue::Float(3.14).sql_length(), Some(4)); // "3.14"
    }

    // ── bd-13r.6: LIKE Semantics (ASCII-only case folding) ──

    #[test]
    fn test_like_ascii_case_insensitive() {
        assert!(sql_like("A", "a", None));
        assert!(sql_like("a", "A", None));
        assert!(sql_like("hello", "HELLO", None));
        assert!(sql_like("HELLO", "hello", None));
        assert!(sql_like("HeLLo", "hEllO", None));
    }

    #[test]
    fn test_like_unicode_case_sensitive_without_icu() {
        // Without ICU, Unicode case folding does NOT occur.
        assert!(!sql_like("ä", "Ä", None));
        assert!(!sql_like("Ä", "ä", None));
        // But exact match works.
        assert!(sql_like("ä", "ä", None));
    }

    #[test]
    fn test_like_fast_path_does_not_fold_ascii_punctuation() {
        assert!(!sql_like("[", "{", None));
        assert!(!sql_like("@", "`", None));
    }

    #[test]
    fn test_like_escape_handling() {
        // Escape literal % with backslash.
        assert!(sql_like("100\\%", "100%", Some('\\')));
        assert!(!sql_like("100\\%", "100x", Some('\\')));

        // Escape literal _.
        assert!(sql_like("a\\_b", "a_b", Some('\\')));
        assert!(!sql_like("a\\_b", "axb", Some('\\')));
    }

    #[test]
    fn test_like_wildcards_basic() {
        // % matches zero or more characters.
        assert!(sql_like("%", "", None));
        assert!(sql_like("%", "anything", None));
        assert!(sql_like("a%", "abc", None));
        assert!(sql_like("%c", "abc", None));
        assert!(sql_like("a%c", "abc", None));
        assert!(sql_like("a%c", "aXYZc", None));
        assert!(!sql_like("a%c", "abd", None));

        // _ matches exactly one character.
        assert!(sql_like("_", "x", None));
        assert!(!sql_like("_", "", None));
        assert!(!sql_like("_", "xy", None));
        assert!(sql_like("a_c", "abc", None));
        assert!(!sql_like("a_c", "abbc", None));
    }

    #[test]
    fn test_like_combined_wildcards() {
        assert!(sql_like("%_", "a", None));
        assert!(!sql_like("%_", "", None));
        assert!(sql_like("_%_", "ab", None));
        assert!(!sql_like("_%_", "a", None));
        assert!(sql_like("%a%b%", "xaybz", None));
        assert!(!sql_like("%a%b%", "xyz", None));
    }

    #[test]
    fn test_like_exact_match() {
        assert!(sql_like("hello", "hello", None));
        assert!(!sql_like("hello", "world", None));
        assert!(sql_like("", "", None));
        assert!(!sql_like("a", "", None));
        assert!(!sql_like("", "a", None));
    }

    #[test]
    fn test_like_fast_path_repeated_percent_shapes() {
        assert!(sql_like("ab%%", "ABcd", None));
        assert!(sql_like("%%cd", "abCD", None));
        assert!(sql_like("%%bc%%", "xxBCyy", None));
        assert!(sql_like("%%%%", "anything", None));
    }

    #[test]
    fn test_like_fast_path_preserves_mixed_unicode_and_ascii_semantics() {
        assert!(sql_like("%éL%", "héllo", None));
        assert!(!sql_like("%Él%", "héllo", None));
        assert!(sql_like("Stra%", "straße", None));
    }

    #[test]
    fn test_like_contains_fast_path_handles_overlapping_matches() {
        assert!(sql_like("%ana%", "bananas", None));
        assert!(sql_like("%NAN%", "baNanas", None));
        assert!(!sql_like("%ananasx%", "bananas", None));
    }

    #[test]
    fn test_like_contains_fast_path_preserves_non_ascii_byte_matching() {
        assert!(sql_like("%ß%", "straße", None));
        assert!(!sql_like("%SS%", "straße", None));
    }

    // ── format_sqlite_float ────────────────────────────────────────────

    #[test]
    fn test_format_sqlite_float_whole_number() {
        assert_eq!(format_sqlite_float(120.0), "120.0");
        assert_eq!(format_sqlite_float(0.0), "0.0");
        assert_eq!(format_sqlite_float(-42.0), "-42.0");
        assert_eq!(format_sqlite_float(1.0), "1.0");
    }

    #[test]
    fn test_format_sqlite_float_fractional() {
        assert_eq!(format_sqlite_float(3.14), "3.14");
        assert_eq!(format_sqlite_float(0.5), "0.5");
        assert_eq!(format_sqlite_float(-0.001), "-0.001");
    }

    #[test]
    fn test_format_sqlite_float_special() {
        assert_eq!(format_sqlite_float(f64::NAN), "NaN");
        assert_eq!(format_sqlite_float(f64::INFINITY), "Inf");
        assert_eq!(format_sqlite_float(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn test_format_sqlite_float_negative_zero() {
        // SQLite CAST(... AS TEXT) normalizes both zero signs to "0.0".
        assert_eq!(format_sqlite_float(-0.0), "0.0");
        assert_eq!(format_sqlite_float(0.0), "0.0");
    }

    #[test]
    fn test_format_sqlite_float_matches_sqlite_17_digit_text_contract() {
        assert_eq!(format_sqlite_float(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(format_sqlite_float(1.0 / 3.0), "0.33333333333333332");
        assert_eq!(format_sqlite_float(2.0 / 3.0), "0.66666666666666663");
        assert_eq!(format_sqlite_float(1.5e16), "15000000000000000.0");
        assert_eq!(
            format_sqlite_float(123_456_789_012_345.6),
            "123456789012345.59"
        );
        assert_eq!(format_sqlite_float(1.0e308), "1.0e+308");
        assert_eq!(format_sqlite_float(1.0e-308), "1.0e-308");
        assert_eq!(
            format_sqlite_float(9.223_372_036_854_776e18),
            "9.2233720368547758e+18"
        );
    }

    #[test]
    fn test_float_to_text_includes_decimal_point() {
        let v = SqliteValue::Float(100.0);
        assert_eq!(v.to_text(), "100.0");
        let v = SqliteValue::Float(3.14);
        assert_eq!(v.to_text(), "3.14");
    }

    // ── scan_numeric_prefix ──────────────────────────────────────────

    #[test]
    fn test_scan_numeric_prefix_bare_dot() {
        // A bare "." has no digits — not a numeric prefix.
        assert_eq!(scan_numeric_prefix(b"."), 0);
        assert_eq!(scan_numeric_prefix(b"-."), 0);
        assert_eq!(scan_numeric_prefix(b"+."), 0);
        assert_eq!(scan_numeric_prefix(b"..1"), 0);
    }

    #[test]
    fn test_scan_numeric_prefix_valid() {
        assert_eq!(scan_numeric_prefix(b"123"), 3);
        assert_eq!(scan_numeric_prefix(b"3.14"), 4);
        assert_eq!(scan_numeric_prefix(b".5"), 2);
        assert_eq!(scan_numeric_prefix(b"1e10"), 4);
        assert_eq!(scan_numeric_prefix(b"-42abc"), 3);
        assert_eq!(scan_numeric_prefix(b"+.5x"), 3);
        assert_eq!(scan_numeric_prefix(b"0.0"), 3);
    }

    #[test]
    fn test_scan_numeric_prefix_empty_and_non_numeric() {
        assert_eq!(scan_numeric_prefix(b""), 0);
        assert_eq!(scan_numeric_prefix(b"abc"), 0);
        assert_eq!(scan_numeric_prefix(b"+"), 0);
        assert_eq!(scan_numeric_prefix(b"-"), 0);
    }
}
