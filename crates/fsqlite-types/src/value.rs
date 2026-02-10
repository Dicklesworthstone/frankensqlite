use std::cmp::Ordering;
use std::fmt;

use crate::{StorageClass, StrictColumnType, StrictTypeError, TypeAffinity};

/// A dynamically-typed SQLite value.
///
/// Corresponds to C SQLite's `sqlite3_value` / `Mem` type. SQLite has five
/// fundamental storage classes: NULL, INTEGER, REAL, TEXT, and BLOB.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum SqliteValue {
    /// SQL NULL.
    Null,
    /// A 64-bit signed integer.
    Integer(i64),
    /// A 64-bit IEEE 754 floating-point number.
    Float(f64),
    /// A UTF-8 text string.
    Text(String),
    /// A binary large object.
    Blob(Vec<u8>),
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
    /// - NUMERIC affinity: text parsed as integer/real if well-formed.
    /// - INTEGER affinity: like NUMERIC, plus exact-integer reals become integer.
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
                    Self::Text(t)
                }
            },
            TypeAffinity::Numeric => match &self {
                Self::Text(s) => try_coerce_text_to_numeric(s).unwrap_or(self),
                _ => self,
            },
            TypeAffinity::Integer => match &self {
                Self::Text(s) => try_coerce_text_to_numeric(s).unwrap_or(self),
                Self::Float(f) => {
                    let i = *f as i64;
                    if !f.is_nan() && (i as f64) == *f {
                        Self::Integer(i)
                    } else {
                        self
                    }
                }
                _ => self,
            },
            TypeAffinity::Real => match &self {
                Self::Text(s) => try_coerce_text_to_numeric(s)
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
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to extract an integer value.
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract a float value.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Try to extract a text reference.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract a blob reference.
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
    /// - Blob -> 0
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_integer(&self) -> i64 {
        match self {
            Self::Null | Self::Blob(_) => 0,
            Self::Integer(i) => *i,
            Self::Float(f) => *f as i64,
            Self::Text(s) => s.parse::<i64>().unwrap_or_else(|_| {
                // Try parsing as float first, then truncate
                s.parse::<f64>().map_or(0, |f| f as i64)
            }),
        }
    }

    /// Convert to a float following SQLite's type coercion rules.
    ///
    /// - NULL -> 0.0
    /// - Integer -> as f64
    /// - Float -> itself
    /// - Text -> attempt to parse, 0.0 on failure
    /// - Blob -> 0.0
    #[allow(clippy::cast_precision_loss)]
    pub fn to_float(&self) -> f64 {
        match self {
            Self::Null | Self::Blob(_) => 0.0,
            Self::Integer(i) => *i as f64,
            Self::Float(f) => *f,
            Self::Text(s) => s.parse::<f64>().unwrap_or(0.0),
        }
    }

    /// Convert to text following SQLite's type coercion rules.
    pub fn to_text(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => format!("{f}"),
            Self::Text(s) => s.clone(),
            Self::Blob(b) => {
                use std::fmt::Write;
                let mut hex = String::with_capacity(2 + b.len() * 2);
                hex.push_str("X'");
                for byte in b {
                    let _ = write!(hex, "{byte:02X}");
                }
                hex.push('\'');
                hex
            }
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

    /// Add two values following SQLite's overflow semantics.
    ///
    /// - Integer + Integer: checked add; overflows promote to REAL.
    /// - Any REAL operand: float addition.
    /// - NULL propagates (NULL + x = NULL).
    /// - Non-numeric types coerced to numeric first.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sql_add(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Self::Null,
            (Self::Integer(a), Self::Integer(b)) => match a.checked_add(*b) {
                Some(result) => Self::Integer(result),
                None => Self::float_result_or_null(*a as f64 + *b as f64),
            },
            _ => Self::float_result_or_null(self.to_float() + other.to_float()),
        }
    }

    /// Subtract two values following SQLite's overflow semantics.
    ///
    /// Integer - Integer with overflow promotes to REAL.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sql_sub(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Self::Null,
            (Self::Integer(a), Self::Integer(b)) => match a.checked_sub(*b) {
                Some(result) => Self::Integer(result),
                None => Self::float_result_or_null(*a as f64 - *b as f64),
            },
            _ => Self::float_result_or_null(self.to_float() - other.to_float()),
        }
    }

    /// Multiply two values following SQLite's overflow semantics.
    ///
    /// Integer * Integer with overflow promotes to REAL.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sql_mul(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Self::Null,
            (Self::Integer(a), Self::Integer(b)) => match a.checked_mul(*b) {
                Some(result) => Self::Integer(result),
                None => Self::float_result_or_null(*a as f64 * *b as f64),
            },
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
    sql_like_inner(
        &pattern.chars().collect::<Vec<_>>(),
        &text.chars().collect::<Vec<_>>(),
        escape,
        0,
        0,
    )
}

fn sql_like_inner(
    pattern: &[char],
    text: &[char],
    escape: Option<char>,
    pi: usize,
    ti: usize,
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
            if ti >= text.len() || !ascii_ci_eq(pattern[pi], text[ti]) {
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
                    if sql_like_inner(pattern, text, escape, pi, start) {
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
                if ti >= text.len() || !ascii_ci_eq(pc, text[ti]) {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti >= text.len()
}

/// ASCII-only case-insensitive character comparison (SQLite LIKE semantics).
fn ascii_ci_eq(a: char, b: char) -> bool {
    if a == b {
        return true;
    }
    // Only fold ASCII A-Z / a-z.
    a.is_ascii() && b.is_ascii() && a.eq_ignore_ascii_case(&b)
}

/// Accumulator for SQL `sum()` aggregate with SQLite overflow semantics.
///
/// Unlike expression arithmetic (which promotes to REAL on overflow), `sum()`
/// raises an error on integer overflow. This matches C sqlite3 behavior.
#[derive(Debug, Clone)]
pub struct SumAccumulator {
    /// Running integer sum (if still in integer mode).
    int_sum: i64,
    /// Running float sum (if promoted to float mode).
    float_sum: f64,
    /// Whether we've seen any non-NULL value.
    has_value: bool,
    /// Whether we're in float mode (any REAL input or integer overflow).
    is_float: bool,
    /// Whether an integer overflow occurred (error condition).
    overflow: bool,
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SumAccumulator {
    /// Create a new accumulator.
    pub const fn new() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            has_value: false,
            is_float: false,
            overflow: false,
        }
    }

    /// Add a value to the running sum.
    #[allow(clippy::cast_precision_loss)]
    pub fn accumulate(&mut self, val: &SqliteValue) {
        match val {
            SqliteValue::Null => {}
            SqliteValue::Integer(i) => {
                self.has_value = true;
                if self.is_float {
                    self.float_sum += *i as f64;
                } else {
                    match self.int_sum.checked_add(*i) {
                        Some(result) => self.int_sum = result,
                        None => self.overflow = true,
                    }
                }
            }
            SqliteValue::Float(f) => {
                self.has_value = true;
                if !self.is_float {
                    self.float_sum = self.int_sum as f64;
                    self.is_float = true;
                }
                self.float_sum += f;
            }
            other => {
                // TEXT/BLOB coerced to numeric.
                self.has_value = true;
                let n = other.to_float();
                if !self.is_float {
                    self.float_sum = self.int_sum as f64;
                    self.is_float = true;
                }
                self.float_sum += n;
            }
        }
    }

    /// Finalize the sum. Returns `Err` if integer overflow occurred,
    /// `Ok(NULL)` if no non-NULL values were seen, or the sum value.
    pub fn finish(&self) -> Result<SqliteValue, SumOverflowError> {
        if self.overflow {
            return Err(SumOverflowError);
        }
        if !self.has_value {
            return Ok(SqliteValue::Null);
        }
        if self.is_float {
            Ok(SqliteValue::Float(self.float_sum))
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
            Self::Float(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "'{s}'"),
            Self::Blob(b) => {
                f.write_str("X'")?;
                for byte in b {
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

impl PartialOrd for SqliteValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // SQLite sort order: NULL < numeric < text < blob
        let class_a = self.sort_class();
        let class_b = other.sort_class();

        if class_a != class_b {
            return Some(class_a.cmp(&class_b));
        }

        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Integer(a), Self::Integer(b)) => Some(a.cmp(b)),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Float(b)) => Some(int_float_cmp(*a, *b)),
            (Self::Float(a), Self::Integer(b)) => Some(int_float_cmp(*b, *a).reverse()),
            (Self::Text(a), Self::Text(b)) => Some(a.cmp(b)),
            (Self::Blob(a), Self::Blob(b)) => Some(a.cmp(b)),
            _ => None,
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
        Self::Text(s)
    }
}

impl From<&str> for SqliteValue {
    fn from(s: &str) -> Self {
        Self::Text(s.to_owned())
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(b: Vec<u8>) -> Self {
        Self::Blob(b)
    }
}

impl From<&[u8]> for SqliteValue {
    fn from(b: &[u8]) -> Self {
        Self::Blob(b.to_vec())
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
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Try integer first (preferred for NUMERIC affinity).
    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(SqliteValue::Integer(i));
    }
    // Try float. Reject non-finite results (NaN, Infinity) since SQLite
    // does not recognise "nan", "inf", or "infinity" as numeric literals.
    if let Ok(f) = trimmed.parse::<f64>() {
        if !f.is_finite() {
            return None;
        }
        // If the float is an exact integer value, store as integer.
        #[allow(clippy::cast_possible_truncation)]
        let i = f as i64;
        #[allow(clippy::cast_precision_loss)]
        if (i as f64) == f {
            return Some(SqliteValue::Integer(i));
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
fn int_float_cmp(i: i64, r: f64) -> Ordering {
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

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;

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
        let v = SqliteValue::Text("hello".to_owned());
        assert_eq!(v.as_text(), Some("hello"));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
    }

    #[test]
    fn text_numeric_coercion() {
        let v = SqliteValue::Text("123".to_owned());
        assert_eq!(v.to_integer(), 123);
        assert_eq!(v.to_float(), 123.0);

        let v = SqliteValue::Text("3.14".to_owned());
        assert_eq!(v.to_integer(), 3);
        assert_eq!(v.to_float(), 3.14);
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
        let text_value = SqliteValue::Text("123".to_owned());
        let coerced = text_value.apply_affinity(TypeAffinity::Integer);
        assert_eq!(coerced, SqliteValue::Integer(123));
    }

    #[test]
    fn blob_properties() {
        let v = SqliteValue::Blob(vec![0xDE, 0xAD]);
        assert_eq!(v.as_blob(), Some(&[0xDE, 0xAD][..]));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
        assert_eq!(v.to_text(), "X'DEAD'");
    }

    #[test]
    fn display_formatting() {
        assert_eq!(SqliteValue::Null.to_string(), "NULL");
        assert_eq!(SqliteValue::Integer(42).to_string(), "42");
        assert_eq!(SqliteValue::Integer(-1).to_string(), "-1");
        assert_eq!(SqliteValue::Float(1.5).to_string(), "1.5");
        assert_eq!(SqliteValue::Text("hi".to_owned()).to_string(), "'hi'");
        assert_eq!(SqliteValue::Blob(vec![0xCA, 0xFE]).to_string(), "X'CAFE'");
    }

    #[test]
    fn sort_order_null_first() {
        let null = SqliteValue::Null;
        let int = SqliteValue::Integer(0);
        let text = SqliteValue::Text(String::new());
        let blob = SqliteValue::Blob(vec![]);

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
            SqliteValue::Text(String::new()).affinity(),
            TypeAffinity::Text
        );
        assert_eq!(SqliteValue::Blob(vec![]).affinity(), TypeAffinity::Blob);
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
            SqliteValue::Blob(vec![1]).storage_class(),
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
        let val = SqliteValue::Blob(vec![1, 2, 3]);
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
            SqliteValue::Blob(vec![1])
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
            SqliteValue::Blob(vec![0xDE, 0xAD]),
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
        let empty = SqliteValue::Text(String::new());
        // '' IS NULL → false.
        assert!(!empty.is_null());
        // '' IS NOT NULL → true (expressed as !is_null).
        assert!(!empty.is_null());
        // NULL IS NULL → true.
        assert!(SqliteValue::Null.is_null());
    }

    #[test]
    fn test_length_empty_string_zero() {
        let empty = SqliteValue::Text(String::new());
        assert_eq!(empty.sql_length(), Some(0));
    }

    #[test]
    fn test_typeof_empty_string_text() {
        let empty = SqliteValue::Text(String::new());
        assert_eq!(empty.typeof_str(), "text");
        // NULL has typeof "null".
        assert_eq!(SqliteValue::Null.typeof_str(), "null");
    }

    #[test]
    fn test_empty_string_comparisons() {
        let empty1 = SqliteValue::Text(String::new());
        let empty2 = SqliteValue::Text(String::new());
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
        assert_eq!(SqliteValue::Blob(vec![]).typeof_str(), "blob");
    }

    #[test]
    fn test_sql_length_all_types() {
        // NULL → NULL (None).
        assert_eq!(SqliteValue::Null.sql_length(), None);
        // TEXT → character count.
        assert_eq!(SqliteValue::Text("hello".into()).sql_length(), Some(5));
        assert_eq!(SqliteValue::Text(String::new()).sql_length(), Some(0));
        // BLOB → byte count.
        assert_eq!(SqliteValue::Blob(vec![1, 2, 3]).sql_length(), Some(3));
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
}
