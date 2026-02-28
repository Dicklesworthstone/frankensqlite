//! Parameter helpers compatible with rusqlite's `params!` macro.

use fsqlite_types::value::SqliteValue;

/// Construct a parameter slice from heterogeneous values, analogous to
/// `rusqlite::params!`.
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::params;
///
/// let p = params![42_i64, "hello", 3.14_f64];
/// assert_eq!(p.len(), 3);
/// ```
#[macro_export]
macro_rules! params {
    () => {
        &[] as &[$crate::compat::ParamValue]
    };
    ($($val:expr),+ $(,)?) => {
        &[$($crate::compat::ParamValue::from($val)),+] as &[$crate::compat::ParamValue]
    };
}

/// Wrapper around `SqliteValue` that provides additional `From` impls needed
/// by the `params!` macro (bool, u32, u64, usize, `Option<T>`).
#[derive(Debug, Clone, PartialEq)]
pub struct ParamValue(pub SqliteValue);

impl ParamValue {
    /// Unwrap into the inner `SqliteValue`.
    pub fn into_inner(self) -> SqliteValue {
        self.0
    }

    /// Borrow the inner `SqliteValue`.
    pub fn as_sqlite_value(&self) -> &SqliteValue {
        &self.0
    }
}

// --- Forwarding impls for types that SqliteValue already supports ---

impl From<i64> for ParamValue {
    fn from(v: i64) -> Self {
        Self(SqliteValue::from(v))
    }
}

impl From<i32> for ParamValue {
    fn from(v: i32) -> Self {
        Self(SqliteValue::from(v))
    }
}

impl From<f64> for ParamValue {
    fn from(v: f64) -> Self {
        Self(SqliteValue::from(v))
    }
}

impl From<String> for ParamValue {
    fn from(v: String) -> Self {
        Self(SqliteValue::from(v))
    }
}

impl From<&str> for ParamValue {
    fn from(v: &str) -> Self {
        Self(SqliteValue::from(v))
    }
}

impl From<Vec<u8>> for ParamValue {
    fn from(v: Vec<u8>) -> Self {
        Self(SqliteValue::from(v))
    }
}

impl From<&[u8]> for ParamValue {
    fn from(v: &[u8]) -> Self {
        Self(SqliteValue::from(v))
    }
}

// --- Additional conversions that rusqlite supports but SqliteValue does not ---

impl From<bool> for ParamValue {
    fn from(v: bool) -> Self {
        Self(SqliteValue::Integer(if v { 1 } else { 0 }))
    }
}

impl From<u32> for ParamValue {
    fn from(v: u32) -> Self {
        Self(SqliteValue::Integer(i64::from(v)))
    }
}

impl From<u64> for ParamValue {
    fn from(v: u64) -> Self {
        Self(SqliteValue::Integer(
            i64::try_from(v).unwrap_or(i64::MAX),
        ))
    }
}

impl From<usize> for ParamValue {
    fn from(v: usize) -> Self {
        Self(SqliteValue::Integer(
            i64::try_from(v).unwrap_or(i64::MAX),
        ))
    }
}

impl<T: Into<ParamValue>> From<Option<T>> for ParamValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self(SqliteValue::Null),
        }
    }
}

impl From<SqliteValue> for ParamValue {
    fn from(v: SqliteValue) -> Self {
        Self(v)
    }
}

/// Convert an iterator of values into a `Vec<SqliteValue>`, analogous to
/// `rusqlite::params_from_iter`.
pub fn params_from_iter(
    iter: impl IntoIterator<Item = impl Into<ParamValue>>,
) -> Vec<SqliteValue> {
    iter.into_iter().map(|v| v.into().into_inner()).collect()
}

/// Convert a `&[ParamValue]` slice to `Vec<SqliteValue>` for passing to
/// fsqlite query methods.
pub fn param_slice_to_values(params: &[ParamValue]) -> Vec<SqliteValue> {
    params.iter().map(|p| p.0.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_macro_empty() {
        let p = params![];
        assert!(p.is_empty());
    }

    #[test]
    fn params_macro_mixed_types() {
        let p = params![42_i64, "hello", 3.14_f64, true];
        assert_eq!(p.len(), 4);
        assert_eq!(p[0].0, SqliteValue::Integer(42));
        assert_eq!(p[1].0, SqliteValue::Text("hello".to_owned()));
        assert_eq!(p[2].0, SqliteValue::Float(3.14));
        assert_eq!(p[3].0, SqliteValue::Integer(1));
    }

    #[test]
    fn params_macro_trailing_comma() {
        let p = params![1_i64, 2_i64,];
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn param_value_bool() {
        assert_eq!(ParamValue::from(true).0, SqliteValue::Integer(1));
        assert_eq!(ParamValue::from(false).0, SqliteValue::Integer(0));
    }

    #[test]
    fn param_value_unsigned() {
        assert_eq!(ParamValue::from(42_u32).0, SqliteValue::Integer(42));
        assert_eq!(ParamValue::from(100_u64).0, SqliteValue::Integer(100));
        assert_eq!(ParamValue::from(200_usize).0, SqliteValue::Integer(200));
    }

    #[test]
    fn param_value_option() {
        let some: ParamValue = Some(42_i64).into();
        assert_eq!(some.0, SqliteValue::Integer(42));

        let none: ParamValue = Option::<i64>::None.into();
        assert_eq!(none.0, SqliteValue::Null);
    }

    #[test]
    fn params_from_iter_vec() {
        let values: Vec<i64> = vec![1, 2, 3];
        let result = params_from_iter(values);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], SqliteValue::Integer(1));
    }

    #[test]
    fn param_slice_conversion() {
        let p = params![10_i64, "text"];
        let values = param_slice_to_values(p);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqliteValue::Integer(10));
    }
}
