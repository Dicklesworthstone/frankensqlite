//! Typed row extraction, analogous to rusqlite's `row.get::<_, T>(idx)`.

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

use crate::Row;

/// Extract a Rust value from a `SqliteValue`.
///
/// Implementors mirror the types that `rusqlite::types::FromSql` supports.
pub trait FromSqliteValue: Sized {
    /// Try to convert a `SqliteValue` reference into `Self`.
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError>;
}

impl FromSqliteValue for i32 {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Integer(n) => Self::try_from(*n).map_err(|_| FrankenError::TypeMismatch {
                expected: "i32".into(),
                actual: format!("integer({n})"),
            }),
            other => Err(FrankenError::TypeMismatch {
                expected: "integer".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for i64 {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Integer(n) => Ok(*n),
            other => Err(FrankenError::TypeMismatch {
                expected: "integer".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for u32 {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Integer(n) => Self::try_from(*n).map_err(|_| FrankenError::TypeMismatch {
                expected: "u32".into(),
                actual: format!("integer({n})"),
            }),
            other => Err(FrankenError::TypeMismatch {
                expected: "integer".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for u64 {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Integer(n) => Self::try_from(*n).map_err(|_| FrankenError::TypeMismatch {
                expected: "u64".into(),
                actual: format!("integer({n})"),
            }),
            other => Err(FrankenError::TypeMismatch {
                expected: "integer".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for usize {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Integer(n) => Self::try_from(*n).map_err(|_| FrankenError::TypeMismatch {
                expected: "usize".into(),
                actual: format!("integer({n})"),
            }),
            other => Err(FrankenError::TypeMismatch {
                expected: "integer".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for f64 {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Float(f) => Ok(*f),
            SqliteValue::Integer(n) => Ok(*n as Self),
            other => Err(FrankenError::TypeMismatch {
                expected: "real".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for String {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Text(s) => Ok(s.clone()),
            other => Err(FrankenError::TypeMismatch {
                expected: "text".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for Vec<u8> {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Blob(b) => Ok(b.clone()),
            other => Err(FrankenError::TypeMismatch {
                expected: "blob".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl FromSqliteValue for bool {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        match val {
            SqliteValue::Integer(n) => Ok(*n != 0),
            other => Err(FrankenError::TypeMismatch {
                expected: "integer (bool)".into(),
                actual: other.typeof_str().into(),
            }),
        }
    }
}

impl<T: FromSqliteValue> FromSqliteValue for Option<T> {
    fn from_sqlite_value(val: &SqliteValue) -> Result<Self, FrankenError> {
        if val.is_null() {
            Ok(None)
        } else {
            T::from_sqlite_value(val).map(Some)
        }
    }
}

/// Extension trait for typed column access on `Row`.
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::RowExt;
///
/// let row = conn.query_row("SELECT 42, 'hello'")?;
/// let id: i64 = row.get_typed(0)?;
/// let name: String = row.get_typed(1)?;
/// ```
pub trait RowExt {
    /// Get a typed value from the row at the given column index.
    fn get_typed<T: FromSqliteValue>(&self, index: usize) -> Result<T, FrankenError>;
}

impl RowExt for Row {
    fn get_typed<T: FromSqliteValue>(&self, index: usize) -> Result<T, FrankenError> {
        let val = self.get(index).ok_or_else(|| FrankenError::NoSuchColumn {
            name: format!("column index {index}"),
        })?;
        T::from_sqlite_value(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(v: SqliteValue) -> SqliteValue {
        v
    }

    #[test]
    fn i64_from_integer() {
        assert_eq!(
            i64::from_sqlite_value(&val(SqliteValue::Integer(42))).unwrap(),
            42
        );
    }

    #[test]
    fn i32_from_integer() {
        assert_eq!(
            i32::from_sqlite_value(&val(SqliteValue::Integer(42))).unwrap(),
            42
        );
    }

    #[test]
    fn i32_overflow() {
        assert!(i32::from_sqlite_value(&val(SqliteValue::Integer(i64::MAX))).is_err());
    }

    #[test]
    fn u32_from_integer() {
        assert_eq!(
            u32::from_sqlite_value(&val(SqliteValue::Integer(42))).unwrap(),
            42
        );
    }

    #[test]
    fn u64_from_integer() {
        assert_eq!(
            u64::from_sqlite_value(&val(SqliteValue::Integer(42))).unwrap(),
            42
        );
    }

    #[test]
    fn u64_negative_fails() {
        assert!(u64::from_sqlite_value(&val(SqliteValue::Integer(-1))).is_err());
    }

    #[test]
    fn f64_from_float() {
        let v = f64::from_sqlite_value(&val(SqliteValue::Float(3.14))).unwrap();
        assert!((v - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn f64_from_integer_coercion() {
        let v = f64::from_sqlite_value(&val(SqliteValue::Integer(42))).unwrap();
        assert!((v - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn string_from_text() {
        assert_eq!(
            String::from_sqlite_value(&val(SqliteValue::Text("hello".into()))).unwrap(),
            "hello"
        );
    }

    #[test]
    fn bool_from_integer() {
        assert!(bool::from_sqlite_value(&val(SqliteValue::Integer(1))).unwrap());
        assert!(!bool::from_sqlite_value(&val(SqliteValue::Integer(0))).unwrap());
    }

    #[test]
    fn option_none_from_null() {
        let result: Option<i64> =
            Option::<i64>::from_sqlite_value(&val(SqliteValue::Null)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn option_some_from_value() {
        let result: Option<i64> =
            Option::<i64>::from_sqlite_value(&val(SqliteValue::Integer(42))).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn type_mismatch_error() {
        assert!(i64::from_sqlite_value(&val(SqliteValue::Text("not a number".into()))).is_err());
    }
}
