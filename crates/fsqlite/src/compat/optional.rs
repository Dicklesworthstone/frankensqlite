//! Optional extension trait, analogous to `rusqlite::OptionalExtension`.

use fsqlite_error::FrankenError;

/// Convert a `QueryReturnedNoRows` error into `Ok(None)`, analogous to
/// `rusqlite::OptionalExtension`.
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::OptionalExtension;
///
/// let maybe_row = conn.query_row("SELECT 1 WHERE 0").optional()?;
/// assert!(maybe_row.is_none());
/// ```
pub trait OptionalExtension<T> {
    /// If the result is `Err(QueryReturnedNoRows)`, convert to `Ok(None)`.
    /// All other errors pass through unchanged.
    fn optional(self) -> Result<Option<T>, FrankenError>;
}

impl<T> OptionalExtension<T> for Result<T, FrankenError> {
    fn optional(self) -> Result<Option<T>, FrankenError> {
        match self {
            Ok(val) => Ok(Some(val)),
            Err(FrankenError::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_becomes_some() {
        let result: Result<i32, FrankenError> = Ok(42);
        assert_eq!(result.optional().unwrap(), Some(42));
    }

    #[test]
    fn no_rows_becomes_none() {
        let result: Result<i32, FrankenError> = Err(FrankenError::QueryReturnedNoRows);
        assert_eq!(result.optional().unwrap(), None);
    }

    #[test]
    fn other_error_passes_through() {
        let result: Result<i32, FrankenError> = Err(FrankenError::DatabaseFull);
        let err = result.optional().unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseFull));
    }
}
