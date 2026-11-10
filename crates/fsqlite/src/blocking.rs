//! A synchronous API shaped like [rusqlite](https://docs.rs/rusqlite), so a
//! Rust application on rusqlite can move to FrankenSQLite by changing its
//! imports and keep its database files.
//!
//! ```ignore
//! use fsqlite::blocking::{Connection, OptionalExtension, params};
//!
//! let conn = Connection::open("app.db")?;
//! conn.execute("INSERT INTO person (name, age) VALUES (?1, ?2)", params!["Ada", 36])?;
//! let age: Option<i64> = conn
//!     .query_row("SELECT age FROM person WHERE name = ?1", ["Ada"], |row| row.get(0))
//!     .optional()?;
//! ```
//!
//! Every call drives the engine on a current-thread runtime owned by the
//! calling thread (the same synchronous boundary as the C API), so there is
//! no thread hop per statement. Like the engine's [`crate::Connection`], a
//! [`Connection`] is not `Send`: open it on the thread that uses it, or give
//! each worker thread its own connection. Unlike rusqlite it cannot be moved
//! into another thread or a `Send` connection pool.
//!
//! Covered: opening (`open`, `open_in_memory`, `open_with_flags`), `execute`,
//! `execute_batch`, `prepare`/`prepare_cached`, `query_row`, statements
//! (`execute`, `insert`, `query`, `query_map`, `query_and_then`, `query_row`,
//! `exists`, column and parameter metadata), `Rows`/`Row` with [`FromSql`]
//! by index or column name, positional and named parameters ([`params!`],
//! [`named_params!`], arrays, tuples, slices, [`params_from_iter`]),
//! transactions with [`TransactionBehavior`] and [`DropBehavior`], pragmas,
//! `busy_timeout`, `changes`, `last_insert_rowid`, [`OptionalExtension`], and
//! an [`Error`] whose variants and [`ErrorCode`]s match rusqlite's. Not yet:
//! user-defined functions, savepoint objects, online backup, blob I/O, hooks,
//! and the `chrono`/`uuid`/`serde_json` conversions.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use fsqlite_error::FrankenError;
use fsqlite_parser::{BindParameterSlots, bind_parameter_slots};
use fsqlite_types::SmallText;
use fsqlite_types::value::SqliteValue;

pub use crate::compat::OpenFlags;
pub use ffi::ErrorCode;
pub use types::{FromSql, ToSql};

/// Drive an engine future to completion on this thread's runtime.
fn block_on<F: Future>(future: F) -> F::Output {
    try_block_on(future).expect("fsqlite::blocking used during thread teardown")
}

/// [`block_on`], or `None` once the thread's runtime has been torn down (a
/// connection dropped from another thread-local's destructor).
fn try_block_on<F: Future>(future: F) -> Option<F::Output> {
    thread_local! {
        static RUNTIME: asupersync::runtime::Runtime =
            asupersync::runtime::RuntimeBuilder::current_thread()
                .build()
                .expect("build the fsqlite::blocking runtime");
    }
    RUNTIME.try_with(|runtime| runtime.block_on(future)).ok()
}

/// SQLite result codes, as rusqlite's `ffi` module names them.
pub mod ffi {
    use std::fmt;

    /// The primary result code of a failed call.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[non_exhaustive]
    pub enum ErrorCode {
        InternalMalfunction,
        PermissionDenied,
        OperationAborted,
        DatabaseBusy,
        DatabaseLocked,
        OutOfMemory,
        ReadOnly,
        OperationInterrupted,
        SystemIoFailure,
        DatabaseCorrupt,
        NotFound,
        DiskFull,
        CannotOpen,
        FileLockingProtocolFailed,
        SchemaChanged,
        TooBig,
        ConstraintViolation,
        TypeMismatch,
        ApiMisuse,
        NoLargeFileSupport,
        AuthorizationForStatementDenied,
        ParameterOutOfRange,
        NotADatabase,
        Unknown,
    }

    /// A result code with its extended code, as in `SqliteFailure`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Error {
        pub code: ErrorCode,
        pub extended_code: i32,
    }

    impl Error {
        /// The error for SQLite result code `result_code` (primary or extended).
        #[must_use]
        pub fn new(result_code: i32) -> Self {
            Self {
                code: code_of(result_code & 0xff),
                extended_code: result_code,
            }
        }
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{:?} (extended code {})", self.code, self.extended_code)
        }
    }

    impl std::error::Error for Error {}

    const fn code_of(primary: i32) -> ErrorCode {
        match primary {
            SQLITE_INTERNAL => ErrorCode::InternalMalfunction,
            SQLITE_PERM => ErrorCode::PermissionDenied,
            SQLITE_ABORT => ErrorCode::OperationAborted,
            SQLITE_BUSY => ErrorCode::DatabaseBusy,
            SQLITE_LOCKED => ErrorCode::DatabaseLocked,
            SQLITE_NOMEM => ErrorCode::OutOfMemory,
            SQLITE_READONLY => ErrorCode::ReadOnly,
            SQLITE_INTERRUPT => ErrorCode::OperationInterrupted,
            SQLITE_IOERR => ErrorCode::SystemIoFailure,
            SQLITE_CORRUPT => ErrorCode::DatabaseCorrupt,
            SQLITE_NOTFOUND => ErrorCode::NotFound,
            SQLITE_FULL => ErrorCode::DiskFull,
            SQLITE_CANTOPEN => ErrorCode::CannotOpen,
            SQLITE_PROTOCOL => ErrorCode::FileLockingProtocolFailed,
            SQLITE_SCHEMA => ErrorCode::SchemaChanged,
            SQLITE_TOOBIG => ErrorCode::TooBig,
            SQLITE_CONSTRAINT => ErrorCode::ConstraintViolation,
            SQLITE_MISMATCH => ErrorCode::TypeMismatch,
            SQLITE_MISUSE => ErrorCode::ApiMisuse,
            SQLITE_NOLFS => ErrorCode::NoLargeFileSupport,
            SQLITE_AUTH => ErrorCode::AuthorizationForStatementDenied,
            SQLITE_RANGE => ErrorCode::ParameterOutOfRange,
            SQLITE_NOTADB => ErrorCode::NotADatabase,
            _ => ErrorCode::Unknown,
        }
    }

    pub const SQLITE_OK: i32 = 0;
    pub const SQLITE_ERROR: i32 = 1;
    pub const SQLITE_INTERNAL: i32 = 2;
    pub const SQLITE_PERM: i32 = 3;
    pub const SQLITE_ABORT: i32 = 4;
    pub const SQLITE_BUSY: i32 = 5;
    pub const SQLITE_LOCKED: i32 = 6;
    pub const SQLITE_NOMEM: i32 = 7;
    pub const SQLITE_READONLY: i32 = 8;
    pub const SQLITE_INTERRUPT: i32 = 9;
    pub const SQLITE_IOERR: i32 = 10;
    pub const SQLITE_CORRUPT: i32 = 11;
    pub const SQLITE_NOTFOUND: i32 = 12;
    pub const SQLITE_FULL: i32 = 13;
    pub const SQLITE_CANTOPEN: i32 = 14;
    pub const SQLITE_PROTOCOL: i32 = 15;
    pub const SQLITE_SCHEMA: i32 = 17;
    pub const SQLITE_TOOBIG: i32 = 18;
    pub const SQLITE_CONSTRAINT: i32 = 19;
    pub const SQLITE_MISMATCH: i32 = 20;
    pub const SQLITE_MISUSE: i32 = 21;
    pub const SQLITE_NOLFS: i32 = 22;
    pub const SQLITE_AUTH: i32 = 23;
    pub const SQLITE_RANGE: i32 = 25;
    pub const SQLITE_NOTADB: i32 = 26;
    pub const SQLITE_CONSTRAINT_CHECK: i32 = SQLITE_CONSTRAINT | (1 << 8);
    pub const SQLITE_CONSTRAINT_FOREIGNKEY: i32 = SQLITE_CONSTRAINT | (3 << 8);
    pub const SQLITE_CONSTRAINT_NOTNULL: i32 = SQLITE_CONSTRAINT | (5 << 8);
    pub const SQLITE_CONSTRAINT_PRIMARYKEY: i32 = SQLITE_CONSTRAINT | (6 << 8);
    pub const SQLITE_CONSTRAINT_UNIQUE: i32 = SQLITE_CONSTRAINT | (8 << 8);
    pub const SQLITE_CONSTRAINT_ROWID: i32 = SQLITE_CONSTRAINT | (10 << 8);
}

/// Errors, with the variants of `rusqlite::Error` that applications match.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The engine failed; the message is the engine's.
    SqliteFailure(ffi::Error, Option<String>),
    /// A value could not be converted from its SQL type.
    FromSqlConversionFailure(
        usize,
        types::Type,
        Box<dyn std::error::Error + Send + Sync + 'static>,
    ),
    /// An integer column value does not fit the requested type.
    IntegralValueOutOfRange(usize, i64),
    /// TEXT that is not UTF-8 was read as a string.
    Utf8Error(std::str::Utf8Error),
    /// A named parameter that the statement does not have.
    InvalidParameterName(String),
    /// A path that is not valid UTF-8.
    InvalidPath(PathBuf),
    /// `execute` ran a statement that returned rows.
    ExecuteReturnedResults,
    /// `query_row` found no row.
    QueryReturnedNoRows,
    /// A column index past the last column.
    InvalidColumnIndex(usize),
    /// A column name the statement does not have.
    InvalidColumnName(String),
    /// A column value of a type the requested Rust type does not accept.
    InvalidColumnType(usize, String, types::Type),
    /// `insert` changed a number of rows other than one.
    StatementChangedRows(usize),
    /// A parameter value could not be converted to SQL.
    ToSqlConversionFailure(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// The query is not read-only.
    InvalidQuery,
    /// More than one statement where one was expected.
    MultipleStatement,
    /// The number of parameters given, then the number the statement has.
    InvalidParameterCount(usize, usize),
}

/// `std::result::Result` with this module's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// The engine's error, for [`Error::SqliteFailure`].
    #[must_use]
    pub const fn sqlite_error(&self) -> Option<&ffi::Error> {
        match self {
            Self::SqliteFailure(error, _) => Some(error),
            _ => None,
        }
    }

    /// The primary result code, for [`Error::SqliteFailure`].
    #[must_use]
    pub const fn sqlite_error_code(&self) -> Option<ErrorCode> {
        match self {
            Self::SqliteFailure(error, _) => Some(error.code),
            _ => None,
        }
    }
}

impl From<FrankenError> for Error {
    fn from(error: FrankenError) -> Self {
        match error {
            FrankenError::QueryReturnedNoRows => Self::QueryReturnedNoRows,
            other => Self::SqliteFailure(
                ffi::Error::new(other.extended_error_code()),
                Some(other.to_string()),
            ),
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::Utf8Error(error)
    }
}

impl From<types::FromSqlError> for Error {
    fn from(error: types::FromSqlError) -> Self {
        match error {
            types::FromSqlError::OutOfRange(value) => {
                Self::IntegralValueOutOfRange(UNKNOWN_COLUMN, value)
            }
            types::FromSqlError::Other(source) => {
                Self::FromSqlConversionFailure(UNKNOWN_COLUMN, types::Type::Null, source)
            }
            other => {
                Self::FromSqlConversionFailure(UNKNOWN_COLUMN, types::Type::Null, Box::new(other))
            }
        }
    }
}

const UNKNOWN_COLUMN: usize = usize::MAX;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SqliteFailure(error, None) => write!(f, "{error}"),
            Self::SqliteFailure(_, Some(message)) => f.write_str(message),
            Self::FromSqlConversionFailure(index, ty, error) => {
                if *index == UNKNOWN_COLUMN {
                    write!(f, "Conversion error from type {ty}: {error}")
                } else {
                    write!(
                        f,
                        "Conversion error from type {ty} at index: {index}, {error}"
                    )
                }
            }
            Self::IntegralValueOutOfRange(index, value) => {
                if *index == UNKNOWN_COLUMN {
                    write!(f, "Integer {value} out of range")
                } else {
                    write!(f, "Integer {value} out of range at index {index}")
                }
            }
            Self::Utf8Error(error) => write!(f, "{error}"),
            Self::InvalidParameterName(name) => write!(f, "Invalid parameter name: {name}"),
            Self::InvalidPath(path) => write!(f, "Invalid path: {}", path.to_string_lossy()),
            Self::ExecuteReturnedResults => {
                f.write_str("Execute returned results - did you mean to call query?")
            }
            Self::QueryReturnedNoRows => f.write_str("Query returned no rows"),
            Self::InvalidColumnIndex(index) => write!(f, "Invalid column index: {index}"),
            Self::InvalidColumnName(name) => write!(f, "Invalid column name: {name}"),
            Self::InvalidColumnType(index, name, ty) => {
                write!(
                    f,
                    "Invalid column type {ty} at index: {index}, name: {name}"
                )
            }
            Self::StatementChangedRows(changed) => write!(f, "Query changed {changed} rows"),
            Self::ToSqlConversionFailure(error) => write!(f, "{error}"),
            Self::InvalidQuery => f.write_str("Query is not read-only"),
            Self::MultipleStatement => f.write_str("Multiple statements provided"),
            Self::InvalidParameterCount(given, needed) => write!(
                f,
                "Wrong number of parameters passed to query. Got {given}, needed {needed}"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SqliteFailure(error, _) => Some(error),
            Self::Utf8Error(error) => Some(error),
            Self::FromSqlConversionFailure(_, _, error) | Self::ToSqlConversionFailure(error) => {
                Some(&**error)
            }
            _ => None,
        }
    }
}

/// SQL values and the conversions between them and Rust types.
pub mod types {
    use std::borrow::Cow;
    use std::fmt;
    use std::rc::Rc;
    use std::sync::Arc;

    use super::{Error, Result};

    /// The storage class of a value.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Type {
        Null,
        Integer,
        Real,
        Text,
        Blob,
    }

    impl fmt::Display for Type {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Self::Null => "Null",
                Self::Integer => "Integer",
                Self::Real => "Real",
                Self::Text => "Text",
                Self::Blob => "Blob",
            })
        }
    }

    /// The SQL NULL, as a parameter.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct Null;

    /// An owned SQL value.
    #[derive(Clone, Debug, PartialEq)]
    pub enum Value {
        Null,
        Integer(i64),
        Real(f64),
        Text(String),
        Blob(Vec<u8>),
    }

    impl Value {
        #[must_use]
        pub const fn data_type(&self) -> Type {
            match self {
                Self::Null => Type::Null,
                Self::Integer(_) => Type::Integer,
                Self::Real(_) => Type::Real,
                Self::Text(_) => Type::Text,
                Self::Blob(_) => Type::Blob,
            }
        }
    }

    /// A borrowed SQL value. TEXT is bytes: SQLite does not promise UTF-8.
    #[derive(Copy, Clone, Debug, PartialEq)]
    pub enum ValueRef<'a> {
        Null,
        Integer(i64),
        Real(f64),
        Text(&'a [u8]),
        Blob(&'a [u8]),
    }

    impl<'a> ValueRef<'a> {
        #[must_use]
        pub const fn data_type(&self) -> Type {
            match self {
                ValueRef::Null => Type::Null,
                ValueRef::Integer(_) => Type::Integer,
                ValueRef::Real(_) => Type::Real,
                ValueRef::Text(_) => Type::Text,
                ValueRef::Blob(_) => Type::Blob,
            }
        }

        pub const fn as_i64(&self) -> FromSqlResult<i64> {
            match self {
                ValueRef::Integer(i) => Ok(*i),
                _ => Err(FromSqlError::InvalidType),
            }
        }

        pub const fn as_i64_or_null(&self) -> FromSqlResult<Option<i64>> {
            match self {
                ValueRef::Null => Ok(None),
                ValueRef::Integer(i) => Ok(Some(*i)),
                _ => Err(FromSqlError::InvalidType),
            }
        }

        pub const fn as_f64(&self) -> FromSqlResult<f64> {
            match self {
                ValueRef::Real(f) => Ok(*f),
                _ => Err(FromSqlError::InvalidType),
            }
        }

        pub const fn as_f64_or_null(&self) -> FromSqlResult<Option<f64>> {
            match self {
                ValueRef::Null => Ok(None),
                ValueRef::Real(f) => Ok(Some(*f)),
                _ => Err(FromSqlError::InvalidType),
            }
        }

        pub fn as_str(&self) -> FromSqlResult<&'a str> {
            match *self {
                ValueRef::Text(bytes) => {
                    std::str::from_utf8(bytes).map_err(|error| FromSqlError::Other(Box::new(error)))
                }
                _ => Err(FromSqlError::InvalidType),
            }
        }

        pub fn as_str_or_null(&self) -> FromSqlResult<Option<&'a str>> {
            match *self {
                ValueRef::Null => Ok(None),
                _ => self.as_str().map(Some),
            }
        }

        pub const fn as_blob(&self) -> FromSqlResult<&'a [u8]> {
            match *self {
                ValueRef::Blob(bytes) => Ok(bytes),
                _ => Err(FromSqlError::InvalidType),
            }
        }

        pub const fn as_blob_or_null(&self) -> FromSqlResult<Option<&'a [u8]>> {
            match *self {
                ValueRef::Null => Ok(None),
                ValueRef::Blob(bytes) => Ok(Some(bytes)),
                _ => Err(FromSqlError::InvalidType),
            }
        }

        /// The bytes of TEXT or a BLOB.
        pub const fn as_bytes(&self) -> FromSqlResult<&'a [u8]> {
            match *self {
                ValueRef::Text(bytes) | ValueRef::Blob(bytes) => Ok(bytes),
                _ => Err(FromSqlError::InvalidType),
            }
        }
    }

    impl From<ValueRef<'_>> for Value {
        fn from(value: ValueRef<'_>) -> Self {
            match value {
                ValueRef::Null => Self::Null,
                ValueRef::Integer(i) => Self::Integer(i),
                ValueRef::Real(f) => Self::Real(f),
                ValueRef::Text(bytes) => Self::Text(String::from_utf8_lossy(bytes).into_owned()),
                ValueRef::Blob(bytes) => Self::Blob(bytes.to_vec()),
            }
        }
    }

    impl<'a> From<&'a Value> for ValueRef<'a> {
        fn from(value: &'a Value) -> Self {
            match value {
                Value::Null => ValueRef::Null,
                Value::Integer(i) => ValueRef::Integer(*i),
                Value::Real(f) => ValueRef::Real(*f),
                Value::Text(text) => ValueRef::Text(text.as_bytes()),
                Value::Blob(bytes) => ValueRef::Blob(bytes),
            }
        }
    }

    impl<'a> From<&'a str> for ValueRef<'a> {
        fn from(text: &'a str) -> Self {
            ValueRef::Text(text.as_bytes())
        }
    }

    impl<'a> From<&'a [u8]> for ValueRef<'a> {
        fn from(bytes: &'a [u8]) -> Self {
            ValueRef::Blob(bytes)
        }
    }

    macro_rules! value_from {
        ($($ty:ty => |$v:ident| $e:expr),+ $(,)?) => {$(
            impl From<$ty> for Value {
                fn from($v: $ty) -> Self {
                    $e
                }
            }
        )+};
    }

    value_from! {
        Null => |_v| Self::Null,
        bool => |v| Self::Integer(i64::from(v)),
        i8 => |v| Self::Integer(i64::from(v)),
        i16 => |v| Self::Integer(i64::from(v)),
        i32 => |v| Self::Integer(i64::from(v)),
        i64 => |v| Self::Integer(v),
        isize => |v| Self::Integer(v as i64),
        u8 => |v| Self::Integer(i64::from(v)),
        u16 => |v| Self::Integer(i64::from(v)),
        u32 => |v| Self::Integer(i64::from(v)),
        f32 => |v| Self::Real(f64::from(v)),
        f64 => |v| Self::Real(v),
        String => |v| Self::Text(v),
        Vec<u8> => |v| Self::Blob(v),
    }

    impl<T: Into<Self>> From<Option<T>> for Value {
        fn from(value: Option<T>) -> Self {
            value.map_or(Self::Null, Into::into)
        }
    }

    /// What a [`ToSql`] value binds as.
    #[derive(Clone, Debug, PartialEq)]
    #[non_exhaustive]
    pub enum ToSqlOutput<'a> {
        Borrowed(ValueRef<'a>),
        Owned(Value),
    }

    impl<'a, T: ?Sized> From<&'a T> for ToSqlOutput<'a>
    where
        &'a T: Into<ValueRef<'a>>,
    {
        fn from(value: &'a T) -> Self {
            ToSqlOutput::Borrowed(value.into())
        }
    }

    impl<'a> From<ValueRef<'a>> for ToSqlOutput<'a> {
        fn from(value: ValueRef<'a>) -> Self {
            ToSqlOutput::Borrowed(value)
        }
    }

    macro_rules! to_sql_output_from_value {
        ($($ty:ty),+ $(,)?) => {$(
            impl From<$ty> for ToSqlOutput<'_> {
                fn from(value: $ty) -> Self {
                    ToSqlOutput::Owned(value.into())
                }
            }
        )+};
    }

    impl From<Value> for ToSqlOutput<'_> {
        fn from(value: Value) -> Self {
            ToSqlOutput::Owned(value)
        }
    }

    to_sql_output_from_value!(
        Null,
        bool,
        i8,
        i16,
        i32,
        i64,
        isize,
        u8,
        u16,
        u32,
        f32,
        f64,
        String,
        Vec<u8>
    );

    /// A Rust value that binds as a SQL parameter.
    pub trait ToSql {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>>;
    }

    impl ToSql for ToSqlOutput<'_> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            Ok(match self {
                ToSqlOutput::Borrowed(value) => ToSqlOutput::Borrowed(*value),
                ToSqlOutput::Owned(value) => ToSqlOutput::Borrowed(ValueRef::from(value)),
            })
        }
    }

    impl<T: ToSql + ?Sized> ToSql for &T {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            (**self).to_sql()
        }
    }

    impl<T: ToSql + ?Sized> ToSql for Box<T> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            (**self).to_sql()
        }
    }

    impl<T: ToSql + ?Sized> ToSql for Rc<T> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            (**self).to_sql()
        }
    }

    impl<T: ToSql + ?Sized> ToSql for Arc<T> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            (**self).to_sql()
        }
    }

    impl<T: ToSql + ToOwned + ?Sized> ToSql for Cow<'_, T> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            (**self).to_sql()
        }
    }

    impl<T: ToSql> ToSql for Option<T> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            match self {
                None => Ok(ToSqlOutput::Borrowed(ValueRef::Null)),
                Some(value) => value.to_sql(),
            }
        }
    }

    macro_rules! to_sql_owned {
        ($($ty:ty),+ $(,)?) => {$(
            impl ToSql for $ty {
                fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                    Ok(ToSqlOutput::from(*self))
                }
            }
        )+};
    }

    to_sql_owned!(Null, bool, i8, i16, i32, i64, isize, u8, u16, u32, f32, f64);

    macro_rules! to_sql_checked_unsigned {
        ($($ty:ty),+ $(,)?) => {$(
            impl ToSql for $ty {
                fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                    i64::try_from(*self)
                        .map(|value| ToSqlOutput::Owned(Value::Integer(value)))
                        .map_err(|error| Error::ToSqlConversionFailure(Box::new(error)))
                }
            }
        )+};
    }

    to_sql_checked_unsigned!(u64, usize);

    impl ToSql for str {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::Borrowed(ValueRef::Text(self.as_bytes())))
        }
    }

    impl ToSql for String {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            self.as_str().to_sql()
        }
    }

    impl ToSql for [u8] {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self)))
        }
    }

    impl ToSql for Vec<u8> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            self.as_slice().to_sql()
        }
    }

    impl<const N: usize> ToSql for [u8; N] {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            self.as_slice().to_sql()
        }
    }

    impl ToSql for Value {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::Borrowed(ValueRef::from(self)))
        }
    }

    impl ToSql for ValueRef<'_> {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::Borrowed(*self))
        }
    }

    /// Why a column value could not become the requested Rust type.
    #[derive(Debug)]
    #[non_exhaustive]
    pub enum FromSqlError {
        InvalidType,
        OutOfRange(i64),
        InvalidBlobSize {
            expected_size: usize,
            blob_size: usize,
        },
        Other(Box<dyn std::error::Error + Send + Sync + 'static>),
    }

    impl PartialEq for FromSqlError {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::InvalidType, Self::InvalidType) => true,
                (Self::OutOfRange(a), Self::OutOfRange(b)) => a == b,
                (
                    Self::InvalidBlobSize {
                        expected_size: a,
                        blob_size: b,
                    },
                    Self::InvalidBlobSize {
                        expected_size: c,
                        blob_size: d,
                    },
                ) => a == c && b == d,
                _ => false,
            }
        }
    }

    impl fmt::Display for FromSqlError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::InvalidType => f.write_str("Invalid type"),
                Self::OutOfRange(value) => write!(f, "Value {value} out of range"),
                Self::InvalidBlobSize {
                    expected_size,
                    blob_size,
                } => write!(
                    f,
                    "Cannot read {expected_size} byte value out of {blob_size} byte blob"
                ),
                Self::Other(error) => write!(f, "{error}"),
            }
        }
    }

    impl std::error::Error for FromSqlError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Other(error) => Some(&**error),
                _ => None,
            }
        }
    }

    pub type FromSqlResult<T> = std::result::Result<T, FromSqlError>;

    /// A Rust type a column value converts into.
    pub trait FromSql: Sized {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self>;
    }

    macro_rules! from_sql_integral {
        ($($ty:ty),+ $(,)?) => {$(
            impl FromSql for $ty {
                fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                    let wide = value.as_i64()?;
                    <$ty>::try_from(wide).map_err(|_| FromSqlError::OutOfRange(wide))
                }
            }
        )+};
    }

    from_sql_integral!(i8, i16, i32, isize, u8, u16, u32, u64, usize);

    impl FromSql for i64 {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_i64()
        }
    }

    impl FromSql for f64 {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            match value {
                ValueRef::Integer(i) => Ok(i as Self),
                ValueRef::Real(f) => Ok(f),
                _ => Err(FromSqlError::InvalidType),
            }
        }
    }

    impl FromSql for f32 {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            match value {
                ValueRef::Integer(i) => Ok(i as Self),
                ValueRef::Real(f) => Ok(f as Self),
                _ => Err(FromSqlError::InvalidType),
            }
        }
    }

    impl FromSql for bool {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_i64().map(|i| i != 0)
        }
    }

    impl FromSql for String {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_str().map(ToOwned::to_owned)
        }
    }

    impl FromSql for Box<str> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_str().map(Into::into)
        }
    }

    impl FromSql for Rc<str> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_str().map(Into::into)
        }
    }

    impl FromSql for Arc<str> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_str().map(Into::into)
        }
    }

    impl FromSql for Vec<u8> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_blob().map(<[u8]>::to_vec)
        }
    }

    impl FromSql for Box<[u8]> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_blob().map(Into::into)
        }
    }

    impl<const N: usize> FromSql for [u8; N] {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            let bytes = value.as_blob()?;
            bytes.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
                expected_size: N,
                blob_size: bytes.len(),
            })
        }
    }

    impl<T: FromSql> FromSql for Option<T> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            match value {
                ValueRef::Null => Ok(None),
                _ => T::column_result(value).map(Some),
            }
        }
    }

    impl FromSql for Value {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            Ok(value.into())
        }
    }
}

use types::{ToSqlOutput, ValueRef};

fn engine_value(output: ToSqlOutput<'_>) -> SqliteValue {
    match output {
        ToSqlOutput::Borrowed(value) => match value {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(i) => SqliteValue::Integer(i),
            ValueRef::Real(f) => SqliteValue::Float(f),
            ValueRef::Text(bytes) => SqliteValue::Text(SmallText::from_bytes(bytes)),
            ValueRef::Blob(bytes) => SqliteValue::from(bytes),
        },
        ToSqlOutput::Owned(value) => match value {
            types::Value::Null => SqliteValue::Null,
            types::Value::Integer(i) => SqliteValue::Integer(i),
            types::Value::Real(f) => SqliteValue::Float(f),
            types::Value::Text(text) => SqliteValue::from(text),
            types::Value::Blob(bytes) => SqliteValue::from(bytes),
        },
    }
}

fn bind_one(value: &dyn ToSql) -> Result<SqliteValue> {
    value.to_sql().map(engine_value)
}

fn value_ref(value: &SqliteValue) -> ValueRef<'_> {
    match value {
        SqliteValue::Null => ValueRef::Null,
        SqliteValue::Integer(i) => ValueRef::Integer(*i),
        SqliteValue::Float(f) => ValueRef::Real(*f),
        SqliteValue::Text(text) => ValueRef::Text(text.as_bytes_direct()),
        SqliteValue::Blob(bytes) => ValueRef::Blob(bytes),
    }
}

/// Parameters for a statement, positional or named.
///
/// Positional: `[]`, arrays, tuples, slices of `&dyn ToSql`, [`params!`],
/// [`params_from_iter`]. Named: slices of `(&str, &dyn ToSql)`,
/// [`named_params!`].
pub trait Params {
    #[doc(hidden)]
    fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>>;
}

fn bind_positional<'v>(
    slots: &BindParameterSlots,
    values: impl ExactSizeIterator<Item = &'v (dyn ToSql + 'v)>,
) -> Result<Vec<SqliteValue>> {
    if values.len() != slots.count {
        return Err(Error::InvalidParameterCount(values.len(), slots.count));
    }
    values.map(bind_one).collect()
}

fn bind_named<'v>(
    slots: &BindParameterSlots,
    pairs: impl Iterator<Item = (&'v str, &'v (dyn ToSql + 'v))>,
) -> Result<Vec<SqliteValue>> {
    let mut bound = vec![SqliteValue::Null; slots.count];
    for (name, value) in pairs {
        let index = slots
            .index_of(name)
            .ok_or_else(|| Error::InvalidParameterName(name.to_owned()))?;
        bound[index - 1] = bind_one(value)?;
    }
    Ok(bound)
}

impl Params for [&(dyn ToSql + Send + Sync); 0] {
    fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
        bind_positional(slots, std::iter::empty())
    }
}

impl Params for () {
    fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
        bind_positional(slots, std::iter::empty())
    }
}

impl Params for &[&dyn ToSql] {
    fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
        bind_positional(slots, self.iter().copied())
    }
}

impl Params for &[(&str, &dyn ToSql)] {
    fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
        bind_named(slots, self.iter().map(|(name, value)| (*name, *value)))
    }
}

macro_rules! params_for_arrays {
    ($($n:literal)+) => {$(
        impl<T: ToSql> Params for [T; $n] {
            fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
                bind_positional(slots, self.iter().map(|value| value as &dyn ToSql))
            }
        }
        impl<T: ToSql + ?Sized> Params for &[&T; $n] {
            fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
                bind_positional(slots, self.iter().map(|value| value as &dyn ToSql))
            }
        }
        impl<T: ToSql + ?Sized> Params for &[(&str, &T); $n] {
            fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
                bind_named(slots, self.iter().map(|(name, value)| (*name, value as &dyn ToSql)))
            }
        }
    )+};
}

params_for_arrays!(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32);

macro_rules! params_for_tuples {
    ($(($($field:tt $ty:ident),+))+) => {$(
        impl<$($ty: ToSql),+> Params for ($($ty,)+) {
            fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
                let values: &[&dyn ToSql] = &[$(&self.$field),+];
                bind_positional(slots, values.iter().copied())
            }
        }
    )+};
}

params_for_tuples! {
    (0 A)
    (0 A, 1 B)
    (0 A, 1 B, 2 C)
    (0 A, 1 B, 2 C, 3 D)
    (0 A, 1 B, 2 C, 3 D, 4 E)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M, 13 N)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M, 13 N, 14 O)
    (0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M, 13 N, 14 O, 15 P)
}

/// Positional parameters from an iterator, see [`params_from_iter`].
#[derive(Clone, Debug)]
pub struct ParamsFromIter<I>(I);

/// Bind the items of `iter` as positional parameters, in order.
pub fn params_from_iter<I>(iter: I) -> ParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
    ParamsFromIter(iter)
}

impl<I> Params for ParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
    fn bind_to(self, slots: &BindParameterSlots) -> Result<Vec<SqliteValue>> {
        let bound = self
            .0
            .into_iter()
            .map(|value| bind_one(&value))
            .collect::<Result<Vec<_>>>()?;
        if bound.len() != slots.count {
            return Err(Error::InvalidParameterCount(bound.len(), slots.count));
        }
        Ok(bound)
    }
}

/// Positional parameters, as rusqlite's `params!`:
/// `conn.execute("INSERT INTO t VALUES (?1, ?2)", params![id, name])`.
#[doc(hidden)]
#[macro_export]
macro_rules! __fsqlite_blocking_params {
    () => {
        &[] as &[&dyn $crate::blocking::ToSql]
    };
    ($($param:expr),+ $(,)?) => {
        &[$(&$param as &dyn $crate::blocking::ToSql),+] as &[&dyn $crate::blocking::ToSql]
    };
}

/// Named parameters, as rusqlite's `named_params!`:
/// `stmt.execute(named_params! { ":id": id, ":name": name })`.
#[doc(hidden)]
#[macro_export]
macro_rules! __fsqlite_blocking_named_params {
    () => {
        &[] as &[(&str, &dyn $crate::blocking::ToSql)]
    };
    ($($name:literal: $value:expr),+ $(,)?) => {
        &[$(($name, &$value as &dyn $crate::blocking::ToSql)),+]
            as &[(&str, &dyn $crate::blocking::ToSql)]
    };
}

pub use crate::__fsqlite_blocking_named_params as named_params;
pub use crate::__fsqlite_blocking_params as params;

/// A schema name for pragma helpers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseName<'a> {
    Main,
    Temp,
    Attached(&'a str),
}

pub const MAIN_DB: DatabaseName<'static> = DatabaseName::Main;
pub const TEMP_DB: DatabaseName<'static> = DatabaseName::Temp;

impl fmt::Display for DatabaseName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Main => f.write_str("main"),
            Self::Temp => f.write_str("temp"),
            Self::Attached(name) => f.write_str(&quote_identifier(name)),
        }
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A parameter value as SQL literal text, for `PRAGMA name = value`.
fn sql_literal(value: &dyn ToSql) -> Result<String> {
    use std::fmt::Write as _;
    Ok(match bind_one(value)? {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(i) => i.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(text) => {
            format!(
                "'{}'",
                String::from_utf8_lossy(text.as_bytes_direct()).replace('\'', "''")
            )
        }
        SqliteValue::Blob(bytes) => {
            let mut literal = String::with_capacity(bytes.len() * 2 + 3);
            literal.push_str("x'");
            for byte in bytes.iter() {
                let _ = write!(literal, "{byte:02x}");
            }
            literal.push('\'');
            literal
        }
    })
}

/// Entries kept in a connection's parameter-layout cache before it is reset.
const PARAMETER_CACHE_LIMIT: usize = 512;

/// A connection to a database file or an in-memory database.
pub struct Connection {
    inner: Option<crate::Connection>,
    parameter_slots: RefCell<HashMap<Box<str>, Rc<BindParameterSlots>>>,
    no_parameters: Rc<BindParameterSlots>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("path", &self.path())
            .finish_non_exhaustive()
    }
}

impl Connection {
    fn wrap(inner: crate::Connection) -> Self {
        Self {
            inner: Some(inner),
            parameter_slots: RefCell::new(HashMap::new()),
            no_parameters: Rc::new(BindParameterSlots::default()),
        }
    }

    fn path_text(path: &Path) -> Result<&str> {
        path.to_str()
            .ok_or_else(|| Error::InvalidPath(path.to_path_buf()))
    }

    /// Open (and create if missing) the database at `path`; `":memory:"`
    /// opens a private in-memory database.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = Self::path_text(path.as_ref())?;
        Ok(Self::wrap(block_on(crate::Connection::open(path))?))
    }

    /// Open a private in-memory database.
    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Open the database at `path` with SQLite's open flags (read-only,
    /// read-write without create, URI filenames).
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path = Self::path_text(path.as_ref())?;
        Ok(Self::wrap(block_on(crate::compat::open_with_flags(
            path, flags,
        ))?))
    }

    /// Open a private in-memory database with `flags`.
    pub fn open_in_memory_with_flags(flags: OpenFlags) -> Result<Self> {
        Self::open_with_flags(":memory:", flags)
    }

    /// The engine connection, for FrankenSQLite APIs this module does not
    /// mirror. Its methods are `async`.
    #[must_use]
    pub fn as_async(&self) -> &crate::Connection {
        self.inner
            .as_ref()
            .expect("connection is open until dropped")
    }

    /// The parameter layout of `sql`, cached per SQL text.
    fn slots(&self, sql: &str) -> Rc<BindParameterSlots> {
        if !sql
            .bytes()
            .any(|byte| matches!(byte, b'?' | b':' | b'@' | b'$'))
        {
            return Rc::clone(&self.no_parameters);
        }
        if let Some(slots) = self.parameter_slots.borrow().get(sql) {
            return Rc::clone(slots);
        }
        let slots = Rc::new(bind_parameter_slots(sql));
        let mut cache = self.parameter_slots.borrow_mut();
        if cache.len() >= PARAMETER_CACHE_LIMIT {
            cache.clear();
        }
        cache.insert(sql.into(), Rc::clone(&slots));
        slots
    }

    /// Run one statement with `params`; returns the number of rows changed.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        let values = params.bind_to(&self.slots(sql))?;
        Ok(block_on(self.as_async().execute_with_params(sql, &values))?)
    }

    /// Run every statement in `sql`, without parameters.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        Ok(block_on(self.as_async().execute_batch(sql))?)
    }

    /// Compile `sql` into a statement.
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let slots = self.slots(sql);
        let stmt = block_on(self.as_async().prepare(sql))?;
        let names = stmt.column_names();
        Ok(Statement {
            conn: self,
            stmt,
            slots,
            names,
        })
    }

    /// [`Connection::prepare`]; the engine caches compiled statements itself.
    pub fn prepare_cached(&self, sql: &str) -> Result<Statement<'_>> {
        self.prepare(sql)
    }

    /// Run a query and map its first row with `f`; [`Error::QueryReturnedNoRows`]
    /// when it has none. Further rows are ignored.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.prepare(sql)?.query_row(params, f)
    }

    /// [`Connection::query_row`] with a closure returning its own error type.
    pub fn query_row_and_then<T, E, P, F>(&self, sql: &str, params: P, f: F) -> Result<T, E>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T, E>,
        E: From<Error>,
    {
        let mut stmt = self.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let row = rows.next()?.ok_or(Error::QueryReturnedNoRows)?;
        f(row)
    }

    /// The rowid of the most recent successful INSERT on this connection.
    #[must_use]
    pub fn last_insert_rowid(&self) -> i64 {
        self.as_async().last_insert_rowid()
    }

    fn counter(&self, sql: &str) -> u64 {
        block_on(self.as_async().query_row(sql))
            .ok()
            .and_then(|row| match row.get(0) {
                Some(SqliteValue::Integer(n)) => u64::try_from(*n).ok(),
                _ => None,
            })
            .unwrap_or(0)
    }

    /// Rows changed by the most recent INSERT, UPDATE or DELETE.
    #[must_use]
    pub fn changes(&self) -> u64 {
        self.counter("SELECT changes()")
    }

    /// Rows changed by INSERT, UPDATE and DELETE since the connection opened.
    #[must_use]
    pub fn total_changes(&self) -> u64 {
        self.counter("SELECT total_changes()")
    }

    /// Whether no explicit transaction is open.
    #[must_use]
    pub fn is_autocommit(&self) -> bool {
        !self.as_async().in_transaction()
    }

    /// The database file path (`":memory:"` for an in-memory database).
    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.inner.as_ref().map(crate::Connection::path)
    }

    /// Begin a DEFERRED transaction (FrankenSQLite runs it as a concurrent
    /// writer: `BEGIN` is `BEGIN CONCURRENT`).
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        Transaction::new(self, TransactionBehavior::Deferred)
    }

    /// Begin a transaction with `behavior`.
    pub fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>> {
        Transaction::new(self, behavior)
    }

    /// Begin a DEFERRED transaction through a shared reference; nesting is
    /// the caller's responsibility (a nested BEGIN fails).
    pub fn unchecked_transaction(&self) -> Result<Transaction<'_>> {
        Transaction::new_unchecked(self, TransactionBehavior::Deferred)
    }

    /// Set the busy timeout (`PRAGMA busy_timeout`).
    pub fn busy_timeout(&self, timeout: Duration) -> Result<()> {
        let millis = i64::try_from(timeout.as_millis()).unwrap_or(i64::from(i32::MAX));
        self.execute_batch(&format!("PRAGMA busy_timeout = {millis}"))
    }

    fn pragma_sql(schema: Option<DatabaseName<'_>>, name: &str) -> String {
        schema.map_or_else(
            || format!("PRAGMA {name}"),
            |schema| format!("PRAGMA {schema}.{name}"),
        )
    }

    /// `PRAGMA [schema.]name = value`.
    #[allow(clippy::needless_pass_by_value)] // rusqlite's signature
    pub fn pragma_update<V: ToSql>(
        &self,
        schema: Option<DatabaseName<'_>>,
        name: &str,
        value: V,
    ) -> Result<()> {
        let sql = format!(
            "{} = {}",
            Self::pragma_sql(schema, name),
            sql_literal(&value)?
        );
        self.execute_batch(&sql)
    }

    /// `PRAGMA [schema.]name = value`, mapping the row it returns with `f`.
    #[allow(clippy::needless_pass_by_value)] // rusqlite's signature
    pub fn pragma_update_and_check<V, T, F>(
        &self,
        schema: Option<DatabaseName<'_>>,
        name: &str,
        value: V,
        f: F,
    ) -> Result<T>
    where
        V: ToSql,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let sql = format!(
            "{} = {}",
            Self::pragma_sql(schema, name),
            sql_literal(&value)?
        );
        self.query_row(&sql, [], f)
    }

    /// Read a one-value pragma (`PRAGMA [schema.]name`) and map its row.
    pub fn pragma_query_value<T, F>(
        &self,
        schema: Option<DatabaseName<'_>>,
        name: &str,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.query_row(&Self::pragma_sql(schema, name), [], f)
    }

    /// Read a pragma's rows (`PRAGMA [schema.]name`), calling `f` on each.
    pub fn pragma_query<F>(
        &self,
        schema: Option<DatabaseName<'_>>,
        name: &str,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&Row<'_>) -> Result<()>,
    {
        let mut stmt = self.prepare(&Self::pragma_sql(schema, name))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            f(row)?;
        }
        Ok(())
    }

    /// Close the connection: roll back an open transaction and checkpoint.
    /// Dropping it does the same; `close` reports a failure.
    pub fn close(mut self) -> Result<(), (Self, Error)> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        match block_on(inner.close_in_place()) {
            Ok(()) => {
                self.inner = None;
                Ok(())
            }
            Err(error) => Err((self, error.into())),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            let _ = try_block_on(inner.close_in_place());
        }
    }
}

/// A compiled statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    stmt: crate::PreparedStatement<'conn>,
    slots: Rc<BindParameterSlots>,
    names: Vec<String>,
}

impl fmt::Debug for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Statement")
            .field("columns", &self.names)
            .field("parameters", &self.slots.count)
            .finish_non_exhaustive()
    }
}

impl<'conn> Statement<'conn> {
    /// Run the statement with `params`; returns the number of rows changed.
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        let values = params.bind_to(&self.slots)?;
        Ok(block_on(self.stmt.execute_with_params(&values))?)
    }

    /// Run an INSERT that must insert exactly one row; returns its rowid.
    pub fn insert<P: Params>(&mut self, params: P) -> Result<i64> {
        match self.execute(params)? {
            1 => Ok(self.conn.last_insert_rowid()),
            changed => Err(Error::StatementChangedRows(changed)),
        }
    }

    /// Run the query; step through its rows with [`Rows::next`].
    pub fn query<P: Params>(&mut self, params: P) -> Result<Rows<'_>> {
        let values = params.bind_to(&self.slots)?;
        let rows = block_on(self.stmt.query_with_params(&values))?;
        Ok(Rows {
            names: &self.names,
            rows: rows.into_iter(),
            current: None,
        })
    }

    /// Run the query and map each row with `f`.
    pub fn query_map<T, P, F>(&mut self, params: P, f: F) -> Result<MappedRows<'_, F>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        Ok(MappedRows {
            rows: self.query(params)?,
            map: f,
        })
    }

    /// Run the query and map each row with `f`, which returns its own error.
    pub fn query_and_then<T, E, P, F>(&mut self, params: P, f: F) -> Result<AndThenRows<'_, F>>
    where
        P: Params,
        E: From<Error>,
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        Ok(AndThenRows {
            rows: self.query(params)?,
            map: f,
        })
    }

    /// Run the query and map its first row; [`Error::QueryReturnedNoRows`]
    /// when it has none.
    pub fn query_row<T, P, F>(&mut self, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut rows = self.query(params)?;
        let row = rows.next()?.ok_or(Error::QueryReturnedNoRows)?;
        f(row)
    }

    /// Whether the query returns at least one row.
    pub fn exists<P: Params>(&mut self, params: P) -> Result<bool> {
        Ok(self.query(params)?.next()?.is_some())
    }

    #[must_use]
    pub fn column_count(&self) -> usize {
        self.names.len()
    }

    #[must_use]
    pub fn column_names(&self) -> Vec<&str> {
        self.names.iter().map(String::as_str).collect()
    }

    pub fn column_name(&self, index: usize) -> Result<&str> {
        self.names
            .get(index)
            .map(String::as_str)
            .ok_or(Error::InvalidColumnIndex(index))
    }

    /// The index of column `name` (ASCII case-insensitive, like SQLite).
    pub fn column_index(&self, name: &str) -> Result<usize> {
        column_index(&self.names, name)
    }

    #[must_use]
    pub fn parameter_count(&self) -> usize {
        self.slots.count
    }

    /// The 1-based index of named parameter `name` (sigil included).
    pub fn parameter_index(&self, name: &str) -> Result<Option<usize>> {
        Ok(self.slots.index_of(name))
    }

    /// The name of parameter `index` (1-based), `None` for `?`/`?NNN`.
    #[must_use]
    pub fn parameter_name(&self, index: usize) -> Option<&str> {
        self.slots.name_of(index)
    }

    /// Drop the statement, reporting a failure (there is none to report).
    pub fn finalize(self) -> Result<()> {
        Ok(())
    }
}

fn column_index(names: &[String], name: &str) -> Result<usize> {
    names
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::InvalidColumnName(name.to_owned()))
}

/// The rows of a query, stepped with [`Rows::next`].
pub struct Rows<'stmt> {
    names: &'stmt [String],
    rows: std::vec::IntoIter<crate::Row>,
    current: Option<Row<'stmt>>,
}

impl<'stmt> Rows<'stmt> {
    /// The next row, or `None` after the last one.
    #[allow(clippy::should_implement_trait)] // rusqlite's lending `next`
    pub fn next(&mut self) -> Result<Option<&Row<'stmt>>> {
        self.current = self.rows.next().map(|values| Row {
            names: self.names,
            values,
        });
        Ok(self.current.as_ref())
    }

    /// Map each remaining row with `f`.
    pub fn mapped<F, T>(self, f: F) -> MappedRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        MappedRows { rows: self, map: f }
    }

    /// Map each remaining row with `f`, which returns its own error.
    pub fn and_then<F, T, E>(self, f: F) -> AndThenRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        AndThenRows { rows: self, map: f }
    }
}

/// The rows of a query mapped through a closure; an iterator of results.
pub struct MappedRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<T, F> Iterator for MappedRows<'_, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        let map = &mut self.map;
        self.rows.next().transpose().map(|row| row.and_then(map))
    }
}

/// The rows of a query mapped through a closure with its own error type.
pub struct AndThenRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<T, E, F> Iterator for AndThenRows<'_, F>
where
    E: From<Error>,
    F: FnMut(&Row<'_>) -> Result<T, E>,
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Result<T, E>> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row| row.map_err(E::from).and_then(map))
    }
}

/// A column index or name, for [`Row::get`].
pub trait RowIndex {
    #[doc(hidden)]
    fn index_in(&self, names: &[String]) -> Result<usize>;
}

impl RowIndex for usize {
    fn index_in(&self, names: &[String]) -> Result<usize> {
        if *self < names.len() {
            Ok(*self)
        } else {
            Err(Error::InvalidColumnIndex(*self))
        }
    }
}

impl RowIndex for &str {
    fn index_in(&self, names: &[String]) -> Result<usize> {
        column_index(names, self)
    }
}

/// One row of a query.
pub struct Row<'stmt> {
    names: &'stmt [String],
    values: crate::Row,
}

impl fmt::Debug for Row<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.names.iter().zip(self.values.values()))
            .finish()
    }
}

impl Row<'_> {
    /// Column `index` (a position or a column name) as `T`.
    pub fn get<I: RowIndex, T: FromSql>(&self, index: I) -> Result<T> {
        let index = index.index_in(self.names)?;
        let value = self.value_at(index);
        T::column_result(value).map_err(|error| match error {
            types::FromSqlError::InvalidType => {
                Error::InvalidColumnType(index, self.names[index].clone(), value.data_type())
            }
            types::FromSqlError::OutOfRange(wide) => Error::IntegralValueOutOfRange(index, wide),
            types::FromSqlError::Other(source) => {
                Error::FromSqlConversionFailure(index, value.data_type(), source)
            }
            other @ types::FromSqlError::InvalidBlobSize { .. } => {
                Error::FromSqlConversionFailure(index, value.data_type(), Box::new(other))
            }
        })
    }

    /// [`Row::get`], panicking on error.
    pub fn get_unwrap<I: RowIndex, T: FromSql>(&self, index: I) -> T {
        self.get(index).expect("Row::get_unwrap failed")
    }

    /// Column `index` as a borrowed value.
    pub fn get_ref<I: RowIndex>(&self, index: I) -> Result<ValueRef<'_>> {
        Ok(self.value_at(index.index_in(self.names)?))
    }

    /// [`Row::get_ref`], panicking on error.
    pub fn get_ref_unwrap<I: RowIndex>(&self, index: I) -> ValueRef<'_> {
        self.get_ref(index).expect("Row::get_ref_unwrap failed")
    }

    fn value_at(&self, index: usize) -> ValueRef<'_> {
        self.values.get(index).map_or(ValueRef::Null, value_ref)
    }
}

/// How [`Connection::transaction_with_behavior`] begins.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransactionBehavior {
    Deferred,
    Immediate,
    Exclusive,
}

/// What an unfinished [`Transaction`] does when dropped.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropBehavior {
    Rollback,
    Commit,
    Ignore,
    Panic,
}

/// An open transaction; dereferences to its [`Connection`]. Dropping it
/// without [`commit`](Transaction::commit) rolls back.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    drop_behavior: DropBehavior,
    finished: bool,
}

impl fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("drop_behavior", &self.drop_behavior)
            .finish_non_exhaustive()
    }
}

impl<'conn> Transaction<'conn> {
    /// Begin a transaction on `conn`.
    pub fn new(conn: &'conn mut Connection, behavior: TransactionBehavior) -> Result<Self> {
        Self::new_unchecked(conn, behavior)
    }

    /// Begin a transaction through a shared reference.
    pub fn new_unchecked(conn: &'conn Connection, behavior: TransactionBehavior) -> Result<Self> {
        conn.execute_batch(match behavior {
            TransactionBehavior::Deferred => "BEGIN",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
        })?;
        Ok(Self {
            conn,
            drop_behavior: DropBehavior::Rollback,
            finished: false,
        })
    }

    #[must_use]
    pub const fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    pub const fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior;
    }

    pub fn commit(mut self) -> Result<()> {
        self.finished = true;
        self.conn.execute_batch("COMMIT")
    }

    pub fn rollback(mut self) -> Result<()> {
        self.finished = true;
        self.conn.execute_batch("ROLLBACK")
    }

    /// End the transaction the way dropping it would, reporting a failure.
    pub fn finish(mut self) -> Result<()> {
        self.finish_by_drop_behavior()
    }

    fn finish_by_drop_behavior(&mut self) -> Result<()> {
        if std::mem::replace(&mut self.finished, true) || self.conn.is_autocommit() {
            return Ok(());
        }
        match self.drop_behavior {
            DropBehavior::Commit => self
                .conn
                .execute_batch("COMMIT")
                .or_else(|_| self.conn.execute_batch("ROLLBACK")),
            DropBehavior::Rollback => self.conn.execute_batch("ROLLBACK"),
            DropBehavior::Ignore => Ok(()),
            DropBehavior::Panic => panic!("Transaction dropped unexpectedly."),
        }
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        let _ = self.finish_by_drop_behavior();
    }
}

/// `.optional()` on a query result: [`Error::QueryReturnedNoRows`] becomes
/// `Ok(None)`.
pub trait OptionalExtension<T> {
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExtension<T> for Result<T> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod stock {
        pub use rusqlite::*;
    }

    mod ours {
        pub use crate::blocking::*;
    }

    /// The same rusqlite-style code, compiled against module `$m`, run on a
    /// fresh database at `$path`. Returns a trace of every result, with errors
    /// reduced to their kind and result codes (messages differ by engine).
    macro_rules! scenario {
        ($m:ident, $path:expr) => {{
            use $m::{
                Connection, OptionalExtension, TransactionBehavior, named_params, params,
                params_from_iter,
            };
            fn kind(error: &$m::Error) -> String {
                match error {
                    $m::Error::QueryReturnedNoRows => "no-rows".to_owned(),
                    $m::Error::SqliteFailure(error, _) => {
                        format!("sqlite {:?} {}", error.code, error.extended_code)
                    }
                    $m::Error::InvalidColumnType(index, name, ty) => {
                        format!("column-type {index} {name} {ty}")
                    }
                    $m::Error::IntegralValueOutOfRange(index, value) => {
                        format!("out-of-range {index} {value}")
                    }
                    $m::Error::InvalidParameterCount(given, needed) => {
                        format!("parameter-count {given} {needed}")
                    }
                    $m::Error::InvalidParameterName(name) => format!("parameter-name {name}"),
                    $m::Error::InvalidColumnIndex(index) => format!("column-index {index}"),
                    $m::Error::InvalidColumnName(name) => format!("column-name {name}"),
                    $m::Error::StatementChangedRows(changed) => format!("changed {changed}"),
                    $m::Error::FromSqlConversionFailure(index, ty, _) => {
                        format!("conversion {index} {ty}")
                    }
                    $m::Error::ToSqlConversionFailure(_) => "to-sql".to_owned(),
                    other => format!("other {other}"),
                }
            }
            fn show<T: std::fmt::Debug>(result: $m::Result<T>) -> String {
                match result {
                    Ok(value) => format!("ok {value:?}"),
                    Err(error) => format!("err {}", kind(&error)),
                }
            }
            let mut trace: Vec<String> = Vec::new();
            let mut conn = Connection::open($path).unwrap();
            conn.execute_batch(
                "CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, \
                 age INTEGER, score REAL, data BLOB);
                 CREATE TABLE note(id INTEGER PRIMARY KEY, person INTEGER, body TEXT);",
            )
            .unwrap();
            trace.push(show(conn.execute(
                "INSERT INTO person(name, age, score, data) VALUES (?1, ?2, ?3, ?4)",
                params!["Ada", 36, 9.5, vec![1_u8, 2, 3]],
            )));
            trace.push(show(conn.execute(
                "INSERT INTO person(name, age, score) VALUES (?, ?, ?)",
                ("Grace", 45_i64, None::<f64>),
            )));
            trace.push(show(conn.execute(
                "INSERT INTO person(name, age) VALUES (?1, ?2)",
                ["Linus", "54"],
            )));
            trace.push(show(conn.execute(
                "INSERT INTO person(name, age, score) VALUES (:name, :age, :name_len)",
                named_params! { ":name": "Barbara", ":age": 81, ":name_len": 7.0 },
            )));
            trace.push(show(conn.execute(
                "INSERT INTO person(name, age) VALUES (?1, ?2)",
                params_from_iter(["Ken", "80"]),
            )));
            trace.push(format!("rowid {}", conn.last_insert_rowid()));
            {
                let mut insert = conn
                    .prepare("INSERT INTO note(person, body) VALUES (?1, ?2)")
                    .unwrap();
                for (person, body) in [(1, "analytical engine"), (2, "COBOL"), (2, "bug")] {
                    trace.push(show(insert.insert(params![person, body])));
                }
                trace.push(format!(
                    "parameters {} {:?} {:?}",
                    insert.parameter_count(),
                    insert.parameter_index(":x").unwrap(),
                    insert.column_count()
                ));
            }
            trace.push(show(conn.query_row(
                "SELECT id, name, age, score, data FROM person WHERE name = ?1",
                ["Ada"],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>("name")?,
                        row.get::<_, Option<i64>>("AGE")?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                    ))
                },
            )));
            trace.push(show(conn.query_row(
                "SELECT score, age, age > 40 FROM person WHERE name = 'Grace'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<f64>>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get_ref(0)?.data_type(),
                        row.get_ref(1)?.data_type(),
                    ))
                },
            )));
            trace.push(show(
                conn.query_row(
                    "SELECT age FROM person WHERE name = ?1",
                    ["Nobody"],
                    |row| row.get::<_, i64>(0),
                )
                .optional(),
            ));
            trace.push(show(conn.query_row(
                "SELECT age FROM person WHERE name = ?1",
                ["Nobody"],
                |row| row.get::<_, i64>(0),
            )));
            trace.push(show(conn.query_row(
                "SELECT name FROM person ORDER BY id",
                [],
                |row| row.get::<_, String>(0),
            )));
            {
                let mut stmt = conn
                    .prepare(
                        "SELECT p.name, count(n.id) AS notes FROM person p LEFT JOIN note n \
                              ON n.person = p.id GROUP BY p.id ORDER BY p.id",
                    )
                    .unwrap();
                trace.push(format!("columns {:?}", stmt.column_names()));
                let named: $m::Result<Vec<(String, i64)>> = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get("notes")?)))
                    .unwrap()
                    .collect();
                trace.push(show(named));
                let lengths: Result<Vec<usize>, Box<dyn std::error::Error>> = stmt
                    .query_and_then([], |row| Ok(row.get::<_, String>(0)?.len()))
                    .unwrap()
                    .collect();
                trace.push(format!("{:?}", lengths.map_err(|error| error.to_string())));
                let mut rows = stmt.query([]).unwrap();
                let mut stepped = Vec::new();
                while let Some(row) = rows.next().unwrap() {
                    stepped.push(row.get::<_, i64>(1).unwrap());
                }
                trace.push(format!("stepped {stepped:?}"));
            }
            trace.push(show(
                conn.prepare("SELECT 1 FROM note WHERE body = ?1")
                    .unwrap()
                    .exists(["COBOL"]),
            ));
            // Errors applications match on.
            trace.push(show(
                conn.execute("INSERT INTO person(name) VALUES (?1)", ["Ada"]),
            ));
            trace.push(show(conn.execute("INSERT INTO person(age) VALUES (1)", [])));
            trace.push(show(conn.query_row(
                "SELECT name FROM person WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )));
            trace.push(show(
                conn.query_row("SELECT 300", [], |row| row.get::<_, u8>(0)),
            ));
            trace.push(show(
                conn.query_row("SELECT -1", [], |row| row.get::<_, u64>(0)),
            ));
            trace.push(show(
                conn.query_row("SELECT 1", [], |row| row.get::<_, i64>(1)),
            ));
            trace.push(show(
                conn.query_row("SELECT 1 AS a", [], |row| row.get::<_, i64>("b")),
            ));
            trace.push(show(conn.execute("SELECT ?1, ?2", [1])));
            trace.push(show(conn.execute(
                "UPDATE person SET age = :age WHERE id = 1",
                named_params! { ":wrong": 1 },
            )));
            trace.push(show(conn.execute("SELECT ?1", [u64::MAX])));
            trace.push(show(
                conn.prepare("INSERT INTO note(person) SELECT id FROM person WHERE 0")
                    .unwrap()
                    .insert([]),
            ));
            // Transactions.
            {
                let tx = conn.transaction().unwrap();
                trace.push(format!("autocommit in tx {}", tx.is_autocommit()));
                tx.execute("UPDATE person SET age = age + 1", []).unwrap();
                trace.push(format!("changes {}", tx.changes()));
                tx.commit().unwrap();
            }
            {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                tx.execute("DELETE FROM note", []).unwrap();
                // Dropped: rolls back.
            }
            {
                let tx = conn.unchecked_transaction().unwrap();
                tx.execute("INSERT INTO note(person, body) VALUES (3, 'kernel')", [])
                    .unwrap();
                tx.rollback().unwrap();
            }
            trace.push(format!("autocommit {}", conn.is_autocommit()));
            trace.push(show(conn.query_row(
                "SELECT count(*), sum(age) FROM person JOIN note ON note.person = person.id",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )));
            conn.pragma_update(None, "user_version", 7).unwrap();
            trace.push(show(conn.pragma_query_value(None, "user_version", |row| {
                row.get::<_, i64>(0)
            })));
            conn.busy_timeout(std::time::Duration::from_millis(250))
                .unwrap();
            trace.push(show(conn.pragma_query_value(None, "busy_timeout", |row| {
                row.get::<_, i64>(0)
            })));
            trace.push(show(
                conn.execute("DELETE FROM note WHERE person = ?1", [2]),
            ));
            trace.push(format!("changes {}", conn.changes()));
            trace
        }};
    }

    fn temp_path(dir: &tempfile::TempDir, name: &str) -> String {
        dir.path().join(name).to_string_lossy().into_owned()
    }

    #[test]
    fn rusqlite_code_compiles_and_behaves_the_same() {
        let dir = tempfile::tempdir().unwrap();
        let theirs = scenario!(stock, temp_path(&dir, "stock.db"));
        let mine = scenario!(ours, temp_path(&dir, "fsqlite.db"));
        for (index, (expected, actual)) in theirs.iter().zip(&mine).enumerate() {
            assert_eq!(actual, expected, "trace step {index}");
        }
        assert_eq!(mine.len(), theirs.len());
        assert!(
            theirs
                .iter()
                .any(|line| line.contains("ConstraintViolation")),
            "the scenario must exercise a constraint error: {theirs:?}"
        );
    }

    /// The drop-in promise: a file rusqlite wrote opens here, takes writes,
    /// and opens again in rusqlite with every row and a clean integrity check.
    #[test]
    fn database_files_move_between_rusqlite_and_fsqlite() {
        let dir = tempfile::tempdir().unwrap();
        let path = temp_path(&dir, "shared.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "CREATE TABLE item(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);
                     CREATE INDEX item_name ON item(name);",
                )
                .unwrap();
            for i in 0..500_i64 {
                stock
                    .execute(
                        "INSERT INTO item(name, qty) VALUES (?1, ?2)",
                        rusqlite::params![format!("item-{i}"), i % 7],
                    )
                    .unwrap();
            }
        }
        {
            let mut conn = Connection::open(&path).unwrap();
            let total: i64 = conn
                .query_row(
                    "SELECT sum(qty) FROM item WHERE name LIKE 'item-1%'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let expected: i64 = (0..500_i64)
                .filter(|i| format!("item-{i}").starts_with("item-1"))
                .map(|i| i % 7)
                .sum();
            assert_eq!(total, expected);
            let tx = conn.transaction().unwrap();
            {
                let mut insert = tx
                    .prepare("INSERT INTO item(name, qty) VALUES (?1, ?2)")
                    .unwrap();
                for i in 500..800_i64 {
                    insert.execute(params![format!("item-{i}"), i % 7]).unwrap();
                }
            }
            tx.execute("UPDATE item SET qty = qty * 10 WHERE id % 50 = 0", [])
                .unwrap();
            tx.commit().unwrap();
            conn.close().map_err(|(_, error)| error).unwrap();
        }
        let stock = rusqlite::Connection::open(&path).unwrap();
        let check: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(check, "ok");
        let (count, qty): (i64, i64) = stock
            .query_row("SELECT count(*), sum(qty) FROM item", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        let expected_qty: i64 = (0..800_i64)
            .map(|i| {
                if (i + 1) % 50 == 0 {
                    (i % 7) * 10
                } else {
                    i % 7
                }
            })
            .sum();
        assert_eq!((count, qty), (800, expected_qty));
        let by_index: i64 = stock
            .query_row("SELECT qty FROM item WHERE name = 'item-799'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(by_index, 799 % 7 * 10);
    }

    #[test]
    fn dropping_a_connection_rolls_back_its_open_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = temp_path(&dir, "drop.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
                .unwrap();
            conn.execute_batch("BEGIN; INSERT INTO t VALUES (2);")
                .unwrap();
            assert!(!conn.is_autocommit());
        }
        let conn = Connection::open(&path).unwrap();
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "the uncommitted row must not survive the drop");
    }
}
