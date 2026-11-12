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
//! transactions and nested [`Savepoint`]s with [`TransactionBehavior`] and
//! [`DropBehavior`], pragmas,
//! `busy_timeout`, `changes`, `last_insert_rowid`, [`OptionalExtension`],
//! user-defined scalar functions ([`Connection::create_scalar_function`]),
//! and an [`Error`] whose variants and [`ErrorCode`]s match rusqlite's. Not
//! yet: aggregate and window functions, online backup, blob I/O, hooks, and
//! the `chrono`/`uuid`/`serde_json` conversions.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use fsqlite_error::FrankenError;
use fsqlite_parser::{BindParameterSlots, bind_parameter_slots};
use fsqlite_types::SmallText;
use fsqlite_types::value::SqliteValue;

pub use crate::compat::OpenFlags;
pub use ffi::ErrorCode;
pub use types::{FromSql, ToSql};

/// Drive an engine future to completion on the calling thread.
///
/// A blocking connection never leaves its thread and is opened with
/// [`dedicated_thread_env`], so its futures run their page I/O inline and
/// almost always finish on the first poll. Polling them here, and parking only
/// when one really is pending, costs a few nanoseconds per call. Entering an
/// async runtime for every call cost about 25 µs, several times the statement
/// itself (bd-w4pr1.23). The `AsyncConnection` worker thread drives the same
/// futures the same way.
///
/// This uses no thread-local state that can already be gone, so it also works
/// for a connection dropped from another thread-local's destructor.
///
/// The call runs on the calling thread's stack when at least
/// [`BLOCKING_CALL_STACK_RED_ZONE_BYTES`] of it are left, and otherwise on a
/// heap-allocated segment (`stacker`, the crate rustc and fsqlite-core use for
/// the same purpose). A rusqlite application's own `cargo test` builds this
/// crate at opt-level 0, where one statement can need about 2 MiB of stack:
/// as much as Rust gives every spawned and test thread (bd-w4pr1.28).
fn block_on<F: Future>(future: F) -> F::Output {
    stacker::maybe_grow(
        BLOCKING_CALL_STACK_RED_ZONE_BYTES,
        BLOCKING_CALL_STACK_SEGMENT_BYTES,
        || drive_on_current_stack(future),
    )
}

/// Stack a blocking call needs free before it drives the engine on the
/// calling thread's stack. An unoptimised build of the engine reserves very
/// large poll frames (each statement shape has its own chain of about 2 MiB);
/// an optimised one fits a statement in under 512 KiB. The release value is
/// below a 2 MiB thread's free stack, so an optimised build never switches
/// stacks on an ordinary thread.
#[cfg(debug_assertions)]
const BLOCKING_CALL_STACK_RED_ZONE_BYTES: usize = 4 * 1024 * 1024;
#[cfg(not(debug_assertions))]
const BLOCKING_CALL_STACK_RED_ZONE_BYTES: usize = 1024 * 1024;

/// Size of the stack segment a blocking call moves to when the calling thread
/// is short of [`BLOCKING_CALL_STACK_RED_ZONE_BYTES`]. Pages are committed only
/// as they are touched.
const BLOCKING_CALL_STACK_SEGMENT_BYTES: usize = 8 * 1024 * 1024;

/// [`block_on`] on whatever stack the caller is running on.
fn drive_on_current_stack<F: Future>(future: F) -> F::Output {
    /// Wakes the thread that is driving the future.
    struct Unpark {
        thread: Thread,
        notified: AtomicBool,
    }

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.notified.store(true, Ordering::Release);
            self.thread.unpark();
        }
    }

    let unpark = Arc::new(Unpark {
        thread: thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&unpark));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        // `park` can return spuriously, and a wake can arrive before it is
        // reached; the flag makes both cases correct.
        while !unpark.notified.swap(false, Ordering::Acquire) {
            thread::park();
        }
    }
}

/// The environment of a blocking connection: one whose whole life, open to
/// close, is spent on the thread that opened it.
///
/// The type guarantees that (a [`Connection`] is not `Send`), which is the
/// condition under which the engine may run bounded page reads and writes
/// inline instead of handing each one to another thread.
fn dedicated_thread_env() -> crate::ConnectionEnv {
    let mut env = crate::ConnectionEnv::default();
    env.mark_blocking_io_inline_safe();
    env
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
    /// A user-defined function's argument has a type its requested Rust type
    /// does not accept ([`functions::Context::get`]).
    InvalidFunctionParameterType(usize, types::Type),
    /// An error a user-defined function returns for its own reasons.
    UserFunctionError(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A user-defined function panicked.
    UnwindingPanic,
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
            Self::InvalidFunctionParameterType(index, ty) => {
                write!(f, "Invalid function parameter type {ty} at index {index}")
            }
            Self::UserFunctionError(error) => error.fmt(f),
            Self::UnwindingPanic => f.write_str("unwinding panic"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SqliteFailure(error, _) => Some(error),
            Self::Utf8Error(error) => Some(error),
            Self::FromSqlConversionFailure(_, _, error)
            | Self::ToSqlConversionFailure(error)
            | Self::UserFunctionError(error) => Some(&**error),
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

/// User-defined SQL functions, as rusqlite's `functions` module: register a
/// closure with [`Connection::create_scalar_function`].
pub mod functions {
    use std::marker::PhantomData;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Mutex, PoisonError};

    use fsqlite_error::FrankenError;
    use fsqlite_types::value::SqliteValue;

    use super::types::{FromSql, FromSqlError, ToSql, ValueRef};
    use super::{Error, Result, engine_value, value_ref};

    bitflags::bitflags! {
        /// SQLite's function flags, with their `SQLITE_*` names and values.
        /// Text always reaches a function as UTF-8, and of the rest only
        /// [`FunctionFlags::SQLITE_DETERMINISTIC`] changes anything here:
        /// without it the function cannot appear in an index expression.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct FunctionFlags: i32 {
            const SQLITE_UTF8 = 1;
            const SQLITE_UTF16LE = 2;
            const SQLITE_UTF16BE = 3;
            const SQLITE_UTF16 = 4;
            const SQLITE_DETERMINISTIC = 0x0000_0800;
            const SQLITE_DIRECTONLY = 0x0008_0000;
            const SQLITE_SUBTYPE = 0x0010_0000;
            const SQLITE_INNOCUOUS = 0x0020_0000;
            const SQLITE_RESULT_SUBTYPE = 0x0100_0000;
            const SQLITE_SELFORDER1 = 0x0200_0000;
        }
    }

    impl Default for FunctionFlags {
        fn default() -> Self {
            Self::SQLITE_UTF8
        }
    }

    /// The arguments of one call of a user-defined function.
    pub struct Context<'a> {
        args: &'a [SqliteValue],
    }

    impl Context<'_> {
        /// The number of arguments.
        #[must_use]
        pub const fn len(&self) -> usize {
            self.args.len()
        }

        /// Whether the call has no arguments.
        #[must_use]
        pub const fn is_empty(&self) -> bool {
            self.args.is_empty()
        }

        /// Argument `idx` as a `T`.
        ///
        /// # Panics
        ///
        /// If `idx` is not below [`Context::len`], as in rusqlite.
        pub fn get<T: FromSql>(&self, idx: usize) -> Result<T> {
            let value = self.get_raw(idx);
            T::column_result(value).map_err(|error| match error {
                FromSqlError::InvalidType => {
                    Error::InvalidFunctionParameterType(idx, value.data_type())
                }
                FromSqlError::OutOfRange(wide) => Error::IntegralValueOutOfRange(idx, wide),
                FromSqlError::Other(source) => {
                    Error::FromSqlConversionFailure(idx, value.data_type(), source)
                }
                other => Error::FromSqlConversionFailure(idx, value.data_type(), Box::new(other)),
            })
        }

        /// Argument `idx` as it is.
        ///
        /// # Panics
        ///
        /// If `idx` is not below [`Context::len`], as in rusqlite.
        #[must_use]
        pub fn get_raw(&self, idx: usize) -> ValueRef<'_> {
            value_ref(&self.args[idx])
        }
    }

    /// A closure registered as a scalar SQL function.
    pub(super) struct ScalarClosure<F, T> {
        name: String,
        n_arg: i32,
        deterministic: bool,
        /// The engine calls functions through `&self` from a `Sync` registry;
        /// the closure is `FnMut + Send`, so it is called under a lock. Only
        /// the connection's own thread ever takes it.
        func: Mutex<F>,
        output: PhantomData<fn() -> T>,
    }

    impl<F, T> ScalarClosure<F, T> {
        pub(super) fn new(name: &str, n_arg: i32, flags: FunctionFlags, func: F) -> Self {
            Self {
                name: name.to_owned(),
                n_arg,
                deterministic: flags.contains(FunctionFlags::SQLITE_DETERMINISTIC),
                func: Mutex::new(func),
                output: PhantomData,
            }
        }

        pub(super) const fn deterministic(&self) -> bool {
            self.deterministic
        }
    }

    impl<F, T> fsqlite_func::ScalarFunction for ScalarClosure<F, T>
    where
        F: FnMut(&Context<'_>) -> Result<T> + Send,
        T: ToSql,
    {
        fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            let context = Context { args };
            // A panic in an earlier call poisons the lock; the closure stays
            // callable, as it does in rusqlite after an `UnwindingPanic`.
            let mut func = self.func.lock().unwrap_or_else(PoisonError::into_inner);
            catch_unwind(AssertUnwindSafe(|| (*func)(&context)))
                .unwrap_or(Err(Error::UnwindingPanic))
                .and_then(|value| value.to_sql().map(engine_value))
                // SQLite reports a function's error as SQLITE_ERROR with the
                // error's text (`sqlite3_result_error`), and so does rusqlite.
                .map_err(|error| FrankenError::FunctionError(error.to_string()))
        }

        fn is_deterministic(&self) -> bool {
            self.deterministic
        }

        fn num_args(&self) -> i32 {
            self.n_arg
        }

        fn name(&self) -> &str {
            &self.name
        }
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
    /// Boxed: the engine connection is kilobytes, and this handle is moved.
    inner: Option<Box<crate::Connection>>,
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
            inner: Some(Box::new(inner)),
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
        Ok(Self::wrap(block_on(crate::Connection::open_with_env(
            path,
            dedicated_thread_env(),
        ))?))
    }

    /// Open a private in-memory database.
    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Open the database at `path` with SQLite's open flags (read-only,
    /// read-write without create, URI filenames).
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path = Self::path_text(path.as_ref())?;
        Ok(Self::wrap(block_on(
            crate::compat::open_with_flags_with_env(path, flags, dedicated_thread_env()),
        )?))
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
            .as_deref()
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
            stmt: Box::new(stmt),
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
        self.inner.as_deref().map(crate::Connection::path)
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

    /// Begin a savepoint, opening a transaction if necessary. Dropping it
    /// rolls back its changes; [`Savepoint::commit`] releases it instead.
    pub fn savepoint(&mut self) -> Result<Savepoint<'_>> {
        Savepoint::new(self)
    }

    /// Begin a savepoint with a safely quoted, caller-provided name.
    pub fn savepoint_with_name<T: Into<String>>(&mut self, name: T) -> Result<Savepoint<'_>> {
        Savepoint::with_name(self, name)
    }

    /// Set the busy timeout (`PRAGMA busy_timeout`).
    pub fn busy_timeout(&self, timeout: Duration) -> Result<()> {
        let millis = timeout.as_millis().min(i32::MAX as u128);
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

    /// Register `x_func` as the SQL function `fn_name` taking `n_arg`
    /// arguments (`-1`: any number), replacing a function with the same name
    /// and argument count, built-in or not. A function named `regexp` with
    /// two arguments also implements the `REGEXP` operator.
    ///
    /// An error `x_func` returns, and a panic in it ([`Error::UnwindingPanic`]),
    /// fails the statement with SQLITE_ERROR and the error's text, as in
    /// SQLite. Statements prepared before the call must be prepared again.
    pub fn create_scalar_function<F, T>(
        &self,
        fn_name: &str,
        n_arg: i32,
        flags: functions::FunctionFlags,
        x_func: F,
    ) -> Result<()>
    where
        F: FnMut(&functions::Context<'_>) -> Result<T> + Send + 'static,
        T: ToSql + 'static,
    {
        // The arguments `sqlite3_create_function` refuses as misuse.
        let max_args = i32::from(fsqlite_types::limits::MAX_FUNCTION_ARG);
        if !(-1..=max_args).contains(&n_arg) || fn_name.is_empty() || fn_name.len() > 255 {
            return Err(Error::SqliteFailure(
                ffi::Error::new(ffi::SQLITE_MISUSE),
                Some("bad parameter or other API misuse".to_owned()),
            ));
        }
        let function = functions::ScalarClosure::new(fn_name, n_arg, flags, x_func);
        if function.deterministic() {
            self.as_async()
                .register_deterministic_scalar_function(function);
        } else {
            self.as_async()
                .register_nondeterministic_scalar_function(function);
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
            let _ = block_on(inner.close_in_place());
        }
    }
}

/// The engine statement behind a [`Statement`].
///
/// Held as a trait object because `dyn EngineStatement + 'conn` is covariant
/// in `'conn` and the engine's `PreparedStatement<'conn>` is not. That lets a
/// [`Row`] hold `&'stmt Statement<'stmt>` and lend it out through `AsRef`,
/// as rusqlite's does (`row.as_ref().column_index(name)`).
trait EngineStatement {
    fn execute(&self, values: &[SqliteValue]) -> Result<usize>;
    fn query(&self, values: &[SqliteValue]) -> Result<Vec<crate::Row>>;
}

impl EngineStatement for crate::PreparedStatement<'_> {
    fn execute(&self, values: &[SqliteValue]) -> Result<usize> {
        Ok(block_on(self.execute_with_params(values))?)
    }

    fn query(&self, values: &[SqliteValue]) -> Result<Vec<crate::Row>> {
        Ok(block_on(self.query_with_params(values))?)
    }
}

/// A compiled statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    stmt: Box<dyn EngineStatement + 'conn>,
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

impl Statement<'_> {
    /// Run the statement with `params`; returns the number of rows changed.
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        let values = params.bind_to(&self.slots)?;
        self.stmt.execute(&values)
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
        let rows = self.stmt.query(&values)?;
        Ok(Rows {
            stmt: self,
            pending: rows.into_iter(),
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

    /// The result column names, in order (an iterator, as in rusqlite 0.41).
    pub fn column_names(&self) -> impl DoubleEndedIterator<Item = &str> + ExactSizeIterator {
        self.names.iter().map(String::as_str)
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
    stmt: &'stmt Statement<'stmt>,
    pending: std::vec::IntoIter<crate::Row>,
    current: Option<Row<'stmt>>,
}

impl<'stmt> Rows<'stmt> {
    /// The next row, or `None` after the last one.
    #[allow(clippy::should_implement_trait)] // rusqlite's lending `next`
    pub fn next(&mut self) -> Result<Option<&Row<'stmt>>> {
        self.current = self.pending.next().map(|values| Row {
            stmt: self.stmt,
            values,
        });
        Ok(self.current.as_ref())
    }

    /// The statement these rows come from (rusqlite's `Rows::as_ref`).
    #[allow(clippy::should_implement_trait)] // rusqlite's inherent `as_ref`
    #[must_use]
    pub const fn as_ref(&self) -> Option<&Statement<'stmt>> {
        Some(self.stmt)
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
    stmt: &'stmt Statement<'stmt>,
    values: crate::Row,
}

impl fmt::Debug for Row<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.stmt.names.iter().zip(self.values.values()))
            .finish()
    }
}

/// The statement a row comes from, for its column metadata
/// (`row.as_ref().column_index(name)`), as in rusqlite.
impl<'stmt> AsRef<Statement<'stmt>> for Row<'stmt> {
    fn as_ref(&self) -> &Statement<'stmt> {
        self.stmt
    }
}

impl Row<'_> {
    /// Column `index` (a position or a column name) as `T`.
    pub fn get<I: RowIndex, T: FromSql>(&self, index: I) -> Result<T> {
        let names = &self.stmt.names;
        let index = index.index_in(names)?;
        let value = self.value_at(index);
        T::column_result(value).map_err(|error| match error {
            types::FromSqlError::InvalidType => {
                Error::InvalidColumnType(index, names[index].clone(), value.data_type())
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
        Ok(self.value_at(index.index_in(&self.stmt.names)?))
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

/// What an unfinished [`Transaction`] or [`Savepoint`] does when dropped.
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

    /// Begin a nested scope that rolls back independently of this transaction.
    pub fn savepoint(&mut self) -> Result<Savepoint<'_>> {
        Savepoint::begin(self.conn, "_fsqlite_sp".to_owned())
    }

    /// Begin a nested scope with a caller-provided savepoint name.
    pub fn savepoint_with_name<T: Into<String>>(&mut self, name: T) -> Result<Savepoint<'_>> {
        Savepoint::begin(self.conn, name.into())
    }

    #[must_use]
    pub const fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    pub const fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior;
    }

    pub fn commit(mut self) -> Result<()> {
        // A failed COMMIT (for example, a deferred foreign-key violation)
        // leaves the SQL transaction open. Keep its drop policy armed until
        // the engine accepts the commit, as rusqlite does.
        self.conn.execute_batch("COMMIT")?;
        self.finished = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
        self.finished = true;
        Ok(())
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
            // The caller opted into this with set_drop_behavior, as in
            // rusqlite: an unfinished transaction is a bug they want loud.
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

/// A nested transaction scope that dereferences to its [`Connection`].
///
/// Dropping a savepoint rolls back its work and releases the savepoint, leaving
/// an enclosing transaction intact. [`commit`](Self::commit) releases the
/// scope without undoing its changes. Only releasing the outermost savepoint
/// that opened a transaction commits that transaction to the database.
///
/// Unlike [`Transaction::rollback`], [`rollback`](Self::rollback) borrows the
/// savepoint and leaves it active, so callers can retry work within the scope.
pub struct Savepoint<'conn> {
    conn: &'conn Connection,
    name: String,
    drop_behavior: DropBehavior,
    finished: bool,
}

impl fmt::Debug for Savepoint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Savepoint")
            .field("name", &self.name)
            .field("drop_behavior", &self.drop_behavior)
            .finish_non_exhaustive()
    }
}

impl<'conn> Savepoint<'conn> {
    fn begin(conn: &'conn Connection, name: String) -> Result<Self> {
        conn.execute_batch(&format!("SAVEPOINT {}", quote_identifier(&name)))?;
        Ok(Self {
            conn,
            name,
            drop_behavior: DropBehavior::Rollback,
            finished: false,
        })
    }

    /// Begin a savepoint on `conn`. The mutable borrow prevents its parent
    /// from being ended while this scope is still in use.
    pub fn new(conn: &'conn mut Connection) -> Result<Self> {
        Self::begin(conn, "_fsqlite_sp".to_owned())
    }

    /// Begin a savepoint with a safely quoted, caller-provided name.
    pub fn with_name<T: Into<String>>(conn: &'conn mut Connection, name: T) -> Result<Self> {
        Self::begin(conn, name.into())
    }

    /// Begin another savepoint inside this one.
    pub fn savepoint(&mut self) -> Result<Savepoint<'_>> {
        Self::begin(self.conn, "_fsqlite_sp".to_owned())
    }

    /// Begin a named savepoint inside this one.
    pub fn savepoint_with_name<T: Into<String>>(&mut self, name: T) -> Result<Savepoint<'_>> {
        Self::begin(self.conn, name.into())
    }

    #[must_use]
    pub const fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    pub const fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior;
    }

    /// Release this savepoint, preserving its changes in the parent scope.
    /// On failure its configured drop policy still applies.
    pub fn commit(mut self) -> Result<()> {
        self.release()
    }

    fn release(&mut self) -> Result<()> {
        self.conn
            .execute_batch(&format!("RELEASE {}", quote_identifier(&self.name)))?;
        self.finished = true;
        Ok(())
    }

    /// Undo work since this savepoint began without releasing it. Further
    /// statements, repeated rollbacks, and a later commit remain valid.
    pub fn rollback(&mut self) -> Result<()> {
        self.conn
            .execute_batch(&format!("ROLLBACK TO {}", quote_identifier(&self.name)))
    }

    /// Apply the drop policy now, reporting any cleanup failure.
    pub fn finish(mut self) -> Result<()> {
        self.finish_by_drop_behavior()
    }

    fn finish_by_drop_behavior(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        match self.drop_behavior {
            DropBehavior::Commit => self.release().or_else(|_| {
                self.rollback()?;
                self.release()
            }),
            DropBehavior::Rollback => {
                self.rollback()?;
                self.release()
            }
            DropBehavior::Ignore => {
                self.finished = true;
                Ok(())
            }
            DropBehavior::Panic => {
                // finish() also runs Drop during unwinding; do not panic twice.
                self.finished = true;
                panic!("Savepoint dropped unexpectedly.")
            }
        }
    }
}

impl Deref for Savepoint<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl Drop for Savepoint<'_> {
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
                trace.push(format!(
                    "columns {:?}",
                    stmt.column_names().collect::<Vec<_>>()
                ));
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
            // A row lends out its statement's column metadata (casr's
            // tolerant column lookup: `row.as_ref().column_index(name)`).
            trace.push(show(conn.query_row(
                "SELECT 1 AS one, 2 AS Two",
                [],
                |row| {
                    let stmt = row.as_ref();
                    Ok((
                        stmt.column_count(),
                        stmt.column_index("two")?,
                        stmt.column_name(0)?.to_owned(),
                        stmt.column_index("three").is_err(),
                    ))
                },
            )));
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
                conn.query_row("SELECT -1", [], |row| row.get::<_, u32>(0)),
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

    /// rusqlite code that registers scalar functions, compiled against module
    /// `$m`. A function's own error text is what the application sees in both
    /// engines, so those errors keep their message; errors the engine words
    /// itself (arity, index rules) keep only their codes.
    macro_rules! udf_scenario {
        ($m:ident) => {{
            use std::sync::Arc;
            use std::sync::atomic::{AtomicI64, Ordering};
            use $m::Connection;
            use $m::functions::FunctionFlags;
            use $m::types::Value;
            fn show<T: std::fmt::Debug>(result: $m::Result<T>) -> String {
                match result {
                    Ok(value) => format!("ok {value:?}"),
                    Err($m::Error::SqliteFailure(error, message)) => {
                        format!("err {:?} {} {message:?}", error.code, error.extended_code)
                    }
                    Err(other) => format!("err other {other}"),
                }
            }
            // Errors the engine words itself at prepare (arity, index rules):
            // rusqlite reports those as `SqlInputError`, which this module does
            // not have yet, so compare only that the statement fails.
            fn fails<T: std::fmt::Debug>(result: $m::Result<T>) -> String {
                match result {
                    Ok(value) => format!("ok {value:?}"),
                    Err(_) => "err".to_owned(),
                }
            }
            let mut trace: Vec<String> = Vec::new();
            let conn = Connection::open_in_memory().unwrap();
            let pure = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
            // NULL in, NULL out (rano's `rano_report_utc` shape).
            conn.create_scalar_function("shout", 1, pure, |ctx| {
                let value: Option<String> = ctx.get(0)?;
                Ok(value.map(|value| value.to_uppercase()))
            })
            .unwrap();
            conn.create_scalar_function("halve", 1, pure, |ctx| {
                let value: f64 = ctx.get(0)?;
                Ok(value / 2.0)
            })
            .unwrap();
            // The function's own error (rano's `rano_utc` shape).
            conn.create_scalar_function("checked", 1, pure, |ctx| {
                let value: i64 = ctx.get(0)?;
                if value < 0 {
                    return Err($m::Error::UserFunctionError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("negative input {value}"),
                    ))));
                }
                Ok(value * 10)
            })
            .unwrap();
            conn.create_scalar_function("nargs", -1, pure, |ctx| {
                Ok(i64::try_from(ctx.len()).unwrap())
            })
            .unwrap();
            // Not deterministic: every call must run.
            let calls = Arc::new(AtomicI64::new(0));
            {
                let calls = Arc::clone(&calls);
                conn.create_scalar_function("tick", 0, FunctionFlags::SQLITE_UTF8, move |_ctx| {
                    Ok(calls.fetch_add(1, Ordering::SeqCst) + 1)
                })
                .unwrap();
            }
            // `X REGEXP Y` calls regexp(Y, X).
            conn.create_scalar_function("regexp", 2, pure, |ctx| {
                let pattern: String = ctx.get(0)?;
                let text: Option<String> = ctx.get(1)?;
                Ok(text.is_some_and(|text| text.contains(&pattern)))
            })
            .unwrap();
            conn.create_scalar_function("kind", 1, pure, |ctx| {
                Ok(format!("{:?}", ctx.get_raw(0).data_type()))
            })
            .unwrap();
            conn.create_scalar_function("boom", 0, pure, |_ctx| -> $m::Result<i64> {
                panic!("a user function panicked on purpose")
            })
            .unwrap();
            let value = |sql: &str| show(conn.query_row(sql, [], |row| row.get::<_, Value>(0)));
            trace.push(value("SELECT shout('ada')"));
            trace.push(value("SELECT shout(NULL)"));
            trace.push(value("SELECT halve(9)"));
            trace.push(value("SELECT halve('nine')"));
            trace.push(value("SELECT checked(4)"));
            trace.push(value("SELECT checked(-2)"));
            trace.push(value("SELECT checked(1.5)"));
            trace.push(value("SELECT nargs()"));
            trace.push(value("SELECT nargs(1, 'a', NULL, x'00')"));
            trace.push(value("SELECT tick()"));
            trace.push(value("SELECT tick()"));
            trace.push(value("SELECT boom()"));
            trace.push(value("SELECT checked(3)"));
            trace.push(fails(
                conn.query_row("SELECT halve(1, 2)", [], |row| row.get::<_, Value>(0)),
            ));
            trace.push(show(conn.query_row(
                "SELECT kind(1), kind(1.5), kind('t'), kind(x'00'), kind(NULL)",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )));
            trace.push(show(conn.query_row(
                "SELECT 'banana' REGEXP 'nan', 'banana' REGEXP 'x', NULL REGEXP 'x'",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )));
            // In statements over a table, with parameters and an index.
            conn.execute_batch("CREATE TABLE t(name TEXT)").unwrap();
            for name in ["grace", "ada", "linus"] {
                trace.push(show(
                    conn.execute("INSERT INTO t(name) VALUES (shout(?1))", [name]),
                ));
            }
            trace.push(show(conn.execute(
                "UPDATE t SET name = name || halve(5) WHERE name REGEXP 'AD'",
                [],
            )));
            trace.push(fails(conn.execute("CREATE INDEX t_tick ON t(tick())", [])));
            // rusqlite's `execute` of DDL returns `sqlite3_changes()`, which
            // still holds the count of the last INSERT/UPDATE/DELETE; this
            // module returns the statement's own 0. Compare only success.
            trace.push(show(
                conn.execute("CREATE INDEX t_shout ON t(shout(name))", [])
                    .map(|_| ()),
            ));
            trace.push(show(
                conn.prepare("SELECT name FROM t WHERE shout(name) = ?1 ORDER BY name")
                    .and_then(|mut stmt| {
                        stmt.query_map(["LINUS"], |row| row.get::<_, String>(0))?
                            .collect::<$m::Result<Vec<_>>>()
                    }),
            ));
            trace.push(show(conn.query_row(
                "SELECT count(*), sum(tick() > 0) FROM t WHERE name REGEXP 'A'",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )));
            trace.push(format!("calls {}", calls.load(Ordering::SeqCst)));
            // Replacing a built-in, and the arguments SQLite refuses.
            conn.create_scalar_function("upper", 1, pure, |_ctx| Ok("replaced"))
                .unwrap();
            trace.push(value("SELECT upper('a')"));
            // SQLite leaves the message of a misuse to earlier calls; compare the code.
            trace.push(format!(
                "misuse {:?}",
                conn.create_scalar_function("bad", -2, pure, |_ctx| Ok(0_i64))
                    .err()
                    .and_then(|error| error.sqlite_error_code())
            ));
            trace
        }};
    }

    #[test]
    fn user_functions_behave_like_rusqlite() {
        let theirs = udf_scenario!(stock);
        let mine = udf_scenario!(ours);
        for (index, (expected, actual)) in theirs.iter().zip(&mine).enumerate() {
            assert_eq!(actual, expected, "trace step {index}");
        }
        assert_eq!(mine.len(), theirs.len());
        // The scenario must reach a function's own error, a type error, a
        // panic, and per-call evaluation of a non-deterministic function.
        for needle in [
            "negative input -2",
            "Invalid function parameter type Text at index 0",
            "unwinding panic",
            "ok Integer(2)",
            "misuse Some(ApiMisuse)",
        ] {
            assert!(
                theirs.iter().any(|line| line.contains(needle)),
                "the scenario must reach {needle:?}: {theirs:?}"
            );
        }
    }

    /// rusqlite code that hits PRIMARY KEY and UNIQUE conflicts, compiled
    /// against module `$m`, on connection `$conn`. Stock words both "UNIQUE
    /// constraint failed: ..."; applications tell them apart only by the
    /// extended code (SQLITE_CONSTRAINT_PRIMARYKEY 1555, _UNIQUE 2067, and
    /// _ROWID 2579 for a hidden rowid), so errors keep their code, extended
    /// code and message (bd-w4pr1.22, bd-w4pr1.32). The
    /// conflict resolutions that must still treat a PRIMARY KEY conflict as a
    /// uniqueness conflict (OR IGNORE, OR REPLACE, UPSERT) are traced with the
    /// rows they leave.
    macro_rules! key_conflict_scenario {
        ($m:ident, $conn:expr) => {{
            use $m::params;
            fn show<T: std::fmt::Debug>(result: $m::Result<T>) -> String {
                match result {
                    Ok(value) => format!("ok {value:?}"),
                    Err($m::Error::SqliteFailure(error, message)) => {
                        format!("err {:?} {} {message:?}", error.code, error.extended_code)
                    }
                    Err(other) => format!("err other {other}"),
                }
            }
            let conn: $m::Connection = $conn;
            let mut trace: Vec<String> = Vec::new();
            let run = |sql: &str| show(conn.execute(sql, []));
            let rows = |sql: &str| {
                show(conn.prepare(sql).and_then(|mut stmt| {
                    stmt.query_map([], |row| row.get::<_, String>(0))?
                        .collect::<$m::Result<Vec<_>>>()
                }))
            };
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE);
                 CREATE TABLE tpk(id TEXT PRIMARY KEY, v INTEGER UNIQUE);
                 CREATE TABLE cpk(a INTEGER, b TEXT, v TEXT UNIQUE, PRIMARY KEY(a, b));
                 CREATE TABLE wr(a INTEGER, b TEXT, v TEXT UNIQUE, PRIMARY KEY(a, b))
                     WITHOUT ROWID;
                 CREATE TABLE upk(k TEXT PRIMARY KEY);
                 CREATE UNIQUE INDEX upk_k ON upk(k);
                 CREATE TABLE Hr(a TEXT);
                 CREATE TABLE Ti(id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO Hr(rowid, a) VALUES (1, 'x'), (2, 'y');
                 INSERT INTO Ti VALUES (1, 'x');
                 CREATE TEMP TABLE tmp(id TEXT PRIMARY KEY, v TEXT UNIQUE);
                 CREATE TEMP TABLE tmpi(id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t VALUES (1, 'one'), (2, 'two');
                 INSERT INTO tpk VALUES ('a', 1), ('b', 2);
                 INSERT INTO cpk VALUES (1, 'x', 'p'), (1, 'y', 'q');
                 INSERT INTO wr VALUES (1, 'x', 'p'), (1, 'y', 'q');
                 INSERT INTO upk VALUES ('k');
                 INSERT INTO tmp VALUES ('a', 'p');
                 INSERT INTO tmpi VALUES (1, 'p');",
            )
            .unwrap();
            // INTEGER PRIMARY KEY: the rowid is the key.
            trace.push(run("INSERT INTO t VALUES (1, 'dup')"));
            trace.push(show(
                conn.prepare("INSERT INTO t(id, v) VALUES (?1, ?2)")
                    .and_then(|mut stmt| stmt.execute(params![2, "dup"])),
            ));
            trace.push(run("UPDATE t SET id = 1 WHERE id = 2"));
            // Negative: a UNIQUE column of the same table stays _UNIQUE.
            trace.push(run("INSERT INTO t VALUES (3, 'one')"));
            trace.push(run("UPDATE t SET v = 'one' WHERE id = 2"));
            trace.push(run("INSERT OR IGNORE INTO t VALUES (1, 'ignored')"));
            trace.push(run("INSERT OR REPLACE INTO t VALUES (1, 'replaced')"));
            trace.push(run(
                "INSERT INTO t VALUES (2, 'upsert') ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            ));
            trace.push(run(
                "INSERT INTO t VALUES (2, 'nothing') ON CONFLICT(id) DO NOTHING",
            ));
            trace.push(rows(
                "SELECT quote(id) || '|' || quote(v) FROM t ORDER BY id",
            ));
            // A non-integer PRIMARY KEY of a rowid table is an index.
            trace.push(run("INSERT INTO tpk VALUES ('a', 9)"));
            trace.push(show(
                conn.prepare("INSERT INTO tpk VALUES (?1, ?2)")
                    .and_then(|mut stmt| stmt.execute(params!["b", 9])),
            ));
            trace.push(run("UPDATE tpk SET id = 'a' WHERE id = 'b'"));
            trace.push(run("INSERT INTO tpk VALUES ('c', 1)"));
            trace.push(run("INSERT OR IGNORE INTO tpk VALUES ('a', 7)"));
            trace.push(run("INSERT OR REPLACE INTO tpk VALUES ('a', 7)"));
            trace.push(run(
                "INSERT INTO tpk VALUES ('b', 8) ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            ));
            trace.push(rows(
                "SELECT quote(id) || '|' || quote(v) FROM tpk ORDER BY id",
            ));
            // A composite PRIMARY KEY names every key column.
            trace.push(run("INSERT INTO cpk VALUES (1, 'x', 'r')"));
            trace.push(run("INSERT INTO cpk VALUES (2, 'x', 'p')"));
            trace.push(run("INSERT INTO cpk VALUES (1, 'y', 's') \
                 ON CONFLICT(a, b) DO UPDATE SET v = excluded.v"));
            trace.push(rows(
                "SELECT quote(a) || '|' || quote(b) || '|' || quote(v) FROM cpk ORDER BY a, b",
            ));
            // A WITHOUT ROWID table's PRIMARY KEY is the table itself.
            trace.push(run("INSERT INTO wr VALUES (1, 'x', 'r')"));
            trace.push(run("UPDATE wr SET b = 'x' WHERE b = 'y'"));
            trace.push(run("INSERT INTO wr VALUES (2, 'x', 'p')"));
            trace.push(run("INSERT OR IGNORE INTO wr VALUES (1, 'x', 'z')"));
            trace.push(run("INSERT OR REPLACE INTO wr VALUES (1, 'x', 'z')"));
            trace.push(run("INSERT INTO wr VALUES (1, 'y', 'w') \
                 ON CONFLICT(a, b) DO UPDATE SET v = excluded.v"));
            trace.push(rows(
                "SELECT quote(a) || '|' || quote(b) || '|' || quote(v) FROM wr ORDER BY a, b",
            ));
            // Negative: a user's UNIQUE index over the PRIMARY KEY column is
            // newer, so stock checks it first and reports _UNIQUE.
            trace.push(run("INSERT INTO upk VALUES ('k')"));
            // TEMP tables keep their constraints in the in-memory store.
            trace.push(run("INSERT INTO tmp VALUES ('a', 'q')"));
            trace.push(run("INSERT INTO tmp VALUES ('b', 'p')"));
            trace.push(run("INSERT INTO tmpi VALUES (1, 'q')"));
            // A hidden rowid is not a PRIMARY KEY: stock reports
            // SQLITE_CONSTRAINT_ROWID and names it `<declared table>.rowid`,
            // whichever alias or case the statement used (bd-w4pr1.32).
            trace.push(run("INSERT INTO Hr(rowid, a) VALUES (1, 'dup')"));
            trace.push(run("INSERT INTO hr(_ROWID_, a) VALUES (2, 'dup')"));
            trace.push(show(
                conn.prepare("INSERT INTO HR(oid, a) VALUES (?1, ?2)")
                    .and_then(|mut stmt| stmt.execute(params![1, "dup"])),
            ));
            trace.push(run("UPDATE Hr SET rowid = 1 WHERE rowid = 2"));
            trace.push(run(
                "INSERT OR IGNORE INTO Hr(rowid, a) VALUES (1, 'ignored')",
            ));
            trace.push(run(
                "INSERT OR REPLACE INTO Hr(rowid, a) VALUES (2, 'replaced')",
            ));
            trace.push(rows(
                "SELECT rowid || '|' || quote(a) FROM Hr ORDER BY rowid",
            ));
            // Negative: an INTEGER PRIMARY KEY is the rowid and stays a PRIMARY
            // KEY conflict, named after the declared table.
            trace.push(run("INSERT INTO ti VALUES (1, 'dup')"));
            trace.push(show(
                conn.prepare("INSERT INTO TI(id, v) VALUES (?1, ?2)")
                    .and_then(|mut stmt| stmt.execute(params![1, "dup"])),
            ));
            trace
        }};
    }

    #[test]
    fn primary_key_conflicts_report_constraint_primarykey_like_rusqlite() {
        let dir = tempfile::tempdir().unwrap();
        let runs = [
            (
                key_conflict_scenario!(stock, stock::Connection::open_in_memory().unwrap()),
                key_conflict_scenario!(ours, ours::Connection::open_in_memory().unwrap()),
            ),
            (
                key_conflict_scenario!(
                    stock,
                    stock::Connection::open(temp_path(&dir, "keys-stock.db")).unwrap()
                ),
                key_conflict_scenario!(
                    ours,
                    ours::Connection::open(temp_path(&dir, "keys-fsqlite.db")).unwrap()
                ),
            ),
        ];
        for (run, (theirs, mine)) in runs.iter().enumerate() {
            for (index, (expected, actual)) in theirs.iter().zip(mine).enumerate() {
                assert_eq!(actual, expected, "run {run} trace step {index}");
            }
            assert_eq!(mine.len(), theirs.len(), "run {run}");
            // The scenario must reach a PRIMARY KEY conflict of every kind and
            // the UNIQUE conflicts that must not turn into one.
            for needle in [
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: t.id")"#,
                r#"err ConstraintViolation 2067 Some("UNIQUE constraint failed: t.v")"#,
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: tpk.id")"#,
                r#"err ConstraintViolation 2067 Some("UNIQUE constraint failed: tpk.v")"#,
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: cpk.a, cpk.b")"#,
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: wr.a, wr.b")"#,
                r#"err ConstraintViolation 2067 Some("UNIQUE constraint failed: wr.v")"#,
                r#"err ConstraintViolation 2067 Some("UNIQUE constraint failed: upk.k")"#,
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: tmp.id")"#,
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: tmpi.id")"#,
                r#"err ConstraintViolation 2579 Some("UNIQUE constraint failed: Hr.rowid")"#,
                r#"err ConstraintViolation 1555 Some("UNIQUE constraint failed: Ti.id")"#,
            ] {
                assert!(
                    theirs.iter().any(|line| line == needle),
                    "run {run}: the scenario must reach {needle}: {theirs:?}"
                );
            }
        }
    }

    /// rusqlite code whose WHERE clauses carry terms that read no column,
    /// compiled against module `$m`, on connection `$conn` (bd-w4pr1.13.22).
    /// The engine tests such a term once before the scan instead of per row;
    /// every statement must still answer, change rows and fail exactly as
    /// stock does, including a term that raises (stock raises it even when the
    /// table is empty), bound and named parameters, LEFT JOIN ON terms (not
    /// WHERE terms), and `random()`, which must stay per row.
    macro_rules! constant_where_scenario {
        ($m:ident, $conn:expr) => {{
            use $m::{named_params, params};
            fn show<T: std::fmt::Debug>(result: $m::Result<T>) -> String {
                match result {
                    Ok(value) => format!("ok {value:?}"),
                    Err($m::Error::SqliteFailure(error, message)) => {
                        format!("err {:?} {} {message:?}", error.code, error.extended_code)
                    }
                    Err(other) => format!("err other {other}"),
                }
            }
            let conn: $m::Connection = $conn;
            let mut trace: Vec<String> = Vec::new();
            let run = |sql: &str| show(conn.execute(sql, []));
            let rows = |sql: &str| {
                show(conn.prepare(sql).and_then(|mut stmt| {
                    stmt.query_map([], |row| row.get::<_, String>(0))?
                        .collect::<$m::Result<Vec<_>>>()
                }))
            };
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);
                 CREATE TABLE empty(x);
                 CREATE TABLE u(id INTEGER PRIMARY KEY, t_id INTEGER);
                 INSERT INTO u VALUES (1, 1), (2, 2), (3, 30);",
            )
            .unwrap();
            {
                let mut insert = conn.prepare("INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
                for id in 1..=60_i64 {
                    insert
                        .execute(params![id, id % 7, format!("r{id}")])
                        .unwrap();
                }
            }
            for sql in [
                "SELECT quote(count(*)) FROM t WHERE 0",
                "SELECT quote(count(*)) FROM t WHERE 1 = 0",
                "SELECT quote(count(*)) FROM t WHERE NULL",
                "SELECT quote(count(*)) FROM t WHERE 1 != 1",
                "SELECT quote(count(*)) FROM t WHERE 1 = 1 AND a > 3",
                "SELECT quote(count(*)) FROM t WHERE abs(-1) = 1 AND id <= 9",
                "SELECT quote(id) FROM t WHERE 0",
                "SELECT quote(id) FROM t WHERE 'abc'",
                "SELECT quote(id) FROM t WHERE 1 = 1 AND a = 2 ORDER BY id",
                "SELECT quote(sum(a)) || '|' || quote(max(b)) || '|' || quote(count(*)) \
                 FROM t WHERE 0",
                "SELECT quote(sum(a)) FROM t WHERE 2 > 1 AND id <= 10",
                // GROUP BY output kept to a bare key (the bare aggregate is read
                // below): an expression over a key or an aggregate fails on a
                // file today (bd-w4pr1.34).
                "SELECT b FROM t WHERE 0 GROUP BY b",
                "SELECT b FROM t WHERE 1 = 1 AND id <= 3 GROUP BY b ORDER BY b",
                "SELECT quote(t.id) || '|' || quote(u.id) FROM t \
                 LEFT JOIN u ON u.t_id = t.id AND 0 WHERE t.id <= 3 ORDER BY t.id",
                "SELECT quote(t.id) || '|' || quote(u.id) FROM t \
                 LEFT JOIN u ON u.t_id = t.id WHERE 0",
                "SELECT quote(count(*)) FROM t WHERE abs(-9223372036854775808)",
                "SELECT quote(count(*)) FROM empty WHERE abs(-9223372036854775808)",
                "SELECT quote(count(*)) FROM t WHERE random() IS NOT NULL",
                // bd-w4pr1.33: every "integer overflow" is SQLITE_ERROR in stock.
                "SELECT quote(sum(x)) FROM (SELECT 9223372036854775807 AS x UNION ALL SELECT 1)",
                "SELECT quote(sum(x) OVER ()) FROM \
                 (SELECT 9223372036854775807 AS x UNION ALL SELECT 1)",
            ] {
                trace.push(rows(sql));
            }
            for sql in [
                "SELECT count(*) FROM t WHERE 0 GROUP BY a",
                "SELECT count(*) FROM t WHERE 1 = 1 AND id <= 14 GROUP BY a ORDER BY a",
            ] {
                trace.push(show(conn.prepare(sql).and_then(|mut stmt| {
                    stmt.query_map([], |row| row.get::<_, i64>(0))?
                        .collect::<$m::Result<Vec<_>>>()
                })));
            }
            for flag in [0_i64, 1] {
                trace.push(show(
                    conn.prepare("SELECT count(*) FROM t WHERE ?1 = 1")
                        .and_then(|mut stmt| {
                            stmt.query_row(params![flag], |row| row.get::<_, i64>(0))
                        }),
                ));
                trace.push(show(
                    conn.prepare("SELECT count(*) FROM t WHERE :on AND a < 2")
                        .and_then(|mut stmt| {
                            stmt.query_row(named_params! {":on": flag}, |row| row.get::<_, i64>(0))
                        }),
                ));
            }
            trace.push(run("UPDATE t SET b = 'zero' WHERE 0"));
            trace.push(run("UPDATE t SET b = 'one' WHERE 1 = 1 AND id = 3"));
            trace.push(run(
                "UPDATE t SET b = 'raise' WHERE abs(-9223372036854775808)",
            ));
            trace.push(run("DELETE FROM t WHERE 1 = 0"));
            trace.push(rows("DELETE FROM t WHERE 0 RETURNING quote(id)"));
            for flag in [0_i64, 1] {
                trace.push(show(
                    conn.prepare("DELETE FROM t WHERE ?1 = 1 AND id > 55")
                        .and_then(|mut stmt| stmt.execute(params![flag])),
                ));
            }
            trace.push(rows(
                "SELECT quote(count(*)) || '|' || quote(sum(id)) || '|' || \
                 group_concat(b, ',') FROM t",
            ));
            trace
        }};
    }

    /// rusqlite code whose prepared GROUP BY result columns are expressions
    /// over a group key or an aggregate (bd-w4pr1.34): on a file database the
    /// code generator refused these at prepare ("not implemented"), so the
    /// statement now runs through the dispatcher. A real error in a GROUP BY
    /// statement must still fail at prepare, exactly as stock reports it.
    macro_rules! group_by_expression_scenario {
        ($m:ident, $conn:expr) => {{
            use $m::params;
            fn show<T: std::fmt::Debug>(result: $m::Result<T>) -> String {
                match result {
                    Ok(value) => format!("ok {value:?}"),
                    Err($m::Error::SqliteFailure(error, message)) => {
                        format!("err {:?} {} {message:?}", error.code, error.extended_code)
                    }
                    Err(other) => format!("err other {other}"),
                }
            }
            let conn: $m::Connection = $conn;
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, n INTEGER);",
            )
            .unwrap();
            {
                let mut insert = conn
                    .prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4)")
                    .unwrap();
                for id in 1..=40_i64 {
                    insert
                        .execute(params![id, id % 4, format!("k{}", id % 3), id * 10])
                        .unwrap();
                }
            }
            let mut trace: Vec<String> = Vec::new();
            for sql in [
                "SELECT quote(a) || '|' || quote(count(*)) FROM t GROUP BY a ORDER BY a",
                "SELECT a || ':' || count(*) FROM t GROUP BY a",
                "SELECT quote(count(*)) FROM t GROUP BY a ORDER BY a",
                "SELECT upper(b) FROM t GROUP BY b ORDER BY b",
                "SELECT quote(a) || '|' || quote(count(*)) FROM t WHERE 0 GROUP BY a",
                "SELECT (a + 1) || '/' || (sum(n) * 2) || '/' || max(b) FROM t \
                 WHERE id > 5 GROUP BY a ORDER BY a",
                "SELECT b || '=' || total(n) FROM t GROUP BY b HAVING count(*) > 13 ORDER BY b",
            ] {
                trace.push(show(conn.prepare(sql).and_then(|mut stmt| {
                    stmt.query_map([], |row| row.get::<_, String>(0))?
                        .collect::<$m::Result<Vec<_>>>()
                })));
            }
            trace
        }};
    }

    #[test]
    fn group_by_result_expressions_match_rusqlite() {
        let dir = tempfile::tempdir().unwrap();
        let runs = [
            (
                group_by_expression_scenario!(stock, stock::Connection::open_in_memory().unwrap()),
                group_by_expression_scenario!(ours, ours::Connection::open_in_memory().unwrap()),
            ),
            (
                group_by_expression_scenario!(
                    stock,
                    stock::Connection::open(temp_path(&dir, "group-expr-stock.db")).unwrap()
                ),
                group_by_expression_scenario!(
                    ours,
                    ours::Connection::open(temp_path(&dir, "group-expr-fsqlite.db")).unwrap()
                ),
            ),
        ];
        for (run, (theirs, mine)) in runs.iter().enumerate() {
            for (index, (expected, actual)) in theirs.iter().zip(mine).enumerate() {
                assert_eq!(actual, expected, "run {run} trace step {index}");
            }
            assert_eq!(mine.len(), theirs.len(), "run {run}");
            // Non-vacuity: the grouped answers are real rows.
            assert!(
                theirs[0].starts_with("ok [\"0|10\""),
                "run {run}: unexpected stock rows {theirs:?}"
            );
        }

        // Negative: on a file, where an uncompilable GROUP BY now falls back
        // to the dispatcher, a real error is still a prepare-time error.
        let conn = ours::Connection::open(temp_path(&dir, "group-expr-negative.db")).unwrap();
        conn.execute_batch("CREATE TABLE t(a INTEGER, b TEXT);")
            .unwrap();
        let Err(error) = conn.prepare("SELECT quote(nosuch) || count(*) FROM t GROUP BY a") else {
            panic!("an unknown column must fail at prepare");
        };
        assert!(
            error.to_string().contains("no such column: nosuch"),
            "unexpected prepare error: {error}"
        );
    }

    #[test]
    fn constant_where_terms_match_rusqlite() {
        let dir = tempfile::tempdir().unwrap();
        let runs = [
            (
                constant_where_scenario!(stock, stock::Connection::open_in_memory().unwrap()),
                constant_where_scenario!(ours, ours::Connection::open_in_memory().unwrap()),
            ),
            (
                constant_where_scenario!(
                    stock,
                    stock::Connection::open(temp_path(&dir, "const-where-stock.db")).unwrap()
                ),
                constant_where_scenario!(
                    ours,
                    ours::Connection::open(temp_path(&dir, "const-where-fsqlite.db")).unwrap()
                ),
            ),
        ];
        for (run, (theirs, mine)) in runs.iter().enumerate() {
            for (index, (expected, actual)) in theirs.iter().zip(mine).enumerate() {
                assert_eq!(actual, expected, "run {run} trace step {index}");
            }
            assert_eq!(mine.len(), theirs.len(), "run {run}");
            // The comparison must cover the shapes that matter: a constant-false
            // count that still returns its row, a raised term, and a filtered
            // DELETE that really deleted rows.
            for needle in [
                r#"ok ["0"]"#,
                "ok []",
                "err Unknown 1 Some(\"integer overflow\")",
                "ok 5",
                "ok 0",
            ] {
                assert!(
                    theirs.iter().any(|line| line == needle),
                    "run {run}: the scenario must reach {needle}: {theirs:?}"
                );
            }
        }
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
        // item-799 is id 800: 799 % 7 = 1, multiplied by 10 (id % 50 == 0).
        assert_eq!(by_index, 10);
    }

    /// bd-w4pr1.21: resetting a database the SQLite way (deleting the file
    /// and its -wal, -shm and -journal) must let the path be used again, even
    /// though FrankenSQLite's own sidecars stay behind.
    #[test]
    fn a_database_deleted_the_sqlite_way_can_be_created_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = temp_path(&dir, "reset.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE old(x); INSERT INTO old VALUES (1), (2), (3);")
                .unwrap();
        }
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let file = format!("{path}{suffix}");
            if std::path::Path::new(&file).exists() {
                std::fs::remove_file(&file).unwrap();
            }
        }
        {
            let conn = Connection::open(&path).expect("a deleted database is created again");
            let tables: i64 = conn
                .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
                .unwrap();
            assert_eq!(tables, 0, "the recreated database starts empty");
            conn.execute_batch("CREATE TABLE fresh(y); INSERT INTO fresh VALUES ('new');")
                .unwrap();
        }
        let conn = Connection::open(&path).expect("the recreated database opens again");
        let value: String = conn
            .query_row("SELECT y FROM fresh", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "new");
        assert!(
            conn.query_row("SELECT count(*) FROM old", [], |row| row.get::<_, i64>(0))
                .is_err(),
            "nothing of the deleted database comes back"
        );
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

    /// Compile the same nesting and migration-style rollback code against
    /// both APIs, then compare all intermediate rows and transaction states.
    #[test]
    fn savepoint_scopes_match_rusqlite() {
        macro_rules! savepoint_scenario {
            ($m:ident, $path:expr) => {{
                fn snapshot(conn: &$m::Connection) -> (bool, Vec<i64>) {
                    let values = conn
                        .prepare("SELECT id FROM item ORDER BY id")
                        .unwrap()
                        .query_map([], |row| row.get(0))
                        .unwrap()
                        .collect::<$m::Result<Vec<_>>>()
                        .unwrap();
                    (conn.is_autocommit(), values)
                }
                let mut conn = $m::Connection::open($path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE item(id INTEGER PRIMARY KEY); INSERT INTO item VALUES (0);",
                )
                .unwrap();
                let mut trace = Vec::new();
                {
                    let mut tx = conn.transaction().unwrap();
                    tx.execute("INSERT INTO item VALUES (1)", []).unwrap();
                    {
                        let mut outer = tx.savepoint().unwrap();
                        assert_eq!(outer.drop_behavior(), $m::DropBehavior::Rollback);
                        outer.execute("INSERT INTO item VALUES (2)", []).unwrap();
                        {
                            let inner = outer.savepoint().unwrap();
                            inner.execute("INSERT INTO item VALUES (3)", []).unwrap();
                            inner.commit().unwrap();
                        }
                        trace.push(snapshot(&outer));
                        outer.rollback().unwrap();
                        trace.push(snapshot(&outer));
                        outer.execute("INSERT INTO item VALUES (4)", []).unwrap();
                        outer.rollback().unwrap();
                        trace.push(snapshot(&outer));
                        outer.execute("INSERT INTO item VALUES (5)", []).unwrap();
                        {
                            let mut named = outer.savepoint_with_name("same name").unwrap();
                            named.execute("INSERT INTO item VALUES (6)", []).unwrap();
                            {
                                let nested = named.savepoint_with_name("same name").unwrap();
                                nested.execute("INSERT INTO item VALUES (7)", []).unwrap();
                                // Only the most recent savepoint with this name rolls back.
                            }
                            trace.push(snapshot(&named));
                            named.commit().unwrap();
                        }
                        outer.commit().unwrap();
                    }
                    trace.push(snapshot(&tx));
                    tx.rollback().unwrap();
                }
                trace.push(snapshot(&conn));
                {
                    let sp = conn
                        .savepoint_with_name("migration\"; DROP TABLE item; --")
                        .unwrap();
                    sp.execute("INSERT INTO item VALUES (10)", []).unwrap();
                    sp.commit().unwrap();
                }
                trace.push(snapshot(&conn));
                {
                    let sp = conn.savepoint().unwrap();
                    sp.execute("INSERT INTO item VALUES (11)", []).unwrap();
                }
                trace.push(snapshot(&conn));
                {
                    let mut sp = $m::Savepoint::new(&mut conn).unwrap();
                    sp.execute("INSERT INTO item VALUES (12)", []).unwrap();
                    sp.set_drop_behavior($m::DropBehavior::Commit);
                    sp.finish().unwrap();
                }
                trace.push(snapshot(&conn));
                {
                    let mut sp = $m::Savepoint::with_name(&mut conn, "manual").unwrap();
                    sp.execute("INSERT INTO item VALUES (13)", []).unwrap();
                    sp.set_drop_behavior($m::DropBehavior::Ignore);
                    sp.finish().unwrap();
                }
                trace.push(snapshot(&conn));
                conn.execute_batch("ROLLBACK TO manual; RELEASE manual")
                    .unwrap();
                trace.push(snapshot(&conn));
                {
                    let mut outer = conn.savepoint().unwrap();
                    outer.execute("INSERT INTO item VALUES (14)", []).unwrap();
                    let inner = outer.savepoint().unwrap();
                    inner.execute("INSERT INTO item VALUES (15)", []).unwrap();
                    inner.commit().unwrap();
                    // A released child is still undone when its parent rolls back.
                }
                trace.push(snapshot(&conn));
                {
                    let mut tx = conn.transaction().unwrap();
                    let sp = tx.savepoint_with_name("migration step").unwrap();
                    sp.execute("INSERT INTO item VALUES (16)", []).unwrap();
                    sp.finish().unwrap();
                    tx.commit().unwrap();
                }
                trace.push(snapshot(&conn));
                conn.close().map_err(|(_, error)| error).unwrap();
                trace
            }};
        }

        let dir = tempfile::tempdir().unwrap();
        for file_backed in [false, true] {
            let theirs = savepoint_scenario!(
                stock,
                if file_backed {
                    temp_path(&dir, "savepoints-stock.db")
                } else {
                    ":memory:".to_owned()
                }
            );
            let mine = savepoint_scenario!(
                ours,
                if file_backed {
                    temp_path(&dir, "savepoints-fsqlite.db")
                } else {
                    ":memory:".to_owned()
                }
            );
            assert_eq!(mine, theirs, "file_backed={file_backed}");
            assert_eq!(mine.last(), Some(&(true, vec![0, 10, 12])));
        }
        // Reopen through stock SQLite to verify that outer releases persist
        // only the surviving rows and leave a usable, valid database file.
        let conn = rusqlite::Connection::open(temp_path(&dir, "savepoints-fsqlite.db")).unwrap();
        let rows: Vec<i64> = conn
            .prepare("SELECT id FROM item ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows, [0, 10, 12]);
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn failed_savepoint_release_cleans_up_like_rusqlite() {
        macro_rules! failed_release {
            ($m:ident, $path:expr) => {{
                let mut conn = $m::Connection::open($path).unwrap();
                conn.execute_batch(
                    "PRAGMA foreign_keys = ON;
                     CREATE TABLE parent(id INTEGER PRIMARY KEY);
                     CREATE TABLE child(pid REFERENCES parent(id)
                         DEFERRABLE INITIALLY DEFERRED);",
                )
                .unwrap();
                let sp = conn.savepoint().unwrap();
                sp.execute("INSERT INTO child VALUES (99)", []).unwrap();
                let error = sp.commit().unwrap_err();
                let after_error = (
                    error.sqlite_error().unwrap().extended_code,
                    conn.is_autocommit(),
                    conn.query_row("SELECT count(*) FROM child", [], |row| row.get::<_, i64>(0))
                        .unwrap(),
                );
                // Releasing a nested savepoint must defer the FK check until
                // the enclosing transaction commits, allowing it to repair it.
                let mut tx = conn.transaction().unwrap();
                let sp = tx.savepoint().unwrap();
                sp.execute("INSERT INTO child VALUES (1)", []).unwrap();
                sp.commit().unwrap();
                tx.execute("INSERT INTO parent VALUES (1)", []).unwrap();
                tx.commit().unwrap();
                (
                    after_error,
                    conn.query_row("SELECT count(*) FROM child", [], |row| row.get::<_, i64>(0))
                        .unwrap(),
                )
            }};
        }
        let dir = tempfile::tempdir().unwrap();
        for file_backed in [false, true] {
            let theirs = failed_release!(
                stock,
                if file_backed {
                    temp_path(&dir, "failed-release-stock.db")
                } else {
                    ":memory:".to_owned()
                }
            );
            let mine = failed_release!(
                ours,
                if file_backed {
                    temp_path(&dir, "failed-release-fsqlite.db")
                } else {
                    ":memory:".to_owned()
                }
            );
            assert_eq!(theirs, ((787, true, 0), 1));
            assert_eq!(mine, theirs, "file_backed={file_backed}");
        }
    }

    /// A failed COMMIT consumes the Rust transaction, but SQLite leaves the
    /// SQL transaction open. Its drop policy must still clean up that work.
    #[test]
    fn failed_transaction_commit_obeys_drop_policy() {
        macro_rules! failed_commit {
            ($m:ident, $path:expr) => {{
                let mut conn = $m::Connection::open($path).unwrap();
                conn.execute_batch(
                    "PRAGMA foreign_keys = ON;
                     CREATE TABLE parent(id INTEGER PRIMARY KEY);
                     CREATE TABLE child(pid REFERENCES parent(id)
                         DEFERRABLE INITIALLY DEFERRED);
                     INSERT INTO parent VALUES (1);
                     INSERT INTO child VALUES (1);",
                )
                .unwrap();
                let mut trace = Vec::new();
                for behavior in [
                    $m::DropBehavior::Rollback,
                    $m::DropBehavior::Commit,
                    $m::DropBehavior::Ignore,
                ] {
                    let mut tx = conn.transaction().unwrap();
                    tx.set_drop_behavior(behavior);
                    tx.execute("INSERT INTO child VALUES (99)", []).unwrap();
                    let error = tx.commit().unwrap_err();
                    let rows: i64 = conn
                        .query_row("SELECT count(*) FROM child", [], |row| row.get(0))
                        .unwrap();
                    trace.push((
                        error.sqlite_error().unwrap().extended_code,
                        conn.is_autocommit(),
                        rows,
                    ));
                    if !conn.is_autocommit() {
                        conn.execute_batch("ROLLBACK").unwrap();
                    }
                    // A failed commit must not strand the connection: the
                    // next valid transaction can both begin and commit.
                    let tx = conn.transaction().unwrap();
                    tx.execute("INSERT INTO parent VALUES (2)", []).unwrap();
                    tx.execute("INSERT INTO child VALUES (2)", []).unwrap();
                    tx.commit().unwrap();
                    conn.execute_batch(
                        "DELETE FROM child WHERE pid = 2; DELETE FROM parent WHERE id = 2;",
                    )
                    .unwrap();
                }
                trace
            }};
        }

        let dir = tempfile::tempdir().unwrap();
        for file_backed in [false, true] {
            let theirs = failed_commit!(
                stock,
                if file_backed {
                    temp_path(&dir, "failed-commit-stock.db")
                } else {
                    ":memory:".to_owned()
                }
            );
            let mine = failed_commit!(
                ours,
                if file_backed {
                    temp_path(&dir, "failed-commit-fsqlite.db")
                } else {
                    ":memory:".to_owned()
                }
            );
            assert_eq!(
                theirs,
                [(787, true, 1), (787, true, 1), (787, false, 2)],
                "stock must distinguish cleanup from an explicit Ignore policy"
            );
            assert_eq!(mine, theirs, "file_backed={file_backed}");
        }
    }

    /// A future that is pending until another thread wakes it must resume: a
    /// driver that polled once, or lost a wake that arrived before it parked,
    /// would hang or return early here.
    #[test]
    fn block_on_resumes_a_future_woken_from_another_thread() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct Shared {
            ready: bool,
            waker: Option<Waker>,
        }

        struct WaitForSignal(Arc<Mutex<Shared>>);

        impl Future for WaitForSignal {
            type Output = u32;

            fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
                let mut shared = self.0.lock().unwrap();
                if shared.ready {
                    Poll::Ready(7)
                } else {
                    shared.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }

        for delay_ms in [0_u64, 1, 20] {
            let shared = Arc::new(Mutex::new(Shared::default()));
            let signal = Arc::clone(&shared);
            let waker_thread = thread::spawn(move || {
                thread::sleep(Duration::from_millis(delay_ms));
                loop {
                    let mut shared = signal.lock().unwrap();
                    if let Some(waker) = shared.waker.take() {
                        shared.ready = true;
                        drop(shared);
                        waker.wake();
                        return;
                    }
                    drop(shared);
                    thread::yield_now();
                }
            });
            assert_eq!(block_on(WaitForSignal(shared)), 7);
            waker_thread.join().unwrap();
        }
    }

    /// Two connections on two threads: a writer that waits for the lock gets
    /// it once the holder commits, and a wait longer than `busy_timeout`
    /// fails with SQLITE_BUSY, as rusqlite's does. Both depend on the engine
    /// waking, or timing out, a call that is parked in `block_on`.
    #[test]
    fn a_blocked_writer_waits_for_the_lock_and_honours_busy_timeout() {
        use std::sync::mpsc;
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let path = temp_path(&dir, "busy.db");
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(x);")
            .unwrap();

        // `hold` is how long the first writer keeps its transaction open after
        // the second one has been told to start.
        let run = |hold: Duration, timeout: Duration| {
            let (locked_tx, locked_rx) = mpsc::channel();
            let holder_path = path.clone();
            let holder = thread::spawn(move || {
                let conn = Connection::open(&holder_path).unwrap();
                conn.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
                    .unwrap();
                locked_tx.send(()).unwrap();
                thread::sleep(hold);
                conn.execute_batch("COMMIT;").unwrap();
            });
            locked_rx.recv().unwrap();
            let conn = Connection::open(&path).unwrap();
            conn.busy_timeout(timeout).unwrap();
            let started = Instant::now();
            let outcome = conn.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (2); COMMIT;");
            let waited = started.elapsed();
            holder.join().unwrap();
            (outcome, waited)
        };

        let (outcome, waited) = run(Duration::from_millis(300), Duration::from_secs(20));
        outcome.expect("the waiting writer gets the lock once the holder commits");
        assert!(
            waited < Duration::from_secs(10),
            "waited {waited:?} for a lock held for 300 ms"
        );

        let (outcome, waited) = run(Duration::from_millis(1500), Duration::from_millis(100));
        match outcome {
            Err(Error::SqliteFailure(error, _)) => {
                assert_eq!(error.code, ErrorCode::DatabaseBusy);
            }
            other => panic!("expected SQLITE_BUSY after the timeout, got {other:?}"),
        }
        assert!(
            waited < Duration::from_millis(1400),
            "busy_timeout of 100 ms returned after {waited:?}, not before the holder released"
        );
    }

    /// A connection owned by a thread-local is dropped while the thread shuts
    /// down. The drop still closes it: the open transaction is rolled back
    /// and the file opens cleanly afterwards.
    #[test]
    fn a_connection_dropped_during_thread_teardown_still_closes() {
        thread_local! {
            static HELD: RefCell<Option<Connection>> = const { RefCell::new(None) };
        }

        let dir = tempfile::tempdir().unwrap();
        let path = temp_path(&dir, "teardown.db");
        let worker_path = path.clone();
        thread::spawn(move || {
            let conn = Connection::open(&worker_path).unwrap();
            conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
                .unwrap();
            conn.execute_batch("BEGIN; INSERT INTO t VALUES (2);")
                .unwrap();
            HELD.with(|held| *held.borrow_mut() = Some(conn));
        })
        .join()
        .unwrap();

        let conn = Connection::open(&path).unwrap();
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "the uncommitted row must not survive the thread");
        conn.execute("INSERT INTO t VALUES (3)", []).unwrap();
        let check: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(check, "ok");
    }

    /// `u64`/`usize` convert with range checks (rusqlite has them only behind
    /// its `fallible_uint` feature, so there is no stock oracle here).
    #[test]
    fn unsigned_conversions_are_range_checked() {
        let conn = Connection::open_in_memory().unwrap();
        let big: u64 = conn
            .query_row("SELECT ?1", [i64::MAX as u64], |row| row.get(0))
            .unwrap();
        assert_eq!(big, i64::MAX as u64);
        assert!(matches!(
            conn.query_row("SELECT ?1", [u64::MAX], |row| row.get::<_, u64>(0)),
            Err(Error::ToSqlConversionFailure(_))
        ));
        assert!(matches!(
            conn.query_row("SELECT -1", [], |row| row.get::<_, u64>(0)),
            Err(Error::IntegralValueOutOfRange(0, -1))
        ));
        assert!(matches!(
            conn.query_row("SELECT -1", [], |row| row.get::<_, usize>(0)),
            Err(Error::IntegralValueOutOfRange(0, -1))
        ));
    }
}
