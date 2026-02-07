use std::path::PathBuf;

use thiserror::Error;

/// Primary error type for FrankenSQLite operations.
///
/// Modeled after SQLite's error codes with Rust-idiomatic structure.
/// Follows the pattern from beads_rust: structured variants for common cases,
/// recovery hints for user-facing errors.
#[derive(Error, Debug)]
pub enum FrankenError {
    // === Database Errors ===
    /// Database file not found.
    #[error("database not found: '{path}'")]
    DatabaseNotFound { path: PathBuf },

    /// Database file is locked by another process.
    #[error("database is locked: '{path}'")]
    DatabaseLocked { path: PathBuf },

    /// Database file is corrupt.
    #[error("database disk image is malformed: {detail}")]
    DatabaseCorrupt { detail: String },

    /// Database file is not a valid SQLite database.
    #[error("file is not a database: '{path}'")]
    NotADatabase { path: PathBuf },

    /// Database is full (max page count reached).
    #[error("database is full")]
    DatabaseFull,

    /// Database schema has changed since the statement was prepared.
    #[error("database schema has changed")]
    SchemaChanged,

    // === I/O Errors ===
    /// File I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Disk I/O error during database read.
    #[error("disk I/O error reading page {page}")]
    IoRead { page: u32 },

    /// Disk I/O error during database write.
    #[error("disk I/O error writing page {page}")]
    IoWrite { page: u32 },

    /// Short read (fewer bytes than expected).
    #[error("short read: expected {expected} bytes, got {actual}")]
    ShortRead { expected: usize, actual: usize },

    // === SQL Errors ===
    /// SQL syntax error.
    #[error("near \"{token}\": syntax error")]
    SyntaxError { token: String },

    /// SQL parsing error at a specific position.
    #[error("SQL error at offset {offset}: {detail}")]
    ParseError { offset: usize, detail: String },

    /// No such table.
    #[error("no such table: {name}")]
    NoSuchTable { name: String },

    /// No such column.
    #[error("no such column: {name}")]
    NoSuchColumn { name: String },

    /// No such index.
    #[error("no such index: {name}")]
    NoSuchIndex { name: String },

    /// Table already exists.
    #[error("table {name} already exists")]
    TableExists { name: String },

    /// Index already exists.
    #[error("index {name} already exists")]
    IndexExists { name: String },

    /// Ambiguous column reference.
    #[error("ambiguous column name: {name}")]
    AmbiguousColumn { name: String },

    // === Constraint Errors ===
    /// UNIQUE constraint violation.
    #[error("UNIQUE constraint failed: {columns}")]
    UniqueViolation { columns: String },

    /// NOT NULL constraint violation.
    #[error("NOT NULL constraint failed: {column}")]
    NotNullViolation { column: String },

    /// CHECK constraint violation.
    #[error("CHECK constraint failed: {name}")]
    CheckViolation { name: String },

    /// FOREIGN KEY constraint violation.
    #[error("FOREIGN KEY constraint failed")]
    ForeignKeyViolation,

    /// PRIMARY KEY constraint violation.
    #[error("PRIMARY KEY constraint failed")]
    PrimaryKeyViolation,

    // === Transaction Errors ===
    /// Cannot start a transaction within a transaction.
    #[error("cannot start a transaction within a transaction")]
    NestedTransaction,

    /// No transaction is active.
    #[error("cannot commit - no transaction is active")]
    NoActiveTransaction,

    /// Transaction was rolled back due to constraint violation.
    #[error("transaction rolled back: {reason}")]
    TransactionRolledBack { reason: String },

    // === MVCC Errors ===
    /// Page-level write conflict (another transaction modified the same page).
    #[error("write conflict on page {page}: held by transaction {holder}")]
    WriteConflict { page: u32, holder: u64 },

    /// Serialization failure (first-committer-wins violation).
    #[error("serialization failure: page {page} was modified after snapshot")]
    SerializationFailure { page: u32 },

    /// Snapshot is too old (required versions have been garbage collected).
    #[error("snapshot too old: transaction {txn_id} is below GC horizon")]
    SnapshotTooOld { txn_id: u64 },

    // === BUSY ===
    /// Database is busy (the SQLite classic).
    #[error("database is busy")]
    Busy,

    /// Database is busy due to recovery.
    #[error("database is busy (recovery in progress)")]
    BusyRecovery,

    // === Type Errors ===
    /// Type mismatch in column access.
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Integer overflow during computation.
    #[error("integer overflow")]
    IntegerOverflow,

    /// Value out of range.
    #[error("{what} out of range: {value}")]
    OutOfRange { what: String, value: String },

    // === Limit Errors ===
    /// String or BLOB exceeds the size limit.
    #[error("string or BLOB exceeds size limit")]
    TooBig,

    /// Too many columns.
    #[error("too many columns: {count} (max {max})")]
    TooManyColumns { count: usize, max: usize },

    /// SQL statement too long.
    #[error("SQL statement too long: {length} bytes (max {max})")]
    SqlTooLong { length: usize, max: usize },

    /// Expression tree too deep.
    #[error("expression tree too deep (max {max})")]
    ExpressionTooDeep { max: usize },

    /// Too many attached databases.
    #[error("too many attached databases (max {max})")]
    TooManyAttached { max: usize },

    /// Too many function arguments.
    #[error("too many arguments to function {name}")]
    TooManyArguments { name: String },

    // === WAL Errors ===
    /// WAL file is corrupt.
    #[error("WAL file is corrupt: {detail}")]
    WalCorrupt { detail: String },

    /// WAL checkpoint failed.
    #[error("WAL checkpoint failed: {detail}")]
    CheckpointFailed { detail: String },

    // === VFS Errors ===
    /// File locking failed.
    #[error("file locking failed: {detail}")]
    LockFailed { detail: String },

    /// Cannot open file.
    #[error("unable to open database file: '{path}'")]
    CannotOpen { path: PathBuf },

    // === Internal Errors ===
    /// Internal logic error (should never happen).
    #[error("internal error: {0}")]
    Internal(String),

    /// Operation is not supported by the current backend or configuration.
    #[error("unsupported operation")]
    Unsupported,

    /// Feature not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Abort due to callback.
    #[error("callback requested query abort")]
    Abort,

    /// Authorization denied.
    #[error("authorization denied")]
    AuthDenied,

    /// Out of memory.
    #[error("out of memory")]
    OutOfMemory,
}

/// SQLite result/error codes for wire protocol compatibility.
///
/// These match the numeric values from C SQLite's `sqlite3.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorCode {
    /// Successful result.
    Ok = 0,
    /// Generic error.
    Error = 1,
    /// Internal logic error.
    Internal = 2,
    /// Access permission denied.
    Perm = 3,
    /// Callback requested abort.
    Abort = 4,
    /// Database file is locked.
    Busy = 5,
    /// Table is locked.
    Locked = 6,
    /// Out of memory.
    NoMem = 7,
    /// Attempt to write a read-only database.
    ReadOnly = 8,
    /// Interrupted by `sqlite3_interrupt()`.
    Interrupt = 9,
    /// Disk I/O error.
    IoErr = 10,
    /// Database disk image is malformed.
    Corrupt = 11,
    /// Not found (internal).
    NotFound = 12,
    /// Database or disk is full.
    Full = 13,
    /// Unable to open database file.
    CantOpen = 14,
    /// Locking protocol error.
    Protocol = 15,
    /// (Not used).
    Empty = 16,
    /// Database schema has changed.
    Schema = 17,
    /// String or BLOB exceeds size limit.
    TooBig = 18,
    /// Constraint violation.
    Constraint = 19,
    /// Data type mismatch.
    Mismatch = 20,
    /// Library used incorrectly.
    Misuse = 21,
    /// OS feature not available.
    NoLfs = 22,
    /// Authorization denied.
    Auth = 23,
    /// Not used.
    Format = 24,
    /// Bind parameter out of range.
    Range = 25,
    /// Not a database file.
    NotADb = 26,
    /// Notification (not an error).
    Notice = 27,
    /// Warning (not an error).
    Warning = 28,
    /// `sqlite3_step()` has another row ready.
    Row = 100,
    /// `sqlite3_step()` has finished executing.
    Done = 101,
}

impl FrankenError {
    /// Map this error to a SQLite error code for compatibility.
    #[allow(clippy::match_same_arms)]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::DatabaseNotFound { .. } | Self::CannotOpen { .. } => ErrorCode::CantOpen,
            Self::DatabaseLocked { .. } => ErrorCode::Locked,
            Self::DatabaseCorrupt { .. } | Self::WalCorrupt { .. } => ErrorCode::Corrupt,
            Self::NotADatabase { .. } => ErrorCode::NotADb,
            Self::DatabaseFull => ErrorCode::Full,
            Self::SchemaChanged => ErrorCode::Schema,
            Self::Io(_)
            | Self::IoRead { .. }
            | Self::IoWrite { .. }
            | Self::ShortRead { .. }
            | Self::CheckpointFailed { .. } => ErrorCode::IoErr,
            Self::SyntaxError { .. }
            | Self::ParseError { .. }
            | Self::NoSuchTable { .. }
            | Self::NoSuchColumn { .. }
            | Self::NoSuchIndex { .. }
            | Self::TableExists { .. }
            | Self::IndexExists { .. }
            | Self::AmbiguousColumn { .. }
            | Self::NestedTransaction
            | Self::NoActiveTransaction
            | Self::TransactionRolledBack { .. }
            | Self::TooManyColumns { .. }
            | Self::SqlTooLong { .. }
            | Self::ExpressionTooDeep { .. }
            | Self::TooManyAttached { .. }
            | Self::TooManyArguments { .. }
            | Self::NotImplemented(_) => ErrorCode::Error,
            Self::UniqueViolation { .. }
            | Self::NotNullViolation { .. }
            | Self::CheckViolation { .. }
            | Self::ForeignKeyViolation
            | Self::PrimaryKeyViolation => ErrorCode::Constraint,
            Self::WriteConflict { .. }
            | Self::SerializationFailure { .. }
            | Self::Busy
            | Self::BusyRecovery
            | Self::SnapshotTooOld { .. }
            | Self::LockFailed { .. } => ErrorCode::Busy,
            Self::TypeMismatch { .. } => ErrorCode::Mismatch,
            Self::IntegerOverflow | Self::OutOfRange { .. } => ErrorCode::Range,
            Self::TooBig => ErrorCode::TooBig,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Abort => ErrorCode::Abort,
            Self::AuthDenied => ErrorCode::Auth,
            Self::OutOfMemory => ErrorCode::NoMem,
        }
    }

    /// Whether the user can likely fix this without code changes.
    pub const fn is_user_recoverable(&self) -> bool {
        matches!(
            self,
            Self::DatabaseNotFound { .. }
                | Self::DatabaseLocked { .. }
                | Self::Busy
                | Self::BusyRecovery
                | Self::SyntaxError { .. }
                | Self::ParseError { .. }
                | Self::NoSuchTable { .. }
                | Self::NoSuchColumn { .. }
                | Self::TypeMismatch { .. }
                | Self::CannotOpen { .. }
        )
    }

    /// Human-friendly suggestion for fixing this error.
    pub const fn suggestion(&self) -> Option<&'static str> {
        match self {
            Self::DatabaseNotFound { .. } => Some("Check the file path or create a new database"),
            Self::DatabaseLocked { .. } => {
                Some("Close other connections or wait for the lock to be released")
            }
            Self::Busy | Self::BusyRecovery => Some("Retry the operation after a short delay"),
            Self::WriteConflict { .. } | Self::SerializationFailure { .. } => {
                Some("Retry the transaction; the conflict is transient")
            }
            Self::SnapshotTooOld { .. } => Some("Begin a new transaction to get a fresh snapshot"),
            Self::DatabaseCorrupt { .. } => {
                Some("Run PRAGMA integrity_check; restore from backup if needed")
            }
            Self::TooBig => Some("Reduce the size of the value being inserted"),
            Self::NotImplemented(_) => Some("This feature is not yet available in FrankenSQLite"),
            _ => None,
        }
    }

    /// Whether this is a transient error that may succeed on retry.
    pub const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Busy
                | Self::BusyRecovery
                | Self::DatabaseLocked { .. }
                | Self::WriteConflict { .. }
                | Self::SerializationFailure { .. }
        )
    }

    /// Get the process exit code for this error (for CLI use).
    pub const fn exit_code(&self) -> i32 {
        self.error_code() as i32
    }

    /// Create a syntax error.
    pub fn syntax(token: impl Into<String>) -> Self {
        Self::SyntaxError {
            token: token.into(),
        }
    }

    /// Create a parse error.
    pub fn parse(offset: usize, detail: impl Into<String>) -> Self {
        Self::ParseError {
            offset,
            detail: detail.into(),
        }
    }

    /// Create an internal error.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Create a not-implemented error.
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Self::NotImplemented(feature.into())
    }
}

/// Result type alias using `FrankenError`.
pub type Result<T> = std::result::Result<T, FrankenError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let err = FrankenError::syntax("SELEC");
        assert_eq!(err.to_string(), r#"near "SELEC": syntax error"#);
    }

    #[test]
    fn error_display_corrupt() {
        let err = FrankenError::DatabaseCorrupt {
            detail: "invalid page header".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "database disk image is malformed: invalid page header"
        );
    }

    #[test]
    fn error_display_write_conflict() {
        let err = FrankenError::WriteConflict {
            page: 42,
            holder: 7,
        };
        assert_eq!(
            err.to_string(),
            "write conflict on page 42: held by transaction 7"
        );
    }

    #[test]
    fn error_code_mapping() {
        assert_eq!(FrankenError::syntax("x").error_code(), ErrorCode::Error);
        assert_eq!(FrankenError::Busy.error_code(), ErrorCode::Busy);
        assert_eq!(
            FrankenError::DatabaseCorrupt {
                detail: String::new()
            }
            .error_code(),
            ErrorCode::Corrupt
        );
        assert_eq!(FrankenError::DatabaseFull.error_code(), ErrorCode::Full);
        assert_eq!(FrankenError::TooBig.error_code(), ErrorCode::TooBig);
        assert_eq!(FrankenError::OutOfMemory.error_code(), ErrorCode::NoMem);
        assert_eq!(FrankenError::AuthDenied.error_code(), ErrorCode::Auth);
    }

    #[test]
    fn user_recoverable() {
        assert!(FrankenError::Busy.is_user_recoverable());
        assert!(FrankenError::syntax("x").is_user_recoverable());
        assert!(!FrankenError::internal("bug").is_user_recoverable());
        assert!(!FrankenError::DatabaseFull.is_user_recoverable());
    }

    #[test]
    fn is_transient() {
        assert!(FrankenError::Busy.is_transient());
        assert!(FrankenError::BusyRecovery.is_transient());
        assert!(FrankenError::WriteConflict { page: 1, holder: 1 }.is_transient());
        assert!(!FrankenError::DatabaseFull.is_transient());
        assert!(!FrankenError::syntax("x").is_transient());
    }

    #[test]
    fn suggestions() {
        assert!(FrankenError::Busy.suggestion().is_some());
        assert!(FrankenError::not_implemented("CTE").suggestion().is_some());
        assert!(FrankenError::DatabaseFull.suggestion().is_none());
    }

    #[test]
    fn convenience_constructors() {
        // Keep test strings clearly non-sensitive so UBS doesn't flag them as secrets.
        let expected_kw = "kw_where";
        let err = FrankenError::syntax(expected_kw);
        assert!(matches!(
            err,
            FrankenError::SyntaxError { token: got_kw } if got_kw == expected_kw
        ));

        let err = FrankenError::parse(42, "unexpected token");
        assert!(matches!(err, FrankenError::ParseError { offset: 42, .. }));

        let err = FrankenError::internal("assertion failed");
        assert!(matches!(err, FrankenError::Internal(msg) if msg == "assertion failed"));

        let err = FrankenError::not_implemented("window functions");
        assert!(matches!(err, FrankenError::NotImplemented(msg) if msg == "window functions"));
    }

    #[test]
    fn io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: FrankenError = io_err.into();
        assert!(matches!(err, FrankenError::Io(_)));
        assert_eq!(err.error_code(), ErrorCode::IoErr);
    }

    #[test]
    fn error_code_values() {
        assert_eq!(ErrorCode::Ok as i32, 0);
        assert_eq!(ErrorCode::Error as i32, 1);
        assert_eq!(ErrorCode::Busy as i32, 5);
        assert_eq!(ErrorCode::Constraint as i32, 19);
        assert_eq!(ErrorCode::Row as i32, 100);
        assert_eq!(ErrorCode::Done as i32, 101);
    }

    #[test]
    fn exit_code() {
        assert_eq!(FrankenError::Busy.exit_code(), 5);
        assert_eq!(FrankenError::internal("x").exit_code(), 2);
        assert_eq!(FrankenError::syntax("x").exit_code(), 1);
    }

    #[test]
    fn constraint_errors() {
        let err = FrankenError::UniqueViolation {
            columns: "users.email".to_owned(),
        };
        assert_eq!(err.to_string(), "UNIQUE constraint failed: users.email");
        assert_eq!(err.error_code(), ErrorCode::Constraint);

        let err = FrankenError::NotNullViolation {
            column: "name".to_owned(),
        };
        assert_eq!(err.to_string(), "NOT NULL constraint failed: name");

        assert_eq!(
            FrankenError::ForeignKeyViolation.to_string(),
            "FOREIGN KEY constraint failed"
        );
    }

    #[test]
    fn mvcc_errors() {
        let err = FrankenError::WriteConflict {
            page: 5,
            holder: 10,
        };
        assert!(err.is_transient());
        assert_eq!(err.error_code(), ErrorCode::Busy);

        let err = FrankenError::SerializationFailure { page: 5 };
        assert!(err.is_transient());

        let err = FrankenError::SnapshotTooOld { txn_id: 42 };
        assert!(!err.is_transient());
        assert!(err.suggestion().is_some());
    }
}
