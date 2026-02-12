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

    /// Query executed successfully but produced no rows.
    #[error("query returned no rows")]
    QueryReturnedNoRows,

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

    /// Concurrent transaction commit failed due to page conflict (SQLITE_BUSY_SNAPSHOT).
    /// Another transaction committed changes to pages in the write set since the
    /// snapshot was established.
    #[error("database is busy (snapshot conflict on pages: {conflicting_pages})")]
    BusySnapshot { conflicting_pages: String },

    /// BEGIN CONCURRENT is not available without fsqlite-shm (§5.6.6.2).
    #[error(
        "BEGIN CONCURRENT unavailable: fsqlite-shm not present (multi-writer MVCC requires shared memory coordination)"
    )]
    ConcurrentUnavailable,

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

    /// SQL function domain/runtime error (analogous to `sqlite3_result_error`).
    #[error("{0}")]
    FunctionError(String),

    /// Attempt to write a read-only database or virtual table.
    #[error("attempt to write a readonly database")]
    ReadOnly,
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
            Self::DatabaseLocked { .. } => ErrorCode::Busy,
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
            | Self::QueryReturnedNoRows
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
            | Self::NotImplemented(_)
            | Self::FunctionError(_)
            | Self::ConcurrentUnavailable => ErrorCode::Error,
            Self::UniqueViolation { .. }
            | Self::NotNullViolation { .. }
            | Self::CheckViolation { .. }
            | Self::ForeignKeyViolation
            | Self::PrimaryKeyViolation => ErrorCode::Constraint,
            Self::WriteConflict { .. }
            | Self::SerializationFailure { .. }
            | Self::Busy
            | Self::BusyRecovery
            | Self::BusySnapshot { .. }
            | Self::SnapshotTooOld { .. }
            | Self::LockFailed { .. } => ErrorCode::Busy,
            Self::TypeMismatch { .. } => ErrorCode::Mismatch,
            Self::IntegerOverflow | Self::OutOfRange { .. } => ErrorCode::Range,
            Self::TooBig => ErrorCode::TooBig,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Abort => ErrorCode::Abort,
            Self::AuthDenied => ErrorCode::Auth,
            Self::OutOfMemory => ErrorCode::NoMem,
            Self::Unsupported => ErrorCode::NoLfs,
            Self::ReadOnly => ErrorCode::ReadOnly,
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
                | Self::BusySnapshot { .. }
                | Self::Unsupported
                | Self::SyntaxError { .. }
                | Self::ParseError { .. }
                | Self::QueryReturnedNoRows
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
            Self::BusySnapshot { .. } => {
                Some("Retry the transaction; another writer committed to the same pages")
            }
            Self::WriteConflict { .. } | Self::SerializationFailure { .. } => {
                Some("Retry the transaction; the conflict is transient")
            }
            Self::SnapshotTooOld { .. } => Some("Begin a new transaction to get a fresh snapshot"),
            Self::DatabaseCorrupt { .. } => {
                Some("Run PRAGMA integrity_check; restore from backup if needed")
            }
            Self::TooBig => Some("Reduce the size of the value being inserted"),
            Self::NotImplemented(_) => Some("This feature is not yet available in FrankenSQLite"),
            Self::ConcurrentUnavailable => Some(
                "Use a filesystem that supports shared memory, or use BEGIN (serialized) instead",
            ),
            Self::QueryReturnedNoRows => Some("Use query() when zero rows are acceptable"),
            _ => None,
        }
    }

    /// Whether this is a transient error that may succeed on retry.
    pub const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Busy
                | Self::BusyRecovery
                | Self::BusySnapshot { .. }
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

    /// Create a function domain error.
    pub fn function_error(msg: impl Into<String>) -> Self {
        Self::FunctionError(msg.into())
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
        assert_eq!(
            FrankenError::QueryReturnedNoRows.error_code(),
            ErrorCode::Error
        );
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
        assert!(FrankenError::QueryReturnedNoRows.is_user_recoverable());
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

    // ---- Additional comprehensive tests for bd-2ddl coverage ----

    #[test]
    fn display_database_not_found() {
        let err = FrankenError::DatabaseNotFound {
            path: PathBuf::from("/tmp/test.db"),
        };
        assert_eq!(err.to_string(), "database not found: '/tmp/test.db'");
    }

    #[test]
    fn display_database_locked() {
        let err = FrankenError::DatabaseLocked {
            path: PathBuf::from("/tmp/test.db"),
        };
        assert_eq!(err.to_string(), "database is locked: '/tmp/test.db'");
    }

    #[test]
    fn display_not_a_database() {
        let err = FrankenError::NotADatabase {
            path: PathBuf::from("/tmp/random.bin"),
        };
        assert_eq!(err.to_string(), "file is not a database: '/tmp/random.bin'");
    }

    #[test]
    fn display_database_full() {
        assert_eq!(FrankenError::DatabaseFull.to_string(), "database is full");
    }

    #[test]
    fn display_schema_changed() {
        assert_eq!(
            FrankenError::SchemaChanged.to_string(),
            "database schema has changed"
        );
    }

    #[test]
    fn display_io_read_write() {
        let err = FrankenError::IoRead { page: 17 };
        assert_eq!(err.to_string(), "disk I/O error reading page 17");

        let err = FrankenError::IoWrite { page: 42 };
        assert_eq!(err.to_string(), "disk I/O error writing page 42");
    }

    #[test]
    fn display_short_read() {
        let err = FrankenError::ShortRead {
            expected: 4096,
            actual: 2048,
        };
        assert_eq!(err.to_string(), "short read: expected 4096 bytes, got 2048");
    }

    #[test]
    fn display_no_such_table_column_index() {
        assert_eq!(
            FrankenError::NoSuchTable {
                name: "users".to_owned()
            }
            .to_string(),
            "no such table: users"
        );
        assert_eq!(
            FrankenError::NoSuchColumn {
                name: "email".to_owned()
            }
            .to_string(),
            "no such column: email"
        );
        assert_eq!(
            FrankenError::NoSuchIndex {
                name: "idx_email".to_owned()
            }
            .to_string(),
            "no such index: idx_email"
        );
    }

    #[test]
    fn display_already_exists() {
        assert_eq!(
            FrankenError::TableExists {
                name: "t1".to_owned()
            }
            .to_string(),
            "table t1 already exists"
        );
        assert_eq!(
            FrankenError::IndexExists {
                name: "i1".to_owned()
            }
            .to_string(),
            "index i1 already exists"
        );
    }

    #[test]
    fn display_ambiguous_column() {
        let err = FrankenError::AmbiguousColumn {
            name: "id".to_owned(),
        };
        assert_eq!(err.to_string(), "ambiguous column name: id");
    }

    #[test]
    fn display_transaction_errors() {
        assert_eq!(
            FrankenError::NestedTransaction.to_string(),
            "cannot start a transaction within a transaction"
        );
        assert_eq!(
            FrankenError::NoActiveTransaction.to_string(),
            "cannot commit - no transaction is active"
        );
        assert_eq!(
            FrankenError::TransactionRolledBack {
                reason: "constraint".to_owned()
            }
            .to_string(),
            "transaction rolled back: constraint"
        );
    }

    #[test]
    fn display_serialization_failure() {
        let err = FrankenError::SerializationFailure { page: 99 };
        assert_eq!(
            err.to_string(),
            "serialization failure: page 99 was modified after snapshot"
        );
    }

    #[test]
    fn display_snapshot_too_old() {
        let err = FrankenError::SnapshotTooOld { txn_id: 100 };
        assert_eq!(
            err.to_string(),
            "snapshot too old: transaction 100 is below GC horizon"
        );
    }

    #[test]
    fn display_busy_variants() {
        assert_eq!(FrankenError::Busy.to_string(), "database is busy");
        assert_eq!(
            FrankenError::BusyRecovery.to_string(),
            "database is busy (recovery in progress)"
        );
    }

    #[test]
    fn display_concurrent_unavailable() {
        let err = FrankenError::ConcurrentUnavailable;
        assert!(err.to_string().contains("BEGIN CONCURRENT unavailable"));
    }

    #[test]
    fn display_type_errors() {
        let err = FrankenError::TypeMismatch {
            expected: "INTEGER".to_owned(),
            actual: "TEXT".to_owned(),
        };
        assert_eq!(err.to_string(), "type mismatch: expected INTEGER, got TEXT");

        assert_eq!(
            FrankenError::IntegerOverflow.to_string(),
            "integer overflow"
        );

        let err = FrankenError::OutOfRange {
            what: "page number".to_owned(),
            value: "0".to_owned(),
        };
        assert_eq!(err.to_string(), "page number out of range: 0");
    }

    #[test]
    fn display_limit_errors() {
        assert_eq!(
            FrankenError::TooBig.to_string(),
            "string or BLOB exceeds size limit"
        );

        let err = FrankenError::TooManyColumns {
            count: 2001,
            max: 2000,
        };
        assert_eq!(err.to_string(), "too many columns: 2001 (max 2000)");

        let err = FrankenError::SqlTooLong {
            length: 2_000_000,
            max: 1_000_000,
        };
        assert_eq!(
            err.to_string(),
            "SQL statement too long: 2000000 bytes (max 1000000)"
        );

        let err = FrankenError::ExpressionTooDeep { max: 1000 };
        assert_eq!(err.to_string(), "expression tree too deep (max 1000)");

        let err = FrankenError::TooManyAttached { max: 10 };
        assert_eq!(err.to_string(), "too many attached databases (max 10)");

        let err = FrankenError::TooManyArguments {
            name: "my_func".to_owned(),
        };
        assert_eq!(err.to_string(), "too many arguments to function my_func");
    }

    #[test]
    fn display_wal_errors() {
        let err = FrankenError::WalCorrupt {
            detail: "invalid checksum".to_owned(),
        };
        assert_eq!(err.to_string(), "WAL file is corrupt: invalid checksum");

        let err = FrankenError::CheckpointFailed {
            detail: "busy".to_owned(),
        };
        assert_eq!(err.to_string(), "WAL checkpoint failed: busy");
    }

    #[test]
    fn display_vfs_errors() {
        let err = FrankenError::LockFailed {
            detail: "permission denied".to_owned(),
        };
        assert_eq!(err.to_string(), "file locking failed: permission denied");

        let err = FrankenError::CannotOpen {
            path: PathBuf::from("/readonly/test.db"),
        };
        assert_eq!(
            err.to_string(),
            "unable to open database file: '/readonly/test.db'"
        );
    }

    #[test]
    fn display_internal_errors() {
        assert_eq!(
            FrankenError::Internal("assertion failed".to_owned()).to_string(),
            "internal error: assertion failed"
        );
        assert_eq!(
            FrankenError::Unsupported.to_string(),
            "unsupported operation"
        );
        assert_eq!(
            FrankenError::NotImplemented("CTE".to_owned()).to_string(),
            "not implemented: CTE"
        );
        assert_eq!(
            FrankenError::Abort.to_string(),
            "callback requested query abort"
        );
        assert_eq!(FrankenError::AuthDenied.to_string(), "authorization denied");
        assert_eq!(FrankenError::OutOfMemory.to_string(), "out of memory");
        assert_eq!(
            FrankenError::ReadOnly.to_string(),
            "attempt to write a readonly database"
        );
    }

    #[test]
    fn display_function_error() {
        let err = FrankenError::FunctionError("domain error".to_owned());
        assert_eq!(err.to_string(), "domain error");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn error_code_comprehensive_mapping() {
        // Database errors
        assert_eq!(
            FrankenError::DatabaseNotFound {
                path: PathBuf::new()
            }
            .error_code(),
            ErrorCode::CantOpen
        );
        assert_eq!(
            FrankenError::DatabaseLocked {
                path: PathBuf::new()
            }
            .error_code(),
            ErrorCode::Busy
        );
        assert_eq!(
            FrankenError::NotADatabase {
                path: PathBuf::new()
            }
            .error_code(),
            ErrorCode::NotADb
        );
        assert_eq!(FrankenError::SchemaChanged.error_code(), ErrorCode::Schema);

        // I/O errors
        assert_eq!(
            FrankenError::IoRead { page: 1 }.error_code(),
            ErrorCode::IoErr
        );
        assert_eq!(
            FrankenError::IoWrite { page: 1 }.error_code(),
            ErrorCode::IoErr
        );
        assert_eq!(
            FrankenError::ShortRead {
                expected: 1,
                actual: 0
            }
            .error_code(),
            ErrorCode::IoErr
        );

        // SQL errors map to Error
        assert_eq!(
            FrankenError::NoSuchTable {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::NoSuchColumn {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::NoSuchIndex {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::TableExists {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::IndexExists {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::AmbiguousColumn {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );

        // Transaction errors
        assert_eq!(
            FrankenError::NestedTransaction.error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::NoActiveTransaction.error_code(),
            ErrorCode::Error
        );

        // MVCC errors map to Busy
        assert_eq!(
            FrankenError::SerializationFailure { page: 1 }.error_code(),
            ErrorCode::Busy
        );
        assert_eq!(
            FrankenError::SnapshotTooOld { txn_id: 1 }.error_code(),
            ErrorCode::Busy
        );
        assert_eq!(
            FrankenError::LockFailed {
                detail: String::new()
            }
            .error_code(),
            ErrorCode::Busy
        );

        // Type errors
        assert_eq!(
            FrankenError::TypeMismatch {
                expected: String::new(),
                actual: String::new()
            }
            .error_code(),
            ErrorCode::Mismatch
        );
        assert_eq!(FrankenError::IntegerOverflow.error_code(), ErrorCode::Range);
        assert_eq!(
            FrankenError::OutOfRange {
                what: String::new(),
                value: String::new()
            }
            .error_code(),
            ErrorCode::Range
        );

        // Limit errors
        assert_eq!(
            FrankenError::TooManyColumns { count: 1, max: 1 }.error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::SqlTooLong { length: 1, max: 1 }.error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::ExpressionTooDeep { max: 1 }.error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::TooManyAttached { max: 1 }.error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::TooManyArguments {
                name: String::new()
            }
            .error_code(),
            ErrorCode::Error
        );

        // WAL errors
        assert_eq!(
            FrankenError::WalCorrupt {
                detail: String::new()
            }
            .error_code(),
            ErrorCode::Corrupt
        );
        assert_eq!(
            FrankenError::CheckpointFailed {
                detail: String::new()
            }
            .error_code(),
            ErrorCode::IoErr
        );

        // VFS errors
        assert_eq!(
            FrankenError::CannotOpen {
                path: PathBuf::new()
            }
            .error_code(),
            ErrorCode::CantOpen
        );

        // Internal/misc errors
        assert_eq!(
            FrankenError::Internal(String::new()).error_code(),
            ErrorCode::Internal
        );
        assert_eq!(FrankenError::Unsupported.error_code(), ErrorCode::NoLfs);
        assert_eq!(FrankenError::Abort.error_code(), ErrorCode::Abort);
        assert_eq!(FrankenError::ReadOnly.error_code(), ErrorCode::ReadOnly);
        assert_eq!(
            FrankenError::FunctionError(String::new()).error_code(),
            ErrorCode::Error
        );
        assert_eq!(
            FrankenError::ConcurrentUnavailable.error_code(),
            ErrorCode::Error
        );
    }

    #[test]
    fn is_user_recoverable_comprehensive() {
        // Recoverable
        assert!(
            FrankenError::DatabaseNotFound {
                path: PathBuf::new()
            }
            .is_user_recoverable()
        );
        assert!(
            FrankenError::DatabaseLocked {
                path: PathBuf::new()
            }
            .is_user_recoverable()
        );
        assert!(FrankenError::BusyRecovery.is_user_recoverable());
        assert!(FrankenError::Unsupported.is_user_recoverable());
        assert!(
            FrankenError::ParseError {
                offset: 0,
                detail: String::new()
            }
            .is_user_recoverable()
        );
        assert!(
            FrankenError::NoSuchTable {
                name: String::new()
            }
            .is_user_recoverable()
        );
        assert!(
            FrankenError::NoSuchColumn {
                name: String::new()
            }
            .is_user_recoverable()
        );
        assert!(
            FrankenError::TypeMismatch {
                expected: String::new(),
                actual: String::new()
            }
            .is_user_recoverable()
        );
        assert!(
            FrankenError::CannotOpen {
                path: PathBuf::new()
            }
            .is_user_recoverable()
        );

        // Not recoverable
        assert!(
            !FrankenError::NotADatabase {
                path: PathBuf::new()
            }
            .is_user_recoverable()
        );
        assert!(!FrankenError::TooBig.is_user_recoverable());
        assert!(!FrankenError::OutOfMemory.is_user_recoverable());
        assert!(!FrankenError::WriteConflict { page: 1, holder: 1 }.is_user_recoverable());
        assert!(
            !FrankenError::UniqueViolation {
                columns: String::new()
            }
            .is_user_recoverable()
        );
        assert!(!FrankenError::ReadOnly.is_user_recoverable());
        assert!(!FrankenError::Abort.is_user_recoverable());
    }

    #[test]
    fn is_transient_comprehensive() {
        // Transient
        assert!(
            FrankenError::DatabaseLocked {
                path: PathBuf::new()
            }
            .is_transient()
        );
        assert!(FrankenError::SerializationFailure { page: 1 }.is_transient());

        // Not transient
        assert!(
            !FrankenError::DatabaseCorrupt {
                detail: String::new()
            }
            .is_transient()
        );
        assert!(
            !FrankenError::NotADatabase {
                path: PathBuf::new()
            }
            .is_transient()
        );
        assert!(!FrankenError::TooBig.is_transient());
        assert!(!FrankenError::Internal(String::new()).is_transient());
        assert!(!FrankenError::OutOfMemory.is_transient());
        assert!(
            !FrankenError::UniqueViolation {
                columns: String::new()
            }
            .is_transient()
        );
        assert!(!FrankenError::ReadOnly.is_transient());
        assert!(!FrankenError::ConcurrentUnavailable.is_transient());
    }

    #[test]
    fn suggestion_comprehensive() {
        // Has suggestion
        assert!(
            FrankenError::DatabaseNotFound {
                path: PathBuf::new()
            }
            .suggestion()
            .is_some()
        );
        assert!(
            FrankenError::DatabaseLocked {
                path: PathBuf::new()
            }
            .suggestion()
            .is_some()
        );
        assert!(FrankenError::BusyRecovery.suggestion().is_some());
        assert!(
            FrankenError::WriteConflict { page: 1, holder: 1 }
                .suggestion()
                .is_some()
        );
        assert!(
            FrankenError::SerializationFailure { page: 1 }
                .suggestion()
                .is_some()
        );
        assert!(
            FrankenError::SnapshotTooOld { txn_id: 1 }
                .suggestion()
                .is_some()
        );
        assert!(
            FrankenError::DatabaseCorrupt {
                detail: String::new()
            }
            .suggestion()
            .is_some()
        );
        assert!(FrankenError::TooBig.suggestion().is_some());
        assert!(FrankenError::ConcurrentUnavailable.suggestion().is_some());
        assert!(FrankenError::QueryReturnedNoRows.suggestion().is_some());

        // No suggestion
        assert!(FrankenError::Abort.suggestion().is_none());
        assert!(FrankenError::AuthDenied.suggestion().is_none());
        assert!(FrankenError::OutOfMemory.suggestion().is_none());
        assert!(FrankenError::Internal(String::new()).suggestion().is_none());
        assert!(FrankenError::ReadOnly.suggestion().is_none());
    }

    #[test]
    fn error_code_enum_repr_values() {
        // Verify all ErrorCode numeric values match C SQLite
        assert_eq!(ErrorCode::Internal as i32, 2);
        assert_eq!(ErrorCode::Perm as i32, 3);
        assert_eq!(ErrorCode::Abort as i32, 4);
        assert_eq!(ErrorCode::Locked as i32, 6);
        assert_eq!(ErrorCode::NoMem as i32, 7);
        assert_eq!(ErrorCode::ReadOnly as i32, 8);
        assert_eq!(ErrorCode::Interrupt as i32, 9);
        assert_eq!(ErrorCode::IoErr as i32, 10);
        assert_eq!(ErrorCode::Corrupt as i32, 11);
        assert_eq!(ErrorCode::NotFound as i32, 12);
        assert_eq!(ErrorCode::Full as i32, 13);
        assert_eq!(ErrorCode::CantOpen as i32, 14);
        assert_eq!(ErrorCode::Protocol as i32, 15);
        assert_eq!(ErrorCode::Empty as i32, 16);
        assert_eq!(ErrorCode::Schema as i32, 17);
        assert_eq!(ErrorCode::TooBig as i32, 18);
        assert_eq!(ErrorCode::Mismatch as i32, 20);
        assert_eq!(ErrorCode::Misuse as i32, 21);
        assert_eq!(ErrorCode::NoLfs as i32, 22);
        assert_eq!(ErrorCode::Auth as i32, 23);
        assert_eq!(ErrorCode::Format as i32, 24);
        assert_eq!(ErrorCode::Range as i32, 25);
        assert_eq!(ErrorCode::NotADb as i32, 26);
        assert_eq!(ErrorCode::Notice as i32, 27);
        assert_eq!(ErrorCode::Warning as i32, 28);
    }

    #[test]
    fn error_code_clone_eq() {
        let code = ErrorCode::Busy;
        let cloned = code;
        assert_eq!(code, cloned);
        assert_eq!(code, ErrorCode::Busy);
        assert_ne!(code, ErrorCode::Error);
    }

    #[test]
    fn function_error_constructor() {
        let err = FrankenError::function_error("division by zero");
        assert!(matches!(err, FrankenError::FunctionError(ref msg) if msg == "division by zero"));
        assert_eq!(err.error_code(), ErrorCode::Error);
    }

    #[test]
    fn constraint_error_codes_all_variants() {
        assert_eq!(
            FrankenError::CheckViolation {
                name: "ck1".to_owned()
            }
            .error_code(),
            ErrorCode::Constraint
        );
        assert_eq!(
            FrankenError::PrimaryKeyViolation.error_code(),
            ErrorCode::Constraint
        );
        assert_eq!(
            FrankenError::CheckViolation {
                name: "ck1".to_owned()
            }
            .to_string(),
            "CHECK constraint failed: ck1"
        );
        assert_eq!(
            FrankenError::PrimaryKeyViolation.to_string(),
            "PRIMARY KEY constraint failed"
        );
    }

    #[test]
    fn exit_code_matches_error_code() {
        // exit_code() should be the i32 repr of error_code()
        let cases: Vec<FrankenError> = vec![
            FrankenError::DatabaseFull,
            FrankenError::TooBig,
            FrankenError::OutOfMemory,
            FrankenError::AuthDenied,
            FrankenError::Abort,
            FrankenError::ReadOnly,
            FrankenError::Unsupported,
        ];
        for err in cases {
            assert_eq!(err.exit_code(), err.error_code() as i32);
        }
    }
}
