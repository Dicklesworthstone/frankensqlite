//! End-to-end differential testing and benchmark harness for FrankenSQLite.
//!
//! This crate provides the infrastructure for:
//! - **Golden copy management**: loading, hashing, and comparing database snapshots
//! - **Workload generation**: deterministic, seeded workload creation
//! - **Differential comparison**: running identical SQL against FrankenSQLite and C SQLite
//! - **Corruption injection**: byte/page/sector-level corruption for recovery testing

pub mod canonicalize;
pub mod comparison;
pub mod corruption;
pub mod executor;
pub mod fsqlite_executor;
pub mod golden;
pub mod oplog;
pub mod report;
pub mod run_workspace;
pub mod sqlite_executor;
pub mod workload;

/// Result type alias used throughout the E2E harness.
pub type E2eResult<T> = Result<T, E2eError>;

/// Errors that can arise during E2E testing.
#[derive(Debug, thiserror::Error)]
pub enum E2eError {
    /// An I/O error from the filesystem.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// A FrankenSQLite error.
    #[error("fsqlite: {0}")]
    Fsqlite(String),

    /// A C SQLite (rusqlite) error.
    #[error("rusqlite: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    /// Hash mismatch on a golden copy.
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    /// A result divergence between the two engines.
    #[error("divergence: {0}")]
    Divergence(String),
}
