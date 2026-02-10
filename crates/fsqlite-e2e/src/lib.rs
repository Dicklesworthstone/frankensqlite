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
pub mod corruption_scenarios;
pub mod executor;
pub mod fsqlite_executor;
pub mod golden;
pub mod oplog;
pub mod recovery_demo;
pub mod report;
pub mod run_workspace;
pub mod sqlite_executor;
pub mod workload;

/// Determinism and durability knobs that the harness sets consistently
/// on **both** sqlite3 and FrankenSQLite runs.
///
/// This struct is the single source of truth for configuration that must
/// match between the two engines to ensure fair comparison.  Convert it to
/// per-engine executor configs with [`HarnessSettings::to_sqlite3_pragmas`]
/// and [`HarnessSettings::to_fsqlite_pragmas`].
///
/// Bead: bd-1w6k.2.3
#[derive(Debug, Clone)]
pub struct HarnessSettings {
    /// Journal mode: `"wal"`, `"delete"`, `"truncate"`, etc.
    pub journal_mode: String,
    /// Synchronous level: `"OFF"`, `"NORMAL"`, `"FULL"`, `"EXTRA"`.
    pub synchronous: String,
    /// Page cache size.  Negative = KiB, positive = pages (SQLite semantics).
    pub cache_size: i64,
    /// Page size for newly created databases (512..=65536, power of two).
    pub page_size: u32,
    /// Busy timeout in milliseconds for lock contention.
    pub busy_timeout_ms: u32,
    /// Whether to request MVCC concurrent-writer mode (FrankenSQLite-specific).
    pub concurrent_mode: bool,
    /// Whether to run `PRAGMA integrity_check` (via rusqlite) after each run and
    /// record the outcome in the report.
    pub run_integrity_check: bool,
}

impl Default for HarnessSettings {
    fn default() -> Self {
        Self {
            journal_mode: "wal".to_owned(),
            synchronous: "NORMAL".to_owned(),
            cache_size: -2000,
            page_size: 4096,
            busy_timeout_ms: 5000,
            concurrent_mode: false,
            run_integrity_check: true,
        }
    }
}

impl HarnessSettings {
    /// Produce the PRAGMA statements for a sqlite3 CLI or rusqlite run.
    #[must_use]
    pub fn to_sqlite3_pragmas(&self) -> Vec<String> {
        vec![
            format!("PRAGMA busy_timeout={};", self.busy_timeout_ms),
            format!("PRAGMA journal_mode={};", self.journal_mode),
            format!("PRAGMA synchronous={};", self.synchronous),
            format!("PRAGMA cache_size={};", self.cache_size),
            format!("PRAGMA page_size={};", self.page_size),
        ]
    }

    /// Produce the PRAGMA statements for a FrankenSQLite run.
    ///
    /// Includes the same knobs as [`Self::to_sqlite3_pragmas`] plus any
    /// FrankenSQLite-specific settings (e.g. `fsqlite.concurrent_mode`).
    #[must_use]
    pub fn to_fsqlite_pragmas(&self) -> Vec<String> {
        vec![
            format!("PRAGMA busy_timeout={};", self.busy_timeout_ms),
            format!("PRAGMA journal_mode={};", self.journal_mode),
            format!("PRAGMA synchronous={};", self.synchronous),
            format!("PRAGMA cache_size={};", self.cache_size),
            format!("PRAGMA page_size={};", self.page_size),
        ]
    }

    /// Build an [`executor::ExecutorConfig`] for the sqlite3 CLI from these settings.
    #[must_use]
    pub fn to_executor_config(&self) -> executor::ExecutorConfig {
        executor::ExecutorConfig {
            journal_mode: self.journal_mode.clone(),
            synchronous: self.synchronous.clone(),
            busy_timeout_ms: self.busy_timeout_ms,
            ..executor::ExecutorConfig::default()
        }
    }

    /// Build an [`fsqlite_executor::FsqliteExecConfig`] from these settings.
    #[must_use]
    pub fn to_fsqlite_exec_config(&self) -> fsqlite_executor::FsqliteExecConfig {
        fsqlite_executor::FsqliteExecConfig {
            pragmas: self.to_fsqlite_pragmas(),
            concurrent_mode: self.concurrent_mode,
            run_integrity_check: self.run_integrity_check,
        }
    }

    /// Build an [`sqlite_executor::SqliteExecConfig`] from these settings.
    ///
    /// Inherits the default retry/backoff/integrity-check behaviour and
    /// overrides only the PRAGMA list to use this settings object.
    #[must_use]
    pub fn to_sqlite_exec_config(&self) -> sqlite_executor::SqliteExecConfig {
        let defaults = sqlite_executor::SqliteExecConfig::default();
        sqlite_executor::SqliteExecConfig {
            pragmas: self.to_sqlite3_pragmas(),
            max_busy_retries: defaults.max_busy_retries,
            busy_backoff: defaults.busy_backoff,
            busy_backoff_max: defaults.busy_backoff_max,
            run_integrity_check: self.run_integrity_check,
        }
    }
}

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
