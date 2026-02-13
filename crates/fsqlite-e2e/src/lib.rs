//! End-to-end differential testing and benchmark harness for FrankenSQLite.
//!
//! This crate provides the infrastructure for:
//! - **Golden copy management**: loading, hashing, and comparing database snapshots
//! - **Workload generation**: deterministic, seeded workload creation
//! - **Differential comparison**: running identical SQL against FrankenSQLite and C SQLite
//! - **Corruption injection**: byte/page/sector-level corruption for recovery testing

pub mod baseline;
pub mod batch_runner;
pub mod bench_summary;
pub mod benchmark;
pub mod canonicalize;
pub mod ci_artifacts;
pub mod ci_smoke;
pub mod comparison;
pub mod concurrency_showcase;
pub mod corruption;
pub mod corruption_demo_sqlite;
pub mod corruption_scenarios;
pub mod corruption_walkthrough;
pub mod executor;
pub mod failure_bundle;
pub mod fairness;
pub mod fixture_metadata;
pub mod fixture_select;
pub mod fsqlite_baseline;
pub mod fsqlite_executor;
pub mod fsqlite_recovery_demo;
pub mod golden;
pub mod logging;
pub mod methodology;
pub mod mismatch_artifacts;
pub mod oplog;
pub mod perf_runner;
pub mod recording;
pub mod recovery_demo;
pub mod recovery_runner;
pub mod report;
pub mod report_render;
pub mod run_workspace;
pub mod smoke;
pub mod sqlite3_baseline;
pub mod sqlite_executor;
pub mod validation;
pub mod verification_gates;
pub mod workload;

// ─── Deterministic Seed Constants (bd-mblr.4.3.1) ────────────────────────────
//
// FrankenSQLite E2E tests use deterministic seeding to ensure reproducibility.
// All scenarios derive their RNG state from a base seed, enabling exact replay
// of any test execution by specifying the same seed.

/// Canonical default seed for all E2E scenarios.
///
/// The value 0xFRANKEN (as ASCII bytes: "FRANKEN") serves as a memorable,
/// project-specific default that is unlikely to collide with common test seeds
/// like 0, 1, or 42.
///
/// ## Usage
///
/// ```rust
/// use fsqlite_e2e::FRANKEN_SEED;
///
/// let seed = std::env::var("E2E_SEED")
///     .ok()
///     .and_then(|s| s.parse::<u64>().ok())
///     .unwrap_or(FRANKEN_SEED);
/// ```
///
/// ## CLI Override
///
/// All E2E binaries accept `--seed <u64>` to override the default:
/// ```sh
/// cargo run -p fsqlite-e2e --bin realdb_e2e -- --seed 12345 run
/// ```
///
/// ## Reproducibility Contract
///
/// Given identical:
/// - Seed value
/// - RNG algorithm (StdRng/ChaCha12)
/// - rand crate version (0.8.x)
/// - Scenario ID
///
/// The test execution MUST produce identical:
/// - Operation sequences
/// - Database states
/// - Corruption patterns (for COR-* scenarios)
pub const FRANKEN_SEED: u64 = 0x4652_414E_4B45_4E; // "FRANKEN" as ASCII bytes

/// Minimum valid seed value (0 is reserved for "use default").
pub const SEED_MIN: u64 = 1;

/// Maximum valid seed value.
pub const SEED_MAX: u64 = u64::MAX;

/// Derives a worker-specific seed from a base seed and worker ID.
///
/// This ensures each worker in a concurrent scenario has a distinct but
/// deterministic RNG stream.
///
/// ## Algorithm
///
/// `worker_seed = base_seed ^ (worker_id as u64 * 0x9E3779B97F4A7C15)`
///
/// The multiplier is the golden ratio constant, providing good distribution.
#[inline]
#[must_use]
pub const fn derive_worker_seed(base_seed: u64, worker_id: u16) -> u64 {
    base_seed ^ ((worker_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Derives a scenario-specific seed from a base seed and scenario ID hash.
///
/// This allows different scenarios to have independent RNG streams while
/// maintaining reproducibility.
#[inline]
#[must_use]
pub const fn derive_scenario_seed(base_seed: u64, scenario_hash: u64) -> u64 {
    base_seed ^ scenario_hash
}

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
            concurrent_mode: true,
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
        let concurrent_mode = if self.concurrent_mode { "ON" } else { "OFF" };
        vec![
            format!("PRAGMA busy_timeout={};", self.busy_timeout_ms),
            format!("PRAGMA journal_mode={};", self.journal_mode),
            format!("PRAGMA synchronous={};", self.synchronous),
            format!("PRAGMA cache_size={};", self.cache_size),
            format!("PRAGMA page_size={};", self.page_size),
            format!("PRAGMA fsqlite.concurrent_mode={concurrent_mode};"),
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

#[cfg(test)]
mod tests {
    use super::HarnessSettings;

    #[test]
    fn to_fsqlite_pragmas_includes_concurrent_mode_on_by_default() {
        let settings = HarnessSettings::default();
        let pragmas = settings.to_fsqlite_pragmas();
        assert!(
            pragmas
                .iter()
                .any(|p| p == "PRAGMA fsqlite.concurrent_mode=ON;")
        );
    }

    #[test]
    fn to_fsqlite_pragmas_includes_concurrent_mode_off_when_disabled() {
        let settings = HarnessSettings {
            concurrent_mode: false,
            ..HarnessSettings::default()
        };
        let pragmas = settings.to_fsqlite_pragmas();
        assert!(
            pragmas
                .iter()
                .any(|p| p == "PRAGMA fsqlite.concurrent_mode=OFF;")
        );
    }
}
