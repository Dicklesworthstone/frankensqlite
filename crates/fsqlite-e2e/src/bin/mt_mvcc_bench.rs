//! `mt-mvcc-bench` — real multi-threaded MVCC writer benchmark (IMPL-4a).
//!
//! Why this exists: it is the standalone scale harness for real multi-threaded
//! MVCC writer runs. It spawns N OS threads, each with its OWN
//! `Connection::open(path)` against the SAME shared file-backed database, so
//! the MVCC page-lock table, commit coordinator, and SSI validator are
//! exercised under real contention. The comprehensive benchmark now uses the
//! same one-connection-per-thread shape for its full-matrix concurrent rows;
//! this binary adds 16- through 128-thread coverage, separate-table mode, startup
//! diagnostics, and pass-over-pass history gates.
//!
//! For each thread count we measure:
//!   - FrankenSQLite file-backed database, one Connection per thread,
//!     `PRAGMA fsqlite.concurrent_mode=ON` + `BEGIN CONCURRENT`.
//!   - C SQLite (rusqlite) file-backed WAL, one Connection per thread,
//!     `journal_mode=WAL`, matched `synchronous=NORMAL|FULL`, `busy_timeout=5000`.
//!
//! By default, each thread inserts `--rows-per-thread` rows into the shared
//! table `bench(id INTEGER PRIMARY KEY, payload TEXT)` using disjoint rowid
//! ranges (`thread_id * 1_000_000 + i`) so there are no logical row conflicts
//! — only page-level contention on the table's btree. `--separate-tables`
//! gives each worker its own `bench_N` table to measure the no-shared-btree
//! concurrent writer shape.
//!
//! Output is a tab-separated table suitable for grepping / redirection:
//!
//! ```text
//! threads | fsqlite_wps | sqlite_wps | throughput_ratio | fsqlite_ms_p50 | ...
//!       1 | 12345       | 23456      | 0.53x            | 81.00          | ...
//! ```
//!
//! `throughput_ratio = fsqlite_wps / sqlite_wps`. Values above 1.0x mean
//! FrankenSQLite is faster than C SQLite WAL under equal multi-threaded load.
//! `time_ratio = fsqlite_batch_ms / sqlite_batch_ms`; lower is better.
//!
//! ## CLI
//!
//! ```text
//! mt-mvcc-bench [--rows-per-thread=1000] [--threads=1,2,4,8,16,32,64,128] [--iters=21]
//! [--synchronous=normal|full]
//! [--json-output=PATH] [--summary-md=PATH]
//! [--separate-tables] [--one-row-per-transaction]
//! ```
//!
//! ## Caveats
//!
//! * `BEGIN CONCURRENT` requires `PRAGMA fsqlite.concurrent_mode=ON;` to be
//!   set on each per-thread connection (see
//!   `crates/fsqlite-harness/tests/bd_3plop_4_lock_contention_storms.rs`).
//!   The benchmark fails closed unless both the default-on API guard and the
//!   effective PRAGMA readback prove concurrent mode stayed enabled. It never
//!   falls back to plain `BEGIN`.
//! * In `--one-row-per-transaction` mode, both engines retry a complete
//!   BEGIN/INSERT/COMMIT transaction after any retryable stage failure. The
//!   FrankenSQLite arm also retries transient statement-preparation failures
//!   before the first transaction under the same shared worker deadline. The
//!   default bulk mode retains its v7 engine-specific retry policy. Hard row-
//!   level failures remain separate from offered, attempted, retried, and
//!   database-proven committed work so a failed arm cannot inflate throughput.
//! * Each paired round creates fresh tempfiles so no database state carries
//!   across runs. Every F/C claim is preceded by a same-invocation interleaved
//!   C/C A/A null. The verdict uses a bootstrap CI for the per-round median
//!   ratio; CV and MAD are provenance only.
//! * Release tooling may supply a canonical resolved dependency/feature graph
//!   SHA-256 through `FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256`
//!   at build time. Ordinary builds leave that receipt explicitly unavailable.

// bd-mnlk2 / bd-zavyn: the hoisted timed windows await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per transaction attempt; boxing them would put an allocation inside the
// timed window.
#![allow(clippy::large_futures)]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::io::{Read as _, Write as _};
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    fmt::{Display, Write as _},
    fs,
    path::Path,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ─── Defaults ─────────────────────────────────────────────────────────────

const DEFAULT_ROWS_PER_THREAD: usize = 1_000;
const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128];
const DEFAULT_ITERS: usize = 21;
const DEFAULT_WAL_AUTOCHECKPOINT_PAGES: i64 = 1_000;
const CONTRACT_BOOTSTRAP_REPS: usize = 10_000;
const DEFAULT_HISTORY_JSON: &str = ".bench-history/mt-mvcc-bench.latest.json";
const DEFAULT_SEPARATE_TABLES_HISTORY_JSON: &str =
    ".bench-history/mt-mvcc-bench.separate-tables.latest.json";
const DEFAULT_ONE_ROW_HISTORY_JSON: &str =
    ".bench-history/mt-mvcc-bench.one-row-per-transaction.latest.json";
const DEFAULT_SEPARATE_TABLES_ONE_ROW_HISTORY_JSON: &str =
    ".bench-history/mt-mvcc-bench.separate-tables.one-row-per-transaction.latest.json";
const ROWID_BASE_STRIDE: i64 = 1_000_000;
const MAX_RETRIES: usize = 512;
const RETRY_SLEEP_MS: u64 = 1;
const MAX_RETRY_SLEEP_MS: u64 = 25;
// These identities are part of the history-comparison contract. Any semantic
// change to either retry algorithm must update the matching identity and
// advance the report schema before the resulting measurements can compare
// with prior artifacts.
const CSQLITE_RETRY_ALGORITHM: &str = "csqlite.per-operation.fixed-1ms.busy-or-locked.max-512.v1";
const FSQLITE_RETRY_BACKOFF_ALGORITHM: &str = "fsqlite.whole-transaction.step-exp-every-8-cap-25ms-plus-thread-attempt-jitter-0-to-4ms.max-512-or-timeout.v1";
const CSQLITE_ONE_ROW_RETRY_ALGORITHM: &str = "csqlite.whole-one-row-transaction.fixed-1ms.busy-or-locked.max-512-or-shared-worker-timeout.v1";
const FSQLITE_ONE_ROW_RETRY_BACKOFF_ALGORITHM: &str = "fsqlite.prepare-rollback-cleanup-or-whole-one-row-transaction.step-exp-every-8-cap-25ms-plus-thread-attempt-jitter-0-to-4ms.max-512-or-shared-worker-timeout.v3";
const CSQLITE_ONE_ROW_RETRY_UNIT: &str = "whole one-row BEGIN/INSERT/COMMIT transaction attempt";
const FSQLITE_ONE_ROW_RETRY_UNIT: &str = "statement preparation, retryable ROLLBACK cleanup, or whole one-row BEGIN CONCURRENT/INSERT/COMMIT transaction attempt";
const FSQLITE_RETRYABLE_ERRORS: &str = "Busy|BusyRecovery|BusySnapshot|DatabaseLocked|\
    WriteConflict|SerializationFailure|PageBufferCapacityExhausted";
/// Base wall-clock retry budget for one bulk transaction attempt loop, or one
/// shared worker deadline covering statement preparation and all row
/// transactions in one-row mode.
/// Scaled up with
/// offered work by [`fsqlite_retry_timeout`] — the fixed 5s was exceeded by
/// queueing alone at 64 writers x 1000-row txns (bd-caa6u).
const FSQLITE_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
/// Pessimistic whole-run contention floor used to scale the retry budget.
/// The first full-matrix run showed 10k wps was still optimistic at peak
/// contention: the 64-writer arm (11s budget) starved 58 txns past the
/// envelope while the 128-writer arm (17.8s) passed with zero failures.
/// 5k wps gives 64 writers ~17.8s and 128 writers ~30.6s.
const RETRY_BUDGET_FLOOR_WPS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionGranularity {
    Bulk,
    OneRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SynchronousMode {
    Normal,
    Full,
}

impl SynchronousMode {
    const fn pragma_value(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
        }
    }

    const fn receipt_value(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Full => "full",
        }
    }
}

/// Wall-clock retry budget for one transaction attempt loop, scaled with the
/// total offered work so a txn that legitimately waits behind a 64/128-writer
/// convoy tail is not misreported as exhausted (bd-caa6u).
fn fsqlite_retry_timeout(threads: usize, rows_per_thread: usize) -> Duration {
    let scaled_secs =
        (threads as u64).saturating_mul(rows_per_thread as u64) / RETRY_BUDGET_FLOOR_WPS;
    FSQLITE_RETRY_TIMEOUT + Duration::from_secs(scaled_secs)
}

fn retry_timeout_millis(timeout: Duration) -> Result<u64, String> {
    u64::try_from(timeout.as_millis())
        .map_err(|_| format!("retry timeout {timeout:?} exceeds reportable range"))
}

fn retry_policy_receipt_for_granularity(
    retry_timeout: Duration,
    retry_timeout_overridden: bool,
    transaction_granularity: TransactionGranularity,
) -> Result<RetryPolicyReceipt, String> {
    let (
        csqlite_retry_unit,
        csqlite_retry_algorithm,
        fsqlite_retry_unit,
        fsqlite_retry_backoff_algorithm,
    ) = match transaction_granularity {
        TransactionGranularity::Bulk => (
            "individual INSERT or COMMIT operation",
            CSQLITE_RETRY_ALGORITHM,
            "whole BEGIN CONCURRENT transaction attempt",
            FSQLITE_RETRY_BACKOFF_ALGORITHM,
        ),
        TransactionGranularity::OneRow => (
            CSQLITE_ONE_ROW_RETRY_UNIT,
            CSQLITE_ONE_ROW_RETRY_ALGORITHM,
            FSQLITE_ONE_ROW_RETRY_UNIT,
            FSQLITE_ONE_ROW_RETRY_BACKOFF_ALGORITHM,
        ),
    };
    let retry_timeout_ms = retry_timeout_millis(retry_timeout)?;
    let one_row_mode = transaction_granularity == TransactionGranularity::OneRow;
    Ok(RetryPolicyReceipt {
        csqlite_busy_timeout_ms: 5_000,
        csqlite_max_operation_retries: if transaction_granularity == TransactionGranularity::Bulk {
            MAX_RETRIES
        } else {
            0
        },
        csqlite_max_transaction_retries: (transaction_granularity
            == TransactionGranularity::OneRow)
            .then_some(MAX_RETRIES),
        csqlite_retry_sleep_ms: RETRY_SLEEP_MS,
        csqlite_retry_unit: csqlite_retry_unit.to_owned(),
        csqlite_retry_algorithm: csqlite_retry_algorithm.to_owned(),
        shared_worker_retry_timeout_ms: one_row_mode.then_some(retry_timeout_ms),
        shared_worker_retry_timeout_overridden: one_row_mode.then_some(retry_timeout_overridden),
        fsqlite_transaction_timeout_ms: retry_timeout_ms,
        fsqlite_max_transaction_retries: MAX_RETRIES,
        fsqlite_retry_sleep_base_ms: RETRY_SLEEP_MS,
        fsqlite_retry_sleep_cap_ms: MAX_RETRY_SLEEP_MS + 4,
        fsqlite_retry_unit: fsqlite_retry_unit.to_owned(),
        fsqlite_retry_backoff_algorithm: fsqlite_retry_backoff_algorithm.to_owned(),
        fsqlite_retryable_errors: FSQLITE_RETRYABLE_ERRORS.to_owned(),
        fsqlite_timeout_overridden: retry_timeout_overridden,
    })
}

#[cfg(test)]
fn retry_policy_receipt(
    fsqlite_timeout: Duration,
    fsqlite_timeout_overridden: bool,
) -> Result<RetryPolicyReceipt, String> {
    retry_policy_receipt_for_granularity(
        fsqlite_timeout,
        fsqlite_timeout_overridden,
        TransactionGranularity::Bulk,
    )
}

fn fsqlite_error_is_retryable(error: &fsqlite::FrankenError) -> bool {
    matches!(
        error,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::DatabaseLocked { .. }
            | fsqlite::FrankenError::WriteConflict { .. }
            | fsqlite::FrankenError::SerializationFailure { .. }
            | fsqlite::FrankenError::PageBufferCapacityExhausted { .. }
    )
}

fn csqlite_error_is_retryable(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

const SHARED_INSERT_SQL: &str = "INSERT INTO bench (id, payload) VALUES (?1, ?2)";
const STARTUP_COORDINATION_TIMEOUT: Duration = Duration::from_secs(5);
const PASS_OVER_PASS_SCHEMA_V1: &str = "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1";
const PASS_OVER_PASS_MAX_RATIO_DROP_PCT: f64 = 5.0;
const REPORT_SCHEMA_V7: &str = "fsqlite-e2e.mt_mvcc_bench_report.v7";
#[cfg(test)]
const REPORT_SCHEMA_V8: &str = "fsqlite-e2e.mt_mvcc_bench_report.v8";
#[cfg(test)]
const REPORT_SCHEMA_V9: &str = "fsqlite-e2e.mt_mvcc_bench_report.v9";
const REPORT_SCHEMA_V10: &str = "fsqlite-e2e.mt_mvcc_bench_report.v10";
const SETTINGS_INTERPRETATION: &str = "Both engines proved the listed effective PRAGMA values; \
    equal names and readbacks do not establish cross-engine semantic equivalence.";
const ACCOUNTING_INTERPRETATION: &str = "offered and committed writes share one row unit; \
    attempted_writes counts physical INSERT calls; retried_operations records the existing \
    engine-specific retry unit and is provenance only, not a cross-engine comparison metric.";
const TIMING_INTERPRETATION: &str = "workload_elapsed_ns begins only after every worker has \
    opened and proved its effective settings, and ends at the last worker's transaction terminal \
    point before connection teardown; worker_startup_elapsed_ns is reported separately.";
const NON_CITABLE_REASON: &str = "v7 binds the running executable, build/runtime source identity, \
    Cargo.lock, invocation, toolchain, and measurement host to this same-invocation comparison, \
    but bd-uh1fv still requires external watchdog, sanitized environment, matched retry/deadline \
    semantics, external validation (and, when absent, capture) of a build-attested resolved \
    dependency/feature-graph digest, counterbalanced topology receipts, immutable manifest, \
    retained baseline history, and independent verification.";
const NON_CITABLE_REASON_V10: &str = "v10 binds the explicit one-row transaction/retry-unit \
    contract to bounded Busy/BusyRecovery ROLLBACK cleanup under the shared worker deadline and \
    an exact v3 FSQLite retry identity; it retains an optional build-attested resolved \
    dependency/feature-graph digest, but remains non-citable: bd-uh1fv still requires an external \
    watchdog, sanitized environment, matched retry/deadline semantics, counterbalanced topology \
    receipts, immutable manifest, retained baseline history, and independent verification; a \
    default build also leaves the graph digest unavailable.";
const RELEASE_REGRESSION_SCOPE: &str = "Narrow same-process, same-host F/C writer-throughput \
    comparison for only the requested mt-mvcc-bench workload/configurations; this report does not \
    cover the shipped release profile, other workloads or platforms, long-term baseline retention, \
    independent reproduction, or overall release eligibility.";
const RELEASE_REGRESSION_SCOPE_V10: &str = "Narrow same-process, same-host F/C writer-throughput \
    comparison for only this report's attested selected Cargo profile and the requested \
    mt-mvcc-bench workload/configurations; this report does not cover other workloads or platforms, \
    long-term baseline retention, independent reproduction, or overall release eligibility.";
const EMBEDDED_BUILD_CARGO_LOCK: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"));
const DEPENDENCY_GRAPH_ATTESTATION_AVAILABLE: &str = "available: the lowercase SHA-256 was \
    supplied at build time through the rerun-sensitive \
    FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256 attestation input";
const DEPENDENCY_GRAPH_ATTESTATION_UNAVAILABLE: &str = "unavailable: \
    FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256 was not supplied at build time; \
    ordinary non-release builds do not invent a dependency/feature graph digest";

impl TransactionGranularity {
    const fn report_schema(self) -> &'static str {
        match self {
            Self::Bulk => REPORT_SCHEMA_V7,
            Self::OneRow => REPORT_SCHEMA_V10,
        }
    }

    const fn non_citable_reason(self) -> &'static str {
        match self {
            Self::Bulk => NON_CITABLE_REASON,
            Self::OneRow => NON_CITABLE_REASON_V10,
        }
    }

    const fn release_regression_scope(self) -> &'static str {
        match self {
            Self::Bulk => RELEASE_REGRESSION_SCOPE,
            Self::OneRow => RELEASE_REGRESSION_SCOPE_V10,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Bulk => "one_bulk_transaction_per_worker",
            Self::OneRow => "one_row_per_transaction",
        }
    }
}

macro_rules! human_output {
    ($json_stdout:expr, $($arg:tt)*) => {
        if $json_stdout {
            eprintln!($($arg)*);
        } else {
            println!($($arg)*);
        }
    };
}

fn bytes_to_lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn sha256_bytes(bytes: &[u8]) -> String {
    bytes_to_lower_hex(&Sha256::digest(bytes))
}

fn parse_optional_lower_sha256(value: &str) -> Result<Option<String>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() != 64 {
        return Err(format!(
            "expected 64 lowercase hexadecimal characters, got {}",
            value.len()
        ));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("digest contains a non-lowercase-hexadecimal character".to_owned());
    }
    Ok(Some(value.to_owned()))
}

fn resolved_dependency_feature_graph_attestation(
    value: &str,
) -> Result<(Option<String>, &'static str), String> {
    let digest = parse_optional_lower_sha256(value)?;
    let limitation = if digest.is_some() {
        DEPENDENCY_GRAPH_ATTESTATION_AVAILABLE
    } else {
        DEPENDENCY_GRAPH_ATTESTATION_UNAVAILABLE
    };
    Ok((digest, limitation))
}

fn file_identity(path: &Path) -> String {
    let Ok(bytes) = fs::read(path) else {
        return format!("unavailable:{}", path.display());
    };
    format!(
        "{}:{}:{}",
        path.display(),
        sha256_bytes(&bytes),
        bytes.len()
    )
}

fn self_identity() -> String {
    let Ok(path) = std::env::current_exe() else {
        return "unavailable current_exe".to_owned();
    };
    let Ok(bytes) = fs::read(&path) else {
        return format!("unavailable read_error {}", path.display());
    };
    format!(
        "{} ({} bytes) {}",
        sha256_bytes(&bytes),
        bytes.len(),
        path.display()
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FileSnapshotReceipt {
    sha256: Option<String>,
    bytes_read: Option<u64>,
    metadata_size_bytes: Option<u64>,
    unix_device: Option<u64>,
    unix_inode: Option<u64>,
    error: Option<String>,
}

impl FileSnapshotReceipt {
    fn unavailable(error: impl Into<String>) -> Self {
        Self {
            sha256: None,
            bytes_read: None,
            metadata_size_bytes: None,
            unix_device: None,
            unix_inode: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone)]
struct ExecutableIdentityStart {
    current_exe_path: Option<PathBuf>,
    canonical_path: Option<PathBuf>,
    path_resolution_error: Option<String>,
    path_used: Option<PathBuf>,
    before_measurement: FileSnapshotReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ExecutableIdentityReceipt {
    current_exe_path: Option<String>,
    canonical_path: Option<String>,
    path_resolution_error: Option<String>,
    process_id: u32,
    before_measurement: FileSnapshotReceipt,
    after_measurement: FileSnapshotReceipt,
    unchanged_during_measurement: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BuildSourceIdentityReceipt {
    workspace_root: String,
    git_sha: String,
    git_branch: String,
    git_tree_state: String,
    build_nonce: String,
    build_input_tracking: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RuntimeSourceIdentityReceipt {
    workspace_root: String,
    canonical_workspace_root: Option<String>,
    git_sha: Option<String>,
    git_branch: Option<String>,
    git_tree_state: String,
    matches_build_git_sha: Option<bool>,
    discovery_errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RuntimeSourceStabilityReceipt {
    before_measurement: RuntimeSourceIdentityReceipt,
    after_measurement: RuntimeSourceIdentityReceipt,
    same_clean_git_identity_at_capture_points: Option<bool>,
    stability_limitation: &'static str,
}

#[derive(Debug, Clone)]
struct CargoLockIdentityStart {
    embedded_build_sha256: String,
    embedded_build_size_bytes: u64,
    runtime_path: PathBuf,
    before_measurement: FileSnapshotReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CargoLockIdentityReceipt {
    embedded_build_sha256: String,
    embedded_build_size_bytes: u64,
    runtime_path: String,
    before_measurement: FileSnapshotReceipt,
    after_measurement: FileSnapshotReceipt,
    before_matches_embedded_build: Option<bool>,
    after_matches_embedded_build: Option<bool>,
    unchanged_at_capture_points: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SubjectIdentityReceipt {
    executable: ExecutableIdentityReceipt,
    build_source: BuildSourceIdentityReceipt,
    runtime_source: RuntimeSourceStabilityReceipt,
    cargo_lock: CargoLockIdentityReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RustflagsReceipt {
    cargo_encoded_rustflags_present: bool,
    encoded_hex: String,
    decoded_arguments: Option<Vec<String>>,
    decode_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BuildConfigurationReceipt {
    cargo_profile: String,
    selected_profile: String,
    profile_label: String,
    opt_level: String,
    debug: String,
    target: String,
    build_host: String,
    enabled_features: Vec<String>,
    rustflags: RustflagsReceipt,
    profile_overrides_hex: String,
    native_build_overrides_hex: String,
    rustc_version_verbose: String,
    cargo_version: String,
    resolved_dependency_feature_graph_sha256: Option<String>,
    resolved_dependency_feature_graph_limitation: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct InvocationReceipt {
    argv_lossy: Vec<String>,
    argv_raw_hex: Vec<String>,
    raw_encoding: &'static str,
    length_prefixed_argv_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CpuTopologyReceipt {
    logical_cpu_directories: Option<usize>,
    physical_package_count: Option<usize>,
    physical_core_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct StaticMeasurementHostReceipt {
    hostname: Option<String>,
    cpu_model: Option<String>,
    available_parallelism: Option<usize>,
    cpu_online: Option<String>,
    cpu_present: Option<String>,
    cpu_possible: Option<String>,
    cpu_isolated: Option<String>,
    cpu_topology: CpuTopologyReceipt,
    scaling_governors_by_cpu: BTreeMap<String, String>,
    kernel_release: Option<String>,
    kernel_version: Option<String>,
    numa_online_nodes: Option<String>,
    numa_possible_nodes: Option<String>,
    numa_node_directories: Option<usize>,
    unavailable_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DynamicMeasurementHostReceipt {
    unix_epoch_millis: Option<u64>,
    process_cpu_affinity_mask: Option<String>,
    process_cpu_affinity_list: Option<String>,
    proc_self_cgroup: Option<String>,
    cpuset_cpus_effective: Option<String>,
    cpuset_mems_effective: Option<String>,
    load_average: Option<String>,
    pressure_cpu: Option<String>,
    pressure_memory: Option<String>,
    pressure_io: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MeasurementHostReceipt {
    host: StaticMeasurementHostReceipt,
    before_measurement: DynamicMeasurementHostReceipt,
    after_measurement: DynamicMeasurementHostReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ComparisonEnvironmentReceipt {
    build_configuration: BuildConfigurationReceipt,
    invocation: InvocationReceipt,
    measurement_host: MeasurementHostReceipt,
}

#[derive(Debug, Clone)]
struct ProvenanceCapture {
    executable: ExecutableIdentityStart,
    build_source: BuildSourceIdentityReceipt,
    runtime_source_before_measurement: RuntimeSourceIdentityReceipt,
    cargo_lock: CargoLockIdentityStart,
    build_configuration: BuildConfigurationReceipt,
    invocation: InvocationReceipt,
    host: StaticMeasurementHostReceipt,
    host_before_measurement: DynamicMeasurementHostReceipt,
}

fn snapshot_file(path: &Path) -> FileSnapshotReceipt {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) => {
            return FileSnapshotReceipt::unavailable(format!("open {}: {error}", path.display()));
        }
    };
    let mut errors = Vec::new();
    // Both metadata and bytes come from this one open handle. A concurrent
    // path replacement therefore cannot splice one file's digest together
    // with another file's device/inode identity.
    let metadata = file.metadata();
    let mut bytes = Vec::new();
    let (sha256, bytes_read) = match file.read_to_end(&mut bytes) {
        Ok(bytes_read) => (Some(sha256_bytes(&bytes)), u64::try_from(bytes_read).ok()),
        Err(error) => {
            errors.push(format!("read open handle for {}: {error}", path.display()));
            (None, None)
        }
    };
    let (metadata_size_bytes, unix_device, unix_inode) = match metadata {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                (
                    Some(metadata.len()),
                    Some(metadata.dev()),
                    Some(metadata.ino()),
                )
            }
            #[cfg(not(unix))]
            {
                (Some(metadata.len()), None, None)
            }
        }
        Err(error) => {
            errors.push(format!("metadata {}: {error}", path.display()));
            (None, None, None)
        }
    };
    FileSnapshotReceipt {
        sha256,
        bytes_read,
        metadata_size_bytes,
        unix_device,
        unix_inode,
        error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
}

fn begin_executable_identity() -> ExecutableIdentityStart {
    match std::env::current_exe() {
        Ok(current_exe_path) => {
            let (canonical_path, path_resolution_error) = match current_exe_path.canonicalize() {
                Ok(path) => (Some(path), None),
                Err(error) => (
                    None,
                    Some(format!(
                        "canonicalize {}: {error}",
                        current_exe_path.display()
                    )),
                ),
            };
            let path_used = canonical_path.as_ref().unwrap_or(&current_exe_path).clone();
            let before_measurement = snapshot_file(&path_used);
            ExecutableIdentityStart {
                current_exe_path: Some(current_exe_path),
                canonical_path,
                path_resolution_error,
                path_used: Some(path_used),
                before_measurement,
            }
        }
        Err(error) => ExecutableIdentityStart {
            current_exe_path: None,
            canonical_path: None,
            path_resolution_error: Some(format!("current_exe: {error}")),
            path_used: None,
            before_measurement: FileSnapshotReceipt::unavailable(format!("current_exe: {error}")),
        },
    }
}

fn finish_executable_identity(start: ExecutableIdentityStart) -> ExecutableIdentityReceipt {
    let after_measurement = start.path_used.as_ref().map_or_else(
        || FileSnapshotReceipt::unavailable("running executable path unavailable"),
        |path| snapshot_file(path),
    );
    let unchanged_during_measurement =
        file_snapshots_match(&start.before_measurement, &after_measurement);
    ExecutableIdentityReceipt {
        current_exe_path: start
            .current_exe_path
            .map(|path| path.to_string_lossy().into_owned()),
        canonical_path: start
            .canonical_path
            .map(|path| path.to_string_lossy().into_owned()),
        path_resolution_error: start.path_resolution_error,
        process_id: std::process::id(),
        before_measurement: start.before_measurement,
        after_measurement,
        unchanged_during_measurement,
    }
}

fn build_tree_state(dirty: &str) -> String {
    match dirty {
        "false" => "clean".to_owned(),
        "true" => "dirty".to_owned(),
        other => format!("unknown:{other}"),
    }
}

fn collect_build_source_identity() -> BuildSourceIdentityReceipt {
    BuildSourceIdentityReceipt {
        workspace_root: env!("FSQLITE_BENCH_BUILD_WORKSPACE_ROOT").to_owned(),
        git_sha: env!("FSQLITE_BENCH_BUILD_GIT_SHA").to_owned(),
        git_branch: env!("FSQLITE_BENCH_BUILD_GIT_BRANCH").to_owned(),
        git_tree_state: build_tree_state(env!("FSQLITE_BENCH_BUILD_GIT_DIRTY")),
        build_nonce: env!("FSQLITE_BENCH_BUILD_NONCE").to_owned(),
        build_input_tracking: env!("FSQLITE_BENCH_BUILD_INPUT_TRACKING").to_owned(),
    }
}

fn command_stdout(mut command: Command) -> Result<String, String> {
    let debug_command = format!("{command:?}");
    let output = command
        .output()
        .map_err(|error| format!("execute {debug_command}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{debug_command} exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map(|stdout| stdout.trim().to_owned())
        .map_err(|error| format!("decode stdout from {debug_command}: {error}"))
}

fn git_stdout(workspace_root: &Path, args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("git");
    command.arg("-C").arg(workspace_root).args(args);
    command_stdout(command)
}

fn collect_runtime_source_identity(
    build_source: &BuildSourceIdentityReceipt,
) -> RuntimeSourceIdentityReceipt {
    let workspace_root = PathBuf::from(&build_source.workspace_root);
    let mut discovery_errors = Vec::new();
    let canonical_workspace_root = match workspace_root.canonicalize() {
        Ok(path) => Some(path),
        Err(error) => {
            discovery_errors.push(format!(
                "canonicalize runtime workspace {}: {error}",
                workspace_root.display()
            ));
            None
        }
    };
    let git_root = canonical_workspace_root
        .as_deref()
        .unwrap_or(workspace_root.as_path());
    let git_sha = match git_stdout(git_root, &["rev-parse", "--verify", "HEAD"]) {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) => {
            discovery_errors.push("runtime git SHA was empty".to_owned());
            None
        }
        Err(error) => {
            discovery_errors.push(error);
            None
        }
    };
    let git_branch = match git_stdout(git_root, &["branch", "--show-current"]) {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) => Some("detached".to_owned()),
        Err(error) => {
            discovery_errors.push(error);
            None
        }
    };
    let git_tree_state = match git_stdout(
        git_root,
        &["status", "--porcelain=v1", "--untracked-files=normal"],
    ) {
        Ok(status) if status.is_empty() => "clean".to_owned(),
        Ok(_) => "dirty".to_owned(),
        Err(error) => {
            discovery_errors.push(error);
            "unavailable".to_owned()
        }
    };
    RuntimeSourceIdentityReceipt {
        workspace_root: workspace_root.to_string_lossy().into_owned(),
        canonical_workspace_root: canonical_workspace_root
            .map(|path| path.to_string_lossy().into_owned()),
        matches_build_git_sha: git_sha.as_deref().map(|sha| sha == build_source.git_sha),
        git_sha,
        git_branch,
        git_tree_state,
        discovery_errors,
    }
}

fn begin_cargo_lock_identity(build_source: &BuildSourceIdentityReceipt) -> CargoLockIdentityStart {
    let runtime_path = PathBuf::from(&build_source.workspace_root).join("Cargo.lock");
    CargoLockIdentityStart {
        embedded_build_sha256: sha256_bytes(EMBEDDED_BUILD_CARGO_LOCK),
        embedded_build_size_bytes: u64::try_from(EMBEDDED_BUILD_CARGO_LOCK.len())
            .expect("Cargo.lock length fits u64"),
        before_measurement: snapshot_file(&runtime_path),
        runtime_path,
    }
}

fn file_snapshots_match(before: &FileSnapshotReceipt, after: &FileSnapshotReceipt) -> Option<bool> {
    if before.error.is_some() || after.error.is_some() {
        return None;
    }
    let unix_identity_matches = |before: Option<u64>, after: Option<u64>| match (before, after) {
        (Some(before), Some(after)) => before == after,
        (None, None) => true,
        _ => false,
    };
    match (
        before.sha256.as_ref(),
        after.sha256.as_ref(),
        before.bytes_read,
        after.bytes_read,
        before.metadata_size_bytes,
        after.metadata_size_bytes,
    ) {
        (
            Some(before_hash),
            Some(after_hash),
            Some(before_bytes),
            Some(after_bytes),
            Some(before_metadata_size),
            Some(after_metadata_size),
        ) => Some(
            before_hash == after_hash
                && before_bytes == after_bytes
                && before_metadata_size == after_metadata_size
                && before_metadata_size == before_bytes
                && after_metadata_size == after_bytes
                && unix_identity_matches(before.unix_device, after.unix_device)
                && unix_identity_matches(before.unix_inode, after.unix_inode),
        ),
        _ => None,
    }
}

fn finish_cargo_lock_identity(start: CargoLockIdentityStart) -> CargoLockIdentityReceipt {
    let after_measurement = snapshot_file(&start.runtime_path);
    let before_matches_embedded_build = start
        .before_measurement
        .sha256
        .as_deref()
        .map(|sha| sha == start.embedded_build_sha256);
    let after_matches_embedded_build = after_measurement
        .sha256
        .as_deref()
        .map(|sha| sha == start.embedded_build_sha256);
    let unchanged_at_capture_points =
        file_snapshots_match(&start.before_measurement, &after_measurement);
    CargoLockIdentityReceipt {
        embedded_build_sha256: start.embedded_build_sha256,
        embedded_build_size_bytes: start.embedded_build_size_bytes,
        runtime_path: start.runtime_path.to_string_lossy().into_owned(),
        before_measurement: start.before_measurement,
        after_measurement,
        before_matches_embedded_build,
        after_matches_embedded_build,
        unchanged_at_capture_points,
    }
}

fn runtime_source_stability(
    before_measurement: RuntimeSourceIdentityReceipt,
    after_measurement: RuntimeSourceIdentityReceipt,
) -> RuntimeSourceStabilityReceipt {
    let same_clean_git_identity_at_capture_points = match (
        before_measurement.git_sha.as_deref(),
        after_measurement.git_sha.as_deref(),
    ) {
        (Some(before_sha), Some(after_sha))
            if before_sha != after_sha
                || before_measurement.git_tree_state != after_measurement.git_tree_state =>
        {
            Some(false)
        }
        (Some(_), Some(_))
            if before_measurement.git_tree_state == "clean"
                && after_measurement.git_tree_state == "clean" =>
        {
            Some(true)
        }
        _ => None,
    };
    RuntimeSourceStabilityReceipt {
        before_measurement,
        after_measurement,
        same_clean_git_identity_at_capture_points,
        stability_limitation: "dirty worktree content is not hashed by the existing build \
            attestation, so equality is asserted only for the same clean Git commit at both \
            capture points",
    }
}

fn decode_hex(encoded: &str) -> Result<Vec<u8>, String> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let (chunks, remainder) = encoded.as_bytes().as_chunks::<2>();
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for chunk in chunks {
        let high = nibble(chunk[0]).ok_or_else(|| format!("non-hex byte 0x{:02x}", chunk[0]))?;
        let low = nibble(chunk[1]).ok_or_else(|| format!("non-hex byte 0x{:02x}", chunk[1]))?;
        decoded.push((high << 4) | low);
    }
    if remainder.is_empty() {
        Ok(decoded)
    } else {
        Err("hex value has odd length".to_owned())
    }
}

fn collect_rustflags() -> RustflagsReceipt {
    let encoded_hex = env!("FSQLITE_BENCH_BUILD_RUSTFLAGS_HEX").to_owned();
    let present = env!("FSQLITE_BENCH_BUILD_ENCODED_RUSTFLAGS_PRESENT") == "true";
    match decode_hex(&encoded_hex).and_then(|bytes| {
        String::from_utf8(bytes).map_err(|error| format!("RUSTFLAGS are not UTF-8: {error}"))
    }) {
        Ok(decoded) => RustflagsReceipt {
            cargo_encoded_rustflags_present: present,
            encoded_hex,
            decoded_arguments: Some(
                decoded
                    .split('\u{1f}')
                    .filter(|argument| !argument.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ),
            decode_error: None,
        },
        Err(error) => RustflagsReceipt {
            cargo_encoded_rustflags_present: present,
            encoded_hex,
            decoded_arguments: None,
            decode_error: Some(error),
        },
    }
}

fn collect_build_configuration() -> BuildConfigurationReceipt {
    let (resolved_dependency_feature_graph_sha256, resolved_dependency_feature_graph_limitation) =
        resolved_dependency_feature_graph_attestation(env!(
            "FSQLITE_BENCH_BUILD_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256"
        ))
        .expect("fsqlite-e2e/build.rs must reject an invalid dependency/feature graph digest");
    BuildConfigurationReceipt {
        cargo_profile: env!("FSQLITE_BENCH_BUILD_PROFILE").to_owned(),
        selected_profile: env!("FSQLITE_BENCH_BUILD_SELECTED_PROFILE").to_owned(),
        profile_label: env!("FSQLITE_BENCH_BUILD_PROFILE_LABEL").to_owned(),
        opt_level: env!("FSQLITE_BENCH_BUILD_OPT_LEVEL").to_owned(),
        debug: env!("FSQLITE_BENCH_BUILD_DEBUG").to_owned(),
        target: env!("FSQLITE_BENCH_BUILD_TARGET").to_owned(),
        build_host: env!("FSQLITE_BENCH_BUILD_HOST").to_owned(),
        enabled_features: env!("FSQLITE_BENCH_BUILD_FEATURES")
            .split(',')
            .filter(|feature| !feature.is_empty())
            .map(str::to_owned)
            .collect(),
        rustflags: collect_rustflags(),
        profile_overrides_hex: env!("FSQLITE_BENCH_BUILD_PROFILE_OVERRIDES_HEX").to_owned(),
        native_build_overrides_hex: env!("FSQLITE_BENCH_BUILD_NATIVE_OVERRIDES_HEX").to_owned(),
        rustc_version_verbose: env!("FSQLITE_BENCH_BUILD_RUSTC_VERSION").to_owned(),
        cargo_version: env!("FSQLITE_BENCH_BUILD_CARGO_VERSION").to_owned(),
        resolved_dependency_feature_graph_sha256,
        resolved_dependency_feature_graph_limitation,
    }
}

#[cfg(unix)]
fn os_str_raw_bytes(value: &OsStr) -> (&'static str, Vec<u8>) {
    use std::os::unix::ffi::OsStrExt as _;
    ("unix_os_str_bytes", value.as_bytes().to_vec())
}

#[cfg(windows)]
fn os_str_raw_bytes(value: &OsStr) -> (&'static str, Vec<u8>) {
    use std::os::windows::ffi::OsStrExt as _;
    let bytes = value
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    ("windows_utf16le", bytes)
}

#[cfg(not(any(unix, windows)))]
fn os_str_raw_bytes(value: &OsStr) -> (&'static str, Vec<u8>) {
    ("lossy_utf8", value.to_string_lossy().as_bytes().to_vec())
}

fn collect_invocation(argv: Vec<OsString>) -> InvocationReceipt {
    let mut raw_encoding = "empty_argv";
    let mut canonical = Vec::new();
    let mut argv_raw_hex = Vec::with_capacity(argv.len());
    let mut argv_lossy = Vec::with_capacity(argv.len());
    for argument in argv {
        let (encoding, raw) = os_str_raw_bytes(&argument);
        raw_encoding = encoding;
        let raw_len = u64::try_from(raw.len()).expect("argument length fits u64");
        canonical.extend_from_slice(&raw_len.to_le_bytes());
        canonical.extend_from_slice(&raw);
        argv_raw_hex.push(bytes_to_lower_hex(&raw));
        argv_lossy.push(argument.to_string_lossy().into_owned());
    }
    InvocationReceipt {
        argv_lossy,
        argv_raw_hex,
        raw_encoding,
        length_prefixed_argv_sha256: sha256_bytes(&canonical),
    }
}

fn read_trimmed(path: impl AsRef<Path>) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn cpu_model() -> Option<String> {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").ok()?;
    for key in ["model name", "Hardware", "Processor"] {
        if let Some(value) = cpuinfo.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name.trim() == key).then(|| value.trim().to_owned())
        }) {
            return Some(value);
        }
    }
    None
}

fn numeric_directories(path: &Path, prefix: &str) -> Option<Vec<String>> {
    let mut names = fs::read_dir(path)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| {
            name.strip_prefix(prefix).is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    Some(names)
}

fn collect_cpu_topology() -> (CpuTopologyReceipt, BTreeMap<String, String>) {
    let cpu_root = Path::new("/sys/devices/system/cpu");
    let Some(cpu_names) = numeric_directories(cpu_root, "cpu") else {
        return (
            CpuTopologyReceipt {
                logical_cpu_directories: None,
                physical_package_count: None,
                physical_core_count: None,
            },
            BTreeMap::new(),
        );
    };
    let mut packages = BTreeSet::new();
    let mut cores = BTreeSet::new();
    let mut governors = BTreeMap::new();
    for cpu_name in &cpu_names {
        let cpu_path = cpu_root.join(cpu_name);
        let package = read_trimmed(cpu_path.join("topology/physical_package_id"));
        let core = read_trimmed(cpu_path.join("topology/core_id"));
        if let Some(package) = package.as_ref() {
            packages.insert(package.clone());
        }
        if let (Some(package), Some(core)) = (package, core) {
            cores.insert((package, core));
        }
        if let Some(governor) = read_trimmed(cpu_path.join("cpufreq/scaling_governor")) {
            governors.insert(cpu_name.clone(), governor);
        }
    }
    (
        CpuTopologyReceipt {
            logical_cpu_directories: Some(cpu_names.len()),
            physical_package_count: (!packages.is_empty()).then_some(packages.len()),
            physical_core_count: (!cores.is_empty()).then_some(cores.len()),
        },
        governors,
    )
}

fn collect_static_measurement_host() -> StaticMeasurementHostReceipt {
    let hostname = read_trimmed("/etc/hostname").or_else(|| {
        let command = Command::new("hostname");
        command_stdout(command)
            .ok()
            .filter(|value| !value.is_empty())
    });
    let cpu_model = cpu_model();
    let available_parallelism = std::thread::available_parallelism()
        .ok()
        .map(|value| value.get());
    let cpu_online = read_trimmed("/sys/devices/system/cpu/online");
    let cpu_present = read_trimmed("/sys/devices/system/cpu/present");
    let cpu_possible = read_trimmed("/sys/devices/system/cpu/possible");
    let cpu_isolated = read_trimmed("/sys/devices/system/cpu/isolated");
    let (cpu_topology, scaling_governors_by_cpu) = collect_cpu_topology();
    let kernel_release = read_trimmed("/proc/sys/kernel/osrelease");
    let kernel_version = read_trimmed("/proc/version");
    let numa_online_nodes = read_trimmed("/sys/devices/system/node/online");
    let numa_possible_nodes = read_trimmed("/sys/devices/system/node/possible");
    let numa_node_directories =
        numeric_directories(Path::new("/sys/devices/system/node"), "node").map(|nodes| nodes.len());
    let mut unavailable_fields = Vec::new();
    for (name, available) in [
        ("hostname", hostname.is_some()),
        ("cpu_model", cpu_model.is_some()),
        ("available_parallelism", available_parallelism.is_some()),
        ("cpu_online", cpu_online.is_some()),
        ("cpu_present", cpu_present.is_some()),
        ("cpu_possible", cpu_possible.is_some()),
        ("kernel_release", kernel_release.is_some()),
        ("kernel_version", kernel_version.is_some()),
        ("numa_online_nodes", numa_online_nodes.is_some()),
        ("numa_possible_nodes", numa_possible_nodes.is_some()),
        ("numa_node_directories", numa_node_directories.is_some()),
    ] {
        if !available {
            unavailable_fields.push(name.to_owned());
        }
    }
    if cpu_isolated.is_none() {
        unavailable_fields.push("cpu_isolated".to_owned());
    }
    if scaling_governors_by_cpu.is_empty() {
        unavailable_fields.push("scaling_governors_by_cpu".to_owned());
    }
    StaticMeasurementHostReceipt {
        hostname,
        cpu_model,
        available_parallelism,
        cpu_online,
        cpu_present,
        cpu_possible,
        cpu_isolated,
        cpu_topology,
        scaling_governors_by_cpu,
        kernel_release,
        kernel_version,
        numa_online_nodes,
        numa_possible_nodes,
        numa_node_directories,
        unavailable_fields,
    }
}

fn proc_status_value(name: &str) -> Option<String> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key == name).then(|| value.trim().to_owned())
        })
}

fn current_cgroup_path(proc_self_cgroup: &str) -> Option<PathBuf> {
    for line in proc_self_cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let relative_path = fields.next()?.trim_start_matches('/');
        if hierarchy == "0" && controllers.is_empty() {
            return Some(Path::new("/sys/fs/cgroup").join(relative_path));
        }
        if controllers
            .split(',')
            .any(|controller| controller == "cpuset")
        {
            return Some(Path::new("/sys/fs/cgroup/cpuset").join(relative_path));
        }
    }
    None
}

fn collect_dynamic_measurement_host() -> DynamicMeasurementHostReceipt {
    let unix_epoch_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok());
    let proc_self_cgroup = read_trimmed("/proc/self/cgroup");
    let cgroup_path = proc_self_cgroup.as_deref().and_then(current_cgroup_path);
    let cpuset_cpus_effective = cgroup_path.as_deref().and_then(|path| {
        read_trimmed(path.join("cpuset.cpus.effective"))
            .or_else(|| read_trimmed(path.join("cpuset.cpus")))
    });
    let cpuset_mems_effective = cgroup_path.as_deref().and_then(|path| {
        read_trimmed(path.join("cpuset.mems.effective"))
            .or_else(|| read_trimmed(path.join("cpuset.mems")))
    });
    DynamicMeasurementHostReceipt {
        unix_epoch_millis,
        process_cpu_affinity_mask: proc_status_value("Cpus_allowed"),
        process_cpu_affinity_list: proc_status_value("Cpus_allowed_list"),
        proc_self_cgroup,
        cpuset_cpus_effective,
        cpuset_mems_effective,
        load_average: read_trimmed("/proc/loadavg"),
        pressure_cpu: read_trimmed("/proc/pressure/cpu"),
        pressure_memory: read_trimmed("/proc/pressure/memory"),
        pressure_io: read_trimmed("/proc/pressure/io"),
    }
}

fn is_known_receipt_value(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value != "unknown" && !value.starts_with("unknown:")
}

fn file_snapshot_is_complete(snapshot: &FileSnapshotReceipt) -> bool {
    snapshot.error.is_none()
        && snapshot
            .sha256
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && snapshot.bytes_read.is_some_and(|bytes| bytes > 0)
        && snapshot.metadata_size_bytes == snapshot.bytes_read
        && matches!(
            (snapshot.unix_device, snapshot.unix_inode),
            (Some(_), Some(_)) | (None, None)
        )
}

fn executable_identity_is_valid(executable: &ExecutableIdentityReceipt) -> bool {
    executable
        .current_exe_path
        .as_deref()
        .is_some_and(is_known_receipt_value)
        && executable
            .canonical_path
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && executable.path_resolution_error.is_none()
        && executable.process_id != 0
        && file_snapshot_is_complete(&executable.before_measurement)
        && file_snapshot_is_complete(&executable.after_measurement)
        && file_snapshots_match(
            &executable.before_measurement,
            &executable.after_measurement,
        ) == Some(true)
        && executable.unchanged_during_measurement == Some(true)
}

fn runtime_source_capture_is_valid(
    capture: &RuntimeSourceIdentityReceipt,
    build_source: &BuildSourceIdentityReceipt,
) -> bool {
    capture.workspace_root == build_source.workspace_root
        && capture
            .canonical_workspace_root
            .as_deref()
            .is_some_and(|root| root == build_source.workspace_root)
        && capture
            .git_sha
            .as_deref()
            .is_some_and(|sha| sha == build_source.git_sha)
        && capture
            .git_branch
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && capture.git_tree_state == "clean"
        && capture.matches_build_git_sha == Some(true)
        && capture.discovery_errors.is_empty()
}

fn source_identity_is_valid(subject: &SubjectIdentityReceipt) -> bool {
    let build = &subject.build_source;
    let runtime = &subject.runtime_source;
    is_known_receipt_value(&build.workspace_root)
        && is_known_receipt_value(&build.git_sha)
        && is_known_receipt_value(&build.git_branch)
        && build.git_tree_state == "clean"
        && build.build_input_tracking == "complete"
        && runtime_source_capture_is_valid(&runtime.before_measurement, build)
        && runtime_source_capture_is_valid(&runtime.after_measurement, build)
        && runtime.before_measurement.git_branch == runtime.after_measurement.git_branch
        && runtime.same_clean_git_identity_at_capture_points == Some(true)
}

fn cargo_lock_identity_is_valid(cargo_lock: &CargoLockIdentityReceipt) -> bool {
    cargo_lock.embedded_build_size_bytes > 0
        && is_known_receipt_value(&cargo_lock.embedded_build_sha256)
        && is_known_receipt_value(&cargo_lock.runtime_path)
        && file_snapshot_is_complete(&cargo_lock.before_measurement)
        && file_snapshot_is_complete(&cargo_lock.after_measurement)
        && cargo_lock.before_measurement.sha256.as_deref()
            == Some(cargo_lock.embedded_build_sha256.as_str())
        && cargo_lock.after_measurement.sha256.as_deref()
            == Some(cargo_lock.embedded_build_sha256.as_str())
        && cargo_lock.before_measurement.bytes_read == Some(cargo_lock.embedded_build_size_bytes)
        && cargo_lock.after_measurement.bytes_read == Some(cargo_lock.embedded_build_size_bytes)
        && file_snapshots_match(
            &cargo_lock.before_measurement,
            &cargo_lock.after_measurement,
        ) == Some(true)
        && cargo_lock.before_matches_embedded_build == Some(true)
        && cargo_lock.after_matches_embedded_build == Some(true)
        && cargo_lock.unchanged_at_capture_points == Some(true)
}

fn build_configuration_is_valid(configuration: &BuildConfigurationReceipt) -> bool {
    let dependency_graph_attestation_valid = configuration
        .resolved_dependency_feature_graph_sha256
        .as_deref()
        .map_or_else(
            || {
                configuration.resolved_dependency_feature_graph_limitation
                    == DEPENDENCY_GRAPH_ATTESTATION_UNAVAILABLE
            },
            |digest| {
                parse_optional_lower_sha256(digest).is_ok_and(|value| value.is_some())
                    && configuration.resolved_dependency_feature_graph_limitation
                        == DEPENDENCY_GRAPH_ATTESTATION_AVAILABLE
            },
        );
    [
        configuration.cargo_profile.as_str(),
        configuration.selected_profile.as_str(),
        configuration.profile_label.as_str(),
        configuration.opt_level.as_str(),
        configuration.debug.as_str(),
        configuration.target.as_str(),
        configuration.build_host.as_str(),
        configuration.rustc_version_verbose.as_str(),
        configuration.cargo_version.as_str(),
    ]
    .into_iter()
    .all(is_known_receipt_value)
        && configuration.rustflags.decode_error.is_none()
        && configuration.rustflags.decoded_arguments.is_some()
        && dependency_graph_attestation_valid
}

fn invocation_is_valid(invocation: &InvocationReceipt) -> bool {
    !invocation.argv_raw_hex.is_empty()
        && invocation.argv_raw_hex.len() == invocation.argv_lossy.len()
        && is_known_receipt_value(invocation.raw_encoding)
        && is_known_receipt_value(&invocation.length_prefixed_argv_sha256)
}

fn stable_required_placement(host: &MeasurementHostReceipt) -> bool {
    let before = &host.before_measurement;
    let after = &host.after_measurement;
    let stable_nonempty = |before: Option<&str>, after: Option<&str>| {
        before.is_some_and(is_known_receipt_value) && before == after
    };
    before.unix_epoch_millis.is_some()
        && after.unix_epoch_millis >= before.unix_epoch_millis
        && stable_nonempty(
            before.process_cpu_affinity_mask.as_deref(),
            after.process_cpu_affinity_mask.as_deref(),
        )
        && stable_nonempty(
            before.process_cpu_affinity_list.as_deref(),
            after.process_cpu_affinity_list.as_deref(),
        )
        && stable_nonempty(
            before.proc_self_cgroup.as_deref(),
            after.proc_self_cgroup.as_deref(),
        )
        && stable_nonempty(
            before.cpuset_cpus_effective.as_deref(),
            after.cpuset_cpus_effective.as_deref(),
        )
        && stable_nonempty(
            before.cpuset_mems_effective.as_deref(),
            after.cpuset_mems_effective.as_deref(),
        )
}

fn measurement_host_is_valid(host: &MeasurementHostReceipt) -> bool {
    let static_host = &host.host;
    static_host
        .hostname
        .as_deref()
        .is_some_and(is_known_receipt_value)
        && static_host
            .cpu_model
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && static_host
            .available_parallelism
            .is_some_and(|count| count > 0)
        && static_host
            .cpu_online
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && static_host
            .cpu_present
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && static_host
            .cpu_possible
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && static_host
            .cpu_topology
            .logical_cpu_directories
            .is_some_and(|count| count > 0)
        && static_host
            .kernel_release
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && static_host
            .kernel_version
            .as_deref()
            .is_some_and(is_known_receipt_value)
        && stable_required_placement(host)
}

fn provenance_evidence_is_valid(
    subject: &SubjectIdentityReceipt,
    environment: &ComparisonEnvironmentReceipt,
) -> bool {
    // This predicate only proves that the v7/v10 in-process diagnostic receipts
    // were complete and stable for this run. It does not fill the
    // schema-specific external-verification gaps, so a true result must remain
    // non-citable.
    executable_identity_is_valid(&subject.executable)
        && source_identity_is_valid(subject)
        && cargo_lock_identity_is_valid(&subject.cargo_lock)
        && build_configuration_is_valid(&environment.build_configuration)
        && invocation_is_valid(&environment.invocation)
        && measurement_host_is_valid(&environment.measurement_host)
}

impl ProvenanceCapture {
    fn begin() -> Self {
        let build_source = collect_build_source_identity();
        Self {
            executable: begin_executable_identity(),
            runtime_source_before_measurement: collect_runtime_source_identity(&build_source),
            cargo_lock: begin_cargo_lock_identity(&build_source),
            build_source,
            build_configuration: collect_build_configuration(),
            invocation: collect_invocation(std::env::args_os().collect()),
            host: collect_static_measurement_host(),
            host_before_measurement: collect_dynamic_measurement_host(),
        }
    }

    fn finish(self) -> (SubjectIdentityReceipt, ComparisonEnvironmentReceipt) {
        let runtime_source_after_measurement = collect_runtime_source_identity(&self.build_source);
        let subject_identity = SubjectIdentityReceipt {
            executable: finish_executable_identity(self.executable),
            build_source: self.build_source,
            runtime_source: runtime_source_stability(
                self.runtime_source_before_measurement,
                runtime_source_after_measurement,
            ),
            cargo_lock: finish_cargo_lock_identity(self.cargo_lock),
        };
        let comparison_environment = ComparisonEnvironmentReceipt {
            build_configuration: self.build_configuration,
            invocation: self.invocation,
            measurement_host: MeasurementHostReceipt {
                host: self.host,
                before_measurement: self.host_before_measurement,
                after_measurement: collect_dynamic_measurement_host(),
            },
        };
        (subject_identity, comparison_environment)
    }
}

// ─── CLI parsing (manual — no clap in workspace) ─────────────────────────

#[derive(Debug, Clone)]
struct Options {
    rows_per_thread: usize,
    threads: Vec<usize>,
    iters: usize,
    json_output: Option<PathBuf>,
    json_stdout: bool,
    summary_md: Option<PathBuf>,
    history_json: PathBuf,
    apples_to_apples: bool,
    separate_tables: bool,
    transaction_granularity: TransactionGranularity,
    synchronous: SynchronousMode,
    /// Fixed retry deadline override in seconds; one-row mode shares this
    /// deadline across every transaction attempted by a worker. When unset,
    /// the budget scales with threads x rows (bd-caa6u).
    retry_timeout_secs: Option<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rows_per_thread: DEFAULT_ROWS_PER_THREAD,
            threads: DEFAULT_THREADS.to_vec(),
            iters: DEFAULT_ITERS,
            json_output: None,
            json_stdout: false,
            summary_md: None,
            history_json: PathBuf::from(DEFAULT_HISTORY_JSON),
            apples_to_apples: false,
            separate_tables: false,
            transaction_granularity: TransactionGranularity::Bulk,
            synchronous: SynchronousMode::Normal,
            retry_timeout_secs: None,
        }
    }
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "usage: mt-mvcc-bench [--rows-per-thread=N] [--threads=N,N,...] [--iters=N] \\\n\
         [--json-output=PATH] [--json-stdout] [--summary-md=PATH] [--history-json=PATH] \\\n\
         [--apples-to-apples] \\\n\
         [--separate-tables] [--one-row-per-transaction] [--retry-timeout-secs=N] \\\n\
         [--synchronous=normal|full]\n\
         \n\
         defaults: --rows-per-thread={DEFAULT_ROWS_PER_THREAD} \
         --threads=1,2,4,8,16,32,64,128 --iters={DEFAULT_ITERS}\n\
         note: --apples-to-apples is a compatibility flag; this benchmark already\n\
         uses the prepared-statement/file-backed/shared-db path on both engines.\n\
         note: writer counts above MAX_CONCURRENT_WRITERS are reported and skipped;\n\
         counts above host available_parallelism are measured only as non-comparable diagnostics.\n\
         note: --one-row-per-transaction retries each complete one-row transaction and emits the\n\
         non-citable v10 report; the default bulk transaction retains the v7 analyzer contract.\n\
         note: --rows-per-thread=0 reduces the run to shared-file worker open + synchronized start,\n\
         which is the minimal repro for the 13+ thread startup-open failure."
    );
    std::process::exit(code);
}

fn print_usage_error(message: impl Display) -> ! {
    eprintln!("{message}");
    print_usage_and_exit(2);
}

fn parse_thread_count(raw: &str) -> Result<usize, String> {
    let threads = raw
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("invalid thread count in --threads: {raw}"))?;
    if threads == 0 {
        return Err("--threads values must be >= 1".to_owned());
    }
    Ok(threads)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParseArgsError {
    Help,
    Message(String),
}

fn parse_args_from<I, S>(args: I) -> Result<Options, ParseArgsError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut opts = Options::default();
    let mut history_json_explicit = false;
    let mut args = args.into_iter().map(Into::into);
    while let Some(arg) = args.next() {
        if arg == "--apples-to-apples" {
            opts.apples_to_apples = true;
            continue;
        }
        if arg == "--separate-tables" {
            opts.separate_tables = true;
            continue;
        }
        if arg == "--one-row-per-transaction" {
            opts.transaction_granularity = TransactionGranularity::OneRow;
            continue;
        }
        if arg == "--json-stdout" {
            opts.json_stdout = true;
            continue;
        }
        let (key, val) = if let Some(eq) = arg.find('=') {
            (arg[..eq].to_owned(), arg[eq + 1..].to_owned())
        } else if arg == "--help" || arg == "-h" {
            return Err(ParseArgsError::Help);
        } else {
            // Support space-separated form.
            let v = args.next().ok_or_else(|| {
                ParseArgsError::Message(format!("missing value for argument `{arg}`"))
            })?;
            (arg, v)
        };
        match key.as_str() {
            "--rows-per-thread" => {
                opts.rows_per_thread = val.parse().map_err(|_| {
                    ParseArgsError::Message(format!("invalid --rows-per-thread: {val}"))
                })?;
            }
            "--retry-timeout-secs" => {
                opts.retry_timeout_secs = Some(val.parse().map_err(|_| {
                    ParseArgsError::Message(format!("invalid --retry-timeout-secs: {val}"))
                })?);
            }
            "--threads" => {
                opts.threads = val
                    .split(',')
                    .map(|value| parse_thread_count(value).map_err(ParseArgsError::Message))
                    .collect::<Result<Vec<_>, _>>()?;
                if opts.threads.is_empty() {
                    return Err(ParseArgsError::Message(
                        "--threads must contain at least one value".to_owned(),
                    ));
                }
            }
            "--iters" => {
                opts.iters = val
                    .parse()
                    .map_err(|_| ParseArgsError::Message(format!("invalid --iters: {val}")))?;
                if opts.iters == 0 {
                    return Err(ParseArgsError::Message("--iters must be >= 1".to_owned()));
                }
            }
            "--synchronous" => {
                opts.synchronous = match val.trim().to_ascii_lowercase().as_str() {
                    "normal" => SynchronousMode::Normal,
                    "full" => SynchronousMode::Full,
                    _ => {
                        return Err(ParseArgsError::Message(format!(
                            "invalid --synchronous: {val}; expected normal or full"
                        )));
                    }
                };
            }
            "--json-output" => {
                opts.json_output = Some(PathBuf::from(val));
            }
            "--summary-md" => {
                opts.summary_md = Some(PathBuf::from(val));
            }
            "--history-json" => {
                opts.history_json = PathBuf::from(val);
                history_json_explicit = true;
            }
            other => {
                return Err(ParseArgsError::Message(format!(
                    "unknown argument: {other}"
                )));
            }
        }
    }
    if !history_json_explicit {
        opts.history_json =
            PathBuf::from(match (opts.separate_tables, opts.transaction_granularity) {
                (false, TransactionGranularity::Bulk) => DEFAULT_HISTORY_JSON,
                (true, TransactionGranularity::Bulk) => DEFAULT_SEPARATE_TABLES_HISTORY_JSON,
                (false, TransactionGranularity::OneRow) => DEFAULT_ONE_ROW_HISTORY_JSON,
                (true, TransactionGranularity::OneRow) => {
                    DEFAULT_SEPARATE_TABLES_ONE_ROW_HISTORY_JSON
                }
            });
    }
    Ok(opts)
}

fn parse_args() -> Options {
    match parse_args_from(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(ParseArgsError::Help) => print_usage_and_exit(0),
        Err(ParseArgsError::Message(message)) => print_usage_error(message),
    }
}

// ─── Reported per-config result ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EffectiveSettings {
    page_size_bytes: i64,
    journal_mode: String,
    synchronous: String,
    cache_size: i64,
    busy_timeout_ms: i64,
    wal_autocheckpoint_pages: i64,
    concurrent_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkAccounting {
    /// Logical writes the caller offered to this arm.
    offered_writes: usize,
    /// Physical INSERT calls, including work later rolled back and retried.
    attempted_writes: usize,
    /// Rows proven committed by the post-run database oracle.
    succeeded_writes: usize,
    /// Additional attempts actually performed after a transient failure.
    retried_operations: usize,
    /// Offered writes not present in the committed database.
    failed_writes: usize,
    /// Failure count independently reported by the worker loops.
    worker_reported_failed_writes: usize,
    exact: bool,
    diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CommittedStateOracle {
    expected_rows: usize,
    observed_rows: usize,
    expected_id_sum: i64,
    observed_id_sum: i64,
    expected_payload_sha256: String,
    observed_payload_sha256: String,
    integrity_check: Vec<String>,
    valid: bool,
    diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SampleEvidence {
    #[serde(default)]
    worker_startup_elapsed_ns: u128,
    #[serde(alias = "elapsed_ns")]
    workload_elapsed_ns: u128,
    settings: EffectiveSettings,
    accounting: WorkAccounting,
    committed_state: CommittedStateOracle,
}

#[derive(Debug, Clone)]
struct RunResult {
    /// Worker spawn/open/configuration time, excluded from throughput.
    worker_startup_elapsed: Duration,
    /// Synchronized writer work through the last transaction terminal point.
    workload_elapsed: Duration,
    settings: EffectiveSettings,
    accounting: WorkAccounting,
    committed_state: CommittedStateOracle,
}

impl RunResult {
    fn writes_per_sec(&self) -> f64 {
        let secs = self.workload_elapsed.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            let n = self.accounting.succeeded_writes as f64;
            n / secs
        }
    }

    fn elapsed_ms(&self) -> f64 {
        self.workload_elapsed.as_secs_f64() * 1_000.0
    }

    fn correctness_valid(&self) -> bool {
        self.accounting.exact && self.accounting.failed_writes == 0 && self.committed_state.valid
    }

    fn sample_evidence(&self) -> SampleEvidence {
        SampleEvidence {
            worker_startup_elapsed_ns: self.worker_startup_elapsed.as_nanos(),
            workload_elapsed_ns: self.workload_elapsed.as_nanos(),
            settings: self.settings.clone(),
            accounting: self.accounting.clone(),
            committed_state: self.committed_state.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct RunStats {
    samples: Vec<RunResult>,
}

impl RunStats {
    fn new(samples: Vec<RunResult>) -> Self {
        if let Some(first) = samples.first() {
            assert!(
                samples
                    .iter()
                    .all(|sample| sample.settings == first.settings),
                "all samples in one arm must have identical effective settings"
            );
        }
        Self { samples }
    }

    fn total_failed_rows(&self) -> usize {
        self.samples
            .iter()
            .map(|sample| sample.accounting.failed_writes)
            .sum()
    }

    fn total_offered_writes(&self) -> usize {
        self.samples
            .iter()
            .map(|sample| sample.accounting.offered_writes)
            .sum()
    }

    fn total_attempted_writes(&self) -> usize {
        self.samples
            .iter()
            .map(|sample| sample.accounting.attempted_writes)
            .sum()
    }

    fn total_succeeded_writes(&self) -> usize {
        self.samples
            .iter()
            .map(|sample| sample.accounting.succeeded_writes)
            .sum()
    }

    fn total_retried_operations(&self) -> usize {
        self.samples
            .iter()
            .map(|sample| sample.accounting.retried_operations)
            .sum()
    }

    fn all_correctness_valid(&self) -> bool {
        self.samples.iter().all(RunResult::correctness_valid)
    }

    fn sample_evidence(&self) -> Vec<SampleEvidence> {
        self.samples
            .iter()
            .map(RunResult::sample_evidence)
            .collect()
    }

    fn p50_writes_per_sec(&self) -> f64 {
        self.percentile_by(RunResult::writes_per_sec, 0.50)
    }

    fn p95_writes_per_sec(&self) -> f64 {
        self.percentile_by(RunResult::writes_per_sec, 0.95)
    }

    fn p99_writes_per_sec(&self) -> f64 {
        self.percentile_by(RunResult::writes_per_sec, 0.99)
    }

    fn p50_elapsed_ms(&self) -> f64 {
        self.percentile_by(RunResult::elapsed_ms, 0.50)
    }

    fn p95_elapsed_ms(&self) -> f64 {
        self.percentile_by(RunResult::elapsed_ms, 0.95)
    }

    fn p99_elapsed_ms(&self) -> f64 {
        self.percentile_by(RunResult::elapsed_ms, 0.99)
    }

    fn percentile_by(&self, value: fn(&RunResult) -> f64, percentile: f64) -> f64 {
        let values = self.samples.iter().map(value).collect();
        percentile_value(values, percentile)
    }
}

#[derive(Debug, Clone)]
struct RatioStats {
    median: f64,
    ci95: (f64, f64),
    cv_pct: f64,
    mad: f64,
}

#[derive(Debug, Clone)]
struct PairedRunStats {
    arm_a: RunStats,
    arm_b: RunStats,
    ratio: RatioStats,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MedianCiContractReport {
    null_ratio_median: f64,
    null_ratio_ci95_low: f64,
    null_ratio_ci95_high: f64,
    null_ratio_cv_pct: f64,
    null_ratio_mad: f64,
    claim_ratio_median: f64,
    claim_ratio_ci95_low: f64,
    claim_ratio_ci95_high: f64,
    claim_ratio_cv_pct: f64,
    claim_ratio_mad: f64,
    null_radius: f64,
    min_decidable_gain: f64,
    max_decidable_regression: f64,
    claim_margin: Option<f64>,
    cv_gate: String,
    verdict: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RetryPolicyReceipt {
    csqlite_busy_timeout_ms: u64,
    csqlite_max_operation_retries: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    csqlite_max_transaction_retries: Option<usize>,
    csqlite_retry_sleep_ms: u64,
    csqlite_retry_unit: String,
    #[serde(default)]
    csqlite_retry_algorithm: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shared_worker_retry_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shared_worker_retry_timeout_overridden: Option<bool>,
    fsqlite_transaction_timeout_ms: u64,
    fsqlite_max_transaction_retries: usize,
    fsqlite_retry_sleep_base_ms: u64,
    fsqlite_retry_sleep_cap_ms: u64,
    fsqlite_retry_unit: String,
    #[serde(default)]
    fsqlite_retry_backoff_algorithm: String,
    #[serde(default)]
    fsqlite_retryable_errors: String,
    fsqlite_timeout_overridden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TransactionContractReceipt {
    granularity: &'static str,
    rows_per_transaction: usize,
    prepared_statement_scope: &'static str,
    duplicate_after_ambiguous_commit_policy: &'static str,
    csqlite_retry_unit: String,
    fsqlite_retry_unit: String,
}

fn transaction_contract_receipt(
    transaction_granularity: TransactionGranularity,
    retry_policy: &RetryPolicyReceipt,
) -> Option<TransactionContractReceipt> {
    (transaction_granularity == TransactionGranularity::OneRow).then(|| {
        TransactionContractReceipt {
            granularity: transaction_granularity.label(),
            rows_per_transaction: 1,
            prepared_statement_scope: "one successfully prepared statement per worker, reused across row transactions; transient preparation failures retry under the shared worker deadline",
            duplicate_after_ambiguous_commit_policy: "fail_closed; a duplicate is never accepted as proof of exact id+payload",
            csqlite_retry_unit: retry_policy.csqlite_retry_unit.clone(),
            fsqlite_retry_unit: retry_policy.fsqlite_retry_unit.clone(),
        }
    })
}

fn transaction_contract_is_valid(
    schema_version: &str,
    transaction_granularity: TransactionGranularity,
    rows_per_thread: usize,
    contract: Option<&TransactionContractReceipt>,
    configuration_receipts: &[ConfigurationReceipt],
) -> bool {
    match transaction_granularity {
        TransactionGranularity::Bulk => schema_version == REPORT_SCHEMA_V7 && contract.is_none(),
        TransactionGranularity::OneRow => {
            let Some(contract) = contract else {
                return false;
            };
            if schema_version != REPORT_SCHEMA_V10
                || rows_per_thread == 0
                || contract.granularity != TransactionGranularity::OneRow.label()
                || contract.rows_per_transaction != 1
                || contract.prepared_statement_scope
                    != "one successfully prepared statement per worker, reused across row transactions; transient preparation failures retry under the shared worker deadline"
                || contract.duplicate_after_ambiguous_commit_policy
                    != "fail_closed; a duplicate is never accepted as proof of exact id+payload"
            {
                return false;
            }
            !configuration_receipts.is_empty()
                && configuration_receipts.iter().all(|configuration| {
                    configuration.retry_policy.as_ref().is_some_and(|policy| {
                        policy.csqlite_max_operation_retries == 0
                            && policy.csqlite_max_transaction_retries == Some(MAX_RETRIES)
                            && policy.shared_worker_retry_timeout_ms
                                == Some(policy.fsqlite_transaction_timeout_ms)
                            && policy.shared_worker_retry_timeout_overridden
                                == Some(policy.fsqlite_timeout_overridden)
                            && policy.csqlite_retry_unit == CSQLITE_ONE_ROW_RETRY_UNIT
                            && policy.fsqlite_retry_unit == FSQLITE_ONE_ROW_RETRY_UNIT
                            && policy.csqlite_retry_unit == contract.csqlite_retry_unit
                            && policy.fsqlite_retry_unit == contract.fsqlite_retry_unit
                            && policy.csqlite_retry_algorithm == CSQLITE_ONE_ROW_RETRY_ALGORITHM
                            && policy.fsqlite_retry_backoff_algorithm
                                == FSQLITE_ONE_ROW_RETRY_BACKOFF_ALGORITHM
                    })
                })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConfigurationReceipt {
    writers: usize,
    available_parallelism: Option<usize>,
    max_supported_writers: usize,
    #[serde(default)]
    wal_autocheckpoint_pages: Option<i64>,
    #[serde(default)]
    wal_autocheckpoint_overridden: Option<bool>,
    #[serde(default)]
    offered_writes_per_sample: Option<usize>,
    #[serde(default)]
    retry_policy: Option<RetryPolicyReceipt>,
    status: String,
    comparison_eligible: bool,
    measured: bool,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RoundOrderReceipt {
    round_index: usize,
    execution_order: [String; 4],
}

fn round_order_receipt(round_index: usize) -> RoundOrderReceipt {
    let order = if round_index.is_multiple_of(2) {
        [
            "csqlite_null_a",
            "csqlite_null_b",
            "csqlite_baseline",
            "fsqlite_candidate",
        ]
    } else {
        [
            "fsqlite_candidate",
            "csqlite_baseline",
            "csqlite_null_b",
            "csqlite_null_a",
        ]
    };
    RoundOrderReceipt {
        round_index,
        execution_order: order.map(str::to_owned),
    }
}

fn round_order_receipts_are_valid(receipts: &[RoundOrderReceipt], rounds: usize) -> bool {
    receipts.len() == rounds
        && receipts
            .iter()
            .enumerate()
            .all(|(round_index, receipt)| receipt == &round_order_receipt(round_index))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ThreadTruthReport {
    configuration: ConfigurationReceipt,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    round_order_receipts: Vec<RoundOrderReceipt>,
    null_c_a_samples: Vec<SampleEvidence>,
    null_c_b_samples: Vec<SampleEvidence>,
    sqlite_samples: Vec<SampleEvidence>,
    fsqlite_samples: Vec<SampleEvidence>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ThreadComparisonReport {
    threads: usize,
    fsqlite_wps_p50: f64,
    fsqlite_wps_p95: f64,
    fsqlite_wps_p99: f64,
    sqlite_wps_p50: f64,
    sqlite_wps_p95: f64,
    sqlite_wps_p99: f64,
    throughput_ratio: f64,
    fsqlite_ms_p50: f64,
    fsqlite_ms_p95: f64,
    fsqlite_ms_p99: f64,
    sqlite_ms_p50: f64,
    sqlite_ms_p95: f64,
    sqlite_ms_p99: f64,
    time_ratio: f64,
    fsqlite_failed_rows: usize,
    sqlite_failed_rows: usize,
    #[serde(default)]
    median_ci_contract: Option<MedianCiContractReport>,
    #[serde(default)]
    truth: Option<ThreadTruthReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct MtMvccBenchReport {
    schema_version: &'static str,
    citable: bool,
    measurement_evidence_valid: bool,
    non_citable_reason: &'static str,
    release_regression_scope: &'static str,
    subject_identity: SubjectIdentityReceipt,
    comparison_environment: ComparisonEnvironmentReceipt,
    settings_interpretation: &'static str,
    accounting_interpretation: &'static str,
    timing_interpretation: &'static str,
    workload_shape: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_contract: Option<TransactionContractReceipt>,
    rows_per_thread: usize,
    iterations: usize,
    configuration_receipts: Vec<ConfigurationReceipt>,
    thread_results: Vec<ThreadComparisonReport>,
    pass_over_pass_gate: PassOverPassGateReport,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct PassOverPassGateReport {
    schema_version: &'static str,
    history_json_path: String,
    threshold_ratio_drop_pct: f64,
    status: &'static str,
    previous_report_found: bool,
    comparable_pair_count: usize,
    regressions: Vec<RatioRegression>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct RatioRegression {
    threads: usize,
    previous_ratio: f64,
    current_ratio: f64,
    ratio_drop_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoricalMtMvccBenchReport {
    schema_version: Option<String>,
    citable: Option<bool>,
    measurement_evidence_valid: Option<bool>,
    subject_identity: Option<serde_json::Value>,
    comparison_environment: Option<serde_json::Value>,
    settings_interpretation: Option<String>,
    accounting_interpretation: Option<String>,
    timing_interpretation: Option<String>,
    workload_shape: Option<String>,
    rows_per_thread: Option<usize>,
    iterations: Option<usize>,
    configuration_receipts: Option<Vec<ConfigurationReceipt>>,
    #[serde(default)]
    thread_results: Vec<ThreadComparisonReport>,
}

#[derive(Debug, Clone)]
struct FsqliteRetryBudget {
    attempts: usize,
    started: Instant,
    timeout: Duration,
}

impl FsqliteRetryBudget {
    fn new(timeout: Duration) -> Self {
        Self {
            attempts: 0,
            started: Instant::now(),
            timeout,
        }
    }

    const fn with_started(started: Instant, timeout: Duration) -> Self {
        Self {
            attempts: 0,
            started,
            timeout,
        }
    }

    const fn attempts(&self) -> usize {
        self.attempts
    }

    fn next_wait(&mut self, tid: usize) -> Option<Duration> {
        if self.attempts >= MAX_RETRIES || self.started.elapsed() >= self.timeout {
            return None;
        }
        self.attempts += 1;
        let exp_shift = (self.attempts / 8).min(5);
        let base_ms = RETRY_SLEEP_MS
            .saturating_mul(1_u64 << exp_shift)
            .min(MAX_RETRY_SLEEP_MS);
        let jitter_ms = ((tid as u64).wrapping_mul(7) + (self.attempts as u64).wrapping_mul(3)) % 5;
        Some(Duration::from_millis(base_ms.saturating_add(jitter_ms)))
    }
}

#[derive(Debug, Clone, Copy)]
struct OneRowWorkerRetryDeadline {
    started: Instant,
    timeout: Duration,
}

impl OneRowWorkerRetryDeadline {
    fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    #[cfg(test)]
    const fn with_started(started: Instant, timeout: Duration) -> Self {
        Self { started, timeout }
    }

    const fn fsqlite_budget(self) -> FsqliteRetryBudget {
        FsqliteRetryBudget::with_started(self.started, self.timeout)
    }

    fn allows_retry(self, retries: usize) -> bool {
        retries < MAX_RETRIES && self.started.elapsed() < self.timeout
    }
}

#[derive(Debug, Clone)]
struct StartupFailure {
    tid: usize,
    error: String,
}

#[derive(Debug)]
struct WorkerWork {
    settings: EffectiveSettings,
    attempted_writes: usize,
    retried_operations: usize,
    reported_failed_writes: usize,
    workload_finished: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupResultKind {
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
struct StartupOutcome {
    tid: usize,
    kind: StartupResultKind,
    error: Option<String>,
}

#[derive(Debug, Default)]
struct StartupGateState {
    release: bool,
    abort: bool,
}

#[derive(Debug)]
struct WorkerAggregate {
    attempted_writes: usize,
    retried_operations: usize,
    reported_failed_writes: usize,
    workload_finished: Instant,
}

fn collect_startup_outcomes(
    engine: &str,
    threads: usize,
    startup_rx: &mpsc::Receiver<StartupOutcome>,
) -> Result<Vec<StartupFailure>, String> {
    let mut failures = Vec::new();
    for _ in 0..threads {
        let outcome = startup_rx
            .recv_timeout(STARTUP_COORDINATION_TIMEOUT)
            .map_err(|error| {
                format!(
                    "{engine} startup coordination timed out after {:?}: {error}",
                    STARTUP_COORDINATION_TIMEOUT
                )
            })?;
        if outcome.kind == StartupResultKind::Failed {
            failures.push(StartupFailure {
                tid: outcome.tid,
                error: outcome
                    .error
                    .unwrap_or_else(|| "unknown startup failure".to_owned()),
            });
        }
    }
    Ok(failures)
}

fn publish_startup_decision(
    startup_gate: &Arc<(Mutex<StartupGateState>, Condvar)>,
    release: bool,
) -> Instant {
    let (gate_lock, gate_cv) = &**startup_gate;
    let mut gate_state = gate_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let published_at = Instant::now();
    gate_state.release = release;
    gate_state.abort = !release;
    gate_cv.notify_all();
    published_at
}

fn join_worker_handles(
    engine: &str,
    handles: Vec<thread::JoinHandle<Result<WorkerWork, String>>>,
    expected_settings: &EffectiveSettings,
) -> Result<WorkerAggregate, String> {
    let mut attempted_writes = 0usize;
    let mut retried_operations = 0usize;
    let mut reported_failed_writes = 0usize;
    let mut workload_finished = None::<Instant>;
    let mut errors = Vec::new();

    for (tid, handle) in handles.into_iter().enumerate() {
        let work = match handle.join() {
            Ok(Ok(work)) => work,
            Ok(Err(error)) => {
                errors.push(format!("{engine} worker t{tid} failed: {error}"));
                continue;
            }
            Err(_) => {
                errors.push(format!("{engine} worker t{tid} panicked"));
                continue;
            }
        };
        if work.settings != *expected_settings {
            errors.push(format!(
                "{engine} worker {tid} settings differ from schema connection: \
                 init={expected_settings:?}, worker={:?}",
                work.settings
            ));
        }
        match attempted_writes.checked_add(work.attempted_writes) {
            Some(total) => attempted_writes = total,
            None => errors.push(format!("{engine} aggregate write-attempt overflow")),
        }
        match retried_operations.checked_add(work.retried_operations) {
            Some(total) => retried_operations = total,
            None => errors.push(format!("{engine} aggregate retry overflow")),
        }
        match reported_failed_writes.checked_add(work.reported_failed_writes) {
            Some(total) => reported_failed_writes = total,
            None => errors.push(format!("{engine} aggregate failure overflow")),
        }
        workload_finished = Some(workload_finished.map_or(work.workload_finished, |current| {
            current.max(work.workload_finished)
        }));
    }

    if !errors.is_empty() {
        return Err(errors.join("; "));
    }
    let workload_finished = workload_finished
        .ok_or_else(|| format!("{engine} produced no worker completion timestamp"))?;
    Ok(WorkerAggregate {
        attempted_writes,
        retried_operations,
        reported_failed_writes,
        workload_finished,
    })
}

fn cleanup_workers_after_startup_failure(
    engine: &str,
    startup_gate: &Arc<(Mutex<StartupGateState>, Condvar)>,
    handles: Vec<thread::JoinHandle<Result<WorkerWork, String>>>,
    expected_settings: &EffectiveSettings,
    primary_error: String,
) -> String {
    publish_startup_decision(startup_gate, false);
    match join_worker_handles(engine, handles, expected_settings) {
        Ok(_) => primary_error,
        Err(cleanup_error) => format!("{primary_error}; worker cleanup: {cleanup_error}"),
    }
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one sample");
    values.sort_by(f64::total_cmp);
    let upper = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[upper - 1], values[upper])
    } else {
        values[upper]
    }
}

fn bootstrap_median_ci95(ratios: &[f64]) -> (f64, f64) {
    assert!(!ratios.is_empty(), "bootstrap requires at least one sample");
    let mut state = 0x7a25_2026_c011_cafe_u64;
    let mut bootstrap_medians = Vec::with_capacity(CONTRACT_BOOTSTRAP_REPS);
    let mut resample = vec![0.0; ratios.len()];
    let len_u64 = u64::try_from(ratios.len()).expect("sample count fits in u64");

    for _ in 0..CONTRACT_BOOTSTRAP_REPS {
        for value in &mut resample {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let index = usize::try_from(state % len_u64).expect("sample index fits in usize");
            *value = ratios[index];
        }
        bootstrap_medians.push(median(&mut resample));
    }

    bootstrap_medians.sort_by(f64::total_cmp);
    let low = CONTRACT_BOOTSTRAP_REPS * 25 / 1_000;
    let high = (CONTRACT_BOOTSTRAP_REPS * 975 / 1_000).min(CONTRACT_BOOTSTRAP_REPS - 1);
    (bootstrap_medians[low], bootstrap_medians[high])
}

#[allow(clippy::cast_precision_loss)]
fn ratio_stats(ratios: &[f64]) -> RatioStats {
    let mut ratios_for_median = ratios.to_vec();
    let ratio_median = median(&mut ratios_for_median);
    let mean = ratios.iter().sum::<f64>() / ratios.len() as f64;
    let variance = if ratios.len() > 1 {
        ratios
            .iter()
            .map(|ratio| (ratio - mean).powi(2))
            .sum::<f64>()
            / (ratios.len() - 1) as f64
    } else {
        0.0
    };
    let cv_pct = if mean == 0.0 {
        0.0
    } else {
        variance.sqrt() / mean.abs() * 100.0
    };
    let mut deviations = ratios
        .iter()
        .map(|ratio| (ratio - ratio_median).abs())
        .collect::<Vec<_>>();

    RatioStats {
        median: ratio_median,
        ci95: bootstrap_median_ci95(ratios),
        cv_pct,
        mad: median(&mut deviations),
    }
}

fn paired_run_stats(arm_a: Vec<RunResult>, arm_b: Vec<RunResult>) -> PairedRunStats {
    assert_eq!(
        arm_a.len(),
        arm_b.len(),
        "paired arms must contain the same number of rounds"
    );
    assert!(!arm_a.is_empty(), "paired run requires at least one round");
    let ratios = arm_a
        .iter()
        .zip(&arm_b)
        .map(|(baseline, candidate)| {
            assert_eq!(
                baseline.accounting.offered_writes, candidate.accounting.offered_writes,
                "paired arms must receive equal offered work"
            );
            let baseline_wps = baseline.writes_per_sec();
            if baseline_wps <= 0.0 {
                0.0
            } else {
                candidate.writes_per_sec() / baseline_wps
            }
        })
        .collect::<Vec<_>>();

    PairedRunStats {
        arm_a: RunStats::new(arm_a),
        arm_b: RunStats::new(arm_b),
        ratio: ratio_stats(&ratios),
    }
}

fn median_ci_contract(
    null: &PairedRunStats,
    claim: &PairedRunStats,
    configuration: &ConfigurationReceipt,
) -> MedianCiContractReport {
    let null_radius = (null.ratio.ci95.0 - 1.0)
        .abs()
        .max((null.ratio.ci95.1 - 1.0).abs());
    let decisive_effect = (2.0 * null_radius).max(0.01);
    let min_decidable_gain = 1.0 + decisive_effect;
    let max_decidable_regression = 1.0 - decisive_effect;
    let claim_effect = (claim.ratio.median - 1.0).abs();
    let claim_margin = if null_radius == 0.0 {
        None
    } else {
        Some(claim_effect / null_radius)
    };
    let failed_rows = null.arm_a.total_failed_rows()
        + null.arm_b.total_failed_rows()
        + claim.arm_a.total_failed_rows()
        + claim.arm_b.total_failed_rows();
    let offered_writes = null.arm_a.total_offered_writes()
        + null.arm_b.total_offered_writes()
        + claim.arm_a.total_offered_writes()
        + claim.arm_b.total_offered_writes();
    let verdict = if offered_writes == 0 {
        "INVALID_ZERO_COMMITTED_WORK"
    } else if failed_rows != 0 {
        "INVALID_FAILED_ROWS"
    } else if !null.arm_a.all_correctness_valid()
        || !null.arm_b.all_correctness_valid()
        || !claim.arm_a.all_correctness_valid()
        || !claim.arm_b.all_correctness_valid()
    {
        "INVALID_CORRECTNESS_ORACLE"
    } else if !configuration.comparison_eligible {
        match configuration.status.as_str() {
            "oversubscribed" => "INVALID_OVERSUBSCRIBED",
            "capacity_unknown" => "INVALID_CAPACITY_UNKNOWN",
            _ => "INVALID_CONFIGURATION",
        }
    } else if claim.ratio.ci95.0 > min_decidable_gain {
        "FSQLITE_FASTER"
    } else if claim.ratio.ci95.1 < max_decidable_regression {
        "FSQLITE_SLOWER"
    } else {
        "INCONCLUSIVE"
    };

    MedianCiContractReport {
        null_ratio_median: null.ratio.median,
        null_ratio_ci95_low: null.ratio.ci95.0,
        null_ratio_ci95_high: null.ratio.ci95.1,
        null_ratio_cv_pct: null.ratio.cv_pct,
        null_ratio_mad: null.ratio.mad,
        claim_ratio_median: claim.ratio.median,
        claim_ratio_ci95_low: claim.ratio.ci95.0,
        claim_ratio_ci95_high: claim.ratio.ci95.1,
        claim_ratio_cv_pct: claim.ratio.cv_pct,
        claim_ratio_mad: claim.ratio.mad,
        null_radius,
        min_decidable_gain,
        max_decidable_regression,
        claim_margin,
        cv_gate: "never".to_owned(),
        verdict: verdict.to_owned(),
    }
}

fn build_thread_report(
    threads: usize,
    null: &PairedRunStats,
    claim: &PairedRunStats,
    configuration: &ConfigurationReceipt,
    round_order_receipts: &[RoundOrderReceipt],
) -> ThreadComparisonReport {
    let sqlite = &claim.arm_a;
    let fsqlite = &claim.arm_b;
    let fsqlite_wps_p50 = fsqlite.p50_writes_per_sec();
    let sqlite_wps_p50 = sqlite.p50_writes_per_sec();
    let throughput_ratio = claim.ratio.median;
    let fsqlite_ms_p50 = fsqlite.p50_elapsed_ms();
    let sqlite_ms_p50 = sqlite.p50_elapsed_ms();
    let time_ratio = if sqlite_ms_p50 > 0.0 {
        fsqlite_ms_p50 / sqlite_ms_p50
    } else {
        0.0
    };

    ThreadComparisonReport {
        threads,
        fsqlite_wps_p50,
        fsqlite_wps_p95: fsqlite.p95_writes_per_sec(),
        fsqlite_wps_p99: fsqlite.p99_writes_per_sec(),
        sqlite_wps_p50,
        sqlite_wps_p95: sqlite.p95_writes_per_sec(),
        sqlite_wps_p99: sqlite.p99_writes_per_sec(),
        throughput_ratio,
        fsqlite_ms_p50,
        fsqlite_ms_p95: fsqlite.p95_elapsed_ms(),
        fsqlite_ms_p99: fsqlite.p99_elapsed_ms(),
        sqlite_ms_p50,
        sqlite_ms_p95: sqlite.p95_elapsed_ms(),
        sqlite_ms_p99: sqlite.p99_elapsed_ms(),
        time_ratio,
        fsqlite_failed_rows: fsqlite.total_failed_rows(),
        sqlite_failed_rows: sqlite.total_failed_rows(),
        median_ci_contract: Some(median_ci_contract(null, claim, configuration)),
        truth: Some(ThreadTruthReport {
            configuration: configuration.clone(),
            round_order_receipts: round_order_receipts.to_vec(),
            null_c_a_samples: null.arm_a.sample_evidence(),
            null_c_b_samples: null.arm_b.sample_evidence(),
            sqlite_samples: sqlite.sample_evidence(),
            fsqlite_samples: fsqlite.sample_evidence(),
        }),
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create parent directory {}: {error}", parent.display()))?;
    }
    Ok(())
}

fn render_markdown_summary(report: &MtMvccBenchReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# mt-mvcc-bench Summary\n");
    let _ = writeln!(out, "- Citable: `{}`", report.citable);
    let _ = writeln!(
        out,
        "- Measurement evidence valid: `{}`",
        report.measurement_evidence_valid
    );
    let _ = writeln!(out, "- Non-citable reason: {}", report.non_citable_reason);
    let _ = writeln!(
        out,
        "- Release regression scope: {}",
        report.release_regression_scope
    );
    let _ = writeln!(
        out,
        "- Executable unchanged during measurement: `{:?}`",
        report
            .subject_identity
            .executable
            .unchanged_during_measurement
    );
    let _ = writeln!(
        out,
        "- Settings interpretation: {}",
        report.settings_interpretation
    );
    let _ = writeln!(
        out,
        "- Accounting interpretation: {}",
        report.accounting_interpretation
    );
    let _ = writeln!(
        out,
        "- Timing interpretation: {}",
        report.timing_interpretation
    );
    let _ = writeln!(out, "- Workload shape: `{}`", report.workload_shape);
    if let Some(contract) = &report.transaction_contract {
        let _ = writeln!(
            out,
            "- Transaction granularity: `{}` (`{}` row per transaction)",
            contract.granularity, contract.rows_per_transaction
        );
        let _ = writeln!(
            out,
            "- C SQLite retry unit: `{}`",
            contract.csqlite_retry_unit
        );
        let _ = writeln!(
            out,
            "- FrankenSQLite retry unit: `{}`",
            contract.fsqlite_retry_unit
        );
        let _ = writeln!(
            out,
            "- Duplicate-after-ambiguous-commit policy: `{}`",
            contract.duplicate_after_ambiguous_commit_policy
        );
    }
    let _ = writeln!(out, "- Rows per thread: `{}`", report.rows_per_thread);
    let _ = writeln!(out, "- Iterations: `{}`", report.iterations);
    let _ = writeln!(out, "- Schema: `{}`\n", report.schema_version);
    let gate = &report.pass_over_pass_gate;
    let _ = writeln!(
        out,
        "- Pass-over-pass gate: `{}` (comparable pairs `{}`, threshold `{:.2}%`, history `{}`)",
        gate.status,
        gate.comparable_pair_count,
        gate.threshold_ratio_drop_pct,
        gate.history_json_path
    );
    if !gate.regressions.is_empty() {
        let _ = writeln!(out, "- Regressions:");
        for regression in &gate.regressions {
            let _ = writeln!(
                out,
                "  - {} threads: {:.2}x -> {:.2}x ({:.2}% drop)",
                regression.threads,
                regression.previous_ratio,
                regression.current_ratio,
                regression.ratio_drop_pct
            );
        }
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Threads | Configuration | fsqlite p50 wps | sqlite p50 wps | F/C median | F/C median CI95 | C/C A/A CI95 | Verdict | fsqlite committed/offered | sqlite committed/offered | fsqlite failed | sqlite failed |"
    );
    let _ = writeln!(
        out,
        "|---------|:--------------|-----------------:|---------------:|-----------:|----------------:|-------------:|:--------|----------------------------:|--------------------------:|---------------:|--------------:|"
    );
    for row in &report.thread_results {
        let (claim_ci, null_ci, verdict) = row.median_ci_contract.as_ref().map_or_else(
            || {
                (
                    "unavailable".to_owned(),
                    "unavailable".to_owned(),
                    "unavailable",
                )
            },
            |contract| {
                (
                    format!(
                        "[{:.3}, {:.3}]",
                        contract.claim_ratio_ci95_low, contract.claim_ratio_ci95_high
                    ),
                    format!(
                        "[{:.3}, {:.3}]",
                        contract.null_ratio_ci95_low, contract.null_ratio_ci95_high
                    ),
                    contract.verdict.as_str(),
                )
            },
        );
        let (
            configuration_status,
            fsqlite_committed,
            fsqlite_offered,
            sqlite_committed,
            sqlite_offered,
        ) = row.truth.as_ref().map_or_else(
            || ("unavailable", 0, 0, 0, 0),
            |truth| {
                (
                    truth.configuration.status.as_str(),
                    truth
                        .fsqlite_samples
                        .iter()
                        .map(|sample| sample.accounting.succeeded_writes)
                        .sum::<usize>(),
                    truth
                        .fsqlite_samples
                        .iter()
                        .map(|sample| sample.accounting.offered_writes)
                        .sum::<usize>(),
                    truth
                        .sqlite_samples
                        .iter()
                        .map(|sample| sample.accounting.succeeded_writes)
                        .sum::<usize>(),
                    truth
                        .sqlite_samples
                        .iter()
                        .map(|sample| sample.accounting.offered_writes)
                        .sum::<usize>(),
                )
            },
        );
        let _ = writeln!(
            out,
            "| {} | {} | {:.0} | {:.0} | {:.3}x | {} | {} | {} | {}/{} | {}/{} | {} | {} |",
            row.threads,
            configuration_status,
            row.fsqlite_wps_p50,
            row.sqlite_wps_p50,
            row.throughput_ratio,
            claim_ci,
            null_ci,
            verdict,
            fsqlite_committed,
            fsqlite_offered,
            sqlite_committed,
            sqlite_offered,
            row.fsqlite_failed_rows,
            row.sqlite_failed_rows
        );
    }
    if report
        .configuration_receipts
        .iter()
        .any(|receipt| !receipt.measured)
    {
        let _ = writeln!(out, "\n## Unmeasured configurations\n");
        for receipt in report
            .configuration_receipts
            .iter()
            .filter(|receipt| !receipt.measured)
        {
            let _ = writeln!(
                out,
                "- {} writers: `{}` — {}",
                receipt.writers, receipt.status, receipt.reason
            );
        }
    }
    out
}

fn write_json_report(path: &Path, report: &MtMvccBenchReport) -> Result<(), String> {
    ensure_parent_dir(path)?;
    let json = serde_json::to_vec_pretty(report)
        .map_err(|error| format!("serialize mt-mvcc bench report: {error}"))?;
    fs::write(path, json).map_err(|error| format!("write json report {}: {error}", path.display()))
}

fn write_canonical_json_stdout(report: &MtMvccBenchReport) -> Result<(), String> {
    // Serialize fully before touching stdout so serialization failure cannot
    // leave a partial JSON value in a machine-consumed stream. Struct field
    // order and BTreeMap ordering make this representation deterministic.
    let mut json = serde_json::to_vec(report)
        .map_err(|error| format!("serialize mt-mvcc bench stdout report: {error}"))?;
    json.push(b'\n');
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    lock.write_all(&json)
        .map_err(|error| format!("write mt-mvcc bench stdout report: {error}"))?;
    lock.flush()
        .map_err(|error| format!("flush mt-mvcc bench stdout report: {error}"))
}

const fn history_update_is_allowed(_report: &MtMvccBenchReport) -> bool {
    // V7 deliberately carries diagnostic provenance but is not independently
    // citable. Its own `citable` bit cannot authenticate the report, so even a
    // hand-edited v7 document must never become a trusted baseline. A future
    // schema may re-enable updates only through an explicit external verifier;
    // arbitrary new or misspelled schema strings must remain fail-closed.
    false
}

fn write_markdown_summary(path: &Path, report: &MtMvccBenchReport) -> Result<(), String> {
    ensure_parent_dir(path)?;
    fs::write(path, render_markdown_summary(report))
        .map_err(|error| format!("write markdown summary {}: {error}", path.display()))
}

fn load_previous_report(path: &Path) -> Result<Option<HistoricalMtMvccBenchReport>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .map_err(|error| format!("read history report {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("parse history report {}: {error}", path.display()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EffectiveSettingsFingerprint {
    sqlite: EffectiveSettings,
    fsqlite: EffectiveSettings,
    wal_autocheckpoint_pages: i64,
    wal_autocheckpoint_overridden: bool,
    offered_writes_per_sample: usize,
    retry_policy: RetryPolicyReceipt,
}

#[derive(Debug, Clone)]
struct ComparableHistoryRow {
    throughput_ratio: f64,
    settings: EffectiveSettingsFingerprint,
}

fn sample_evidence_is_valid(sample: &SampleEvidence) -> bool {
    let accounting = &sample.accounting;
    let committed = &sample.committed_state;
    sample.worker_startup_elapsed_ns > 0
        && sample.workload_elapsed_ns > 0
        && accounting.offered_writes > 0
        && accounting.exact
        && accounting.diagnostics.is_empty()
        && accounting.attempted_writes >= accounting.succeeded_writes
        && accounting.succeeded_writes == accounting.offered_writes
        && accounting.failed_writes == 0
        && accounting.worker_reported_failed_writes == 0
        && committed.valid
        && committed.diagnostics.is_empty()
        && committed.expected_rows == accounting.succeeded_writes
        && committed.observed_rows == accounting.succeeded_writes
        && committed.expected_id_sum == committed.observed_id_sum
        && committed.expected_payload_sha256 == committed.observed_payload_sha256
        && committed.integrity_check.len() == 1
        && committed
            .integrity_check
            .first()
            .is_some_and(|result| result == "ok")
}

fn uniform_effective_settings(
    samples: &[SampleEvidence],
    expected_offered_writes: usize,
) -> Option<EffectiveSettings> {
    let first = samples.first()?.settings.clone();
    samples
        .iter()
        .all(|sample| {
            sample_evidence_is_valid(sample)
                && sample.accounting.offered_writes == expected_offered_writes
                && sample.settings == first
        })
        .then_some(first)
}

fn valid_truth_settings(
    row: &ThreadComparisonReport,
    rows_per_thread: usize,
    transaction_granularity: TransactionGranularity,
) -> Option<EffectiveSettingsFingerprint> {
    let truth = row.truth.as_ref()?;
    let configuration = &truth.configuration;
    let rounds = truth.null_c_a_samples.len();
    if rounds == 0
        || truth.null_c_b_samples.len() != rounds
        || truth.sqlite_samples.len() != rounds
        || truth.fsqlite_samples.len() != rounds
        || (transaction_granularity == TransactionGranularity::OneRow
            && !round_order_receipts_are_valid(&truth.round_order_receipts, rounds))
        || configuration.writers != row.threads
        || !configuration.measured
        || !configuration.comparison_eligible
        || configuration.status != "supported"
        || configuration.wal_autocheckpoint_overridden != Some(false)
    {
        return None;
    }
    let expected_offered_writes = row.threads.checked_mul(rows_per_thread)?;
    let offered_writes_per_sample = configuration.offered_writes_per_sample?;
    if offered_writes_per_sample == 0 || offered_writes_per_sample != expected_offered_writes {
        return None;
    }
    let retry_policy = configuration.retry_policy.clone()?;
    let expected_retry_policy = retry_policy_receipt_for_granularity(
        fsqlite_retry_timeout(row.threads, rows_per_thread),
        false,
        transaction_granularity,
    )
    .ok()?;
    if retry_policy != expected_retry_policy {
        return None;
    }
    let wal_autocheckpoint_pages = configuration.wal_autocheckpoint_pages?;
    if wal_autocheckpoint_pages < 0 {
        return None;
    }

    let null_c_a = uniform_effective_settings(&truth.null_c_a_samples, offered_writes_per_sample)?;
    let null_c_b = uniform_effective_settings(&truth.null_c_b_samples, offered_writes_per_sample)?;
    let sqlite = uniform_effective_settings(&truth.sqlite_samples, offered_writes_per_sample)?;
    let fsqlite = uniform_effective_settings(&truth.fsqlite_samples, offered_writes_per_sample)?;
    let expected_sqlite =
        expected_effective_settings("sqlite_wal_single_writer", wal_autocheckpoint_pages);
    let expected_fsqlite = expected_effective_settings("fsqlite_mvcc_on", wal_autocheckpoint_pages);
    if null_c_a != expected_sqlite
        || null_c_b != expected_sqlite
        || sqlite != expected_sqlite
        || fsqlite != expected_fsqlite
    {
        return None;
    }

    Some(EffectiveSettingsFingerprint {
        sqlite,
        fsqlite,
        wal_autocheckpoint_pages,
        wal_autocheckpoint_overridden: false,
        offered_writes_per_sample,
        retry_policy,
    })
}

fn run_result_from_sample_evidence(sample: &SampleEvidence) -> Option<RunResult> {
    if !sample_evidence_is_valid(sample) {
        return None;
    }
    let startup_seconds = u64::try_from(sample.worker_startup_elapsed_ns / 1_000_000_000).ok()?;
    let startup_nanoseconds =
        u32::try_from(sample.worker_startup_elapsed_ns % 1_000_000_000).ok()?;
    let workload_seconds = u64::try_from(sample.workload_elapsed_ns / 1_000_000_000).ok()?;
    let workload_nanoseconds = u32::try_from(sample.workload_elapsed_ns % 1_000_000_000).ok()?;
    Some(RunResult {
        worker_startup_elapsed: Duration::new(startup_seconds, startup_nanoseconds),
        workload_elapsed: Duration::new(workload_seconds, workload_nanoseconds),
        settings: sample.settings.clone(),
        accounting: sample.accounting.clone(),
        committed_state: sample.committed_state.clone(),
    })
}

fn paired_stats_from_sample_evidence(
    arm_a: &[SampleEvidence],
    arm_b: &[SampleEvidence],
) -> Option<PairedRunStats> {
    if arm_a.is_empty() || arm_a.len() != arm_b.len() {
        return None;
    }
    let arm_a_settings = &arm_a.first()?.settings;
    let arm_b_settings = &arm_b.first()?.settings;
    if !arm_a
        .iter()
        .all(|sample| &sample.settings == arm_a_settings)
        || !arm_b
            .iter()
            .all(|sample| &sample.settings == arm_b_settings)
    {
        return None;
    }
    let offered_work_matches = arm_a.iter().zip(arm_b).all(|(baseline, candidate)| {
        baseline.accounting.offered_writes == candidate.accounting.offered_writes
    });
    if !offered_work_matches {
        return None;
    }
    let arm_a = arm_a
        .iter()
        .map(run_result_from_sample_evidence)
        .collect::<Option<Vec<_>>>()?;
    let arm_b = arm_b
        .iter()
        .map(run_result_from_sample_evidence)
        .collect::<Option<Vec<_>>>()?;
    Some(paired_run_stats(arm_a, arm_b))
}

fn valid_median_ci_contract(row: &ThreadComparisonReport) -> Option<&MedianCiContractReport> {
    let truth = row.truth.as_ref()?;
    let contract = row.median_ci_contract.as_ref()?;
    let null = paired_stats_from_sample_evidence(&truth.null_c_a_samples, &truth.null_c_b_samples)?;
    let claim = paired_stats_from_sample_evidence(&truth.sqlite_samples, &truth.fsqlite_samples)?;
    let expected = median_ci_contract(&null, &claim, &truth.configuration);
    (expected.eq(contract)
        && row.throughput_ratio.to_bits() == expected.claim_ratio_median.to_bits()
        && row.fsqlite_failed_rows == 0
        && row.sqlite_failed_rows == 0)
        .then_some(contract)
}

fn comparable_history_row(
    row: &ThreadComparisonReport,
    rows_per_thread: usize,
    transaction_granularity: TransactionGranularity,
) -> Option<ComparableHistoryRow> {
    let settings = valid_truth_settings(row, rows_per_thread, transaction_granularity)?;
    let contract = valid_median_ci_contract(row)?;
    Some(ComparableHistoryRow {
        throughput_ratio: contract.claim_ratio_median,
        settings,
    })
}

fn comparable_rows_by_thread(
    rows: &[ThreadComparisonReport],
    rows_per_thread: usize,
    transaction_granularity: TransactionGranularity,
) -> BTreeMap<usize, Vec<ComparableHistoryRow>> {
    let mut grouped = BTreeMap::<usize, Vec<ComparableHistoryRow>>::new();
    for row in rows {
        if let Some(comparable) =
            comparable_history_row(row, rows_per_thread, transaction_granularity)
        {
            grouped.entry(row.threads).or_default().push(comparable);
        }
    }
    grouped
}

fn history_evidence_is_invalid(
    wal_autocheckpoint_overridden: bool,
    retry_timeout_overridden: bool,
    rows_per_thread: usize,
    iterations: usize,
    transaction_granularity: TransactionGranularity,
    rows: &[ThreadComparisonReport],
    configuration_receipts: &[ConfigurationReceipt],
) -> bool {
    if wal_autocheckpoint_overridden
        || retry_timeout_overridden
        || iterations == 0
        || rows.is_empty()
        || rows.len() != configuration_receipts.len()
        || configuration_receipts
            .iter()
            .enumerate()
            .any(|(index, receipt)| {
                configuration_receipts[..index]
                    .iter()
                    .any(|prior| prior.writers == receipt.writers)
            })
        || configuration_receipts.iter().any(|receipt| {
            !receipt.comparison_eligible
                || receipt.wal_autocheckpoint_pages.is_none()
                || receipt.wal_autocheckpoint_overridden != Some(false)
                || receipt.offered_writes_per_sample.is_none()
                || receipt
                    .retry_policy
                    .as_ref()
                    .is_none_or(|policy| policy.fsqlite_timeout_overridden)
        })
        || rows.iter().any(|row| {
            row.truth.as_ref().is_none_or(|truth| {
                truth.null_c_a_samples.len() != iterations
                    || truth.null_c_b_samples.len() != iterations
                    || truth.sqlite_samples.len() != iterations
                    || truth.fsqlite_samples.len() != iterations
            })
        })
        || rows
            .iter()
            .zip(configuration_receipts)
            .any(|(row, receipt)| {
                row.threads != receipt.writers
                    || receipt.offered_writes_per_sample != row.threads.checked_mul(rows_per_thread)
                    || row
                        .truth
                        .as_ref()
                        .is_none_or(|truth| truth.configuration != *receipt)
            })
    {
        return true;
    }
    let comparable = comparable_rows_by_thread(rows, rows_per_thread, transaction_granularity);
    comparable.len() != rows.len() || comparable.values().any(|candidates| candidates.len() != 1)
}

fn historical_report_matches_contract(
    previous: &HistoricalMtMvccBenchReport,
    current_workload_shape: &str,
    current_rows_per_thread: usize,
    current_iterations: usize,
    current_transaction_granularity: TransactionGranularity,
) -> bool {
    let Some(configuration_receipts) = previous.configuration_receipts.as_deref() else {
        return false;
    };
    current_transaction_granularity == TransactionGranularity::Bulk
        && previous.schema_version.as_deref() == Some(REPORT_SCHEMA_V7)
        && previous.citable == Some(true)
        && previous.measurement_evidence_valid == Some(true)
        && previous
            .subject_identity
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .is_some_and(|identity| !identity.is_empty())
        && previous
            .comparison_environment
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .is_some_and(|environment| !environment.is_empty())
        && previous.settings_interpretation.as_deref() == Some(SETTINGS_INTERPRETATION)
        && previous.accounting_interpretation.as_deref() == Some(ACCOUNTING_INTERPRETATION)
        && previous.timing_interpretation.as_deref() == Some(TIMING_INTERPRETATION)
        && previous
            .workload_shape
            .as_deref()
            .is_some_and(|shape| shape == current_workload_shape)
        && previous.rows_per_thread == Some(current_rows_per_thread)
        && previous.iterations == Some(current_iterations)
        && !history_evidence_is_invalid(
            false,
            false,
            current_rows_per_thread,
            current_iterations,
            TransactionGranularity::Bulk,
            &previous.thread_results,
            configuration_receipts,
        )
}

struct PassOverPassGateInput<'a> {
    history_json: &'a Path,
    previous: Option<&'a HistoricalMtMvccBenchReport>,
    historical_baseline_authentication: HistoricalBaselineAuthentication,
    current_rows: &'a [ThreadComparisonReport],
    current_configuration_receipts: &'a [ConfigurationReceipt],
    current_workload_shape: &'a str,
    current_rows_per_thread: usize,
    current_iterations: usize,
    current_transaction_granularity: TransactionGranularity,
    current_wal_autocheckpoint_overridden: bool,
    current_retry_timeout_overridden: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoricalBaselineAuthentication {
    Unavailable,
    #[cfg(test)]
    VerifiedTestFixture,
}

impl HistoricalBaselineAuthentication {
    const fn is_independently_verified(self) -> bool {
        match self {
            Self::Unavailable => false,
            #[cfg(test)]
            Self::VerifiedTestFixture => true,
        }
    }
}

fn build_pass_over_pass_gate(input: PassOverPassGateInput<'_>) -> PassOverPassGateReport {
    let PassOverPassGateInput {
        history_json,
        previous,
        historical_baseline_authentication,
        current_rows,
        current_configuration_receipts,
        current_workload_shape,
        current_rows_per_thread,
        current_iterations,
        current_transaction_granularity,
        current_wal_autocheckpoint_overridden,
        current_retry_timeout_overridden,
    } = input;
    let previous_report_found = previous.is_some();
    if !historical_baseline_authentication.is_independently_verified() {
        return PassOverPassGateReport {
            schema_version: PASS_OVER_PASS_SCHEMA_V1,
            history_json_path: history_json.display().to_string(),
            threshold_ratio_drop_pct: PASS_OVER_PASS_MAX_RATIO_DROP_PCT,
            status: "disabled_non_citable",
            previous_report_found,
            comparable_pair_count: 0,
            regressions: Vec::new(),
        };
    }
    let previous = previous.filter(|previous| {
        historical_report_matches_contract(
            previous,
            current_workload_shape,
            current_rows_per_thread,
            current_iterations,
            current_transaction_granularity,
        )
    });
    let current_evidence_valid = !history_evidence_is_invalid(
        current_wal_autocheckpoint_overridden,
        current_retry_timeout_overridden,
        current_rows_per_thread,
        current_iterations,
        current_transaction_granularity,
        current_rows,
        current_configuration_receipts,
    );
    let mut comparable_pair_count = 0usize;
    let mut regressions = Vec::new();
    if let Some(previous) = previous {
        let previous_by_threads = comparable_rows_by_thread(
            &previous.thread_results,
            current_rows_per_thread,
            TransactionGranularity::Bulk,
        );
        let current_by_threads = if current_evidence_valid {
            comparable_rows_by_thread(
                current_rows,
                current_rows_per_thread,
                current_transaction_granularity,
            )
        } else {
            BTreeMap::new()
        };
        for (threads, current_candidates) in &current_by_threads {
            let Some(previous_candidates) = previous_by_threads.get(threads) else {
                continue;
            };
            if previous_candidates.len() != 1 || current_candidates.len() != 1 {
                continue;
            }
            let previous_row = &previous_candidates[0];
            let current_row = &current_candidates[0];
            if previous_row.settings != current_row.settings {
                continue;
            }
            comparable_pair_count += 1;
            if current_row.throughput_ratio >= previous_row.throughput_ratio {
                continue;
            }
            let ratio_drop_pct = ((previous_row.throughput_ratio - current_row.throughput_ratio)
                / previous_row.throughput_ratio)
                * 100.0;
            if ratio_drop_pct > PASS_OVER_PASS_MAX_RATIO_DROP_PCT {
                regressions.push(RatioRegression {
                    threads: *threads,
                    previous_ratio: previous_row.throughput_ratio,
                    current_ratio: current_row.throughput_ratio,
                    ratio_drop_pct,
                });
            }
        }
    }
    let status = if previous.is_none() {
        "no_prior_report"
    } else if comparable_pair_count == 0 {
        "no_comparable_rows"
    } else if regressions.is_empty() {
        "passed"
    } else {
        "failed"
    };
    PassOverPassGateReport {
        schema_version: PASS_OVER_PASS_SCHEMA_V1,
        history_json_path: history_json.display().to_string(),
        threshold_ratio_drop_pct: PASS_OVER_PASS_MAX_RATIO_DROP_PCT,
        status,
        previous_report_found,
        comparable_pair_count,
        regressions,
    }
}

fn format_startup_failures(label: &str, failures: &[StartupFailure]) -> String {
    let details = failures
        .iter()
        .map(|failure| format!("t{}={}", failure.tid, failure.error))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{label} startup failed before synchronized start: {details}")
}

fn workload_shape(separate_tables: bool) -> &'static str {
    if separate_tables {
        "separate_tables"
    } else {
        "shared_table"
    }
}

fn configuration_receipt(
    writers: usize,
    rows_per_thread: usize,
    available_parallelism: Option<usize>,
    wal_autocheckpoint_pages: i64,
    wal_autocheckpoint_overridden: bool,
    retry_policy: RetryPolicyReceipt,
) -> ConfigurationReceipt {
    let max_supported_writers = fsqlite_mvcc::MAX_CONCURRENT_WRITERS;
    let offered_writes_per_sample = writers.checked_mul(rows_per_thread);
    let (status, comparison_eligible, measured, reason) = if offered_writes_per_sample.is_none() {
        (
            "unsupported".to_owned(),
            false,
            false,
            format!(
                "{writers} writers x {rows_per_thread} rows overflows offered-work accounting; \
                     arm skipped"
            ),
        )
    } else if writers == 0 {
        (
            "unsupported".to_owned(),
            false,
            false,
            "zero writers cannot produce a multi-writer measurement; arm skipped".to_owned(),
        )
    } else if writers > max_supported_writers {
        (
            "unsupported".to_owned(),
            false,
            false,
            format!(
                "requested {writers} writers exceeds FrankenSQLite's explicit \
                     MAX_CONCURRENT_WRITERS={max_supported_writers}; arm skipped"
            ),
        )
    } else if let Some(available_parallelism) = available_parallelism {
        if writers > available_parallelism {
            (
                "oversubscribed".to_owned(),
                false,
                true,
                format!(
                    "{writers} writers exceeds host \
                         available_parallelism={available_parallelism}; raw diagnostic samples \
                         are not eligible for a performance verdict"
                ),
            )
        } else {
            (
                "supported".to_owned(),
                true,
                true,
                format!(
                    "{writers} writers is within host \
                         available_parallelism={available_parallelism} and \
                         MAX_CONCURRENT_WRITERS={max_supported_writers}"
                ),
            )
        }
    } else {
        (
            "capacity_unknown".to_owned(),
            false,
            true,
            "host available_parallelism could not be determined; raw diagnostic \
                 samples are not eligible for a performance verdict"
                .to_owned(),
        )
    };
    let mut receipt = ConfigurationReceipt {
        writers,
        available_parallelism,
        max_supported_writers,
        wal_autocheckpoint_pages: Some(wal_autocheckpoint_pages),
        wal_autocheckpoint_overridden: Some(wal_autocheckpoint_overridden),
        offered_writes_per_sample,
        retry_policy: Some(retry_policy),
        status,
        comparison_eligible,
        measured,
        reason,
    };
    if wal_autocheckpoint_overridden {
        receipt.comparison_eligible = false;
        if receipt.status == "supported" {
            "diagnostic_override".clone_into(&mut receipt.status);
        }
        receipt.reason.push_str(&format!(
            "; explicit wal_autocheckpoint={wal_autocheckpoint_pages} override is diagnostic-only \
             and cannot update or compare against default-cadence history"
        ));
    }
    let retry_timeout_overridden = receipt
        .retry_policy
        .as_ref()
        .is_some_and(|policy| policy.fsqlite_timeout_overridden);
    let retry_timeout_is_shared = receipt
        .retry_policy
        .as_ref()
        .is_some_and(|policy| policy.shared_worker_retry_timeout_ms.is_some());
    if retry_timeout_overridden {
        receipt.comparison_eligible = false;
        if receipt.status == "supported" {
            "diagnostic_override".clone_into(&mut receipt.status);
        }
        if retry_timeout_is_shared {
            receipt.reason.push_str(
                "; explicit shared C SQLite/FrankenSQLite worker retry-timeout override is \
                 diagnostic-only and cannot update or compare against default-policy history",
            );
        } else {
            receipt.reason.push_str(
                "; explicit FrankenSQLite-only retry-timeout override is diagnostic-only and \
                 cannot update or compare against default-policy history",
            );
        }
    }
    receipt
}

fn validate_workload_bounds(rows_per_thread: usize, separate_tables: bool) -> Result<(), String> {
    i64::try_from(rows_per_thread)
        .map_err(|_| format!("rows_per_thread={rows_per_thread} exceeds the i64 row-id domain"))?;
    let shared_row_id_stride = usize::try_from(ROWID_BASE_STRIDE)
        .map_err(|_| "ROWID_BASE_STRIDE does not fit usize".to_owned())?;
    if !separate_tables && rows_per_thread > shared_row_id_stride {
        return Err(format!(
            "rows_per_thread={rows_per_thread} exceeds the shared-table disjoint row-id stride \
             {ROWID_BASE_STRIDE}; use --separate-tables or at most {ROWID_BASE_STRIDE} rows"
        ));
    }
    Ok(())
}

fn worker_table_count(threads: usize, separate_tables: bool) -> usize {
    if separate_tables { threads } else { 1 }
}

fn worker_table_name(tid: usize, separate_tables: bool) -> String {
    if separate_tables {
        format!("bench_{tid}")
    } else {
        "bench".to_owned()
    }
}

fn create_table_sql(table_name: &str) -> String {
    format!("CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, payload TEXT);")
}

fn create_tables_sql(threads: usize, separate_tables: bool) -> String {
    let table_count = worker_table_count(threads, separate_tables);
    let mut sql = String::new();
    for tid in 0..table_count {
        let table_name = worker_table_name(tid, separate_tables);
        sql.push_str(&create_table_sql(&table_name));
    }
    sql
}

fn worker_insert_sql(tid: usize, separate_tables: bool) -> String {
    // SQL identifiers cannot be bound as parameters. The only dynamic
    // identifier here is generated from the zero-based worker index, not from
    // user text, so it cannot escape the `bench_N` table-name shape.
    if separate_tables {
        format!("INSERT INTO bench_{tid} (id, payload) VALUES (?1, ?2)")
    } else {
        SHARED_INSERT_SQL.to_owned()
    }
}

fn normalize_fsqlite_value(value: &fsqlite::SqliteValue) -> String {
    match value {
        fsqlite::SqliteValue::Null => "null".to_owned(),
        fsqlite::SqliteValue::Integer(value) => value.to_string(),
        fsqlite::SqliteValue::Float(value) => value.to_string(),
        fsqlite::SqliteValue::Text(value) => value.as_ref().to_ascii_lowercase(),
        fsqlite::SqliteValue::Blob(value) => format!("blob:{}", value.len()),
    }
}

fn query_fsqlite_scalar(conn: &fsqlite::Connection, sql: &str) -> Result<String, String> {
    let rows = fsqlite_e2e::block_on(conn.query(sql))
        .map_err(|error| format!("FrankenSQLite `{sql}` failed: {error}"))?;
    if rows.len() != 1 {
        return Err(format!(
            "FrankenSQLite `{sql}` returned {} rows, expected exactly one",
            rows.len()
        ));
    }
    rows[0]
        .get(0)
        .map(normalize_fsqlite_value)
        .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no first column"))
}

fn query_rusqlite_scalar(conn: &rusqlite::Connection, sql: &str) -> Result<String, String> {
    conn.query_row(sql, [], |row| {
        let value = row.get_ref(0)?;
        Ok(match value {
            rusqlite::types::ValueRef::Null => "null".to_owned(),
            rusqlite::types::ValueRef::Integer(value) => value.to_string(),
            rusqlite::types::ValueRef::Real(value) => value.to_string(),
            rusqlite::types::ValueRef::Text(value) => {
                String::from_utf8_lossy(value).to_ascii_lowercase()
            }
            rusqlite::types::ValueRef::Blob(value) => format!("blob:{}", value.len()),
        })
    })
    .map_err(|error| format!("C SQLite `{sql}` failed: {error}"))
}

fn normalized_synchronous(value: &str) -> Result<String, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "off" => Ok("off".to_owned()),
        "1" | "normal" => Ok("normal".to_owned()),
        "2" | "full" => Ok("full".to_owned()),
        "3" | "extra" => Ok("extra".to_owned()),
        _ => Err(format!("unrecognized PRAGMA synchronous value `{value}`")),
    }
}

fn parse_effective_settings<F>(
    mut query: F,
    concurrent_mode: &str,
) -> Result<EffectiveSettings, String>
where
    F: FnMut(&str) -> Result<String, String>,
{
    let page_size = query("PRAGMA page_size;")?;
    let journal_mode = query("PRAGMA journal_mode;")?;
    let synchronous = query("PRAGMA synchronous;")?;
    let cache_size = query("PRAGMA cache_size;")?;
    let busy_timeout = query("PRAGMA busy_timeout;")?;
    let wal_autocheckpoint = query("PRAGMA wal_autocheckpoint;")?;
    Ok(EffectiveSettings {
        page_size_bytes: page_size
            .parse()
            .map_err(|error| format!("invalid PRAGMA page_size `{page_size}`: {error}"))?,
        journal_mode: journal_mode.to_ascii_lowercase(),
        synchronous: normalized_synchronous(&synchronous)?,
        cache_size: cache_size
            .parse()
            .map_err(|error| format!("invalid PRAGMA cache_size `{cache_size}`: {error}"))?,
        busy_timeout_ms: busy_timeout
            .parse()
            .map_err(|error| format!("invalid PRAGMA busy_timeout `{busy_timeout}`: {error}"))?,
        wal_autocheckpoint_pages: wal_autocheckpoint.parse().map_err(|error| {
            format!("invalid PRAGMA wal_autocheckpoint `{wal_autocheckpoint}`: {error}")
        })?,
        concurrent_mode: concurrent_mode.to_owned(),
    })
}

fn expected_effective_settings(
    concurrent_mode: &str,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> EffectiveSettings {
    EffectiveSettings {
        page_size_bytes: 4_096,
        journal_mode: "wal".to_owned(),
        synchronous: synchronous.receipt_value().to_owned(),
        cache_size: -64_000,
        busy_timeout_ms: 5_000,
        wal_autocheckpoint_pages,
        concurrent_mode: concurrent_mode.to_owned(),
    }
}

fn verify_effective_settings(
    engine: &str,
    observed: EffectiveSettings,
    concurrent_mode: &str,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<EffectiveSettings, String> {
    let expected =
        expected_effective_settings(concurrent_mode, wal_autocheckpoint_pages, synchronous);
    if observed != expected {
        return Err(format!(
            "{engine} effective settings mismatch: expected {expected:?}, observed {observed:?}"
        ));
    }
    Ok(observed)
}

/// Resolve the checkpoint-cadence setting once and pass the exact value to
/// every connection in both arms. Invalid or non-Unicode overrides fail closed.
fn bench_wal_autocheckpoint_pages() -> Result<(i64, bool), String> {
    match std::env::var("FSQLITE_BENCH_WAL_AUTOCHECKPOINT") {
        Ok(raw) => {
            let pages = raw.parse::<i64>().map_err(|error| {
                format!(
                    "invalid FSQLITE_BENCH_WAL_AUTOCHECKPOINT={raw:?}; expected a non-negative integer: {error}"
                )
            })?;
            if pages < 0 {
                return Err(format!(
                    "invalid FSQLITE_BENCH_WAL_AUTOCHECKPOINT={raw:?}; expected a non-negative integer"
                ));
            }
            Ok((pages, true))
        }
        Err(std::env::VarError::NotPresent) => Ok((DEFAULT_WAL_AUTOCHECKPOINT_PAGES, false)),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err("FSQLITE_BENCH_WAL_AUTOCHECKPOINT is not valid Unicode".to_owned())
        }
    }
}

fn configure_fsqlite_connection(
    conn: &fsqlite::Connection,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<EffectiveSettings, String> {
    if !conn.is_concurrent_mode_default() {
        return Err(
            "FrankenSQLite concurrent-writer mode was not default-on before benchmark setup"
                .to_owned(),
        );
    }
    for pragma in [
        "PRAGMA page_size=4096;".to_owned(),
        "PRAGMA journal_mode=WAL;".to_owned(),
        format!("PRAGMA synchronous={};", synchronous.pragma_value()),
        "PRAGMA cache_size=-64000;".to_owned(),
        "PRAGMA busy_timeout=5000;".to_owned(),
        format!("PRAGMA wal_autocheckpoint={wal_autocheckpoint_pages};"),
        "PRAGMA fsqlite.concurrent_mode=ON;".to_owned(),
    ] {
        fsqlite_e2e::block_on(conn.execute(&pragma))
            .map_err(|error| format!("FrankenSQLite `{pragma}` failed: {error}"))?;
    }
    let concurrent_mode = query_fsqlite_scalar(conn, "PRAGMA fsqlite.concurrent_mode;")?;
    if !matches!(concurrent_mode.as_str(), "1" | "true" | "on") {
        return Err(format!(
            "FrankenSQLite concurrent-mode readback was `{concurrent_mode}`, expected enabled"
        ));
    }
    if !conn.is_concurrent_mode_default() {
        return Err(
            "FrankenSQLite concurrent-writer mode became disabled during benchmark setup"
                .to_owned(),
        );
    }
    let observed =
        parse_effective_settings(|sql| query_fsqlite_scalar(conn, sql), "fsqlite_mvcc_on")?;
    verify_effective_settings(
        "FrankenSQLite",
        observed,
        "fsqlite_mvcc_on",
        wal_autocheckpoint_pages,
        synchronous,
    )
}

fn configure_rusqlite_connection(
    conn: &rusqlite::Connection,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<EffectiveSettings, String> {
    conn.execute_batch(&format!(
        "PRAGMA page_size=4096;\
         PRAGMA journal_mode=WAL;\
         PRAGMA synchronous={};\
         PRAGMA cache_size=-64000;\
         PRAGMA busy_timeout=5000;\
         PRAGMA wal_autocheckpoint={wal_autocheckpoint_pages};",
        synchronous.pragma_value()
    ))
    .map_err(|error| format!("C SQLite performance PRAGMAs failed: {error}"))?;
    let observed = parse_effective_settings(
        |sql| query_rusqlite_scalar(conn, sql),
        "sqlite_wal_single_writer",
    )?;
    verify_effective_settings(
        "C SQLite",
        observed,
        "sqlite_wal_single_writer",
        wal_autocheckpoint_pages,
        synchronous,
    )
}

fn prepare_fsqlite_schema(
    path: &str,
    threads: usize,
    separate_tables: bool,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<EffectiveSettings, String> {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.to_owned()))
        .map_err(|error| format!("fsqlite open (init): {error}"))?;
    let settings = configure_fsqlite_connection(&conn, wal_autocheckpoint_pages, synchronous)?;
    for tid in 0..worker_table_count(threads, separate_tables) {
        let table_name = worker_table_name(tid, separate_tables);
        let create_sql = create_table_sql(&table_name);
        fsqlite_e2e::block_on(conn.execute(&create_sql))
            .map_err(|error| format!("create table {table_name}: {error}"))?;
    }
    Ok(settings)
}

#[allow(clippy::cast_precision_loss)]
fn percentile_value(mut values: Vec<f64>, percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    if values.len() == 1 {
        return values[0];
    }
    let rank = percentile.clamp(0.0, 1.0) * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return values[lower];
    }
    let fraction = rank - lower as f64;
    (values[upper] - values[lower]).mul_add(fraction, values[lower])
}

#[derive(Debug)]
struct DatabaseState {
    rows: usize,
    id_sum: i64,
    payload_sha256: String,
}

fn hash_committed_row(
    hasher: &mut Sha256,
    table_index: usize,
    id: i64,
    payload: &str,
) -> Result<(), String> {
    let table_index =
        u64::try_from(table_index).map_err(|_| "table index exceeds u64".to_owned())?;
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| "payload length exceeds u64".to_owned())?;
    hasher.update(table_index.to_le_bytes());
    hasher.update(id.to_le_bytes());
    hasher.update(payload_len.to_le_bytes());
    hasher.update(payload.as_bytes());
    Ok(())
}

fn expected_database_state(
    threads: usize,
    rows_per_thread: usize,
    separate_tables: bool,
) -> Result<DatabaseState, String> {
    let expected_rows = threads
        .checked_mul(rows_per_thread)
        .ok_or_else(|| "expected row count overflow".to_owned())?;
    let mut observed_rows = 0usize;
    let mut id_sum = 0i64;
    let mut hasher = Sha256::new();
    for tid in 0..threads {
        let tid_i64 = i64::try_from(tid).map_err(|_| format!("writer index {tid} exceeds i64"))?;
        let base = if separate_tables {
            0
        } else {
            tid_i64
                .checked_mul(ROWID_BASE_STRIDE)
                .ok_or_else(|| format!("writer {tid} row-id base overflow"))?
        };
        let table_index = if separate_tables { tid } else { 0 };
        for row_index in 0..rows_per_thread {
            let row_index_i64 = i64::try_from(row_index)
                .map_err(|_| format!("row index {row_index} exceeds i64"))?;
            let id = base
                .checked_add(row_index_i64)
                .ok_or_else(|| format!("writer {tid} row id overflow"))?;
            let payload = format!("tid{tid}_i{row_index_i64}");
            observed_rows = observed_rows
                .checked_add(1)
                .ok_or_else(|| "expected row count overflow".to_owned())?;
            id_sum = id_sum
                .checked_add(id)
                .ok_or_else(|| "expected id sum overflow".to_owned())?;
            hash_committed_row(&mut hasher, table_index, id, &payload)?;
        }
    }
    if observed_rows != expected_rows {
        return Err(format!(
            "expected-state row enumeration mismatch: product={expected_rows}, enumerated={observed_rows}"
        ));
    }
    Ok(DatabaseState {
        rows: observed_rows,
        id_sum,
        payload_sha256: bytes_to_lower_hex(&hasher.finalize()),
    })
}

fn build_committed_state_oracle(
    expected: DatabaseState,
    observed: DatabaseState,
    integrity_check: Vec<String>,
) -> CommittedStateOracle {
    let mut diagnostics = Vec::new();
    if observed.rows != expected.rows {
        diagnostics.push(format!(
            "committed row count mismatch: expected {}, observed {}",
            expected.rows, observed.rows
        ));
    }
    if observed.id_sum != expected.id_sum {
        diagnostics.push(format!(
            "committed id sum mismatch: expected {}, observed {}",
            expected.id_sum, observed.id_sum
        ));
    }
    if observed.payload_sha256 != expected.payload_sha256 {
        diagnostics.push(format!(
            "committed payload hash mismatch: expected {}, observed {}",
            expected.payload_sha256, observed.payload_sha256
        ));
    }
    if integrity_check != ["ok"] {
        diagnostics.push(format!(
            "PRAGMA integrity_check returned diagnostics: {integrity_check:?}"
        ));
    }
    CommittedStateOracle {
        expected_rows: expected.rows,
        observed_rows: observed.rows,
        expected_id_sum: expected.id_sum,
        observed_id_sum: observed.id_sum,
        expected_payload_sha256: expected.payload_sha256,
        observed_payload_sha256: observed.payload_sha256,
        integrity_check,
        valid: diagnostics.is_empty(),
        diagnostics,
    }
}

fn query_fsqlite_committed_state(
    path: &str,
    threads: usize,
    separate_tables: bool,
) -> Result<(DatabaseState, Vec<String>), String> {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.to_owned()))
        .map_err(|error| format!("FrankenSQLite post-run verifier open failed: {error}"))?;
    let mut rows_total = 0usize;
    let mut id_sum = 0i64;
    let mut hasher = Sha256::new();
    for table_index in 0..worker_table_count(threads, separate_tables) {
        let table_name = worker_table_name(table_index, separate_tables);
        let sql = format!("SELECT id, payload FROM {table_name} ORDER BY id");
        let rows = fsqlite_e2e::block_on(conn.query(&sql))
            .map_err(|error| format!("FrankenSQLite post-run `{sql}` failed: {error}"))?;
        for row in rows {
            let id = row
                .get(0)
                .and_then(fsqlite::SqliteValue::as_integer)
                .ok_or_else(|| {
                    format!("FrankenSQLite post-run `{sql}` returned a non-integer id")
                })?;
            let payload = match row.get(1) {
                Some(fsqlite::SqliteValue::Text(payload)) => payload.as_ref(),
                _ => {
                    return Err(format!(
                        "FrankenSQLite post-run `{sql}` returned a non-text payload"
                    ));
                }
            };
            rows_total = rows_total
                .checked_add(1)
                .ok_or_else(|| "FrankenSQLite post-run row count overflow".to_owned())?;
            id_sum = id_sum
                .checked_add(id)
                .ok_or_else(|| "FrankenSQLite post-run id sum overflow".to_owned())?;
            hash_committed_row(&mut hasher, table_index, id, payload)?;
        }
    }
    let integrity_rows = fsqlite_e2e::block_on(conn.query("PRAGMA integrity_check;"))
        .map_err(|error| format!("FrankenSQLite `PRAGMA integrity_check` failed: {error}"))?;
    let integrity_check = integrity_rows
        .iter()
        .map(|row| {
            row.get(0).map(normalize_fsqlite_value).ok_or_else(|| {
                "FrankenSQLite `PRAGMA integrity_check` row omitted column 0".to_owned()
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        DatabaseState {
            rows: rows_total,
            id_sum,
            payload_sha256: bytes_to_lower_hex(&hasher.finalize()),
        },
        integrity_check,
    ))
}

fn query_rusqlite_committed_state(
    path: &str,
    threads: usize,
    separate_tables: bool,
) -> Result<(DatabaseState, Vec<String>), String> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|error| format!("C SQLite post-run verifier open failed: {error}"))?;
    let mut rows_total = 0usize;
    let mut id_sum = 0i64;
    let mut hasher = Sha256::new();
    for table_index in 0..worker_table_count(threads, separate_tables) {
        let table_name = worker_table_name(table_index, separate_tables);
        let sql = format!("SELECT id, payload FROM {table_name} ORDER BY id");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|error| format!("C SQLite post-run prepare `{sql}` failed: {error}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|error| format!("C SQLite post-run query `{sql}` failed: {error}"))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| format!("C SQLite post-run row `{sql}` failed: {error}"))?
        {
            let id = row
                .get::<_, i64>(0)
                .map_err(|error| format!("C SQLite post-run id decode failed: {error}"))?;
            let payload = row
                .get::<_, String>(1)
                .map_err(|error| format!("C SQLite post-run payload decode failed: {error}"))?;
            rows_total = rows_total
                .checked_add(1)
                .ok_or_else(|| "C SQLite post-run row count overflow".to_owned())?;
            id_sum = id_sum
                .checked_add(id)
                .ok_or_else(|| "C SQLite post-run id sum overflow".to_owned())?;
            hash_committed_row(&mut hasher, table_index, id, &payload)?;
        }
    }
    let mut stmt = conn
        .prepare("PRAGMA integrity_check;")
        .map_err(|error| format!("C SQLite integrity_check prepare failed: {error}"))?;
    let integrity_check: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| format!("C SQLite integrity_check query failed: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("C SQLite integrity_check row failed: {error}"))?
        .into_iter()
        .map(|message| message.to_ascii_lowercase())
        .collect();
    Ok((
        DatabaseState {
            rows: rows_total,
            id_sum,
            payload_sha256: bytes_to_lower_hex(&hasher.finalize()),
        },
        integrity_check,
    ))
}

fn build_work_accounting(
    offered_writes: usize,
    attempted_writes: usize,
    retried_operations: usize,
    worker_reported_failed_writes: usize,
    committed_writes: usize,
) -> WorkAccounting {
    let mut diagnostics = Vec::new();
    let failed_writes = if committed_writes <= offered_writes {
        offered_writes - committed_writes
    } else {
        diagnostics.push(format!(
            "committed rows {committed_writes} exceed offered writes {offered_writes}"
        ));
        0
    };
    if attempted_writes < committed_writes {
        diagnostics.push(format!(
            "physical write attempts {attempted_writes} are fewer than committed writes {committed_writes}"
        ));
    }
    if worker_reported_failed_writes != failed_writes {
        diagnostics.push(format!(
            "worker failure accounting mismatch: workers reported {worker_reported_failed_writes}, committed-state delta proves {failed_writes}"
        ));
    }
    if committed_writes
        .checked_add(failed_writes)
        .is_none_or(|accounted| accounted != offered_writes)
    {
        diagnostics.push(format!(
            "offered-work accounting mismatch: offered {offered_writes}, committed {committed_writes}, failed {failed_writes}"
        ));
    }
    WorkAccounting {
        offered_writes,
        attempted_writes,
        succeeded_writes: committed_writes,
        retried_operations,
        failed_writes,
        worker_reported_failed_writes,
        exact: diagnostics.is_empty(),
        diagnostics,
    }
}

// ─── FrankenSQLite workload ──────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OneRowRetryStage {
    Prepare,
    Begin,
    Insert,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OneRowFailureDisposition {
    Retry,
    FailClosed,
}

const fn one_row_failure_disposition(
    stage: OneRowRetryStage,
    retryable: bool,
    rollback_succeeded: Option<bool>,
) -> OneRowFailureDisposition {
    let transaction_state_is_known = match stage {
        OneRowRetryStage::Prepare | OneRowRetryStage::Begin => rollback_succeeded.is_none(),
        OneRowRetryStage::Insert | OneRowRetryStage::Commit => {
            matches!(rollback_succeeded, Some(true))
        }
    };
    if retryable && transaction_state_is_known {
        OneRowFailureDisposition::Retry
    } else {
        OneRowFailureDisposition::FailClosed
    }
}

impl Display for OneRowRetryStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Prepare => "PREPARE",
            Self::Begin => "BEGIN",
            Self::Insert => "INSERT",
            Self::Commit => "COMMIT",
        })
    }
}

enum OneRowAttempt {
    Committed,
    Retry {
        stage: OneRowRetryStage,
        error: fsqlite::FrankenError,
    },
    RollbackRequired {
        stage: OneRowRetryStage,
        error: fsqlite::FrankenError,
    },
}

fn prepare_fsqlite_one_row_with_retry<'context, Context, Prepared, Prepare>(
    context: &'context Context,
    tid: usize,
    worker_retry_deadline: OneRowWorkerRetryDeadline,
    mut prepare: Prepare,
) -> Result<(Prepared, usize), String>
where
    Context: ?Sized,
    Prepare: FnMut(&'context Context) -> Result<Prepared, fsqlite::FrankenError>,
{
    let mut retry_budget = worker_retry_deadline.fsqlite_budget();
    loop {
        match prepare(context) {
            Ok(prepared) => return Ok((prepared, retry_budget.attempts())),
            Err(error)
                if one_row_failure_disposition(
                    OneRowRetryStage::Prepare,
                    fsqlite_error_is_retryable(&error),
                    None,
                ) == OneRowFailureDisposition::Retry =>
            {
                let Some(wait) = retry_budget.next_wait(tid) else {
                    return Err(format!(
                        "[fsqlite t{tid}] one-row PREPARE exhausted shared worker retry budget \
                         after {} retries: {error}",
                        retry_budget.attempts()
                    ));
                };
                thread::sleep(wait);
            }
            Err(error) => {
                return Err(format!(
                    "[fsqlite t{tid}] one-row PREPARE failed after {} retries: {error}",
                    retry_budget.attempts()
                ));
            }
        }
    }
}

const fn fsqlite_rollback_error_is_retryable(error: &fsqlite::FrankenError) -> bool {
    matches!(
        error,
        fsqlite::FrankenError::Busy | fsqlite::FrankenError::BusyRecovery
    )
}

fn rollback_fsqlite_one_row_with_retry<Rollback>(
    tid: usize,
    retry_budget: &mut FsqliteRetryBudget,
    mut rollback: Rollback,
) -> Result<usize, String>
where
    Rollback: FnMut() -> Result<(), fsqlite::FrankenError>,
{
    let mut retries = 0usize;
    loop {
        match rollback() {
            Ok(()) => return Ok(retries),
            Err(error) if fsqlite_rollback_error_is_retryable(&error) => {
                let Some(wait) = retry_budget.next_wait(tid) else {
                    return Err(format!(
                        "one-row ROLLBACK exhausted shared worker retry budget after {} retries: {error}",
                        retry_budget.attempts()
                    ));
                };
                retries = retries
                    .checked_add(1)
                    .ok_or_else(|| "one-row ROLLBACK retry counter overflow".to_owned())?;
                thread::sleep(wait);
            }
            Err(error) => {
                return Err(format!(
                    "one-row ROLLBACK failed after {} retries: {error}",
                    retry_budget.attempts()
                ));
            }
        }
    }
}

fn run_fsqlite_one_row_transactions(
    conn: &fsqlite::Connection,
    stmt: &fsqlite::PreparedStatement<'_>,
    tid: usize,
    base: i64,
    rows_per_thread: usize,
    worker_retry_deadline: OneRowWorkerRetryDeadline,
) -> Result<(usize, usize), String> {
    let mut attempted_writes = 0usize;
    let mut retried_transactions = 0usize;
    for row_index in 0..rows_per_thread {
        let row_index = i64::try_from(row_index)
            .map_err(|_| format!("[fsqlite t{tid}] row index exceeds i64"))?;
        let id = base
            .checked_add(row_index)
            .ok_or_else(|| format!("[fsqlite t{tid}] row id overflow"))?;
        let params = [
            fsqlite::SqliteValue::Integer(id),
            fsqlite::SqliteValue::Text(format!("tid{tid}_i{row_index}").into()),
        ];
        let mut retry_budget = worker_retry_deadline.fsqlite_budget();

        loop {
            let outcome = fsqlite_e2e::block_on(async {
                if let Err(error) = conn.execute("BEGIN CONCURRENT").await {
                    if one_row_failure_disposition(
                        OneRowRetryStage::Begin,
                        fsqlite_error_is_retryable(&error),
                        None,
                    ) == OneRowFailureDisposition::Retry
                    {
                        return Ok(OneRowAttempt::Retry {
                            stage: OneRowRetryStage::Begin,
                            error,
                        });
                    }
                    return Err(format!(
                        "[fsqlite t{tid}] one-row BEGIN failed for id {id}: {error}"
                    ));
                }

                attempted_writes = attempted_writes
                    .checked_add(1)
                    .ok_or_else(|| format!("[fsqlite t{tid}] write-attempt counter overflow"))?;
                if let Err(error) = stmt.execute_with_params(&params).await {
                    return Ok(OneRowAttempt::RollbackRequired {
                        stage: OneRowRetryStage::Insert,
                        error,
                    });
                }

                match conn.execute("COMMIT").await {
                    Ok(_) => Ok(OneRowAttempt::Committed),
                    Err(error) => Ok(OneRowAttempt::RollbackRequired {
                        stage: OneRowRetryStage::Commit,
                        error,
                    }),
                }
            })?;

            let (stage, error, rollback_retries) = match outcome {
                OneRowAttempt::Committed => break,
                OneRowAttempt::Retry { stage, error } => (stage, error, 0),
                OneRowAttempt::RollbackRequired { stage, error } => {
                    let rollback_retries = rollback_fsqlite_one_row_with_retry(
                        tid,
                        &mut retry_budget,
                        || fsqlite_e2e::block_on(conn.execute("ROLLBACK")).map(|_| ()),
                    )
                    .map_err(|rollback_error| match stage {
                        OneRowRetryStage::Insert => format!(
                            "[fsqlite t{tid}] one-row INSERT failed for id {id}: {error}; \
                             mandatory rollback also failed: {rollback_error}"
                        ),
                        OneRowRetryStage::Commit => format!(
                            "[fsqlite t{tid}] one-row COMMIT failed for id {id}: {error}; \
                             mandatory rollback also failed: {rollback_error}; ambiguous \
                             commit state is fail-closed"
                        ),
                        OneRowRetryStage::Prepare | OneRowRetryStage::Begin => format!(
                            "[fsqlite t{tid}] one-row {stage} unexpectedly required rollback for \
                             id {id}: {error}"
                        ),
                    })?;
                    if one_row_failure_disposition(
                        stage,
                        fsqlite_error_is_retryable(&error),
                        Some(true),
                    ) != OneRowFailureDisposition::Retry
                    {
                        return Err(match stage {
                            OneRowRetryStage::Insert => format!(
                                "[fsqlite t{tid}] one-row INSERT failed for id {id}: {error}; \
                                 duplicate and constraint errors are fail-closed and are never \
                                 accepted as proof of the intended id+payload"
                            ),
                            OneRowRetryStage::Commit => format!(
                                "[fsqlite t{tid}] one-row COMMIT failed for id {id}: {error}"
                            ),
                            OneRowRetryStage::Prepare | OneRowRetryStage::Begin => format!(
                                "[fsqlite t{tid}] one-row {stage} unexpectedly required rollback \
                                 for id {id}: {error}"
                            ),
                        });
                    }
                    (stage, error, rollback_retries)
                }
            };
            retried_transactions = retried_transactions
                .checked_add(rollback_retries)
                .ok_or_else(|| format!("[fsqlite t{tid}] retry counter overflow"))?;
            let Some(wait) = retry_budget.next_wait(tid) else {
                return Err(format!(
                    "[fsqlite t{tid}] one-row transaction for id {id} exhausted retry \
                     budget at {stage} after {} retries: {error}",
                    retry_budget.attempts()
                ));
            };
            retried_transactions = retried_transactions
                .checked_add(1)
                .ok_or_else(|| format!("[fsqlite t{tid}] retry counter overflow"))?;
            thread::sleep(wait);
        }
    }
    Ok((attempted_writes, retried_transactions))
}

fn run_rusqlite_one_row_transactions(
    conn: &rusqlite::Connection,
    stmt: &mut rusqlite::Statement<'_>,
    tid: usize,
    base: i64,
    rows_per_thread: usize,
    retry_timeout: Duration,
) -> Result<(usize, usize), String> {
    let mut attempted_writes = 0usize;
    let mut retried_transactions = 0usize;
    let worker_retry_deadline = OneRowWorkerRetryDeadline::new(retry_timeout);
    for row_index in 0..rows_per_thread {
        let row_index = i64::try_from(row_index)
            .map_err(|_| format!("[sqlite t{tid}] row index exceeds i64"))?;
        let id = base
            .checked_add(row_index)
            .ok_or_else(|| format!("[sqlite t{tid}] row id overflow"))?;
        let payload = format!("tid{tid}_i{row_index}");
        let mut retries = 0usize;

        loop {
            if let Err(error) = conn.execute_batch("BEGIN") {
                if one_row_failure_disposition(
                    OneRowRetryStage::Begin,
                    csqlite_error_is_retryable(&error),
                    None,
                ) == OneRowFailureDisposition::Retry
                    && worker_retry_deadline.allows_retry(retries)
                {
                    retries += 1;
                    retried_transactions = retried_transactions
                        .checked_add(1)
                        .ok_or_else(|| format!("[sqlite t{tid}] retry counter overflow"))?;
                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                    continue;
                }
                return Err(format!(
                    "[sqlite t{tid}] one-row BEGIN failed for id {id} after {retries} retries: \
                     {error}"
                ));
            }

            attempted_writes = attempted_writes
                .checked_add(1)
                .ok_or_else(|| format!("[sqlite t{tid}] write-attempt counter overflow"))?;
            if let Err(error) = stmt.execute(rusqlite::params![id, &payload]) {
                conn.execute_batch("ROLLBACK").map_err(|rollback_error| {
                    format!(
                        "[sqlite t{tid}] one-row INSERT failed for id {id}: {error}; mandatory \
                         rollback also failed: {rollback_error}"
                    )
                })?;
                if one_row_failure_disposition(
                    OneRowRetryStage::Insert,
                    csqlite_error_is_retryable(&error),
                    Some(true),
                ) == OneRowFailureDisposition::Retry
                    && worker_retry_deadline.allows_retry(retries)
                {
                    retries += 1;
                    retried_transactions = retried_transactions
                        .checked_add(1)
                        .ok_or_else(|| format!("[sqlite t{tid}] retry counter overflow"))?;
                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                    continue;
                }
                return Err(format!(
                    "[sqlite t{tid}] one-row INSERT failed for id {id} after {retries} retries: \
                     {error}; duplicate and constraint errors are fail-closed and are never \
                     accepted as proof of the intended id+payload"
                ));
            }

            match conn.execute_batch("COMMIT") {
                Ok(()) => break,
                Err(error) => {
                    conn.execute_batch("ROLLBACK").map_err(|rollback_error| {
                        format!(
                            "[sqlite t{tid}] one-row COMMIT failed for id {id}: {error}; \
                             mandatory rollback also failed: {rollback_error}; ambiguous commit \
                             state is fail-closed"
                        )
                    })?;
                    if one_row_failure_disposition(
                        OneRowRetryStage::Commit,
                        csqlite_error_is_retryable(&error),
                        Some(true),
                    ) == OneRowFailureDisposition::Retry
                        && worker_retry_deadline.allows_retry(retries)
                    {
                        retries += 1;
                        retried_transactions = retried_transactions
                            .checked_add(1)
                            .ok_or_else(|| format!("[sqlite t{tid}] retry counter overflow"))?;
                        thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                        continue;
                    }
                    return Err(format!(
                        "[sqlite t{tid}] one-row COMMIT failed for id {id} after {retries} \
                         retries: {error}"
                    ));
                }
            }
        }
    }
    Ok((attempted_writes, retried_transactions))
}

fn open_fsqlite_worker(
    path: &str,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<(fsqlite::Connection, EffectiveSettings), String> {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.to_owned()))
        .map_err(|error| format!("fsqlite open (worker): {error}"))?;
    let settings = configure_fsqlite_connection(&conn, wal_autocheckpoint_pages, synchronous)?;
    Ok((conn, settings))
}

fn run_fsqlite(
    threads: usize,
    rows_per_thread: usize,
    separate_tables: bool,
    transaction_granularity: TransactionGranularity,
    retry_timeout: Duration,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<RunResult, String> {
    let tmp = tempfile::NamedTempFile::new()
        .map_err(|error| format!("FrankenSQLite tempfile creation failed: {error}"))?;
    let path = tmp
        .path()
        .to_str()
        .ok_or_else(|| "FrankenSQLite tempfile path is not UTF-8".to_owned())?
        .to_owned();

    let init_settings = prepare_fsqlite_schema(
        &path,
        threads,
        separate_tables,
        wal_autocheckpoint_pages,
        synchronous,
    )?;

    let path = Arc::new(path);
    let startup_gate = Arc::new((Mutex::new(StartupGateState::default()), Condvar::new()));
    let (startup_tx, startup_rx) = mpsc::channel::<StartupOutcome>();
    let mut handles = Vec::with_capacity(threads);

    let worker_startup_started = Instant::now();
    for tid in 0..threads {
        let path = Arc::clone(&path);
        let startup_gate = Arc::clone(&startup_gate);
        let startup_tx = startup_tx.clone();
        let handle = thread::spawn(move || -> Result<WorkerWork, String> {
            // Each thread owns its own Connection (Connection: !Send + !Sync).
            let (conn, settings) =
                match open_fsqlite_worker(path.as_str(), wal_autocheckpoint_pages, synchronous) {
                    Ok(worker) => {
                        let _ = startup_tx.send(StartupOutcome {
                            tid,
                            kind: StartupResultKind::Ready,
                            error: None,
                        });
                        worker
                    }
                    Err(error) => {
                        let _ = startup_tx.send(StartupOutcome {
                            tid,
                            kind: StartupResultKind::Failed,
                            error: Some(error.clone()),
                        });
                        return Err(error);
                    }
                };

            let (gate_lock, gate_cv) = &*startup_gate;
            let mut gate_state = gate_lock
                .lock()
                .map_err(|_| "fsqlite startup gate poisoned".to_owned())?;
            while !gate_state.release && !gate_state.abort {
                gate_state = gate_cv
                    .wait(gate_state)
                    .map_err(|_| "fsqlite startup gate poisoned while waiting".to_owned())?;
            }
            if gate_state.abort {
                return Err(format!(
                    "fsqlite t{tid} startup aborted after peer open failure"
                ));
            }
            drop(gate_state);

            #[allow(clippy::cast_possible_wrap)]
            let base = if separate_tables {
                0
            } else {
                tid as i64 * ROWID_BASE_STRIDE
            };
            // The v7 bulk path below prepares once per transaction attempt.
            // The v10 one-row path retains one successfully prepared statement
            // per worker across all row transactions, matching its rusqlite
            // reference arm. Transient preparation failures may retry under
            // the shared worker deadline. In both modes, bind+execute rather
            // than SQL formatting is the per-row operation.
            //
            // Using `format!` per-iter on the fsqlite side was an
            // apples-to-oranges artifact that pinned `Lexer::tokenize_into`
            // at 2.53% self-time and drove 12%+ allocator churn on MT 8t
            // (2026-04-23 capture `fsqlite-t3b-validation-185110`).
            let insert_sql = worker_insert_sql(tid, separate_tables);

            if transaction_granularity == TransactionGranularity::OneRow {
                let worker_retry_deadline = OneRowWorkerRetryDeadline::new(retry_timeout);
                let (stmt, prepare_retries) = prepare_fsqlite_one_row_with_retry(
                    &conn,
                    tid,
                    worker_retry_deadline,
                    |worker_conn| fsqlite_e2e::block_on(worker_conn.prepare(&insert_sql)),
                )?;
                let (attempted_writes, transaction_retries) = run_fsqlite_one_row_transactions(
                    &conn,
                    &stmt,
                    tid,
                    base,
                    rows_per_thread,
                    worker_retry_deadline,
                )?;
                let retried_operations = prepare_retries
                    .checked_add(transaction_retries)
                    .ok_or_else(|| format!("[fsqlite t{tid}] retry counter overflow"))?;
                return Ok(WorkerWork {
                    settings,
                    attempted_writes,
                    retried_operations,
                    reported_failed_writes: 0,
                    workload_finished: Instant::now(),
                });
            }

            // Single transaction spanning all rows; retry on transient
            // conflicts by rolling back and reopening the transaction.
            //
            // bd-mnlk2 / bd-zavyn: each transaction attempt runs inside ONE
            // runtime entry. The previous shape entered the harness runtime
            // once per BEGIN/prepare/row/COMMIT, putting a ~333 ns bridge
            // tax on every FrankenSQLite row while the rusqlite arm paid
            // nothing, so published F/C ratios from this binary
            // under-reported FrankenSQLite. The transient-retry backoff
            // sleeps *outside* the entered runtime (Gate 0 requirement:
            // never hold a sync sleep inside a current-thread runtime that
            // owns engine progress).
            enum TxnRetry {
                Begin,
                Insert(i64),
                Commit,
            }
            let mut retry_budget = FsqliteRetryBudget::new(retry_timeout);
            let mut attempted_writes = 0usize;
            let mut retried_operations = 0usize;
            let final_failed = loop {
                let mut attempt_failed = 0usize;
                let outcome = fsqlite_e2e::block_on(async {
                    if let Err(e) = conn.execute("BEGIN CONCURRENT").await {
                        if fsqlite_error_is_retryable(&e) {
                            return Ok(Some((TxnRetry::Begin, e.to_string())));
                        }
                        return Err(format!(
                            "[fsqlite t{tid}] BEGIN failed after {} retries: {e}",
                            retry_budget.attempts()
                        ));
                    }

                    let stmt = match conn.prepare(&insert_sql).await {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = conn.execute("ROLLBACK").await;
                            return Err(format!("[fsqlite t{tid}] prepare failed: {e}"));
                        }
                    };

                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..rows_per_thread as i64 {
                        let id = base + i;
                        let payload = format!("tid{tid}_i{i}");
                        let params = [
                            fsqlite::SqliteValue::Integer(id),
                            fsqlite::SqliteValue::Text(payload.into()),
                        ];
                        attempted_writes = attempted_writes.checked_add(1).ok_or_else(|| {
                            format!("[fsqlite t{tid}] write-attempt counter overflow")
                        })?;
                        match stmt.execute_with_params(&params).await {
                            Ok(_) => {}
                            Err(e) if fsqlite_error_is_retryable(&e) => {
                                let _ = conn.execute("ROLLBACK").await;
                                return Ok(Some((TxnRetry::Insert(id), e.to_string())));
                            }
                            Err(e) => {
                                eprintln!("[fsqlite t{tid}] INSERT {id} failed: {e}");
                                attempt_failed =
                                    attempt_failed.checked_add(1).ok_or_else(|| {
                                        format!("[fsqlite t{tid}] failed-write counter overflow")
                                    })?;
                            }
                        }
                    }

                    match conn.execute("COMMIT").await {
                        Ok(_) => Ok(None),
                        Err(e) if fsqlite_error_is_retryable(&e) => {
                            let _ = conn.execute("ROLLBACK").await;
                            Ok(Some((TxnRetry::Commit, e.to_string())))
                        }
                        Err(e) => {
                            let _ = conn.execute("ROLLBACK").await;
                            Err(format!("[fsqlite t{tid}] COMMIT failed: {e}"))
                        }
                    }
                })?;

                match outcome {
                    None => {
                        break attempt_failed;
                    }
                    Some((what, error)) => {
                        if let Some(wait) = retry_budget.next_wait(tid) {
                            retried_operations =
                                retried_operations.checked_add(1).ok_or_else(|| {
                                    format!("[fsqlite t{tid}] retry counter overflow")
                                })?;
                            thread::sleep(wait);
                        } else {
                            // Budget exhaustion is a MEASUREMENT-ENVELOPE
                            // event, not an engine correctness failure
                            // (bd-caa6u): mirror the C arm — count the whole
                            // transaction's rows as failed (which flags the
                            // result row via fsqlite_failed) and keep the
                            // run alive instead of killing every thread's
                            // data. The stderr line keeps the distinction
                            // auditable.
                            let stage = match what {
                                TxnRetry::Begin => "BEGIN".to_owned(),
                                TxnRetry::Insert(id) => format!("INSERT {id}"),
                                TxnRetry::Commit => "COMMIT".to_owned(),
                            };
                            eprintln!(
                                "[fsqlite t{tid}] {stage} exhausted retry budget \
                                 ({:?} wall clock, {} attempts): {error} — counting \
                                 {rows_per_thread} rows failed and continuing",
                                retry_budget.timeout,
                                retry_budget.attempts()
                            );
                            let _ = fsqlite_e2e::block_on(conn.execute("ROLLBACK"));
                            break rows_per_thread;
                        }
                    }
                }
            };

            Ok(WorkerWork {
                settings,
                attempted_writes,
                retried_operations,
                reported_failed_writes: final_failed,
                workload_finished: Instant::now(),
            })
        });
        handles.push(handle);
    }
    drop(startup_tx);

    let startup_failures = match collect_startup_outcomes("FrankenSQLite", threads, &startup_rx) {
        Ok(failures) => failures,
        Err(error) => {
            return Err(cleanup_workers_after_startup_failure(
                "FrankenSQLite",
                &startup_gate,
                handles,
                &init_settings,
                error,
            ));
        }
    };
    if !startup_failures.is_empty() {
        let error = format_startup_failures("FrankenSQLite", &startup_failures);
        return Err(cleanup_workers_after_startup_failure(
            "FrankenSQLite",
            &startup_gate,
            handles,
            &init_settings,
            error,
        ));
    }

    let worker_startup_elapsed = worker_startup_started.elapsed();
    let workload_started = publish_startup_decision(&startup_gate, true);
    let work = join_worker_handles("FrankenSQLite", handles, &init_settings)?;
    let workload_elapsed = work
        .workload_finished
        .checked_duration_since(workload_started)
        .ok_or_else(|| "FrankenSQLite worker completion preceded workload start".to_owned())?;
    let expected = expected_database_state(threads, rows_per_thread, separate_tables)?;
    let (observed, integrity_check) =
        query_fsqlite_committed_state(path.as_str(), threads, separate_tables)?;
    let committed_state = build_committed_state_oracle(expected, observed, integrity_check);
    let offered_writes = threads
        .checked_mul(rows_per_thread)
        .ok_or_else(|| "FrankenSQLite offered-write count overflow".to_owned())?;
    let accounting = build_work_accounting(
        offered_writes,
        work.attempted_writes,
        work.retried_operations,
        work.reported_failed_writes,
        committed_state.observed_rows,
    );

    Ok(RunResult {
        worker_startup_elapsed,
        workload_elapsed,
        settings: init_settings,
        accounting,
        committed_state,
    })
}

// ─── C SQLite (rusqlite) workload ────────────────────────────────────────

fn run_rusqlite(
    threads: usize,
    rows_per_thread: usize,
    separate_tables: bool,
    transaction_granularity: TransactionGranularity,
    retry_timeout: Duration,
    wal_autocheckpoint_pages: i64,
    synchronous: SynchronousMode,
) -> Result<RunResult, String> {
    let tmp = tempfile::NamedTempFile::new()
        .map_err(|error| format!("C SQLite tempfile creation failed: {error}"))?;
    let path = tmp
        .path()
        .to_str()
        .ok_or_else(|| "C SQLite tempfile path is not UTF-8".to_owned())?
        .to_owned();

    let init_settings = {
        let conn = rusqlite::Connection::open(&path)
            .map_err(|error| format!("C SQLite init open failed: {error}"))?;
        let settings =
            configure_rusqlite_connection(&conn, wal_autocheckpoint_pages, synchronous)?;
        conn.execute_batch(&create_tables_sql(threads, separate_tables))
            .map_err(|error| format!("C SQLite init schema failed: {error}"))?;
        settings
    };

    let path = Arc::new(path);
    let startup_gate = Arc::new((Mutex::new(StartupGateState::default()), Condvar::new()));
    let (startup_tx, startup_rx) = mpsc::channel::<StartupOutcome>();
    let mut handles = Vec::with_capacity(threads);

    let worker_startup_started = Instant::now();
    for tid in 0..threads {
        let path = Arc::clone(&path);
        let startup_gate = Arc::clone(&startup_gate);
        let startup_tx = startup_tx.clone();
        let handle = thread::spawn(move || -> Result<WorkerWork, String> {
            use rusqlite::OpenFlags;
            let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX;
            let setup = (|| {
                let conn = rusqlite::Connection::open_with_flags(path.as_str(), flags)
                    .map_err(|error| format!("C SQLite worker {tid} open failed: {error}"))?;
                let settings =
                    configure_rusqlite_connection(&conn, wal_autocheckpoint_pages, synchronous)
                        .map_err(|error| format!("C SQLite worker {tid} setup failed: {error}"))?;
                Ok::<_, String>((conn, settings))
            })();
            let (conn, settings) = match setup {
                Ok(worker) => {
                    let _ = startup_tx.send(StartupOutcome {
                        tid,
                        kind: StartupResultKind::Ready,
                        error: None,
                    });
                    worker
                }
                Err(error) => {
                    let _ = startup_tx.send(StartupOutcome {
                        tid,
                        kind: StartupResultKind::Failed,
                        error: Some(error.clone()),
                    });
                    return Err(error);
                }
            };

            let (gate_lock, gate_cv) = &*startup_gate;
            let mut gate_state = gate_lock
                .lock()
                .map_err(|_| "C SQLite startup gate poisoned".to_owned())?;
            while !gate_state.release && !gate_state.abort {
                gate_state = gate_cv
                    .wait(gate_state)
                    .map_err(|_| "C SQLite startup gate poisoned while waiting".to_owned())?;
            }
            if gate_state.abort {
                return Err(format!(
                    "C SQLite t{tid} startup aborted after peer open failure"
                ));
            }
            drop(gate_state);

            #[allow(clippy::cast_possible_wrap)]
            let base = if separate_tables {
                0
            } else {
                tid as i64 * ROWID_BASE_STRIDE
            };
            let mut failed = 0usize;
            let mut attempted_writes = 0usize;
            let mut retried_operations = 0usize;
            let insert_sql = worker_insert_sql(tid, separate_tables);

            if transaction_granularity == TransactionGranularity::OneRow {
                let mut stmt = conn
                    .prepare(&insert_sql)
                    .map_err(|error| format!("[sqlite t{tid}] prepare failed: {error}"))?;
                let (attempted_writes, retried_operations) = run_rusqlite_one_row_transactions(
                    &conn,
                    &mut stmt,
                    tid,
                    base,
                    rows_per_thread,
                    retry_timeout,
                )?;
                return Ok(WorkerWork {
                    settings,
                    attempted_writes,
                    retried_operations,
                    reported_failed_writes: 0,
                    workload_finished: Instant::now(),
                });
            }

            conn.execute_batch("BEGIN")
                .map_err(|error| format!("[sqlite t{tid}] BEGIN failed: {error}"))?;
            {
                let mut stmt = conn
                    .prepare(&insert_sql)
                    .map_err(|error| format!("[sqlite t{tid}] prepare failed: {error}"))?;
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..rows_per_thread as i64 {
                    let id = base + i;
                    let payload = format!("tid{tid}_i{i}");
                    let mut retry = 0usize;
                    loop {
                        attempted_writes = attempted_writes.checked_add(1).ok_or_else(|| {
                            format!("[sqlite t{tid}] write-attempt counter overflow")
                        })?;
                        match stmt.execute(rusqlite::params![id, &payload]) {
                            Ok(_) => break,
                            Err(e) => {
                                if retry < MAX_RETRIES && csqlite_error_is_retryable(&e) {
                                    retry += 1;
                                    retried_operations =
                                        retried_operations.checked_add(1).ok_or_else(|| {
                                            format!("[sqlite t{tid}] retry counter overflow")
                                        })?;
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                eprintln!("[sqlite t{tid}] INSERT {id} failed: {e}");
                                failed = failed.checked_add(1).ok_or_else(|| {
                                    format!("[sqlite t{tid}] failed-write counter overflow")
                                })?;
                                break;
                            }
                        }
                    }
                }
            }
            // Retry COMMIT on Busy — WAL writer serialisation can race.
            let mut retry = 0usize;
            loop {
                match conn.execute_batch("COMMIT") {
                    Ok(()) => break,
                    Err(e) => {
                        if retry < MAX_RETRIES && csqlite_error_is_retryable(&e) {
                            retry += 1;
                            retried_operations = retried_operations
                                .checked_add(1)
                                .ok_or_else(|| format!("[sqlite t{tid}] retry counter overflow"))?;
                            thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                            continue;
                        }
                        eprintln!("[sqlite t{tid}] COMMIT failed: {e}");
                        let _ = conn.execute_batch("ROLLBACK");
                        failed = rows_per_thread;
                        break;
                    }
                }
            }

            Ok(WorkerWork {
                settings,
                attempted_writes,
                retried_operations,
                reported_failed_writes: failed,
                workload_finished: Instant::now(),
            })
        });
        handles.push(handle);
    }
    drop(startup_tx);

    let startup_failures = match collect_startup_outcomes("C SQLite", threads, &startup_rx) {
        Ok(failures) => failures,
        Err(error) => {
            return Err(cleanup_workers_after_startup_failure(
                "C SQLite",
                &startup_gate,
                handles,
                &init_settings,
                error,
            ));
        }
    };
    if !startup_failures.is_empty() {
        let error = format_startup_failures("C SQLite", &startup_failures);
        return Err(cleanup_workers_after_startup_failure(
            "C SQLite",
            &startup_gate,
            handles,
            &init_settings,
            error,
        ));
    }

    let worker_startup_elapsed = worker_startup_started.elapsed();
    let workload_started = publish_startup_decision(&startup_gate, true);
    let work = join_worker_handles("C SQLite", handles, &init_settings)?;
    let workload_elapsed = work
        .workload_finished
        .checked_duration_since(workload_started)
        .ok_or_else(|| "C SQLite worker completion preceded workload start".to_owned())?;
    let expected = expected_database_state(threads, rows_per_thread, separate_tables)?;
    let (observed, integrity_check) =
        query_rusqlite_committed_state(path.as_str(), threads, separate_tables)?;
    let committed_state = build_committed_state_oracle(expected, observed, integrity_check);
    let offered_writes = threads
        .checked_mul(rows_per_thread)
        .ok_or_else(|| "C SQLite offered-write count overflow".to_owned())?;
    let accounting = build_work_accounting(
        offered_writes,
        work.attempted_writes,
        work.retried_operations,
        work.reported_failed_writes,
        committed_state.observed_rows,
    );

    Ok(RunResult {
        worker_startup_elapsed,
        workload_elapsed,
        settings: init_settings,
        accounting,
        committed_state,
    })
}

// ─── Driver ───────────────────────────────────────────────────────────────

fn collect_contract<N1, N2, A, B>(
    iters: usize,
    mut null_a: N1,
    mut null_b: N2,
    mut baseline: A,
    mut candidate: B,
) -> Result<(PairedRunStats, PairedRunStats, Vec<RoundOrderReceipt>), String>
where
    N1: FnMut() -> Result<RunResult, String>,
    N2: FnMut() -> Result<RunResult, String>,
    A: FnMut() -> Result<RunResult, String>,
    B: FnMut() -> Result<RunResult, String>,
{
    let mut null_a_samples = Vec::with_capacity(iters);
    let mut null_b_samples = Vec::with_capacity(iters);
    let mut baseline_samples = Vec::with_capacity(iters);
    let mut candidate_samples = Vec::with_capacity(iters);
    let mut round_order_receipts = Vec::with_capacity(iters);

    for round in 0..iters {
        let (null_a_sample, null_b_sample, baseline_sample, candidate_sample) = if round % 2 == 0 {
            (null_a()?, null_b()?, baseline()?, candidate()?)
        } else {
            let candidate_sample = candidate()?;
            let baseline_sample = baseline()?;
            let null_b_sample = null_b()?;
            let null_a_sample = null_a()?;
            (
                null_a_sample,
                null_b_sample,
                baseline_sample,
                candidate_sample,
            )
        };
        null_a_samples.push(null_a_sample);
        null_b_samples.push(null_b_sample);
        baseline_samples.push(baseline_sample);
        candidate_samples.push(candidate_sample);
        round_order_receipts.push(round_order_receipt(round));
    }

    Ok((
        paired_run_stats(null_a_samples, null_b_samples),
        paired_run_stats(baseline_samples, candidate_samples),
        round_order_receipts,
    ))
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn main() {
    let opts = parse_args();
    if let Err(error) = run(opts) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn run(opts: Options) -> Result<(), String> {
    let provenance = ProvenanceCapture::begin();
    human_output!(opts.json_stdout, "bench_elf_sha256={}", self_identity());
    human_output!(
        opts.json_stdout,
        "bench_source_sha256 {}",
        file_identity(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/bin/mt_mvcc_bench.rs"
        )))
    );
    validate_workload_bounds(opts.rows_per_thread, opts.separate_tables)?;
    let (wal_autocheckpoint_pages, wal_autocheckpoint_overridden) =
        bench_wal_autocheckpoint_pages()?;
    let available_parallelism = std::thread::available_parallelism()
        .ok()
        .map(|value| value.get());

    eprintln!(
        "mt-mvcc-bench: rows_per_thread={} threads={:?} paired_rounds={} bootstrap_reps={} synchronous={} wal_autocheckpoint={} available_parallelism={available_parallelism:?} apples_to_apples={} separate_tables={} transaction_granularity={}",
        opts.rows_per_thread,
        opts.threads,
        opts.iters,
        CONTRACT_BOOTSTRAP_REPS,
        opts.synchronous.pragma_value(),
        wal_autocheckpoint_pages,
        opts.apples_to_apples,
        opts.separate_tables,
        opts.transaction_granularity.label(),
    );
    eprintln!(
        "mt-mvcc-bench: NON-CITABLE: {}",
        opts.transaction_granularity.non_citable_reason()
    );
    eprintln!("mt-mvcc-bench: settings contract: {SETTINGS_INTERPRETATION}");
    eprintln!("mt-mvcc-bench: accounting contract: {ACCOUNTING_INTERPRETATION}");
    eprintln!("mt-mvcc-bench: timing contract: {TIMING_INTERPRETATION}");
    if wal_autocheckpoint_overridden {
        eprintln!(
            "mt-mvcc-bench: DIAGNOSTIC OVERRIDE wal_autocheckpoint={wal_autocheckpoint_pages} applied to BOTH \
             engines — results are NOT comparable with published default-cadence numbers"
        );
    }

    human_output!(
        opts.json_stdout,
        "threads | configuration | fsqlite_wps | sqlite_wps | throughput_ratio | fsqlite_wps_p95 | fsqlite_wps_p99 | sqlite_wps_p95 | sqlite_wps_p99 | fsqlite_ms_p50 | fsqlite_ms_p95 | fsqlite_ms_p99 | sqlite_ms_p50 | sqlite_ms_p95 | sqlite_ms_p99 | time_ratio | fsqlite_failed | sqlite_failed"
    );
    let mut thread_results = Vec::new();
    let mut configuration_receipts = Vec::with_capacity(opts.threads.len());
    for &n in &opts.threads {
        let retry_timeout = opts.retry_timeout_secs.map_or_else(
            || fsqlite_retry_timeout(n, opts.rows_per_thread),
            Duration::from_secs,
        );
        let retry_policy = retry_policy_receipt_for_granularity(
            retry_timeout,
            opts.retry_timeout_secs.is_some(),
            opts.transaction_granularity,
        )?;
        let configuration = configuration_receipt(
            n,
            opts.rows_per_thread,
            available_parallelism,
            wal_autocheckpoint_pages,
            wal_autocheckpoint_overridden,
            retry_policy,
        );
        human_output!(
            opts.json_stdout,
            "case={} threads={n} configuration_status={} comparison_eligible={} measured={} available_parallelism={:?} max_supported_writers={} offered_writes_per_sample={:?} wal_autocheckpoint_pages={:?} wal_autocheckpoint_overridden={:?} retry_policy={:?} reason={:?}",
            workload_shape(opts.separate_tables),
            configuration.status,
            configuration.comparison_eligible,
            configuration.measured,
            configuration.available_parallelism,
            configuration.max_supported_writers,
            configuration.offered_writes_per_sample,
            configuration.wal_autocheckpoint_pages,
            configuration.wal_autocheckpoint_overridden,
            configuration.retry_policy,
            configuration.reason,
        );
        configuration_receipts.push(configuration.clone());
        if !configuration.measured {
            continue;
        }
        let (null, claim, round_order_receipts) = collect_contract(
            opts.iters,
            || {
                run_rusqlite(
                    n,
                    opts.rows_per_thread,
                    opts.separate_tables,
                    opts.transaction_granularity,
                    retry_timeout,
                    wal_autocheckpoint_pages,
                    opts.synchronous,
                )
            },
            || {
                run_rusqlite(
                    n,
                    opts.rows_per_thread,
                    opts.separate_tables,
                    opts.transaction_granularity,
                    retry_timeout,
                    wal_autocheckpoint_pages,
                    opts.synchronous,
                )
            },
            || {
                run_rusqlite(
                    n,
                    opts.rows_per_thread,
                    opts.separate_tables,
                    opts.transaction_granularity,
                    retry_timeout,
                    wal_autocheckpoint_pages,
                    opts.synchronous,
                )
            },
            || {
                // Registry commit-lock decomposition (bd-i0tn6 evidence):
                // reset before / snapshot after each F invocation so every
                // paired round prints its own hold/wait line, tagged with
                // the thread count. The bench writers run in-process, so
                // the process-global counters are exactly this run's.
                fsqlite_mvcc::reset_registry_commit_lock_metrics();
                let result = run_fsqlite(
                    n,
                    opts.rows_per_thread,
                    opts.separate_tables,
                    opts.transaction_granularity,
                    retry_timeout,
                    wal_autocheckpoint_pages,
                    opts.synchronous,
                );
                let m = fsqlite_mvcc::registry_commit_lock_metrics();
                if m.holds_total > 0 {
                    eprintln!(
                        "registry_lock threads={n} holds={} wait_ns_total={} wait_ns_max={} \
                         hold_ns_total={} hold_ns_max={} mean_hold_us={:.1} mean_wait_us={:.1}",
                        m.holds_total,
                        m.wait_ns_total,
                        m.wait_ns_max,
                        m.hold_ns_total,
                        m.hold_ns_max,
                        m.hold_ns_total as f64 / m.holds_total as f64 / 1_000.0,
                        m.wait_ns_total as f64 / m.holds_total as f64 / 1_000.0,
                    );
                }
                result
            },
        )?;
        let report_round_order_receipts =
            if opts.transaction_granularity == TransactionGranularity::OneRow {
                round_order_receipts.as_slice()
            } else {
                &[]
            };
        let report = build_thread_report(
            n,
            &null,
            &claim,
            &configuration,
            report_round_order_receipts,
        );
        let contract = report
            .median_ci_contract
            .as_ref()
            .expect("current report always carries median-CI evidence");

        human_output!(
            opts.json_stdout,
            "{n:>7} | {configuration_status:>13} | {fs_wps:>11.0} | {cs_wps:>10.0} | {throughput_ratio:>16.2}x | {fs_wps_p95:>15.0} | {fs_wps_p99:>15.0} | {sqlite_wps_p95:>14.0} | {sqlite_wps_p99:>14.0} | {fs_ms_p50:>14.2} | {fs_ms_p95:>14.2} | {fs_ms_p99:>14.2} | {sqlite_ms_p50:>13.2} | {sqlite_ms_p95:>13.2} | {sqlite_ms_p99:>13.2} | {time_ratio:>10.2}x | {fs_failed:>14} | {sqlite_failed:>13}",
            configuration_status = configuration.status,
            fs_wps = report.fsqlite_wps_p50,
            cs_wps = report.sqlite_wps_p50,
            throughput_ratio = report.throughput_ratio,
            fs_wps_p95 = report.fsqlite_wps_p95,
            fs_wps_p99 = report.fsqlite_wps_p99,
            sqlite_wps_p95 = report.sqlite_wps_p95,
            sqlite_wps_p99 = report.sqlite_wps_p99,
            fs_ms_p50 = report.fsqlite_ms_p50,
            fs_ms_p95 = report.fsqlite_ms_p95,
            fs_ms_p99 = report.fsqlite_ms_p99,
            sqlite_ms_p50 = report.sqlite_ms_p50,
            sqlite_ms_p95 = report.sqlite_ms_p95,
            sqlite_ms_p99 = report.sqlite_ms_p99,
            time_ratio = report.time_ratio,
            fs_failed = report.fsqlite_failed_rows,
            sqlite_failed = report.sqlite_failed_rows
        );
        human_output!(
            opts.json_stdout,
            "case={} threads={n} synchronous={} null_c_c ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} cv_gate=never null_a_offered={} null_a_attempted={} null_a_committed={} null_a_retried={} null_a_failed={} null_b_offered={} null_b_attempted={} null_b_committed={} null_b_retried={} null_b_failed={}",
            workload_shape(opts.separate_tables),
            opts.synchronous.pragma_value(),
            contract.null_ratio_median,
            contract.null_ratio_ci95_low,
            contract.null_ratio_ci95_high,
            contract.null_ratio_cv_pct,
            contract.null_ratio_mad,
            null.arm_a.total_offered_writes(),
            null.arm_a.total_attempted_writes(),
            null.arm_a.total_succeeded_writes(),
            null.arm_a.total_retried_operations(),
            null.arm_a.total_failed_rows(),
            null.arm_b.total_offered_writes(),
            null.arm_b.total_attempted_writes(),
            null.arm_b.total_succeeded_writes(),
            null.arm_b.total_retried_operations(),
            null.arm_b.total_failed_rows(),
        );
        human_output!(
            opts.json_stdout,
            "case={} threads={n} synchronous={} claim_f_over_c ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} fsqlite_p50_wps={:.3} sqlite_p50_wps={:.3} fsqlite_offered={} fsqlite_attempted={} fsqlite_committed={} fsqlite_retried={} fsqlite_failed={} sqlite_offered={} sqlite_attempted={} sqlite_committed={} sqlite_retried={} sqlite_failed={}",
            workload_shape(opts.separate_tables),
            opts.synchronous.pragma_value(),
            contract.claim_ratio_median,
            contract.claim_ratio_ci95_low,
            contract.claim_ratio_ci95_high,
            contract.claim_ratio_cv_pct,
            contract.claim_ratio_mad,
            report.fsqlite_wps_p50,
            report.sqlite_wps_p50,
            claim.arm_b.total_offered_writes(),
            claim.arm_b.total_attempted_writes(),
            claim.arm_b.total_succeeded_writes(),
            claim.arm_b.total_retried_operations(),
            claim.arm_b.total_failed_rows(),
            claim.arm_a.total_offered_writes(),
            claim.arm_a.total_attempted_writes(),
            claim.arm_a.total_succeeded_writes(),
            claim.arm_a.total_retried_operations(),
            claim.arm_a.total_failed_rows(),
        );
        let c_settings = &claim
            .arm_a
            .samples
            .first()
            .expect("paired claim has at least one C sample")
            .settings;
        let f_settings = &claim
            .arm_b
            .samples
            .first()
            .expect("paired claim has at least one FrankenSQLite sample")
            .settings;
        human_output!(
            opts.json_stdout,
            "case={} threads={n} effective_settings_readback c={c_settings:?} f={f_settings:?} note=equal_named_values_do_not_claim_cross_engine_semantic_equivalence",
            workload_shape(opts.separate_tables),
        );
        human_output!(
            opts.json_stdout,
            "case={} threads={n} configuration_status={} median_ci_gate={} rule=claim_ci95_beyond_2x_null_radius cv_gate={} null_radius={:.6} claim_margin={} min_decidable_gain={:.6} max_decidable_regression={:.6}",
            workload_shape(opts.separate_tables),
            configuration.status,
            contract.verdict,
            contract.cv_gate,
            contract.null_radius,
            contract
                .claim_margin
                .map_or_else(|| "unbounded".to_owned(), |margin| format!("{margin:.3}"),),
            contract.min_decidable_gain,
            contract.max_decidable_regression,
        );
        thread_results.push(report);
    }

    let previous_report = load_previous_report(&opts.history_json)?;
    let workload_shape = workload_shape(opts.separate_tables);
    let pass_over_pass_gate = build_pass_over_pass_gate(PassOverPassGateInput {
        history_json: &opts.history_json,
        previous: previous_report.as_ref(),
        historical_baseline_authentication: HistoricalBaselineAuthentication::Unavailable,
        current_rows: &thread_results,
        current_configuration_receipts: &configuration_receipts,
        current_workload_shape: workload_shape,
        current_rows_per_thread: opts.rows_per_thread,
        current_iterations: opts.iters,
        current_transaction_granularity: opts.transaction_granularity,
        current_wal_autocheckpoint_overridden: wal_autocheckpoint_overridden,
        current_retry_timeout_overridden: opts.retry_timeout_secs.is_some(),
    });
    eprintln!(
        "mt-mvcc-bench: pass-over-pass status={} comparable_pairs={} previous_report_found={}",
        pass_over_pass_gate.status,
        pass_over_pass_gate.comparable_pair_count,
        pass_over_pass_gate.previous_report_found,
    );
    let workload_evidence_valid = !history_evidence_is_invalid(
        wal_autocheckpoint_overridden,
        opts.retry_timeout_secs.is_some(),
        opts.rows_per_thread,
        opts.iters,
        opts.transaction_granularity,
        &thread_results,
        &configuration_receipts,
    );
    let report_schema = opts.transaction_granularity.report_schema();
    let transaction_contract = configuration_receipts
        .first()
        .and_then(|configuration| configuration.retry_policy.as_ref())
        .and_then(|retry_policy| {
            transaction_contract_receipt(opts.transaction_granularity, retry_policy)
        });
    let transaction_contract_valid = transaction_contract_is_valid(
        report_schema,
        opts.transaction_granularity,
        opts.rows_per_thread,
        transaction_contract.as_ref(),
        &configuration_receipts,
    );
    let (subject_identity, comparison_environment) = provenance.finish();
    let measurement_evidence_valid = workload_evidence_valid
        && transaction_contract_valid
        && provenance_evidence_is_valid(&subject_identity, &comparison_environment);

    let full_report = MtMvccBenchReport {
        schema_version: report_schema,
        citable: false,
        measurement_evidence_valid,
        non_citable_reason: opts.transaction_granularity.non_citable_reason(),
        release_regression_scope: opts.transaction_granularity.release_regression_scope(),
        subject_identity,
        comparison_environment,
        settings_interpretation: SETTINGS_INTERPRETATION,
        accounting_interpretation: ACCOUNTING_INTERPRETATION,
        timing_interpretation: TIMING_INTERPRETATION,
        workload_shape,
        transaction_contract,
        rows_per_thread: opts.rows_per_thread,
        iterations: opts.iters,
        configuration_receipts,
        thread_results,
        pass_over_pass_gate,
    };

    if let Some(path) = opts.json_output.as_deref() {
        write_json_report(path, &full_report)?;
        eprintln!("mt-mvcc-bench: wrote json report {}", path.display());
    }
    if let Some(path) = opts.summary_md.as_deref() {
        write_markdown_summary(path, &full_report)?;
        eprintln!("mt-mvcc-bench: wrote markdown summary {}", path.display());
    }
    if opts.json_stdout {
        write_canonical_json_stdout(&full_report)?;
    }
    if history_update_is_allowed(&full_report) {
        write_json_report(&opts.history_json, &full_report)?;
        eprintln!(
            "mt-mvcc-bench: updated pass-over-pass history {}",
            opts.history_json.display()
        );
    }
    if !full_report.pass_over_pass_gate.regressions.is_empty() {
        eprintln!(
            "mt-mvcc-bench: historical ratio warning (provenance only; median-CI is the decision gate): {}",
            full_report
                .pass_over_pass_gate
                .regressions
                .iter()
                .map(|regression| format!(
                    "{}t {:.2}x -> {:.2}x ({:.2}% drop)",
                    regression.threads,
                    regression.previous_ratio,
                    regression.current_ratio,
                    regression.ratio_drop_pct
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !full_report.measurement_evidence_valid {
        return Err(
            "benchmark evidence is non-comparable or invalid; inspect configuration receipts, \
             committed-state oracles, work accounting, and provenance receipts"
                .to_owned(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_file_snapshot(hash: &str) -> FileSnapshotReceipt {
        FileSnapshotReceipt {
            sha256: Some(hash.to_owned()),
            bytes_read: Some(3),
            metadata_size_bytes: Some(3),
            unix_device: Some(7),
            unix_inode: Some(11),
            error: None,
        }
    }

    fn test_subject_identity() -> SubjectIdentityReceipt {
        SubjectIdentityReceipt {
            executable: ExecutableIdentityReceipt {
                current_exe_path: Some("/test/mt-mvcc-bench".to_owned()),
                canonical_path: Some("/test/mt-mvcc-bench".to_owned()),
                path_resolution_error: None,
                process_id: 42,
                before_measurement: test_file_snapshot("abc123"),
                after_measurement: test_file_snapshot("abc123"),
                unchanged_during_measurement: Some(true),
            },
            build_source: BuildSourceIdentityReceipt {
                workspace_root: "/test/workspace".to_owned(),
                git_sha: "deadbeef".to_owned(),
                git_branch: "main".to_owned(),
                git_tree_state: "clean".to_owned(),
                build_nonce: "nonce".to_owned(),
                build_input_tracking: "complete".to_owned(),
            },
            runtime_source: RuntimeSourceStabilityReceipt {
                before_measurement: test_runtime_source_identity(),
                after_measurement: test_runtime_source_identity(),
                same_clean_git_identity_at_capture_points: Some(true),
                stability_limitation: "test limitation",
            },
            cargo_lock: CargoLockIdentityReceipt {
                embedded_build_sha256: "lockhash".to_owned(),
                embedded_build_size_bytes: 3,
                runtime_path: "/test/workspace/Cargo.lock".to_owned(),
                before_measurement: test_file_snapshot("lockhash"),
                after_measurement: test_file_snapshot("lockhash"),
                before_matches_embedded_build: Some(true),
                after_matches_embedded_build: Some(true),
                unchanged_at_capture_points: Some(true),
            },
        }
    }

    fn test_runtime_source_identity() -> RuntimeSourceIdentityReceipt {
        RuntimeSourceIdentityReceipt {
            workspace_root: "/test/workspace".to_owned(),
            canonical_workspace_root: Some("/test/workspace".to_owned()),
            git_sha: Some("deadbeef".to_owned()),
            git_branch: Some("main".to_owned()),
            git_tree_state: "clean".to_owned(),
            matches_build_git_sha: Some(true),
            discovery_errors: Vec::new(),
        }
    }

    fn test_comparison_environment() -> ComparisonEnvironmentReceipt {
        ComparisonEnvironmentReceipt {
            build_configuration: BuildConfigurationReceipt {
                cargo_profile: "release".to_owned(),
                selected_profile: "release-perf".to_owned(),
                profile_label: "release-perf".to_owned(),
                opt_level: "3".to_owned(),
                debug: "false".to_owned(),
                target: "x86_64-unknown-linux-gnu".to_owned(),
                build_host: "x86_64-unknown-linux-gnu".to_owned(),
                enabled_features: Vec::new(),
                rustflags: RustflagsReceipt {
                    cargo_encoded_rustflags_present: false,
                    encoded_hex: String::new(),
                    decoded_arguments: Some(Vec::new()),
                    decode_error: None,
                },
                profile_overrides_hex: String::new(),
                native_build_overrides_hex: String::new(),
                rustc_version_verbose: "rustc test".to_owned(),
                cargo_version: "cargo test".to_owned(),
                resolved_dependency_feature_graph_sha256: None,
                resolved_dependency_feature_graph_limitation:
                    DEPENDENCY_GRAPH_ATTESTATION_UNAVAILABLE,
            },
            invocation: InvocationReceipt {
                argv_lossy: vec!["mt-mvcc-bench".to_owned(), "--json-stdout".to_owned()],
                argv_raw_hex: vec!["6d74".to_owned(), "2d2d".to_owned()],
                raw_encoding: "unix_os_str_bytes",
                length_prefixed_argv_sha256: "argvhash".to_owned(),
            },
            measurement_host: MeasurementHostReceipt {
                host: StaticMeasurementHostReceipt {
                    hostname: Some("test-host".to_owned()),
                    cpu_model: Some("test-cpu".to_owned()),
                    available_parallelism: Some(8),
                    cpu_online: Some("0-7".to_owned()),
                    cpu_present: Some("0-7".to_owned()),
                    cpu_possible: Some("0-7".to_owned()),
                    cpu_isolated: None,
                    cpu_topology: CpuTopologyReceipt {
                        logical_cpu_directories: Some(8),
                        physical_package_count: Some(1),
                        physical_core_count: Some(4),
                    },
                    scaling_governors_by_cpu: BTreeMap::new(),
                    kernel_release: Some("test-kernel".to_owned()),
                    kernel_version: Some("test-version".to_owned()),
                    numa_online_nodes: Some("0".to_owned()),
                    numa_possible_nodes: Some("0".to_owned()),
                    numa_node_directories: Some(1),
                    unavailable_fields: vec![
                        "cpu_isolated".to_owned(),
                        "scaling_governors_by_cpu".to_owned(),
                    ],
                },
                before_measurement: DynamicMeasurementHostReceipt {
                    unix_epoch_millis: Some(1),
                    process_cpu_affinity_mask: Some("ff".to_owned()),
                    process_cpu_affinity_list: Some("0-7".to_owned()),
                    proc_self_cgroup: Some("0::/test".to_owned()),
                    cpuset_cpus_effective: Some("0-7".to_owned()),
                    cpuset_mems_effective: Some("0".to_owned()),
                    load_average: Some("0.00 0.00 0.00 1/1 1".to_owned()),
                    pressure_cpu: Some("some avg10=0.00".to_owned()),
                    pressure_memory: Some("some avg10=0.00".to_owned()),
                    pressure_io: Some("some avg10=0.00".to_owned()),
                },
                after_measurement: DynamicMeasurementHostReceipt {
                    unix_epoch_millis: Some(2),
                    process_cpu_affinity_mask: Some("ff".to_owned()),
                    process_cpu_affinity_list: Some("0-7".to_owned()),
                    proc_self_cgroup: Some("0::/test".to_owned()),
                    cpuset_cpus_effective: Some("0-7".to_owned()),
                    cpuset_mems_effective: Some("0".to_owned()),
                    load_average: Some("0.00 0.00 0.00 1/1 1".to_owned()),
                    pressure_cpu: Some("some avg10=0.00".to_owned()),
                    pressure_memory: Some("some avg10=0.00".to_owned()),
                    pressure_io: Some("some avg10=0.00".to_owned()),
                },
            },
        }
    }

    fn minimal_v7_report() -> MtMvccBenchReport {
        MtMvccBenchReport {
            schema_version: REPORT_SCHEMA_V7,
            citable: false,
            measurement_evidence_valid: true,
            non_citable_reason: NON_CITABLE_REASON,
            release_regression_scope: RELEASE_REGRESSION_SCOPE,
            subject_identity: test_subject_identity(),
            comparison_environment: test_comparison_environment(),
            settings_interpretation: SETTINGS_INTERPRETATION,
            accounting_interpretation: ACCOUNTING_INTERPRETATION,
            timing_interpretation: TIMING_INTERPRETATION,
            workload_shape: "shared_table",
            transaction_contract: None,
            rows_per_thread: 1,
            iterations: 1,
            configuration_receipts: Vec::new(),
            thread_results: Vec::new(),
            pass_over_pass_gate: PassOverPassGateReport {
                schema_version: PASS_OVER_PASS_SCHEMA_V1,
                history_json_path: DEFAULT_HISTORY_JSON.to_owned(),
                threshold_ratio_drop_pct: PASS_OVER_PASS_MAX_RATIO_DROP_PCT,
                status: "disabled_non_citable",
                previous_report_found: false,
                comparable_pair_count: 0,
                regressions: Vec::new(),
            },
        }
    }

    fn minimal_v10_one_row_report() -> MtMvccBenchReport {
        let retry_policy = retry_policy_receipt_for_granularity(
            fsqlite_retry_timeout(1, 1),
            false,
            TransactionGranularity::OneRow,
        )
        .expect("one-row test retry policy must be representable");
        let configuration = configuration_receipt(
            1,
            1,
            Some(8),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            retry_policy.clone(),
        );
        let mut report = minimal_v7_report();
        report.schema_version = REPORT_SCHEMA_V10;
        report.non_citable_reason = NON_CITABLE_REASON_V10;
        report.release_regression_scope = RELEASE_REGRESSION_SCOPE_V10;
        report.transaction_contract =
            transaction_contract_receipt(TransactionGranularity::OneRow, &retry_policy);
        report.configuration_receipts = vec![configuration];
        report
    }

    #[test]
    fn subject_identity_hashing_uses_sha256_and_file_metadata() {
        assert_eq!(
            sha256_bytes(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );

        let cargo_toml = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let bytes = fs::read(&cargo_toml).expect("test Cargo.toml must be readable");
        let snapshot = snapshot_file(&cargo_toml);

        assert_eq!(
            snapshot.sha256.as_deref(),
            Some(sha256_bytes(&bytes).as_str())
        );
        assert_eq!(snapshot.bytes_read, u64::try_from(bytes.len()).ok());
        assert_eq!(snapshot.metadata_size_bytes, snapshot.bytes_read);
        assert!(snapshot.error.is_none());
    }

    #[test]
    fn file_snapshot_stability_fails_closed_on_incomplete_or_errored_identity() {
        let stable = test_file_snapshot("abc123");
        assert_eq!(file_snapshots_match(&stable, &stable), Some(true));

        let mut errored = stable.clone();
        errored.error = Some("metadata unavailable".to_owned());
        assert_eq!(file_snapshots_match(&stable, &errored), None);

        let mut incomplete = stable.clone();
        incomplete.metadata_size_bytes = None;
        assert_eq!(file_snapshots_match(&stable, &incomplete), None);

        let mut wrong_size = stable.clone();
        wrong_size.metadata_size_bytes = Some(4);
        assert_eq!(file_snapshots_match(&stable, &wrong_size), Some(false));

        let mut portable_before = stable.clone();
        portable_before.unix_device = None;
        portable_before.unix_inode = None;
        let portable_after = portable_before.clone();
        assert_eq!(
            file_snapshots_match(&portable_before, &portable_after),
            Some(true)
        );
    }

    #[test]
    fn provenance_evidence_requires_complete_stable_subject_and_environment() {
        let subject = test_subject_identity();
        let environment = test_comparison_environment();
        assert!(provenance_evidence_is_valid(&subject, &environment));

        let mut invalid = subject.clone();
        invalid.executable.path_resolution_error = Some("canonicalize failed".to_owned());
        assert!(!provenance_evidence_is_valid(&invalid, &environment));

        let mut invalid = subject.clone();
        invalid.executable.after_measurement.sha256 = Some("changed".to_owned());
        assert!(!provenance_evidence_is_valid(&invalid, &environment));

        let mut invalid = subject.clone();
        invalid.build_source.git_tree_state = "dirty".to_owned();
        assert!(!provenance_evidence_is_valid(&invalid, &environment));

        let mut invalid = subject.clone();
        invalid
            .runtime_source
            .before_measurement
            .discovery_errors
            .push("git unavailable".to_owned());
        assert!(!provenance_evidence_is_valid(&invalid, &environment));

        let mut invalid = subject.clone();
        invalid
            .runtime_source
            .after_measurement
            .matches_build_git_sha = Some(false);
        assert!(!provenance_evidence_is_valid(&invalid, &environment));

        let mut invalid = subject.clone();
        invalid.cargo_lock.before_measurement.sha256 = Some("stale-lock".to_owned());
        assert!(!provenance_evidence_is_valid(&invalid, &environment));

        let mut invalid = environment.clone();
        invalid.build_configuration.rustflags.decode_error =
            Some("invalid encoded rustflags".to_owned());
        assert!(!provenance_evidence_is_valid(&subject, &invalid));

        let mut invalid = environment.clone();
        invalid
            .measurement_host
            .after_measurement
            .process_cpu_affinity_list = Some("0-3".to_owned());
        assert!(!provenance_evidence_is_valid(&subject, &invalid));

        let mut invalid = environment;
        invalid
            .measurement_host
            .before_measurement
            .cpuset_cpus_effective = None;
        invalid
            .measurement_host
            .after_measurement
            .cpuset_cpus_effective = None;
        assert!(!provenance_evidence_is_valid(&subject, &invalid));
    }

    #[test]
    fn provenance_evidence_allows_external_nonce_and_optional_host_diagnostics() {
        let mut subject = test_subject_identity();
        // Standalone documented invocations do not set the build nonce. The
        // outer regression gate supplies and verifies one as a stronger
        // freshness contract, but capture completeness must not require it.
        subject.build_source.build_nonce = "unknown".to_owned();
        let mut environment = test_comparison_environment();
        let host = &mut environment.measurement_host.host;
        host.cpu_isolated = None;
        host.scaling_governors_by_cpu.clear();
        host.numa_online_nodes = None;
        host.numa_possible_nodes = None;
        host.numa_node_directories = None;
        environment.measurement_host.before_measurement.load_average = None;
        environment.measurement_host.after_measurement.load_average = None;
        environment.measurement_host.before_measurement.pressure_cpu = None;
        environment.measurement_host.after_measurement.pressure_cpu = None;
        environment
            .measurement_host
            .before_measurement
            .pressure_memory = None;
        environment
            .measurement_host
            .after_measurement
            .pressure_memory = None;
        environment.measurement_host.before_measurement.pressure_io = None;
        environment.measurement_host.after_measurement.pressure_io = None;

        assert!(provenance_evidence_is_valid(&subject, &environment));
    }

    #[test]
    fn v7_report_serializes_split_identity_environment_and_narrow_scope() {
        let value = serde_json::to_value(minimal_v7_report()).expect("v7 report must serialize");

        assert_eq!(value["schema_version"], REPORT_SCHEMA_V7);
        assert_eq!(value["citable"], false);
        assert_eq!(value["measurement_evidence_valid"], true);
        assert_eq!(
            value["pass_over_pass_gate"]["status"],
            "disabled_non_citable"
        );
        assert_eq!(value["release_regression_scope"], RELEASE_REGRESSION_SCOPE);
        assert_eq!(
            value["subject_identity"]["executable"]["unchanged_during_measurement"],
            true
        );
        assert_eq!(
            value["subject_identity"]["runtime_source"]["same_clean_git_identity_at_capture_points"],
            true
        );
        assert_eq!(
            value["subject_identity"]["cargo_lock"]["unchanged_at_capture_points"],
            true
        );
        assert_eq!(
            value["comparison_environment"]["build_configuration"]["selected_profile"],
            "release-perf"
        );
        assert!(
            value["comparison_environment"]["build_configuration"]
                ["resolved_dependency_feature_graph_sha256"]
                .is_null()
        );
        assert_eq!(
            value["comparison_environment"]["measurement_host"]["host"]["hostname"],
            "test-host"
        );
        assert!(value.get("transaction_contract").is_none());
        assert!(value.get("release_eligible").is_none());
    }

    #[test]
    fn optional_dependency_feature_graph_digest_is_strict_lower_sha256() {
        let digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(parse_optional_lower_sha256(""), Ok(None));
        assert_eq!(
            parse_optional_lower_sha256(digest),
            Ok(Some(digest.to_owned()))
        );
        assert!(parse_optional_lower_sha256(&digest[..63]).is_err());
        assert!(parse_optional_lower_sha256(&digest.to_ascii_uppercase()).is_err());
        assert!(parse_optional_lower_sha256(&format!("{}g", &digest[..63])).is_err());

        let (unavailable, limitation) = resolved_dependency_feature_graph_attestation("")
            .expect("an absent digest is an explicit non-release state");
        assert_eq!(unavailable, None);
        assert_eq!(limitation, DEPENDENCY_GRAPH_ATTESTATION_UNAVAILABLE);
        let (available, limitation) = resolved_dependency_feature_graph_attestation(digest)
            .expect("a canonical digest must be accepted");
        assert_eq!(available.as_deref(), Some(digest));
        assert_eq!(limitation, DEPENDENCY_GRAPH_ATTESTATION_AVAILABLE);

        let mut invalid = test_comparison_environment().build_configuration;
        invalid.resolved_dependency_feature_graph_sha256 = Some("A".repeat(64));
        assert!(!build_configuration_is_valid(&invalid));

        let mut mismatched = test_comparison_environment().build_configuration;
        mismatched.resolved_dependency_feature_graph_sha256 = Some(digest.to_owned());
        assert!(!build_configuration_is_valid(&mismatched));
        mismatched.resolved_dependency_feature_graph_limitation =
            DEPENDENCY_GRAPH_ATTESTATION_AVAILABLE;
        assert!(build_configuration_is_valid(&mismatched));
    }

    #[test]
    fn v10_one_row_report_binds_granularity_retry_units_and_non_citable_status() {
        let report = minimal_v10_one_row_report();
        assert!(transaction_contract_is_valid(
            report.schema_version,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            report.transaction_contract.as_ref(),
            &report.configuration_receipts,
        ));

        let value = serde_json::to_value(&report).expect("v10 one-row report must serialize");
        assert_eq!(value["schema_version"], REPORT_SCHEMA_V10);
        assert_eq!(value["citable"], false);
        assert_eq!(
            value["release_regression_scope"],
            RELEASE_REGRESSION_SCOPE_V10,
        );
        assert_eq!(
            value["transaction_contract"]["granularity"],
            "one_row_per_transaction"
        );
        assert_eq!(value["transaction_contract"]["rows_per_transaction"], 1);
        assert_eq!(
            value["configuration_receipts"][0]["retry_policy"]["csqlite_max_operation_retries"],
            0
        );
        assert_eq!(
            value["configuration_receipts"][0]["retry_policy"]["csqlite_max_transaction_retries"],
            MAX_RETRIES
        );
        assert_eq!(
            value["configuration_receipts"][0]["retry_policy"]["shared_worker_retry_timeout_ms"],
            retry_timeout_millis(fsqlite_retry_timeout(1, DEFAULT_ROWS_PER_THREAD))
                .expect("test retry timeout must fit")
        );
        assert_eq!(
            value["configuration_receipts"][0]["retry_policy"]["shared_worker_retry_timeout_overridden"],
            false
        );
        assert!(value.get("release_eligible").is_none());
    }

    #[test]
    fn one_row_report_contract_fails_closed_on_missing_or_mismatched_truth() {
        let report = minimal_v10_one_row_report();
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V7,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            report.transaction_contract.as_ref(),
            &report.configuration_receipts,
        ));
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V8,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            report.transaction_contract.as_ref(),
            &report.configuration_receipts,
        ));
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V9,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            None,
            &report.configuration_receipts,
        ));
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V10,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            None,
            &report.configuration_receipts,
        ));

        let mut invalid_configurations = report.configuration_receipts.clone();
        invalid_configurations[0]
            .retry_policy
            .as_mut()
            .expect("test configuration retry policy")
            .csqlite_retry_unit = "individual INSERT operation".to_owned();
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V10,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            report.transaction_contract.as_ref(),
            &invalid_configurations,
        ));

        let mut missing_shared_deadline = report.configuration_receipts.clone();
        missing_shared_deadline[0]
            .retry_policy
            .as_mut()
            .expect("test configuration retry policy")
            .shared_worker_retry_timeout_ms = None;
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V10,
            TransactionGranularity::OneRow,
            report.rows_per_thread,
            report.transaction_contract.as_ref(),
            &missing_shared_deadline,
        ));

        let mut coordinated_drift = minimal_v10_one_row_report();
        coordinated_drift
            .transaction_contract
            .as_mut()
            .expect("test transaction contract")
            .csqlite_retry_unit = "individual INSERT operation".to_owned();
        coordinated_drift.configuration_receipts[0]
            .retry_policy
            .as_mut()
            .expect("test configuration retry policy")
            .csqlite_retry_unit = "individual INSERT operation".to_owned();
        assert!(!transaction_contract_is_valid(
            REPORT_SCHEMA_V10,
            TransactionGranularity::OneRow,
            coordinated_drift.rows_per_thread,
            coordinated_drift.transaction_contract.as_ref(),
            &coordinated_drift.configuration_receipts,
        ));
    }

    #[test]
    fn invalid_measurement_evidence_is_explicit_in_json() {
        let mut report = minimal_v7_report();
        report.measurement_evidence_valid = false;

        let value = serde_json::to_value(report).expect("invalid v7 report must serialize");

        assert_eq!(value["measurement_evidence_valid"], false);
        assert_eq!(value["citable"], false);
    }

    #[test]
    fn v7_history_is_never_baseline_updatable() {
        let mut report = minimal_v7_report();
        assert!(!history_update_is_allowed(&report));

        report.citable = true;
        assert!(report.measurement_evidence_valid);
        assert!(!history_update_is_allowed(&report));

        report.measurement_evidence_valid = false;
        assert!(!history_update_is_allowed(&report));

        report.schema_version = "future-or-typo-schema";
        report.citable = true;
        report.measurement_evidence_valid = true;
        assert!(!history_update_is_allowed(&report));
    }

    #[test]
    fn json_stdout_argument_is_a_boolean_flag() {
        let options = parse_args_from(["--json-stdout", "--threads=1,8", "--iters", "2"])
            .expect("json stdout arguments must parse");

        assert!(options.json_stdout);
        assert_eq!(options.threads, vec![1, 8]);
        assert_eq!(options.iters, 2);
        assert!(!Options::default().json_stdout);
        assert!(matches!(
            parse_args_from(["--json-stdout=true"]),
            Err(ParseArgsError::Message(message)) if message.contains("unknown argument")
        ));
    }

    #[test]
    fn synchronous_mode_is_explicit_and_rejects_unmatched_values() {
        let defaults = parse_args_from(std::iter::empty::<&str>())
            .expect("default benchmark arguments must parse");
        assert_eq!(defaults.synchronous, SynchronousMode::Normal);

        let full = parse_args_from(["--synchronous=full"])
            .expect("matched FULL durability must be selectable");
        assert_eq!(full.synchronous, SynchronousMode::Full);

        assert!(matches!(
            parse_args_from(["--synchronous=extra"]),
            Err(ParseArgsError::Message(message)) if message.contains("expected normal or full")
        ));
    }

    #[test]
    fn one_row_transaction_flag_is_explicit_and_uses_isolated_history() {
        let defaults = parse_args_from(std::iter::empty::<&str>())
            .expect("default benchmark arguments must parse");
        assert_eq!(
            defaults.transaction_granularity,
            TransactionGranularity::Bulk
        );
        assert_eq!(defaults.history_json, Path::new(DEFAULT_HISTORY_JSON));

        let one_row = parse_args_from(["--one-row-per-transaction"])
            .expect("one-row transaction flag must parse");
        assert_eq!(
            one_row.transaction_granularity,
            TransactionGranularity::OneRow
        );
        assert_eq!(
            one_row.history_json,
            Path::new(DEFAULT_ONE_ROW_HISTORY_JSON)
        );

        let separate = parse_args_from(["--separate-tables", "--one-row-per-transaction"])
            .expect("separate-table one-row mode must parse");
        assert!(separate.separate_tables);
        assert_eq!(
            separate.history_json,
            Path::new(DEFAULT_SEPARATE_TABLES_ONE_ROW_HISTORY_JSON)
        );

        let explicit_history = parse_args_from([
            "--one-row-per-transaction",
            "--history-json=/tmp/explicit-history.json",
        ])
        .expect("one-row mode must preserve an explicit history path");
        assert_eq!(
            explicit_history.history_json,
            Path::new("/tmp/explicit-history.json")
        );

        let explicit_default_history = parse_args_from([
            "--one-row-per-transaction",
            "--history-json=.bench-history/mt-mvcc-bench.latest.json",
        ])
        .expect("an explicit default-valued history path must remain explicit");
        assert_eq!(
            explicit_default_history.history_json,
            Path::new(DEFAULT_HISTORY_JSON)
        );
    }

    #[test]
    fn one_row_retry_policy_reports_engine_specific_retry_truth() {
        let policy = retry_policy_receipt_for_granularity(
            Duration::from_secs(7),
            false,
            TransactionGranularity::OneRow,
        )
        .expect("one-row retry timeout must fit the report");
        assert_eq!(policy.csqlite_max_operation_retries, 0);
        assert_eq!(policy.csqlite_max_transaction_retries, Some(MAX_RETRIES));
        assert_eq!(policy.shared_worker_retry_timeout_ms, Some(7_000));
        assert_eq!(policy.shared_worker_retry_timeout_overridden, Some(false));
        assert_eq!(policy.csqlite_retry_unit, CSQLITE_ONE_ROW_RETRY_UNIT);
        assert_eq!(
            policy.csqlite_retry_algorithm,
            CSQLITE_ONE_ROW_RETRY_ALGORITHM
        );
        assert_eq!(policy.fsqlite_retry_unit, FSQLITE_ONE_ROW_RETRY_UNIT);
        assert_eq!(
            policy.fsqlite_retry_backoff_algorithm,
            FSQLITE_ONE_ROW_RETRY_BACKOFF_ALGORITHM
        );

        let bulk = retry_policy_receipt(Duration::from_secs(7), false)
            .expect("bulk retry timeout must fit the report");
        assert_eq!(bulk.csqlite_max_operation_retries, MAX_RETRIES);
        assert_eq!(bulk.csqlite_max_transaction_retries, None);
        assert_eq!(bulk.shared_worker_retry_timeout_ms, None);
        assert_eq!(bulk.shared_worker_retry_timeout_overridden, None);
        assert_eq!(bulk.csqlite_retry_algorithm, CSQLITE_RETRY_ALGORITHM);
    }

    fn sample_result(elapsed_ms: u64, total_rows: usize, failed_rows: usize) -> RunResult {
        let committed_rows = total_rows - failed_rows;
        RunResult {
            worker_startup_elapsed: Duration::from_millis(1),
            workload_elapsed: Duration::from_millis(elapsed_ms),
            settings: expected_effective_settings("test_engine", DEFAULT_WAL_AUTOCHECKPOINT_PAGES),
            accounting: WorkAccounting {
                offered_writes: total_rows,
                attempted_writes: total_rows,
                succeeded_writes: committed_rows,
                retried_operations: 0,
                failed_writes: failed_rows,
                worker_reported_failed_writes: failed_rows,
                exact: true,
                diagnostics: Vec::new(),
            },
            committed_state: CommittedStateOracle {
                expected_rows: total_rows,
                observed_rows: committed_rows,
                expected_id_sum: 0,
                observed_id_sum: 0,
                expected_payload_sha256: "expected".to_owned(),
                observed_payload_sha256: if failed_rows == 0 {
                    "expected".to_owned()
                } else {
                    "incomplete".to_owned()
                },
                integrity_check: vec!["ok".to_owned()],
                valid: failed_rows == 0,
                diagnostics: Vec::new(),
            },
        }
    }

    fn default_retry_policy(writers: usize, rows_per_thread: usize) -> RetryPolicyReceipt {
        retry_policy_receipt(fsqlite_retry_timeout(writers, rows_per_thread), false)
            .expect("test retry policy must fit")
    }

    fn supported_configuration(writers: usize) -> ConfigurationReceipt {
        configuration_receipt(
            writers,
            DEFAULT_ROWS_PER_THREAD,
            Some(writers),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            default_retry_policy(writers, DEFAULT_ROWS_PER_THREAD),
        )
    }

    fn engine_sample_result(
        elapsed_ms: u64,
        total_rows: usize,
        concurrent_mode: &str,
        wal_autocheckpoint_pages: i64,
    ) -> RunResult {
        let mut sample = sample_result(elapsed_ms, total_rows, 0);
        sample.settings = expected_effective_settings(concurrent_mode, wal_autocheckpoint_pages);
        sample
    }

    fn valid_history_row(
        threads: usize,
        sqlite_elapsed_ms: u64,
        fsqlite_elapsed_ms: u64,
        wal_autocheckpoint_pages: i64,
    ) -> ThreadComparisonReport {
        valid_history_row_with_iterations(
            threads,
            sqlite_elapsed_ms,
            fsqlite_elapsed_ms,
            wal_autocheckpoint_pages,
            1,
        )
    }

    fn valid_history_row_with_iterations(
        threads: usize,
        sqlite_elapsed_ms: u64,
        fsqlite_elapsed_ms: u64,
        wal_autocheckpoint_pages: i64,
        iterations: usize,
    ) -> ThreadComparisonReport {
        let offered_writes = threads
            .checked_mul(DEFAULT_ROWS_PER_THREAD)
            .expect("test offered work must fit");
        let sqlite = engine_sample_result(
            sqlite_elapsed_ms,
            offered_writes,
            "sqlite_wal_single_writer",
            wal_autocheckpoint_pages,
        );
        let fsqlite = engine_sample_result(
            fsqlite_elapsed_ms,
            offered_writes,
            "fsqlite_mvcc_on",
            wal_autocheckpoint_pages,
        );
        let null = paired_run_stats(
            vec![sqlite.clone(); iterations],
            vec![sqlite.clone(); iterations],
        );
        let claim = paired_run_stats(vec![sqlite; iterations], vec![fsqlite; iterations]);
        let configuration = configuration_receipt(
            threads,
            DEFAULT_ROWS_PER_THREAD,
            Some(threads),
            wal_autocheckpoint_pages,
            false,
            default_retry_policy(threads, DEFAULT_ROWS_PER_THREAD),
        );
        build_thread_report(threads, &null, &claim, &configuration, &[])
    }

    fn history_with_rows(
        rows_per_thread: usize,
        thread_results: Vec<ThreadComparisonReport>,
    ) -> HistoricalMtMvccBenchReport {
        history_with_rows_and_iterations(rows_per_thread, 1, thread_results)
    }

    fn history_with_rows_and_iterations(
        rows_per_thread: usize,
        iterations: usize,
        thread_results: Vec<ThreadComparisonReport>,
    ) -> HistoricalMtMvccBenchReport {
        let configuration_receipts = thread_results
            .iter()
            .filter_map(|row| row.truth.as_ref().map(|truth| truth.configuration.clone()))
            .collect();
        HistoricalMtMvccBenchReport {
            schema_version: Some(REPORT_SCHEMA_V7.to_owned()),
            citable: Some(true),
            measurement_evidence_valid: Some(true),
            subject_identity: Some(serde_json::json!({"fixture": "verified subject"})),
            comparison_environment: Some(serde_json::json!({"fixture": "verified environment"})),
            settings_interpretation: Some(SETTINGS_INTERPRETATION.to_owned()),
            accounting_interpretation: Some(ACCOUNTING_INTERPRETATION.to_owned()),
            timing_interpretation: Some(TIMING_INTERPRETATION.to_owned()),
            workload_shape: Some("shared_table".to_owned()),
            rows_per_thread: Some(rows_per_thread),
            iterations: Some(iterations),
            configuration_receipts: Some(configuration_receipts),
            thread_results,
        }
    }

    fn configuration_receipts_from_rows(
        rows: &[ThreadComparisonReport],
    ) -> Vec<ConfigurationReceipt> {
        rows.iter()
            .filter_map(|row| row.truth.as_ref().map(|truth| truth.configuration.clone()))
            .collect()
    }

    fn gate_from_serialized_history(
        history: serde_json::Value,
        current_rows: &[ThreadComparisonReport],
        current_iterations: usize,
    ) -> PassOverPassGateReport {
        let previous = serde_json::from_value::<HistoricalMtMvccBenchReport>(history)
            .expect("test historical report must deserialize");
        let current_configuration_receipts = configuration_receipts_from_rows(current_rows);
        build_pass_over_pass_gate(PassOverPassGateInput {
            history_json: Path::new(DEFAULT_HISTORY_JSON),
            previous: Some(&previous),
            historical_baseline_authentication:
                HistoricalBaselineAuthentication::VerifiedTestFixture,
            current_rows,
            current_configuration_receipts: &current_configuration_receipts,
            current_workload_shape: "shared_table",
            current_rows_per_thread: DEFAULT_ROWS_PER_THREAD,
            current_iterations,
            current_transaction_granularity: TransactionGranularity::Bulk,
            current_wal_autocheckpoint_overridden: false,
            current_retry_timeout_overridden: false,
        })
    }

    fn gate_with_rows(
        previous_rows: Vec<ThreadComparisonReport>,
        current_rows: Vec<ThreadComparisonReport>,
    ) -> PassOverPassGateReport {
        let previous = history_with_rows(1_000, previous_rows);
        let current_configuration_receipts = configuration_receipts_from_rows(&current_rows);
        build_pass_over_pass_gate(PassOverPassGateInput {
            history_json: Path::new(DEFAULT_HISTORY_JSON),
            previous: Some(&previous),
            historical_baseline_authentication:
                HistoricalBaselineAuthentication::VerifiedTestFixture,
            current_rows: &current_rows,
            current_configuration_receipts: &current_configuration_receipts,
            current_workload_shape: "shared_table",
            current_rows_per_thread: 1_000,
            current_iterations: 1,
            current_transaction_granularity: TransactionGranularity::Bulk,
            current_wal_autocheckpoint_overridden: false,
            current_retry_timeout_overridden: false,
        })
    }

    #[test]
    fn thread_report_computes_expected_ratios() {
        let null = paired_run_stats(
            vec![sample_result(100, 1000, 0)],
            vec![sample_result(100, 1000, 0)],
        );
        let claim = paired_run_stats(
            vec![sample_result(100, 1000, 1)],
            vec![sample_result(200, 1000, 3)],
        );

        let report = build_thread_report(4, &null, &claim, &supported_configuration(4), &[]);

        assert_eq!(report.threads, 4);
        assert!((report.fsqlite_wps_p50 - 4985.0).abs() < 0.01);
        assert!((report.sqlite_wps_p50 - 9990.0).abs() < 0.01);
        assert!((report.throughput_ratio - (4985.0 / 9990.0)).abs() < 0.0001);
        assert!((report.time_ratio - 2.0).abs() < 0.0001);
        assert_eq!(report.fsqlite_failed_rows, 3);
        assert_eq!(report.sqlite_failed_rows, 1);
    }

    #[test]
    fn markdown_summary_renders_thread_rows() {
        let report = MtMvccBenchReport {
            schema_version: REPORT_SCHEMA_V7,
            citable: false,
            measurement_evidence_valid: true,
            non_citable_reason: NON_CITABLE_REASON,
            release_regression_scope: RELEASE_REGRESSION_SCOPE,
            subject_identity: test_subject_identity(),
            comparison_environment: test_comparison_environment(),
            settings_interpretation: SETTINGS_INTERPRETATION,
            accounting_interpretation: ACCOUNTING_INTERPRETATION,
            timing_interpretation: TIMING_INTERPRETATION,
            workload_shape: "shared_table",
            transaction_contract: None,
            rows_per_thread: 250,
            iterations: 1,
            configuration_receipts: vec![supported_configuration(8)],
            thread_results: vec![ThreadComparisonReport {
                threads: 8,
                fsqlite_wps_p50: 6090.0,
                fsqlite_wps_p95: 6090.0,
                fsqlite_wps_p99: 6090.0,
                sqlite_wps_p50: 55_406.0,
                sqlite_wps_p95: 55_406.0,
                sqlite_wps_p99: 55_406.0,
                throughput_ratio: 0.11,
                fsqlite_ms_p50: 328.39,
                fsqlite_ms_p95: 328.39,
                fsqlite_ms_p99: 328.39,
                sqlite_ms_p50: 36.10,
                sqlite_ms_p95: 36.10,
                sqlite_ms_p99: 36.10,
                time_ratio: 9.10,
                fsqlite_failed_rows: 0,
                sqlite_failed_rows: 0,
                median_ci_contract: None,
                truth: None,
            }],
            pass_over_pass_gate: PassOverPassGateReport {
                schema_version: PASS_OVER_PASS_SCHEMA_V1,
                history_json_path: DEFAULT_HISTORY_JSON.to_owned(),
                threshold_ratio_drop_pct: PASS_OVER_PASS_MAX_RATIO_DROP_PCT,
                status: "disabled_non_citable",
                previous_report_found: false,
                comparable_pair_count: 0,
                regressions: Vec::new(),
            },
        };

        let rendered = render_markdown_summary(&report);

        assert!(rendered.contains("# mt-mvcc-bench Summary"));
        assert!(rendered.contains("- Workload shape: `shared_table`"));
        assert!(rendered.contains("| 8 | unavailable | 6090 | 55406 | 0.110x | unavailable |"));
        assert!(rendered.contains("Pass-over-pass gate"));
        assert!(rendered.contains("comparable pairs `0`"));
    }

    #[test]
    fn separate_tables_schema_uses_one_table_per_worker() {
        let sql = create_tables_sql(3, true);

        assert!(sql.contains(
            "CREATE TABLE IF NOT EXISTS bench_0 (id INTEGER PRIMARY KEY, payload TEXT);"
        ));
        assert!(sql.contains(
            "CREATE TABLE IF NOT EXISTS bench_1 (id INTEGER PRIMARY KEY, payload TEXT);"
        ));
        assert!(sql.contains(
            "CREATE TABLE IF NOT EXISTS bench_2 (id INTEGER PRIMARY KEY, payload TEXT);"
        ));
        assert!(!sql.contains("CREATE TABLE IF NOT EXISTS bench ("));
    }

    #[test]
    fn shared_table_schema_uses_single_bench_table() {
        let sql = create_tables_sql(8, false);

        assert_eq!(sql.matches("CREATE TABLE IF NOT EXISTS bench ").count(), 1);
        assert!(
            sql.contains(
                "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, payload TEXT);"
            )
        );
        assert!(!sql.contains("bench_0"));
    }

    #[test]
    fn worker_insert_sql_matches_workload_shape() {
        assert_eq!(
            worker_insert_sql(7, false),
            "INSERT INTO bench (id, payload) VALUES (?1, ?2)"
        );
        assert_eq!(
            worker_insert_sql(7, true),
            "INSERT INTO bench_7 (id, payload) VALUES (?1, ?2)"
        );
    }

    #[test]
    fn parse_thread_count_rejects_zero_worker_runs() {
        assert_eq!(parse_thread_count("16").unwrap(), 16);
        assert_eq!(parse_thread_count(" 4 ").unwrap(), 4);
        assert!(parse_thread_count("0").is_err());
        assert!(parse_thread_count("not-a-number").is_err());
    }

    #[test]
    fn default_showcase_matrix_keeps_existing_tiers_and_adds_high_writer_tiers() {
        assert_eq!(DEFAULT_THREADS, &[1, 2, 4, 8, 16, 32, 64, 128]);
    }

    #[test]
    fn configuration_receipts_distinguish_supported_oversubscribed_and_unsupported() {
        let zero = configuration_receipt(
            0,
            DEFAULT_ROWS_PER_THREAD,
            Some(64),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            default_retry_policy(0, DEFAULT_ROWS_PER_THREAD),
        );
        assert_eq!(zero.status, "unsupported");
        assert!(!zero.comparison_eligible);
        assert!(!zero.measured);

        let supported = configuration_receipt(
            32,
            DEFAULT_ROWS_PER_THREAD,
            Some(64),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            default_retry_policy(32, DEFAULT_ROWS_PER_THREAD),
        );
        assert_eq!(supported.status, "supported");
        assert!(supported.comparison_eligible);
        assert!(supported.measured);
        assert_eq!(
            supported.wal_autocheckpoint_pages,
            Some(DEFAULT_WAL_AUTOCHECKPOINT_PAGES)
        );
        assert_eq!(supported.wal_autocheckpoint_overridden, Some(false));
        assert_eq!(
            supported.offered_writes_per_sample,
            Some(32 * DEFAULT_ROWS_PER_THREAD)
        );
        assert_eq!(
            supported
                .retry_policy
                .as_ref()
                .expect("retry policy must be reported"),
            &default_retry_policy(32, DEFAULT_ROWS_PER_THREAD)
        );
        let policy = supported
            .retry_policy
            .as_ref()
            .expect("retry policy must be reported");
        assert_eq!(policy.csqlite_retry_algorithm, CSQLITE_RETRY_ALGORITHM);
        assert_eq!(
            policy.fsqlite_retry_backoff_algorithm,
            FSQLITE_RETRY_BACKOFF_ALGORITHM
        );
        assert_eq!(policy.fsqlite_retryable_errors, FSQLITE_RETRYABLE_ERRORS);

        let oversubscribed = configuration_receipt(
            64,
            DEFAULT_ROWS_PER_THREAD,
            Some(32),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            default_retry_policy(64, DEFAULT_ROWS_PER_THREAD),
        );
        assert_eq!(oversubscribed.status, "oversubscribed");
        assert!(!oversubscribed.comparison_eligible);
        assert!(oversubscribed.measured);

        let unsupported = configuration_receipt(
            fsqlite_mvcc::MAX_CONCURRENT_WRITERS + 1,
            DEFAULT_ROWS_PER_THREAD,
            Some(256),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            default_retry_policy(
                fsqlite_mvcc::MAX_CONCURRENT_WRITERS + 1,
                DEFAULT_ROWS_PER_THREAD,
            ),
        );
        assert_eq!(unsupported.status, "unsupported");
        assert!(!unsupported.comparison_eligible);
        assert!(!unsupported.measured);

        let diagnostic_override = configuration_receipt(
            32,
            DEFAULT_ROWS_PER_THREAD,
            Some(64),
            0,
            true,
            default_retry_policy(32, DEFAULT_ROWS_PER_THREAD),
        );
        assert_eq!(diagnostic_override.status, "diagnostic_override");
        assert!(!diagnostic_override.comparison_eligible);
        assert!(diagnostic_override.measured);
        assert_eq!(diagnostic_override.wal_autocheckpoint_pages, Some(0));
        assert_eq!(
            diagnostic_override.wal_autocheckpoint_overridden,
            Some(true)
        );
        assert!(diagnostic_override.reason.contains("cannot update"));

        let retry_override = configuration_receipt(
            32,
            DEFAULT_ROWS_PER_THREAD,
            Some(64),
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            false,
            retry_policy_receipt(Duration::from_secs(60), true)
                .expect("test retry override must fit"),
        );
        assert_eq!(retry_override.status, "diagnostic_override");
        assert!(!retry_override.comparison_eligible);
        assert!(
            retry_override
                .reason
                .contains("FrankenSQLite-only retry-timeout override")
        );
    }

    #[test]
    fn shared_table_workload_bounds_preserve_disjoint_row_ids() {
        let stride = usize::try_from(ROWID_BASE_STRIDE).unwrap();

        validate_workload_bounds(stride, false).unwrap();
        assert!(validate_workload_bounds(stride + 1, false).is_err());
        validate_workload_bounds(stride + 1, true).unwrap();
    }

    #[test]
    fn work_accounting_uses_database_commits_as_successes() {
        let accounting = build_work_accounting(100, 125, 2, 3, 97);

        assert_eq!(accounting.offered_writes, 100);
        assert_eq!(accounting.attempted_writes, 125);
        assert_eq!(accounting.succeeded_writes, 97);
        assert_eq!(accounting.retried_operations, 2);
        assert_eq!(accounting.failed_writes, 3);
        assert!(accounting.exact);
    }

    #[test]
    fn throughput_uses_only_workload_time_not_worker_startup() {
        let mut result = sample_result(10, 1_000, 0);
        result.worker_startup_elapsed = Duration::from_secs(10);

        assert!((result.writes_per_sec() - 100_000.0).abs() < f64::EPSILON);
        let evidence = result.sample_evidence();
        assert_eq!(
            evidence.worker_startup_elapsed_ns,
            Duration::from_secs(10).as_nanos()
        );
        assert_eq!(
            evidence.workload_elapsed_ns,
            Duration::from_millis(10).as_nanos()
        );
    }

    #[test]
    fn startup_abort_wakes_workers_that_have_not_yet_parked() {
        let gate = Arc::new((Mutex::new(StartupGateState::default()), Condvar::new()));
        let worker_gate = Arc::clone(&gate);
        let handle = thread::spawn(move || {
            let (lock, condvar) = &*worker_gate;
            let mut state = lock.lock().expect("test gate lock");
            while !state.release && !state.abort {
                state = condvar.wait(state).expect("test gate wait");
            }
            state.abort
        });

        publish_startup_decision(&gate, false);
        assert!(
            handle.join().expect("test worker must join"),
            "abort publication must be sticky even when it wins before the worker waits"
        );
    }

    #[test]
    fn worker_join_aggregates_every_terminal_error_before_returning() {
        let settings = expected_effective_settings("test_engine", DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let success_counter = Arc::clone(&completed);
        let final_error_counter = Arc::clone(&completed);
        let success_settings = settings.clone();
        let handles = vec![
            thread::spawn(|| Err("first".to_owned())),
            thread::spawn(move || {
                success_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(WorkerWork {
                    settings: success_settings,
                    attempted_writes: 1,
                    retried_operations: 0,
                    reported_failed_writes: 0,
                    workload_finished: Instant::now(),
                })
            }),
            thread::spawn(move || {
                final_error_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err("last".to_owned())
            }),
        ];

        let error = join_worker_handles("test", handles, &settings)
            .expect_err("terminal worker errors must fail the aggregate");
        assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(error.contains("worker t0 failed: first"));
        assert!(error.contains("worker t2 failed: last"));
    }

    #[test]
    fn pass_over_pass_gate_flags_ratio_drop_over_five_percent() {
        let gate = gate_with_rows(
            vec![valid_history_row(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
            vec![valid_history_row(
                8,
                90,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        );

        assert_eq!(gate.status, "failed");
        assert_eq!(gate.comparable_pair_count, 1);
        assert_eq!(gate.regressions.len(), 1);
        assert_eq!(gate.regressions[0].threads, 8);
        assert!((gate.regressions[0].ratio_drop_pct - 10.0).abs() < 1.0e-6);
    }

    #[test]
    fn pass_over_pass_gate_passes_only_with_a_valid_comparable_pair() {
        let gate = gate_with_rows(
            vec![valid_history_row(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
            vec![valid_history_row(
                8,
                110,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        );

        assert_eq!(gate.status, "passed");
        assert_eq!(gate.comparable_pair_count, 1);
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn pass_over_pass_gate_skips_without_prior_report() {
        let gate = build_pass_over_pass_gate(PassOverPassGateInput {
            history_json: Path::new(DEFAULT_HISTORY_JSON),
            previous: None,
            historical_baseline_authentication:
                HistoricalBaselineAuthentication::VerifiedTestFixture,
            current_rows: &[],
            current_configuration_receipts: &[],
            current_workload_shape: "shared_table",
            current_rows_per_thread: 1000,
            current_iterations: 1,
            current_transaction_granularity: TransactionGranularity::Bulk,
            current_wal_autocheckpoint_overridden: false,
            current_retry_timeout_overridden: false,
        });

        assert_eq!(gate.status, "no_prior_report");
        assert_eq!(gate.comparable_pair_count, 0);
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn pass_over_pass_gate_skips_incompatible_history_shape() {
        let previous = history_with_rows(
            100,
            vec![valid_history_row(
                16,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        );
        let current = vec![valid_history_row(
            16,
            90,
            200,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
        )];
        let current_configuration_receipts = vec![
            current[0]
                .truth
                .as_ref()
                .expect("test current row truth")
                .configuration
                .clone(),
        ];

        let gate = build_pass_over_pass_gate(PassOverPassGateInput {
            history_json: Path::new(DEFAULT_HISTORY_JSON),
            previous: Some(&previous),
            historical_baseline_authentication:
                HistoricalBaselineAuthentication::VerifiedTestFixture,
            current_rows: &current,
            current_configuration_receipts: &current_configuration_receipts,
            current_workload_shape: "shared_table",
            current_rows_per_thread: 1000,
            current_iterations: 1,
            current_transaction_granularity: TransactionGranularity::Bulk,
            current_wal_autocheckpoint_overridden: false,
            current_retry_timeout_overridden: false,
        });

        assert_eq!(gate.status, "no_prior_report");
        assert_eq!(gate.comparable_pair_count, 0);
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn active_v7_history_is_disabled_even_when_json_claims_citable() {
        let current_rows = vec![valid_history_row(
            8,
            90,
            200,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
        )];
        let forged_json = serde_json::to_value(history_with_rows(
            DEFAULT_ROWS_PER_THREAD,
            vec![valid_history_row(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        ))
        .expect("test history must serialize");
        assert_eq!(forged_json["citable"], true);
        let previous = serde_json::from_value::<HistoricalMtMvccBenchReport>(forged_json)
            .expect("test history must deserialize");
        let current_configuration_receipts = configuration_receipts_from_rows(&current_rows);

        let gate = build_pass_over_pass_gate(PassOverPassGateInput {
            history_json: Path::new(DEFAULT_HISTORY_JSON),
            previous: Some(&previous),
            historical_baseline_authentication: HistoricalBaselineAuthentication::Unavailable,
            current_rows: &current_rows,
            current_configuration_receipts: &current_configuration_receipts,
            current_workload_shape: "shared_table",
            current_rows_per_thread: DEFAULT_ROWS_PER_THREAD,
            current_iterations: 1,
            current_transaction_granularity: TransactionGranularity::Bulk,
            current_wal_autocheckpoint_overridden: false,
            current_retry_timeout_overridden: false,
        });

        assert_eq!(gate.status, "disabled_non_citable");
        assert!(gate.previous_report_found);
        assert_eq!(gate.comparable_pair_count, 0);
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn loaded_history_requires_exact_top_level_v7_contract() {
        let previous_row = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let current_rows = vec![valid_history_row(
            8,
            100,
            200,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
        )];
        let base = serde_json::to_value(history_with_rows(
            DEFAULT_ROWS_PER_THREAD,
            vec![previous_row],
        ))
        .expect("test history must serialize");

        let valid_gate = gate_from_serialized_history(base.clone(), &current_rows, 1);
        assert_eq!(valid_gate.status, "passed");

        for field in [
            "settings_interpretation",
            "accounting_interpretation",
            "timing_interpretation",
        ] {
            let mut mutated = base.clone();
            mutated[field] = serde_json::json!("mutated contract");
            let gate = gate_from_serialized_history(mutated, &current_rows, 1);
            assert_eq!(gate.status, "no_prior_report", "field {field}");
        }

        for field in [
            "citable",
            "measurement_evidence_valid",
            "subject_identity",
            "comparison_environment",
            "iterations",
            "configuration_receipts",
            "settings_interpretation",
            "accounting_interpretation",
            "timing_interpretation",
        ] {
            let mut missing = base.clone();
            missing
                .as_object_mut()
                .expect("test history must be an object")
                .remove(field);
            let gate = gate_from_serialized_history(missing, &current_rows, 1);
            assert_eq!(gate.status, "no_prior_report", "missing field {field}");
        }

        for field in ["citable", "measurement_evidence_valid"] {
            let mut false_claim = base.clone();
            false_claim[field] = serde_json::json!(false);
            let gate = gate_from_serialized_history(false_claim, &current_rows, 1);
            assert_eq!(gate.status, "no_prior_report", "false field {field}");
        }

        let mut wrong_iterations = base.clone();
        wrong_iterations["iterations"] = serde_json::json!(2);
        assert_eq!(
            gate_from_serialized_history(wrong_iterations, &current_rows, 1).status,
            "no_prior_report"
        );

        let mut wrong_receipt = base;
        wrong_receipt["configuration_receipts"][0]["writers"] = serde_json::json!(4);
        assert_eq!(
            gate_from_serialized_history(wrong_receipt, &current_rows, 1).status,
            "no_prior_report"
        );
    }

    #[test]
    fn loaded_history_requires_exact_iteration_cardinality_and_retry_identity() {
        let current_rows = vec![valid_history_row_with_iterations(
            8,
            100,
            200,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            2,
        )];
        let one_sample_history = history_with_rows_and_iterations(
            DEFAULT_ROWS_PER_THREAD,
            2,
            vec![valid_history_row(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        );
        let one_sample_json =
            serde_json::to_value(one_sample_history).expect("test history must serialize");
        assert_eq!(
            gate_from_serialized_history(one_sample_json, &current_rows, 2).status,
            "no_prior_report"
        );

        let two_sample_history = history_with_rows_and_iterations(
            DEFAULT_ROWS_PER_THREAD,
            2,
            vec![valid_history_row_with_iterations(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
                2,
            )],
        );
        let base = serde_json::to_value(two_sample_history).expect("test history must serialize");
        assert_eq!(
            gate_from_serialized_history(base.clone(), &current_rows, 2).status,
            "passed"
        );

        for field in [
            "csqlite_retry_algorithm",
            "fsqlite_retry_backoff_algorithm",
            "fsqlite_retryable_errors",
        ] {
            let mut missing = base.clone();
            missing["configuration_receipts"][0]["retry_policy"]
                .as_object_mut()
                .expect("test retry policy must be an object")
                .remove(field);
            let gate = gate_from_serialized_history(missing, &current_rows, 2);
            assert_eq!(gate.status, "no_prior_report", "missing field {field}");
        }
    }

    #[test]
    fn pass_over_pass_gate_rejects_inconsistent_current_top_level_receipt() {
        let previous = history_with_rows(
            DEFAULT_ROWS_PER_THREAD,
            vec![valid_history_row(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        );
        let current_rows = vec![valid_history_row(
            8,
            90,
            200,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
        )];
        let mut current_receipts = configuration_receipts_from_rows(&current_rows);
        current_receipts[0].offered_writes_per_sample = Some(1);

        let gate = build_pass_over_pass_gate(PassOverPassGateInput {
            history_json: Path::new(DEFAULT_HISTORY_JSON),
            previous: Some(&previous),
            historical_baseline_authentication:
                HistoricalBaselineAuthentication::VerifiedTestFixture,
            current_rows: &current_rows,
            current_configuration_receipts: &current_receipts,
            current_workload_shape: "shared_table",
            current_rows_per_thread: DEFAULT_ROWS_PER_THREAD,
            current_iterations: 1,
            current_transaction_granularity: TransactionGranularity::Bulk,
            current_wal_autocheckpoint_overridden: false,
            current_retry_timeout_overridden: false,
        });

        assert!(gate.previous_report_found);
        assert_eq!(gate.status, "no_comparable_rows");
        assert_eq!(gate.comparable_pair_count, 0);
    }

    #[test]
    fn pass_over_pass_gate_rejects_missing_or_invalid_evidence_on_either_side() {
        let valid_previous = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let valid_current = valid_history_row(8, 90, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);

        let mut previous_missing_truth = valid_previous.clone();
        previous_missing_truth.truth = None;
        let mut current_missing_truth = valid_current.clone();
        current_missing_truth.truth = None;
        let mut previous_missing_contract = valid_previous.clone();
        previous_missing_contract.median_ci_contract = None;
        let mut current_invalid_contract = valid_current.clone();
        current_invalid_contract
            .median_ci_contract
            .as_mut()
            .unwrap()
            .verdict = "INVALID_FAILED_ROWS".to_owned();
        let mut previous_invalid_truth = valid_previous.clone();
        previous_invalid_truth
            .truth
            .as_mut()
            .unwrap()
            .sqlite_samples[0]
            .committed_state
            .valid = false;
        let mut current_invalid_truth = valid_current.clone();
        current_invalid_truth
            .truth
            .as_mut()
            .unwrap()
            .fsqlite_samples[0]
            .accounting
            .exact = false;

        for gate in [
            gate_with_rows(vec![previous_missing_truth], vec![valid_current.clone()]),
            gate_with_rows(vec![previous_missing_contract], vec![valid_current.clone()]),
            gate_with_rows(vec![previous_invalid_truth], vec![valid_current.clone()]),
        ] {
            assert_eq!(gate.status, "no_prior_report");
            assert_eq!(gate.comparable_pair_count, 0);
            assert!(gate.regressions.is_empty());
        }

        for gate in [
            gate_with_rows(vec![valid_previous.clone()], vec![current_missing_truth]),
            gate_with_rows(vec![valid_previous.clone()], vec![current_invalid_contract]),
            gate_with_rows(vec![valid_previous.clone()], vec![current_invalid_truth]),
        ] {
            assert_eq!(gate.status, "no_comparable_rows");
            assert_eq!(gate.comparable_pair_count, 0);
            assert!(gate.regressions.is_empty());
        }
    }

    #[test]
    fn pass_over_pass_gate_rejects_unsupported_and_nonoverlapping_rows() {
        let valid_previous = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let valid_current = valid_history_row(8, 90, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let mut unsupported_previous = valid_previous.clone();
        let unsupported_configuration =
            &mut unsupported_previous.truth.as_mut().unwrap().configuration;
        unsupported_configuration.status = "unsupported".to_owned();
        unsupported_configuration.comparison_eligible = false;
        unsupported_configuration.measured = false;
        let mut unsupported_current = valid_current.clone();
        let unsupported_configuration =
            &mut unsupported_current.truth.as_mut().unwrap().configuration;
        unsupported_configuration.status = "unsupported".to_owned();
        unsupported_configuration.comparison_eligible = false;
        unsupported_configuration.measured = false;

        let unsupported_gate =
            gate_with_rows(vec![unsupported_previous], vec![valid_current.clone()]);
        assert_eq!(unsupported_gate.status, "no_prior_report");
        assert_eq!(unsupported_gate.comparable_pair_count, 0);
        let unsupported_current_gate =
            gate_with_rows(vec![valid_previous.clone()], vec![unsupported_current]);
        assert_eq!(unsupported_current_gate.status, "no_comparable_rows");
        assert_eq!(unsupported_current_gate.comparable_pair_count, 0);

        let nonoverlap_gate = gate_with_rows(
            vec![valid_previous],
            vec![valid_history_row(
                16,
                90,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
        );
        assert_eq!(nonoverlap_gate.status, "no_comparable_rows");
        assert_eq!(nonoverlap_gate.comparable_pair_count, 0);
    }

    #[test]
    fn pass_over_pass_gate_rejects_duplicate_eligible_threads_on_either_side() {
        let valid_previous = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let valid_current = valid_history_row(8, 90, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);

        let duplicate_previous_gate = gate_with_rows(
            vec![valid_previous.clone(), valid_previous.clone()],
            vec![valid_current.clone()],
        );
        assert_eq!(duplicate_previous_gate.status, "no_prior_report");
        assert_eq!(duplicate_previous_gate.comparable_pair_count, 0);

        let duplicate_current_gate = gate_with_rows(
            vec![valid_previous],
            vec![valid_current.clone(), valid_current],
        );
        assert_eq!(duplicate_current_gate.status, "no_comparable_rows");
        assert_eq!(duplicate_current_gate.comparable_pair_count, 0);
    }

    #[test]
    fn pass_over_pass_gate_requires_exact_effective_settings_fingerprint() {
        let gate = gate_with_rows(
            vec![valid_history_row(
                8,
                100,
                200,
                DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            )],
            vec![valid_history_row(8, 90, 200, 0)],
        );

        assert_eq!(gate.status, "no_comparable_rows");
        assert_eq!(gate.comparable_pair_count, 0);
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn pass_over_pass_gate_requires_exact_retry_policy_fingerprint() {
        let mut previous = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        previous
            .truth
            .as_mut()
            .expect("test truth")
            .configuration
            .retry_policy
            .as_mut()
            .expect("test retry policy")
            .fsqlite_transaction_timeout_ms += 1;
        let current = valid_history_row(8, 90, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);

        let gate = gate_with_rows(vec![previous], vec![current]);

        assert_eq!(gate.status, "no_prior_report");
        assert_eq!(gate.comparable_pair_count, 0);
    }

    #[test]
    fn pass_over_pass_gate_proves_offered_work_from_writers_times_rows() {
        let mut previous = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let truth = previous.truth.as_mut().expect("test truth");
        let malformed_offered = 7_777;
        truth.configuration.offered_writes_per_sample = Some(malformed_offered);
        for sample in truth
            .null_c_a_samples
            .iter_mut()
            .chain(&mut truth.null_c_b_samples)
            .chain(&mut truth.sqlite_samples)
            .chain(&mut truth.fsqlite_samples)
        {
            sample.accounting.offered_writes = malformed_offered;
            sample.accounting.attempted_writes = malformed_offered;
            sample.accounting.succeeded_writes = malformed_offered;
            sample.committed_state.expected_rows = malformed_offered;
            sample.committed_state.observed_rows = malformed_offered;
        }
        let current = valid_history_row(8, 90, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);

        let gate = gate_with_rows(vec![previous], vec![current]);

        assert_eq!(gate.status, "no_prior_report");
        assert_eq!(gate.comparable_pair_count, 0);
    }

    #[test]
    fn pass_over_pass_gate_rejects_missing_or_overridden_cadence_provenance() {
        let valid_current = valid_history_row(8, 90, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let mut missing_cadence = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let missing_configuration = &mut missing_cadence.truth.as_mut().unwrap().configuration;
        missing_configuration.wal_autocheckpoint_pages = None;
        missing_configuration.wal_autocheckpoint_overridden = None;

        let missing_gate = gate_with_rows(vec![missing_cadence], vec![valid_current.clone()]);
        assert_eq!(missing_gate.status, "no_prior_report");
        assert_eq!(missing_gate.comparable_pair_count, 0);

        let mut overridden = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let overridden_configuration = &mut overridden.truth.as_mut().unwrap().configuration;
        overridden_configuration.wal_autocheckpoint_overridden = Some(true);
        overridden_configuration.status = "diagnostic_override".to_owned();
        overridden_configuration.comparison_eligible = false;

        let override_gate = gate_with_rows(vec![overridden], vec![valid_current]);
        assert_eq!(override_gate.status, "no_prior_report");
        assert_eq!(override_gate.comparable_pair_count, 0);
    }

    #[test]
    fn history_write_rejects_override_independent_of_eligibility_flag() {
        let row = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let mut receipt = row.truth.as_ref().unwrap().configuration.clone();

        assert!(!history_evidence_is_invalid(
            false,
            false,
            DEFAULT_ROWS_PER_THREAD,
            1,
            TransactionGranularity::Bulk,
            std::slice::from_ref(&row),
            std::slice::from_ref(&receipt),
        ));
        assert!(receipt.comparison_eligible);
        assert!(history_evidence_is_invalid(
            true,
            false,
            DEFAULT_ROWS_PER_THREAD,
            1,
            TransactionGranularity::Bulk,
            std::slice::from_ref(&row),
            std::slice::from_ref(&receipt),
        ));
        assert!(history_evidence_is_invalid(
            false,
            true,
            DEFAULT_ROWS_PER_THREAD,
            1,
            TransactionGranularity::Bulk,
            std::slice::from_ref(&row),
            std::slice::from_ref(&receipt),
        ));

        receipt.wal_autocheckpoint_overridden = Some(true);
        receipt.comparison_eligible = true;
        assert!(history_evidence_is_invalid(
            false,
            false,
            DEFAULT_ROWS_PER_THREAD,
            1,
            TransactionGranularity::Bulk,
            std::slice::from_ref(&row),
            std::slice::from_ref(&receipt),
        ));
    }

    #[test]
    fn one_row_measurement_validation_requires_the_v10_retry_policy() {
        let mut row = valid_history_row(8, 100, 200, DEFAULT_WAL_AUTOCHECKPOINT_PAGES);
        let one_row_policy = retry_policy_receipt_for_granularity(
            fsqlite_retry_timeout(8, DEFAULT_ROWS_PER_THREAD),
            false,
            TransactionGranularity::OneRow,
        )
        .expect("one-row validation policy must be representable");
        let truth = row.truth.as_mut().expect("test row truth");
        truth.configuration.retry_policy = Some(one_row_policy);
        truth.round_order_receipts = vec![round_order_receipt(0)];
        let receipt = row
            .truth
            .as_ref()
            .expect("test row truth")
            .configuration
            .clone();

        assert!(!history_evidence_is_invalid(
            false,
            false,
            DEFAULT_ROWS_PER_THREAD,
            1,
            TransactionGranularity::OneRow,
            std::slice::from_ref(&row),
            std::slice::from_ref(&receipt),
        ));
        assert!(history_evidence_is_invalid(
            false,
            false,
            DEFAULT_ROWS_PER_THREAD,
            1,
            TransactionGranularity::Bulk,
            std::slice::from_ref(&row),
            std::slice::from_ref(&receipt),
        ));
    }

    #[test]
    fn median_ci_contract_ignores_cv_as_a_gate() {
        let null = paired_run_stats(
            (0..21).map(|_| sample_result(100, 1000, 0)).collect(),
            (0..21).map(|_| sample_result(100, 1000, 0)).collect(),
        );
        let claim = paired_run_stats(
            (0..21).map(|_| sample_result(100, 1000, 0)).collect(),
            (0..21)
                .map(|round| {
                    if round == 0 {
                        sample_result(1, 1000, 0)
                    } else {
                        sample_result(50, 1000, 0)
                    }
                })
                .collect(),
        );

        let contract = median_ci_contract(&null, &claim, &supported_configuration(8));

        assert!(contract.claim_ratio_cv_pct > 5.0);
        assert_eq!(contract.cv_gate, "never");
        assert_eq!(contract.verdict, "FSQLITE_FASTER");
        assert!(contract.claim_ratio_ci95_low > contract.min_decidable_gain);
    }

    #[test]
    fn retry_budget_allows_busy_timeout_scaled_retries() {
        let mut budget = FsqliteRetryBudget::new(fsqlite_retry_timeout(8, 1000));
        let mut waits = Vec::new();
        for _ in 0..MAX_RETRIES {
            waits.push(
                budget
                    .next_wait(0)
                    .expect("budget should allow configured retry count"),
            );
        }

        assert_eq!(budget.attempts(), MAX_RETRIES);
        assert!(budget.next_wait(0).is_none());
        assert!(
            waits
                .iter()
                .all(|wait| wait.as_millis() <= u128::from(MAX_RETRY_SLEEP_MS + 4))
        );
        assert_eq!(waits[0], Duration::from_millis(4));
        assert_eq!(waits[7], Duration::from_millis(6));
        assert_eq!(waits[39], Duration::from_millis(25));
    }

    #[test]
    fn one_row_retry_budgets_share_one_expiring_worker_deadline() {
        let started = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("test instant must support a two-second lookback");
        let deadline = OneRowWorkerRetryDeadline::with_started(started, Duration::from_secs(1));
        let mut first_row = deadline.fsqlite_budget();
        let mut later_row = deadline.fsqlite_budget();

        assert!(first_row.next_wait(0).is_none());
        assert!(later_row.next_wait(0).is_none());
        assert!(!deadline.allows_retry(0));
        assert_eq!(first_row.attempts(), 0);
        assert_eq!(later_row.attempts(), 0);
    }

    #[test]
    fn collect_contract_receipts_bind_the_actual_counterbalanced_execution_order() {
        let observed = std::cell::RefCell::new(Vec::new());
        let (_, _, receipts) = collect_contract(
            3,
            || {
                observed.borrow_mut().push("csqlite_null_a".to_owned());
                Ok(sample_result(1, 1, 0))
            },
            || {
                observed.borrow_mut().push("csqlite_null_b".to_owned());
                Ok(sample_result(1, 1, 0))
            },
            || {
                observed.borrow_mut().push("csqlite_baseline".to_owned());
                Ok(sample_result(1, 1, 0))
            },
            || {
                observed.borrow_mut().push("fsqlite_candidate".to_owned());
                Ok(sample_result(1, 1, 0))
            },
        )
        .expect("counterbalanced contract collection must complete");

        assert!(round_order_receipts_are_valid(&receipts, 3));
        let receipted = receipts
            .iter()
            .flat_map(|receipt| receipt.execution_order.iter().cloned())
            .collect::<Vec<_>>();
        assert_eq!(observed.into_inner(), receipted);
    }

    #[test]
    fn one_row_failure_policy_retries_only_known_transaction_state() {
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Prepare, true, None),
            OneRowFailureDisposition::Retry
        );
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Begin, true, None),
            OneRowFailureDisposition::Retry
        );
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Insert, true, Some(true)),
            OneRowFailureDisposition::Retry
        );
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Commit, true, Some(true)),
            OneRowFailureDisposition::Retry
        );
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Commit, true, Some(false)),
            OneRowFailureDisposition::FailClosed
        );
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Insert, false, Some(true)),
            OneRowFailureDisposition::FailClosed
        );
        assert_eq!(
            one_row_failure_disposition(OneRowRetryStage::Begin, true, Some(true)),
            OneRowFailureDisposition::FailClosed
        );
    }

    #[test]
    fn one_row_prepare_retries_transient_error_and_counts_it() {
        let mut prepare_calls = 0usize;
        let deadline = OneRowWorkerRetryDeadline::new(Duration::from_secs(1));
        let (prepared, retries) = prepare_fsqlite_one_row_with_retry(&(), 0, deadline, |&()| {
            prepare_calls += 1;
            if prepare_calls == 1 {
                Err(fsqlite::FrankenError::BusyRecovery)
            } else {
                Ok("prepared")
            }
        })
        .expect("transient PREPARE failure must recover inside the shared deadline");

        assert_eq!(prepared, "prepared");
        assert_eq!(prepare_calls, 2);
        assert_eq!(retries, 1);
    }

    #[test]
    fn one_row_prepare_retry_rejects_an_expired_shared_deadline() {
        let started = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("test instant supports a two-second lookback");
        let deadline = OneRowWorkerRetryDeadline::with_started(started, Duration::from_secs(1));
        let mut prepare_calls = 0usize;
        let error = prepare_fsqlite_one_row_with_retry(&(), 0, deadline, |&()| {
            prepare_calls += 1;
            Err::<(), _>(fsqlite::FrankenError::BusyRecovery)
        })
        .expect_err("expired shared deadline must reject a retryable PREPARE failure");

        assert_eq!(prepare_calls, 1);
        assert!(error.contains("one-row PREPARE exhausted shared worker retry budget"));
        assert!(error.contains("after 0 retries"));
    }

    #[test]
    fn one_row_rollback_cleanup_retries_busy_recovery_within_shared_budget() {
        let mut retry_budget = FsqliteRetryBudget::new(Duration::from_secs(1));
        let mut rollback_calls = 0usize;

        let retries = rollback_fsqlite_one_row_with_retry(0, &mut retry_budget, || {
            rollback_calls += 1;
            if rollback_calls == 1 {
                Err(fsqlite::FrankenError::BusyRecovery)
            } else {
                Ok(())
            }
        })
        .expect("a retryable rollback cleanup failure must recover inside the shared budget");

        assert_eq!(rollback_calls, 2);
        assert_eq!(retries, 1);
        assert_eq!(retry_budget.attempts(), 1);
    }

    #[test]
    fn one_row_rollback_cleanup_rejects_an_expired_shared_deadline() {
        let started = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("test instant supports a two-second lookback");
        let mut retry_budget = FsqliteRetryBudget::with_started(started, Duration::from_secs(1));
        let mut rollback_calls = 0usize;

        let error = rollback_fsqlite_one_row_with_retry(0, &mut retry_budget, || {
            rollback_calls += 1;
            Err::<(), _>(fsqlite::FrankenError::BusyRecovery)
        })
        .expect_err("an expired shared deadline must reject a retryable rollback cleanup failure");

        assert_eq!(rollback_calls, 1);
        assert_eq!(retry_budget.attempts(), 0);
        assert!(error.contains("one-row ROLLBACK exhausted shared worker retry budget"));
        assert!(error.contains("after 0 retries"));
    }

    #[test]
    fn one_row_rollback_cleanup_rejects_nonretryable_errors_without_retrying() {
        let mut retry_budget = FsqliteRetryBudget::new(Duration::from_secs(1));
        let mut rollback_calls = 0usize;

        let error = rollback_fsqlite_one_row_with_retry(0, &mut retry_budget, || {
            rollback_calls += 1;
            Err::<(), _>(fsqlite::FrankenError::BusySnapshot {
                conflicting_pages: "2".to_owned(),
            })
        })
        .expect_err("a nonretryable rollback cleanup failure must fail closed");

        assert_eq!(rollback_calls, 1);
        assert_eq!(retry_budget.attempts(), 0);
        assert!(error.contains("one-row ROLLBACK failed after 0 retries"));
        assert!(error.contains("snapshot conflict on pages: 2"));
        assert!(fsqlite_rollback_error_is_retryable(
            &fsqlite::FrankenError::Busy
        ));
        assert!(fsqlite_rollback_error_is_retryable(
            &fsqlite::FrankenError::BusyRecovery
        ));
        assert!(!fsqlite_rollback_error_is_retryable(
            &fsqlite::FrankenError::BusySnapshot {
                conflicting_pages: "2".to_owned(),
            }
        ));
    }

    #[test]
    fn one_row_workloads_commit_exact_separate_table_state() {
        let threads = 2;
        let rows_per_thread = 3;
        let retry_timeout = fsqlite_retry_timeout(threads, rows_per_thread);
        let sqlite = run_rusqlite(
            threads,
            rows_per_thread,
            true,
            TransactionGranularity::OneRow,
            retry_timeout,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            SynchronousMode::Normal,
        )
        .expect("C SQLite one-row workload must complete");
        let fsqlite = run_fsqlite(
            threads,
            rows_per_thread,
            true,
            TransactionGranularity::OneRow,
            retry_timeout,
            DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
            SynchronousMode::Normal,
        )
        .expect("FrankenSQLite one-row workload must complete");

        for (engine, result) in [("C SQLite", sqlite), ("FrankenSQLite", fsqlite)] {
            assert!(
                result.correctness_valid(),
                "{engine} one-row state/oracle proof failed: {result:#?}"
            );
            assert_eq!(result.accounting.offered_writes, 6, "{engine}");
            assert_eq!(result.accounting.succeeded_writes, 6, "{engine}");
            assert_eq!(result.accounting.failed_writes, 0, "{engine}");
            assert_eq!(result.committed_state.expected_rows, 6, "{engine}");
            assert_eq!(result.committed_state.observed_rows, 6, "{engine}");
            assert_eq!(result.committed_state.integrity_check, ["ok"], "{engine}");
        }
    }

    #[test]
    fn fsqlite_retry_classifier_is_pinned_to_the_receipted_error_set() {
        let retryable = [
            fsqlite::FrankenError::Busy,
            fsqlite::FrankenError::BusyRecovery,
            fsqlite::FrankenError::BusySnapshot {
                conflicting_pages: "1".to_owned(),
            },
            fsqlite::FrankenError::DatabaseLocked {
                path: PathBuf::from("test.db"),
            },
            fsqlite::FrankenError::WriteConflict { page: 1, holder: 2 },
            fsqlite::FrankenError::SerializationFailure { page: 1 },
            fsqlite::FrankenError::PageBufferCapacityExhausted {
                operation: "test",
                page_size: 4096,
                max_buffers: 1,
                total_buffers: 1,
                available_buffers: 0,
                cached_clean: 0,
                cached_dirty: 1,
                successful_evictions: 0,
            },
        ];
        assert!(retryable.iter().all(fsqlite_error_is_retryable));
        assert!(!fsqlite_error_is_retryable(
            &fsqlite::FrankenError::DatabaseFull
        ));
    }
}
