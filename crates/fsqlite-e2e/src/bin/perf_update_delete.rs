//! Narrow profiling binary for UPDATE/DELETE fsqlite hot path.
//!
//! Runs the same fsqlite UPDATE/DELETE workload as `comprehensive-bench`'s
//! Section 6, but without the C SQLite comparison or the benchmark reporting
//! ceremony, so perf/flamegraph stacks stay focused on the fsqlite engine.
//!
//! Usage:
//!   perf-update-delete                         # default: 10_000 rows, 10 iters, update+delete, fsqlite only
//!   perf-update-delete 100000 3 update
//!   perf-update-delete 1000   5 delete compare
//!   perf-update-delete 10000 250 delete fsqlite isolated
//!   perf-update-delete 10000 250 delete fsqlite rollback-isolated
//!   perf-update-delete 10000 250 delete fsqlite sparse-isolated
//!   perf-update-delete 1280 1000 delete fsqlite exact-gate
//!
//! Arguments:
//!   `[rows]`   Number of rows to pre-populate (default 10_000)
//!   `[iters]`  Number of outer iterations for profiling (default 10)
//!   `[which]`  "update" | "delete" | "both" (default "both")
//!   `[engine]` "fsqlite" | "sqlite" | "compare" (default "fsqlite")
//!   `[mode]`   "standard" | "isolated" | "rollback-isolated" | "sparse-isolated" | "exact-gate" (default "standard")
//!
//! Environment:
//!   FSQLITE_BENCH_PROFILE_DML=1       Print fsqlite hot-path counters for each measured DML window.
//!   FSQLITE_BENCH_PERF_CTL=`<fifo>`      Enable/disable an attached perf session around exact DML windows.
//!   FSQLITE_BENCH_PERF_ACK=`<fifo>`      Wait for perf control acknowledgements before crossing each boundary.

// bd-mnlk2 / bd-zavyn: the hoisted timed windows await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::process::ExitCode;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use fsqlite_core::connection::{
    HotPathProfileSnapshot, hot_path_profile_enabled, hot_path_profile_snapshot,
    reset_hot_path_profile, set_hot_path_profile_enabled,
};

const DEFAULT_ROWS: usize = 10_000;
const DEFAULT_ITERS: usize = 10;
const EXACT_GATE_WARMUP_ITERS: usize = 3;
const PROFILE_DML_ENV: &str = "FSQLITE_BENCH_PROFILE_DML";
const PERF_CTL_ENV: &str = "FSQLITE_BENCH_PERF_CTL";
const PERF_ACK_ENV: &str = "FSQLITE_BENCH_PERF_ACK";
const USAGE: &str = "\
Usage:
  perf-update-delete                         # default: 10_000 rows, 10 iters, update+delete, fsqlite only
  perf-update-delete 100000 3 update
  perf-update-delete 1000   5 delete compare
  perf-update-delete 10000 250 delete fsqlite isolated
  perf-update-delete 10000 250 delete fsqlite rollback-isolated
  perf-update-delete 10000 250 delete fsqlite sparse-isolated
  perf-update-delete 1280 1000 delete compare exact-gate

Arguments:
  [rows]   Number of rows to pre-populate (default 10_000)
  [iters]  Number of outer iterations for profiling (default 10)
  [which]  \"update\" | \"delete\" | \"both\" (default \"both\")
  [engine] \"fsqlite\" | \"sqlite\" | \"compare\" (default \"fsqlite\")
  [mode]   \"standard\" | \"isolated\" | \"rollback-isolated\" | \"sparse-isolated\" | \"exact-gate\" (default \"standard\")

Environment:
  FSQLITE_BENCH_PROFILE_DML=1       Print fsqlite hot-path counters for each measured DML window.
  FSQLITE_BENCH_PERF_CTL=<fifo>      Enable/disable attached perf events around exact DML windows.
  FSQLITE_BENCH_PERF_ACK=<fifo>      Wait for perf acknowledgements at both window boundaries.";
const BENCH_CREATE_SQL: &str =
    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL)";
const BENCH_INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))";
const BENCHMARK_PRAGMAS: &[&str] = &[
    "PRAGMA page_size = 4096;",
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = -64000;",
    // This profiler never issues FOR SYSTEM_TIME queries. Match
    // comprehensive_bench's write scenarios and suppress the optional
    // MemDatabase history clone that otherwise runs on each explicit COMMIT.
    "PRAGMA fsqlite_capture_time_travel_snapshots=false;",
];
const CSQLITE_BENCHMARK_PRAGMAS: &[&str] = &[
    "PRAGMA page_size = 4096;",
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = -64000;",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkloadKind {
    Update,
    Delete,
    Both,
}

impl WorkloadKind {
    fn parse(raw: &str) -> Result<Self, RunError> {
        match raw {
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "both" => Ok(Self::Both),
            other => Err(RunError::Usage(format!(
                "invalid workload '{other}'; expected update, delete, or both"
            ))),
        }
    }

    fn do_update(self) -> bool {
        matches!(self, Self::Update | Self::Both)
    }

    fn do_delete(self) -> bool {
        matches!(self, Self::Delete | Self::Both)
    }
}

impl fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Update => f.write_str("update"),
            Self::Delete => f.write_str("delete"),
            Self::Both => f.write_str("both"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EngineKind {
    Fsqlite,
    Sqlite,
    Compare,
}

impl EngineKind {
    fn parse(raw: &str) -> Result<Self, RunError> {
        match raw {
            "fsqlite" => Ok(Self::Fsqlite),
            "sqlite" => Ok(Self::Sqlite),
            "compare" => Ok(Self::Compare),
            other => Err(RunError::Usage(format!(
                "invalid engine '{other}'; expected fsqlite, sqlite, or compare"
            ))),
        }
    }

    fn run_fsqlite(self) -> bool {
        matches!(self, Self::Fsqlite | Self::Compare)
    }

    fn run_sqlite(self) -> bool {
        matches!(self, Self::Sqlite | Self::Compare)
    }
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fsqlite => f.write_str("fsqlite"),
            Self::Sqlite => f.write_str("sqlite"),
            Self::Compare => f.write_str("compare"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileMode {
    Standard,
    Isolated,
    RollbackIsolated,
    SparseIsolated,
    ExactGate,
}

impl ProfileMode {
    fn parse(raw: &str) -> Result<Self, RunError> {
        match raw {
            "standard" => Ok(Self::Standard),
            "isolated" => Ok(Self::Isolated),
            "rollback-isolated" => Ok(Self::RollbackIsolated),
            "sparse-isolated" => Ok(Self::SparseIsolated),
            "exact-gate" => Ok(Self::ExactGate),
            other => Err(RunError::Usage(format!(
                "invalid mode '{other}'; expected standard, isolated, rollback-isolated, sparse-isolated, or exact-gate"
            ))),
        }
    }
}

impl fmt::Display for ProfileMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standard => f.write_str("standard"),
            Self::Isolated => f.write_str("isolated"),
            Self::RollbackIsolated => f.write_str("rollback-isolated"),
            Self::SparseIsolated => f.write_str("sparse-isolated"),
            Self::ExactGate => f.write_str("exact-gate"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BenchArgs {
    rows: usize,
    iters: usize,
    workload: WorkloadKind,
    engine: EngineKind,
    profile_mode: ProfileMode,
}

#[derive(Debug, PartialEq, Eq)]
enum RunError {
    Usage(String),
    Runtime(String),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) | Self::Runtime(message) => f.write_str(message),
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("perf-update-delete: {err}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), RunError> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    if is_help_request(&raw_args) {
        println!("{USAGE}");
        return Ok(());
    }
    let args = parse_args(raw_args)?;
    run_benchmark(&args)
}

fn is_help_request(args: &[String]) -> bool {
    args.first()
        .is_some_and(|arg| arg == "-h" || arg == "--help")
}

fn parse_args<I>(args: I) -> Result<BenchArgs, RunError>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();

    let rows = match args.next() {
        Some(raw) => raw.parse::<usize>().map_err(|_| {
            RunError::Usage(format!(
                "invalid rows '{raw}'; expected a non-negative integer"
            ))
        })?,
        None => DEFAULT_ROWS,
    };
    let iters = match args.next() {
        Some(raw) => raw.parse::<usize>().map_err(|_| {
            RunError::Usage(format!(
                "invalid iters '{raw}'; expected a non-negative integer"
            ))
        })?,
        None => DEFAULT_ITERS,
    };
    let workload = match args.next() {
        Some(raw) => WorkloadKind::parse(&raw)?,
        None => WorkloadKind::Both,
    };
    let engine = match args.next() {
        Some(raw) => EngineKind::parse(&raw)?,
        None => EngineKind::Fsqlite,
    };
    let profile_mode = match args.next() {
        Some(raw) => ProfileMode::parse(&raw)?,
        None => ProfileMode::Standard,
    };
    if let Some(extra) = args.next() {
        return Err(RunError::Usage(format!(
            "unexpected extra argument '{extra}'; usage: perf-update-delete [rows] [iters] [update|delete|both] [fsqlite|sqlite|compare] [standard|isolated|rollback-isolated|sparse-isolated|exact-gate]"
        )));
    }
    if iters == 0 {
        return Err(RunError::Usage(
            "iters must be greater than zero".to_string(),
        ));
    }
    if profile_mode == ProfileMode::ExactGate && workload != WorkloadKind::Delete {
        return Err(RunError::Usage(
            "exact-gate mode requires the delete workload".to_string(),
        ));
    }
    Ok(BenchArgs {
        rows,
        iters,
        workload,
        engine,
        profile_mode,
    })
}

fn per_row_ns(total_ns: u128, op_count: usize, iters: usize) -> f64 {
    let total_ops = op_count.saturating_mul(iters);
    if total_ops == 0 {
        0.0
    } else {
        total_ns as f64 / total_ops as f64
    }
}

fn isolated_populate_rows_i64(
    args: &BenchArgs,
    rows_i64: i64,
    delete_count: usize,
) -> Result<i64, RunError> {
    if !args.workload.do_delete()
        || args.profile_mode == ProfileMode::RollbackIsolated
        || delete_count == 0
    {
        return Ok(rows_i64);
    }

    if args.profile_mode == ProfileMode::SparseIsolated {
        let last_iter_base = args
            .iters
            .checked_sub(1)
            .and_then(|last_iter| last_iter.checked_mul(args.rows))
            .ok_or_else(|| {
                RunError::Usage("sparse isolated delete row count overflowed usize".to_string())
            })?;
        let last_delete_offset = delete_count
            .checked_sub(1)
            .and_then(|last_idx| last_idx.checked_mul(20))
            .ok_or_else(|| {
                RunError::Usage("sparse isolated delete row id overflowed usize".to_string())
            })?;
        let required_rows = last_iter_base
            .checked_add(last_delete_offset)
            .and_then(|last_rowid| last_rowid.checked_add(1))
            .ok_or_else(|| {
                RunError::Usage("sparse isolated delete row count overflowed usize".to_string())
            })?;
        let required_rows_i64 = i64::try_from(required_rows).map_err(|_| {
            RunError::Usage("sparse isolated delete row count must fit within i64".to_string())
        })?;
        return Ok(rows_i64.max(required_rows_i64));
    }

    let required_delete_rows = args
        .iters
        .checked_mul(delete_count)
        .ok_or_else(|| RunError::Usage("isolated delete row count overflowed usize".to_string()))?;
    let required_delete_rows_i64 = i64::try_from(required_delete_rows).map_err(|_| {
        RunError::Usage("isolated delete row count must fit within i64".to_string())
    })?;
    Ok(rows_i64.max(required_delete_rows_i64))
}

fn isolated_delete_id(iter: usize, index: usize, delete_count: usize) -> Result<i64, RunError> {
    let row_offset = iter
        .checked_mul(delete_count)
        .and_then(|base| base.checked_add(index))
        .ok_or_else(|| RunError::Usage("isolated delete row id overflowed usize".to_string()))?;
    i64::try_from(row_offset)
        .map_err(|_| RunError::Usage("isolated delete row id must fit within i64".to_string()))
}

fn sparse_isolated_delete_id(iter: usize, index: usize, rows: usize) -> Result<i64, RunError> {
    let row_offset = iter
        .checked_mul(rows)
        .and_then(|base| {
            index
                .checked_mul(20)
                .and_then(|offset| base.checked_add(offset))
        })
        .ok_or_else(|| {
            RunError::Usage("sparse isolated delete row id overflowed usize".to_string())
        })?;
    i64::try_from(row_offset).map_err(|_| {
        RunError::Usage("sparse isolated delete row id must fit within i64".to_string())
    })
}

fn apply_benchmark_pragmas(conn: &fsqlite::Connection) -> Result<(), RunError> {
    for pragma in BENCHMARK_PRAGMAS {
        fsqlite_e2e::block_on(conn.execute(pragma))
            .map_err(|err| RunError::Runtime(format!("apply benchmark pragma {pragma}: {err}")))?;
    }

    if std::env::var("FSQLITE_BENCH_LAB_UNSAFE")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        for pragma in [
            "PRAGMA fsqlite.write_merge = LAB_UNSAFE;",
            "PRAGMA fsqlite.ssi_e_process_alpha = 0.001;",
        ] {
            fsqlite_e2e::block_on(conn.execute(pragma)).map_err(|err| {
                RunError::Runtime(format!("apply benchmark pragma {pragma}: {err}"))
            })?;
        }
    }

    if std::env::var("FSQLITE_BENCH_FUSED_FALLBACK")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.fused_entry_mode = forced_fallback;"))
            .map_err(|err| {
            RunError::Runtime(format!("apply fused fallback benchmark pragma: {err}"))
        })?;
    }

    Ok(())
}

fn apply_csqlite_benchmark_pragmas(conn: &rusqlite::Connection) -> Result<(), RunError> {
    for pragma in CSQLITE_BENCHMARK_PRAGMAS {
        conn.execute_batch(pragma).map_err(|err| {
            RunError::Runtime(format!("apply C SQLite benchmark pragma {pragma}: {err}"))
        })?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TimingTotals {
    total: u128,
    populate: u128,
    update: u128,
    delete: u128,
}

struct PerfControl {
    ctl: File,
    ack: File,
}

impl PerfControl {
    fn open(ctl_path: &str, ack_path: &str) -> Result<Self, String> {
        let ctl = OpenOptions::new()
            .write(true)
            .open(ctl_path)
            .map_err(|err| format!("open perf control FIFO {ctl_path}: {err}"))?;
        let ack = OpenOptions::new()
            .read(true)
            .open(ack_path)
            .map_err(|err| format!("open perf acknowledgement FIFO {ack_path}: {err}"))?;
        Ok(Self { ctl, ack })
    }

    fn command(&mut self, command: &str) -> Result<(), String> {
        writeln!(self.ctl, "{command}")
            .map_err(|err| format!("write perf control command {command}: {err}"))?;
        self.ctl
            .flush()
            .map_err(|err| format!("flush perf control command {command}: {err}"))?;

        // perf writes the acknowledgement as a five-byte C string, including
        // its trailing NUL. Consuming only through the newline leaves that NUL
        // queued at the front of the next acknowledgement.
        let mut ack = [0_u8; 5];
        self.ack
            .read_exact(&mut ack)
            .map_err(|err| format!("read perf acknowledgement for {command}: {err}"))?;
        if &ack != b"ack\n\0" {
            return Err(format!(
                "perf returned an unexpected acknowledgement for {command}: {ack:?}"
            ));
        }
        Ok(())
    }
}

fn perf_control() -> Result<Option<&'static Mutex<PerfControl>>, RunError> {
    static PERF_CONTROL: OnceLock<Result<Option<Mutex<PerfControl>>, String>> = OnceLock::new();
    match PERF_CONTROL.get_or_init(|| {
        let ctl_path = match std::env::var(PERF_CTL_ENV) {
            Ok(path) => path,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(err) => return Err(format!("read {PERF_CTL_ENV}: {err}")),
        };
        let ack_path = std::env::var(PERF_ACK_ENV)
            .map_err(|err| format!("read {PERF_ACK_ENV} while {PERF_CTL_ENV} is set: {err}"))?;
        PerfControl::open(&ctl_path, &ack_path).map(|control| Some(Mutex::new(control)))
    }) {
        Ok(control) => Ok(control.as_ref()),
        Err(message) => Err(RunError::Runtime(message.clone())),
    }
}

fn send_perf_control(command: &str) -> Result<(), RunError> {
    let Some(control) = perf_control()? else {
        return Ok(());
    };
    control
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .command(command)
        .map_err(RunError::Runtime)
}

struct DmlProfileScope {
    state: Option<DmlProfileState>,
}

struct DmlProfileState {
    previous_enabled: Option<bool>,
    perf_enabled: bool,
    label: DmlProfileLabel,
    started_at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DmlProfileOperation {
    Update,
    Delete,
}

impl fmt::Display for DmlProfileOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Update => f.write_str("update"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DmlProfileLabel {
    mode: ProfileMode,
    operation: DmlProfileOperation,
    iter: Option<usize>,
    rows: usize,
    iters: Option<usize>,
}

impl DmlProfileLabel {
    fn iter(mode: ProfileMode, operation: DmlProfileOperation, iter: usize, rows: usize) -> Self {
        Self {
            mode,
            operation,
            iter: Some(iter),
            rows,
            iters: None,
        }
    }

    fn aggregate(
        mode: ProfileMode,
        operation: DmlProfileOperation,
        rows: usize,
        iters: usize,
    ) -> Self {
        Self {
            mode,
            operation,
            iter: None,
            rows,
            iters: Some(iters),
        }
    }
}

impl fmt::Display for DmlProfileLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fsqlite {} {}", self.mode, self.operation)?;
        if let Some(iter) = self.iter {
            write!(f, " iter={iter} rows={}", self.rows)
        } else if let Some(iters) = self.iters {
            write!(f, " rows={} iters={iters}", self.rows)
        } else {
            write!(f, " rows={}", self.rows)
        }
    }
}

impl DmlProfileScope {
    fn start(label: DmlProfileLabel) -> Result<Self, RunError> {
        let profile_enabled = dml_profile_enabled();
        let perf_enabled = perf_control()?.is_some();
        if !profile_enabled && !perf_enabled {
            return Ok(Self { state: None });
        }

        let previous_enabled = if profile_enabled {
            let previous_enabled = hot_path_profile_enabled();
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            Some(previous_enabled)
        } else {
            None
        };
        if perf_enabled {
            send_perf_control("enable")?;
        }

        Ok(Self {
            state: Some(DmlProfileState {
                previous_enabled,
                perf_enabled,
                label,
                started_at: Instant::now(),
            }),
        })
    }

    fn finish(mut self) -> Result<(), RunError> {
        let Some(state) = self.state.take() else {
            return Ok(());
        };

        if state.perf_enabled {
            send_perf_control("disable")?;
        }
        let elapsed_us = state.started_at.elapsed().as_secs_f64() * 1_000_000.0;
        if let Some(previous_enabled) = state.previous_enabled {
            let profile = hot_path_profile_snapshot();
            set_hot_path_profile_enabled(previous_enabled);
            print_dml_profile(state.label, elapsed_us, &profile);
        }
        Ok(())
    }

    fn restore(&mut self) {
        if let Some(state) = self.state.take() {
            if state.perf_enabled {
                let _ = send_perf_control("disable");
            }
            if let Some(previous_enabled) = state.previous_enabled {
                set_hot_path_profile_enabled(previous_enabled);
            }
        }
    }
}

impl Drop for DmlProfileScope {
    fn drop(&mut self) {
        self.restore();
    }
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn dml_profile_enabled() -> bool {
    static PROFILE_DML_ENABLED: OnceLock<bool> = OnceLock::new();
    *PROFILE_DML_ENABLED.get_or_init(|| env_flag(PROFILE_DML_ENV))
}

fn print_dml_profile(label: DmlProfileLabel, elapsed_us: f64, profile: &HotPathProfileSnapshot) {
    eprintln!(
        "    [{label}] dml_profile elapsed_us={elapsed_us:.1} direct_update={} direct_delete={} update_leaf_start={}/{} update_leaf_start_ns={} update_leaf_active={}/{} update_leaf_miss={} update_leaf_active_ns={} update_leaf_flush={}/{} update_leaf_flush_ns={} delete_preflush_ns={} delete_rowid_ns={} delete_active_probe_ns={} delete_cursor_setup_ns={} delete_memdb_abandon={}/{} delete_memory_sync={}/{} delete_qf_ns={} delete_seek_ns={} delete_physical_ns={} delete_leaf_start={}/{} delete_leaf_start_ns={} delete_leaf_active={}/{} delete_leaf_miss={} delete_leaf_miss_shape={} delete_leaf_miss_staged={} delete_leaf_miss_out_of_leaf={} delete_leaf_miss_duplicate={} delete_leaf_miss_empty_leaf={} delete_leaf_miss_last_cell={} delete_leaf_miss_noncompact={} delete_leaf_miss_cell_shape={} delete_leaf_active_ns={} delete_leaf_flush={}/{} delete_leaf_flush_ns={} delete_leaf_materialize={}/{} delete_leaf_write={}/{} delete_leaf_search={}/{} delete_leaf_dupcheck={}/{} delete_leaf_compact={}/{} delete_leaf_cellparse={}/{} fast={} slow={} ud_fast_lane={} ud_instrumented_lane={} begin_ns={} execute_body_ns={} commit_roundtrip_ns={} pager_commit_calls={} pager_phase_a_ns={} pager_wal_ns={} pager_mem_flush_ns={} pager_cache_finish_ns={} parser_cache_hits={} parser_cache_misses={} parser_parse_ns={} bg_checks={} bg_ns={} prepared_lookup_ns={} memdb_refresh={} cached_write_reuses={} cached_write_parks={} page_pool_hits={} page_pool_misses={} record_parse_into={} record_decode_ns={} btree_payload_copy_calls={} btree_payload_copy_bytes={} btree_cell_assembly_calls={} btree_cell_assembly_bytes={} vdbe_opcodes={} vdbe_statements={} vdbe_make_record={}",
        profile.prepared_direct_update_executions,
        profile.prepared_direct_delete_executions,
        profile.prepared_direct_update_leaf_patch_run_start_hits,
        profile.prepared_direct_update_leaf_patch_run_start_attempts,
        profile.prepared_direct_update_leaf_patch_run_start_time_ns,
        profile.prepared_direct_update_leaf_patch_run_active_hits,
        profile.prepared_direct_update_leaf_patch_run_active_attempts,
        profile.prepared_direct_update_leaf_patch_run_active_misses,
        profile.prepared_direct_update_leaf_patch_run_active_time_ns,
        profile.prepared_direct_update_leaf_patch_run_dirty_flushes,
        profile.prepared_direct_update_leaf_patch_run_flushes,
        profile.prepared_direct_update_leaf_patch_run_flush_time_ns,
        profile.prepared_direct_delete_preflush_time_ns,
        profile.prepared_direct_delete_rowid_lookup_time_ns,
        profile.prepared_direct_delete_active_leaf_probe_time_ns,
        profile.prepared_direct_delete_cursor_setup_time_ns,
        profile.prepared_direct_delete_memdb_abandon_calls,
        profile.prepared_direct_delete_memdb_abandon_time_ns,
        profile.prepared_direct_delete_memory_sync_calls,
        profile.prepared_direct_delete_memory_sync_time_ns,
        profile.prepared_direct_delete_qf_time_ns,
        profile.prepared_direct_delete_seek_time_ns,
        profile.prepared_direct_delete_physical_delete_time_ns,
        profile.prepared_direct_delete_leaf_run_start_hits,
        profile.prepared_direct_delete_leaf_run_start_attempts,
        profile.prepared_direct_delete_leaf_run_start_time_ns,
        profile.prepared_direct_delete_leaf_run_active_hits,
        profile.prepared_direct_delete_leaf_run_active_attempts,
        profile.prepared_direct_delete_leaf_run_active_misses,
        profile.prepared_direct_delete_leaf_run_active_miss_shape_mismatches,
        profile.prepared_direct_delete_leaf_run_active_miss_staged_runs,
        profile.prepared_direct_delete_leaf_run_active_miss_rowid_not_in_leaf,
        profile.prepared_direct_delete_leaf_run_active_miss_already_deleted,
        profile.prepared_direct_delete_leaf_run_active_miss_nonroot_would_empty_leaf,
        profile.prepared_direct_delete_leaf_run_active_miss_nonroot_last_cell,
        profile.prepared_direct_delete_leaf_run_active_miss_noncompact_cell_area,
        profile.prepared_direct_delete_leaf_run_active_miss_cell_shape_or_overflow,
        profile.prepared_direct_delete_leaf_run_active_time_ns,
        profile.prepared_direct_delete_leaf_run_dirty_flushes,
        profile.prepared_direct_delete_leaf_run_flushes,
        profile.prepared_direct_delete_leaf_run_flush_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_materialize_calls,
        profile.btree_leaf_reuse.delete_leaf_run_materialize_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_write_calls,
        profile.btree_leaf_reuse.delete_leaf_run_write_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_search_calls,
        profile.btree_leaf_reuse.delete_leaf_run_search_time_ns,
        profile
            .btree_leaf_reuse
            .delete_leaf_run_duplicate_check_calls,
        profile
            .btree_leaf_reuse
            .delete_leaf_run_duplicate_check_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_compact_check_calls,
        profile
            .btree_leaf_reuse
            .delete_leaf_run_compact_check_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_cell_parse_calls,
        profile.btree_leaf_reuse.delete_leaf_run_cell_parse_time_ns,
        profile.parser.fast_path_executions,
        profile.parser.slow_path_executions,
        profile.prepared_update_delete_fast_lane_hits,
        profile.prepared_update_delete_instrumented_lane_hits,
        profile.begin_setup_time_ns,
        profile.execute_body_time_ns,
        profile.commit_txn_roundtrip_time_ns,
        profile.pager_commit.commit_calls,
        profile.pager_commit.phase_a_time_ns,
        profile.pager_commit.wal_commit_time_ns,
        profile.pager_commit.memory_flush_time_ns,
        profile.pager_commit.cache_finish_time_ns,
        profile.parser.parse_cache_hits,
        profile.parser.parse_cache_misses,
        profile.parser.parse_time_ns,
        profile.background_status_checks,
        profile.background_status_time_ns,
        profile.prepared_lookup_time_ns,
        profile.memdb_refresh_count,
        profile.cached_write_txn_reuses,
        profile.cached_write_txn_parks,
        profile.page_buffer_pool_hits,
        profile.page_buffer_pool_misses,
        profile.record_decode.parse_record_into_calls,
        profile.record_decode.decode_time_ns,
        profile.btree_copy_kernels.local_payload_copy_calls,
        profile.btree_copy_kernels.local_payload_copy_bytes,
        profile.btree_copy_kernels.table_leaf_cell_assembly_calls,
        profile.btree_copy_kernels.table_leaf_cell_assembly_bytes,
        profile.vdbe.opcodes_executed_total,
        profile.vdbe.statements_total,
        profile.vdbe.make_record_calls_total,
    );
}

fn run_benchmark(args: &BenchArgs) -> Result<(), RunError> {
    let rows_i64 = i64::try_from(args.rows)
        .map_err(|_| RunError::Usage("rows must fit within i64".to_string()))?;
    let update_count = args.rows / 10;
    let delete_count = args.rows / 20;

    eprintln!(
        "perf-update-delete: rows={} iters={} which={} engine={} mode={} (do_update={} do_delete={} update_count={} delete_count={})",
        args.rows,
        args.iters,
        args.workload,
        args.engine,
        args.profile_mode,
        args.workload.do_update(),
        args.workload.do_delete(),
        update_count,
        delete_count,
    );

    let mut fsqlite_totals = None;
    let mut sqlite_totals = None;

    if args.engine.run_fsqlite() {
        let totals = match args.profile_mode {
            ProfileMode::Standard => {
                run_fsqlite_benchmark(args, rows_i64, update_count, delete_count)?
            }
            ProfileMode::Isolated | ProfileMode::RollbackIsolated | ProfileMode::SparseIsolated => {
                run_fsqlite_isolated_benchmark(args, rows_i64, update_count, delete_count)?
            }
            ProfileMode::ExactGate => {
                run_fsqlite_exact_gate_benchmark(args, rows_i64, delete_count)?
            }
        };
        print_engine_summary("fsqlite", args, update_count, delete_count, totals);
        fsqlite_totals = Some(totals);
    }

    if args.engine.run_sqlite() {
        let totals = match args.profile_mode {
            ProfileMode::Standard => {
                run_sqlite_benchmark(args, rows_i64, update_count, delete_count)?
            }
            ProfileMode::Isolated | ProfileMode::RollbackIsolated | ProfileMode::SparseIsolated => {
                run_sqlite_isolated_benchmark(args, rows_i64, update_count, delete_count)?
            }
            ProfileMode::ExactGate => {
                run_sqlite_exact_gate_benchmark(args, rows_i64, delete_count)?
            }
        };
        print_engine_summary("sqlite", args, update_count, delete_count, totals);
        sqlite_totals = Some(totals);
    }

    if let (Some(fsqlite), Some(sqlite)) = (fsqlite_totals, sqlite_totals) {
        print_comparison_summary(args, fsqlite, sqlite);
    }

    Ok(())
}

fn run_fsqlite_benchmark(
    args: &BenchArgs,
    rows_i64: i64,
    update_count: usize,
    delete_count: usize,
) -> Result<TimingTotals, RunError> {
    let t_all = Instant::now();
    let mut total_update_ns: u128 = 0;
    let mut total_delete_ns: u128 = 0;
    let mut total_populate_ns: u128 = 0;

    for iter in 0..args.iters {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
            .map_err(|err| RunError::Runtime(format!("open in-memory database: {err}")))?;
        apply_benchmark_pragmas(&conn)?;
        fsqlite_e2e::block_on(conn.execute(BENCH_CREATE_SQL))
            .map_err(|err| RunError::Runtime(format!("create benchmark table: {err}")))?;
        fsqlite_e2e::block_on(conn.execute("BEGIN"))
            .map_err(|err| RunError::Runtime(format!("begin populate transaction: {err}")))?;
        let stmt = fsqlite_e2e::block_on(conn.prepare(BENCH_INSERT_SQL))
            .map_err(|err| RunError::Runtime(format!("prepare populate statement: {err}")))?;
        // bd-mnlk2 / bd-zavyn: one runtime entry per timed window so the
        // sample measures the engine, not a ~333 ns bridge entry per row.
        let t0 = Instant::now();
        fsqlite_e2e::block_on(async {
            for i in 0..rows_i64 {
                stmt.execute_with_params(&[fsqlite::SqliteValue::Integer(i)])
                    .await
                    .map_err(|err| RunError::Runtime(format!("populate row {i}: {err}")))?;
            }
            conn.execute("COMMIT")
                .await
                .map_err(|err| RunError::Runtime(format!("commit populate transaction: {err}")))?;
            Ok::<(), RunError>(())
        })?;
        total_populate_ns += t0.elapsed().as_nanos();

        if args.workload.do_update() {
            fsqlite_e2e::block_on(conn.execute("BEGIN"))
                .map_err(|err| RunError::Runtime(format!("begin update transaction: {err}")))?;
            let update =
                fsqlite_e2e::block_on(conn.prepare("UPDATE bench SET value = ?2 WHERE id = ?1"))
                    .map_err(|err| RunError::Runtime(format!("prepare update statement: {err}")))?;
            let profile = DmlProfileScope::start(DmlProfileLabel::iter(
                args.profile_mode,
                DmlProfileOperation::Update,
                iter,
                args.rows,
            ))?;
            let t0 = Instant::now();
            fsqlite_e2e::block_on(async {
                for i in 0..update_count {
                    let id = i64::try_from(i).map_err(|_| {
                        RunError::Usage("update_count index overflowed i64".to_string())
                    })? * 10;
                    update
                        .execute_with_params(&[
                            fsqlite::SqliteValue::Integer(id),
                            fsqlite::SqliteValue::Float(999.99),
                        ])
                        .await
                        .map_err(|err| RunError::Runtime(format!("update row {id}: {err}")))?;
                }
                conn.execute("COMMIT").await.map_err(|err| {
                    RunError::Runtime(format!("commit update transaction: {err}"))
                })?;
                Ok::<(), RunError>(())
            })?;
            total_update_ns += t0.elapsed().as_nanos();
            profile.finish()?;
        }

        if args.workload.do_delete() {
            fsqlite_e2e::block_on(conn.execute("BEGIN"))
                .map_err(|err| RunError::Runtime(format!("begin delete transaction: {err}")))?;
            let delete = fsqlite_e2e::block_on(conn.prepare("DELETE FROM bench WHERE id = ?1"))
                .map_err(|err| RunError::Runtime(format!("prepare delete statement: {err}")))?;
            let profile = DmlProfileScope::start(DmlProfileLabel::iter(
                args.profile_mode,
                DmlProfileOperation::Delete,
                iter,
                args.rows,
            ))?;
            let t0 = Instant::now();
            fsqlite_e2e::block_on(async {
                for i in 0..delete_count {
                    let id = i64::try_from(i).map_err(|_| {
                        RunError::Usage("delete_count index overflowed i64".to_string())
                    })? * 20;
                    delete
                        .execute_with_params(&[fsqlite::SqliteValue::Integer(id)])
                        .await
                        .map_err(|err| RunError::Runtime(format!("delete row {id}: {err}")))?;
                }
                conn.execute("COMMIT").await.map_err(|err| {
                    RunError::Runtime(format!("commit delete transaction: {err}"))
                })?;
                Ok::<(), RunError>(())
            })?;
            total_delete_ns += t0.elapsed().as_nanos();
            profile.finish()?;
        }

        if iter == 0 {
            eprintln!("  (first iter complete)");
        }
    }

    Ok(TimingTotals {
        total: t_all.elapsed().as_nanos(),
        populate: total_populate_ns,
        update: total_update_ns,
        delete: total_delete_ns,
    })
}

/// Profile only the exact prepared-DELETE keep-gate envelope.
///
/// The connection and both prepared statements are reused across every sample,
/// matching `comprehensive-bench`. Perf is enabled only for
/// `BEGIN -> sparse prepared DELETEs -> COMMIT`; the restore transaction runs
/// with events disabled, so neither fixture construction nor restore INSERTs
/// enter the sample distribution.
fn run_fsqlite_exact_gate_benchmark(
    args: &BenchArgs,
    rows_i64: i64,
    delete_count: usize,
) -> Result<TimingTotals, RunError> {
    if delete_count == 0 || !args.rows.is_multiple_of(20) {
        return Err(RunError::Usage(
            "exact-gate rows must be a positive multiple of 20".to_string(),
        ));
    }

    let t_all = Instant::now();
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
        .map_err(|err| RunError::Runtime(format!("open exact-gate database: {err}")))?;
    apply_benchmark_pragmas(&conn)?;
    fsqlite_e2e::block_on(conn.execute(BENCH_CREATE_SQL))
        .map_err(|err| RunError::Runtime(format!("create exact-gate table: {err}")))?;
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .map_err(|err| RunError::Runtime(format!("begin exact-gate population: {err}")))?;
    let insert = fsqlite_e2e::block_on(conn.prepare(BENCH_INSERT_SQL))
        .map_err(|err| RunError::Runtime(format!("prepare exact-gate population: {err}")))?;
    // bd-mnlk2 / bd-zavyn: one runtime entry per timed window so samples
    // measure the engine, not a ~333 ns bridge entry per row.
    let populate_start = Instant::now();
    fsqlite_e2e::block_on(async {
        for rowid in 0..rows_i64 {
            insert
                .execute_with_params(&[fsqlite::SqliteValue::Integer(rowid)])
                .await
                .map_err(|err| {
                    RunError::Runtime(format!("populate exact-gate row {rowid}: {err}"))
                })?;
        }
        conn.execute("COMMIT")
            .await
            .map_err(|err| RunError::Runtime(format!("commit exact-gate population: {err}")))?;
        Ok::<(), RunError>(())
    })?;
    let total_populate_ns = populate_start.elapsed().as_nanos();
    drop(insert);

    let delete = fsqlite_e2e::block_on(conn.prepare("DELETE FROM bench WHERE id = ?1"))
        .map_err(|err| RunError::Runtime(format!("prepare exact-gate DELETE: {err}")))?;
    let restore = fsqlite_e2e::block_on(conn.prepare(BENCH_INSERT_SQL))
        .map_err(|err| RunError::Runtime(format!("prepare exact-gate restore: {err}")))?;

    let total_iters = args
        .iters
        .checked_add(EXACT_GATE_WARMUP_ITERS)
        .ok_or_else(|| RunError::Usage("exact-gate iteration count overflowed".to_string()))?;
    let mut total_delete_ns = 0_u128;
    for iter in 0..total_iters {
        let measured_iter = iter.checked_sub(EXACT_GATE_WARMUP_ITERS);
        let profile = if let Some(sample_idx) = measured_iter {
            DmlProfileScope::start(DmlProfileLabel::iter(
                ProfileMode::ExactGate,
                DmlProfileOperation::Delete,
                sample_idx,
                args.rows,
            ))?
        } else {
            DmlProfileScope { state: None }
        };
        let delete_start = Instant::now();
        fsqlite_e2e::block_on(async {
            conn.execute("BEGIN").await.map_err(|err| {
                RunError::Runtime(format!("begin exact-gate DELETE iteration {iter}: {err}"))
            })?;
            for index in 0..delete_count {
                let rowid = i64::try_from(index)
                    .map_err(|_| RunError::Usage("delete index must fit within i64".to_string()))?
                    * 20;
                let affected = delete
                    .execute_with_params(&[fsqlite::SqliteValue::Integer(rowid)])
                    .await
                    .map_err(|err| {
                        RunError::Runtime(format!(
                            "exact-gate DELETE iteration {iter} row {rowid}: {err}"
                        ))
                    })?;
                if affected != 1 {
                    return Err(RunError::Runtime(format!(
                        "exact-gate DELETE iteration {iter} row {rowid} affected {affected} rows"
                    )));
                }
            }
            conn.execute("COMMIT").await.map_err(|err| {
                RunError::Runtime(format!("commit exact-gate DELETE iteration {iter}: {err}"))
            })?;
            Ok::<(), RunError>(())
        })?;
        if measured_iter.is_some() {
            total_delete_ns = total_delete_ns.saturating_add(delete_start.elapsed().as_nanos());
        }
        profile.finish()?;

        fsqlite_e2e::block_on(conn.execute("BEGIN")).map_err(|err| {
            RunError::Runtime(format!("begin exact-gate restore iteration {iter}: {err}"))
        })?;
        for index in 0..delete_count {
            let rowid = i64::try_from(index)
                .map_err(|_| RunError::Usage("restore index must fit within i64".to_string()))?
                * 20;
            let affected = fsqlite_e2e::block_on(
                restore.execute_with_params(&[fsqlite::SqliteValue::Integer(rowid)]),
            )
            .map_err(|err| {
                RunError::Runtime(format!(
                    "exact-gate restore iteration {iter} row {rowid}: {err}"
                ))
            })?;
            if affected != 1 {
                return Err(RunError::Runtime(format!(
                    "exact-gate restore iteration {iter} row {rowid} affected {affected} rows"
                )));
            }
        }
        fsqlite_e2e::block_on(conn.execute("COMMIT")).map_err(|err| {
            RunError::Runtime(format!("commit exact-gate restore iteration {iter}: {err}"))
        })?;
    }

    Ok(TimingTotals {
        total: t_all.elapsed().as_nanos(),
        populate: total_populate_ns,
        update: 0,
        delete: total_delete_ns,
    })
}

/// Run C SQLite under the same prepared-DELETE envelope as the exact FSQLite gate.
///
/// Perf uses the same acknowledged control boundary, so the resulting profile
/// excludes population and restore work for a same-host frame comparison.
fn run_sqlite_exact_gate_benchmark(
    args: &BenchArgs,
    rows_i64: i64,
    delete_count: usize,
) -> Result<TimingTotals, RunError> {
    if delete_count == 0 || !args.rows.is_multiple_of(20) {
        return Err(RunError::Usage(
            "exact-gate rows must be a positive multiple of 20".to_string(),
        ));
    }

    let t_all = Instant::now();
    let conn = rusqlite::Connection::open_in_memory()
        .map_err(|err| RunError::Runtime(format!("open exact-gate C SQLite database: {err}")))?;
    apply_csqlite_benchmark_pragmas(&conn)?;
    conn.execute(BENCH_CREATE_SQL, [])
        .map_err(|err| RunError::Runtime(format!("create exact-gate C SQLite table: {err}")))?;
    conn.execute_batch("BEGIN")
        .map_err(|err| RunError::Runtime(format!("begin exact-gate C SQLite population: {err}")))?;
    let mut insert = conn.prepare(BENCH_INSERT_SQL).map_err(|err| {
        RunError::Runtime(format!("prepare exact-gate C SQLite population: {err}"))
    })?;
    let populate_start = Instant::now();
    for rowid in 0..rows_i64 {
        insert.execute(rusqlite::params![rowid]).map_err(|err| {
            RunError::Runtime(format!("populate exact-gate C SQLite row {rowid}: {err}"))
        })?;
    }
    conn.execute_batch("COMMIT").map_err(|err| {
        RunError::Runtime(format!("commit exact-gate C SQLite population: {err}"))
    })?;
    let total_populate_ns = populate_start.elapsed().as_nanos();
    drop(insert);

    let mut delete = conn
        .prepare("DELETE FROM bench WHERE id = ?1")
        .map_err(|err| RunError::Runtime(format!("prepare exact-gate C SQLite DELETE: {err}")))?;
    let mut restore = conn
        .prepare(BENCH_INSERT_SQL)
        .map_err(|err| RunError::Runtime(format!("prepare exact-gate C SQLite restore: {err}")))?;

    let total_iters = args
        .iters
        .checked_add(EXACT_GATE_WARMUP_ITERS)
        .ok_or_else(|| RunError::Usage("exact-gate iteration count overflowed".to_string()))?;
    let mut total_delete_ns = 0_u128;
    for iter in 0..total_iters {
        let measured_iter = iter.checked_sub(EXACT_GATE_WARMUP_ITERS);
        let profile = if let Some(sample_idx) = measured_iter {
            DmlProfileScope::start(DmlProfileLabel::iter(
                ProfileMode::ExactGate,
                DmlProfileOperation::Delete,
                sample_idx,
                args.rows,
            ))?
        } else {
            DmlProfileScope { state: None }
        };
        let delete_start = Instant::now();
        conn.execute_batch("BEGIN").map_err(|err| {
            RunError::Runtime(format!(
                "begin exact-gate C SQLite DELETE iteration {iter}: {err}"
            ))
        })?;
        for index in 0..delete_count {
            let rowid = i64::try_from(index)
                .map_err(|_| RunError::Usage("delete index must fit within i64".to_string()))?
                * 20;
            let affected = delete.execute(rusqlite::params![rowid]).map_err(|err| {
                RunError::Runtime(format!(
                    "exact-gate C SQLite DELETE iteration {iter} row {rowid}: {err}"
                ))
            })?;
            if affected != 1 {
                return Err(RunError::Runtime(format!(
                    "exact-gate C SQLite DELETE iteration {iter} row {rowid} affected {affected} rows"
                )));
            }
        }
        conn.execute_batch("COMMIT").map_err(|err| {
            RunError::Runtime(format!(
                "commit exact-gate C SQLite DELETE iteration {iter}: {err}"
            ))
        })?;
        if measured_iter.is_some() {
            total_delete_ns = total_delete_ns.saturating_add(delete_start.elapsed().as_nanos());
        }
        profile.finish()?;

        conn.execute_batch("BEGIN").map_err(|err| {
            RunError::Runtime(format!(
                "begin exact-gate C SQLite restore iteration {iter}: {err}"
            ))
        })?;
        for index in 0..delete_count {
            let rowid = i64::try_from(index)
                .map_err(|_| RunError::Usage("restore index must fit within i64".to_string()))?
                * 20;
            let affected = restore.execute(rusqlite::params![rowid]).map_err(|err| {
                RunError::Runtime(format!(
                    "exact-gate C SQLite restore iteration {iter} row {rowid}: {err}"
                ))
            })?;
            if affected != 1 {
                return Err(RunError::Runtime(format!(
                    "exact-gate C SQLite restore iteration {iter} row {rowid} affected {affected} rows"
                )));
            }
        }
        conn.execute_batch("COMMIT").map_err(|err| {
            RunError::Runtime(format!(
                "commit exact-gate C SQLite restore iteration {iter}: {err}"
            ))
        })?;
    }

    Ok(TimingTotals {
        total: t_all.elapsed().as_nanos(),
        populate: total_populate_ns,
        update: 0,
        delete: total_delete_ns,
    })
}

fn run_fsqlite_isolated_benchmark(
    args: &BenchArgs,
    rows_i64: i64,
    update_count: usize,
    delete_count: usize,
) -> Result<TimingTotals, RunError> {
    let t_all = Instant::now();
    let populate_rows_i64 = isolated_populate_rows_i64(args, rows_i64, delete_count)?;
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
        .map_err(|err| RunError::Runtime(format!("open in-memory database: {err}")))?;
    apply_benchmark_pragmas(&conn)?;
    fsqlite_e2e::block_on(conn.execute(BENCH_CREATE_SQL))
        .map_err(|err| RunError::Runtime(format!("create benchmark table: {err}")))?;
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .map_err(|err| RunError::Runtime(format!("begin populate transaction: {err}")))?;
    let stmt = fsqlite_e2e::block_on(conn.prepare(BENCH_INSERT_SQL))
        .map_err(|err| RunError::Runtime(format!("prepare populate statement: {err}")))?;
    // bd-mnlk2 / bd-zavyn: one runtime entry per timed window so samples
    // measure the engine, not a ~333 ns bridge entry per row.
    let t0 = Instant::now();
    fsqlite_e2e::block_on(async {
        for i in 0..populate_rows_i64 {
            stmt.execute_with_params(&[fsqlite::SqliteValue::Integer(i)])
                .await
                .map_err(|err| RunError::Runtime(format!("populate row {i}: {err}")))?;
        }
        conn.execute("COMMIT")
            .await
            .map_err(|err| RunError::Runtime(format!("commit populate transaction: {err}")))?;
        Ok::<(), RunError>(())
    })?;
    let total_populate_ns = t0.elapsed().as_nanos();

    let mut total_update_ns: u128 = 0;
    let mut total_delete_ns: u128 = 0;

    if args.workload.do_update() {
        let update =
            fsqlite_e2e::block_on(conn.prepare("UPDATE bench SET value = ?2 WHERE id = ?1"))
                .map_err(|err| RunError::Runtime(format!("prepare update statement: {err}")))?;
        fsqlite_e2e::block_on(conn.execute("BEGIN")).map_err(|err| {
            RunError::Runtime(format!("begin isolated update transaction: {err}"))
        })?;
        let profile = DmlProfileScope::start(DmlProfileLabel::aggregate(
            args.profile_mode,
            DmlProfileOperation::Update,
            args.rows,
            args.iters,
        ))?;
        let t0 = Instant::now();
        fsqlite_e2e::block_on(async {
            for iter in 0..args.iters {
                let next_value = (iter as f64).mul_add(0.001, 999.99);
                for i in 0..update_count {
                    let id = i64::try_from(i).map_err(|_| {
                        RunError::Usage("update_count index overflowed i64".to_string())
                    })? * 10;
                    update
                        .execute_with_params(&[
                            fsqlite::SqliteValue::Integer(id),
                            fsqlite::SqliteValue::Float(next_value),
                        ])
                        .await
                        .map_err(|err| RunError::Runtime(format!("update row {id}: {err}")))?;
                }
            }
            Ok::<(), RunError>(())
        })?;
        total_update_ns = t0.elapsed().as_nanos();
        profile.finish()?;
        fsqlite_e2e::block_on(conn.execute("ROLLBACK")).map_err(|err| {
            RunError::Runtime(format!("rollback isolated update transaction: {err}"))
        })?;
    }

    if args.workload.do_delete() {
        let delete = fsqlite_e2e::block_on(conn.prepare("DELETE FROM bench WHERE id = ?1"))
            .map_err(|err| RunError::Runtime(format!("prepare delete statement: {err}")))?;
        if args.profile_mode == ProfileMode::RollbackIsolated {
            for iter in 0..args.iters {
                fsqlite_e2e::block_on(conn.execute("BEGIN")).map_err(|err| {
                    RunError::Runtime(format!(
                        "begin rollback-isolated delete transaction {iter}: {err}"
                    ))
                })?;
                let profile = DmlProfileScope::start(DmlProfileLabel::iter(
                    args.profile_mode,
                    DmlProfileOperation::Delete,
                    iter,
                    args.rows,
                ))?;
                let t0 = Instant::now();
                fsqlite_e2e::block_on(async {
                    for i in 0..delete_count {
                        let id = i64::try_from(i).map_err(|_| {
                            RunError::Usage("delete_count index overflowed i64".to_string())
                        })? * 20;
                        delete
                            .execute_with_params(&[fsqlite::SqliteValue::Integer(id)])
                            .await
                            .map_err(|err| RunError::Runtime(format!("delete row {id}: {err}")))?;
                    }
                    Ok::<(), RunError>(())
                })?;
                total_delete_ns += t0.elapsed().as_nanos();
                profile.finish()?;
                fsqlite_e2e::block_on(conn.execute("ROLLBACK")).map_err(|err| {
                    RunError::Runtime(format!(
                        "rollback rollback-isolated delete transaction {iter}: {err}"
                    ))
                })?;
                if iter == 0 {
                    eprintln!("  (first rollback-isolated delete iter complete)");
                }
            }
        } else {
            fsqlite_e2e::block_on(conn.execute("BEGIN")).map_err(|err| {
                RunError::Runtime(format!("begin isolated delete transaction: {err}"))
            })?;
            let profile = DmlProfileScope::start(DmlProfileLabel::aggregate(
                args.profile_mode,
                DmlProfileOperation::Delete,
                args.rows,
                args.iters,
            ))?;
            let t0 = Instant::now();
            fsqlite_e2e::block_on(async {
                for iter in 0..args.iters {
                    for i in 0..delete_count {
                        let id = if args.profile_mode == ProfileMode::SparseIsolated {
                            sparse_isolated_delete_id(iter, i, args.rows)?
                        } else {
                            isolated_delete_id(iter, i, delete_count)?
                        };
                        delete
                            .execute_with_params(&[fsqlite::SqliteValue::Integer(id)])
                            .await
                            .map_err(|err| RunError::Runtime(format!("delete row {id}: {err}")))?;
                    }
                    if iter == 0 {
                        eprintln!("  (first isolated delete iter complete)");
                    }
                }
                Ok::<(), RunError>(())
            })?;
            total_delete_ns = t0.elapsed().as_nanos();
            profile.finish()?;
            fsqlite_e2e::block_on(conn.execute("COMMIT")).map_err(|err| {
                RunError::Runtime(format!("commit isolated delete transaction: {err}"))
            })?;
        }
    }

    Ok(TimingTotals {
        total: t_all.elapsed().as_nanos(),
        populate: total_populate_ns,
        update: total_update_ns,
        delete: total_delete_ns,
    })
}

fn run_sqlite_benchmark(
    args: &BenchArgs,
    rows_i64: i64,
    update_count: usize,
    delete_count: usize,
) -> Result<TimingTotals, RunError> {
    let t_all = Instant::now();
    let mut total_update_ns: u128 = 0;
    let mut total_delete_ns: u128 = 0;
    let mut total_populate_ns: u128 = 0;

    for iter in 0..args.iters {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|err| RunError::Runtime(format!("open C SQLite in-memory database: {err}")))?;
        apply_csqlite_benchmark_pragmas(&conn)?;
        conn.execute(BENCH_CREATE_SQL, [])
            .map_err(|err| RunError::Runtime(format!("create C SQLite benchmark table: {err}")))?;
        conn.execute_batch("BEGIN").map_err(|err| {
            RunError::Runtime(format!("begin C SQLite populate transaction: {err}"))
        })?;
        let mut stmt = conn.prepare(BENCH_INSERT_SQL).map_err(|err| {
            RunError::Runtime(format!("prepare C SQLite populate statement: {err}"))
        })?;
        let t0 = Instant::now();
        for i in 0..rows_i64 {
            stmt.execute(rusqlite::params![i])
                .map_err(|err| RunError::Runtime(format!("populate C SQLite row {i}: {err}")))?;
        }
        conn.execute_batch("COMMIT").map_err(|err| {
            RunError::Runtime(format!("commit C SQLite populate transaction: {err}"))
        })?;
        total_populate_ns += t0.elapsed().as_nanos();

        if args.workload.do_update() {
            conn.execute_batch("BEGIN").map_err(|err| {
                RunError::Runtime(format!("begin C SQLite update transaction: {err}"))
            })?;
            let mut update = conn
                .prepare("UPDATE bench SET value = ?2 WHERE id = ?1")
                .map_err(|err| {
                    RunError::Runtime(format!("prepare C SQLite update statement: {err}"))
                })?;
            let t0 = Instant::now();
            for i in 0..update_count {
                let id = i64::try_from(i).map_err(|_| {
                    RunError::Usage("update_count index overflowed i64".to_string())
                })? * 10;
                update
                    .execute(rusqlite::params![id, 999.99])
                    .map_err(|err| RunError::Runtime(format!("update C SQLite row {id}: {err}")))?;
            }
            conn.execute_batch("COMMIT").map_err(|err| {
                RunError::Runtime(format!("commit C SQLite update transaction: {err}"))
            })?;
            total_update_ns += t0.elapsed().as_nanos();
        }

        if args.workload.do_delete() {
            conn.execute_batch("BEGIN").map_err(|err| {
                RunError::Runtime(format!("begin C SQLite delete transaction: {err}"))
            })?;
            let mut delete = conn
                .prepare("DELETE FROM bench WHERE id = ?1")
                .map_err(|err| {
                    RunError::Runtime(format!("prepare C SQLite delete statement: {err}"))
                })?;
            let t0 = Instant::now();
            for i in 0..delete_count {
                let id = i64::try_from(i).map_err(|_| {
                    RunError::Usage("delete_count index overflowed i64".to_string())
                })? * 20;
                delete
                    .execute(rusqlite::params![id])
                    .map_err(|err| RunError::Runtime(format!("delete C SQLite row {id}: {err}")))?;
            }
            conn.execute_batch("COMMIT").map_err(|err| {
                RunError::Runtime(format!("commit C SQLite delete transaction: {err}"))
            })?;
            total_delete_ns += t0.elapsed().as_nanos();
        }

        if iter == 0 {
            eprintln!("  (first sqlite iter complete)");
        }
    }

    Ok(TimingTotals {
        total: t_all.elapsed().as_nanos(),
        populate: total_populate_ns,
        update: total_update_ns,
        delete: total_delete_ns,
    })
}

fn run_sqlite_isolated_benchmark(
    args: &BenchArgs,
    rows_i64: i64,
    update_count: usize,
    delete_count: usize,
) -> Result<TimingTotals, RunError> {
    let t_all = Instant::now();
    let populate_rows_i64 = isolated_populate_rows_i64(args, rows_i64, delete_count)?;
    let conn = rusqlite::Connection::open_in_memory()
        .map_err(|err| RunError::Runtime(format!("open C SQLite in-memory database: {err}")))?;
    apply_csqlite_benchmark_pragmas(&conn)?;
    conn.execute(BENCH_CREATE_SQL, [])
        .map_err(|err| RunError::Runtime(format!("create C SQLite benchmark table: {err}")))?;
    conn.execute_batch("BEGIN")
        .map_err(|err| RunError::Runtime(format!("begin C SQLite populate transaction: {err}")))?;
    let mut stmt = conn
        .prepare(BENCH_INSERT_SQL)
        .map_err(|err| RunError::Runtime(format!("prepare C SQLite populate statement: {err}")))?;
    let t0 = Instant::now();
    for i in 0..populate_rows_i64 {
        stmt.execute(rusqlite::params![i])
            .map_err(|err| RunError::Runtime(format!("populate C SQLite row {i}: {err}")))?;
    }
    conn.execute_batch("COMMIT")
        .map_err(|err| RunError::Runtime(format!("commit C SQLite populate transaction: {err}")))?;
    let total_populate_ns = t0.elapsed().as_nanos();

    let mut total_update_ns: u128 = 0;
    let mut total_delete_ns: u128 = 0;

    if args.workload.do_update() {
        let mut update = conn
            .prepare("UPDATE bench SET value = ?2 WHERE id = ?1")
            .map_err(|err| {
                RunError::Runtime(format!("prepare C SQLite update statement: {err}"))
            })?;
        conn.execute_batch("BEGIN").map_err(|err| {
            RunError::Runtime(format!("begin C SQLite isolated update transaction: {err}"))
        })?;
        let t0 = Instant::now();
        for iter in 0..args.iters {
            let next_value = (iter as f64).mul_add(0.001, 999.99);
            for i in 0..update_count {
                let id = i64::try_from(i).map_err(|_| {
                    RunError::Usage("update_count index overflowed i64".to_string())
                })? * 10;
                update
                    .execute(rusqlite::params![id, next_value])
                    .map_err(|err| RunError::Runtime(format!("update C SQLite row {id}: {err}")))?;
            }
        }
        total_update_ns = t0.elapsed().as_nanos();
        conn.execute_batch("ROLLBACK").map_err(|err| {
            RunError::Runtime(format!(
                "rollback C SQLite isolated update transaction: {err}"
            ))
        })?;
    }

    if args.workload.do_delete() {
        let mut delete = conn
            .prepare("DELETE FROM bench WHERE id = ?1")
            .map_err(|err| {
                RunError::Runtime(format!("prepare C SQLite delete statement: {err}"))
            })?;
        if args.profile_mode == ProfileMode::RollbackIsolated {
            for iter in 0..args.iters {
                conn.execute_batch("BEGIN").map_err(|err| {
                    RunError::Runtime(format!(
                        "begin C SQLite rollback-isolated delete transaction {iter}: {err}"
                    ))
                })?;
                let t0 = Instant::now();
                for i in 0..delete_count {
                    let id = i64::try_from(i).map_err(|_| {
                        RunError::Usage("delete_count index overflowed i64".to_string())
                    })? * 20;
                    delete.execute(rusqlite::params![id]).map_err(|err| {
                        RunError::Runtime(format!("delete C SQLite row {id}: {err}"))
                    })?;
                }
                total_delete_ns += t0.elapsed().as_nanos();
                conn.execute_batch("ROLLBACK").map_err(|err| {
                    RunError::Runtime(format!(
                        "rollback C SQLite rollback-isolated delete transaction {iter}: {err}"
                    ))
                })?;
                if iter == 0 {
                    eprintln!("  (first rollback-isolated sqlite delete iter complete)");
                }
            }
        } else {
            conn.execute_batch("BEGIN").map_err(|err| {
                RunError::Runtime(format!("begin C SQLite isolated delete transaction: {err}"))
            })?;
            let t0 = Instant::now();
            for iter in 0..args.iters {
                for i in 0..delete_count {
                    let id = if args.profile_mode == ProfileMode::SparseIsolated {
                        sparse_isolated_delete_id(iter, i, args.rows)?
                    } else {
                        isolated_delete_id(iter, i, delete_count)?
                    };
                    delete.execute(rusqlite::params![id]).map_err(|err| {
                        RunError::Runtime(format!("delete C SQLite row {id}: {err}"))
                    })?;
                }
                if iter == 0 {
                    eprintln!("  (first isolated sqlite delete iter complete)");
                }
            }
            total_delete_ns = t0.elapsed().as_nanos();
            conn.execute_batch("COMMIT").map_err(|err| {
                RunError::Runtime(format!(
                    "commit C SQLite isolated delete transaction: {err}"
                ))
            })?;
        }
    }

    Ok(TimingTotals {
        total: t_all.elapsed().as_nanos(),
        populate: total_populate_ns,
        update: total_update_ns,
        delete: total_delete_ns,
    })
}

fn print_engine_summary(
    engine: &str,
    args: &BenchArgs,
    update_count: usize,
    delete_count: usize,
    totals: TimingTotals,
) {
    let per_row_update = if args.workload.do_update() {
        per_row_ns(totals.update, update_count, args.iters)
    } else {
        0.0
    };
    let per_row_delete = if args.workload.do_delete() {
        per_row_ns(totals.delete, delete_count, args.iters)
    } else {
        0.0
    };
    eprintln!(
        "{engine}: total={}ms populate={}ms update={}ms delete={}ms  |  \
        per-row-update={per_row_update:.0}ns  per-row-delete={per_row_delete:.0}ns",
        totals.total / 1_000_000,
        totals.populate / 1_000_000,
        totals.update / 1_000_000,
        totals.delete / 1_000_000,
    );
}

fn ratio_or_zero(numerator: u128, denominator: u128) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn print_comparison_summary(args: &BenchArgs, fsqlite: TimingTotals, sqlite: TimingTotals) {
    let update_ratio = if args.workload.do_update() {
        ratio_or_zero(fsqlite.update, sqlite.update)
    } else {
        0.0
    };
    let delete_ratio = if args.workload.do_delete() {
        ratio_or_zero(fsqlite.delete, sqlite.delete)
    } else {
        0.0
    };
    eprintln!(
        "fsqlite/sqlite time ratio: total={:.2}x populate={:.2}x update={update_ratio:.2}x delete={delete_ratio:.2}x",
        ratio_or_zero(fsqlite.total, sqlite.total),
        ratio_or_zero(fsqlite.populate, sqlite.populate),
    );
}

#[cfg(test)]
mod tests {
    use super::{
        BENCH_CREATE_SQL, BENCH_INSERT_SQL, BENCHMARK_PRAGMAS, BenchArgs, DEFAULT_ITERS,
        DEFAULT_ROWS, EngineKind, ProfileMode, RunError, WorkloadKind, isolated_populate_rows_i64,
        parse_args, per_row_ns, run_benchmark, sparse_isolated_delete_id,
    };

    #[test]
    fn parse_args_uses_defaults() {
        assert_eq!(
            parse_args(std::iter::empty::<String>()).unwrap(),
            BenchArgs {
                rows: DEFAULT_ROWS,
                iters: DEFAULT_ITERS,
                workload: WorkloadKind::Both,
                engine: EngineKind::Fsqlite,
                profile_mode: ProfileMode::Standard,
            }
        );
    }

    #[test]
    fn help_request_is_detected_before_positional_parsing() {
        assert!(super::is_help_request(&["--help".to_string()]));
        assert!(super::is_help_request(&["-h".to_string()]));
        assert!(!super::is_help_request(&["100".to_string()]));
    }

    #[test]
    fn parse_args_rejects_invalid_workload() {
        let err = parse_args(["100".to_string(), "2".to_string(), "bogus".to_string()])
            .expect_err("invalid workload should fail");
        assert_eq!(
            err,
            RunError::Usage(
                "invalid workload 'bogus'; expected update, delete, or both".to_string()
            )
        );
    }

    #[test]
    fn parse_args_rejects_zero_iters() {
        let err =
            parse_args(["100".to_string(), "0".to_string()]).expect_err("zero iters should fail");
        assert_eq!(
            err,
            RunError::Usage("iters must be greater than zero".to_string())
        );
    }

    #[test]
    fn per_row_ns_returns_zero_for_zero_ops() {
        assert_eq!(per_row_ns(50_000, 0, 5), 0.0);
        assert_eq!(per_row_ns(50_000, 3, 0), 0.0);
    }

    #[test]
    fn parse_args_accepts_small_row_counts() {
        assert_eq!(
            parse_args(["5".to_string(), "1".to_string(), "update".to_string()]).unwrap(),
            BenchArgs {
                rows: 5,
                iters: 1,
                workload: WorkloadKind::Update,
                engine: EngineKind::Fsqlite,
                profile_mode: ProfileMode::Standard,
            }
        );
    }

    #[test]
    fn parse_args_accepts_compare_engine() {
        assert_eq!(
            parse_args([
                "5".to_string(),
                "1".to_string(),
                "both".to_string(),
                "compare".to_string(),
            ])
            .unwrap(),
            BenchArgs {
                rows: 5,
                iters: 1,
                workload: WorkloadKind::Both,
                engine: EngineKind::Compare,
                profile_mode: ProfileMode::Standard,
            }
        );
    }

    #[test]
    fn parse_args_accepts_isolated_mode() {
        assert_eq!(
            parse_args([
                "5".to_string(),
                "3".to_string(),
                "delete".to_string(),
                "fsqlite".to_string(),
                "isolated".to_string(),
            ])
            .unwrap(),
            BenchArgs {
                rows: 5,
                iters: 3,
                workload: WorkloadKind::Delete,
                engine: EngineKind::Fsqlite,
                profile_mode: ProfileMode::Isolated,
            }
        );
    }

    #[test]
    fn parse_args_accepts_rollback_isolated_mode() {
        assert_eq!(
            parse_args([
                "5".to_string(),
                "3".to_string(),
                "delete".to_string(),
                "fsqlite".to_string(),
                "rollback-isolated".to_string(),
            ])
            .unwrap(),
            BenchArgs {
                rows: 5,
                iters: 3,
                workload: WorkloadKind::Delete,
                engine: EngineKind::Fsqlite,
                profile_mode: ProfileMode::RollbackIsolated,
            }
        );
    }

    #[test]
    fn parse_args_accepts_sparse_isolated_mode() {
        assert_eq!(
            parse_args([
                "10000".to_string(),
                "3".to_string(),
                "delete".to_string(),
                "fsqlite".to_string(),
                "sparse-isolated".to_string(),
            ])
            .unwrap(),
            BenchArgs {
                rows: 10000,
                iters: 3,
                workload: WorkloadKind::Delete,
                engine: EngineKind::Fsqlite,
                profile_mode: ProfileMode::SparseIsolated,
            }
        );
    }

    #[test]
    fn parse_args_accepts_exact_gate_mode() {
        assert_eq!(
            parse_args([
                "1280".to_string(),
                "3".to_string(),
                "delete".to_string(),
                "compare".to_string(),
                "exact-gate".to_string(),
            ])
            .unwrap(),
            BenchArgs {
                rows: 1280,
                iters: 3,
                workload: WorkloadKind::Delete,
                engine: EngineKind::Compare,
                profile_mode: ProfileMode::ExactGate,
            }
        );
    }

    #[test]
    fn parse_args_rejects_non_delete_exact_gate_mode() {
        let err = parse_args([
            "1280".to_string(),
            "3".to_string(),
            "both".to_string(),
            "fsqlite".to_string(),
            "exact-gate".to_string(),
        ])
        .expect_err("exact-gate must not admit a non-delete workload");
        assert_eq!(
            err,
            RunError::Usage("exact-gate mode requires the delete workload".to_string())
        );
    }

    #[test]
    fn parse_args_rejects_invalid_engine() {
        let err = parse_args([
            "100".to_string(),
            "2".to_string(),
            "both".to_string(),
            "bogus".to_string(),
        ])
        .expect_err("invalid engine should fail");
        assert_eq!(
            err,
            RunError::Usage(
                "invalid engine 'bogus'; expected fsqlite, sqlite, or compare".to_string()
            )
        );
    }

    #[test]
    fn parse_args_rejects_invalid_mode() {
        let err = parse_args([
            "100".to_string(),
            "2".to_string(),
            "both".to_string(),
            "fsqlite".to_string(),
            "bogus".to_string(),
        ])
        .expect_err("invalid profile mode should fail");
        assert_eq!(
            err,
            RunError::Usage(
                "invalid mode 'bogus'; expected standard, isolated, rollback-isolated, sparse-isolated, or exact-gate".to_string()
            )
        );
    }

    #[test]
    fn sparse_isolated_delete_ids_keep_standard_sparse_shape_per_block() {
        assert_eq!(sparse_isolated_delete_id(0, 0, 10_000).unwrap(), 0);
        assert_eq!(sparse_isolated_delete_id(0, 1, 10_000).unwrap(), 20);
        assert_eq!(sparse_isolated_delete_id(1, 0, 10_000).unwrap(), 10_000);
        assert_eq!(sparse_isolated_delete_id(1, 499, 10_000).unwrap(), 19_980);
    }

    #[test]
    fn sparse_isolated_populates_enough_rows_for_last_sparse_delete() {
        let args = BenchArgs {
            rows: 10_000,
            iters: 3,
            workload: WorkloadKind::Delete,
            engine: EngineKind::Fsqlite,
            profile_mode: ProfileMode::SparseIsolated,
        };
        assert_eq!(
            isolated_populate_rows_i64(&args, 10_000, 500).unwrap(),
            29_981
        );
    }

    #[test]
    fn benchmark_schema_matches_small_record_workload() {
        assert_eq!(
            BENCH_CREATE_SQL,
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL)"
        );
        assert_eq!(
            BENCH_INSERT_SQL,
            "INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))"
        );
    }

    #[test]
    fn benchmark_pragmas_disable_time_travel_capture() {
        assert!(
            BENCHMARK_PRAGMAS.iter().any(|pragma| pragma
                .eq_ignore_ascii_case("PRAGMA fsqlite_capture_time_travel_snapshots=false;")),
            "perf-update-delete should profile UPDATE/DELETE, not optional time-travel snapshot cloning"
        );
    }

    #[test]
    fn run_benchmark_smoke_small_workload() {
        let args = BenchArgs {
            rows: 5,
            iters: 1,
            workload: WorkloadKind::Both,
            engine: EngineKind::Compare,
            profile_mode: ProfileMode::Standard,
        };
        let result = run_benchmark(&args);
        assert!(result.is_ok(), "small smoke workload failed: {result:?}");
    }

    #[test]
    fn run_benchmark_smoke_rollback_isolated_delete() {
        let args = BenchArgs {
            rows: 20,
            iters: 2,
            workload: WorkloadKind::Delete,
            engine: EngineKind::Compare,
            profile_mode: ProfileMode::RollbackIsolated,
        };
        let result = run_benchmark(&args);
        assert!(
            result.is_ok(),
            "rollback-isolated delete workload failed: {result:?}"
        );
    }

    #[test]
    fn run_benchmark_smoke_sparse_isolated_delete() {
        let args = BenchArgs {
            rows: 20,
            iters: 2,
            workload: WorkloadKind::Delete,
            engine: EngineKind::Compare,
            profile_mode: ProfileMode::SparseIsolated,
        };
        let result = run_benchmark(&args);
        assert!(
            result.is_ok(),
            "sparse-isolated delete workload failed: {result:?}"
        );
    }

    #[test]
    fn run_benchmark_smoke_exact_gate_delete() {
        let args = BenchArgs {
            rows: 20,
            iters: 2,
            workload: WorkloadKind::Delete,
            engine: EngineKind::Compare,
            profile_mode: ProfileMode::ExactGate,
        };
        let result = run_benchmark(&args);
        assert!(
            result.is_ok(),
            "exact-gate delete workload failed: {result:?}"
        );
    }
}
