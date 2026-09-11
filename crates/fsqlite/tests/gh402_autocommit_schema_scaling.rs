//! GH#402: file-backed autocommit statement cost grows super-linearly with
//! schema size (8 ms -> 320 ms per CREATE at 600 objects; 340 ms per
//! single-row autocommit INSERT; flat-cheap inside a transaction; :memory:
//! unaffected; cliff between 200 and 300 objects).
//!
//! Two surfaces live here:
//!   * `gh402_measure_autocommit_schema_scaling` — measurement harness
//!     (`--ignored`): prints per-window timings plus hot-path counter deltas
//!     so the super-linear term is attributed, not guessed.
//!   * keeper tests — bounded-work assertions that fail if the per-autocommit
//!     cost regresses back to O(schema) re-materialization.

use fsqlite::{Connection, SqliteValue};
use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
#[cfg(feature = "bench-internals")]
use fsqlite_pager::{PagerCommitProfileSnapshot, pager_commit_profile_snapshot};
use std::path::Path;
use std::time::Instant;

const DEFAULT_TABLES: usize = 300; // 600 schema objects: past the reported 200-300 object cliff.
const WINDOW: usize = 50;

#[cfg(feature = "bench-internals")]
mod cost_diagnostics {
    use std::fmt::Write as _;

    use fsqlite_core::connection::{
        AutocheckpointCapture, AutocheckpointSnapshot, CatalogRowidScanCapture,
        CatalogRowidScanSnapshot,
    };
    use fsqlite_pager::page_cache::s3_fifo_reconstruction_diagnostics::{
        CALLERS, Capture as ReconstructionCapture, Tally,
    };

    const FIELDS: [&str; 12] = [
        "attempts",
        "models_built",
        "empty_refusals",
        "oversized_refusals",
        "resident_pages",
        "trace_entries",
        "replayed_accesses",
        "replayed_insertions",
        "completion_rounds",
        "resident_keys_scanned",
        "missing_page_insertions",
        "exhausted_budgets",
    ];

    fn values(tally: Tally) -> [usize; 12] {
        [
            tally.attempts,
            tally.models_built,
            tally.empty_refusals,
            tally.oversized_refusals,
            tally.resident_pages,
            tally.trace_entries,
            tally.replayed_accesses,
            tally.replayed_insertions,
            tally.completion_rounds,
            tally.resident_keys_scanned,
            tally.missing_page_insertions,
            tally.exhausted_budgets,
        ]
    }

    fn catalog_values(snapshot: CatalogRowidScanSnapshot) -> [u64; 4] {
        [
            snapshot.started_scans,
            snapshot.completed_scans,
            snapshot.rowid_visits,
            snapshot.elapsed_ns,
        ]
    }

    const AUTOCHECKPOINT_FIELDS: [&str; 19] = [
        "entered_calls",
        "completed_calls",
        "incomplete_calls",
        "elapsed_ns",
        "skipped_non_wal",
        "skipped_private_memory",
        "skipped_active_concurrent",
        "skipped_disabled",
        "skipped_below_threshold",
        "skipped_write_pressure",
        "context_refusals",
        "returned_after_pager",
        "pager_attempts",
        "pager_complete",
        "pager_partial",
        "pager_busy",
        "pager_errors",
        "pager_incomplete",
        "pager_elapsed_ns",
    ];

    fn autocheckpoint_values(snapshot: AutocheckpointSnapshot) -> [u64; 19] {
        [
            snapshot.entered_calls,
            snapshot.completed_calls,
            snapshot.incomplete_calls,
            snapshot.elapsed_ns,
            snapshot.skipped_non_wal,
            snapshot.skipped_private_memory,
            snapshot.skipped_active_concurrent,
            snapshot.skipped_disabled,
            snapshot.skipped_below_threshold,
            snapshot.skipped_write_pressure,
            snapshot.context_refusals,
            snapshot.returned_after_pager,
            snapshot.pager_attempts,
            snapshot.pager_complete,
            snapshot.pager_partial,
            snapshot.pager_busy,
            snapshot.pager_errors,
            snapshot.pager_incomplete,
            snapshot.pager_elapsed_ns,
        ]
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    pub struct Phase {
        pub s3: [[usize; 12]; 4],
        pub catalog: [u64; 4],
        pub autocheckpoint: [u64; 19],
    }

    impl Phase {
        fn assert_settled(&self) {
            let counts = &self.autocheckpoint;
            assert_eq!(counts[0], counts[1] + counts[2]);
            assert_eq!(counts[1], counts[4..12].iter().sum::<u64>());
            assert_eq!(counts[12], counts[13..18].iter().sum::<u64>());
            assert_eq!(counts[2], 0, "successful phases have no unfinished calls");
            assert_eq!(
                counts[17], 0,
                "successful phases have no unfinished pager awaits"
            );
            assert_eq!(
                counts[11], counts[12],
                "each settled pager attempt returned"
            );
            assert!(
                counts[3] >= counts[18],
                "pager time is within scheduler time"
            );
            if counts[12] == 0 {
                assert_eq!(counts[18], 0, "no pager time without a pager attempt");
            }
            if counts[0] == 0 {
                assert_eq!(counts[3], 0, "no scheduler time without a call");
            }
        }

        pub(super) fn assert_one_autocheckpoint(
            &self,
            terminal: &str,
            pager_outcome: Option<&str>,
        ) {
            assert!(AUTOCHECKPOINT_FIELDS[4..12].contains(&terminal));
            if let Some(outcome) = pager_outcome {
                assert!(AUTOCHECKPOINT_FIELDS[13..17].contains(&outcome));
            }
            assert_eq!(terminal == "returned_after_pager", pager_outcome.is_some());
            self.assert_settled();
            for (field, name) in AUTOCHECKPOINT_FIELDS.into_iter().enumerate() {
                if matches!(name, "elapsed_ns" | "pager_elapsed_ns") {
                    continue;
                }
                let expected = u64::from(
                    matches!(name, "entered_calls" | "completed_calls")
                        || name == terminal
                        || (name == "pager_attempts" && pager_outcome.is_some())
                        || pager_outcome == Some(name),
                );
                assert_eq!(self.autocheckpoint[field], expected, "{name}: {self:?}");
            }
        }
    }

    pub struct Capture {
        reconstruction: ReconstructionCapture,
        catalog: CatalogRowidScanCapture,
        autocheckpoint: AutocheckpointCapture,
        previous: Phase,
        sum: Phase,
    }

    impl Capture {
        pub(super) fn start() -> Self {
            let catalog = CatalogRowidScanCapture::start().expect("start catalog capture");
            let autocheckpoint =
                AutocheckpointCapture::start().expect("start autocheckpoint capture");
            Self {
                reconstruction: ReconstructionCapture::start(),
                catalog,
                autocheckpoint,
                previous: Phase::default(),
                sum: Phase::default(),
            }
        }

        pub(super) fn catalog_snapshot(&self) -> CatalogRowidScanSnapshot {
            self.catalog.snapshot()
        }

        /// Read the immediate phase's elapsed value before reporting. The outer
        /// schema timer still includes per-window diagnostic overhead; all
        /// feature-enabled timings are a separate population from acceptance.
        /// Inclusive scheduler/pager elapsed times are not CPU self time, and
        /// observation overhead may change the time-based scheduling decisions.
        pub(super) fn report(&mut self, label: &str, phase: &str, sample: Option<usize>) -> Phase {
            let current = Phase {
                s3: self.reconstruction.snapshot().map(values),
                catalog: catalog_values(self.catalog.snapshot()),
                autocheckpoint: autocheckpoint_values(self.autocheckpoint.snapshot()),
            };
            current.assert_settled();
            let sample = sample.map_or_else(|| "none".to_owned(), |n| n.to_string());
            let mut delta = Phase::default();
            for (index, caller) in CALLERS.into_iter().enumerate() {
                let mut line = format!(
                    "[gh402-cost] {label} phase={phase} sample={sample} caller={}",
                    caller.name()
                );
                for (field, name) in FIELDS.into_iter().enumerate() {
                    let value = current.s3[index][field]
                        .checked_sub(self.previous.s3[index][field])
                        .expect("monotonic reconstruction counter");
                    delta.s3[index][field] = value;
                    self.sum.s3[index][field] += value;
                    write!(&mut line, " {name}={value}").expect("format diagnostic record");
                }
                println!("{line}");
            }
            for (field, value) in delta.catalog.iter_mut().enumerate() {
                *value = current.catalog[field]
                    .checked_sub(self.previous.catalog[field])
                    .expect("monotonic catalog counter");
                self.sum.catalog[field] += *value;
            }
            println!(
                "[gh402-cost] {label} phase={phase} sample={sample} caller=catalog \
                 started_scans={} completed_scans={} rowid_visits={} scan_elapsed_ns={}",
                delta.catalog[0], delta.catalog[1], delta.catalog[2], delta.catalog[3]
            );
            let mut line =
                format!("[gh402-cost] {label} phase={phase} sample={sample} caller=autocheckpoint");
            for (field, name) in AUTOCHECKPOINT_FIELDS.into_iter().enumerate() {
                let value = current.autocheckpoint[field]
                    .checked_sub(self.previous.autocheckpoint[field])
                    .expect("monotonic autocheckpoint counter");
                delta.autocheckpoint[field] = value;
                self.sum.autocheckpoint[field] += value;
                write!(&mut line, " {name}={value}").expect("format diagnostic record");
            }
            delta.assert_settled();
            println!("{line}");
            self.previous = current;
            delta
        }

        pub(super) fn finish(self) {
            let actual = Phase {
                s3: self.reconstruction.finish().map(values),
                catalog: catalog_values(self.catalog.finish()),
                autocheckpoint: autocheckpoint_values(self.autocheckpoint.finish()),
            };
            actual.assert_settled();
            assert_eq!(
                actual, self.sum,
                "every observed call belongs to a reported phase"
            );
        }
    }
}

fn table_count() -> usize {
    std::env::var("FSQLITE_GH402_TABLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TABLES)
}

fn file_kb(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len() / 1024).unwrap_or(0)
}

#[derive(Clone, Copy, Default)]
struct Deltas {
    memdb_refresh: u64,
    schema_full_scans: u64,
    parse_ns: u64,
    compile_ns: u64,
    schema_full_reloads: u64,
    schema_light: u64,
    schema_refresh_ns: u64,
    pager_pub: u64,
    begin_setup_ns: u64,
    execute_body_ns: u64,
    commit_pre_txn_ns: u64,
    commit_roundtrip_ns: u64,
    commit_finalize_seq_ns: u64,
    commit_handle_finalize_ns: u64,
    commit_post_maint_ns: u64,
    finalize_post_publish_ns: u64,
    wal_frames_written: u64,
    checkpoint_frames_backfilled: u64,
    checkpoint_duration_us: u64,
    #[cfg(feature = "bench-internals")]
    pager_commit: PagerCommitProfileSnapshot,
}

fn snapshot_deltas() -> Deltas {
    let snap = hot_path_profile_snapshot();
    let wal = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
    Deltas {
        memdb_refresh: snap.memdb_refresh_count,
        schema_full_scans: snap.memdb_txn_schema_full_scans,
        parse_ns: snap.parser.parse_time_ns,
        compile_ns: snap.parser.compile_time_ns,
        schema_full_reloads: snap.prepared_schema_full_reloads,
        schema_light: snap.prepared_schema_lightweight_refreshes,
        schema_refresh_ns: snap.prepared_schema_refresh_time_ns,
        pager_pub: snap.pager_publication_refreshes,
        begin_setup_ns: snap.begin_setup_time_ns,
        execute_body_ns: snap.execute_body_time_ns,
        commit_pre_txn_ns: snap.commit_pre_txn_time_ns,
        commit_roundtrip_ns: snap.commit_txn_roundtrip_time_ns,
        commit_finalize_seq_ns: snap.commit_finalize_seq_time_ns,
        commit_handle_finalize_ns: snap.commit_handle_finalize_time_ns,
        commit_post_maint_ns: snap.commit_post_write_maintenance_time_ns,
        finalize_post_publish_ns: snap.finalize_post_publish_time_ns,
        wal_frames_written: wal.frames_written_total,
        checkpoint_frames_backfilled: wal.checkpoint_frames_backfilled_total,
        checkpoint_duration_us: wal.checkpoint_duration_us_total,
        #[cfg(feature = "bench-internals")]
        pager_commit: snap.pager_commit,
    }
}

/// Reports existing process-global selected-path counters, not a complete
/// commit decomposition. `commit_calls` counts full write-path entries, including
/// failed attempts; read-only, clean and reconciliation early exits bypass it.
/// Phase A includes preparation and waiting. WAL time surrounds the group-commit
/// call; the current WAL path finishes through an unprofiled authorization tail
/// before the non-WAL phase-C counters. Zero phase-C time does not prove no work.
/// Phase-C metadata includes file-size and unlock time. The connection commit
/// timer contains these selected pager intervals; maintenance and checkpointing
/// happen later. Durations include waits, are not CPU self time, and must not be
/// summed across overlapping metadata subintervals or their enclosing timers.
///
/// The public integration harness enables these counters through its existing
/// hot-path profile switch and resets them before each schema case. Samples are
/// individual relaxed atomic loads, not an atomic or caller-owned snapshot; use
/// the existing serial diagnostic invocation. A counter decrease, such as a reset
/// or wrap, fails subtraction instead of fabricating zero work. This adds no reset
/// or engine instrumentation and stays outside paired performance acceptance.
#[cfg(feature = "bench-internals")]
fn pager_commit_report(
    label: &str,
    phase: &str,
    sample: Option<usize>,
    boundary: &str,
    before: &PagerCommitProfileSnapshot,
    after: &PagerCommitProfileSnapshot,
) {
    let sample = sample.map_or_else(|| "none".to_owned(), |value| value.to_string());
    let delta = |after: u64, before: u64| {
        after
            .checked_sub(before)
            .expect("pager commit profile reset or wrapped within a reported interval")
    };
    println!(
        "[gh402-pager-commit] {label} phase={phase} sample={sample} boundary={boundary} \
         scope=process_global coverage=selected_paths \
         commit_calls={} phase_a_time_ns={} wal_commit_time_ns={} \
         memory_flush_time_ns={} journal_commit_time_ns={} phase_c_metadata_time_ns={} \
         file_size_time_ns={} unlock_time_ns={} publish_time_ns={} cache_finish_time_ns={}",
        delta(after.commit_calls, before.commit_calls),
        delta(after.phase_a_time_ns, before.phase_a_time_ns),
        delta(after.wal_commit_time_ns, before.wal_commit_time_ns),
        delta(after.memory_flush_time_ns, before.memory_flush_time_ns),
        delta(after.journal_commit_time_ns, before.journal_commit_time_ns),
        delta(
            after.phase_c_metadata_time_ns,
            before.phase_c_metadata_time_ns
        ),
        delta(after.file_size_time_ns, before.file_size_time_ns),
        delta(after.unlock_time_ns, before.unlock_time_ns),
        delta(after.publish_time_ns, before.publish_time_ns),
        delta(after.cache_finish_time_ns, before.cache_finish_time_ns),
    );
}

#[allow(clippy::cast_precision_loss)]
fn window_report(
    label: &str,
    window_tables: usize,
    elapsed_ms: u128,
    before: &Deltas,
    after: &Deltas,
    db_path: &Path,
    #[cfg(feature = "bench-internals")] pager_phase: (&str, Option<usize>),
) {
    let ms = |a: u64, b: u64| (a - b) / 1_000_000;
    let wal = db_path.with_extension("db-wal");
    println!(
        "[gh402] {label} tables={window_tables:>4} window_ms={elapsed_ms:>7} \
         refresh={}/{}fs parse_ms={} compile_ms={} sched_reload={}f/{}l refresh_ms={} pub={} \
         begin_ms={} body_ms={} commit_ms={}p/{}r/{}s/{}h/{}m/{}pp db_kb={} wal_kb={}",
        after.memdb_refresh - before.memdb_refresh,
        after.schema_full_scans - before.schema_full_scans,
        ms(after.parse_ns, before.parse_ns),
        ms(after.compile_ns, before.compile_ns),
        after.schema_full_reloads - before.schema_full_reloads,
        after.schema_light - before.schema_light,
        ms(after.schema_refresh_ns, before.schema_refresh_ns),
        after.pager_pub - before.pager_pub,
        ms(after.begin_setup_ns, before.begin_setup_ns),
        ms(after.execute_body_ns, before.execute_body_ns),
        ms(after.commit_pre_txn_ns, before.commit_pre_txn_ns),
        ms(after.commit_roundtrip_ns, before.commit_roundtrip_ns),
        ms(after.commit_finalize_seq_ns, before.commit_finalize_seq_ns),
        ms(
            after.commit_handle_finalize_ns,
            before.commit_handle_finalize_ns
        ),
        ms(after.commit_post_maint_ns, before.commit_post_maint_ns),
        ms(
            after.finalize_post_publish_ns,
            before.finalize_post_publish_ns
        ),
        file_kb(db_path),
        file_kb(&wal),
    );
    println!(
        "[gh402] {label} tables={window_tables} wal_frames_written={} checkpoint_frames_backfilled={} checkpoint_us={}",
        after.wal_frames_written - before.wal_frames_written,
        after.checkpoint_frames_backfilled - before.checkpoint_frames_backfilled,
        after.checkpoint_duration_us - before.checkpoint_duration_us,
    );
    #[cfg(feature = "bench-internals")]
    pager_commit_report(
        &format!("{label} window_tables={window_tables}"),
        pager_phase.0,
        pager_phase.1,
        "window",
        &before.pager_commit,
        &after.pager_commit,
    );
}

async fn create_schema_autocommit(
    conn: &Connection,
    tables: usize,
    label: &str,
    db_path: &Path,
    #[cfg(feature = "bench-internals")] mut cost: Option<&mut cost_diagnostics::Capture>,
) -> Vec<u128> {
    let mut window_times = Vec::new();
    let mut window_start = Instant::now();
    let mut before = snapshot_deltas();
    let mut parses_before = conn.schema_reload_parse_count();
    for i in 0..tables {
        conn.execute(&format!(
            "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);"
        ))
        .await
        .expect("create table");
        conn.execute(&format!("CREATE INDEX idx_t{i}_a ON t{i}(a);"))
            .await
            .expect("create index");
        if (i + 1) % WINDOW == 0 {
            let elapsed = window_start.elapsed().as_millis();
            let after = snapshot_deltas();
            window_report(
                label,
                i + 1,
                elapsed,
                &before,
                &after,
                db_path,
                #[cfg(feature = "bench-internals")]
                (&format!("ddl_window_{}", i + 1), None),
            );
            let parses_after = conn.schema_reload_parse_count();
            println!(
                "[gh402] {label} tables={} stored_schema_parse_delta={}",
                i + 1,
                parses_after - parses_before
            );
            parses_before = parses_after;
            window_times.push(elapsed);
            #[cfg(feature = "bench-internals")]
            if let Some(cost) = cost.as_deref_mut() {
                cost.report(label, &format!("ddl_window_{}", i + 1), None);
            }
            before = after;
            window_start = Instant::now();
        }
    }
    window_times
}

/// Measurement harness for GH#402. Run explicitly:
/// `cargo test -p fsqlite --test gh402_autocommit_schema_scaling -- --ignored --nocapture`
#[test]
#[ignore = "GH#402 measurement harness; prints timings and counter deltas"]
fn gh402_measure_autocommit_schema_scaling() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_seq.db");
        let conn = Connection::open(path.to_str().unwrap())
            .await
            .expect("open");
        let windows = create_schema_autocommit(
            &conn,
            table_count(),
            "seq",
            &path,
            #[cfg(feature = "bench-internals")]
            None,
        )
        .await;
        println!("[gh402] seq window_ms trace: {windows:?}");
        println!(
            "[gh402] seq hydration_count={} after schema build",
            conn.memdb_row_hydration_count()
        );

        // 20 autocommit single-row INSERTs after the schema exists.
        let before = snapshot_deltas();
        let t = Instant::now();
        for _ in 0..20 {
            conn.execute("INSERT INTO t0 (a) VALUES ('x');")
                .await
                .expect("insert");
        }
        let after = snapshot_deltas();
        window_report(
            "autocommit-inserts",
            20,
            t.elapsed().as_millis(),
            &before,
            &after,
            &path,
            #[cfg(feature = "bench-internals")]
            ("inserts_txn_false", None),
        );

        // Same 20 INSERTs inside one transaction.
        let before = snapshot_deltas();
        let t = Instant::now();
        conn.execute("BEGIN IMMEDIATE;").await.expect("begin");
        for _ in 0..20 {
            conn.execute("INSERT INTO t0 (a) VALUES ('y');")
                .await
                .expect("insert");
        }
        conn.execute("COMMIT;").await.expect("commit");
        let after = snapshot_deltas();
        window_report(
            "txn-inserts",
            20,
            t.elapsed().as_millis(),
            &before,
            &after,
            &path,
            #[cfg(feature = "bench-internals")]
            ("inserts_txn_true", None),
        );

        // Reopen + first statement.
        let t = Instant::now();
        let c2 = Connection::open(path.to_str().unwrap())
            .await
            .expect("reopen");
        let _ = c2.execute("SELECT 1;").await;
        println!(
            "[gh402] reopen+first statement: {} ms",
            t.elapsed().as_millis()
        );

        set_hot_path_profile_enabled(false);
    });
}

/// Current residual matrix: keep storage mode, schema size and transaction
/// shape separate. Timings are observations, never machine-specific pass bars.
/// Enable `fsqlite/bench-internals` for SQL-phase catalog scans, reconstruction
/// work and automatic-checkpoint outcomes. Observation overhead may change
/// time-based scheduling; this remains separate from paired performance acceptance.
#[test]
#[ignore = "GH#402 residual measurement matrix; run explicitly with --nocapture"]
fn gh402_measure_residual_schema_matrix() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        let maximum = table_count().max(6);
        for tables in [maximum / 6, maximum / 2, maximum] {
            for file_backed in [false, true] {
                for ddl_mode in ["seq", "batch", "txn"] {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let path = dir.path().join("residual.db");
                    let target = if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    };
                    let label = format!(
                        "residual storage={} mode={ddl_mode} tables={tables}",
                        if file_backed { "file" } else { "memory" },
                    );
                    #[cfg(feature = "bench-internals")]
                    let mut cost = cost_diagnostics::Capture::start();
                    let conn = Connection::open(target).await.expect("open");
                    #[cfg(feature = "bench-internals")]
                    cost.report(&label, "initial_open", None);
                    reset_hot_path_profile();
                    #[cfg(feature = "bench-internals")]
                    let catalog_before = cost.catalog_snapshot();
                    let started = Instant::now();
                    if ddl_mode == "txn" {
                        conn.execute("BEGIN IMMEDIATE;").await.expect("begin DDL");
                    }
                    if ddl_mode == "batch" {
                        let mut sql = String::new();
                        for i in 0..tables {
                            sql.push_str(&format!(
                                "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);\n\
                                 CREATE INDEX idx_t{i}_a ON t{i}(a);\n"
                            ));
                        }
                        let before = snapshot_deltas();
                        conn.execute_batch(&sql).await.expect("batch DDL");
                        window_report(
                            &label,
                            tables,
                            started.elapsed().as_millis(),
                            &before,
                            &snapshot_deltas(),
                            &path,
                            #[cfg(feature = "bench-internals")]
                            ("ddl_batch", None),
                        );
                        #[cfg(feature = "bench-internals")]
                        cost.report(&label, "ddl_batch", None);
                    } else {
                        create_schema_autocommit(
                            &conn,
                            tables,
                            &label,
                            &path,
                            #[cfg(feature = "bench-internals")]
                            Some(&mut cost),
                        )
                        .await;
                    }
                    // The final DDL window ends before an explicit COMMIT.
                    // Capture that call separately; do not charge it to the
                    // last window or invent missing executor/frame deltas.
                    #[cfg(feature = "bench-internals")]
                    let ddl_commit_before = (ddl_mode == "txn").then(pager_commit_profile_snapshot);
                    if ddl_mode == "txn" {
                        conn.execute("COMMIT;").await.expect("commit DDL");
                    }
                    #[cfg(feature = "bench-internals")]
                    let ddl_commit_profile =
                        ddl_commit_before.map(|before| (before, pager_commit_profile_snapshot()));
                    println!(
                        "[gh402] {label} schema_total_us={} stored_schema_parses={}",
                        started.elapsed().as_micros(),
                        conn.schema_reload_parse_count()
                    );
                    #[cfg(feature = "bench-internals")]
                    {
                        cost.report(&label, "ddl_complete", None);
                        let catalog_after = cost.catalog_snapshot();
                        let inserts = u64::try_from(tables * 2).unwrap();
                        assert_eq!(
                            catalog_after.started_scans - catalog_before.started_scans,
                            inserts
                        );
                        assert_eq!(
                            catalog_after.completed_scans - catalog_before.completed_scans,
                            inserts
                        );
                        assert_eq!(
                            catalog_after.rowid_visits - catalog_before.rowid_visits,
                            inserts * (inserts - 1) / 2,
                            "actual catalog visits for a fresh table/index-pair schema"
                        );
                        if let Some((before, after)) = ddl_commit_profile {
                            // Print after the existing elapsed read and phase
                            // reports. This is the COMMIT subset of ddl_complete;
                            // it does not include BEGIN or reporting overhead.
                            pager_commit_report(
                                &label,
                                "ddl_complete",
                                None,
                                "explicit_commit",
                                &before,
                                &after,
                            );
                        }
                    }

                    let stock = rusqlite::Connection::open_in_memory().unwrap();
                    for i in 0..tables {
                        stock
                            .execute_batch(&format!(
                                "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);
                                 CREATE INDEX idx_t{i}_a ON t{i}(a);"
                            ))
                            .unwrap();
                    }
                    for (in_transaction, value) in [(false, "x"), (true, "y")] {
                        let before = snapshot_deltas();
                        let started = Instant::now();
                        if in_transaction {
                            conn.execute("BEGIN IMMEDIATE;")
                                .await
                                .expect("begin INSERTs");
                        }
                        for _ in 0..20 {
                            conn.execute(&format!("INSERT INTO t0 (a) VALUES ('{value}');"))
                                .await
                                .expect("insert");
                        }
                        if in_transaction {
                            conn.execute("COMMIT;").await.expect("commit INSERTs");
                        }
                        window_report(
                            &format!("{label} inserts_txn={in_transaction}"),
                            tables,
                            started.elapsed().as_millis(),
                            &before,
                            &snapshot_deltas(),
                            &path,
                            #[cfg(feature = "bench-internals")]
                            (&format!("inserts_txn_{in_transaction}"), None),
                        );
                        #[cfg(feature = "bench-internals")]
                        cost.report(&label, &format!("inserts_txn_{in_transaction}"), None);
                        for _ in 0..20 {
                            stock
                                .execute("INSERT INTO t0 (a) VALUES (?1)", [value])
                                .unwrap();
                        }
                    }
                    let sql = "SELECT id, a FROM t0 ORDER BY id;";
                    let expected: Vec<(i64, String)> = stock
                        .prepare(sql)
                        .unwrap()
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                        .unwrap()
                        .collect::<rusqlite::Result<_>>()
                        .unwrap();
                    let actual = conn.query(sql).await.unwrap();
                    assert_eq!(actual.len(), expected.len());
                    for (actual, (id, value)) in actual.iter().zip(&expected) {
                        assert_eq!(
                            actual.values(),
                            &[
                                SqliteValue::Integer(*id),
                                SqliteValue::Text(value.clone().into())
                            ]
                        );
                    }
                    let count = conn
                        .query_row(
                            "SELECT count(*) FROM sqlite_schema WHERE type IN ('table', 'index');",
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        count.values(),
                        &[SqliteValue::Integer(i64::try_from(tables * 2).unwrap())]
                    );
                    #[cfg(feature = "bench-internals")]
                    cost.report(&label, "stock_and_catalog_validation", None);

                    for sample in 0..3 {
                        let started = Instant::now();
                        let stats = conn.memory_stats().expect("memory_stats");
                        println!(
                            "[gh402] {label} sample={sample} memory_stats_us={} cached_pages={} page_size={}",
                            started.elapsed().as_micros(),
                            stats.page_cache.cached_pages,
                            stats.page_size_bytes
                        );
                        #[cfg(feature = "bench-internals")]
                        {
                            let phase = cost.report(&label, "memory_stats", Some(sample));
                            assert_eq!(phase.s3[0][0], 1, "one statistics reconstruction attempt");
                            assert_eq!(phase.s3[0][4], stats.page_cache.cached_pages);
                            assert!(phase.s3[1..].iter().all(|row| *row == [0; 12]));
                            assert_eq!(phase.catalog, [0; 4]);
                        }
                        if file_backed {
                            let before = snapshot_deltas();
                            let started = Instant::now();
                            let peer = Connection::open(target).await.expect("peer open");
                            let open_us = started.elapsed().as_micros();
                            #[cfg(feature = "bench-internals")]
                            cost.report(&label, "reopen", Some(sample));
                            let started = Instant::now();
                            let row = peer.query_row("SELECT count(*) FROM t0;").await.unwrap();
                            let first_us = started.elapsed().as_micros();
                            #[cfg(feature = "bench-internals")]
                            cost.report(&label, "first_statement", Some(sample));
                            assert_eq!(row.values(), &[SqliteValue::Integer(40)]);
                            println!(
                                "[gh402] {label} sample={sample} reopen_us={open_us} first_statement_us={first_us} stored_schema_parses={}",
                                peer.schema_reload_parse_count()
                            );
                            window_report(
                                &format!("{label} reopen-profile"),
                                tables,
                                0,
                                &before,
                                &snapshot_deltas(),
                                &path,
                                #[cfg(feature = "bench-internals")]
                                ("reopen_and_first_statement", Some(sample)),
                            );
                            peer.close().await.expect("close peer");
                            #[cfg(feature = "bench-internals")]
                            cost.report(&label, "peer_close", Some(sample));
                        }
                    }
                    conn.close().await.expect("close");
                    #[cfg(feature = "bench-internals")]
                    {
                        cost.report(&label, "writer_close", None);
                        cost.finish();
                    }
                }
            }
        }
        set_hot_path_profile_enabled(false);
    });
}

/// Real SQL phases must be distinguishable from the expensive statistics call.
/// The complete diagnostic matrix above records larger schemas independently.
#[cfg(feature = "bench-internals")]
#[test]
fn gh402_cost_capture_attributes_statistics_separately_from_sql() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cost_capture.db");
            let target = if file_backed {
                path.to_str().unwrap()
            } else {
                ":memory:"
            };
            let label = format!("keeper file_backed={file_backed}");
            let mut cost = cost_diagnostics::Capture::start();
            let conn = Connection::open(target).await.unwrap();
            assert_eq!(
                cost.report(&label, "initial_open", None),
                cost_diagnostics::Phase::default()
            );
            conn.execute_batch(
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT); \
                 CREATE INDEX a_v ON a(v); \
                 CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT); \
                 CREATE INDEX b_v ON b(v);",
            )
            .await
            .unwrap();
            let ddl = cost.report(&label, "ddl", None);
            assert_eq!(ddl.s3, [[0; 12]; 4]);
            assert_eq!(ddl.catalog[..3], [4, 4, 6]);
            conn.execute("INSERT INTO a(v) VALUES ('kept');")
                .await
                .unwrap();
            assert_eq!(
                conn.query_row("SELECT v FROM a WHERE id = 1;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Text("kept".into())],
            );
            let inserted = cost.report(&label, "insert_and_read", None);
            assert_eq!(inserted.s3, [[0; 12]; 4]);
            assert_eq!(inserted.catalog, [0; 4]);
            let stats = conn.memory_stats().unwrap();
            let memory = cost.report(&label, "memory_stats", None);
            assert_eq!(memory.s3[0][0], 1);
            assert_eq!(memory.s3[0][1], 1);
            assert_eq!(memory.s3[0][4], stats.page_cache.cached_pages);
            assert!(memory.s3[0][8] > 0, "completion loop actually executed");
            assert!(memory.s3[1..].iter().all(|row| *row == [0; 12]));
            assert_eq!(memory.catalog, [0; 4]);
            assert_eq!(memory.autocheckpoint, [0; 19]);
            if file_backed {
                let peer = Connection::open(target).await.unwrap();
                assert_eq!(
                    cost.report(&label, "reopen", None),
                    cost_diagnostics::Phase::default()
                );
                assert_eq!(
                    peer.query_row("SELECT count(*) FROM a;")
                        .await
                        .unwrap()
                        .values(),
                    &[SqliteValue::Integer(1)],
                );
                assert_eq!(
                    cost.report(&label, "first_statement", None),
                    cost_diagnostics::Phase::default()
                );
                peer.close().await.unwrap();
                assert_eq!(
                    cost.report(&label, "peer_close", None),
                    cost_diagnostics::Phase::default()
                );
            }
            conn.close().await.unwrap();
            assert_eq!(
                cost.report(&label, "writer_close", None),
                cost_diagnostics::Phase::default()
            );
            cost.finish();
        }
    });
}

#[cfg(all(feature = "bench-internals", feature = "native"))]
fn assert_autocheckpoint_stock_rows(path: &Path, expected: &[(i64, String)]) {
    let stock = rusqlite::Connection::open(path).unwrap();
    let actual: Vec<(i64, String)> = stock
        .prepare("SELECT id, v FROM captured ORDER BY id;")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        stock
            .query_row("PRAGMA integrity_check;", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

/// An explicit memory commit reaches maintenance even when retained autocommit
/// would defer it. Private memory normalizes WAL requests to its public memory
/// mode, so both memory and off use the non-WAL exit. The defensive private-WAL
/// exit has diagnostic-record coverage only; this test does not reach it.
#[cfg(feature = "bench-internals")]
#[test]
fn gh402_autocheckpoint_capture_reports_memory_and_non_wal_skips() {
    asupersync::test_utils::run_test(|| async {
        for (mode, reported_mode) in [("wal", "memory"), ("off", "off")] {
            let conn = Connection::open(":memory:").await.unwrap();
            assert_eq!(
                conn.query_row(&format!("PRAGMA journal_mode={mode};"))
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Text(reported_mode.into())],
            );
            conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            let mut cost = cost_diagnostics::Capture::start();
            conn.execute("BEGIN IMMEDIATE;").await.unwrap();
            conn.execute("INSERT INTO captured VALUES (1, 'kept');")
                .await
                .unwrap();
            conn.execute("COMMIT;").await.unwrap();
            cost.report(
                &format!("autocheckpoint memory mode={mode}"),
                "commit",
                None,
            )
            .assert_one_autocheckpoint("skipped_non_wal", None);
            cost.finish();
            assert_eq!(
                conn.query_row("SELECT id, v FROM captured;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(1), SqliteValue::Text("kept".into())],
            );
            conn.close().await.unwrap();
        }
    });
}

/// Threshold controls observe real automatic checkpoint calls, not explicit
/// PRAGMA checkpoints. Urgency removes write-rate timing from the success case.
#[cfg(all(feature = "bench-internals", feature = "native"))]
#[test]
fn gh402_autocheckpoint_capture_reports_disabled_threshold_and_success() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("autocheckpoint_thresholds.db");
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        assert_eq!(
            conn.query_row("PRAGMA journal_mode=WAL;")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Text("wal".into())],
        );
        conn.execute("PRAGMA wal_autocheckpoint=0;").await.unwrap();
        conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        assert_eq!(
            conn.query_row("PRAGMA checkpoint_schedule=FULL;")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Text("FULL".into())],
        );
        for (id, threshold, urgent, terminal, pager_outcome) in [
            (1, 0, 1, "skipped_disabled", None),
            (2, 1_000_000, 1_000_000, "skipped_below_threshold", None),
            (3, 1, 1, "returned_after_pager", Some("pager_complete")),
        ] {
            assert_eq!(
                conn.query_row(&format!("PRAGMA wal_autocheckpoint={threshold};"))
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(threshold)],
            );
            assert_eq!(
                conn.query_row(&format!("PRAGMA checkpoint_urgent_wal_frames={urgent};"))
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(urgent)],
            );
            let mut cost = cost_diagnostics::Capture::start();
            conn.execute("BEGIN IMMEDIATE;").await.unwrap();
            conn.execute(&format!("INSERT INTO captured VALUES ({id}, 'kept-{id}');"))
                .await
                .unwrap();
            conn.execute("COMMIT;").await.unwrap();
            cost.report("autocheckpoint thresholds", terminal, None)
                .assert_one_autocheckpoint(terminal, pager_outcome);
            cost.finish();
        }
        let rows = conn
            .query("SELECT id, v FROM captured ORDER BY id;")
            .await
            .unwrap();
        for (row, id) in rows.iter().zip(1..=3) {
            assert_eq!(
                row.values(),
                &[
                    SqliteValue::Integer(id),
                    SqliteValue::Text(format!("kept-{id}").into()),
                ],
            );
        }
        assert_eq!(rows.len(), 3);
        conn.close().await.unwrap();
        assert_autocheckpoint_stock_rows(
            &path,
            &(1..=3)
                .map(|id| (id, format!("kept-{id}")))
                .collect::<Vec<_>>(),
        );
    });
}

/// A default BEGIN reader blocks at the concurrent-registry check. An opted-out
/// reader still owns a pager transaction, so exclusive maintenance returns Busy.
/// This is a same-process admission control, not native-lock or cancellation proof.
#[cfg(all(feature = "bench-internals", feature = "native"))]
#[test]
fn gh402_autocheckpoint_capture_distinguishes_reader_skip_from_pager_busy() {
    asupersync::test_utils::run_test(|| async {
        for reader_concurrent in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("autocheckpoint_reader.db");
            let target = path.to_str().unwrap();
            let writer = Connection::open(target).await.unwrap();
            assert_eq!(
                writer
                    .query_row("PRAGMA journal_mode=WAL;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Text("wal".into())],
            );
            writer
                .execute("PRAGMA wal_autocheckpoint=0;")
                .await
                .unwrap();
            writer
                .execute_batch(
                    "CREATE TABLE captured (id INTEGER PRIMARY KEY, v TEXT); \
                     INSERT INTO captured VALUES (1, 'kept-1');",
                )
                .await
                .unwrap();
            assert_eq!(
                writer
                    .query_row("PRAGMA fsqlite.concurrent_mode;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(1)],
            );
            let reader = Connection::open(target).await.unwrap();
            if !reader_concurrent {
                reader
                    .execute("PRAGMA fsqlite.concurrent_mode=OFF;")
                    .await
                    .unwrap();
            }
            assert_eq!(
                reader
                    .query_row("PRAGMA fsqlite.concurrent_mode;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(i64::from(reader_concurrent))],
            );
            reader.execute("BEGIN;").await.unwrap();
            assert_eq!(
                reader
                    .query_row("SELECT count(*) FROM captured;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(1)],
            );
            writer
                .execute("PRAGMA wal_autocheckpoint=1;")
                .await
                .unwrap();
            writer
                .query("PRAGMA checkpoint_urgent_wal_frames=1;")
                .await
                .unwrap();
            writer
                .query("PRAGMA checkpoint_schedule=RESTART;")
                .await
                .unwrap();
            let mut cost = cost_diagnostics::Capture::start();
            writer.execute("BEGIN IMMEDIATE;").await.unwrap();
            writer
                .execute("INSERT INTO captured VALUES (2, 'kept-2');")
                .await
                .unwrap();
            writer.execute("COMMIT;").await.unwrap();
            let phase = cost.report(
                &format!("autocheckpoint reader_concurrent={reader_concurrent}"),
                "reader_held",
                None,
            );
            if reader_concurrent {
                phase.assert_one_autocheckpoint("skipped_active_concurrent", None);
            } else {
                phase.assert_one_autocheckpoint("returned_after_pager", Some("pager_busy"));
            }
            cost.finish();
            assert_eq!(
                reader
                    .query_row("SELECT count(*) FROM captured;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(1)],
                "the reader keeps its pre-commit snapshot"
            );
            reader.execute("ROLLBACK;").await.unwrap();
            reader.close().await.unwrap();
            let mut cost = cost_diagnostics::Capture::start();
            writer.execute("BEGIN IMMEDIATE;").await.unwrap();
            writer
                .execute("INSERT INTO captured VALUES (3, 'kept-3');")
                .await
                .unwrap();
            writer.execute("COMMIT;").await.unwrap();
            cost.report("autocheckpoint reader", "reader_released", None)
                .assert_one_autocheckpoint("returned_after_pager", Some("pager_complete"));
            cost.finish();
            assert_eq!(
                writer
                    .query_row("SELECT count(*), sum(id) FROM captured;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(3), SqliteValue::Integer(6)],
            );
            writer.close().await.unwrap();
            assert_autocheckpoint_stock_rows(
                &path,
                &(1..=3)
                    .map(|id| (id, format!("kept-{id}")))
                    .collect::<Vec<_>>(),
            );
        }
    });
}

/// GH#402 keeper: total checkpoint backfill work across a file-backed
/// autocommit DDL loop must stay linear in the frames actually written.
///
/// Pre-fix, every post-commit autocheckpoint restarted from WAL frame 0
/// (`SimplePager::checkpoint` passed `backfilled_frames = 0`) and the trigger
/// keyed on raw WAL length, so once the WAL crossed the adaptive target every
/// autocommit statement re-walked the whole WAL: backfilled-frames grew
/// quadratically (observed ratio >40x at this scale). Post-fix the adapter's
/// generation-tagged watermark resumes where the last checkpoint stopped, so
/// cumulative backfill stays within a small multiple of frames written.
///
/// The counters are process-global, so concurrent tests can only ADD linear
/// noise to both sides of the inequality; the quadratic signature this guards
/// against exceeds the bound by more than an order of magnitude.
#[test]
fn gh402_autocommit_checkpoint_backfill_work_is_linear() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_keeper.db");
        let conn = Connection::open(path.to_str().unwrap())
            .await
            .expect("open");

        let before = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        const KEEPER_TABLES: usize = 250; // 500 objects: safely past the cliff.
        for i in 0..KEEPER_TABLES {
            conn.execute(&format!(
                "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);"
            ))
            .await
            .expect("create table");
            conn.execute(&format!("CREATE INDEX idx_t{i}_a ON t{i}(a);"))
                .await
                .expect("create index");
        }
        let after = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();

        let written = after
            .frames_written_total
            .saturating_sub(before.frames_written_total);
        let backfilled = after
            .checkpoint_frames_backfilled_total
            .saturating_sub(before.checkpoint_frames_backfilled_total);
        println!(
            "[gh402-keeper] frames_written_delta={written} checkpoint_backfilled_delta={backfilled}"
        );
        assert!(
            backfilled <= written.saturating_mul(3).saturating_add(4_000),
            "checkpoint backfill work is super-linear again (GH#402): \
             {backfilled} frames backfilled for {written} frames written — \
             autocheckpoints are re-walking the whole WAL per autocommit statement"
        );
        conn.close().await.expect("close");
    });
}

/// GH#402 keeper: the checkpoint-scheduling change must not weaken
/// cross-connection schema visibility. Connection A holds warm caches; B (a
/// separate connection on the same file) commits DDL + a row; A must observe
/// both without any manual refresh — the schema-cookie / visible-commit-seq
/// staleness check is the guard that any skip-refresh fast path has to pass.
#[test]
fn gh402_cross_connection_schema_change_remains_visible() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_visibility.db");
        let path = path.to_str().unwrap();

        let conn_a = Connection::open(path).await.expect("open A");
        conn_a
            .execute("CREATE TABLE seed (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("seed table");
        conn_a
            .execute("INSERT INTO seed (v) VALUES ('warm');")
            .await
            .expect("seed row");
        // Warm A's prepared/schema caches.
        let rows = conn_a
            .query("SELECT v FROM seed;")
            .await
            .expect("warm read");
        assert_eq!(rows.len(), 1);

        let conn_b = Connection::open(path).await.expect("open B");
        conn_b
            .execute("CREATE TABLE from_b (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("B ddl");
        conn_b
            .execute("INSERT INTO from_b (v) VALUES ('peer');")
            .await
            .expect("B row");

        // A must see B's committed schema object and its row.
        let rows = conn_a
            .query("SELECT v FROM from_b;")
            .await
            .expect("A must see B's new table");
        assert_eq!(rows.len(), 1, "A must see B's committed row");
        conn_b.close().await.expect("close B");
        conn_a.close().await.expect("close A");
    });
}

/// PR#401 invariant keeper (reimplemented in-house, GH#402 companion): a
/// schema-only open never bulk-hydrates file rows into `MemDatabase` through
/// the prepared-query MemDB fast path, and the read-only variant still
/// refuses writes. The gate derives from the open mode (the schema-only
/// family), not a caller-set flag.
#[test]
fn gh402_schema_only_prepared_reads_stay_pager_backed() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_schema_only.db");
        let path = path.to_str().unwrap();

        // Build a small canonical table with an index through a normal open.
        let writer = Connection::open(path).await.expect("open writer");
        writer
            .execute("CREATE TABLE canon (id INTEGER PRIMARY KEY, k TEXT NOT NULL, v TEXT);")
            .await
            .expect("ddl");
        writer
            .execute("CREATE INDEX idx_canon_k ON canon(k);")
            .await
            .expect("index");
        writer.execute("BEGIN;").await.expect("begin");
        for i in 0..64 {
            writer
                .execute(&format!(
                    "INSERT INTO canon (k, v) VALUES ('k{i}', 'v{i}');"
                ))
                .await
                .expect("insert");
        }
        writer.execute("COMMIT;").await.expect("commit");
        writer.close().await.expect("close writer");

        // Read-only schema-only open: parameterized prepared lookup must
        // answer from the pager without hydrating MemDatabase rows.
        let reader = Connection::open_schema_only(path)
            .await
            .expect("schema-only open");
        let stmt = reader
            .prepare("SELECT v FROM canon WHERE k = ?1;")
            .await
            .expect("prepare");
        for i in [3_usize, 41, 3] {
            let rows = stmt
                .query_with_params(&[SqliteValue::from(format!("k{i}"))])
                .await
                .expect("prepared lookup");
            assert_eq!(rows.len(), 1, "lookup k{i} must find its row");
        }
        assert_eq!(
            reader.memdb_row_hydration_count(),
            0,
            "schema-only prepared reads must stay pager-backed \
             (PR#401 invariant): the MemDB fast path bulk-hydrated the file"
        );
        assert!(
            reader
                .execute("INSERT INTO canon (k, v) VALUES ('nope', 'nope');")
                .await
                .is_err(),
            "read-only schema-only open must refuse writes"
        );
        reader.close().await.expect("close reader");
    });
}
