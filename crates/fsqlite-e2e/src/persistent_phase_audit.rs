use std::time::Duration;

use fsqlite_wal::ConsolidationMetricsSnapshot;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PersistentLatencySummary {
    pub sample_count: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PersistentRetryStageCounts {
    /// Total retry attempts across begin/body/commit stages.
    ///
    /// This intentionally excludes `duplicate_after_retry_exits`, which is a
    /// terminal outcome rather than an additional retry stage.
    pub total_retries: u64,
    pub begin_retries: u64,
    pub body_retries: u64,
    pub commit_retries: u64,
    /// Count of duplicate-row exits after a prior retry likely committed.
    pub duplicate_after_retry_exits: u64,
}

impl PersistentRetryStageCounts {
    pub fn merge(&mut self, other: Self) {
        self.total_retries = self.total_retries.saturating_add(other.total_retries);
        self.begin_retries = self.begin_retries.saturating_add(other.begin_retries);
        self.body_retries = self.body_retries.saturating_add(other.body_retries);
        self.commit_retries = self.commit_retries.saturating_add(other.commit_retries);
        self.duplicate_after_retry_exits = self
            .duplicate_after_retry_exits
            .saturating_add(other.duplicate_after_retry_exits);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PersistentOperationTiming {
    pub wall_time: Duration,
    pub begin_retry_handoff: Duration,
    pub statement_execute_body: Duration,
    pub commit_roundtrip: Duration,
    pub rollback_cleanup: Duration,
    pub retry_backoff_sleep: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentOperationBucketSummary {
    pub total_us: u64,
    pub avg_us_per_operation: u64,
    pub latency_us: PersistentLatencySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentMeasuredCommitBucketSummary {
    pub total_us: u64,
    pub avg_us_per_recorded_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentMeasuredCommitSubBuckets {
    pub recorded_commit_count: u64,
    pub commit_center: PersistentMeasuredCommitBucketSummary,
    pub post_commit_cleanup_publish: PersistentMeasuredCommitBucketSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentResidualSummary {
    pub total_us: i64,
    pub avg_us_per_operation: i64,
    pub abs_fraction_basis_points: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentCommitRoundtripGapSummary {
    /// Signed difference between harness-measured `COMMIT` wall time and the
    /// inner pager phase instrumentation totals for the same recorded commits.
    pub total_us: i64,
    pub avg_us_per_recorded_commit: i64,
    pub abs_fraction_basis_points: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentOperationWallTimeAudit {
    pub operation_count: u64,
    pub wall_time: PersistentOperationBucketSummary,
    pub begin_retry_handoff: PersistentOperationBucketSummary,
    pub statement_execute_body: PersistentOperationBucketSummary,
    pub commit_roundtrip: PersistentOperationBucketSummary,
    pub rollback_cleanup: PersistentOperationBucketSummary,
    pub retry_backoff_sleep: PersistentOperationBucketSummary,
    pub retry_stage_counts: PersistentRetryStageCounts,
    pub measured_commit_sub_buckets: Option<PersistentMeasuredCommitSubBuckets>,
    pub measured_commit_roundtrip_gap: Option<PersistentCommitRoundtripGapSummary>,
    pub accounted_total_us: u64,
    pub residual: PersistentResidualSummary,
}

pub const PERSISTENT_PHASE_COMPONENT_COMPARISON_SCHEMA_V1: &str =
    "fsqlite-e2e.persistent_phase_component_comparison.v1";
pub const PERSISTENT_PHASE_COMPONENT_COMPARISON_SUITE_SCHEMA_V1: &str =
    "fsqlite-e2e.persistent_phase_component_comparison_suite.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentPhaseComponentComparisonRow {
    pub component: String,
    pub sqlite_us_per_operation: Option<u64>,
    pub fsqlite_us_per_operation: Option<u64>,
    pub ratio_vs_sqlite: Option<f64>,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentPhaseComponentComparisonReport {
    pub schema_version: String,
    pub benchmark_group: String,
    pub concurrency: usize,
    pub rows_per_thread: i64,
    pub total_rows: u64,
    pub sqlite_throughput_ops_per_sec: f64,
    pub fsqlite_throughput_ops_per_sec: f64,
    pub flusher_lock_wait_fraction_basis_points: Option<u64>,
    pub lock_topology_limited: Option<bool>,
    pub rows: Vec<PersistentPhaseComponentComparisonRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentPhaseComponentComparisonSuite {
    pub schema_version: String,
    pub reports: Vec<PersistentPhaseComponentComparisonReport>,
}

fn component_ratio_vs_sqlite(sqlite: Option<u64>, fsqlite: Option<u64>) -> Option<f64> {
    match (sqlite, fsqlite) {
        (Some(sqlite), Some(fsqlite)) if sqlite > 0 => Some(fsqlite as f64 / sqlite as f64),
        _ => None,
    }
}

fn component_row(
    component: &str,
    sqlite_us_per_operation: Option<u64>,
    fsqlite_us_per_operation: Option<u64>,
    notes: impl Into<String>,
) -> PersistentPhaseComponentComparisonRow {
    PersistentPhaseComponentComparisonRow {
        component: component.to_owned(),
        sqlite_us_per_operation,
        fsqlite_us_per_operation,
        ratio_vs_sqlite: component_ratio_vs_sqlite(
            sqlite_us_per_operation,
            fsqlite_us_per_operation,
        ),
        notes: notes.into(),
    }
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_persistent_phase_component_comparison_report(
    benchmark_group: &str,
    concurrency: usize,
    rows_per_thread: i64,
    total_rows: u64,
    sqlite_throughput_ops_per_sec: f64,
    fsqlite_throughput_ops_per_sec: f64,
    sqlite_audit: &PersistentOperationWallTimeAudit,
    fsqlite_audit: &PersistentOperationWallTimeAudit,
    flusher_lock_wait_fraction_basis_points: Option<u64>,
    lock_topology_limited: Option<bool>,
) -> PersistentPhaseComponentComparisonReport {
    let mut rows = vec![
        component_row(
            "total_wall",
            Some(sqlite_audit.wall_time.avg_us_per_operation),
            Some(fsqlite_audit.wall_time.avg_us_per_operation),
            "end-to-end wall time per completed operation",
        ),
        component_row(
            "begin_retry_handoff",
            Some(sqlite_audit.begin_retry_handoff.avg_us_per_operation),
            Some(fsqlite_audit.begin_retry_handoff.avg_us_per_operation),
            "transaction start or retry-entry handoff time",
        ),
        component_row(
            "statement_execute_body",
            Some(sqlite_audit.statement_execute_body.avg_us_per_operation),
            Some(fsqlite_audit.statement_execute_body.avg_us_per_operation),
            "statement body execution inside the transaction",
        ),
        component_row(
            "commit_roundtrip",
            Some(sqlite_audit.commit_roundtrip.avg_us_per_operation),
            Some(fsqlite_audit.commit_roundtrip.avg_us_per_operation),
            "outer COMMIT roundtrip seen by the harness",
        ),
        component_row(
            "rollback_cleanup",
            Some(sqlite_audit.rollback_cleanup.avg_us_per_operation),
            Some(fsqlite_audit.rollback_cleanup.avg_us_per_operation),
            "rollback or cleanup work after failed attempts",
        ),
        component_row(
            "retry_backoff_sleep",
            Some(sqlite_audit.retry_backoff_sleep.avg_us_per_operation),
            Some(fsqlite_audit.retry_backoff_sleep.avg_us_per_operation),
            "explicit retry sleep requested by the benchmark harness",
        ),
    ];

    if let Some(sub_buckets) = fsqlite_audit.measured_commit_sub_buckets.as_ref() {
        rows.push(component_row(
            "fsqlite_commit_center",
            None,
            Some(sub_buckets.commit_center.avg_us_per_recorded_commit),
            "FrankenSQLite-only measured WAL commit center from inner phase metrics",
        ));
        rows.push(component_row(
            "fsqlite_post_commit_cleanup_publish",
            None,
            Some(
                sub_buckets
                    .post_commit_cleanup_publish
                    .avg_us_per_recorded_commit,
            ),
            "FrankenSQLite-only measured post-commit cleanup and publish work",
        ));
    }

    PersistentPhaseComponentComparisonReport {
        schema_version: PERSISTENT_PHASE_COMPONENT_COMPARISON_SCHEMA_V1.to_owned(),
        benchmark_group: benchmark_group.to_owned(),
        concurrency,
        rows_per_thread,
        total_rows,
        sqlite_throughput_ops_per_sec,
        fsqlite_throughput_ops_per_sec,
        flusher_lock_wait_fraction_basis_points,
        lock_topology_limited,
        rows,
    }
}

#[must_use]
pub fn render_persistent_phase_component_comparison_suite_markdown(
    suite: &PersistentPhaseComponentComparisonSuite,
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "# Persistent Phase Component Comparison\n");

    if suite.reports.is_empty() {
        let _ = writeln!(
            out,
            "_No paired SQLite/FrankenSQLite persistent-phase samples found._"
        );
        return out;
    }

    for report in &suite.reports {
        let _ = writeln!(
            out,
            "## {} (c={}, rows/thread={}, total_rows={})\n",
            report.benchmark_group, report.concurrency, report.rows_per_thread, report.total_rows
        );
        let _ = writeln!(
            out,
            "- sqlite throughput: {:.2} ops/s",
            report.sqlite_throughput_ops_per_sec
        );
        let _ = writeln!(
            out,
            "- FrankenSQLite throughput: {:.2} ops/s",
            report.fsqlite_throughput_ops_per_sec
        );
        if let Some(lock_wait_bps) = report.flusher_lock_wait_fraction_basis_points {
            let _ = writeln!(
                out,
                "- FrankenSQLite flusher lock wait share: {:.2}%",
                lock_wait_bps as f64 / 100.0
            );
        }
        if let Some(lock_topology_limited) = report.lock_topology_limited {
            let _ = writeln!(
                out,
                "- FrankenSQLite lock-topology limited: {}",
                lock_topology_limited
            );
        }
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "| Component | SQLite (us/op) | FrankenSQLite (us/op) | Ratio | Notes |"
        );
        let _ = writeln!(
            out,
            "|-----------|----------------|-----------------------|-------|-------|"
        );
        for row in &report.rows {
            let sqlite = row
                .sqlite_us_per_operation
                .map_or_else(|| "-".to_owned(), |value| value.to_string());
            let fsqlite = row
                .fsqlite_us_per_operation
                .map_or_else(|| "-".to_owned(), |value| value.to_string());
            let ratio = row
                .ratio_vs_sqlite
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.2}x"));
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} |",
                row.component, sqlite, fsqlite, ratio, row.notes
            );
        }
        let _ = writeln!(out);
    }

    out
}

#[must_use]
pub fn duration_micros_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn sum_duration_micros(iter: impl IntoIterator<Item = Duration>) -> u64 {
    iter.into_iter().fold(0_u64, |acc, duration| {
        acc.saturating_add(duration_micros_u64(duration))
    })
}

#[must_use]
pub fn persistent_latency_summary(sorted: &[Duration]) -> PersistentLatencySummary {
    PersistentLatencySummary {
        sample_count: u64::try_from(sorted.len()).unwrap_or(u64::MAX),
        p50_us: duration_micros_u64(percentile(sorted, 50.0)),
        p95_us: duration_micros_u64(percentile(sorted, 95.0)),
        p99_us: duration_micros_u64(percentile(sorted, 99.0)),
        max_us: duration_micros_u64(sorted.last().copied().unwrap_or(Duration::ZERO)),
    }
}

fn summarize_operation_bucket(
    operation_timings: &[PersistentOperationTiming],
    bucket: impl Fn(&PersistentOperationTiming) -> Duration,
) -> PersistentOperationBucketSummary {
    let mut samples: Vec<Duration> = operation_timings.iter().map(bucket).collect();
    samples.sort();
    let total_us = sum_duration_micros(samples.iter().copied());
    let operation_count = u64::try_from(samples.len()).unwrap_or(u64::MAX);
    PersistentOperationBucketSummary {
        total_us,
        avg_us_per_operation: total_us.checked_div(operation_count).unwrap_or(0),
        latency_us: persistent_latency_summary(&samples),
    }
}

fn signed_total_delta_us(left: u64, right: u64) -> i64 {
    let delta = i128::from(left) - i128::from(right);
    delta.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn abs_i64_as_u64(value: i64) -> u64 {
    value.unsigned_abs()
}

fn residual_fraction_basis_points(total_us: u64, residual_us: i64) -> Option<u64> {
    (total_us > 0).then_some(abs_i64_as_u64(residual_us).saturating_mul(10_000) / total_us)
}

pub fn sleep_with_accounting(operation_timing: &mut PersistentOperationTiming, duration: Duration) {
    std::thread::sleep(duration);
    operation_timing.retry_backoff_sleep += duration;
}

#[must_use]
pub fn build_measured_commit_sub_buckets(
    metrics: &ConsolidationMetricsSnapshot,
) -> Option<PersistentMeasuredCommitSubBuckets> {
    let recorded_commit_count = metrics.commit_phase_count;
    if recorded_commit_count == 0 {
        return None;
    }
    let commit_center_total_us = metrics
        .commit_phase_a_us_total
        .saturating_add(metrics.commit_phase_b_us_total);
    let post_commit_cleanup_publish_total_us = metrics
        .commit_phase_c1_us_total
        .saturating_add(metrics.commit_phase_c2_us_total);
    Some(PersistentMeasuredCommitSubBuckets {
        recorded_commit_count,
        commit_center: PersistentMeasuredCommitBucketSummary {
            total_us: commit_center_total_us,
            avg_us_per_recorded_commit: commit_center_total_us
                .checked_div(recorded_commit_count)
                .unwrap_or(0),
        },
        post_commit_cleanup_publish: PersistentMeasuredCommitBucketSummary {
            total_us: post_commit_cleanup_publish_total_us,
            avg_us_per_recorded_commit: post_commit_cleanup_publish_total_us
                .checked_div(recorded_commit_count)
                .unwrap_or(0),
        },
    })
}

#[must_use]
pub fn build_operation_wall_time_audit(
    operation_timings: &[PersistentOperationTiming],
    retry_stage_counts: PersistentRetryStageCounts,
    measured_commit_sub_buckets: Option<PersistentMeasuredCommitSubBuckets>,
) -> PersistentOperationWallTimeAudit {
    let wall_time = summarize_operation_bucket(operation_timings, |timing| timing.wall_time);
    let begin_retry_handoff =
        summarize_operation_bucket(operation_timings, |timing| timing.begin_retry_handoff);
    let statement_execute_body =
        summarize_operation_bucket(operation_timings, |timing| timing.statement_execute_body);
    let commit_roundtrip =
        summarize_operation_bucket(operation_timings, |timing| timing.commit_roundtrip);
    let rollback_cleanup =
        summarize_operation_bucket(operation_timings, |timing| timing.rollback_cleanup);
    let retry_backoff_sleep =
        summarize_operation_bucket(operation_timings, |timing| timing.retry_backoff_sleep);
    let measured_commit_total_us = measured_commit_sub_buckets
        .as_ref()
        .map(|sub_buckets| {
            sub_buckets
                .commit_center
                .total_us
                .saturating_add(sub_buckets.post_commit_cleanup_publish.total_us)
        })
        .unwrap_or(0);
    let accounted_total_us = begin_retry_handoff
        .total_us
        .saturating_add(statement_execute_body.total_us)
        .saturating_add(commit_roundtrip.total_us)
        .saturating_add(rollback_cleanup.total_us)
        .saturating_add(retry_backoff_sleep.total_us);
    let residual_total_us = signed_total_delta_us(wall_time.total_us, accounted_total_us);
    let operation_count = u64::try_from(operation_timings.len()).unwrap_or(u64::MAX);
    let measured_commit_roundtrip_gap = measured_commit_sub_buckets.as_ref().map(|sub_buckets| {
        PersistentCommitRoundtripGapSummary {
            total_us: signed_total_delta_us(commit_roundtrip.total_us, measured_commit_total_us),
            avg_us_per_recorded_commit: signed_total_delta_us(
                commit_roundtrip.total_us,
                measured_commit_total_us,
            )
            .checked_div(i64::try_from(sub_buckets.recorded_commit_count).unwrap_or(i64::MAX))
            .unwrap_or(0),
            abs_fraction_basis_points: residual_fraction_basis_points(
                commit_roundtrip.total_us,
                signed_total_delta_us(commit_roundtrip.total_us, measured_commit_total_us),
            ),
        }
    });
    let residual = PersistentResidualSummary {
        total_us: residual_total_us,
        avg_us_per_operation: residual_total_us
            .checked_div(i64::try_from(operation_count).unwrap_or(i64::MAX))
            .unwrap_or(0),
        abs_fraction_basis_points: residual_fraction_basis_points(
            wall_time.total_us,
            residual_total_us,
        ),
    };

    PersistentOperationWallTimeAudit {
        operation_count,
        wall_time,
        begin_retry_handoff,
        statement_execute_body,
        commit_roundtrip,
        rollback_cleanup,
        retry_backoff_sleep,
        retry_stage_counts,
        measured_commit_sub_buckets,
        measured_commit_roundtrip_gap,
        accounted_total_us,
        residual,
    }
}

#[must_use]
pub fn format_operation_wall_time_audit(audit: &PersistentOperationWallTimeAudit) -> String {
    let measured_commit = audit.measured_commit_sub_buckets.as_ref().map_or_else(
        || {
            "commit_center_avg=n/a post_commit_avg=n/a commit_gap_avg=n/a commit_gap_abs_bp=n/a"
                .to_owned()
        },
        |sub_buckets| {
            let commit_gap = audit.measured_commit_roundtrip_gap.as_ref().map_or_else(
                || "n/a".to_owned(),
                |gap| {
                    format!(
                        "{}us commit_gap_abs_bp={}",
                        gap.avg_us_per_recorded_commit,
                        gap.abs_fraction_basis_points.unwrap_or(0),
                    )
                },
            );
            format!(
                "commit_center_avg={}us post_commit_avg={}us commit_gap_avg={}",
                sub_buckets.commit_center.avg_us_per_recorded_commit,
                sub_buckets
                    .post_commit_cleanup_publish
                    .avg_us_per_recorded_commit,
                commit_gap,
            )
        },
    );
    format!(
        "ops={} wall_avg={}us begin_avg={}us execute_avg={}us commit_avg={}us rollback_avg={}us {} backoff_avg={}us retry_stage={{begin:{}, body:{}, commit:{}, total:{}}} duplicate_after_retry_exits={} accounted_total={}us residual_total={}us residual_avg={}us residual_abs_bp={}",
        audit.operation_count,
        audit.wall_time.avg_us_per_operation,
        audit.begin_retry_handoff.avg_us_per_operation,
        audit.statement_execute_body.avg_us_per_operation,
        audit.commit_roundtrip.avg_us_per_operation,
        audit.rollback_cleanup.avg_us_per_operation,
        measured_commit,
        audit.retry_backoff_sleep.avg_us_per_operation,
        audit.retry_stage_counts.begin_retries,
        audit.retry_stage_counts.body_retries,
        audit.retry_stage_counts.commit_retries,
        audit.retry_stage_counts.total_retries,
        audit.retry_stage_counts.duplicate_after_retry_exits,
        audit.accounted_total_us,
        audit.residual.total_us,
        audit.residual.avg_us_per_operation,
        audit.residual.abs_fraction_basis_points.unwrap_or(0),
    )
}

#[must_use]
pub fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::{
        PERSISTENT_PHASE_COMPONENT_COMPARISON_SUITE_SCHEMA_V1, PersistentCommitRoundtripGapSummary,
        PersistentLatencySummary, PersistentMeasuredCommitBucketSummary,
        PersistentMeasuredCommitSubBuckets, PersistentOperationBucketSummary,
        PersistentOperationTiming, PersistentOperationWallTimeAudit,
        PersistentPhaseComponentComparisonSuite, PersistentResidualSummary,
        PersistentRetryStageCounts, build_measured_commit_sub_buckets,
        build_operation_wall_time_audit, build_persistent_phase_component_comparison_report,
        render_persistent_phase_component_comparison_suite_markdown,
    };
    use fsqlite_wal::{ConsolidationMetricsSnapshot, PhasePercentiles, WakeReasonSnapshot};
    use std::time::Duration;

    fn micros(value: u64) -> Duration {
        Duration::from_micros(value)
    }

    fn latency_summary(avg_us: u64) -> PersistentLatencySummary {
        PersistentLatencySummary {
            sample_count: 2,
            p50_us: avg_us,
            p95_us: avg_us,
            p99_us: avg_us,
            max_us: avg_us,
        }
    }

    fn bucket_summary(avg_us_per_operation: u64) -> PersistentOperationBucketSummary {
        PersistentOperationBucketSummary {
            total_us: avg_us_per_operation.saturating_mul(2),
            avg_us_per_operation,
            latency_us: latency_summary(avg_us_per_operation),
        }
    }

    fn comparison_audit(
        wall_time: u64,
        begin_retry_handoff: u64,
        statement_execute_body: u64,
        commit_roundtrip: u64,
        rollback_cleanup: u64,
        retry_backoff_sleep: u64,
        measured_commit_sub_buckets: Option<PersistentMeasuredCommitSubBuckets>,
    ) -> PersistentOperationWallTimeAudit {
        PersistentOperationWallTimeAudit {
            operation_count: 2,
            wall_time: bucket_summary(wall_time),
            begin_retry_handoff: bucket_summary(begin_retry_handoff),
            statement_execute_body: bucket_summary(statement_execute_body),
            commit_roundtrip: bucket_summary(commit_roundtrip),
            rollback_cleanup: bucket_summary(rollback_cleanup),
            retry_backoff_sleep: bucket_summary(retry_backoff_sleep),
            retry_stage_counts: PersistentRetryStageCounts::default(),
            measured_commit_sub_buckets,
            measured_commit_roundtrip_gap: Some(PersistentCommitRoundtripGapSummary {
                total_us: 0,
                avg_us_per_recorded_commit: 0,
                abs_fraction_basis_points: Some(0),
            }),
            accounted_total_us: wall_time.saturating_mul(2),
            residual: PersistentResidualSummary {
                total_us: 0,
                avg_us_per_operation: 0,
                abs_fraction_basis_points: Some(0),
            },
        }
    }

    #[test]
    fn operation_wall_time_audit_keeps_commit_roundtrip_separate_from_measured_commit() {
        let timings = vec![
            PersistentOperationTiming {
                wall_time: micros(100),
                begin_retry_handoff: micros(10),
                statement_execute_body: micros(20),
                commit_roundtrip: micros(40),
                rollback_cleanup: micros(5),
                retry_backoff_sleep: Duration::ZERO,
            },
            PersistentOperationTiming {
                wall_time: micros(120),
                begin_retry_handoff: micros(20),
                statement_execute_body: micros(30),
                commit_roundtrip: micros(50),
                rollback_cleanup: micros(10),
                retry_backoff_sleep: micros(5),
            },
        ];
        let audit = build_operation_wall_time_audit(
            &timings,
            PersistentRetryStageCounts {
                total_retries: 4,
                begin_retries: 1,
                body_retries: 1,
                commit_retries: 2,
                duplicate_after_retry_exits: 1,
            },
            Some(PersistentMeasuredCommitSubBuckets {
                recorded_commit_count: 2,
                commit_center: PersistentMeasuredCommitBucketSummary {
                    total_us: 30,
                    avg_us_per_recorded_commit: 15,
                },
                post_commit_cleanup_publish: PersistentMeasuredCommitBucketSummary {
                    total_us: 20,
                    avg_us_per_recorded_commit: 10,
                },
            }),
        );

        assert_eq!(audit.commit_roundtrip.total_us, 90);
        assert_eq!(audit.rollback_cleanup.total_us, 15);
        assert_eq!(audit.accounted_total_us, 190);
        assert_eq!(audit.residual.total_us, 30);
        assert_eq!(
            audit
                .measured_commit_roundtrip_gap
                .as_ref()
                .expect("measured commit gap should exist")
                .total_us,
            40
        );
        assert_eq!(audit.retry_stage_counts.total_retries, 4);
        assert_eq!(audit.retry_stage_counts.duplicate_after_retry_exits, 1);
    }

    #[test]
    fn measured_commit_sub_buckets_split_commit_center_and_post_commit() {
        let metrics = ConsolidationMetricsSnapshot {
            groups_flushed: 0,
            frames_consolidated: 0,
            transactions_batched: 0,
            fsyncs_total: 0,
            flush_duration_us_total: 0,
            wait_duration_us_total: 0,
            max_group_size_observed: 0,
            busy_retries: 0,
            prepare_us_total: 0,
            batch_build_us_total: 0,
            conflict_snapshot_us_total: 0,
            lane_prepare_us_total: 0,
            consolidator_lock_wait_us_total: 0,
            consolidator_flushing_wait_us_total: 0,
            flusher_arrival_wait_us_total: 0,
            inner_lock_wait_us_total: 0,
            exclusive_lock_us_total: 0,
            wal_append_us_total: 0,
            flush_frame_prep_us_total: 0,
            append_conflict_check_us_total: 0,
            append_frames_us_total: 0,
            wal_sync_us_total: 0,
            waiter_epoch_wait_us_total: 0,
            flusher_commits: 0,
            waiter_commits: 0,
            commit_phase_a_us_total: 30,
            commit_phase_b_us_total: 60,
            commit_phase_c1_us_total: 15,
            commit_phase_c2_us_total: 9,
            commit_phase_count: 3,
            hist_consolidator_lock_wait: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_arrival_wait: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_wal_backend_lock_wait: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_wal_append: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_exclusive_lock: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_waiter_epoch_wait: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_phase_b: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_wal_sync: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            hist_full_commit: PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            },
            wake_reasons: WakeReasonSnapshot::default(),
            flush_batch_size_histogram: [0; fsqlite_wal::FLUSH_BATCH_SIZE_BUCKETS],
        };

        let sub_buckets =
            build_measured_commit_sub_buckets(&metrics).expect("sub-buckets should exist");

        assert_eq!(sub_buckets.recorded_commit_count, 3);
        assert_eq!(sub_buckets.commit_center.total_us, 90);
        assert_eq!(sub_buckets.commit_center.avg_us_per_recorded_commit, 30);
        assert_eq!(sub_buckets.post_commit_cleanup_publish.total_us, 24);
        assert_eq!(
            sub_buckets
                .post_commit_cleanup_publish
                .avg_us_per_recorded_commit,
            8
        );
    }

    #[test]
    fn persistent_phase_component_comparison_report_keeps_common_and_fsqlite_only_rows() {
        let sqlite_audit = comparison_audit(18, 2, 6, 7, 2, 1, None);
        let fsqlite_audit = comparison_audit(
            54,
            5,
            14,
            21,
            6,
            3,
            Some(PersistentMeasuredCommitSubBuckets {
                recorded_commit_count: 2,
                commit_center: PersistentMeasuredCommitBucketSummary {
                    total_us: 20,
                    avg_us_per_recorded_commit: 10,
                },
                post_commit_cleanup_publish: PersistentMeasuredCommitBucketSummary {
                    total_us: 8,
                    avg_us_per_recorded_commit: 4,
                },
            }),
        );

        let report = build_persistent_phase_component_comparison_report(
            "persistent_concurrent_write_16t",
            16,
            1000,
            16_000,
            8_000.0,
            3_000.0,
            &sqlite_audit,
            &fsqlite_audit,
            Some(1750),
            Some(true),
        );

        assert_eq!(report.benchmark_group, "persistent_concurrent_write_16t");
        assert_eq!(report.rows.len(), 8);
        assert_eq!(report.rows[0].component, "total_wall");
        assert_eq!(report.rows[0].sqlite_us_per_operation, Some(18));
        assert_eq!(report.rows[0].fsqlite_us_per_operation, Some(54));
        assert_eq!(report.rows[0].ratio_vs_sqlite, Some(3.0));
        assert_eq!(report.rows[6].component, "fsqlite_commit_center");
        assert_eq!(report.rows[6].sqlite_us_per_operation, None);
        assert_eq!(report.rows[6].fsqlite_us_per_operation, Some(10));
        assert_eq!(
            report.rows[7].component,
            "fsqlite_post_commit_cleanup_publish"
        );
        assert_eq!(report.flusher_lock_wait_fraction_basis_points, Some(1750));
        assert_eq!(report.lock_topology_limited, Some(true));
    }

    #[test]
    fn persistent_phase_component_comparison_markdown_renders_table_and_metadata() {
        let suite = PersistentPhaseComponentComparisonSuite {
            schema_version: PERSISTENT_PHASE_COMPONENT_COMPARISON_SUITE_SCHEMA_V1.to_owned(),
            reports: vec![build_persistent_phase_component_comparison_report(
                "persistent_concurrent_write_8t",
                8,
                1000,
                8_000,
                6_500.0,
                2_100.0,
                &comparison_audit(15, 2, 5, 6, 1, 1, None),
                &comparison_audit(30, 4, 8, 12, 3, 2, None),
                Some(900),
                Some(false),
            )],
        };

        let markdown = render_persistent_phase_component_comparison_suite_markdown(&suite);

        assert!(markdown.contains("# Persistent Phase Component Comparison"));
        assert!(markdown.contains(
            "## persistent_concurrent_write_8t (c=8, rows/thread=1000, total_rows=8000)"
        ));
        assert!(markdown.contains("- sqlite throughput: 6500.00 ops/s"));
        assert!(markdown.contains("- FrankenSQLite throughput: 2100.00 ops/s"));
        assert!(markdown.contains("- FrankenSQLite flusher lock wait share: 9.00%"));
        assert!(
            markdown
                .contains("| Component | SQLite (us/op) | FrankenSQLite (us/op) | Ratio | Notes |")
        );
        assert!(markdown.contains(
            "| total_wall | 15 | 30 | 2.00x | end-to-end wall time per completed operation |"
        ));
    }
}
