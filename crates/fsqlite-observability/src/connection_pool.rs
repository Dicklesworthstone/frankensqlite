use serde::Serialize;

/// High-level workload shape used for pool-sizing guidance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConnectionPoolWorkloadProfile {
    ReadHeavy,
    Mixed,
    WriteHeavy,
}

/// Point-in-time lifecycle summary for one connection in a pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionLifecycleSnapshot {
    pub connection_id: u64,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub open_transactions: u32,
    pub active_snapshot_age_ms: Option<u64>,
    pub queries_executed: u64,
    pub prepare_calls: u64,
}

/// Aggregate input to the connection-pool validator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionPoolTelemetrySample {
    pub workload_profile: ConnectionPoolWorkloadProfile,
    pub cpu_cores: usize,
    pub configured_pool_size: usize,
    pub observed_active_connections: usize,
    pub peak_concurrent_checkout_requests: usize,
    pub concurrent_writers: usize,
    pub connect_events: u64,
    pub disconnect_events: u64,
    pub measurement_window_ms: u64,
    pub connections: Vec<ConnectionLifecycleSnapshot>,
}

impl ConnectionPoolTelemetrySample {
    /// Total queries observed across every tracked connection.
    #[must_use]
    pub fn total_queries(&self) -> u64 {
        self.connections
            .iter()
            .map(|conn| conn.queries_executed)
            .sum()
    }

    /// Total prepare calls observed across every tracked connection.
    #[must_use]
    pub fn total_prepare_calls(&self) -> u64 {
        self.connections.iter().map(|conn| conn.prepare_calls).sum()
    }

    /// Number of tracked connections idle longer than `threshold_ms`.
    #[must_use]
    pub fn stale_connection_count(&self, threshold_ms: u64) -> usize {
        self.connections
            .iter()
            .filter(|conn| conn.idle_ms >= threshold_ms)
            .count()
    }

    /// Number of long-idle connections that still retain an open snapshot or
    /// transaction context.
    #[must_use]
    pub fn stale_snapshot_holder_count(
        &self,
        idle_threshold_ms: u64,
        snapshot_threshold_ms: u64,
    ) -> usize {
        self.connections
            .iter()
            .filter(|conn| {
                conn.idle_ms >= idle_threshold_ms
                    && (conn.open_transactions > 0
                        || conn
                            .active_snapshot_age_ms
                            .is_some_and(|age| age >= snapshot_threshold_ms))
            })
            .count()
    }

    /// Per-minute connection turnover, using the smaller of connect/disconnect
    /// counts as the completed recycle count.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn turnover_per_minute(&self) -> f64 {
        if self.measurement_window_ms == 0 {
            return 0.0;
        }
        let completed_cycles = self.connect_events.min(self.disconnect_events) as f64;
        completed_cycles * 60_000.0 / self.measurement_window_ms as f64
    }

    /// Fraction of queries that paid a prepare cost during the measurement
    /// window, reported as a percentage.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn prepare_ratio_percent(&self) -> f64 {
        let total_queries = self.total_queries();
        if total_queries == 0 {
            return 0.0;
        }
        (self.total_prepare_calls() as f64 * 100.0) / total_queries as f64
    }
}

/// Severity assigned to a detected pool anti-pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum ConnectionPoolSeverity {
    Info,
    Warn,
    Critical,
}

/// Concrete anti-pattern or advisability finding emitted by the validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConnectionPoolPattern {
    SingleConnectionSerializedWriters,
    OverPooling,
    StaleIdleSnapshot,
    ConnectionThrashing,
    UnpreparedHotLoop,
}

/// One validator finding with evidence and a confidence score.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectionPoolFinding {
    pub pattern: ConnectionPoolPattern,
    pub severity: ConnectionPoolSeverity,
    pub confidence_score: f64,
    pub summary: String,
    pub evidence: Vec<String>,
}

/// Summary metrics included in every validation report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectionPoolSummary {
    pub configured_pool_size: usize,
    pub observed_connection_count: usize,
    pub observed_active_connections: usize,
    pub peak_concurrent_checkout_requests: usize,
    pub concurrent_writers: usize,
    pub stale_connections: usize,
    pub stale_snapshot_holders: usize,
    pub turnover_per_minute: f64,
    pub prepare_ratio_percent: f64,
}

/// Guidance item explaining a best practice for FrankenSQLite MVCC pools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionPoolBestPractice {
    pub title: &'static str,
    pub guidance: &'static str,
}

/// Actionable recommendation emitted by the validator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionPoolRecommendation {
    pub recommended_pool_size: usize,
    pub recommended_max_idle_ms: u64,
    pub recommended_max_age_ms: u64,
    pub rationale: Vec<String>,
    pub best_practices: Vec<ConnectionPoolBestPractice>,
}

/// Overall health classification for a pool sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConnectionPoolHealth {
    Healthy,
    NeedsAttention,
    Critical,
}

/// Full programmatic report returned by the connection-pool validator.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectionPoolValidationReport {
    pub health: ConnectionPoolHealth,
    pub summary: ConnectionPoolSummary,
    pub findings: Vec<ConnectionPoolFinding>,
    pub recommendation: ConnectionPoolRecommendation,
}

impl ConnectionPoolValidationReport {
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.health == ConnectionPoolHealth::Healthy
    }
}

/// Simulated outcome for one candidate pool size.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectionPoolSimulationPoint {
    pub pool_size: usize,
    pub predicted_active_connections: usize,
    pub throughput_score: f64,
    pub efficiency_score: f64,
    pub rationale: Vec<String>,
    pub validation: ConnectionPoolValidationReport,
}

/// Deterministic sweep across candidate pool sizes for one workload sample.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectionPoolSimulationReport {
    pub recommended_pool_size: usize,
    pub points: Vec<ConnectionPoolSimulationPoint>,
}

impl ConnectionPoolSimulationReport {
    /// Lookup a simulation point by candidate pool size.
    #[must_use]
    pub fn point_for_pool_size(&self, pool_size: usize) -> Option<&ConnectionPoolSimulationPoint> {
        self.points
            .iter()
            .find(|point| point.pool_size == pool_size)
    }
}

/// Tunable thresholds for the validator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionPoolValidatorConfig {
    pub stale_idle_threshold_ms: u64,
    pub stale_snapshot_threshold_ms: u64,
    pub over_pool_multiplier: usize,
    pub thrash_cycles_per_minute_warn_threshold: u64,
    pub hot_loop_query_threshold: u64,
    pub hot_loop_prepare_ratio_warn_percent: u64,
    pub recommended_max_idle_ms: u64,
    pub recommended_max_age_ms: u64,
}

impl Default for ConnectionPoolValidatorConfig {
    fn default() -> Self {
        Self {
            stale_idle_threshold_ms: 30_000,
            stale_snapshot_threshold_ms: 5_000,
            over_pool_multiplier: 2,
            thrash_cycles_per_minute_warn_threshold: 8,
            hot_loop_query_threshold: 100,
            hot_loop_prepare_ratio_warn_percent: 60,
            recommended_max_idle_ms: 30_000,
            recommended_max_age_ms: 900_000,
        }
    }
}

/// Stateless analyzer for connection-pool guidance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionPoolValidator {
    config: ConnectionPoolValidatorConfig,
}

impl Default for ConnectionPoolValidator {
    fn default() -> Self {
        Self::new(ConnectionPoolValidatorConfig::default())
    }
}

impl ConnectionPoolValidator {
    #[must_use]
    pub const fn new(config: ConnectionPoolValidatorConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &ConnectionPoolValidatorConfig {
        &self.config
    }

    #[must_use]
    pub fn validate(
        &self,
        sample: &ConnectionPoolTelemetrySample,
    ) -> ConnectionPoolValidationReport {
        let stale_connections = sample.stale_connection_count(self.config.stale_idle_threshold_ms);
        let stale_snapshot_holders = sample.stale_snapshot_holder_count(
            self.config.stale_idle_threshold_ms,
            self.config.stale_snapshot_threshold_ms,
        );
        let summary = ConnectionPoolSummary {
            configured_pool_size: sample.configured_pool_size,
            observed_connection_count: sample.connections.len(),
            observed_active_connections: sample.observed_active_connections,
            peak_concurrent_checkout_requests: sample.peak_concurrent_checkout_requests,
            concurrent_writers: sample.concurrent_writers,
            stale_connections,
            stale_snapshot_holders,
            turnover_per_minute: sample.turnover_per_minute(),
            prepare_ratio_percent: sample.prepare_ratio_percent(),
        };

        let recommended_pool_size = Self::recommended_pool_size(sample);
        let mut findings = Vec::new();

        Self::detect_single_connection_serialization(sample, &mut findings);
        self.detect_over_pooling(
            sample,
            recommended_pool_size,
            stale_connections,
            &mut findings,
        );
        self.detect_stale_snapshots(
            sample,
            stale_connections,
            stale_snapshot_holders,
            &mut findings,
        );
        self.detect_connection_thrashing(sample, &mut findings);
        self.detect_unprepared_hot_loop(sample, &mut findings);

        let health =
            findings.iter().fold(
                ConnectionPoolHealth::Healthy,
                |health, finding| match finding.severity {
                    ConnectionPoolSeverity::Critical => ConnectionPoolHealth::Critical,
                    ConnectionPoolSeverity::Warn if health == ConnectionPoolHealth::Healthy => {
                        ConnectionPoolHealth::NeedsAttention
                    }
                    _ => health,
                },
            );

        let recommendation = self.recommendation_for(
            sample,
            recommended_pool_size,
            stale_snapshot_holders,
            &findings,
        );

        ConnectionPoolValidationReport {
            health,
            summary,
            findings,
            recommendation,
        }
    }

    fn detect_single_connection_serialization(
        sample: &ConnectionPoolTelemetrySample,
        findings: &mut Vec<ConnectionPoolFinding>,
    ) {
        if sample.configured_pool_size > 1
            || sample.peak_concurrent_checkout_requests < 2
            || sample.concurrent_writers < 2
        {
            return;
        }

        findings.push(ConnectionPoolFinding {
            pattern: ConnectionPoolPattern::SingleConnectionSerializedWriters,
            severity: ConnectionPoolSeverity::Critical,
            confidence_score: 0.95,
            summary: "A single connection is being used for a workload that wants concurrent writers.".to_owned(),
            evidence: vec![
                format!("configured_pool_size={}", sample.configured_pool_size),
                format!(
                    "peak_concurrent_checkout_requests={}",
                    sample.peak_concurrent_checkout_requests
                ),
                format!("concurrent_writers={}", sample.concurrent_writers),
                "FrankenSQLite benefits from multiple writer connections; a single shared connection recreates SQLite-style serialization.".to_owned(),
            ],
        });
    }

    fn detect_over_pooling(
        &self,
        sample: &ConnectionPoolTelemetrySample,
        recommended_pool_size: usize,
        stale_connections: usize,
        findings: &mut Vec<ConnectionPoolFinding>,
    ) {
        let pool_limit = recommended_pool_size.saturating_mul(self.config.over_pool_multiplier);
        if sample.configured_pool_size <= pool_limit
            || sample.observed_active_connections > recommended_pool_size
        {
            return;
        }

        findings.push(ConnectionPoolFinding {
            pattern: ConnectionPoolPattern::OverPooling,
            severity: ConnectionPoolSeverity::Warn,
            confidence_score: 0.82,
            summary: "The configured pool is materially larger than observed useful parallelism."
                .to_owned(),
            evidence: vec![
                format!("configured_pool_size={}", sample.configured_pool_size),
                format!("recommended_pool_size={recommended_pool_size}"),
                format!(
                    "observed_active_connections={}",
                    sample.observed_active_connections
                ),
                format!("stale_connections={stale_connections}"),
            ],
        });
    }

    fn detect_stale_snapshots(
        &self,
        sample: &ConnectionPoolTelemetrySample,
        stale_connections: usize,
        stale_snapshot_holders: usize,
        findings: &mut Vec<ConnectionPoolFinding>,
    ) {
        if stale_snapshot_holders == 0 {
            return;
        }

        findings.push(ConnectionPoolFinding {
            pattern: ConnectionPoolPattern::StaleIdleSnapshot,
            severity: if stale_snapshot_holders > 1 {
                ConnectionPoolSeverity::Critical
            } else {
                ConnectionPoolSeverity::Warn
            },
            confidence_score: 0.9,
            summary: "Long-idle connections are retaining snapshots or open transactions."
                .to_owned(),
            evidence: vec![
                format!("stale_connections={stale_connections}"),
                format!("stale_snapshot_holders={stale_snapshot_holders}"),
                format!(
                    "idle_threshold_ms={} snapshot_threshold_ms={}",
                    self.config.stale_idle_threshold_ms, self.config.stale_snapshot_threshold_ms
                ),
            ],
        });

        if sample.connections.iter().any(|conn| {
            conn.open_transactions > 0 && conn.idle_ms >= self.config.stale_idle_threshold_ms
        }) {
            findings.push(ConnectionPoolFinding {
                pattern: ConnectionPoolPattern::StaleIdleSnapshot,
                severity: ConnectionPoolSeverity::Critical,
                confidence_score: 0.88,
                summary: "At least one idle connection still has an open transaction.".to_owned(),
                evidence: sample
                    .connections
                    .iter()
                    .filter(|conn| {
                        conn.open_transactions > 0
                            && conn.idle_ms >= self.config.stale_idle_threshold_ms
                    })
                    .map(|conn| {
                        format!(
                            "connection_id={} idle_ms={} open_transactions={}",
                            conn.connection_id, conn.idle_ms, conn.open_transactions
                        )
                    })
                    .collect(),
            });
        }
    }

    fn detect_connection_thrashing(
        &self,
        sample: &ConnectionPoolTelemetrySample,
        findings: &mut Vec<ConnectionPoolFinding>,
    ) {
        let turnover = sample.turnover_per_minute();
        if turnover < self.config.thrash_cycles_per_minute_warn_threshold as f64 {
            return;
        }

        findings.push(ConnectionPoolFinding {
            pattern: ConnectionPoolPattern::ConnectionThrashing,
            severity: ConnectionPoolSeverity::Warn,
            confidence_score: 0.84,
            summary: "Connections are being opened and closed too aggressively for the observed workload.".to_owned(),
            evidence: vec![
                format!("connect_events={}", sample.connect_events),
                format!("disconnect_events={}", sample.disconnect_events),
                format!("turnover_per_minute={turnover:.2}"),
                format!("measurement_window_ms={}", sample.measurement_window_ms),
            ],
        });
    }

    fn detect_unprepared_hot_loop(
        &self,
        sample: &ConnectionPoolTelemetrySample,
        findings: &mut Vec<ConnectionPoolFinding>,
    ) {
        let total_queries = sample.total_queries();
        let total_prepare_calls = sample.total_prepare_calls();
        if total_queries < self.config.hot_loop_query_threshold
            || total_prepare_calls.saturating_mul(100)
                < total_queries.saturating_mul(self.config.hot_loop_prepare_ratio_warn_percent)
        {
            return;
        }

        findings.push(ConnectionPoolFinding {
            pattern: ConnectionPoolPattern::UnpreparedHotLoop,
            severity: ConnectionPoolSeverity::Warn,
            confidence_score: 0.78,
            summary: "The pool is spending too much work preparing statements in hot query paths."
                .to_owned(),
            evidence: vec![
                format!("total_queries={total_queries}"),
                format!("total_prepare_calls={total_prepare_calls}"),
                format!(
                    "prepare_ratio_percent={:.2}",
                    sample.prepare_ratio_percent()
                ),
            ],
        });
    }

    fn recommendation_for(
        &self,
        sample: &ConnectionPoolTelemetrySample,
        recommended_pool_size: usize,
        stale_snapshot_holders: usize,
        findings: &[ConnectionPoolFinding],
    ) -> ConnectionPoolRecommendation {
        let mut rationale = vec![format!(
            "Start near pool_size={} because FrankenSQLite can use multiple writer connections, but returns diminish past min(cpu_cores={}, write_parallelism={}).",
            recommended_pool_size,
            sample.cpu_cores.max(1),
            Self::write_parallelism_target(sample)
        )];

        if stale_snapshot_holders > 0 {
            rationale.push(format!(
                "Reap or recycle idle connections faster: {} stale snapshot holder(s) can delay cleanup and hold old visibility snapshots.",
                stale_snapshot_holders
            ));
        }
        if findings
            .iter()
            .any(|finding| finding.pattern == ConnectionPoolPattern::ConnectionThrashing)
        {
            rationale.push(
                "Reduce connect/disconnect churn by reusing pooled connections instead of recreating them per request."
                    .to_owned(),
            );
        }
        if findings
            .iter()
            .any(|finding| finding.pattern == ConnectionPoolPattern::UnpreparedHotLoop)
        {
            rationale.push(
                "Prepare hot statements once per connection or enable statement caching in the pool wrapper."
                    .to_owned(),
            );
        }

        ConnectionPoolRecommendation {
            recommended_pool_size,
            recommended_max_idle_ms: self.config.recommended_max_idle_ms,
            recommended_max_age_ms: self.config.recommended_max_age_ms,
            rationale,
            best_practices: best_practices(sample.workload_profile),
        }
    }

    fn recommended_pool_size(sample: &ConnectionPoolTelemetrySample) -> usize {
        let cpu_cores = sample.cpu_cores.max(1);
        let write_parallelism = Self::write_parallelism_target(sample).max(1);
        cpu_cores.min(write_parallelism).max(1)
    }

    fn write_parallelism_target(sample: &ConnectionPoolTelemetrySample) -> usize {
        let observed_need = sample
            .concurrent_writers
            .max(sample.observed_active_connections)
            .max(sample.peak_concurrent_checkout_requests);
        match sample.workload_profile {
            ConnectionPoolWorkloadProfile::ReadHeavy => observed_need.clamp(1, 2),
            ConnectionPoolWorkloadProfile::Mixed | ConnectionPoolWorkloadProfile::WriteHeavy => {
                observed_need.max(2)
            }
        }
    }
}

/// Validate a pool sample with the default thresholds.
#[must_use]
pub fn validate_connection_pool(
    sample: &ConnectionPoolTelemetrySample,
) -> ConnectionPoolValidationReport {
    ConnectionPoolValidator::default().validate(sample)
}

/// Simulate a deterministic sweep across candidate pool sizes.
///
/// This is a heuristic aid, not a benchmark substitute. It projects how the
/// validator would assess nearby pool sizes for the same observed workload.
#[must_use]
pub fn simulate_connection_pool(
    sample: &ConnectionPoolTelemetrySample,
    candidate_pool_sizes: &[usize],
) -> ConnectionPoolSimulationReport {
    let validator = ConnectionPoolValidator::default();
    let recommended_by_validator = ConnectionPoolValidator::recommended_pool_size(sample);
    let candidate_pool_sizes = sanitized_candidate_pool_sizes(
        candidate_pool_sizes,
        sample.configured_pool_size,
        recommended_by_validator,
    );
    let points = candidate_pool_sizes
        .into_iter()
        .map(|pool_size| {
            let projected = projected_sample_for_pool_size(sample, pool_size);
            let validation = validator.validate(&projected);
            let throughput_score =
                projected_throughput_score(sample, pool_size, &validation);
            let efficiency_score = projected_efficiency_score(
                throughput_score,
                projected.observed_active_connections,
                pool_size,
            );
            let mut rationale = vec![format!(
                "Projected active connections: {} / {} for this workload.",
                projected.observed_active_connections, pool_size
            )];
            rationale.push(format!(
                "Validator recommendation for the observed workload is pool_size={recommended_by_validator}."
            ));
            if validation.findings.is_empty() {
                rationale.push(
                    "No pool anti-patterns are predicted for this candidate size.".to_owned(),
                );
            } else {
                rationale.extend(
                    validation
                        .findings
                        .iter()
                        .map(|finding| finding.summary.clone()),
                );
            }
            ConnectionPoolSimulationPoint {
                pool_size,
                predicted_active_connections: projected.observed_active_connections,
                throughput_score,
                efficiency_score,
                rationale,
                validation,
            }
        })
        .collect::<Vec<_>>();
    let recommended_pool_size = recommended_simulated_pool_size(&points, recommended_by_validator);

    ConnectionPoolSimulationReport {
        recommended_pool_size,
        points,
    }
}

fn sanitized_candidate_pool_sizes(
    candidate_pool_sizes: &[usize],
    configured_pool_size: usize,
    recommended_pool_size: usize,
) -> Vec<usize> {
    let mut candidates = Vec::new();
    for pool_size in candidate_pool_sizes
        .iter()
        .copied()
        .chain([configured_pool_size, recommended_pool_size])
    {
        let normalized = pool_size.max(1);
        if !candidates.contains(&normalized) {
            candidates.push(normalized);
        }
    }
    candidates
}

fn projected_sample_for_pool_size(
    sample: &ConnectionPoolTelemetrySample,
    pool_size: usize,
) -> ConnectionPoolTelemetrySample {
    let predicted_active_connections = projected_active_connections(sample, pool_size);
    let mut projected = sample.clone();
    projected.configured_pool_size = pool_size;
    projected.observed_active_connections = predicted_active_connections;
    projected.connections = projected_connections(sample, pool_size, predicted_active_connections);
    projected
}

fn projected_active_connections(sample: &ConnectionPoolTelemetrySample, pool_size: usize) -> usize {
    let observed_need = sample
        .concurrent_writers
        .max(sample.observed_active_connections)
        .max(sample.peak_concurrent_checkout_requests);
    let bounded_need = match sample.workload_profile {
        ConnectionPoolWorkloadProfile::ReadHeavy => observed_need.clamp(1, 2),
        ConnectionPoolWorkloadProfile::Mixed | ConnectionPoolWorkloadProfile::WriteHeavy => {
            observed_need.max(1)
        }
    };
    pool_size.min(bounded_need)
}

fn projected_connections(
    sample: &ConnectionPoolTelemetrySample,
    pool_size: usize,
    predicted_active_connections: usize,
) -> Vec<ConnectionLifecycleSnapshot> {
    const IDLE_EXTRA_CONNECTION_MS: u64 = 45_000;

    let mut projected = Vec::with_capacity(pool_size);
    for idx in 0..pool_size {
        let mut connection =
            sample
                .connections
                .get(idx)
                .cloned()
                .unwrap_or(ConnectionLifecycleSnapshot {
                    connection_id: 0,
                    age_ms: 0,
                    idle_ms: 0,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 0,
                    prepare_calls: 0,
                });
        connection.connection_id = u64::try_from(idx.saturating_add(1)).unwrap_or(u64::MAX);
        if idx < predicted_active_connections {
            connection.idle_ms = connection.idle_ms.min(500);
            if connection.open_transactions == 0 {
                connection.active_snapshot_age_ms =
                    connection.active_snapshot_age_ms.map(|age| age.min(1_000));
            }
        } else {
            connection.idle_ms = connection.idle_ms.max(IDLE_EXTRA_CONNECTION_MS);
            if connection.open_transactions == 0 {
                connection.active_snapshot_age_ms = None;
                connection.queries_executed = 0;
                connection.prepare_calls = 0;
            }
        }
        projected.push(connection);
    }
    projected
}

#[allow(clippy::cast_precision_loss)]
fn projected_throughput_score(
    sample: &ConnectionPoolTelemetrySample,
    pool_size: usize,
    validation: &ConnectionPoolValidationReport,
) -> f64 {
    let recommended_pool_size = ConnectionPoolValidator::recommended_pool_size(sample);
    let recommended_pool_size_f64 = recommended_pool_size.max(1) as f64;
    let useful_parallelism =
        pool_size.min(recommended_pool_size) as f64 / recommended_pool_size_f64;
    let overshoot_ratio =
        pool_size.saturating_sub(recommended_pool_size) as f64 / recommended_pool_size_f64;

    let mut score = overshoot_ratio.mul_add(-10.0, useful_parallelism.mul_add(55.0, 45.0));
    score -= validation
        .findings
        .iter()
        .map(projected_finding_penalty)
        .sum::<f64>();
    score.clamp(1.0, 100.0)
}

#[allow(clippy::cast_precision_loss)]
fn projected_efficiency_score(
    throughput_score: f64,
    predicted_active_connections: usize,
    pool_size: usize,
) -> f64 {
    if pool_size == 0 {
        return 0.0;
    }
    throughput_score * (predicted_active_connections as f64 / pool_size as f64)
}

fn projected_finding_penalty(finding: &ConnectionPoolFinding) -> f64 {
    match finding.pattern {
        ConnectionPoolPattern::SingleConnectionSerializedWriters => 35.0,
        ConnectionPoolPattern::OverPooling => 8.0,
        ConnectionPoolPattern::StaleIdleSnapshot => 12.0,
        ConnectionPoolPattern::ConnectionThrashing => 10.0,
        ConnectionPoolPattern::UnpreparedHotLoop => 6.0,
    }
}

fn recommended_simulated_pool_size(
    points: &[ConnectionPoolSimulationPoint],
    validator_recommendation: usize,
) -> usize {
    let mut best_point = &points[0];
    for point in &points[1..] {
        let throughput_cmp = point
            .throughput_score
            .total_cmp(&best_point.throughput_score);
        if throughput_cmp.is_gt() {
            best_point = point;
            continue;
        }
        if throughput_cmp.is_eq() {
            let efficiency_cmp = point
                .efficiency_score
                .total_cmp(&best_point.efficiency_score);
            if efficiency_cmp.is_gt() {
                best_point = point;
                continue;
            }
            if efficiency_cmp.is_eq() {
                let point_distance = point.pool_size.abs_diff(validator_recommendation);
                let best_distance = best_point.pool_size.abs_diff(validator_recommendation);
                if point_distance < best_distance
                    || (point_distance == best_distance && point.pool_size < best_point.pool_size)
                {
                    best_point = point;
                }
            }
        }
    }
    best_point.pool_size
}

/// MVCC-specific guidance for consumers building pools around FrankenSQLite.
#[must_use]
pub fn best_practices(
    workload_profile: ConnectionPoolWorkloadProfile,
) -> Vec<ConnectionPoolBestPractice> {
    let mut practices = vec![
        ConnectionPoolBestPractice {
            title: "Prefer multiple writer connections",
            guidance: "FrankenSQLite is designed for concurrent writers. Do not collapse the application onto one shared writer connection unless the workload is truly single-threaded.",
        },
        ConnectionPoolBestPractice {
            title: "Start pool sizing at min(cpu_cores, writer concurrency)",
            guidance: "Use enough connections to expose writer parallelism, but stop growing the pool once the workload no longer has real concurrent work to issue.",
        },
        ConnectionPoolBestPractice {
            title: "Recycle stale idle connections",
            guidance: "Idle connections that retain snapshots or transactions can delay cleanup and keep old visibility state alive longer than necessary.",
        },
        ConnectionPoolBestPractice {
            title: "Prepare hot statements per connection",
            guidance: "Avoid reparsing and repreparing the same SQL inside tight loops. Reuse prepared statements or enable a statement cache in the pool wrapper.",
        },
    ];

    if matches!(workload_profile, ConnectionPoolWorkloadProfile::ReadHeavy) {
        practices.push(ConnectionPoolBestPractice {
            title: "Do not over-pool read-heavy workloads",
            guidance:
                "Read-heavy workloads usually need fewer connections than write-heavy ones; extra idle readers mostly add snapshot churn and management overhead.",
        });
    }

    practices
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLifecycleSnapshot, ConnectionPoolHealth, ConnectionPoolPattern,
        ConnectionPoolTelemetrySample, ConnectionPoolValidator, ConnectionPoolValidatorConfig,
        ConnectionPoolWorkloadProfile, best_practices, simulate_connection_pool,
        validate_connection_pool,
    };
    use serde_json::Value;

    fn sample(workload_profile: ConnectionPoolWorkloadProfile) -> ConnectionPoolTelemetrySample {
        ConnectionPoolTelemetrySample {
            workload_profile,
            cpu_cores: 8,
            configured_pool_size: 4,
            observed_active_connections: 3,
            peak_concurrent_checkout_requests: 3,
            concurrent_writers: 3,
            connect_events: 4,
            disconnect_events: 2,
            measurement_window_ms: 60_000,
            connections: vec![
                ConnectionLifecycleSnapshot {
                    connection_id: 1,
                    age_ms: 40_000,
                    idle_ms: 500,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 150,
                    prepare_calls: 5,
                },
                ConnectionLifecycleSnapshot {
                    connection_id: 2,
                    age_ms: 40_000,
                    idle_ms: 250,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 140,
                    prepare_calls: 4,
                },
                ConnectionLifecycleSnapshot {
                    connection_id: 3,
                    age_ms: 40_000,
                    idle_ms: 150,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 135,
                    prepare_calls: 4,
                },
            ],
        }
    }

    #[test]
    fn test_single_connection_serialization_is_detected() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::WriteHeavy);
        sample.configured_pool_size = 1;
        sample.observed_active_connections = 1;
        sample.peak_concurrent_checkout_requests = 4;
        sample.concurrent_writers = 4;

        let report = validate_connection_pool(&sample);
        assert_eq!(report.health, ConnectionPoolHealth::Critical);
        assert!(report.findings.iter().any(|finding| {
            finding.pattern == ConnectionPoolPattern::SingleConnectionSerializedWriters
        }));
    }

    #[test]
    fn test_over_pooling_is_detected_when_parallelism_is_low() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::Mixed);
        sample.configured_pool_size = 24;
        sample.observed_active_connections = 2;
        sample.peak_concurrent_checkout_requests = 2;
        sample.concurrent_writers = 2;

        let report = validate_connection_pool(&sample);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| { finding.pattern == ConnectionPoolPattern::OverPooling })
        );
        assert!(report.recommendation.recommended_pool_size < sample.configured_pool_size);
    }

    #[test]
    fn test_stale_idle_snapshot_holders_are_flagged() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::Mixed);
        sample.connections.push(ConnectionLifecycleSnapshot {
            connection_id: 99,
            age_ms: 120_000,
            idle_ms: 60_000,
            open_transactions: 1,
            active_snapshot_age_ms: Some(10_000),
            queries_executed: 2,
            prepare_calls: 1,
        });

        let report = validate_connection_pool(&sample);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| { finding.pattern == ConnectionPoolPattern::StaleIdleSnapshot })
        );
        assert!(report.summary.stale_snapshot_holders >= 1);
    }

    #[test]
    fn test_connection_thrashing_is_detected() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::ReadHeavy);
        sample.configured_pool_size = 2;
        sample.connect_events = 80;
        sample.disconnect_events = 76;
        sample.measurement_window_ms = 60_000;

        let report = validate_connection_pool(&sample);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| { finding.pattern == ConnectionPoolPattern::ConnectionThrashing })
        );
    }

    #[test]
    fn test_unprepared_hot_loop_is_detected() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::Mixed);
        for conn in &mut sample.connections {
            conn.prepare_calls = conn.queries_executed;
        }

        let report = validate_connection_pool(&sample);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| { finding.pattern == ConnectionPoolPattern::UnpreparedHotLoop })
        );
    }

    #[test]
    fn test_recommendation_is_bounded_by_cpu_and_grows_with_writer_need() {
        let validator = ConnectionPoolValidator::default();

        let mut light = sample(ConnectionPoolWorkloadProfile::Mixed);
        light.cpu_cores = 4;
        light.concurrent_writers = 2;
        light.peak_concurrent_checkout_requests = 2;
        let light_report = validator.validate(&light);

        let mut heavy = sample(ConnectionPoolWorkloadProfile::WriteHeavy);
        heavy.cpu_cores = 4;
        heavy.concurrent_writers = 8;
        heavy.peak_concurrent_checkout_requests = 8;
        let heavy_report = validator.validate(&heavy);

        assert!(
            heavy_report.recommendation.recommended_pool_size
                >= light_report.recommendation.recommended_pool_size
        );
        assert_eq!(heavy_report.recommendation.recommended_pool_size, 4);
    }

    #[test]
    fn test_healthy_pool_has_no_findings() {
        let report = validate_connection_pool(&sample(ConnectionPoolWorkloadProfile::Mixed));
        assert!(report.is_healthy());
        assert!(report.findings.is_empty());
    }

    #[test]
    fn test_best_practices_include_mvcc_specific_guidance() {
        let practices = best_practices(ConnectionPoolWorkloadProfile::WriteHeavy);
        assert!(practices.iter().any(|practice| {
            practice
                .guidance
                .contains("FrankenSQLite is designed for concurrent writers")
        }));
    }

    #[test]
    fn test_custom_thresholds_raise_stale_idle_sensitivity() {
        let validator = ConnectionPoolValidator::new(ConnectionPoolValidatorConfig {
            stale_idle_threshold_ms: 100,
            stale_snapshot_threshold_ms: 100,
            ..ConnectionPoolValidatorConfig::default()
        });
        let mut sample = sample(ConnectionPoolWorkloadProfile::Mixed);
        sample.connections[0].idle_ms = 500;
        sample.connections[0].active_snapshot_age_ms = Some(200);

        let report = validator.validate(&sample);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| { finding.pattern == ConnectionPoolPattern::StaleIdleSnapshot })
        );
    }

    #[test]
    fn test_simulator_prefers_pool_size_matching_writer_parallelism() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::WriteHeavy);
        sample.configured_pool_size = 1;
        sample.observed_active_connections = 1;
        sample.peak_concurrent_checkout_requests = 4;
        sample.concurrent_writers = 4;

        let simulation = simulate_connection_pool(&sample, &[1, 2, 4, 8]);

        assert_eq!(simulation.recommended_pool_size, 4);
        assert!(
            simulation.point_for_pool_size(4).unwrap().throughput_score
                > simulation.point_for_pool_size(1).unwrap().throughput_score
        );
    }

    #[test]
    fn test_simulator_caps_read_heavy_workloads_aggressively() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::ReadHeavy);
        sample.cpu_cores = 8;
        sample.observed_active_connections = 2;
        sample.peak_concurrent_checkout_requests = 6;
        sample.concurrent_writers = 1;

        let simulation = simulate_connection_pool(&sample, &[1, 2, 4]);

        assert_eq!(simulation.recommended_pool_size, 2);
        assert_eq!(
            simulation
                .point_for_pool_size(4)
                .unwrap()
                .predicted_active_connections,
            2
        );
    }

    #[test]
    fn test_simulator_is_stable_across_runs() {
        let sample = sample(ConnectionPoolWorkloadProfile::Mixed);

        let left = simulate_connection_pool(&sample, &[1, 2, 4, 8]);
        let right = simulate_connection_pool(&sample, &[1, 2, 4, 8]);

        assert_eq!(left, right);
    }

    #[test]
    fn test_validation_report_serializes_auditable_fields() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::WriteHeavy);
        sample.configured_pool_size = 1;
        sample.observed_active_connections = 1;
        sample.peak_concurrent_checkout_requests = 4;
        sample.concurrent_writers = 4;

        let report = validate_connection_pool(&sample);
        let value = serde_json::to_value(&report).unwrap();

        assert_eq!(value["health"], Value::String("Critical".to_owned()));
        assert_eq!(value["summary"]["configured_pool_size"], Value::from(1));
        assert_eq!(value["summary"]["concurrent_writers"], Value::from(4));
        assert_eq!(
            value["recommendation"]["recommended_pool_size"],
            Value::from(4)
        );

        let findings = value["findings"].as_array().unwrap();
        let single_connection = findings
            .iter()
            .find(|finding| {
                finding["pattern"] == Value::String("SingleConnectionSerializedWriters".to_owned())
            })
            .expect("expected serialized single-connection finding");
        assert_eq!(
            single_connection["severity"],
            Value::String("Critical".to_owned())
        );
        assert_eq!(single_connection["confidence_score"], Value::from(0.95));
        assert!(
            single_connection["evidence"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry.as_str().is_some_and(|text| {
                        text.contains("FrankenSQLite benefits from multiple writer connections")
                    })
                }),
            "serialized evidence should preserve MVCC-specific guidance",
        );
    }

    #[test]
    fn test_simulation_report_serializes_candidate_metrics() {
        let mut sample = sample(ConnectionPoolWorkloadProfile::WriteHeavy);
        sample.configured_pool_size = 1;
        sample.observed_active_connections = 1;
        sample.peak_concurrent_checkout_requests = 4;
        sample.concurrent_writers = 4;

        let simulation = simulate_connection_pool(&sample, &[1, 2, 4, 8]);
        let value = serde_json::to_value(&simulation).unwrap();

        assert_eq!(value["recommended_pool_size"], Value::from(4));

        let points = value["points"].as_array().unwrap();
        assert_eq!(points.len(), 4);

        let recommended_point = points
            .iter()
            .find(|point| point["pool_size"].as_i64() == Some(4))
            .expect("expected serialized point for pool_size=4");
        let undersized_point = points
            .iter()
            .find(|point| point["pool_size"].as_i64() == Some(1))
            .expect("expected serialized point for pool_size=1");

        assert!(
            recommended_point["throughput_score"].as_f64().unwrap()
                > undersized_point["throughput_score"].as_f64().unwrap(),
            "serialized throughput scores should preserve the recommendation ordering",
        );
        assert_eq!(
            recommended_point["validation"]["recommendation"]["recommended_pool_size"],
            Value::from(4)
        );
        assert!(
            recommended_point["rationale"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry.as_str().is_some_and(|text| {
                        text.contains("Validator recommendation for the observed workload")
                    })
                }),
            "serialized rationale should retain deterministic replay context",
        );
    }

    #[test]
    fn test_docs_validator_example_matches_exported_api() {
        let sample = ConnectionPoolTelemetrySample {
            workload_profile: ConnectionPoolWorkloadProfile::WriteHeavy,
            cpu_cores: 4,
            configured_pool_size: 4,
            observed_active_connections: 4,
            peak_concurrent_checkout_requests: 4,
            concurrent_writers: 4,
            connect_events: 4,
            disconnect_events: 4,
            measurement_window_ms: 60_000,
            connections: vec![
                ConnectionLifecycleSnapshot {
                    connection_id: 1,
                    age_ms: 20_000,
                    idle_ms: 200,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 240,
                    prepare_calls: 8,
                },
                ConnectionLifecycleSnapshot {
                    connection_id: 2,
                    age_ms: 20_000,
                    idle_ms: 250,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 230,
                    prepare_calls: 8,
                },
                ConnectionLifecycleSnapshot {
                    connection_id: 3,
                    age_ms: 20_000,
                    idle_ms: 175,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 225,
                    prepare_calls: 8,
                },
                ConnectionLifecycleSnapshot {
                    connection_id: 4,
                    age_ms: 20_000,
                    idle_ms: 225,
                    open_transactions: 0,
                    active_snapshot_age_ms: None,
                    queries_executed: 235,
                    prepare_calls: 8,
                },
            ],
        };

        let report = validate_connection_pool(&sample);

        assert_eq!(report.recommendation.recommended_pool_size, 4);
        assert!(report.findings.is_empty());
    }

    #[test]
    fn test_docs_simulator_example_matches_exported_api() {
        let sample = ConnectionPoolTelemetrySample {
            workload_profile: ConnectionPoolWorkloadProfile::WriteHeavy,
            cpu_cores: 4,
            configured_pool_size: 1,
            observed_active_connections: 1,
            peak_concurrent_checkout_requests: 4,
            concurrent_writers: 4,
            connect_events: 4,
            disconnect_events: 4,
            measurement_window_ms: 60_000,
            connections: vec![ConnectionLifecycleSnapshot {
                connection_id: 1,
                age_ms: 10_000,
                idle_ms: 100,
                open_transactions: 0,
                active_snapshot_age_ms: None,
                queries_executed: 400,
                prepare_calls: 8,
            }],
        };

        let simulation = simulate_connection_pool(&sample, &[1, 2, 4, 8]);

        assert_eq!(simulation.recommended_pool_size, 4);
        assert!(
            simulation.point_for_pool_size(4).unwrap().throughput_score
                > simulation.point_for_pool_size(1).unwrap().throughput_score
        );
    }
}
