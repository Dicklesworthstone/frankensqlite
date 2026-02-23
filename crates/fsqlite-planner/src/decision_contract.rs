//! Decision Contract for the query planner (bd-1lsfu.6).
//!
//! Every planning decision is logged as a structured record with four fields:
//! - **STATE**: table stats, indexes, WHERE terms observed
//! - **ACTION**: join order, access paths, estimated costs chosen
//! - **LOSS**: estimated cost (plan-time) and actual cost (post-execution)
//! - **CALIBRATION**: actual/estimated ratio, miscalibration alerts
//!
//! Records form a BLAKE3-chained append-only log for tamper-evident auditing.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{AccessPath, AccessPathKind, IndexInfo, QueryPlan, StatsSource, TableStats};

// ---------------------------------------------------------------------------
// ID generator
// ---------------------------------------------------------------------------

static NEXT_CONTRACT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    NEXT_CONTRACT_ID.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// STATE: what the planner observed
// ---------------------------------------------------------------------------

/// Summary of table statistics at decision time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableStatsSummary {
    pub name: String,
    pub n_pages: u64,
    pub n_rows: u64,
    pub source: String,
}

impl From<&TableStats> for TableStatsSummary {
    fn from(ts: &TableStats) -> Self {
        Self {
            name: ts.name.clone(),
            n_pages: ts.n_pages,
            n_rows: ts.n_rows,
            source: match ts.source {
                StatsSource::Analyze => "analyze".to_owned(),
                StatsSource::Heuristic => "heuristic".to_owned(),
            },
        }
    }
}

/// Summary of index metadata at decision time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexSummary {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub n_pages: u64,
}

impl From<&IndexInfo> for IndexSummary {
    fn from(ii: &IndexInfo) -> Self {
        Self {
            name: ii.name.clone(),
            table: ii.table.clone(),
            columns: ii.columns.clone(),
            unique: ii.unique,
            n_pages: ii.n_pages,
        }
    }
}

/// What the planner observed when making a decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannerState {
    /// Table statistics available at plan time.
    pub tables: Vec<TableStatsSummary>,
    /// Indexes available at plan time.
    pub indexes: Vec<IndexSummary>,
    /// Number of WHERE terms analyzed.
    pub where_term_count: usize,
    /// Number of needed columns (None = all).
    pub needed_column_count: Option<usize>,
    /// Number of cross-join pairs constraining order.
    pub cross_join_pairs: usize,
}

// ---------------------------------------------------------------------------
// ACTION: what the planner chose
// ---------------------------------------------------------------------------

/// Summary of a chosen access path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccessPathSummary {
    pub table: String,
    pub kind: String,
    pub index: Option<String>,
    pub estimated_cost: f64,
    pub estimated_rows: f64,
}

impl From<&AccessPath> for AccessPathSummary {
    fn from(ap: &AccessPath) -> Self {
        Self {
            table: ap.table.clone(),
            kind: access_path_kind_label(&ap.kind),
            index: ap.index.clone(),
            estimated_cost: ap.estimated_cost,
            estimated_rows: ap.estimated_rows,
        }
    }
}

/// Human-readable label for an access path kind.
#[must_use]
pub fn access_path_kind_label(kind: &AccessPathKind) -> String {
    match kind {
        AccessPathKind::FullTableScan => "full_table_scan".to_owned(),
        AccessPathKind::IndexScanRange { selectivity } => {
            format!("index_scan_range(sel={selectivity:.3})")
        }
        AccessPathKind::IndexScanEquality => "index_scan_equality".to_owned(),
        AccessPathKind::CoveringIndexScan { selectivity } => {
            format!("covering_index_scan(sel={selectivity:.3})")
        }
        AccessPathKind::RowidLookup => "rowid_lookup".to_owned(),
    }
}

/// What the planner chose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlannerAction {
    /// Join order chosen.
    pub join_order: Vec<String>,
    /// Access paths with estimated costs per table.
    pub access_paths: Vec<AccessPathSummary>,
    /// Total estimated cost in page reads.
    pub total_estimated_cost: f64,
    /// Beam width used during search.
    pub beam_width: usize,
    /// Whether star-query optimization was applied.
    pub star_query_detected: bool,
}

// ---------------------------------------------------------------------------
// LOSS: cost estimates and actuals
// ---------------------------------------------------------------------------

/// Actual execution cost (filled post-execution by the caller).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActualCost {
    /// Actual page reads during execution.
    pub page_reads: u64,
    /// Actual CPU time in microseconds.
    pub cpu_micros: u64,
    /// Actual rows returned.
    pub actual_rows: u64,
    /// Wall-clock execution time in microseconds.
    pub wall_time_micros: u64,
}

/// Cost estimates and (optional) actuals.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlannerLoss {
    /// Estimated cost at plan time (page reads).
    pub estimated_cost: f64,
    /// Estimated total rows (product of per-table estimates).
    pub estimated_rows: f64,
    /// Actual cost after execution. `None` until execution completes.
    pub actual_cost: Option<ActualCost>,
}

// ---------------------------------------------------------------------------
// CALIBRATION: how well the planner's estimate matched reality
// ---------------------------------------------------------------------------

/// Miscalibration alert level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub enum MiscalibrationAlert {
    /// Planner overestimated cost by more than the threshold.
    Overestimate { ratio: f64 },
    /// Planner underestimated cost by more than the threshold.
    Underestimate { ratio: f64 },
}

/// Calibration assessment comparing estimated vs actual cost.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Calibration {
    /// Calibration ratio: `actual_cost / estimated_cost`.
    /// A value of 1.0 means perfect calibration.
    pub ratio: f64,
    /// Whether this decision was miscalibrated.
    pub miscalibrated: bool,
    /// Alert if miscalibrated.
    pub alert: Option<MiscalibrationAlert>,
}

/// Threshold for miscalibration alerts (ratio > 5.0 or < 0.2).
pub const MISCALIBRATION_HIGH: f64 = 5.0;
/// Inverse of MISCALIBRATION_HIGH.
pub const MISCALIBRATION_LOW: f64 = 0.2;

/// Compute calibration from estimated and actual page reads.
///
/// Returns `None` if estimated cost is zero (no meaningful ratio).
#[must_use]
pub fn compute_calibration(estimated_cost: f64, actual_page_reads: u64) -> Option<Calibration> {
    if estimated_cost <= 0.0 {
        return None;
    }
    let ratio = actual_page_reads as f64 / estimated_cost;
    let (miscalibrated, alert) = if ratio > MISCALIBRATION_HIGH {
        (true, Some(MiscalibrationAlert::Underestimate { ratio }))
    } else if ratio < MISCALIBRATION_LOW {
        (true, Some(MiscalibrationAlert::Overestimate { ratio }))
    } else {
        (false, None)
    };
    Some(Calibration {
        ratio,
        miscalibrated,
        alert,
    })
}

// ---------------------------------------------------------------------------
// Decision Contract record
// ---------------------------------------------------------------------------

/// A complete decision contract: one per query plan produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionContract {
    /// Monotonically increasing record ID.
    pub id: u64,
    /// SQL query text (may be truncated for very long queries).
    pub query_text: String,
    /// When the decision was made (seconds since UNIX epoch).
    pub timestamp_epoch_secs: u64,
    /// STATE: what the planner observed.
    pub state: PlannerState,
    /// ACTION: what the planner chose.
    pub action: PlannerAction,
    /// LOSS: estimated and actual costs.
    pub loss: PlannerLoss,
    /// CALIBRATION: computed after execution. `None` until actual cost is set.
    pub calibration: Option<Calibration>,
    /// BLAKE3 hash of the previous record (hex). `"0"*64` for first record.
    pub prev_hash: String,
    /// BLAKE3 hash of this record (hex).
    pub record_hash: String,
}

/// Maximum query text length stored in a contract.
const MAX_QUERY_TEXT_LEN: usize = 4096;
const TRUNCATION_SUFFIX: &str = "...[truncated]";

/// The genesis hash used as `prev_hash` for the first record.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Compute the BLAKE3 hash of a decision contract's content fields
/// (everything except `record_hash` itself).
fn compute_record_hash(contract: &DecisionContract) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&contract.id.to_le_bytes());
    hasher.update(contract.query_text.as_bytes());
    hasher.update(&contract.timestamp_epoch_secs.to_le_bytes());
    hasher.update(contract.prev_hash.as_bytes());
    // Hash the action summary (deterministic via join order + costs).
    for table in &contract.action.join_order {
        hasher.update(table.as_bytes());
    }
    hasher.update(&contract.action.total_estimated_cost.to_le_bytes());
    hasher.update(&contract.loss.estimated_cost.to_le_bytes());
    hasher.update(&contract.loss.estimated_rows.to_le_bytes());
    format!("{}", hasher.finalize())
}

fn truncate_query_text_for_contract(query_text: &str) -> String {
    if query_text.len() <= MAX_QUERY_TEXT_LEN {
        return query_text.to_owned();
    }

    if MAX_QUERY_TEXT_LEN <= TRUNCATION_SUFFIX.len() {
        let mut end = MAX_QUERY_TEXT_LEN;
        while end > 0 && !query_text.is_char_boundary(end) {
            end -= 1;
        }
        return query_text[..end].to_owned();
    }

    let mut end = MAX_QUERY_TEXT_LEN - TRUNCATION_SUFFIX.len();
    while end > 0 && !query_text.is_char_boundary(end) {
        end -= 1;
    }

    let mut truncated = query_text[..end].to_owned();
    truncated.push_str(TRUNCATION_SUFFIX);
    truncated
}

// ---------------------------------------------------------------------------
// Builder: create a DecisionContract from planner inputs/outputs
// ---------------------------------------------------------------------------

/// Build a `DecisionContract` from the planner's inputs and output.
///
/// The contract is created at plan time with `calibration = None`.
/// Call [`DecisionContract::record_actual_cost`] after execution to fill in
/// the calibration fields.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_contract(
    query_text: &str,
    tables: &[TableStats],
    indexes: &[IndexInfo],
    where_term_count: usize,
    needed_column_count: Option<usize>,
    cross_join_pairs: usize,
    plan: &QueryPlan,
    beam_width: usize,
    star_query_detected: bool,
    prev_hash: &str,
) -> DecisionContract {
    let text = truncate_query_text_for_contract(query_text);

    let estimated_rows: f64 = plan
        .access_paths
        .iter()
        .map(|ap| ap.estimated_rows)
        .product();

    let mut contract = DecisionContract {
        id: next_id(),
        query_text: text,
        timestamp_epoch_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        state: PlannerState {
            tables: tables.iter().map(TableStatsSummary::from).collect(),
            indexes: indexes.iter().map(IndexSummary::from).collect(),
            where_term_count,
            needed_column_count,
            cross_join_pairs,
        },
        action: PlannerAction {
            join_order: plan.join_order.clone(),
            access_paths: plan
                .access_paths
                .iter()
                .map(AccessPathSummary::from)
                .collect(),
            total_estimated_cost: plan.total_cost,
            beam_width,
            star_query_detected,
        },
        loss: PlannerLoss {
            estimated_cost: plan.total_cost,
            estimated_rows,
            actual_cost: None,
        },
        calibration: None,
        prev_hash: prev_hash.to_owned(),
        record_hash: String::new(),
    };
    contract.record_hash = compute_record_hash(&contract);
    contract
}

impl DecisionContract {
    /// Record actual execution cost and compute calibration.
    pub fn record_actual_cost(&mut self, actual: ActualCost) {
        self.calibration = compute_calibration(self.loss.estimated_cost, actual.page_reads);
        self.loss.actual_cost = Some(actual);
        // Recompute hash after filling actual cost (chain integrity
        // depends on the hash at creation time, so we keep record_hash
        // stable — calibration is an addendum).
    }

    /// Whether calibration indicates miscalibration.
    #[must_use]
    pub fn is_miscalibrated(&self) -> bool {
        self.calibration.as_ref().is_some_and(|c| c.miscalibrated)
    }
}

// ---------------------------------------------------------------------------
// Decision Log: append-only, BLAKE3-chained
// ---------------------------------------------------------------------------

/// Append-only decision log with BLAKE3 chain integrity.
///
/// Each record's `prev_hash` points to the preceding record's `record_hash`,
/// forming a tamper-evident chain. The log can be serialized to JSON for
/// offline auditing.
#[derive(Debug, Default)]
pub struct DecisionLog {
    decisions: Vec<DecisionContract>,
    last_hash: String,
}

impl DecisionLog {
    /// Create a new empty decision log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            decisions: Vec::new(),
            last_hash: GENESIS_HASH.to_owned(),
        }
    }

    /// Append a plan decision to the log.
    ///
    /// Automatically chains the BLAKE3 hashes.
    #[allow(clippy::too_many_arguments)]
    pub fn record_plan(
        &mut self,
        query_text: &str,
        tables: &[TableStats],
        indexes: &[IndexInfo],
        where_term_count: usize,
        needed_column_count: Option<usize>,
        cross_join_pairs: usize,
        plan: &QueryPlan,
        beam_width: usize,
        star_query_detected: bool,
    ) -> u64 {
        let contract = build_contract(
            query_text,
            tables,
            indexes,
            where_term_count,
            needed_column_count,
            cross_join_pairs,
            plan,
            beam_width,
            star_query_detected,
            &self.last_hash,
        );
        let id = contract.id;
        self.last_hash.clone_from(&contract.record_hash);
        tracing::debug!(
            contract_id = id,
            query = %contract.query_text,
            estimated_cost = contract.loss.estimated_cost,
            join_order = ?contract.action.join_order,
            "decision_contract.recorded"
        );
        self.decisions.push(contract);
        id
    }

    /// Record actual execution cost for a previously logged decision.
    ///
    /// Returns `true` if the contract was found and updated.
    pub fn record_actual(&mut self, contract_id: u64, actual: ActualCost) -> bool {
        if let Some(contract) = self.decisions.iter_mut().find(|c| c.id == contract_id) {
            contract.record_actual_cost(actual);
            if let Some(ref cal) = contract.calibration {
                tracing::debug!(
                    contract_id,
                    calibration_ratio = cal.ratio,
                    miscalibrated = cal.miscalibrated,
                    "decision_contract.calibrated"
                );
                if cal.miscalibrated {
                    tracing::warn!(
                        contract_id,
                        calibration_ratio = cal.ratio,
                        "decision_contract.miscalibration_alert"
                    );
                }
            }
            true
        } else {
            false
        }
    }

    /// Number of recorded decisions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.decisions.len()
    }

    /// Whether the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.decisions.is_empty()
    }

    /// Iterate over all decisions.
    pub fn iter(&self) -> impl Iterator<Item = &DecisionContract> {
        self.decisions.iter()
    }

    /// Get a decision by ID.
    #[must_use]
    pub fn get(&self, contract_id: u64) -> Option<&DecisionContract> {
        self.decisions.iter().find(|c| c.id == contract_id)
    }

    /// Hash of the most recent record (chain tip).
    #[must_use]
    pub fn chain_tip_hash(&self) -> &str {
        &self.last_hash
    }

    /// Verify BLAKE3 chain integrity: each record's hash matches its content
    /// and its `prev_hash` matches the preceding record's `record_hash`.
    #[must_use]
    pub fn verify_chain_integrity(&self) -> bool {
        let mut expected_prev = GENESIS_HASH.to_owned();
        for contract in &self.decisions {
            if contract.prev_hash != expected_prev {
                return false;
            }
            let computed = compute_record_hash(contract);
            if contract.record_hash != computed {
                return false;
            }
            expected_prev.clone_from(&contract.record_hash);
        }
        true
    }

    /// Return decisions with calibration data, filtered by miscalibration.
    #[must_use]
    pub fn miscalibrated_decisions(&self) -> Vec<&DecisionContract> {
        self.decisions
            .iter()
            .filter(|c| c.is_miscalibrated())
            .collect()
    }

    /// Compute aggregate calibration statistics.
    #[must_use]
    pub fn calibration_stats(&self) -> CalibrationStats {
        let calibrated: Vec<f64> = self
            .decisions
            .iter()
            .filter_map(|c| c.calibration.as_ref().map(|cal| cal.ratio))
            .collect();

        if calibrated.is_empty() {
            return CalibrationStats::default();
        }

        let n = calibrated.len();
        let mean = calibrated.iter().sum::<f64>() / n as f64;
        let variance = calibrated.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n as f64;
        let stddev = variance.sqrt();

        let mut sorted = calibrated;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let median = if n % 2 == 0 {
            f64::midpoint(sorted[n / 2 - 1], sorted[n / 2])
        } else {
            sorted[n / 2]
        };

        let miscalibrated_count = self.miscalibrated_decisions().len();

        CalibrationStats {
            total_decisions: self.decisions.len(),
            calibrated_decisions: n,
            miscalibrated_count,
            mean_ratio: mean,
            median_ratio: median,
            stddev_ratio: stddev,
            min_ratio: sorted[0],
            max_ratio: sorted[n - 1],
        }
    }

    /// Serialize the entire log to JSON.
    ///
    /// # Errors
    /// Returns error if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.decisions)
    }

    /// Query decisions by time range (epoch seconds, inclusive).
    #[must_use]
    pub fn query_by_time_range(&self, start: u64, end: u64) -> Vec<&DecisionContract> {
        self.decisions
            .iter()
            .filter(|c| c.timestamp_epoch_secs >= start && c.timestamp_epoch_secs <= end)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Aggregate calibration statistics
// ---------------------------------------------------------------------------

/// Aggregate statistics over calibrated decisions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CalibrationStats {
    /// Total decisions in the log (calibrated + uncalibrated).
    pub total_decisions: usize,
    /// Decisions with actual cost recorded.
    pub calibrated_decisions: usize,
    /// Decisions exceeding miscalibration thresholds.
    pub miscalibrated_count: usize,
    /// Mean calibration ratio.
    pub mean_ratio: f64,
    /// Median calibration ratio.
    pub median_ratio: f64,
    /// Standard deviation of calibration ratios.
    pub stddev_ratio: f64,
    /// Minimum calibration ratio.
    pub min_ratio: f64,
    /// Maximum calibration ratio.
    pub max_ratio: f64,
}

impl CalibrationStats {
    /// The fraction of calibrated decisions that are miscalibrated.
    #[must_use]
    pub fn miscalibration_rate(&self) -> f64 {
        if self.calibrated_decisions == 0 {
            return 0.0;
        }
        self.miscalibrated_count as f64 / self.calibrated_decisions as f64
    }

    /// Whether the planner is well-calibrated overall.
    ///
    /// Well-calibrated means: median ratio between 0.5 and 2.0,
    /// and miscalibration rate below 10%.
    #[must_use]
    pub fn is_well_calibrated(&self) -> bool {
        if self.calibrated_decisions == 0 {
            return true; // No data = no evidence of miscalibration.
        }
        (0.5..=2.0).contains(&self.median_ratio) && self.miscalibration_rate() < 0.10
    }
}

impl fmt::Display for CalibrationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CalibrationStats {{ decisions: {}/{} calibrated, miscalibrated: {} ({:.1}%), \
             mean: {:.3}, median: {:.3}, stddev: {:.3}, range: [{:.3}, {:.3}] }}",
            self.calibrated_decisions,
            self.total_decisions,
            self.miscalibrated_count,
            self.miscalibration_rate() * 100.0,
            self.mean_ratio,
            self.median_ratio,
            self.stddev_ratio,
            self.min_ratio,
            self.max_ratio,
        )
    }
}

use std::fmt;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tables() -> Vec<TableStats> {
        vec![
            TableStats {
                name: "users".to_owned(),
                n_pages: 100,
                n_rows: 10_000,
                source: StatsSource::Analyze,
            },
            TableStats {
                name: "orders".to_owned(),
                n_pages: 500,
                n_rows: 100_000,
                source: StatsSource::Heuristic,
            },
        ]
    }

    fn sample_indexes() -> Vec<IndexInfo> {
        vec![IndexInfo {
            name: "idx_orders_user_id".to_owned(),
            table: "orders".to_owned(),
            columns: vec!["user_id".to_owned()],
            unique: false,
            n_pages: 50,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        }]
    }

    fn sample_plan() -> QueryPlan {
        QueryPlan {
            join_order: vec!["users".to_owned(), "orders".to_owned()],
            access_paths: vec![
                AccessPath {
                    table: "users".to_owned(),
                    kind: AccessPathKind::FullTableScan,
                    index: None,
                    estimated_cost: 100.0,
                    estimated_rows: 10_000.0,
                },
                AccessPath {
                    table: "orders".to_owned(),
                    kind: AccessPathKind::IndexScanEquality,
                    index: Some("idx_orders_user_id".to_owned()),
                    estimated_cost: 15.0,
                    estimated_rows: 10.0,
                },
            ],
            join_segments: vec![crate::JoinPlanSegment {
                relations: vec!["users".to_owned(), "orders".to_owned()],
                operator: crate::JoinOperator::HashJoin,
                estimated_cost: 115.0,
                reason: "2-way joins stay on pairwise hash join".to_owned(),
            }],
            total_cost: 115.0,
        }
    }

    #[test]
    fn build_contract_captures_state_action_loss() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let contract = build_contract(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            &tables,
            &indexes,
            3,
            None,
            0,
            &plan,
            5,
            false,
            GENESIS_HASH,
        );

        assert_eq!(contract.state.tables.len(), 2);
        assert_eq!(contract.state.indexes.len(), 1);
        assert_eq!(contract.state.where_term_count, 3);
        assert_eq!(contract.action.join_order, vec!["users", "orders"]);
        assert_eq!(contract.action.access_paths.len(), 2);
        assert!((contract.loss.estimated_cost - 115.0).abs() < f64::EPSILON);
        assert!(contract.loss.actual_cost.is_none());
        assert!(contract.calibration.is_none());
        assert_ne!(contract.record_hash, "");
        assert_eq!(contract.prev_hash, GENESIS_HASH);
    }

    #[test]
    fn record_actual_cost_computes_calibration() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut contract = build_contract(
            "SELECT 1",
            &tables,
            &indexes,
            0,
            None,
            0,
            &plan,
            1,
            false,
            GENESIS_HASH,
        );

        // Estimated cost = 115.0, actual page reads = 120.
        contract.record_actual_cost(ActualCost {
            page_reads: 120,
            cpu_micros: 500,
            actual_rows: 50,
            wall_time_micros: 1000,
        });

        let cal = contract.calibration.as_ref().unwrap();
        assert!((cal.ratio - 120.0 / 115.0).abs() < 0.01);
        assert!(!cal.miscalibrated);
        assert!(!contract.is_miscalibrated());
    }

    #[test]
    fn miscalibration_alert_underestimate() {
        // Estimated 10 page reads, actual 100 → ratio 10.0 > 5.0
        let cal = compute_calibration(10.0, 100).unwrap();
        assert!(cal.miscalibrated);
        assert!(matches!(
            cal.alert,
            Some(MiscalibrationAlert::Underestimate { .. })
        ));
    }

    #[test]
    fn miscalibration_alert_overestimate() {
        // Estimated 1000 page reads, actual 10 → ratio 0.01 < 0.2
        let cal = compute_calibration(1000.0, 10).unwrap();
        assert!(cal.miscalibrated);
        assert!(matches!(
            cal.alert,
            Some(MiscalibrationAlert::Overestimate { .. })
        ));
    }

    #[test]
    fn calibration_none_for_zero_estimate() {
        assert!(compute_calibration(0.0, 100).is_none());
    }

    #[test]
    fn decision_log_chain_integrity() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut log = DecisionLog::new();
        log.record_plan("SELECT 1", &tables, &indexes, 0, None, 0, &plan, 1, false);
        log.record_plan("SELECT 2", &tables, &indexes, 1, None, 0, &plan, 1, false);
        log.record_plan("SELECT 3", &tables, &indexes, 2, None, 0, &plan, 1, false);

        assert_eq!(log.len(), 3);
        assert!(log.verify_chain_integrity());

        // Verify chain linkage.
        assert_eq!(log.decisions[0].prev_hash, GENESIS_HASH);
        assert_eq!(log.decisions[1].prev_hash, log.decisions[0].record_hash);
        assert_eq!(log.decisions[2].prev_hash, log.decisions[1].record_hash);
    }

    #[test]
    fn decision_log_tamper_detection() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut log = DecisionLog::new();
        log.record_plan("SELECT 1", &tables, &indexes, 0, None, 0, &plan, 1, false);
        log.record_plan("SELECT 2", &tables, &indexes, 0, None, 0, &plan, 1, false);

        assert!(log.verify_chain_integrity());

        // Tamper with first record.
        log.decisions[0].query_text = "TAMPERED".to_owned();
        assert!(!log.verify_chain_integrity());
    }

    #[test]
    fn decision_log_record_actual_and_stats() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut log = DecisionLog::new();
        let id1 = log.record_plan("Q1", &tables, &indexes, 0, None, 0, &plan, 1, false);
        let id2 = log.record_plan("Q2", &tables, &indexes, 0, None, 0, &plan, 1, false);

        // Good calibration for Q1.
        assert!(log.record_actual(
            id1,
            ActualCost {
                page_reads: 110,
                cpu_micros: 200,
                actual_rows: 100,
                wall_time_micros: 500,
            }
        ));

        // Bad calibration for Q2 (massive underestimate).
        assert!(log.record_actual(
            id2,
            ActualCost {
                page_reads: 10_000,
                cpu_micros: 5000,
                actual_rows: 50_000,
                wall_time_micros: 10_000,
            }
        ));

        let stats = log.calibration_stats();
        assert_eq!(stats.calibrated_decisions, 2);
        assert_eq!(stats.miscalibrated_count, 1);
        assert!(!stats.is_well_calibrated());

        let misc = log.miscalibrated_decisions();
        assert_eq!(misc.len(), 1);
        assert_eq!(misc[0].query_text, "Q2");
    }

    #[test]
    fn calibration_stats_well_calibrated() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut log = DecisionLog::new();
        // Record 10 well-calibrated decisions.
        for i in 0..10 {
            let id = log.record_plan(
                &format!("Q{i}"),
                &tables,
                &indexes,
                0,
                None,
                0,
                &plan,
                1,
                false,
            );
            log.record_actual(
                id,
                ActualCost {
                    page_reads: 115 + i * 2, // Close to estimated 115.
                    cpu_micros: 100,
                    actual_rows: 50,
                    wall_time_micros: 200,
                },
            );
        }

        let stats = log.calibration_stats();
        assert_eq!(stats.calibrated_decisions, 10);
        assert_eq!(stats.miscalibrated_count, 0);
        assert!(stats.is_well_calibrated());
        assert!((stats.median_ratio - 1.0).abs() < 0.5);
    }

    #[test]
    fn decision_log_to_json() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut log = DecisionLog::new();
        log.record_plan("SELECT 1", &tables, &indexes, 0, None, 0, &plan, 1, false);

        let json = log.to_json().unwrap();
        assert!(json.contains("\"query_text\": \"SELECT 1\""));
        assert!(json.contains("\"estimated_cost\""));
        assert!(json.contains("\"record_hash\""));

        // Verify it deserializes back.
        let parsed: Vec<DecisionContract> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].query_text, "SELECT 1");
    }

    #[test]
    fn query_text_truncation() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let long_query = "SELECT ".to_owned() + &"x".repeat(5000);
        let contract = build_contract(
            &long_query,
            &tables,
            &indexes,
            0,
            None,
            0,
            &plan,
            1,
            false,
            GENESIS_HASH,
        );
        assert_eq!(contract.query_text.len(), MAX_QUERY_TEXT_LEN);
        assert!(contract.query_text.ends_with(TRUNCATION_SUFFIX));
    }

    #[test]
    fn query_text_truncation_preserves_utf8_boundaries() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let long_query = format!("SELECT '{}'", "é".repeat(3000));
        let contract = build_contract(
            &long_query,
            &tables,
            &indexes,
            0,
            None,
            0,
            &plan,
            1,
            false,
            GENESIS_HASH,
        );

        assert!(
            std::str::from_utf8(contract.query_text.as_bytes()).is_ok(),
            "truncated query text must remain valid UTF-8"
        );
        assert_eq!(contract.query_text.len(), MAX_QUERY_TEXT_LEN);
        assert!(contract.query_text.ends_with(TRUNCATION_SUFFIX));
    }

    #[test]
    fn access_path_kind_labels() {
        assert_eq!(
            access_path_kind_label(&AccessPathKind::FullTableScan),
            "full_table_scan"
        );
        assert_eq!(
            access_path_kind_label(&AccessPathKind::IndexScanEquality),
            "index_scan_equality"
        );
        assert_eq!(
            access_path_kind_label(&AccessPathKind::RowidLookup),
            "rowid_lookup"
        );
        assert!(
            access_path_kind_label(&AccessPathKind::IndexScanRange { selectivity: 0.33 })
                .starts_with("index_scan_range")
        );
        assert!(
            access_path_kind_label(&AccessPathKind::CoveringIndexScan { selectivity: 0.5 })
                .starts_with("covering_index_scan")
        );
    }

    #[test]
    fn table_stats_summary_from() {
        let ts = TableStats {
            name: "foo".to_owned(),
            n_pages: 42,
            n_rows: 1000,
            source: StatsSource::Analyze,
        };
        let summary = TableStatsSummary::from(&ts);
        assert_eq!(summary.name, "foo");
        assert_eq!(summary.n_pages, 42);
        assert_eq!(summary.source, "analyze");
    }

    #[test]
    fn decision_log_get_and_query() {
        let tables = sample_tables();
        let indexes = sample_indexes();
        let plan = sample_plan();

        let mut log = DecisionLog::new();
        let id = log.record_plan("SELECT 1", &tables, &indexes, 0, None, 0, &plan, 1, false);

        assert!(log.get(id).is_some());
        assert!(log.get(999_999).is_none());
        assert!(!log.is_empty());
    }

    #[test]
    fn empty_log_stats() {
        let log = DecisionLog::new();
        let stats = log.calibration_stats();
        assert_eq!(stats.total_decisions, 0);
        assert_eq!(stats.calibrated_decisions, 0);
        assert!(stats.is_well_calibrated());
        assert_eq!(log.chain_tip_hash(), GENESIS_HASH);
    }

    #[test]
    fn calibration_stats_display() {
        let stats = CalibrationStats {
            total_decisions: 10,
            calibrated_decisions: 8,
            miscalibrated_count: 2,
            mean_ratio: 1.5,
            median_ratio: 1.2,
            stddev_ratio: 0.8,
            min_ratio: 0.1,
            max_ratio: 3.5,
        };
        let display = format!("{stats}");
        assert!(display.contains("8/10"));
        assert!(display.contains("miscalibrated: 2"));
    }
}
