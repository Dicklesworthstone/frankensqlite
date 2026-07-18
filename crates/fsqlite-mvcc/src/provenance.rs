//! Provenance semirings for query lineage (bd-19u.3, §8.9).
//!
//! Tracks which input tuples contributed to each output row using semiring
//! annotations propagated through query execution. Supports:
//!
//!   - **Why provenance**: which base tuples justify an output tuple's existence
//!   - **How provenance**: the derivation structure (join/union/project operations)
//!   - **Why-not provenance**: which missing base tuples would produce a missing output
//!
//! # Semiring Algebra
//!
//! Annotations form a commutative semiring `(K, +, *, 0, 1)`:
//!   - `+` models alternative derivations (union/OR)
//!   - `*` models combined contributions (join/AND)
//!   - `0` is the annotation for non-contributing tuples
//!   - `1` is the identity annotation for unconditional contribution
//!
//! # Reference
//!
//! Green, Karvounarakis, Tannen 2007: "Provenance Semirings"

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

// ---------------------------------------------------------------------------
// Global metrics
// ---------------------------------------------------------------------------

/// Total provenance annotations propagated across all queries.
static FSQLITE_PROVENANCE_ANNOTATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total provenance queries served (why/why-not/how).
static FSQLITE_PROVENANCE_QUERIES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total annotated output rows emitted.
static FSQLITE_PROVENANCE_ROWS_EMITTED: AtomicU64 = AtomicU64::new(0);

/// Snapshot of provenance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProvenanceMetrics {
    pub fsqlite_provenance_annotations_total: u64,
    pub fsqlite_provenance_queries_total: u64,
    pub fsqlite_provenance_rows_emitted: u64,
}

/// Take a snapshot of provenance metrics.
#[must_use]
pub fn provenance_metrics() -> ProvenanceMetrics {
    ProvenanceMetrics {
        fsqlite_provenance_annotations_total: FSQLITE_PROVENANCE_ANNOTATIONS_TOTAL
            .load(Ordering::Relaxed),
        fsqlite_provenance_queries_total: FSQLITE_PROVENANCE_QUERIES_TOTAL.load(Ordering::Relaxed),
        fsqlite_provenance_rows_emitted: FSQLITE_PROVENANCE_ROWS_EMITTED.load(Ordering::Relaxed),
    }
}

/// Reset provenance metrics to zero.
pub fn reset_provenance_metrics() {
    FSQLITE_PROVENANCE_ANNOTATIONS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_PROVENANCE_QUERIES_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_PROVENANCE_ROWS_EMITTED.store(0, Ordering::Relaxed);
}

fn record_annotation() {
    FSQLITE_PROVENANCE_ANNOTATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn record_query() {
    FSQLITE_PROVENANCE_QUERIES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn record_row_emitted() {
    FSQLITE_PROVENANCE_ROWS_EMITTED.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Tuple identifiers
// ---------------------------------------------------------------------------

/// A unique identifier for a base table tuple.
///
/// `(table_root_page, rowid)` uniquely identifies any row in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct TupleId {
    /// Root page of the table (identifies the table).
    pub table_root: u32,
    /// Row ID within the table.
    pub rowid: i64,
}

impl TupleId {
    /// Create a new tuple identifier.
    #[must_use]
    pub fn new(table_root: u32, rowid: i64) -> Self {
        Self { table_root, rowid }
    }
}

impl fmt::Display for TupleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{}:{}", self.table_root, self.rowid)
    }
}

// ---------------------------------------------------------------------------
// Provenance token (semiring element)
// ---------------------------------------------------------------------------

/// A provenance token representing the annotation of a derived value.
///
/// In the semiring framework:
///   - `Zero` is the additive identity (tuple not produced)
///   - `One` is the multiplicative identity (unconditional)
///   - `Base(id)` is a generator (one base tuple contributes)
///   - `Plus(a, b)` is alternative derivation: a + b
///   - `Times(a, b)` is combined contribution: a * b
///
/// This corresponds to the polynomial semiring `N[X]` from Green et al. 2007.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum ProvenanceToken {
    /// Additive identity: this value was not produced.
    Zero,
    /// Multiplicative identity: unconditional contribution.
    One,
    /// Base tuple generator.
    Base(TupleId),
    /// Alternative derivation (union/OR).
    Plus(Box<Self>, Box<Self>),
    /// Combined contribution (join/AND).
    Times(Box<Self>, Box<Self>),
}

impl fmt::Debug for ProvenanceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => write!(f, "0"),
            Self::One => write!(f, "1"),
            Self::Base(id) => write!(f, "{id}"),
            Self::Plus(a, b) => write!(f, "({a:?} + {b:?})"),
            Self::Times(a, b) => write!(f, "({a:?} * {b:?})"),
        }
    }
}

impl fmt::Display for ProvenanceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl ProvenanceToken {
    /// Create a base tuple annotation.
    #[must_use]
    pub fn base(table_root: u32, rowid: i64) -> Self {
        record_annotation();
        Self::Base(TupleId::new(table_root, rowid))
    }

    /// Semiring addition: alternative derivation.
    ///
    /// Simplifications: `0 + x = x`, `x + 0 = x`.
    #[must_use]
    pub fn plus(self, other: Self) -> Self {
        record_annotation();
        match (&self, &other) {
            (Self::Zero, _) => other,
            (_, Self::Zero) => self,
            _ => Self::Plus(Box::new(self), Box::new(other)),
        }
    }

    /// Semiring multiplication: combined contribution.
    ///
    /// Simplifications: `0 * x = 0`, `1 * x = x`, `x * 0 = 0`, `x * 1 = x`.
    #[must_use]
    pub fn times(self, other: Self) -> Self {
        record_annotation();
        match (&self, &other) {
            (Self::Zero, _) | (_, Self::Zero) => Self::Zero,
            (Self::One, _) => other,
            (_, Self::One) => self,
            _ => Self::Times(Box::new(self), Box::new(other)),
        }
    }

    /// Check if this is the zero element.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        matches!(self, Self::Zero)
    }

    /// Check if this is the one element.
    #[must_use]
    pub fn is_one(&self) -> bool {
        matches!(self, Self::One)
    }

    /// Extract the set of base tuple IDs (why provenance).
    ///
    /// Returns all generator `TupleId`s reachable in the expression tree.
    #[must_use]
    pub fn why_provenance(&self) -> BTreeSet<TupleId> {
        record_query();
        let mut result = BTreeSet::new();
        self.collect_base_ids(&mut result);
        result
    }

    fn collect_base_ids(&self, out: &mut BTreeSet<TupleId>) {
        match self {
            Self::Zero | Self::One => {}
            Self::Base(id) => {
                out.insert(*id);
            }
            Self::Plus(a, b) | Self::Times(a, b) => {
                a.collect_base_ids(out);
                b.collect_base_ids(out);
            }
        }
    }

    /// Count the number of nodes in the provenance expression tree.
    #[must_use]
    pub fn tree_size(&self) -> usize {
        match self {
            Self::Zero | Self::One | Self::Base(_) => 1,
            Self::Plus(a, b) | Self::Times(a, b) => 1 + a.tree_size() + b.tree_size(),
        }
    }

    /// Compute the how-provenance representation as a human-readable string.
    #[must_use]
    pub fn how_provenance(&self) -> String {
        record_query();
        format!("{self:?}")
    }
}

// ---------------------------------------------------------------------------
// Provenance set (materialized annotation for a result row)
// ---------------------------------------------------------------------------

/// A materialized provenance annotation for one output row.
///
/// Contains the full semiring token for "how" provenance and a lazily-computed
/// "why" contributor set.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct ProvenanceAnnotation {
    /// The semiring expression describing how this row was derived.
    pub token: ProvenanceToken,
    /// Output row index (0-based within the result set).
    pub output_row: u64,
}

impl fmt::Debug for ProvenanceAnnotation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProvenanceAnnotation")
            .field("output_row", &self.output_row)
            .field("token", &self.token)
            .finish()
    }
}

impl ProvenanceAnnotation {
    /// Create a new annotation for an output row.
    pub fn new(output_row: u64, token: ProvenanceToken) -> Self {
        record_row_emitted();
        tracing::debug!(
            target: "fsqlite.provenance",
            output_row,
            tree_size = token.tree_size(),
            "provenance_annotation_created"
        );
        Self { token, output_row }
    }

    /// Get the why-provenance (contributing base tuples).
    #[must_use]
    pub fn why(&self) -> BTreeSet<TupleId> {
        self.token.why_provenance()
    }

    /// Get the how-provenance (derivation expression).
    #[must_use]
    pub fn how(&self) -> String {
        self.token.how_provenance()
    }
}

// ---------------------------------------------------------------------------
// Provenance tracker (per-query execution context)
// ---------------------------------------------------------------------------

/// Provenance query mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ProvenanceMode {
    /// No provenance tracking (default, zero overhead).
    Disabled,
    /// Track why-provenance only (set of contributing tuples).
    Why,
    /// Track full how-provenance (semiring expression tree).
    How,
}

/// Per-query provenance tracker.
///
/// Attached to a `VdbeEngine` execution context when provenance tracking is
/// enabled. Maintains per-register annotation state that flows through opcodes.
pub struct ProvenanceTracker {
    mode: ProvenanceMode,
    query_id: u64,
    /// Per-register provenance annotation (parallel to the register file).
    register_annotations: Vec<ProvenanceToken>,
    /// Collected output row annotations.
    output_annotations: Vec<ProvenanceAnnotation>,
    /// Total annotations propagated in this query.
    annotations_propagated: u64,
}

impl fmt::Debug for ProvenanceTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProvenanceTracker")
            .field("mode", &self.mode)
            .field("query_id", &self.query_id)
            .field("register_count", &self.register_annotations.len())
            .field("output_count", &self.output_annotations.len())
            .field("annotations_propagated", &self.annotations_propagated)
            .finish()
    }
}

impl ProvenanceTracker {
    /// Create a new provenance tracker.
    #[must_use]
    pub fn new(mode: ProvenanceMode, query_id: u64, register_count: usize) -> Self {
        tracing::info!(
            target: "fsqlite.provenance",
            query_id,
            ?mode,
            register_count,
            "provenance_tracker_created"
        );
        Self {
            mode,
            query_id,
            register_annotations: vec![ProvenanceToken::Zero; register_count],
            output_annotations: Vec::new(),
            annotations_propagated: 0,
        }
    }

    /// Get the tracking mode.
    #[must_use]
    pub fn mode(&self) -> ProvenanceMode {
        self.mode
    }

    /// Get the query ID.
    #[must_use]
    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    /// Annotate a register with a base tuple provenance.
    ///
    /// Called when a Column opcode reads a value from a cursor.
    pub fn annotate_base(&mut self, register: usize, table_root: u32, rowid: i64) {
        if self.mode == ProvenanceMode::Disabled {
            return;
        }
        if register < self.register_annotations.len() {
            self.register_annotations[register] = ProvenanceToken::base(table_root, rowid);
            self.annotations_propagated += 1;
        }
    }

    /// Propagate a copy: `dst = src` annotation.
    pub fn propagate_copy(&mut self, dst: usize, src: usize) {
        if self.mode == ProvenanceMode::Disabled {
            return;
        }
        if src < self.register_annotations.len() && dst < self.register_annotations.len() {
            self.register_annotations[dst] = self.register_annotations[src].clone();
            self.annotations_propagated += 1;
        }
    }

    /// Propagate a binary operation: `dst = f(a, b)`.
    ///
    /// The result's provenance is `a * b` (both inputs contribute).
    pub fn propagate_binary(&mut self, dst: usize, src_a: usize, src_b: usize) {
        if self.mode == ProvenanceMode::Disabled {
            return;
        }
        let len = self.register_annotations.len();
        if src_a < len && src_b < len && dst < len {
            let token_a = self.register_annotations[src_a].clone();
            let token_b = self.register_annotations[src_b].clone();
            self.register_annotations[dst] = token_a.times(token_b);
            self.annotations_propagated += 1;
        }
    }

    /// Propagate a union: `dst = dst + src`.
    ///
    /// Used for UNION operations where the same output position can be
    /// produced by multiple derivations.
    pub fn propagate_union(&mut self, dst: usize, src: usize) {
        if self.mode == ProvenanceMode::Disabled {
            return;
        }
        let len = self.register_annotations.len();
        if src < len && dst < len {
            let existing = self.register_annotations[dst].clone();
            let incoming = self.register_annotations[src].clone();
            self.register_annotations[dst] = existing.plus(incoming);
            self.annotations_propagated += 1;
        }
    }

    /// Clear a register's annotation (e.g., for Null opcode).
    pub fn clear_register(&mut self, register: usize) {
        if register < self.register_annotations.len() {
            self.register_annotations[register] = ProvenanceToken::Zero;
        }
    }

    /// Record a result row emission.
    ///
    /// Collects the provenance for the specified register range and creates
    /// an output annotation with the combined (times) provenance of all
    /// output columns.
    pub fn record_result_row(&mut self, registers: &[usize]) {
        if self.mode == ProvenanceMode::Disabled {
            return;
        }

        let output_row = self.output_annotations.len() as u64;
        let len = self.register_annotations.len();

        // Combine all output column annotations with `times`.
        let mut combined = ProvenanceToken::One;
        for &reg in registers {
            if reg < len {
                combined = combined.times(self.register_annotations[reg].clone());
            }
        }

        let annotation = ProvenanceAnnotation::new(output_row, combined);

        tracing::trace!(
            target: "fsqlite.provenance",
            query_id = self.query_id,
            annotations_propagated = self.annotations_propagated,
            "provenance_track"
        );

        self.output_annotations.push(annotation);
    }

    /// Get the annotation for a specific register.
    #[must_use]
    pub fn register_annotation(&self, register: usize) -> Option<&ProvenanceToken> {
        self.register_annotations.get(register)
    }

    /// Get all output annotations.
    #[must_use]
    pub fn output_annotations(&self) -> &[ProvenanceAnnotation] {
        &self.output_annotations
    }

    /// Total annotations propagated during this query.
    #[must_use]
    pub fn annotations_propagated(&self) -> u64 {
        self.annotations_propagated
    }

    /// Produce a summary report.
    #[must_use]
    pub fn summary(&self) -> ProvenanceReport {
        ProvenanceReport {
            query_id: self.query_id,
            mode: self.mode,
            output_rows: self.output_annotations.len() as u64,
            annotations_propagated: self.annotations_propagated,
            annotations: self.output_annotations.clone(),
        }
    }
}

/// Summary report of provenance tracking for one query.
#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceReport {
    pub query_id: u64,
    pub mode: ProvenanceMode,
    pub output_rows: u64,
    pub annotations_propagated: u64,
    pub annotations: Vec<ProvenanceAnnotation>,
}

// ---------------------------------------------------------------------------
// Why-not provenance (witness generation)
// ---------------------------------------------------------------------------

/// Result of a why-not provenance query.
///
/// Explains why a particular tuple was NOT in the output.
#[derive(Debug, Clone, Serialize)]
pub struct WhyNotResult {
    /// The tuple that was expected but missing.
    pub missing_tuple: Vec<String>,
    /// Base tuples that would need to exist for the tuple to appear.
    pub missing_witnesses: Vec<TupleId>,
    /// Explanation text.
    pub explanation: String,
}

/// Compute why-not provenance for a missing output tuple.
///
/// Given a set of existing base tuples and expected contributors, identifies
/// which base tuples are missing that would produce the expected output.
#[must_use]
pub fn why_not(
    existing_base: &BTreeSet<TupleId>,
    expected_contributors: &[TupleId],
) -> WhyNotResult {
    record_query();

    let missing: Vec<TupleId> = expected_contributors
        .iter()
        .filter(|id| !existing_base.contains(id))
        .copied()
        .collect();

    let explanation = if missing.is_empty() {
        "All expected base tuples exist; the missing output may be due to a filter predicate."
            .to_string()
    } else {
        format!(
            "Missing {} base tuple(s): {}",
            missing.len(),
            missing
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };

    tracing::info!(
        target: "fsqlite.provenance",
        missing_count = missing.len(),
        "why_not_result"
    );

    WhyNotResult {
        missing_tuple: Vec::new(),
        missing_witnesses: missing,
        explanation,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semiring_zero_identity() {
        let a = ProvenanceToken::base(1, 42);

        // 0 + a = a
        let result = ProvenanceToken::Zero.plus(a.clone());
        assert_eq!(result, a);

        // a + 0 = a
        let result2 = a.clone().plus(ProvenanceToken::Zero);
        assert_eq!(result2, a);

        // 0 * a = 0
        let result3 = ProvenanceToken::Zero.times(a.clone());
        assert!(result3.is_zero());

        // a * 0 = 0
        let result4 = a.times(ProvenanceToken::Zero);
        assert!(result4.is_zero());

        println!("[PASS] semiring zero identity laws");
    }

    #[test]
    fn test_semiring_one_identity() {
        let a = ProvenanceToken::base(1, 42);

        // 1 * a = a
        let result = ProvenanceToken::One.times(a.clone());
        assert_eq!(result, a);

        // a * 1 = a
        let result2 = a.clone().times(ProvenanceToken::One);
        assert_eq!(result2, a);

        println!("[PASS] semiring one identity laws");
    }

    #[test]
    fn test_why_provenance() {
        let t1 = ProvenanceToken::base(1, 10);
        let t2 = ProvenanceToken::base(2, 20);
        let t3 = ProvenanceToken::base(1, 30);

        // Join: t1 * t2
        let join = t1.times(t2);
        // Union: join + t3
        let result = join.plus(t3);

        let why = result.why_provenance();
        assert_eq!(why.len(), 3);
        assert!(why.contains(&TupleId::new(1, 10)));
        assert!(why.contains(&TupleId::new(2, 20)));
        assert!(why.contains(&TupleId::new(1, 30)));

        println!("[PASS] why provenance: 3 contributors extracted");
    }

    #[test]
    fn test_how_provenance() {
        let t1 = ProvenanceToken::base(1, 10);
        let t2 = ProvenanceToken::base(2, 20);
        let join = t1.times(t2);

        let how = join.how_provenance();
        assert!(how.contains("t1:10"));
        assert!(how.contains("t2:20"));
        assert!(how.contains('*'));

        println!("[PASS] how provenance: derivation expression contains join");
    }

    #[test]
    fn test_tracker_basic_flow() {
        let mut tracker = ProvenanceTracker::new(ProvenanceMode::How, 1, 10);

        // Simulate: Column cursor=0 col=0 reg=1 (from table 100, rowid 42)
        tracker.annotate_base(1, 100, 42);
        // Simulate: Column cursor=1 col=0 reg=2 (from table 200, rowid 99)
        tracker.annotate_base(2, 200, 99);
        // Simulate: Add reg=1 reg=2 reg=3
        tracker.propagate_binary(3, 1, 2);
        // Simulate: ResultRow regs=[1, 3]
        tracker.record_result_row(&[1, 3]);

        let annotations = tracker.output_annotations();
        assert_eq!(annotations.len(), 1);

        let why = annotations[0].why();
        assert_eq!(why.len(), 2);
        assert!(why.contains(&TupleId::new(100, 42)));
        assert!(why.contains(&TupleId::new(200, 99)));

        println!("[PASS] tracker basic flow: annotate -> propagate -> result");
    }

    #[test]
    fn test_tracker_disabled_mode() {
        let mut tracker = ProvenanceTracker::new(ProvenanceMode::Disabled, 1, 10);

        tracker.annotate_base(1, 100, 42);
        tracker.record_result_row(&[1]);

        // In disabled mode, no annotations should be collected.
        assert!(tracker.output_annotations().is_empty());
        assert_eq!(tracker.annotations_propagated(), 0);

        println!("[PASS] tracker disabled mode: zero overhead");
    }

    #[test]
    fn test_why_not() {
        let mut existing = BTreeSet::new();
        existing.insert(TupleId::new(1, 10));
        existing.insert(TupleId::new(1, 20));

        let expected = vec![TupleId::new(1, 10), TupleId::new(2, 30)];

        let result = why_not(&existing, &expected);
        assert_eq!(result.missing_witnesses.len(), 1);
        assert_eq!(result.missing_witnesses[0], TupleId::new(2, 30));

        println!("[PASS] why-not: identified 1 missing witness");
    }

    #[test]
    fn test_tree_size() {
        let t1 = ProvenanceToken::base(1, 10);
        let t2 = ProvenanceToken::base(2, 20);
        let t3 = ProvenanceToken::base(3, 30);

        assert_eq!(t1.tree_size(), 1);

        let join = t1.times(t2);
        assert_eq!(join.tree_size(), 3); // Times + 2 Base

        let union = join.plus(t3);
        assert_eq!(union.tree_size(), 5); // Plus + Times + 3 Base

        println!("[PASS] tree size: correctly counted expression nodes");
    }

    #[test]
    fn test_metrics_integration() {
        let before = provenance_metrics();

        let mut tracker = ProvenanceTracker::new(ProvenanceMode::How, 42, 5);
        tracker.annotate_base(0, 1, 10);
        tracker.annotate_base(1, 2, 20);
        tracker.propagate_binary(2, 0, 1);
        tracker.record_result_row(&[0, 2]);

        let after = provenance_metrics();
        let annotations_delta = after.fsqlite_provenance_annotations_total
            - before.fsqlite_provenance_annotations_total;
        let rows_delta =
            after.fsqlite_provenance_rows_emitted - before.fsqlite_provenance_rows_emitted;
        assert!(
            annotations_delta > 0,
            "expected annotations delta > 0, got {annotations_delta}"
        );
        assert!(rows_delta > 0, "expected rows delta > 0, got {rows_delta}");

        let json = serde_json::to_string(&after).unwrap();
        assert!(json.contains("fsqlite_provenance_annotations_total"));

        println!("[PASS] metrics: annotations_delta={annotations_delta} rows_delta={rows_delta}");
    }

    #[test]
    fn test_report_summary() {
        let mut tracker = ProvenanceTracker::new(ProvenanceMode::Why, 99, 5);
        tracker.annotate_base(0, 1, 10);
        tracker.record_result_row(&[0]);
        tracker.annotate_base(0, 1, 20);
        tracker.record_result_row(&[0]);

        let report = tracker.summary();
        assert_eq!(report.query_id, 99);
        assert_eq!(report.output_rows, 2);
        assert!(report.annotations_propagated > 0);

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"query_id\":99"));

        println!("[PASS] report summary: {} output rows", report.output_rows);
    }
}
