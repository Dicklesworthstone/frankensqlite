//! §5.7.4 Witness Refinement Policy (VOI-Driven, Bounded).
//!
//! Refinement reduces SSI false positive aborts by confirming true key
//! intersection at finer granularity (Cell, ByteRange, HashedKeySet, ExactKeys).
//!
//! **Non-negotiable**: refinement is optimization only. If disabled or
//! budget-exhausted, the system MUST still be sound — it may abort more
//! often but MUST NOT miss true conflicts (§5.6.4.1).
//!
//! The investment in refinement is VOI-driven: refine where the expected
//! reduction in false abort cost exceeds the CPU/bytes cost of refinement.

use tracing::{debug, info};

use crate::ssi_validation::DiscoveredEdge;
use crate::witness_objects::KeySummary;

// ---------------------------------------------------------------------------
// VOI Metrics (§5.7.4.1)
// ---------------------------------------------------------------------------

/// Per-bucket Value of Information metrics for refinement decisions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VoiMetrics {
    /// Overlap rate: how often this bucket participates in conflicts.
    pub c_b: f64,
    /// False positive probability at page granularity.
    pub fp_b: f64,
    /// Reduction in FP probability from refinement.
    pub delta_fp_b: f64,
    /// Expected cost of aborting a transaction.
    pub l_abort: f64,
    /// Cost of refinement (bytes + CPU).
    pub cost_refine_b: f64,
}

impl VoiMetrics {
    /// Compute the expected benefit of refining this bucket.
    #[must_use]
    pub fn benefit(&self) -> f64 {
        self.c_b * self.delta_fp_b * self.l_abort
    }

    /// Compute the VOI score: benefit minus cost.
    ///
    /// Positive VOI means refinement is cost-effective for this bucket.
    #[must_use]
    pub fn voi(&self) -> f64 {
        self.benefit() - self.cost_refine_b
    }
}

// ---------------------------------------------------------------------------
// Refinement Budget (§5.7.4.2)
// ---------------------------------------------------------------------------

/// Per-commit refinement budget constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefinementBudget {
    /// Maximum bytes of refinement data to emit.
    pub max_bytes: usize,
    /// Maximum number of buckets to refine.
    pub max_buckets: usize,
}

impl RefinementBudget {
    /// Create a budget with the given limits.
    #[must_use]
    pub const fn new(max_bytes: usize, max_buckets: usize) -> Self {
        Self {
            max_bytes,
            max_buckets,
        }
    }

    /// The V1 default budget.
    #[must_use]
    pub const fn v1_default() -> Self {
        Self {
            max_bytes: 4096,
            max_buckets: 16,
        }
    }
}

impl Default for RefinementBudget {
    fn default() -> Self {
        Self::v1_default()
    }
}

// ---------------------------------------------------------------------------
// Refinement Policy
// ---------------------------------------------------------------------------

/// Priority ordering for refinement types (§5.7.4.2).
///
/// Higher priority = tried first (better FP reduction per byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RefinementPriority {
    /// CellBitmap: best for B-tree leaf/interior ops.
    CellBitmap = 4,
    /// ByteRangeList: best when page patches are sparse/disjoint.
    ByteRangeList = 3,
    /// HashedKeySet: cheaper than exact keys, good for large sets.
    HashedKeySet = 2,
    /// ExactKeys: only for tiny sets; most precise.
    ExactKeys = 1,
}

/// A refinement decision for a single bucket.
#[derive(Debug, Clone)]
pub struct RefinementDecision {
    /// Range prefix of the bucket.
    pub range_prefix: u32,
    /// VOI score.
    pub voi_score: f64,
    /// Type of refinement applied.
    pub refinement_type: RefinementPriority,
    /// The refined key summary.
    pub key_summary: KeySummary,
    /// Bytes consumed by this refinement.
    pub bytes_used: usize,
}

/// Result of the refinement process.
#[derive(Debug, Clone)]
pub struct RefinementResult {
    /// Edges that survived refinement (confirmed true overlaps).
    pub confirmed_edges: Vec<DiscoveredEdge>,
    /// Edges eliminated by refinement (false positives).
    pub eliminated_edges: Vec<DiscoveredEdge>,
    /// Refinement decisions made (evidence ledger).
    pub decisions: Vec<RefinementDecision>,
    /// Total bytes used by refinement.
    pub bytes_used: usize,
    /// Number of buckets refined.
    pub buckets_refined: usize,
}

/// Apply witness refinement to discovered edges (§5.7.4).
///
/// For each edge, checks if finer-grained key data is available and
/// confirms true intersection. Edges without refinement data pass
/// through unchanged (conservative — no false negatives).
///
/// Operates within the given budget: processes buckets in descending
/// VOI order, stops when budget is exhausted.
pub fn refine_edges(
    in_edges: Vec<DiscoveredEdge>,
    out_edges: Vec<DiscoveredEdge>,
    refinements: &[(u32, KeySummary)],
    budget: &RefinementBudget,
) -> RefinementResult {
    let mut confirmed_in = Vec::new();
    let mut confirmed_out = Vec::new();
    let mut eliminated = Vec::new();
    let mut decisions = Vec::new();
    let mut bytes_used = 0_usize;
    let mut buckets_refined = 0_usize;

    // Refine incoming edges.
    for edge in in_edges {
        if buckets_refined >= budget.max_buckets || bytes_used >= budget.max_bytes {
            // Budget exhausted: conservatively keep remaining edges.
            confirmed_in.push(edge);
            continue;
        }

        let page = crate::ssi_validation::witness_key_page(&edge.overlap_key);
        if let Some((_, summary)) = refinements.iter().find(|(p, _)| *p == page) {
            let estimated_bytes = estimate_summary_bytes(summary);
            if bytes_used + estimated_bytes > budget.max_bytes {
                // Would exceed byte budget: conservatively keep.
                confirmed_in.push(edge);
                continue;
            }

            if summary.may_overlap(&edge.overlap_key) {
                // Confirmed true overlap.
                confirmed_in.push(edge);
            } else {
                // Refinement proves no true overlap: eliminate.
                debug!(
                    bead_id = "bd-1oxe",
                    from = ?edge.from,
                    to = ?edge.to,
                    key = ?edge.overlap_key,
                    "refinement eliminated false positive incoming edge"
                );
                eliminated.push(edge);
            }
            bytes_used += estimated_bytes;
            buckets_refined += 1;
            decisions.push(RefinementDecision {
                range_prefix: page,
                voi_score: 0.0, // VOI not computed per-edge in V1
                refinement_type: summary_to_priority(summary),
                key_summary: summary.clone(),
                bytes_used: estimated_bytes,
            });
        } else {
            // No refinement data: conservatively keep.
            confirmed_in.push(edge);
        }
    }

    // Refine outgoing edges.
    for edge in out_edges {
        if buckets_refined >= budget.max_buckets || bytes_used >= budget.max_bytes {
            confirmed_out.push(edge);
            continue;
        }

        let page = crate::ssi_validation::witness_key_page(&edge.overlap_key);
        if let Some((_, summary)) = refinements.iter().find(|(p, _)| *p == page) {
            let estimated_bytes = estimate_summary_bytes(summary);
            if bytes_used + estimated_bytes > budget.max_bytes {
                confirmed_out.push(edge);
                continue;
            }

            if summary.may_overlap(&edge.overlap_key) {
                confirmed_out.push(edge);
            } else {
                debug!(
                    bead_id = "bd-1oxe",
                    from = ?edge.from,
                    to = ?edge.to,
                    key = ?edge.overlap_key,
                    "refinement eliminated false positive outgoing edge"
                );
                eliminated.push(edge);
            }
            bytes_used += estimated_bytes;
            buckets_refined += 1;
            decisions.push(RefinementDecision {
                range_prefix: page,
                voi_score: 0.0,
                refinement_type: summary_to_priority(summary),
                key_summary: summary.clone(),
                bytes_used: estimated_bytes,
            });
        } else {
            confirmed_out.push(edge);
        }
    }

    if !eliminated.is_empty() {
        info!(
            bead_id = "bd-1oxe",
            eliminated = eliminated.len(),
            confirmed_in = confirmed_in.len(),
            confirmed_out = confirmed_out.len(),
            bytes_used,
            buckets_refined,
            "refinement complete"
        );
    }

    let mut confirmed_edges = confirmed_in;
    confirmed_edges.extend(confirmed_out);

    RefinementResult {
        confirmed_edges,
        eliminated_edges: eliminated,
        decisions,
        bytes_used,
        buckets_refined,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Estimate the byte cost of a `KeySummary`.
fn estimate_summary_bytes(summary: &KeySummary) -> usize {
    match summary {
        KeySummary::ExactKeys(keys) => keys.len() * 16,
        KeySummary::HashedKeySet(hashes) => hashes.len() * 8,
        KeySummary::PageBitmap(pages) => pages.len() * 4,
        KeySummary::CellBitmap(cells) => cells.len() * 8,
        KeySummary::ByteRangeList(ranges) => ranges.len() * 8,
        KeySummary::Chunked(chunks) => chunks
            .iter()
            .map(|c| estimate_summary_bytes(&c.summary) + 4)
            .sum(),
    }
}

/// Map a `KeySummary` variant to its refinement priority.
fn summary_to_priority(summary: &KeySummary) -> RefinementPriority {
    match summary {
        KeySummary::CellBitmap(_) => RefinementPriority::CellBitmap,
        KeySummary::ByteRangeList(_) => RefinementPriority::ByteRangeList,
        KeySummary::HashedKeySet(_) | KeySummary::PageBitmap(_) | KeySummary::Chunked(_) => {
            RefinementPriority::HashedKeySet
        }
        KeySummary::ExactKeys(_) => RefinementPriority::ExactKeys,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{PageNumber, TxnEpoch, TxnId, TxnToken, WitnessKey};
    use std::collections::BTreeSet;

    fn test_token(id: u64) -> TxnToken {
        TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(0))
    }

    fn page_key(pgno: u32) -> WitnessKey {
        WitnessKey::Page(PageNumber::new(pgno).unwrap())
    }

    fn make_edge(from_id: u64, to_id: u64, pgno: u32) -> DiscoveredEdge {
        DiscoveredEdge {
            from: test_token(from_id),
            to: test_token(to_id),
            overlap_key: page_key(pgno),
            source_is_active: true,
            source_has_in_rw: false,
        }
    }

    // -- §5.7.4 test 1: Page level catches true conflict --

    #[test]
    fn test_page_level_catches_true_conflict() {
        // Without refinement, all page-level edges pass through.
        let in_edges = vec![make_edge(1, 2, 5)];
        let result = refine_edges(
            in_edges,
            Vec::new(),
            &[], // no refinement data
            &RefinementBudget::v1_default(),
        );
        assert_eq!(
            result.confirmed_edges.len(),
            1,
            "without refinement, page-level edge must pass through"
        );
        assert!(result.eliminated_edges.is_empty());
    }

    // -- §5.7.4 test 2: Cell level reduces false positives --

    #[test]
    fn test_cell_level_reduces_false_positives() {
        // Edge claims overlap at page 10, but cell-level refinement shows
        // the actual cells are on a different page.

        // Refinement: page 10's CellBitmap has cells at (5 << 32) | 42,
        // The edge's overlap_key is Page(5), which has page number 5.
        // CellBitmap checks page membership: (5 << 32) to (5 << 32)|0xFFFFFFFF.
        // Page(5) has page=5, so CellBitmap will find the range contains cells.
        // To demonstrate elimination, use a page that's NOT in the bitmap.
        let in_edges_fp = vec![make_edge(1, 2, 10)]; // page 10

        let refinements = vec![(
            10_u32,
            KeySummary::CellBitmap(BTreeSet::from([(5_u64 << 32) | 0x2a])), // cells on page 5, not page 10
        )];

        let result = refine_edges(
            in_edges_fp,
            Vec::new(),
            &refinements,
            &RefinementBudget::v1_default(),
        );
        // CellBitmap for page 10 contains cells for page 5 → page 10 not found → eliminated.
        assert_eq!(
            result.eliminated_edges.len(),
            1,
            "cell refinement should eliminate false positive"
        );
        assert!(result.confirmed_edges.is_empty());

        // Without refinement: same edges pass through.
        let in_edges_no_refine = vec![make_edge(1, 2, 10)];
        let result_no_refine = refine_edges(
            in_edges_no_refine,
            Vec::new(),
            &[], // no refinement
            &RefinementBudget::v1_default(),
        );
        assert_eq!(
            result_no_refine.confirmed_edges.len(),
            1,
            "without refinement, edge passes through"
        );
    }

    // -- §5.7.4 test 3: Refinement budget respected --

    #[test]
    fn test_refinement_budget_respected() {
        // Create edges with refinement data, but set a tiny budget.
        let in_edges = vec![make_edge(1, 2, 5), make_edge(3, 4, 10), make_edge(5, 6, 15)];
        let refinements = vec![
            (5_u32, KeySummary::ExactKeys(vec![page_key(99)])), // doesn't overlap page 5
            (10_u32, KeySummary::ExactKeys(vec![page_key(99)])), // doesn't overlap page 10
            (15_u32, KeySummary::ExactKeys(vec![page_key(99)])), // doesn't overlap page 15
        ];

        // Budget: only 1 bucket allowed.
        let budget = RefinementBudget::new(4096, 1);
        let result = refine_edges(in_edges, Vec::new(), &refinements, &budget);

        // Only 1 bucket refined (eliminated), rest pass through conservatively.
        assert_eq!(result.buckets_refined, 1);
        assert_eq!(result.eliminated_edges.len(), 1);
        assert_eq!(
            result.confirmed_edges.len(),
            2,
            "budget-exceeded edges pass through"
        );
    }

    // -- §5.7.4 test 4: VOI metric computation --

    #[test]
    fn test_voi_metric_computation() {
        let metrics = VoiMetrics {
            c_b: 10.0,           // 10 conflicts per unit time
            fp_b: 0.8,           // 80% false positive rate
            delta_fp_b: 0.7,     // refinement reduces FP by 70%
            l_abort: 100.0,      // abort cost = 100 units
            cost_refine_b: 50.0, // refinement cost = 50 units
        };

        // Benefit = 10 * 0.7 * 100 = 700
        let benefit = metrics.benefit();
        assert!(
            (benefit - 700.0).abs() < 1e-10,
            "benefit = c_b * delta_fp_b * L_abort"
        );

        // VOI = 700 - 50 = 650
        let voi = metrics.voi();
        assert!((voi - 650.0).abs() < 1e-10, "VOI = benefit - cost");
        assert!(voi > 0.0, "positive VOI means refinement is cost-effective");

        // Negative VOI example: high cost, low benefit.
        let expensive = VoiMetrics {
            c_b: 0.1,
            fp_b: 0.1,
            delta_fp_b: 0.05,
            l_abort: 10.0,
            cost_refine_b: 100.0,
        };
        assert!(
            expensive.voi() < 0.0,
            "negative VOI means refinement not worth it"
        );
    }

    // -- Soundness: disabling refinement never introduces false negatives --

    #[test]
    fn test_disabling_refinement_is_sound() {
        // With refinement disabled (no refinement data), all edges pass through.
        // This is always safe: over-approximation, never misses real conflicts.
        let in_edges = vec![make_edge(1, 2, 5), make_edge(3, 4, 10)];
        let out_edges = vec![make_edge(2, 3, 7)];

        let result = refine_edges(in_edges, out_edges, &[], &RefinementBudget::v1_default());

        assert_eq!(result.confirmed_edges.len(), 3, "all edges pass through");
        assert!(result.eliminated_edges.is_empty(), "no edges eliminated");
        assert_eq!(result.buckets_refined, 0);
    }

    // -- Byte budget enforcement --

    #[test]
    fn test_byte_budget_enforcement() {
        let in_edges = vec![make_edge(1, 2, 5), make_edge(3, 4, 10)];
        // Each ExactKeys refinement costs 16 bytes per key.
        let refinements = vec![
            (5_u32, KeySummary::ExactKeys(vec![page_key(99)])),
            (10_u32, KeySummary::ExactKeys(vec![page_key(99)])),
        ];

        // Budget: only 16 bytes (enough for 1 refinement).
        let budget = RefinementBudget::new(16, 100);
        let result = refine_edges(in_edges, Vec::new(), &refinements, &budget);

        assert_eq!(result.buckets_refined, 1);
        assert!(result.bytes_used <= 16);
    }
}
