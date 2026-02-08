//! §3.5.3 Deterministic Repair Symbol Generation.
//!
//! Given an ECS object and a repair symbol count R, the set of repair symbols
//! is deterministic: same object + same R = identical repair symbols. This
//! enables verification without original, incremental repair, idempotent writes,
//! and appendable redundancy.
//!
//! ## Repair Symbol Budget
//!
//! ```text
//! slack_decode = 2  // V1 default: target K_source+2 decode slack (RFC 6330 Annex B)
//! R = max(slack_decode, ceil(K_source * overhead_percent / 100))
//! ```
//!
//! ## Seed Derivation
//!
//! ```text
//! seed = xxh3_64(object_id_bytes)
//! ```
//!
//! This makes "the object" a platonic mathematical entity: any replica can
//! regenerate missing repair symbols (within policy) without coordination.

use fsqlite_types::ObjectId;
use tracing::{info, warn};
use xxhash_rust::xxh3::xxh3_64;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// V1 decode slack: target K_source + 2 for negligible decode failure (RFC 6330 Annex B).
pub const DEFAULT_SLACK_DECODE: u32 = 2;

/// Default overhead percentage.
pub const DEFAULT_OVERHEAD_PERCENT: u32 = 20;

// ---------------------------------------------------------------------------
// Repair Config
// ---------------------------------------------------------------------------

/// Configuration for deterministic repair symbol generation (§3.5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairConfig {
    /// Additive decode slack: extra symbols beyond K_source for negligible
    /// decode failure probability.
    pub slack_decode: u32,
    /// Multiplicative overhead percentage: `PRAGMA raptorq_overhead = <percent>`.
    pub overhead_percent: u32,
}

impl RepairConfig {
    /// Create a repair config with default values.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slack_decode: DEFAULT_SLACK_DECODE,
            overhead_percent: DEFAULT_OVERHEAD_PERCENT,
        }
    }

    /// Create with a specific overhead percentage.
    #[must_use]
    pub const fn with_overhead(overhead_percent: u32) -> Self {
        Self {
            slack_decode: DEFAULT_SLACK_DECODE,
            overhead_percent,
        }
    }
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Repair Budget
// ---------------------------------------------------------------------------

/// Computed repair symbol budget for a given K_source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairBudget {
    /// Number of source symbols.
    pub k_source: u32,
    /// Computed number of repair symbols.
    pub repair_count: u32,
    /// Maximum tolerated erasure fraction (without coordination).
    pub loss_fraction_max_permille: u32,
    /// Whether this budget has zero erasure tolerance (small-K warning).
    pub underprovisioned: bool,
}

/// Compute the repair symbol count R for a given K_source and config (§3.5.3).
///
/// Formula: `R = max(slack_decode, ceil(K_source * overhead_percent / 100))`
///
/// Returns a `RepairBudget` with the computed R and derived metrics.
#[must_use]
pub fn compute_repair_budget(k_source: u32, config: &RepairConfig) -> RepairBudget {
    // R = max(slack_decode, ceil(K_source * overhead_percent / 100))
    let overhead_r = (u64::from(k_source) * u64::from(config.overhead_percent))
        .div_ceil(100);
    #[allow(clippy::cast_possible_truncation)]
    let overhead_r = overhead_r as u32;
    let repair_count = config.slack_decode.max(overhead_r);

    // loss_fraction_max = max(0, (R - slack_decode) / (K_source + R))
    // Expressed as permille (parts per thousand) for integer precision.
    let loss_fraction_max_permille = if repair_count > config.slack_decode {
        let numerator = u64::from(repair_count - config.slack_decode) * 1000;
        let denominator = u64::from(k_source) + u64::from(repair_count);
        #[allow(clippy::cast_possible_truncation)]
        let result = (numerator / denominator) as u32;
        result
    } else {
        0
    };

    let underprovisioned = loss_fraction_max_permille == 0 && k_source > 0;

    if underprovisioned {
        warn!(
            k_source,
            repair_count,
            overhead_percent = config.overhead_percent,
            "small-K underprovisioning: loss_fraction_max = 0, no erasure tolerance beyond decode slack"
        );
    }

    RepairBudget {
        k_source,
        repair_count,
        loss_fraction_max_permille,
        underprovisioned,
    }
}

// ---------------------------------------------------------------------------
// Seed Derivation
// ---------------------------------------------------------------------------

/// Derive a deterministic seed from an `ObjectId` (§3.5.3, §3.5.9).
///
/// `seed = xxh3_64(object_id_bytes)`
///
/// This seed is wired through `RaptorQConfig` or sender construction to
/// ensure repair symbol generation is deterministic for a given ObjectId.
#[must_use]
pub fn derive_repair_seed(object_id: &ObjectId) -> u64 {
    xxh3_64(object_id.as_bytes())
}

// ---------------------------------------------------------------------------
// Repair Symbol ESI Range
// ---------------------------------------------------------------------------

/// Compute the Encoding Symbol Identifier (ESI) range for repair symbols.
///
/// Repair symbols have ESIs in `[K_source, K_source + R)`.
#[must_use]
pub fn repair_esi_range(k_source: u32, repair_count: u32) -> std::ops::Range<u32> {
    k_source..k_source + repair_count
}

// ---------------------------------------------------------------------------
// Adaptive Overhead Evidence Ledger
// ---------------------------------------------------------------------------

/// Evidence ledger entry emitted on every adaptive overhead retune (§3.5.3).
#[derive(Debug, Clone, PartialEq)]
pub struct OverheadRetuneEntry {
    /// Previous overhead percentage.
    pub old_overhead_percent: u32,
    /// New overhead percentage.
    pub new_overhead_percent: u32,
    /// Observed e-value trajectory (most recent value).
    pub e_value: f64,
    /// Old loss fraction max (permille).
    pub old_loss_fraction_max_permille: u32,
    /// New loss fraction max (permille).
    pub new_loss_fraction_max_permille: u32,
    /// K_source at the time of retune.
    pub k_source: u32,
}

/// Record an adaptive overhead retune in the evidence ledger (§3.5.3).
///
/// Returns the ledger entry for persistence.
#[must_use]
pub fn record_overhead_retune(
    k_source: u32,
    old_config: &RepairConfig,
    new_overhead_percent: u32,
    e_value: f64,
) -> OverheadRetuneEntry {
    let old_budget = compute_repair_budget(k_source, old_config);
    let new_config = RepairConfig::with_overhead(new_overhead_percent);
    let new_budget = compute_repair_budget(k_source, &new_config);

    let entry = OverheadRetuneEntry {
        old_overhead_percent: old_config.overhead_percent,
        new_overhead_percent,
        e_value,
        old_loss_fraction_max_permille: old_budget.loss_fraction_max_permille,
        new_loss_fraction_max_permille: new_budget.loss_fraction_max_permille,
        k_source,
    };

    info!(
        old_overhead = old_config.overhead_percent,
        new_overhead = new_overhead_percent,
        e_value,
        old_loss_fraction_max_permille = old_budget.loss_fraction_max_permille,
        new_loss_fraction_max_permille = new_budget.loss_fraction_max_permille,
        k_source,
        "adaptive overhead retune — evidence ledger entry"
    );

    entry
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- bd-1hi.22 test 1: Repair symbol count formula --

    #[test]
    fn test_repair_symbol_count_formula() {
        let config = RepairConfig::new(); // 20% overhead, slack=2.

        // K=100, 20% → R = max(2, ceil(20)) = 20.
        let b = compute_repair_budget(100, &config);
        assert_eq!(b.repair_count, 20);

        // K=3, 20% → R = max(2, ceil(0.6)) = max(2, 1) = 2.
        let b = compute_repair_budget(3, &config);
        assert_eq!(b.repair_count, 2);

        // K=1, 20% → R = max(2, ceil(0.2)) = max(2, 1) = 2.
        let b = compute_repair_budget(1, &config);
        assert_eq!(b.repair_count, 2);

        // K=56403, 20% → R = max(2, ceil(11280.6)) = max(2, 11281) = 11281.
        let b = compute_repair_budget(56403, &config);
        assert_eq!(b.repair_count, 11281);
    }

    // -- bd-1hi.22 test 2: Same object → same seed (deterministic) --

    #[test]
    fn test_repair_deterministic_same_object() {
        let oid = ObjectId::derive_from_canonical_bytes(b"test_object_payload_1");
        let seed1 = derive_repair_seed(&oid);
        let seed2 = derive_repair_seed(&oid);
        assert_eq!(seed1, seed2, "same ObjectId must produce same seed");
    }

    // -- bd-1hi.22 test 3: Different object → different seed --

    #[test]
    fn test_repair_deterministic_different_object() {
        let oid1 = ObjectId::derive_from_canonical_bytes(b"object_A");
        let oid2 = ObjectId::derive_from_canonical_bytes(b"object_B");
        let seed1 = derive_repair_seed(&oid1);
        let seed2 = derive_repair_seed(&oid2);
        assert_ne!(
            seed1, seed2,
            "different ObjectIds must produce different seeds"
        );
    }

    // -- bd-1hi.22 test 4: Seed derivation is xxh3_64(object_id_bytes) --

    #[test]
    fn test_repair_seed_derivation() {
        let oid = ObjectId::derive_from_canonical_bytes(b"seed_test_payload");
        let expected_seed = xxh3_64(oid.as_bytes());
        let actual_seed = derive_repair_seed(&oid);
        assert_eq!(
            actual_seed, expected_seed,
            "seed must be xxh3_64(object_id_bytes)"
        );
    }

    // -- bd-1hi.22 test 5: Loss fraction max computation --

    #[test]
    fn test_loss_fraction_max_computation() {
        let config = RepairConfig::new();

        // K=100, R=20: loss_fraction_max = (20-2)/(100+20) = 18/120 = 0.15 = 150‰.
        let b = compute_repair_budget(100, &config);
        assert_eq!(b.loss_fraction_max_permille, 150);

        // K=3, R=2: loss_fraction_max = max(0, (2-2)/(3+2)) = 0.
        let b = compute_repair_budget(3, &config);
        assert_eq!(b.loss_fraction_max_permille, 0);
    }

    // -- bd-1hi.22 test 6: Small-K underprovisioning warning --

    #[test]
    fn test_small_k_underprovisioning_warning() {
        let config = RepairConfig::new();

        // K=3 with 20% overhead → R=2, loss_fraction_max=0, underprovisioned.
        let b = compute_repair_budget(3, &config);
        assert!(
            b.underprovisioned,
            "K=3 must be flagged as underprovisioned"
        );
        assert_eq!(b.loss_fraction_max_permille, 0);

        // K=100 → R=20, loss_fraction_max=150‰, NOT underprovisioned.
        let b = compute_repair_budget(100, &config);
        assert!(!b.underprovisioned, "K=100 should not be underprovisioned");
    }

    // -- bd-1hi.22 test 7: Repair symbol ESI range --

    #[test]
    fn test_repair_symbol_esi_range() {
        let range = repair_esi_range(100, 20);
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 120);
        assert_eq!(range.len(), 20);

        // All ESIs are unique (by definition of a range).
        let esis: Vec<u32> = range.collect();
        assert_eq!(esis.len(), 20);
        for (i, &esi) in esis.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let expected = 100 + i as u32;
            assert_eq!(esi, expected);
        }
    }

    // -- bd-1hi.22 test 8: Repair symbols decode compatible --
    //
    // NOTE: Full encode/decode test requires asupersync which is a dev-dependency.
    // This test verifies the budget computation allows sufficient decode slack.

    #[test]
    fn test_repair_symbols_decode_compatible() {
        let config = RepairConfig::new();

        // K=100, R=20. If we drop 2 source symbols, we need at least K+slack=102
        // symbols to decode. We have K-2 source + R=20 repair = 118 symbols available.
        // 118 >= 102, so decode should succeed.
        let b = compute_repair_budget(100, &config);
        let available_after_loss = (b.k_source - 2) + b.repair_count;
        let needed = b.k_source + config.slack_decode;
        assert!(
            available_after_loss >= needed,
            "must have enough symbols to decode after losing 2 source symbols: available={available_after_loss}, needed={needed}"
        );
    }

    // -- bd-1hi.22 test 9: PRAGMA raptorq_overhead --

    #[test]
    fn test_pragma_raptorq_overhead() {
        // 50% overhead.
        let config = RepairConfig::with_overhead(50);
        let b = compute_repair_budget(100, &config);
        assert_eq!(b.repair_count, 50, "K=100 with 50% → R=max(2,50)=50");

        // 10% overhead.
        let config = RepairConfig::with_overhead(10);
        let b = compute_repair_budget(100, &config);
        assert_eq!(b.repair_count, 10, "K=100 with 10% → R=max(2,10)=10");

        // 1% overhead for large K.
        let config = RepairConfig::with_overhead(1);
        let b = compute_repair_budget(1000, &config);
        assert_eq!(b.repair_count, 10, "K=1000 with 1% → R=max(2,10)=10");
    }

    // -- bd-1hi.22 test 10: Adaptive overhead evidence ledger --

    #[test]
    fn test_adaptive_overhead_evidence_ledger() {
        let old_config = RepairConfig::with_overhead(20);
        let entry = record_overhead_retune(100, &old_config, 40, 0.85);

        assert_eq!(entry.old_overhead_percent, 20);
        assert_eq!(entry.new_overhead_percent, 40);
        assert!((entry.e_value - 0.85).abs() < f64::EPSILON);
        assert_eq!(entry.old_loss_fraction_max_permille, 150); // (20-2)/(100+20)*1000
        assert_eq!(entry.k_source, 100);

        // New budget: K=100, 40% → R=40, loss_fraction_max = (40-2)/(100+40)*1000 = 38000/140 = 271.
        assert_eq!(entry.new_loss_fraction_max_permille, 271);
    }

    // -- bd-1hi.22 test 11: prop_repair_deterministic --

    #[test]
    fn prop_repair_deterministic() {
        // For multiple payloads, seed derivation is deterministic.
        for i in 0..100_u64 {
            let payload = i.to_le_bytes();
            let oid = ObjectId::derive_from_canonical_bytes(&payload);
            let seed_a = derive_repair_seed(&oid);
            let seed_b = derive_repair_seed(&oid);
            assert_eq!(
                seed_a, seed_b,
                "seed must be deterministic for payload {i}"
            );
        }
    }

    // -- bd-1hi.22 test 12: prop_loss_fraction_monotonic --

    #[test]
    fn prop_loss_fraction_monotonic() {
        let config = RepairConfig::new();

        // Increasing K_source should generally increase or maintain loss_fraction_max
        // (once past the small-K threshold).
        let mut prev_loss = 0u32;
        for k in [10, 20, 50, 100, 500, 1000, 5000] {
            let b = compute_repair_budget(k, &config);
            assert!(
                b.loss_fraction_max_permille >= prev_loss || k <= 10,
                "loss fraction should be monotonically non-decreasing for K={k}: {} < {}",
                b.loss_fraction_max_permille,
                prev_loss
            );
            prev_loss = b.loss_fraction_max_permille;
        }

        // Increasing R (via overhead) always increases loss_fraction_max.
        for overhead in [10, 20, 30, 50, 100] {
            let config_low = RepairConfig::with_overhead(overhead);
            let config_high = RepairConfig::with_overhead(overhead + 10);
            let b_low = compute_repair_budget(100, &config_low);
            let b_high = compute_repair_budget(100, &config_high);
            assert!(
                b_high.loss_fraction_max_permille >= b_low.loss_fraction_max_permille,
                "increasing overhead must increase loss_fraction_max: {}% -> {}%, {} vs {}",
                overhead,
                overhead + 10,
                b_low.loss_fraction_max_permille,
                b_high.loss_fraction_max_permille
            );
        }
    }
}
