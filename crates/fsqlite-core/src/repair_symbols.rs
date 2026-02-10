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
//! R_formula = max(slack_decode, ceil(K_source * overhead_percent / 100))
//! R = min(max_repair_symbols, max(R_formula, small_k_min_repair when K_source <= small_k_clamp_max_k))
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
//!
//! ## Guardrails
//!
//! - Union-only hardening: increasing redundancy is always append-safe.
//! - Decreases are never automatic here; any decrease must be justified by
//!   evidence-ledger policy outside this selector.
//! - Runtime budget policy changes are epoch-boundary only (`policy_epoch` monotone).
//! - Budget selection is arithmetic-only and intentionally avoids symbol encoding,
//!   so it is safe to run on critical paths.

use std::collections::HashMap;
use std::fmt;

use fsqlite_types::ObjectId;
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

#[path = "policy_controller.rs"]
pub mod policy_controller;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// V1 decode slack: target K_source + 2 for negligible decode failure (RFC 6330 Annex B).
pub const DEFAULT_SLACK_DECODE: u32 = 2;

/// Default overhead percentage.
pub const DEFAULT_OVERHEAD_PERCENT: u32 = 20;

/// Hard floor for user-provided overhead percentages.
pub const MIN_OVERHEAD_PERCENT: u32 = 1;

/// Hard ceiling for user-provided overhead percentages.
pub const MAX_OVERHEAD_PERCENT: u32 = 500;

/// For tiny objects, enforce a stronger minimum repair budget.
pub const DEFAULT_SMALL_K_CLAMP_MAX_K: u32 = 8;

/// For tiny objects (`K <= DEFAULT_SMALL_K_CLAMP_MAX_K`), enforce at least this many repair symbols.
pub const DEFAULT_SMALL_K_MIN_REPAIR: u32 = 3;

/// Absolute cap on generated repair symbols (explicit anti-footgun guardrail).
pub const DEFAULT_MAX_REPAIR_SYMBOLS: u32 = 250_000;

/// Versioned policy identifier for repair-budget selection.
pub const REPAIR_BUDGET_POLICY_ID: &str = "rq_budget_v1";

/// Initial policy epoch. Runtime retunes MUST apply only at epoch boundaries.
pub const INITIAL_REPAIR_POLICY_EPOCH: u64 = 0;

/// E-value threshold at which failure drift alerts fire.
pub const DEFAULT_FAILURE_ALERT_THRESHOLD: f64 = 20.0;

/// Wilson interval z-score used for conservative upper-bound monitoring.
pub const DEFAULT_WILSON_Z: f64 = 3.0;

/// Bead identifier for adaptive redundancy autopilot scope.
pub const ADAPTIVE_REDUNDANCY_BEAD_ID: &str = "bd-1hi.30";

/// Structured logging standard reference for this bead.
pub const ADAPTIVE_REDUNDANCY_LOGGING_STANDARD: &str = "bd-1fpm";

/// Default debug throttling interval for monitor updates.
pub const DEFAULT_DEBUG_EVERY_ATTEMPTS: u64 = 64;

/// Minimum sample count before WARN drift diagnostics are emitted.
pub const MIN_ATTEMPTS_FOR_WARN: u64 = 64;

/// Minimum sample count before INFO alert decisions are emitted.
pub const MIN_ATTEMPTS_FOR_ALERT: u64 = 128;

/// Trigger source for a redundancy policy decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedundancyTrigger {
    /// Automatic escalation after anytime-valid bound rejection.
    EprocessReject,
    /// Conservative auto-decrease after long stable operation.
    EprocessSafeDecrease,
    /// Explicit operator/diagnostic retune.
    Manual,
}

impl fmt::Display for RedundancyTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EprocessReject => write!(f, "eprocess_reject"),
            Self::EprocessSafeDecrease => write!(f, "eprocess_safe_decrease"),
            Self::Manual => write!(f, "manual"),
        }
    }
}

/// Object-class specific repair budget defaults.
///
/// Commit markers/proofs are durability-critical and therefore stricter than
/// page-history objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepairObjectClass {
    /// Commit marker object.
    CommitMarker,
    /// Commit proof object.
    CommitProof,
    /// Historical page/object payload.
    PageHistory,
    /// Snapshot data block.
    SnapshotBlock,
    /// WAL FEC group symbols.
    WalFecGroup,
    /// Generic ECS object.
    GenericEcs,
}

/// Policy knobs for one repair object class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectRepairPolicy {
    /// Versioned policy id used for explainability and replay.
    pub policy_id: &'static str,
    /// Object class.
    pub object_class: RepairObjectClass,
    /// Default overhead percentage for this class.
    pub default_overhead_percent: u32,
    /// Small-K threshold for clamp activation.
    pub small_k_clamp_max_k: u32,
    /// Minimum repair symbols when small-K clamp is active.
    pub small_k_min_repair: u32,
    /// Absolute maximum repair symbols for this class.
    pub max_repair_symbols: u32,
}

impl ObjectRepairPolicy {
    /// Return the default policy row for an object class.
    #[must_use]
    pub const fn for_class(object_class: RepairObjectClass) -> Self {
        match object_class {
            RepairObjectClass::CommitMarker => Self {
                policy_id: REPAIR_BUDGET_POLICY_ID,
                object_class,
                default_overhead_percent: 60,
                small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
                small_k_min_repair: 4,
                max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            },
            RepairObjectClass::CommitProof => Self {
                policy_id: REPAIR_BUDGET_POLICY_ID,
                object_class,
                default_overhead_percent: 50,
                small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
                small_k_min_repair: 4,
                max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            },
            RepairObjectClass::PageHistory => Self {
                policy_id: REPAIR_BUDGET_POLICY_ID,
                object_class,
                default_overhead_percent: 20,
                small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
                small_k_min_repair: DEFAULT_SMALL_K_MIN_REPAIR,
                max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            },
            RepairObjectClass::SnapshotBlock => Self {
                policy_id: REPAIR_BUDGET_POLICY_ID,
                object_class,
                default_overhead_percent: 25,
                small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
                small_k_min_repair: DEFAULT_SMALL_K_MIN_REPAIR,
                max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            },
            RepairObjectClass::WalFecGroup => Self {
                policy_id: REPAIR_BUDGET_POLICY_ID,
                object_class,
                default_overhead_percent: 30,
                small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
                small_k_min_repair: DEFAULT_SMALL_K_MIN_REPAIR,
                max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            },
            RepairObjectClass::GenericEcs => Self {
                policy_id: REPAIR_BUDGET_POLICY_ID,
                object_class,
                default_overhead_percent: DEFAULT_OVERHEAD_PERCENT,
                small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
                small_k_min_repair: DEFAULT_SMALL_K_MIN_REPAIR,
                max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            },
        }
    }
}

/// Budget policy updates are only allowed at epoch boundaries.
#[must_use]
pub const fn can_apply_policy_change(current_epoch: u64, requested_epoch: u64) -> bool {
    requested_epoch > current_epoch
}

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
    /// Small-K clamp activation threshold.
    pub small_k_clamp_max_k: u32,
    /// Small-K clamp minimum R.
    pub small_k_min_repair: u32,
    /// Explicit upper bound for R.
    pub max_repair_symbols: u32,
    /// Versioned policy id for explainability/proofs.
    pub policy_id: &'static str,
    /// Monotone policy epoch. Runtime changes are epoch-boundary only.
    pub policy_epoch: u64,
}

impl RepairConfig {
    /// Create a repair config with default values.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slack_decode: DEFAULT_SLACK_DECODE,
            overhead_percent: DEFAULT_OVERHEAD_PERCENT,
            small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
            small_k_min_repair: DEFAULT_SMALL_K_MIN_REPAIR,
            max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            policy_id: REPAIR_BUDGET_POLICY_ID,
            policy_epoch: INITIAL_REPAIR_POLICY_EPOCH,
        }
    }

    /// Create with a specific overhead percentage.
    #[must_use]
    pub const fn with_overhead(overhead_percent: u32) -> Self {
        Self {
            slack_decode: DEFAULT_SLACK_DECODE,
            overhead_percent,
            small_k_clamp_max_k: DEFAULT_SMALL_K_CLAMP_MAX_K,
            small_k_min_repair: DEFAULT_SMALL_K_MIN_REPAIR,
            max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            policy_id: REPAIR_BUDGET_POLICY_ID,
            policy_epoch: INITIAL_REPAIR_POLICY_EPOCH,
        }
    }

    /// Build a class-specific config row from the default policy table.
    #[must_use]
    pub const fn for_object_class(object_class: RepairObjectClass, policy_epoch: u64) -> Self {
        let policy = ObjectRepairPolicy::for_class(object_class);
        Self {
            slack_decode: DEFAULT_SLACK_DECODE,
            overhead_percent: policy.default_overhead_percent,
            small_k_clamp_max_k: policy.small_k_clamp_max_k,
            small_k_min_repair: policy.small_k_min_repair,
            max_repair_symbols: policy.max_repair_symbols,
            policy_id: policy.policy_id,
            policy_epoch,
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
    /// Versioned policy id used for this decision.
    pub policy_id: &'static str,
    /// Policy epoch used for this decision.
    pub policy_epoch: u64,
    /// Overhead as requested by caller/PRAGMA.
    pub overhead_percent_requested: u32,
    /// Overhead after policy clamping.
    pub overhead_percent_applied: u32,
    /// Whether small-K clamp increased `repair_count`.
    pub small_k_clamped: bool,
    /// Whether max-R guardrail capped `repair_count`.
    pub max_repair_capped: bool,
    /// Maximum tolerated erasure fraction (without coordination).
    pub loss_fraction_max_permille: u32,
    /// Whether this budget has zero erasure tolerance (small-K warning).
    pub underprovisioned: bool,
}

/// Explicit policy surface for deterministic repair-count selection.
///
/// This function performs only arithmetic and guardrails; it does not encode
/// any symbols and is safe on critical paths.
#[must_use]
pub fn select_repair_count(k_source: u32, overhead_percent: u32) -> u32 {
    select_repair_count_with_config(k_source, &RepairConfig::with_overhead(overhead_percent))
}

/// Compute a budget using per-object policy defaults plus an optional overhead override.
///
/// Runtime policy updates MUST use epoch-boundary increments.
#[must_use]
pub fn compute_repair_budget_for_object(
    k_source: u32,
    object_class: RepairObjectClass,
    overhead_percent_override: Option<u32>,
    policy_epoch: u64,
) -> RepairBudget {
    let mut config = RepairConfig::for_object_class(object_class, policy_epoch);
    if let Some(overhead_percent) = overhead_percent_override {
        config.overhead_percent = overhead_percent;
    }
    compute_repair_budget(k_source, &config)
}

#[must_use]
fn select_repair_count_with_config(k_source: u32, config: &RepairConfig) -> u32 {
    let bounded_overhead_percent = config
        .overhead_percent
        .clamp(MIN_OVERHEAD_PERCENT, MAX_OVERHEAD_PERCENT);
    let overhead_r = (u64::from(k_source) * u64::from(bounded_overhead_percent)).div_ceil(100);
    #[allow(clippy::cast_possible_truncation)]
    let overhead_r = overhead_r as u32;
    let formula_r = config.slack_decode.max(overhead_r);
    let small_k_floor = if k_source > 0 && k_source <= config.small_k_clamp_max_k {
        config.small_k_min_repair.max(config.slack_decode)
    } else {
        config.slack_decode
    };
    let max_r = config.max_repair_symbols.max(config.slack_decode);

    formula_r.max(small_k_floor).min(max_r)
}

/// Compute the repair symbol count R for a given K_source and config (§3.5.3).
///
/// Base formula: `R_formula = max(slack_decode, ceil(K_source * overhead_percent / 100))`
///
/// Final selection applies policy guardrails:
/// - small-K clamp
/// - explicit max-R cap
/// - bounded overhead percentage
///
/// Returns a `RepairBudget` with the computed R and derived metrics.
#[must_use]
pub fn compute_repair_budget(k_source: u32, config: &RepairConfig) -> RepairBudget {
    let overhead_percent_applied = config
        .overhead_percent
        .clamp(MIN_OVERHEAD_PERCENT, MAX_OVERHEAD_PERCENT);
    let overhead_r = (u64::from(k_source) * u64::from(overhead_percent_applied)).div_ceil(100);
    #[allow(clippy::cast_possible_truncation)]
    let overhead_r = overhead_r as u32;
    let formula_r = config.slack_decode.max(overhead_r);
    let repair_count = select_repair_count_with_config(k_source, config);
    let small_k_floor = if k_source > 0 && k_source <= config.small_k_clamp_max_k {
        config.small_k_min_repair.max(config.slack_decode)
    } else {
        config.slack_decode
    };
    let small_k_clamped = repair_count > formula_r && repair_count == small_k_floor;
    let max_repair_capped = repair_count < formula_r.max(small_k_floor);
    let overhead_was_clamped = overhead_percent_applied != config.overhead_percent;

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

    if overhead_was_clamped {
        warn!(
            requested_overhead_percent = config.overhead_percent,
            applied_overhead_percent = overhead_percent_applied,
            min_overhead_percent = MIN_OVERHEAD_PERCENT,
            max_overhead_percent = MAX_OVERHEAD_PERCENT,
            "repair budget overhead clamped to policy bounds"
        );
    }

    if max_repair_capped {
        warn!(
            k_source,
            repair_count,
            requested_formula_r = formula_r.max(small_k_floor),
            max_repair_symbols = config.max_repair_symbols,
            policy_id = config.policy_id,
            policy_epoch = config.policy_epoch,
            "repair budget capped by explicit max-R guardrail"
        );
    }

    if underprovisioned {
        warn!(
            k_source,
            repair_count,
            overhead_percent = overhead_percent_applied,
            policy_id = config.policy_id,
            policy_epoch = config.policy_epoch,
            "small-K underprovisioning: loss_fraction_max = 0, no erasure tolerance beyond decode slack"
        );
    }

    RepairBudget {
        k_source,
        repair_count,
        policy_id: config.policy_id,
        policy_epoch: config.policy_epoch,
        overhead_percent_requested: config.overhead_percent,
        overhead_percent_applied,
        small_k_clamped,
        max_repair_capped,
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
    /// Trigger source for this policy decision.
    pub trigger: RedundancyTrigger,
    /// Monotone regime identifier from the controller.
    pub regime_id: u64,
    /// Anytime-valid upper bound used as hard gate.
    pub p_upper: f64,
}

// ---------------------------------------------------------------------------
// Failure Probability Monitoring (§3.1.1)
// ---------------------------------------------------------------------------

/// Object type for decode-attempt telemetry bucketing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecodeObjectType {
    /// WAL commit-group decode.
    WalCommitGroup,
    /// Snapshot block decode.
    SnapshotBlock,
    /// Generic ECS object decode.
    EcsObject,
}

/// Decode attempt sample used by failure-rate monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeAttempt {
    /// Number of source symbols K.
    pub k_source: u32,
    /// Number of received symbols used for decode.
    pub symbols_received: u32,
    /// Overhead symbols (`symbols_received - k_source`, saturating).
    pub overhead: u32,
    /// Symbol size in bytes.
    pub symbol_size: u32,
    /// `true` if decode succeeded.
    pub success: bool,
    /// Decode duration in microseconds.
    pub decode_time_us: u64,
    /// Decode object class.
    pub object_type: DecodeObjectType,
}

impl DecodeAttempt {
    /// Create a decode-attempt sample.
    #[must_use]
    pub const fn new(
        k_source: u32,
        symbols_received: u32,
        symbol_size: u32,
        success: bool,
        decode_time_us: u64,
        object_type: DecodeObjectType,
    ) -> Self {
        Self {
            k_source,
            symbols_received,
            overhead: symbols_received.saturating_sub(k_source),
            symbol_size,
            success,
            decode_time_us,
            object_type,
        }
    }
}

/// K-buckets used by the monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KRangeBucket {
    /// K in [1, 10].
    K1To10,
    /// K in [11, 100].
    K11To100,
    /// K in [101, 1000].
    K101To1000,
    /// K in [1001, 10000].
    K1001To10000,
    /// K in [10001, 56403].
    K10001To56403,
    /// K outside RFC 6330 V1 block limit.
    KAbove56403,
}

impl KRangeBucket {
    /// Map a `k_source` value to its monitor bucket.
    #[must_use]
    pub const fn from_k(k_source: u32) -> Self {
        match k_source {
            0..=10 => Self::K1To10,
            11..=100 => Self::K11To100,
            101..=1000 => Self::K101To1000,
            1001..=10_000 => Self::K1001To10000,
            10_001..=56_403 => Self::K10001To56403,
            _ => Self::KAbove56403,
        }
    }
}

impl fmt::Display for KRangeBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::K1To10 => write!(f, "[1,10]"),
            Self::K11To100 => write!(f, "[11,100]"),
            Self::K101To1000 => write!(f, "[101,1000]"),
            Self::K1001To10000 => write!(f, "[1001,10000]"),
            Self::K10001To56403 => write!(f, "[10001,56403]"),
            Self::KAbove56403 => write!(f, ">56403"),
        }
    }
}

/// Monitor bucket key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FailureBucketKey {
    /// K-range bucket.
    pub k_range: KRangeBucket,
    /// Overhead bucket: 0, 1, 2, or 3 (= 3+).
    pub overhead_bucket: u32,
}

impl FailureBucketKey {
    /// Build a bucket key from an attempt.
    #[must_use]
    pub const fn from_attempt(attempt: DecodeAttempt) -> Self {
        let overhead_bucket = if attempt.overhead > 3 {
            3
        } else {
            attempt.overhead
        };
        Self {
            k_range: KRangeBucket::from_k(attempt.k_source),
            overhead_bucket,
        }
    }
}

/// E-process state for one `(K-range, overhead)` bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FailureEProcessState {
    /// Running e-value.
    pub e_value: f64,
    /// Observed attempts in this bucket.
    pub total_attempts: u64,
    /// Observed failures in this bucket.
    pub total_failures: u64,
    /// Null bound `P_fail <= null_rate`.
    pub null_rate: f64,
    /// Alert threshold on e-value.
    pub alert_threshold: f64,
    /// Conservative upper bound on observed failure rate.
    pub p_upper: f64,
    /// Whether a WARN has already been emitted.
    pub warned: bool,
    /// Whether an INFO alert has already been emitted.
    pub alerted: bool,
}

impl FailureEProcessState {
    /// Create a fresh e-process state.
    #[must_use]
    pub const fn new(null_rate: f64, alert_threshold: f64) -> Self {
        Self {
            e_value: 1.0,
            total_attempts: 0,
            total_failures: 0,
            null_rate,
            alert_threshold,
            p_upper: 1.0,
            warned: false,
            alerted: false,
        }
    }

    /// Point estimate (for diagnostics only, not alerting decisions).
    #[must_use]
    pub fn observed_rate_point(self) -> f64 {
        if self.total_attempts == 0 {
            0.0
        } else {
            self.total_failures as f64 / self.total_attempts as f64
        }
    }
}

/// Monitor log levels aligned to harness logging standards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorLogLevel {
    /// DEBUG-level diagnostic event.
    Debug,
    /// INFO-level alert event.
    Info,
    /// WARN-level approaching-threshold event.
    Warn,
    /// ERROR-level unrecoverable event.
    Error,
}

/// Structured monitor event emitted by [`FailureRateMonitor::update`].
#[derive(Debug, Clone, PartialEq)]
pub struct MonitorEvent {
    /// Event severity.
    pub level: MonitorLogLevel,
    /// Bucket for this event.
    pub bucket: FailureBucketKey,
    /// Attempts observed in this bucket.
    pub attempts: u64,
    /// Failures observed in this bucket.
    pub failures: u64,
    /// Current e-value for the bucket.
    pub e_value: f64,
    /// Conservative upper bound for failure rate.
    pub p_upper: f64,
    /// Null-rate budget for the bucket.
    pub null_rate: f64,
    /// Static event message.
    pub message: &'static str,
}

/// Result of updating the failure monitor with one attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct MonitorUpdate {
    /// Bucket that was updated.
    pub bucket: FailureBucketKey,
    /// Updated state snapshot.
    pub state: FailureEProcessState,
    /// Emitted monitor events for this update.
    pub events: Vec<MonitorEvent>,
}

/// Runtime monitor for RaptorQ decode failure probability (§3.1.1).
#[derive(Debug)]
pub struct FailureRateMonitor {
    buckets: HashMap<FailureBucketKey, FailureEProcessState>,
    debug_every_attempts: u64,
    wilson_z: f64,
}

impl FailureRateMonitor {
    /// Create a monitor with default policy.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            debug_every_attempts: DEFAULT_DEBUG_EVERY_ATTEMPTS,
            wilson_z: DEFAULT_WILSON_Z,
        }
    }

    /// Create a monitor with explicit debug and confidence controls.
    #[must_use]
    pub fn with_policy(debug_every_attempts: u64, wilson_z: f64) -> Self {
        Self {
            buckets: HashMap::new(),
            debug_every_attempts: debug_every_attempts.max(1),
            wilson_z: if wilson_z > 0.0 {
                wilson_z
            } else {
                DEFAULT_WILSON_Z
            },
        }
    }

    /// Read state for a specific bucket.
    #[must_use]
    pub fn state_for(&self, key: FailureBucketKey) -> Option<FailureEProcessState> {
        self.buckets.get(&key).copied()
    }

    /// Adaptive redundancy signal: increase repair overhead under drift.
    ///
    /// Returns:
    /// - `0` when no adjustment is needed
    /// - `1` when warning-level drift is observed
    /// - `2` when alert-level drift is observed
    #[must_use]
    pub fn recommended_redundancy_bump(&self, attempt: DecodeAttempt) -> u32 {
        let key = FailureBucketKey::from_attempt(attempt);
        let Some(state) = self.state_for(key) else {
            return 0;
        };
        if state.alerted {
            2
        } else {
            u32::from(state.warned)
        }
    }

    /// Update monitor state with one decode attempt.
    #[allow(clippy::too_many_lines)]
    pub fn update(&mut self, attempt: DecodeAttempt) -> MonitorUpdate {
        let bucket = FailureBucketKey::from_attempt(attempt);
        let null_rate = conservative_null_rate(bucket.overhead_bucket);
        let state = self.buckets.entry(bucket).or_insert_with(|| {
            FailureEProcessState::new(null_rate, DEFAULT_FAILURE_ALERT_THRESHOLD)
        });

        let x = if attempt.success { 0.0 } else { 1.0 };
        let lambda = eprocess_bet_size(state.null_rate);
        let factor = lambda.mul_add(x - state.null_rate, 1.0).max(1e-12);
        state.e_value *= factor;
        state.total_attempts += 1;
        if !attempt.success {
            state.total_failures += 1;
        }
        state.p_upper =
            wilson_upper_bound(state.total_failures, state.total_attempts, self.wilson_z);

        let mut events = Vec::new();
        let should_emit_debug =
            !attempt.success || state.total_attempts % self.debug_every_attempts == 0;
        if should_emit_debug {
            debug!(
                k_range = %bucket.k_range,
                overhead_bucket = bucket.overhead_bucket,
                attempts = state.total_attempts,
                failures = state.total_failures,
                p_upper = state.p_upper,
                p_hat = state.observed_rate_point(),
                null_rate = state.null_rate,
                e_value = state.e_value,
                decode_time_us = attempt.decode_time_us,
                symbol_size = attempt.symbol_size,
                object_type = ?attempt.object_type,
                "failure monitor update"
            );
            events.push(MonitorEvent {
                level: MonitorLogLevel::Debug,
                bucket,
                attempts: state.total_attempts,
                failures: state.total_failures,
                e_value: state.e_value,
                p_upper: state.p_upper,
                null_rate: state.null_rate,
                message: "failure monitor update",
            });
        }

        let warn_rate_budget = (state.null_rate * 1.25).max(0.08);
        let near_threshold = state.total_attempts >= MIN_ATTEMPTS_FOR_WARN
            && (state.e_value >= state.alert_threshold * 0.5 || state.p_upper > warn_rate_budget);
        if near_threshold && !state.warned {
            state.warned = true;
            warn!(
                k_range = %bucket.k_range,
                overhead_bucket = bucket.overhead_bucket,
                attempts = state.total_attempts,
                failures = state.total_failures,
                p_upper = state.p_upper,
                null_rate = state.null_rate,
                e_value = state.e_value,
                "decode failure drift approaching threshold"
            );
            events.push(MonitorEvent {
                level: MonitorLogLevel::Warn,
                bucket,
                attempts: state.total_attempts,
                failures: state.total_failures,
                e_value: state.e_value,
                p_upper: state.p_upper,
                null_rate: state.null_rate,
                message: "decode failure drift approaching threshold",
            });
        }

        let alert_rate_budget = (state.null_rate * 2.0).max(0.15);
        let alert = state.total_attempts >= MIN_ATTEMPTS_FOR_ALERT
            && (state.e_value >= state.alert_threshold || state.p_upper > alert_rate_budget);
        if alert && !state.alerted {
            state.alerted = true;
            info!(
                k_range = %bucket.k_range,
                overhead_bucket = bucket.overhead_bucket,
                attempts = state.total_attempts,
                failures = state.total_failures,
                p_upper = state.p_upper,
                null_rate = state.null_rate,
                e_value = state.e_value,
                "decode failure drift alert triggered"
            );
            events.push(MonitorEvent {
                level: MonitorLogLevel::Info,
                bucket,
                attempts: state.total_attempts,
                failures: state.total_failures,
                e_value: state.e_value,
                p_upper: state.p_upper,
                null_rate: state.null_rate,
                message: "decode failure drift alert triggered",
            });
        }

        let k_plus_two_failure = !attempt.success
            && attempt.symbols_received >= attempt.k_source.saturating_add(2)
            && attempt.k_source > 0;
        if k_plus_two_failure {
            error!(
                k_source = attempt.k_source,
                symbols_received = attempt.symbols_received,
                overhead = attempt.overhead,
                symbol_size = attempt.symbol_size,
                object_type = ?attempt.object_type,
                "decode failed despite conservative K+2 policy"
            );
            events.push(MonitorEvent {
                level: MonitorLogLevel::Error,
                bucket,
                attempts: state.total_attempts,
                failures: state.total_failures,
                e_value: state.e_value,
                p_upper: state.p_upper,
                null_rate: state.null_rate,
                message: "decode failed despite conservative K+2 policy",
            });
        }

        MonitorUpdate {
            bucket,
            state: *state,
            events,
        }
    }
}

impl Default for FailureRateMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Conservative null-rate bound from RFC 6330 Annex B guidance.
#[must_use]
pub const fn conservative_null_rate(overhead_bucket: u32) -> f64 {
    match overhead_bucket {
        0 => 0.02,
        1 => 0.001,
        _ => 0.000_01,
    }
}

/// Betting weight for the one-step e-process update.
#[must_use]
pub fn eprocess_bet_size(null_rate: f64) -> f64 {
    if null_rate <= 0.001 { 0.5 } else { 0.75 }
}

/// Conservative upper bound for Bernoulli failure probability via Wilson interval.
#[must_use]
pub fn wilson_upper_bound(failures: u64, attempts: u64, z: f64) -> f64 {
    if attempts == 0 {
        return 1.0;
    }

    let n = attempts as f64;
    let p_hat = failures as f64 / n;
    let z2 = z * z;
    let center = p_hat + z2 / (2.0 * n);
    let margin = z * (p_hat.mul_add(1.0 - p_hat, z2 / (4.0 * n)) / n).sqrt();
    ((center + margin) / (1.0 + z2 / n)).clamp(0.0, 1.0)
}

/// Decode-failure probability under i.i.d. symbol loss.
///
/// Formula: `P(loss) = Σ_{i=(N-K+1)}^{N} C(N,i) p^i (1-p)^(N-i)`.
///
/// Where:
/// - `N` = `total_symbols`
/// - `K` = `k_required`
/// - `p` = per-symbol loss probability
#[must_use]
pub fn failure_probability_formula(
    total_symbols: u32,
    k_required: u32,
    loss_probability: f64,
) -> f64 {
    if k_required == 0 {
        return 0.0;
    }
    if k_required > total_symbols {
        return 1.0;
    }

    let p = loss_probability.clamp(0.0, 1.0);
    if p <= f64::EPSILON {
        return 0.0;
    }
    if (1.0 - p) <= f64::EPSILON {
        return 1.0;
    }

    let max_losses_without_failure = total_symbols - k_required;
    let mut probability = 0.0;
    for losses in max_losses_without_failure + 1..=total_symbols {
        probability += binomial_probability(total_symbols, losses, p);
    }
    probability.clamp(0.0, 1.0)
}

fn binomial_probability(n: u32, k: u32, p: f64) -> f64 {
    if k > n {
        return 0.0;
    }
    let ln_comb = ln_n_choose_k(n, k);
    let failures_term = f64::from(k) * p.ln();
    let successes_term = f64::from(n - k) * (1.0 - p).ln();
    (ln_comb + failures_term + successes_term).exp()
}

fn ln_n_choose_k(n: u32, k: u32) -> f64 {
    let k_small = k.min(n - k);
    if k_small == 0 {
        return 0.0;
    }

    let mut acc = 0.0;
    for i in 1..=k_small {
        let numerator = f64::from(n - k_small + i);
        let denominator = f64::from(i);
        acc += (numerator / denominator).ln();
    }
    acc
}

/// Policy knobs for adaptive redundancy autopilot (§3.5.12).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveRedundancyPolicy {
    /// Minimum allowed overhead percentage.
    pub overhead_min: u32,
    /// Maximum allowed overhead percentage.
    pub overhead_max: u32,
    /// Optional safe-decrease bound for conservative `p_upper`.
    pub p_upper_safe_decrease_budget: f64,
    /// Minimum samples required before safe decrease is considered.
    pub safe_decrease_min_attempts: u64,
    /// Fixed percentage-point step when safe decrease is applied.
    pub safe_decrease_step_percent: u32,
    /// Warn-only upper bound for conservative `p_upper`.
    pub p_upper_warn_budget: f64,
    /// Alert threshold for conservative `p_upper`.
    pub p_upper_alert_budget: f64,
    /// Bound above which durability contract is considered violated.
    pub p_upper_violation_budget: f64,
}

impl Default for AdaptiveRedundancyPolicy {
    fn default() -> Self {
        Self {
            overhead_min: 5,
            overhead_max: 200,
            p_upper_safe_decrease_budget: 0.005,
            safe_decrease_min_attempts: 2_048,
            safe_decrease_step_percent: 5,
            p_upper_warn_budget: 0.08,
            p_upper_alert_budget: 0.15,
            p_upper_violation_budget: 0.50,
        }
    }
}

/// Deterministic policy decision from adaptive redundancy autopilot.
#[derive(Debug, Clone, PartialEq)]
pub struct RedundancyAutopilotDecision {
    /// Previous overhead percentage.
    pub old_overhead_percent: u32,
    /// New overhead percentage.
    pub new_overhead_percent: u32,
    /// Trigger for this decision.
    pub trigger: RedundancyTrigger,
    /// Monotone regime id for replay stability.
    pub regime_id: u64,
    /// Snapshot upper-bound estimate used as the hard guardrail.
    pub p_upper: f64,
    /// E-process value at decision time (diagnostic).
    pub e_value: f64,
    /// Whether retroactive hardening should be enqueued.
    pub retroactive_hardening_enqueued: bool,
    /// Whether integrity sweeps should be escalated.
    pub integrity_sweeps_escalated: bool,
    /// Whether policy entered a contract-violation window.
    pub durability_contract_violated: bool,
    /// Append-only evidence entry for this decision.
    pub evidence_entry: OverheadRetuneEntry,
}

impl AdaptiveRedundancyPolicy {
    fn maybe_safe_decrease(
        &self,
        current_overhead_percent: u32,
        k_source: u32,
        state: FailureEProcessState,
        regime_id: u64,
    ) -> Option<RedundancyAutopilotDecision> {
        if state.p_upper > self.p_upper_safe_decrease_budget
            || state.total_attempts < self.safe_decrease_min_attempts
            || current_overhead_percent <= self.overhead_min
        {
            return None;
        }

        let decrease_step = self.safe_decrease_step_percent.max(1);
        let new_overhead_percent = current_overhead_percent
            .saturating_sub(decrease_step)
            .max(self.overhead_min);
        let trigger = RedundancyTrigger::EprocessSafeDecrease;

        info!(
            bead_id = ADAPTIVE_REDUNDANCY_BEAD_ID,
            logging_standard = ADAPTIVE_REDUNDANCY_LOGGING_STANDARD,
            old_overhead = current_overhead_percent,
            new_overhead = new_overhead_percent,
            trigger = %trigger,
            regime_id,
            p_upper = state.p_upper,
            total_attempts = state.total_attempts,
            "adaptive redundancy conservative decrease applied"
        );

        let evidence_entry = record_overhead_retune_with_context(
            k_source,
            &RepairConfig::with_overhead(current_overhead_percent),
            new_overhead_percent,
            state.e_value,
            trigger,
            regime_id,
            state.p_upper,
        );

        Some(RedundancyAutopilotDecision {
            old_overhead_percent: current_overhead_percent,
            new_overhead_percent,
            trigger,
            regime_id,
            p_upper: state.p_upper,
            e_value: state.e_value,
            retroactive_hardening_enqueued: false,
            integrity_sweeps_escalated: false,
            durability_contract_violated: false,
            evidence_entry,
        })
    }

    fn hardening_decision(
        &self,
        current_overhead_percent: u32,
        k_source: u32,
        state: FailureEProcessState,
        regime_id: u64,
    ) -> RedundancyAutopilotDecision {
        let new_overhead_percent = current_overhead_percent
            .saturating_mul(2)
            .max(self.overhead_min)
            .min(self.overhead_max);
        let trigger = RedundancyTrigger::EprocessReject;
        let durability_contract_violated = state.p_upper > self.p_upper_violation_budget;

        if durability_contract_violated {
            error!(
                bead_id = ADAPTIVE_REDUNDANCY_BEAD_ID,
                logging_standard = ADAPTIVE_REDUNDANCY_LOGGING_STANDARD,
                old_overhead = current_overhead_percent,
                new_overhead = new_overhead_percent,
                regime_id,
                p_upper = state.p_upper,
                violation_budget = self.p_upper_violation_budget,
                "durability contract violated: repair insufficiency proof required"
            );
        }

        info!(
            bead_id = ADAPTIVE_REDUNDANCY_BEAD_ID,
            logging_standard = ADAPTIVE_REDUNDANCY_LOGGING_STANDARD,
            old_overhead = current_overhead_percent,
            new_overhead = new_overhead_percent,
            trigger = %trigger,
            regime_id,
            p_upper = state.p_upper,
            e_value = state.e_value,
            "adaptive redundancy policy change applied"
        );

        let evidence_entry = record_overhead_retune_with_context(
            k_source,
            &RepairConfig::with_overhead(current_overhead_percent),
            new_overhead_percent,
            state.e_value,
            trigger,
            regime_id,
            state.p_upper,
        );

        RedundancyAutopilotDecision {
            old_overhead_percent: current_overhead_percent,
            new_overhead_percent,
            trigger,
            regime_id,
            p_upper: state.p_upper,
            e_value: state.e_value,
            retroactive_hardening_enqueued: true,
            integrity_sweeps_escalated: true,
            durability_contract_violated,
            evidence_entry,
        }
    }

    /// Evaluate adaptive redundancy policy using the anytime-valid bound
    /// (`p_upper`) as the hard guardrail.
    #[must_use]
    pub fn evaluate(
        &self,
        current_overhead_percent: u32,
        k_source: u32,
        state: FailureEProcessState,
        regime_id: u64,
    ) -> Option<RedundancyAutopilotDecision> {
        debug!(
            bead_id = ADAPTIVE_REDUNDANCY_BEAD_ID,
            logging_standard = ADAPTIVE_REDUNDANCY_LOGGING_STANDARD,
            old_overhead = current_overhead_percent,
            regime_id,
            p_upper = state.p_upper,
            e_value = state.e_value,
            "adaptive redundancy policy evaluation"
        );

        if let Some(decision) =
            self.maybe_safe_decrease(current_overhead_percent, k_source, state, regime_id)
        {
            return Some(decision);
        }

        if state.p_upper <= self.p_upper_warn_budget {
            return None;
        }

        if state.p_upper <= self.p_upper_alert_budget {
            warn!(
                bead_id = ADAPTIVE_REDUNDANCY_BEAD_ID,
                logging_standard = ADAPTIVE_REDUNDANCY_LOGGING_STANDARD,
                old_overhead = current_overhead_percent,
                regime_id,
                p_upper = state.p_upper,
                warn_budget = self.p_upper_warn_budget,
                alert_budget = self.p_upper_alert_budget,
                "entering durable-but-not-repairable warning window"
            );
            return None;
        }

        Some(self.hardening_decision(current_overhead_percent, k_source, state, regime_id))
    }
}

/// Record an adaptive overhead retune in the evidence ledger (§3.5.12).
///
/// Returns the ledger entry for persistence.
#[must_use]
pub fn record_overhead_retune_with_context(
    k_source: u32,
    old_config: &RepairConfig,
    new_overhead_percent: u32,
    e_value: f64,
    trigger: RedundancyTrigger,
    regime_id: u64,
    p_upper: f64,
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
        trigger,
        regime_id,
        p_upper,
    };

    info!(
        bead_id = ADAPTIVE_REDUNDANCY_BEAD_ID,
        logging_standard = ADAPTIVE_REDUNDANCY_LOGGING_STANDARD,
        old_overhead = old_config.overhead_percent,
        new_overhead = new_overhead_percent,
        trigger = %trigger,
        regime_id,
        e_value,
        p_upper,
        old_loss_fraction_max_permille = old_budget.loss_fraction_max_permille,
        new_loss_fraction_max_permille = new_budget.loss_fraction_max_permille,
        k_source,
        "adaptive overhead retune — evidence ledger entry"
    );

    entry
}

/// Backward-compatible helper for callers without trigger context.
#[must_use]
pub fn record_overhead_retune(
    k_source: u32,
    old_config: &RepairConfig,
    new_overhead_percent: u32,
    e_value: f64,
) -> OverheadRetuneEntry {
    record_overhead_retune_with_context(
        k_source,
        old_config,
        new_overhead_percent,
        e_value,
        RedundancyTrigger::Manual,
        0,
        1.0,
    )
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

        // K=3, 20% → formula gives 2, small-K clamp raises to 3.
        let b = compute_repair_budget(3, &config);
        assert_eq!(b.repair_count, 3);
        assert!(b.small_k_clamped);

        // K=1, 20% → formula gives 2, small-K clamp raises to 3.
        let b = compute_repair_budget(1, &config);
        assert_eq!(b.repair_count, 3);
        assert!(b.small_k_clamped);

        // K=56403, 20% → R = max(2, ceil(11280.6)) = max(2, 11281) = 11281.
        let b = compute_repair_budget(56403, &config);
        assert_eq!(b.repair_count, 11281);
        assert_eq!(b.policy_id, REPAIR_BUDGET_POLICY_ID);
        assert_eq!(b.policy_epoch, INITIAL_REPAIR_POLICY_EPOCH);
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

        // K=3, R=3 after small-K clamp: loss_fraction_max = (3-2)/(3+3) = 1/6 = 166‰.
        let b = compute_repair_budget(3, &config);
        assert_eq!(b.loss_fraction_max_permille, 166);
    }

    // -- bd-1hi.22 test 6: Small-K underprovisioning warning --

    #[test]
    fn test_small_k_underprovisioning_warning() {
        let config = RepairConfig::new();

        // K=3 with 20% overhead uses small-K clamp (R=3), so we are no longer underprovisioned.
        let b = compute_repair_budget(3, &config);
        assert!(
            !b.underprovisioned,
            "K=3 should be hardened by small-K clamp"
        );
        assert_eq!(b.loss_fraction_max_permille, 166);

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

    #[test]
    fn test_select_repair_count_policy_surface() {
        assert_eq!(select_repair_count(1, 20), 3);
        assert_eq!(select_repair_count(3, 20), 3);
        assert_eq!(select_repair_count(100, 20), 20);
    }

    #[test]
    fn test_repair_budget_bounds_and_clamps() {
        let config = RepairConfig {
            slack_decode: DEFAULT_SLACK_DECODE,
            overhead_percent: 900,
            small_k_clamp_max_k: 8,
            small_k_min_repair: 3,
            max_repair_symbols: 25,
            policy_id: REPAIR_BUDGET_POLICY_ID,
            policy_epoch: 7,
        };
        let b = compute_repair_budget(100, &config);
        assert_eq!(b.overhead_percent_applied, MAX_OVERHEAD_PERCENT);
        assert_eq!(b.repair_count, 25);
        assert!(b.max_repair_capped);
        assert_eq!(b.policy_epoch, 7);
    }

    fn simulate_decode_with_losses(
        k_source: u32,
        config: &RepairConfig,
        losses: u32,
    ) -> (bool, u32, u32) {
        let budget = compute_repair_budget(k_source, config);
        let total_symbols = budget.k_source.saturating_add(budget.repair_count);
        let received = total_symbols.saturating_sub(losses.min(total_symbols));
        let required = budget.k_source.saturating_add(config.slack_decode);
        (received >= required, received, required)
    }

    #[test]
    fn test_bd_166a_mapping_invariants_and_small_k_clamps() {
        for overhead in [1_u32, 5, 20, 33, 100] {
            let config = RepairConfig::with_overhead(overhead);

            for k in 1..=config.small_k_clamp_max_k {
                let budget = compute_repair_budget(k, &config);
                assert!(budget.repair_count >= config.small_k_min_repair);
                assert!(budget.repair_count >= config.slack_decode);
                assert!(!budget.underprovisioned);
            }

            let start = config.small_k_clamp_max_k.saturating_add(1);
            let mut prev = compute_repair_budget(start, &config).repair_count;
            assert!(prev >= config.slack_decode);

            for k in start.saturating_add(1)..=512 {
                let budget = compute_repair_budget(k, &config);
                assert!(
                    budget.repair_count >= prev,
                    "repair count must be monotone above small-k clamp boundary: overhead={overhead} k={k} prev={prev} curr={}",
                    budget.repair_count
                );
                assert!(budget.repair_count >= config.slack_decode);
                prev = budget.repair_count;
            }
        }
    }

    #[test]
    fn test_bd_166a_rounding_matches_ceil_rule() {
        for (k_source, overhead_percent) in [(9_u32, 25_u32), (11, 33), (101, 17), (257, 7)] {
            let config = RepairConfig::with_overhead(overhead_percent);
            let budget = compute_repair_budget(k_source, &config);

            let rounded = (u64::from(k_source) * u64::from(overhead_percent)).div_ceil(100);
            let rounded = u32::try_from(rounded).expect("rounded repair count must fit u32");
            let expected = rounded.max(config.slack_decode);
            assert_eq!(
                budget.repair_count, expected,
                "repair count must follow ceil mapping for k={k_source} overhead={overhead_percent}"
            );
        }
    }

    #[test]
    fn test_bd_166a_loss_simulation_within_budget_succeeds() {
        for (k_source, overhead_percent) in [(32_u32, 20_u32), (128, 25), (512, 33)] {
            let config = RepairConfig::with_overhead(overhead_percent);
            let budget = compute_repair_budget(k_source, &config);
            let tolerated_losses = budget.repair_count.saturating_sub(config.slack_decode);

            let (success, received, required) =
                simulate_decode_with_losses(k_source, &config, tolerated_losses);
            assert!(
                success,
                "decode should succeed at tolerated erasure bound: k={k_source} overhead={overhead_percent} received={received} required={required}"
            );
        }
    }

    #[test]
    fn test_bd_166a_loss_simulation_beyond_budget_emits_artifact() {
        let k_source = 128;
        let config = RepairConfig::with_overhead(20);
        let budget = compute_repair_budget(k_source, &config);
        let tolerated_losses = budget.repair_count.saturating_sub(config.slack_decode);
        let losses = tolerated_losses.saturating_add(1);

        let (success, received, required) = simulate_decode_with_losses(k_source, &config, losses);
        assert!(
            !success,
            "decode should fail beyond tolerance: losses={losses} received={received} required={required}"
        );

        let state = make_state_with_counts(0.25, 37.0, 10_000, 1_000);
        let decision = AdaptiveRedundancyPolicy::default()
            .evaluate(config.overhead_percent, k_source, state, 166)
            .expect("failure above budget should trigger explainable hardening decision");
        assert_eq!(decision.trigger, RedundancyTrigger::EprocessReject);
        assert_eq!(
            decision.evidence_entry.trigger,
            RedundancyTrigger::EprocessReject
        );
        assert_eq!(decision.evidence_entry.regime_id, 166);
        assert!(decision.evidence_entry.p_upper > 0.15);
    }

    #[test]
    fn test_object_policy_defaults_stricter_for_commit_artifacts() {
        let marker = ObjectRepairPolicy::for_class(RepairObjectClass::CommitMarker);
        let proof = ObjectRepairPolicy::for_class(RepairObjectClass::CommitProof);
        let history = ObjectRepairPolicy::for_class(RepairObjectClass::PageHistory);
        assert!(marker.default_overhead_percent > history.default_overhead_percent);
        assert!(proof.default_overhead_percent > history.default_overhead_percent);
        assert!(marker.small_k_min_repair >= history.small_k_min_repair);
    }

    #[test]
    fn test_compute_repair_budget_for_object_policy() {
        let marker_budget = compute_repair_budget_for_object(
            50,
            RepairObjectClass::CommitMarker,
            None,
            INITIAL_REPAIR_POLICY_EPOCH + 1,
        );
        let history_budget = compute_repair_budget_for_object(
            50,
            RepairObjectClass::PageHistory,
            None,
            INITIAL_REPAIR_POLICY_EPOCH + 1,
        );
        assert!(marker_budget.repair_count > history_budget.repair_count);
        assert_eq!(marker_budget.policy_id, REPAIR_BUDGET_POLICY_ID);
        assert_eq!(marker_budget.policy_epoch, INITIAL_REPAIR_POLICY_EPOCH + 1);
    }

    #[test]
    fn test_policy_change_epoch_boundary_only() {
        assert!(!can_apply_policy_change(5, 5));
        assert!(!can_apply_policy_change(5, 4));
        assert!(can_apply_policy_change(5, 6));
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
            assert_eq!(seed_a, seed_b, "seed must be deterministic for payload {i}");
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

    // -- bd-1hi.7 test 9: test_failure_probability_formula --

    #[test]
    fn test_failure_probability_formula() {
        // N=3, K=2, p=0.1:
        // P(loss) = C(3,2)*0.1^2*0.9 + C(3,3)*0.1^3 = 0.027 + 0.001 = 0.028
        let p = failure_probability_formula(3, 2, 0.1);
        assert!((p - 0.028).abs() < 1e-12, "expected 0.028, got {p:.12}");

        // Exactly-K decode with N=K=5 and p=0.2:
        // failure when >=1 symbol lost => 1 - (0.8^5) = 0.67232
        let p = failure_probability_formula(5, 5, 0.2);
        assert!(
            (p - 0.672_32).abs() < 1e-10,
            "expected 0.67232, got {p:.12}"
        );
    }

    // -- bd-1hi.7 test 10: test_failure_monitoring_e_process --

    #[test]
    fn test_failure_monitoring_e_process() {
        let mut monitor = FailureRateMonitor::new();
        let attempt = DecodeAttempt::new(100, 102, 4096, true, 250, DecodeObjectType::EcsObject);

        for _ in 0..500 {
            let _ = monitor.update(attempt);
        }

        let key = FailureBucketKey::from_attempt(attempt);
        let state = monitor
            .state_for(key)
            .expect("monitor state should exist after updates");

        assert_eq!(state.total_attempts, 500);
        assert_eq!(state.total_failures, 0);
        assert!(
            state.e_value < 1.0,
            "success-only stream should not inflate e-value"
        );
        assert!(
            !state.alerted,
            "no alert expected under stable success stream"
        );
    }

    // -- bd-1hi.7 test 11: test_failure_alert_on_drift --

    #[test]
    fn test_failure_alert_on_drift() {
        let mut monitor = FailureRateMonitor::new();

        // Baseline stable period.
        for _ in 0..100 {
            let _ = monitor.update(DecodeAttempt::new(
                100,
                102,
                4096,
                true,
                250,
                DecodeObjectType::WalCommitGroup,
            ));
        }

        // Deterministic elevated-failure phase.
        let mut saw_alert = false;
        for i in 0..500 {
            let success = i % 3 != 0; // ~33% failures, far above K+2 null.
            let update = monitor.update(DecodeAttempt::new(
                100,
                102,
                4096,
                success,
                250,
                DecodeObjectType::WalCommitGroup,
            ));
            saw_alert |= update
                .events
                .iter()
                .any(|event| event.level == MonitorLogLevel::Info);
        }

        assert!(
            saw_alert,
            "monitor must emit INFO alert when drift exceeds conservative envelope"
        );
    }

    // -- bd-1hi.7 test 12: test_failure_p_upper_conservative --

    #[test]
    fn test_failure_p_upper_conservative() {
        let mut monitor = FailureRateMonitor::with_policy(8, DEFAULT_WILSON_Z);

        // 1 failure in 100 observations.
        for i in 0..100 {
            let success = i != 50;
            let _ = monitor.update(DecodeAttempt::new(
                100,
                100,
                4096,
                success,
                300,
                DecodeObjectType::SnapshotBlock,
            ));
        }

        let key = FailureBucketKey {
            k_range: KRangeBucket::K11To100,
            overhead_bucket: 0,
        };
        let state = monitor
            .state_for(key)
            .expect("state should exist for overhead-0 bucket");

        let p_hat = state.observed_rate_point();
        assert!(
            state.p_upper >= p_hat,
            "p_upper must be conservative: p_upper={} p_hat={}",
            state.p_upper,
            p_hat
        );
        assert!(
            state.p_upper > 0.01,
            "with 1/100 failures and z=3, p_upper should stay conservative"
        );
    }

    fn make_state(p_upper: f64, e_value: f64) -> FailureEProcessState {
        make_state_with_counts(p_upper, e_value, 256, 64)
    }

    fn make_state_with_counts(
        p_upper: f64,
        e_value: f64,
        total_attempts: u64,
        total_failures: u64,
    ) -> FailureEProcessState {
        FailureEProcessState {
            e_value,
            total_attempts,
            total_failures,
            null_rate: 0.02,
            alert_threshold: DEFAULT_FAILURE_ALERT_THRESHOLD,
            p_upper,
            warned: true,
            alerted: true,
        }
    }

    #[test]
    fn test_redundancy_autopilot() {
        let policy = AdaptiveRedundancyPolicy::default();
        let state = make_state(0.32, 41.0);
        let decision = policy
            .evaluate(20, 100, state, 7)
            .expect("p_upper above alert budget must trigger autopilot");
        assert_eq!(decision.old_overhead_percent, 20);
        assert_eq!(decision.new_overhead_percent, 40);
        assert_eq!(decision.trigger, RedundancyTrigger::EprocessReject);
        assert!(decision.retroactive_hardening_enqueued);
        assert!(decision.integrity_sweeps_escalated);
    }

    #[test]
    fn test_redundancy_increases_on_corruption() {
        let mut monitor = FailureRateMonitor::new();
        for i in 0..600 {
            let success = i % 4 != 0; // 25% corruption/failure rate.
            let _ = monitor.update(DecodeAttempt::new(
                100,
                102,
                4096,
                success,
                250,
                DecodeObjectType::EcsObject,
            ));
        }
        let key = FailureBucketKey {
            k_range: KRangeBucket::K11To100,
            overhead_bucket: 2,
        };
        let state = monitor
            .state_for(key)
            .expect("monitor bucket should exist after updates");
        let decision = AdaptiveRedundancyPolicy::default()
            .evaluate(20, 100, state, 11)
            .expect("high corruption must trigger redundancy increase");
        assert!(decision.new_overhead_percent > decision.old_overhead_percent);
    }

    #[test]
    fn test_redundancy_evidence_logged() {
        let policy = AdaptiveRedundancyPolicy::default();
        let state = make_state(0.28, 33.0);
        let decision = policy.evaluate(25, 100, state, 99).expect("decision");
        assert_eq!(
            decision.evidence_entry.trigger,
            RedundancyTrigger::EprocessReject
        );
        assert_eq!(decision.evidence_entry.regime_id, 99);
        assert!(decision.evidence_entry.p_upper > policy.p_upper_alert_budget);
        assert_eq!(decision.evidence_entry.new_overhead_percent, 50);
    }

    #[test]
    fn test_policy_uses_p_upper_guardrail() {
        let policy = AdaptiveRedundancyPolicy::default();
        let state = make_state(0.01, 10_000.0); // high e-value, safe p_upper.
        let decision = policy.evaluate(20, 100, state, 3);
        assert!(
            decision.is_none(),
            "hard gate must be p_upper, not diagnostic e_value under optional stopping"
        );
    }

    #[test]
    fn test_redundancy_safe_regime_stays_stable() {
        let policy = AdaptiveRedundancyPolicy::default();
        let state = make_state_with_counts(0.01, 0.8, 10_000, 1);
        let decision = policy.evaluate(40, 100, state, 91);
        assert!(
            decision.is_none(),
            "safe regime below warn budget should not thrash overhead policy"
        );
    }

    #[test]
    fn test_redundancy_safe_decrease_requires_strong_evidence() {
        let policy = AdaptiveRedundancyPolicy::default();
        let state = make_state_with_counts(0.001, 0.2, 1_024, 0);
        let decision = policy.evaluate(80, 100, state, 92);
        assert!(
            decision.is_none(),
            "safe decrease must wait for minimum-sample confidence"
        );
    }

    #[test]
    fn test_redundancy_safe_decrease_is_conservative_and_explainable() {
        let policy = AdaptiveRedundancyPolicy::default();
        let state = make_state_with_counts(0.001, 0.2, 10_000, 0);
        let decision = policy
            .evaluate(80, 100, state, 93)
            .expect("very stable regime should permit conservative decrease");
        assert_eq!(decision.trigger, RedundancyTrigger::EprocessSafeDecrease);
        assert!(decision.new_overhead_percent < decision.old_overhead_percent);
        assert_eq!(decision.new_overhead_percent, 75);
        assert!(!decision.retroactive_hardening_enqueued);
        assert!(!decision.integrity_sweeps_escalated);
        assert_eq!(
            decision.evidence_entry.trigger,
            RedundancyTrigger::EprocessSafeDecrease
        );
        assert_eq!(decision.evidence_entry.regime_id, 93);
    }

    #[test]
    fn test_bd_1hi_30_unit_compliance_gate() {
        assert_eq!(ADAPTIVE_REDUNDANCY_BEAD_ID, "bd-1hi.30");
        assert_eq!(ADAPTIVE_REDUNDANCY_LOGGING_STANDARD, "bd-1fpm");
        let policy = AdaptiveRedundancyPolicy::default();
        assert!(policy.overhead_min > 0);
        assert!(policy.overhead_max >= policy.overhead_min);
        assert!(policy.safe_decrease_step_percent > 0);
        assert!(policy.safe_decrease_min_attempts >= 1_024);
        assert!(policy.p_upper_warn_budget > policy.p_upper_safe_decrease_budget);
        assert!(policy.p_upper_alert_budget > policy.p_upper_warn_budget);
    }

    #[test]
    fn prop_bd_1hi_30_structure_compliance() {
        let policy = AdaptiveRedundancyPolicy::default();
        for overhead in [5_u32, 20, 64, 120] {
            for p_upper in [0.16_f64, 0.30, 0.70] {
                let state = make_state(p_upper, 25.0);
                let decision = policy
                    .evaluate(overhead, 100, state, 42)
                    .expect("p_upper above alert budget must produce decision");
                assert!(decision.new_overhead_percent >= decision.old_overhead_percent);
                assert!(decision.new_overhead_percent <= policy.overhead_max);
                assert!(decision.evidence_entry.new_overhead_percent <= policy.overhead_max);
            }
        }
    }

    #[test]
    fn test_e2e_bd_1hi_30_compliance() {
        let mut monitor = FailureRateMonitor::new();
        for _ in 0..200 {
            let _ = monitor.update(DecodeAttempt::new(
                100,
                102,
                4096,
                true,
                220,
                DecodeObjectType::WalCommitGroup,
            ));
        }
        for i in 0..400 {
            let success = i % 2 == 0;
            let _ = monitor.update(DecodeAttempt::new(
                100,
                102,
                4096,
                success,
                260,
                DecodeObjectType::WalCommitGroup,
            ));
        }

        let key = FailureBucketKey {
            k_range: KRangeBucket::K11To100,
            overhead_bucket: 2,
        };
        let state = monitor.state_for(key).expect("state after drift");
        let policy = AdaptiveRedundancyPolicy::default();
        let first = policy
            .evaluate(20, 100, state, 123)
            .expect("autopilot decision");
        let second = policy
            .evaluate(20, 100, state, 123)
            .expect("deterministic replay");
        assert_eq!(first, second, "decision must be replay-stable");
        assert!(first.retroactive_hardening_enqueued);
        assert!(first.integrity_sweeps_escalated);
        assert_eq!(first.trigger, RedundancyTrigger::EprocessReject);
    }
}
