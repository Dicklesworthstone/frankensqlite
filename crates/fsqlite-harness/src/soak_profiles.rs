//! Soak workload profiles and history invariant checks (bd-mblr.7.2.1).
//!
//! Defines long-run soak test profiles parameterised by contention mix,
//! schema churn rate, checkpoint cadence, and duration.  Each profile
//! declares explicit **history invariants** that must hold across the
//! entire soak run; violations are reported as [`InvariantViolation`]s
//! with enough context for automated triage.
//!
//! # Design
//!
//! A [`SoakProfile`] describes *what* the soak executor should do.
//! A [`HistoryInvariant`] describes *what must remain true* throughout.
//! A [`SoakWorkloadSpec`] ties profiles to invariants with deterministic
//! seed derivation.  The executor (bd-mblr.7.2.2) consumes the spec.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.2.1";

/// Domain tag for soak seed derivation.
const SEED_DOMAIN: &[u8] = b"soak_profile";

// ---------------------------------------------------------------------------
// Contention mix
// ---------------------------------------------------------------------------

/// Relative proportions of reader vs writer transactions in the workload.
///
/// Ratios are normalised to `readers + writers = 100`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentionMix {
    /// Percentage of reader transactions (0..=100).
    pub reader_pct: u8,
    /// Percentage of writer transactions (0..=100).
    pub writer_pct: u8,
}

impl ContentionMix {
    /// Create a mix, clamping so that `reader_pct + writer_pct == 100`.
    #[must_use]
    pub fn new(reader_pct: u8, writer_pct: u8) -> Self {
        let total = u16::from(reader_pct) + u16::from(writer_pct);
        if total == 0 {
            return Self {
                reader_pct: 50,
                writer_pct: 50,
            };
        }
        #[allow(clippy::cast_possible_truncation)]
        let r = ((u16::from(reader_pct) * 100) / total) as u8;
        Self {
            reader_pct: r,
            writer_pct: 100 - r,
        }
    }

    /// Read-heavy: 90/10.
    #[must_use]
    pub fn read_heavy() -> Self {
        Self {
            reader_pct: 90,
            writer_pct: 10,
        }
    }

    /// Balanced: 50/50.
    #[must_use]
    pub fn balanced() -> Self {
        Self {
            reader_pct: 50,
            writer_pct: 50,
        }
    }

    /// Write-heavy: 20/80.
    #[must_use]
    pub fn write_heavy() -> Self {
        Self {
            reader_pct: 20,
            writer_pct: 80,
        }
    }

    /// Validate the mix sums to 100.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.reader_pct + self.writer_pct == 100
    }
}

impl fmt::Display for ContentionMix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R{}:W{}", self.reader_pct, self.writer_pct)
    }
}

// ---------------------------------------------------------------------------
// Schema churn
// ---------------------------------------------------------------------------

/// How often DDL operations (CREATE/ALTER/DROP) are injected into the workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SchemaChurnRate {
    /// No DDL during soak run.
    None,
    /// DDL every ~1000 transactions.
    Low,
    /// DDL every ~100 transactions.
    Medium,
    /// DDL every ~10 transactions.
    High,
}

impl SchemaChurnRate {
    /// Approximate transaction interval between DDL operations.
    #[must_use]
    pub fn interval_hint(&self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Low => Some(1000),
            Self::Medium => Some(100),
            Self::High => Some(10),
        }
    }
}

impl fmt::Display for SchemaChurnRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Low => write!(f, "low(~1000)"),
            Self::Medium => write!(f, "med(~100)"),
            Self::High => write!(f, "high(~10)"),
        }
    }
}

// ---------------------------------------------------------------------------
// Checkpoint cadence
// ---------------------------------------------------------------------------

/// How frequently WAL checkpoints are triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CheckpointCadence {
    /// Checkpoint every ~50 transactions.
    Aggressive,
    /// Checkpoint every ~500 transactions (default SQLite-like).
    Normal,
    /// Checkpoint every ~5000 transactions.
    Deferred,
    /// Never checkpoint during the soak run.
    Disabled,
}

impl CheckpointCadence {
    /// Approximate transaction interval between checkpoints.
    #[must_use]
    pub fn interval_hint(&self) -> Option<u64> {
        match self {
            Self::Aggressive => Some(50),
            Self::Normal => Some(500),
            Self::Deferred => Some(5000),
            Self::Disabled => None,
        }
    }
}

impl fmt::Display for CheckpointCadence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aggressive => write!(f, "aggressive(~50)"),
            Self::Normal => write!(f, "normal(~500)"),
            Self::Deferred => write!(f, "deferred(~5000)"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction complexity
// ---------------------------------------------------------------------------

/// Complexity level of individual transactions in the workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TransactionComplexity {
    /// Single-statement transactions (1 INSERT/SELECT).
    Simple,
    /// Multi-statement transactions (3-10 statements).
    Moderate,
    /// Complex transactions with subqueries, joins, CTEs (10-50 statements).
    Complex,
    /// Mixed: randomly selected from the above.
    Mixed,
}

impl fmt::Display for TransactionComplexity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple => write!(f, "simple"),
            Self::Moderate => write!(f, "moderate"),
            Self::Complex => write!(f, "complex"),
            Self::Mixed => write!(f, "mixed"),
        }
    }
}

// ---------------------------------------------------------------------------
// Concurrency level
// ---------------------------------------------------------------------------

/// Number of concurrent connections for the soak run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConcurrencyLevel {
    /// Number of concurrent connections.
    pub connections: u16,
}

impl ConcurrencyLevel {
    /// Single connection (sequential).
    #[must_use]
    pub fn sequential() -> Self {
        Self { connections: 1 }
    }

    /// Light concurrency (4 connections).
    #[must_use]
    pub fn light() -> Self {
        Self { connections: 4 }
    }

    /// Moderate concurrency (16 connections).
    #[must_use]
    pub fn moderate() -> Self {
        Self { connections: 16 }
    }

    /// Heavy concurrency (64 connections).
    #[must_use]
    pub fn heavy() -> Self {
        Self { connections: 64 }
    }
}

impl fmt::Display for ConcurrencyLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}conn", self.connections)
    }
}

// ---------------------------------------------------------------------------
// Soak profile
// ---------------------------------------------------------------------------

/// A complete soak test profile describing the workload shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakProfile {
    /// Human-readable profile name (e.g., "heavy-contention-long-run").
    pub name: String,

    /// Profile description.
    pub description: String,

    /// Contention mix (reader/writer ratio).
    pub contention: ContentionMix,

    /// Schema churn rate.
    pub schema_churn: SchemaChurnRate,

    /// Checkpoint cadence.
    pub checkpoint_cadence: CheckpointCadence,

    /// Transaction complexity.
    pub transaction_complexity: TransactionComplexity,

    /// Concurrency level.
    pub concurrency: ConcurrencyLevel,

    /// Target number of transactions to execute.
    pub target_transactions: u64,

    /// Maximum wall-clock duration in seconds (safety bound).
    pub max_duration_secs: u64,

    /// Invariant check interval: check invariants every N transactions.
    pub invariant_check_interval: u64,

    /// Whether to inject fault scenarios (crash, power-loss simulation).
    pub fault_injection_enabled: bool,

    /// Scenario IDs this profile covers (for traceability).
    pub scenario_ids: Vec<String>,
}

impl SoakProfile {
    /// Validate the profile for internal consistency.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.name.is_empty() {
            errors.push("profile name must not be empty".to_owned());
        }
        if !self.contention.is_valid() {
            errors.push(format!(
                "contention mix invalid: {}+{} != 100",
                self.contention.reader_pct, self.contention.writer_pct
            ));
        }
        if self.target_transactions == 0 {
            errors.push("target_transactions must be > 0".to_owned());
        }
        if self.max_duration_secs == 0 {
            errors.push("max_duration_secs must be > 0".to_owned());
        }
        if self.invariant_check_interval == 0 {
            errors.push("invariant_check_interval must be > 0".to_owned());
        }
        if self.invariant_check_interval > self.target_transactions {
            errors.push(format!(
                "invariant_check_interval ({}) > target_transactions ({})",
                self.invariant_check_interval, self.target_transactions
            ));
        }

        errors
    }

    /// Derive a deterministic seed for this profile from a root seed.
    #[must_use]
    pub fn derive_seed(&self, root_seed: u64) -> u64 {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(&root_seed.to_le_bytes());
        buf.extend_from_slice(SEED_DOMAIN);
        buf.extend_from_slice(self.name.as_bytes());
        xxh3_64(&buf)
    }
}

impl fmt::Display for SoakProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} {} chkpt={} txn={} {} target={} max={}s",
            self.name,
            self.contention,
            self.schema_churn,
            self.checkpoint_cadence,
            self.transaction_complexity,
            self.concurrency,
            self.target_transactions,
            self.max_duration_secs,
        )
    }
}

// ---------------------------------------------------------------------------
// Preset profiles
// ---------------------------------------------------------------------------

/// Light soak: quick smoke test, minimal contention.
#[must_use]
pub fn profile_light() -> SoakProfile {
    SoakProfile {
        name: "light".to_owned(),
        description: "Quick smoke soak: 1K txns, sequential, no schema churn".to_owned(),
        contention: ContentionMix::read_heavy(),
        schema_churn: SchemaChurnRate::None,
        checkpoint_cadence: CheckpointCadence::Normal,
        transaction_complexity: TransactionComplexity::Simple,
        concurrency: ConcurrencyLevel::sequential(),
        target_transactions: 1_000,
        max_duration_secs: 60,
        invariant_check_interval: 100,
        fault_injection_enabled: false,
        scenario_ids: vec!["SOAK-001".to_owned()],
    }
}

/// Moderate soak: balanced concurrency with some schema churn.
#[must_use]
pub fn profile_moderate() -> SoakProfile {
    SoakProfile {
        name: "moderate".to_owned(),
        description: "Balanced soak: 10K txns, 4 connections, low schema churn".to_owned(),
        contention: ContentionMix::balanced(),
        schema_churn: SchemaChurnRate::Low,
        checkpoint_cadence: CheckpointCadence::Normal,
        transaction_complexity: TransactionComplexity::Moderate,
        concurrency: ConcurrencyLevel::light(),
        target_transactions: 10_000,
        max_duration_secs: 300,
        invariant_check_interval: 500,
        fault_injection_enabled: false,
        scenario_ids: vec!["SOAK-002".to_owned()],
    }
}

/// Heavy soak: high concurrency with aggressive checkpointing.
#[must_use]
pub fn profile_heavy() -> SoakProfile {
    SoakProfile {
        name: "heavy".to_owned(),
        description:
            "Heavy soak: 100K txns, 16 connections, medium schema churn, aggressive checkpoint"
                .to_owned(),
        contention: ContentionMix::write_heavy(),
        schema_churn: SchemaChurnRate::Medium,
        checkpoint_cadence: CheckpointCadence::Aggressive,
        transaction_complexity: TransactionComplexity::Complex,
        concurrency: ConcurrencyLevel::moderate(),
        target_transactions: 100_000,
        max_duration_secs: 1800,
        invariant_check_interval: 1_000,
        fault_injection_enabled: true,
        scenario_ids: vec!["SOAK-003".to_owned()],
    }
}

/// Stress soak: extreme conditions for maximum coverage.
#[must_use]
pub fn profile_stress() -> SoakProfile {
    SoakProfile {
        name: "stress".to_owned(),
        description: "Stress soak: 500K txns, 64 connections, high schema churn, deferred checkpoint, fault injection".to_owned(),
        contention: ContentionMix::write_heavy(),
        schema_churn: SchemaChurnRate::High,
        checkpoint_cadence: CheckpointCadence::Deferred,
        transaction_complexity: TransactionComplexity::Mixed,
        concurrency: ConcurrencyLevel::heavy(),
        target_transactions: 500_000,
        max_duration_secs: 3600,
        invariant_check_interval: 5_000,
        fault_injection_enabled: true,
        scenario_ids: vec!["SOAK-004".to_owned()],
    }
}

/// All preset profiles.
#[must_use]
pub fn all_presets() -> Vec<SoakProfile> {
    vec![
        profile_light(),
        profile_moderate(),
        profile_heavy(),
        profile_stress(),
    ]
}

// ---------------------------------------------------------------------------
// History invariants
// ---------------------------------------------------------------------------

/// Classification of invariants by enforcement mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum InvariantClass {
    /// Hardware/CAS-enforced: violations indicate a fundamental bug.
    Hard,
    /// Software logic: violations indicate logic errors.
    Soft,
    /// Statistical: violations indicate drift beyond acceptable bounds.
    Statistical,
}

/// A named history invariant that can be checked at soak checkpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryInvariant {
    /// Unique invariant identifier (e.g., "SOAK-INV-001").
    pub id: String,

    /// Human-readable name.
    pub name: String,

    /// Description of what this invariant checks.
    pub description: String,

    /// Invariant classification.
    pub class: InvariantClass,

    /// Which MVCC invariant(s) this checks (INV-1..INV-7), if applicable.
    pub mvcc_invariant_refs: Vec<String>,

    /// Severity: 0 = critical (abort soak), 1 = high, 2 = warning.
    pub severity: u8,
}

impl HistoryInvariant {
    /// Whether a violation of this invariant should abort the soak run.
    #[must_use]
    pub fn is_abort_on_violation(&self) -> bool {
        self.severity == 0
    }
}

impl fmt::Display for HistoryInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} ({:?}, sev={})",
            self.id, self.name, self.class, self.severity
        )
    }
}

// ---------------------------------------------------------------------------
// Canonical invariant catalog
// ---------------------------------------------------------------------------

/// Build the canonical set of soak history invariants.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn canonical_invariants() -> Vec<HistoryInvariant> {
    vec![
        HistoryInvariant {
            id: "SOAK-INV-001".to_owned(),
            name: "monotone_counters".to_owned(),
            description: "TxnId and CommitSeq are strictly monotonically increasing across all checkpoints".to_owned(),
            class: InvariantClass::Hard,
            mvcc_invariant_refs: vec!["INV-1".to_owned()],
            severity: 0,
        },
        HistoryInvariant {
            id: "SOAK-INV-002".to_owned(),
            name: "no_lost_updates".to_owned(),
            description: "Every committed write is visible in subsequent reads under the same or later snapshot".to_owned(),
            class: InvariantClass::Hard,
            mvcc_invariant_refs: vec!["INV-6".to_owned()],
            severity: 0,
        },
        HistoryInvariant {
            id: "SOAK-INV-003".to_owned(),
            name: "snapshot_isolation".to_owned(),
            description: "Reads within a snapshot never observe partial transactions".to_owned(),
            class: InvariantClass::Hard,
            mvcc_invariant_refs: vec!["INV-5".to_owned(), "INV-6".to_owned()],
            severity: 0,
        },
        HistoryInvariant {
            id: "SOAK-INV-004".to_owned(),
            name: "serializable_history".to_owned(),
            description: "The committed transaction history is equivalent to some serial order (SSI)".to_owned(),
            class: InvariantClass::Soft,
            mvcc_invariant_refs: vec!["INV-7".to_owned()],
            severity: 0,
        },
        HistoryInvariant {
            id: "SOAK-INV-005".to_owned(),
            name: "checkpoint_consistency".to_owned(),
            description: "After checkpoint, all committed data is durable and WAL is consistent".to_owned(),
            class: InvariantClass::Hard,
            mvcc_invariant_refs: vec![],
            severity: 0,
        },
        HistoryInvariant {
            id: "SOAK-INV-006".to_owned(),
            name: "wal_bounded_growth".to_owned(),
            description: "WAL size stays within expected bounds relative to checkpoint cadence".to_owned(),
            class: InvariantClass::Soft,
            mvcc_invariant_refs: vec![],
            severity: 1,
        },
        HistoryInvariant {
            id: "SOAK-INV-007".to_owned(),
            name: "version_chain_bounded".to_owned(),
            description: "Version chain lengths stay bounded; GC reclaims old versions".to_owned(),
            class: InvariantClass::Soft,
            mvcc_invariant_refs: vec!["INV-3".to_owned()],
            severity: 1,
        },
        HistoryInvariant {
            id: "SOAK-INV-008".to_owned(),
            name: "lock_table_bounded".to_owned(),
            description: "Page lock table size stays proportional to active transactions".to_owned(),
            class: InvariantClass::Soft,
            mvcc_invariant_refs: vec!["INV-2".to_owned()],
            severity: 1,
        },
        HistoryInvariant {
            id: "SOAK-INV-009".to_owned(),
            name: "no_phantom_rows".to_owned(),
            description: "Range scans under SSI do not observe phantom inserts from concurrent transactions".to_owned(),
            class: InvariantClass::Hard,
            mvcc_invariant_refs: vec![],
            severity: 0,
        },
        HistoryInvariant {
            id: "SOAK-INV-010".to_owned(),
            name: "ssi_false_positive_rate".to_owned(),
            description: "SSI abort rate stays below configured threshold (statistical quality)".to_owned(),
            class: InvariantClass::Statistical,
            mvcc_invariant_refs: vec!["INV-SSI-FP".to_owned()],
            severity: 2,
        },
        HistoryInvariant {
            id: "SOAK-INV-011".to_owned(),
            name: "memory_bounded".to_owned(),
            description: "Heap allocations stay bounded; no monotonic growth indicating leaks".to_owned(),
            class: InvariantClass::Statistical,
            mvcc_invariant_refs: vec![],
            severity: 1,
        },
        HistoryInvariant {
            id: "SOAK-INV-012".to_owned(),
            name: "latency_stability".to_owned(),
            description: "P99 transaction latency stays within 10x of initial P99 (no degradation)".to_owned(),
            class: InvariantClass::Statistical,
            mvcc_invariant_refs: vec![],
            severity: 2,
        },
    ]
}

// ---------------------------------------------------------------------------
// Invariant violation
// ---------------------------------------------------------------------------

/// A violation detected during a soak checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantViolation {
    /// Which invariant was violated.
    pub invariant_id: String,

    /// Transaction count at which the violation was detected.
    pub at_transaction: u64,

    /// Wall-clock elapsed seconds at detection.
    pub at_elapsed_secs: f64,

    /// Human-readable description of the violation.
    pub description: String,

    /// Observed value (for numerical invariants).
    pub observed: Option<String>,

    /// Expected bound (for numerical invariants).
    pub expected_bound: Option<String>,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] at txn={}: {}",
            self.invariant_id, self.at_transaction, self.description
        )
    }
}

// ---------------------------------------------------------------------------
// Checkpoint snapshot
// ---------------------------------------------------------------------------

/// A snapshot of system state captured at an invariant check point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointSnapshot {
    /// Transaction count when snapshot was taken.
    pub transaction_count: u64,

    /// Wall-clock elapsed seconds.
    pub elapsed_secs: f64,

    /// Highest TxnId observed.
    pub max_txn_id: u64,

    /// Highest CommitSeq observed.
    pub max_commit_seq: u64,

    /// Number of active (uncommitted) transactions.
    pub active_transactions: u32,

    /// WAL size in pages.
    pub wal_pages: u64,

    /// Maximum version chain length observed.
    pub max_version_chain_len: u32,

    /// Page lock table size.
    pub lock_table_size: u32,

    /// Approximate heap usage in bytes.
    pub heap_bytes: u64,

    /// P99 transaction latency in microseconds.
    pub p99_latency_us: u64,

    /// SSI abort count since last checkpoint.
    pub ssi_aborts_since_last: u64,

    /// Total committed transactions since last checkpoint.
    pub commits_since_last: u64,
}

// ---------------------------------------------------------------------------
// Invariant evaluation
// ---------------------------------------------------------------------------

/// Result of evaluating all invariants at a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantCheckResult {
    /// The checkpoint snapshot.
    pub snapshot: CheckpointSnapshot,

    /// Violations detected (may be empty).
    pub violations: Vec<InvariantViolation>,

    /// Whether any critical (severity=0) invariant was violated.
    pub has_critical_violation: bool,

    /// Number of invariants checked.
    pub invariants_checked: usize,

    /// Number of invariants that passed.
    pub invariants_passed: usize,
}

/// Evaluate invariants against a sequence of checkpoint snapshots.
///
/// This function compares the current snapshot to the previous one
/// to detect monotonicity violations and drift.
#[must_use]
pub fn evaluate_invariants(
    invariants: &[HistoryInvariant],
    current: &CheckpointSnapshot,
    previous: Option<&CheckpointSnapshot>,
) -> InvariantCheckResult {
    let mut violations = Vec::new();

    for inv in invariants {
        match inv.id.as_str() {
            "SOAK-INV-001" => check_monotone_counters(inv, current, previous, &mut violations),
            "SOAK-INV-006" => check_wal_bounded(inv, current, &mut violations),
            "SOAK-INV-007" => check_version_chain_bounded(inv, current, &mut violations),
            "SOAK-INV-008" => check_lock_table_bounded(inv, current, &mut violations),
            "SOAK-INV-010" => check_ssi_fp_rate(inv, current, &mut violations),
            "SOAK-INV-011" => check_memory_bounded(inv, current, previous, &mut violations),
            "SOAK-INV-012" => check_latency_stability(inv, current, previous, &mut violations),
            // Invariants 002-005, 009 require execution-level checking (not snapshot-only).
            // They are evaluated by the executor (bd-mblr.7.2.2), not here.
            _ => {}
        }
    }

    let has_critical = violations.iter().any(|v| {
        invariants
            .iter()
            .find(|inv| inv.id == v.invariant_id)
            .is_some_and(HistoryInvariant::is_abort_on_violation)
    });

    let checked = invariants.len();
    let violated_ids: std::collections::BTreeSet<&str> =
        violations.iter().map(|v| v.invariant_id.as_str()).collect();
    let passed = checked - violated_ids.len();

    InvariantCheckResult {
        snapshot: current.clone(),
        violations,
        has_critical_violation: has_critical,
        invariants_checked: checked,
        invariants_passed: passed,
    }
}

// ---------------------------------------------------------------------------
// Individual invariant checks
// ---------------------------------------------------------------------------

/// Maximum version chain length before flagging.
const MAX_VERSION_CHAIN_LEN: u32 = 10_000;

/// Maximum lock table size relative to active transactions.
const LOCK_TABLE_RATIO_LIMIT: u32 = 100;

/// Maximum SSI false-positive rate.
const SSI_FP_RATE_LIMIT: f64 = 0.10;

/// Maximum heap growth factor between checkpoints.
const HEAP_GROWTH_FACTOR_LIMIT: f64 = 2.0;

/// Maximum latency degradation factor.
const LATENCY_DEGRADATION_LIMIT: f64 = 10.0;

/// Maximum WAL pages before warning.
const WAL_PAGES_LIMIT: u64 = 100_000;

fn check_monotone_counters(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    previous: Option<&CheckpointSnapshot>,
    violations: &mut Vec<InvariantViolation>,
) {
    if let Some(prev) = previous {
        if current.max_txn_id < prev.max_txn_id {
            violations.push(InvariantViolation {
                invariant_id: inv.id.clone(),
                at_transaction: current.transaction_count,
                at_elapsed_secs: current.elapsed_secs,
                description: format!(
                    "TxnId regressed: {} -> {}",
                    prev.max_txn_id, current.max_txn_id
                ),
                observed: Some(current.max_txn_id.to_string()),
                expected_bound: Some(format!(">= {}", prev.max_txn_id)),
            });
        }
        if current.max_commit_seq < prev.max_commit_seq {
            violations.push(InvariantViolation {
                invariant_id: inv.id.clone(),
                at_transaction: current.transaction_count,
                at_elapsed_secs: current.elapsed_secs,
                description: format!(
                    "CommitSeq regressed: {} -> {}",
                    prev.max_commit_seq, current.max_commit_seq
                ),
                observed: Some(current.max_commit_seq.to_string()),
                expected_bound: Some(format!(">= {}", prev.max_commit_seq)),
            });
        }
    }
}

fn check_wal_bounded(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    violations: &mut Vec<InvariantViolation>,
) {
    if current.wal_pages > WAL_PAGES_LIMIT {
        violations.push(InvariantViolation {
            invariant_id: inv.id.clone(),
            at_transaction: current.transaction_count,
            at_elapsed_secs: current.elapsed_secs,
            description: format!(
                "WAL size {} pages exceeds limit {}",
                current.wal_pages, WAL_PAGES_LIMIT
            ),
            observed: Some(current.wal_pages.to_string()),
            expected_bound: Some(format!("<= {WAL_PAGES_LIMIT}")),
        });
    }
}

fn check_version_chain_bounded(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    violations: &mut Vec<InvariantViolation>,
) {
    if current.max_version_chain_len > MAX_VERSION_CHAIN_LEN {
        violations.push(InvariantViolation {
            invariant_id: inv.id.clone(),
            at_transaction: current.transaction_count,
            at_elapsed_secs: current.elapsed_secs,
            description: format!(
                "version chain length {} exceeds limit {}",
                current.max_version_chain_len, MAX_VERSION_CHAIN_LEN
            ),
            observed: Some(current.max_version_chain_len.to_string()),
            expected_bound: Some(format!("<= {MAX_VERSION_CHAIN_LEN}")),
        });
    }
}

fn check_lock_table_bounded(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    violations: &mut Vec<InvariantViolation>,
) {
    let limit = current
        .active_transactions
        .saturating_mul(LOCK_TABLE_RATIO_LIMIT);
    if current.lock_table_size > limit && current.active_transactions > 0 {
        violations.push(InvariantViolation {
            invariant_id: inv.id.clone(),
            at_transaction: current.transaction_count,
            at_elapsed_secs: current.elapsed_secs,
            description: format!(
                "lock table size {} exceeds {}x active transactions ({})",
                current.lock_table_size, LOCK_TABLE_RATIO_LIMIT, current.active_transactions
            ),
            observed: Some(current.lock_table_size.to_string()),
            expected_bound: Some(format!("<= {limit}")),
        });
    }
}

fn check_ssi_fp_rate(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    violations: &mut Vec<InvariantViolation>,
) {
    if current.commits_since_last == 0 {
        return;
    }
    let total = current.ssi_aborts_since_last + current.commits_since_last;
    #[allow(clippy::cast_precision_loss)]
    let fp_rate = current.ssi_aborts_since_last as f64 / total as f64;
    if fp_rate > SSI_FP_RATE_LIMIT {
        violations.push(InvariantViolation {
            invariant_id: inv.id.clone(),
            at_transaction: current.transaction_count,
            at_elapsed_secs: current.elapsed_secs,
            description: format!(
                "SSI false-positive rate {fp_rate:.4} exceeds limit {SSI_FP_RATE_LIMIT}"
            ),
            observed: Some(format!("{fp_rate:.4}")),
            expected_bound: Some(format!("<= {SSI_FP_RATE_LIMIT}")),
        });
    }
}

fn check_memory_bounded(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    previous: Option<&CheckpointSnapshot>,
    violations: &mut Vec<InvariantViolation>,
) {
    if let Some(prev) = previous {
        if prev.heap_bytes > 0 {
            #[allow(clippy::cast_precision_loss)]
            let growth = current.heap_bytes as f64 / prev.heap_bytes as f64;
            if growth > HEAP_GROWTH_FACTOR_LIMIT {
                violations.push(InvariantViolation {
                    invariant_id: inv.id.clone(),
                    at_transaction: current.transaction_count,
                    at_elapsed_secs: current.elapsed_secs,
                    description: format!(
                        "heap grew {growth:.2}x between checkpoints ({}->{})",
                        prev.heap_bytes, current.heap_bytes
                    ),
                    observed: Some(format!("{growth:.2}x")),
                    expected_bound: Some(format!("<= {HEAP_GROWTH_FACTOR_LIMIT}x")),
                });
            }
        }
    }
}

fn check_latency_stability(
    inv: &HistoryInvariant,
    current: &CheckpointSnapshot,
    previous: Option<&CheckpointSnapshot>,
    violations: &mut Vec<InvariantViolation>,
) {
    if let Some(prev) = previous {
        if prev.p99_latency_us > 0 {
            #[allow(clippy::cast_precision_loss)]
            let degradation = current.p99_latency_us as f64 / prev.p99_latency_us as f64;
            if degradation > LATENCY_DEGRADATION_LIMIT {
                violations.push(InvariantViolation {
                    invariant_id: inv.id.clone(),
                    at_transaction: current.transaction_count,
                    at_elapsed_secs: current.elapsed_secs,
                    description: format!(
                        "P99 latency degraded {degradation:.1}x ({}us->{}us)",
                        prev.p99_latency_us, current.p99_latency_us
                    ),
                    observed: Some(format!("{degradation:.1}x")),
                    expected_bound: Some(format!("<= {LATENCY_DEGRADATION_LIMIT}x")),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Soak workload spec
// ---------------------------------------------------------------------------

/// A complete soak workload specification tying profiles to invariants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakWorkloadSpec {
    /// Root seed for deterministic derivation.
    pub root_seed: u64,

    /// The soak profile to execute.
    pub profile: SoakProfile,

    /// History invariants to check during the run.
    pub invariants: Vec<HistoryInvariant>,

    /// Derived seed for this specific run.
    pub run_seed: u64,
}

impl SoakWorkloadSpec {
    /// Build a spec from a profile, using canonical invariants.
    #[must_use]
    pub fn from_profile(profile: SoakProfile, root_seed: u64) -> Self {
        let run_seed = profile.derive_seed(root_seed);
        Self {
            root_seed,
            profile,
            invariants: canonical_invariants(),
            run_seed,
        }
    }

    /// Validate the spec.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = self.profile.validate();

        if self.invariants.is_empty() {
            errors.push("no invariants defined".to_owned());
        }

        let ids: std::collections::BTreeSet<&str> =
            self.invariants.iter().map(|inv| inv.id.as_str()).collect();
        if ids.len() != self.invariants.len() {
            errors.push("duplicate invariant IDs".to_owned());
        }

        // Must have at least one critical invariant.
        let has_critical = self.invariants.iter().any(|inv| inv.severity == 0);
        if !has_critical {
            errors.push("no critical (severity=0) invariants defined".to_owned());
        }

        errors
    }

    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Coverage metrics
// ---------------------------------------------------------------------------

/// Coverage report for soak profiles and invariants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakCoverage {
    /// Number of profiles available.
    pub profile_count: usize,

    /// Number of invariants in the catalog.
    pub invariant_count: usize,

    /// Invariants by class.
    pub by_class: BTreeMap<String, usize>,

    /// Invariants by severity.
    pub by_severity: BTreeMap<u8, usize>,

    /// MVCC invariant references covered.
    pub mvcc_refs_covered: Vec<String>,

    /// Profile names.
    pub profile_names: Vec<String>,
}

/// Compute coverage metrics for a set of profiles and invariants.
#[must_use]
pub fn compute_soak_coverage(
    profiles: &[SoakProfile],
    invariants: &[HistoryInvariant],
) -> SoakCoverage {
    let mut by_class: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_severity: BTreeMap<u8, usize> = BTreeMap::new();
    let mut mvcc_refs: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for inv in invariants {
        *by_class.entry(format!("{:?}", inv.class)).or_insert(0) += 1;
        *by_severity.entry(inv.severity).or_insert(0) += 1;
        for r in &inv.mvcc_invariant_refs {
            mvcc_refs.insert(r.clone());
        }
    }

    SoakCoverage {
        profile_count: profiles.len(),
        invariant_count: invariants.len(),
        by_class,
        by_severity,
        mvcc_refs_covered: mvcc_refs.into_iter().collect(),
        profile_names: profiles.iter().map(|p| p.name.clone()).collect(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Profile construction and validation ---

    #[test]
    fn test_all_presets_valid() {
        for profile in all_presets() {
            let errors = profile.validate();
            assert!(
                errors.is_empty(),
                "profile '{}' has validation errors: {:?}",
                profile.name,
                errors
            );
        }
    }

    #[test]
    fn test_preset_names_unique() {
        let profiles = all_presets();
        let names: std::collections::BTreeSet<&str> =
            profiles.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names.len(), profiles.len(), "duplicate preset names");
    }

    #[test]
    fn test_preset_count() {
        assert_eq!(all_presets().len(), 4);
    }

    #[test]
    fn test_light_profile() {
        let p = profile_light();
        assert_eq!(p.name, "light");
        assert_eq!(p.target_transactions, 1_000);
        assert_eq!(p.concurrency.connections, 1);
        assert_eq!(p.schema_churn, SchemaChurnRate::None);
        assert!(!p.fault_injection_enabled);
    }

    #[test]
    fn test_stress_profile() {
        let p = profile_stress();
        assert_eq!(p.name, "stress");
        assert_eq!(p.target_transactions, 500_000);
        assert_eq!(p.concurrency.connections, 64);
        assert_eq!(p.schema_churn, SchemaChurnRate::High);
        assert!(p.fault_injection_enabled);
    }

    #[test]
    fn test_profile_display() {
        let p = profile_light();
        let s = p.to_string();
        assert!(s.contains("light"), "display should contain name: {s}");
        assert!(
            s.contains("R90:W10"),
            "display should contain contention: {s}"
        );
    }

    #[test]
    fn test_invalid_profile_zero_transactions() {
        let mut p = profile_light();
        p.target_transactions = 0;
        let errors = p.validate();
        assert!(
            errors.iter().any(|e| e.contains("target_transactions")),
            "should catch zero transactions: {errors:?}"
        );
    }

    #[test]
    fn test_invalid_profile_zero_duration() {
        let mut p = profile_light();
        p.max_duration_secs = 0;
        let errors = p.validate();
        assert!(
            errors.iter().any(|e| e.contains("max_duration_secs")),
            "should catch zero duration: {errors:?}"
        );
    }

    #[test]
    fn test_invalid_profile_check_interval_exceeds_target() {
        let mut p = profile_light();
        p.invariant_check_interval = p.target_transactions + 1;
        let errors = p.validate();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("invariant_check_interval")),
            "should catch interval > target: {errors:?}"
        );
    }

    // --- Contention mix ---

    #[test]
    fn test_contention_mix_normalised() {
        let m = ContentionMix::new(30, 70);
        assert_eq!(m.reader_pct + m.writer_pct, 100);
    }

    #[test]
    fn test_contention_mix_zero_clamp() {
        let m = ContentionMix::new(0, 0);
        assert_eq!(m.reader_pct, 50);
        assert_eq!(m.writer_pct, 50);
    }

    #[test]
    fn test_contention_mix_presets() {
        let r = ContentionMix::read_heavy();
        assert_eq!(r.reader_pct, 90);
        let b = ContentionMix::balanced();
        assert_eq!(b.reader_pct, 50);
        let w = ContentionMix::write_heavy();
        assert_eq!(w.reader_pct, 20);
    }

    #[test]
    fn test_contention_mix_display() {
        let m = ContentionMix::balanced();
        assert_eq!(m.to_string(), "R50:W50");
    }

    // --- Schema churn ---

    #[test]
    fn test_schema_churn_intervals() {
        assert!(SchemaChurnRate::None.interval_hint().is_none());
        assert_eq!(SchemaChurnRate::Low.interval_hint(), Some(1000));
        assert_eq!(SchemaChurnRate::Medium.interval_hint(), Some(100));
        assert_eq!(SchemaChurnRate::High.interval_hint(), Some(10));
    }

    // --- Checkpoint cadence ---

    #[test]
    fn test_checkpoint_cadence_intervals() {
        assert_eq!(CheckpointCadence::Aggressive.interval_hint(), Some(50));
        assert_eq!(CheckpointCadence::Normal.interval_hint(), Some(500));
        assert_eq!(CheckpointCadence::Deferred.interval_hint(), Some(5000));
        assert!(CheckpointCadence::Disabled.interval_hint().is_none());
    }

    // --- Concurrency level ---

    #[test]
    fn test_concurrency_levels() {
        assert_eq!(ConcurrencyLevel::sequential().connections, 1);
        assert_eq!(ConcurrencyLevel::light().connections, 4);
        assert_eq!(ConcurrencyLevel::moderate().connections, 16);
        assert_eq!(ConcurrencyLevel::heavy().connections, 64);
    }

    // --- History invariants ---

    #[test]
    fn test_canonical_invariants_non_empty() {
        let invs = canonical_invariants();
        assert!(
            invs.len() >= 10,
            "expected >= 10 invariants, got {}",
            invs.len()
        );
    }

    #[test]
    fn test_canonical_invariant_ids_unique() {
        let invs = canonical_invariants();
        let ids: std::collections::BTreeSet<&str> =
            invs.iter().map(|inv| inv.id.as_str()).collect();
        assert_eq!(ids.len(), invs.len(), "duplicate invariant IDs");
    }

    #[test]
    fn test_canonical_invariants_have_critical() {
        let invs = canonical_invariants();
        let critical = invs.iter().filter(|inv| inv.severity == 0).count();
        assert!(
            critical >= 3,
            "expected >= 3 critical invariants, got {critical}"
        );
    }

    #[test]
    fn test_invariant_abort_on_critical() {
        let inv = &canonical_invariants()[0]; // monotone_counters, severity=0
        assert!(inv.is_abort_on_violation());

        // Find a non-critical one.
        let non_critical = canonical_invariants()
            .into_iter()
            .find(|inv| inv.severity > 0)
            .expect("should have non-critical invariant");
        assert!(!non_critical.is_abort_on_violation());
    }

    #[test]
    fn test_invariant_classes_represented() {
        let invs = canonical_invariants();
        let classes: std::collections::BTreeSet<InvariantClass> =
            invs.iter().map(|inv| inv.class).collect();
        assert!(classes.contains(&InvariantClass::Hard));
        assert!(classes.contains(&InvariantClass::Soft));
        assert!(classes.contains(&InvariantClass::Statistical));
    }

    #[test]
    fn test_invariant_mvcc_coverage() {
        let invs = canonical_invariants();
        let mvcc_refs: std::collections::BTreeSet<&str> = invs
            .iter()
            .flat_map(|inv| inv.mvcc_invariant_refs.iter().map(String::as_str))
            .collect();
        // Must cover at least INV-1, INV-2, INV-3, INV-5, INV-6, INV-7
        for expected in &["INV-1", "INV-2", "INV-3", "INV-5", "INV-6", "INV-7"] {
            assert!(mvcc_refs.contains(expected), "missing MVCC ref: {expected}");
        }
    }

    // --- Invariant evaluation ---

    fn make_healthy_snapshot(txn_count: u64) -> CheckpointSnapshot {
        CheckpointSnapshot {
            transaction_count: txn_count,
            elapsed_secs: txn_count as f64 * 0.001,
            max_txn_id: txn_count * 2,
            max_commit_seq: txn_count,
            active_transactions: 4,
            wal_pages: 500,
            max_version_chain_len: 50,
            lock_table_size: 10,
            heap_bytes: 1_000_000,
            p99_latency_us: 100,
            ssi_aborts_since_last: 1,
            commits_since_last: 100,
        }
    }

    #[test]
    fn test_evaluate_invariants_all_pass() {
        let invs = canonical_invariants();
        let snap1 = make_healthy_snapshot(1000);
        let snap2 = make_healthy_snapshot(2000);
        let result = evaluate_invariants(&invs, &snap2, Some(&snap1));

        assert!(!result.has_critical_violation);
        assert!(
            result.violations.is_empty(),
            "expected no violations: {:?}",
            result.violations
        );
        assert_eq!(result.invariants_checked, invs.len());
        assert_eq!(result.invariants_passed, invs.len());
    }

    #[test]
    fn test_evaluate_first_snapshot_no_previous() {
        let invs = canonical_invariants();
        let snap = make_healthy_snapshot(1000);
        let result = evaluate_invariants(&invs, &snap, None);

        assert!(!result.has_critical_violation);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_detect_txn_id_regression() {
        let invs = canonical_invariants();
        let snap1 = make_healthy_snapshot(2000);
        let mut snap2 = make_healthy_snapshot(3000);
        snap2.max_txn_id = 100; // Regression!

        let result = evaluate_invariants(&invs, &snap2, Some(&snap1));
        assert!(
            result.has_critical_violation,
            "TxnId regression is critical"
        );
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-001"),
            "should flag SOAK-INV-001: {:?}",
            result.violations
        );
    }

    #[test]
    fn test_detect_commit_seq_regression() {
        let invs = canonical_invariants();
        let snap1 = make_healthy_snapshot(2000);
        let mut snap2 = make_healthy_snapshot(3000);
        snap2.max_commit_seq = 1; // Regression!

        let result = evaluate_invariants(&invs, &snap2, Some(&snap1));
        assert!(result.has_critical_violation);
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-001"),
        );
    }

    #[test]
    fn test_detect_wal_overflow() {
        let invs = canonical_invariants();
        let mut snap = make_healthy_snapshot(1000);
        snap.wal_pages = WAL_PAGES_LIMIT + 1;

        let result = evaluate_invariants(&invs, &snap, None);
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-006"),
            "should flag WAL overflow: {:?}",
            result.violations
        );
    }

    #[test]
    fn test_detect_version_chain_overflow() {
        let invs = canonical_invariants();
        let mut snap = make_healthy_snapshot(1000);
        snap.max_version_chain_len = MAX_VERSION_CHAIN_LEN + 1;

        let result = evaluate_invariants(&invs, &snap, None);
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-007"),
        );
    }

    #[test]
    fn test_detect_lock_table_overflow() {
        let invs = canonical_invariants();
        let mut snap = make_healthy_snapshot(1000);
        snap.active_transactions = 2;
        snap.lock_table_size = LOCK_TABLE_RATIO_LIMIT * 2 + 1;

        let result = evaluate_invariants(&invs, &snap, None);
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-008"),
        );
    }

    #[test]
    fn test_detect_ssi_fp_rate() {
        let invs = canonical_invariants();
        let mut snap = make_healthy_snapshot(1000);
        snap.ssi_aborts_since_last = 50;
        snap.commits_since_last = 50; // 50% abort rate

        let result = evaluate_invariants(&invs, &snap, None);
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-010"),
        );
    }

    #[test]
    fn test_detect_memory_growth() {
        let invs = canonical_invariants();
        let snap1 = make_healthy_snapshot(1000);
        let mut snap2 = make_healthy_snapshot(2000);
        snap2.heap_bytes = snap1.heap_bytes * 3; // 3x growth

        let result = evaluate_invariants(&invs, &snap2, Some(&snap1));
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-011"),
        );
    }

    #[test]
    fn test_detect_latency_degradation() {
        let invs = canonical_invariants();
        let snap1 = make_healthy_snapshot(1000);
        let mut snap2 = make_healthy_snapshot(2000);
        snap2.p99_latency_us = snap1.p99_latency_us * 20; // 20x degradation

        let result = evaluate_invariants(&invs, &snap2, Some(&snap1));
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.invariant_id == "SOAK-INV-012"),
        );
    }

    // --- Deterministic seeds ---

    #[test]
    fn test_profile_seed_deterministic() {
        let p = profile_light();
        let s1 = p.derive_seed(42);
        let s2 = p.derive_seed(42);
        assert_eq!(s1, s2, "same root seed should produce same result");
    }

    #[test]
    fn test_profile_seed_varies_with_root() {
        let p = profile_light();
        let s1 = p.derive_seed(42);
        let s2 = p.derive_seed(99);
        assert_ne!(
            s1, s2,
            "different root seeds should produce different results"
        );
    }

    #[test]
    fn test_profile_seed_varies_with_name() {
        let s1 = profile_light().derive_seed(42);
        let s2 = profile_heavy().derive_seed(42);
        assert_ne!(s1, s2, "different profiles should produce different seeds");
    }

    // --- Soak workload spec ---

    #[test]
    fn test_spec_from_profile_valid() {
        let spec = SoakWorkloadSpec::from_profile(profile_moderate(), 42);
        let errors = spec.validate();
        assert!(errors.is_empty(), "spec should be valid: {errors:?}");
        assert!(!spec.invariants.is_empty());
        assert_ne!(spec.run_seed, 0);
    }

    #[test]
    fn test_spec_json_roundtrip() {
        let spec = SoakWorkloadSpec::from_profile(profile_light(), 42);
        let json = spec.to_json().expect("serialize");
        let restored = SoakWorkloadSpec::from_json(&json).expect("deserialize");

        assert_eq!(restored.root_seed, spec.root_seed);
        assert_eq!(restored.run_seed, spec.run_seed);
        assert_eq!(restored.profile.name, spec.profile.name);
        assert_eq!(restored.invariants.len(), spec.invariants.len());
    }

    #[test]
    fn test_spec_deterministic() {
        let spec1 = SoakWorkloadSpec::from_profile(profile_heavy(), 42);
        let spec2 = SoakWorkloadSpec::from_profile(profile_heavy(), 42);
        assert_eq!(spec1.run_seed, spec2.run_seed);
    }

    // --- Coverage ---

    #[test]
    fn test_soak_coverage_complete() {
        let profiles = all_presets();
        let invs = canonical_invariants();
        let cov = compute_soak_coverage(&profiles, &invs);

        assert_eq!(cov.profile_count, 4);
        assert_eq!(cov.invariant_count, invs.len());
        assert!(
            cov.by_class.len() >= 3,
            "should have Hard, Soft, Statistical"
        );
        assert!(!cov.mvcc_refs_covered.is_empty());
        assert_eq!(cov.profile_names.len(), 4);
    }

    // --- Violation display ---

    #[test]
    fn test_violation_display() {
        let v = InvariantViolation {
            invariant_id: "SOAK-INV-001".to_owned(),
            at_transaction: 5000,
            at_elapsed_secs: 2.5,
            description: "TxnId regressed".to_owned(),
            observed: Some("100".to_owned()),
            expected_bound: Some(">= 5000".to_owned()),
        };
        let s = v.to_string();
        assert!(s.contains("SOAK-INV-001"));
        assert!(s.contains("5000"));
        assert!(s.contains("TxnId regressed"));
    }
}
