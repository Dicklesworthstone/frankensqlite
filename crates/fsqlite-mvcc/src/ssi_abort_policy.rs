//! Decision-theoretic SSI abort policy: victim selection + loss minimization (§5.7.3).
//!
//! Provides the Bayesian decision framework for WHEN and WHOM to abort when a
//! dangerous structure is detected, plus continuous monitoring via e-process and
//! conformal calibration.

use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite_types::{CommitSeq, PageNumber, TxnToken};

// ---------------------------------------------------------------------------
// Loss matrix (§5.7.3 Bayesian Decision Framework)
// ---------------------------------------------------------------------------

/// Loss parameters for the SSI abort decision.
///
/// `L_miss` = cost of letting an anomaly through (data corruption risk).
/// `L_fp`   = cost of a false-positive abort (wasted work, retry).
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)] // f64 does not impl Eq
pub struct LossMatrix {
    /// Cost of a missed anomaly (default: 1000).
    pub l_miss: f64,
    /// Cost of a false-positive abort (default: 1).
    pub l_fp: f64,
}

impl Default for LossMatrix {
    fn default() -> Self {
        Self {
            l_miss: 1000.0,
            l_fp: 1.0,
        }
    }
}

impl LossMatrix {
    /// Compute the abort threshold: P(anomaly) > threshold ⟹ abort.
    ///
    /// `threshold = L_fp / (L_fp + L_miss)`
    #[must_use]
    pub fn abort_threshold(&self) -> f64 {
        self.l_fp / (self.l_fp + self.l_miss)
    }

    /// Expected loss of committing given P(anomaly).
    #[must_use]
    pub fn expected_loss_commit(&self, p_anomaly: f64) -> f64 {
        p_anomaly * self.l_miss
    }

    /// Expected loss of aborting given P(anomaly).
    #[must_use]
    pub fn expected_loss_abort(&self, p_anomaly: f64) -> f64 {
        (1.0 - p_anomaly) * self.l_fp
    }

    /// Should we abort? Returns true if `E[Loss|commit] > E[Loss|abort]`.
    #[must_use]
    pub fn should_abort(&self, p_anomaly: f64) -> bool {
        p_anomaly > self.abort_threshold()
    }
}

// ---------------------------------------------------------------------------
// Transaction cost estimation
// ---------------------------------------------------------------------------

/// Approximation of `L(T)` = cost of aborting a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnCost {
    /// Number of pages in the write set.
    pub write_set_size: u32,
    /// Duration in microseconds.
    pub duration_us: u64,
}

impl TxnCost {
    /// Combined cost metric: write_set_size + duration_us/1000.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn loss(&self) -> f64 {
        f64::from(self.write_set_size) + (self.duration_us as f64) / 1000.0
    }
}

// ---------------------------------------------------------------------------
// Victim selection (§5.7.3 Policy)
// ---------------------------------------------------------------------------

/// Cycle status for a dangerous structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleStatus {
    /// T1 and T3 both committed — confirmed anomaly.
    Confirmed,
    /// Only one end committed — potential anomaly.
    Potential,
}

/// Which transaction to abort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Victim {
    /// Abort T2 (the pivot).
    Pivot,
    /// Abort T3 (the other active participant).
    Other,
}

/// Result of a victim selection decision.
#[derive(Debug, Clone)]
pub struct VictimDecision {
    pub victim: Victim,
    pub cycle_status: CycleStatus,
    pub pivot_cost: f64,
    pub other_cost: f64,
    pub reason: &'static str,
}

impl fmt::Display for VictimDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "victim={:?} cycle={:?} pivot_cost={:.1} other_cost={:.1} reason={}",
            self.victim, self.cycle_status, self.pivot_cost, self.other_cost, self.reason
        )
    }
}

/// Select which transaction to abort in a dangerous structure.
///
/// # Policy
///
/// 1. **Confirmed cycle (T1, T3 both committed):** MUST abort T2 (pivot).
///    Safety is mandatory.
/// 2. **Potential cycle:** Compare costs and abort the cheaper participant.
///    On ties, default to aborting pivot for deterministic behavior.
#[must_use]
pub fn select_victim(
    status: CycleStatus,
    pivot_cost: TxnCost,
    other_cost: TxnCost,
) -> VictimDecision {
    let pivot_l = pivot_cost.loss();
    let other_l = other_cost.loss();

    match status {
        CycleStatus::Confirmed => {
            // Safety first: MUST abort pivot. No choice.
            VictimDecision {
                victim: Victim::Pivot,
                cycle_status: status,
                pivot_cost: pivot_l,
                other_cost: other_l,
                reason: "confirmed_cycle_must_abort_pivot",
            }
        }
        CycleStatus::Potential => {
            // Optimize for retry cost: abort the cheaper participant.
            if pivot_l < other_l {
                VictimDecision {
                    victim: Victim::Pivot,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_abort_cheaper_pivot",
                }
            } else if other_l < pivot_l {
                VictimDecision {
                    victim: Victim::Other,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_abort_cheaper_other",
                }
            } else {
                // Tie-breaker for deterministic behavior.
                VictimDecision {
                    victim: Victim::Pivot,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_tie_abort_pivot",
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SSI abort decision envelope (auditable logging)
// ---------------------------------------------------------------------------

/// Full audit record for an SSI abort/commit decision.
#[derive(Debug, Clone)]
pub struct AbortDecisionEnvelope {
    pub has_in_rw: bool,
    pub has_out_rw: bool,
    pub p_anomaly: f64,
    pub loss_matrix: LossMatrix,
    pub threshold: f64,
    pub expected_loss_commit: f64,
    pub expected_loss_abort: f64,
    pub decision: AbortDecision,
    pub victim: Option<VictimDecision>,
}

/// The binary decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortDecision {
    Commit,
    Abort,
}

impl AbortDecisionEnvelope {
    /// Build an envelope from evidence.
    #[must_use]
    pub fn evaluate(
        has_in_rw: bool,
        has_out_rw: bool,
        p_anomaly: f64,
        loss_matrix: LossMatrix,
        victim: Option<VictimDecision>,
    ) -> Self {
        let threshold = loss_matrix.abort_threshold();
        let el_commit = loss_matrix.expected_loss_commit(p_anomaly);
        let el_abort = loss_matrix.expected_loss_abort(p_anomaly);
        let decision = if has_in_rw && has_out_rw && loss_matrix.should_abort(p_anomaly) {
            AbortDecision::Abort
        } else {
            AbortDecision::Commit
        };
        Self {
            has_in_rw,
            has_out_rw,
            p_anomaly,
            loss_matrix,
            threshold,
            expected_loss_commit: el_commit,
            expected_loss_abort: el_abort,
            decision,
            victim,
        }
    }
}

// ---------------------------------------------------------------------------
// SSI Evidence Ledger (galaxy-brain decision cards)
// ---------------------------------------------------------------------------

/// Decision type emitted by SSI commit-time validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SsiDecisionType {
    CommitAllowed,
    AbortWriteSkew,
    AbortPhantom,
    AbortCycle,
}

impl SsiDecisionType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitAllowed => "COMMIT_ALLOWED",
            Self::AbortWriteSkew => "ABORT_WRITE_SKEW",
            Self::AbortPhantom => "ABORT_PHANTOM",
            Self::AbortCycle => "ABORT_CYCLE",
        }
    }
}

impl fmt::Display for SsiDecisionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SsiDecisionType {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "COMMIT_ALLOWED" => Ok(Self::CommitAllowed),
            "ABORT_WRITE_SKEW" => Ok(Self::AbortWriteSkew),
            "ABORT_PHANTOM" => Ok(Self::AbortPhantom),
            "ABORT_CYCLE" => Ok(Self::AbortCycle),
            _ => Err("unrecognized SSI decision type"),
        }
    }
}

/// Compact read-set summary stored in each galaxy-brain card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiReadSetSummary {
    pub page_count: usize,
    pub top_k_pages: Vec<PageNumber>,
    pub bloom_fingerprint: u64,
}

impl SsiReadSetSummary {
    #[must_use]
    pub fn from_pages(read_set_pages: &[PageNumber], top_k: usize) -> Self {
        let mut sorted = read_set_pages.to_vec();
        sorted.sort_by_key(|page| page.get());
        sorted.dedup();
        let top_k_pages = sorted.iter().copied().take(top_k.max(1)).collect();
        Self {
            page_count: sorted.len(),
            top_k_pages,
            bloom_fingerprint: read_set_fingerprint(&sorted),
        }
    }
}

/// Card payload before chain hash / epoch assignment.
#[derive(Debug, Clone)]
pub struct SsiDecisionCardDraft {
    pub decision_type: SsiDecisionType,
    pub txn: TxnToken,
    pub snapshot_seq: CommitSeq,
    pub commit_seq: Option<CommitSeq>,
    pub conflicting_txns: Vec<TxnToken>,
    pub conflict_pages: Vec<PageNumber>,
    pub read_set_pages: Vec<PageNumber>,
    pub write_set: Vec<PageNumber>,
    pub rationale: String,
    pub timestamp_unix_ns: u64,
}

impl SsiDecisionCardDraft {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        decision_type: SsiDecisionType,
        txn: TxnToken,
        snapshot_seq: CommitSeq,
        conflicting_txns: Vec<TxnToken>,
        conflict_pages: Vec<PageNumber>,
        read_set_pages: Vec<PageNumber>,
        write_set: Vec<PageNumber>,
        rationale: impl Into<String>,
    ) -> Self {
        Self {
            decision_type,
            txn,
            snapshot_seq,
            commit_seq: None,
            conflicting_txns,
            conflict_pages,
            read_set_pages,
            write_set,
            rationale: rationale.into(),
            timestamp_unix_ns: now_unix_ns(),
        }
    }

    #[must_use]
    pub const fn with_commit_seq(mut self, commit_seq: CommitSeq) -> Self {
        self.commit_seq = Some(commit_seq);
        self
    }

    #[must_use]
    pub const fn with_timestamp_unix_ns(mut self, timestamp_unix_ns: u64) -> Self {
        self.timestamp_unix_ns = timestamp_unix_ns;
        self
    }
}

/// Immutable append-only galaxy-brain card persisted by the evidence ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiDecisionCard {
    pub decision_type: SsiDecisionType,
    pub txn: TxnToken,
    pub snapshot_seq: CommitSeq,
    pub commit_seq: Option<CommitSeq>,
    pub conflicting_txns: Vec<TxnToken>,
    pub conflict_pages: Vec<PageNumber>,
    pub read_set_summary: SsiReadSetSummary,
    pub write_set: Vec<PageNumber>,
    pub rationale: String,
    pub timestamp_unix_ns: u64,
    pub decision_epoch: u64,
    pub chain_hash: [u8; 32],
}

impl SsiDecisionCard {
    #[must_use]
    pub fn chain_hash_hex(&self) -> String {
        hex_encode(self.chain_hash)
    }
}

/// Filter options for listing evidence cards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SsiDecisionQuery {
    pub txn_id: Option<u64>,
    pub decision_type: Option<SsiDecisionType>,
    pub timestamp_start_ns: Option<u64>,
    pub timestamp_end_ns: Option<u64>,
}

#[derive(Debug)]
struct SsiEvidenceLedgerState {
    capacity: usize,
    next_epoch: u64,
    chain_tip: [u8; 32],
    entries: VecDeque<SsiDecisionCard>,
}

impl SsiEvidenceLedgerState {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_epoch: 1,
            chain_tip: [0_u8; 32],
            entries: VecDeque::new(),
        }
    }

    fn append(&mut self, draft: SsiDecisionCardDraft) {
        let mut conflicting_txns = draft.conflicting_txns;
        conflicting_txns.sort_by(|left, right| {
            left.id
                .get()
                .cmp(&right.id.get())
                .then_with(|| left.epoch.get().cmp(&right.epoch.get()))
        });
        conflicting_txns.dedup();

        let mut conflict_pages = draft.conflict_pages;
        conflict_pages.sort_by_key(|page| page.get());
        conflict_pages.dedup();

        let mut write_set = draft.write_set;
        write_set.sort_by_key(|page| page.get());
        write_set.dedup();

        let mut read_set_pages = draft.read_set_pages;
        read_set_pages.sort_by_key(|page| page.get());
        read_set_pages.dedup();
        let read_set_summary = SsiReadSetSummary::from_pages(&read_set_pages, 8);

        let decision_epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.saturating_add(1);
        let chain_hash = compute_chain_hash(
            self.chain_tip,
            draft.decision_type,
            draft.txn,
            draft.snapshot_seq,
            draft.commit_seq,
            decision_epoch,
            draft.timestamp_unix_ns,
            &conflicting_txns,
            &conflict_pages,
            &read_set_pages,
            &write_set,
            &draft.rationale,
        );
        self.chain_tip = chain_hash;

        if self.entries.len() == self.capacity {
            let _ = self.entries.pop_front();
        }
        self.entries.push_back(SsiDecisionCard {
            decision_type: draft.decision_type,
            txn: draft.txn,
            snapshot_seq: draft.snapshot_seq,
            commit_seq: draft.commit_seq,
            conflicting_txns,
            conflict_pages,
            read_set_summary,
            write_set,
            rationale: draft.rationale,
            timestamp_unix_ns: draft.timestamp_unix_ns,
            decision_epoch,
            chain_hash,
        });
    }
}

/// Bounded append-only ledger for SSI decision cards.
#[derive(Debug)]
pub struct SsiEvidenceLedger {
    state: Arc<Mutex<SsiEvidenceLedgerState>>,
    pending: Arc<AtomicUsize>,
    tx: Option<mpsc::Sender<SsiDecisionCardDraft>>,
}

impl SsiEvidenceLedger {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let state = Arc::new(Mutex::new(SsiEvidenceLedgerState::new(capacity)));
        let pending = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        let worker_state = Arc::clone(&state);
        let worker_pending = Arc::clone(&pending);
        let worker = thread::Builder::new()
            .name("fsqlite-ssi-ledger".to_owned())
            .spawn(move || {
                while let Ok(draft) = rx.recv() {
                    with_locked_state(&worker_state, |inner| inner.append(draft));
                    let _ = worker_pending.fetch_sub(1, Ordering::AcqRel);
                }
            });

        let tx = if worker.is_ok() { Some(tx) } else { None };

        Self { state, pending, tx }
    }

    /// Non-blocking append path used from commit/abort critical sections.
    pub fn record_async(&self, draft: SsiDecisionCardDraft) {
        if let Some(tx) = &self.tx {
            let _ = self.pending.fetch_add(1, Ordering::AcqRel);
            if tx.send(draft.clone()).is_ok() {
                return;
            }
            let _ = self.pending.fetch_sub(1, Ordering::AcqRel);
        }
        self.record_sync(draft);
    }

    /// Immediate append fallback used when the async worker is unavailable.
    pub fn record_sync(&self, draft: SsiDecisionCardDraft) {
        with_locked_state(&self.state, |inner| inner.append(draft));
    }

    /// Return all retained cards in insertion order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<SsiDecisionCard> {
        self.await_quiescence(Duration::from_millis(25));
        with_locked_state(&self.state, |inner| inner.entries.iter().cloned().collect())
    }

    /// Return cards matching the given query.
    #[must_use]
    pub fn query(&self, query: &SsiDecisionQuery) -> Vec<SsiDecisionCard> {
        self.await_quiescence(Duration::from_millis(25));
        with_locked_state(&self.state, |inner| {
            inner
                .entries
                .iter()
                .filter(|entry| query.txn_id.is_none_or(|txn| entry.txn.id.get() == txn))
                .filter(|entry| {
                    query
                        .decision_type
                        .is_none_or(|decision_type| entry.decision_type == decision_type)
                })
                .filter(|entry| {
                    query
                        .timestamp_start_ns
                        .is_none_or(|start| entry.timestamp_unix_ns >= start)
                })
                .filter(|entry| {
                    query
                        .timestamp_end_ns
                        .is_none_or(|end| entry.timestamp_unix_ns <= end)
                })
                .cloned()
                .collect()
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.await_quiescence(Duration::from_millis(25));
        with_locked_state(&self.state, |inner| inner.entries.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn await_quiescence(&self, timeout: Duration) {
        let start = Instant::now();
        while self.pending.load(Ordering::Acquire) > 0 && start.elapsed() < timeout {
            thread::sleep(Duration::from_micros(50));
        }
    }
}

impl Default for SsiEvidenceLedger {
    fn default() -> Self {
        Self::new(1024)
    }
}

fn with_locked_state<T>(
    state: &Arc<Mutex<SsiEvidenceLedgerState>>,
    f: impl FnOnce(&mut SsiEvidenceLedgerState) -> T,
) -> T {
    match state.lock() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            f(&mut guard)
        }
    }
}

fn now_unix_ns() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    let nanos = duration.as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn read_set_fingerprint(read_set_pages: &[PageNumber]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:ssi_read_set_fingerprint:v1");
    for page in read_set_pages {
        hasher.update(&page.get().to_le_bytes());
    }
    let hash = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

#[allow(clippy::too_many_arguments)]
fn compute_chain_hash(
    previous_hash: [u8; 32],
    decision_type: SsiDecisionType,
    txn: TxnToken,
    snapshot_seq: CommitSeq,
    commit_seq: Option<CommitSeq>,
    decision_epoch: u64,
    timestamp_unix_ns: u64,
    conflicting_txns: &[TxnToken],
    conflict_pages: &[PageNumber],
    read_set_pages: &[PageNumber],
    write_set: &[PageNumber],
    rationale: &str,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:ssi_evidence_ledger:v1");
    hasher.update(&previous_hash);
    hasher.update(decision_type.as_str().as_bytes());
    hasher.update(&txn.id.get().to_le_bytes());
    hasher.update(&txn.epoch.get().to_le_bytes());
    hasher.update(&snapshot_seq.get().to_le_bytes());
    hasher.update(&commit_seq.map_or(0_u64, CommitSeq::get).to_le_bytes());
    hasher.update(&decision_epoch.to_le_bytes());
    hasher.update(&timestamp_unix_ns.to_le_bytes());

    for token in conflicting_txns {
        hasher.update(&token.id.get().to_le_bytes());
        hasher.update(&token.epoch.get().to_le_bytes());
    }
    for page in conflict_pages {
        hasher.update(&page.get().to_le_bytes());
    }
    for page in read_set_pages {
        hasher.update(&page.get().to_le_bytes());
    }
    for page in write_set {
        hasher.update(&page.get().to_le_bytes());
    }
    hasher.update(rationale.as_bytes());

    let hash = hasher.finalize();
    *hash.as_bytes()
}

fn hex_encode(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0_u8; 64];
    for (idx, byte) in bytes.iter().copied().enumerate() {
        out[idx * 2] = HEX[usize::from(byte >> 4)];
        out[idx * 2 + 1] = HEX[usize::from(byte & 0x0F)];
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// E-Process monitor for INV-SSI-FP (§5.7.3)
// ---------------------------------------------------------------------------

/// Configuration for the SSI false-positive e-process monitor.
#[derive(Debug, Clone, Copy)]
pub struct SsiFpMonitorConfig {
    /// Null hypothesis false-positive rate (e.g., 0.05 = 5%).
    pub p0: f64,
    /// Bet parameter (lambda) for the e-process.
    pub lambda: f64,
    /// Significance level alpha (reject when e-value > 1/alpha).
    pub alpha: f64,
    /// Maximum e-value (cap to prevent overflow).
    pub max_evalue: f64,
}

impl Default for SsiFpMonitorConfig {
    fn default() -> Self {
        Self {
            p0: 0.05,
            lambda: 0.3,
            alpha: 0.01,
            max_evalue: 1e12,
        }
    }
}

/// E-process monitor for tracking SSI false-positive rate.
///
/// Each observation is a binary: `true` = false positive, `false` = true positive.
/// The e-process multiplicatively updates with bet `lambda`:
///
/// `e_t = e_{t-1} * (1 + lambda * (X_t - p0))`
///
/// When `e_value > 1/alpha`, the null hypothesis (FP rate <= p0) is rejected.
#[derive(Debug, Clone)]
pub struct SsiFpMonitor {
    config: SsiFpMonitorConfig,
    e_value: f64,
    observations: u64,
    false_positives: u64,
    alert_triggered: bool,
}

impl SsiFpMonitor {
    #[must_use]
    pub fn new(config: SsiFpMonitorConfig) -> Self {
        Self {
            config,
            e_value: 1.0,
            observations: 0,
            false_positives: 0,
            alert_triggered: false,
        }
    }

    /// Observe one SSI abort outcome.
    ///
    /// `is_false_positive`: true if retrospective row-level replay shows the
    /// abort was unnecessary.
    pub fn observe(&mut self, is_false_positive: bool) {
        self.observations += 1;
        let x = if is_false_positive {
            self.false_positives += 1;
            1.0
        } else {
            0.0
        };

        // Multiplicative update: e_t = e_{t-1} * (1 + lambda * (X_t - p0))
        let factor = self.config.lambda.mul_add(x - self.config.p0, 1.0);
        self.e_value = (self.e_value * factor).min(self.config.max_evalue);

        // Clamp below 0 (can happen if p0 > 0 and we observe true positive).
        if self.e_value < 0.0 {
            self.e_value = 0.0;
        }

        // Check threshold.
        if self.e_value > 1.0 / self.config.alpha {
            self.alert_triggered = true;
        }
    }

    #[must_use]
    pub fn e_value(&self) -> f64 {
        self.e_value
    }

    #[must_use]
    pub fn observations(&self) -> u64 {
        self.observations
    }

    #[must_use]
    pub fn false_positives(&self) -> u64 {
        self.false_positives
    }

    #[must_use]
    pub fn alert_triggered(&self) -> bool {
        self.alert_triggered
    }

    /// The rejection threshold: 1/alpha.
    #[must_use]
    pub fn rejection_threshold(&self) -> f64 {
        1.0 / self.config.alpha
    }

    /// Observed false-positive rate.
    #[must_use]
    pub fn observed_fp_rate(&self) -> f64 {
        if self.observations == 0 {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        {
            self.false_positives as f64 / self.observations as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Conformal Calibrator for page-level coarseness (§5.7.3)
// ---------------------------------------------------------------------------

/// Configuration for conformal calibration.
#[derive(Debug, Clone, Copy)]
pub struct ConformalConfig {
    /// Coverage level (e.g., 0.05 for 95% coverage).
    pub alpha: f64,
    /// Minimum number of calibration samples before producing bounds.
    pub min_calibration_samples: usize,
}

impl Default for ConformalConfig {
    fn default() -> Self {
        Self {
            alpha: 0.05,
            min_calibration_samples: 30,
        }
    }
}

/// Conformal calibrator: produces distribution-free prediction intervals
/// for the page-level vs row-level abort rate difference.
#[derive(Debug, Clone)]
pub struct ConformalCalibrator {
    config: ConformalConfig,
    /// Calibration residuals (abort rate deltas).
    residuals: Vec<f64>,
}

impl ConformalCalibrator {
    #[must_use]
    pub fn new(config: ConformalConfig) -> Self {
        Self {
            config,
            residuals: Vec::new(),
        }
    }

    /// Add a calibration sample: the difference between page-level and
    /// row-level abort rates for a workload window.
    pub fn add_sample(&mut self, abort_rate_delta: f64) {
        self.residuals.push(abort_rate_delta);
    }

    /// Whether we have enough samples to produce a bound.
    #[must_use]
    pub fn is_calibrated(&self) -> bool {
        self.residuals.len() >= self.config.min_calibration_samples
    }

    /// The upper bound of the prediction interval.
    ///
    /// At coverage `1-alpha`, the conformal quantile is the `ceil((1-alpha)*(n+1))`-th
    /// order statistic. Returns `None` if not yet calibrated.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn upper_bound(&self) -> Option<f64> {
        if !self.is_calibrated() {
            return None;
        }
        let mut sorted = self.residuals.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        #[allow(clippy::cast_precision_loss)]
        let q_idx = ((1.0 - self.config.alpha) * (sorted.len() + 1) as f64).ceil() as usize;
        let idx = q_idx.min(sorted.len()) - 1;
        Some(sorted[idx])
    }

    /// Check whether a new observation is within the calibrated band.
    #[must_use]
    pub fn is_conforming(&self, abort_rate_delta: f64) -> Option<bool> {
        self.upper_bound().map(|ub| abort_rate_delta <= ub)
    }

    /// Number of calibration samples.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.residuals.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use fsqlite_types::{TxnEpoch, TxnId};

    use super::*;

    const BEAD_ID: &str = "bd-3t3.12";

    #[test]
    fn test_loss_matrix_abort_threshold() {
        // Test 1: Verify abort threshold = L_fp / (L_fp + L_miss).
        let default = LossMatrix::default();
        let threshold = default.abort_threshold();
        #[allow(clippy::approx_constant)]
        let expected = 1.0 / 1001.0;
        assert!(
            (threshold - expected).abs() < 1e-10,
            "bead_id={BEAD_ID} default_threshold={threshold} expected={expected}"
        );

        // Different ratios.
        let m2 = LossMatrix {
            l_miss: 100.0,
            l_fp: 10.0,
        };
        let t2 = m2.abort_threshold();
        assert!(
            (t2 - 10.0 / 110.0).abs() < 1e-10,
            "bead_id={BEAD_ID} ratio_100_10"
        );

        // Equal costs: threshold = 0.5.
        let m3 = LossMatrix {
            l_miss: 1.0,
            l_fp: 1.0,
        };
        assert!(
            (m3.abort_threshold() - 0.5).abs() < 1e-10,
            "bead_id={BEAD_ID} equal_costs"
        );
    }

    #[test]
    fn test_victim_selection_confirmed_cycle() {
        // Test 2: T1 committed, T3 committed → MUST abort T2.
        let pivot = TxnCost {
            write_set_size: 100,
            duration_us: 50_000,
        };
        let other = TxnCost {
            write_set_size: 1,
            duration_us: 100,
        };
        let decision = select_victim(CycleStatus::Confirmed, pivot, other);
        assert_eq!(
            decision.victim,
            Victim::Pivot,
            "bead_id={BEAD_ID} confirmed_aborts_pivot"
        );
        assert_eq!(decision.cycle_status, CycleStatus::Confirmed);
        assert!(decision.reason.contains("confirmed"));
    }

    #[test]
    fn test_victim_selection_potential_cycle_heavy_t3() {
        // Test 3: L(T2)=1, L(T3)=1000. Policy prefers aborting T2 (cheaper).
        let pivot = TxnCost {
            write_set_size: 1,
            duration_us: 0,
        };
        let other = TxnCost {
            write_set_size: 1000,
            duration_us: 0,
        };
        let decision = select_victim(CycleStatus::Potential, pivot, other);
        assert_eq!(
            decision.victim,
            Victim::Pivot,
            "bead_id={BEAD_ID} cheaper_pivot_aborted"
        );
        assert!(
            decision.pivot_cost < decision.other_cost,
            "bead_id={BEAD_ID} pivot_cost_lower"
        );
    }

    #[test]
    fn test_victim_selection_potential_cycle_cheaper_other() {
        let pivot = TxnCost {
            write_set_size: 1000,
            duration_us: 0,
        };
        let other = TxnCost {
            write_set_size: 1,
            duration_us: 0,
        };
        let decision = select_victim(CycleStatus::Potential, pivot, other);
        assert_eq!(
            decision.victim,
            Victim::Other,
            "bead_id={BEAD_ID} cheaper_other_aborted"
        );
        assert!(decision.reason.contains("cheaper_other"));
    }

    #[test]
    fn test_smarter_victim_selection() {
        let decision = select_victim(
            CycleStatus::Potential,
            TxnCost {
                write_set_size: 500,
                duration_us: 50_000,
            },
            TxnCost {
                write_set_size: 1,
                duration_us: 100,
            },
        );
        assert_eq!(
            decision.victim,
            Victim::Other,
            "bead_id={BEAD_ID} policy must consider abort cost, not always abort pivot"
        );
    }

    #[test]
    fn test_victim_selection_potential_cycle_equal_cost() {
        // Test 4: L(T2) ~ L(T3). Default: abort pivot T2.
        let cost = TxnCost {
            write_set_size: 50,
            duration_us: 10_000,
        };
        let decision = select_victim(CycleStatus::Potential, cost, cost);
        assert_eq!(
            decision.victim,
            Victim::Pivot,
            "bead_id={BEAD_ID} equal_cost_default_pivot"
        );
    }

    #[test]
    fn test_overapproximation_safety() {
        // Test 5: has_in_rw=true, has_out_rw=true, but T1 not yet committed
        // → still aborts (deliberate overapproximation). No false negative.
        let lm = LossMatrix::default();
        // Even tiny P(anomaly) exceeds threshold (1/1001 ~ 0.001).
        let p_anomaly = 0.01; // 1% chance — well above threshold.
        let envelope = AbortDecisionEnvelope::evaluate(true, true, p_anomaly, lm, None);
        assert_eq!(
            envelope.decision,
            AbortDecision::Abort,
            "bead_id={BEAD_ID} overapprox_aborts"
        );
    }

    #[test]
    fn test_eprocess_ssi_fp_monitor_under_threshold() {
        // Test 6: Feed 100 observations with FP rate=3%. E-process stays
        // below 1/alpha=100.
        let mut monitor = SsiFpMonitor::new(SsiFpMonitorConfig::default());
        for i in 0..100 {
            let is_fp = (i % 33) == 0; // ~3% FP rate.
            monitor.observe(is_fp);
        }
        assert!(
            monitor.e_value() < monitor.rejection_threshold(),
            "bead_id={BEAD_ID} under_threshold: e={} threshold={}",
            monitor.e_value(),
            monitor.rejection_threshold()
        );
        assert!(!monitor.alert_triggered(), "bead_id={BEAD_ID} no_alert");
    }

    #[test]
    fn test_eprocess_ssi_fp_monitor_exceeds_threshold() {
        // Test 7: Feed observations with FP rate=15%. E-process exceeds
        // 1/alpha=100.
        let mut monitor = SsiFpMonitor::new(SsiFpMonitorConfig {
            p0: 0.05,
            lambda: 0.3,
            alpha: 0.01,
            max_evalue: 1e12,
        });
        // 15% FP rate: 1 in ~7.
        for i in 0..200 {
            let is_fp = (i % 7) < 1; // ~14.3% FP rate.
            monitor.observe(is_fp);
        }
        assert!(
            monitor.alert_triggered(),
            "bead_id={BEAD_ID} alert_triggered: e={} threshold={}",
            monitor.e_value(),
            monitor.rejection_threshold()
        );
    }

    #[test]
    fn test_conformal_calibrator_within_band() {
        // Test 8: Page-level abort rate delta within calibrated band → conforming.
        let mut cal = ConformalCalibrator::new(ConformalConfig::default());
        // Calibration: deltas all between 0.01 and 0.05.
        for i in 0..30 {
            #[allow(clippy::cast_precision_loss)]
            let delta = 0.04f64.mul_add(f64::from(i) / 29.0, 0.01);
            cal.add_sample(delta);
        }
        assert!(cal.is_calibrated());
        let ub = cal.upper_bound().expect("calibrated");
        // Upper bound should be around 0.05.
        assert!(ub >= 0.04, "bead_id={BEAD_ID} upper_bound={ub}");

        // New observation within band.
        assert_eq!(
            cal.is_conforming(0.03),
            Some(true),
            "bead_id={BEAD_ID} within_band"
        );
    }

    #[test]
    fn test_conformal_calibrator_outside_band() {
        // Test 9: Page-level abort rate delta exceeds band → non-conforming.
        let mut cal = ConformalCalibrator::new(ConformalConfig::default());
        // Calibration: deltas between 0.01 and 0.03.
        for i in 0..30 {
            #[allow(clippy::cast_precision_loss)]
            let delta = 0.02f64.mul_add(f64::from(i) / 29.0, 0.01);
            cal.add_sample(delta);
        }
        assert!(cal.is_calibrated());

        // Observation way outside band.
        assert_eq!(
            cal.is_conforming(0.50),
            Some(false),
            "bead_id={BEAD_ID} outside_band"
        );
    }

    #[test]
    fn test_abort_decision_auditable_logging() {
        // Test 10: Verify abort decision logs all required fields.
        let lm = LossMatrix::default();
        let victim = select_victim(
            CycleStatus::Potential,
            TxnCost {
                write_set_size: 5,
                duration_us: 1000,
            },
            TxnCost {
                write_set_size: 50,
                duration_us: 10_000,
            },
        );
        let envelope = AbortDecisionEnvelope::evaluate(true, true, 0.5, lm, Some(victim));

        // All required fields present.
        assert!(envelope.has_in_rw);
        assert!(envelope.has_out_rw);
        assert!((envelope.p_anomaly - 0.5).abs() < 1e-10);
        assert!((envelope.threshold - lm.abort_threshold()).abs() < 1e-10);
        assert!(
            (envelope.expected_loss_commit - 500.0).abs() < 1e-10,
            "bead_id={BEAD_ID} el_commit={}",
            envelope.expected_loss_commit
        );
        assert!(
            (envelope.expected_loss_abort - 0.5).abs() < 1e-10,
            "bead_id={BEAD_ID} el_abort={}",
            envelope.expected_loss_abort
        );
        assert_eq!(envelope.decision, AbortDecision::Abort);
        let v = envelope.victim.expect("victim present");
        assert_eq!(v.victim, Victim::Pivot);
        assert!(
            !v.to_string().is_empty(),
            "bead_id={BEAD_ID} victim_display"
        );
    }

    fn token(raw: u64, epoch: u32) -> TxnToken {
        TxnToken::new(
            TxnId::new(raw).expect("token raw id must be non-zero"),
            TxnEpoch::new(epoch),
        )
    }

    fn page(raw: u32) -> PageNumber {
        PageNumber::new(raw).expect("page number must be non-zero")
    }

    #[test]
    fn test_ssi_evidence_ledger_append_only_chain_and_capacity() {
        let ledger = SsiEvidenceLedger::new(2);
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::CommitAllowed,
                token(1, 1),
                CommitSeq::new(10),
                Vec::new(),
                vec![page(7)],
                vec![page(1), page(2)],
                vec![page(7)],
                "clean_commit",
            )
            .with_commit_seq(CommitSeq::new(11))
            .with_timestamp_unix_ns(1_000),
        );
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortWriteSkew,
                token(2, 1),
                CommitSeq::new(11),
                vec![token(1, 1)],
                vec![page(7), page(9)],
                vec![page(7), page(8)],
                vec![page(9)],
                "pivot_in_out_rw",
            )
            .with_timestamp_unix_ns(2_000),
        );
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortCycle,
                token(3, 1),
                CommitSeq::new(12),
                vec![token(1, 1), token(2, 1)],
                vec![page(7)],
                vec![page(7)],
                vec![page(7)],
                "fcw_conflict",
            )
            .with_timestamp_unix_ns(3_000),
        );

        let cards = ledger.snapshot();
        assert_eq!(cards.len(), 2, "bead_id={BEAD_ID} bounded_capacity");
        assert_eq!(cards[0].txn.id.get(), 2);
        assert_eq!(cards[1].txn.id.get(), 3);
        assert_ne!(
            cards[0].chain_hash, cards[1].chain_hash,
            "bead_id={BEAD_ID} chain_hash_must_advance"
        );
    }

    #[test]
    fn test_ssi_evidence_ledger_query_filters() {
        let ledger = SsiEvidenceLedger::new(8);
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::CommitAllowed,
                token(10, 1),
                CommitSeq::new(20),
                Vec::new(),
                Vec::new(),
                vec![page(3), page(4)],
                vec![page(9)],
                "commit",
            )
            .with_timestamp_unix_ns(10_000),
        );
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortWriteSkew,
                token(11, 2),
                CommitSeq::new(20),
                vec![token(10, 1)],
                vec![page(9)],
                vec![page(9), page(10)],
                vec![page(11)],
                "pivot_abort",
            )
            .with_timestamp_unix_ns(20_000),
        );

        let by_txn = ledger.query(&SsiDecisionQuery {
            txn_id: Some(11),
            ..SsiDecisionQuery::default()
        });
        assert_eq!(by_txn.len(), 1);
        assert_eq!(by_txn[0].txn.id.get(), 11);

        let by_type = ledger.query(&SsiDecisionQuery {
            decision_type: Some(SsiDecisionType::AbortWriteSkew),
            ..SsiDecisionQuery::default()
        });
        assert_eq!(by_type.len(), 1);
        assert_eq!(by_type[0].decision_type, SsiDecisionType::AbortWriteSkew);

        let by_time = ledger.query(&SsiDecisionQuery {
            timestamp_start_ns: Some(15_000),
            timestamp_end_ns: Some(25_000),
            ..SsiDecisionQuery::default()
        });
        assert_eq!(by_time.len(), 1);
        assert_eq!(by_time[0].txn.id.get(), 11);
    }
}
